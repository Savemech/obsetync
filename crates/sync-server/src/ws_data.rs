//! Sealed WebSocket data lane for bounded bulk object RPCs.
//!
//! This is deliberately a sibling of the realtime socket: large object
//! traffic cannot head-of-line block root notifications, presence, or CRDT
//! frames. The lane reuses the existing single-use ticket and directional
//! AEAD keys, while the inner `OBW1` frame carries binary bulk payloads and a
//! request id. `OBW2` is an explicitly negotiated additive version which
//! fragments logical RPCs into small, independently sealed WebSocket frames.
//! No transfer state survives a disconnect; clients recover by
//! checking content addresses and may continue over bulk HTTP.

use crate::error::ServerError;
use crate::secure;
use crate::state::SharedState;
use crate::transport_memory::TransportMemoryReservation;
use crate::ws_ticket;
use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
    response::Response,
};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::collections::{HashMap, VecDeque};
use tokio_util::sync::CancellationToken;

const FRAME_MAGIC: &[u8; 4] = b"OBW1";
const FRAME_HEADER_BYTES: usize = 18;
const FRAME_V2_MAGIC: &[u8; 4] = b"OBW2";
const FRAME_V2_HEADER_BYTES: usize = 30;
const FRAGMENT_FIRST: u8 = 1;
const FRAGMENT_LAST: u8 = 2;
const HELLO_MAGIC: &[u8; 4] = b"OWH1";
const HELLO_ACK_MAGIC: &[u8; 4] = b"OWA1";
const HELLO_BYTES: usize = 24;
const HELLO_V2_MAGIC: &[u8; 4] = b"OWH2";
const HELLO_ACK_V2_MAGIC: &[u8; 4] = b"OWA2";
const HELLO_V2_BYTES: usize = 32;
const ERROR_MAGIC: &[u8; 4] = b"OWE1";
const ERROR_HEADER_BYTES: usize = 14;
const MAX_ERROR_MESSAGE_BYTES: usize = 256;
const AEAD_OVERHEAD_BYTES: usize = 12 + 16;

pub(crate) const MAX_PAYLOAD_BYTES: usize = crate::api::WS_FRAME_BYTES;
const MAX_SEALED_MESSAGE_BYTES: usize =
    FRAME_HEADER_BYTES + MAX_PAYLOAD_BYTES + AEAD_OVERHEAD_BYTES;
const MAX_INFLIGHT_REQUESTS: usize = 4;
const MAX_INFLIGHT_BYTES: usize = 32 * 1024 * 1024;
const MAX_FRAGMENT_PAYLOAD_BYTES: usize = 64 * 1024;
const MAX_FRAGMENTS: usize = 64;
/// Keep part of the negotiated window pending so CANCEL can retire work
/// before it enters storage. Total queued + running ownership is still bound
/// by the peer-negotiated request and byte credits.
const MAX_RUNNING_REQUESTS: usize = 2;
const HANDSHAKE_DEADLINE_SECS: u64 = 10;
const BUSY_RETRY_AFTER_MS: u32 = 25;
const MAX_CONTROL_FRAGMENT_BURST: usize = 4;
pub(crate) const WIRE_V1_VERSION: u16 = 1;
pub(crate) const WIRE_V2_VERSION: u16 = 2;
pub(crate) const WIRE_VERSIONS: [u16; 2] = [WIRE_V1_VERSION, WIRE_V2_VERSION];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum FrameType {
    Hello = 1,
    HelloAck = 2,
    CheckObjects = 3,
    CheckResult = 4,
    PutPack = 5,
    PutAck = 6,
    GetPack = 7,
    GetResult = 8,
    Cancel = 9,
    Error = 10,
}

impl TryFrom<u8> for FrameType {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, ProtocolError> {
        match value {
            1 => Ok(Self::Hello),
            2 => Ok(Self::HelloAck),
            3 => Ok(Self::CheckObjects),
            4 => Ok(Self::CheckResult),
            5 => Ok(Self::PutPack),
            6 => Ok(Self::PutAck),
            7 => Ok(Self::GetPack),
            8 => Ok(Self::GetResult),
            9 => Ok(Self::Cancel),
            10 => Ok(Self::Error),
            _ => Err(ProtocolError::new("unknown data frame type")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DecodedFrame<'a> {
    kind: FrameType,
    request_id: u64,
    payload: &'a [u8],
}

#[derive(Debug, Clone, Copy)]
struct DecodedFragment<'a> {
    kind: FrameType,
    request_id: u64,
    logical_len: usize,
    offset: usize,
    fragment_index: usize,
    fragment_count: usize,
    payload: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WireVersion {
    V1,
    V2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LaneLimits {
    max_payload_bytes: usize,
    max_inflight_requests: usize,
    max_inflight_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NegotiatedLane {
    wire: WireVersion,
    limits: LaneLimits,
    max_fragment_payload_bytes: usize,
    max_fragments: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProtocolError {
    message: &'static str,
}

impl ProtocolError {
    const fn new(message: &'static str) -> Self {
        Self { message }
    }
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for ProtocolError {}

#[derive(zeroize::Zeroize, zeroize::ZeroizeOnDrop)]
struct SealCtx {
    c2s: [u8; 32],
    s2c: [u8; 32],
    seq_in: u64,
    seq_out: u64,
}

impl SealCtx {
    fn from_ticket(ticket: &ws_ticket::WsTicket) -> Option<Self> {
        let mut c2s = [0u8; 32];
        let mut s2c = [0u8; 32];
        hex::decode_to_slice(&ticket.c2s_key_hex, &mut c2s).ok()?;
        hex::decode_to_slice(&ticket.s2c_key_hex, &mut s2c).ok()?;
        Some(Self {
            c2s,
            s2c,
            seq_in: 0,
            seq_out: 0,
        })
    }

    fn open(&mut self, frame: &[u8]) -> Option<Vec<u8>> {
        let plaintext = secure::ws_data_open(&self.c2s, "c2s", self.seq_in, frame).ok()?;
        self.seq_in = self.seq_in.checked_add(1)?;
        Some(plaintext)
    }

    fn seal(&mut self, plaintext: &[u8]) -> Option<Vec<u8>> {
        let frame = secure::ws_data_seal(&self.s2c, "s2c", self.seq_out, plaintext).ok()?;
        self.seq_out = self.seq_out.checked_add(1)?;
        Some(frame)
    }
}

struct RpcResponse {
    request_id: u64,
    kind: FrameType,
    payload: Vec<u8>,
}

struct ActiveRpc {
    cancellation: CancellationToken,
    cancel_requested: bool,
    task: tokio::task::JoinHandle<()>,
    owner: RpcOwner,
}

struct ReadyRpc {
    response: RpcResponse,
    owner: RpcOwner,
    priority: OutboundPriority,
    offset: usize,
    next_fragment_index: usize,
}

struct RpcOwner {
    telemetry: crate::perf::WsDataRpcGuard,
    _memory: TransportMemoryReservation,
}

struct QueuedRpc<T> {
    request_id: u64,
    kind: FrameType,
    payload: Vec<u8>,
    owner: T,
}

struct Reassembly<T> {
    request_id: u64,
    kind: FrameType,
    logical_len: usize,
    fragment_count: usize,
    next_fragment_index: usize,
    payload: Vec<u8>,
    owner: T,
}

#[derive(Debug, Clone, Copy)]
struct RejectedReassembly {
    kind: FrameType,
    logical_len: usize,
    fragment_count: usize,
    next_fragment_index: usize,
    next_offset: usize,
}

impl RejectedReassembly {
    fn start(fragment: DecodedFragment<'_>) -> Result<Self, ProtocolError> {
        if fragment.fragment_index != 0 {
            return Err(ProtocolError::new(
                "rejected fragmented request must start at index zero",
            ));
        }
        Ok(Self {
            kind: fragment.kind,
            logical_len: fragment.logical_len,
            fragment_count: fragment.fragment_count,
            next_fragment_index: 1,
            next_offset: fragment.payload.len(),
        })
    }

    fn push(&mut self, fragment: DecodedFragment<'_>) -> Result<bool, ProtocolError> {
        if fragment.kind != self.kind
            || fragment.logical_len != self.logical_len
            || fragment.fragment_count != self.fragment_count
            || fragment.fragment_index != self.next_fragment_index
            || fragment.offset != self.next_offset
        {
            return Err(ProtocolError::new("rejected fragment sequence mismatch"));
        }
        self.next_fragment_index += 1;
        self.next_offset = self
            .next_offset
            .checked_add(fragment.payload.len())
            .ok_or_else(|| ProtocolError::new("rejected fragment offset overflow"))?;
        Ok(self.is_complete())
    }

    fn is_complete(&self) -> bool {
        self.next_offset == self.logical_len && self.next_fragment_index == self.fragment_count
    }
}

fn start_rejected_reassembly(
    rejected: &mut HashMap<u64, RejectedReassembly>,
    fragment: DecodedFragment<'_>,
    limit: usize,
) -> Result<(), ProtocolError> {
    let state = RejectedReassembly::start(fragment)?;
    if state.is_complete() {
        return Ok(());
    }
    if rejected.len() >= limit || rejected.contains_key(&fragment.request_id) {
        return Err(ProtocolError::new("rejected reassembly capacity exceeded"));
    }
    rejected.insert(fragment.request_id, state);
    Ok(())
}

fn drain_rejected_fragment(
    rejected: &mut HashMap<u64, RejectedReassembly>,
    fragment: DecodedFragment<'_>,
) -> Result<bool, ProtocolError> {
    let Some(state) = rejected.get_mut(&fragment.request_id) else {
        return Ok(false);
    };
    if state.push(fragment)? {
        rejected.remove(&fragment.request_id);
    }
    Ok(true)
}

impl<T> Reassembly<T> {
    fn start(fragment: DecodedFragment<'_>, owner: T) -> Result<Self, ProtocolError> {
        if fragment.fragment_index != 0 {
            return Err(ProtocolError::new(
                "fragmented request must start at index zero",
            ));
        }
        let mut payload = Vec::with_capacity(fragment.logical_len);
        payload.extend_from_slice(fragment.payload);
        Ok(Self {
            request_id: fragment.request_id,
            kind: fragment.kind,
            logical_len: fragment.logical_len,
            fragment_count: fragment.fragment_count,
            next_fragment_index: 1,
            payload,
            owner,
        })
    }

    fn push(&mut self, fragment: DecodedFragment<'_>) -> Result<bool, ProtocolError> {
        if fragment.request_id != self.request_id
            || fragment.kind != self.kind
            || fragment.logical_len != self.logical_len
            || fragment.fragment_count != self.fragment_count
            || fragment.fragment_index != self.next_fragment_index
            || fragment.offset != self.payload.len()
        {
            return Err(ProtocolError::new("fragment sequence mismatch"));
        }
        self.payload.extend_from_slice(fragment.payload);
        self.next_fragment_index += 1;
        Ok(self.payload.len() == self.logical_len
            && self.next_fragment_index == self.fragment_count)
    }

    fn is_complete(&self) -> bool {
        self.payload.len() == self.logical_len && self.next_fragment_index == self.fragment_count
    }

    fn into_queued(self) -> QueuedRpc<T> {
        debug_assert!(self.is_complete());
        QueuedRpc {
            request_id: self.request_id,
            kind: self.kind,
            payload: self.payload,
            owner: self.owner,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RpcPhase {
    Assembling,
    Queued,
    Running,
    Sending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutboundLane {
    Control,
    Bulk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutboundPriority {
    Urgent,
    Control,
    Bulk,
}

fn next_outbound_lane(
    control_front: Option<OutboundPriority>,
    bulk_ready: usize,
    control_burst: usize,
) -> Option<OutboundLane> {
    if control_front == Some(OutboundPriority::Urgent)
        || control_front.is_some()
            && (bulk_ready == 0 || control_burst < MAX_CONTROL_FRAGMENT_BURST)
    {
        Some(OutboundLane::Control)
    } else if bulk_ready != 0 {
        Some(OutboundLane::Bulk)
    } else if control_front.is_some() {
        Some(OutboundLane::Control)
    } else {
        None
    }
}

fn response_priority(kind: FrameType, cancelled: bool) -> OutboundPriority {
    if cancelled {
        OutboundPriority::Urgent
    } else if matches!(kind, FrameType::CheckResult | FrameType::Error) {
        OutboundPriority::Control
    } else {
        OutboundPriority::Bulk
    }
}

/// Accounts the larger of live request and possible response ownership. A
/// tiny GET is therefore charged for the bounded object pack it can create,
/// not merely for its request hash list.
struct AdmissionLedger {
    max_requests: usize,
    max_bytes: usize,
    used_bytes: usize,
    entries: HashMap<u64, (usize, RpcPhase)>,
}

impl AdmissionLedger {
    fn new(limits: LaneLimits) -> Self {
        Self {
            max_requests: limits.max_inflight_requests,
            max_bytes: limits.max_inflight_bytes,
            used_bytes: 0,
            entries: HashMap::new(),
        }
    }

    fn admit(&mut self, request_id: u64, bytes: usize, phase: RpcPhase) -> bool {
        let Some(next) = self.used_bytes.checked_add(bytes) else {
            return false;
        };
        if self.entries.len() >= self.max_requests
            || next > self.max_bytes
            || self.entries.contains_key(&request_id)
        {
            return false;
        }
        self.used_bytes = next;
        self.entries.insert(request_id, (bytes, phase));
        true
    }

    fn mark_queued(&mut self, request_id: u64) {
        self.mark_phase(request_id, RpcPhase::Assembling, RpcPhase::Queued);
    }

    fn mark_running(&mut self, request_id: u64) {
        self.mark_phase(request_id, RpcPhase::Queued, RpcPhase::Running);
    }

    fn mark_sending(&mut self, request_id: u64) {
        let entry = self
            .entries
            .get_mut(&request_id)
            .expect("RPC must retain admission while sending");
        entry.1 = RpcPhase::Sending;
    }

    fn mark_phase(&mut self, request_id: u64, from: RpcPhase, to: RpcPhase) {
        let entry = self
            .entries
            .get_mut(&request_id)
            .expect("RPC must retain admission across phases");
        debug_assert_eq!(entry.1, from);
        entry.1 = to;
    }

    fn is_queued(&self, request_id: u64) -> bool {
        self.entries
            .get(&request_id)
            .is_some_and(|entry| entry.1 == RpcPhase::Queued)
    }

    fn is_assembling(&self, request_id: u64) -> bool {
        self.entries
            .get(&request_id)
            .is_some_and(|entry| entry.1 == RpcPhase::Assembling)
    }

    fn finish(&mut self, request_id: u64) -> bool {
        let Some((bytes, _)) = self.entries.remove(&request_id) else {
            return false;
        };
        self.used_bytes = self
            .used_bytes
            .checked_sub(bytes)
            .expect("RPC admission bytes underflow");
        true
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(u16)]
enum ErrorCode {
    InvalidRequest = 1,
    Busy = 2,
    Internal = 3,
    Cancelled = 4,
}

pub async fn ws_data_route(
    State(state): State<SharedState>,
    upgrade: WebSocketUpgrade,
) -> Response {
    upgrade
        .max_message_size(MAX_SEALED_MESSAGE_BYTES)
        .max_frame_size(MAX_SEALED_MESSAGE_BYTES)
        .on_upgrade(move |socket| data_entry(state, socket))
}

async fn data_entry(state: SharedState, socket: WebSocket) {
    let (mut sink, mut stream) = socket.split();
    let auth = tokio::time::timeout(
        std::time::Duration::from_secs(HANDSHAKE_DEADLINE_SECS),
        stream.next(),
    )
    .await;
    let ticket_hex = match auth {
        Ok(Some(Ok(Message::Text(text)))) => {
            let value = serde_json::from_str::<serde_json::Value>(&text).ok();
            value.and_then(|value| {
                (value.get("v").and_then(|item| item.as_u64()) == Some(2)
                    && value.get("t").and_then(|item| item.as_str()) == Some("auth"))
                .then(|| {
                    value
                        .get("ticket")
                        .and_then(|item| item.as_str())
                        .map(str::to_owned)
                })
                .flatten()
            })
        }
        _ => None,
    };
    let Some(ticket_hex) = ticket_hex else {
        let _ = sink
            .send(Message::Text("invalid data-lane auth".into()))
            .await;
        return;
    };
    let layout = state.layout.clone();
    let claimed = match state
        .control_io
        .run(move || ws_ticket::claim(&layout, &ticket_hex))
        .await
    {
        Ok(claimed) => claimed,
        Err(error) => {
            tracing::warn!(%error, "ws-data: ticket claim unavailable");
            let _ = sink
                .send(Message::Text("data-lane ticket unavailable".into()))
                .await;
            return;
        }
    };
    let Some(mut ticket) = claimed else {
        let _ = sink
            .send(Message::Text("invalid data-lane ticket".into()))
            .await;
        return;
    };
    let session_cancelled = state.devices.session_token(&ticket.device_id);
    if ticket.c2s_key_hex.is_empty() || ticket.s2c_key_hex.is_empty() || session_cancelled.is_none()
    {
        let _ = sink
            .send(Message::Text("data lane unavailable".into()))
            .await;
        return;
    }
    let session_cancelled = session_cancelled.expect("checked above");
    let device_short = ticket.device_id[..ticket.device_id.len().min(12)].to_owned();
    let Some(mut seal) = SealCtx::from_ticket(&ticket) else {
        return;
    };
    zeroize::Zeroize::zeroize(&mut ticket.c2s_key_hex);
    zeroize::Zeroize::zeroize(&mut ticket.s2c_key_hex);

    let Some(receive_bytes) = receive_reservation_bytes(MAX_PAYLOAD_BYTES) else {
        return;
    };
    let Ok(receive_memory) = state.transport_memory.try_reserve(receive_bytes) else {
        tracing::warn!(device = %device_short, "ws-data: process receive budget exhausted");
        return;
    };
    let hello = tokio::select! {
        _ = session_cancelled.cancelled() => return,
        result = tokio::time::timeout(
            std::time::Duration::from_secs(HANDSHAKE_DEADLINE_SECS),
            read_hello(&mut stream, &mut seal),
        ) => result,
    };
    let Ok(Some(requested)) = hello else {
        return;
    };
    let Some(negotiated) = negotiate_lane(requested) else {
        return;
    };
    let hello_ack = match encode_hello_ack(negotiated) {
        Ok(payload) => payload,
        Err(_) => return,
    };
    if send_logical_frame(
        &mut sink,
        &mut seal,
        negotiated,
        FrameType::HelloAck,
        0,
        &hello_ack,
    )
    .await
    .is_err()
    {
        return;
    }

    tracing::info!(
        device = %device_short,
        wire = ?negotiated.wire,
        max_payload_bytes = negotiated.limits.max_payload_bytes,
        max_inflight_requests = negotiated.limits.max_inflight_requests,
        max_inflight_bytes = negotiated.limits.max_inflight_bytes,
        "ws-data: sealed session ready"
    );
    run_session(
        state,
        sink,
        stream,
        seal,
        negotiated,
        &ticket.device_id,
        &device_short,
        session_cancelled,
        receive_memory,
    )
    .await;
}

async fn read_hello(
    stream: &mut SplitStream<WebSocket>,
    seal: &mut SealCtx,
) -> Option<NegotiatedLane> {
    while let Some(Ok(message)) = stream.next().await {
        match message {
            Message::Binary(data) => {
                let plaintext = seal.open(&data)?;
                // Version selection is the authenticated outer magic, never a
                // guessed payload shape. The ticket was already claimed, so a
                // failed OBW2 attempt can only downgrade with a fresh ticket,
                // fresh sequence space, and an explicit OBW1 hello.
                match plaintext.get(..4) {
                    Some(magic) if magic == FRAME_MAGIC => {
                        let frame = decode_frame(&plaintext, MAX_PAYLOAD_BYTES).ok()?;
                        if frame.kind != FrameType::Hello || frame.request_id != 0 {
                            return None;
                        }
                        return decode_hello(frame.payload)
                            .ok()
                            .map(|limits| NegotiatedLane {
                                wire: WireVersion::V1,
                                limits,
                                max_fragment_payload_bytes: limits.max_payload_bytes,
                                max_fragments: 1,
                            });
                    }
                    Some(magic) if magic == FRAME_V2_MAGIC => {
                        let fragment = decode_fragment(
                            &plaintext,
                            MAX_PAYLOAD_BYTES,
                            MAX_FRAGMENT_PAYLOAD_BYTES,
                            MAX_FRAGMENTS,
                        )
                        .ok()?;
                        if fragment.kind != FrameType::Hello
                            || fragment.request_id != 0
                            || fragment.fragment_count != 1
                        {
                            return None;
                        }
                        return decode_hello_v2(fragment.payload).ok();
                    }
                    _ => return None,
                }
            }
            Message::Close(_) => return None,
            Message::Ping(_) | Message::Pong(_) => {}
            _ => return None,
        }
    }
    None
}

async fn run_session(
    state: SharedState,
    mut sink: SplitSink<WebSocket, Message>,
    mut stream: SplitStream<WebSocket>,
    mut seal: SealCtx,
    negotiated: NegotiatedLane,
    owner_key: &str,
    device_short: &str,
    session_cancelled: CancellationToken,
    _receive_memory: TransportMemoryReservation,
) {
    let limits = negotiated.limits;
    let (response_tx, mut response_rx) =
        tokio::sync::mpsc::channel::<RpcResponse>(limits.max_inflight_requests);
    let mut assembling = HashMap::<u64, Reassembly<RpcOwner>>::new();
    let mut rejected = HashMap::<u64, RejectedReassembly>::new();
    let mut queued = VecDeque::<QueuedRpc<RpcOwner>>::new();
    let mut active = HashMap::<u64, ActiveRpc>::new();
    let mut control_ready = VecDeque::<ReadyRpc>::new();
    let mut bulk_ready = VecDeque::<ReadyRpc>::new();
    let mut admission = AdmissionLedger::new(limits);
    let mut last_request_id = 0u64;
    let mut control_fragment_burst = 0usize;
    let mut control_dispatch_burst = 0usize;

    loop {
        tokio::select! {
            biased;
            _ = session_cancelled.cancelled() => {
                break;
            }
            _ = std::future::ready(()), if next_outbound_lane(
                control_ready.front().map(|request| request.priority),
                bulk_ready.len(),
                control_fragment_burst,
            ) == Some(OutboundLane::Control) => {
                let priority = control_ready
                    .front()
                    .expect("guarded by scheduler")
                    .priority;
                let sending = state.perf.begin_ws_data_response();
                let Ok(completed) = send_ready_head(
                    &mut sink,
                    &mut seal,
                    negotiated,
                    &mut control_ready,
                ).await else { break };
                drop(sending);
                if priority == OutboundPriority::Control {
                    control_fragment_burst = control_fragment_burst
                        .saturating_add(1)
                        .min(MAX_CONTROL_FRAGMENT_BURST);
                }
                if completed {
                    finish_ready_head(&mut control_ready, &mut admission);
                    dispatch_queued(
                        &state,
                        owner_key,
                        &mut queued,
                        &mut control_dispatch_burst,
                        &mut active,
                        &mut admission,
                        &response_tx,
                        limits.max_payload_bytes,
                    );
                }
            }
            message = stream.next() => {
                match message {
                    Some(Ok(Message::Binary(data))) => {
                        let Some(mut plaintext) = seal.open(&data) else {
                            tracing::warn!(device = %device_short, "ws-data: AEAD frame rejected");
                            break;
                        };
                        if plaintext.get(..4) == Some(FRAME_MAGIC.as_slice())
                            && negotiated.wire != WireVersion::V1
                            || plaintext.get(..4) == Some(FRAME_V2_MAGIC.as_slice())
                                && negotiated.wire != WireVersion::V2
                        {
                            tracing::warn!(device = %device_short, "ws-data: wire version changed after hello");
                            break;
                        }

                        if negotiated.wire == WireVersion::V1 {
                            let Ok(frame) = decode_frame(&plaintext, limits.max_payload_bytes) else {
                                tracing::warn!(device = %device_short, "ws-data: malformed OBW1 frame");
                                break;
                            };
                            if frame.kind == FrameType::Cancel {
                                if !frame.payload.is_empty() || frame.request_id == 0 {
                                    break;
                                }
                                cancel_request(
                                    frame.request_id,
                                    &mut assembling,
                                    &mut queued,
                                    &mut active,
                                    &mut control_ready,
                                    &mut bulk_ready,
                                    &mut admission,
                                );
                                continue;
                            }
                            if !is_request_kind(frame.kind) || frame.request_id <= last_request_id {
                                tracing::warn!(device = %device_short, "ws-data: invalid or reused request id");
                                break;
                            }
                            last_request_id = frame.request_id;
                            let request_id = frame.request_id;
                            let request_kind = frame.kind;
                            let payload_len = frame.payload.len();
                            let Some(reserved_bytes) = rpc_reservation_bytes(
                                request_kind,
                                payload_len,
                                limits.max_payload_bytes,
                            ) else { break };
                            let process_memory = state.transport_memory.try_reserve(reserved_bytes);
                            if process_memory.is_err()
                                || !admission.admit(request_id, reserved_bytes, RpcPhase::Queued)
                            {
                                let payload = encode_error(ErrorCode::Busy, BUSY_RETRY_AFTER_MS, "data lane busy");
                                if send_logical_frame(
                                    &mut sink,
                                    &mut seal,
                                    negotiated,
                                    FrameType::Error,
                                    request_id,
                                    &payload,
                                ).await.is_err() { break; }
                                continue;
                            }
                            plaintext.copy_within(FRAME_HEADER_BYTES.., 0);
                            plaintext.truncate(payload_len);
                            queued.push_back(QueuedRpc {
                                request_id,
                                kind: request_kind,
                                payload: plaintext,
                                owner: RpcOwner {
                                    telemetry: state.perf.begin_ws_data_rpc(),
                                    _memory: process_memory.expect("checked process admission"),
                                },
                            });
                        } else {
                            let Ok(fragment) = decode_fragment(
                                &plaintext,
                                limits.max_payload_bytes,
                                negotiated.max_fragment_payload_bytes,
                                negotiated.max_fragments,
                            ) else {
                                tracing::warn!(device = %device_short, "ws-data: malformed OBW2 fragment");
                                break;
                            };
                            if fragment.kind == FrameType::Cancel {
                                if fragment.logical_len != 0 || fragment.request_id == 0 {
                                    break;
                                }
                                if rejected.remove(&fragment.request_id).is_some() {
                                    continue;
                                }
                                cancel_request(
                                    fragment.request_id,
                                    &mut assembling,
                                    &mut queued,
                                    &mut active,
                                    &mut control_ready,
                                    &mut bulk_ready,
                                    &mut admission,
                                );
                                continue;
                            }
                            match drain_rejected_fragment(&mut rejected, fragment) {
                                Ok(true) => continue,
                                Ok(false) => {}
                                Err(_) => break,
                            }
                            if let Some(reassembly) = assembling.get_mut(&fragment.request_id) {
                                let completed = match reassembly.push(fragment) {
                                    Ok(completed) => completed,
                                    Err(_) => break,
                                };
                                if completed {
                                    let request = assembling
                                        .remove(&fragment.request_id)
                                        .expect("completed reassembly must remain owned")
                                        .into_queued();
                                    admission.mark_queued(fragment.request_id);
                                    queued.push_back(request);
                                }
                            } else {
                                if !is_request_kind(fragment.kind)
                                    || fragment.fragment_index != 0
                                    || fragment.request_id <= last_request_id
                                {
                                    tracing::warn!(device = %device_short, "ws-data: invalid, gapped, or reused OBW2 request");
                                    break;
                                }
                                last_request_id = fragment.request_id;
                                let Some(reserved_bytes) = rpc_reservation_bytes(
                                    fragment.kind,
                                    fragment.logical_len,
                                    limits.max_payload_bytes,
                                ) else { break };
                                // Both budgets are acquired before the logical payload Vec is
                                // allocated, so malicious fragment metadata cannot allocate first.
                                let process_memory = state.transport_memory.try_reserve(reserved_bytes);
                                if process_memory.is_err()
                                    || !admission.admit(
                                        fragment.request_id,
                                        reserved_bytes,
                                        RpcPhase::Assembling,
                                    )
                                {
                                    if start_rejected_reassembly(
                                        &mut rejected,
                                        fragment,
                                        limits
                                            .max_inflight_requests
                                            .min(MAX_INFLIGHT_REQUESTS),
                                    )
                                    .is_err()
                                    {
                                        break;
                                    }
                                    let payload = encode_error(ErrorCode::Busy, BUSY_RETRY_AFTER_MS, "data lane busy");
                                    if send_logical_frame(
                                        &mut sink,
                                        &mut seal,
                                        negotiated,
                                        FrameType::Error,
                                        fragment.request_id,
                                        &payload,
                                    ).await.is_err() { break; }
                                    continue;
                                }
                                let request_id = fragment.request_id;
                                let reassembly = match Reassembly::start(
                                    fragment,
                                    RpcOwner {
                                        telemetry: state.perf.begin_ws_data_rpc(),
                                        _memory: process_memory.expect("checked process admission"),
                                    },
                                ) {
                                    Ok(reassembly) => reassembly,
                                    Err(_) => break,
                                };
                                if reassembly.is_complete() {
                                    admission.mark_queued(request_id);
                                    queued.push_back(reassembly.into_queued());
                                } else {
                                    assembling.insert(request_id, reassembly);
                                }
                            }
                        }
                        dispatch_queued(
                            &state,
                            owner_key,
                            &mut queued,
                            &mut control_dispatch_burst,
                            &mut active,
                            &mut admission,
                            &response_tx,
                            limits.max_payload_bytes,
                        );
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if sink.send(Message::Pong(payload)).await.is_err() { break; }
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {
                        tracing::warn!(device = %device_short, "ws-data: non-binary message after auth");
                        break;
                    }
                    Some(Err(_)) => break,
                }
            }
            response = response_rx.recv() => {
                let Some(mut response) = response else { break };
                let Some(active_rpc) = active.remove(&response.request_id) else { continue };
                let cancelled = active_rpc.cancel_requested;
                response = honor_cancel(response, cancelled);
                admission.mark_sending(response.request_id);
                let ready = ReadyRpc {
                    priority: response_priority(response.kind, cancelled),
                    response,
                    owner: active_rpc.owner,
                    offset: 0,
                    next_fragment_index: 0,
                };
                match ready.priority {
                    OutboundPriority::Urgent => control_ready.push_front(ready),
                    OutboundPriority::Control => control_ready.push_back(ready),
                    OutboundPriority::Bulk => bulk_ready.push_back(ready),
                }
            }
            _ = std::future::ready(()), if next_outbound_lane(
                control_ready.front().map(|request| request.priority),
                bulk_ready.len(),
                control_fragment_burst,
            ) == Some(OutboundLane::Bulk) => {
                let sending = state.perf.begin_ws_data_response();
                let Ok(completed) = send_ready_head(
                    &mut sink,
                    &mut seal,
                    negotiated,
                    &mut bulk_ready,
                ).await else { break };
                drop(sending);
                control_fragment_burst = 0;
                if completed {
                    finish_ready_head(&mut bulk_ready, &mut admission);
                    dispatch_queued(
                        &state,
                        owner_key,
                        &mut queued,
                        &mut control_dispatch_burst,
                        &mut active,
                        &mut admission,
                        &response_tx,
                        limits.max_payload_bytes,
                    );
                }
            }
        }
    }

    for (_, reassembly) in assembling {
        reassembly.owner.telemetry.finish(false);
    }
    for queued_rpc in queued {
        queued_rpc.owner.telemetry.finish(false);
    }
    for ready in control_ready.into_iter().chain(bulk_ready) {
        ready.owner.telemetry.finish(false);
    }
    for (_, rpc) in active {
        rpc.cancellation.cancel();
        rpc.task.abort();
    }
    tracing::info!(device = %device_short, "ws-data: session closed");
}

fn dispatch_queued(
    state: &SharedState,
    owner_key: &str,
    queued: &mut VecDeque<QueuedRpc<RpcOwner>>,
    control_burst: &mut usize,
    active: &mut HashMap<u64, ActiveRpc>,
    admission: &mut AdmissionLedger,
    response_tx: &tokio::sync::mpsc::Sender<RpcResponse>,
    max_payload_bytes: usize,
) {
    while active.len() < MAX_RUNNING_REQUESTS {
        let Some(queued_rpc) = take_next_queued(queued, control_burst) else {
            break;
        };
        let QueuedRpc {
            request_id,
            kind: request_kind,
            payload,
            owner,
        } = queued_rpc;
        admission.mark_running(request_id);
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task_state = state.clone();
        let task_owner = owner_key.to_owned();
        let task_tx = response_tx.clone();
        let task = tokio::spawn(async move {
            let response = tokio::select! {
                biased;
                _ = task_cancellation.cancelled() => RpcResponse {
                    request_id,
                    kind: FrameType::Error,
                    payload: encode_error(ErrorCode::Cancelled, 0, "request cancelled"),
                },
                response = execute_rpc(
                    &task_state,
                    &task_owner,
                    request_id,
                    request_kind,
                    payload,
                    max_payload_bytes,
                ) => response,
            };
            let _ = task_tx.send(response).await;
        });
        active.insert(
            request_id,
            ActiveRpc {
                cancellation,
                cancel_requested: false,
                task,
                owner,
            },
        );
    }
}

fn take_queued<T>(queued: &mut VecDeque<QueuedRpc<T>>, request_id: u64) -> Option<QueuedRpc<T>> {
    let position = queued
        .iter()
        .position(|request| request.request_id == request_id)?;
    queued.remove(position)
}

fn take_next_queued<T>(
    queued: &mut VecDeque<QueuedRpc<T>>,
    control_burst: &mut usize,
) -> Option<QueuedRpc<T>> {
    let control = queued
        .iter()
        .position(|request| request.kind == FrameType::CheckObjects);
    let bulk = queued
        .iter()
        .position(|request| request.kind != FrameType::CheckObjects);
    let position = match (control, bulk) {
        (Some(control), Some(_)) if *control_burst < MAX_CONTROL_FRAGMENT_BURST => {
            *control_burst += 1;
            control
        }
        (Some(_), Some(bulk)) => {
            *control_burst = 0;
            bulk
        }
        (Some(control), None) => {
            *control_burst = (*control_burst + 1).min(MAX_CONTROL_FRAGMENT_BURST);
            control
        }
        (None, Some(bulk)) => {
            *control_burst = 0;
            bulk
        }
        (None, None) => return None,
    };
    queued.remove(position)
}

fn take_ready(ready: &mut VecDeque<ReadyRpc>, request_id: u64) -> Option<ReadyRpc> {
    let position = ready
        .iter()
        .position(|request| request.response.request_id == request_id)?;
    ready.remove(position)
}

fn take_reassembly<T>(
    assembling: &mut HashMap<u64, Reassembly<T>>,
    request_id: u64,
) -> Option<Reassembly<T>> {
    assembling.remove(&request_id)
}

fn is_request_kind(kind: FrameType) -> bool {
    matches!(
        kind,
        FrameType::CheckObjects | FrameType::PutPack | FrameType::GetPack
    )
}

fn cancelled_ready(request_id: u64, owner: RpcOwner, message: &'static str) -> ReadyRpc {
    ReadyRpc {
        response: RpcResponse {
            request_id,
            kind: FrameType::Error,
            payload: encode_error(ErrorCode::Cancelled, 0, message),
        },
        owner,
        priority: OutboundPriority::Urgent,
        offset: 0,
        next_fragment_index: 0,
    }
}

#[allow(clippy::too_many_arguments)]
fn cancel_request(
    request_id: u64,
    assembling: &mut HashMap<u64, Reassembly<RpcOwner>>,
    queued: &mut VecDeque<QueuedRpc<RpcOwner>>,
    active: &mut HashMap<u64, ActiveRpc>,
    control_ready: &mut VecDeque<ReadyRpc>,
    bulk_ready: &mut VecDeque<ReadyRpc>,
    admission: &mut AdmissionLedger,
) {
    if admission.is_assembling(request_id) {
        let request = take_reassembly(assembling, request_id)
            .expect("assembling admission must own a reassembly");
        admission.mark_sending(request_id);
        control_ready.push_front(cancelled_ready(
            request_id,
            request.owner,
            "request cancelled during reassembly",
        ));
    } else if admission.is_queued(request_id) {
        let request =
            take_queued(queued, request_id).expect("queued admission must own a queued request");
        admission.mark_sending(request_id);
        control_ready.push_front(cancelled_ready(
            request_id,
            request.owner,
            "request cancelled before storage dispatch",
        ));
    } else if let Some(rpc) = active.get_mut(&request_id) {
        rpc.cancel_requested = true;
        rpc.cancellation.cancel();
    } else if let Some(request) =
        take_ready(bulk_ready, request_id).or_else(|| take_ready(control_ready, request_id))
    {
        control_ready.push_front(cancelled_ready(
            request_id,
            request.owner,
            "request cancelled during response",
        ));
    }
}

async fn send_ready_head(
    sink: &mut SplitSink<WebSocket, Message>,
    seal: &mut SealCtx,
    negotiated: NegotiatedLane,
    ready: &mut VecDeque<ReadyRpc>,
) -> Result<bool, ()> {
    let request = ready.front_mut().expect("guarded by non-empty queue");
    match negotiated.wire {
        WireVersion::V1 => {
            if request.next_fragment_index != 0 {
                return Err(());
            }
            send_v1_frame(
                sink,
                seal,
                request.response.kind,
                request.response.request_id,
                &request.response.payload,
                negotiated.limits.max_payload_bytes,
            )
            .await?;
            request.offset = request.response.payload.len();
            request.next_fragment_index = 1;
            Ok(true)
        }
        WireVersion::V2 => {
            let total = request.response.payload.len();
            let fragment_count =
                fragment_count(total, negotiated.max_fragment_payload_bytes).ok_or(())?;
            if fragment_count > negotiated.max_fragments {
                return Err(());
            }
            let end = request
                .offset
                .checked_add(negotiated.max_fragment_payload_bytes)
                .map(|end| end.min(total))
                .ok_or(())?;
            let plaintext = encode_fragment(
                request.response.kind,
                request.response.request_id,
                total,
                request.offset,
                request.next_fragment_index,
                fragment_count,
                &request.response.payload[request.offset..end],
                negotiated.max_fragment_payload_bytes,
                negotiated.max_fragments,
            )
            .map_err(|_| ())?;
            send_plaintext(sink, seal, &plaintext).await?;
            request.offset = end;
            request.next_fragment_index += 1;
            Ok(request.next_fragment_index == fragment_count)
        }
    }
}

fn finish_ready_head(ready: &mut VecDeque<ReadyRpc>, admission: &mut AdmissionLedger) {
    let request = ready
        .pop_front()
        .expect("completed response must remain queued");
    assert!(admission.finish(request.response.request_id));
    request
        .owner
        .telemetry
        .finish(request.response.kind != FrameType::Error);
}

fn rpc_reservation_bytes(
    kind: FrameType,
    request_bytes: usize,
    max_payload_bytes: usize,
) -> Option<usize> {
    let response_bytes = match kind {
        // GET can turn a tiny hash list into a full negotiated response pack.
        FrameType::GetPack => max_payload_bytes,
        // CHECK bitmaps and PUT status arrays are smaller than a valid input;
        // retain enough space for a bounded error even for malformed input.
        FrameType::CheckObjects | FrameType::PutPack => {
            ERROR_HEADER_BYTES.checked_add(MAX_ERROR_MESSAGE_BYTES)?
        }
        _ => return None,
    };
    Some(request_bytes.max(response_bytes))
}

pub(crate) fn receive_reservation_bytes(max_payload_bytes: usize) -> Option<usize> {
    let opened = FRAME_HEADER_BYTES.checked_add(max_payload_bytes)?;
    let sealed = opened.checked_add(AEAD_OVERHEAD_BYTES)?;
    opened.checked_add(sealed)
}

async fn execute_rpc(
    state: &SharedState,
    owner_key: &str,
    request_id: u64,
    request_kind: FrameType,
    payload: Vec<u8>,
    max_payload_bytes: usize,
) -> RpcResponse {
    let result = match request_kind {
        FrameType::CheckObjects => {
            crate::api::process_bulk_check(state, payload.into(), max_payload_bytes)
                .await
                .map(|payload| (FrameType::CheckResult, payload))
        }
        FrameType::PutPack => {
            crate::api::process_bulk_put(state, owner_key, payload.into(), max_payload_bytes)
                .await
                .map(|payload| (FrameType::PutAck, payload))
        }
        FrameType::GetPack => crate::api::process_bulk_get(
            state,
            payload.into(),
            max_payload_bytes,
            max_payload_bytes,
        )
        .await
        .map(|payload| (FrameType::GetResult, payload)),
        _ => Err(ServerError::BadRequest("unsupported data RPC".into())),
    };
    match result {
        Ok((kind, payload)) if payload.len() <= max_payload_bytes => RpcResponse {
            request_id,
            kind,
            payload,
        },
        Ok(_) => RpcResponse {
            request_id,
            kind: FrameType::Error,
            payload: encode_error(ErrorCode::Internal, 0, "response exceeds frame limit"),
        },
        Err(error) => error_response(request_id, error),
    }
}

fn error_response(request_id: u64, error: ServerError) -> RpcResponse {
    let (code, retry_after_ms, message) = match error {
        ServerError::BadRequest(_) | ServerError::PayloadTooLarge(_) => {
            (ErrorCode::InvalidRequest, 0, "invalid data RPC")
        }
        ServerError::ServiceUnavailable(_) => {
            (ErrorCode::Busy, BUSY_RETRY_AFTER_MS, "storage busy")
        }
        _ => (ErrorCode::Internal, 0, "data RPC failed"),
    };
    RpcResponse {
        request_id,
        kind: FrameType::Error,
        payload: encode_error(code, retry_after_ms, message),
    }
}

fn honor_cancel(mut response: RpcResponse, cancel_requested: bool) -> RpcResponse {
    if cancel_requested {
        response.kind = FrameType::Error;
        response.payload = encode_error(ErrorCode::Cancelled, 0, "request cancelled");
    }
    response
}

async fn send_plaintext(
    sink: &mut SplitSink<WebSocket, Message>,
    seal: &mut SealCtx,
    plaintext: &[u8],
) -> Result<(), ()> {
    let sealed = seal.seal(plaintext).ok_or(())?;
    sink.send(Message::Binary(sealed.into()))
        .await
        .map_err(|_| ())
}

async fn send_v1_frame(
    sink: &mut SplitSink<WebSocket, Message>,
    seal: &mut SealCtx,
    kind: FrameType,
    request_id: u64,
    payload: &[u8],
    max_payload_bytes: usize,
) -> Result<(), ()> {
    let plaintext = encode_frame(kind, request_id, payload, max_payload_bytes).map_err(|_| ())?;
    send_plaintext(sink, seal, &plaintext).await
}

async fn send_logical_frame(
    sink: &mut SplitSink<WebSocket, Message>,
    seal: &mut SealCtx,
    negotiated: NegotiatedLane,
    kind: FrameType,
    request_id: u64,
    payload: &[u8],
) -> Result<(), ()> {
    match negotiated.wire {
        WireVersion::V1 => {
            send_v1_frame(
                sink,
                seal,
                kind,
                request_id,
                payload,
                negotiated.limits.max_payload_bytes,
            )
            .await
        }
        WireVersion::V2 => {
            let count =
                fragment_count(payload.len(), negotiated.max_fragment_payload_bytes).ok_or(())?;
            if count != 1 {
                return Err(());
            }
            let plaintext = encode_fragment(
                kind,
                request_id,
                payload.len(),
                0,
                0,
                1,
                payload,
                negotiated.max_fragment_payload_bytes,
                negotiated.max_fragments,
            )
            .map_err(|_| ())?;
            send_plaintext(sink, seal, &plaintext).await
        }
    }
}

fn decode_frame(input: &[u8], max_payload_bytes: usize) -> Result<DecodedFrame<'_>, ProtocolError> {
    if input.len() < FRAME_HEADER_BYTES {
        return Err(ProtocolError::new("truncated data frame header"));
    }
    if input.get(..4) != Some(FRAME_MAGIC.as_slice()) {
        return Err(ProtocolError::new("invalid data frame magic"));
    }
    let kind = FrameType::try_from(input[4])?;
    if input[5] != 0 {
        return Err(ProtocolError::new("unsupported data frame flags"));
    }
    let request_id = u64::from_le_bytes(
        input[6..14]
            .try_into()
            .map_err(|_| ProtocolError::new("truncated request id"))?,
    );
    let payload_len = u32::from_le_bytes(
        input[14..18]
            .try_into()
            .map_err(|_| ProtocolError::new("truncated payload length"))?,
    ) as usize;
    if payload_len > max_payload_bytes {
        return Err(ProtocolError::new("data frame payload exceeds limit"));
    }
    let expected = FRAME_HEADER_BYTES
        .checked_add(payload_len)
        .ok_or_else(|| ProtocolError::new("data frame length overflow"))?;
    if input.len() != expected {
        return Err(ProtocolError::new("data frame length mismatch"));
    }
    Ok(DecodedFrame {
        kind,
        request_id,
        payload: &input[FRAME_HEADER_BYTES..],
    })
}

fn encode_frame(
    kind: FrameType,
    request_id: u64,
    payload: &[u8],
    max_payload_bytes: usize,
) -> Result<Vec<u8>, ProtocolError> {
    if payload.len() > max_payload_bytes {
        return Err(ProtocolError::new("data frame payload exceeds limit"));
    }
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| ProtocolError::new("data frame payload length overflow"))?;
    let capacity = FRAME_HEADER_BYTES
        .checked_add(payload.len())
        .ok_or_else(|| ProtocolError::new("data frame length overflow"))?;
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(FRAME_MAGIC);
    output.push(kind as u8);
    output.push(0);
    output.extend_from_slice(&request_id.to_le_bytes());
    output.extend_from_slice(&payload_len.to_le_bytes());
    output.extend_from_slice(payload);
    Ok(output)
}

fn fragment_count(logical_len: usize, max_fragment_payload_bytes: usize) -> Option<usize> {
    if max_fragment_payload_bytes == 0 {
        return None;
    }
    if logical_len == 0 {
        return Some(1);
    }
    logical_len
        .checked_add(max_fragment_payload_bytes - 1)
        .map(|rounded| rounded / max_fragment_payload_bytes)
}

fn decode_fragment(
    input: &[u8],
    max_logical_bytes: usize,
    max_fragment_payload_bytes: usize,
    max_fragments: usize,
) -> Result<DecodedFragment<'_>, ProtocolError> {
    if input.len() < FRAME_V2_HEADER_BYTES {
        return Err(ProtocolError::new("truncated OBW2 fragment header"));
    }
    if input.get(..4) != Some(FRAME_V2_MAGIC.as_slice()) {
        return Err(ProtocolError::new("invalid OBW2 fragment magic"));
    }
    let kind = FrameType::try_from(input[4])?;
    let flags = input[5];
    if flags & !(FRAGMENT_FIRST | FRAGMENT_LAST) != 0 {
        return Err(ProtocolError::new("unsupported OBW2 fragment flags"));
    }
    let request_id = u64::from_le_bytes(input[6..14].try_into().unwrap());
    let fragment_len = u32::from_le_bytes(input[14..18].try_into().unwrap()) as usize;
    let logical_len = u32::from_le_bytes(input[18..22].try_into().unwrap()) as usize;
    let offset = u32::from_le_bytes(input[22..26].try_into().unwrap()) as usize;
    let fragment_index = u16::from_le_bytes(input[26..28].try_into().unwrap()) as usize;
    let fragment_count_value = u16::from_le_bytes(input[28..30].try_into().unwrap()) as usize;
    if logical_len > max_logical_bytes
        || fragment_len > max_fragment_payload_bytes
        || fragment_count_value == 0
        || fragment_count_value > max_fragments
    {
        return Err(ProtocolError::new(
            "OBW2 fragment exceeds negotiated limits",
        ));
    }
    let canonical_count = fragment_count(logical_len, max_fragment_payload_bytes)
        .ok_or_else(|| ProtocolError::new("invalid OBW2 fragment cap"))?;
    let canonical_offset = fragment_index
        .checked_mul(max_fragment_payload_bytes)
        .ok_or_else(|| ProtocolError::new("OBW2 fragment offset overflow"))?;
    let canonical_len = logical_len
        .checked_sub(canonical_offset)
        .map(|remaining| remaining.min(max_fragment_payload_bytes))
        .ok_or_else(|| ProtocolError::new("OBW2 fragment starts past logical payload"))?;
    let canonical_flags = (if fragment_index == 0 {
        FRAGMENT_FIRST
    } else {
        0
    }) | (if fragment_index + 1 == fragment_count_value {
        FRAGMENT_LAST
    } else {
        0
    });
    if fragment_count_value != canonical_count
        || fragment_index >= fragment_count_value
        || offset != canonical_offset
        || fragment_len != canonical_len
        || flags != canonical_flags
    {
        return Err(ProtocolError::new("non-canonical OBW2 fragment metadata"));
    }
    let expected = FRAME_V2_HEADER_BYTES
        .checked_add(fragment_len)
        .ok_or_else(|| ProtocolError::new("OBW2 fragment length overflow"))?;
    if input.len() != expected {
        return Err(ProtocolError::new("OBW2 fragment length mismatch"));
    }
    Ok(DecodedFragment {
        kind,
        request_id,
        logical_len,
        offset,
        fragment_index,
        fragment_count: fragment_count_value,
        payload: &input[FRAME_V2_HEADER_BYTES..],
    })
}

#[allow(clippy::too_many_arguments)]
fn encode_fragment(
    kind: FrameType,
    request_id: u64,
    logical_len: usize,
    offset: usize,
    fragment_index: usize,
    fragment_count_value: usize,
    payload: &[u8],
    max_fragment_payload_bytes: usize,
    max_fragments: usize,
) -> Result<Vec<u8>, ProtocolError> {
    let logical = u32::try_from(logical_len)
        .map_err(|_| ProtocolError::new("OBW2 logical length overflow"))?;
    let offset_u32 =
        u32::try_from(offset).map_err(|_| ProtocolError::new("OBW2 offset overflow"))?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| ProtocolError::new("OBW2 fragment length overflow"))?;
    let index = u16::try_from(fragment_index)
        .map_err(|_| ProtocolError::new("OBW2 fragment index overflow"))?;
    let count = u16::try_from(fragment_count_value)
        .map_err(|_| ProtocolError::new("OBW2 fragment count overflow"))?;
    if fragment_count_value == 0 || fragment_count_value > max_fragments {
        return Err(ProtocolError::new("OBW2 fragment count exceeds limit"));
    }
    let flags = (if fragment_index == 0 {
        FRAGMENT_FIRST
    } else {
        0
    }) | (if fragment_index + 1 == fragment_count_value {
        FRAGMENT_LAST
    } else {
        0
    });
    let capacity = FRAME_V2_HEADER_BYTES
        .checked_add(payload.len())
        .ok_or_else(|| ProtocolError::new("OBW2 fragment length overflow"))?;
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(FRAME_V2_MAGIC);
    output.push(kind as u8);
    output.push(flags);
    output.extend_from_slice(&request_id.to_le_bytes());
    output.extend_from_slice(&payload_len.to_le_bytes());
    output.extend_from_slice(&logical.to_le_bytes());
    output.extend_from_slice(&offset_u32.to_le_bytes());
    output.extend_from_slice(&index.to_le_bytes());
    output.extend_from_slice(&count.to_le_bytes());
    output.extend_from_slice(payload);
    decode_fragment(
        &output,
        logical_len,
        max_fragment_payload_bytes,
        max_fragments,
    )?;
    Ok(output)
}

fn decode_hello(payload: &[u8]) -> Result<LaneLimits, ProtocolError> {
    if payload.len() != HELLO_BYTES || payload.get(..4) != Some(HELLO_MAGIC.as_slice()) {
        return Err(ProtocolError::new("invalid data-lane hello"));
    }
    if u16::from_le_bytes(payload[4..6].try_into().unwrap()) != WIRE_V1_VERSION
        || payload[12..16] != [0, 0, 0, 0]
    {
        return Err(ProtocolError::new("unsupported data-lane hello"));
    }
    Ok(LaneLimits {
        max_inflight_requests: u16::from_le_bytes(payload[6..8].try_into().unwrap()) as usize,
        max_payload_bytes: u32::from_le_bytes(payload[8..12].try_into().unwrap()) as usize,
        max_inflight_bytes: usize::try_from(u64::from_le_bytes(
            payload[16..24].try_into().unwrap(),
        ))
        .map_err(|_| ProtocolError::new("hello byte budget exceeds platform range"))?,
    })
}

fn decode_hello_v2(payload: &[u8]) -> Result<NegotiatedLane, ProtocolError> {
    if payload.len() != HELLO_V2_BYTES || payload.get(..4) != Some(HELLO_V2_MAGIC.as_slice()) {
        return Err(ProtocolError::new("invalid OBW2 data-lane hello"));
    }
    if u16::from_le_bytes(payload[4..6].try_into().unwrap()) != WIRE_V2_VERSION
        || payload[12..16] != [0, 0, 0, 0]
        || payload[30..32] != [0, 0]
    {
        return Err(ProtocolError::new("unsupported OBW2 data-lane hello"));
    }
    Ok(NegotiatedLane {
        wire: WireVersion::V2,
        limits: LaneLimits {
            max_inflight_requests: u16::from_le_bytes(payload[6..8].try_into().unwrap()) as usize,
            max_payload_bytes: u32::from_le_bytes(payload[8..12].try_into().unwrap()) as usize,
            max_inflight_bytes: usize::try_from(u64::from_le_bytes(
                payload[16..24].try_into().unwrap(),
            ))
            .map_err(|_| ProtocolError::new("hello byte budget exceeds platform range"))?,
        },
        max_fragment_payload_bytes: u32::from_le_bytes(payload[24..28].try_into().unwrap())
            as usize,
        max_fragments: u16::from_le_bytes(payload[28..30].try_into().unwrap()) as usize,
    })
}

fn encode_hello(magic: &[u8; 4], limits: LaneLimits) -> Result<Vec<u8>, ProtocolError> {
    let requests = u16::try_from(limits.max_inflight_requests)
        .map_err(|_| ProtocolError::new("hello request cap overflow"))?;
    let payload = u32::try_from(limits.max_payload_bytes)
        .map_err(|_| ProtocolError::new("hello payload cap overflow"))?;
    let bytes = u64::try_from(limits.max_inflight_bytes)
        .map_err(|_| ProtocolError::new("hello byte cap overflow"))?;
    let mut output = Vec::with_capacity(HELLO_BYTES);
    output.extend_from_slice(magic);
    output.extend_from_slice(&WIRE_V1_VERSION.to_le_bytes());
    output.extend_from_slice(&requests.to_le_bytes());
    output.extend_from_slice(&payload.to_le_bytes());
    output.extend_from_slice(&0u32.to_le_bytes());
    output.extend_from_slice(&bytes.to_le_bytes());
    Ok(output)
}

fn encode_hello_v2(magic: &[u8; 4], negotiated: NegotiatedLane) -> Result<Vec<u8>, ProtocolError> {
    let requests = u16::try_from(negotiated.limits.max_inflight_requests)
        .map_err(|_| ProtocolError::new("hello request cap overflow"))?;
    let logical = u32::try_from(negotiated.limits.max_payload_bytes)
        .map_err(|_| ProtocolError::new("hello logical cap overflow"))?;
    let bytes = u64::try_from(negotiated.limits.max_inflight_bytes)
        .map_err(|_| ProtocolError::new("hello byte cap overflow"))?;
    let fragment = u32::try_from(negotiated.max_fragment_payload_bytes)
        .map_err(|_| ProtocolError::new("hello fragment cap overflow"))?;
    let fragments = u16::try_from(negotiated.max_fragments)
        .map_err(|_| ProtocolError::new("hello fragment count overflow"))?;
    let mut output = Vec::with_capacity(HELLO_V2_BYTES);
    output.extend_from_slice(magic);
    output.extend_from_slice(&WIRE_V2_VERSION.to_le_bytes());
    output.extend_from_slice(&requests.to_le_bytes());
    output.extend_from_slice(&logical.to_le_bytes());
    output.extend_from_slice(&0u32.to_le_bytes());
    output.extend_from_slice(&bytes.to_le_bytes());
    output.extend_from_slice(&fragment.to_le_bytes());
    output.extend_from_slice(&fragments.to_le_bytes());
    output.extend_from_slice(&0u16.to_le_bytes());
    Ok(output)
}

fn encode_hello_ack(negotiated: NegotiatedLane) -> Result<Vec<u8>, ProtocolError> {
    match negotiated.wire {
        WireVersion::V1 => encode_hello(HELLO_ACK_MAGIC, negotiated.limits),
        WireVersion::V2 => encode_hello_v2(HELLO_ACK_V2_MAGIC, negotiated),
    }
}

fn negotiate_lane(requested: NegotiatedLane) -> Option<NegotiatedLane> {
    match requested.wire {
        WireVersion::V1 => negotiate_limits(requested.limits).map(|limits| NegotiatedLane {
            wire: WireVersion::V1,
            limits,
            max_fragment_payload_bytes: limits.max_payload_bytes,
            max_fragments: 1,
        }),
        WireVersion::V2 => {
            if requested.max_fragment_payload_bytes < 1024 || requested.max_fragments == 0 {
                return None;
            }
            let fragment = requested
                .max_fragment_payload_bytes
                .min(MAX_FRAGMENT_PAYLOAD_BYTES);
            let fragments = requested.max_fragments.min(MAX_FRAGMENTS);
            let representable = fragment.checked_mul(fragments)?;
            let mut limits = requested.limits;
            limits.max_payload_bytes = limits
                .max_payload_bytes
                .min(MAX_PAYLOAD_BYTES)
                .min(representable);
            let limits = negotiate_limits(limits)?;
            Some(NegotiatedLane {
                wire: WireVersion::V2,
                limits,
                max_fragment_payload_bytes: fragment,
                max_fragments: fragments,
            })
        }
    }
}

fn negotiate_limits(requested: LaneLimits) -> Option<LaneLimits> {
    if requested.max_payload_bytes < 1024
        || requested.max_inflight_requests == 0
        || requested.max_inflight_bytes < requested.max_payload_bytes
    {
        return None;
    }
    Some(LaneLimits {
        max_payload_bytes: requested.max_payload_bytes.min(MAX_PAYLOAD_BYTES),
        max_inflight_requests: requested.max_inflight_requests.min(MAX_INFLIGHT_REQUESTS),
        max_inflight_bytes: requested.max_inflight_bytes.min(MAX_INFLIGHT_BYTES),
    })
}

fn encode_error(code: ErrorCode, retry_after_ms: u32, message: &str) -> Vec<u8> {
    let bytes = message.as_bytes();
    let bytes = &bytes[..bytes.len().min(MAX_ERROR_MESSAGE_BYTES)];
    let message_len = bytes.len() as u16;
    let mut output = Vec::with_capacity(ERROR_HEADER_BYTES + bytes.len());
    output.extend_from_slice(ERROR_MAGIC);
    output.extend_from_slice(&(code as u16).to_le_bytes());
    output.extend_from_slice(&0u16.to_le_bytes());
    output.extend_from_slice(&retry_after_ms.to_le_bytes());
    output.extend_from_slice(&message_len.to_le_bytes());
    output.extend_from_slice(bytes);
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn binary_frame_round_trip_preserves_request_and_payload() {
        let encoded = encode_frame(FrameType::PutPack, 42, b"binary\0payload", 1024).unwrap();
        let decoded = decode_frame(&encoded, 1024).unwrap();
        assert_eq!(decoded.kind, FrameType::PutPack);
        assert_eq!(decoded.request_id, 42);
        assert_eq!(decoded.payload, b"binary\0payload");
    }

    #[test]
    fn obw1_encoding_is_byte_for_byte_stable() {
        let encoded =
            encode_frame(FrameType::CheckObjects, 0x0102_0304_0506_0708, b"xy", 1024).unwrap();
        assert_eq!(
            encoded,
            vec![b'O', b'B', b'W', b'1', 3, 0, 8, 7, 6, 5, 4, 3, 2, 1, 2, 0, 0, 0, b'x', b'y',]
        );
    }

    #[test]
    fn frame_decoder_rejects_all_structural_mismatches() {
        let valid = encode_frame(FrameType::CheckObjects, 1, b"x", 1024).unwrap();
        assert!(decode_frame(&valid[..FRAME_HEADER_BYTES - 1], 1024).is_err());
        let mut bad_magic = valid.clone();
        bad_magic[0] = b'X';
        assert!(decode_frame(&bad_magic, 1024).is_err());
        let mut bad_type = valid.clone();
        bad_type[4] = 255;
        assert!(decode_frame(&bad_type, 1024).is_err());
        let mut bad_flags = valid.clone();
        bad_flags[5] = 1;
        assert!(decode_frame(&bad_flags, 1024).is_err());
        let mut bad_length = valid.clone();
        bad_length[14..18].copy_from_slice(&2u32.to_le_bytes());
        assert!(decode_frame(&bad_length, 1024).is_err());
        let mut trailing = valid;
        trailing.push(0);
        assert!(decode_frame(&trailing, 1024).is_err());
    }

    #[test]
    fn payload_cap_is_checked_before_returning_a_slice() {
        let mut header = vec![0u8; FRAME_HEADER_BYTES];
        header[..4].copy_from_slice(FRAME_MAGIC);
        header[4] = FrameType::PutPack as u8;
        header[6..14].copy_from_slice(&1u64.to_le_bytes());
        header[14..18].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_frame(&header, MAX_PAYLOAD_BYTES).is_err());
    }

    #[test]
    fn hello_negotiation_uses_the_lower_cap_on_every_dimension() {
        let requested = LaneLimits {
            max_payload_bytes: 8 * 1024 * 1024,
            max_inflight_requests: 16,
            max_inflight_bytes: 64 * 1024 * 1024,
        };
        let encoded = encode_hello(HELLO_MAGIC, requested).unwrap();
        let decoded = decode_hello(&encoded).unwrap();
        assert_eq!(decoded, requested);
        assert_eq!(
            negotiate_limits(decoded),
            Some(LaneLimits {
                max_payload_bytes: MAX_PAYLOAD_BYTES,
                max_inflight_requests: MAX_INFLIGHT_REQUESTS,
                max_inflight_bytes: MAX_INFLIGHT_BYTES,
            })
        );
    }

    #[test]
    fn hello_rejects_zero_credit_and_impossible_byte_budget() {
        assert!(negotiate_limits(LaneLimits {
            max_payload_bytes: 1024,
            max_inflight_requests: 0,
            max_inflight_bytes: 1024,
        })
        .is_none());
        assert!(negotiate_limits(LaneLimits {
            max_payload_bytes: 2048,
            max_inflight_requests: 1,
            max_inflight_bytes: 1024,
        })
        .is_none());
    }

    #[test]
    fn hello_versions_are_explicit_and_never_inferred() {
        let v1_limits = LaneLimits {
            max_payload_bytes: MAX_PAYLOAD_BYTES,
            max_inflight_requests: MAX_INFLIGHT_REQUESTS,
            max_inflight_bytes: MAX_INFLIGHT_BYTES,
        };
        let v1_hello = encode_hello(HELLO_MAGIC, v1_limits).unwrap();
        assert!(decode_hello_v2(&v1_hello).is_err());

        let v2_requested = NegotiatedLane {
            wire: WireVersion::V2,
            limits: v1_limits,
            max_fragment_payload_bytes: MAX_FRAGMENT_PAYLOAD_BYTES,
            max_fragments: MAX_FRAGMENTS,
        };
        let v2_hello = encode_hello_v2(HELLO_V2_MAGIC, v2_requested).unwrap();
        assert!(decode_hello(&v2_hello).is_err());
        assert_eq!(decode_hello_v2(&v2_hello).unwrap(), v2_requested);

        let v1_outer = encode_frame(FrameType::Hello, 0, &v1_hello, MAX_PAYLOAD_BYTES).unwrap();
        assert!(decode_fragment(
            &v1_outer,
            MAX_PAYLOAD_BYTES,
            MAX_FRAGMENT_PAYLOAD_BYTES,
            MAX_FRAGMENTS,
        )
        .is_err());
        let v2_outer = encode_fragment(
            FrameType::Hello,
            0,
            v2_hello.len(),
            0,
            0,
            1,
            &v2_hello,
            MAX_FRAGMENT_PAYLOAD_BYTES,
            MAX_FRAGMENTS,
        )
        .unwrap();
        assert!(decode_frame(&v2_outer, MAX_PAYLOAD_BYTES).is_err());
    }

    #[test]
    fn obw2_negotiation_caps_every_fragment_dimension() {
        let requested = NegotiatedLane {
            wire: WireVersion::V2,
            limits: LaneLimits {
                max_payload_bytes: 8 * 1024 * 1024,
                max_inflight_requests: 16,
                max_inflight_bytes: 64 * 1024 * 1024,
            },
            max_fragment_payload_bytes: 128 * 1024,
            max_fragments: 128,
        };
        assert_eq!(
            negotiate_lane(requested),
            Some(NegotiatedLane {
                wire: WireVersion::V2,
                limits: LaneLimits {
                    max_payload_bytes: MAX_PAYLOAD_BYTES,
                    max_inflight_requests: MAX_INFLIGHT_REQUESTS,
                    max_inflight_bytes: MAX_INFLIGHT_BYTES,
                },
                max_fragment_payload_bytes: MAX_FRAGMENT_PAYLOAD_BYTES,
                max_fragments: MAX_FRAGMENTS,
            })
        );
    }

    #[test]
    fn obw2_reassembles_maximum_payload_from_canonical_bounded_fragments() {
        let payload: Vec<u8> = (0..MAX_PAYLOAD_BYTES).map(|index| index as u8).collect();
        let count = fragment_count(payload.len(), MAX_FRAGMENT_PAYLOAD_BYTES).unwrap();
        assert_eq!(count, MAX_FRAGMENTS);
        let mut reassembly = None;
        for index in 0..count {
            let offset = index * MAX_FRAGMENT_PAYLOAD_BYTES;
            let end = (offset + MAX_FRAGMENT_PAYLOAD_BYTES).min(payload.len());
            let encoded = encode_fragment(
                FrameType::PutPack,
                71,
                payload.len(),
                offset,
                index,
                count,
                &payload[offset..end],
                MAX_FRAGMENT_PAYLOAD_BYTES,
                MAX_FRAGMENTS,
            )
            .unwrap();
            assert!(encoded.len() <= FRAME_V2_HEADER_BYTES + MAX_FRAGMENT_PAYLOAD_BYTES);
            let decoded = decode_fragment(
                &encoded,
                MAX_PAYLOAD_BYTES,
                MAX_FRAGMENT_PAYLOAD_BYTES,
                MAX_FRAGMENTS,
            )
            .unwrap();
            if index == 0 {
                reassembly = Some(Reassembly::start(decoded, ()).unwrap());
            } else {
                let completed = reassembly.as_mut().unwrap().push(decoded).unwrap();
                assert_eq!(completed, index + 1 == count);
            }
        }
        let complete = reassembly.unwrap();
        assert!(complete.is_complete());
        assert_eq!(complete.into_queued().payload, payload);
    }

    #[test]
    fn obw2_rejects_duplicate_gap_overflow_and_noncanonical_metadata() {
        let logical_len = MAX_FRAGMENT_PAYLOAD_BYTES * 2 + 1;
        let bytes = vec![7u8; logical_len];
        let make = |index: usize| {
            let offset = index * MAX_FRAGMENT_PAYLOAD_BYTES;
            let end = (offset + MAX_FRAGMENT_PAYLOAD_BYTES).min(logical_len);
            encode_fragment(
                FrameType::PutPack,
                9,
                logical_len,
                offset,
                index,
                3,
                &bytes[offset..end],
                MAX_FRAGMENT_PAYLOAD_BYTES,
                MAX_FRAGMENTS,
            )
            .unwrap()
        };
        let first = make(0);
        let second = make(1);
        let third = make(2);
        let mut reassembly = Reassembly::start(
            decode_fragment(
                &first,
                MAX_PAYLOAD_BYTES,
                MAX_FRAGMENT_PAYLOAD_BYTES,
                MAX_FRAGMENTS,
            )
            .unwrap(),
            (),
        )
        .unwrap();
        let third_decoded = decode_fragment(
            &third,
            MAX_PAYLOAD_BYTES,
            MAX_FRAGMENT_PAYLOAD_BYTES,
            MAX_FRAGMENTS,
        )
        .unwrap();
        assert!(reassembly.push(third_decoded).is_err(), "gap was accepted");
        let second_decoded = decode_fragment(
            &second,
            MAX_PAYLOAD_BYTES,
            MAX_FRAGMENT_PAYLOAD_BYTES,
            MAX_FRAGMENTS,
        )
        .unwrap();
        assert!(!reassembly.push(second_decoded).unwrap());
        assert!(
            reassembly.push(second_decoded).is_err(),
            "duplicate was accepted"
        );
        assert!(reassembly.push(third_decoded).unwrap());

        let mut wrong_offset = second.clone();
        wrong_offset[22..26].copy_from_slice(&1u32.to_le_bytes());
        assert!(decode_fragment(
            &wrong_offset,
            MAX_PAYLOAD_BYTES,
            MAX_FRAGMENT_PAYLOAD_BYTES,
            MAX_FRAGMENTS,
        )
        .is_err());
        let mut wrong_count = first.clone();
        wrong_count[28..30].copy_from_slice(&65u16.to_le_bytes());
        assert!(decode_fragment(
            &wrong_count,
            MAX_PAYLOAD_BYTES,
            MAX_FRAGMENT_PAYLOAD_BYTES,
            MAX_FRAGMENTS,
        )
        .is_err());
        let mut wrong_flags = first;
        wrong_flags[5] = FRAGMENT_LAST;
        assert!(decode_fragment(
            &wrong_flags,
            MAX_PAYLOAD_BYTES,
            MAX_FRAGMENT_PAYLOAD_BYTES,
            MAX_FRAGMENTS,
        )
        .is_err());
    }

    #[test]
    fn cancel_control_preempts_between_two_bulk_fragments() {
        let mut bulk_fragments = 2usize;
        assert_eq!(
            next_outbound_lane(None, bulk_fragments, 0),
            Some(OutboundLane::Bulk)
        );
        bulk_fragments -= 1; // first bulk WebSocket frame completed

        assert_eq!(
            next_outbound_lane(
                Some(OutboundPriority::Urgent),
                bulk_fragments,
                MAX_CONTROL_FRAGMENT_BURST,
            ),
            Some(OutboundLane::Control),
            "CANCEL did not preempt bulk even after the normal control burst"
        );
        assert_eq!(
            next_outbound_lane(None, bulk_fragments, MAX_CONTROL_FRAGMENT_BURST),
            Some(OutboundLane::Bulk)
        );
    }

    #[test]
    fn rejected_fragment_tombstones_are_bounded_canonical_and_multiplex_safe() {
        let payload = vec![7u8; 2 * MAX_FRAGMENT_PAYLOAD_BYTES + 1];
        let encoded = |request_id, index| {
            encode_fragment(
                FrameType::PutPack,
                request_id,
                payload.len(),
                index * MAX_FRAGMENT_PAYLOAD_BYTES,
                index,
                3,
                &payload[index * MAX_FRAGMENT_PAYLOAD_BYTES
                    ..((index + 1) * MAX_FRAGMENT_PAYLOAD_BYTES).min(payload.len())],
                MAX_FRAGMENT_PAYLOAD_BYTES,
                MAX_FRAGMENTS,
            )
            .unwrap()
        };
        fn decode(bytes: &[u8]) -> DecodedFragment<'_> {
            decode_fragment(
                bytes,
                MAX_PAYLOAD_BYTES,
                MAX_FRAGMENT_PAYLOAD_BYTES,
                MAX_FRAGMENTS,
            )
            .unwrap()
        }

        let first = encoded(10, 0);
        let tail = encoded(10, 1);
        let last = encoded(10, 2);
        let unrelated = encoded(11, 0);
        let mut rejected = HashMap::new();
        start_rejected_reassembly(&mut rejected, decode(&first), 2).unwrap();
        assert_eq!(rejected.len(), 1, "Busy must create one metadata tombstone");
        assert!(!drain_rejected_fragment(&mut rejected, decode(&unrelated)).unwrap());
        assert!(drain_rejected_fragment(&mut rejected, decode(&tail)).unwrap());
        assert_eq!(rejected.len(), 1);
        assert!(drain_rejected_fragment(&mut rejected, decode(&last)).unwrap());
        assert!(rejected.is_empty(), "LAST did not retire rejected metadata");

        start_rejected_reassembly(&mut rejected, decode(&first), 2).unwrap();
        assert!(drain_rejected_fragment(&mut rejected, decode(&last)).is_err());
        assert_eq!(
            rejected.len(),
            1,
            "gapped tail escaped fail-closed ownership"
        );
        rejected.clear();

        let first_20 = encoded(20, 0);
        let first_21 = encoded(21, 0);
        let first_22 = encoded(22, 0);
        start_rejected_reassembly(&mut rejected, decode(&first_20), 2).unwrap();
        start_rejected_reassembly(&mut rejected, decode(&first_21), 2).unwrap();
        assert!(start_rejected_reassembly(&mut rejected, decode(&first_22), 2).is_err());
        assert_eq!(
            rejected.len(),
            2,
            "missing tails exceeded the negotiated cap"
        );
        assert!(
            rejected.remove(&20).is_some(),
            "CANCEL could not retire a tombstone"
        );
    }

    #[test]
    fn check_result_preempts_at_fragment_boundary_but_bulk_cannot_starve() {
        assert_eq!(
            response_priority(FrameType::CheckResult, false),
            OutboundPriority::Control
        );
        assert_eq!(
            response_priority(FrameType::PutAck, false),
            OutboundPriority::Bulk
        );

        let mut bulk_fragments = 3usize;
        let mut check_fragments = 0usize;
        let mut control_burst = 0usize;
        let mut order = Vec::new();
        assert_eq!(
            next_outbound_lane(None, bulk_fragments, control_burst),
            Some(OutboundLane::Bulk)
        );
        bulk_fragments -= 1;
        order.push("bulk");

        check_fragments = 6; // completed CHECK arrives after the first bulk frame
        while bulk_fragments + check_fragments > 0 {
            let lane = next_outbound_lane(
                (check_fragments > 0).then_some(OutboundPriority::Control),
                bulk_fragments,
                control_burst,
            )
            .expect("work remains");
            match lane {
                OutboundLane::Control => {
                    check_fragments -= 1;
                    control_burst = (control_burst + 1).min(MAX_CONTROL_FRAGMENT_BURST);
                    order.push("check");
                }
                OutboundLane::Bulk => {
                    bulk_fragments -= 1;
                    control_burst = 0;
                    order.push("bulk");
                }
            }
        }
        assert_eq!(
            order,
            ["bulk", "check", "check", "check", "check", "bulk", "check", "check", "bulk",],
            "semantic control did not preempt or bounded fairness did not resume bulk"
        );
    }

    #[test]
    fn check_request_dispatch_is_semantic_and_bulk_bounded() {
        let mut queued = VecDeque::new();
        let mut push = |request_id, kind| {
            queued.push_back(QueuedRpc {
                request_id,
                kind,
                payload: Vec::new(),
                owner: (),
            });
        };
        push(1, FrameType::PutPack);
        for request_id in 2..=7 {
            push(request_id, FrameType::CheckObjects);
        }
        push(8, FrameType::GetPack);

        let mut burst = 0;
        let order: Vec<u64> = std::iter::from_fn(|| {
            take_next_queued(&mut queued, &mut burst).map(|request| request.request_id)
        })
        .collect();
        assert_eq!(
            order,
            [2, 3, 4, 5, 1, 6, 7, 8],
            "CheckObjects did not bypass bulk or bounded dispatch starved it"
        );
    }

    #[test]
    fn cancel_wins_race_with_already_completed_worker_response() {
        let response = honor_cancel(
            RpcResponse {
                request_id: 88,
                kind: FrameType::GetResult,
                payload: vec![7; 2 * MAX_FRAGMENT_PAYLOAD_BYTES],
            },
            true,
        );
        assert_eq!(response.request_id, 88);
        assert_eq!(response.kind, FrameType::Error);
        assert_eq!(response.payload.get(..4), Some(ERROR_MAGIC.as_slice()));
        assert!(response.payload.len() < MAX_FRAGMENT_PAYLOAD_BYTES);
    }

    #[test]
    fn error_payload_is_strictly_bounded() {
        let encoded = encode_error(ErrorCode::Internal, 0, &"x".repeat(1000));
        assert_eq!(encoded.len(), ERROR_HEADER_BYTES + MAX_ERROR_MESSAGE_BYTES);
        assert_eq!(
            u16::from_le_bytes(encoded[12..14].try_into().unwrap()) as usize,
            MAX_ERROR_MESSAGE_BYTES
        );
    }

    #[test]
    fn response_ownership_is_included_in_negotiated_byte_credit() {
        assert_eq!(
            receive_reservation_bytes(4096),
            Some((FRAME_HEADER_BYTES + 4096) * 2 + AEAD_OVERHEAD_BYTES),
            "receive slot did not cover sealed plus opened ownership",
        );
        assert!(
            receive_reservation_bytes(MAX_PAYLOAD_BYTES).unwrap()
                < crate::transport_memory::PROCESS_MEMORY_BYTES
        );
        assert_eq!(
            rpc_reservation_bytes(FrameType::GetPack, 49, 4 * 1024 * 1024),
            Some(4 * 1024 * 1024),
            "tiny GET was charged only for its request instead of its possible pack",
        );
        assert_eq!(
            rpc_reservation_bytes(FrameType::PutPack, 4096, 4 * 1024 * 1024),
            Some(4096),
        );
        assert_eq!(
            rpc_reservation_bytes(FrameType::CheckObjects, 1, 4096),
            Some(ERROR_HEADER_BYTES + MAX_ERROR_MESSAGE_BYTES),
        );
        assert_eq!(rpc_reservation_bytes(FrameType::Cancel, 0, 4096), None);
    }

    #[test]
    fn ledger_bounds_queued_running_and_response_lifetime() {
        let mut ledger = AdmissionLedger::new(LaneLimits {
            max_payload_bytes: 1024,
            max_inflight_requests: 4,
            max_inflight_bytes: 2048,
        });
        assert!(ledger.admit(1, 1024, RpcPhase::Queued));
        assert!(ledger.admit(2, 1024, RpcPhase::Queued));
        assert!(
            !ledger.admit(3, 1, RpcPhase::Queued),
            "byte cap admitted a third owner"
        );
        ledger.mark_running(1);
        assert!(!ledger.is_queued(1));
        assert!(ledger.is_queued(2));
        assert!(ledger.finish(1));
        assert_eq!(ledger.used_bytes, 1024);
        assert!(ledger.admit(3, 1024, RpcPhase::Queued));
        assert!(!ledger.finish(99));
        assert!(ledger.finish(2));
        assert!(ledger.finish(3));
        assert_eq!(ledger.used_bytes, 0);
        assert!(ledger.entries.is_empty());
    }

    #[test]
    fn fragmented_response_keeps_admission_until_last_frame() {
        let mut ledger = AdmissionLedger::new(LaneLimits {
            max_payload_bytes: 2 * MAX_FRAGMENT_PAYLOAD_BYTES,
            max_inflight_requests: 1,
            max_inflight_bytes: 2 * MAX_FRAGMENT_PAYLOAD_BYTES,
        });
        assert!(ledger.admit(10, 2 * MAX_FRAGMENT_PAYLOAD_BYTES, RpcPhase::Queued));
        ledger.mark_running(10);
        ledger.mark_sending(10);

        assert!(
            !ledger.admit(11, 1, RpcPhase::Queued),
            "first response fragment released request or byte admission early"
        );
        assert_eq!(ledger.used_bytes, 2 * MAX_FRAGMENT_PAYLOAD_BYTES);

        assert!(ledger.finish(10)); // only after the second/last frame is sent
        assert!(ledger.admit(11, 1, RpcPhase::Queued));
    }

    #[test]
    fn queued_cancel_removes_and_drops_payload_owner_without_dispatch() {
        struct DropOwner(Arc<AtomicUsize>);
        impl Drop for DropOwner {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::AcqRel);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let mut queued = VecDeque::from([
            QueuedRpc {
                request_id: 1,
                kind: FrameType::GetPack,
                payload: vec![1; 1024],
                owner: DropOwner(Arc::clone(&drops)),
            },
            QueuedRpc {
                request_id: 2,
                kind: FrameType::PutPack,
                payload: vec![2; 2048],
                owner: DropOwner(Arc::clone(&drops)),
            },
        ]);
        let cancelled = take_queued(&mut queued, 2).expect("queued request was not cancellable");
        assert_eq!(cancelled.request_id, 2);
        assert_eq!(queued.front().unwrap().request_id, 1);
        assert_eq!(drops.load(Ordering::Acquire), 0);
        drop(cancelled);
        assert_eq!(drops.load(Ordering::Acquire), 1);
        assert!(take_queued(&mut queued, 2).is_none());
        drop(queued);
        assert_eq!(drops.load(Ordering::Acquire), 2);
    }

    #[test]
    fn cancel_during_reassembly_releases_only_after_control_response() {
        struct DropOwner(Arc<AtomicUsize>);
        impl Drop for DropOwner {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::AcqRel);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let encoded = encode_fragment(FrameType::PutPack, 55, 8, 0, 0, 2, b"abcd", 4, 2).unwrap();
        let fragment = decode_fragment(&encoded, 8, 4, 2).unwrap();
        let mut assembling = HashMap::new();
        assembling.insert(
            55,
            Reassembly::start(fragment, DropOwner(Arc::clone(&drops))).unwrap(),
        );
        let mut ledger = AdmissionLedger::new(LaneLimits {
            max_payload_bytes: 8,
            max_inflight_requests: 1,
            max_inflight_bytes: 8,
        });
        assert!(ledger.admit(55, 8, RpcPhase::Assembling));

        let cancelled = take_reassembly(&mut assembling, 55).unwrap();
        assert!(assembling.is_empty());
        assert!(!ledger.admit(56, 1, RpcPhase::Queued));
        assert_eq!(drops.load(Ordering::Acquire), 0);
        assert!(ledger.finish(55));
        assert_eq!(ledger.used_bytes, 0);
        drop(cancelled);
        assert_eq!(drops.load(Ordering::Acquire), 1);
    }
}
