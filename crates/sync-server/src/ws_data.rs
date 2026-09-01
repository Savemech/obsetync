//! Sealed WebSocket data lane for bounded bulk object RPCs.
//!
//! This is deliberately a sibling of the realtime socket: large object
//! traffic cannot head-of-line block root notifications, presence, or CRDT
//! frames. The lane reuses the existing single-use ticket and directional
//! AEAD keys, while the inner `OBW1` frame carries binary bulk payloads and a
//! request id. No transfer state survives a disconnect; clients recover by
//! checking content addresses and may continue over bulk HTTP.

use crate::error::ServerError;
use crate::secure;
use crate::state::SharedState;
use crate::ws_ticket;
use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
    response::Response,
};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

const FRAME_MAGIC: &[u8; 4] = b"OBW1";
const FRAME_HEADER_BYTES: usize = 18;
const HELLO_MAGIC: &[u8; 4] = b"OWH1";
const HELLO_ACK_MAGIC: &[u8; 4] = b"OWA1";
const HELLO_BYTES: usize = 24;
const ERROR_MAGIC: &[u8; 4] = b"OWE1";
const ERROR_HEADER_BYTES: usize = 14;
const MAX_ERROR_MESSAGE_BYTES: usize = 256;
const AEAD_OVERHEAD_BYTES: usize = 12 + 16;

pub(crate) const MAX_PAYLOAD_BYTES: usize = crate::api::WS_FRAME_BYTES;
const MAX_SEALED_MESSAGE_BYTES: usize =
    FRAME_HEADER_BYTES + MAX_PAYLOAD_BYTES + AEAD_OVERHEAD_BYTES;
const MAX_INFLIGHT_REQUESTS: usize = 4;
const MAX_INFLIGHT_BYTES: usize = 32 * 1024 * 1024;
const HANDSHAKE_DEADLINE_SECS: u64 = 10;
const BUSY_RETRY_AFTER_MS: u32 = 25;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LaneLimits {
    max_payload_bytes: usize,
    max_inflight_requests: usize,
    max_inflight_bytes: usize,
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
    bytes: usize,
    cancellation: CancellationToken,
    task: tokio::task::JoinHandle<()>,
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
    let Some(mut ticket) = ws_ticket::claim(&state.layout, &ticket_hex) else {
        let _ = sink
            .send(Message::Text("invalid data-lane ticket".into()))
            .await;
        return;
    };
    if ticket.c2s_key_hex.is_empty()
        || ticket.s2c_key_hex.is_empty()
        || crate::devices::is_revoked(&state.layout, &ticket.device_id)
    {
        let _ = sink
            .send(Message::Text("data lane unavailable".into()))
            .await;
        return;
    }
    let device_short = ticket.device_id[..ticket.device_id.len().min(12)].to_owned();
    let Some(mut seal) = SealCtx::from_ticket(&ticket) else {
        return;
    };
    zeroize::Zeroize::zeroize(&mut ticket.c2s_key_hex);
    zeroize::Zeroize::zeroize(&mut ticket.s2c_key_hex);

    let hello = tokio::time::timeout(
        std::time::Duration::from_secs(HANDSHAKE_DEADLINE_SECS),
        read_hello(&mut stream, &mut seal),
    )
    .await;
    let Ok(Some(requested)) = hello else {
        return;
    };
    let Some(limits) = negotiate_limits(requested) else {
        return;
    };
    let hello_ack = match encode_hello(HELLO_ACK_MAGIC, limits) {
        Ok(payload) => payload,
        Err(_) => return,
    };
    if send_frame(
        &mut sink,
        &mut seal,
        FrameType::HelloAck,
        0,
        &hello_ack,
        MAX_PAYLOAD_BYTES,
    )
    .await
    .is_err()
    {
        return;
    }

    tracing::info!(
        device = %device_short,
        max_payload_bytes = limits.max_payload_bytes,
        max_inflight_requests = limits.max_inflight_requests,
        max_inflight_bytes = limits.max_inflight_bytes,
        "ws-data: sealed session ready"
    );
    run_session(state, sink, stream, seal, limits, &device_short).await;
}

async fn read_hello(stream: &mut SplitStream<WebSocket>, seal: &mut SealCtx) -> Option<LaneLimits> {
    while let Some(Ok(message)) = stream.next().await {
        match message {
            Message::Binary(data) => {
                let plaintext = seal.open(&data)?;
                let frame = decode_frame(&plaintext, MAX_PAYLOAD_BYTES).ok()?;
                if frame.kind != FrameType::Hello || frame.request_id != 0 {
                    return None;
                }
                return decode_hello(frame.payload).ok();
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
    limits: LaneLimits,
    device_short: &str,
) {
    let (response_tx, mut response_rx) =
        tokio::sync::mpsc::channel::<RpcResponse>(limits.max_inflight_requests);
    let mut active = HashMap::<u64, ActiveRpc>::new();
    let mut active_bytes = 0usize;
    let mut last_request_id = 0u64;

    loop {
        tokio::select! {
            response = response_rx.recv() => {
                let Some(response) = response else { break };
                if let Some(active_rpc) = active.remove(&response.request_id) {
                    active_bytes = active_bytes.saturating_sub(active_rpc.bytes);
                }
                if send_frame(
                    &mut sink,
                    &mut seal,
                    response.kind,
                    response.request_id,
                    &response.payload,
                    limits.max_payload_bytes,
                ).await.is_err() {
                    break;
                }
            }
            message = stream.next() => {
                match message {
                    Some(Ok(Message::Binary(data))) => {
                        let Some(plaintext) = seal.open(&data) else {
                            tracing::warn!(device = %device_short, "ws-data: AEAD frame rejected");
                            break;
                        };
                        let Ok(frame) = decode_frame(&plaintext, limits.max_payload_bytes) else {
                            tracing::warn!(device = %device_short, "ws-data: malformed inner frame");
                            break;
                        };
                        if frame.kind == FrameType::Cancel {
                            if !frame.payload.is_empty() || frame.request_id == 0 {
                                break;
                            }
                            if let Some(rpc) = active.get(&frame.request_id) {
                                rpc.cancellation.cancel();
                            }
                            continue;
                        }
                        if !matches!(
                            frame.kind,
                            FrameType::CheckObjects | FrameType::PutPack | FrameType::GetPack
                        ) || frame.request_id <= last_request_id {
                            tracing::warn!(device = %device_short, "ws-data: invalid or reused request id");
                            break;
                        }
                        last_request_id = frame.request_id;
                        let next_bytes = active_bytes.checked_add(frame.payload.len());
                        if active.len() >= limits.max_inflight_requests
                            || next_bytes.is_none_or(|bytes| bytes > limits.max_inflight_bytes)
                        {
                            let payload = encode_error(
                                ErrorCode::Busy,
                                BUSY_RETRY_AFTER_MS,
                                "data lane busy",
                            );
                            if send_frame(
                                &mut sink,
                                &mut seal,
                                FrameType::Error,
                                frame.request_id,
                                &payload,
                                limits.max_payload_bytes,
                            ).await.is_err() {
                                break;
                            }
                            continue;
                        }

                        let payload = frame.payload.to_vec();
                        let payload_bytes = payload.len();
                        active_bytes = next_bytes.unwrap_or(active_bytes);
                        let cancellation = CancellationToken::new();
                        let task_cancellation = cancellation.clone();
                        let task_state = state.clone();
                        let task_tx = response_tx.clone();
                        let request_id = frame.request_id;
                        let request_kind = frame.kind;
                        let max_payload_bytes = limits.max_payload_bytes;
                        let task = tokio::spawn(async move {
                            let response = tokio::select! {
                                _ = task_cancellation.cancelled() => RpcResponse {
                                    request_id,
                                    kind: FrameType::Error,
                                    payload: encode_error(ErrorCode::Cancelled, 0, "request cancelled"),
                                },
                                response = execute_rpc(
                                    &task_state,
                                    request_id,
                                    request_kind,
                                    &payload,
                                    max_payload_bytes,
                                ) => response,
                            };
                            let _ = task_tx.send(response).await;
                        });
                        active.insert(request_id, ActiveRpc {
                            bytes: payload_bytes,
                            cancellation,
                            task,
                        });
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if sink.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
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
        }
    }

    for (_, rpc) in active {
        rpc.cancellation.cancel();
        rpc.task.abort();
    }
    tracing::info!(device = %device_short, "ws-data: session closed");
}

async fn execute_rpc(
    state: &SharedState,
    request_id: u64,
    request_kind: FrameType,
    payload: &[u8],
    max_payload_bytes: usize,
) -> RpcResponse {
    let result = match request_kind {
        FrameType::CheckObjects => {
            crate::api::process_bulk_check(state, payload, max_payload_bytes)
                .map(|payload| (FrameType::CheckResult, payload))
        }
        FrameType::PutPack => crate::api::process_bulk_put(state, payload, max_payload_bytes)
            .await
            .map(|payload| (FrameType::PutAck, payload)),
        FrameType::GetPack => {
            crate::api::process_bulk_get(state, payload, max_payload_bytes, max_payload_bytes)
                .map(|payload| (FrameType::GetResult, payload))
        }
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

async fn send_frame(
    sink: &mut SplitSink<WebSocket, Message>,
    seal: &mut SealCtx,
    kind: FrameType,
    request_id: u64,
    payload: &[u8],
    max_payload_bytes: usize,
) -> Result<(), ()> {
    let plaintext = encode_frame(kind, request_id, payload, max_payload_bytes).map_err(|_| ())?;
    let sealed = seal.seal(&plaintext).ok_or(())?;
    sink.send(Message::Binary(sealed.into()))
        .await
        .map_err(|_| ())
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

fn decode_hello(payload: &[u8]) -> Result<LaneLimits, ProtocolError> {
    if payload.len() != HELLO_BYTES || payload.get(..4) != Some(HELLO_MAGIC.as_slice()) {
        return Err(ProtocolError::new("invalid data-lane hello"));
    }
    if u16::from_le_bytes(payload[4..6].try_into().unwrap()) != 1 || payload[12..16] != [0, 0, 0, 0]
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

fn encode_hello(magic: &[u8; 4], limits: LaneLimits) -> Result<Vec<u8>, ProtocolError> {
    let requests = u16::try_from(limits.max_inflight_requests)
        .map_err(|_| ProtocolError::new("hello request cap overflow"))?;
    let payload = u32::try_from(limits.max_payload_bytes)
        .map_err(|_| ProtocolError::new("hello payload cap overflow"))?;
    let bytes = u64::try_from(limits.max_inflight_bytes)
        .map_err(|_| ProtocolError::new("hello byte cap overflow"))?;
    let mut output = Vec::with_capacity(HELLO_BYTES);
    output.extend_from_slice(magic);
    output.extend_from_slice(&1u16.to_le_bytes());
    output.extend_from_slice(&requests.to_le_bytes());
    output.extend_from_slice(&payload.to_le_bytes());
    output.extend_from_slice(&0u32.to_le_bytes());
    output.extend_from_slice(&bytes.to_le_bytes());
    Ok(output)
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

    #[test]
    fn binary_frame_round_trip_preserves_request_and_payload() {
        let encoded = encode_frame(FrameType::PutPack, 42, b"binary\0payload", 1024).unwrap();
        let decoded = decode_frame(&encoded, 1024).unwrap();
        assert_eq!(decoded.kind, FrameType::PutPack);
        assert_eq!(decoded.request_id, 42);
        assert_eq!(decoded.payload, b"binary\0payload");
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
    fn error_payload_is_strictly_bounded() {
        let encoded = encode_error(ErrorCode::Internal, 0, &"x".repeat(1000));
        assert_eq!(encoded.len(), ERROR_HEADER_BYTES + MAX_ERROR_MESSAGE_BYTES);
        assert_eq!(
            u16::from_le_bytes(encoded[12..14].try_into().unwrap()) as usize,
            MAX_ERROR_MESSAGE_BYTES
        );
    }
}
