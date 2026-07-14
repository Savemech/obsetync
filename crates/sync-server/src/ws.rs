//! Realtime channel: notify (Ph2) + presence (Ph3) over WebSocket.
//!
//! Two session flavors:
//!
//! **v2 (sealed, current)** — no ticket in the URL. The client's FIRST text
//! frame is a plaintext `{"v":2,"t":"auth","ticket":"<hex>"}`; the ticket was
//! minted over the sealed HTTP channel together with an X25519 exchange, so
//! both sides already hold directional session keys (secure::derive_ws_keys).
//! The server burns the ticket, answers with a SEALED `{"t":"ready"}`, and
//! from then on every frame in both directions is a Binary message sealed by
//! secure::ws_seal/ws_open with per-direction sequence counters (replay and
//! reordering die on AAD). Presence frames — which name file paths — only
//! exist in this flavor.
//!
//! **v1 (legacy, plaintext)** — `?ticket=` in the URL, JSON text frames,
//! root-change notifications ONLY (presence is filtered out so file paths
//! never travel plaintext). Kept so a not-yet-updated fleet keeps its
//! realtime pulls; remove after every device is ≥ 1.8.0.
//!
//! Inner frame vocabulary (either flavor):
//!   C→S  {"t":"sub","vaults":[...]}, {"t":"ping"},
//!        {"t":"presence","vault":..,"file":..|null,"state":"active"|"idle"}   (v2 only)
//!   S→C  {"t":"root","vault":..,"root":..}, {"t":"pong"}, {"t":"bye"},
//!        {"t":"ready"} (v2 handshake ack),
//!        {"t":"presence","vault":..,"device":..,"name":..,"file":..,"state":..} (v2 only)

use crate::secure;
use crate::state::SharedState;
use crate::ws_ticket;
use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};

/// Cap on vaults per subscription frame — a self-hosted fleet syncs a
/// handful of vaults; anything bigger is a client bug.
const MAX_VAULTS_PER_SUB: usize = 16;
/// The client must auth (v2) and subscribe within this window.
const HANDSHAKE_DEADLINE_SECS: u64 = 10;

#[derive(serde::Deserialize)]
pub struct WsQuery {
    /// Legacy v1 auth. v2 clients omit it and auth with their first frame.
    ticket: Option<String>,
}

pub async fn ws_route(
    State(state): State<SharedState>,
    Query(q): Query<WsQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    match q.ticket {
        // v1 legacy: burn the ticket BEFORE upgrading — a rejected handshake
        // must not leave a spendable ticket behind.
        Some(ticket) => {
            let Some(t) = ws_ticket::claim(&state.layout, &ticket) else {
                tracing::warn!("ws: rejected v1 connection (unknown/expired/reused ticket)");
                return (StatusCode::UNAUTHORIZED, "invalid ticket").into_response();
            };
            if crate::devices::is_revoked(&state.layout, &t.device_id) {
                return (StatusCode::FORBIDDEN, "device revoked").into_response();
            }
            let device_short = t.device_id[..t.device_id.len().min(12)].to_string();
            tracing::info!(device = %device_short, "ws: v1 session opening (plaintext, deprecated)");
            upgrade.on_upgrade(move |socket| session(state, socket, t, false))
        }
        // v2: auth happens as the first frame after upgrade.
        None => upgrade.on_upgrade(move |socket| v2_entry(state, socket)),
    }
}

/// v2 pre-session: read the plaintext auth frame, burn the ticket, verify
/// session keys exist, ack with a sealed "ready".
async fn v2_entry(state: SharedState, socket: WebSocket) {
    let (mut sink, mut stream) = socket.split();

    let auth = tokio::time::timeout(
        std::time::Duration::from_secs(HANDSHAKE_DEADLINE_SECS),
        stream.next(),
    )
    .await;
    let ticket_hex = match auth {
        Ok(Some(Ok(Message::Text(text)))) => {
            let v: Option<serde_json::Value> = serde_json::from_str(&text).ok();
            match v.and_then(|v| {
                (v.get("t").and_then(|t| t.as_str()) == Some("auth"))
                    .then(|| v.get("ticket").and_then(|t| t.as_str()).map(String::from))
                    .flatten()
            }) {
                Some(t) => t,
                None => {
                    let _ = sink
                        .send(Message::Text(bye("expected auth frame").into()))
                        .await;
                    return;
                }
            }
        }
        _ => {
            let _ = sink.send(Message::Text(bye("auth deadline").into())).await;
            return;
        }
    };

    let Some(ticket) = ws_ticket::claim(&state.layout, &ticket_hex) else {
        tracing::warn!("ws: rejected v2 auth (unknown/expired/reused ticket)");
        let _ = sink.send(Message::Text(bye("invalid ticket").into())).await;
        return;
    };
    if ticket.c2s_key_hex.is_empty() || ticket.s2c_key_hex.is_empty() {
        let _ = sink
            .send(Message::Text(
                bye("ticket lacks session keys — re-mint with client_eph_pub").into(),
            ))
            .await;
        return;
    }
    if crate::devices::is_revoked(&state.layout, &ticket.device_id) {
        let _ = sink.send(Message::Text(bye("device revoked").into())).await;
        return;
    }

    let device_short = ticket.device_id[..ticket.device_id.len().min(12)].to_string();
    tracing::info!(device = %device_short, "ws: v2 session opening (sealed)");
    session_v2(state, sink, stream, ticket).await;
}

/// Decoded session keys + directional counters for a v2 session.
struct SealCtx {
    c2s: [u8; 32],
    s2c: [u8; 32],
    seq_in: u64,
    seq_out: u64,
}

impl SealCtx {
    fn from_ticket(t: &ws_ticket::WsTicket) -> Option<Self> {
        let mut c2s = [0u8; 32];
        let mut s2c = [0u8; 32];
        hex::decode_to_slice(&t.c2s_key_hex, &mut c2s).ok()?;
        hex::decode_to_slice(&t.s2c_key_hex, &mut s2c).ok()?;
        Some(Self {
            c2s,
            s2c,
            seq_in: 0,
            seq_out: 0,
        })
    }

    fn seal(&mut self, inner_json: &str) -> Option<Vec<u8>> {
        let framed = secure::ws_seal(&self.s2c, "s2c", self.seq_out, inner_json.as_bytes()).ok()?;
        self.seq_out += 1;
        Some(framed)
    }

    fn open(&mut self, frame: &[u8]) -> Option<String> {
        let plain = secure::ws_open(&self.c2s, "c2s", self.seq_in, frame).ok()?;
        self.seq_in += 1;
        String::from_utf8(plain).ok()
    }
}

async fn session_v2(
    state: SharedState,
    mut sink: SplitSink<WebSocket, Message>,
    mut stream: SplitStream<WebSocket>,
    ticket: ws_ticket::WsTicket,
) {
    let device_short = ticket.device_id[..ticket.device_id.len().min(12)].to_string();
    let Some(mut seal) = SealCtx::from_ticket(&ticket) else {
        let _ = sink
            .send(Message::Text(bye("corrupt session keys").into()))
            .await;
        return;
    };

    // Sealed handshake ack — also proves to the client that both sides
    // derived the same keys before it sends anything sensitive.
    let ready = serde_json::json!({"v": 2, "t": "ready"}).to_string();
    match seal.seal(&ready) {
        Some(frame) => {
            if sink.send(Message::Binary(frame.into())).await.is_err() {
                return;
            }
        }
        None => return,
    }

    // Wait for the (sealed) sub frame.
    let vaults = match tokio::time::timeout(
        std::time::Duration::from_secs(HANDSHAKE_DEADLINE_SECS),
        read_sub_sealed(&mut stream, &mut seal),
    )
    .await
    {
        Ok(Some(v)) if !v.is_empty() => v,
        _ => {
            if let Some(f) = seal.seal(&bye("expected sub frame")) {
                let _ = sink.send(Message::Binary(f.into())).await;
            }
            return;
        }
    };
    tracing::info!(device = %device_short, vaults = ?vaults, "ws: v2 subscribed");

    // Device name for presence frames — fetched once.
    let device_name = crate::devices::get_device(&state.layout, &ticket.device_id)
        .map(|d| d.name)
        .unwrap_or_else(|| device_short.clone());

    // Fan-in: per-vault forwarders → one outbound queue.
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<String>(32);
    let mut forwarders = Vec::new();
    for vault in &vaults {
        let mut rx = state.subscribe_roots(vault);
        let tx = out_tx.clone();
        forwarders.push(tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(frame) => {
                        if tx.send(frame).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }));
    }
    drop(out_tx);

    // Fresh subscriber immediately learns who is where.
    for vault in &vaults {
        for frame in state.presence_snapshot(vault) {
            if let Some(f) = seal.seal(&frame) {
                if sink.send(Message::Binary(f.into())).await.is_err() {
                    return;
                }
            }
        }
    }

    loop {
        tokio::select! {
            frame = out_rx.recv() => {
                match frame {
                    Some(inner) => {
                        let Some(f) = seal.seal(&inner) else { break };
                        if sink.send(Message::Binary(f.into())).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
            msg = stream.next() => {
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        // A frame that fails to open is tampering or a
                        // desynced counter — kill the session, the client
                        // reconnects with a fresh ticket.
                        let Some(inner) = seal.open(&data) else {
                            tracing::warn!(device = %device_short, "ws: sealed frame failed to open — closing");
                            break;
                        };
                        let Ok(v) = serde_json::from_str::<serde_json::Value>(&inner) else { continue };
                        match v.get("t").and_then(|t| t.as_str()) {
                            Some("ping") => {
                                let pong = serde_json::json!({"v": 2, "t": "pong"}).to_string();
                                let Some(f) = seal.seal(&pong) else { break };
                                if sink.send(Message::Binary(f.into())).await.is_err() {
                                    break;
                                }
                            }
                            Some("presence") => {
                                let vault = v.get("vault").and_then(|x| x.as_str()).unwrap_or("");
                                if vaults.iter().any(|s| s == vault) {
                                    let file = v.get("file").and_then(|x| x.as_str()).map(String::from);
                                    let p_state = v.get("state").and_then(|x| x.as_str()).unwrap_or("active");
                                    state.set_presence(
                                        vault,
                                        &ticket.device_id,
                                        &device_name,
                                        file,
                                        if p_state == "idle" { "idle" } else { "active" },
                                    );
                                }
                            }
                            _ => {} // re-subs etc. ignored in v2.0
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {} // text after handshake / protocol pings
                    Some(Err(_)) => break,
                }
            }
        }
    }

    for f in forwarders {
        f.abort();
    }
    // Session over → this device is gone from every vault it was visible in.
    for vault in &vaults {
        state.clear_presence(vault, &ticket.device_id);
    }
    tracing::info!(device = %device_short, "ws: v2 session closed");
}

/// Read sealed frames until the sub arrives, return its vault list.
async fn read_sub_sealed(
    stream: &mut SplitStream<WebSocket>,
    seal: &mut SealCtx,
) -> Option<Vec<String>> {
    while let Some(Ok(msg)) = stream.next().await {
        if let Message::Binary(data) = msg {
            let inner = seal.open(&data)?;
            let v: serde_json::Value = serde_json::from_str(&inner).ok()?;
            if v.get("t").and_then(|t| t.as_str()) == Some("sub") {
                return Some(extract_vaults(&v));
            }
        }
    }
    None
}

/// v1 legacy session: plaintext frames, ROOT NOTIFICATIONS ONLY.
async fn session(state: SharedState, socket: WebSocket, ticket: ws_ticket::WsTicket, _v2: bool) {
    let device_short = ticket.device_id[..ticket.device_id.len().min(12)].to_string();
    let (mut sink, mut stream) = socket.split();

    let vaults = match tokio::time::timeout(
        std::time::Duration::from_secs(HANDSHAKE_DEADLINE_SECS),
        read_sub_plain(&mut stream),
    )
    .await
    {
        Ok(Some(v)) if !v.is_empty() => v,
        _ => {
            let _ = sink
                .send(Message::Text(bye("expected sub frame").into()))
                .await;
            return;
        }
    };
    tracing::info!(device = %device_short, vaults = ?vaults, "ws: v1 subscribed");

    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<String>(32);
    let mut forwarders = Vec::new();
    for vault in &vaults {
        let mut rx = state.subscribe_roots(vault);
        let tx = out_tx.clone();
        forwarders.push(tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(frame) => {
                        // Plaintext wire: presence frames (file paths!) must
                        // not leak here — root notifications only.
                        let is_root = serde_json::from_str::<serde_json::Value>(&frame)
                            .ok()
                            .and_then(|v| v.get("t").and_then(|t| t.as_str()).map(|t| t == "root"))
                            .unwrap_or(false);
                        if is_root && tx.send(frame).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }));
    }
    drop(out_tx);

    loop {
        tokio::select! {
            frame = out_rx.recv() => {
                match frame {
                    Some(f) => {
                        if sink.send(Message::Text(f.into())).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
            msg = stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let is_ping = serde_json::from_str::<serde_json::Value>(&text)
                            .ok()
                            .and_then(|v| v.get("t").and_then(|t| t.as_str()).map(|t| t == "ping"))
                            .unwrap_or(false);
                        if is_ping {
                            let pong = serde_json::json!({"v": 1, "t": "pong"}).to_string();
                            if sink.send(Message::Text(pong.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
        }
    }

    for f in forwarders {
        f.abort();
    }
    tracing::info!(device = %device_short, "ws: v1 session closed");
}

/// Read plaintext frames until the sub arrives (v1).
async fn read_sub_plain(stream: &mut SplitStream<WebSocket>) -> Option<Vec<String>> {
    while let Some(Ok(msg)) = stream.next().await {
        if let Message::Text(text) = msg {
            let v: serde_json::Value = serde_json::from_str(&text).ok()?;
            if v.get("t").and_then(|t| t.as_str()) == Some("sub") {
                return Some(extract_vaults(&v));
            }
        }
    }
    None
}

fn extract_vaults(v: &serde_json::Value) -> Vec<String> {
    v.get("vaults")
        .and_then(|x| x.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(String::from))
                .take(MAX_VAULTS_PER_SUB)
                .collect()
        })
        .unwrap_or_default()
}

fn bye(reason: &str) -> String {
    serde_json::json!({"v": 1, "t": "bye", "reason": reason}).to_string()
}
