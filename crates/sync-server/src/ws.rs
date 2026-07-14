//! Ph2 notify channel: WebSocket sessions that push "root changed" frames.
//!
//! Protocol (versioned JSON frames; `presence`/`ops`/`cursor` types are
//! reserved for Ph3/Ph4 — see tasks/realtime-roadmap.md):
//!
//!   client → server   {"v":1,"t":"sub","vaults":["example-vault"]}
//!                     {"v":1,"t":"ping"}
//!   server → client   {"v":1,"t":"root","vault":"example-vault","root":"<hex64>"}
//!                     {"v":1,"t":"pong"}
//!                     {"v":1,"t":"bye","reason":"..."}
//!
//! The frames carry no secrets — a root hash is observable via polling
//! anyway; actual data still travels over the sealed HTTP pull. Auth is a
//! single-use TTL ticket minted over the sealed channel (ws_ticket.rs):
//! this route CANNOT sit behind `secure_envelope`, which buffers entire
//! responses and would kill the stream.

use crate::state::SharedState;
use crate::ws_ticket;
use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};

/// Cap on vaults per subscription frame — a self-hosted fleet syncs a
/// handful of vaults; anything bigger is a client bug.
const MAX_VAULTS_PER_SUB: usize = 16;
/// The client must subscribe within this window or the session is closed.
const SUB_DEADLINE_SECS: u64 = 10;

#[derive(serde::Deserialize)]
pub struct WsQuery {
    ticket: String,
}

pub async fn ws_route(
    State(state): State<SharedState>,
    Query(q): Query<WsQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    // Burn the ticket BEFORE upgrading — a rejected handshake must not leave
    // a spendable ticket behind.
    let Some(device_id) = ws_ticket::claim(&state.layout, &q.ticket) else {
        tracing::warn!("ws: rejected connection (unknown/expired/reused ticket)");
        return (StatusCode::UNAUTHORIZED, "invalid ticket").into_response();
    };
    if crate::devices::is_revoked(&state.layout, &device_id) {
        return (StatusCode::FORBIDDEN, "device revoked").into_response();
    }
    let device_short = device_id[..device_id.len().min(12)].to_string();
    tracing::info!(device = %device_short, "ws: session opening");

    upgrade.on_upgrade(move |socket| session(state, socket, device_short))
}

async fn session(state: SharedState, socket: WebSocket, device_short: String) {
    let (mut sink, mut stream) = socket.split();

    // First frame must be a sub.
    let vaults = match tokio::time::timeout(
        std::time::Duration::from_secs(SUB_DEADLINE_SECS),
        read_sub(&mut stream),
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

    tracing::info!(device = %device_short, vaults = ?vaults, "ws: subscribed");

    // Fan-in: one forwarder task per vault feeds a single outbound queue.
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<String>(32);
    let mut forwarders = Vec::new();
    for vault in &vaults {
        let mut rx = state.subscribe_roots(vault);
        let tx = out_tx.clone();
        let vault = vault.clone();
        forwarders.push(tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(root) => {
                        let frame = serde_json::json!({
                            "v": 1, "t": "root", "vault": vault, "root": root,
                        })
                        .to_string();
                        if tx.send(frame).await.is_err() {
                            break; // session gone
                        }
                    }
                    // Lagged: the client missed frames; its regular poll is
                    // the catch-up path, and newer frames still flow.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }));
    }
    drop(out_tx);

    // Main loop: forward outbound frames; answer pings; exit on close.
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
                        // Re-subs after the first are ignored in v1.
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {} // binary/protocol ping — tungstenite handles pongs
                    Some(Err(_)) => break,
                }
            }
        }
    }

    for f in forwarders {
        f.abort();
    }
    tracing::info!(device = %device_short, "ws: session closed");
}

/// Read frames until the sub arrives (ignoring pings), return its vaults.
async fn read_sub(
    stream: &mut futures_util::stream::SplitStream<WebSocket>,
) -> Option<Vec<String>> {
    while let Some(Ok(msg)) = stream.next().await {
        if let Message::Text(text) = msg {
            let v: serde_json::Value = serde_json::from_str(&text).ok()?;
            if v.get("t").and_then(|t| t.as_str()) == Some("sub") {
                let vaults: Vec<String> = v
                    .get("vaults")
                    .and_then(|x| x.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|s| s.as_str().map(String::from))
                            .take(MAX_VAULTS_PER_SUB)
                            .collect()
                    })
                    .unwrap_or_default();
                return Some(vaults);
            }
        }
    }
    None
}

fn bye(reason: &str) -> String {
    serde_json::json!({"v": 1, "t": "bye", "reason": reason}).to_string()
}
