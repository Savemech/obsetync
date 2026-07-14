//! Ph2 notify channel (1.7.0): ticket-authenticated WebSocket that pushes
//! "root changed" frames so clients pull within seconds instead of on the
//! 30s poll.
//!
//! Covered here:
//! - A subscribed session receives a `{"t":"root",...}` frame carrying the
//!   new root hash promptly after another device pushes.
//! - Tickets are single-use (replay → 401) and garbage tickets are rejected.
//!
//! Frames carry no secrets — data still travels over the sealed HTTP pull;
//! this suite only exercises the wake-up path.

use e2e_tests::*;
use futures_util::{SinkExt, StreamExt};
use sync_core::hash::hash_to_hex;
use tokio_tungstenite::tungstenite::Message;

async fn mint_ticket(client: &WireClient) -> String {
    let r = client.raw("POST", "/api/v1/ws-ticket", &[]).await.unwrap();
    assert!(r.status.is_success(), "ws-ticket: {}", r.status);
    let v: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
    v["ticket"].as_str().unwrap().to_string()
}

fn ws_url(env: &E2eEnv, ticket: &str) -> String {
    format!(
        "{}/api/v1/ws?ticket={}",
        env.base_url.replace("http://", "ws://"),
        ticket
    )
}

#[tokio::test]
async fn subscribed_session_receives_root_frame_on_push() {
    let env = harness().await;
    let vault = unique_vault_id("ws-notify");
    let watcher = WireClient::new(&env, env.enroll_device("ws-watcher").await.unwrap());
    let pusher = WireClient::new(&env, env.enroll_device("ws-pusher").await.unwrap());

    // Baseline so the push below is a plain fast-forward.
    let base = vec![("doc.md".to_string(), b"base\n".to_vec())];
    let (base_root, _) = push_vault_snapshot(&pusher, &vault, &base, ZERO_HASH_HEX)
        .await
        .unwrap();
    let parent = hash_to_hex(&base_root.hash());

    // Watcher opens the notify channel and subscribes.
    let ticket = mint_ticket(&watcher).await;
    let (mut ws, _resp) = tokio_tungstenite::connect_async(ws_url(&env, &ticket))
        .await
        .expect("ws connect");
    ws.send(Message::Text(
        serde_json::json!({"v":1,"t":"sub","vaults":[vault]})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    // Give the server a beat to register the subscription before pushing.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Pusher advances the vault.
    let v2 = vec![("doc.md".to_string(), b"edited\n".to_vec())];
    let (new_root, resp) = push_vault_snapshot(&pusher, &vault, &v2, &parent)
        .await
        .unwrap();
    assert!(resp.accepted || resp.merged, "push failed: {resp:?}");
    let new_root_hex = hash_to_hex(&new_root.hash());

    // The frame must arrive promptly (generous 5s bound; typical is ms).
    let frame = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
                    if v["t"] == "root" && v["vault"] == vault.as_str() {
                        return v["root"].as_str().unwrap().to_string();
                    }
                }
                Some(Ok(_)) => continue,
                other => panic!("ws stream ended unexpectedly: {other:?}"),
            }
        }
    })
    .await
    .expect("no root frame within 5s — notify channel is not firing");

    assert_eq!(
        frame, new_root_hex,
        "frame must carry the exact new current root"
    );

    // App-level ping keeps the session alive and answers with pong.
    ws.send(Message::Text(
        serde_json::json!({"v":1,"t":"ping"}).to_string().into(),
    ))
    .await
    .unwrap();
    let pong = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if let Some(Ok(Message::Text(text))) = ws.next().await {
                let v: serde_json::Value = serde_json::from_str(&text).unwrap();
                if v["t"] == "pong" {
                    return true;
                }
            }
        }
    })
    .await
    .expect("no pong");
    assert!(pong);
}

#[tokio::test]
async fn tickets_are_single_use_and_garbage_is_rejected() {
    let env = harness().await;
    let device = WireClient::new(&env, env.enroll_device("ws-tickets").await.unwrap());

    // First use succeeds…
    let ticket = mint_ticket(&device).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_url(&env, &ticket))
        .await
        .expect("first use must connect");
    // …and burns the ticket: replay is refused at the handshake.
    let replay = tokio_tungstenite::connect_async(ws_url(&env, &ticket)).await;
    match replay {
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
            assert_eq!(resp.status().as_u16(), 401, "replayed ticket must 401");
        }
        other => panic!("replayed ticket must be rejected, got {other:?}"),
    }
    let _ = ws.close(None).await;

    // Garbage ticket → 401 too.
    let garbage = tokio_tungstenite::connect_async(ws_url(&env, &"ab".repeat(32))).await;
    match garbage {
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
            assert_eq!(resp.status().as_u16(), 401);
        }
        other => panic!("garbage ticket must be rejected, got {other:?}"),
    }
}
