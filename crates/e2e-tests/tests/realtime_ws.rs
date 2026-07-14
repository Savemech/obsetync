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

// --- v2: sealed frames + presence (Ph3) --------------------------------------

mod v2 {
    use super::*;
    use aes_gcm::aead::{Aead, Payload};
    use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
    use hkdf::Hkdf;
    use sha2::Sha256;

    const WS_AAD_PREFIX: &[u8] = b"obsetync/ws/v2";

    pub struct WsCrypto {
        c2s: [u8; 32],
        s2c: [u8; 32],
        seq_in: u64,
        seq_out: u64,
    }

    impl WsCrypto {
        pub fn derive(shared: &[u8], ticket_hex: &str) -> Self {
            let key = |info: &[u8]| -> [u8; 32] {
                let hk = Hkdf::<Sha256>::new(Some(ticket_hex.as_bytes()), shared);
                let mut out = [0u8; 32];
                hk.expand(info, &mut out).unwrap();
                out
            };
            Self {
                c2s: key(b"obsetync/ws/v2/c2s"),
                s2c: key(b"obsetync/ws/v2/s2c"),
                seq_in: 0,
                seq_out: 0,
            }
        }

        fn aad(dir: &str, seq: u64) -> Vec<u8> {
            let mut aad = WS_AAD_PREFIX.to_vec();
            aad.push(b' ');
            aad.extend_from_slice(dir.as_bytes());
            aad.push(b' ');
            aad.extend_from_slice(&seq.to_be_bytes());
            aad
        }

        pub fn seal(&mut self, inner: &str) -> Vec<u8> {
            let mut nonce = [0u8; 12];
            use rand::RngCore;
            rand::rng().fill_bytes(&mut nonce);
            let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.c2s));
            let aad = Self::aad("c2s", self.seq_out);
            self.seq_out += 1;
            let ct = cipher
                .encrypt(
                    Nonce::from_slice(&nonce),
                    Payload {
                        msg: inner.as_bytes(),
                        aad: &aad,
                    },
                )
                .unwrap();
            let mut out = nonce.to_vec();
            out.extend_from_slice(&ct);
            out
        }

        pub fn open(&mut self, frame: &[u8]) -> serde_json::Value {
            let (nonce, ct) = frame.split_at(12);
            let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.s2c));
            let aad = Self::aad("s2c", self.seq_in);
            self.seq_in += 1;
            let plain = cipher
                .decrypt(Nonce::from_slice(nonce), Payload { msg: ct, aad: &aad })
                .expect("sealed frame must open");
            serde_json::from_slice(&plain).unwrap()
        }
    }

    /// Full v2 client: keypair → mint → connect → auth → sealed ready → sub.
    pub async fn open_v2_session(
        env: &E2eEnv,
        client: &WireClient,
        vault: &str,
    ) -> (
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        WsCrypto,
    ) {
        // Ephemeral keys + mint with our pubkey.
        use rand::RngCore;
        let mut priv_bytes = [0u8; 32];
        rand::rng().fill_bytes(&mut priv_bytes);
        let client_secret = x25519_dalek::StaticSecret::from(priv_bytes);
        let client_pub = x25519_dalek::PublicKey::from(&client_secret);
        use base64::prelude::*;
        let body = serde_json::json!({
            "client_eph_pub": BASE64_STANDARD.encode(client_pub.as_bytes()),
        })
        .to_string();
        let r = client
            .raw("POST", "/api/v1/ws-ticket", body.as_bytes())
            .await
            .unwrap();
        assert!(r.status.is_success(), "v2 mint: {}", r.status);
        let v: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
        let ticket = v["ticket"].as_str().unwrap().to_string();
        let server_pub_b64 = v["server_eph_pub"]
            .as_str()
            .expect("v2 mint must return server_eph_pub");
        let server_pub_bytes: [u8; 32] = BASE64_STANDARD
            .decode(server_pub_b64)
            .unwrap()
            .try_into()
            .unwrap();
        let shared = client_secret.diffie_hellman(&x25519_dalek::PublicKey::from(server_pub_bytes));
        let mut crypto = WsCrypto::derive(shared.as_bytes(), &ticket);

        // Connect WITHOUT ticket in URL; auth is the first (plaintext) frame.
        let url = format!("{}/api/v1/ws", env.base_url.replace("http://", "ws://"));
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        ws.send(Message::Text(
            serde_json::json!({"v":2,"t":"auth","ticket":ticket})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();

        // Sealed ready must arrive (s2c seq 0).
        let ready = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match ws.next().await {
                    Some(Ok(Message::Binary(data))) => return crypto.open(&data),
                    Some(Ok(_)) => continue,
                    other => panic!("ws ended before ready: {other:?}"),
                }
            }
        })
        .await
        .expect("no sealed ready");
        assert_eq!(ready["t"], "ready");

        // Sealed sub (c2s seq 0).
        let sub = crypto.seal(&serde_json::json!({"v":2,"t":"sub","vaults":[vault]}).to_string());
        ws.send(Message::Binary(sub.into())).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        (ws, crypto)
    }
}

#[tokio::test]
async fn v2_sealed_session_delivers_root_frames() {
    let env = harness().await;
    let vault = unique_vault_id("ws-v2");
    let watcher = WireClient::new(&env, env.enroll_device("v2-watcher").await.unwrap());
    let pusher = WireClient::new(&env, env.enroll_device("v2-pusher").await.unwrap());

    let base = vec![("doc.md".to_string(), b"base\n".to_vec())];
    let (base_root, _) = push_vault_snapshot(&pusher, &vault, &base, ZERO_HASH_HEX)
        .await
        .unwrap();
    let parent = hash_to_hex(&base_root.hash());

    let (mut ws, mut crypto) = v2::open_v2_session(&env, &watcher, &vault).await;

    let v2_files = vec![("doc.md".to_string(), b"edited\n".to_vec())];
    let (new_root, _) = push_vault_snapshot(&pusher, &vault, &v2_files, &parent)
        .await
        .unwrap();
    let new_root_hex = hash_to_hex(&new_root.hash());

    let frame = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Binary(data))) => {
                    let v = crypto.open(&data);
                    if v["t"] == "root" && v["vault"] == vault.as_str() {
                        return v["root"].as_str().unwrap().to_string();
                    }
                }
                Some(Ok(_)) => continue,
                other => panic!("ws ended: {other:?}"),
            }
        }
    })
    .await
    .expect("no sealed root frame");
    assert_eq!(frame, new_root_hex);
}

#[tokio::test]
async fn v2_presence_roundtrip_between_sessions() {
    let env = harness().await;
    let vault = unique_vault_id("ws-presence");
    let alice = WireClient::new(&env, env.enroll_device("presence-alice").await.unwrap());
    let bob = WireClient::new(&env, env.enroll_device("presence-bob").await.unwrap());

    // Vault must exist for enrollment-independent flows; push a baseline.
    push_vault_snapshot(
        &alice,
        &vault,
        &[("a.md".to_string(), b"x\n".to_vec())],
        ZERO_HASH_HEX,
    )
    .await
    .unwrap();

    let (mut ws_a, mut crypto_a) = v2::open_v2_session(&env, &alice, &vault).await;
    let (mut ws_b, mut crypto_b) = v2::open_v2_session(&env, &bob, &vault).await;

    // Bob announces he's editing notes/secret.md (sealed — the whole point).
    let presence = crypto_b.seal(
        &serde_json::json!({
            "v":2, "t":"presence", "vault": vault, "file": "notes/secret.md", "state": "active",
        })
        .to_string(),
    );
    ws_b.send(Message::Binary(presence.into())).await.unwrap();

    // Alice receives it with bob's device NAME resolved by the server.
    let seen = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match ws_a.next().await {
                Some(Ok(Message::Binary(data))) => {
                    let v = crypto_a.open(&data);
                    if v["t"] == "presence" && v["state"] == "active" {
                        return v;
                    }
                }
                Some(Ok(_)) => continue,
                other => panic!("ws ended: {other:?}"),
            }
        }
    })
    .await
    .expect("no presence frame");
    assert_eq!(seen["file"], "notes/secret.md");
    assert_eq!(seen["name"], "presence-bob");

    // Bob disconnects → alice gets the offline frame (presence cleared).
    drop(ws_b);
    let offline = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match ws_a.next().await {
                Some(Ok(Message::Binary(data))) => {
                    let v = crypto_a.open(&data);
                    if v["t"] == "presence" && v["state"] == "offline" {
                        return v;
                    }
                }
                Some(Ok(_)) => continue,
                other => panic!("ws ended: {other:?}"),
            }
        }
    })
    .await
    .expect("no offline frame");
    assert_eq!(offline["name"], "presence-bob");
}

// --- Ph4: CRDT op relay + durable per-note log --------------------------------

/// Split a raw CRDT log ([u32 LE len][blob]…) into blobs (mirrors the server).
fn split_crdt_frames(log: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut i = 0;
    while i + 4 <= log.len() {
        let len = u32::from_le_bytes([log[i], log[i + 1], log[i + 2], log[i + 3]]) as usize;
        i += 4;
        if i + len > log.len() {
            break;
        }
        out.push(log[i..i + len].to_vec());
        i += len;
    }
    out
}

#[tokio::test]
async fn crdt_ops_relay_and_durable_log() {
    let env = harness().await;
    let vault = unique_vault_id("crdt");
    let a = WireClient::new(&env, env.enroll_device("crdt-a").await.unwrap());
    let b = WireClient::new(&env, env.enroll_device("crdt-b").await.unwrap());
    // Vault must exist.
    push_vault_snapshot(
        &a,
        &vault,
        &[("seed.md".to_string(), b"x\n".to_vec())],
        ZERO_HASH_HEX,
    )
    .await
    .unwrap();

    let (mut ws_a, mut crypto_a) = v2::open_v2_session(&env, &a, &vault).await;
    let (mut ws_b, mut crypto_b) = v2::open_v2_session(&env, &b, &vault).await;

    let note = "notes/live.md";
    let update = b"YJS-UPDATE-ONE".to_vec();
    use base64::prelude::*;
    let ops = serde_json::json!({
        "v": 2, "t": "ops", "vault": vault, "note": note,
        "update": BASE64_STANDARD.encode(&update),
    })
    .to_string();

    // A publishes one op.
    ws_a.send(Message::Binary(crypto_a.seal(&ops).into()))
        .await
        .unwrap();

    // (1) RELAY: B receives the exact op frame.
    let seen = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match ws_b.next().await {
                Some(Ok(Message::Binary(data))) => {
                    let v = crypto_b.open(&data);
                    if v["t"] == "ops" && v["note"] == note {
                        return v;
                    }
                }
                Some(Ok(_)) => continue,
                other => panic!("ws_b ended: {other:?}"),
            }
        }
    })
    .await
    .expect("B never received the relayed op");
    assert_eq!(
        BASE64_STANDARD
            .decode(seen["update"].as_str().unwrap())
            .unwrap(),
        update,
        "relayed op must carry A's exact update bytes",
    );

    // (2) DURABILITY: a fresh device fetches the note's log over sealed HTTP
    // and finds A's update — durability doesn't depend on any client staying.
    let c = WireClient::new(&env, env.enroll_device("crdt-c").await.unwrap());
    let r = c
        .raw("POST", &format!("/api/v1/crdt/{vault}"), note.as_bytes())
        .await
        .unwrap();
    assert!(r.status.is_success(), "crdt get: {}", r.status);
    let frames = split_crdt_frames(&r.body);
    assert_eq!(
        frames,
        vec![update.clone()],
        "durable log must hold A's update"
    );

    // (3) COMPACT: replace the log with a snapshot; the log is now just that.
    let snapshot = b"YJS-SNAPSHOT-V2".to_vec();
    let mut compact_body = (note.len() as u16).to_le_bytes().to_vec();
    compact_body.extend_from_slice(note.as_bytes());
    compact_body.extend_from_slice(&snapshot);
    let r = c
        .raw(
            "POST",
            &format!("/api/v1/crdt/{vault}/compact"),
            &compact_body,
        )
        .await
        .unwrap();
    assert!(r.status.is_success(), "crdt compact: {}", r.status);
    let r = c
        .raw("POST", &format!("/api/v1/crdt/{vault}"), note.as_bytes())
        .await
        .unwrap();
    assert_eq!(
        split_crdt_frames(&r.body),
        vec![snapshot],
        "after compaction the log is exactly the snapshot",
    );
}
