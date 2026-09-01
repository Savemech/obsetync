//! Sealed WebSocket bulk data lane: independent crypto/protocol client.
//!
//! Covers HELLO negotiation, multiplexed binary CHECK/PUT, bounded GET, and
//! the critical unknown-result recovery rule when the socket disappears
//! before the client consumes an ACK.

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
use base64::prelude::*;
use e2e_tests::*;
use futures_util::{SinkExt, StreamExt};
use hkdf::Hkdf;
use sha2::Sha256;
use sync_core::hash::{hash_bytes, FileHash};
use tokio_tungstenite::tungstenite::Message;

const WS_AAD_PREFIX: &[u8] = b"obsetync/ws-data/v1";
const FRAME_MAGIC: &[u8; 4] = b"OBW1";
const FRAME_HEADER_BYTES: usize = 18;
const FRAME_LIMIT: usize = 4 * 1024 * 1024;

const HELLO: u8 = 1;
const HELLO_ACK: u8 = 2;
const CHECK_OBJECTS: u8 = 3;
const CHECK_RESULT: u8 = 4;
const PUT_PACK: u8 = 5;
const PUT_ACK: u8 = 6;
const GET_PACK: u8 = 7;
const GET_RESULT: u8 = 8;

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct WsCrypto {
    c2s: [u8; 32],
    s2c: [u8; 32],
    seq_in: u64,
    seq_out: u64,
}

impl WsCrypto {
    fn derive(shared: &[u8], ticket_hex: &str) -> Self {
        let key = |info: &[u8]| -> [u8; 32] {
            let hkdf = Hkdf::<Sha256>::new(Some(ticket_hex.as_bytes()), shared);
            let mut output = [0u8; 32];
            hkdf.expand(info, &mut output).unwrap();
            output
        };
        Self {
            c2s: key(b"obsetync/ws/v2/c2s"),
            s2c: key(b"obsetync/ws/v2/s2c"),
            seq_in: 0,
            seq_out: 0,
        }
    }

    fn aad(direction: &str, sequence: u64) -> Vec<u8> {
        let mut aad = WS_AAD_PREFIX.to_vec();
        aad.push(b' ');
        aad.extend_from_slice(direction.as_bytes());
        aad.push(b' ');
        aad.extend_from_slice(&sequence.to_be_bytes());
        aad
    }

    fn seal(&mut self, plaintext: &[u8]) -> Vec<u8> {
        let mut nonce = [0u8; 12];
        use rand::RngCore;
        rand::rng().fill_bytes(&mut nonce);
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.c2s));
        let ciphertext = cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: &Self::aad("c2s", self.seq_out),
                },
            )
            .unwrap();
        self.seq_out += 1;
        let mut output = nonce.to_vec();
        output.extend_from_slice(&ciphertext);
        output
    }

    fn open(&mut self, frame: &[u8]) -> Vec<u8> {
        let (nonce, ciphertext) = frame.split_at(12);
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.s2c));
        let plaintext = cipher
            .decrypt(
                Nonce::from_slice(nonce),
                Payload {
                    msg: ciphertext,
                    aad: &Self::aad("s2c", self.seq_in),
                },
            )
            .expect("data-lane frame must authenticate");
        self.seq_in += 1;
        plaintext
    }
}

#[derive(Debug)]
struct DataFrame {
    kind: u8,
    request_id: u64,
    payload: Vec<u8>,
}

fn encode_frame(kind: u8, request_id: u64, payload: &[u8]) -> Vec<u8> {
    assert!(payload.len() <= FRAME_LIMIT);
    let mut output = Vec::with_capacity(FRAME_HEADER_BYTES + payload.len());
    output.extend_from_slice(FRAME_MAGIC);
    output.push(kind);
    output.push(0);
    output.extend_from_slice(&request_id.to_le_bytes());
    output.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    output.extend_from_slice(payload);
    output
}

fn decode_frame(input: &[u8]) -> DataFrame {
    assert!(input.len() >= FRAME_HEADER_BYTES);
    assert_eq!(&input[..4], FRAME_MAGIC);
    assert_eq!(input[5], 0);
    let request_id = u64::from_le_bytes(input[6..14].try_into().unwrap());
    let payload_len = u32::from_le_bytes(input[14..18].try_into().unwrap()) as usize;
    assert!(payload_len <= FRAME_LIMIT);
    assert_eq!(input.len(), FRAME_HEADER_BYTES + payload_len);
    DataFrame {
        kind: input[4],
        request_id,
        payload: input[FRAME_HEADER_BYTES..].to_vec(),
    }
}

fn hello_payload() -> Vec<u8> {
    let mut output = Vec::with_capacity(24);
    output.extend_from_slice(b"OWH1");
    output.extend_from_slice(&1u16.to_le_bytes());
    output.extend_from_slice(&4u16.to_le_bytes());
    output.extend_from_slice(&(FRAME_LIMIT as u32).to_le_bytes());
    output.extend_from_slice(&0u32.to_le_bytes());
    output.extend_from_slice(&(32u64 * 1024 * 1024).to_le_bytes());
    output
}

async fn open_authenticated_data_socket(env: &E2eEnv, client: &WireClient) -> (WsStream, WsCrypto) {
    use rand::RngCore;
    let mut private_bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut private_bytes);
    let client_secret = x25519_dalek::StaticSecret::from(private_bytes);
    let client_public = x25519_dalek::PublicKey::from(&client_secret);
    let mint_body = serde_json::json!({
        "client_eph_pub": BASE64_STANDARD.encode(client_public.as_bytes()),
    })
    .to_string();
    let response = client
        .raw("POST", "/api/v1/ws-ticket", mint_body.as_bytes())
        .await
        .unwrap();
    assert!(response.status.is_success());
    let value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    let ticket = value["ticket"].as_str().unwrap().to_owned();
    let server_public: [u8; 32] = BASE64_STANDARD
        .decode(value["server_eph_pub"].as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap();
    let shared = client_secret.diffie_hellman(&x25519_dalek::PublicKey::from(server_public));
    let crypto = WsCrypto::derive(shared.as_bytes(), &ticket);

    let url = format!(
        "{}/api/v1/ws-data",
        env.base_url.replace("http://", "ws://")
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    socket
        .send(Message::Text(
            serde_json::json!({"v":2,"t":"auth","ticket":ticket})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    (socket, crypto)
}

async fn open_data_session(env: &E2eEnv, client: &WireClient) -> (WsStream, WsCrypto) {
    let (mut socket, mut crypto) = open_authenticated_data_socket(env, client).await;
    send_frame(&mut socket, &mut crypto, HELLO, 0, &hello_payload()).await;
    let ack = receive_frame(&mut socket, &mut crypto).await;
    assert_eq!(ack.kind, HELLO_ACK);
    assert_eq!(ack.request_id, 0);
    assert_eq!(&ack.payload[..4], b"OWA1");
    assert_eq!(u16::from_le_bytes(ack.payload[4..6].try_into().unwrap()), 1);
    assert!(u16::from_le_bytes(ack.payload[6..8].try_into().unwrap()) <= 4);
    assert!(u32::from_le_bytes(ack.payload[8..12].try_into().unwrap()) as usize <= FRAME_LIMIT);
    (socket, crypto)
}

async fn send_frame(
    socket: &mut WsStream,
    crypto: &mut WsCrypto,
    kind: u8,
    request_id: u64,
    payload: &[u8],
) {
    let plaintext = encode_frame(kind, request_id, payload);
    socket
        .send(Message::Binary(crypto.seal(&plaintext).into()))
        .await
        .unwrap();
}

async fn receive_frame(socket: &mut WsStream, crypto: &mut WsCrypto) -> DataFrame {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Binary(data))) => {
                    return decode_frame(&crypto.open(&data));
                }
                Some(Ok(_)) => continue,
                other => panic!("data lane ended before response: {other:?}"),
            }
        }
    })
    .await
    .expect("data-lane response timeout")
}

async fn assert_socket_ends(socket: &mut WsStream) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return,
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                Some(Ok(other)) => panic!("unexpected frame before protocol close: {other:?}"),
            }
        }
    })
    .await
    .expect("server did not close the invalid data-lane session");
}

fn check_request(hash: &FileHash) -> Vec<u8> {
    let mut output = Vec::with_capacity(41);
    output.extend_from_slice(b"OBC1");
    output.push(0); // content
    output.extend_from_slice(&1u32.to_le_bytes());
    output.extend_from_slice(hash);
    output
}

fn put_request(hash: &FileHash, bytes: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(52 + bytes.len());
    output.extend_from_slice(b"OBP1");
    output.extend_from_slice(&0u16.to_le_bytes());
    output.extend_from_slice(&1u32.to_le_bytes());
    output.push(0); // content
    output.push(0); // record flags
    output.extend_from_slice(hash);
    output.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    output.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    output.extend_from_slice(bytes);
    output
}

fn get_request(hash: &FileHash) -> Vec<u8> {
    let mut output = Vec::with_capacity(49);
    output.extend_from_slice(b"OBG1");
    output.push(0); // content
    output.extend_from_slice(&1u32.to_le_bytes());
    output.extend_from_slice(&0u32.to_le_bytes());
    output.extend_from_slice(&(1024u32 * 1024).to_le_bytes());
    output.extend_from_slice(hash);
    output
}

fn assert_check(frame: &DataFrame, request_id: u64, needed: bool) {
    assert_eq!(frame.kind, CHECK_RESULT);
    assert_eq!(frame.request_id, request_id);
    assert_eq!(&frame.payload[..4], b"OBA1");
    assert_eq!(
        u32::from_le_bytes(frame.payload[4..8].try_into().unwrap()),
        1
    );
    assert_eq!(frame.payload[8] & 1 != 0, needed);
}

fn assert_put_ack(frame: &DataFrame, request_id: u64) {
    assert_eq!(frame.kind, PUT_ACK);
    assert_eq!(frame.request_id, request_id);
    assert_eq!(&frame.payload[..4], b"OBK1");
    assert_eq!(
        u32::from_le_bytes(frame.payload[4..8].try_into().unwrap()),
        1
    );
    assert!(matches!(frame.payload[8], 0 | 1));
}

fn assert_get(frame: &DataFrame, request_id: u64, hash: &FileHash, bytes: &[u8]) {
    assert_eq!(frame.kind, GET_RESULT);
    assert_eq!(frame.request_id, request_id);
    assert_eq!(&frame.payload[..4], b"OBD1");
    let bitmap_bytes = 1usize;
    let pack = &frame.payload[12 + bitmap_bytes..];
    assert_eq!(&pack[..4], b"OBP1");
    assert_eq!(u32::from_le_bytes(pack[6..10].try_into().unwrap()), 1);
    assert_eq!(&pack[12..44], hash);
    assert_eq!(&pack[52..], bytes);
}

#[tokio::test]
async fn sealed_data_lane_multiplexes_and_recovers_unknown_put_results() {
    let env = harness().await;
    let client = WireClient::new(&env, env.enroll_device("ws-data").await.unwrap());

    // Socket loss is safe at both handshake boundaries: after one-time auth
    // and after HELLO_ACK. Reconnect always mints independent session keys.
    let (mut auth_only, _auth_crypto) = open_authenticated_data_socket(&env, &client).await;
    auth_only.close(None).await.unwrap();
    let (mut hello_only, _hello_crypto) = open_data_session(&env, &client).await;
    hello_only.close(None).await.unwrap();

    // Losing a CHECK response has no durable side effect. A fresh session can
    // repeat the same content-address query without retaining RPC state.
    let never_stored = hash_bytes(b"check interrupted before response");
    let (mut interrupted_check, mut interrupted_check_crypto) =
        open_data_session(&env, &client).await;
    send_frame(
        &mut interrupted_check,
        &mut interrupted_check_crypto,
        CHECK_OBJECTS,
        1,
        &check_request(&never_stored),
    )
    .await;
    interrupted_check.close(None).await.unwrap();
    let (mut checked_again, mut checked_again_crypto) = open_data_session(&env, &client).await;
    send_frame(
        &mut checked_again,
        &mut checked_again_crypto,
        CHECK_OBJECTS,
        1,
        &check_request(&never_stored),
    )
    .await;
    assert_check(
        &receive_frame(&mut checked_again, &mut checked_again_crypto).await,
        1,
        true,
    );
    checked_again.close(None).await.unwrap();

    let bytes = b"multiplexed websocket object";
    let hash = hash_bytes(bytes);
    let (mut socket, mut crypto) = open_data_session(&env, &client).await;

    // Both requests are in flight together; request ids make either response
    // order unambiguous. The check is allowed to race the durable PUT.
    send_frame(
        &mut socket,
        &mut crypto,
        CHECK_OBJECTS,
        1,
        &check_request(&hash),
    )
    .await;
    send_frame(
        &mut socket,
        &mut crypto,
        PUT_PACK,
        2,
        &put_request(&hash, bytes),
    )
    .await;
    let first = receive_frame(&mut socket, &mut crypto).await;
    let second = receive_frame(&mut socket, &mut crypto).await;
    let mut by_id =
        std::collections::HashMap::from([(first.request_id, first), (second.request_id, second)]);
    let check = by_id.remove(&1).expect("CHECK response missing");
    assert_eq!(check.kind, CHECK_RESULT);
    assert_put_ack(by_id.get(&2).expect("PUT response missing"), 2);

    send_frame(
        &mut socket,
        &mut crypto,
        CHECK_OBJECTS,
        3,
        &check_request(&hash),
    )
    .await;
    assert_check(&receive_frame(&mut socket, &mut crypto).await, 3, false);
    send_frame(&mut socket, &mut crypto, GET_PACK, 4, &get_request(&hash)).await;
    assert_get(
        &receive_frame(&mut socket, &mut crypto).await,
        4,
        &hash,
        bytes,
    );

    // A lost GET result is likewise replay-free: no apply checkpoint moves
    // until the client has consumed and verified the returned object bytes.
    send_frame(&mut socket, &mut crypto, GET_PACK, 5, &get_request(&hash)).await;
    socket.close(None).await.unwrap();
    let (mut socket, mut crypto) = open_data_session(&env, &client).await;
    send_frame(&mut socket, &mut crypto, GET_PACK, 1, &get_request(&hash)).await;
    assert_get(
        &receive_frame(&mut socket, &mut crypto).await,
        1,
        &hash,
        bytes,
    );

    // Drop a second PUT without consuming its ACK. Its outcome is unknown by
    // design; a fresh session checks the address and retries only if missing.
    let interrupted_bytes = b"unknown result must remain idempotent";
    let interrupted_hash = hash_bytes(interrupted_bytes);
    send_frame(
        &mut socket,
        &mut crypto,
        PUT_PACK,
        2,
        &put_request(&interrupted_hash, interrupted_bytes),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    socket.close(None).await.unwrap();

    let (mut recovered_socket, mut recovered_crypto) = open_data_session(&env, &client).await;
    send_frame(
        &mut recovered_socket,
        &mut recovered_crypto,
        CHECK_OBJECTS,
        1,
        &check_request(&interrupted_hash),
    )
    .await;
    let check = receive_frame(&mut recovered_socket, &mut recovered_crypto).await;
    let missing = check.payload[8] & 1 != 0;
    if missing {
        send_frame(
            &mut recovered_socket,
            &mut recovered_crypto,
            PUT_PACK,
            2,
            &put_request(&interrupted_hash, interrupted_bytes),
        )
        .await;
        assert_put_ack(
            &receive_frame(&mut recovered_socket, &mut recovered_crypto).await,
            2,
        );
    }
    send_frame(
        &mut recovered_socket,
        &mut recovered_crypto,
        GET_PACK,
        3,
        &get_request(&interrupted_hash),
    )
    .await;
    assert_get(
        &receive_frame(&mut recovered_socket, &mut recovered_crypto).await,
        3,
        &interrupted_hash,
        interrupted_bytes,
    );

    // A repeated request id is a session-level protocol violation. Closing
    // the lane prevents ambiguous response correlation or replay.
    send_frame(
        &mut recovered_socket,
        &mut recovered_crypto,
        CHECK_OBJECTS,
        3,
        &check_request(&interrupted_hash),
    )
    .await;
    assert_socket_ends(&mut recovered_socket).await;

    // Structural failure after successful AEAD authentication must also fail
    // closed; otherwise an attacker-controlled length/type could desync the
    // multiplexed parser while keeping the crypto sequence valid.
    let (mut malformed_socket, mut malformed_crypto) = open_data_session(&env, &client).await;
    let mut malformed = encode_frame(CHECK_OBJECTS, 1, &check_request(&hash));
    malformed[0] = b'X';
    malformed_socket
        .send(Message::Binary(malformed_crypto.seal(&malformed).into()))
        .await
        .unwrap();
    assert_socket_ends(&mut malformed_socket).await;

    let (mut plaintext_socket, _plaintext_crypto) = open_data_session(&env, &client).await;
    plaintext_socket
        .send(Message::Text("post-auth plaintext".into()))
        .await
        .unwrap();
    assert_socket_ends(&mut plaintext_socket).await;
}
