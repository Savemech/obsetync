//! Sealed WebSocket bulk data lane: independent crypto/protocol client.
//!
//! Covers explicit OBW1 compatibility, OBW2 fragmentation/reassembly,
//! multiplexed binary CHECK/PUT, bounded GET, and the critical unknown-result
//! recovery rule when the socket disappears before the client consumes an ACK.

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
const FRAME_V2_MAGIC: &[u8; 4] = b"OBW2";
const FRAME_V2_HEADER_BYTES: usize = 30;
const FRAGMENT_FIRST: u8 = 1;
const FRAGMENT_LAST: u8 = 2;
const FRAME_LIMIT: usize = 4 * 1024 * 1024;
const V2_LOGICAL_LIMIT: usize = 64 * 1024;
const V2_FRAGMENT_BYTES: usize = 1024;
const V2_MAX_FRAGMENTS: usize = 64;

const HELLO: u8 = 1;
const HELLO_ACK: u8 = 2;
const CHECK_OBJECTS: u8 = 3;
const CHECK_RESULT: u8 = 4;
const PUT_PACK: u8 = 5;
const PUT_ACK: u8 = 6;
const GET_PACK: u8 = 7;
const GET_RESULT: u8 = 8;
const CANCEL: u8 = 9;
const ERROR: u8 = 10;

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

#[derive(Debug, Clone, Copy)]
struct V2Lane {
    max_payload_bytes: usize,
    max_inflight_requests: usize,
    max_inflight_bytes: usize,
    max_fragment_payload_bytes: usize,
    max_fragments: usize,
}

#[derive(Debug)]
struct DataFragment {
    kind: u8,
    request_id: u64,
    logical_len: usize,
    offset: usize,
    fragment_index: usize,
    fragment_count: usize,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct FragmentAssembly {
    kind: u8,
    logical_len: usize,
    fragment_count: usize,
    next_fragment_index: usize,
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

fn fragment_count(logical_len: usize, max_fragment_payload_bytes: usize) -> usize {
    assert!(max_fragment_payload_bytes > 0);
    if logical_len == 0 {
        1
    } else {
        logical_len.div_ceil(max_fragment_payload_bytes)
    }
}

fn encode_fragment(
    kind: u8,
    request_id: u64,
    logical_len: usize,
    offset: usize,
    fragment_index: usize,
    fragment_count_value: usize,
    payload: &[u8],
    max_fragment_payload_bytes: usize,
    max_fragments: usize,
) -> Vec<u8> {
    assert!(logical_len <= FRAME_LIMIT);
    assert!(fragment_count_value > 0 && fragment_count_value <= max_fragments);
    assert_eq!(
        fragment_count_value,
        fragment_count(logical_len, max_fragment_payload_bytes)
    );
    assert!(fragment_index < fragment_count_value);
    assert_eq!(offset, fragment_index * max_fragment_payload_bytes);
    assert_eq!(
        payload.len(),
        logical_len
            .saturating_sub(offset)
            .min(max_fragment_payload_bytes)
    );
    let flags = (if fragment_index == 0 {
        FRAGMENT_FIRST
    } else {
        0
    }) | (if fragment_index + 1 == fragment_count_value {
        FRAGMENT_LAST
    } else {
        0
    });
    let mut output = Vec::with_capacity(FRAME_V2_HEADER_BYTES + payload.len());
    output.extend_from_slice(FRAME_V2_MAGIC);
    output.push(kind);
    output.push(flags);
    output.extend_from_slice(&request_id.to_le_bytes());
    output.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    output.extend_from_slice(&(logical_len as u32).to_le_bytes());
    output.extend_from_slice(&(offset as u32).to_le_bytes());
    output.extend_from_slice(&(fragment_index as u16).to_le_bytes());
    output.extend_from_slice(&(fragment_count_value as u16).to_le_bytes());
    output.extend_from_slice(payload);
    output
}

fn encode_logical_fragments(
    kind: u8,
    request_id: u64,
    payload: &[u8],
    max_fragment_payload_bytes: usize,
    max_fragments: usize,
) -> Vec<Vec<u8>> {
    let count = fragment_count(payload.len(), max_fragment_payload_bytes);
    assert!(count <= max_fragments);
    (0..count)
        .map(|index| {
            let offset = index * max_fragment_payload_bytes;
            let end = (offset + max_fragment_payload_bytes).min(payload.len());
            encode_fragment(
                kind,
                request_id,
                payload.len(),
                offset,
                index,
                count,
                &payload[offset..end],
                max_fragment_payload_bytes,
                max_fragments,
            )
        })
        .collect()
}

fn decode_fragment(input: &[u8], lane: V2Lane) -> DataFragment {
    assert!(input.len() >= FRAME_V2_HEADER_BYTES);
    assert_eq!(&input[..4], FRAME_V2_MAGIC);
    let flags = input[5];
    assert_eq!(flags & !(FRAGMENT_FIRST | FRAGMENT_LAST), 0);
    let request_id = u64::from_le_bytes(input[6..14].try_into().unwrap());
    let fragment_len = u32::from_le_bytes(input[14..18].try_into().unwrap()) as usize;
    let logical_len = u32::from_le_bytes(input[18..22].try_into().unwrap()) as usize;
    let offset = u32::from_le_bytes(input[22..26].try_into().unwrap()) as usize;
    let fragment_index = u16::from_le_bytes(input[26..28].try_into().unwrap()) as usize;
    let fragment_count_value = u16::from_le_bytes(input[28..30].try_into().unwrap()) as usize;
    assert!(logical_len <= lane.max_payload_bytes);
    assert!(fragment_len <= lane.max_fragment_payload_bytes);
    assert!(fragment_count_value > 0 && fragment_count_value <= lane.max_fragments);
    assert_eq!(
        fragment_count_value,
        fragment_count(logical_len, lane.max_fragment_payload_bytes)
    );
    assert!(fragment_index < fragment_count_value);
    assert_eq!(offset, fragment_index * lane.max_fragment_payload_bytes);
    assert_eq!(
        fragment_len,
        logical_len
            .saturating_sub(offset)
            .min(lane.max_fragment_payload_bytes)
    );
    assert_eq!(input.len(), FRAME_V2_HEADER_BYTES + fragment_len);
    let expected_flags = (if fragment_index == 0 {
        FRAGMENT_FIRST
    } else {
        0
    }) | (if fragment_index + 1 == fragment_count_value {
        FRAGMENT_LAST
    } else {
        0
    });
    assert_eq!(flags, expected_flags);
    DataFragment {
        kind: input[4],
        request_id,
        logical_len,
        offset,
        fragment_index,
        fragment_count: fragment_count_value,
        payload: input[FRAME_V2_HEADER_BYTES..].to_vec(),
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

fn hello_v2_payload() -> Vec<u8> {
    let mut output = Vec::with_capacity(32);
    output.extend_from_slice(b"OWH2");
    output.extend_from_slice(&2u16.to_le_bytes());
    output.extend_from_slice(&4u16.to_le_bytes());
    output.extend_from_slice(&(V2_LOGICAL_LIMIT as u32).to_le_bytes());
    output.extend_from_slice(&0u32.to_le_bytes());
    output.extend_from_slice(&(4u64 * V2_LOGICAL_LIMIT as u64).to_le_bytes());
    output.extend_from_slice(&(V2_FRAGMENT_BYTES as u32).to_le_bytes());
    output.extend_from_slice(&(V2_MAX_FRAGMENTS as u16).to_le_bytes());
    output.extend_from_slice(&0u16.to_le_bytes());
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

async fn open_data_session_v2(env: &E2eEnv, client: &WireClient) -> (WsStream, WsCrypto, V2Lane) {
    let (mut socket, mut crypto) = open_authenticated_data_socket(env, client).await;
    let hello = hello_v2_payload();
    let outer = encode_fragment(
        HELLO,
        0,
        hello.len(),
        0,
        0,
        1,
        &hello,
        64 * 1024,
        V2_MAX_FRAGMENTS,
    );
    send_plaintext(&mut socket, &mut crypto, &outer).await;

    let requested = V2Lane {
        max_payload_bytes: V2_LOGICAL_LIMIT,
        max_inflight_requests: 4,
        max_inflight_bytes: 4 * V2_LOGICAL_LIMIT,
        max_fragment_payload_bytes: V2_FRAGMENT_BYTES,
        max_fragments: V2_MAX_FRAGMENTS,
    };
    let ack = receive_fragment(&mut socket, &mut crypto, requested).await;
    assert_eq!(ack.kind, HELLO_ACK);
    assert_eq!(ack.request_id, 0);
    assert_eq!(ack.fragment_count, 1);
    assert_eq!(ack.payload.len(), 32);
    assert_eq!(&ack.payload[..4], b"OWA2");
    assert_eq!(u16::from_le_bytes(ack.payload[4..6].try_into().unwrap()), 2);
    assert_eq!(&ack.payload[12..16], &[0, 0, 0, 0]);
    assert_eq!(&ack.payload[30..32], &[0, 0]);
    let lane = V2Lane {
        max_inflight_requests: u16::from_le_bytes(ack.payload[6..8].try_into().unwrap()) as usize,
        max_payload_bytes: u32::from_le_bytes(ack.payload[8..12].try_into().unwrap()) as usize,
        max_inflight_bytes: usize::try_from(u64::from_le_bytes(
            ack.payload[16..24].try_into().unwrap(),
        ))
        .unwrap(),
        max_fragment_payload_bytes: u32::from_le_bytes(ack.payload[24..28].try_into().unwrap())
            as usize,
        max_fragments: u16::from_le_bytes(ack.payload[28..30].try_into().unwrap()) as usize,
    };
    assert!(lane.max_payload_bytes > 0 && lane.max_payload_bytes <= requested.max_payload_bytes);
    assert!(
        lane.max_inflight_requests > 0
            && lane.max_inflight_requests <= requested.max_inflight_requests
    );
    assert!(lane.max_inflight_bytes >= lane.max_payload_bytes);
    assert!(lane.max_inflight_bytes <= requested.max_inflight_bytes);
    assert_eq!(lane.max_fragment_payload_bytes, V2_FRAGMENT_BYTES);
    assert!(lane.max_fragments > 0 && lane.max_fragments <= requested.max_fragments);
    (socket, crypto, lane)
}

async fn send_plaintext(socket: &mut WsStream, crypto: &mut WsCrypto, plaintext: &[u8]) {
    socket
        .send(Message::Binary(crypto.seal(plaintext).into()))
        .await
        .unwrap();
}

async fn send_frame(
    socket: &mut WsStream,
    crypto: &mut WsCrypto,
    kind: u8,
    request_id: u64,
    payload: &[u8],
) {
    let plaintext = encode_frame(kind, request_id, payload);
    send_plaintext(socket, crypto, &plaintext).await;
}

async fn send_fragments(socket: &mut WsStream, crypto: &mut WsCrypto, fragments: &[Vec<u8>]) {
    for fragment in fragments {
        send_plaintext(socket, crypto, fragment).await;
    }
}

async fn send_logical_v2(
    socket: &mut WsStream,
    crypto: &mut WsCrypto,
    lane: V2Lane,
    kind: u8,
    request_id: u64,
    payload: &[u8],
) -> usize {
    assert!(payload.len() <= lane.max_payload_bytes);
    let fragments = encode_logical_fragments(
        kind,
        request_id,
        payload,
        lane.max_fragment_payload_bytes,
        lane.max_fragments,
    );
    let count = fragments.len();
    send_fragments(socket, crypto, &fragments).await;
    count
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

async fn receive_fragment(
    socket: &mut WsStream,
    crypto: &mut WsCrypto,
    lane: V2Lane,
) -> DataFragment {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Binary(data))) => {
                    return decode_fragment(&crypto.open(&data), lane);
                }
                Some(Ok(_)) => continue,
                other => panic!("data lane ended before OBW2 fragment: {other:?}"),
            }
        }
    })
    .await
    .expect("OBW2 fragment timeout")
}

async fn receive_logical_v2(
    socket: &mut WsStream,
    crypto: &mut WsCrypto,
    lane: V2Lane,
    expected: usize,
) -> Vec<(DataFrame, usize)> {
    let mut assembling = std::collections::HashMap::<u64, FragmentAssembly>::new();
    let mut completed = Vec::with_capacity(expected);
    while completed.len() < expected {
        let fragment = receive_fragment(socket, crypto, lane).await;
        if fragment.fragment_index == 0 {
            assert_eq!(fragment.offset, 0);
            assert!(!assembling.contains_key(&fragment.request_id));
            let request_id = fragment.request_id;
            let count = fragment.fragment_count;
            let assembly = FragmentAssembly {
                kind: fragment.kind,
                logical_len: fragment.logical_len,
                fragment_count: count,
                next_fragment_index: 1,
                payload: fragment.payload,
            };
            if count == 1 {
                assert_eq!(assembly.payload.len(), assembly.logical_len);
                completed.push((
                    DataFrame {
                        kind: assembly.kind,
                        request_id,
                        payload: assembly.payload,
                    },
                    count,
                ));
            } else {
                assembling.insert(request_id, assembly);
            }
            continue;
        }

        let request_id = fragment.request_id;
        let assembly = assembling
            .get_mut(&request_id)
            .expect("OBW2 response started with a non-FIRST fragment");
        assert_eq!(fragment.kind, assembly.kind);
        assert_eq!(fragment.logical_len, assembly.logical_len);
        assert_eq!(fragment.fragment_count, assembly.fragment_count);
        assert_eq!(fragment.fragment_index, assembly.next_fragment_index);
        assert_eq!(fragment.offset, assembly.payload.len());
        assembly.payload.extend_from_slice(&fragment.payload);
        assembly.next_fragment_index += 1;
        if assembly.next_fragment_index == assembly.fragment_count {
            let assembly = assembling.remove(&request_id).unwrap();
            assert_eq!(assembly.payload.len(), assembly.logical_len);
            completed.push((
                DataFrame {
                    kind: assembly.kind,
                    request_id,
                    payload: assembly.payload,
                },
                assembly.fragment_count,
            ));
        }
    }
    assert!(assembling.is_empty());
    completed
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

fn get_request_with_budget(hash: &FileHash, max_response_bytes: usize) -> Vec<u8> {
    let mut output = Vec::with_capacity(49);
    output.extend_from_slice(b"OBG1");
    output.push(0); // content
    output.extend_from_slice(&1u32.to_le_bytes());
    output.extend_from_slice(&0u32.to_le_bytes());
    output.extend_from_slice(
        &u32::try_from(max_response_bytes)
            .expect("GET response budget must fit the wire")
            .to_le_bytes(),
    );
    output.extend_from_slice(hash);
    output
}

fn get_request(hash: &FileHash) -> Vec<u8> {
    get_request_with_budget(hash, 1024 * 1024)
}

fn assert_check(frame: &DataFrame, request_id: u64, needed: bool) {
    assert_eq!(frame.kind, CHECK_RESULT);
    assert_eq!(frame.request_id, request_id);
    assert_eq!(frame.payload.len(), 9);
    assert_eq!(&frame.payload[..4], b"OBA1");
    assert_eq!(
        u32::from_le_bytes(frame.payload[4..8].try_into().unwrap()),
        1
    );
    assert_eq!(frame.payload[8] & 1 != 0, needed);
    assert_eq!(frame.payload[8] & !1, 0);
}

fn assert_put_ack(frame: &DataFrame, request_id: u64) {
    assert_eq!(frame.kind, PUT_ACK);
    assert_eq!(frame.request_id, request_id);
    assert_eq!(frame.payload.len(), 9);
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
    assert!(frame.payload.len() >= 13);
    assert_eq!(&frame.payload[..4], b"OBD1");
    assert_eq!(
        u32::from_le_bytes(frame.payload[4..8].try_into().unwrap()),
        1
    );
    assert_eq!(
        u32::from_le_bytes(frame.payload[8..12].try_into().unwrap()),
        1
    );
    assert_eq!(frame.payload[12], 0);
    let bitmap_bytes = 1usize;
    let pack = &frame.payload[12 + bitmap_bytes..];
    assert_eq!(pack.len(), 52 + bytes.len());
    assert_eq!(&pack[..4], b"OBP1");
    assert_eq!(&pack[4..6], &[0, 0]);
    assert_eq!(u32::from_le_bytes(pack[6..10].try_into().unwrap()), 1);
    assert_eq!(&pack[10..12], &[0, 0]);
    assert_eq!(&pack[12..44], hash);
    assert_eq!(
        u32::from_le_bytes(pack[44..48].try_into().unwrap()) as usize,
        bytes.len()
    );
    assert_eq!(
        u32::from_le_bytes(pack[48..52].try_into().unwrap()) as usize,
        bytes.len()
    );
    assert_eq!(&pack[52..], bytes);
}

fn assert_error(frame: &DataFrame, request_id: u64, code: u16) -> u32 {
    assert_eq!(frame.kind, ERROR);
    assert_eq!(frame.request_id, request_id);
    assert!(frame.payload.len() >= 14);
    assert_eq!(&frame.payload[..4], b"OWE1");
    assert_eq!(
        u16::from_le_bytes(frame.payload[4..6].try_into().unwrap()),
        code
    );
    assert_eq!(&frame.payload[6..8], &[0, 0]);
    let retry_after_ms = u32::from_le_bytes(frame.payload[8..12].try_into().unwrap());
    let message_len = u16::from_le_bytes(frame.payload[12..14].try_into().unwrap()) as usize;
    assert_eq!(frame.payload.len(), 14 + message_len);
    assert!(std::str::from_utf8(&frame.payload[14..]).is_ok());
    retry_after_ms
}

fn assert_cancelled(frame: &DataFrame, request_id: u64) {
    assert_eq!(assert_error(frame, request_id, 4), 0);
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

#[tokio::test]
async fn sealed_data_lane_obw2_fragments_interleaved_puts_and_gets() {
    let env = harness().await;
    let client = WireClient::new(&env, env.enroll_device("ws-data-obw2").await.unwrap());
    let (mut socket, mut crypto, lane) = open_data_session_v2(&env, &client).await;

    let first_bytes: Vec<u8> = (0..(3 * V2_FRAGMENT_BYTES + 137))
        .map(|index| (index % 251) as u8)
        .collect();
    let second_bytes: Vec<u8> = (0..(2 * V2_FRAGMENT_BYTES + 19))
        .map(|index| (255 - index % 239) as u8)
        .collect();
    let first_hash = hash_bytes(&first_bytes);
    let second_hash = hash_bytes(&second_bytes);
    let first_put = encode_logical_fragments(
        PUT_PACK,
        1,
        &put_request(&first_hash, &first_bytes),
        lane.max_fragment_payload_bytes,
        lane.max_fragments,
    );
    let second_put = encode_logical_fragments(
        PUT_PACK,
        2,
        &put_request(&second_hash, &second_bytes),
        lane.max_fragment_payload_bytes,
        lane.max_fragments,
    );
    assert!(first_put.len() > 1 && second_put.len() > 1);
    assert!(first_put
        .iter()
        .chain(&second_put)
        .all(|fragment| fragment.len() <= FRAME_V2_HEADER_BYTES + lane.max_fragment_payload_bytes));

    // Begin request 2 before finishing request 1. The production server must
    // retain two independently bounded reassemblies and correlate both ACKs.
    for index in 0..first_put.len().max(second_put.len()) {
        if let Some(fragment) = first_put.get(index) {
            send_plaintext(&mut socket, &mut crypto, fragment).await;
        }
        if let Some(fragment) = second_put.get(index) {
            send_plaintext(&mut socket, &mut crypto, fragment).await;
        }
    }
    let put_responses = receive_logical_v2(&mut socket, &mut crypto, lane, 2).await;
    let mut puts: std::collections::HashMap<u64, DataFrame> = put_responses
        .into_iter()
        .map(|(frame, fragments)| {
            assert_eq!(fragments, 1, "bounded PUT ACK unexpectedly fragmented");
            (frame.request_id, frame)
        })
        .collect();
    assert_put_ack(&puts.remove(&1).expect("first PUT ACK missing"), 1);
    assert_put_ack(&puts.remove(&2).expect("second PUT ACK missing"), 2);
    assert!(puts.is_empty());

    assert_eq!(
        send_logical_v2(
            &mut socket,
            &mut crypto,
            lane,
            GET_PACK,
            3,
            &get_request_with_budget(&first_hash, lane.max_payload_bytes),
        )
        .await,
        1
    );
    assert_eq!(
        send_logical_v2(
            &mut socket,
            &mut crypto,
            lane,
            GET_PACK,
            4,
            &get_request_with_budget(&second_hash, lane.max_payload_bytes),
        )
        .await,
        1
    );
    let get_responses = receive_logical_v2(&mut socket, &mut crypto, lane, 2).await;
    let mut gets: std::collections::HashMap<u64, (DataFrame, usize)> = get_responses
        .into_iter()
        .map(|(frame, fragments)| (frame.request_id, (frame, fragments)))
        .collect();
    let (first, first_fragments) = gets.remove(&3).expect("first GET result missing");
    let (second, second_fragments) = gets.remove(&4).expect("second GET result missing");
    assert!(first_fragments > 1 && second_fragments > 1);
    assert_get(&first, 3, &first_hash, &first_bytes);
    assert_get(&second, 4, &second_hash, &second_bytes);
    assert!(gets.is_empty());

    // Fill both negotiated admission dimensions with incomplete logical PUTs:
    // every request reserves max_payload_bytes, and their count reaches the
    // negotiated request cap. The next request must receive a bounded BUSY
    // response without closing or desynchronizing the lane.
    assert_eq!(
        lane.max_inflight_bytes,
        lane.max_payload_bytes * lane.max_inflight_requests
    );
    let cancelled_bytes = vec![19u8; lane.max_payload_bytes - 52];
    let cancelled_hash = hash_bytes(&cancelled_bytes);
    let cancelled_request = put_request(&cancelled_hash, &cancelled_bytes);
    assert_eq!(cancelled_request.len(), lane.max_payload_bytes);
    let first_cancel_id = 5u64;
    for request_id in first_cancel_id..first_cancel_id + lane.max_inflight_requests as u64 {
        let fragments = encode_logical_fragments(
            PUT_PACK,
            request_id,
            &cancelled_request,
            lane.max_fragment_payload_bytes,
            lane.max_fragments,
        );
        assert_eq!(fragments.len(), lane.max_fragments);
        send_plaintext(&mut socket, &mut crypto, &fragments[0]).await;
    }

    let overflow_id = first_cancel_id + lane.max_inflight_requests as u64;
    assert_eq!(
        send_logical_v2(
            &mut socket,
            &mut crypto,
            lane,
            PUT_PACK,
            overflow_id,
            &cancelled_request,
        )
        .await,
        lane.max_fragments
    );
    let mut busy = receive_logical_v2(&mut socket, &mut crypto, lane, 1).await;
    let (busy, fragments) = busy.pop().unwrap();
    assert_eq!(fragments, 1);
    assert!(assert_error(&busy, overflow_id, 2) > 0);

    // CANCEL is a canonical zero-length control request using the target's id.
    // Receiving every terminal response is the release boundary: only then
    // may the server return that request's admission ownership to the lane.
    for request_id in first_cancel_id..overflow_id {
        assert_eq!(
            send_logical_v2(&mut socket, &mut crypto, lane, CANCEL, request_id, &[]).await,
            1
        );
    }
    let cancelled =
        receive_logical_v2(&mut socket, &mut crypto, lane, lane.max_inflight_requests).await;
    let mut cancelled_by_id = std::collections::HashMap::new();
    for (frame, fragments) in cancelled {
        assert_eq!(fragments, 1);
        let request_id = frame.request_id;
        assert!(cancelled_by_id.insert(request_id, frame).is_none());
    }
    for request_id in first_cancel_id..overflow_id {
        assert_cancelled(
            &cancelled_by_id
                .remove(&request_id)
                .expect("CANCEL terminal response missing"),
            request_id,
        );
    }
    assert!(cancelled_by_id.is_empty());

    // Refill the complete byte and request window. If any cancellation leaked
    // either admission dimension, at least one target becomes a rejected BUSY
    // tombstone and therefore cannot produce the expected CANCEL response.
    let refill_first_id = overflow_id + 1;
    let refill_end_id = refill_first_id + lane.max_inflight_requests as u64;
    for request_id in refill_first_id..refill_end_id {
        let fragments = encode_logical_fragments(
            PUT_PACK,
            request_id,
            &cancelled_request,
            lane.max_fragment_payload_bytes,
            lane.max_fragments,
        );
        send_plaintext(&mut socket, &mut crypto, &fragments[0]).await;
    }
    for request_id in refill_first_id..refill_end_id {
        send_logical_v2(&mut socket, &mut crypto, lane, CANCEL, request_id, &[]).await;
    }
    let refilled =
        receive_logical_v2(&mut socket, &mut crypto, lane, lane.max_inflight_requests).await;
    let mut refilled_by_id = std::collections::HashMap::new();
    for (frame, fragments) in refilled {
        assert_eq!(fragments, 1);
        let request_id = frame.request_id;
        assert!(refilled_by_id.insert(request_id, frame).is_none());
    }
    for request_id in refill_first_id..refill_end_id {
        assert_cancelled(
            &refilled_by_id
                .remove(&request_id)
                .expect("refilled request was not admitted and cancelled"),
            request_id,
        );
    }
    assert!(refilled_by_id.is_empty());

    // No partial PUT reached storage, and the same session remains usable.
    let check_id = refill_end_id;
    assert_eq!(
        send_logical_v2(
            &mut socket,
            &mut crypto,
            lane,
            CHECK_OBJECTS,
            check_id,
            &check_request(&cancelled_hash),
        )
        .await,
        1
    );
    let mut checked = receive_logical_v2(&mut socket, &mut crypto, lane, 1).await;
    let (checked, fragments) = checked.pop().unwrap();
    assert_eq!(fragments, 1);
    assert_check(&checked, check_id, true);
    socket.close(None).await.unwrap();
}

#[tokio::test]
async fn sealed_data_lane_obw2_rejects_fragment_limits_and_downgrades_freshly_to_obw1() {
    let env = harness().await;
    let client = WireClient::new(
        &env,
        env.enroll_device("ws-data-obw2-reject").await.unwrap(),
    );
    let bytes = vec![7u8; V2_FRAGMENT_BYTES + 1];
    let hash = hash_bytes(&bytes);
    let request = put_request(&hash, &bytes);

    for case in [
        "logical-limit",
        "fragment-payload-limit",
        "fragment-count-limit",
        "gapped-first-fragment",
    ] {
        let (mut socket, mut crypto, lane) = open_data_session_v2(&env, &client).await;
        let fragments = encode_logical_fragments(
            PUT_PACK,
            1,
            &request,
            lane.max_fragment_payload_bytes,
            lane.max_fragments,
        );
        assert!(fragments.len() > 1);
        let mut malformed = match case {
            "logical-limit" => {
                let mut first = fragments[0].clone();
                let over_limit = u32::try_from(lane.max_payload_bytes + 1).unwrap();
                first[18..22].copy_from_slice(&over_limit.to_le_bytes());
                first
            }
            "fragment-payload-limit" => {
                let mut first = fragments[0].clone();
                let over_limit = u32::try_from(lane.max_fragment_payload_bytes + 1).unwrap();
                first[14..18].copy_from_slice(&over_limit.to_le_bytes());
                first.push(0);
                first
            }
            "fragment-count-limit" => {
                let mut first = fragments[0].clone();
                let over_limit = u16::try_from(lane.max_fragments + 1).unwrap();
                first[28..30].copy_from_slice(&over_limit.to_le_bytes());
                first
            }
            "gapped-first-fragment" => fragments[1].clone(),
            _ => unreachable!(),
        };
        // Keep the mutation explicit and consumed once; AEAD remains valid, so
        // closure proves the production inner parser rejected the fragment.
        send_plaintext(&mut socket, &mut crypto, &malformed).await;
        malformed.fill(0);
        assert_socket_ends(&mut socket).await;
    }

    // Downgrade never reuses the rejected socket, ticket, key, or sequence
    // space. A fresh explicit OBW1 HELLO remains compatible on the V2 server.
    let (mut socket, mut crypto) = open_data_session(&env, &client).await;
    let absent = hash_bytes(b"fresh OBW1 downgrade after rejected OBW2");
    send_frame(
        &mut socket,
        &mut crypto,
        CHECK_OBJECTS,
        1,
        &check_request(&absent),
    )
    .await;
    assert_check(&receive_frame(&mut socket, &mut crypto).await, 1, true);
    socket.close(None).await.unwrap();
}
