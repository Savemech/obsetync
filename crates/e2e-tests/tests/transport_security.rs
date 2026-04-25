//! Wire-format security boundary: the AEAD envelope must reject every
//! tampering vector the threat model calls out.

use e2e_tests::*;
use rand::TryRngCore;
use reqwest::StatusCode;
use sync_core::hash::{hash_bytes, hash_to_hex};
use x25519_dalek::{PublicKey, StaticSecret};

#[tokio::test]
async fn plaintext_request_to_protected_endpoint_is_rejected() {
    let env = harness().await;
    // Hit a protected route directly with no envelope. The middleware must
    // reject before the handler runs.
    let resp = env
        .http
        .post(format!("{}/api/v1/chunks/check", env.base_url))
        .header("X-Obsetync-Method", "POST")
        .body(b"[]".to_vec())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status() == StatusCode::UNAUTHORIZED || resp.status() == StatusCode::BAD_REQUEST,
        "plaintext POST must not be accepted, got {}",
        resp.status()
    );
}

#[tokio::test]
async fn envelope_encrypted_against_wrong_pubkey_is_unauthorized() {
    let env = harness().await;
    // Enroll properly so the bearer token is valid; then swap in a forged
    // server pubkey before encrypting. The server's box.priv won't match,
    // so AEAD decrypt fails and the middleware returns 401.
    let mut creds = env.enroll_device("wrong-pubkey").await.unwrap();
    let mut seed = [0u8; 32];
    rand::rngs::OsRng.try_fill_bytes(&mut seed).unwrap();
    let bogus_priv = StaticSecret::from(seed);
    creds.server_box_pub = PublicKey::from(&bogus_priv);

    let client = WireClient::new(&env, creds);
    let r = client
        .raw("POST", "/api/v1/chunks/check", b"[]")
        .await
        .unwrap();
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn unknown_bearer_token_is_unauthorized() {
    let env = harness().await;
    // Real server pubkey + bearer that's never been registered. The envelope
    // decrypts cleanly but `lookup_token` finds nothing.
    let creds = env.enroll_device("scaffold").await.unwrap();
    let mut bogus = creds.clone();
    bogus.bearer = (0..64)
        .map(|i| std::char::from_digit((i + 1) as u32 % 16, 16).unwrap())
        .collect();
    let client = WireClient::new(&env, bogus);
    let r = client
        .raw("POST", "/api/v1/chunks/check", b"[]")
        .await
        .unwrap();
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn cross_path_replay_is_rejected() {
    // AAD binds method+path. An envelope signed for /chunk/A must NOT decrypt
    // when replayed against /chunk/B — the AAD changes and AES-GCM fails.
    let env = harness().await;
    let creds = env.enroll_device("replay-cross-path").await.unwrap();
    let _client = WireClient::new(&env, creds.clone());

    // Capture a valid envelope for path1, then replay against path2.
    let path1 = format!("/api/v1/chunk/{}", hash_to_hex(&hash_bytes(b"path1")));
    let path2 = format!("/api/v1/chunk/{}", hash_to_hex(&hash_bytes(b"path2")));

    // Build the envelope using the exact AAD (method=PUT, path=path1).
    // The harness doesn't expose a "build envelope only" helper, so go via
    // the WireClient with normal flow first to confirm the path1 PUT works,
    // then forge a request whose body is a captured envelope from a fresh
    // generation against path1, but POST it to path2.
    use aes_gcm::aead::{Aead, Payload};
    use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
    use hkdf::Hkdf;
    use sha2::Sha256;

    let mut seed = [0u8; KEY_LEN];
    rand::rngs::OsRng.try_fill_bytes(&mut seed).unwrap();
    let eph = StaticSecret::from(seed);
    let shared = eph.diffie_hellman(&creds.server_box_pub);

    let mut nonce = [0u8; NONCE_LEN];
    rand::rngs::OsRng.try_fill_bytes(&mut nonce).unwrap();

    let mut key = [0u8; KEY_LEN];
    Hkdf::<Sha256>::new(Some(&nonce[..]), shared.as_bytes())
        .expand(b"obsetync/v1/c2s", &mut key)
        .unwrap();
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key));

    // AAD encodes path1; we'll send it to path2.
    let aad_path1 = format!("obsetync/v1 PUT {}", path1);
    let mut plaintext = Vec::with_capacity(64 + 4);
    plaintext.extend_from_slice(creds.bearer.as_bytes());
    plaintext.extend_from_slice(b"path1");

    let ct = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &plaintext,
                aad: aad_path1.as_bytes(),
            },
        )
        .unwrap();

    let our_pub = PublicKey::from(&eph);
    let mut envelope = Vec::with_capacity(REQUEST_HEADER_LEN + ct.len());
    envelope.push(WIRE_VERSION);
    envelope.extend_from_slice(&nonce);
    envelope.extend_from_slice(our_pub.as_bytes());
    envelope.extend_from_slice(&ct);

    // Replay against path2.
    let resp = env
        .http
        .post(format!("{}{}", env.base_url, path2))
        .header("X-Obsetync-Method", "PUT")
        .body(envelope)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "AEAD AAD must bind path; cross-path replay must 401"
    );
}

#[tokio::test]
async fn cross_method_replay_is_rejected() {
    // Mirror: same path, but X-Obsetync-Method differs from what the AAD
    // was signed with. AEAD AAD includes method, so this fails.
    let env = harness().await;
    let creds = env.enroll_device("replay-cross-method").await.unwrap();

    let path = format!("/api/v1/chunk/{}", hash_to_hex(&hash_bytes(b"any")));

    use aes_gcm::aead::{Aead, Payload};
    use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
    use hkdf::Hkdf;
    use sha2::Sha256;

    let mut seed = [0u8; KEY_LEN];
    rand::rngs::OsRng.try_fill_bytes(&mut seed).unwrap();
    let eph = StaticSecret::from(seed);
    let shared = eph.diffie_hellman(&creds.server_box_pub);

    let mut nonce = [0u8; NONCE_LEN];
    rand::rngs::OsRng.try_fill_bytes(&mut nonce).unwrap();
    let mut key = [0u8; KEY_LEN];
    Hkdf::<Sha256>::new(Some(&nonce[..]), shared.as_bytes())
        .expand(b"obsetync/v1/c2s", &mut key)
        .unwrap();
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key));

    // Signed for PUT, sent as GET via the X-Obsetync-Method header.
    let aad = format!("obsetync/v1 PUT {}", path);
    let mut plaintext = Vec::with_capacity(64);
    plaintext.extend_from_slice(creds.bearer.as_bytes());
    let ct = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &plaintext,
                aad: aad.as_bytes(),
            },
        )
        .unwrap();

    let our_pub = PublicKey::from(&eph);
    let mut envelope = Vec::new();
    envelope.push(WIRE_VERSION);
    envelope.extend_from_slice(&nonce);
    envelope.extend_from_slice(our_pub.as_bytes());
    envelope.extend_from_slice(&ct);

    let resp = env
        .http
        .post(format!("{}{}", env.base_url, path))
        .header("X-Obsetync-Method", "GET") // server uses this in AAD
        .body(envelope)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn tampered_ciphertext_is_rejected() {
    let env = harness().await;
    let creds = env.enroll_device("tampered").await.unwrap();
    let client = WireClient::new(&env, creds.clone());

    // Build a known-good envelope using the harness, then bit-flip the last
    // byte (the tag) and POST manually.
    use aes_gcm::aead::{Aead, Payload};
    use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
    use hkdf::Hkdf;
    use sha2::Sha256;

    let path = "/api/v1/chunks/check";
    let mut seed = [0u8; KEY_LEN];
    rand::rngs::OsRng.try_fill_bytes(&mut seed).unwrap();
    let eph = StaticSecret::from(seed);
    let shared = eph.diffie_hellman(&creds.server_box_pub);

    let mut nonce = [0u8; NONCE_LEN];
    rand::rngs::OsRng.try_fill_bytes(&mut nonce).unwrap();
    let mut key = [0u8; KEY_LEN];
    Hkdf::<Sha256>::new(Some(&nonce[..]), shared.as_bytes())
        .expand(b"obsetync/v1/c2s", &mut key)
        .unwrap();
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key));

    let aad = format!("obsetync/v1 POST {}", path);
    let mut plaintext = Vec::new();
    plaintext.extend_from_slice(creds.bearer.as_bytes());
    plaintext.extend_from_slice(b"[]");
    let mut ct = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &plaintext,
                aad: aad.as_bytes(),
            },
        )
        .unwrap();

    // Flip a byte in the GCM tag.
    let last = ct.len() - 1;
    ct[last] ^= 0x01;

    let our_pub = PublicKey::from(&eph);
    let mut envelope = Vec::new();
    envelope.push(WIRE_VERSION);
    envelope.extend_from_slice(&nonce);
    envelope.extend_from_slice(our_pub.as_bytes());
    envelope.extend_from_slice(&ct);

    let resp = env
        .http
        .post(format!("{}{}", env.base_url, path))
        .header("X-Obsetync-Method", "POST")
        .body(envelope)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Sanity: with the same client an *un*tampered request still works.
    let needed = client.chunks_check(&[]).await.unwrap();
    assert!(needed.is_empty());
}

#[tokio::test]
async fn bad_wire_version_byte_is_rejected() {
    let env = harness().await;
    // 100 bytes of zeros — first byte is wire version 0x00, not 0x01.
    let resp = env
        .http
        .post(format!("{}/api/v1/chunks/check", env.base_url))
        .header("X-Obsetync-Method", "POST")
        .body(vec![0u8; 100])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
