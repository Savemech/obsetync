//! Full-stack checks for the transport-v2 security boundary.

use e2e_tests::*;
use rand::TryRngCore;
use reqwest::StatusCode;
use sync_core::hash::{hash_bytes, hash_to_hex};
use x25519_dalek::{PublicKey, StaticSecret};

fn assert_decoy(status: StatusCode, body: &[u8]) {
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.len(), 256);
    assert!(body.iter().all(|byte| *byte == 0));
}

#[tokio::test]
async fn plaintext_request_to_protected_endpoint_gets_constant_decoy() {
    let env = harness().await;
    let resp = env
        .http
        .post(format!("{}/api/v1/chunks/check", env.base_url))
        .header("X-Obsetync-Method", "POST")
        .body(b"[]".to_vec())
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.bytes().await.unwrap();
    assert_decoy(status, &body);
}

#[tokio::test]
async fn envelope_encrypted_against_wrong_pubkey_is_unauthorized() {
    let env = harness().await;
    let mut creds = env.enroll_device("wrong-pubkey").await.unwrap();
    let mut seed = [0u8; 32];
    rand::rngs::OsRng.try_fill_bytes(&mut seed).unwrap();
    creds.server_box_pub = PublicKey::from(&StaticSecret::from(seed));

    let client = WireClient::new(&env, creds);
    let response = client
        .raw("POST", "/api/v1/chunks/check", b"[]")
        .await
        .unwrap();
    assert_eq!(response.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn unknown_bearer_status_is_encrypted() {
    let env = harness().await;
    let mut creds = env.enroll_device("unknown-bearer").await.unwrap();
    creds.bearer = "fe".repeat(32);
    let client = WireClient::new(&env, creds);

    let response = client
        .raw("POST", "/api/v1/chunks/check", b"[]")
        .await
        .unwrap();
    assert_eq!(response.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()["error"],
        "unknown_bearer",
    );
}

#[tokio::test]
async fn cross_path_replay_is_rejected() {
    let env = harness().await;
    let client = WireClient::new(&env, env.enroll_device("replay-cross-path").await.unwrap());
    let path1 = format!("/api/v1/chunk/{}", hash_to_hex(&hash_bytes(b"path1")));
    let path2 = format!("/api/v1/chunk/{}", hash_to_hex(&hash_bytes(b"path2")));
    let sealed = client.seal_for_test("PUT", &path1, b"path1").unwrap();

    let response = client
        .send_sealed_for_test(&sealed, "PUT", &path2)
        .await
        .unwrap();
    assert_eq!(response.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn cross_method_replay_is_rejected() {
    let env = harness().await;
    let client = WireClient::new(
        &env,
        env.enroll_device("replay-cross-method").await.unwrap(),
    );
    let path = format!("/api/v1/chunk/{}", hash_to_hex(&hash_bytes(b"method")));
    let sealed = client.seal_for_test("PUT", &path, b"method").unwrap();

    let response = client
        .send_sealed_for_test(&sealed, "GET", &path)
        .await
        .unwrap();
    assert_eq!(response.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn exact_same_request_replay_is_rejected_once_opened() {
    let env = harness().await;
    let client = WireClient::new(
        &env,
        env.enroll_device("replay-same-endpoint").await.unwrap(),
    );
    let path = "/api/v1/root/replay-does-not-exist";
    let sealed = client.seal_for_test("GET", path, b"").unwrap();

    let first = client
        .send_sealed_for_test(&sealed, "GET", path)
        .await
        .unwrap();
    assert_eq!(first.status, StatusCode::NOT_FOUND);

    let replay = client
        .send_sealed_for_test(&sealed, "GET", path)
        .await
        .unwrap();
    assert_eq!(replay.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&replay.body).unwrap()["error"],
        "replay",
    );
}

#[tokio::test]
async fn tampered_ciphertext_gets_decoy() {
    let env = harness().await;
    let client = WireClient::new(&env, env.enroll_device("tampered").await.unwrap());
    let mut sealed = client
        .seal_for_test("POST", "/api/v1/chunks/check", b"[]")
        .unwrap();
    sealed.corrupt_last_byte();

    let response = client
        .send_sealed_for_test(&sealed, "POST", "/api/v1/chunks/check")
        .await
        .unwrap();
    assert_eq!(response.status, StatusCode::UNAUTHORIZED);

    let needed = client.chunks_check(&[]).await.unwrap();
    assert!(needed.is_empty());
}

#[tokio::test]
async fn bad_wire_version_gets_constant_decoy() {
    let env = harness().await;
    let resp = env
        .http
        .post(format!("{}/api/v1/chunks/check", env.base_url))
        .header("X-Obsetync-Method", "POST")
        .body(vec![0u8; 200])
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.bytes().await.unwrap();
    assert_decoy(status, &body);
}
