//! Enrollment lifecycle: create code → claim → use bearer → revoke.

use e2e_tests::*;
use reqwest::StatusCode;

#[tokio::test]
async fn enroll_two_devices_yields_distinct_credentials() {
    let env = harness().await;
    let a = env.enroll_device("phone-A").await.unwrap();
    let b = env.enroll_device("desktop-B").await.unwrap();
    assert_ne!(a.code, b.code);
    assert_ne!(a.device_id, b.device_id);
    assert_ne!(a.bearer, b.bearer);
    // But they share the same server pubkey — there's only one server identity.
    assert_eq!(a.server_box_pub.as_bytes(), b.server_box_pub.as_bytes());
}

#[tokio::test]
async fn enrolled_device_can_authenticate_against_sync_api() {
    let env = harness().await;
    let creds = env.enroll_device("auth-test").await.unwrap();
    let client = WireClient::new(&env, creds);

    // Hit a protected endpoint that requires no prior state — chunks/check
    // returns a JSON list, so authentication success is enough.
    let needed = client.chunks_check(&[]).await.unwrap();
    assert!(needed.is_empty(), "empty input → empty needed list");
}

#[tokio::test]
async fn unknown_enrollment_code_is_rejected() {
    let env = harness().await;
    let resp = env
        .http
        .get(format!("{}/admin/enrollment/BOGUS-0000", env.admin_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = resp.text().await.unwrap();
    // Server returns JSON {"error": "..."}.
    assert!(body.contains("not found") || body.contains("error"));
}

#[tokio::test]
async fn claimed_code_cannot_be_reclaimed() {
    let env = harness().await;
    let creds = env.enroll_device("one-shot").await.unwrap();

    let resp = env
        .http
        .get(format!(
            "{}/admin/enrollment/{}",
            env.admin_url, creds.code
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "second claim must fail — enrollment is single-use"
    );
}

#[tokio::test]
async fn revoked_device_is_forbidden() {
    let env = harness().await;
    let creds = env.enroll_device("revocation-target").await.unwrap();
    let client = WireClient::new(&env, creds.clone());

    // Sanity: works before revoke.
    client.chunks_check(&[]).await.unwrap();

    env.revoke_device(&creds.device_id).await.unwrap();

    // After revoke: middleware must refuse with 403 plaintext.
    let r = client
        .raw("POST", "/api/v1/chunks/check", b"[]")
        .await
        .unwrap();
    assert_eq!(
        r.status,
        StatusCode::FORBIDDEN,
        "revoked device must get 403, body={}",
        String::from_utf8_lossy(&r.body)
    );
}
