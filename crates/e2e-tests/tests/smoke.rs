//! Smoke tests — confirm the stack is reachable and basic admin endpoints
//! return well-shaped responses.

use base64::prelude::*;
use e2e_tests::*;

#[tokio::test]
async fn health_endpoint_is_public_plaintext() {
    let env = harness().await;
    let resp = env
        .http
        .get(format!("{}/health", env.base_url))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "health must return 2xx");
    let text = resp.text().await.unwrap();
    assert!(
        text.contains("\"ok\":true"),
        "expected JSON {{\"ok\":true}}, got {}",
        text
    );
}

#[tokio::test]
async fn admin_root_redirects_to_dashboard() {
    let env = harness().await;
    // Don't follow redirects so we can assert the redirect itself.
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let resp = client
        .get(format!("{}/", env.admin_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 308);
    let loc = resp
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|h| h.to_str().ok());
    assert_eq!(loc, Some("/admin"));
}

#[tokio::test]
async fn admin_dashboard_renders() {
    let env = harness().await;
    let resp = env
        .http
        .get(format!("{}/admin", env.admin_url))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let html = resp.text().await.unwrap();
    assert!(html.contains("ObsetyNC Server"));
}

#[tokio::test]
async fn enrollment_bundle_includes_valid_box_pubkey() {
    let env = harness().await;
    let creds = env.enroll_device("smoke-pubkey").await.unwrap();
    // server_box_pub round-trips through base64; PublicKey enforces 32 bytes.
    let bytes = BASE64_STANDARD.encode(creds.server_box_pub.as_bytes());
    assert_eq!(BASE64_STANDARD.decode(bytes).unwrap().len(), 32);
    assert_eq!(creds.bearer.len(), 64);
    assert!(creds.bearer.chars().all(|c| c.is_ascii_hexdigit()));
}
