//! Smoke tests — confirm the stack is reachable and basic admin endpoints
//! return well-shaped responses.

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
    let body: serde_json::Value = serde_json::from_slice(&resp.bytes().await.unwrap()).unwrap();
    assert_eq!(body["ok"].as_bool(), Some(true));
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
async fn enrollment_bundle_grants_sync_access() {
    let env = harness().await;
    let creds = env.enroll_device("smoke-pubkey").await.unwrap();
    assert_eq!(creds.bearer.len(), 64);
    assert!(creds.bearer.chars().all(|c| c.is_ascii_hexdigit()));
    let client = WireClient::new(&env, creds);
    assert!(client.chunks_check(&[]).await.unwrap().is_empty());
}
