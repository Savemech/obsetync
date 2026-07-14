//! Root history, API-driven rollback, and admin tar export (1.6.0).
//!
//! - GET /api/v1/history/{vault} — recent roots newest-first with the
//!   RootNode metadata (created_ms, device, total_files, current flag).
//!   Powers the plugin's rollback UI.
//! - POST /api/v1/rollback/{vault} — move `current` back to any root in
//!   history over the sealed sync channel (the admin UI equivalent, but
//!   authenticated). Devices converge on their next pull.
//! - GET /admin/vaults/{vault}/export/{root} — the vault materialized as a
//!   tar archive at any historical root.

use e2e_tests::*;
use std::collections::HashMap;
use std::io::Read;
use sync_core::hash::hash_to_hex;

#[derive(serde::Deserialize)]
struct HistoryEntry {
    root: String,
    created_ms: u64,
    total_files: u64,
    current: bool,
}

#[derive(serde::Deserialize)]
struct HistoryResponse {
    roots: Vec<HistoryEntry>,
}

async fn fetch_history(client: &WireClient, vault: &str) -> HistoryResponse {
    let r = client
        .raw("GET", &format!("/api/v1/history/{vault}"), &[])
        .await
        .unwrap();
    assert!(r.status.is_success(), "history: {}", r.status);
    serde_json::from_slice(&r.body).unwrap()
}

#[tokio::test]
async fn history_lists_roots_newest_first_and_rollback_converges() {
    let env = harness().await;
    let vault = unique_vault_id("history");
    let a = WireClient::new(&env, env.enroll_device("history-A").await.unwrap());

    // Chain of three states.
    let v1 = vec![("doc.md".to_string(), b"version one\n".to_vec())];
    let v2 = vec![("doc.md".to_string(), b"version two\n".to_vec())];
    let v3 = vec![("doc.md".to_string(), b"version three\n".to_vec())];

    let (r1, _) = push_vault_snapshot(&a, &vault, &v1, ZERO_HASH_HEX)
        .await
        .unwrap();
    let p1 = hash_to_hex(&r1.hash());
    let (r2, _) = push_vault_snapshot(&a, &vault, &v2, &p1).await.unwrap();
    let p2 = hash_to_hex(&r2.hash());
    let (r3, _) = push_vault_snapshot(&a, &vault, &v3, &p2).await.unwrap();
    let p3 = hash_to_hex(&r3.hash());

    // History: all three present, newest first, current flagged once.
    let hist = fetch_history(&a, &vault).await;
    assert!(
        hist.roots.len() >= 3,
        "want ≥3 roots, got {}",
        hist.roots.len()
    );
    let hashes: Vec<&str> = hist.roots.iter().map(|e| e.root.as_str()).collect();
    assert!(
        hashes.contains(&p1.as_str())
            && hashes.contains(&p2.as_str())
            && hashes.contains(&p3.as_str())
    );
    for w in hist.roots.windows(2) {
        assert!(
            w[0].created_ms >= w[1].created_ms,
            "history must be newest-first"
        );
    }
    let currents: Vec<&HistoryEntry> = hist.roots.iter().filter(|e| e.current).collect();
    assert_eq!(currents.len(), 1);
    assert_eq!(currents[0].root, p3);
    assert_eq!(currents[0].total_files, 1);

    // Rollback to v1 over the sealed API.
    let r = a
        .raw("POST", &format!("/api/v1/rollback/{vault}"), p1.as_bytes())
        .await
        .unwrap();
    assert!(r.status.is_success(), "rollback: {}", r.status);

    // A fresh observer converges onto version one.
    let observer = WireClient::new(&env, env.enroll_device("history-obs").await.unwrap());
    let seen: HashMap<String, Vec<u8>> = pull_vault_snapshot(&observer, &vault)
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(
        seen.get("doc.md").map(|v| v.as_slice()),
        Some(&b"version one\n"[..]),
        "post-rollback pull must serve the rolled-back content",
    );

    // History now flags v1 as current.
    let hist = fetch_history(&a, &vault).await;
    let current = hist.roots.iter().find(|e| e.current).unwrap();
    assert_eq!(current.root, p1);

    // Rollback to garbage → 404, current unchanged.
    let bogus = "ff".repeat(32);
    let r = a
        .raw(
            "POST",
            &format!("/api/v1/rollback/{vault}"),
            bogus.as_bytes(),
        )
        .await
        .unwrap();
    assert_eq!(r.status.as_u16(), 404, "unknown root must 404");
}

#[tokio::test]
async fn admin_export_serves_tar_of_any_root() {
    let env = harness().await;
    let vault = unique_vault_id("export");
    let a = WireClient::new(&env, env.enroll_device("export-A").await.unwrap());

    let v1 = vec![
        ("notes/a.md".to_string(), b"alpha v1\n".to_vec()),
        ("notes/b.md".to_string(), b"beta\n".to_vec()),
    ];
    let v2 = vec![
        ("notes/a.md".to_string(), b"alpha v2\n".to_vec()),
        ("notes/b.md".to_string(), b"beta\n".to_vec()),
    ];
    let (r1, _) = push_vault_snapshot(&a, &vault, &v1, ZERO_HASH_HEX)
        .await
        .unwrap();
    let p1 = hash_to_hex(&r1.hash());
    push_vault_snapshot(&a, &vault, &v2, &p1).await.unwrap();

    // Export the HISTORICAL root (v1), not current — the whole point of
    // keeping every root is being able to materialize any of them.
    let url = format!("{}/admin/vaults/{}/export/{}", env.admin_url, vault, p1);
    let resp = env.http.get(&url).send().await.unwrap();
    assert!(resp.status().is_success(), "export: {}", resp.status());
    let disposition = resp
        .headers()
        .get("content-disposition")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(disposition.contains(".tar"), "got {disposition:?}");
    let bytes = resp.bytes().await.unwrap();

    // Parse the tar and verify exact v1 contents.
    let mut archive = tar::Archive::new(bytes.as_ref());
    let mut files: HashMap<String, Vec<u8>> = HashMap::new();
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        let path = entry.path().unwrap().to_string_lossy().to_string();
        let mut data = Vec::new();
        entry.read_to_end(&mut data).unwrap();
        files.insert(path, data);
    }
    assert_eq!(files.len(), 2, "tar must hold exactly the v1 file set");
    assert_eq!(
        files.get("notes/a.md").map(|v| v.as_slice()),
        Some(&b"alpha v1\n"[..]),
    );
    assert_eq!(
        files.get("notes/b.md").map(|v| v.as_slice()),
        Some(&b"beta\n"[..]),
    );

    // Unknown root → error page, not a broken tar.
    let url = format!(
        "{}/admin/vaults/{}/export/{}",
        env.admin_url,
        vault,
        "ee".repeat(32)
    );
    let resp = env.http.get(&url).send().await.unwrap();
    assert!(!resp.status().is_success());
}
