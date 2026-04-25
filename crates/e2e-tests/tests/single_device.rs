//! One device pushing and pulling its own state. No conflicts.
//! Establishes the baseline sync invariants the conflict tests build on.

use e2e_tests::*;
use sync_core::hash::{hash_bytes, hash_to_hex};

#[tokio::test]
async fn first_push_creates_vault_and_round_trips() {
    let env = harness().await;
    let creds = env.enroll_device("solo-1").await.unwrap();
    let client = WireClient::new(&env, creds);
    let vault = unique_vault_id("solo-rt");

    let files = vec![
        ("notes/jan.md".to_string(), b"# January\n".to_vec()),
        ("notes/feb.md".to_string(), b"# February\n".to_vec()),
        ("readme.md".to_string(), b"top-level file\n".to_vec()),
    ];

    let (root, resp) = push_vault_snapshot(&client, &vault, &files, ZERO_HASH_HEX)
        .await
        .unwrap();
    assert!(resp.accepted, "first push must be accepted, got {:?}", resp);
    assert_eq!(resp.root_hash, hash_to_hex(&root.hash()));

    // Pull it back: every (path, content) must round-trip exactly.
    let mut pulled = pull_vault_snapshot(&client, &vault).await.unwrap();
    pulled.sort_by(|a, b| a.0.cmp(&b.0));
    let mut expected = files.clone();
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(pulled, expected);
}

#[tokio::test]
async fn fast_forward_push_with_correct_parent_is_accepted() {
    let env = harness().await;
    let creds = env.enroll_device("solo-ff").await.unwrap();
    let client = WireClient::new(&env, creds);
    let vault = unique_vault_id("solo-ff");

    let v1 = vec![("a.md".to_string(), b"v1\n".to_vec())];
    let (root1, _) = push_vault_snapshot(&client, &vault, &v1, ZERO_HASH_HEX)
        .await
        .unwrap();

    // Edit, then push with parent = root1's hash → fast-forward path.
    let v2 = vec![("a.md".to_string(), b"v2 with edit\n".to_vec())];
    let (_root2, resp) = push_vault_snapshot(&client, &vault, &v2, &hash_to_hex(&root1.hash()))
        .await
        .unwrap();
    assert!(
        resp.accepted && !resp.merged,
        "ff push should be accepted=true, merged=false; got {:?}",
        resp
    );
    assert!(resp.conflicts.is_empty());

    // Pull confirms server now serves v2.
    let pulled = pull_vault_snapshot(&client, &vault).await.unwrap();
    assert_eq!(pulled, vec![("a.md".to_string(), b"v2 with edit\n".to_vec())]);
}

#[tokio::test]
async fn diff_against_zero_root_returns_full_inventory() {
    // Fresh-client semantics: device_root=0…0 is the bootstrap signal.
    // Server should respond with every current file as Added.
    let env = harness().await;
    let creds = env.enroll_device("solo-diff-zero").await.unwrap();
    let client = WireClient::new(&env, creds);
    let vault = unique_vault_id("solo-diff-zero");

    let files = vec![
        ("x.md".to_string(), b"x\n".to_vec()),
        ("y.md".to_string(), b"y\n".to_vec()),
    ];
    push_vault_snapshot(&client, &vault, &files, ZERO_HASH_HEX)
        .await
        .unwrap();

    let diff = client.post_diff(&vault, ZERO_HASH_HEX).await.unwrap();
    match diff {
        DiffResponse::Deltas(deltas) => {
            assert_eq!(deltas.len(), 2);
            for d in &deltas {
                match d {
                    WireDelta::Added { .. } => {}
                    other => panic!("expected Added, got {:?}", other),
                }
            }
        }
        DiffResponse::InSync => panic!("zero-root diff must NOT report in-sync"),
    }
}

#[tokio::test]
async fn diff_at_current_root_returns_in_sync() {
    let env = harness().await;
    let creds = env.enroll_device("solo-diff-cur").await.unwrap();
    let client = WireClient::new(&env, creds);
    let vault = unique_vault_id("solo-diff-cur");

    let files = vec![("a.md".to_string(), b"a\n".to_vec())];
    let (root, _) = push_vault_snapshot(&client, &vault, &files, ZERO_HASH_HEX)
        .await
        .unwrap();

    let diff = client
        .post_diff(&vault, &hash_to_hex(&root.hash()))
        .await
        .unwrap();
    match diff {
        DiffResponse::InSync => {}
        DiffResponse::Deltas(d) => panic!("expected InSync, got {:?}", d),
    }
}

#[tokio::test]
async fn put_chunk_rejects_hash_mismatch() {
    let env = harness().await;
    let creds = env.enroll_device("solo-hash").await.unwrap();
    let client = WireClient::new(&env, creds);

    // Claim hash of "foo" but upload bytes of "bar".
    let claimed = hash_bytes(b"foo");
    let r = client
        .raw("PUT", &format!("/api/v1/chunk/{}", hash_to_hex(&claimed)), b"bar")
        .await
        .unwrap();
    assert!(
        r.status.is_client_error(),
        "expected 4xx hash-mismatch, got {} body={}",
        r.status,
        String::from_utf8_lossy(&r.body)
    );
}

#[tokio::test]
async fn chunks_check_filters_to_only_missing() {
    let env = harness().await;
    let creds = env.enroll_device("solo-check").await.unwrap();
    let client = WireClient::new(&env, creds);

    // Upload one chunk; ask the server about it + a non-existent one.
    let bytes = b"chunk-bytes-for-check".to_vec();
    let h_uploaded = hash_bytes(&bytes);
    client.put_chunk(&h_uploaded, &bytes).await.unwrap();

    let h_missing = hash_bytes(b"never-uploaded");
    let needed = client
        .chunks_check(&[h_uploaded, h_missing])
        .await
        .unwrap();
    assert_eq!(needed.len(), 1);
    assert_eq!(needed[0], hash_to_hex(&h_missing));
}
