//! Two devices sharing a vault. Cooperative flows only — conflicts live in
//! their own file. Asserts the loop:
//!     A push → B pull → B edit → B push (fast-forward) → A pull picks up B.

use e2e_tests::*;
use sync_core::hash::hash_to_hex;

/// Spin up two devices and a vault. Convenience wrapper used by every test
/// in this file.
async fn pair(env: &E2eEnv, label: &str) -> (WireClient, WireClient, String) {
    let a = env.enroll_device(&format!("{}-A", label)).await.unwrap();
    let b = env.enroll_device(&format!("{}-B", label)).await.unwrap();
    (
        WireClient::new(env, a),
        WireClient::new(env, b),
        unique_vault_id(label),
    )
}

#[tokio::test]
async fn b_pulls_state_a_pushed() {
    let env = harness().await;
    let (a, b, vault) = pair(&env, "pair-pull").await;

    let files = vec![
        ("notes/welcome.md".to_string(), b"hello from A\n".to_vec()),
        ("img/header.png".to_string(), b"\x89PNG fake\n".to_vec()),
    ];
    push_vault_snapshot(&a, &vault, &files, ZERO_HASH_HEX)
        .await
        .unwrap();

    let pulled = pull_vault_snapshot(&b, &vault).await.unwrap();
    let mut expected = files.clone();
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(pulled, expected);
}

#[tokio::test]
async fn b_fast_forward_push_after_pulling_a() {
    let env = harness().await;
    let (a, b, vault) = pair(&env, "pair-ff").await;

    // A's initial push.
    let v1 = vec![("a.md".to_string(), b"line one\n".to_vec())];
    let (root1, _) = push_vault_snapshot(&a, &vault, &v1, ZERO_HASH_HEX)
        .await
        .unwrap();

    // B pulls, then makes an edit and pushes with parent = root1.
    // From the server's POV B's parent matches current → fast-forward.
    let _pulled = pull_vault_snapshot(&b, &vault).await.unwrap();
    let v2 = vec![("a.md".to_string(), b"line one\nline two\n".to_vec())];
    let (_root2, resp) = push_vault_snapshot(&b, &vault, &v2, &hash_to_hex(&root1.hash()))
        .await
        .unwrap();
    assert!(
        resp.accepted && !resp.merged,
        "B's parent matches current → ff, got {:?}",
        resp
    );

    // A pulls, sees B's edit.
    let pulled = pull_vault_snapshot(&a, &vault).await.unwrap();
    assert_eq!(
        pulled,
        vec![("a.md".to_string(), b"line one\nline two\n".to_vec())]
    );
}

#[tokio::test]
async fn b_stale_parent_triggers_server_merge() {
    // Classic divergence: A pushes v1, then *also* pushes v2 (so server
    // current=v2). B never saw v2, edits a different file, pushes parent=v1.
    // Server can't fast-forward → server-side merge runs and resolves
    // automatically because A and B touched different files.
    let env = harness().await;
    let (a, b, vault) = pair(&env, "pair-merge").await;

    let v1 = vec![("a.md".to_string(), b"a v1\n".to_vec())];
    let (root1, _) = push_vault_snapshot(&a, &vault, &v1, ZERO_HASH_HEX)
        .await
        .unwrap();

    // A keeps editing a.md while B is offline.
    let v2 = vec![("a.md".to_string(), b"a v2 by A\n".to_vec())];
    push_vault_snapshot(&a, &vault, &v2, &hash_to_hex(&root1.hash()))
        .await
        .unwrap();

    // B comes back. B's known parent is still root1, but B edits b.md,
    // not a.md — so the merge is conflict-free.
    let v_b = vec![
        ("a.md".to_string(), b"a v1\n".to_vec()), // unchanged from B's POV
        ("b.md".to_string(), b"b created by B\n".to_vec()),
    ];
    let (_root_b, resp) = push_vault_snapshot(&b, &vault, &v_b, &hash_to_hex(&root1.hash()))
        .await
        .unwrap();
    assert!(
        resp.merged,
        "stale parent must trigger server-side merge, got {:?}",
        resp
    );
    assert!(
        resp.conflicts.is_empty(),
        "different files → no conflicts; got {:?}",
        resp.conflicts
    );

    // After merge, both files exist and carry the right content.
    let pulled = pull_vault_snapshot(&a, &vault).await.unwrap();
    let by_path: std::collections::HashMap<_, _> = pulled.into_iter().collect();
    assert_eq!(
        by_path.get("a.md").map(|v| v.as_slice()),
        Some(&b"a v2 by A\n"[..])
    );
    assert_eq!(
        by_path.get("b.md").map(|v| v.as_slice()),
        Some(&b"b created by B\n"[..])
    );
}

#[tokio::test]
async fn vaults_are_isolated_between_ids() {
    // Two vaults on the same server with the same device must not bleed state.
    let env = harness().await;
    let creds = env.enroll_device("multi-vault").await.unwrap();
    let client = WireClient::new(&env, creds);

    let vault_x = unique_vault_id("iso-x");
    let vault_y = unique_vault_id("iso-y");

    push_vault_snapshot(
        &client,
        &vault_x,
        &[("x.md".to_string(), b"in x\n".to_vec())],
        ZERO_HASH_HEX,
    )
    .await
    .unwrap();
    push_vault_snapshot(
        &client,
        &vault_y,
        &[("y.md".to_string(), b"in y\n".to_vec())],
        ZERO_HASH_HEX,
    )
    .await
    .unwrap();

    let x_files = pull_vault_snapshot(&client, &vault_x).await.unwrap();
    let y_files = pull_vault_snapshot(&client, &vault_y).await.unwrap();
    assert_eq!(x_files, vec![("x.md".to_string(), b"in x\n".to_vec())]);
    assert_eq!(y_files, vec![("y.md".to_string(), b"in y\n".to_vec())]);
}
