//! The conflict-resolution truth table, exercised end-to-end through HTTP.
//!
//! All scenarios share the same skeleton:
//!   1. Device A pushes a baseline `v1`.
//!   2. Device A pushes `v_a` (parent = v1) → server `current` is now `v_a`.
//!   3. Device B pushes `v_b` (parent = v1) → server can't fast-forward,
//!      runs three-way merge with base=v1, side_a=v_a, side_b=v_b.
//!   4. We assert the response (`merged`, `conflicts`, `auto_resolved`) and
//!      the post-merge tree contents.
//!
//! The expected behaviour comes straight from
//! `sync_core::merge::merge_entry_lists`:
//!   - both sides equal base → keep base
//!   - one side unchanged → take changed side (auto)
//!   - both changed differently → conflict (record + keep A in-tree)
//!   - add by one side only → take it
//!   - add same path by both, same hash → no conflict
//!   - add same path by both, different hash → conflict (base=zero)
//!   - delete-vs-modify → modify wins (change beats delete)

use e2e_tests::*;
use sync_core::hash::{hash_bytes, hash_to_hex};

async fn pair(env: &E2eEnv, label: &str) -> (WireClient, WireClient, String) {
    let a = env.enroll_device(&format!("{}-A", label)).await.unwrap();
    let b = env.enroll_device(&format!("{}-B", label)).await.unwrap();
    (
        WireClient::new(env, a),
        WireClient::new(env, b),
        unique_vault_id(label),
    )
}

/// Push a baseline vault as device `a`, then both A and B push divergent
/// updates with parent = the baseline. Returns B's PutRootResponse.
async fn diverge(
    a: &WireClient,
    b: &WireClient,
    vault: &str,
    base: Vec<(String, Vec<u8>)>,
    v_a: Vec<(String, Vec<u8>)>,
    v_b: Vec<(String, Vec<u8>)>,
) -> PutRootResponse {
    let (root_base, _) = push_vault_snapshot(a, vault, &base, ZERO_HASH_HEX)
        .await
        .unwrap();
    let parent = hash_to_hex(&root_base.hash());

    push_vault_snapshot(a, vault, &v_a, &parent).await.unwrap();
    let (_root_b, resp) = push_vault_snapshot(b, vault, &v_b, &parent).await.unwrap();
    resp
}

#[tokio::test]
async fn modify_different_files_auto_resolves_no_conflict() {
    let env = harness().await;
    let (a, b, vault) = pair(&env, "conflict-diff-files").await;

    let resp = diverge(
        &a,
        &b,
        &vault,
        vec![
            ("a.md".into(), b"base-a\n".to_vec()),
            ("b.md".into(), b"base-b\n".to_vec()),
        ],
        vec![
            ("a.md".into(), b"A edited\n".to_vec()),
            ("b.md".into(), b"base-b\n".to_vec()),
        ],
        vec![
            ("a.md".into(), b"base-a\n".to_vec()),
            ("b.md".into(), b"B edited\n".to_vec()),
        ],
    )
    .await;

    assert!(
        resp.merged,
        "stale parent must trigger merge, got {:?}",
        resp
    );
    assert!(resp.conflicts.is_empty(), "{:?}", resp.conflicts);
    assert!(resp.auto_resolved >= 2);

    let pulled = pull_vault_snapshot(&a, &vault).await.unwrap();
    let map: std::collections::HashMap<_, _> = pulled.into_iter().collect();
    assert_eq!(
        map.get("a.md").map(|v| v.as_slice()),
        Some(&b"A edited\n"[..])
    );
    assert_eq!(
        map.get("b.md").map(|v| v.as_slice()),
        Some(&b"B edited\n"[..])
    );
}

#[tokio::test]
async fn modify_same_file_different_bytes_flags_conflict() {
    let env = harness().await;
    let (a, b, vault) = pair(&env, "conflict-same-file").await;

    let resp = diverge(
        &a,
        &b,
        &vault,
        vec![("doc.md".into(), b"v1\n".to_vec())],
        vec![("doc.md".into(), b"A's version\n".to_vec())],
        vec![("doc.md".into(), b"B's version\n".to_vec())],
    )
    .await;

    assert!(resp.merged);
    assert_eq!(resp.conflicts.len(), 1, "got {:?}", resp.conflicts);
    let c = &resp.conflicts[0];
    assert_eq!(c.path, "doc.md");
    assert_eq!(c.base_hash, hash_to_hex(&hash_bytes(b"v1\n")));
    assert_eq!(c.side_a_hash, hash_to_hex(&hash_bytes(b"A's version\n")));
    assert_eq!(c.side_b_hash, hash_to_hex(&hash_bytes(b"B's version\n")));

    // Server keeps A in the merged tree (the "side_a" winner per merge logic);
    // B's version is preserved on the server (its content blob was uploaded)
    // and surfaced via `conflicts` so the client can mint a conflict-copy.
    let pulled = pull_vault_snapshot(&a, &vault).await.unwrap();
    assert_eq!(
        pulled,
        vec![("doc.md".to_string(), b"A's version\n".to_vec())]
    );
    // B's version blob still retrievable by hash:
    let b_bytes = a.get_content(&hash_bytes(b"B's version\n")).await.unwrap();
    assert_eq!(b_bytes, b"B's version\n");
}

#[tokio::test]
async fn add_same_path_same_content_is_not_a_conflict() {
    let env = harness().await;
    let (a, b, vault) = pair(&env, "conflict-add-same").await;

    let identical_bytes = b"both devices typed the same thing\n".to_vec();
    let resp = diverge(
        &a,
        &b,
        &vault,
        vec![("seed.md".into(), b"seed\n".to_vec())],
        vec![
            ("seed.md".into(), b"seed\n".to_vec()),
            ("new.md".into(), identical_bytes.clone()),
        ],
        vec![
            ("seed.md".into(), b"seed\n".to_vec()),
            ("new.md".into(), identical_bytes.clone()),
        ],
    )
    .await;

    assert!(resp.merged);
    assert!(
        resp.conflicts.is_empty(),
        "identical add must not flag a conflict, got {:?}",
        resp.conflicts
    );

    let pulled = pull_vault_snapshot(&a, &vault).await.unwrap();
    let map: std::collections::HashMap<_, _> = pulled.into_iter().collect();
    assert_eq!(map.get("new.md"), Some(&identical_bytes));
}

#[tokio::test]
async fn add_same_path_different_content_is_a_conflict() {
    let env = harness().await;
    let (a, b, vault) = pair(&env, "conflict-add-diff").await;

    let resp = diverge(
        &a,
        &b,
        &vault,
        vec![("seed.md".into(), b"seed\n".to_vec())],
        vec![
            ("seed.md".into(), b"seed\n".to_vec()),
            ("collide.md".into(), b"A's add\n".to_vec()),
        ],
        vec![
            ("seed.md".into(), b"seed\n".to_vec()),
            ("collide.md".into(), b"B's add\n".to_vec()),
        ],
    )
    .await;

    assert!(resp.merged);
    assert_eq!(resp.conflicts.len(), 1);
    let c = &resp.conflicts[0];
    assert_eq!(c.path, "collide.md");
    // No common ancestor for an add-add collision — base hash is zero.
    assert_eq!(
        c.base_hash, ZERO_HASH_HEX,
        "add-add base must be zero hash, got {}",
        c.base_hash
    );
}

#[tokio::test]
async fn modify_beats_delete() {
    let env = harness().await;
    let (a, b, vault) = pair(&env, "conflict-modify-vs-delete").await;

    // Base has both. A modifies x, B deletes x. Per merge_entry_lists:
    //   (Some(_base), Some(a), None) where a != base → take A.
    let resp = diverge(
        &a,
        &b,
        &vault,
        vec![
            ("x.md".into(), b"original\n".to_vec()),
            ("y.md".into(), b"untouched\n".to_vec()),
        ],
        vec![
            ("x.md".into(), b"A's edit survives\n".to_vec()),
            ("y.md".into(), b"untouched\n".to_vec()),
        ],
        vec![
            // x.md missing from B's snapshot = delete
            ("y.md".into(), b"untouched\n".to_vec()),
        ],
    )
    .await;

    assert!(resp.merged);
    assert!(
        resp.conflicts.is_empty(),
        "modify-vs-delete is auto-resolved (modify wins), got conflicts {:?}",
        resp.conflicts
    );

    let pulled = pull_vault_snapshot(&a, &vault).await.unwrap();
    let map: std::collections::HashMap<_, _> = pulled.into_iter().collect();
    assert_eq!(
        map.get("x.md").map(|v| v.as_slice()),
        Some(&b"A's edit survives\n"[..])
    );
}

#[tokio::test]
async fn delete_beats_unchanged() {
    // A makes a *different* edit (so server.current diverges from base).
    // B doesn't see A's edit and deletes x.md against parent=base. The
    // server merges base ⇄ A ⇄ B; for x.md, A is unchanged from base while B
    // deleted it → honor delete. For A's edit on z.md, A wins (auto).
    let env = harness().await;
    let (a, b, vault) = pair(&env, "conflict-delete-clean").await;

    let resp = diverge(
        &a,
        &b,
        &vault,
        // base
        vec![
            ("x.md".into(), b"original\n".to_vec()),
            ("y.md".into(), b"keeper\n".to_vec()),
            ("z.md".into(), b"z-base\n".to_vec()),
        ],
        // v_a: edits z.md only; x.md and y.md unchanged from base
        vec![
            ("x.md".into(), b"original\n".to_vec()),
            ("y.md".into(), b"keeper\n".to_vec()),
            ("z.md".into(), b"z-edited-by-A\n".to_vec()),
        ],
        // v_b: deletes x.md, leaves y.md and z.md (z.md still at base content)
        vec![
            ("y.md".into(), b"keeper\n".to_vec()),
            ("z.md".into(), b"z-base\n".to_vec()),
        ],
    )
    .await;

    assert!(
        resp.merged,
        "real divergence must trigger merge: {:?}",
        resp
    );
    assert!(resp.conflicts.is_empty(), "{:?}", resp.conflicts);

    let pulled = pull_vault_snapshot(&a, &vault).await.unwrap();
    let map: std::collections::HashMap<_, _> = pulled.into_iter().collect();
    assert!(!map.contains_key("x.md"), "x.md should be honoured-deleted");
    assert_eq!(
        map.get("y.md").map(|v| v.as_slice()),
        Some(&b"keeper\n"[..])
    );
    // A's edit to z.md must survive the merge.
    assert_eq!(
        map.get("z.md").map(|v| v.as_slice()),
        Some(&b"z-edited-by-A\n"[..])
    );
}

#[tokio::test]
async fn changes_in_different_directories_no_conflict() {
    // A adds notes/x, B adds photos/y. Different top-level prefixes → the
    // tree-level merge sees them as independent additions.
    let env = harness().await;
    let (a, b, vault) = pair(&env, "conflict-different-dirs").await;

    let resp = diverge(
        &a,
        &b,
        &vault,
        vec![("seed.md".into(), b"seed\n".to_vec())],
        vec![
            ("seed.md".into(), b"seed\n".to_vec()),
            ("notes/x.md".into(), b"x\n".to_vec()),
        ],
        vec![
            ("seed.md".into(), b"seed\n".to_vec()),
            ("photos/y.png".into(), b"\x89PNG\n".to_vec()),
        ],
    )
    .await;

    assert!(resp.merged);
    assert!(resp.conflicts.is_empty());

    let pulled = pull_vault_snapshot(&a, &vault).await.unwrap();
    let paths: Vec<_> = pulled.iter().map(|(p, _)| p.as_str()).collect();
    assert!(paths.contains(&"notes/x.md"));
    assert!(paths.contains(&"photos/y.png"));
    assert!(paths.contains(&"seed.md"));
}

#[tokio::test]
async fn merged_root_is_retrievable_via_get_root() {
    // After the merge, GET /api/v1/root/{vault} must return the *merged* root
    // and its hash must match the one in the put response.
    let env = harness().await;
    let (a, b, vault) = pair(&env, "conflict-get-merged").await;

    let resp = diverge(
        &a,
        &b,
        &vault,
        vec![("a.md".into(), b"v1\n".to_vec())],
        vec![("a.md".into(), b"A v2\n".to_vec())],
        vec![
            ("a.md".into(), b"v1\n".to_vec()),
            ("b.md".into(), b"new\n".to_vec()),
        ],
    )
    .await;
    assert!(resp.merged);

    let merged = a.get_root(&vault).await.unwrap();
    assert_eq!(hash_to_hex(&merged.hash()), resp.root_hash);
}
