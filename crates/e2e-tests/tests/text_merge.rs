//! Content-level three-way text merge (1.5.0): when two devices edit
//! DIFFERENT parts of the same text file, the server merges the lines and
//! both edits survive in one file — no conflict, no copy. Overlapping edits
//! keep the pre-1.5.0 behavior (side A wins the tree + a conflict record;
//! covered by conflicts.rs::modify_same_file_different_bytes_flags_conflict).
//!
//! The ≥1MiB size gate is covered by sync-core unit tests
//! (merge::tests::text_merge_binary_ext_and_size_gated) — the e2e harness's
//! push helper only speaks small blobs, so a large-file e2e would test the
//! harness, not the server.

use e2e_tests::*;
use std::collections::HashMap;
use sync_core::hash::hash_to_hex;

async fn pair(env: &E2eEnv, label: &str) -> (WireClient, WireClient, String) {
    let a = env.enroll_device(&format!("{label}-A")).await.unwrap();
    let b = env.enroll_device(&format!("{label}-B")).await.unwrap();
    (
        WireClient::new(env, a),
        WireClient::new(env, b),
        unique_vault_id(label),
    )
}

/// Base pushed by A; then A and B push divergent versions with parent = base.
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
async fn nonoverlapping_same_file_edits_auto_merge() {
    let env = harness().await;
    let (a, b, vault) = pair(&env, "textmerge-clean").await;

    let base = b"# Title\n\nsection one\n\nsection two\n".to_vec();
    // A rewrites section one; B rewrites section two — disjoint line ranges.
    let a_ver = b"# Title\n\nsection one EDITED BY A\n\nsection two\n".to_vec();
    let b_ver = b"# Title\n\nsection one\n\nsection two EDITED BY B\n".to_vec();

    let resp = diverge(
        &a,
        &b,
        &vault,
        vec![("doc.md".into(), base)],
        vec![("doc.md".into(), a_ver)],
        vec![("doc.md".into(), b_ver)],
    )
    .await;

    assert!(resp.merged, "divergent push must merge, got {resp:?}");
    assert!(
        resp.conflicts.is_empty(),
        "non-overlapping same-file edits must NOT conflict: {:?}",
        resp.conflicts
    );
    assert!(
        resp.text_merged >= 1,
        "server must report the text merge, got {resp:?}"
    );

    // Both devices converge onto the SAME merged bytes with both edits.
    let expected = b"# Title\n\nsection one EDITED BY A\n\nsection two EDITED BY B\n";
    for (who, client) in [("A", &a), ("B", &b)] {
        let map: HashMap<_, _> = pull_vault_snapshot(client, &vault)
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(
            map.get("doc.md").map(|v| v.as_slice()),
            Some(&expected[..]),
            "device {who} must see the line-merged document",
        );
    }
}

#[tokio::test]
async fn non_utf8_md_falls_back_to_conflict() {
    let env = harness().await;
    let (a, b, vault) = pair(&env, "textmerge-nonutf8").await;

    // .md extension but invalid UTF-8 — a lossy merge would corrupt bytes,
    // so the server must keep the old behavior: A wins + conflict record.
    let base = vec![0xFF, 0x00, b'b', b'a', b's', b'e', b'\n'];
    let a_ver = vec![0xFF, 0x00, b'A', b'\n'];
    let b_ver = vec![0xFF, 0x00, b'B', b'\n'];

    let resp = diverge(
        &a,
        &b,
        &vault,
        vec![("bin.md".into(), base)],
        vec![("bin.md".into(), a_ver.clone())],
        vec![("bin.md".into(), b_ver)],
    )
    .await;

    assert!(resp.merged);
    assert_eq!(resp.text_merged, 0, "non-UTF-8 must never text-merge");
    assert_eq!(resp.conflicts.len(), 1, "got {:?}", resp.conflicts);

    // Side A's bytes intact in the merged tree — no lossy rewriting.
    let map: HashMap<_, _> = pull_vault_snapshot(&a, &vault)
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(map.get("bin.md").map(|v| v.as_slice()), Some(&a_ver[..]));
}
