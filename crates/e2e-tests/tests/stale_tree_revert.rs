//! Regression: stale-tree revert / fleet-wide data loss via a trusted client
//! parent on `PUT /api/v1/root/{vault}`.
//!
//! ## The bug (protocol level)
//!
//! The server's `put_root` (crates/sync-server/src/api.rs) decides how to
//! integrate an incoming root purely from the *client-claimed* parent hash
//! carried in the 64-byte body prefix — it never checks that the incoming
//! Merkle tree actually descends from that parent:
//!
//!   * `current == claimed_parent`  → "fast-forward accepted": the server
//!     `set_current_root(incoming)` — wholesale replacing the good tree.
//!   * `current != claimed_parent`  → three-way merge that uses the
//!     *claimed* parent as the merge BASE ("use the parent hash as the base").
//!
//! A device whose in-memory tree is days stale but which keeps polling the
//! server every ~30s can therefore fabricate a fresh-looking parent and either
//! clobber or "merge-revert" the vault. Every file created on other devices
//! since the stale tree's epoch is silently deleted/reverted for the whole
//! fleet.
//!
//! Production incident-log evidence:
//!   put_root: fast-forward accepted vault=example-vault root=226b4fb0 parent=27cd3dfd
//!   put_root: merged divergent roots parent=c9c0647d current=89da5b31 auto_resolved=441 conflicts=3
//!
//! ## What these tests encode
//!
//! Both tests drive the exact production shape through the real HTTP + AEAD
//! stack and assert the CORRECT post-fix invariant: a stale device pushing an
//! old tree must NOT be able to make the fleet lose files A legitimately
//! created. The push may be rejected, or merged in a way that preserves the
//! newer files — either is acceptable — but the authoritative tree any other
//! device pulls afterwards must still contain f1..f5.
//!
//! On today's (buggy) server BOTH tests FAIL: the fleet observer sees only a
//! subset of the files. Each failing `assert_eq!` prints the observed-vs-
//! expected file set. When the parent-trust bug is fixed these turn green.

use e2e_tests::*;
use std::collections::BTreeMap;
use sync_core::chunk::RootNode;
use sync_core::hash::hash_to_hex;

/// The five files device A creates over time. `f1` exists at epoch E0 (the last
/// time device B synced); `f2..f5` are created afterwards on A and pushed as a
/// chain of fast-forward roots, so the server's `current` advances R1 → … → R5.
fn a_files() -> Vec<(String, Vec<u8>)> {
    vec![
        (
            "f1.md".to_string(),
            b"f1 - created at epoch E0 (B has this one)\n".to_vec(),
        ),
        (
            "f2.md".to_string(),
            b"f2 - created by A after E0\n".to_vec(),
        ),
        (
            "f3.md".to_string(),
            b"f3 - created by A after E0\n".to_vec(),
        ),
        (
            "f4.md".to_string(),
            b"f4 - created by A after E0\n".to_vec(),
        ),
        (
            "f5.md".to_string(),
            b"f5 - created by A after E0\n".to_vec(),
        ),
    ]
}

/// Shared setup for both variants:
///   1. A and B enrol on a fresh vault.
///   2. A pushes f1; B syncs at epoch E0 — its in-memory tree freezes at {f1}.
///   3. A pushes f2..f5 as a fast-forward chain → server current advances to R5.
///
/// Returns the two device clients, the vault id, the per-step roots
/// `[R1, R2, R3, R4, R5]`, and B's stale `{f1}` snapshot (what its frozen tree
/// still holds).
async fn diverged_fleet(
    env: &E2eEnv,
    label: &str,
) -> (
    WireClient,
    WireClient,
    String,
    Vec<RootNode>,
    Vec<(String, Vec<u8>)>,
) {
    let vault = unique_vault_id(label);
    let a = WireClient::new(
        env,
        env.enroll_device(&format!("{}-A", label)).await.unwrap(),
    );
    let b = WireClient::new(
        env,
        env.enroll_device(&format!("{}-B", label)).await.unwrap(),
    );

    let files = a_files();

    // --- epoch E0: A creates f1; B syncs and freezes its tree at {f1}. ---
    let (root1, resp1) = push_vault_snapshot(&a, &vault, &files[..1], ZERO_HASH_HEX)
        .await
        .unwrap();
    assert!(
        resp1.accepted,
        "first push must be accepted, got {:?}",
        resp1
    );

    let b_stale = pull_vault_snapshot(&b, &vault).await.unwrap();
    assert_eq!(
        b_stale,
        files[..1].to_vec(),
        "B at epoch E0 must see exactly {{f1}}, got {:?}",
        b_stale
    );

    // --- A keeps working while B is idle: pushes f2..f5 as a fast-forward chain.
    //     Every step's parent == server current, so the server advances cleanly. ---
    let mut roots = vec![root1];
    for i in 1..files.len() {
        let parent = hash_to_hex(&roots[i - 1].hash());
        let (root, resp) = push_vault_snapshot(&a, &vault, &files[..=i], &parent)
            .await
            .unwrap();
        assert!(
            resp.accepted && !resp.merged,
            "A's chain step {} must fast-forward, got {:?}",
            i + 1,
            resp
        );
        roots.push(root);
    }

    (a, b, vault, roots, b_stale)
}

/// A fresh fleet member enrols and does a full pull — this models any OTHER
/// device that polls after B's push has replicated fleet-wide. Returns the
/// authoritative vault contents keyed by path.
async fn fleet_view(env: &E2eEnv, vault: &str, label: &str) -> BTreeMap<String, Vec<u8>> {
    let observer = WireClient::new(env, env.enroll_device(label).await.unwrap());
    pull_vault_snapshot(&observer, vault)
        .await
        .unwrap()
        .into_iter()
        .collect()
}

/// Assert the fleet still holds every one of A's files with A's exact bytes.
/// Fails loudly (observed-vs-expected file set) the moment any file was
/// silently reverted or deleted.
fn assert_no_data_loss(seen: &BTreeMap<String, Vec<u8>>, files: &[(String, Vec<u8>)], how: &str) {
    for (path, content) in files {
        assert_eq!(
            seen.get(path).map(|v| v.as_slice()),
            Some(content.as_slice()),
            "DATA LOSS ({how}): '{path}' was silently reverted/deleted. The fleet \
             now sees {seen:?} but every one of {expected:?} must survive. A stale \
             device must never be able to make the fleet lose files created on \
             other devices.",
            how = how,
            path = path,
            seen = seen.keys().collect::<Vec<_>>(),
            expected = files.iter().map(|(p, _)| p).collect::<Vec<_>>(),
        );
    }
}

/// T1 — stale-tree fast-forward clobber.
///
/// B polls the *current* root (its 30s heartbeat) and pushes its stale {f1}
/// tree claiming `parent == current`. The server takes the "fast-forward
/// accepted" branch and replaces the good {f1..f5} tree with {f1}.
///
/// OBSERVED (buggy) result: `put_root` returns `{accepted:true, merged:false}`
/// and a subsequent fleet pull sees only f1 → f2..f5 gone.
/// CORRECT (asserted) result: f1..f5 all still present fleet-wide.
#[tokio::test]
async fn stale_parent_fast_forward_clobbers() {
    let env = harness().await;
    let (_a, b, vault, roots, b_stale) = diverged_fleet(&env, "stale-ff").await;
    let files = a_files();
    let current_root = roots.last().unwrap(); // R5 = {f1..f5}

    // B's 30s heartbeat: POLL the server, learn the fresh current root — while
    // B's own Merkle tree is still the stale {f1}.
    let polled = b.get_root(&vault).await.unwrap();
    let polled_hex = hash_to_hex(&polled.hash());
    assert_eq!(
        polled_hex,
        hash_to_hex(&current_root.hash()),
        "B must have polled the true current root R5"
    );

    // B pushes its STALE {f1} tree claiming parent = the freshly-polled current.
    let push_res = push_vault_snapshot(&b, &vault, &b_stale, &polled_hex).await;
    match &push_res {
        Ok((_r, resp)) => {
            // OBSERVED TODAY (bug): current == claimed_parent → the server takes
            // the `put_root: fast-forward accepted` branch and set_current_root()
            // to B's stale {f1}, discarding f2..f5. resp == {accepted:true,
            // merged:false}. Mirrors the incident line:
            //   "put_root: fast-forward accepted ... root=226b4fb0 parent=27cd3dfd"
            eprintln!(
                "[T1] put_root accepted a stale-tree fast-forward (buggy): {:?}",
                resp
            );
        }
        Err(e) => {
            // Post-fix ACCEPTABLE: server refuses a fast-forward whose incoming
            // tree does not descend from the claimed parent.
            eprintln!(
                "[T1] put_root rejected the stale fast-forward (post-fix ok): {}",
                e
            );
        }
    }

    // CORRECT BEHAVIOUR — regardless of accept/reject, no file A created may
    // vanish. On the buggy server this fails: the observer sees only {f1}.
    let seen = fleet_view(&env, &vault, "stale-ff-observer").await;
    assert_no_data_loss(&seen, &files, "stale-parent fast-forward clobber");
}

/// T2 — poisoned merge base.
///
/// Same stale device, but it claims `parent == R4` (one step behind current
/// R5), forcing the three-way merge. The server uses the *claimed* parent as
/// the merge base, so f2..f4 — present in base and current, merely absent from
/// B's stale tree — are treated as B-deletions and reverted (auto_resolved).
///
/// OBSERVED (buggy) result: `put_root` returns `{merged:true, auto_resolved>0}`
/// and a fleet pull sees only {f1, f5} → f2..f4 reverted.
/// CORRECT (asserted) result: f1..f5 all still present fleet-wide.
#[tokio::test]
async fn poisoned_merge_base_reverts() {
    let env = harness().await;
    let (_a, b, vault, roots, b_stale) = diverged_fleet(&env, "poison-merge").await;
    let files = a_files();

    let current_root = roots.last().unwrap(); // R5 = {f1..f5}
    let one_behind = &roots[roots.len() - 2]; // R4 = {f1..f4}

    let parent_hex = hash_to_hex(&one_behind.hash());
    assert_ne!(
        parent_hex,
        hash_to_hex(&current_root.hash()),
        "T2 must claim a parent != current to force the merge path"
    );

    // B pushes its STALE {f1} tree claiming parent = R4 (one behind current).
    //   base   = R4 {f1,f2,f3,f4}   (poisoned: not B's real ancestor)
    //   side_a = R5 {f1..f5}        (server current)
    //   side_b = {f1}               (B's stale incoming)
    // → f2/f3/f4 look like B-deletes against an unchanged current → reverted;
    //   only f5 (new in current, absent from base) survives.
    let push_res = push_vault_snapshot(&b, &vault, &b_stale, &parent_hex).await;
    match &push_res {
        Ok((_r, resp)) => {
            // OBSERVED TODAY (bug): merged=true, auto_resolved>0 — a silent mass
            // revert. Mirrors the incident line:
            //   "put_root: merged divergent roots parent=c9c0647d current=89da5b31 auto_resolved=441 conflicts=3"
            eprintln!(
                "[T2] put_root merged against a poisoned base (buggy): {:?}",
                resp
            );
        }
        Err(e) => {
            eprintln!(
                "[T2] put_root rejected the poisoned merge (post-fix ok): {}",
                e
            );
        }
    }

    // CORRECT BEHAVIOUR — A's newer files must survive the merge with A's exact
    // content. On the buggy server this fails: the observer sees only {f1, f5}.
    let seen = fleet_view(&env, &vault, "poison-merge-observer").await;
    assert_no_data_loss(&seen, &files, "poisoned merge base");
}
