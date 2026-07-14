//! Regression: concurrent pushes to one vault must never lose an update.
//!
//! `put_root` is a read-modify-write over `vaults/<id>/current`:
//! read current → decide (fast-forward / merge) → set current. Before the
//! per-vault lock, two devices pushing at the same moment could both read
//! the same `current`, both pass the `parent == current` fast-forward check
//! (or one merge against a by-then-stale current), and the second
//! `set_current_root` overwrote the first — the losing push stayed in
//! `roots/` history but silently vanished from the vault's advertised state.
//! The 30-second poll jitter made this rare in practice; anything that
//! synchronizes device activity (a notify channel, a fleet waking up
//! together) turns it into routine silent data loss.
//!
//! The test drives the exact window: two devices push simultaneously with
//! the SAME parent, each editing a DIFFERENT file (so merge semantics are
//! trivially auto-resolvable and any lost edit is unambiguously the
//! serialization bug, not conflict policy). Whatever the interleaving —
//! FF+merge or merge+FF — BOTH edits must be present in the final vault.
//! Several rounds make the pre-lock race reliably reproducible while the
//! post-lock behavior stays green.

use e2e_tests::*;
use std::collections::BTreeMap;
use sync_core::hash::hash_to_hex;

const ROUNDS: usize = 8;

#[tokio::test]
async fn concurrent_pushes_with_same_parent_lose_nothing() {
    let env = harness().await;

    for round in 0..ROUNDS {
        let vault = unique_vault_id(&format!("race-ff-{round}"));
        let a = WireClient::new(
            &env,
            env.enroll_device(&format!("race-{round}-A")).await.unwrap(),
        );
        let b = WireClient::new(
            &env,
            env.enroll_device(&format!("race-{round}-B")).await.unwrap(),
        );

        // Shared base: both files exist so each side's push is a pure edit.
        let base = vec![
            ("a.md".to_string(), b"a base\n".to_vec()),
            ("b.md".to_string(), b"b base\n".to_vec()),
        ];
        let (base_root, resp) = push_vault_snapshot(&a, &vault, &base, ZERO_HASH_HEX)
            .await
            .unwrap();
        assert!(resp.accepted, "base push must be accepted, got {resp:?}");
        let parent = hash_to_hex(&base_root.hash());

        // A edits a.md, B edits b.md — different files, same claimed parent.
        let a_files = vec![
            (
                "a.md".to_string(),
                format!("a EDITED by A r{round}\n").into_bytes(),
            ),
            ("b.md".to_string(), b"b base\n".to_vec()),
        ];
        let b_files = vec![
            ("a.md".to_string(), b"a base\n".to_vec()),
            (
                "b.md".to_string(),
                format!("b EDITED by B r{round}\n").into_bytes(),
            ),
        ];

        // Fire both pushes with both requests genuinely in flight at once.
        let (ra, rb) = tokio::join!(
            push_vault_snapshot(&a, &vault, &a_files, &parent),
            push_vault_snapshot(&b, &vault, &b_files, &parent),
        );
        let (_, resp_a) = ra.unwrap();
        let (_, resp_b) = rb.unwrap();

        // A third observer pulls the authoritative state.
        let observer = WireClient::new(
            &env,
            env.enroll_device(&format!("race-{round}-obs"))
                .await
                .unwrap(),
        );
        let seen: BTreeMap<String, Vec<u8>> = pull_vault_snapshot(&observer, &vault)
            .await
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(
            seen.get("a.md").map(|v| v.as_slice()),
            Some(format!("a EDITED by A r{round}\n").as_bytes()),
            "round {round}: A's edit was silently dropped \
             (responses: A={resp_a:?} B={resp_b:?}) — put_root lost an update",
        );
        assert_eq!(
            seen.get("b.md").map(|v| v.as_slice()),
            Some(format!("b EDITED by B r{round}\n").as_bytes()),
            "round {round}: B's edit was silently dropped \
             (responses: A={resp_a:?} B={resp_b:?}) — put_root lost an update",
        );
    }
}
