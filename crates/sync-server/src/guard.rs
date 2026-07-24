//! Pre-commit guard for `put_root`.
//!
//! Incident 2026-07-13: a device with a stale in-memory Merkle tree but a
//! freshly-polled "current" root pushed `parent == current`, so the server
//! fast-forwarded wholesale onto a 2-day-old tree and reverted the fleet.
//! The claimed parent is self-asserted and cannot be trusted as proof that
//! the pusher's tree actually descends from it — so before committing ANY
//! new current root (fast-forward *or* merge result) we scan its content
//! against the root it replaces and refuse commits that look like mass
//! reverts:
//!
//! * `mtime_regression` — files whose content changes *backwards in time*
//!   (candidate hash differs AND candidate mtime is older). Legitimate work
//!   (edits, renames, folder moves, bulk AI rewrites) moves mtimes forward;
//!   a stale tree drags thousands of them backwards. Primary signal, very
//!   low false-positive rate.
//! * `blast_radius` — deletions + content changes exceeding a fraction of
//!   the vault. Secondary, catches stale trees whose files carry no mtimes.
//!
//! Device clocks are NOT trusted to agree: entry mtimes are stamped by
//! whichever device pushed the file, and fleets legitimately run with
//! hours of skew (no NTP, wrong RTC). A content change only counts as a
//! regression when it moves mtime backwards by MORE than the skew
//! allowance — far enough that no honest clock disagreement explains it.
//! The incident regressions were ~48h; the default allowance is 12h.
//!
//! Tunables (env, read once):
//!   OBSETYNC_GUARD                enforce | warn | off   (default: warn — scan
//!                                 + log but don't block; recover via rollback)
//!   OBSETYNC_GUARD_MTIME_K        max backwards-content files   (default: 25)
//!   OBSETYNC_GUARD_MTIME_SKEW_MS  clock-skew allowance before a backwards
//!                                 mtime counts (default: 43_200_000 = 12h)
//!   OBSETYNC_GUARD_BLAST_PCT      max (deletes+changes)% of vault (default: 25)
//!   OBSETYNC_GUARD_BLAST_MIN      blast floor in files, so small vaults aren't
//!                                 throttled by the percentage (default: 200)

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::OnceLock;

use sync_core::chunk::RootNode;
use sync_core::store::DiskChunkStore;
use sync_core::tree::load_all_entries;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardMode {
    Enforce,
    Warn,
    Off,
}

#[derive(Debug, Clone, Copy)]
pub struct GuardConfig {
    pub mode: GuardMode,
    pub mtime_regression_max: usize,
    /// How far backwards an mtime may move before it counts as a
    /// regression. Absorbs honest inter-device clock skew.
    pub mtime_skew_ms: u64,
    pub blast_pct: u64,
    pub blast_min: u64,
}

impl GuardConfig {
    fn from_env() -> Self {
        // Default WARN: the tripwire still SCANS and logs a loud line for a
        // mass deletion/revert, but does NOT block. The honest-parent client
        // fix (1.4.0) prevents the incident class at the source, and every root
        // is kept forever (COW) so a bad push is a one-click GUI rollback —
        // blocking legitimate bulk cleanups was more friction than value. Set
        // OBSETYNC_GUARD=enforce to block again, or =off to silence the scan.
        let mode = match std::env::var("OBSETYNC_GUARD").as_deref() {
            Ok("off") => GuardMode::Off,
            Ok("enforce") => GuardMode::Enforce,
            _ => GuardMode::Warn,
        };
        let parse = |key: &str, default: u64| -> u64 {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        Self {
            mode,
            mtime_regression_max: parse("OBSETYNC_GUARD_MTIME_K", 25) as usize,
            mtime_skew_ms: parse("OBSETYNC_GUARD_MTIME_SKEW_MS", 43_200_000),
            blast_pct: parse("OBSETYNC_GUARD_BLAST_PCT", 25),
            blast_min: parse("OBSETYNC_GUARD_BLAST_MIN", 200),
        }
    }
}

pub fn config() -> &'static GuardConfig {
    static CONFIG: OnceLock<GuardConfig> = OnceLock::new();
    CONFIG.get_or_init(GuardConfig::from_env)
}

/// Content-level comparison of the root being replaced vs its replacement.
#[derive(Debug, Default, Clone)]
pub struct GuardScan {
    /// Paths present in current but absent from candidate.
    pub deletions: usize,
    /// Paths present in both with differing content hash.
    pub content_changes: usize,
    /// Subset of `content_changes` where the candidate's mtime is older —
    /// content moving backwards in time, the stale-tree signature.
    pub mtime_regressions: usize,
    /// Paths only in candidate.
    pub additions: usize,
    /// File count of the root being replaced.
    pub current_total: u64,
}

impl GuardScan {
    /// Which tripwire (if any) this scan sets off under `cfg`.
    pub fn triggered(&self, cfg: &GuardConfig) -> Option<&'static str> {
        if self.mtime_regressions > cfg.mtime_regression_max {
            return Some("mtime_regression");
        }
        let blast = (self.deletions + self.content_changes) as u64;
        let ceiling = std::cmp::max(cfg.blast_min, self.current_total * cfg.blast_pct / 100);
        if blast > ceiling {
            return Some("blast_radius");
        }
        None
    }

    /// Machine-readable body for the 409 so clients can explain the refusal.
    pub fn reject_body(&self, reason: &str) -> String {
        serde_json::json!({
            "error": "guard_rejected",
            "reason": reason,
            "deletions": self.deletions,
            "content_changes": self.content_changes,
            "mtime_regressions": self.mtime_regressions,
            "current_total": self.current_total,
            "hint": "push looks like a stale-tree mass revert; pull + reconcile, then retry",
        })
        .to_string()
    }
}

/// Walk `current` and `candidate`, comparing only subtrees whose child
/// hashes differ (unchanged top-level prefixes are skipped wholesale), and
/// count deletions / content changes / mtime regressions.
///
/// Runs on a blocking thread with a LocalSet because sync-core's store
/// futures are `!Send` — same pattern as `bridge::run_merge`.
pub async fn scan(
    index_base: PathBuf,
    current: RootNode,
    candidate: RootNode,
) -> Result<GuardScan, String> {
    tokio::task::spawn_blocking(move || {
        let store = DiskChunkStore::new(&index_base);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let skew_ms = config().mtime_skew_ms;
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            scan_inner(&store, &current, &candidate, skew_ms)
                .await
                .map_err(|e| e.to_string())
        })
    })
    .await
    .map_err(|e| format!("join error: {}", e))?
}

async fn scan_inner(
    store: &DiskChunkStore,
    current: &RootNode,
    candidate: &RootNode,
    skew_ms: u64,
) -> Result<GuardScan, sync_core::chunk::ChunkError> {
    let mut out = GuardScan {
        current_total: current.total_files,
        ..Default::default()
    };

    let cur_children: HashMap<&String, &sync_core::hash::FileHash> =
        current.children.iter().map(|(p, h)| (p, h)).collect();
    let cand_children: HashMap<&String, &sync_core::hash::FileHash> =
        candidate.children.iter().map(|(p, h)| (p, h)).collect();

    // Prefixes in current: deleted wholesale, or changed, or identical.
    for (prefix, cur_hash) in &current.children {
        match cand_children.get(prefix) {
            Some(cand_hash) if *cand_hash == cur_hash => {} // identical subtree
            Some(cand_hash) => {
                let cur_entries = load_all_entries(store, cur_hash).await?;
                let cand_entries = load_all_entries(store, cand_hash).await?;
                compare_entries(&cur_entries, &cand_entries, skew_ms, &mut out);
            }
            None => {
                let cur_entries = load_all_entries(store, cur_hash).await?;
                out.deletions += cur_entries.len();
            }
        }
    }
    // Prefixes only in candidate: pure additions.
    for (prefix, cand_hash) in &candidate.children {
        if !cur_children.contains_key(prefix) {
            let cand_entries = load_all_entries(store, cand_hash).await?;
            out.additions += cand_entries.len();
        }
    }

    Ok(out)
}

/// Two-pointer compare of sorted entry lists (same ordering guarantee as
/// `diff_entries` in sync-core). A backwards mtime only counts as a
/// regression when it exceeds `skew_ms` — honest fleets run with hours of
/// clock skew, and entry mtimes are stamped by whichever device pushed.
fn compare_entries(
    cur: &[sync_core::chunk::FileEntry],
    cand: &[sync_core::chunk::FileEntry],
    skew_ms: u64,
    out: &mut GuardScan,
) {
    let mut i = 0;
    let mut j = 0;
    while i < cur.len() && j < cand.len() {
        match cur[i].path.cmp(&cand[j].path) {
            std::cmp::Ordering::Equal => {
                if cur[i].hash != cand[j].hash {
                    out.content_changes += 1;
                    if cand[j].mtime_ms.saturating_add(skew_ms) < cur[i].mtime_ms {
                        out.mtime_regressions += 1;
                    }
                }
                i += 1;
                j += 1;
            }
            std::cmp::Ordering::Less => {
                out.deletions += 1;
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.additions += 1;
                j += 1;
            }
        }
    }
    out.deletions += cur.len() - i;
    out.additions += cand.len() - j;
}

#[cfg(test)]
mod tests {
    use super::*;
    use sync_core::chunk::FileEntry;
    use sync_core::hash::hash_bytes;
    use sync_core::tree::build_tree;
    use tempfile::tempdir;

    const HOUR: u64 = 3_600_000;
    const DAY: u64 = 24 * HOUR;
    /// Arbitrary "now" epoch-ms for tests.
    const T0: u64 = 1_800_000_000_000;

    fn entry(path: &str, content: &str, mtime: u64) -> FileEntry {
        FileEntry::new(path.into(), hash_bytes(content.as_bytes()), mtime, 10)
    }

    async fn build_on_disk(dir: PathBuf, entries: Vec<FileEntry>) -> RootNode {
        tokio::task::spawn_blocking(move || {
            let store = DiskChunkStore::new(&dir);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async move {
                build_tree(&store, entries, "v", "d").await.unwrap()
            })
        })
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stale_tree_shows_regressions_and_deletions() {
        let dir = tempdir().unwrap();
        // Current: three recent files.
        let current = build_on_disk(
            dir.path().to_path_buf(),
            vec![
                entry("n/a.md", "a-v2", T0),
                entry("n/b.md", "b-v2", T0),
                entry("n/c.md", "c", T0),
            ],
        )
        .await;
        // Candidate: stale tree — 2-day-old content for a/b (well past any
        // clock-skew allowance), c missing. The incident signature.
        let candidate = build_on_disk(
            dir.path().to_path_buf(),
            vec![
                entry("n/a.md", "a-v1", T0 - 2 * DAY),
                entry("n/b.md", "b-v1", T0 - 2 * DAY),
            ],
        )
        .await;

        let scan = scan(dir.path().to_path_buf(), current, candidate)
            .await
            .unwrap();
        assert_eq!(scan.content_changes, 2);
        assert_eq!(scan.mtime_regressions, 2);
        assert_eq!(scan.deletions, 1);

        let cfg = GuardConfig {
            mode: GuardMode::Enforce,
            mtime_regression_max: 1,
            mtime_skew_ms: 12 * HOUR,
            blast_pct: 25,
            blast_min: 200,
        };
        assert_eq!(scan.triggered(&cfg), Some("mtime_regression"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn forward_edits_do_not_trigger() {
        let dir = tempdir().unwrap();
        let current = build_on_disk(
            dir.path().to_path_buf(),
            vec![entry("n/a.md", "a-v1", T0), entry("n/b.md", "b", T0)],
        )
        .await;
        // Candidate: normal work — a edited forward, d added.
        let candidate = build_on_disk(
            dir.path().to_path_buf(),
            vec![
                entry("n/a.md", "a-v2", T0 + HOUR),
                entry("n/b.md", "b", T0),
                entry("n/d.md", "d", T0 + HOUR),
            ],
        )
        .await;

        let scan = scan(dir.path().to_path_buf(), current, candidate)
            .await
            .unwrap();
        assert_eq!(scan.mtime_regressions, 0);
        assert_eq!(scan.deletions, 0);
        assert_eq!(scan.content_changes, 1);
        assert_eq!(scan.additions, 1);

        let cfg = GuardConfig {
            mode: GuardMode::Enforce,
            mtime_regression_max: 25,
            mtime_skew_ms: 12 * HOUR,
            blast_pct: 25,
            blast_min: 200,
        };
        assert_eq!(scan.triggered(&cfg), None);
    }

    /// Honest cross-device edits from a laptop whose clock lags a couple
    /// hours must NOT count as regressions — mtimes are stamped by whichever
    /// device pushed, and fleets are not required to run NTP.
    #[tokio::test(flavor = "multi_thread")]
    async fn clock_skew_within_allowance_is_not_regression() {
        let dir = tempdir().unwrap();
        let current = build_on_disk(
            dir.path().to_path_buf(),
            vec![
                entry("n/a.md", "a-desktop", T0),
                entry("n/b.md", "b-desktop", T0),
            ],
        )
        .await;
        // Laptop clock runs 2h behind but genuinely edits both files.
        let candidate = build_on_disk(
            dir.path().to_path_buf(),
            vec![
                entry("n/a.md", "a-laptop", T0 - 2 * HOUR),
                entry("n/b.md", "b-laptop", T0 - 2 * HOUR),
            ],
        )
        .await;

        let scan = scan(dir.path().to_path_buf(), current, candidate)
            .await
            .unwrap();
        assert_eq!(scan.content_changes, 2);
        assert_eq!(
            scan.mtime_regressions, 0,
            "2h backwards is within the 12h skew allowance"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn identical_subtrees_are_skipped() {
        let dir = tempdir().unwrap();
        let shared = vec![entry("keep/x.md", "x", T0)];
        let mut cur = shared.clone();
        cur.push(entry("work/y.md", "y-v2", T0));
        let mut cand = shared;
        cand.push(entry("work/y.md", "y-v1", T0 - 2 * DAY));

        let current = build_on_disk(dir.path().to_path_buf(), cur).await;
        let candidate = build_on_disk(dir.path().to_path_buf(), cand).await;

        let scan = scan(dir.path().to_path_buf(), current, candidate)
            .await
            .unwrap();
        assert_eq!(scan.content_changes, 1);
        assert_eq!(scan.mtime_regressions, 1);
    }
}
