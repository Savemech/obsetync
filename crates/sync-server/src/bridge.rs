use std::path::PathBuf;
use sync_core::chunk::RootNode;
use sync_core::content_store::DiskContentStore;
use sync_core::diff::FileDelta;
use sync_core::merge::MergeResult;
use sync_core::store::DiskChunkStore;

/// Run sync-core's `merge_trees` in a blocking task with a LocalSet
/// to handle the `!Send` futures from `ChunkStore` trait.
///
/// `content_base` is the small-blob content root (`<data>/content`) — the
/// merge reads base/A/B file bytes from it for content-level text merges
/// and writes merged blobs back so pullers can fetch them by hash.
pub async fn run_merge(
    index_base: PathBuf,
    content_base: PathBuf,
    base: RootNode,
    side_a: RootNode,
    side_b: RootNode,
) -> Result<MergeResult, String> {
    tokio::task::spawn_blocking(move || {
        let store = DiskChunkStore::new(&index_base);
        let content = DiskContentStore::new(&content_base);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            sync_core::merge::merge_trees(&store, &content, &base, &side_a, &side_b)
                .await
                .map_err(|e| e.to_string())
        })
    })
    .await
    .map_err(|e| format!("join error: {}", e))?
}

/// Load every file entry reachable from a root (all subtrees flattened),
/// in a blocking task with a LocalSet — used by the admin export to
/// materialize a snapshot without touching merge/diff logic.
pub async fn run_list_entries(
    index_base: PathBuf,
    root: RootNode,
) -> Result<Vec<sync_core::chunk::FileEntry>, String> {
    tokio::task::spawn_blocking(move || {
        let store = DiskChunkStore::new(&index_base);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            let mut entries = Vec::with_capacity(root.total_files as usize);
            for (_prefix, child_hash) in &root.children {
                let mut child = sync_core::tree::load_all_entries(&store, child_hash)
                    .await
                    .map_err(|e| e.to_string())?;
                entries.append(&mut child);
            }
            entries.sort();
            Ok(entries)
        })
    })
    .await
    .map_err(|e| format!("join error: {}", e))?
}

/// Rebuild `root` with every entry matching one of `patterns` removed — the
/// admin "purge" action (Slice 2b). A one-shot, COW-reversible cleanup that
/// lifts a build-output tree (target/, node_modules/, …) out of the shared
/// Merkle tree so that ignoring clients can reach parity again. Returns
/// `(new_root, removed, kept)`. Reuses the same flatten-then-rebuild path as
/// export + build_tree, so it can't desync from how roots are normally built.
pub async fn run_purge(
    index_base: PathBuf,
    root: RootNode,
    patterns: Vec<String>,
) -> Result<(RootNode, usize, usize), String> {
    tokio::task::spawn_blocking(move || {
        let store = DiskChunkStore::new(&index_base);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            let vault_id = root.vault_id.clone();
            let mut entries = Vec::with_capacity(root.total_files as usize);
            for (_prefix, child_hash) in &root.children {
                let mut child = sync_core::tree::load_all_entries(&store, child_hash)
                    .await
                    .map_err(|e| e.to_string())?;
                entries.append(&mut child);
            }
            let before = entries.len();
            entries.retain(|e| !crate::ignore_match::matches_any(&e.path, &patterns));
            let kept = entries.len();
            let removed = before - kept;
            let new_root = sync_core::tree::build_tree(&store, entries, &vault_id, "admin-purge")
                .await
                .map_err(|e| e.to_string())?;
            Ok((new_root, removed, kept))
        })
    })
    .await
    .map_err(|e| format!("join error: {}", e))?
}

/// Run sync-core's `compute_deltas` in a blocking task with a LocalSet.
pub async fn run_diff(
    index_base: PathBuf,
    from_root: RootNode,
    to_root: RootNode,
) -> Result<Vec<FileDelta>, String> {
    tokio::task::spawn_blocking(move || {
        let store = DiskChunkStore::new(&index_base);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            sync_core::diff::compute_deltas(&store, &from_root, &to_root)
                .await
                .map_err(|e| e.to_string())
        })
    })
    .await
    .map_err(|e| format!("join error: {}", e))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use sync_core::chunk::FileEntry;
    use sync_core::hash::hash_bytes;
    use sync_core::store::DiskChunkStore;
    use sync_core::tree::build_tree;
    use tempfile::tempdir;

    fn make_entry(path: &str, content: &str) -> FileEntry {
        FileEntry::new(path.into(), hash_bytes(content.as_bytes()), 1, 1)
    }

    /// Build a tree on a temp index dir. Uses spawn_blocking + a fresh
    /// current-thread runtime so it composes cleanly inside the outer
    /// `#[tokio::test]` (multi-threaded) runtime that drives the test.
    async fn build_tree_on_disk(dir: PathBuf, entries: Vec<FileEntry>) -> RootNode {
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
    async fn run_diff_on_identical_roots_returns_empty() {
        let dir = tempdir().unwrap();
        let root =
            build_tree_on_disk(dir.path().to_path_buf(), vec![make_entry("a.md", "x")]).await;
        let deltas = run_diff(dir.path().to_path_buf(), root.clone(), root)
            .await
            .unwrap();
        assert!(deltas.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_diff_detects_addition() {
        let dir = tempdir().unwrap();
        let r1 = build_tree_on_disk(dir.path().to_path_buf(), vec![make_entry("a.md", "x")]).await;
        let r2 = build_tree_on_disk(
            dir.path().to_path_buf(),
            vec![make_entry("a.md", "x"), make_entry("b.md", "y")],
        )
        .await;
        let deltas = run_diff(dir.path().to_path_buf(), r1, r2).await.unwrap();
        assert_eq!(deltas.len(), 1);
        assert!(matches!(
            &deltas[0],
            sync_core::diff::FileDelta::Added { path, .. } if path == "b.md"
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_merge_passes_through_to_sync_core() {
        let dir = tempdir().unwrap();
        let base =
            build_tree_on_disk(dir.path().to_path_buf(), vec![make_entry("a.md", "x")]).await;
        let side_a =
            build_tree_on_disk(dir.path().to_path_buf(), vec![make_entry("a.md", "y")]).await;
        let side_b = base.clone();
        let result = run_merge(
            dir.path().to_path_buf(),
            dir.path().join("content"),
            base,
            side_a,
            side_b,
        )
        .await
        .unwrap();
        assert!(result.file_conflicts.is_empty());
        assert_eq!(result.new_root.total_files, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_purge_removes_matched_subtree_and_keeps_real_notes() {
        let dir = tempdir().unwrap();
        let root = build_tree_on_disk(
            dir.path().to_path_buf(),
            vec![
                make_entry("notes/a.md", "x"),
                make_entry("proj/target/debug/lib.rmeta", "junk1"),
                make_entry("proj/target/deps/other.rlib", "junk2"),
                make_entry("proj/src/main.rs", "code"),
                make_entry("target.md", "a note that merely mentions target"),
            ],
        )
        .await;

        let (new_root, removed, kept) =
            run_purge(dir.path().to_path_buf(), root, vec!["target/".to_string()])
                .await
                .unwrap();

        // The two files under a `target/` dir go; the real note that merely has
        // "target" in its name stays, as do the other real files.
        assert_eq!(removed, 2);
        assert_eq!(kept, 3);
        assert_eq!(new_root.total_files, 3);

        let entries = run_list_entries(dir.path().to_path_buf(), new_root)
            .await
            .unwrap();
        let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
        assert!(paths.contains(&"notes/a.md"));
        assert!(paths.contains(&"proj/src/main.rs"));
        assert!(paths.contains(&"target.md")); // a real note, NOT purged
        assert!(!paths.iter().any(|p| p.contains("/target/")));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_purge_no_match_removes_nothing() {
        let dir = tempdir().unwrap();
        let root = build_tree_on_disk(
            dir.path().to_path_buf(),
            vec![make_entry("a.md", "x"), make_entry("b.md", "y")],
        )
        .await;
        let (_new, removed, kept) = run_purge(
            dir.path().to_path_buf(),
            root,
            vec!["node_modules/".to_string()],
        )
        .await
        .unwrap();
        assert_eq!(removed, 0);
        assert_eq!(kept, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_merge_returns_conflicts_for_divergent_edits() {
        let dir = tempdir().unwrap();
        let base =
            build_tree_on_disk(dir.path().to_path_buf(), vec![make_entry("a.md", "x")]).await;
        let side_a =
            build_tree_on_disk(dir.path().to_path_buf(), vec![make_entry("a.md", "side-a")]).await;
        let side_b =
            build_tree_on_disk(dir.path().to_path_buf(), vec![make_entry("a.md", "side-b")]).await;
        let result = run_merge(
            dir.path().to_path_buf(),
            dir.path().join("content"),
            base,
            side_a,
            side_b,
        )
        .await
        .unwrap();
        assert_eq!(result.file_conflicts.len(), 1);
        assert_eq!(result.file_conflicts[0].path, "a.md");
    }
}
