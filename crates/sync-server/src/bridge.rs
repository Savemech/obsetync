use std::path::PathBuf;
use sync_core::chunk::RootNode;
use sync_core::diff::FileDelta;
use sync_core::merge::MergeResult;
use sync_core::store::DiskChunkStore;

/// Run sync-core's `merge_trees` in a blocking task with a LocalSet
/// to handle the `!Send` futures from `ChunkStore` trait.
pub async fn run_merge(
    index_base: PathBuf,
    base: RootNode,
    side_a: RootNode,
    side_b: RootNode,
) -> Result<MergeResult, String> {
    tokio::task::spawn_blocking(move || {
        let store = DiskChunkStore::new(&index_base);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            sync_core::merge::merge_trees(&store, &base, &side_a, &side_b)
                .await
                .map_err(|e| e.to_string())
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
        let result = run_merge(dir.path().to_path_buf(), base, side_a, side_b)
            .await
            .unwrap();
        assert!(result.file_conflicts.is_empty());
        assert_eq!(result.new_root.total_files, 1);
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
        let result = run_merge(dir.path().to_path_buf(), base, side_a, side_b)
            .await
            .unwrap();
        assert_eq!(result.file_conflicts.len(), 1);
        assert_eq!(result.file_conflicts[0].path, "a.md");
    }
}
