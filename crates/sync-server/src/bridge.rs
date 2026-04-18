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
