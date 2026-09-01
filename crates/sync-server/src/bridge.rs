use crate::storage_writer::StorageWriter;
use sync_core::chunk::RootNode;
use sync_core::diff::FileDelta;
#[cfg(test)]
use sync_core::merge::MergeResult;
use sync_core::merge::VersionedMergeResult;
use sync_core::versioned_root::VersionedRoot;

const MAX_ROOT_FILES: u64 = 5_000_000;
const JS_MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Run sync-core's `merge_trees` in a blocking task with a LocalSet
/// to handle the `!Send` futures from `ChunkStore` trait.
///
/// The same packed store supplies index nodes and small-file bytes. Merged
/// index/content objects return through its single durable writer.
#[cfg(test)]
pub async fn run_merge(
    store: StorageWriter,
    base: RootNode,
    side_a: RootNode,
    side_b: RootNode,
) -> Result<MergeResult, String> {
    store
        .run_blocking(move |store| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async {
                sync_core::merge::merge_trees(&store, &store, &base, &side_a, &side_b)
                    .await
                    .map_err(|e| e.to_string())
            })
        })
        .await
        .map_err(|e| e.to_string())?
}

pub async fn run_versioned_merge(
    store: StorageWriter,
    base: VersionedRoot,
    side_a: VersionedRoot,
    side_b: VersionedRoot,
) -> Result<VersionedMergeResult, String> {
    store
        .run_blocking(move |store| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async {
                sync_core::merge::merge_versioned_trees(&store, &store, &base, &side_a, &side_b)
                    .await
                    .map_err(|e| e.to_string())
            })
        })
        .await
        .map_err(|e| e.to_string())?
}

/// Load every file entry reachable from a root (all subtrees flattened),
/// in a blocking task with a LocalSet — used by the admin export to
/// materialize a snapshot without touching merge/diff logic.
#[cfg(test)]
pub async fn run_list_entries(
    store: StorageWriter,
    root: RootNode,
) -> Result<Vec<sync_core::chunk::FileEntry>, String> {
    store
        .run_blocking(move |store| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async {
                // Stored roots are normally trusted, but never turn a corrupt
                // total_files field into a giant eager allocation.
                let mut entries =
                    Vec::with_capacity(usize::try_from(root.total_files).unwrap_or(0).min(100_000));
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
        .map_err(|e| e.to_string())?
}

pub async fn run_versioned_list_entries(
    store: StorageWriter,
    root: VersionedRoot,
) -> Result<Vec<sync_core::chunk::FileEntry>, String> {
    store
        .run_blocking(move |store| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async {
                sync_core::versioned_root::load_all_entries(&store, &root)
                    .await
                    .map_err(|e| e.to_string())
            })
        })
        .await
        .map_err(|e| e.to_string())?
}

/// Rebuild one semantic snapshot in another tree format. The immutable file
/// entries/content hashes are unchanged; only index nodes and the root
/// envelope are regenerated. Used by explicit admin activation/rollback of
/// Tree v2, never by an ordinary client push.
pub async fn run_project_root_version(
    store: StorageWriter,
    root: VersionedRoot,
    target_version: u32,
    device_id: String,
) -> Result<VersionedRoot, String> {
    store
        .run_blocking(move |store| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async {
                let entries = sync_core::versioned_root::load_all_entries(&store, &root)
                    .await
                    .map_err(|e| e.to_string())?;
                sync_core::versioned_root::build_root(
                    &store,
                    entries,
                    target_version,
                    root.vault_id(),
                    &device_id,
                )
                .await
                .map_err(|e| e.to_string())
            })
        })
        .await
        .map_err(|e| e.to_string())?
}

/// Validate and flatten an untrusted uploaded root before it can become the
/// current vault state. The first implementation is deliberately routed
/// through the same loader as admin export; structural checks are layered in
/// by the root-validation regression tests below.
pub async fn run_validate_root(
    store: StorageWriter,
    root: RootNode,
) -> Result<Vec<sync_core::chunk::FileEntry>, String> {
    store
        .run_blocking(move |store| {
            if root.version != 1 {
                return Err(format!("unsupported root version {}", root.version));
            }
            if root.total_files > MAX_ROOT_FILES {
                return Err(format!(
                    "root declares too many files: {}",
                    root.total_files
                ));
            }
            if root.children.len() as u64 > root.total_files && root.total_files != 0 {
                return Err("root has more top-level children than files".to_string());
            }
            if root.total_files == 0 && !root.children.is_empty() {
                return Err("empty root has child nodes".to_string());
            }

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async {
                let mut entries =
                    Vec::with_capacity(usize::try_from(root.total_files).unwrap_or(0).min(100_000));
                let mut previous_prefix: Option<&str> = None;
                for (prefix, child_hash) in &root.children {
                    if previous_prefix.is_some_and(|previous| previous >= prefix.as_str()) {
                        return Err("root child prefixes are not strictly sorted".to_string());
                    }
                    if !valid_top_level_prefix(prefix) {
                        return Err(format!("invalid root child prefix {prefix:?}"));
                    }
                    previous_prefix = Some(prefix);

                    let child = sync_core::tree::load_all_entries(&store, child_hash)
                        .await
                        .map_err(|e| e.to_string())?;
                    // Each prefix is its own sorted namespace. The canonical root
                    // order puts the root-level prefix (`""`) first, but a root
                    // file such as `seed.md` can lexically follow `notes/x.md` in
                    // the next prefix, so ordering must not leak across children.
                    let mut previous_path: Option<String> = None;
                    for entry in child {
                        if !valid_vault_path(&entry.path) {
                            return Err(format!("unsafe vault path {:?}", entry.path));
                        }
                        if top_level_prefix(&entry.path) != prefix {
                            return Err(format!(
                                "entry {:?} is stored under wrong root prefix {:?}",
                                entry.path, prefix
                            ));
                        }
                        if previous_path
                            .as_ref()
                            .is_some_and(|previous| previous >= &entry.path)
                        {
                            return Err("root entries are not strictly path-sorted".to_string());
                        }
                        if entry.size_bytes > JS_MAX_SAFE_INTEGER
                            || entry.mtime_ms > JS_MAX_SAFE_INTEGER
                        {
                            return Err(format!(
                                "entry {:?} exceeds client integer range",
                                entry.path
                            ));
                        }
                        previous_path = Some(entry.path.clone());
                        entries.push(entry);
                        if entries.len() as u64 > root.total_files {
                            return Err("root contains more entries than declared".to_string());
                        }
                    }
                }
                if entries.len() as u64 != root.total_files {
                    return Err(format!(
                        "root declares {} files but contains {}",
                        root.total_files,
                        entries.len()
                    ));
                }
                Ok(entries)
            })
        })
        .await
        .map_err(|e| e.to_string())?
}

pub async fn run_validate_versioned_root(
    store: StorageWriter,
    root: VersionedRoot,
) -> Result<Vec<sync_core::chunk::FileEntry>, String> {
    match root {
        VersionedRoot::V1(root) => run_validate_root(store, root).await,
        VersionedRoot::V2(root) => {
            if root.total_files() > MAX_ROOT_FILES {
                return Err(format!(
                    "root declares too many files: {}",
                    root.total_files()
                ));
            }
            store
                .run_blocking(move |store| {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| e.to_string())?;
                    let local = tokio::task::LocalSet::new();
                    local.block_on(&rt, async {
                        let entries = sync_core::tree_v2::load_all_entries(&store, &root.tree)
                            .await
                            .map_err(|e| e.to_string())?;
                        for entry in &entries {
                            if !valid_vault_path(&entry.path) {
                                return Err(format!("unsafe vault path {:?}", entry.path));
                            }
                            if entry.size_bytes > JS_MAX_SAFE_INTEGER
                                || entry.mtime_ms > JS_MAX_SAFE_INTEGER
                            {
                                return Err(format!(
                                    "entry {:?} exceeds client integer range",
                                    entry.path
                                ));
                            }
                        }
                        Ok(entries)
                    })
                })
                .await
                .map_err(|e| e.to_string())?
        }
    }
}

fn valid_vault_path(path: &str) -> bool {
    if path.is_empty() || path.len() > 4096 || path.starts_with('/') || path.contains('\\') {
        return false;
    }
    if path.chars().any(|ch| ch.is_control()) {
        return false;
    }
    path.split('/')
        .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

fn top_level_prefix(path: &str) -> &str {
    path.find('/').map_or("", |index| &path[..=index])
}

fn valid_top_level_prefix(prefix: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }
    let Some(segment) = prefix.strip_suffix('/') else {
        return false;
    };
    !segment.contains('/') && valid_vault_path(segment)
}

/// Rebuild `root` with every entry matching one of `patterns` removed — the
/// admin "purge" action (Slice 2b). A one-shot, COW-reversible cleanup that
/// lifts a build-output tree (target/, node_modules/, …) out of the shared
/// Merkle tree so that ignoring clients can reach parity again. Returns
/// `(new_root, removed, kept)`. Reuses the same flatten-then-rebuild path as
/// export + build_tree, so it can't desync from how roots are normally built.
#[cfg(test)]
pub async fn run_purge(
    store: StorageWriter,
    root: RootNode,
    patterns: Vec<String>,
) -> Result<(RootNode, usize, usize), String> {
    store
        .run_blocking(move |store| {
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
                let new_root =
                    sync_core::tree::build_tree(&store, entries, &vault_id, "admin-purge")
                        .await
                        .map_err(|e| e.to_string())?;
                Ok((new_root, removed, kept))
            })
        })
        .await
        .map_err(|e| e.to_string())?
}

pub async fn run_versioned_purge(
    store: StorageWriter,
    root: VersionedRoot,
    patterns: Vec<String>,
) -> Result<(VersionedRoot, usize, usize), String> {
    store
        .run_blocking(move |store| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async {
                let version = root.version();
                let vault_id = root.vault_id().to_owned();
                let parent = root.hash();
                let mut entries = sync_core::versioned_root::load_all_entries(&store, &root)
                    .await
                    .map_err(|e| e.to_string())?;
                let before = entries.len();
                entries.retain(|entry| !crate::ignore_match::matches_any(&entry.path, &patterns));
                let kept = entries.len();
                let removed = before - kept;
                let mut new_root = sync_core::versioned_root::build_root(
                    &store,
                    entries,
                    version,
                    &vault_id,
                    "admin-purge",
                )
                .await
                .map_err(|e| e.to_string())?;
                new_root.set_history_metadata(0, Some(parent), "admin-purge");
                Ok((new_root, removed, kept))
            })
        })
        .await
        .map_err(|e| e.to_string())?
}

/// Run sync-core's `compute_deltas` in a blocking task with a LocalSet.
#[cfg(test)]
pub async fn run_diff(
    store: StorageWriter,
    from_root: RootNode,
    to_root: RootNode,
) -> Result<Vec<FileDelta>, String> {
    Ok(run_diff_with_stats(store, from_root, to_root).await?.deltas)
}

#[cfg(test)]
pub async fn run_diff_with_stats(
    store: StorageWriter,
    from_root: RootNode,
    to_root: RootNode,
) -> Result<sync_core::diff::DiffResult, String> {
    store
        .run_blocking(move |store| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async {
                sync_core::diff::compute_deltas_with_stats(&store, &from_root, &to_root)
                    .await
                    .map_err(|e| e.to_string())
            })
        })
        .await
        .map_err(|e| e.to_string())?
}

pub async fn run_versioned_diff_with_stats(
    store: StorageWriter,
    from_root: VersionedRoot,
    to_root: VersionedRoot,
) -> Result<sync_core::diff::DiffResult, String> {
    store
        .run_blocking(move |store| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async {
                sync_core::diff::compute_versioned_deltas_with_stats(&store, &from_root, &to_root)
                    .await
                    .map_err(|e| e.to_string())
            })
        })
        .await
        .map_err(|e| e.to_string())?
}

pub async fn run_versioned_diff(
    store: StorageWriter,
    from_root: VersionedRoot,
    to_root: VersionedRoot,
) -> Result<Vec<FileDelta>, String> {
    Ok(run_versioned_diff_with_stats(store, from_root, to_root)
        .await?
        .deltas)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::perf::ServerPerfCounters;
    use crate::storage::StorageLayout;
    use std::sync::Arc;
    use sync_core::chunk::FileEntry;
    use sync_core::hash::hash_bytes;
    use sync_core::tree::build_tree;
    use tempfile::tempdir;

    fn make_entry(path: &str, content: &str) -> FileEntry {
        FileEntry::new(path.into(), hash_bytes(content.as_bytes()), 1, 1)
    }

    /// Build a tree on a temp index dir. Uses spawn_blocking + a fresh
    /// current-thread runtime so it composes cleanly inside the outer
    /// `#[tokio::test]` (multi-threaded) runtime that drives the test.
    fn test_writer(path: &std::path::Path) -> StorageWriter {
        let layout = StorageLayout::new(path);
        layout.init_directories().unwrap();
        StorageWriter::start(layout, Arc::new(ServerPerfCounters::default())).unwrap()
    }

    async fn build_tree_on_store(store: StorageWriter, entries: Vec<FileEntry>) -> RootNode {
        tokio::task::spawn_blocking(move || {
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
        let store = test_writer(dir.path());
        let root = build_tree_on_store(store.clone(), vec![make_entry("a.md", "x")]).await;
        let deltas = run_diff(store, root.clone(), root).await.unwrap();
        assert!(deltas.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_diff_detects_addition() {
        let dir = tempdir().unwrap();
        let store = test_writer(dir.path());
        let r1 = build_tree_on_store(store.clone(), vec![make_entry("a.md", "x")]).await;
        let r2 = build_tree_on_store(
            store.clone(),
            vec![make_entry("a.md", "x"), make_entry("b.md", "y")],
        )
        .await;
        let deltas = run_diff(store, r1, r2).await.unwrap();
        assert_eq!(deltas.len(), 1);
        assert!(matches!(
            &deltas[0],
            sync_core::diff::FileDelta::Added { path, .. } if path == "b.md"
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn root_validation_rejects_false_file_count() {
        let dir = tempdir().unwrap();
        let store = test_writer(dir.path());
        let mut root = build_tree_on_store(store.clone(), vec![make_entry("a.md", "x")]).await;
        root.total_files = 999;
        assert!(run_validate_root(store, root).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn root_validation_rejects_unsafe_entry_path() {
        let dir = tempdir().unwrap();
        let store = test_writer(dir.path());
        let root = build_tree_on_store(store.clone(), vec![make_entry("../outside.md", "x")]).await;
        assert!(run_validate_root(store, root).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn root_validation_accepts_root_files_with_directory_prefixes() {
        let dir = tempdir().unwrap();
        let store = test_writer(dir.path());
        let root = build_tree_on_store(
            store.clone(),
            vec![
                make_entry("seed.md", "seed"),
                make_entry("notes/x.md", "x"),
                make_entry("photos/y.png", "y"),
            ],
        )
        .await;

        let entries = run_validate_root(store, root).await.unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_merge_passes_through_to_sync_core() {
        let dir = tempdir().unwrap();
        let store = test_writer(dir.path());
        let base = build_tree_on_store(store.clone(), vec![make_entry("a.md", "x")]).await;
        let side_a = build_tree_on_store(store.clone(), vec![make_entry("a.md", "y")]).await;
        let side_b = base.clone();
        let result = run_merge(store, base, side_a, side_b).await.unwrap();
        assert!(result.file_conflicts.is_empty());
        assert_eq!(result.new_root.total_files, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_purge_removes_matched_subtree_and_keeps_real_notes() {
        let dir = tempdir().unwrap();
        let store = test_writer(dir.path());
        let root = build_tree_on_store(
            store.clone(),
            vec![
                make_entry("notes/a.md", "x"),
                make_entry("proj/target/debug/lib.rmeta", "junk1"),
                make_entry("proj/target/deps/other.rlib", "junk2"),
                make_entry("proj/src/main.rs", "code"),
                make_entry("target.md", "a note that merely mentions target"),
            ],
        )
        .await;

        let (new_root, removed, kept) = run_purge(store.clone(), root, vec!["target/".to_string()])
            .await
            .unwrap();

        // The two files under a `target/` dir go; the real note that merely has
        // "target" in its name stays, as do the other real files.
        assert_eq!(removed, 2);
        assert_eq!(kept, 3);
        assert_eq!(new_root.total_files, 3);

        let entries = run_list_entries(store, new_root).await.unwrap();
        let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
        assert!(paths.contains(&"notes/a.md"));
        assert!(paths.contains(&"proj/src/main.rs"));
        assert!(paths.contains(&"target.md")); // a real note, NOT purged
        assert!(!paths.iter().any(|p| p.contains("/target/")));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_purge_no_match_removes_nothing() {
        let dir = tempdir().unwrap();
        let store = test_writer(dir.path());
        let root = build_tree_on_store(
            store.clone(),
            vec![make_entry("a.md", "x"), make_entry("b.md", "y")],
        )
        .await;
        let (_new, removed, kept) = run_purge(store, root, vec!["node_modules/".to_string()])
            .await
            .unwrap();
        assert_eq!(removed, 0);
        assert_eq!(kept, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_merge_returns_conflicts_for_divergent_edits() {
        let dir = tempdir().unwrap();
        let store = test_writer(dir.path());
        let base = build_tree_on_store(store.clone(), vec![make_entry("a.md", "x")]).await;
        let side_a = build_tree_on_store(store.clone(), vec![make_entry("a.md", "side-a")]).await;
        let side_b = build_tree_on_store(store.clone(), vec![make_entry("a.md", "side-b")]).await;
        let result = run_merge(store, base, side_a, side_b).await.unwrap();
        assert_eq!(result.file_conflicts.len(), 1);
        assert_eq!(result.file_conflicts[0].path, "a.md");
    }
}
