use crate::chunk::{ChunkError, FileEntry, RootNode};
use crate::conflict::{three_way_text_merge, FileConflict, TextMergeResult};
use crate::content_store::ContentStore;
use crate::fastcdc_chunker::should_chunk;
use crate::hash::{hash_bytes, FileHash};
use crate::store::ChunkStore;
use crate::sync_rules::is_text_file;
use crate::tree::load_all_entries;
use crate::versioned_root::VersionedRoot;
use std::collections::HashMap;

/// Result of a three-way tree merge.
#[derive(Debug)]
pub struct MergeResult {
    /// The merged root node.
    pub new_root: RootNode,
    /// File-level conflicts that need resolution via D-003.
    pub file_conflicts: Vec<FileConflict>,
    /// Files that were auto-resolved (one side changed, take that side).
    pub auto_resolved_count: usize,
    /// Same-file two-sided edits that were auto-merged at the CONTENT level
    /// (three-way line merge of non-overlapping changes). Counted separately
    /// from `auto_resolved_count` so callers can tell tree-level picks from
    /// genuine text merges.
    pub text_merged_count: usize,
}

#[derive(Debug)]
pub struct VersionedMergeResult {
    pub new_root: VersionedRoot,
    pub file_conflicts: Vec<FileConflict>,
    pub auto_resolved_count: usize,
    pub text_merged_count: usize,
}

/// A modify-modify candidate deferred to the content-level text-merge
/// post-pass: both sides changed `path` relative to base. Side A's entry is
/// provisionally in the merged list; the post-pass either replaces it with a
/// line-merged version or records a `FileConflict`.
struct TextMergeTodo {
    merged_index: usize,
    base_hash: FileHash,
    base_size: u64,
    b_hash: FileHash,
    b_size: u64,
    b_mtime: u64,
}

/// Three-way merge of two trees that diverged from a common ancestor.
/// Resolves at the tree level; same-file two-sided TEXT edits get a
/// content-level three-way line merge (non-overlapping changes from both
/// sides survive in one file). Returns file-level conflicts for whatever
/// could not be merged.
///
/// This is the server-side merge (D-006): when two devices push competing roots,
/// the server calls this to produce a merged root without rejecting either push.
pub async fn merge_trees<S: ChunkStore, C: ContentStore>(
    store: &S,
    content: &C,
    base: &RootNode,
    side_a: &RootNode, // current server root
    side_b: &RootNode, // incoming push
) -> Result<MergeResult, ChunkError> {
    // Collect all prefixes across base, A, and B.
    let base_map: HashMap<&str, &FileHash> =
        base.children.iter().map(|(p, h)| (p.as_str(), h)).collect();
    let a_map: HashMap<&str, &FileHash> = side_a
        .children
        .iter()
        .map(|(p, h)| (p.as_str(), h))
        .collect();
    let b_map: HashMap<&str, &FileHash> = side_b
        .children
        .iter()
        .map(|(p, h)| (p.as_str(), h))
        .collect();

    let mut all_prefixes: Vec<&str> = base_map
        .keys()
        .chain(a_map.keys())
        .chain(b_map.keys())
        .copied()
        .collect();
    all_prefixes.sort();
    all_prefixes.dedup();

    let mut merged_entries: Vec<FileEntry> = Vec::new();
    let mut file_conflicts = Vec::new();
    let mut auto_resolved = 0usize;
    let mut text_todo: Vec<TextMergeTodo> = Vec::new();

    for prefix in all_prefixes {
        let base_hash = base_map.get(prefix).copied();
        let a_hash = a_map.get(prefix).copied();
        let b_hash = b_map.get(prefix).copied();

        if a_hash.is_none() && b_hash.is_none() {
            continue;
        }
        if a_hash == base_hash || b_hash == base_hash {
            let selected = if a_hash == base_hash { b_hash } else { a_hash };
            if let Some(hash) = selected {
                auto_resolved += usize::from(selected != base_hash);
                merged_entries.extend(load_all_entries(store, hash).await?);
            }
            continue;
        }

        // A deleted prefix is an empty side of a divergent merge. Resolve
        // its files individually so an edit cannot resurrect untouched siblings.
        let base_entries = load_prefix(store, base_hash).await?;
        let a_entries = load_prefix(store, a_hash).await?;
        let b_entries = load_prefix(store, b_hash).await?;
        merge_entry_lists(
            &base_entries,
            &a_entries,
            &b_entries,
            &mut merged_entries,
            &mut file_conflicts,
            &mut auto_resolved,
            &mut text_todo,
        );
    }

    let text_merged =
        resolve_text_merges(content, text_todo, &mut merged_entries, &mut file_conflicts).await?;

    // Build a new tree from merged entries.
    let mut all_entries = merged_entries;
    all_entries.sort(); // FileEntry implements Ord by path
    all_entries.dedup_by(|a, b| a.path == b.path); // remove any duplicates from same prefix appearing twice
    let new_root = crate::tree::build_tree(store, all_entries, &side_a.vault_id, "server").await?;

    // Set parent hash to side_a's hash (the server's current root before merge).
    let merged_root = RootNode {
        parent_hash: Some(side_a.hash()),
        ..new_root
    };

    Ok(MergeResult {
        new_root: merged_root,
        file_conflicts,
        auto_resolved_count: auto_resolved,
        text_merged_count: text_merged,
    })
}

async fn load_prefix<S: ChunkStore>(
    store: &S,
    hash: Option<&FileHash>,
) -> Result<Vec<FileEntry>, ChunkError> {
    match hash {
        Some(hash) => load_all_entries(store, hash).await,
        None => Ok(Vec::new()),
    }
}

/// Three-way merge across the Tree v1/v2 migration boundary. Pure v1 keeps
/// the optimized prefix-aware implementation above. Any pair involving v2 is
/// flattened through strict versioned loaders, merged by the same canonical
/// file-state resolver and rebuilt in the current server root's version
/// (`side_a`). This makes history/rollback coexistence correct before a later
/// range-aware three-way merge optimizes the rare divergent path.
pub async fn merge_versioned_trees<S: ChunkStore, C: ContentStore>(
    store: &S,
    content: &C,
    base: &VersionedRoot,
    side_a: &VersionedRoot,
    side_b: &VersionedRoot,
) -> Result<VersionedMergeResult, ChunkError> {
    if let (VersionedRoot::V1(base), VersionedRoot::V1(side_a), VersionedRoot::V1(side_b)) =
        (base, side_a, side_b)
    {
        let result = merge_trees(store, content, base, side_a, side_b).await?;
        return Ok(VersionedMergeResult {
            new_root: VersionedRoot::V1(result.new_root),
            file_conflicts: result.file_conflicts,
            auto_resolved_count: result.auto_resolved_count,
            text_merged_count: result.text_merged_count,
        });
    }

    let base_entries = crate::versioned_root::load_all_entries(store, base).await?;
    let a_entries = crate::versioned_root::load_all_entries(store, side_a).await?;
    let b_entries = crate::versioned_root::load_all_entries(store, side_b).await?;
    let mut merged_entries = Vec::with_capacity(a_entries.len().max(b_entries.len()));
    let mut file_conflicts = Vec::new();
    let mut auto_resolved = 0usize;
    let mut text_todo = Vec::new();
    merge_entry_lists(
        &base_entries,
        &a_entries,
        &b_entries,
        &mut merged_entries,
        &mut file_conflicts,
        &mut auto_resolved,
        &mut text_todo,
    );

    let text_merged =
        resolve_text_merges(content, text_todo, &mut merged_entries, &mut file_conflicts).await?;

    merged_entries.sort();
    merged_entries.dedup_by(|left, right| left.path == right.path);
    let mut new_root = crate::versioned_root::build_root(
        store,
        merged_entries,
        side_a.version(),
        side_a.vault_id(),
        "server",
    )
    .await?;
    new_root.set_history_metadata(0, Some(side_a.hash()), "server");
    Ok(VersionedMergeResult {
        new_root,
        file_conflicts,
        auto_resolved_count: auto_resolved,
        text_merged_count: text_merged,
    })
}

async fn resolve_text_merges<C: ContentStore>(
    content: &C,
    todos: Vec<TextMergeTodo>,
    merged_entries: &mut [FileEntry],
    conflicts: &mut Vec<FileConflict>,
) -> Result<usize, ChunkError> {
    let mut count = 0;
    for todo in todos {
        let entry = &mut merged_entries[todo.merged_index];
        if try_text_merge(content, &todo, entry).await? {
            count += 1;
        } else {
            conflicts.push(FileConflict {
                path: entry.path.clone(),
                base_hash: todo.base_hash,
                side_a_hash: entry.hash,
                side_b_hash: todo.b_hash,
            });
        }
    }
    Ok(count)
}

/// Updates the provisional entry on a successful line merge. Ineligible inputs
/// and overlapping edits leave it intact so the caller can record a conflict.
async fn try_text_merge<C: ContentStore>(
    content: &C,
    todo: &TextMergeTodo,
    entry: &mut FileEntry,
) -> Result<bool, ChunkError> {
    // v1 gates: text extensions only, small-blob storage only (files at or
    // above FILE_CHUNK_THRESHOLD live as FastCDC manifests, not blobs).
    if !is_text_file(&entry.path)
        || should_chunk(todo.base_size)
        || should_chunk(entry.size_bytes)
        || should_chunk(todo.b_size)
    {
        return Ok(false);
    }

    // A missing blob is not an error — just fall back to the conflict path.
    let optional = |result| match result {
        Ok(bytes) => Ok(Some(bytes)),
        Err(ChunkError::NotFound(_)) => Ok(None),
        Err(error) => Err(error),
    };
    let (base_bytes, a_bytes, b_bytes) = match (
        optional(content.get(&todo.base_hash).await)?,
        optional(content.get(&entry.hash).await)?,
        optional(content.get(&todo.b_hash).await)?,
    ) {
        (Some(base), Some(a), Some(b)) => (base, a, b),
        _ => return Ok(false),
    };

    let merged = match three_way_text_merge(&base_bytes, &a_bytes, &b_bytes) {
        TextMergeResult::Merged { content } => content,
        TextMergeResult::Overlap => return Ok(false),
    };

    // A merged blob crossing the chunking threshold would be stored where
    // pullers won't look (clients fetch blob-vs-manifest by size).
    if should_chunk(merged.len() as u64) {
        return Ok(false);
    }

    let merged_hash = hash_bytes(&merged);
    let merged_len = merged.len() as u64;
    if merged_hash != entry.hash {
        content.put(merged_hash, merged).await?;
    }

    entry.hash = merged_hash;
    entry.size_bytes = merged_len;
    // max(a, b): deterministic (the root hash covers mtime, so a retried
    // merge must reproduce the same root) and never behind side A (the
    // guard's mtime-regression tripwire must not see merges as reverts).
    entry.mtime_ms = entry.mtime_ms.max(todo.b_mtime);
    Ok(true)
}

/// Merge two entry lists against a common base.
///
/// Stays synchronous and store-free on purpose: modify-modify cases are not
/// decided here — they are pushed onto `text_todo` (with side A provisionally
/// in `merged`) for the content-level post-pass in `merge_trees`.
fn merge_entry_lists(
    base_entries: &[FileEntry],
    a_entries: &[FileEntry],
    b_entries: &[FileEntry],
    merged: &mut Vec<FileEntry>,
    conflicts: &mut Vec<FileConflict>,
    auto_resolved: &mut usize,
    text_todo: &mut Vec<TextMergeTodo>,
) {
    let mut base = base_entries.iter().peekable();
    let mut a = a_entries.iter().peekable();
    let mut b = b_entries.iter().peekable();
    while let Some(path) = [base.peek().copied(), a.peek().copied(), b.peek().copied()]
        .into_iter()
        .flatten()
        .map(|entry| entry.path.as_str())
        .min()
    {
        let base_entry = base.next_if(|entry| entry.path == path);
        let a_entry = a.next_if(|entry| entry.path == path);
        let b_entry = b.next_if(|entry| entry.path == path);

        match (base_entry, a_entry, b_entry) {
            (Some(base), Some(a), Some(b)) if a.hash == base.hash && b.hash == base.hash => {
                merged.push(base.clone());
            }
            (Some(base), Some(a), Some(b)) if b.hash == base.hash && a.hash != base.hash => {
                *auto_resolved += 1;
                merged.push(a.clone());
            }
            (Some(base), Some(a), Some(b)) if a.hash == base.hash && b.hash != base.hash => {
                *auto_resolved += 1;
                merged.push(b.clone());
            }
            (Some(base), Some(a), Some(b)) if a.hash != base.hash && b.hash != base.hash => {
                // Side A goes in provisionally; the text-merge post-pass in
                // merge_trees either replaces it with a line-merged version
                // or records the conflict.
                merged.push(a.clone());
                if a.hash == b.hash {
                    // Both sides made the identical edit — not a conflict.
                    *auto_resolved += 1;
                } else {
                    text_todo.push(TextMergeTodo {
                        merged_index: merged.len() - 1,
                        base_hash: base.hash,
                        base_size: base.size_bytes,
                        b_hash: b.hash,
                        b_size: b.size_bytes,
                        b_mtime: b.mtime_ms,
                    });
                }
            }
            (None, Some(a), None) => {
                *auto_resolved += 1;
                merged.push(a.clone());
            }
            (None, None, Some(b)) => {
                *auto_resolved += 1;
                merged.push(b.clone());
            }
            (None, Some(a), Some(b)) => {
                merged.push(a.clone());
                if a.hash != b.hash {
                    conflicts.push(FileConflict {
                        path: path.to_string(),
                        base_hash: crate::hash::ZERO_HASH,
                        side_a_hash: a.hash,
                        side_b_hash: b.hash,
                    });
                }
            }
            (Some(base), None, Some(b)) if b.hash == base.hash => {}
            (Some(_base), None, Some(b)) => {
                *auto_resolved += 1;
                merged.push(b.clone());
            }
            (Some(base), Some(a), None) if a.hash == base.hash => {}
            (Some(_base), Some(a), None) => {
                *auto_resolved += 1;
                merged.push(a.clone());
            }
            (Some(_), None, None) | (None, None, None) => {}
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::FileEntry;
    use crate::content_store::MemoryContentStore;
    use crate::fastcdc_chunker::FILE_CHUNK_THRESHOLD;
    use crate::hash::hash_bytes;
    use crate::store::MemoryChunkStore;
    use crate::tree::build_tree;

    fn make_entry(path: &str, content: &str) -> FileEntry {
        FileEntry::new(path.into(), hash_bytes(content.as_bytes()), 1000, 100)
    }

    #[tokio::test]
    async fn merge_non_conflicting() {
        let store = MemoryChunkStore::new();
        let content = MemoryContentStore::new();

        // Base: a.md and b.md.
        let base_entries = vec![
            make_entry("notes/a.md", "aaa"),
            make_entry("notes/b.md", "bbb"),
        ];
        let base = build_tree(&store, base_entries, "v", "d").await.unwrap();

        // Side A: modified a.md.
        let a_entries = vec![
            make_entry("notes/a.md", "aaa-modified"),
            make_entry("notes/b.md", "bbb"),
        ];
        let side_a = build_tree(&store, a_entries, "v", "d").await.unwrap();

        // Side B: added c.md.
        let b_entries = vec![
            make_entry("notes/a.md", "aaa"),
            make_entry("notes/b.md", "bbb"),
            make_entry("notes/c.md", "ccc"),
        ];
        let side_b = build_tree(&store, b_entries, "v", "d").await.unwrap();

        let result = merge_trees(&store, &content, &base, &side_a, &side_b)
            .await
            .unwrap();

        assert!(result.file_conflicts.is_empty(), "expected no conflicts");
        assert_eq!(result.new_root.total_files, 3);
    }

    #[tokio::test]
    async fn merge_with_conflict() {
        let store = MemoryChunkStore::new();
        let content = MemoryContentStore::new();

        let base_entries = vec![make_entry("notes/a.md", "base")];
        let base = build_tree(&store, base_entries, "v", "d").await.unwrap();

        let a_entries = vec![make_entry("notes/a.md", "side-a")];
        let side_a = build_tree(&store, a_entries, "v", "d").await.unwrap();

        let b_entries = vec![make_entry("notes/a.md", "side-b")];
        let side_b = build_tree(&store, b_entries, "v", "d").await.unwrap();

        let result = merge_trees(&store, &content, &base, &side_a, &side_b)
            .await
            .unwrap();

        assert_eq!(result.file_conflicts.len(), 1);
        assert_eq!(result.file_conflicts[0].path, "notes/a.md");
    }

    #[tokio::test]
    async fn merge_identical_sides_no_changes() {
        let store = MemoryChunkStore::new();
        let content = MemoryContentStore::new();
        let entries = vec![make_entry("notes/a.md", "aaa")];
        let root = build_tree(&store, entries, "v", "d").await.unwrap();
        let result = merge_trees(&store, &content, &root, &root, &root)
            .await
            .unwrap();
        assert!(result.file_conflicts.is_empty());
        assert_eq!(result.auto_resolved_count, 0);
    }

    #[tokio::test]
    async fn merge_new_dir_in_a_only() {
        let store = MemoryChunkStore::new();
        let content = MemoryContentStore::new();
        let base = build_tree(&store, vec![make_entry("a.md", "x")], "v", "d")
            .await
            .unwrap();
        let side_a = build_tree(
            &store,
            vec![make_entry("a.md", "x"), make_entry("photos/p.png", "img")],
            "v",
            "d",
        )
        .await
        .unwrap();
        let side_b = base.clone();
        let result = merge_trees(&store, &content, &base, &side_a, &side_b)
            .await
            .unwrap();
        assert!(result.file_conflicts.is_empty());
        assert_eq!(result.new_root.total_files, 2);
    }

    #[tokio::test]
    async fn merge_new_dir_in_b_only() {
        let store = MemoryChunkStore::new();
        let content = MemoryContentStore::new();
        let base = build_tree(&store, vec![make_entry("a.md", "x")], "v", "d")
            .await
            .unwrap();
        let side_a = base.clone();
        let side_b = build_tree(
            &store,
            vec![make_entry("a.md", "x"), make_entry("photos/p.png", "img")],
            "v",
            "d",
        )
        .await
        .unwrap();
        let result = merge_trees(&store, &content, &base, &side_a, &side_b)
            .await
            .unwrap();
        assert!(result.file_conflicts.is_empty());
        assert_eq!(result.new_root.total_files, 2);
    }

    #[tokio::test]
    async fn merge_both_sides_add_same_file_no_conflict() {
        let store = MemoryChunkStore::new();
        let content = MemoryContentStore::new();
        let base = build_tree(&store, vec![make_entry("a.md", "x")], "v", "d")
            .await
            .unwrap();
        let side_a = build_tree(
            &store,
            vec![make_entry("a.md", "x"), make_entry("notes/c.md", "ccc")],
            "v",
            "d",
        )
        .await
        .unwrap();
        let side_b = build_tree(
            &store,
            vec![make_entry("a.md", "x"), make_entry("notes/c.md", "ccc")],
            "v",
            "d",
        )
        .await
        .unwrap();
        let result = merge_trees(&store, &content, &base, &side_a, &side_b)
            .await
            .unwrap();
        // Same content added on both sides — must not flag a conflict.
        assert!(
            result.file_conflicts.is_empty(),
            "{:?}",
            result.file_conflicts
        );
        assert_eq!(result.new_root.total_files, 2);
    }

    #[tokio::test]
    async fn merge_both_sides_add_different_content_flags_conflict() {
        let store = MemoryChunkStore::new();
        let content = MemoryContentStore::new();
        let base = build_tree(&store, vec![make_entry("a.md", "x")], "v", "d")
            .await
            .unwrap();
        let side_a = build_tree(
            &store,
            vec![make_entry("a.md", "x"), make_entry("notes/c.md", "ccc-A")],
            "v",
            "d",
        )
        .await
        .unwrap();
        let side_b = build_tree(
            &store,
            vec![make_entry("a.md", "x"), make_entry("notes/c.md", "ccc-B")],
            "v",
            "d",
        )
        .await
        .unwrap();
        let result = merge_trees(&store, &content, &base, &side_a, &side_b)
            .await
            .unwrap();
        assert_eq!(result.file_conflicts.len(), 1);
        assert_eq!(result.file_conflicts[0].path, "notes/c.md");
        // base_hash for an add-add conflict is ZERO_HASH (no common ancestor file).
        assert_eq!(result.file_conflicts[0].base_hash, crate::hash::ZERO_HASH);
    }

    #[tokio::test]
    async fn merge_result_root_stays_out_of_the_byte_addressed_chunk_store() {
        // Root hashes intentionally exclude history metadata, unlike leaf and
        // internal-node addresses. The caller persists the returned root in
        // its per-vault history rather than mixing it into the byte CAS.
        let store = MemoryChunkStore::new();
        let content = MemoryContentStore::new();
        let base = build_tree(&store, vec![make_entry("a.md", "x")], "v", "d")
            .await
            .unwrap();
        let side_a = build_tree(&store, vec![make_entry("a.md", "y")], "v", "d")
            .await
            .unwrap();
        let result = merge_trees(&store, &content, &base, &side_a, &base)
            .await
            .unwrap();
        let merged_hash = result.new_root.hash();
        assert!(matches!(
            store.get(&merged_hash).await.unwrap_err(),
            crate::chunk::ChunkError::NotFound(_),
        ));
    }

    #[tokio::test]
    async fn merge_deletion_and_change() {
        let store = MemoryChunkStore::new();
        let content = MemoryContentStore::new();

        let base_entries = vec![
            make_entry("notes/a.md", "aaa"),
            make_entry("notes/b.md", "bbb"),
        ];
        let base = build_tree(&store, base_entries, "v", "d").await.unwrap();

        // A deletes b.md.
        let a_entries = vec![make_entry("notes/a.md", "aaa")];
        let side_a = build_tree(&store, a_entries, "v", "d").await.unwrap();

        // B modifies b.md.
        let b_entries = vec![
            make_entry("notes/a.md", "aaa"),
            make_entry("notes/b.md", "bbb-modified"),
        ];
        let side_b = build_tree(&store, b_entries, "v", "d").await.unwrap();

        let result = merge_trees(&store, &content, &base, &side_a, &side_b)
            .await
            .unwrap();

        // Change should win over delete.
        assert_eq!(result.new_root.total_files, 2);
    }

    // --- Content-level text merge -----------------------------------------

    /// Entry whose hash matches `content`, registered in the content store.
    async fn seeded_entry(
        content_store: &MemoryContentStore,
        path: &str,
        content: &str,
        mtime: u64,
    ) -> FileEntry {
        let bytes = content.as_bytes().to_vec();
        let hash = hash_bytes(&bytes);
        content_store.put(hash, bytes).await.unwrap();
        FileEntry::new(path.into(), hash, mtime, content.len() as u64)
    }

    #[tokio::test]
    async fn text_merge_nonoverlapping_same_file() {
        let store = MemoryChunkStore::new();
        let content = MemoryContentStore::new();

        let base_txt = "line1\nline2\nline3\nline4\nline5\n";
        let a_txt = "line1\nA-EDIT\nline3\nline4\nline5\n"; // A edits line 2
        let b_txt = "line1\nline2\nline3\nline4\nB-EDIT\n"; // B edits line 5

        let base_e = seeded_entry(&content, "notes/a.md", base_txt, 1000).await;
        let a_e = seeded_entry(&content, "notes/a.md", a_txt, 2000).await;
        let b_e = seeded_entry(&content, "notes/a.md", b_txt, 3000).await;

        let base = build_tree(&store, vec![base_e], "v", "d").await.unwrap();
        let side_a = build_tree(&store, vec![a_e], "v", "d").await.unwrap();
        let side_b = build_tree(&store, vec![b_e], "v", "d").await.unwrap();

        let result = merge_trees(&store, &content, &base, &side_a, &side_b)
            .await
            .unwrap();

        assert!(
            result.file_conflicts.is_empty(),
            "non-overlapping edits must auto-merge: {:?}",
            result.file_conflicts
        );
        assert_eq!(result.text_merged_count, 1);

        // The merged entry must point at the exact merged bytes, retrievable
        // from the content store, with mtime = max(a, b).
        let expected = "line1\nA-EDIT\nline3\nline4\nB-EDIT\n";
        let entries = load_all_entries(&store, &result.new_root.children[0].1)
            .await
            .unwrap();
        let merged_entry = entries.iter().find(|e| e.path == "notes/a.md").unwrap();
        assert_eq!(merged_entry.hash, hash_bytes(expected.as_bytes()));
        assert_eq!(merged_entry.mtime_ms, 3000);
        assert_eq!(merged_entry.size_bytes, expected.len() as u64);
        let stored = content.get(&merged_entry.hash).await.unwrap();
        assert_eq!(stored, expected.as_bytes());
    }

    #[tokio::test]
    async fn text_merge_overlap_falls_back_to_conflict() {
        let store = MemoryChunkStore::new();
        let content = MemoryContentStore::new();

        let base_e = seeded_entry(&content, "notes/a.md", "same line\n", 1000).await;
        let a_e = seeded_entry(&content, "notes/a.md", "edited by A\n", 2000).await;
        let b_e = seeded_entry(&content, "notes/a.md", "edited by B\n", 3000).await;

        let base = build_tree(&store, vec![base_e], "v", "d").await.unwrap();
        let side_a = build_tree(&store, vec![a_e.clone()], "v", "d")
            .await
            .unwrap();
        let side_b = build_tree(&store, vec![b_e], "v", "d").await.unwrap();

        let result = merge_trees(&store, &content, &base, &side_a, &side_b)
            .await
            .unwrap();

        assert_eq!(result.text_merged_count, 0);
        assert_eq!(result.file_conflicts.len(), 1);
        // Side A stays in the tree.
        let entries = load_all_entries(&store, &result.new_root.children[0].1)
            .await
            .unwrap();
        assert_eq!(entries[0].hash, a_e.hash);
    }

    #[tokio::test]
    async fn text_merge_missing_content_falls_back() {
        let store = MemoryChunkStore::new();
        let content = MemoryContentStore::new(); // deliberately EMPTY

        let base = build_tree(&store, vec![make_entry("notes/a.md", "base")], "v", "d")
            .await
            .unwrap();
        let side_a = build_tree(&store, vec![make_entry("notes/a.md", "side-a")], "v", "d")
            .await
            .unwrap();
        let side_b = build_tree(&store, vec![make_entry("notes/a.md", "side-b")], "v", "d")
            .await
            .unwrap();

        let result = merge_trees(&store, &content, &base, &side_a, &side_b)
            .await
            .unwrap();

        assert_eq!(result.text_merged_count, 0);
        assert_eq!(result.file_conflicts.len(), 1, "graceful conflict fallback");
    }

    #[tokio::test]
    async fn text_merge_non_utf8_falls_back() {
        let store = MemoryChunkStore::new();
        let content = MemoryContentStore::new();

        // .md extension but invalid UTF-8 bytes — merging via from_utf8_lossy
        // would silently corrupt; must fall back to conflict.
        let seed_raw = |bytes: Vec<u8>, mtime: u64| {
            let hash = hash_bytes(&bytes);
            let len = bytes.len() as u64;
            (bytes, hash, mtime, len)
        };
        let (b0, h0, m0, l0) = seed_raw(vec![0xFF, 0x00, 0x01], 1000);
        let (b1, h1, m1, l1) = seed_raw(vec![0xFF, 0x00, 0x02], 2000);
        let (b2, h2, m2, l2) = seed_raw(vec![0xFF, 0x00, 0x03], 3000);
        content.put(h0, b0).await.unwrap();
        content.put(h1, b1).await.unwrap();
        content.put(h2, b2).await.unwrap();

        let mk = |h, m, l| FileEntry::new("notes/a.md".into(), h, m, l);
        let base = build_tree(&store, vec![mk(h0, m0, l0)], "v", "d")
            .await
            .unwrap();
        let side_a = build_tree(&store, vec![mk(h1, m1, l1)], "v", "d")
            .await
            .unwrap();
        let side_b = build_tree(&store, vec![mk(h2, m2, l2)], "v", "d")
            .await
            .unwrap();

        let result = merge_trees(&store, &content, &base, &side_a, &side_b)
            .await
            .unwrap();

        assert_eq!(result.text_merged_count, 0);
        assert_eq!(result.file_conflicts.len(), 1);
    }

    #[tokio::test]
    async fn text_merge_binary_ext_and_size_gated() {
        let store = MemoryChunkStore::new();
        let content = MemoryContentStore::new();

        // Binary extension — never text-merged even with valid UTF-8 content.
        let base_e = seeded_entry(&content, "img/pic.png", "base", 1000).await;
        let a_e = seeded_entry(&content, "img/pic.png", "side-a", 2000).await;
        let b_e = seeded_entry(&content, "img/pic.png", "side-b", 3000).await;
        let base = build_tree(&store, vec![base_e], "v", "d").await.unwrap();
        let side_a = build_tree(&store, vec![a_e], "v", "d").await.unwrap();
        let side_b = build_tree(&store, vec![b_e], "v", "d").await.unwrap();
        let result = merge_trees(&store, &content, &base, &side_a, &side_b)
            .await
            .unwrap();
        assert_eq!(result.text_merged_count, 0);
        assert_eq!(result.file_conflicts.len(), 1);

        // Size gate: entries at/above the chunk threshold are not eligible
        // even for a text extension (their bytes live as manifests, not blobs).
        let store2 = MemoryChunkStore::new();
        let content2 = MemoryContentStore::new();
        let big = |c: &str, m: u64| {
            let hash = hash_bytes(c.as_bytes());
            FileEntry::new("notes/big.md".into(), hash, m, FILE_CHUNK_THRESHOLD)
        };
        let base = build_tree(&store2, vec![big("base", 1000)], "v", "d")
            .await
            .unwrap();
        let side_a = build_tree(&store2, vec![big("side-a", 2000)], "v", "d")
            .await
            .unwrap();
        let side_b = build_tree(&store2, vec![big("side-b", 3000)], "v", "d")
            .await
            .unwrap();
        let result = merge_trees(&store2, &content2, &base, &side_a, &side_b)
            .await
            .unwrap();
        assert_eq!(result.text_merged_count, 0);
        assert_eq!(result.file_conflicts.len(), 1);
    }

    #[tokio::test]
    async fn chunked_base_is_never_opened_as_a_small_blob() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoryChunkStore::new();
        let content = crate::content_store::DiskContentStore::new(dir.path());
        let base_hash = hash_bytes(&vec![b'x'; FILE_CHUNK_THRESHOLD as usize]);
        let base_entry = FileEntry::new("a.md".into(), base_hash, 1, FILE_CHUNK_THRESHOLD);
        // A directory at the unused blob address makes any attempted read fail.
        let hex = crate::hash::hash_to_hex(&base_hash);
        std::fs::create_dir_all(dir.path().join(&hex[..2]).join(&hex[2..])).unwrap();
        let base = build_tree(&store, vec![base_entry], "v", "d")
            .await
            .unwrap();
        let mut sides = Vec::new();
        for bytes in [b"a", b"b"] {
            let hash = hash_bytes(bytes);
            content.put(hash, bytes.to_vec()).await.unwrap();
            let entry = FileEntry::new("a.md".into(), hash, 2, 1);
            sides.push(build_tree(&store, vec![entry], "v", "d").await.unwrap());
        }
        let result = merge_trees(&store, &content, &base, &sides[0], &sides[1])
            .await
            .unwrap();
        assert_eq!(result.text_merged_count, 0);
        assert_eq!(result.file_conflicts.len(), 1);
        assert_eq!(result.file_conflicts[0].base_hash, base_hash);
    }

    #[tokio::test]
    async fn every_mixed_version_merge_preserves_semantics_and_current_format() {
        let store = MemoryChunkStore::new();
        let content = MemoryContentStore::new();
        let base_entries = vec![
            make_entry("a.md", "a0"),
            make_entry("b.md", "b0"),
            make_entry("dir/edit", "old"),
            make_entry("dir/drop", "unchanged"),
        ];
        let a_entries = vec![make_entry("a.md", "a1"), make_entry("b.md", "b0")];
        let b_entries = vec![
            make_entry("a.md", "a0"),
            make_entry("b.md", "b0"),
            make_entry("c.md", "c1"),
            make_entry("dir/edit", "new"),
            make_entry("dir/drop", "unchanged"),
        ];
        let expected = vec![
            make_entry("a.md", "a1"),
            make_entry("b.md", "b0"),
            make_entry("c.md", "c1"),
            make_entry("dir/edit", "new"),
        ];
        let mut bases = Vec::new();
        let mut sides_a = Vec::new();
        let mut sides_b = Vec::new();
        for version in [
            crate::versioned_root::TREE_V1,
            crate::versioned_root::TREE_V2,
        ] {
            bases.push(
                crate::versioned_root::build_root(
                    &store,
                    base_entries.clone(),
                    version,
                    "vault",
                    "base",
                )
                .await
                .unwrap(),
            );
            sides_a.push(
                crate::versioned_root::build_root(&store, a_entries.clone(), version, "vault", "a")
                    .await
                    .unwrap(),
            );
            sides_b.push(
                crate::versioned_root::build_root(&store, b_entries.clone(), version, "vault", "b")
                    .await
                    .unwrap(),
            );
        }

        for base in &bases {
            for side_a in &sides_a {
                for side_b in &sides_b {
                    let result = merge_versioned_trees(&store, &content, base, side_a, side_b)
                        .await
                        .unwrap();
                    assert_eq!(result.new_root.version(), side_a.version());
                    assert_eq!(result.new_root.parent_hash(), Some(side_a.hash()));
                    assert_eq!(result.file_conflicts.len(), 0);
                    assert_eq!(
                        crate::versioned_root::load_all_entries(&store, &result.new_root)
                            .await
                            .unwrap(),
                        expected,
                        "base {} / current {} / incoming {}",
                        base.version(),
                        side_a.version(),
                        side_b.version(),
                    );
                }
            }
        }
    }
}
