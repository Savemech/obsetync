use crate::chunk::{ChunkError, FileEntry, RootNode};
use crate::conflict::{three_way_text_merge, FileConflict, TextMergeResult};
use crate::content_store::ContentStore;
use crate::fastcdc_chunker::FILE_CHUNK_THRESHOLD;
use crate::hash::{hash_bytes, FileHash};
use crate::store::ChunkStore;
use crate::sync_rules::is_text_file;
use crate::tree::load_all_entries;
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

/// A modify-modify candidate deferred to the content-level text-merge
/// post-pass: both sides changed `path` relative to base. Side A's entry is
/// provisionally in the merged list; the post-pass either replaces it with a
/// line-merged version or records a `FileConflict`.
struct TextMergeTodo {
    path: String,
    base_hash: FileHash,
    a: FileEntry,
    b: FileEntry,
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

        match (base_hash, a_hash, b_hash) {
            // Both same as base — no change.
            (Some(bh), Some(ah), Some(bh2)) if ah == bh && bh2 == bh => {
                let entries = load_all_entries(store, bh).await?;
                for e in entries {
                    merged_entries.push(e);
                }
            }
            // Only A changed — take A.
            (Some(bh), Some(ah), Some(bh2)) if bh2 == bh && ah != bh => {
                auto_resolved += 1;
                let entries = load_all_entries(store, ah).await?;
                for e in entries {
                    merged_entries.push(e);
                }
            }
            // Only B changed — take B.
            (Some(bh), Some(ah), Some(bh2)) if ah == bh && bh2 != bh => {
                auto_resolved += 1;
                let entries = load_all_entries(store, bh2).await?;
                for e in entries {
                    merged_entries.push(e);
                }
            }
            // Both changed — recurse into entries for file-level merge.
            (Some(bh), Some(ah), Some(bh2)) if ah != bh && bh2 != bh => {
                let base_entries = load_all_entries(store, bh).await?;
                let a_entries = load_all_entries(store, ah).await?;
                let b_entries = load_all_entries(store, bh2).await?;

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
            // New in A only.
            (None, Some(ah), None) => {
                auto_resolved += 1;
                let entries = load_all_entries(store, ah).await?;
                for e in entries {
                    merged_entries.push(e);
                }
            }
            // New in B only.
            (None, None, Some(bh)) => {
                auto_resolved += 1;
                let entries = load_all_entries(store, bh).await?;
                for e in entries {
                    merged_entries.push(e);
                }
            }
            // New in both A and B — merge entries.
            (None, Some(ah), Some(bh)) => {
                let a_entries = load_all_entries(store, ah).await?;
                let b_entries = load_all_entries(store, bh).await?;
                merge_entry_lists(
                    &[],
                    &a_entries,
                    &b_entries,
                    &mut merged_entries,
                    &mut file_conflicts,
                    &mut auto_resolved,
                    // base = [] here, so the modify-modify arm (which requires a
                    // base entry) never fires — no text todos from this call.
                    &mut text_todo,
                );
            }
            // Deleted by A only (B has it, base has it, A doesn't).
            (Some(_bh), None, Some(bh2)) => {
                // A deleted this prefix. If B didn't change it from base, honor deletion.
                // If B changed it, keep B's version (B wins over deletion).
                if let Some(base_h) = base_hash {
                    if bh2 == base_h {
                        // B unchanged, A deleted — honor deletion.
                    } else {
                        // B changed, A deleted — keep B.
                        auto_resolved += 1;
                        let entries = load_all_entries(store, bh2).await?;
                        for e in entries {
                            merged_entries.push(e);
                        }
                    }
                }
            }
            // Deleted by B only.
            (Some(_bh), Some(ah), None) => {
                if let Some(base_h) = base_hash {
                    if ah == base_h {
                        // A unchanged, B deleted — honor deletion.
                    } else {
                        // A changed, B deleted — keep A.
                        auto_resolved += 1;
                        let entries = load_all_entries(store, ah).await?;
                        for e in entries {
                            merged_entries.push(e);
                        }
                    }
                }
            }
            // Both deleted — stay deleted.
            (Some(_), None, None) => {}
            // Nothing exists anywhere.
            (None, None, None) => {}
            // Catch remaining cases.
            _ => {}
        }
    }

    // --- Content-level text-merge post-pass -------------------------------
    // Every modify-modify candidate got side A provisionally; here we try a
    // real three-way LINE merge so non-overlapping edits from both devices
    // survive in one file. Anything ineligible or overlapping falls back to
    // today's behavior: A stays in the tree + a FileConflict is recorded.
    let mut text_merged = 0usize;
    for todo in &text_todo {
        let resolved = try_text_merge(content, todo, &mut merged_entries).await?;
        if resolved {
            text_merged += 1;
        } else {
            file_conflicts.push(FileConflict {
                path: todo.path.clone(),
                base_hash: todo.base_hash,
                side_a_hash: todo.a.hash,
                side_b_hash: todo.b.hash,
            });
        }
    }

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

    // Store the merged root.
    let root_bytes = merged_root.serialize();
    let root_hash = merged_root.hash();
    store.put(root_hash, root_bytes).await?;

    Ok(MergeResult {
        new_root: merged_root,
        file_conflicts,
        auto_resolved_count: auto_resolved,
        text_merged_count: text_merged,
    })
}

/// Attempt the content-level three-way merge for one modify-modify todo.
/// On success, patches the file's entry in `merged_entries` (hash/size/mtime)
/// and returns true. Any ineligibility — non-text extension, either side or
/// the merged output ≥ the chunking threshold, non-UTF-8 content, a missing
/// blob, or overlapping edits — returns false (caller records the conflict).
async fn try_text_merge<C: ContentStore>(
    content: &C,
    todo: &TextMergeTodo,
    merged_entries: &mut [FileEntry],
) -> Result<bool, ChunkError> {
    // v1 gates: text extensions only, small-blob storage only (files at or
    // above FILE_CHUNK_THRESHOLD live as FastCDC manifests, not blobs).
    if !is_text_file(&todo.path)
        || todo.a.size_bytes >= FILE_CHUNK_THRESHOLD
        || todo.b.size_bytes >= FILE_CHUNK_THRESHOLD
    {
        return Ok(false);
    }

    // A missing blob is not an error — just fall back to the conflict path.
    let (base_bytes, a_bytes, b_bytes) = match (
        content.get(&todo.base_hash).await,
        content.get(&todo.a.hash).await,
        content.get(&todo.b.hash).await,
    ) {
        (Ok(base), Ok(a), Ok(b)) => (base, a, b),
        _ => return Ok(false),
    };

    // three_way_text_merge decodes with from_utf8_lossy — merging non-UTF-8
    // input would silently corrupt bytes. Require strict UTF-8 on all three.
    if std::str::from_utf8(&base_bytes).is_err()
        || std::str::from_utf8(&a_bytes).is_err()
        || std::str::from_utf8(&b_bytes).is_err()
    {
        return Ok(false);
    }

    let merged = match three_way_text_merge(&base_bytes, &a_bytes, &b_bytes) {
        TextMergeResult::Merged { content } => content,
        TextMergeResult::Overlap => return Ok(false),
    };

    // A merged blob crossing the chunking threshold would be stored where
    // pullers won't look (clients fetch blob-vs-manifest by size).
    if merged.len() as u64 >= FILE_CHUNK_THRESHOLD {
        return Ok(false);
    }

    let merged_hash = hash_bytes(&merged);
    let merged_len = merged.len() as u64;
    if merged_hash != todo.a.hash {
        content.put(merged_hash, merged).await?;
    }

    if let Some(entry) = merged_entries.iter_mut().find(|e| e.path == todo.path) {
        entry.hash = merged_hash;
        entry.size_bytes = merged_len;
        // max(a, b): deterministic (the root hash covers mtime, so a retried
        // merge must reproduce the same root) and never behind side A (the
        // guard's mtime-regression tripwire must not see merges as reverts).
        entry.mtime_ms = todo.a.mtime_ms.max(todo.b.mtime_ms);
    }
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
    // All three slices are sorted by path. Three-pointer merge: O(n+m+k), zero map allocations.

    // Collect all unique paths in sorted order using a merge of three sorted iterators.
    let mut paths: Vec<&str> =
        Vec::with_capacity(base_entries.len().max(a_entries.len()).max(b_entries.len()));

    // Merge three sorted slices into a deduplicated sorted path list.
    {
        let mut ii = 0usize;
        let mut ji = 0usize;
        let mut ki = 0usize;
        loop {
            let a_path = a_entries.get(ii).map(|e| e.path.as_str());
            let b_path = b_entries.get(ji).map(|e| e.path.as_str());
            let c_path = base_entries.get(ki).map(|e| e.path.as_str());

            let min = match (a_path, b_path, c_path) {
                (None, None, None) => break,
                (Some(a), None, None) => {
                    ii += 1;
                    a
                }
                (None, Some(b), None) => {
                    ji += 1;
                    b
                }
                (None, None, Some(c)) => {
                    ki += 1;
                    c
                }
                (Some(a), Some(b), None) => match a.cmp(b) {
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal => {
                        if a == b {
                            ji += 1;
                        }
                        ii += 1;
                        a
                    }
                    std::cmp::Ordering::Greater => {
                        ji += 1;
                        b
                    }
                },
                (Some(a), None, Some(c)) => match a.cmp(c) {
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal => {
                        if a == c {
                            ki += 1;
                        }
                        ii += 1;
                        a
                    }
                    std::cmp::Ordering::Greater => {
                        ki += 1;
                        c
                    }
                },
                (None, Some(b), Some(c)) => match b.cmp(c) {
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal => {
                        if b == c {
                            ki += 1;
                        }
                        ji += 1;
                        b
                    }
                    std::cmp::Ordering::Greater => {
                        ki += 1;
                        c
                    }
                },
                (Some(a), Some(b), Some(c)) => {
                    let min3 = a.min(b).min(c);
                    if a == min3 {
                        ii += 1;
                    }
                    if b == min3 {
                        ji += 1;
                    }
                    if c == min3 {
                        ki += 1;
                    }
                    min3
                }
            };

            if paths.last().copied() != Some(min) {
                paths.push(min);
            }
        }
    }

    // Build lookup maps — only for path membership checks, reusing sorted positions.
    // Use sorted binary search instead of HashMap.
    let base_map: HashMap<&str, &FileEntry> =
        base_entries.iter().map(|e| (e.path.as_str(), e)).collect();
    let a_map: HashMap<&str, &FileEntry> = a_entries.iter().map(|e| (e.path.as_str(), e)).collect();
    let b_map: HashMap<&str, &FileEntry> = b_entries.iter().map(|e| (e.path.as_str(), e)).collect();

    for path in paths {
        let base_entry = base_map.get(path);
        let a_entry = a_map.get(path);
        let b_entry = b_map.get(path);

        match (base_entry, a_entry, b_entry) {
            (Some(base), Some(a), Some(b)) if a.hash == base.hash && b.hash == base.hash => {
                merged.push((*base).clone());
            }
            (Some(base), Some(a), Some(b)) if b.hash == base.hash && a.hash != base.hash => {
                *auto_resolved += 1;
                merged.push((*a).clone());
            }
            (Some(base), Some(a), Some(b)) if a.hash == base.hash && b.hash != base.hash => {
                *auto_resolved += 1;
                merged.push((*b).clone());
            }
            (Some(base), Some(a), Some(b)) if a.hash != base.hash && b.hash != base.hash => {
                // Side A goes in provisionally; the text-merge post-pass in
                // merge_trees either replaces it with a line-merged version
                // or records the conflict.
                merged.push((*a).clone());
                if a.hash == b.hash {
                    // Both sides made the identical edit — not a conflict.
                    *auto_resolved += 1;
                } else {
                    text_todo.push(TextMergeTodo {
                        path: path.to_string(),
                        base_hash: base.hash,
                        a: (*a).clone(),
                        b: (*b).clone(),
                    });
                }
            }
            (None, Some(a), None) => {
                *auto_resolved += 1;
                merged.push((*a).clone());
            }
            (None, None, Some(b)) => {
                *auto_resolved += 1;
                merged.push((*b).clone());
            }
            (None, Some(a), Some(b)) => {
                merged.push((*a).clone());
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
                merged.push((*b).clone());
            }
            (Some(base), Some(a), None) if a.hash == base.hash => {}
            (Some(_base), Some(a), None) => {
                *auto_resolved += 1;
                merged.push((*a).clone());
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
    async fn merge_result_root_is_persisted() {
        // merge_trees stores the merged root in the chunk store before returning.
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
        // The merged root bytes must now be retrievable from the store.
        let bytes = store.get(&merged_hash).await.unwrap();
        let decoded = crate::chunk::RootNode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.hash(), merged_hash);
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
}
