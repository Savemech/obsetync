use crate::chunk::{ChunkError, FileEntry, RootNode};
use crate::conflict::FileConflict;
use crate::hash::FileHash;
use crate::store::ChunkStore;
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
}

/// Three-way merge of two trees that diverged from a common ancestor.
/// Resolves at the tree level. Returns file-level conflicts for D-003 handling.
///
/// This is the server-side merge (D-006): when two devices push competing roots,
/// the server calls this to produce a merged root without rejecting either push.
pub async fn merge_trees<S: ChunkStore>(
    store: &S,
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
    })
}

/// Merge two entry lists against a common base.
fn merge_entry_lists(
    base_entries: &[FileEntry],
    a_entries: &[FileEntry],
    b_entries: &[FileEntry],
    merged: &mut Vec<FileEntry>,
    conflicts: &mut Vec<FileConflict>,
    auto_resolved: &mut usize,
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
                merged.push((*a).clone());
                conflicts.push(FileConflict {
                    path: path.to_string(),
                    base_hash: base.hash,
                    side_a_hash: a.hash,
                    side_b_hash: b.hash,
                });
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
    use crate::hash::hash_bytes;
    use crate::store::MemoryChunkStore;
    use crate::tree::build_tree;

    fn make_entry(path: &str, content: &str) -> FileEntry {
        FileEntry::new(path.into(), hash_bytes(content.as_bytes()), 1000, 100)
    }

    #[tokio::test]
    async fn merge_non_conflicting() {
        let store = MemoryChunkStore::new();

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

        let result = merge_trees(&store, &base, &side_a, &side_b).await.unwrap();

        assert!(result.file_conflicts.is_empty(), "expected no conflicts");
        assert_eq!(result.new_root.total_files, 3);
    }

    #[tokio::test]
    async fn merge_with_conflict() {
        let store = MemoryChunkStore::new();

        let base_entries = vec![make_entry("notes/a.md", "base")];
        let base = build_tree(&store, base_entries, "v", "d").await.unwrap();

        let a_entries = vec![make_entry("notes/a.md", "side-a")];
        let side_a = build_tree(&store, a_entries, "v", "d").await.unwrap();

        let b_entries = vec![make_entry("notes/a.md", "side-b")];
        let side_b = build_tree(&store, b_entries, "v", "d").await.unwrap();

        let result = merge_trees(&store, &base, &side_a, &side_b).await.unwrap();

        assert_eq!(result.file_conflicts.len(), 1);
        assert_eq!(result.file_conflicts[0].path, "notes/a.md");
    }

    #[tokio::test]
    async fn merge_identical_sides_no_changes() {
        let store = MemoryChunkStore::new();
        let entries = vec![make_entry("notes/a.md", "aaa")];
        let root = build_tree(&store, entries, "v", "d").await.unwrap();
        let result = merge_trees(&store, &root, &root, &root).await.unwrap();
        assert!(result.file_conflicts.is_empty());
        assert_eq!(result.auto_resolved_count, 0);
    }

    #[tokio::test]
    async fn merge_new_dir_in_a_only() {
        let store = MemoryChunkStore::new();
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
        let result = merge_trees(&store, &base, &side_a, &side_b).await.unwrap();
        assert!(result.file_conflicts.is_empty());
        assert_eq!(result.new_root.total_files, 2);
    }

    #[tokio::test]
    async fn merge_new_dir_in_b_only() {
        let store = MemoryChunkStore::new();
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
        let result = merge_trees(&store, &base, &side_a, &side_b).await.unwrap();
        assert!(result.file_conflicts.is_empty());
        assert_eq!(result.new_root.total_files, 2);
    }

    #[tokio::test]
    async fn merge_both_sides_add_same_file_no_conflict() {
        let store = MemoryChunkStore::new();
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
        let result = merge_trees(&store, &base, &side_a, &side_b).await.unwrap();
        // Same content added on both sides — must not flag a conflict.
        assert!(result.file_conflicts.is_empty(), "{:?}", result.file_conflicts);
        assert_eq!(result.new_root.total_files, 2);
    }

    #[tokio::test]
    async fn merge_both_sides_add_different_content_flags_conflict() {
        let store = MemoryChunkStore::new();
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
        let result = merge_trees(&store, &base, &side_a, &side_b).await.unwrap();
        assert_eq!(result.file_conflicts.len(), 1);
        assert_eq!(result.file_conflicts[0].path, "notes/c.md");
        // base_hash for an add-add conflict is ZERO_HASH (no common ancestor file).
        assert_eq!(result.file_conflicts[0].base_hash, crate::hash::ZERO_HASH);
    }

    #[tokio::test]
    async fn merge_result_root_is_persisted() {
        // merge_trees stores the merged root in the chunk store before returning.
        let store = MemoryChunkStore::new();
        let base = build_tree(&store, vec![make_entry("a.md", "x")], "v", "d")
            .await
            .unwrap();
        let side_a = build_tree(&store, vec![make_entry("a.md", "y")], "v", "d")
            .await
            .unwrap();
        let result = merge_trees(&store, &base, &side_a, &base).await.unwrap();
        let merged_hash = result.new_root.hash();
        // The merged root bytes must now be retrievable from the store.
        let bytes = store.get(&merged_hash).await.unwrap();
        let decoded = crate::chunk::RootNode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.hash(), merged_hash);
    }

    #[tokio::test]
    async fn merge_deletion_and_change() {
        let store = MemoryChunkStore::new();

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

        let result = merge_trees(&store, &base, &side_a, &side_b).await.unwrap();

        // Change should win over delete.
        assert_eq!(result.new_root.total_files, 2);
    }
}
