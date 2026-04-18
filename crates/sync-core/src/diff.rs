use crate::chunk::{ChunkError, RootNode};
use crate::hash::FileHash;
use crate::store::ChunkStore;
use crate::tree::load_all_entries;

/// A single file-level change between two roots.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum FileDelta {
    Added {
        path: String,
        hash: FileHash,
        size: u64,
    },
    Modified {
        path: String,
        hash: FileHash,
        size: u64,
    },
    Deleted {
        path: String,
    },
    Renamed {
        path: String,
        old_path: String,
        hash: FileHash,
    },
}

/// Compute explicit file-level deltas between two roots.
/// Walks both trees, compares leaf entries, returns a list of changes.
/// O(changed subtrees * entries per chunk), not O(total files).
pub async fn compute_deltas<S: ChunkStore>(
    store: &S,
    from_root: &RootNode,
    to_root: &RootNode,
) -> Result<Vec<FileDelta>, ChunkError> {
    if from_root.hash() == to_root.hash() {
        return Ok(vec![]);
    }

    // root.children is Vec<(String, FileHash)> sorted by prefix (BTreeMap order preserved).
    // Two-pointer merge over sorted prefix lists — no HashMap, no HashSet.
    let from_children = &from_root.children;
    let to_children = &to_root.children;

    let mut raw_deltas = Vec::new();
    let mut i = 0;
    let mut j = 0;

    while i < from_children.len() && j < to_children.len() {
        let (from_prefix, from_hash) = &from_children[i];
        let (to_prefix, to_hash) = &to_children[j];

        match from_prefix.cmp(to_prefix) {
            std::cmp::Ordering::Equal => {
                if from_hash != to_hash {
                    let from_entries = load_all_entries(store, from_hash).await?;
                    let to_entries = load_all_entries(store, to_hash).await?;
                    diff_entries(&from_entries, &to_entries, &mut raw_deltas);
                }
                i += 1;
                j += 1;
            }
            std::cmp::Ordering::Less => {
                // prefix exists only in from — entire directory deleted
                let entries = load_all_entries(store, from_hash).await?;
                for e in entries {
                    raw_deltas.push(FileDelta::Deleted { path: e.path });
                }
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                // prefix exists only in to — entire directory added
                let entries = load_all_entries(store, to_hash).await?;
                for e in entries {
                    raw_deltas.push(FileDelta::Added {
                        path: e.path,
                        hash: e.hash,
                        size: e.size_bytes,
                    });
                }
                j += 1;
            }
        }
    }

    // Remaining from_children = entire directories deleted.
    for (_, hash) in &from_children[i..] {
        let entries = load_all_entries(store, hash).await?;
        for e in entries {
            raw_deltas.push(FileDelta::Deleted { path: e.path });
        }
    }

    // Remaining to_children = entire directories added.
    for (_, hash) in &to_children[j..] {
        let entries = load_all_entries(store, hash).await?;
        for e in entries {
            raw_deltas.push(FileDelta::Added {
                path: e.path,
                hash: e.hash,
                size: e.size_bytes,
            });
        }
    }

    let deltas = detect_renames(raw_deltas);
    Ok(deltas)
}

/// Diff two sorted entry lists and produce raw deltas.
fn diff_entries(
    from: &[crate::chunk::FileEntry],
    to: &[crate::chunk::FileEntry],
    deltas: &mut Vec<FileDelta>,
) {
    // Both slices are sorted by path (guaranteed by LeafChunk::new → entries.sort()).
    // Two-pointer merge: O(n+m), zero allocations.
    let mut i = 0;
    let mut j = 0;

    while i < from.len() && j < to.len() {
        let ord = from[i].path.cmp(&to[j].path);
        match ord {
            std::cmp::Ordering::Equal => {
                if from[i].hash != to[j].hash {
                    deltas.push(FileDelta::Modified {
                        path: to[j].path.clone(),
                        hash: to[j].hash,
                        size: to[j].size_bytes,
                    });
                }
                i += 1;
                j += 1;
            }
            std::cmp::Ordering::Less => {
                deltas.push(FileDelta::Deleted {
                    path: from[i].path.clone(),
                });
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                deltas.push(FileDelta::Added {
                    path: to[j].path.clone(),
                    hash: to[j].hash,
                    size: to[j].size_bytes,
                });
                j += 1;
            }
        }
    }
    for entry in &from[i..] {
        deltas.push(FileDelta::Deleted {
            path: entry.path.clone(),
        });
    }
    for entry in &to[j..] {
        deltas.push(FileDelta::Added {
            path: entry.path.clone(),
            hash: entry.hash,
            size: entry.size_bytes,
        });
    }
}

/// Detect renames by matching content hashes between Deleted and Added entries.
/// TODO: requires adding hash to FileDelta::Deleted or a separate store pass.
fn detect_renames(deltas: Vec<FileDelta>) -> Vec<FileDelta> {
    deltas
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
    async fn diff_identical_roots() {
        let store = MemoryChunkStore::new();
        let entries = vec![make_entry("a.md", "aaa"), make_entry("b.md", "bbb")];
        let root = build_tree(&store, entries, "v", "d").await.unwrap();

        let deltas = compute_deltas(&store, &root, &root).await.unwrap();
        assert!(deltas.is_empty());
    }

    #[tokio::test]
    async fn diff_added_file() {
        let store = MemoryChunkStore::new();
        let entries1 = vec![make_entry("notes/a.md", "aaa")];
        let root1 = build_tree(&store, entries1, "v", "d").await.unwrap();

        let entries2 = vec![
            make_entry("notes/a.md", "aaa"),
            make_entry("notes/b.md", "bbb"),
        ];
        let root2 = build_tree(&store, entries2, "v", "d").await.unwrap();

        let deltas = compute_deltas(&store, &root1, &root2).await.unwrap();
        assert_eq!(deltas.len(), 1);
        assert!(matches!(&deltas[0], FileDelta::Added { path, .. } if path == "notes/b.md"));
    }

    #[tokio::test]
    async fn diff_deleted_file() {
        let store = MemoryChunkStore::new();
        let entries1 = vec![
            make_entry("notes/a.md", "aaa"),
            make_entry("notes/b.md", "bbb"),
        ];
        let root1 = build_tree(&store, entries1, "v", "d").await.unwrap();

        let entries2 = vec![make_entry("notes/a.md", "aaa")];
        let root2 = build_tree(&store, entries2, "v", "d").await.unwrap();

        let deltas = compute_deltas(&store, &root1, &root2).await.unwrap();
        assert_eq!(deltas.len(), 1);
        assert!(matches!(&deltas[0], FileDelta::Deleted { path } if path == "notes/b.md"));
    }

    #[tokio::test]
    async fn diff_modified_file() {
        let store = MemoryChunkStore::new();
        let entries1 = vec![make_entry("notes/a.md", "old content")];
        let root1 = build_tree(&store, entries1, "v", "d").await.unwrap();

        let entries2 = vec![make_entry("notes/a.md", "new content")];
        let root2 = build_tree(&store, entries2, "v", "d").await.unwrap();

        let deltas = compute_deltas(&store, &root1, &root2).await.unwrap();
        assert_eq!(deltas.len(), 1);
        assert!(matches!(&deltas[0], FileDelta::Modified { path, .. } if path == "notes/a.md"));
    }

    #[tokio::test]
    async fn diff_new_directory() {
        let store = MemoryChunkStore::new();
        let entries1 = vec![make_entry("notes/a.md", "aaa")];
        let root1 = build_tree(&store, entries1, "v", "d").await.unwrap();

        let entries2 = vec![
            make_entry("notes/a.md", "aaa"),
            make_entry("photos/pic.png", "img"),
        ];
        let root2 = build_tree(&store, entries2, "v", "d").await.unwrap();

        let deltas = compute_deltas(&store, &root1, &root2).await.unwrap();
        assert_eq!(deltas.len(), 1);
        assert!(matches!(&deltas[0], FileDelta::Added { path, .. } if path == "photos/pic.png"));
    }
}
