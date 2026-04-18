use crate::chunk::{ChunkError, FileEntry, InternalNode, LeafChunk, RootNode};
use crate::hash::FileHash;
use crate::store::ChunkStore;
use std::collections::BTreeMap;

/// Current time as Unix milliseconds.
/// Uses js_sys::Date::now() in WASM (SystemTime panics on wasm32-unknown-unknown).
#[cfg(feature = "wasm")]
fn now_ms() -> u64 {
    js_sys::Date::now() as u64
}

#[cfg(not(feature = "wasm"))]
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Target entries per leaf chunk. Tune based on observed chunk churn.
pub const TARGET_CHUNK_ENTRIES: usize = 1000;

/// Extract the top-level directory prefix from a path.
/// "notes/2024/jan.md" -> "notes/"
/// "readme.md" -> ""  (root-level file)
fn top_level_prefix(path: &str) -> &str {
    match path.find('/') {
        Some(idx) => &path[..=idx],
        None => "",
    }
}

/// Build a full Merkle tree from a flat list of FileEntry.
/// Returns the root node and all chunks that were created (to be stored).
///
/// Only called for initial index build or full rebuild.
pub async fn build_tree<S: ChunkStore>(
    store: &S,
    entries: Vec<FileEntry>,
    vault_id: &str,
    device_id: &str,
) -> Result<RootNode, ChunkError> {
    // 1. Group entries by top-level directory.
    let mut groups: BTreeMap<String, Vec<FileEntry>> = BTreeMap::new();
    for entry in entries {
        let prefix = top_level_prefix(&entry.path).to_string();
        groups.entry(prefix).or_default().push(entry);
    }

    let mut total_files = 0u64;
    let mut root_children = Vec::new();

    // 2. For each directory group: build leaf chunks and internal node.
    for (prefix, mut group_entries) in groups {
        group_entries.sort();
        total_files += group_entries.len() as u64;

        // Split into leaf chunks of TARGET_CHUNK_ENTRIES.
        let leaf_hashes = build_leaf_chunks(store, &group_entries).await?;

        if leaf_hashes.len() == 1 {
            // Single leaf chunk — use its hash directly as the directory's hash.
            root_children.push((prefix, leaf_hashes[0]));
        } else {
            // Multiple leaf chunks — create an internal node.
            let children: Vec<_> = leaf_hashes
                .into_iter()
                .enumerate()
                .map(|(i, hash)| (format!("{}{}", prefix, i), hash))
                .collect();
            let node = InternalNode::new(children);
            let node_bytes = node.serialize();
            let node_hash = crate::hash::hash_bytes(&node_bytes);
            store.put(node_hash, node_bytes).await?;
            root_children.push((prefix, node_hash));
        }
    }

    let now_ms = now_ms();

    let root = RootNode {
        vault_id: vault_id.to_string(),
        created_ms: now_ms,
        version: 1,
        children: root_children,
        total_files,
        parent_hash: None,
        device_id: device_id.to_string(),
    };

    Ok(root)
}

/// Build leaf chunks from a sorted list of entries.
/// Returns the hashes of all created leaf chunks.
async fn build_leaf_chunks<S: ChunkStore>(
    store: &S,
    entries: &[FileEntry],
) -> Result<Vec<FileHash>, ChunkError> {
    let mut hashes = Vec::new();

    for chunk_entries in entries.chunks(TARGET_CHUNK_ENTRIES) {
        let leaf = LeafChunk::new(chunk_entries.to_vec());
        let bytes = leaf.serialize();
        let hash = crate::hash::hash_bytes(&bytes);
        store.put(hash, bytes).await?;
        hashes.push(hash);
    }

    if hashes.is_empty() {
        // Empty directory — create an empty leaf chunk.
        let leaf = LeafChunk::new(vec![]);
        let bytes = leaf.serialize();
        let hash = crate::hash::hash_bytes(&bytes);
        store.put(hash, bytes).await?;
        hashes.push(hash);
    }

    Ok(hashes)
}

/// Incremental update: apply changes to an existing tree.
/// Only re-chunks leaf chunks containing changed files.
/// Returns a new RootNode with updated hashes.
pub async fn update_tree<S: ChunkStore>(
    store: &S,
    root: &RootNode,
    changed: &[FileEntry],
    deleted: &[String],
) -> Result<RootNode, ChunkError> {
    // Group changes by top-level prefix.
    let mut changes_by_prefix: BTreeMap<String, (Vec<FileEntry>, Vec<String>)> = BTreeMap::new();

    for entry in changed {
        let prefix = top_level_prefix(&entry.path).to_string();
        changes_by_prefix
            .entry(prefix)
            .or_default()
            .0
            .push(entry.clone());
    }

    for path in deleted {
        let prefix = top_level_prefix(path).to_string();
        changes_by_prefix
            .entry(prefix)
            .or_default()
            .1
            .push(path.clone());
    }

    let mut new_children = root.children.clone();
    let mut total_files = root.total_files;

    for (prefix, (adds, dels)) in &changes_by_prefix {
        // Find the existing child for this prefix.
        let existing_hash = new_children
            .iter()
            .find(|(p, _)| p == prefix)
            .map(|(_, h)| *h);

        // Load existing entries from the store.
        let mut entries = if let Some(hash) = existing_hash {
            load_all_entries(store, &hash).await?
        } else {
            vec![]
        };

        let old_count = entries.len() as u64;

        // Apply deletions.
        for del_path in dels {
            entries.retain(|e| &e.path != del_path);
        }

        // Apply additions/modifications.
        for add in adds {
            if let Some(existing) = entries.iter_mut().find(|e| e.path == add.path) {
                *existing = add.clone();
            } else {
                entries.push(add.clone());
            }
        }

        entries.sort();
        let new_count = entries.len() as u64;
        total_files = total_files - old_count + new_count;

        if entries.is_empty() {
            // Directory is now empty — remove from root children.
            new_children.retain(|(p, _)| p != prefix);
        } else {
            // Rebuild leaf chunks for this prefix.
            let leaf_hashes = build_leaf_chunks(store, &entries).await?;

            let new_hash = if leaf_hashes.len() == 1 {
                leaf_hashes[0]
            } else {
                let children: Vec<_> = leaf_hashes
                    .into_iter()
                    .enumerate()
                    .map(|(i, hash)| (format!("{}{}", prefix, i), hash))
                    .collect();
                let node = InternalNode::new(children);
                let node_bytes = node.serialize();
                let node_hash = crate::hash::hash_bytes(&node_bytes);
                store.put(node_hash, node_bytes).await?;
                node_hash
            };

            // Update or insert the child in root.
            if let Some(child) = new_children.iter_mut().find(|(p, _)| p == prefix) {
                child.1 = new_hash;
            } else {
                new_children.push((prefix.clone(), new_hash));
                new_children.sort_by(|a, b| a.0.cmp(&b.0));
            }
        }
    }

    let now_ms = now_ms();

    let new_root = RootNode {
        vault_id: root.vault_id.clone(),
        created_ms: now_ms,
        version: root.version,
        children: new_children,
        total_files,
        parent_hash: Some(root.hash()),
        device_id: root.device_id.clone(),
    };

    Ok(new_root)
}

/// Load all FileEntry from a subtree (internal node or leaf chunk).
pub async fn load_all_entries<S: ChunkStore>(
    store: &S,
    hash: &FileHash,
) -> Result<Vec<FileEntry>, ChunkError> {
    let bytes = store.get(hash).await?;

    // Try as leaf chunk first.
    if let Ok(leaf) = LeafChunk::deserialize(&bytes) {
        return Ok(leaf.entries);
    }

    // Try as internal node — recurse into children.
    if let Ok(node) = InternalNode::deserialize(&bytes) {
        let mut all_entries = Vec::new();
        for (_prefix, child_hash) in &node.children {
            let child_entries = Box::pin(load_all_entries(store, child_hash)).await?;
            all_entries.extend(child_entries);
        }
        return Ok(all_entries);
    }

    Err(ChunkError::Deserialize(
        "could not parse as LeafChunk or InternalNode".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::hash_bytes;
    use crate::store::MemoryChunkStore;

    fn make_entry(path: &str) -> FileEntry {
        FileEntry::new(path.to_string(), hash_bytes(path.as_bytes()), 1000, 100)
    }

    #[tokio::test]
    async fn build_small_tree() {
        let store = MemoryChunkStore::new();
        let entries = vec![
            make_entry("notes/a.md"),
            make_entry("notes/b.md"),
            make_entry("assets/pic.png"),
        ];

        let root = build_tree(&store, entries, "test-vault", "desktop")
            .await
            .unwrap();

        assert_eq!(root.total_files, 3);
        assert_eq!(root.vault_id, "test-vault");
        assert!(root.parent_hash.is_none());
        assert!(root.children.len() >= 2); // at least "notes/" and "assets/"
    }

    #[tokio::test]
    async fn update_tree_add_file() {
        let store = MemoryChunkStore::new();
        let entries = vec![make_entry("notes/a.md"), make_entry("notes/b.md")];

        let root = build_tree(&store, entries, "test", "dev").await.unwrap();
        assert_eq!(root.total_files, 2);

        let new_entry = make_entry("notes/c.md");
        let updated = update_tree(&store, &root, &[new_entry], &[]).await.unwrap();

        assert_eq!(updated.total_files, 3);
        assert_eq!(updated.parent_hash, Some(root.hash()));
        assert_ne!(updated.hash(), root.hash());
    }

    #[tokio::test]
    async fn update_tree_delete_file() {
        let store = MemoryChunkStore::new();
        let entries = vec![
            make_entry("notes/a.md"),
            make_entry("notes/b.md"),
            make_entry("notes/c.md"),
        ];

        let root = build_tree(&store, entries, "test", "dev").await.unwrap();
        assert_eq!(root.total_files, 3);

        let updated = update_tree(&store, &root, &[], &["notes/b.md".to_string()])
            .await
            .unwrap();

        assert_eq!(updated.total_files, 2);
        assert_ne!(updated.hash(), root.hash());
    }

    #[tokio::test]
    async fn update_tree_modify_file() {
        let store = MemoryChunkStore::new();
        let entries = vec![make_entry("notes/a.md"), make_entry("notes/b.md")];

        let root = build_tree(&store, entries, "test", "dev").await.unwrap();

        let modified = FileEntry::new("notes/a.md".into(), hash_bytes(b"new content"), 2000, 200);
        let updated = update_tree(&store, &root, &[modified], &[]).await.unwrap();

        assert_eq!(updated.total_files, 2);
        assert_ne!(updated.hash(), root.hash());
    }

    #[tokio::test]
    async fn build_tree_with_root_level_files() {
        let store = MemoryChunkStore::new();
        let entries = vec![make_entry("readme.md"), make_entry("notes/a.md")];

        let root = build_tree(&store, entries, "test", "dev").await.unwrap();
        assert_eq!(root.total_files, 2);
        // Should have both "" prefix (root-level) and "notes/" prefix.
    }
}
