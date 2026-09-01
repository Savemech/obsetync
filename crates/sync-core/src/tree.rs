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

enum FinalPathState {
    Upsert(FileEntry),
    Delete(String),
}

impl FinalPathState {
    fn path(&self) -> &str {
        match self {
            Self::Upsert(entry) => &entry.path,
            Self::Delete(path) => path,
        }
    }
}

/// Apply one prefix's final path states in linear time after normalization.
///
/// `existing` is strictly path-sorted by the validated tree format. Upserts
/// are stable-sorted so the last input value for a duplicate path wins, which
/// preserves the legacy sequential-update behavior. Deletes are applied first
/// semantically, so an upsert for the same path always wins.
fn merge_prefix_entries(
    existing: Vec<FileEntry>,
    mut sorted_upserts: Vec<FileEntry>,
    mut deletions: Vec<String>,
) -> Vec<FileEntry> {
    debug_assert!(existing.windows(2).all(|pair| pair[0].path < pair[1].path));

    // Stable ordering is required: duplicate paths retain their input order,
    // allowing the final occurrence to become the final state.
    if !sorted_upserts
        .windows(2)
        .all(|pair| pair[0].path <= pair[1].path)
    {
        sorted_upserts.sort_by(|left, right| left.path.cmp(&right.path));
    }
    let mut upserts: Vec<FileEntry> = Vec::with_capacity(sorted_upserts.len());
    for entry in sorted_upserts {
        if let Some(previous) = upserts.last_mut() {
            if previous.path == entry.path {
                *previous = entry;
                continue;
            }
        }
        upserts.push(entry);
    }

    if !deletions.windows(2).all(|pair| pair[0] <= pair[1]) {
        deletions.sort_unstable();
    }
    deletions.dedup();

    // Merge the two normalized operation streams. Equality selects Upsert,
    // matching the old delete-all-then-upsert-all ordering.
    let mut final_states = Vec::with_capacity(upserts.len() + deletions.len());
    let mut upserts = upserts.into_iter().peekable();
    let mut deletions = deletions.into_iter().peekable();
    while let (Some(upsert), Some(deletion)) = (upserts.peek(), deletions.peek()) {
        match upsert.path.as_str().cmp(deletion.as_str()) {
            std::cmp::Ordering::Less => {
                final_states.push(FinalPathState::Upsert(upserts.next().unwrap()));
            }
            std::cmp::Ordering::Equal => {
                final_states.push(FinalPathState::Upsert(upserts.next().unwrap()));
                deletions.next();
            }
            std::cmp::Ordering::Greater => {
                final_states.push(FinalPathState::Delete(deletions.next().unwrap()));
            }
        }
    }
    final_states.extend(upserts.map(FinalPathState::Upsert));
    final_states.extend(deletions.map(FinalPathState::Delete));

    // One two-pointer pass over existing entries and normalized final states.
    // Both iterators move monotonically; no retain/find scan is repeated.
    let capacity = existing.len().saturating_add(final_states.len());
    let mut merged = Vec::with_capacity(capacity);
    let mut existing = existing.into_iter().peekable();
    let mut final_states = final_states.into_iter().peekable();
    while let (Some(entry), Some(state)) = (existing.peek(), final_states.peek()) {
        match entry.path.as_str().cmp(state.path()) {
            std::cmp::Ordering::Less => merged.push(existing.next().unwrap()),
            std::cmp::Ordering::Equal => {
                existing.next();
                if let FinalPathState::Upsert(entry) = final_states.next().unwrap() {
                    merged.push(entry);
                }
            }
            std::cmp::Ordering::Greater => {
                if let FinalPathState::Upsert(entry) = final_states.next().unwrap() {
                    merged.push(entry);
                }
            }
        }
    }
    merged.extend(existing);
    for state in final_states {
        if let FinalPathState::Upsert(entry) = state {
            merged.push(entry);
        }
    }
    merged
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

    for (prefix, (adds, dels)) in changes_by_prefix {
        // Root children are canonical and strictly prefix-sorted. Reuse the
        // binary-search insertion point instead of scanning (and later
        // sorting) the complete root child list for every changed prefix.
        let child_position =
            new_children.binary_search_by(|(child_prefix, _)| child_prefix.cmp(&prefix));
        let existing_hash = child_position.ok().map(|index| new_children[index].1);

        // Load existing entries from the store.
        let entries = if let Some(hash) = existing_hash {
            load_all_entries(store, &hash).await?
        } else {
            vec![]
        };

        let old_count = entries.len() as u64;
        let entries = merge_prefix_entries(entries, adds, dels);
        let new_count = entries.len() as u64;
        total_files = total_files - old_count + new_count;

        if entries.is_empty() {
            // Directory is now empty — remove from root children.
            if let Ok(index) = child_position {
                new_children.remove(index);
            }
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
            match child_position {
                Ok(index) => new_children[index].1 = new_hash,
                Err(index) => new_children.insert(index, (prefix, new_hash)),
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
    const MAX_VISITED_NODES: usize = 1_000_000;
    const MAX_LOADED_ENTRIES: usize = 10_000_000;

    // Iterative traversal avoids an attacker-controlled internal-node depth
    // overflowing the server stack before uploaded roots are validated.
    let mut pending = vec![*hash];
    let mut all_entries = Vec::new();
    let mut visited = 0usize;
    while let Some(next_hash) = pending.pop() {
        visited += 1;
        if visited > MAX_VISITED_NODES {
            return Err(ChunkError::Deserialize("tree has too many nodes".into()));
        }
        let bytes = store.get(&next_hash).await?;
        if let Ok(leaf) = LeafChunk::deserialize(&bytes) {
            if all_entries.len().saturating_add(leaf.entries.len()) > MAX_LOADED_ENTRIES {
                return Err(ChunkError::Deserialize("tree has too many entries".into()));
            }
            all_entries.extend(leaf.entries);
            continue;
        }
        if let Ok(node) = InternalNode::deserialize(&bytes) {
            // Order does not matter here because the flattened boundary is
            // explicitly path-sorted below. Reverse keeps the common small
            // case intuitive while using a LIFO work stack.
            pending.extend(
                node.children
                    .iter()
                    .rev()
                    .map(|(_, child_hash)| *child_hash),
            );
            continue;
        }
        return Err(ChunkError::Deserialize(
            "could not parse as LeafChunk or InternalNode".into(),
        ));
    }

    // Internal-node labels contain decimal chunk indexes. Their stable
    // lexical order is `0, 1, 10, 11, 2, ...`, which is not necessarily
    // FileEntry path order once a directory spans more than ten leaves.
    // diff/merge both use a two-pointer walk and require this invariant.
    all_entries.sort();
    Ok(all_entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::hash_bytes;
    use crate::store::MemoryChunkStore;
    use std::collections::BTreeMap;

    fn make_entry(path: &str) -> FileEntry {
        FileEntry::new(path.to_string(), hash_bytes(path.as_bytes()), 1000, 100)
    }

    fn make_revision(path: &str, revision: u64) -> FileEntry {
        FileEntry::new(
            path.to_owned(),
            hash_bytes(format!("{path}:{revision}").as_bytes()),
            revision,
            revision + 1,
        )
    }

    fn map_oracle(
        existing: &[FileEntry],
        changed: &[FileEntry],
        deleted: &[String],
    ) -> Vec<FileEntry> {
        let mut expected: BTreeMap<String, FileEntry> = existing
            .iter()
            .cloned()
            .map(|entry| (entry.path.clone(), entry))
            .collect();
        for path in deleted {
            expected.remove(path);
        }
        for entry in changed {
            expected.insert(entry.path.clone(), entry.clone());
        }
        expected.into_values().collect()
    }

    #[derive(Clone, Copy)]
    struct TestRng(u64);

    impl TestRng {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0
        }

        fn below(&mut self, upper: usize) -> usize {
            (self.next() as usize) % upper
        }
    }

    #[test]
    fn prefix_merge_preserves_legacy_final_state_rules() {
        let existing = vec![
            make_revision("dir/a.md", 1),
            make_revision("dir/b.md", 1),
            make_revision("dir/c.md", 1),
        ];
        let changed = vec![
            make_revision("dir/b.md", 2),
            make_revision("dir/d.md", 1),
            make_revision("dir/b.md", 3),
        ];
        let deleted = vec![
            "dir/b.md".to_owned(),
            "dir/c.md".to_owned(),
            "dir/c.md".to_owned(),
            "dir/missing.md".to_owned(),
        ];

        let actual = merge_prefix_entries(existing.clone(), changed.clone(), deleted.clone());
        assert_eq!(actual, map_oracle(&existing, &changed, &deleted));
        assert_eq!(
            actual
                .iter()
                .map(|entry| entry.path.as_str())
                .collect::<Vec<_>>(),
            ["dir/a.md", "dir/b.md", "dir/d.md"]
        );
        assert_eq!(actual[1], make_revision("dir/b.md", 3));
    }

    #[test]
    fn prefix_merge_matches_map_oracle_for_random_operations() {
        let mut rng = TestRng(0x6f62_7365_7479_6e63);
        for case in 0..1_000u64 {
            let existing_count = rng.below(300);
            let existing: Vec<_> = (0..existing_count)
                .map(|index| make_revision(&format!("dir/{index:04}.md"), case))
                .collect();
            let changed: Vec<_> = (0..rng.below(200))
                .map(|revision| {
                    let index = rng.below(450);
                    make_revision(
                        &format!("dir/{index:04}.md"),
                        case * 1_000 + revision as u64,
                    )
                })
                .collect();
            let deleted: Vec<_> = (0..rng.below(200))
                .map(|_| format!("dir/{:04}.md", rng.below(450)))
                .collect();

            assert_eq!(
                merge_prefix_entries(existing.clone(), changed.clone(), deleted.clone()),
                map_oracle(&existing, &changed, &deleted),
                "final-state mismatch in generated case {case}"
            );
        }
    }

    #[tokio::test]
    async fn loading_wide_internal_node_returns_entries_in_path_order() {
        let store = MemoryChunkStore::new();
        let mut children = Vec::new();
        let mut expected = Vec::new();

        // InternalNode sorts child labels lexicographically, so labels 10/11
        // precede label 2. Consumers such as diff/merge still require the
        // flattened FileEntry stream itself to be path-sorted.
        for index in 0..12 {
            let path = format!("dir/{index:02}.md");
            expected.push(path.clone());
            let leaf = LeafChunk::new(vec![make_entry(&path)]);
            let bytes = leaf.serialize();
            let hash = hash_bytes(&bytes);
            store.put(hash, bytes).await.unwrap();
            children.push((format!("dir/{index}"), hash));
        }
        let internal = InternalNode::new(children);
        let bytes = internal.serialize();
        let hash = hash_bytes(&bytes);
        store.put(hash, bytes).await.unwrap();

        let loaded = load_all_entries(&store, &hash).await.unwrap();
        let paths: Vec<_> = loaded.into_iter().map(|entry| entry.path).collect();
        assert_eq!(paths, expected);
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

    #[test]
    fn top_level_prefix_extracts_first_dir() {
        assert_eq!(top_level_prefix("notes/2024/jan.md"), "notes/");
        assert_eq!(top_level_prefix("notes/a.md"), "notes/");
        assert_eq!(top_level_prefix("readme.md"), "");
        assert_eq!(top_level_prefix(""), "");
        assert_eq!(top_level_prefix("/"), "/");
        assert_eq!(top_level_prefix("a/"), "a/");
    }

    #[tokio::test]
    async fn build_tree_empty_input() {
        let store = MemoryChunkStore::new();
        let root = build_tree(&store, vec![], "v", "d").await.unwrap();
        assert_eq!(root.total_files, 0);
        assert!(root.children.is_empty());
        assert_eq!(root.vault_id, "v");
        assert_eq!(root.device_id, "d");
        assert!(root.parent_hash.is_none());
    }

    #[tokio::test]
    async fn load_all_entries_round_trip_through_build_tree() {
        let store = MemoryChunkStore::new();
        let entries = vec![
            make_entry("notes/a.md"),
            make_entry("notes/b.md"),
            make_entry("photos/p.png"),
        ];
        let root = build_tree(&store, entries.clone(), "v", "d").await.unwrap();

        // Sum entries across every prefix and confirm round-trip.
        let mut all = Vec::new();
        for (_, h) in &root.children {
            let mut sub = load_all_entries(&store, h).await.unwrap();
            all.append(&mut sub);
        }
        all.sort();
        let mut sorted_in = entries.clone();
        sorted_in.sort();
        assert_eq!(all.len(), sorted_in.len());
        for (a, b) in all.iter().zip(sorted_in.iter()) {
            assert_eq!(a.path, b.path);
            assert_eq!(a.hash, b.hash);
        }
    }

    #[tokio::test]
    async fn update_tree_delete_last_file_in_prefix_removes_prefix() {
        let store = MemoryChunkStore::new();
        let entries = vec![make_entry("only/a.md"), make_entry("notes/b.md")];
        let root = build_tree(&store, entries, "v", "d").await.unwrap();
        assert!(root.children.iter().any(|(p, _)| p == "only/"));

        let updated = update_tree(&store, &root, &[], &["only/a.md".to_string()])
            .await
            .unwrap();
        assert!(!updated.children.iter().any(|(p, _)| p == "only/"));
        assert_eq!(updated.total_files, 1);
    }

    #[tokio::test]
    async fn update_tree_no_changes_keeps_total() {
        let store = MemoryChunkStore::new();
        let entries = vec![make_entry("a.md"), make_entry("notes/b.md")];
        let root = build_tree(&store, entries, "v", "d").await.unwrap();
        let updated = update_tree(&store, &root, &[], &[]).await.unwrap();
        assert_eq!(updated.total_files, root.total_files);
        assert_eq!(updated.parent_hash, Some(root.hash()));
    }

    #[tokio::test]
    async fn update_tree_add_to_new_prefix() {
        let store = MemoryChunkStore::new();
        let root = build_tree(&store, vec![make_entry("notes/a.md")], "v", "d")
            .await
            .unwrap();
        let updated = update_tree(&store, &root, &[make_entry("photos/p.png")], &[])
            .await
            .unwrap();
        assert_eq!(updated.total_files, 2);
        assert!(updated.children.iter().any(|(p, _)| p == "photos/"));
    }

    #[tokio::test]
    async fn build_tree_splits_into_multiple_leaf_chunks() {
        // Force >1 leaf chunk per prefix by exceeding TARGET_CHUNK_ENTRIES.
        let store = MemoryChunkStore::new();
        let n = TARGET_CHUNK_ENTRIES + 5;
        let entries: Vec<_> = (0..n)
            .map(|i| make_entry(&format!("notes/{:05}.md", i)))
            .collect();
        let root = build_tree(&store, entries, "v", "d").await.unwrap();
        assert_eq!(root.total_files, n as u64);
        // Round-trip via load_all_entries through the InternalNode.
        let notes_hash = root
            .children
            .iter()
            .find(|(p, _)| p == "notes/")
            .map(|(_, h)| *h)
            .unwrap();
        let entries_back = load_all_entries(&store, &notes_hash).await.unwrap();
        assert_eq!(entries_back.len(), n);
    }

    #[tokio::test]
    async fn load_all_entries_errors_on_unknown_hash() {
        let store = MemoryChunkStore::new();
        let err = load_all_entries(&store, &hash_bytes(b"unknown")).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn update_tree_matches_flat_rebuild_across_random_batches() {
        let store = MemoryChunkStore::new();
        let initial: Vec<_> = (0..250)
            .map(|index| {
                let path = if index % 7 == 0 {
                    format!("root-{index:04}.md")
                } else {
                    format!("dir-{}/{index:04}.md", index % 5)
                };
                make_revision(&path, 0)
            })
            .collect();
        let mut flat: BTreeMap<String, FileEntry> = initial
            .iter()
            .cloned()
            .map(|entry| (entry.path.clone(), entry))
            .collect();
        let mut root = build_tree(&store, initial, "vault", "device")
            .await
            .unwrap();
        let mut rng = TestRng(0x7472_6565_2d76_3121);

        for batch in 1..=100u64 {
            let path_for = |index: usize| {
                if index % 7 == 0 {
                    format!("root-{index:04}.md")
                } else {
                    format!("dir-{}/{index:04}.md", index % 5)
                }
            };
            let changed: Vec<_> = (0..1 + rng.below(25))
                .map(|revision| {
                    let path = path_for(rng.below(400));
                    make_revision(&path, batch * 1_000 + revision as u64)
                })
                .collect();
            let deleted: Vec<_> = (0..rng.below(25))
                .map(|_| path_for(rng.below(400)))
                .collect();

            for path in &deleted {
                flat.remove(path);
            }
            for entry in &changed {
                flat.insert(entry.path.clone(), entry.clone());
            }
            root = update_tree(&store, &root, &changed, &deleted)
                .await
                .unwrap();

            let oracle_store = MemoryChunkStore::new();
            let oracle = build_tree(
                &oracle_store,
                flat.values().cloned().collect(),
                "vault",
                "device",
            )
            .await
            .unwrap();
            assert_eq!(
                root.hash(),
                oracle.hash(),
                "root mismatch after batch {batch}"
            );
            assert_eq!(root.total_files, flat.len() as u64);
        }
    }

    #[tokio::test]
    async fn update_tree_handles_mass_delete_and_more_than_ten_leaves() {
        let store = MemoryChunkStore::new();
        let count = TARGET_CHUNK_ENTRIES * 10 + 25;
        let initial: Vec<_> = (0..count)
            .map(|index| make_revision(&format!("wide/{index:05}.md"), 1))
            .collect();
        let root = build_tree(&store, initial.clone(), "vault", "device")
            .await
            .unwrap();
        let deleted: Vec<_> = (0..8_000)
            .map(|index| format!("wide/{index:05}.md"))
            .chain(std::iter::repeat_n("wide/00042.md".to_owned(), 20))
            .collect();
        let changed = vec![
            make_revision("wide/09000.md", 2),
            make_revision("wide/09000.md", 3),
            make_revision("wide/12000.md", 1),
            make_revision("wide/00042.md", 4),
        ];

        let updated = update_tree(&store, &root, &changed, &deleted)
            .await
            .unwrap();
        let expected = map_oracle(&initial, &changed, &deleted);
        let oracle_store = MemoryChunkStore::new();
        let oracle = build_tree(&oracle_store, expected.clone(), "vault", "device")
            .await
            .unwrap();
        assert_eq!(updated.hash(), oracle.hash());
        assert_eq!(updated.total_files, expected.len() as u64);

        let wide_hash = updated
            .children
            .iter()
            .find(|(prefix, _)| prefix == "wide/")
            .unwrap()
            .1;
        assert_eq!(
            load_all_entries(&store, &wide_hash).await.unwrap(),
            expected
        );
    }
}
