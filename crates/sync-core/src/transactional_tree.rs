use crate::chunk::{ChunkError, FileEntry, InternalNode, LeafChunk, RootNode};
use crate::hash::{hash_bytes, FileHash};
use crate::store::MemoryChunkStore;
use std::collections::HashSet;

const MAX_MARKED_NODES: usize = 1_000_000;
const MAX_MARKED_ENTRIES: usize = 10_000_000;
const MAX_TREE_DEPTH: usize = 64;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct ChunkGcStats {
    pub before: u64,
    pub reachable: u64,
    pub removed: u64,
    pub after: u64,
    pub bytes_removed: u64,
}

/// Mark all byte-addressed nodes reachable from one or more roots.
///
/// The complete graph is validated before the caller can sweep anything:
/// missing, corrupt, hash-mismatched, unknown, or pathologically deep graphs
/// fail closed and leave the store untouched.
pub fn mark_reachable_chunks<'a>(
    store: &MemoryChunkStore,
    roots: impl IntoIterator<Item = &'a RootNode>,
) -> Result<HashSet<FileHash>, ChunkError> {
    let mut pending = Vec::new();
    for root in roots {
        pending.extend(root.children.iter().map(|(_, hash)| (*hash, 0usize)));
    }

    let mut reachable = HashSet::new();
    let mut entries = 0usize;
    while let Some((hash, depth)) = pending.pop() {
        if reachable.contains(&hash) {
            continue;
        }
        if reachable.len() >= MAX_MARKED_NODES {
            return Err(ChunkError::Deserialize("tree has too many nodes".into()));
        }
        if depth > MAX_TREE_DEPTH {
            return Err(ChunkError::Deserialize("tree is too deep".into()));
        }

        let bytes = store
            .get_chunk(&hash)
            .ok_or_else(|| ChunkError::NotFound(crate::hash::hash_to_hex(&hash)))?;
        if hash_bytes(&bytes) != hash {
            return Err(ChunkError::Deserialize(format!(
                "index object {} failed content-address validation",
                crate::hash::hash_to_hex(&hash),
            )));
        }
        reachable.insert(hash);

        if let Ok(leaf) = LeafChunk::deserialize(&bytes) {
            entries = entries.saturating_add(leaf.entries.len());
            if entries > MAX_MARKED_ENTRIES {
                return Err(ChunkError::Deserialize("tree has too many entries".into()));
            }
            continue;
        }
        if let Ok(node) = InternalNode::deserialize(&bytes) {
            if pending.len().saturating_add(node.children.len()) > MAX_MARKED_NODES {
                return Err(ChunkError::Deserialize("tree has too many nodes".into()));
            }
            pending.extend(
                node.children
                    .into_iter()
                    .rev()
                    .map(|(_, child)| (child, depth + 1)),
            );
            continue;
        }
        return Err(ChunkError::Deserialize(
            "could not parse index object as LeafChunk or InternalNode".into(),
        ));
    }
    Ok(reachable)
}

pub fn sweep_store<'a>(
    store: &MemoryChunkStore,
    roots: impl IntoIterator<Item = &'a RootNode>,
) -> Result<ChunkGcStats, ChunkError> {
    let reachable = mark_reachable_chunks(store, roots)?;
    Ok(sweep_marked(store, &reachable))
}

fn sweep_marked(store: &MemoryChunkStore, reachable: &HashSet<FileHash>) -> ChunkGcStats {
    let (before, after, bytes_removed) = store.retain_chunks(reachable);
    ChunkGcStats {
        before: before as u64,
        reachable: reachable.len() as u64,
        removed: before.saturating_sub(after) as u64,
        after: after as u64,
        bytes_removed,
    }
}

/// Local immutable Merkle graph with an explicit candidate transaction.
/// Root pointers are cheap clones; chunk bytes are shared by content hash.
pub struct TransactionalTree {
    committed_root: Option<RootNode>,
    candidate_root: Option<RootNode>,
    candidate_start_chunks: Option<HashSet<FileHash>>,
    store: MemoryChunkStore,
    vault_id: String,
    device_id: String,
}

impl TransactionalTree {
    pub fn new(vault_id: &str, device_id: &str) -> Self {
        Self {
            committed_root: None,
            candidate_root: None,
            candidate_start_chunks: None,
            store: MemoryChunkStore::new(),
            vault_id: vault_id.to_owned(),
            device_id: device_id.to_owned(),
        }
    }

    pub fn load_root_without_chunks(&mut self, root: RootNode) {
        self.committed_root = Some(root);
        self.candidate_root = None;
        self.candidate_start_chunks = None;
        self.store = MemoryChunkStore::new();
    }

    pub async fn rebuild(&mut self, entries: Vec<FileEntry>) -> Result<(), ChunkError> {
        let replacement_store = MemoryChunkStore::new();
        let replacement_root =
            crate::tree::build_tree(&replacement_store, entries, &self.vault_id, &self.device_id)
                .await?;
        mark_reachable_chunks(&replacement_store, [&replacement_root])?;
        self.store = replacement_store;
        self.committed_root = Some(replacement_root);
        self.candidate_root = None;
        self.candidate_start_chunks = None;
        Ok(())
    }

    pub fn begin_candidate(&mut self) -> Result<(), ChunkError> {
        if self.has_candidate() {
            return Err(state_error("candidate already active"));
        }
        let committed = self
            .committed_root
            .as_ref()
            .ok_or_else(|| state_error("no committed root"))?;
        // Validate before exposing a candidate. Do not sweep here: legacy
        // stores may still contain history until commit/abort completes.
        mark_reachable_chunks(&self.store, [committed])?;
        self.candidate_root = Some(committed.clone());
        self.candidate_start_chunks = Some(self.store.all_chunk_hashes().into_iter().collect());
        Ok(())
    }

    pub async fn apply_candidate(
        &mut self,
        changed: &[FileEntry],
        deleted: &[String],
    ) -> Result<(), ChunkError> {
        let root = self
            .candidate_root
            .clone()
            .ok_or_else(|| state_error("no active candidate"))?;
        if changed.is_empty() && deleted.is_empty() {
            return Ok(());
        }
        let updated = crate::tree::update_tree(&self.store, &root, changed, deleted).await?;
        self.candidate_root = Some(updated);
        Ok(())
    }

    pub fn commit_candidate(&mut self) -> Result<ChunkGcStats, ChunkError> {
        let candidate = self
            .candidate_root
            .as_ref()
            .ok_or_else(|| state_error("no active candidate"))?;
        let reachable = mark_reachable_chunks(&self.store, [candidate])?;
        let stats = sweep_marked(&self.store, &reachable);
        self.committed_root = self.candidate_root.take();
        self.candidate_start_chunks = None;
        Ok(stats)
    }

    pub fn abort_candidate(&mut self) -> Result<ChunkGcStats, ChunkError> {
        if !self.has_candidate() {
            return Err(state_error("no active candidate"));
        }
        let reachable = match self.committed_root.as_ref() {
            Some(root) => mark_reachable_chunks(&self.store, [root])?,
            None => HashSet::new(),
        };
        let stats = sweep_marked(&self.store, &reachable);
        self.candidate_root = None;
        self.candidate_start_chunks = None;
        Ok(stats)
    }

    pub async fn apply_committed(
        &mut self,
        changed: &[FileEntry],
        deleted: &[String],
    ) -> Result<ChunkGcStats, ChunkError> {
        self.begin_candidate()?;
        if let Err(error) = self.apply_candidate(changed, deleted).await {
            let _ = self.abort_candidate();
            return Err(error);
        }
        match self.commit_candidate() {
            Ok(stats) => Ok(stats),
            Err(error) => {
                let _ = self.abort_candidate();
                Err(error)
            }
        }
    }

    pub fn has_candidate(&self) -> bool {
        self.candidate_root.is_some()
    }

    pub fn committed_root(&self) -> Option<&RootNode> {
        self.committed_root.as_ref()
    }

    pub fn candidate_root(&self) -> Option<&RootNode> {
        self.candidate_root.as_ref()
    }

    pub fn committed_root_hash(&self) -> Option<FileHash> {
        self.committed_root.as_ref().map(RootNode::hash)
    }

    pub fn candidate_root_hash(&self) -> Option<FileHash> {
        self.candidate_root.as_ref().map(RootNode::hash)
    }

    pub fn committed_chunk_hashes(&self) -> Result<Vec<FileHash>, ChunkError> {
        let root = self
            .committed_root
            .as_ref()
            .ok_or_else(|| state_error("no committed root"))?;
        sorted_hashes(mark_reachable_chunks(&self.store, [root])?)
    }

    pub fn candidate_chunk_hashes(&self) -> Result<Vec<FileHash>, ChunkError> {
        let root = self
            .candidate_root
            .as_ref()
            .ok_or_else(|| state_error("no active candidate"))?;
        sorted_hashes(mark_reachable_chunks(&self.store, [root])?)
    }

    pub fn new_candidate_chunk_hashes(&self) -> Result<Vec<FileHash>, ChunkError> {
        let baseline = self
            .candidate_start_chunks
            .as_ref()
            .ok_or_else(|| state_error("no active candidate"))?;
        let mut hashes = self.candidate_chunk_hashes()?;
        hashes.retain(|hash| !baseline.contains(hash));
        Ok(hashes)
    }

    pub fn chunk_bytes(&self, hash: &FileHash) -> Option<Vec<u8>> {
        self.store.get_chunk(hash)
    }

    pub fn store_len(&self) -> usize {
        self.store.len()
    }
}

fn sorted_hashes(hashes: HashSet<FileHash>) -> Result<Vec<FileHash>, ChunkError> {
    let mut hashes: Vec<_> = hashes.into_iter().collect();
    hashes.sort();
    Ok(hashes)
}

fn state_error(message: &str) -> ChunkError {
    ChunkError::Deserialize(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::FileEntry;
    use crate::hash::hash_bytes;
    use crate::store::{ChunkStore, MemoryChunkStore};
    use crate::tree::build_tree;

    fn entry(path: &str, revision: u64) -> FileEntry {
        FileEntry::new(
            path.to_owned(),
            hash_bytes(format!("{path}:{revision}").as_bytes()),
            revision,
            revision + 1,
        )
    }

    #[tokio::test]
    async fn candidate_abort_restores_root_and_sweeps_candidate_chunks() {
        let mut tree = TransactionalTree::new("vault", "device");
        tree.rebuild(vec![entry("a/file.md", 1), entry("b/file.md", 1)])
            .await
            .unwrap();
        let committed = tree.committed_root_hash().unwrap();
        let committed_chunks = tree.committed_chunk_hashes().unwrap();

        tree.begin_candidate().unwrap();
        tree.apply_candidate(&[entry("a/file.md", 2)], &[])
            .await
            .unwrap();
        assert_ne!(tree.candidate_root_hash().unwrap(), committed);
        assert_eq!(tree.committed_root_hash().unwrap(), committed);
        assert!(!tree.new_candidate_chunk_hashes().unwrap().is_empty());

        let stats = tree.abort_candidate().unwrap();
        assert_eq!(tree.committed_root_hash().unwrap(), committed);
        assert!(!tree.has_candidate());
        assert_eq!(tree.committed_chunk_hashes().unwrap(), committed_chunks);
        assert_eq!(stats.after, stats.reachable);
        assert!(stats.removed > 0);
    }

    #[tokio::test]
    async fn candidate_commit_keeps_shared_chunks_once_and_removes_old_branch() {
        let mut tree = TransactionalTree::new("vault", "device");
        tree.rebuild(vec![entry("a/file.md", 1), entry("b/file.md", 1)])
            .await
            .unwrap();
        let committed = tree.committed_chunk_hashes().unwrap();

        tree.begin_candidate().unwrap();
        tree.apply_candidate(&[entry("a/file.md", 2)], &[])
            .await
            .unwrap();
        let candidate = tree.candidate_chunk_hashes().unwrap();
        let shared = committed
            .iter()
            .filter(|hash| candidate.contains(hash))
            .count();
        assert_eq!(shared, 1, "unchanged branch was not shared");
        assert_eq!(tree.store_len(), 3, "shared branch was stored twice");
        assert_eq!(tree.new_candidate_chunk_hashes().unwrap().len(), 1);

        let candidate_root = tree.candidate_root_hash().unwrap();
        let stats = tree.commit_candidate().unwrap();
        assert_eq!(tree.committed_root_hash().unwrap(), candidate_root);
        assert_eq!(tree.store_len(), candidate.len());
        assert_eq!(stats.after, stats.reachable);
        assert_eq!(stats.removed, 1);
    }

    #[tokio::test]
    async fn ten_thousand_commits_have_zero_unreachable_growth() {
        let mut tree = TransactionalTree::new("vault", "device");
        tree.rebuild(vec![entry("storm/file.md", 0)]).await.unwrap();

        for revision in 1..=10_000 {
            tree.begin_candidate().unwrap();
            tree.apply_candidate(&[entry("storm/file.md", revision)], &[])
                .await
                .unwrap();
            let stats = tree.commit_candidate().unwrap();
            assert_eq!(stats.after, stats.reachable);
            assert_eq!(tree.store_len(), 1);
        }
    }

    #[tokio::test]
    async fn failed_mark_does_not_sweep_any_chunk() {
        let store = MemoryChunkStore::new();
        let root = build_tree(
            &store,
            vec![entry("a/file.md", 1), entry("b/file.md", 1)],
            "vault",
            "device",
        )
        .await
        .unwrap();
        let orphan = hash_bytes(b"orphan");
        store.insert_chunk(orphan, b"orphan".to_vec());
        store.delete(&root.children[0].1).await.unwrap();
        let mut before = store.all_chunk_hashes();
        before.sort();

        assert!(sweep_store(&store, [&root]).is_err());
        let mut after = store.all_chunk_hashes();
        after.sort();
        assert_eq!(after, before, "failed mark performed a partial sweep");
    }

    #[tokio::test]
    async fn sweep_retains_transitively_reachable_internal_children() {
        let store = MemoryChunkStore::new();
        let entries = (0..1_001)
            .map(|index| entry(&format!("wide/{index:04}.md"), 1))
            .collect();
        let root = build_tree(&store, entries, "vault", "device")
            .await
            .unwrap();
        let reachable = mark_reachable_chunks(&store, [&root]).unwrap();
        assert_eq!(
            reachable.len(),
            3,
            "expected two leaves and one internal node"
        );
        let orphan = hash_bytes(b"unreachable but valid");
        store.insert_chunk(orphan, b"unreachable but valid".to_vec());

        let stats = sweep_store(&store, [&root]).unwrap();
        assert_eq!(stats.reachable, 3);
        assert_eq!(stats.removed, 1);
        assert!(reachable.iter().all(|hash| store.get_chunk(hash).is_some()));
    }

    #[test]
    fn unknown_node_type_fails_before_sweep() {
        let store = MemoryChunkStore::new();
        let invalid_bytes = b"not a flatbuffer node".to_vec();
        let invalid_hash = hash_bytes(&invalid_bytes);
        store.insert_chunk(invalid_hash, invalid_bytes);
        let orphan = hash_bytes(b"orphan remains");
        store.insert_chunk(orphan, b"orphan remains".to_vec());
        let root = crate::chunk::RootNode {
            vault_id: "vault".into(),
            created_ms: 0,
            version: 1,
            children: vec![("".into(), invalid_hash)],
            total_files: 0,
            parent_hash: None,
            device_id: "device".into(),
        };
        let before = store.len();

        assert!(sweep_store(&store, [&root]).is_err());
        assert_eq!(store.len(), before);
        assert!(store.get_chunk(&orphan).is_some());
    }

    #[tokio::test]
    async fn rebuild_replaces_store_instead_of_accumulating_old_graph() {
        let mut tree = TransactionalTree::new("vault", "device");
        tree.rebuild(vec![entry("old/file.md", 1)]).await.unwrap();
        let old = tree.committed_chunk_hashes().unwrap();

        tree.rebuild(vec![entry("new/file.md", 2)]).await.unwrap();
        let new = tree.committed_chunk_hashes().unwrap();
        assert_eq!(tree.store_len(), new.len());
        assert!(old.iter().all(|hash| !new.contains(hash)));
    }

    #[tokio::test]
    async fn rebuild_is_an_explicit_recovery_boundary_for_an_active_candidate() {
        let mut tree = TransactionalTree::new("vault", "device");
        tree.rebuild(vec![entry("old/file.md", 1)]).await.unwrap();
        tree.begin_candidate().unwrap();
        tree.apply_candidate(&[entry("old/file.md", 2)], &[])
            .await
            .unwrap();

        tree.rebuild(vec![entry("recovered/file.md", 3)])
            .await
            .unwrap();
        assert!(!tree.has_candidate());
        assert_eq!(
            tree.store_len(),
            tree.committed_chunk_hashes().unwrap().len()
        );
    }

    #[tokio::test]
    async fn nested_candidate_is_rejected_without_changing_state() {
        let mut tree = TransactionalTree::new("vault", "device");
        tree.rebuild(vec![entry("file.md", 1)]).await.unwrap();
        tree.begin_candidate().unwrap();
        let root = tree.candidate_root_hash();
        assert!(tree.begin_candidate().is_err());
        assert_eq!(tree.candidate_root_hash(), root);
    }
}
