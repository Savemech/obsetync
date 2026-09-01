use crate::chunk::{ChunkError, FileEntry};
use crate::hash::FileHash;
use crate::store::MemoryChunkStore;
use crate::transactional_tree::{sorted_hashes, sweep_marked, ChunkGcStats};
use crate::tree_v2::RootNodeV2;
use std::collections::HashSet;

/// Candidate transaction over the v2 immutable range graph. It mirrors the
/// established v1 transaction boundary while keeping the two formats
/// impossible to mix inside one in-memory tree instance.
pub struct TransactionalTreeV2 {
    committed_root: Option<RootNodeV2>,
    candidate_root: Option<RootNodeV2>,
    candidate_start_chunks: Option<HashSet<FileHash>>,
    store: MemoryChunkStore,
    vault_id: String,
    device_id: String,
}

impl TransactionalTreeV2 {
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

    pub fn load_root_without_chunks(&mut self, root: RootNodeV2) {
        self.committed_root = Some(root);
        self.candidate_root = None;
        self.candidate_start_chunks = None;
        self.store = MemoryChunkStore::new();
    }

    pub async fn rebuild(&mut self, entries: Vec<FileEntry>) -> Result<(), ChunkError> {
        let replacement_store = MemoryChunkStore::new();
        let tree = crate::tree_v2::build_tree(&replacement_store, entries).await?;
        let replacement_root = RootNodeV2::new(&self.vault_id, &self.device_id, tree)?;
        crate::tree_v2::reachable_hashes(&replacement_store, &replacement_root.tree).await?;
        self.store = replacement_store;
        self.committed_root = Some(replacement_root);
        self.candidate_root = None;
        self.candidate_start_chunks = None;
        Ok(())
    }

    pub async fn begin_candidate(&mut self) -> Result<(), ChunkError> {
        if self.has_candidate() {
            return Err(state_error("candidate already active"));
        }
        let committed = self
            .committed_root
            .as_ref()
            .ok_or_else(|| state_error("no committed root"))?;
        crate::tree_v2::reachable_hashes(&self.store, &committed.tree).await?;
        self.candidate_root = Some(committed.clone());
        self.candidate_start_chunks = Some(self.store.all_chunk_hashes().into_iter().collect());
        Ok(())
    }

    pub async fn apply_candidate(
        &mut self,
        changed: &[FileEntry],
        deleted: &[String],
    ) -> Result<(), ChunkError> {
        let mut root = self
            .candidate_root
            .clone()
            .ok_or_else(|| state_error("no active candidate"))?;
        if changed.is_empty() && deleted.is_empty() {
            return Ok(());
        }
        let previous = root.hash();
        root.tree = crate::tree_v2::update_tree(&self.store, &root.tree, changed, deleted)
            .await?
            .0;
        root.parent_hash = Some(previous);
        self.candidate_root = Some(root);
        Ok(())
    }

    pub async fn commit_candidate(&mut self) -> Result<ChunkGcStats, ChunkError> {
        let candidate = self
            .candidate_root
            .as_ref()
            .ok_or_else(|| state_error("no active candidate"))?;
        let reachable = crate::tree_v2::reachable_hashes(&self.store, &candidate.tree).await?;
        let stats = sweep_marked(&self.store, &reachable);
        self.committed_root = self.candidate_root.take();
        self.candidate_start_chunks = None;
        Ok(stats)
    }

    pub async fn abort_candidate(&mut self) -> Result<ChunkGcStats, ChunkError> {
        if !self.has_candidate() {
            return Err(state_error("no active candidate"));
        }
        let reachable = match self.committed_root.as_ref() {
            Some(root) => crate::tree_v2::reachable_hashes(&self.store, &root.tree).await?,
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
        self.begin_candidate().await?;
        if let Err(error) = self.apply_candidate(changed, deleted).await {
            let _ = self.abort_candidate().await;
            return Err(error);
        }
        match self.commit_candidate().await {
            Ok(stats) => Ok(stats),
            Err(error) => {
                let _ = self.abort_candidate().await;
                Err(error)
            }
        }
    }

    pub fn has_candidate(&self) -> bool {
        self.candidate_root.is_some()
    }

    pub fn committed_root(&self) -> Option<&RootNodeV2> {
        self.committed_root.as_ref()
    }

    pub fn candidate_root(&self) -> Option<&RootNodeV2> {
        self.candidate_root.as_ref()
    }

    pub fn committed_root_hash(&self) -> Option<FileHash> {
        self.committed_root.as_ref().map(RootNodeV2::hash)
    }

    pub fn candidate_root_hash(&self) -> Option<FileHash> {
        self.candidate_root.as_ref().map(RootNodeV2::hash)
    }

    pub async fn committed_chunk_hashes(&self) -> Result<Vec<FileHash>, ChunkError> {
        let root = self
            .committed_root
            .as_ref()
            .ok_or_else(|| state_error("no committed root"))?;
        sorted_hashes(crate::tree_v2::reachable_hashes(&self.store, &root.tree).await?)
    }

    pub async fn candidate_chunk_hashes(&self) -> Result<Vec<FileHash>, ChunkError> {
        let root = self
            .candidate_root
            .as_ref()
            .ok_or_else(|| state_error("no active candidate"))?;
        sorted_hashes(crate::tree_v2::reachable_hashes(&self.store, &root.tree).await?)
    }

    pub async fn new_candidate_chunk_hashes(&self) -> Result<Vec<FileHash>, ChunkError> {
        let baseline = self
            .candidate_start_chunks
            .as_ref()
            .ok_or_else(|| state_error("no active candidate"))?;
        let mut hashes = self.candidate_chunk_hashes().await?;
        hashes.retain(|hash| !baseline.contains(hash));
        Ok(hashes)
    }

    pub fn chunk_bytes(&self, hash: &FileHash) -> Option<Vec<u8>> {
        self.store.get_chunk(hash)
    }

    #[cfg(test)]
    pub fn store_len(&self) -> usize {
        self.store.len()
    }
}

fn state_error(message: &str) -> ChunkError {
    ChunkError::Deserialize(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::hash_bytes;

    fn entry(path: &str, revision: u64) -> FileEntry {
        FileEntry::new(
            path.to_owned(),
            hash_bytes(format!("{path}:{revision}").as_bytes()),
            revision,
            revision + 1,
        )
    }

    #[tokio::test]
    async fn candidate_commit_and_abort_leave_only_reachable_v2_nodes() {
        let mut tree = TransactionalTreeV2::new("vault", "device");
        tree.rebuild(
            (0..5_000)
                .map(|index| entry(&format!("wide/{index:08}.md"), index as u64))
                .collect(),
        )
        .await
        .unwrap();
        let committed = tree.committed_root_hash().unwrap();
        let committed_chunks = tree.committed_chunk_hashes().await.unwrap();

        tree.begin_candidate().await.unwrap();
        tree.apply_candidate(&[entry("wide/00002500.md", 99_999)], &[])
            .await
            .unwrap();
        assert_ne!(tree.candidate_root_hash(), Some(committed));
        assert!(!tree.new_candidate_chunk_hashes().await.unwrap().is_empty());
        let abort = tree.abort_candidate().await.unwrap();
        assert_eq!(tree.committed_root_hash(), Some(committed));
        assert_eq!(
            tree.committed_chunk_hashes().await.unwrap(),
            committed_chunks
        );
        assert_eq!(abort.after, abort.reachable);

        tree.begin_candidate().await.unwrap();
        tree.apply_candidate(&[entry("wide/00002500.md", 100_000)], &[])
            .await
            .unwrap();
        let candidate = tree.candidate_root_hash().unwrap();
        let commit = tree.commit_candidate().await.unwrap();
        assert_eq!(tree.committed_root_hash(), Some(candidate));
        assert_eq!(commit.after, commit.reachable);
        assert_eq!(tree.store_len() as u64, commit.after);
    }
}
