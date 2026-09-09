use crate::candidate_mutation_v2::{V2CandidateMutation, V2MutationProgress};
use crate::chunk::{ChunkError, FileEntry};
use crate::hash::FileHash;
use crate::store::{ChunkRetirement, ChunkStoreMemory, MemoryChunkStore};
use crate::transactional_tree::{
    sorted_hashes, sweep_marked, CandidateOpenMemoryPlanV1, ChunkGcStats,
};
use crate::tree_metadata::{
    CandidateBaselineMemory, RootMetadataMemory, StringMemory, TreeMetadataMemory,
};
use crate::tree_v2::{RootNodeV2, V2ReachabilityCursor, V2ReachabilityProgress};
use std::collections::HashSet;

const MAX_SAFE_OUTPUT_SETTLEMENT_BYTES: u64 = (1u64 << 53) - 1;

/// Exact requested bytes in the stable V2 graph after a successful candidate
/// commit/abort sweep. This deliberately excludes map buckets/capacity,
/// metadata other than the surviving root range endpoints, scratch, linear
/// memory and RSS. `counters_valid=false` is never release authority.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct V2StableOutputMemoryV1 {
    pub node_payload_bytes: u64,
    pub range_endpoint_resident_requested_bytes: u64,
    pub resident_admission_bytes: u64,
    pub counters_valid: bool,
}

pub(crate) struct RootRetirementV2 {
    vault_id: Option<String>,
    child: Option<crate::tree_v2::RangeRef>,
    device_id: Option<String>,
    metadata: RootMetadataMemory,
}

impl RootRetirementV2 {
    pub(crate) fn from_root(root: RootNodeV2) -> Self {
        let metadata = RootMetadataMemory::from_v2(&root);
        Self::new(root, metadata)
    }
    fn new(root: RootNodeV2, metadata: RootMetadataMemory) -> Self {
        debug_assert_eq!(metadata, RootMetadataMemory::from_v2(&root));
        let RootNodeV2 {
            vault_id,
            tree,
            device_id,
            ..
        } = root;
        Self {
            vault_id: Some(vault_id),
            child: tree.child,
            device_id: Some(device_id),
            metadata,
        }
    }

    pub(crate) fn metadata_memory(&self) -> RootMetadataMemory {
        self.metadata
    }

    pub(crate) fn retire_one(&mut self) -> bool {
        if let Some(child) = self.child.take() {
            self.metadata.strings.subtract(&child.min_path);
            self.metadata.strings.subtract(&child.max_path);
            self.refresh_validity();
            self.clear_if_empty();
            return true;
        }
        if retire_string_owner(&mut self.vault_id, &mut self.metadata.strings) {
            self.refresh_validity();
            self.clear_if_empty();
            return true;
        }
        self.refresh_validity();
        if retire_string_owner(&mut self.device_id, &mut self.metadata.strings) {
            self.refresh_validity();
            self.clear_if_empty();
            return true;
        }
        self.refresh_validity();
        self.clear_if_empty();
        false
    }

    fn refresh_validity(&mut self) {
        self.metadata.counters_valid &= self.metadata.strings.counters_valid;
    }

    fn clear_if_empty(&mut self) {
        if self.child.is_none() && self.vault_id.is_none() && self.device_id.is_none() {
            self.metadata = RootMetadataMemory::default();
        }
    }
}

/// Cooperative destruction owner for a detached resident Tree v2. Conversion
/// only moves existing owners; each successful tick destroys one root/range
/// descriptor, resident node buffer, or identity string. Its generation
/// baseline is scalar; drained map backing release remains indivisible.
pub(crate) struct TreeRetirementV2 {
    committed: Option<RootRetirementV2>,
    candidate: Option<RootRetirementV2>,
    candidate_start_chunks: Option<u64>,
    candidate_start_chunks_metadata: CandidateBaselineMemory,
    chunks: ChunkRetirement,
    vault_id: Option<String>,
    device_id: Option<String>,
    tree_ids_metadata: StringMemory,
}

impl TreeRetirementV2 {
    pub(crate) fn chunk_memory(&self) -> ChunkStoreMemory {
        self.chunks.memory_summary()
    }

    pub(crate) fn metadata_memory(&self) -> TreeMetadataMemory {
        let committed = self.committed.as_ref().map_or_else(
            RootMetadataMemory::default,
            RootRetirementV2::metadata_memory,
        );
        let candidate = self.candidate.as_ref().map_or_else(
            RootMetadataMemory::default,
            RootRetirementV2::metadata_memory,
        );
        TreeMetadataMemory::new(
            committed,
            candidate,
            self.tree_ids_metadata,
            self.candidate_start_chunks_metadata,
        )
    }

    pub(crate) fn retire_one(&mut self) -> bool {
        if let Some(root) = &mut self.committed {
            if root.retire_one() {
                return true;
            }
            self.committed = None;
        }
        if let Some(root) = &mut self.candidate {
            if root.retire_one() {
                return true;
            }
            self.candidate = None;
        }
        if self.candidate_start_chunks.take().is_some() {
            self.candidate_start_chunks = None;
            self.candidate_start_chunks_metadata = CandidateBaselineMemory::default();
        }
        if self.chunks.next().is_some() {
            return true;
        }
        if retire_string_owner(&mut self.vault_id, &mut self.tree_ids_metadata) {
            return true;
        }
        retire_string_owner(&mut self.device_id, &mut self.tree_ids_metadata)
    }
}

/// Candidate transaction over the v2 immutable range graph. It mirrors the
/// established v1 transaction boundary while keeping the two formats
/// impossible to mix inside one in-memory tree instance.
pub struct TransactionalTreeV2 {
    committed_root: Option<RootNodeV2>,
    candidate_root: Option<RootNodeV2>,
    candidate_start_chunks: Option<u64>,
    store: MemoryChunkStore,
    vault_id: String,
    device_id: String,
    committed_metadata: RootMetadataMemory,
    candidate_metadata: RootMetadataMemory,
    candidate_start_chunks_metadata: CandidateBaselineMemory,
    tree_ids_metadata: StringMemory,
}

impl TransactionalTreeV2 {
    pub fn new(vault_id: &str, device_id: &str) -> Self {
        let vault_id = vault_id.to_owned();
        let device_id = device_id.to_owned();
        let tree_ids_metadata = StringMemory::of(&vault_id).combine(StringMemory::of(&device_id));
        Self {
            committed_root: None,
            candidate_root: None,
            candidate_start_chunks: None,
            store: MemoryChunkStore::new(),
            vault_id,
            device_id,
            committed_metadata: RootMetadataMemory::default(),
            candidate_metadata: RootMetadataMemory::default(),
            candidate_start_chunks_metadata: CandidateBaselineMemory::default(),
            tree_ids_metadata,
        }
    }

    /// Publish a graph whose private rebuild cursor already validated the
    /// complete content-addressed closure. This deliberately performs no
    /// second traversal or chunk copy at the ready-only finish boundary.
    pub(crate) fn from_verified_replacement(
        root: RootNodeV2,
        store: MemoryChunkStore,
        vault_id: String,
        device_id: String,
    ) -> Self {
        debug_assert_eq!(root.vault_id, vault_id);
        debug_assert_eq!(root.device_id, device_id);
        let committed_metadata = RootMetadataMemory::from_v2(&root);
        let tree_ids_metadata = StringMemory::of(&vault_id).combine(StringMemory::of(&device_id));
        Self {
            committed_root: Some(root),
            candidate_root: None,
            candidate_start_chunks: None,
            store,
            vault_id,
            device_id,
            committed_metadata,
            candidate_metadata: RootMetadataMemory::default(),
            candidate_start_chunks_metadata: CandidateBaselineMemory::default(),
            tree_ids_metadata,
        }
    }

    /// Irreversibly detach this resident graph for bounded retirement. No
    /// graph walk, copy, collection, or element destruction occurs here.
    pub(crate) fn into_retirement(self) -> TreeRetirementV2 {
        let Self {
            committed_root,
            candidate_root,
            candidate_start_chunks,
            store,
            vault_id,
            device_id,
            committed_metadata,
            candidate_metadata,
            candidate_start_chunks_metadata,
            tree_ids_metadata,
        } = self;
        TreeRetirementV2 {
            committed: committed_root.map(|root| RootRetirementV2::new(root, committed_metadata)),
            candidate: candidate_root.map(|root| RootRetirementV2::new(root, candidate_metadata)),
            candidate_start_chunks,
            candidate_start_chunks_metadata,
            chunks: store.into_chunks(),
            vault_id: Some(vault_id),
            device_id: Some(device_id),
            tree_ids_metadata,
        }
    }

    pub fn load_root_without_chunks(&mut self, root: RootNodeV2) {
        let committed_metadata = RootMetadataMemory::from_v2(&root);
        self.committed_root = Some(root);
        self.committed_metadata = committed_metadata;
        self.candidate_root = None;
        self.candidate_metadata = RootMetadataMemory::default();
        self.candidate_start_chunks = None;
        self.candidate_start_chunks_metadata = CandidateBaselineMemory::default();
        self.store = MemoryChunkStore::new();
    }

    pub async fn rebuild(&mut self, entries: Vec<FileEntry>) -> Result<(), ChunkError> {
        let replacement_store = MemoryChunkStore::new();
        let tree = crate::tree_v2::build_tree(&replacement_store, entries).await?;
        let replacement_root = RootNodeV2::new(&self.vault_id, &self.device_id, tree)?;
        crate::tree_v2::reachable_hashes(&replacement_store, &replacement_root.tree).await?;
        let committed_metadata = RootMetadataMemory::from_v2(&replacement_root);
        self.store = replacement_store;
        self.committed_root = Some(replacement_root);
        self.committed_metadata = committed_metadata;
        self.candidate_root = None;
        self.candidate_metadata = RootMetadataMemory::default();
        self.candidate_start_chunks = None;
        self.candidate_start_chunks_metadata = CandidateBaselineMemory::default();
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
        self.open_candidate_after_validation()?;
        Ok(())
    }

    pub(crate) fn open_candidate_after_validation(&mut self) -> Result<(), ChunkError> {
        let mut prepared = Some(self.prepare_candidate_root()?);
        self.open_prepared_candidate(&mut prepared)
    }

    pub(crate) fn candidate_open_memory_plan(
        &self,
    ) -> Result<CandidateOpenMemoryPlanV1, ChunkError> {
        if self.has_candidate() {
            return Err(state_error("candidate already active"));
        }
        self.store.candidate_baseline_preflight()?;
        let root = self
            .committed_root
            .as_ref()
            .ok_or_else(|| state_error("no committed root"))?;
        let length = |value: &str| {
            u64::try_from(value.len())
                .map_err(|_| state_error("candidate root string request overflow"))
        };
        let add = |left: u64, right: u64| {
            left.checked_add(right)
                .ok_or_else(|| state_error("candidate root string request overflow"))
        };
        let identity = add(length(&root.vault_id)?, length(&root.device_id)?)?;
        let endpoint = root
            .tree
            .child
            .as_ref()
            .map(|range| add(length(&range.min_path)?, length(&range.max_path)?))
            .transpose()?
            .unwrap_or(0);
        let total = identity
            .checked_add(endpoint)
            .filter(|n| *n <= MAX_SAFE_OUTPUT_SETTLEMENT_BYTES)
            .ok_or_else(|| state_error("candidate root string request is not JS-exact"))?;
        let resident = self.store.len();
        if u64::try_from(resident)
            .ok()
            .is_none_or(|count| count > MAX_SAFE_OUTPUT_SETTLEMENT_BYTES)
        {
            return Err(state_error("candidate resident count is not JS-exact"));
        }
        Ok(CandidateOpenMemoryPlanV1 {
            schema: 1,
            scope: "v2-candidate-open-root",
            resident_chunk_count: resident,
            root_string_count: if root.tree.child.is_some() { 4 } else { 2 },
            root_identity_requested_bytes: identity,
            root_endpoint_requested_bytes: endpoint,
            root_string_requested_bytes: total,
            baseline_key_snapshot_requested_bytes: 0,
            peak_admission_bytes: total,
            baseline_strategy: "insertion-generation-v1",
        })
    }

    pub(crate) fn prepare_candidate_root(&self) -> Result<RootNodeV2, ChunkError> {
        self.candidate_open_memory_plan()?;
        self.committed_root
            .as_ref()
            .ok_or_else(|| state_error("no committed root"))?
            .try_clone_exact()
    }

    /// A prepared root belongs to this exclusively-owned tree job. Every
    /// fallible preflight precedes taking it or advancing the insertion cut.
    pub(crate) fn open_prepared_candidate(
        &mut self,
        prepared: &mut Option<RootNodeV2>,
    ) -> Result<(), ChunkError> {
        if self.has_candidate() {
            return Err(state_error("candidate already active"));
        }
        let candidate_root = prepared
            .as_ref()
            .ok_or_else(|| state_error("candidate root is not prepared"))?;
        if self.committed_root.as_ref() != Some(candidate_root) {
            return Err(state_error(
                "prepared candidate root no longer matches committed state",
            ));
        }
        let cut = self.store.candidate_baseline_preflight()?;
        let candidate_metadata = RootMetadataMemory::from_v2(&candidate_root);
        self.store.open_candidate_baseline(cut)?;
        self.candidate_root = prepared.take();
        self.candidate_metadata = candidate_metadata;
        self.candidate_start_chunks = Some(cut);
        self.candidate_start_chunks_metadata = CandidateBaselineMemory::scalar();
        Ok(())
    }

    pub(crate) async fn step_candidate_validation(
        &self,
        cursor: &mut V2ReachabilityCursor,
        max_units: usize,
    ) -> Result<V2ReachabilityProgress, ChunkError> {
        cursor.step(&self.store, max_units).await
    }

    pub async fn apply_candidate(
        &mut self,
        changed: &[FileEntry],
        deleted: &[String],
    ) -> Result<(), ChunkError> {
        if self.candidate_root.is_none() {
            return Err(state_error("no active candidate"));
        }
        if changed.is_empty() && deleted.is_empty() {
            return Ok(());
        }
        let (previous, next) = {
            let root = self
                .candidate_root
                .as_ref()
                .ok_or_else(|| state_error("no active candidate"))?;
            let previous = root.hash();
            let next = crate::tree_v2::update_tree(&self.store, &root.tree, changed, deleted)
                .await?
                .0;
            (previous, next)
        };
        let root = self
            .candidate_root
            .as_mut()
            .expect("candidate checked before update");
        root.tree = next;
        root.parent_hash = Some(previous);
        let candidate_metadata = RootMetadataMemory::from_v2(root);
        self.candidate_metadata = candidate_metadata;
        Ok(())
    }

    pub(crate) fn begin_candidate_mutation(
        &self,
        changed: Vec<FileEntry>,
        deleted: Vec<String>,
    ) -> Result<Option<V2CandidateMutation>, ChunkError> {
        let root = self
            .candidate_root
            .as_ref()
            .ok_or_else(|| state_error("no active candidate"))?;
        if changed.is_empty() && deleted.is_empty() {
            return Ok(None);
        }
        V2CandidateMutation::new(root.tree.clone(), changed, deleted).map(Some)
    }

    pub(crate) fn step_candidate_mutation(
        &self,
        cursor: &mut V2CandidateMutation,
        max_units: usize,
    ) -> Result<V2MutationProgress, ChunkError> {
        cursor.step(&self.store, max_units)
    }

    pub(crate) fn finish_candidate_mutation(
        &mut self,
        cursor: &mut V2CandidateMutation,
    ) -> Result<(), ChunkError> {
        let (previous, next) = {
            let root = self
                .candidate_root
                .as_ref()
                .ok_or_else(|| state_error("no active candidate"))?;
            let previous = root.hash();
            let (next, _) = cursor.finish(&self.store, &root.tree)?;
            (previous, next)
        };
        let root = self
            .candidate_root
            .as_mut()
            .expect("candidate checked before mutation finish");
        root.tree = next;
        root.parent_hash = Some(previous);
        let candidate_metadata = RootMetadataMemory::from_v2(root);
        self.candidate_metadata = candidate_metadata;
        Ok(())
    }

    fn stable_root_endpoint_requested_bytes(root: &RootNodeV2) -> Result<u64, ChunkError> {
        let Some(child) = root.tree.child.as_ref() else {
            return Ok(0);
        };
        let min = u64::try_from(child.min_path.len())
            .map_err(|_| state_error("V2 stable root endpoint byte count overflow"))?;
        let max = u64::try_from(child.max_path.len())
            .map_err(|_| state_error("V2 stable root endpoint byte count overflow"))?;
        min.checked_add(max)
            .filter(|bytes| *bytes <= MAX_SAFE_OUTPUT_SETTLEMENT_BYTES)
            .ok_or_else(|| state_error("V2 stable root endpoint byte count is not JS-exact"))
    }

    /// Fail before reachability/sweep when the eventual post-settlement total
    /// cannot be an exact JS integer. Sweep only removes payload buffers, so a
    /// valid pre-sweep upper bound makes the normal post-sweep report infallible.
    fn preflight_output_settlement_v1(&self, root: &RootNodeV2) -> Result<u64, ChunkError> {
        let endpoint_bytes = Self::stable_root_endpoint_requested_bytes(root)?;
        let memory = self.store.memory_summary();
        if !memory.counters_valid
            || u64::try_from(memory.chunks)
                .ok()
                .is_none_or(|chunks| chunks > MAX_SAFE_OUTPUT_SETTLEMENT_BYTES)
            || memory.payload_bytes > MAX_SAFE_OUTPUT_SETTLEMENT_BYTES
            || memory
                .payload_bytes
                .checked_add(endpoint_bytes)
                .is_none_or(|bytes| bytes > MAX_SAFE_OUTPUT_SETTLEMENT_BYTES)
        {
            return Err(state_error(
                "V2 stable output memory counters are not exact",
            ));
        }
        Ok(endpoint_bytes)
    }

    fn output_settlement_v1_after_sweep(&self, endpoint_bytes: u64) -> V2StableOutputMemoryV1 {
        let memory = self.store.memory_summary();
        let total = memory.payload_bytes.checked_add(endpoint_bytes);
        let counters_valid = memory.counters_valid
            && memory.payload_bytes <= MAX_SAFE_OUTPUT_SETTLEMENT_BYTES
            && endpoint_bytes <= MAX_SAFE_OUTPUT_SETTLEMENT_BYTES
            && total.is_some_and(|bytes| bytes <= MAX_SAFE_OUTPUT_SETTLEMENT_BYTES);
        V2StableOutputMemoryV1 {
            node_payload_bytes: memory.payload_bytes,
            range_endpoint_resident_requested_bytes: endpoint_bytes,
            resident_admission_bytes: total.unwrap_or(u64::MAX),
            counters_valid,
        }
    }

    /// Additive settlement API: legacy commit keeps its historical return
    /// shape, while admitted hosts get one report produced by the same native
    /// call which completed the validated sweep.
    pub(crate) async fn commit_candidate_output_settlement_v1(
        &mut self,
    ) -> Result<(ChunkGcStats, V2StableOutputMemoryV1), ChunkError> {
        let endpoint_bytes = self.preflight_output_settlement_v1(
            self.candidate_root
                .as_ref()
                .ok_or_else(|| state_error("no active candidate"))?,
        )?;
        let stats = self.commit_candidate().await?;
        Ok((stats, self.output_settlement_v1_after_sweep(endpoint_bytes)))
    }

    /// Additive settlement API for rollback to the stable committed graph.
    pub(crate) async fn abort_candidate_output_settlement_v1(
        &mut self,
    ) -> Result<(ChunkGcStats, V2StableOutputMemoryV1), ChunkError> {
        if !self.has_candidate() {
            return Err(state_error("no active candidate"));
        }
        let endpoint_bytes = match self.committed_root.as_ref() {
            Some(root) => self.preflight_output_settlement_v1(root)?,
            None => 0,
        };
        // A candidate cannot normally exist without a committed V2 root, but
        // keep the empty fallback exact for structural/native tests.
        if self.committed_root.is_none() {
            let memory = self.store.memory_summary();
            if !memory.counters_valid
                || u64::try_from(memory.chunks)
                    .ok()
                    .is_none_or(|chunks| chunks > MAX_SAFE_OUTPUT_SETTLEMENT_BYTES)
                || memory.payload_bytes > MAX_SAFE_OUTPUT_SETTLEMENT_BYTES
            {
                return Err(state_error(
                    "V2 stable output memory counters are not exact",
                ));
            }
        }
        let stats = self.abort_candidate().await?;
        Ok((stats, self.output_settlement_v1_after_sweep(endpoint_bytes)))
    }

    pub async fn commit_candidate(&mut self) -> Result<ChunkGcStats, ChunkError> {
        let candidate = self
            .candidate_root
            .as_ref()
            .ok_or_else(|| state_error("no active candidate"))?;
        let reachable = crate::tree_v2::reachable_hashes(&self.store, &candidate.tree).await?;
        self.store.close_candidate_baseline(
            self.candidate_start_chunks
                .ok_or_else(|| state_error("candidate baseline is missing"))?,
        )?;
        let stats = sweep_marked(&self.store, &reachable);
        self.committed_root = self.candidate_root.take();
        self.committed_metadata = std::mem::take(&mut self.candidate_metadata);
        self.candidate_start_chunks = None;
        self.candidate_start_chunks_metadata = CandidateBaselineMemory::default();
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
        self.store.close_candidate_baseline(
            self.candidate_start_chunks
                .ok_or_else(|| state_error("candidate baseline is missing"))?,
        )?;
        let stats = sweep_marked(&self.store, &reachable);
        self.candidate_root = None;
        self.candidate_metadata = RootMetadataMemory::default();
        self.candidate_start_chunks = None;
        self.candidate_start_chunks_metadata = CandidateBaselineMemory::default();
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
        let root = self
            .candidate_root
            .as_ref()
            .ok_or_else(|| state_error("no active candidate"))?;
        self.candidate_chunk_plan_from_reachable(
            crate::tree_v2::reachable_hashes(&self.store, &root.tree).await?,
        )
        .map(|(_, fresh)| fresh)
    }

    pub(crate) fn candidate_chunk_plan_from_reachable(
        &self,
        reachable: HashSet<FileHash>,
    ) -> Result<(Vec<FileHash>, Vec<FileHash>), ChunkError> {
        if self.candidate_root.is_none() {
            return Err(state_error("no active candidate"));
        }
        let baseline = self
            .candidate_start_chunks
            .as_ref()
            .ok_or_else(|| state_error("candidate chunk baseline is missing"))?;
        let all = sorted_hashes(reachable)?;
        let mut fresh = Vec::new();
        for hash in &all {
            if self.store.is_fresh_since(hash, *baseline)? {
                fresh.push(*hash);
            }
        }
        Ok((all, fresh))
    }

    pub(crate) fn candidate_chunk_is_fresh(&self, hash: &FileHash) -> Result<bool, ChunkError> {
        if self.candidate_root.is_none() {
            return Err(state_error("no active candidate"));
        }
        let baseline = self
            .candidate_start_chunks
            .as_ref()
            .ok_or_else(|| state_error("candidate chunk baseline is missing"))?;
        self.store.is_fresh_since(hash, *baseline)
    }

    pub fn chunk_bytes(&self, hash: &FileHash) -> Option<Vec<u8>> {
        self.store.get_chunk(hash)
    }

    pub fn chunk_byte_length(&self, hash: &FileHash) -> Option<usize> {
        self.store.chunk_byte_length(hash)
    }

    pub(crate) fn copy_chunk_range(
        &self,
        hash: &FileHash,
        captured_length: usize,
        offset: usize,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ChunkError> {
        self.store
            .copy_chunk_range(hash, captured_length, offset, max_bytes)
    }

    #[cfg(test)]
    pub fn store_len(&self) -> usize {
        self.store.len()
    }

    pub(crate) fn chunk_memory(&self) -> ChunkStoreMemory {
        self.store.memory_summary()
    }

    pub(crate) fn metadata_memory(&self) -> TreeMetadataMemory {
        TreeMetadataMemory::new(
            self.committed_metadata,
            self.candidate_metadata,
            self.tree_ids_metadata,
            self.candidate_start_chunks_metadata,
        )
    }

    #[cfg(test)]
    pub(crate) fn resident_store_for_test(&self) -> &MemoryChunkStore {
        &self.store
    }
}

fn state_error(message: &str) -> ChunkError {
    ChunkError::Deserialize(message.to_owned())
}

fn retire_string_owner(value: &mut Option<String>, memory: &mut StringMemory) -> bool {
    let Some(value) = value.take() else {
        return false;
    };
    let had_allocation = value.capacity() > 0;
    memory.subtract(&value);
    had_allocation
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::hash_bytes;
    use crate::tree_v2::{TreeV2Root, TREE_VERSION};

    fn entry(path: &str, revision: u64) -> FileEntry {
        FileEntry::new(
            path.to_owned(),
            hash_bytes(format!("{path}:{revision}").as_bytes()),
            revision,
            revision + 1,
        )
    }

    fn string_with_capacity(value: &str, capacity: usize) -> String {
        let mut result = String::with_capacity(capacity.max(value.len()));
        result.push_str(value);
        result
    }

    fn metadata_oracle(tree: &TransactionalTreeV2) -> TreeMetadataMemory {
        TreeMetadataMemory::new(
            tree.committed_root
                .as_ref()
                .map_or_else(RootMetadataMemory::default, RootMetadataMemory::from_v2),
            tree.candidate_root
                .as_ref()
                .map_or_else(RootMetadataMemory::default, RootMetadataMemory::from_v2),
            StringMemory::of(&tree.vault_id).combine(StringMemory::of(&tree.device_id)),
            tree.candidate_start_chunks
                .as_ref()
                .map_or_else(CandidateBaselineMemory::default, |_| {
                    CandidateBaselineMemory::scalar()
                }),
        )
    }

    fn assert_metadata_oracle(tree: &TransactionalTreeV2) {
        assert_eq!(tree.metadata_memory(), metadata_oracle(tree));
    }

    fn stable_output_oracle(tree: &TransactionalTreeV2) -> V2StableOutputMemoryV1 {
        let node_payload_bytes = tree
            .store
            .all_chunks()
            .into_iter()
            .map(|(_, bytes)| bytes.len() as u64)
            .sum::<u64>();
        let range_endpoint_resident_requested_bytes = tree
            .committed_root()
            .and_then(|root| root.tree.child.as_ref())
            .map_or(0, |child| {
                (child.min_path.len() + child.max_path.len()) as u64
            });
        V2StableOutputMemoryV1 {
            node_payload_bytes,
            range_endpoint_resident_requested_bytes,
            resident_admission_bytes: node_payload_bytes + range_endpoint_resident_requested_bytes,
            counters_valid: true,
        }
    }

    fn stable_chunks(tree: &TransactionalTreeV2) -> Vec<(FileHash, Vec<u8>)> {
        let mut chunks = tree.store.all_chunks();
        chunks.sort_by_key(|(hash, _)| *hash);
        chunks
    }

    fn candidate_identity_owners(tree: &TransactionalTreeV2) -> [(usize, usize); 2] {
        let root = tree.candidate_root().expect("candidate identity fixture");
        [
            (root.vault_id.as_ptr() as usize, root.vault_id.capacity()),
            (root.device_id.as_ptr() as usize, root.device_id.capacity()),
        ]
    }

    fn root_retirement_oracle(root: &RootRetirementV2) -> RootMetadataMemory {
        let mut strings = StringMemory::default();
        if let Some(value) = &root.vault_id {
            strings.add(value);
        }
        if let Some(child) = &root.child {
            strings.add(&child.min_path);
            strings.add(&child.max_path);
        }
        if let Some(value) = &root.device_id {
            strings.add(value);
        }
        if root.vault_id.is_none() && root.child.is_none() && root.device_id.is_none() {
            RootMetadataMemory::default()
        } else {
            RootMetadataMemory {
                roots: 1,
                strings,
                counters_valid: strings.counters_valid,
                ..RootMetadataMemory::default()
            }
        }
    }

    fn assert_retirement_root_oracles(retirement: &TreeRetirementV2) {
        let snapshot = retirement.metadata_memory();
        assert_eq!(
            snapshot.committed,
            retirement
                .committed
                .as_ref()
                .map_or_else(RootMetadataMemory::default, root_retirement_oracle)
        );
        assert_eq!(
            snapshot.candidate,
            retirement
                .candidate
                .as_ref()
                .map_or_else(RootMetadataMemory::default, root_retirement_oracle)
        );
        let mut tree_ids = StringMemory::default();
        if let Some(value) = &retirement.vault_id {
            tree_ids.add(value);
        }
        if let Some(value) = &retirement.device_id {
            tree_ids.add(value);
        }
        assert_eq!(snapshot.tree_ids, tree_ids);
        assert!(snapshot.counters_valid);
    }

    fn drain_candidate_mutation(tree: &TransactionalTreeV2, cursor: &mut V2CandidateMutation) {
        loop {
            let progress = tree.step_candidate_mutation(cursor, 256).unwrap();
            if progress.done {
                return;
            }
            if progress.phase == "plan ready" {
                let plan = cursor.output_memory_plan_v1().unwrap();
                cursor
                    .resume_after_output_memory_plan_v1(
                        plan.node_payload_bytes,
                        plan.range_endpoint_peak_requested_bytes,
                        plan.range_endpoint_resident_requested_bytes,
                    )
                    .unwrap();
            } else {
                assert!(progress.units > 0);
            }
        }
    }

    #[tokio::test]
    async fn metadata_sidecars_follow_v2_create_load_rebuild_and_candidate_lifecycle() {
        let root_vault = string_with_capacity("vault", 101);
        let root_device = string_with_capacity("", 73);
        let root = RootNodeV2::new(
            root_vault,
            root_device,
            TreeV2Root {
                version: TREE_VERSION,
                total_files: 0,
                child: None,
            },
        )
        .unwrap();
        let tree_vault = string_with_capacity("vault", 47);
        let tree_device = string_with_capacity("", 31);
        let mut tree = TransactionalTreeV2::from_verified_replacement(
            root,
            MemoryChunkStore::new(),
            tree_vault,
            tree_device,
        );
        assert_metadata_oracle(&tree);
        assert_ne!(
            tree.metadata_memory().committed.strings.capacity_bytes,
            tree.metadata_memory().tree_ids.capacity_bytes,
            "distinct physical root/tree ID capacities were copied as one cache"
        );

        tree.begin_candidate().await.unwrap();
        assert_metadata_oracle(&tree);
        assert!(tree.metadata_memory().candidate_baseline.present);
        assert_eq!(tree.metadata_memory().candidate_baseline.hashes, 0);
        assert_ne!(
            tree.metadata_memory().committed.strings.capacity_bytes,
            tree.metadata_memory().candidate.strings.capacity_bytes,
            "candidate clone copied the source capacity cache instead of measuring its owner"
        );

        let empty_identity = candidate_identity_owners(&tree);
        tree.apply_candidate(&[], &[]).await.unwrap();
        assert_eq!(candidate_identity_owners(&tree), empty_identity);
        let direct_identity = candidate_identity_owners(&tree);
        tree.apply_candidate(&[entry("direct.md", 1)], &[])
            .await
            .unwrap();
        assert_eq!(
            candidate_identity_owners(&tree),
            direct_identity,
            "direct candidate update cloned and discarded the root identity owners"
        );
        assert_metadata_oracle(&tree);
        let committed_before_abort = tree.committed_metadata;
        tree.abort_candidate().await.unwrap();
        assert_metadata_oracle(&tree);
        assert_eq!(tree.committed_metadata, committed_before_abort);
        assert_eq!(tree.candidate_metadata, RootMetadataMemory::default());
        assert_eq!(
            tree.candidate_start_chunks_metadata,
            CandidateBaselineMemory::default()
        );

        tree.begin_candidate().await.unwrap();
        let mut mutation = tree
            .begin_candidate_mutation(vec![entry("stepped.md", 2)], Vec::new())
            .unwrap()
            .unwrap();
        let resident_before_steps = tree.metadata_memory();
        drain_candidate_mutation(&tree, &mut mutation);
        assert_eq!(tree.metadata_memory(), resident_before_steps);
        let stepped_identity = candidate_identity_owners(&tree);
        tree.finish_candidate_mutation(&mut mutation).unwrap();
        assert_eq!(
            candidate_identity_owners(&tree),
            stepped_identity,
            "stepped candidate finish cloned and discarded the root identity owners"
        );
        assert_metadata_oracle(&tree);
        let candidate_before_commit = tree.candidate_metadata;
        tree.commit_candidate().await.unwrap();
        assert_metadata_oracle(&tree);
        assert_eq!(tree.committed_metadata, candidate_before_commit);
        assert_eq!(tree.candidate_metadata, RootMetadataMemory::default());

        tree.begin_candidate().await.unwrap();
        let mut stale = tree
            .begin_candidate_mutation(vec![entry("stale.md", 3)], Vec::new())
            .unwrap()
            .unwrap();
        drain_candidate_mutation(&tree, &mut stale);
        tree.apply_candidate(&[entry("newer.md", 4)], &[])
            .await
            .unwrap();
        let after_newer = tree.metadata_memory();
        assert!(tree.finish_candidate_mutation(&mut stale).is_err());
        assert_eq!(tree.metadata_memory(), after_newer);
        assert_metadata_oracle(&tree);

        tree.rebuild(vec![entry("rebuilt.md", 5)]).await.unwrap();
        assert_metadata_oracle(&tree);
        assert_eq!(tree.candidate_metadata, RootMetadataMemory::default());
        assert_eq!(
            tree.candidate_start_chunks_metadata,
            CandidateBaselineMemory::default()
        );
        let before_failed_rebuild = tree.metadata_memory();
        assert!(tree
            .rebuild(vec![entry("duplicate.md", 6), entry("duplicate.md", 7)])
            .await
            .is_err());
        assert_eq!(tree.metadata_memory(), before_failed_rebuild);
        assert_metadata_oracle(&tree);

        let loaded = RootNodeV2::new(
            string_with_capacity("vault", 89),
            string_with_capacity("loaded-device", 113),
            TreeV2Root {
                version: TREE_VERSION,
                total_files: 0,
                child: None,
            },
        )
        .unwrap();
        tree.load_root_without_chunks(loaded);
        assert_metadata_oracle(&tree);
        assert_eq!(tree.store_len(), 0);
        assert_eq!(tree.candidate_metadata, RootMetadataMemory::default());

        let mut no_candidate = TransactionalTreeV2::new("vault", "device");
        assert!(no_candidate.apply_candidate(&[], &[]).await.is_err());
    }

    #[tokio::test]
    async fn metadata_retirement_tracks_actual_v2_owner_release_and_scalar_baseline() {
        let mut tree = TransactionalTreeV2::new("retirement-vault", "retirement-device");
        tree.rebuild(
            (0..2_000)
                .map(|index| entry(&format!("retire/{index:08}.md"), index))
                .collect(),
        )
        .await
        .unwrap();
        tree.begin_candidate().await.unwrap();
        assert_metadata_oracle(&tree);
        let resident_metadata = tree.metadata_memory();
        assert_eq!(
            resident_metadata.candidate_baseline,
            CandidateBaselineMemory::scalar()
        );

        let mut retirement = tree.into_retirement();
        assert_eq!(retirement.metadata_memory(), resident_metadata);
        assert_retirement_root_oracles(&retirement);

        let mut saw_scalar_release = false;
        let mut previous = retirement.metadata_memory();
        let mut units = 0usize;
        loop {
            let worked = retirement.retire_one();
            let current = retirement.metadata_memory();
            assert_retirement_root_oracles(&retirement);
            assert_eq!(current.candidate_baseline.hashes, 0);
            assert_eq!(current.candidate_baseline.capacity_slots, 0);
            assert_eq!(current.candidate_baseline.logical_hash_bytes, 0);
            if previous.candidate_baseline.present && !current.candidate_baseline.present {
                saw_scalar_release = true;
                assert_eq!(previous.candidate_baseline.hashes, 0);
                assert_eq!(
                    current.candidate_baseline,
                    CandidateBaselineMemory::default()
                );
            }
            previous = current;
            if !worked {
                break;
            }
            units += 1;
            assert!(units < 100_000, "metadata retirement did not converge");
        }
        assert!(saw_scalar_release);
        assert_eq!(retirement.metadata_memory(), TreeMetadataMemory::default());
        assert!(!retirement.retire_one());
    }

    #[tokio::test]
    async fn metadata_validation_errors_retain_v2_candidate_owners() {
        let mut tree = TransactionalTreeV2::new("vault", "device");
        tree.rebuild(vec![entry("file.md", 1)]).await.unwrap();
        assert_metadata_oracle(&tree);

        let committed_hash = tree
            .committed_root()
            .unwrap()
            .tree
            .child
            .as_ref()
            .unwrap()
            .hash;
        let committed_bytes = tree.store.get_chunk(&committed_hash).unwrap();
        let before_failed_begin = tree.metadata_memory();
        tree.store.insert_chunk(committed_hash, b"corrupt".to_vec());
        assert!(tree.begin_candidate().await.is_err());
        assert_eq!(tree.metadata_memory(), before_failed_begin);
        assert!(!tree.has_candidate());
        tree.store.insert_chunk(committed_hash, committed_bytes);

        tree.begin_candidate().await.unwrap();
        let before_failed_abort = tree.metadata_memory();
        let committed_bytes = tree.store.get_chunk(&committed_hash).unwrap();
        tree.store.insert_chunk(committed_hash, b"corrupt".to_vec());
        let corrupt_abort_chunks = stable_chunks(&tree);
        assert!(tree.abort_candidate_output_settlement_v1().await.is_err());
        assert_eq!(tree.metadata_memory(), before_failed_abort);
        assert_eq!(stable_chunks(&tree), corrupt_abort_chunks);
        assert!(tree.has_candidate());
        tree.store.insert_chunk(committed_hash, committed_bytes);
        tree.abort_candidate().await.unwrap();
        assert_metadata_oracle(&tree);

        tree.begin_candidate().await.unwrap();
        tree.apply_candidate(&[entry("file.md", 2)], &[])
            .await
            .unwrap();
        let candidate_hash = tree
            .candidate_root()
            .unwrap()
            .tree
            .child
            .as_ref()
            .unwrap()
            .hash;
        let candidate_bytes = tree.store.get_chunk(&candidate_hash).unwrap();
        let before_failed_commit = tree.metadata_memory();
        tree.store.insert_chunk(candidate_hash, b"corrupt".to_vec());
        let corrupt_commit_chunks = stable_chunks(&tree);
        assert!(tree.commit_candidate_output_settlement_v1().await.is_err());
        assert_eq!(tree.metadata_memory(), before_failed_commit);
        assert_eq!(stable_chunks(&tree), corrupt_commit_chunks);
        assert!(tree.has_candidate());
        tree.store.insert_chunk(candidate_hash, candidate_bytes);
        tree.commit_candidate().await.unwrap();
        assert_metadata_oracle(&tree);
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
        let (abort, abort_memory) = tree.abort_candidate_output_settlement_v1().await.unwrap();
        assert_eq!(tree.committed_root_hash(), Some(committed));
        assert_eq!(
            tree.committed_chunk_hashes().await.unwrap(),
            committed_chunks
        );
        assert_eq!(abort.after, abort.reachable);
        assert_eq!(abort_memory, stable_output_oracle(&tree));

        tree.begin_candidate().await.unwrap();
        tree.apply_candidate(&[entry("wide/00002500.md", 100_000)], &[])
            .await
            .unwrap();
        let candidate = tree.candidate_root_hash().unwrap();
        let (commit, commit_memory) = tree.commit_candidate_output_settlement_v1().await.unwrap();
        assert_eq!(tree.committed_root_hash(), Some(candidate));
        assert_eq!(commit.after, commit.reachable);
        assert_eq!(tree.store_len() as u64, commit.after);
        assert_eq!(commit_memory, stable_output_oracle(&tree));
    }

    #[tokio::test]
    async fn output_settlement_counts_utf8_endpoints_and_empty_graph_exactly() {
        let mut tree = TransactionalTreeV2::new("vault", "device");
        tree.rebuild(vec![entry("é/猫.md", 1), entry("Ω/β.md", 2)])
            .await
            .unwrap();
        tree.begin_candidate().await.unwrap();
        let (commit, memory) = tree.commit_candidate_output_settlement_v1().await.unwrap();
        assert_eq!(commit.removed, 0);
        assert_eq!(memory, stable_output_oracle(&tree));
        let child = tree.committed_root().unwrap().tree.child.as_ref().unwrap();
        assert_eq!(
            memory.range_endpoint_resident_requested_bytes,
            (child.min_path.len() + child.max_path.len()) as u64
        );

        let committed_bytes = tree.committed_root().unwrap().serialize().unwrap();
        let committed_chunks = stable_chunks(&tree);
        tree.begin_candidate().await.unwrap();
        tree.apply_candidate(&[entry("aaa/αβγδε.md", 3)], &[])
            .await
            .unwrap();
        assert_ne!(tree.candidate_root_hash(), tree.committed_root_hash());
        let changed_child = tree.candidate_root().unwrap().tree.child.as_ref().unwrap();
        assert_ne!(
            (changed_child.min_path.len() + changed_child.max_path.len()) as u64,
            memory.range_endpoint_resident_requested_bytes,
            "abort fixture must distinguish candidate and committed endpoint totals"
        );
        let (abort, abort_memory) = tree.abort_candidate_output_settlement_v1().await.unwrap();
        assert!(
            abort.removed > 0,
            "UTF-8 abort fixture did not sweep candidate nodes"
        );
        assert_eq!(abort.after, abort.reachable);
        assert_eq!(abort_memory, stable_output_oracle(&tree));
        assert_eq!(abort_memory, memory);
        assert_eq!(
            tree.committed_root().unwrap().serialize().unwrap(),
            committed_bytes
        );
        assert_eq!(stable_chunks(&tree), committed_chunks);
        assert!(!tree.has_candidate());

        tree.begin_candidate().await.unwrap();
        tree.apply_candidate(&[], &["é/猫.md".to_owned(), "Ω/β.md".to_owned()])
            .await
            .unwrap();
        let (empty, empty_memory) = tree.commit_candidate_output_settlement_v1().await.unwrap();
        assert_eq!(empty.after, 0);
        assert_eq!(
            empty_memory,
            V2StableOutputMemoryV1 {
                counters_valid: true,
                ..V2StableOutputMemoryV1::default()
            }
        );

        let empty_root_bytes = tree.committed_root().unwrap().serialize().unwrap();
        tree.begin_candidate().await.unwrap();
        assert_eq!(tree.candidate_root_hash(), tree.committed_root_hash());
        let (empty_abort, empty_abort_memory) =
            tree.abort_candidate_output_settlement_v1().await.unwrap();
        assert_eq!(empty_abort, ChunkGcStats::default());
        assert_eq!(empty_abort_memory, stable_output_oracle(&tree));
        assert_eq!(empty_abort_memory, empty_memory);
        assert_eq!(
            tree.committed_root().unwrap().serialize().unwrap(),
            empty_root_bytes
        );
        assert!(stable_chunks(&tree).is_empty());
        assert!(!tree.has_candidate());
    }

    #[tokio::test]
    async fn output_settlement_counter_preflight_never_sweeps() {
        for outcome in ["commit", "abort"] {
            for (payload, valid) in [
                (0, false),
                (MAX_SAFE_OUTPUT_SETTLEMENT_BYTES, true),
                (MAX_SAFE_OUTPUT_SETTLEMENT_BYTES + 1, true),
            ] {
                let mut tree = TransactionalTreeV2::new("vault", "device");
                tree.rebuild(vec![entry("stable.md", 1)]).await.unwrap();
                tree.begin_candidate().await.unwrap();
                tree.apply_candidate(&[entry("candidate.md", 2)], &[])
                    .await
                    .unwrap();
                tree.store
                    .set_payload_memory_counter_for_test(payload, valid);
                let committed = tree.committed_root_hash();
                let candidate = tree.candidate_root_hash();
                let chunks = stable_chunks(&tree);
                let result = match outcome {
                    "commit" => tree.commit_candidate_output_settlement_v1().await,
                    "abort" => tree.abort_candidate_output_settlement_v1().await,
                    _ => unreachable!(),
                };
                assert!(result.is_err());
                assert_eq!(tree.committed_root_hash(), committed);
                assert_eq!(tree.candidate_root_hash(), candidate);
                assert_eq!(stable_chunks(&tree), chunks);
                assert!(tree.has_candidate());
            }
        }
    }

    #[tokio::test]
    async fn candidate_chunk_plan_preserves_all_resident_v2_baseline() {
        let mut tree = TransactionalTreeV2::new("vault", "device");
        tree.rebuild(vec![entry("file.md", 1)]).await.unwrap();

        let future_store = MemoryChunkStore::new();
        let future_tree = crate::tree_v2::build_tree(&future_store, vec![entry("file.md", 2)])
            .await
            .unwrap();
        let future_hash = future_tree.child.as_ref().unwrap().hash;
        tree.store
            .insert_chunk(future_hash, future_store.get_chunk(&future_hash).unwrap());

        tree.begin_candidate().await.unwrap();
        tree.apply_candidate(&[entry("file.md", 2)], &[])
            .await
            .unwrap();
        let root = tree.candidate_root().unwrap();
        let reachable = crate::tree_v2::reachable_hashes(&tree.store, &root.tree)
            .await
            .unwrap();
        let (all, fresh) = tree.candidate_chunk_plan_from_reachable(reachable).unwrap();

        assert_eq!(all, tree.candidate_chunk_hashes().await.unwrap());
        assert_eq!(fresh, tree.new_candidate_chunk_hashes().await.unwrap());
        assert_eq!(all, vec![future_hash]);
        assert!(fresh.is_empty(), "resident orphan was treated as fresh");
    }

    #[tokio::test]
    async fn candidate_open_root_copy_preserves_history_and_rejects_stale_or_exhausted_publication()
    {
        let mut tree = TransactionalTreeV2::new("αβγδεζηθι", "κλμνξοπρστ");
        tree.rebuild(vec![entry("資料/😀.md", 1)]).await.unwrap();
        let root = tree.committed_root.as_mut().unwrap();
        root.created_ms = 123_456;
        root.parent_hash = Some(hash_bytes(b"literal history"));
        root.vault_id.reserve(256);
        root.tree.child.as_mut().unwrap().min_path.reserve(128);
        tree.committed_metadata = RootMetadataMemory::from_v2(root);
        let original = root.clone();
        let bytes = original.serialize().unwrap();
        let mut prepared = Some(tree.prepare_candidate_root().unwrap());
        assert_eq!(prepared.as_ref().unwrap(), &original);
        assert_eq!(prepared.as_ref().unwrap().serialize().unwrap(), bytes);
        let plan = tree.candidate_open_memory_plan().unwrap();
        assert_eq!(plan.root_string_count, 4);
        assert_eq!(
            plan.root_identity_requested_bytes,
            (original.vault_id.len() + original.device_id.len()) as u64
        );
        assert_eq!(plan.baseline_key_snapshot_requested_bytes, 0);
        tree.committed_root.as_mut().unwrap().created_ms += 1;
        assert!(tree.open_prepared_candidate(&mut prepared).is_err());
        assert_eq!(prepared.as_ref().unwrap(), &original);
        assert_eq!(tree.store.candidate_baseline_preflight().unwrap(), 0);
        tree.committed_root.as_mut().unwrap().created_ms -= 1;
        tree.store.set_insertion_generation_for_test(u64::MAX);
        assert!(tree.open_prepared_candidate(&mut prepared).is_err());
        assert!(prepared.is_some() && !tree.has_candidate());
        tree.store.set_insertion_generation_for_test(0);
        tree.open_prepared_candidate(&mut prepared).unwrap();
        assert!(prepared.is_none());
        assert_eq!(
            tree.candidate_root.as_ref().unwrap().serialize().unwrap(),
            bytes
        );
        assert_eq!(
            tree.metadata_memory().candidate_baseline,
            CandidateBaselineMemory::scalar()
        );
        tree.abort_candidate().await.unwrap();
        assert_eq!(
            tree.committed_root.as_ref().unwrap().serialize().unwrap(),
            bytes
        );
        assert_eq!(tree.store.candidate_baseline_preflight().unwrap(), 1);
    }

    #[tokio::test]
    async fn resident_retirement_counts_one_owned_payload_per_unit() {
        let empty = TransactionalTreeV2::new("vault", "");
        let mut empty_retirement = empty.into_retirement();
        assert!(empty_retirement.retire_one()); // non-empty vault id
        assert!(!empty_retirement.retire_one());
        assert!(!empty_retirement.retire_one());

        let mut tree = TransactionalTreeV2::new("vault", "device");
        tree.rebuild(
            (0..2_000)
                .map(|index| entry(&format!("retire/{index:08}.md"), index))
                .collect(),
        )
        .await
        .unwrap();
        tree.begin_candidate().await.unwrap();
        let chunks = tree.store.len();
        assert!(tree.candidate_start_chunks.is_some());
        let mut retirement = tree.into_retirement();
        let mut units = 0usize;
        while retirement.retire_one() {
            units += 1;
            assert!(units < 100_000, "resident retirement did not converge");
        }
        // Two non-empty roots each own child/vault/device, the transaction
        // owns two identity strings and each node buffer is an independent
        // cleanup unit. The scalar baseline has no key/backing retirement.
        assert_eq!(units, 6 + 2 + chunks);
        assert!(!retirement.retire_one());
    }
}
