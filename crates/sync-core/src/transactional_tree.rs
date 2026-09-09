use crate::candidate_mutation_v1::{CandidateMutationProgress, CandidateMutationV1};
use crate::chunk::{ChunkError, FileEntry, InternalNode, LeafChunk, RootNode};
use crate::hash::{hash_bytes, FileHash};
use crate::store::{ChunkRetirement, ChunkStoreMemory, MemoryChunkStore};
use crate::tree_metadata::{
    CandidateBaselineMemory, OwnedRootV1, RootMetadataMemory, StringMemory, TreeMetadataMemory,
};
use std::collections::HashSet;

const MAX_MARKED_NODES: usize = 1_000_000;
const MAX_MARKED_ENTRIES: usize = 10_000_000;
const MAX_TREE_DEPTH: usize = 64;
const MAX_SAFE_OUTPUT_SETTLEMENT_BYTES: u64 = (1u64 << 53) - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CandidateOpenMemoryPlanV1 {
    pub schema: u8,
    pub scope: &'static str,
    pub resident_chunk_count: usize,
    pub root_string_count: usize,
    pub root_identity_requested_bytes: u64,
    pub root_endpoint_requested_bytes: u64,
    pub root_string_requested_bytes: u64,
    pub baseline_key_snapshot_requested_bytes: u64,
    pub peak_admission_bytes: u64,
    pub baseline_strategy: &'static str,
}

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
pub(crate) struct ReachabilityCursor {
    pending: Vec<(FileHash, usize)>,
    expanding: Option<ChildExpansion>,
    reachable: HashSet<FileHash>,
    entries: usize,
    completed: usize,
    seeding_complete: bool,
    retiring: bool,
    retiring_hashes: Option<std::collections::hash_set::IntoIter<FileHash>>,
}

struct ChildExpansion {
    children: Vec<(String, FileHash)>,
    depth: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReachabilityProgress {
    pub units: usize,
    pub completed: usize,
    pub remaining: usize,
    pub reachable: usize,
    pub done: bool,
}

impl ReachabilityCursor {
    pub(crate) fn new() -> Self {
        Self {
            pending: Vec::new(),
            expanding: None,
            reachable: HashSet::new(),
            entries: 0,
            completed: 0,
            seeding_complete: false,
            retiring: false,
            retiring_hashes: None,
        }
    }

    pub(crate) fn seed_root_child(&mut self, hash: FileHash) -> Result<(), ChunkError> {
        if self.retiring || self.seeding_complete {
            return Err(ChunkError::Deserialize(
                "reachability root seeding is complete".into(),
            ));
        }
        self.pending.push((hash, 0));
        self.completed = self.completed.saturating_add(1);
        Ok(())
    }

    pub(crate) fn finish_seeding(&mut self) {
        assert!(!self.retiring, "retiring reachability cannot resume");
        self.seeding_complete = true;
    }

    /// One unit owns exactly one pending graph edge. Decoding and validating
    /// the addressed node is intentionally still an indivisible native
    /// primitive; callers bound how many such primitives share one host turn.
    pub(crate) fn step(
        &mut self,
        store: &MemoryChunkStore,
        max_units: usize,
    ) -> Result<ReachabilityProgress, ChunkError> {
        if self.retiring {
            return Err(ChunkError::Deserialize("reachability is retiring".into()));
        }
        if max_units == 0 {
            return Err(ChunkError::Deserialize(
                "reachability step budget must be positive".into(),
            ));
        }
        if !self.seeding_complete {
            return Err(ChunkError::Deserialize(
                "reachability root seeding is incomplete".into(),
            ));
        }
        let mut units = 0usize;
        while units < max_units {
            if let Some(expanding) = &mut self.expanding {
                let (_, hash) = expanding
                    .children
                    .pop()
                    .expect("an empty child expansion is never retained");
                self.pending.push((hash, expanding.depth));
                units += 1;
                self.completed = self.completed.saturating_add(1);
                if expanding.children.is_empty() {
                    self.expanding = None;
                }
                continue;
            }
            let Some((hash, depth)) = self.pending.pop() else {
                break;
            };
            units += 1;
            self.completed = self.completed.saturating_add(1);
            if self.reachable.contains(&hash) {
                continue;
            }
            if self.reachable.len() >= MAX_MARKED_NODES {
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
            self.reachable.insert(hash);

            if let Ok(leaf) = LeafChunk::deserialize(&bytes) {
                self.entries = self.entries.saturating_add(leaf.entries.len());
                if self.entries > MAX_MARKED_ENTRIES {
                    return Err(ChunkError::Deserialize("tree has too many entries".into()));
                }
                continue;
            }
            if let Ok(node) = InternalNode::deserialize(&bytes) {
                if self.pending.len().saturating_add(node.children.len()) > MAX_MARKED_NODES {
                    return Err(ChunkError::Deserialize("tree has too many nodes".into()));
                }
                if !node.children.is_empty() {
                    self.expanding = Some(ChildExpansion {
                        children: node.children,
                        depth: depth + 1,
                    });
                }
                continue;
            }
            return Err(ChunkError::Deserialize(
                "could not parse index object as LeafChunk or InternalNode".into(),
            ));
        }
        Ok(self.progress(units))
    }

    pub(crate) fn progress(&self, units: usize) -> ReachabilityProgress {
        ReachabilityProgress {
            units,
            completed: self.completed,
            remaining: self.pending.len()
                + self
                    .expanding
                    .as_ref()
                    .map(|value| value.children.len())
                    .unwrap_or(0),
            reachable: self.reachable.len(),
            done: !self.retiring
                && self.seeding_complete
                && self.pending.is_empty()
                && self.expanding.is_none(),
        }
    }

    pub(crate) fn finish(self) -> Result<HashSet<FileHash>, ChunkError> {
        if self.retiring
            || !self.seeding_complete
            || !self.pending.is_empty()
            || self.expanding.is_some()
        {
            return Err(ChunkError::Deserialize(
                "reachability validation is incomplete".into(),
            ));
        }
        Ok(self.reachable)
    }

    pub(crate) fn take_reachable(&mut self) -> Result<HashSet<FileHash>, ChunkError> {
        if self.retiring
            || !self.seeding_complete
            || !self.pending.is_empty()
            || self.expanding.is_some()
        {
            return Err(ChunkError::Deserialize(
                "reachability validation is incomplete".into(),
            ));
        }
        Ok(std::mem::take(&mut self.reachable))
    }

    /// One child/pending/reachable descriptor per unit. Starting retirement
    /// permanently closes graph validation. Hash iterator scans and drained
    /// backing-allocation destruction remain allocator/runtime residuals.
    pub(crate) fn retire_one(&mut self) -> bool {
        self.retiring = true;
        if let Some(expanding) = &mut self.expanding {
            if expanding.children.pop().is_some() {
                return true;
            }
            self.expanding = None;
        }
        if self.pending.pop().is_some() {
            return true;
        }
        let hashes = self
            .retiring_hashes
            .get_or_insert_with(|| std::mem::take(&mut self.reachable).into_iter());
        hashes.next().is_some()
    }
}

pub fn mark_reachable_chunks<'a>(
    store: &MemoryChunkStore,
    roots: impl IntoIterator<Item = &'a RootNode>,
) -> Result<HashSet<FileHash>, ChunkError> {
    let mut cursor = ReachabilityCursor::new();
    for root in roots {
        for (_, hash) in &root.children {
            cursor.seed_root_child(*hash)?;
        }
    }
    cursor.finish_seeding();
    while !cursor.progress(0).done {
        cursor.step(store, usize::MAX)?;
    }
    cursor.finish()
}

pub fn sweep_store<'a>(
    store: &MemoryChunkStore,
    roots: impl IntoIterator<Item = &'a RootNode>,
) -> Result<ChunkGcStats, ChunkError> {
    let reachable = mark_reachable_chunks(store, roots)?;
    Ok(sweep_marked(store, &reachable))
}

pub(crate) fn sweep_marked(
    store: &MemoryChunkStore,
    reachable: &HashSet<FileHash>,
) -> ChunkGcStats {
    let (before, after, bytes_removed) = store.retain_chunks(reachable);
    ChunkGcStats {
        before: before as u64,
        reachable: reachable.len() as u64,
        removed: before.saturating_sub(after) as u64,
        after: after as u64,
        bytes_removed,
    }
}

#[cfg(test)]
mod retirement_tests {
    use super::*;

    fn metadata_oracle(tree: &TransactionalTree) -> TreeMetadataMemory {
        TreeMetadataMemory::new(
            tree.committed_root
                .as_ref()
                .map_or_else(RootMetadataMemory::default, |root| {
                    RootMetadataMemory::from_v1(root.value())
                }),
            tree.candidate_root
                .as_ref()
                .map_or_else(RootMetadataMemory::default, |root| {
                    RootMetadataMemory::from_v1(root.value())
                }),
            StringMemory::of(&tree.vault_id).combine(StringMemory::of(&tree.device_id)),
            tree.candidate_start_chunks
                .as_ref()
                .map_or_else(CandidateBaselineMemory::default, |_| {
                    CandidateBaselineMemory::scalar()
                }),
        )
    }

    fn assert_metadata_oracle(tree: &TransactionalTree) {
        assert_eq!(tree.metadata_memory(), metadata_oracle(tree));
    }

    fn root_units(root: &Option<OwnedRootV1>) -> usize {
        root.as_ref().map_or(0, |root| {
            let root = root.value();
            root.children.len()
                + usize::from(root.vault_id.capacity() > 0)
                + usize::from(root.device_id.capacity() > 0)
        })
    }

    #[tokio::test]
    async fn tree_retirement_v1_counts_every_resident_candidate_owner_once() {
        let mut tree = TransactionalTree::new("vault", "device");
        let entries = (0u32..1_003)
            .map(|index| {
                FileEntry::new(
                    format!("notes/{index:04}.md"),
                    hash_bytes(&index.to_le_bytes()),
                    1,
                    1,
                )
            })
            .collect();
        tree.rebuild(entries).await.unwrap();
        tree.store
            .insert_chunk(hash_bytes(b"orphan"), b"orphan".to_vec());
        tree.begin_candidate().unwrap();
        tree.apply_candidate(
            &[FileEntry::new(
                "new/path.md".into(),
                hash_bytes(b"new"),
                2,
                2,
            )],
            &[],
        )
        .await
        .unwrap();
        let expected = root_units(&tree.committed_root)
            + root_units(&tree.candidate_root)
            + tree.store.len()
            + 2;
        let committed_children = tree
            .committed_root
            .as_ref()
            .unwrap()
            .value()
            .children
            .as_ptr();
        let candidate_children = tree
            .candidate_root
            .as_ref()
            .unwrap()
            .value()
            .children
            .as_ptr();
        let vault = tree.vault_id.as_ptr();
        let mut retirement = tree.into_retirement();
        assert_eq!(
            retirement
                .committed_root
                .as_ref()
                .unwrap()
                .value()
                .children
                .as_ptr(),
            committed_children
        );
        assert_eq!(
            retirement
                .candidate_root
                .as_ref()
                .unwrap()
                .value()
                .children
                .as_ptr(),
            candidate_children
        );
        assert_eq!(
            retirement.vault_id.as_ptr(),
            vault,
            "conversion cloned owner metadata"
        );
        let mut units = 0;
        while retirement.retire_one() {
            units += 1;
            assert!(units <= expected);
        }
        assert_eq!(units, expected);
        assert!(retirement.committed_root.is_none() && retirement.candidate_root.is_none());
        assert!(retirement.candidate_start_chunks.is_none());
        assert_eq!(retirement.chunks.len(), 0);
        assert!(!retirement.retire_one());
    }

    #[tokio::test]
    async fn metadata_sidecars_follow_v1_candidate_mutation_commit_and_abort() {
        let mut tree = TransactionalTree::new("metadata-vault", "metadata-device");
        assert_metadata_oracle(&tree);
        tree.rebuild(
            (0u32..1_003)
                .map(|index| {
                    FileEntry::new(
                        format!("dir-{index:04}/note.md"),
                        hash_bytes(&index.to_le_bytes()),
                        index as u64,
                        1,
                    )
                })
                .collect(),
        )
        .await
        .unwrap();
        assert_metadata_oracle(&tree);

        tree.begin_candidate().unwrap();
        assert_metadata_oracle(&tree);
        assert!(tree.metadata_memory().candidate_baseline.present);
        assert_ne!(
            tree.committed_root
                .as_ref()
                .unwrap()
                .value()
                .children
                .as_ptr(),
            tree.candidate_root
                .as_ref()
                .unwrap()
                .value()
                .children
                .as_ptr(),
            "candidate clone reused the committed Vec owner"
        );
        tree.apply_candidate(
            &[FileEntry::new(
                "new-prefix/new.md".into(),
                hash_bytes(b"direct"),
                9_999,
                6,
            )],
            &[],
        )
        .await
        .unwrap();
        assert_metadata_oracle(&tree);
        let committed_before_abort = tree.committed_root.as_ref().unwrap().metadata();
        tree.abort_candidate().unwrap();
        assert_metadata_oracle(&tree);
        assert_eq!(
            tree.committed_root.as_ref().unwrap().metadata(),
            committed_before_abort
        );

        tree.begin_candidate().unwrap();
        let mut mutation = tree
            .begin_candidate_mutation(
                vec![FileEntry::new(
                    "stepped/publish.md".into(),
                    hash_bytes(b"stepped"),
                    10_000,
                    7,
                )],
                Vec::new(),
            )
            .unwrap()
            .unwrap();
        let before_steps = tree.metadata_memory();
        loop {
            let progress = tree.step_candidate_mutation(&mut mutation, 256).unwrap();
            if progress.done {
                break;
            }
            assert!(progress.units > 0);
        }
        assert_eq!(tree.metadata_memory(), before_steps);
        tree.finish_candidate_mutation(&mut mutation).unwrap();
        assert_metadata_oracle(&tree);
        let candidate_before_commit = tree.candidate_root.as_ref().unwrap().metadata();
        tree.commit_candidate().unwrap();
        assert_metadata_oracle(&tree);
        assert_eq!(
            tree.committed_root.as_ref().unwrap().metadata(),
            candidate_before_commit,
            "commit remeasured instead of moving the candidate owner"
        );
        assert_eq!(
            tree.metadata_memory().candidate_baseline,
            CandidateBaselineMemory::default()
        );
    }

    #[tokio::test]
    async fn metadata_retirement_v1_keeps_vec_backing_and_retires_scalar_baseline() {
        let mut tree = TransactionalTree::new("retirement-vault", "retirement-device");
        tree.rebuild(
            (0u32..2_000)
                .map(|index| {
                    FileEntry::new(
                        format!("wide/{index:08}.md"),
                        hash_bytes(&index.to_le_bytes()),
                        index as u64,
                        1,
                    )
                })
                .collect(),
        )
        .await
        .unwrap();
        tree.begin_candidate().unwrap();
        assert_metadata_oracle(&tree);
        let resident = tree.metadata_memory();
        assert!(resident.committed.v1_children_backing_capacity_bytes > 0);
        assert_eq!(
            resident.candidate_baseline,
            CandidateBaselineMemory::scalar()
        );

        let mut retirement = tree.into_retirement();
        assert_eq!(retirement.metadata_memory(), resident);
        let committed_vec_capacity = resident.committed.v1_children_backing_capacity_bytes;
        let mut saw_scalar_baseline_retired = false;
        let mut previous = retirement.metadata_memory();
        let mut units = 0usize;
        loop {
            let worked = retirement.retire_one();
            let current = retirement.metadata_memory();
            assert!(current.counters_valid);
            assert!(current.committed.v1_children_length <= previous.committed.v1_children_length);
            if current.committed.roots > 0 {
                assert_eq!(
                    current.committed.v1_children_backing_capacity_bytes, committed_vec_capacity,
                    "drained v1 Vec backing was released before its root owner"
                );
            }
            if previous.candidate_baseline.present && !current.candidate_baseline.present {
                saw_scalar_baseline_retired = true;
            }
            previous = current;
            if !worked {
                break;
            }
            units += 1;
            assert!(units < 100_000, "v1 metadata retirement did not converge");
        }
        assert!(saw_scalar_baseline_retired);
        assert_eq!(
            retirement.metadata_memory().candidate_baseline,
            CandidateBaselineMemory::default()
        );
        assert!(!retirement.retire_one());
    }

    #[test]
    fn tree_retirement_v1_moves_node_buffers_without_copying() {
        let tree = TransactionalTree::new("vault", "device");
        let bytes = vec![7; 64 * 1024];
        let pointer = bytes.as_ptr();
        tree.store.insert_chunk(hash_bytes(&bytes), bytes);
        let mut retirement = tree.into_retirement();
        let (_, bytes) = retirement.chunks.next().unwrap();
        assert_eq!(
            bytes.as_ptr(),
            pointer,
            "conversion copied the resident graph"
        );
        drop(bytes);
        assert!(retirement.retire_one()); // outer vault identity
        assert!(retirement.retire_one()); // outer device identity
        assert!(!retirement.retire_one());
    }

    #[test]
    fn reachability_retirement_v1_drains_expansion_pending_and_marked_descriptors() {
        let mut cursor = ReachabilityCursor::new();
        for index in 0u32..3 {
            cursor
                .seed_root_child(hash_bytes(&index.to_le_bytes()))
                .unwrap();
        }
        cursor.expanding = Some(ChildExpansion {
            children: (0u32..1_000)
                .map(|index| (format!("prefix/{index}"), hash_bytes(&index.to_le_bytes())))
                .collect(),
            depth: 1,
        });
        cursor.reachable = (0u32..100)
            .map(|index| hash_bytes(&index.to_le_bytes()))
            .collect();
        assert!(cursor.retire_one());
        assert_eq!(cursor.expanding.as_ref().unwrap().children.len(), 999);
        assert!(cursor.seed_root_child([0; 32]).is_err());
        assert!(cursor.step(&MemoryChunkStore::new(), 1).is_err());
        assert!(!cursor.progress(0).done);
        let mut units = 1;
        while cursor.retire_one() {
            units += 1;
        }
        assert_eq!(units, 1_103);
        assert!(
            cursor.pending.is_empty() && cursor.expanding.is_none() && cursor.reachable.is_empty()
        );
        assert_eq!(cursor.retiring_hashes.as_ref().unwrap().len(), 0);
        assert!(!cursor.retire_one());
        assert!(cursor.finish().is_err());
    }
}

/// Local immutable Merkle graph with an explicit candidate transaction.
/// Root pointers are cheap clones; chunk bytes are shared by content hash.
pub struct TransactionalTree {
    committed_root: Option<OwnedRootV1>,
    candidate_root: Option<OwnedRootV1>,
    candidate_start_chunks: Option<u64>,
    store: MemoryChunkStore,
    vault_id: String,
    device_id: String,
}

/// Owned retirement of a resident graph. Construction only moves owners;
/// each call drops one root child/identity, baseline hash, or node buffer.
/// One node buffer can be large in v1; hash-iterator scans and drained map/
/// vector backing deallocation remain explicit synchronous residuals.
pub(crate) struct TreeRetirementV1 {
    committed_root: Option<OwnedRootV1>,
    candidate_root: Option<OwnedRootV1>,
    candidate_start_chunks: Option<u64>,
    candidate_start_memory: CandidateBaselineMemory,
    chunks: ChunkRetirement,
    vault_id: String,
    device_id: String,
}

fn retire_owned_root_one(root: &mut Option<OwnedRootV1>) -> bool {
    let Some(owner) = root else {
        return false;
    };
    if owner.retire_child() || owner.retire_vault() || owner.retire_device() {
        return true;
    }
    // The drained Vec backing and zero-capacity String descriptors stay
    // charged until this exact owner is destroyed.
    *root = None;
    false
}

pub(crate) fn retire_string_one(value: &mut String) -> bool {
    if value.capacity() > 0 {
        drop(std::mem::take(value));
        true
    } else {
        false
    }
}

impl TreeRetirementV1 {
    pub(crate) fn chunk_memory(&self) -> ChunkStoreMemory {
        self.chunks.memory_summary()
    }

    pub(crate) fn metadata_memory(&self) -> TreeMetadataMemory {
        TreeMetadataMemory::new(
            self.committed_root
                .as_ref()
                .map_or_else(RootMetadataMemory::default, OwnedRootV1::metadata),
            self.candidate_root
                .as_ref()
                .map_or_else(RootMetadataMemory::default, OwnedRootV1::metadata),
            StringMemory::of(&self.vault_id).combine(StringMemory::of(&self.device_id)),
            self.candidate_start_memory,
        )
    }

    pub(crate) fn retire_one(&mut self) -> bool {
        if retire_owned_root_one(&mut self.committed_root)
            || retire_owned_root_one(&mut self.candidate_root)
        {
            return true;
        }
        if self.candidate_start_chunks.take().is_some() {
            self.candidate_start_memory = CandidateBaselineMemory::default();
        }
        if self.chunks.next().is_some()
            || retire_string_one(&mut self.vault_id)
            || retire_string_one(&mut self.device_id)
        {
            return true;
        }
        false
    }
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

    pub(crate) fn into_retirement(self) -> TreeRetirementV1 {
        let candidate_start_chunks = self.candidate_start_chunks;
        let candidate_start_memory = candidate_start_chunks
            .map_or_else(CandidateBaselineMemory::default, |_| {
                CandidateBaselineMemory::scalar()
            });
        TreeRetirementV1 {
            committed_root: self.committed_root,
            candidate_root: self.candidate_root,
            candidate_start_chunks,
            candidate_start_memory,
            chunks: self.store.into_chunks(),
            vault_id: self.vault_id,
            device_id: self.device_id,
        }
    }

    /// Install an exclusively owned replacement whose complete closure was
    /// already checked by ReachabilityCursor. No traversal, sweep or live-tree
    /// mutation occurs here. Only the private rebuild owner may use this
    /// constructor; unvalidated roots must use the normal validation paths.
    pub(crate) fn from_verified_replacement(root: OwnedRootV1, store: MemoryChunkStore) -> Self {
        Self {
            vault_id: root.value().vault_id.clone(),
            device_id: root.value().device_id.clone(),
            committed_root: Some(root),
            candidate_root: None,
            candidate_start_chunks: None,
            store,
        }
    }

    pub fn load_root_without_chunks(&mut self, root: RootNode) {
        self.committed_root = Some(OwnedRootV1::measured(root));
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
        self.committed_root = Some(OwnedRootV1::measured(replacement_root));
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
        mark_reachable_chunks(&self.store, [committed.value()])?;
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
        let exact = |value: usize| {
            u64::try_from(value).map_err(|_| state_error("candidate root request overflow"))
        };
        let add = |left: u64, right: u64| {
            left.checked_add(right)
                .ok_or_else(|| state_error("candidate root request overflow"))
        };
        let identity = add(
            exact(root.value().vault_id.len())?,
            exact(root.value().device_id.len())?,
        )?;
        let metadata = root.metadata();
        let string_count = root
            .value()
            .children
            .len()
            .checked_add(2)
            .ok_or_else(|| state_error("candidate root string count overflow"))?;
        if !metadata.counters_valid
            || !metadata.strings.counters_valid
            || metadata.strings.strings != string_count
        {
            return Err(state_error("candidate root memory counters are invalid"));
        }
        let child_strings = metadata
            .strings
            .length_bytes
            .checked_sub(identity)
            .ok_or_else(|| state_error("candidate root memory counters are invalid"))?;
        let child = exact(root.value().children.len())?
            .checked_mul(exact(std::mem::size_of::<(String, FileHash)>())?)
            .and_then(|backing| backing.checked_add(child_strings))
            .ok_or_else(|| state_error("candidate root request overflow"))?;
        let total = identity
            .checked_add(child)
            .filter(|value| *value <= MAX_SAFE_OUTPUT_SETTLEMENT_BYTES)
            .ok_or_else(|| state_error("candidate root request is not JS-exact"))?;
        let resident = self.store.len();
        if u64::try_from(resident)
            .ok()
            .is_none_or(|count| count > MAX_SAFE_OUTPUT_SETTLEMENT_BYTES)
        {
            return Err(state_error("candidate resident count is not JS-exact"));
        }
        Ok(CandidateOpenMemoryPlanV1 {
            schema: 1,
            scope: "v1-candidate-open-root",
            resident_chunk_count: resident,
            root_string_count: string_count,
            root_identity_requested_bytes: identity,
            // The stable cross-version ABI names this endpoint bytes. For v1
            // it covers every non-identity root request: child-prefix Strings
            // plus the fixed-width children Vec backing allocation.
            root_endpoint_requested_bytes: child,
            root_string_requested_bytes: total,
            baseline_key_snapshot_requested_bytes: 0,
            peak_admission_bytes: total,
            baseline_strategy: "insertion-generation-v1",
        })
    }

    pub(crate) fn prepare_candidate_root(&self) -> Result<OwnedRootV1, ChunkError> {
        self.candidate_open_memory_plan()?;
        let root = self
            .committed_root
            .as_ref()
            .ok_or_else(|| state_error("no committed root"))?
            .value();
        let copy = |value: &str| -> Result<String, ChunkError> {
            let mut result = String::new();
            result
                .try_reserve_exact(value.len())
                .map_err(|_| state_error("candidate root string allocation failed"))?;
            result.push_str(value);
            Ok(result)
        };
        let vault_id = copy(&root.vault_id)?;
        let device_id = copy(&root.device_id)?;
        let mut child_strings = StringMemory::default();
        let mut children = Vec::new();
        children
            .try_reserve_exact(root.children.len())
            .map_err(|_| state_error("candidate root children allocation failed"))?;
        for (prefix, hash) in &root.children {
            let prefix = copy(prefix)?;
            child_strings.add(&prefix);
            children.push((prefix, *hash));
        }
        Ok(OwnedRootV1::metered(
            RootNode {
                vault_id,
                created_ms: root.created_ms,
                version: root.version,
                children,
                total_files: root.total_files,
                parent_hash: root.parent_hash,
                device_id,
            },
            child_strings,
        ))
    }

    /// Every fallible root allocation and baseline preflight precedes the
    /// publication cut. The prepared root is moved, never cloned, on success.
    pub(crate) fn open_prepared_candidate(
        &mut self,
        prepared: &mut Option<OwnedRootV1>,
    ) -> Result<(), ChunkError> {
        if self.has_candidate() {
            return Err(state_error("candidate already active"));
        }
        let candidate_root = prepared
            .as_ref()
            .ok_or_else(|| state_error("candidate root is not prepared"))?;
        let matches_committed = self.committed_root.as_ref().is_some_and(|committed| {
            let left = committed.value();
            let right = candidate_root.value();
            left.vault_id == right.vault_id
                && left.created_ms == right.created_ms
                && left.version == right.version
                && left.children == right.children
                && left.total_files == right.total_files
                && left.parent_hash == right.parent_hash
                && left.device_id == right.device_id
        });
        if !matches_committed {
            return Err(state_error(
                "prepared candidate root no longer matches committed state",
            ));
        }
        let cut = self.store.candidate_baseline_preflight()?;
        self.store.open_candidate_baseline(cut)?;
        self.candidate_root = prepared.take();
        self.candidate_start_chunks = Some(cut);
        Ok(())
    }

    pub(crate) fn step_candidate_validation(
        &self,
        cursor: &mut ReachabilityCursor,
        max_units: usize,
    ) -> Result<ReachabilityProgress, ChunkError> {
        cursor.step(&self.store, max_units)
    }

    pub async fn apply_candidate(
        &mut self,
        changed: &[FileEntry],
        deleted: &[String],
    ) -> Result<(), ChunkError> {
        let root = self
            .candidate_root
            .as_ref()
            .ok_or_else(|| state_error("no active candidate"))?;
        if changed.is_empty() && deleted.is_empty() {
            return Ok(());
        }
        let updated = crate::tree::update_tree(&self.store, root.value(), changed, deleted).await?;
        self.candidate_root = Some(OwnedRootV1::measured(updated));
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn begin_candidate_mutation(
        &self,
        changed: Vec<FileEntry>,
        deleted: Vec<String>,
    ) -> Result<Option<CandidateMutationV1>, ChunkError> {
        if changed.is_empty() && deleted.is_empty() {
            return Ok(None);
        }
        Ok(Some(CandidateMutationV1::new(
            self.prepare_candidate_mutation_root()?,
            changed,
            deleted,
        )))
    }

    /// Clone the exact current candidate root only after the host has admitted
    /// the V1 mutation root plan. Every fallible allocation precedes moving the
    /// parsed operation owner into the mutation cursor, so allocation failure
    /// leaves that same operation owner retryable.
    pub(crate) fn prepare_candidate_mutation_root(&self) -> Result<RootNode, ChunkError> {
        let root = self
            .candidate_root
            .as_ref()
            .ok_or_else(|| state_error("no active candidate"))?
            .value();
        let copy = |value: &str| -> Result<String, ChunkError> {
            let mut result = String::new();
            result
                .try_reserve_exact(value.len())
                .map_err(|_| state_error("candidate mutation root string allocation failed"))?;
            result.push_str(value);
            Ok(result)
        };
        let vault_id = copy(&root.vault_id)?;
        let device_id = copy(&root.device_id)?;
        let mut children = Vec::new();
        children
            .try_reserve_exact(root.children.len())
            .map_err(|_| state_error("candidate mutation root children allocation failed"))?;
        for (prefix, hash) in &root.children {
            children.push((copy(prefix)?, *hash));
        }
        Ok(RootNode {
            vault_id,
            created_ms: root.created_ms,
            version: root.version,
            children,
            total_files: root.total_files,
            parent_hash: root.parent_hash,
            device_id,
        })
    }

    pub(crate) fn step_candidate_mutation(
        &self,
        cursor: &mut CandidateMutationV1,
        max_units: usize,
    ) -> Result<CandidateMutationProgress, ChunkError> {
        cursor.step(&self.store, max_units)
    }

    pub(crate) fn finish_candidate_mutation(
        &mut self,
        cursor: &mut CandidateMutationV1,
    ) -> Result<(), ChunkError> {
        if self.candidate_root.is_none() {
            return Err(state_error("no active candidate"));
        }
        let root = cursor.finish_with_clock(&self.store, crate::tree::now_ms)?;
        self.candidate_root = Some(OwnedRootV1::measured(root));
        Ok(())
    }

    pub fn commit_candidate(&mut self) -> Result<ChunkGcStats, ChunkError> {
        let candidate = self
            .candidate_root
            .as_ref()
            .ok_or_else(|| state_error("no active candidate"))?;
        let reachable = mark_reachable_chunks(&self.store, [candidate.value()])?;
        self.store.close_candidate_baseline(
            self.candidate_start_chunks
                .ok_or_else(|| state_error("candidate baseline is missing"))?,
        )?;
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
            Some(root) => mark_reachable_chunks(&self.store, [root.value()])?,
            None => HashSet::new(),
        };
        self.store.close_candidate_baseline(
            self.candidate_start_chunks
                .ok_or_else(|| state_error("candidate baseline is missing"))?,
        )?;
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
        self.committed_root.as_ref().map(OwnedRootV1::value)
    }

    pub fn candidate_root(&self) -> Option<&RootNode> {
        self.candidate_root.as_ref().map(OwnedRootV1::value)
    }

    pub fn committed_root_hash(&self) -> Option<FileHash> {
        self.committed_root.as_ref().map(|root| root.value().hash())
    }

    pub fn candidate_root_hash(&self) -> Option<FileHash> {
        self.candidate_root.as_ref().map(|root| root.value().hash())
    }

    pub fn committed_chunk_hashes(&self) -> Result<Vec<FileHash>, ChunkError> {
        let root = self
            .committed_root
            .as_ref()
            .ok_or_else(|| state_error("no committed root"))?;
        sorted_hashes(mark_reachable_chunks(&self.store, [root.value()])?)
    }

    pub fn candidate_chunk_hashes(&self) -> Result<Vec<FileHash>, ChunkError> {
        let root = self
            .candidate_root
            .as_ref()
            .ok_or_else(|| state_error("no active candidate"))?;
        sorted_hashes(mark_reachable_chunks(&self.store, [root.value()])?)
    }

    pub fn new_candidate_chunk_hashes(&self) -> Result<Vec<FileHash>, ChunkError> {
        let root = self
            .candidate_root
            .as_ref()
            .ok_or_else(|| state_error("no active candidate"))?;
        self.candidate_chunk_plan_from_reachable(mark_reachable_chunks(
            &self.store,
            [root.value()],
        )?)
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

    pub fn store_len(&self) -> usize {
        self.store.len()
    }

    pub(crate) fn chunk_memory(&self) -> ChunkStoreMemory {
        self.store.memory_summary()
    }

    pub(crate) fn metadata_memory(&self) -> TreeMetadataMemory {
        TreeMetadataMemory::new(
            self.committed_root
                .as_ref()
                .map_or_else(RootMetadataMemory::default, OwnedRootV1::metadata),
            self.candidate_root
                .as_ref()
                .map_or_else(RootMetadataMemory::default, OwnedRootV1::metadata),
            StringMemory::of(&self.vault_id).combine(StringMemory::of(&self.device_id)),
            self.candidate_start_chunks
                .map_or_else(CandidateBaselineMemory::default, |_| {
                    CandidateBaselineMemory::scalar()
                }),
        )
    }

    #[cfg(test)]
    pub(crate) fn resident_store_for_test(&self) -> &MemoryChunkStore {
        &self.store
    }
}

pub(crate) fn sorted_hashes(hashes: HashSet<FileHash>) -> Result<Vec<FileHash>, ChunkError> {
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
    async fn reachability_cursor_matches_complete_mark_at_one_unit() {
        let mut rows = Vec::new();
        for index in 0..2_500u64 {
            rows.push(entry(&format!("wide/{index:05}.md"), index));
        }
        rows.push(entry("other/a.md", 1));
        let store = MemoryChunkStore::new();
        let root = build_tree(&store, rows, "vault", "device").await.unwrap();
        let expected = mark_reachable_chunks(&store, [&root]).unwrap();

        let mut cursor = ReachabilityCursor::new();
        for (_, hash) in &root.children {
            cursor.seed_root_child(*hash).unwrap();
        }
        cursor.finish_seeding();
        assert!(cursor.step(&store, 0).is_err());
        let mut steps = 0usize;
        while !cursor.progress(0).done {
            let progress = cursor.step(&store, 1).unwrap();
            assert_eq!(progress.units, 1);
            steps += 1;
        }
        let actual = cursor.finish().unwrap();
        assert_eq!(actual, expected);
        assert!(
            steps > actual.len(),
            "child expansion was not separately stepped"
        );
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
    async fn candidate_chunk_plan_preserves_all_resident_baseline() {
        let mut tree = TransactionalTree::new("vault", "device");
        tree.rebuild(vec![entry("file.md", 1)]).await.unwrap();

        // Seed the exact future leaf as unreachable history before candidate
        // open. Legacy semantics treat every resident hash as already known.
        let future_store = MemoryChunkStore::new();
        let future_root = build_tree(&future_store, vec![entry("file.md", 2)], "vault", "device")
            .await
            .unwrap();
        let future_hash = future_root.children[0].1;
        tree.store
            .insert_chunk(future_hash, future_store.get_chunk(&future_hash).unwrap());

        tree.begin_candidate().unwrap();
        tree.apply_candidate(&[entry("file.md", 2)], &[])
            .await
            .unwrap();
        let root = tree.candidate_root().unwrap();
        let reachable = mark_reachable_chunks(&tree.store, [root]).unwrap();
        let (all, fresh) = tree.candidate_chunk_plan_from_reachable(reachable).unwrap();

        assert_eq!(all, tree.candidate_chunk_hashes().unwrap());
        assert_eq!(fresh, tree.new_candidate_chunk_hashes().unwrap());
        assert_eq!(all, vec![future_hash]);
        assert!(fresh.is_empty(), "resident orphan was treated as fresh");
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
