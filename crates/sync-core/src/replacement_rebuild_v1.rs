//! Private full v1 replacement, not a mutation of the resident tree.
//!
//! Grouping, stable sorting, row/child movement and root seeding are scheduled
//! units. Each step stops after one node codec/hash or one validation graph
//! edge, even when the caller allows many cheap units. A v1 node's bytes are
//! NOT capped here: its legacy 1000-row leaf boundary must remain unchanged.
//! Allocation/reallocation, string comparisons/copies, one node codec/hash,
//! and drained backing-allocation destructor work remain synchronous residuals.
//! Explicit retirement releases populated fields one cleanup unit at a time;
//! directly dropping a non-retired cursor still drops its remaining owners.
//! This is not
//! a wall-clock, native heap/RSS, or durable-storage bound.

use crate::chunk::{ChunkError, FileEntry, InternalNode, LeafChunk, RootNode};
use crate::hash::{hash_bytes, FileHash};
use crate::replacement_rebuild::{
    ReplacementBuildProgress, ReplacementOutputMemoryPlanV1, StableSort,
};
use crate::store::{ChunkRetirement, ChunkStoreMemory, MemoryChunkStore};
use crate::transactional_tree::{ReachabilityCursor, TransactionalTree, TreeRetirementV1};
use crate::tree::TARGET_CHUNK_ENTRIES;
use crate::tree_metadata::{OwnedRootV1, TreeMetadataMemory};
use crate::tree_work_memory::{
    ReplacementInputMemory, ReplacementSortMemory, ReplacementV1EntriesMemory,
    ReplacementV1GraphMemory, VecBackingMemory,
};
use std::collections::{btree_map, BTreeMap};
use std::vec::IntoIter;

type Children = Vec<(String, FileHash)>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    PlanReady,
    Group,
    NextGroup,
    SortRows,
    MoveLeaf,
    EncodeLeaf,
    RetireLeaf,
    MoveChildren,
    SortChildren,
    EncodeInternal,
    RetireChildren,
    SeedRoot,
    Validate,
    Ready,
    Failed,
    Retiring,
}

fn invalid(message: &str) -> ChunkError {
    ChunkError::Deserialize(message.into())
}

pub(crate) struct ReplacementRebuildV1 {
    phase: Phase,
    completed: usize,
    input: Option<IntoIter<FileEntry>>,
    groups: BTreeMap<String, Vec<FileEntry>>,
    prefix: Option<String>,
    row_sort: Option<StableSort<FileEntry>>,
    rows: Option<IntoIter<FileEntry>>,
    leaf: Option<Vec<FileEntry>>,
    retiring_rows: Option<IntoIter<FileEntry>>,
    entry_memory: ReplacementV1EntriesMemory,
    graph_memory: ReplacementV1GraphMemory,
    output_memory_plan: ReplacementOutputMemoryPlanV1,
    leaf_hashes: Vec<FileHash>,
    hashes: Option<IntoIter<FileHash>>,
    children: Children,
    child_sort: Option<StableSort<(String, FileHash)>>,
    retiring_children: Option<IntoIter<(String, FileHash)>>,
    internal_hash: Option<FileHash>,
    root_children: Children,
    total_files: u64,
    vault_id: Option<String>,
    device_id: Option<String>,
    root: Option<OwnedRootV1>,
    store: MemoryChunkStore,
    validation: Option<ReachabilityCursor>,
    seeded: usize,
    result: Option<TransactionalTree>,
    retiring_chunks: Option<ChunkRetirement>,
    retiring_result: Option<TreeRetirementV1>,
    #[cfg(test)]
    atomic_steps: usize,
}

impl ReplacementRebuildV1 {
    /// Only immutable-node containers, not rows/sort/root/closure metadata.
    /// Ready and partially retired cursors can own these through different
    /// fields; account every actual owner without walking any graph.
    pub(crate) fn chunk_memory(&self) -> ChunkStoreMemory {
        self.store
            .memory_summary()
            .combine(
                self.result
                    .as_ref()
                    .map_or_else(ChunkStoreMemory::default, TransactionalTree::chunk_memory),
            )
            .combine(
                self.retiring_chunks
                    .as_ref()
                    .map_or_else(ChunkStoreMemory::default, ChunkRetirement::memory_summary),
            )
            .combine(
                self.retiring_result
                    .as_ref()
                    .map_or_else(ChunkStoreMemory::default, TreeRetirementV1::chunk_memory),
            )
    }

    /// Exact top-level Vec backing for the currently active row or child
    /// indirect sorter. All other builder containers and nested strings are
    /// outside this additive component.
    pub(crate) fn sort_memory(&self) -> ReplacementSortMemory {
        debug_assert!(self.row_sort.is_none() || self.child_sort.is_none());
        if let Some(sorter) = &self.row_sort {
            return ReplacementSortMemory::entries(sorter.memory());
        }
        if let Some(sorter) = &self.child_sort {
            return ReplacementSortMemory::children(sorter.memory());
        }
        ReplacementSortMemory::default()
    }

    pub(crate) fn entry_memory(&self) -> ReplacementV1EntriesMemory {
        self.entry_memory
    }

    pub(crate) fn graph_memory(&self) -> ReplacementV1GraphMemory {
        self.graph_memory
    }

    /// O(1) ownership transfer of entries. Identity copies are deferred until
    /// the output-memory witness is admitted.
    #[cfg(test)]
    pub(crate) fn new(entries: Vec<FileEntry>, vault_id: &str, device_id: &str) -> Self {
        let entry_memory = ReplacementInputMemory::new(&entries);
        Self::new_metered(entries, entry_memory, vault_id, device_id)
    }

    /// Carry the already exact accepted-input sidecar into the V1 builder;
    /// production never rescans every nested path allocation at this boundary.
    #[cfg(test)]
    pub(crate) fn new_metered(
        entries: Vec<FileEntry>,
        entry_memory: ReplacementInputMemory,
        vault_id: &str,
        device_id: &str,
    ) -> Self {
        let input_json_bytes = entries
            .iter()
            .try_fold(2usize, |total, entry| {
                total
                    .checked_add(entry.path.len())
                    .and_then(|value| value.checked_add(160))
            })
            .unwrap_or(usize::MAX);
        let mut result = Self::new_metered_with_output_plan(
            entries,
            entry_memory,
            vault_id,
            device_id,
            input_json_bytes,
        );
        let plan = result.output_memory_plan;
        result
            .resume_after_output_memory_plan_v1(
                plan.node_payload_bytes,
                plan.range_endpoint_peak_requested_bytes,
                plan.range_endpoint_resident_requested_bytes,
                vault_id,
                device_id,
            )
            .expect("test/legacy Tree v1 output plan must be internally consistent");
        result
    }

    /// Production constructor: preserve the pre-output barrier until the host
    /// has admitted the returned conservative witness.
    pub(crate) fn new_metered_with_output_plan(
        entries: Vec<FileEntry>,
        mut entry_memory: ReplacementInputMemory,
        vault_id: &str,
        device_id: &str,
        input_json_bytes: usize,
    ) -> Self {
        entry_memory.transferred(&entries);
        let output_memory_plan = Self::conservative_output_plan(
            entries.len(),
            input_json_bytes,
            vault_id.len(),
            device_id.len(),
        )
        .unwrap_or(ReplacementOutputMemoryPlanV1 {
            node_payload_bytes: u64::MAX,
            range_endpoint_peak_requested_bytes: u64::MAX,
            range_endpoint_resident_requested_bytes: u64::MAX,
            peak_admission_bytes: u64::MAX,
            resident_admission_bytes: u64::MAX,
        });
        Self {
            phase: Phase::PlanReady,
            completed: 0,
            input: Some(entries.into_iter()),
            groups: BTreeMap::new(),
            prefix: None,
            row_sort: None,
            rows: None,
            leaf: None,
            retiring_rows: None,
            entry_memory: ReplacementV1EntriesMemory::from_input(entry_memory),
            graph_memory: ReplacementV1GraphMemory::default(),
            output_memory_plan,
            leaf_hashes: Vec::new(),
            hashes: None,
            children: Vec::new(),
            child_sort: None,
            retiring_children: None,
            internal_hash: None,
            root_children: Vec::new(),
            total_files: 0,
            vault_id: None,
            device_id: None,
            root: None,
            store: MemoryChunkStore::new(),
            validation: Some(ReachabilityCursor::new()),
            seeded: 0,
            result: None,
            retiring_chunks: None,
            retiring_result: None,
            #[cfg(test)]
            atomic_steps: 0,
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.phase == Phase::Ready
    }

    fn conservative_output_plan(
        entry_count: usize,
        input_json_bytes: usize,
        vault_id_bytes: usize,
        device_id_bytes: usize,
    ) -> Result<ReplacementOutputMemoryPlanV1, ChunkError> {
        const MAX_SAFE_INTEGER: u64 = (1u64 << 53) - 1;
        let entries = u64::try_from(entry_count)
            .map_err(|_| invalid("Tree v1 output entry count overflow"))?;
        let input = u64::try_from(input_json_bytes)
            .map_err(|_| invalid("Tree v1 output input byte count overflow"))?;
        let identities = u64::try_from(vault_id_bytes)
            .ok()
            .and_then(|value| {
                u64::try_from(device_id_bytes)
                    .ok()
                    .and_then(|other| value.checked_add(other))
            })
            .ok_or_else(|| invalid("Tree v1 output identity byte count overflow"))?;

        // JSON already contains every path, a 64-byte hex hash and all scalar
        // fields. The per-row allowance covers FlatBuffer layout plus the
        // worst-case one-leaf/one-internal-node topology. Ready validates the
        // actual retained component against this conservative witness.
        let node_payload_bytes = input
            .checked_mul(2)
            .and_then(|bytes| {
                entries
                    .checked_mul(256)
                    .and_then(|extra| bytes.checked_add(extra))
            })
            .and_then(|bytes| bytes.checked_add(128 * 1024))
            .filter(|bytes| *bytes <= MAX_SAFE_INTEGER)
            .ok_or_else(|| invalid("Tree v1 output node byte count overflow"))?;
        // Every root label is a source prefix. Factor two covers its String
        // request and geometric Vec backing; identities and an empty-root
        // allowance are charged independently.
        let root_requested_bytes = input
            .checked_mul(2)
            .and_then(|bytes| {
                entries
                    .checked_mul(128)
                    .and_then(|extra| bytes.checked_add(extra))
            })
            .and_then(|bytes| {
                identities
                    .checked_mul(4)
                    .and_then(|extra| bytes.checked_add(extra))
            })
            .and_then(|bytes| bytes.checked_add(128 * 1024))
            .filter(|bytes| *bytes <= MAX_SAFE_INTEGER)
            .ok_or_else(|| invalid("Tree v1 output root byte count overflow"))?;
        let total = node_payload_bytes
            .checked_add(root_requested_bytes)
            .filter(|bytes| *bytes <= MAX_SAFE_INTEGER)
            .ok_or_else(|| invalid("Tree v1 output admission byte count overflow"))?;
        Ok(ReplacementOutputMemoryPlanV1 {
            node_payload_bytes,
            range_endpoint_peak_requested_bytes: root_requested_bytes,
            range_endpoint_resident_requested_bytes: root_requested_bytes,
            peak_admission_bytes: total,
            resident_admission_bytes: total,
        })
    }

    pub(crate) fn output_memory_plan_v1(
        &self,
    ) -> Result<ReplacementOutputMemoryPlanV1, ChunkError> {
        if self.phase != Phase::PlanReady {
            return Err(invalid(
                "Tree v1 replacement output memory plan is not ready",
            ));
        }
        if self.output_memory_plan.peak_admission_bytes > (1u64 << 53) - 1 {
            return Err(invalid("Tree v1 replacement output memory plan overflow"));
        }
        Ok(self.output_memory_plan)
    }

    pub(crate) fn resume_after_output_memory_plan_v1(
        &mut self,
        expected_node_payload_bytes: u64,
        expected_root_peak_requested_bytes: u64,
        expected_root_resident_requested_bytes: u64,
        vault_id: &str,
        device_id: &str,
    ) -> Result<(), ChunkError> {
        let expected = self.output_memory_plan_v1()?;
        if expected_node_payload_bytes != expected.node_payload_bytes
            || expected_root_peak_requested_bytes != expected.range_endpoint_peak_requested_bytes
            || expected_root_resident_requested_bytes
                != expected.range_endpoint_resident_requested_bytes
        {
            return Err(invalid(
                "Tree v1 replacement output memory plan witness changed",
            ));
        }
        let exact_copy = |value: &str| -> Result<String, ChunkError> {
            let mut owned = String::new();
            owned
                .try_reserve_exact(value.len())
                .map_err(|_| invalid("Tree v1 replacement identity allocation failed"))?;
            owned.push_str(value);
            Ok(owned)
        };
        // Both allocations are prepared before either is installed. Failure
        // therefore leaves PlanReady and the same input owner retryable.
        let vault_id = exact_copy(vault_id)?;
        let device_id = exact_copy(device_id)?;
        self.graph_memory = ReplacementV1GraphMemory::new(&vault_id, &device_id);
        self.vault_id = Some(vault_id);
        self.device_id = Some(device_id);
        self.phase = Phase::Group;
        Ok(())
    }

    fn verify_ready_output_bound(&self) -> Result<(), ChunkError> {
        let result = self
            .result
            .as_ref()
            .ok_or_else(|| invalid("ready Tree v1 replacement has no output owner"))?;
        let chunks = result.chunk_memory();
        let metadata = result.metadata_memory();
        if !chunks.counters_valid
            || !metadata.counters_valid
            || !metadata.committed.counters_valid
            || !metadata.committed.strings.counters_valid
            || !metadata.tree_ids.counters_valid
        {
            return Err(invalid(
                "Tree v1 replacement output memory counters are invalid",
            ));
        }
        let root_requested = metadata
            .committed
            .strings
            .capacity_bytes
            .checked_add(metadata.committed.v1_children_backing_capacity_bytes)
            .and_then(|bytes| bytes.checked_add(metadata.tree_ids.capacity_bytes))
            .ok_or_else(|| invalid("Tree v1 replacement output memory total overflow"))?;
        if chunks.payload_bytes > self.output_memory_plan.node_payload_bytes
            || root_requested
                > self
                    .output_memory_plan
                    .range_endpoint_resident_requested_bytes
        {
            return Err(invalid(
                "Tree v1 replacement output exceeded its admitted witness",
            ));
        }
        Ok(())
    }

    fn progress(&self, units: usize) -> ReplacementBuildProgress {
        ReplacementBuildProgress {
            done: self.is_ready(),
            units,
            completed: self.completed,
            phase: match self.phase {
                Phase::PlanReady => "plan ready",
                Phase::Group => "group",
                Phase::NextGroup => "next-group",
                Phase::SortRows => "sort-entries",
                Phase::MoveLeaf => "move-leaf",
                Phase::EncodeLeaf => "encode-leaf",
                Phase::RetireLeaf => "retire-leaf",
                Phase::MoveChildren => "move-internal",
                Phase::SortChildren => "sort-internal",
                Phase::EncodeInternal => "encode-internal",
                Phase::RetireChildren => "retire-internal",
                Phase::SeedRoot => "seed-root",
                Phase::Validate => "validate",
                Phase::Ready => "ready",
                Phase::Failed => "failed",
                Phase::Retiring => "retiring",
            },
        }
    }

    pub(crate) fn step(
        &mut self,
        max_units: usize,
    ) -> Result<ReplacementBuildProgress, ChunkError> {
        if self.phase == Phase::Failed || self.phase == Phase::Retiring {
            return Err(invalid("replacement rebuild cursor failed"));
        }
        if max_units == 0 {
            return Err(invalid("replacement rebuild step budget must be positive"));
        }
        let mut units = 0;
        while units < max_units && !self.is_ready() {
            if self.phase == Phase::PlanReady {
                return Ok(self.progress(units));
            }
            let Some(completed) = self.completed.checked_add(1) else {
                self.phase = Phase::Failed;
                return Err(invalid("replacement rebuild progress overflow"));
            };
            let stop = match self.tick() {
                Ok(stop) => stop,
                Err(error) => {
                    self.phase = Phase::Failed;
                    return Err(error);
                }
            };
            units += 1;
            self.completed = completed;
            if stop {
                break;
            }
        }
        Ok(self.progress(units))
    }

    /// One cheap unit, or one atomic primitive followed by an immediate yield
    /// to the caller. Transitions also consume a unit; none hide a row loop.
    fn tick(&mut self) -> Result<bool, ChunkError> {
        match self.phase {
            Phase::Group => {
                if let Some(entry) = self.input.as_mut().and_then(Iterator::next) {
                    let prefix = entry.path.find('/').map_or("", |end| &entry.path[..=end]);
                    match self.groups.entry(prefix.to_owned()) {
                        btree_map::Entry::Occupied(mut group) => {
                            let before = VecBackingMemory::of(group.get());
                            group.get_mut().push(entry);
                            let after = VecBackingMemory::of(group.get());
                            self.entry_memory.input_to_existing_group(before, after);
                        }
                        btree_map::Entry::Vacant(group) => {
                            self.graph_memory.add_group_key(group.key());
                            let group = group.insert(vec![entry]);
                            self.entry_memory.input_to_new_group(group);
                        }
                    }
                } else {
                    // Exhaustion does not free Vec::IntoIter backing. Drop it
                    // as its own unit before grouping can advance.
                    self.input = None;
                    self.entry_memory.release_input();
                    self.phase = Phase::NextGroup;
                    return Ok(true);
                }
            }
            Phase::NextGroup => {
                if let Some((_, entries)) = self.groups.first_key_value() {
                    // Preflight before pop so a synthetic arithmetic failure
                    // cannot bulk-drop an extracted group outside retirement.
                    let total_files = self
                        .total_files
                        .checked_add(entries.len() as u64)
                        .ok_or_else(|| invalid("replacement rebuild file count overflow"))?;
                    let (prefix, entries) = self.groups.pop_first().unwrap();
                    self.graph_memory.group_key_to_prefix(&prefix);
                    self.entry_memory.group_to_sort(&entries);
                    self.total_files = total_files;
                    self.prefix = Some(prefix);
                    self.row_sort = Some(StableSort::new(entries));
                    self.phase = Phase::SortRows;
                } else {
                    if self.leaf_hashes.capacity() != 0 {
                        let allocation = std::mem::take(&mut self.leaf_hashes);
                        self.graph_memory.release_leaf_hashes();
                        drop(allocation);
                        return Ok(true);
                    }
                    // Same lineage/clock boundary as tree::build_tree: root
                    // creation follows all directory graph construction.
                    let root = RootNode {
                        vault_id: self.vault_id.take().ok_or_else(|| {
                            invalid("replacement rebuild lost its vault identity")
                        })?,
                        device_id: self.device_id.take().ok_or_else(|| {
                            invalid("replacement rebuild lost its device identity")
                        })?,
                        created_ms: crate::tree::now_ms(),
                        version: 1,
                        children: std::mem::take(&mut self.root_children),
                        total_files: self.total_files,
                        parent_hash: None,
                    };
                    let child_strings = self.graph_memory.root_to_metadata();
                    self.root = Some(OwnedRootV1::metered(root, child_strings));
                    self.phase = Phase::SeedRoot;
                }
            }
            Phase::SortRows => {
                if !self.row_sort.as_mut().unwrap().tick() {
                    let rows = self.row_sort.take().unwrap().finish();
                    self.entry_memory.sort_to_rows(&rows);
                    self.rows = Some(rows.into_iter());
                    self.phase = Phase::MoveLeaf;
                }
            }
            Phase::MoveLeaf => {
                if let Some(entry) = self.rows.as_mut().and_then(Iterator::next) {
                    let leaf_was_owned = self.leaf.is_some();
                    let leaf = self.leaf.get_or_insert_with(Vec::new);
                    leaf.push(entry);
                    self.entry_memory.rows_to_leaf(leaf_was_owned, leaf);
                    if leaf.len() == TARGET_CHUNK_ENTRIES {
                        self.phase = Phase::EncodeLeaf;
                    }
                } else if self.rows.is_some() {
                    // The last row was yielded on a prior unit; release the
                    // retained source Vec backing separately.
                    self.rows = None;
                    self.entry_memory.release_rows();
                    return Ok(true);
                } else if self.leaf.as_ref().is_some_and(|leaf| !leaf.is_empty()) {
                    self.phase = Phase::EncodeLeaf;
                } else if self.leaf_hashes.len() == 1 {
                    let prefix = self
                        .prefix
                        .take()
                        .ok_or_else(|| invalid("replacement rebuild lost its group prefix"))?;
                    self.root_children
                        .push((prefix, self.leaf_hashes.pop().unwrap()));
                    self.graph_memory.refresh_leaf_hashes(&self.leaf_hashes);
                    let prefix = &self.root_children.last().unwrap().0;
                    self.graph_memory
                        .prefix_to_root_child(prefix, &self.root_children);
                    self.phase = Phase::NextGroup;
                } else if self.leaf_hashes.len() > 1 {
                    let hashes = std::mem::take(&mut self.leaf_hashes);
                    self.graph_memory.leaf_hashes_to_hashes();
                    self.hashes = Some(hashes.into_iter());
                    self.phase = Phase::MoveChildren;
                } else {
                    return Err(invalid(
                        "replacement rebuild encountered an empty directory group",
                    ));
                }
            }
            Phase::EncodeLeaf => {
                // Already stable-sorted by path; LeafChunk::new would repeat
                // a synchronous sort. The unchanged codec preserves bytes.
                let leaf = LeafChunk {
                    entries: self
                        .leaf
                        .take()
                        .expect("encode leaf phase must own entry rows"),
                };
                self.entry_memory.leaf_to_retiring_rows(&leaf.entries);
                let bytes = leaf.serialize();
                let hash = hash_bytes(&bytes);
                self.store.insert_chunk(hash, bytes);
                self.leaf_hashes.push(hash);
                self.graph_memory.refresh_leaf_hashes(&self.leaf_hashes);
                self.retiring_rows = Some(leaf.entries.into_iter());
                self.phase = Phase::RetireLeaf;
                #[cfg(test)]
                {
                    self.atomic_steps += 1;
                }
                return Ok(true);
            }
            Phase::RetireLeaf => {
                if let Some(entry) = self.retiring_rows.as_mut().and_then(Iterator::next) {
                    self.entry_memory.retire_encoded_row(&entry);
                    drop(entry);
                } else {
                    self.retiring_rows = None;
                    self.entry_memory.release_retiring_rows();
                    self.phase = Phase::MoveLeaf;
                    return Ok(true);
                }
            }
            Phase::MoveChildren => {
                if let Some(hash) = self.hashes.as_mut().and_then(Iterator::next) {
                    self.graph_memory.retire_hash();
                    // Decimal labels sort lexically, including 0,1,10,11,2.
                    let prefix = self
                        .prefix
                        .as_deref()
                        .ok_or_else(|| invalid("replacement rebuild lost its group prefix"))?;
                    self.children
                        .push((format!("{}{}", prefix, self.children.len()), hash));
                    let label = &self.children.last().unwrap().0;
                    self.graph_memory.push_child(label, &self.children);
                } else if self.hashes.is_some() {
                    self.hashes = None;
                    self.graph_memory.release_hashes();
                    return Ok(true);
                } else {
                    let children = std::mem::take(&mut self.children);
                    self.graph_memory.children_to_sort();
                    self.child_sort = Some(StableSort::new(children));
                    self.phase = Phase::SortChildren;
                }
            }
            Phase::SortChildren => {
                if !self.child_sort.as_mut().unwrap().tick() {
                    self.children = self.child_sort.take().unwrap().finish();
                    self.graph_memory.sort_to_children(&self.children);
                    self.phase = Phase::EncodeInternal;
                }
            }
            Phase::EncodeInternal => {
                // Labels are unique and cooperatively sorted, matching
                // InternalNode::new without its whole-vector sort.
                let internal = InternalNode {
                    children: std::mem::take(&mut self.children),
                };
                self.graph_memory.children_to_retiring();
                let bytes = internal.serialize();
                let hash = hash_bytes(&bytes);
                self.store.insert_chunk(hash, bytes);
                self.internal_hash = Some(hash);
                self.retiring_children = Some(internal.children.into_iter());
                self.phase = Phase::RetireChildren;
                #[cfg(test)]
                {
                    self.atomic_steps += 1;
                }
                return Ok(true);
            }
            Phase::RetireChildren => {
                if let Some(child) = self.retiring_children.as_mut().and_then(Iterator::next) {
                    self.graph_memory.retire_child_from_iter(&child.0);
                    drop(child);
                } else if self.retiring_children.is_some() {
                    self.retiring_children = None;
                    self.graph_memory.release_retiring_children();
                    return Ok(true);
                } else {
                    let prefix = self
                        .prefix
                        .take()
                        .ok_or_else(|| invalid("replacement rebuild lost its group prefix"))?;
                    self.root_children
                        .push((prefix, self.internal_hash.take().unwrap()));
                    let prefix = &self.root_children.last().unwrap().0;
                    self.graph_memory
                        .prefix_to_root_child(prefix, &self.root_children);
                    self.phase = Phase::NextGroup;
                }
            }
            Phase::SeedRoot => {
                if let Some((_, hash)) = self
                    .root
                    .as_ref()
                    .unwrap()
                    .value()
                    .children
                    .get(self.seeded)
                {
                    self.validation.as_mut().unwrap().seed_root_child(*hash)?;
                    self.seeded += 1;
                } else {
                    self.validation.as_mut().unwrap().finish_seeding();
                    self.phase = Phase::Validate;
                }
            }
            Phase::Validate => {
                let validation = self.validation.as_mut().unwrap();
                let progress = validation.step(&self.store, 1)?;
                if progress.done {
                    self.validation.take().unwrap().finish()?;
                    let root = self.root.take().unwrap();
                    self.result = Some(TransactionalTree::from_verified_replacement(
                        root,
                        std::mem::take(&mut self.store),
                    ));
                    self.verify_ready_output_bound()?;
                    self.phase = Phase::Ready;
                }
                #[cfg(test)]
                {
                    self.atomic_steps += 1;
                }
                return Ok(true);
            }
            Phase::PlanReady => return Ok(false),
            Phase::Ready => {}
            Phase::Failed => return Err(invalid("replacement rebuild cursor failed")),
            Phase::Retiring => return Err(invalid("replacement rebuild is retiring")),
        }
        Ok(false)
    }

    /// No build, validation, root serialization or graph scan at finish. The
    /// caller owns atomic installation and retirement of its previous tree.
    pub(crate) fn take_ready(&mut self) -> Result<TransactionalTree, ChunkError> {
        if !self.is_ready() {
            return Err(invalid("replacement rebuild is not ready"));
        }
        let result = self
            .result
            .take()
            .ok_or_else(|| invalid("ready replacement has no verified tree"))?;
        self.phase = Phase::Retiring;
        Ok(result)
    }

    pub(crate) fn finish(mut self) -> Result<TransactionalTree, ChunkError> {
        self.take_ready()
    }

    /// Exact after a provisional root or complete tree owner exists. Earlier
    /// grouping/sort worksets remain explicitly unmeasured by the metadata
    /// report; their assembly owners use the separate V1 graph component.
    pub(crate) fn metadata_memory(&self) -> Option<TreeMetadataMemory> {
        self.result
            .as_ref()
            .map(TransactionalTree::metadata_memory)
            .or_else(|| {
                self.root.as_ref().map(|root| {
                    TreeMetadataMemory::new(
                        root.metadata(),
                        Default::default(),
                        Default::default(),
                        Default::default(),
                    )
                })
            })
            .or_else(|| {
                self.retiring_result
                    .as_ref()
                    .map(TreeRetirementV1::metadata_memory)
            })
    }

    /// Irreversible cleanup of one populated row/string descriptor/node
    /// buffer, or one scalar/sparse bookkeeping slot. Nested grouped vectors
    /// are drained before removing their key. False means no populated owner
    /// remains; empty backing allocations and map-iterator scans are residual.
    pub(crate) fn retire_one(&mut self) -> bool {
        self.phase = Phase::Retiring;
        if let Some(input) = &mut self.input {
            if let Some(entry) = input.next() {
                self.entry_memory.retire_input(&entry);
                drop(entry);
                return true;
            }
            self.input = None;
            self.entry_memory.release_input();
            return true;
        }
        if let Some(mut group) = self.groups.first_entry() {
            let before = VecBackingMemory::of(group.get());
            if let Some(entry) = group.get_mut().pop() {
                let after = VecBackingMemory::of(group.get());
                self.entry_memory.retire_group_entry(before, after, &entry);
                drop(entry);
                return true;
            }
            if group.get().capacity() != 0 {
                let allocation = std::mem::take(group.get_mut());
                self.entry_memory.release_group(&allocation);
                drop(allocation);
                return true;
            }
            // Removing an empty group drops its single prefix string, not a
            // nested row collection; its Vec backing was a prior unit.
            let (key, group) = group.remove_entry();
            self.graph_memory.retire_group_key(&key);
            drop((key, group));
            return true;
        }
        if let Some(prefix) = self.prefix.take() {
            self.graph_memory.retire_prefix(&prefix);
            drop(prefix);
            return true;
        }
        if let Some(sorter) = &mut self.row_sort {
            let entry_memory = &mut self.entry_memory;
            if sorter.retire_one_with(|entry| entry_memory.retire_sorting_row(entry)) {
                return true;
            }
            self.row_sort = None;
            // StableSort retains its drained values backing after both index
            // arrays; destroy that final owner as its own unit.
            return true;
        }
        if let Some(rows) = &mut self.rows {
            if let Some(entry) = rows.next() {
                self.entry_memory.retire_row(&entry);
                drop(entry);
                return true;
            }
            self.rows = None;
            self.entry_memory.release_rows();
            return true;
        }
        if let Some(leaf) = &mut self.leaf {
            if let Some(entry) = leaf.pop() {
                self.entry_memory.retire_leaf_row(&entry);
                drop(entry);
                return true;
            }
            self.leaf = None;
            self.entry_memory.release_leaf();
            return true;
        }
        if let Some(rows) = &mut self.retiring_rows {
            if let Some(entry) = rows.next() {
                self.entry_memory.retire_encoded_row(&entry);
                drop(entry);
                return true;
            }
            self.retiring_rows = None;
            self.entry_memory.release_retiring_rows();
            return true;
        }
        if self.leaf_hashes.pop().is_some() {
            self.graph_memory.refresh_leaf_hashes(&self.leaf_hashes);
            return true;
        }
        if self.leaf_hashes.capacity() != 0 {
            let allocation = std::mem::take(&mut self.leaf_hashes);
            self.graph_memory.release_leaf_hashes();
            drop(allocation);
            return true;
        }
        if let Some(hashes) = &mut self.hashes {
            if hashes.next().is_some() {
                self.graph_memory.retire_hash();
                return true;
            }
            self.hashes = None;
            self.graph_memory.release_hashes();
            return true;
        }
        if let Some((label, hash)) = self.children.pop() {
            self.graph_memory
                .retire_child_from_vec(&label, &self.children);
            drop((label, hash));
            return true;
        }
        if self.children.capacity() != 0 {
            let allocation = std::mem::take(&mut self.children);
            self.graph_memory.release_children();
            drop(allocation);
            return true;
        }
        if let Some(sorter) = &mut self.child_sort {
            let graph_memory = &mut self.graph_memory;
            if sorter.retire_one_with(|child| graph_memory.retire_sorting_child(&child.0)) {
                return true;
            }
            self.child_sort = None;
            // As for row sort, the drained values Vec still owns its backing
            // after both index arrays. Do not combine its drop with the next
            // child/root retirement owner.
            return true;
        }
        if let Some(children) = &mut self.retiring_children {
            if let Some((label, hash)) = children.next() {
                self.graph_memory.retire_child_from_iter(&label);
                drop((label, hash));
                return true;
            }
            self.retiring_children = None;
            self.graph_memory.release_retiring_children();
            return true;
        }
        if self.internal_hash.take().is_some() {
            return true;
        }
        if let Some((label, hash)) = self.root_children.pop() {
            self.graph_memory
                .retire_root_child(&label, &self.root_children);
            drop((label, hash));
            return true;
        }
        if self.root_children.capacity() != 0 {
            let allocation = std::mem::take(&mut self.root_children);
            self.graph_memory.release_root_children();
            drop(allocation);
            return true;
        }
        if let Some(root) = &mut self.root {
            if root.retire_child() || root.retire_vault() || root.retire_device() {
                return true;
            }
            self.root = None;
            return true;
        }
        if let Some(validation) = &mut self.validation {
            if validation.retire_one() {
                return true;
            }
            self.validation = None;
        }
        let chunks = self
            .retiring_chunks
            .get_or_insert_with(|| std::mem::take(&mut self.store).into_chunks());
        if chunks.next().is_some() {
            return true;
        }
        if self.retiring_result.is_none() {
            self.retiring_result = self.result.take().map(TransactionalTree::into_retirement);
        }
        if let Some(result) = &mut self.retiring_result {
            if result.retire_one() {
                return true;
            }
            self.retiring_result = None;
        }
        if let Some(vault_id) = self.vault_id.take() {
            self.graph_memory.retire_identity(&vault_id);
            drop(vault_id);
            return true;
        }
        if let Some(device_id) = self.device_id.take() {
            self.graph_memory.retire_identity(&device_id);
            drop(device_id);
            return true;
        }
        debug_assert!(self.graph_memory.owners_empty());
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree_metadata::StringMemory;
    use std::collections::BTreeMap;

    fn entry(path: String, value: u64) -> FileEntry {
        FileEntry::new(path, hash_bytes(&value.to_le_bytes()), value, value + 1)
    }

    fn corpus(count: usize, mixed: bool) -> Vec<FileEntry> {
        (0..count)
            .rev()
            .map(|index| {
                let path = if mixed {
                    match index % 41 {
                        0 => format!("root-{index:06}.md"),
                        group => format!("dir-{group:02}/note-{index:06}.md"),
                    }
                } else {
                    format!("notes/{index:06}.md")
                };
                entry(path, index as u64)
            })
            .collect()
    }

    fn metered_corpus(count: usize) -> Vec<FileEntry> {
        let mut rows = Vec::with_capacity(count + 23);
        for index in (0..count).rev() {
            let text = match index % 4 {
                0 => format!("root-𐀀-{index:04}.md"),
                group => format!("é-{group}/note-{index:04}.md"),
            };
            let mut path = String::with_capacity(text.len() + index % 11 + 7);
            path.push_str(&text);
            rows.push(entry(path, index as u64));
        }
        rows
    }

    fn add_paths(memory: &mut StringMemory, rows: &[FileEntry]) {
        for row in rows {
            memory.add(&row.path);
        }
    }

    fn assert_entry_memory_exact(job: &ReplacementRebuildV1) {
        let memory = job.entry_memory();
        assert!(memory.counters_valid, "V1 entry counters became invalid");

        assert_eq!(memory.input.owners, usize::from(job.input.is_some()));
        if let Some(input) = &job.input {
            assert_eq!(memory.input.length_slots, input.len());
            assert!(memory.input.capacity_slots >= input.len());
        } else {
            assert_eq!(memory.input, VecBackingMemory::default());
        }

        let metered_groups = job
            .groups
            .values()
            .filter(|group| group.capacity() != 0)
            .collect::<Vec<_>>();
        assert_eq!(memory.groups.owners, metered_groups.len());
        assert_eq!(
            memory.groups.length_slots,
            metered_groups
                .iter()
                .map(|group| group.len())
                .sum::<usize>()
        );
        assert_eq!(
            memory.groups.capacity_slots,
            metered_groups
                .iter()
                .map(|group| group.capacity())
                .sum::<usize>()
        );
        assert_eq!(
            memory.groups.backing_capacity_bytes,
            (memory.groups.capacity_slots * std::mem::size_of::<FileEntry>()) as u64
        );

        if let Some(sorter) = &job.row_sort {
            assert_eq!(memory.sorting_rows, sorter.values_for_test().len());
            assert_eq!(
                memory.sorting_rows,
                job.sort_memory().entries.values.length_slots
            );
        } else {
            assert_eq!(memory.sorting_rows, 0);
        }
        assert_eq!(memory.rows.owners, usize::from(job.rows.is_some()));
        if let Some(rows) = &job.rows {
            assert_eq!(memory.rows.length_slots, rows.len());
        }
        assert_eq!(memory.leaf.owners, usize::from(job.leaf.is_some()));
        if let Some(leaf) = &job.leaf {
            assert_eq!(memory.leaf, VecBackingMemory::of(leaf));
        }
        assert_eq!(
            memory.retiring_rows.owners,
            usize::from(job.retiring_rows.is_some())
        );
        if let Some(rows) = &job.retiring_rows {
            assert_eq!(memory.retiring_rows.length_slots, rows.len());
        }

        let mut paths = StringMemory::default();
        if let Some(input) = &job.input {
            add_paths(&mut paths, input.as_slice());
        }
        for group in job.groups.values() {
            add_paths(&mut paths, group);
        }
        if let Some(sorter) = &job.row_sort {
            add_paths(&mut paths, sorter.values_for_test());
        }
        if let Some(rows) = &job.rows {
            add_paths(&mut paths, rows.as_slice());
        }
        if let Some(leaf) = &job.leaf {
            add_paths(&mut paths, leaf);
        }
        if let Some(rows) = &job.retiring_rows {
            add_paths(&mut paths, rows.as_slice());
        }
        assert_eq!(memory.paths, paths);
    }

    fn assert_retained_vec_backing(actual: VecBackingMemory, expected: VecBackingMemory) {
        assert_eq!(actual.owners, 1);
        assert_eq!(actual.capacity_slots, expected.capacity_slots);
        assert_eq!(actual.slot_size_bytes, expected.slot_size_bytes);
        assert_eq!(
            actual.backing_capacity_bytes,
            expected.backing_capacity_bytes
        );
    }

    fn allocated_vec<T>(values: &Vec<T>) -> VecBackingMemory {
        if values.capacity() == 0 {
            VecBackingMemory::default()
        } else {
            VecBackingMemory::of(values)
        }
    }

    fn add_child_labels(memory: &mut StringMemory, children: &[(String, FileHash)]) {
        for (label, _) in children {
            memory.add(label);
        }
    }

    fn assert_graph_memory_exact(job: &ReplacementRebuildV1) {
        let memory = job.graph_memory();
        assert!(memory.counters_valid, "V1 graph counters became invalid");

        let mut group_keys = StringMemory::default();
        for key in job.groups.keys() {
            group_keys.add(key);
        }
        assert_eq!(memory.group_keys, group_keys);

        let prefix = job
            .prefix
            .as_ref()
            .map_or_else(StringMemory::default, StringMemory::of);
        assert_eq!(memory.prefix, prefix);
        assert_eq!(memory.leaf_hashes, allocated_vec(&job.leaf_hashes));

        if let Some(hashes) = &job.hashes {
            assert_eq!(memory.hashes.owners, 1);
            assert_eq!(memory.hashes.length_slots, hashes.len());
            assert!(memory.hashes.capacity_slots >= hashes.len());
            assert_eq!(
                memory.hashes.slot_size_bytes,
                std::mem::size_of::<FileHash>()
            );
            assert_eq!(
                memory.hashes.backing_capacity_bytes,
                (memory.hashes.capacity_slots * std::mem::size_of::<FileHash>()) as u64
            );
        } else {
            assert_eq!(memory.hashes, VecBackingMemory::default());
        }

        assert_eq!(memory.children, allocated_vec(&job.children));
        assert_eq!(
            memory.sorting_children,
            job.child_sort
                .as_ref()
                .map_or(0, |sorter| sorter.values_for_test().len())
        );
        if let Some(children) = &job.retiring_children {
            assert_eq!(memory.retiring_children.owners, 1);
            assert_eq!(memory.retiring_children.length_slots, children.len());
            assert!(memory.retiring_children.capacity_slots >= children.len());
            assert_eq!(
                memory.retiring_children.slot_size_bytes,
                std::mem::size_of::<(String, FileHash)>()
            );
            assert_eq!(
                memory.retiring_children.backing_capacity_bytes,
                (memory.retiring_children.capacity_slots
                    * std::mem::size_of::<(String, FileHash)>()) as u64
            );
        } else {
            assert_eq!(memory.retiring_children, VecBackingMemory::default());
        }

        let mut child_labels = StringMemory::default();
        add_child_labels(&mut child_labels, &job.children);
        if let Some(sorter) = &job.child_sort {
            add_child_labels(&mut child_labels, sorter.values_for_test());
        }
        if let Some(children) = &job.retiring_children {
            add_child_labels(&mut child_labels, children.as_slice());
        }
        assert_eq!(memory.child_labels, child_labels);

        assert_eq!(memory.root_children, allocated_vec(&job.root_children));
        let mut root_labels = StringMemory::default();
        add_child_labels(&mut root_labels, &job.root_children);
        assert_eq!(memory.root_child_labels, root_labels);

        let mut identities = StringMemory::default();
        if let Some(value) = &job.vault_id {
            identities.add(value);
        }
        if let Some(value) = &job.device_id {
            identities.add(value);
        }
        assert_eq!(memory.identities, identities);

        if job.root.is_some() || job.result.is_some() || job.retiring_result.is_some() {
            assert_eq!(memory, ReplacementV1GraphMemory::default());
        }
    }

    fn chunks(tree: &TransactionalTree) -> BTreeMap<FileHash, Vec<u8>> {
        tree.resident_store_for_test()
            .all_chunks()
            .into_iter()
            .collect()
    }

    fn drain(job: &mut ReplacementRebuildV1, budget: usize) -> (usize, usize) {
        let mut calls = 0;
        let mut completed = 0;
        while !job.is_ready() {
            let atoms = job.atomic_steps;
            let progress = job.step(budget).unwrap();
            calls += 1;
            assert!(progress.units > 0 && progress.units <= budget);
            assert_eq!(progress.completed, completed + progress.units);
            assert_eq!(progress.done, job.is_ready());
            assert!(
                job.atomic_steps - atoms <= 1,
                "one step bundled native codecs/edges"
            );
            if job.atomic_steps != atoms {
                assert!(
                    matches!(
                        job.phase,
                        Phase::RetireLeaf | Phase::RetireChildren | Phase::Validate | Phase::Ready
                    ),
                    "step continued beyond an atomic boundary"
                );
            }
            completed = progress.completed;
            assert!(
                completed < 4_000_000,
                "replacement stopped making finite progress"
            );
        }
        let progress = job.step(budget).unwrap();
        assert!(progress.done);
        assert_eq!(progress.units, 0);
        assert_eq!(progress.completed, completed);
        (calls, completed)
    }

    async fn compare_to_legacy(rows: Vec<FileEntry>, budget: usize) -> (usize, usize) {
        let pointer = rows.as_ptr();
        let expected_count = rows.len();
        let mut legacy = TransactionalTree::new("replacement-v1", "device");
        legacy.rebuild(rows.clone()).await.unwrap();
        let mut job = ReplacementRebuildV1::new(rows, "replacement-v1", "device");
        assert_eq!(
            job.input.as_ref().unwrap().as_slice().as_ptr(),
            pointer,
            "new copied the entire input"
        );
        assert_eq!(job.input.as_ref().unwrap().len(), expected_count);
        assert!(job.groups.is_empty() && job.store.is_empty() && job.root.is_none());
        assert_eq!(job.completed, 0);
        let started = crate::tree::now_ms();
        let counts = drain(&mut job, budget);
        let rebuilt = job.finish().unwrap();
        let actual = rebuilt.committed_root().unwrap();
        let mut expected = legacy.committed_root().unwrap().clone();
        // Native SystemTime cannot be frozen here. Normalize ONLY that field;
        // packaged WASM parity freezes Date.now for exact unmodified bytes.
        assert!((started..=crate::tree::now_ms()).contains(&actual.created_ms));
        expected.created_ms = actual.created_ms;
        assert_eq!(actual.serialize(), expected.serialize());
        assert_eq!(actual.hash(), expected.hash());
        assert_eq!(actual.total_files, expected_count as u64);
        assert_eq!(actual.parent_hash, None);
        assert_eq!(chunks(&rebuilt), chunks(&legacy));
        assert!(!rebuilt.has_candidate());
        counts
    }

    #[tokio::test]
    async fn replacement_v1_differential_empty_unicode_duplicates_and_leaf_boundaries() {
        let unusual = vec![
            entry("\u{10000}.md".into(), 1),
            entry("\u{e000}.md".into(), 2),
            entry("z.md".into(), 3),
            entry("a/note.md".into(), 4),
            entry("a/note.md".into(), 5),
            entry("a/note.md".into(), 6),
        ];
        for rows in [
            Vec::new(),
            unusual,
            corpus(1_001, false),
            corpus(1_027, true),
        ] {
            let first = compare_to_legacy(rows.clone(), 1).await;
            let batched = compare_to_legacy(rows, 256).await;
            assert_eq!(first.1, batched.1, "budget changed logical work count");
            assert!(batched.0 <= first.0);
        }
    }

    #[tokio::test]
    async fn replacement_v1_25k_exact_graph_and_decimal_internal_order() {
        for mixed in [false, true] {
            let rows = corpus(25_000, mixed);
            let first = compare_to_legacy(rows.clone(), 1).await;
            let batched = compare_to_legacy(rows.clone(), 256).await;
            assert_eq!(first.1, batched.1);
            assert!(
                batched.0 < first.0 / 20,
                "cheap metadata was hardcoded to one host turn per row"
            );
            assert!(batched.0 < 10_000);
            if !mixed {
                let mut job = ReplacementRebuildV1::new(rows, "replacement-v1", "device");
                drain(&mut job, 256);
                let rebuilt = job.finish().unwrap();
                let hash = rebuilt.committed_root().unwrap().children[0].1;
                let internal =
                    InternalNode::deserialize(&rebuilt.chunk_bytes(&hash).unwrap()).unwrap();
                assert_eq!(internal.children.len(), 25);
                assert_eq!(internal.children[0].0, "notes/0");
                assert_eq!(internal.children[1].0, "notes/1");
                assert_eq!(internal.children[2].0, "notes/10");
                assert!(internal
                    .children
                    .windows(2)
                    .all(|pair| pair[0].0 < pair[1].0));
            }
        }
    }

    type LiveState = (
        Vec<u8>,
        Option<Vec<u8>>,
        BTreeMap<FileHash, Vec<u8>>,
        Vec<FileHash>,
    );

    fn live_state(tree: &TransactionalTree) -> LiveState {
        (
            tree.committed_root().unwrap().serialize(),
            tree.candidate_root().map(RootNode::serialize),
            chunks(tree),
            tree.new_candidate_chunk_hashes().unwrap(),
        )
    }

    async fn live_candidate() -> TransactionalTree {
        let mut tree = TransactionalTree::new("live", "owner");
        tree.rebuild(corpus(3, true)).await.unwrap();
        tree.resident_store_for_test().insert_chunk(
            hash_bytes(b"orphan before candidate"),
            b"orphan before candidate".to_vec(),
        );
        tree.begin_candidate().unwrap();
        tree.apply_candidate(&[entry("active.md".into(), 500)], &[])
            .await
            .unwrap();
        tree
    }

    #[tokio::test]
    async fn replacement_v1_cancel_each_phase_never_touches_live_candidate_or_store() {
        let live = live_candidate().await;
        let before = live_state(&live);
        for phase in [
            Phase::Group,
            Phase::NextGroup,
            Phase::SortRows,
            Phase::MoveLeaf,
            Phase::EncodeLeaf,
            Phase::RetireLeaf,
            Phase::MoveChildren,
            Phase::SortChildren,
            Phase::EncodeInternal,
            Phase::RetireChildren,
            Phase::SeedRoot,
            Phase::Validate,
            Phase::Ready,
        ] {
            let mut job = ReplacementRebuildV1::new(corpus(1_001, false), "replacement", "new");
            while job.phase != phase {
                assert!(!job.is_ready(), "test never visited {phase:?}");
                job.step(1).unwrap();
            }
            assert_eq!(live_state(&live), before);
            drop(job); // Caller cancellation owns the entire private cursor.
            assert_eq!(live_state(&live), before);
        }
        assert!(ReplacementRebuildV1::new(corpus(1, false), "new", "new")
            .finish()
            .is_err());
        assert_eq!(live_state(&live), before);
    }

    #[tokio::test]
    async fn replacement_v1_validation_failure_is_terminal_and_private() {
        let live = live_candidate().await;
        let before = live_state(&live);
        let mut job = ReplacementRebuildV1::new(corpus(1_001, false), "new", "new");
        assert!(job.step(0).is_err());
        assert_eq!(
            job.phase,
            Phase::Group,
            "invalid budget poisoned valid ownership"
        );
        assert_eq!(job.completed, 0);
        while job.phase != Phase::Validate {
            job.step(1).unwrap();
        }
        let hash = job.root.as_ref().unwrap().value().children[0].1;
        job.store
            .insert_chunk(hash, b"corrupt staged root child".to_vec());
        assert!(job.step(256).is_err());
        assert_eq!(job.phase, Phase::Failed);
        assert!(!job.is_ready());
        assert!(job.step(1).is_err());
        assert!(job.finish().is_err());
        assert_eq!(live_state(&live), before);

        let mut job = ReplacementRebuildV1::new(corpus(1, false), "new", "new");
        job.completed = usize::MAX;
        assert!(job.step(1).is_err());
        assert_eq!(
            job.input.as_ref().unwrap().len(),
            1,
            "overflow consumed another input row"
        );
        assert_eq!(job.phase, Phase::Failed);
        assert_eq!(live_state(&live), before);
    }

    #[tokio::test]
    async fn replacement_v1_large_legacy_node_is_one_explicit_atomic_primitive() {
        let long = "é".repeat(40_000);
        let rows = vec![entry(format!("{long}.md"), 1), entry("short.md".into(), 2)];
        compare_to_legacy(rows.clone(), 256).await;
        let mut job = ReplacementRebuildV1::new(rows, "new", "new");
        while job.phase != Phase::EncodeLeaf {
            job.step(1).unwrap();
        }
        let atoms = job.atomic_steps;
        let progress = job.step(256).unwrap();
        assert_eq!(progress.units, 1);
        assert_eq!(job.atomic_steps, atoms + 1);
        assert_eq!(job.phase, Phase::RetireLeaf);
        assert!(
            job.store.all_chunks()[0].1.len() > 64 * 1024,
            "test no longer covers the explicitly unbounded v1 codec residual"
        );
    }

    fn retire_to_empty(job: &mut ReplacementRebuildV1) -> usize {
        let mut units = 0;
        while job.retire_one() {
            units += 1;
            assert_eq!(job.phase, Phase::Retiring);
            assert!(!job.is_ready());
            assert!(job.step(1).is_err());
            assert!(job.take_ready().is_err());
            assert!(
                units < 300_000,
                "retirement did not make bounded explicit progress"
            );
        }
        assert!(job.input.is_none());
        assert!(job.groups.is_empty() && job.row_sort.is_none() && job.child_sort.is_none());
        assert!(job.rows.is_none() && job.leaf.is_none() && job.retiring_rows.is_none());
        assert_eq!(
            job.leaf_hashes.len()
                + job.hashes.as_ref().map_or(0, ExactSizeIterator::len)
                + job.children.len(),
            0
        );
        assert_eq!(
            job.retiring_children
                .as_ref()
                .map_or(0, ExactSizeIterator::len)
                + job.root_children.len(),
            0
        );
        assert!(job.root.is_none() && job.internal_hash.is_none() && job.validation.is_none());
        assert!(job.store.is_empty() && job.result.is_none() && job.retiring_result.is_none());
        assert_eq!(job.retiring_chunks.as_ref().unwrap().len(), 0);
        assert!(job.prefix.is_none() && job.vault_id.is_none() && job.device_id.is_none());
        assert_eq!(job.graph_memory(), ReplacementV1GraphMemory::default());
        assert!(!job.retire_one());
        units
    }

    #[tokio::test]
    async fn replacement_v1_retirement_at_every_phase_and_failed_owner_is_private() {
        let live = live_candidate().await;
        let before = live_state(&live);
        for phase in [
            Phase::Group,
            Phase::NextGroup,
            Phase::SortRows,
            Phase::MoveLeaf,
            Phase::EncodeLeaf,
            Phase::RetireLeaf,
            Phase::MoveChildren,
            Phase::SortChildren,
            Phase::EncodeInternal,
            Phase::RetireChildren,
            Phase::SeedRoot,
            Phase::Validate,
            Phase::Ready,
        ] {
            let mut job = ReplacementRebuildV1::new(corpus(1_001, false), "replacement", "new");
            while job.phase != phase {
                assert!(!job.is_ready(), "test never visited {phase:?}");
                job.step(1).unwrap();
            }
            assert_eq!(live_state(&live), before);
            assert!(retire_to_empty(&mut job) > 0);
            drop(job);
            assert_eq!(live_state(&live), before);
        }
        let mut failed = ReplacementRebuildV1::new(corpus(1_001, false), "replacement", "new");
        while failed.phase != Phase::Validate {
            failed.step(1).unwrap();
        }
        let hash = failed.root.as_ref().unwrap().value().children[0].1;
        failed
            .store
            .insert_chunk(hash, b"bad staged bytes".to_vec());
        assert!(failed.step(256).is_err());
        assert_eq!(failed.phase, Phase::Failed);
        assert!(retire_to_empty(&mut failed) > 0);
        assert_eq!(live_state(&live), before);
    }

    #[test]
    fn replacement_v1_reports_entry_and_child_sort_backing_separately() {
        let mut job = ReplacementRebuildV1::new(corpus(1_001, false), "vault", "device");
        assert_eq!(job.sort_memory(), ReplacementSortMemory::default());
        while job.phase != Phase::SortRows {
            job.step(1).unwrap();
        }
        let entries = job.sort_memory();
        assert_eq!(entries.entries.sorters, 1);
        assert_eq!(entries.children.sorters, 0);
        assert_eq!(entries.entries.values.length_slots, 1_001);
        assert_eq!(
            entries.entries.values.slot_size_bytes,
            std::mem::size_of::<FileEntry>()
        );
        assert!(entries.entries.source_indices.capacity_slots >= 1_001);
        assert!(entries.entries.target_indices.capacity_slots >= 1_001);

        while job.phase != Phase::SortChildren {
            job.step(1).unwrap();
        }
        let children = job.sort_memory();
        assert_eq!(children.entries.sorters, 0);
        assert_eq!(children.children.sorters, 1);
        assert_eq!(children.children.values.length_slots, 2);
        assert_eq!(
            children.children.values.slot_size_bytes,
            std::mem::size_of::<(String, FileHash)>()
        );
        assert!(children.counters_valid);
    }

    #[test]
    fn replacement_v1_entry_memory_tracks_unicode_spare_capacity_and_releases_backing() {
        let rows = metered_corpus(67);
        let accepted = ReplacementInputMemory::new(&rows);
        let mut job = ReplacementRebuildV1::new_metered(rows, accepted, "vault", "device");
        assert_eq!(job.entry_memory.input, accepted.entries);
        assert_eq!(job.entry_memory.paths, accepted.paths);
        assert_entry_memory_exact(&job);

        let mut saw_input_empty = false;
        let mut saw_group_aggregate = false;
        let mut saw_entry_sort = false;
        let mut saw_rows_empty = false;
        let mut saw_retiring_rows_empty = false;
        let mut rows_backing = None;
        let mut retiring_rows_backing = None;
        while !job.is_ready() {
            job.step(1).unwrap();
            assert_entry_memory_exact(&job);
            if job.input.is_some() {
                assert_retained_vec_backing(job.entry_memory.input, accepted.entries);
            }
            if job.rows.is_some() {
                let expected = *rows_backing.get_or_insert(job.entry_memory.rows);
                assert_retained_vec_backing(job.entry_memory.rows, expected);
            } else {
                rows_backing = None;
            }
            if job.retiring_rows.is_some() {
                let expected = *retiring_rows_backing.get_or_insert(job.entry_memory.retiring_rows);
                assert_retained_vec_backing(job.entry_memory.retiring_rows, expected);
            } else {
                retiring_rows_backing = None;
            }
            saw_input_empty |= job.input.as_ref().is_some_and(|input| {
                input.len() == 0 && job.entry_memory.input.capacity_slots > 67
            });
            saw_group_aggregate |= job.entry_memory.groups.owners > 1;
            saw_entry_sort |= job.row_sort.is_some()
                && job.entry_memory.sorting_rows == job.sort_memory().entries.values.length_slots;
            saw_rows_empty |= job
                .rows
                .as_ref()
                .is_some_and(|rows| rows.len() == 0 && job.entry_memory.rows.capacity_slots > 0);
            saw_retiring_rows_empty |= job.retiring_rows.as_ref().is_some_and(|rows| {
                rows.len() == 0 && job.entry_memory.retiring_rows.capacity_slots > 0
            });
        }
        assert!(saw_input_empty && saw_group_aggregate && saw_entry_sort);
        assert!(saw_rows_empty && saw_retiring_rows_empty);
        assert_eq!(job.entry_memory(), ReplacementV1EntriesMemory::default());
    }

    #[test]
    fn replacement_v1_graph_memory_tracks_exact_assembly_transfers_and_retained_backing() {
        let mut rows = Vec::with_capacity(1_033);
        for index in (0..1_001).rev() {
            rows.push(entry(format!("a/note-{index:04}.md"), index as u64));
        }
        for index in (0..7).rev() {
            rows.push(entry(format!("é/note-{index}.md"), 2_000 + index as u64));
        }
        rows.push(entry("root-𐀀.md".into(), 3_000));

        let mut job = ReplacementRebuildV1::new(rows, "vault", "device");
        assert_graph_memory_exact(&job);
        let mut saw_group_keys = false;
        let mut saw_prefix = false;
        let mut saw_leaf_hashes = false;
        let mut saw_exhausted_hashes = false;
        let mut saw_sorting_children = false;
        let mut saw_exhausted_retiring_children = false;
        let mut saw_root_children = false;
        let mut saw_root_transfer = false;

        while !job.is_ready() {
            let before = job.graph_memory();
            let transfers_root = job.phase == Phase::NextGroup
                && job.groups.is_empty()
                && !job.root_children.is_empty();
            job.step(1).unwrap();
            assert_graph_memory_exact(&job);
            let memory = job.graph_memory();
            saw_group_keys |= memory.group_keys.strings > 1;
            saw_prefix |= memory.prefix.strings == 1;
            saw_leaf_hashes |= memory.leaf_hashes.length_slots > 1;
            saw_exhausted_hashes |= memory.hashes.owners == 1 && memory.hashes.length_slots == 0;
            saw_sorting_children |= memory.sorting_children > 1;
            saw_exhausted_retiring_children |=
                memory.retiring_children.owners == 1 && memory.retiring_children.length_slots == 0;
            saw_root_children |= memory.root_children.length_slots > 1;

            if transfers_root && job.root.is_some() {
                let metadata = job.metadata_memory().unwrap().committed;
                assert_eq!(
                    metadata.v1_children_length,
                    before.root_children.length_slots
                );
                assert_eq!(
                    metadata.v1_children_capacity,
                    before.root_children.capacity_slots
                );
                assert_eq!(
                    metadata.v1_children_backing_capacity_bytes,
                    before.root_children.backing_capacity_bytes
                );
                assert_eq!(
                    metadata.strings,
                    before.root_child_labels.combine(before.identities)
                );
                assert_eq!(memory, ReplacementV1GraphMemory::default());
                saw_root_transfer = true;
            }
        }
        assert!(saw_group_keys && saw_prefix && saw_leaf_hashes);
        assert!(saw_exhausted_hashes && saw_sorting_children);
        assert!(saw_exhausted_retiring_children && saw_root_children && saw_root_transfer);
    }

    #[test]
    fn replacement_v1_graph_memory_remains_exact_during_cancel_and_invalid_telemetry() {
        for checkpoint in [
            Phase::Group,
            Phase::MoveChildren,
            Phase::SortChildren,
            Phase::RetireChildren,
            Phase::SeedRoot,
            Phase::Validate,
            Phase::Ready,
        ] {
            let mut job = ReplacementRebuildV1::new(corpus(1_001, false), "vault", "device");
            while job.phase != checkpoint {
                job.step(1).unwrap();
            }
            assert_graph_memory_exact(&job);
            while job.retire_one() {
                assert_graph_memory_exact(&job);
            }
            assert_eq!(job.graph_memory(), ReplacementV1GraphMemory::default());
        }

        let mut job = ReplacementRebuildV1::new(corpus(1_001, false), "vault", "device");
        while job.phase != Phase::SortChildren {
            job.step(1).unwrap();
        }
        job.graph_memory.counters_valid = false;
        job.graph_memory.child_labels.counters_valid = false;
        job.graph_memory.leaf_hashes.counters_valid = false;
        while job.retire_one() {}
        assert!(job.graph_memory().owners_empty());
        assert!(!job.graph_memory().counters_valid);
        assert!(!job.graph_memory().child_labels.counters_valid);
        assert!(!job.graph_memory().leaf_hashes.counters_valid);
    }

    #[test]
    fn replacement_v1_total_file_overflow_retains_group_for_bounded_retirement() {
        let mut job = ReplacementRebuildV1::new(metered_corpus(3), "vault", "device");
        while job.phase != Phase::NextGroup {
            job.step(1).unwrap();
        }
        let before = job.entry_memory();
        let group_count = job.groups.len();
        job.total_files = u64::MAX;
        assert!(job.step(1).is_err());
        assert_eq!(job.phase, Phase::Failed);
        assert_eq!(job.groups.len(), group_count);
        assert_eq!(job.entry_memory(), before);
        assert!(job.row_sort.is_none());
        assert_entry_memory_exact(&job);
        while job.retire_one() {
            assert_entry_memory_exact(&job);
        }
        assert_eq!(job.entry_memory(), ReplacementV1EntriesMemory::default());
    }

    #[test]
    fn replacement_v1_invalid_telemetry_never_controls_cleanup() {
        let mut build = ReplacementRebuildV1::new(metered_corpus(9), "vault", "device");
        build.entry_memory.counters_valid = false;
        drain(&mut build, 17);
        let invalid_empty = ReplacementV1EntriesMemory {
            counters_valid: false,
            ..ReplacementV1EntriesMemory::default()
        };
        assert_eq!(build.entry_memory(), invalid_empty);
        let tree = build.take_ready().unwrap();
        assert_eq!(tree.committed_root().unwrap().total_files, 9);
        let mut tree = tree.into_retirement();
        while tree.retire_one() {}

        let mut job = ReplacementRebuildV1::new(metered_corpus(67), "vault", "device");
        while job.phase != Phase::SortRows {
            job.step(1).unwrap();
        }
        for _ in 0..19 {
            job.step(1).unwrap();
        }
        job.entry_memory.counters_valid = false;
        while job.retire_one() {}

        assert!(job.input.is_none());
        assert!(job.groups.is_empty());
        assert!(job.row_sort.is_none());
        assert!(job.rows.is_none());
        assert!(job.leaf.is_none());
        assert!(job.retiring_rows.is_none());
        assert_eq!(job.entry_memory(), invalid_empty);
    }

    #[test]
    fn replacement_v1_entry_memory_remains_exact_during_cancel_and_failure() {
        for checkpoint in [
            Phase::Group,
            Phase::SortRows,
            Phase::MoveLeaf,
            Phase::RetireLeaf,
        ] {
            let mut job = ReplacementRebuildV1::new(metered_corpus(127), "vault", "device");
            while job.phase != checkpoint {
                job.step(1).unwrap();
            }
            if checkpoint == Phase::Group {
                for _ in 0..31 {
                    job.step(1).unwrap();
                }
            } else if checkpoint == Phase::SortRows {
                for _ in 0..53 {
                    job.step(1).unwrap();
                }
            } else if checkpoint == Phase::MoveLeaf {
                for _ in 0..17 {
                    job.step(1).unwrap();
                }
            } else {
                for _ in 0..19 {
                    job.step(1).unwrap();
                }
            }
            assert_entry_memory_exact(&job);
            while job.retire_one() {
                assert_entry_memory_exact(&job);
            }
            assert_entry_memory_exact(&job);
            assert_eq!(job.entry_memory(), ReplacementV1EntriesMemory::default());
        }

        let mut failed = ReplacementRebuildV1::new(metered_corpus(11), "vault", "device");
        failed.completed = usize::MAX;
        assert!(failed.step(1).is_err());
        assert_entry_memory_exact(&failed);
        while failed.retire_one() {
            assert_entry_memory_exact(&failed);
        }
        assert_eq!(failed.entry_memory(), ReplacementV1EntriesMemory::default());
    }

    #[test]
    fn replacement_v1_retirement_partial_25k_grouping_and_sort_does_not_resume() {
        let mut new = ReplacementRebuildV1::new(corpus(25_000, false), "vault", "device");
        assert_eq!(
            retire_to_empty(&mut new),
            25_003,
            "new input cleanup must expose each row, backing, and identities"
        );
        for sort_units in [0, 12_500, 25_123, 74_999, 175_000] {
            let mut job = ReplacementRebuildV1::new(corpus(25_000, false), "vault", "device");
            while job.phase != Phase::SortRows {
                job.step(1).unwrap();
            }
            for _ in 0..sort_units {
                assert_eq!(job.phase, Phase::SortRows);
                job.step(1).unwrap();
            }
            let units = retire_to_empty(&mut job);
            assert!(
                units >= 25_003 && units <= 75_003,
                "row/slot cleanup count differs: {units}"
            );
        }
        let mut grouped = ReplacementRebuildV1::new(corpus(25_000, true), "vault", "device");
        for _ in 0..12_500 {
            grouped.step(1).unwrap();
        }
        let groups = grouped.groups.len();
        assert_eq!(
            retire_to_empty(&mut grouped),
            25_000 + groups * 2 + 3,
            "rows, group backing, map keys, input backing, and identities retire separately"
        );
    }

    #[tokio::test]
    async fn replacement_v1_take_ready_preserves_errors_and_retire_does_not_drop_taken_tree() {
        let mut incomplete = ReplacementRebuildV1::new(corpus(5, false), "vault", "device");
        let pointer = incomplete.input.as_ref().unwrap().as_slice().as_ptr();
        assert!(incomplete.take_ready().is_err());
        assert_eq!(
            incomplete.input.as_ref().unwrap().as_slice().as_ptr(),
            pointer
        );
        assert_eq!(incomplete.input.as_ref().unwrap().len(), 5);
        drain(&mut incomplete, 256);
        let tree = incomplete.take_ready().unwrap();
        let bytes = tree.committed_root().unwrap().serialize();
        let resident = chunks(&tree);
        assert!(incomplete.take_ready().is_err());
        assert!(incomplete.step(1).is_err());
        retire_to_empty(&mut incomplete);
        assert_eq!(tree.committed_root().unwrap().serialize(), bytes);
        assert_eq!(chunks(&tree), resident);
        let mut retirement = tree.into_retirement();
        assert!(retirement.retire_one());
        while retirement.retire_one() {}

        // A violated ready invariant must return an owned error, not panic or
        // consume the other fields. Cleanup remains available after retry.
        let mut missing = ReplacementRebuildV1::new(corpus(1, false), "vault", "device");
        drain(&mut missing, 256);
        let owner = missing.result.take().unwrap();
        assert!(missing.take_ready().is_err());
        assert!(missing.take_ready().is_err());
        retire_to_empty(&mut missing);
        let mut owner = owner.into_retirement();
        while owner.retire_one() {}
    }
}
