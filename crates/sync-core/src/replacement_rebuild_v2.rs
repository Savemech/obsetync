//! Private, cooperative construction of a complete Tree v2 replacement.
//!
//! Every call processes explicit logical units. Sorting and segmentation use
//! one row/descriptor per unit; emission and closure validation use at most one
//! indivisible node codec/hash per unit. A codec allocation (up to the Tree v2
//! node cap) and allocator growth/backing-buffer release remain synchronous
//! residuals. Explicit retire_one drains abandoned owners; direct Drop is
//! unchanged. Input rows and obsolete range descriptors are retired one per
//! cheap unit before readiness. Nothing in this cursor aliases a live tree.

use crate::chunk::{ChunkError, FileEntry};
use crate::hash::{hash_bytes, FileHash};
use crate::replacement_rebuild::{
    ReplacementBuildProgress, ReplacementOutputMemoryPlanV1, SortWorkspace, StableSort,
};
use crate::store::{ChunkRetirement, ChunkStoreMemory, MemoryChunkStore};
use crate::transactional_tree_v2::TransactionalTreeV2;
use crate::tree_metadata::{
    CandidateBaselineMemory, RootMetadataMemory, StringMemory, TreeMetadataMemory,
};
use crate::tree_v2::{
    copy_path_with_exact_request, copy_range_with_exact_request, encode_internal, encode_leaf,
    gear_hash, leaf_record_len, range_record_len, replacement_root_serialized_length,
    validate_loaded_node, validate_path, validate_range, RangeRef, RootNodeV2, TreeV2Root, V2Node,
    INTERNAL_ANCHOR_MASK, INTERNAL_HEADER_BYTES, LEAF_ANCHOR_MASK, LEAF_HEADER_BYTES,
    MAX_INTERNAL_CHILDREN, MAX_LEAF_ENTRIES, MAX_LOADED_ENTRIES, MAX_NODE_BYTES, MAX_TREE_DEPTH,
    MAX_VISITED_NODES, MIN_INTERNAL_CHILDREN, MIN_LEAF_BYTES, MIN_LEAF_ENTRIES, TREE_VERSION,
};
use crate::tree_work_memory::{
    ReplacementInputMemory, ReplacementSortMemory, ReplacementV2PlanningMemory,
    ReplacementV2RangesMemory, V2RangeVector,
};
use std::collections::{hash_map, hash_set, HashMap, HashSet};
use std::vec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Sort,
    ValidateEntries,
    SegmentLeaves,
    PlanLeafDescriptors,
    PlanValidateRanges,
    PlanSegmentRanges,
    PlanEmitParents,
    PlanRetireRanges,
    PlanRoot,
    PlanReady,
    EmitLeaves,
    ValidateRanges,
    SegmentRanges,
    EmitParents,
    RetireRanges,
    Root,
    Closure,
    CleanupClosure,
    CleanupEntries,
    Ready,
    Failed,
    Retiring,
}

impl Phase {
    fn label(self) -> &'static str {
        match self {
            Self::Sort => "sort",
            Self::ValidateEntries => "validate entries",
            Self::SegmentLeaves => "segment leaves",
            Self::PlanLeafDescriptors => "plan leaf descriptors",
            Self::PlanValidateRanges => "validate planned range level",
            Self::PlanSegmentRanges => "segment planned range level",
            Self::PlanEmitParents => "plan internal level",
            Self::PlanRetireRanges => "retire planned range level",
            Self::PlanRoot => "plan root",
            Self::PlanReady => "plan ready",
            Self::EmitLeaves => "emit leaves",
            Self::ValidateRanges => "validate range level",
            Self::SegmentRanges => "segment range level",
            Self::EmitParents => "emit internal level",
            Self::RetireRanges => "retire range level",
            Self::Root => "build root",
            Self::Closure => "validate closure",
            Self::CleanupClosure => "cleanup closure",
            Self::CleanupEntries => "cleanup entries",
            Self::Ready => "ready",
            Self::Failed => "failed",
            Self::Retiring => "retiring",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Span {
    start: usize,
    end: usize,
    bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PlannedInternalSpan {
    span: Span,
    child_height: u16,
}

#[derive(Debug, Clone, Copy)]
struct PlannedRange {
    min_entry: usize,
    max_entry: usize,
    file_count: u64,
    serialized_bytes: u32,
    height: u16,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReplacementBuildPlanV2 {
    pub node_count: u64,
    pub leaf_count: u64,
    pub internal_count: u64,
    pub node_payload_bytes: u64,
    pub max_node_bytes: u32,
    pub stored_root_bytes: u32,
}

impl ReplacementBuildPlanV2 {
    fn add_node(&mut self, bytes: usize, leaf: bool) -> Result<(), ChunkError> {
        let bytes = u64::try_from(bytes).map_err(|_| invalid("Tree v2 plan byte overflow"))?;
        self.node_count = self
            .node_count
            .checked_add(1)
            .ok_or_else(|| invalid("Tree v2 plan node count overflow"))?;
        if self.node_count > MAX_VISITED_NODES as u64 {
            return Err(invalid("Tree v2 exceeds the node limit"));
        }
        if leaf {
            self.leaf_count = self
                .leaf_count
                .checked_add(1)
                .ok_or_else(|| invalid("Tree v2 plan leaf count overflow"))?;
        } else {
            self.internal_count = self
                .internal_count
                .checked_add(1)
                .ok_or_else(|| invalid("Tree v2 plan internal count overflow"))?;
        }
        self.node_payload_bytes = self
            .node_payload_bytes
            .checked_add(bytes)
            .ok_or_else(|| invalid("Tree v2 plan byte overflow"))?;
        self.max_node_bytes = self.max_node_bytes.max(bytes as u32);
        Ok(())
    }
}

/// `V2ReachabilityCursor` operates through the async `ChunkStore` trait. This
/// rebuild API is deliberately synchronous and owns a concrete private memory
/// store, so it mirrors that cursor's descriptor/content/budget algorithm using
/// direct detached reads instead of hiding a future poll inside one step.
struct ClosureValidation {
    pending: Vec<RangeRef>,
    expanding: Option<vec::IntoIter<RangeRef>>,
    reachable: HashSet<FileHash>,
    descriptors: HashMap<FileHash, RangeRef>,
    visited_nodes: usize,
    visited_entries: usize,
}

impl ClosureValidation {
    fn new(
        root: &TreeV2Root,
        range_memory: &mut ReplacementV2RangesMemory,
    ) -> Result<Self, ChunkError> {
        let pending = root
            .child
            .as_ref()
            .map(copy_range_with_exact_request)
            .transpose()?
            .into_iter()
            .collect::<Vec<_>>();
        let pending_paths = pending
            .first()
            .map_or_else(StringMemory::default, ReplacementV2RangesMemory::endpoints);
        range_memory.adopt_vector(V2RangeVector::ClosurePending, &pending, pending_paths);
        Ok(Self {
            pending,
            expanding: None,
            reachable: HashSet::new(),
            descriptors: HashMap::new(),
            visited_nodes: 0,
            visited_entries: 0,
        })
    }

    /// `true` consumes exactly one graph-edge, node-validation, or exhausted
    /// expansion-backing release unit.
    fn tick(
        &mut self,
        store: &MemoryChunkStore,
        range_memory: &mut ReplacementV2RangesMemory,
    ) -> Result<bool, ChunkError> {
        if let Some(children) = &mut self.expanding {
            if let Some(child) = children.next() {
                range_memory.remove_vector(V2RangeVector::ClosureExpanding, &child);
                self.pending.push(child);
                range_memory.push_vector(
                    V2RangeVector::ClosurePending,
                    &self.pending,
                    self.pending.last().unwrap(),
                );
                return Ok(true);
            }
            // Keep the exhausted iterator owner observable until a distinct
            // cooperative unit releases its retained Vec backing.
            self.expanding = None;
            range_memory.release_vector(V2RangeVector::ClosureExpanding);
            return Ok(true);
        }

        let Some(range) = self.pending.pop() else {
            if self.pending.capacity() != 0 {
                let allocation = std::mem::take(&mut self.pending);
                range_memory.release_vector(V2RangeVector::ClosurePending);
                drop(allocation);
                return Ok(true);
            }
            return Ok(false);
        };
        range_memory.remove_vector(V2RangeVector::ClosurePending, &range);
        if let Some(previous) = self.descriptors.get(&range.hash) {
            if previous != &range {
                return Err(invalid(
                    "Tree v2 repeated hash has a different child descriptor",
                ));
            }
            return Ok(true);
        }

        self.visited_nodes = self.visited_nodes.saturating_add(1);
        if range.height > MAX_TREE_DEPTH || self.visited_nodes > MAX_VISITED_NODES {
            return Err(invalid("Tree v2 exceeds the node limit"));
        }
        let bytes = store
            .get_chunk(&range.hash)
            .ok_or_else(|| invalid("private Tree v2 replacement chunk is missing"))?;
        match validate_loaded_node(&range, &bytes)? {
            V2Node::Leaf(entries) => {
                self.visited_entries = self.visited_entries.saturating_add(entries.len());
                if self.visited_entries > MAX_LOADED_ENTRIES {
                    return Err(invalid("Tree v2 exceeds the entry limit"));
                }
            }
            V2Node::Internal { children, .. } => {
                if !children.is_empty() {
                    let mut paths = StringMemory::default();
                    for child in &children {
                        paths.add(&child.min_path);
                        paths.add(&child.max_path);
                    }
                    range_memory.adopt_vector(V2RangeVector::ClosureExpanding, &children, paths);
                    self.expanding = Some(children.into_iter());
                }
            }
        }
        self.reachable.insert(range.hash);
        let hash = range.hash;
        self.descriptors.insert(hash, range);
        range_memory.add_descriptor(self.descriptors.get(&hash).unwrap());
        Ok(true)
    }
}

struct RootBuildRetirement {
    vault_id: Option<String>,
    child: Option<RangeRef>,
    device_id: Option<String>,
    metadata: RootMetadataMemory,
}

impl RootBuildRetirement {
    fn new(root: RootNodeV2) -> Self {
        let metadata = RootMetadataMemory::from_v2(&root);
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

    fn metadata_memory(&self) -> RootMetadataMemory {
        self.metadata
    }

    fn retire_one(&mut self) -> bool {
        if let Some(child) = self.child.take() {
            self.metadata.strings.subtract(&child.min_path);
            self.metadata.strings.subtract(&child.max_path);
            self.refresh_validity();
            self.clear_if_empty();
            return true;
        }
        if let Some(value) = self.vault_id.take() {
            self.metadata.strings.subtract(&value);
            self.refresh_validity();
            self.clear_if_empty();
            return true;
        }
        self.refresh_validity();
        if let Some(value) = self.device_id.take() {
            self.metadata.strings.subtract(&value);
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
            let counters_valid = self.metadata.counters_valid;
            let strings_valid = self.metadata.strings.counters_valid;
            self.metadata = RootMetadataMemory::default();
            self.metadata.counters_valid = counters_valid;
            self.metadata.strings.counters_valid = strings_valid;
        }
    }
}

struct ClosureRetirement {
    pending: Option<vec::IntoIter<RangeRef>>,
    expanding: Option<vec::IntoIter<RangeRef>>,
    reachable: hash_set::IntoIter<FileHash>,
    descriptors: hash_map::IntoIter<FileHash, RangeRef>,
}

impl ClosureRetirement {
    fn new(closure: ClosureValidation) -> Self {
        let ClosureValidation {
            pending,
            expanding,
            reachable,
            descriptors,
            ..
        } = closure;
        Self {
            pending: (pending.capacity() != 0).then(|| pending.into_iter()),
            expanding,
            reachable: reachable.into_iter(),
            descriptors: descriptors.into_iter(),
        }
    }

    fn retire_one(&mut self, range_memory: &mut ReplacementV2RangesMemory) -> bool {
        if let Some(pending) = &mut self.pending {
            if let Some(range) = pending.next() {
                range_memory.remove_vector(V2RangeVector::ClosurePending, &range);
                return true;
            }
            self.pending = None;
            range_memory.release_vector(V2RangeVector::ClosurePending);
            return true;
        }
        if let Some(children) = &mut self.expanding {
            if let Some(range) = children.next() {
                range_memory.remove_vector(V2RangeVector::ClosureExpanding, &range);
                return true;
            }
            self.expanding = None;
            range_memory.release_vector(V2RangeVector::ClosureExpanding);
            return true;
        }
        if self.reachable.next().is_some() {
            return true;
        }
        if let Some((_, range)) = self.descriptors.next() {
            range_memory.remove_descriptor(&range);
            return true;
        }
        false
    }
}

struct ReplacementRetirementV2 {
    sorter: Option<StableSort<FileEntry>>,
    entries: Option<vec::IntoIter<FileEntry>>,
    entry_memory: ReplacementInputMemory,
    planning_memory: ReplacementV2PlanningMemory,
    spans: Option<vec::IntoIter<Span>>,
    leaf_spans: Option<vec::IntoIter<Span>>,
    planned_ranges: Option<vec::IntoIter<PlannedRange>>,
    next_planned_ranges: Option<vec::IntoIter<PlannedRange>>,
    planned_spans: Option<vec::IntoIter<Span>>,
    planned_internal_spans: Option<vec::IntoIter<PlannedInternalSpan>>,
    ranges: Option<vec::IntoIter<RangeRef>>,
    next_ranges: Option<vec::IntoIter<RangeRef>>,
    range_memory: ReplacementV2RangesMemory,
    root: Option<RootBuildRetirement>,
    closure: Option<ClosureRetirement>,
    retiring_descriptors: hash_map::IntoIter<FileHash, RangeRef>,
    chunks: ChunkRetirement,
    vault_id: Option<String>,
    device_id: Option<String>,
}

impl ReplacementRetirementV2 {
    fn sort_memory(&self) -> ReplacementSortMemory {
        self.sorter
            .as_ref()
            .map_or_else(ReplacementSortMemory::default, |sorter| {
                ReplacementSortMemory::entries(sorter.memory())
            })
    }

    fn planning_memory(&self) -> ReplacementV2PlanningMemory {
        self.planning_memory
    }

    fn range_memory(&self) -> ReplacementV2RangesMemory {
        self.range_memory
    }

    fn post_sort_entry_memory(&self) -> Option<ReplacementInputMemory> {
        self.sorter.is_none().then_some(self.entry_memory)
    }

    fn retire_one(&mut self) -> bool {
        if let Some(sorter) = &mut self.sorter {
            if sorter.retire_one() {
                return true;
            }
            self.sorter = None;
            // The sorter destroyed every nested path and released its values
            // Vec before post-sort ownership became observable.
            self.entry_memory = ReplacementInputMemory::default();
            return true;
        }
        if let Some(entries) = &mut self.entries {
            if let Some(entry) = entries.next() {
                self.entry_memory.retire_entry(&entry);
                return true;
            }
            self.entries = None;
            self.entry_memory.release_backing();
            return true;
        }
        if let Some(spans) = &mut self.spans {
            if spans.next().is_some() {
                self.planning_memory.retire_spans();
                return true;
            }
            self.spans = None;
            self.planning_memory.release_spans();
            return true;
        }
        if let Some(spans) = &mut self.leaf_spans {
            if spans.next().is_some() {
                self.planning_memory.retire_leaf_spans();
                return true;
            }
            self.leaf_spans = None;
            self.planning_memory.release_leaf_spans();
            return true;
        }
        if let Some(ranges) = &mut self.planned_ranges {
            if ranges.next().is_some() {
                self.planning_memory.retire_planned_range();
                return true;
            }
            self.planned_ranges = None;
            self.planning_memory.release_planned_ranges();
            return true;
        }
        if let Some(ranges) = &mut self.next_planned_ranges {
            if ranges.next().is_some() {
                self.planning_memory.retire_next_planned_range();
                return true;
            }
            self.next_planned_ranges = None;
            self.planning_memory.release_next_planned_ranges();
            return true;
        }
        if let Some(spans) = &mut self.planned_spans {
            if spans.next().is_some() {
                self.planning_memory.retire_planned_span();
                return true;
            }
            self.planned_spans = None;
            self.planning_memory.release_planned_spans();
            return true;
        }
        if let Some(spans) = &mut self.planned_internal_spans {
            if spans.next().is_some() {
                self.planning_memory.retire_planned_internal_span();
                return true;
            }
            self.planned_internal_spans = None;
            self.planning_memory.release_planned_internal_spans();
            return true;
        }
        if let Some(ranges) = &mut self.ranges {
            if let Some(range) = ranges.next() {
                self.range_memory
                    .remove_vector(V2RangeVector::Ranges, &range);
                return true;
            }
            self.ranges = None;
            self.range_memory.release_vector(V2RangeVector::Ranges);
            return true;
        }
        if let Some(ranges) = &mut self.next_ranges {
            if let Some(range) = ranges.next() {
                self.range_memory
                    .remove_vector(V2RangeVector::NextRanges, &range);
                return true;
            }
            self.next_ranges = None;
            self.range_memory.release_vector(V2RangeVector::NextRanges);
            return true;
        }
        if let Some(root) = &mut self.root {
            if root.retire_one() {
                return true;
            }
            self.root = None;
        }
        if let Some(closure) = &mut self.closure {
            if closure.retire_one(&mut self.range_memory) {
                return true;
            }
            self.closure = None;
        }
        if let Some((_, range)) = self.retiring_descriptors.next() {
            self.range_memory.remove_descriptor(&range);
            return true;
        }
        if self.chunks.next().is_some() {
            return true;
        }
        if self.vault_id.take().is_some() {
            return true;
        }
        self.device_id.take().is_some()
    }
}

/// A job-local Tree v2 graph. `new` only moves the supplied input and copies
/// the bounded root identifiers; all input-sized work happens in `step`.
pub(crate) struct ReplacementRebuildV2 {
    phase: Phase,
    sorter: Option<StableSort<FileEntry>>,
    entries: Vec<FileEntry>,
    entries_owned: bool,
    entry_memory: ReplacementInputMemory,
    cursor: usize,
    segment_start: usize,
    segment_bytes: usize,
    spans: Vec<Span>,
    leaf_spans: Vec<Span>,
    planned_ranges: Vec<PlannedRange>,
    next_planned_ranges: Vec<PlannedRange>,
    planned_spans: Vec<Span>,
    planned_internal_spans: Vec<PlannedInternalSpan>,
    planned_height: u16,
    plan: ReplacementBuildPlanV2,
    planned_range_endpoint_requested_bytes: u64,
    output_memory_plan: ReplacementOutputMemoryPlanV1,
    emitted_nodes: u64,
    emitted_payload_bytes: u64,
    emitted_internal_spans: usize,
    emit: usize,
    ranges: Vec<RangeRef>,
    next_ranges: Vec<RangeRef>,
    range_memory: ReplacementV2RangesMemory,
    range_height: u16,
    root: Option<RootNodeV2>,
    closure: Option<ClosureValidation>,
    retiring_descriptors: hash_map::IntoIter<FileHash, RangeRef>,
    store: MemoryChunkStore,
    vault_id: String,
    device_id: String,
    completed: usize,
    retirement: Option<ReplacementRetirementV2>,
    #[cfg(test)]
    atomic_steps: usize,
}

impl ReplacementRebuildV2 {
    /// O(1) immutable-node component ownership; other builder fields excluded.
    pub(crate) fn chunk_memory(&self) -> ChunkStoreMemory {
        self.store.memory_summary().combine(
            self.retirement
                .as_ref()
                .map_or_else(ChunkStoreMemory::default, |cursor| {
                    cursor.chunks.memory_summary()
                }),
        )
    }

    /// Exact top-level Vec backing for the active indirect entry sorter,
    /// including a sorter already transferred into deferred retirement.
    pub(crate) fn sort_memory(&self) -> ReplacementSortMemory {
        if let Some(sorter) = &self.sorter {
            return ReplacementSortMemory::entries(sorter.memory());
        }
        self.retirement.as_ref().map_or_else(
            ReplacementSortMemory::default,
            ReplacementRetirementV2::sort_memory,
        )
    }

    /// Exact top-level backing for the six POD descriptor Vecs used by V2
    /// replacement planning and emission, including deferred retirement.
    pub(crate) fn planning_memory(&self) -> ReplacementV2PlanningMemory {
        if let Some(retirement) = &self.retirement {
            return retirement.planning_memory();
        }
        ReplacementV2PlanningMemory::new(
            &self.spans,
            &self.leaf_spans,
            &self.planned_ranges,
            &self.next_planned_ranges,
            &self.planned_spans,
            &self.planned_internal_spans,
        )
    }

    /// Exact retained backing and endpoint Strings for V2 range vectors and
    /// closure descriptors, including a builder in deferred retirement.
    pub(crate) fn range_memory(&self) -> ReplacementV2RangesMemory {
        self.retirement
            .as_ref()
            .map_or(self.range_memory, ReplacementRetirementV2::range_memory)
    }

    /// Exact V2 entry Vec and nested path String ownership after the indirect
    /// sorter returns its values. While the sorter owns them, its top-level
    /// Vec is already covered by the sort ABI and this post-sort component is
    /// deliberately unavailable rather than double-counted.
    pub(crate) fn post_sort_entry_memory(&self) -> Option<ReplacementInputMemory> {
        if let Some(retirement) = &self.retirement {
            return retirement.post_sort_entry_memory();
        }
        if self.sorter.is_some() {
            return None;
        }
        Some(if self.entries_owned {
            self.entry_memory
        } else {
            ReplacementInputMemory::default()
        })
    }

    /// Exact completed/provisional root and builder identity owners once the
    /// root exists. Earlier entry/sort/plan/range owners are outside this
    /// component and must not be represented by a fabricated zero.
    pub(crate) fn metadata_memory(&self) -> Option<TreeMetadataMemory> {
        if let Some(root) = &self.root {
            return Some(TreeMetadataMemory::new(
                RootMetadataMemory::from_v2(root),
                RootMetadataMemory::default(),
                StringMemory::of(&self.vault_id).combine(StringMemory::of(&self.device_id)),
                CandidateBaselineMemory::default(),
            ));
        }
        let retirement = self.retirement.as_ref()?;
        let mut identities = StringMemory::default();
        if let Some(value) = &retirement.vault_id {
            identities.add(value);
        }
        if let Some(value) = &retirement.device_id {
            identities.add(value);
        }
        Some(TreeMetadataMemory::new(
            retirement.root.as_ref().map_or_else(
                RootMetadataMemory::default,
                RootBuildRetirement::metadata_memory,
            ),
            RootMetadataMemory::default(),
            identities,
            CandidateBaselineMemory::default(),
        ))
    }

    #[cfg(test)]
    pub(crate) fn new(entries: Vec<FileEntry>, vault_id: &str, device_id: &str) -> Self {
        let entry_memory = ReplacementInputMemory::new(&entries);
        let workspace = SortWorkspace::try_new(entries.len()).unwrap();
        Self::new_metered_with_workspace(
            entries,
            entry_memory,
            workspace,
            vault_id.to_owned(),
            device_id.to_owned(),
        )
    }

    /// Move prepared owners and the exact accepted-input meter into the builder
    /// without allocation or another O(n) path-capacity scan. Workspace and ID
    /// preparation must happen while the caller still owns its original input.
    pub(crate) fn new_metered_with_workspace(
        entries: Vec<FileEntry>,
        mut entry_memory: ReplacementInputMemory,
        workspace: SortWorkspace,
        vault_id: String,
        device_id: String,
    ) -> Self {
        entry_memory.transferred(&entries);
        Self {
            phase: Phase::Sort,
            sorter: Some(StableSort::with_workspace(entries, workspace)),
            entries: Vec::new(),
            entries_owned: false,
            entry_memory,
            cursor: 0,
            segment_start: 0,
            segment_bytes: LEAF_HEADER_BYTES,
            spans: Vec::new(),
            leaf_spans: Vec::new(),
            planned_ranges: Vec::new(),
            next_planned_ranges: Vec::new(),
            planned_spans: Vec::new(),
            planned_internal_spans: Vec::new(),
            planned_height: 0,
            plan: ReplacementBuildPlanV2::default(),
            planned_range_endpoint_requested_bytes: 0,
            output_memory_plan: ReplacementOutputMemoryPlanV1::default(),
            emitted_nodes: 0,
            emitted_payload_bytes: 0,
            emitted_internal_spans: 0,
            emit: 0,
            ranges: Vec::new(),
            next_ranges: Vec::new(),
            range_memory: ReplacementV2RangesMemory::default(),
            range_height: 0,
            root: None,
            closure: None,
            retiring_descriptors: HashMap::new().into_iter(),
            store: MemoryChunkStore::new(),
            vault_id,
            device_id,
            completed: 0,
            retirement: None,
            #[cfg(test)]
            atomic_steps: 0,
        }
    }

    pub(crate) fn step(
        &mut self,
        max_units: usize,
    ) -> Result<ReplacementBuildProgress, ChunkError> {
        if max_units == 0 {
            self.phase = Phase::Failed;
            return Err(invalid("replacement rebuild step budget must be positive"));
        }
        if self.phase == Phase::Failed {
            return Err(invalid("replacement rebuild is in a failed state"));
        }
        if self.phase == Phase::Retiring {
            return Err(invalid("replacement rebuild retirement has started"));
        }

        if self.phase == Phase::PlanReady {
            return Ok(self.progress(0));
        }

        let mut units = 0usize;
        while units < max_units && self.phase != Phase::Ready && self.phase != Phase::PlanReady {
            let stop_after_unit = matches!(
                self.phase,
                Phase::EmitLeaves
                    | Phase::ValidateRanges
                    | Phase::EmitParents
                    | Phase::Root
                    | Phase::Closure
            );
            match self.tick() {
                Ok(true) => {
                    units += 1;
                    self.completed = self.completed.checked_add(1).ok_or_else(|| {
                        self.phase = Phase::Failed;
                        invalid("replacement rebuild progress overflow")
                    })?;
                    // A node codec/hash or one graph-edge validation is the
                    // last primitive in this native turn even when the caller
                    // supplied a larger scalar budget.
                    if stop_after_unit {
                        #[cfg(test)]
                        {
                            self.atomic_steps += 1;
                        }
                        break;
                    }
                }
                Ok(false) => {}
                Err(error) => {
                    self.phase = Phase::Failed;
                    return Err(error);
                }
            }
        }
        Ok(self.progress(units))
    }

    fn progress(&self, units: usize) -> ReplacementBuildProgress {
        ReplacementBuildProgress {
            done: self.is_ready(),
            units,
            completed: self.completed,
            phase: self.phase.label(),
        }
    }

    pub(crate) fn plan(&self) -> Result<ReplacementBuildPlanV2, ChunkError> {
        if self.phase != Phase::PlanReady {
            return Err(invalid("Tree v2 replacement plan is not ready"));
        }
        Ok(self.plan)
    }

    /// Return the version-1 immutable-output admission plan. This remains a
    /// scoped subset: endpoint bytes cover RangeRef String allocation requests
    /// only and deliberately exclude entry/decode paths and container backing.
    pub(crate) fn output_memory_plan_v1(
        &self,
    ) -> Result<ReplacementOutputMemoryPlanV1, ChunkError> {
        if self.phase != Phase::PlanReady {
            return Err(invalid(
                "Tree v2 replacement output memory plan is not ready",
            ));
        }
        Ok(self.output_memory_plan)
    }

    /// Cross the allocation boundary only after the caller has admitted the
    /// exact logical node payload total it just observed. This is not a claim
    /// about allocator capacity, planning scratch, or total native heap.
    pub(crate) fn resume_after_plan(
        &mut self,
        expected_node_payload_bytes: u64,
    ) -> Result<(), ChunkError> {
        if self.phase != Phase::PlanReady {
            return Err(invalid("Tree v2 replacement plan is not ready"));
        }
        if expected_node_payload_bytes != self.plan.node_payload_bytes {
            return Err(invalid("Tree v2 replacement plan byte witness changed"));
        }
        self.resume_plan_ready();
        Ok(())
    }

    /// Cross the same boundary using every independently planned byte
    /// component. A stale or partially copied host plan cannot start emission.
    pub(crate) fn resume_after_output_memory_plan_v1(
        &mut self,
        expected_node_payload_bytes: u64,
        expected_range_endpoint_peak_requested_bytes: u64,
        expected_range_endpoint_resident_requested_bytes: u64,
    ) -> Result<(), ChunkError> {
        if self.phase != Phase::PlanReady {
            return Err(invalid(
                "Tree v2 replacement output memory plan is not ready",
            ));
        }
        let expected = self.output_memory_plan;
        if expected_node_payload_bytes != expected.node_payload_bytes
            || expected_range_endpoint_peak_requested_bytes
                != expected.range_endpoint_peak_requested_bytes
            || expected_range_endpoint_resident_requested_bytes
                != expected.range_endpoint_resident_requested_bytes
        {
            return Err(invalid(
                "Tree v2 replacement output memory plan witness changed",
            ));
        }
        self.resume_plan_ready();
        Ok(())
    }

    fn resume_plan_ready(&mut self) {
        self.spans = std::mem::take(&mut self.leaf_spans);
        self.emit = 0;
        self.emitted_internal_spans = 0;
        self.phase = Phase::EmitLeaves;
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.phase == Phase::Ready
    }

    /// Move the verified result without consuming this cursor's drained
    /// backing allocations. Every validation happens before the first move;
    /// success irreversibly disables further build work.
    pub(crate) fn take_ready(&mut self) -> Result<TransactionalTreeV2, ChunkError> {
        if self.phase != Phase::Ready {
            return Err(invalid("replacement rebuild is not ready"));
        }
        if self.root.is_none() {
            return Err(invalid("verified replacement root is missing"));
        }
        let root = self.root.take().unwrap();
        let store = std::mem::take(&mut self.store);
        let vault_id = std::mem::take(&mut self.vault_id);
        let device_id = std::mem::take(&mut self.device_id);
        self.phase = Phase::Retiring;
        Ok(TransactionalTreeV2::from_verified_replacement(
            root, store, vault_id, device_id,
        ))
    }

    pub(crate) fn finish(mut self) -> Result<TransactionalTreeV2, ChunkError> {
        self.take_ready()
    }

    /// Start or continue irreversible bounded cleanup. `true` destroys one
    /// populated owner; `false` means only drained backing allocations remain.
    pub(crate) fn retire_one(&mut self) -> bool {
        if self.retirement.is_none() {
            self.phase = Phase::Retiring;
            let planning_memory = self.planning_memory();
            let sorter = self.sorter.take();
            let entries = (sorter.is_none() && self.entries_owned)
                .then(|| std::mem::take(&mut self.entries).into_iter());
            self.entries_owned = false;
            let ranges = std::mem::take(&mut self.ranges);
            let ranges = (ranges.capacity() != 0).then(|| ranges.into_iter());
            let next_ranges = std::mem::take(&mut self.next_ranges);
            let next_ranges = (next_ranges.capacity() != 0).then(|| next_ranges.into_iter());
            self.retirement = Some(ReplacementRetirementV2 {
                sorter,
                entries,
                entry_memory: self.entry_memory,
                planning_memory,
                spans: Some(std::mem::take(&mut self.spans).into_iter()),
                leaf_spans: Some(std::mem::take(&mut self.leaf_spans).into_iter()),
                planned_ranges: Some(std::mem::take(&mut self.planned_ranges).into_iter()),
                next_planned_ranges: Some(
                    std::mem::take(&mut self.next_planned_ranges).into_iter(),
                ),
                planned_spans: Some(std::mem::take(&mut self.planned_spans).into_iter()),
                planned_internal_spans: Some(
                    std::mem::take(&mut self.planned_internal_spans).into_iter(),
                ),
                ranges,
                next_ranges,
                range_memory: std::mem::take(&mut self.range_memory),
                root: self.root.take().map(RootBuildRetirement::new),
                closure: self.closure.take().map(ClosureRetirement::new),
                retiring_descriptors: std::mem::replace(
                    &mut self.retiring_descriptors,
                    HashMap::new().into_iter(),
                ),
                chunks: std::mem::take(&mut self.store).into_chunks(),
                vault_id: Some(std::mem::take(&mut self.vault_id)),
                device_id: Some(std::mem::take(&mut self.device_id)),
            });
        }
        self.retirement.as_mut().unwrap().retire_one()
    }

    /// `true` means exactly one declared unit was completed. Phase transitions
    /// themselves are free and may chain before the next unit.
    fn tick(&mut self) -> Result<bool, ChunkError> {
        match self.phase {
            Phase::Sort => {
                let sorter = self
                    .sorter
                    .as_mut()
                    .expect("sort phase must own its stable sorter");
                if sorter.tick() {
                    return Ok(true);
                }
                self.entries = self.sorter.take().unwrap().finish();
                self.entries_owned = true;
                self.entry_memory.transferred(&self.entries);
                if self.entries.len() > MAX_LOADED_ENTRIES {
                    return Err(invalid("Tree v2 exceeds the entry limit"));
                }
                self.cursor = 0;
                self.phase = Phase::ValidateEntries;
                Ok(false)
            }
            Phase::ValidateEntries => {
                if self.cursor == self.entries.len() {
                    self.cursor = 0;
                    self.segment_start = 0;
                    self.segment_bytes = LEAF_HEADER_BYTES;
                    self.phase = Phase::SegmentLeaves;
                    return Ok(false);
                }
                let entry = &self.entries[self.cursor];
                validate_path(&entry.path)?;
                if self.cursor > 0 && self.entries[self.cursor - 1].path >= entry.path {
                    return Err(invalid("Tree v2 entries are not strictly ordered"));
                }
                self.cursor += 1;
                Ok(true)
            }
            Phase::SegmentLeaves => {
                if self.cursor == self.entries.len() {
                    if self.segment_start < self.entries.len() {
                        self.spans.push(Span {
                            start: self.segment_start,
                            end: self.entries.len(),
                            bytes: self.segment_bytes,
                        });
                    }
                    self.leaf_spans = std::mem::take(&mut self.spans);
                    self.cursor = 0;
                    self.plan = ReplacementBuildPlanV2::default();
                    self.planned_range_endpoint_requested_bytes = 0;
                    self.output_memory_plan = ReplacementOutputMemoryPlanV1::default();
                    self.phase = Phase::PlanLeafDescriptors;
                    return Ok(false);
                }
                let index = self.cursor;
                let length = leaf_record_len(&self.entries[index]);
                self.segment_record(
                    index,
                    length,
                    LEAF_HEADER_BYTES,
                    MIN_LEAF_ENTRIES,
                    MAX_LEAF_ENTRIES,
                    MIN_LEAF_BYTES,
                    LEAF_ANCHOR_MASK,
                    gear_hash(self.entries[index].path.as_bytes()),
                )?;
                self.cursor += 1;
                Ok(true)
            }
            Phase::PlanLeafDescriptors => {
                if self.cursor == self.leaf_spans.len() {
                    self.cursor = 0;
                    self.start_planned_range_level_or_root()?;
                    return Ok(false);
                }
                let span = self.leaf_spans[self.cursor];
                let serialized_bytes = u32::try_from(span.bytes).map_err(|_| {
                    invalid("Tree v2 planned leaf length does not fit its descriptor")
                })?;
                self.add_planned_range_endpoint_bytes(span.start, span.end - 1)?;
                self.plan.add_node(span.bytes, true)?;
                self.planned_ranges.push(PlannedRange {
                    min_entry: span.start,
                    max_entry: span.end - 1,
                    file_count: (span.end - span.start) as u64,
                    serialized_bytes,
                    height: 0,
                });
                self.cursor += 1;
                Ok(true)
            }
            Phase::PlanValidateRanges => {
                if self.cursor == self.planned_ranges.len() {
                    self.cursor = 0;
                    self.segment_start = 0;
                    self.segment_bytes = INTERNAL_HEADER_BYTES;
                    self.planned_spans.clear();
                    self.phase = Phase::PlanSegmentRanges;
                    return Ok(false);
                }
                let current = self.planned_ranges[self.cursor];
                if current.height != self.planned_height
                    || current.min_entry > current.max_entry
                    || current.max_entry >= self.entries.len()
                    || current.serialized_bytes == 0
                    || current.serialized_bytes as usize > MAX_NODE_BYTES
                {
                    return Err(invalid("Tree v2 planned range is inconsistent"));
                }
                if self.cursor > 0 {
                    let previous = self.planned_ranges[self.cursor - 1];
                    if previous.max_entry >= current.min_entry {
                        return Err(invalid("Tree v2 planned ranges overlap or are unordered"));
                    }
                }
                self.cursor += 1;
                Ok(true)
            }
            Phase::PlanSegmentRanges => {
                if self.cursor == self.planned_ranges.len() {
                    if self.segment_start < self.planned_ranges.len() {
                        self.planned_spans.push(Span {
                            start: self.segment_start,
                            end: self.planned_ranges.len(),
                            bytes: self.segment_bytes,
                        });
                    }
                    self.next_planned_ranges.clear();
                    self.emit = 0;
                    self.phase = Phase::PlanEmitParents;
                    return Ok(false);
                }
                let index = self.cursor;
                let range = self.planned_ranges[index];
                let length = self.planned_range_record_len(range)?;
                let anchor_hash = gear_hash(self.entries[range.max_entry].path.as_bytes());
                self.segment_planned_record(index, length, anchor_hash)?;
                self.cursor += 1;
                Ok(true)
            }
            Phase::PlanEmitParents => {
                if self.emit == self.planned_spans.len() {
                    self.emit = 0;
                    self.phase = Phase::PlanRetireRanges;
                    return Ok(false);
                }
                let span = self.planned_spans[self.emit];
                let children = &self.planned_ranges[span.start..span.end];
                let file_count = children.iter().try_fold(0u64, |sum, child| {
                    sum.checked_add(child.file_count)
                        .ok_or_else(|| invalid("Tree v2 file count overflow"))
                })?;
                let serialized_bytes = u32::try_from(span.bytes).map_err(|_| {
                    invalid("Tree v2 planned internal length does not fit its descriptor")
                })?;
                let first = children[0];
                let last = children[children.len() - 1];
                self.add_planned_range_endpoint_bytes(first.min_entry, last.max_entry)?;
                self.plan.add_node(span.bytes, false)?;
                self.planned_internal_spans.push(PlannedInternalSpan {
                    span,
                    child_height: self.planned_height,
                });
                self.next_planned_ranges.push(PlannedRange {
                    min_entry: first.min_entry,
                    max_entry: last.max_entry,
                    file_count,
                    serialized_bytes,
                    height: self.planned_height + 1,
                });
                self.emit += 1;
                Ok(true)
            }
            Phase::PlanRetireRanges => {
                if self.planned_ranges.pop().is_some() {
                    return Ok(true);
                }
                self.planned_ranges = std::mem::take(&mut self.next_planned_ranges);
                self.start_planned_range_level_or_root()?;
                Ok(false)
            }
            Phase::PlanRoot => {
                let (child_paths, root_endpoint_bytes) = match self.planned_ranges.as_slice() {
                    [] => (None, 0),
                    [child] => {
                        let min_path = self.entries[child.min_entry].path.as_str();
                        let max_path = self.entries[child.max_entry].path.as_str();
                        (
                            Some((min_path, max_path)),
                            Self::endpoint_pair_bytes(min_path, max_path)?,
                        )
                    }
                    _ => return Err(invalid("Tree v2 planned root level is incomplete")),
                };
                self.plan.stored_root_bytes = u32::try_from(replacement_root_serialized_length(
                    &self.vault_id,
                    &self.device_id,
                    child_paths,
                )?)
                .map_err(|_| invalid("Tree v2 planned root length does not fit the ABI"))?;
                self.output_memory_plan = Self::build_output_memory_plan_v1(
                    self.plan.node_payload_bytes,
                    self.planned_range_endpoint_requested_bytes,
                    root_endpoint_bytes,
                )?;
                self.planned_ranges = Vec::new();
                self.next_planned_ranges = Vec::new();
                self.planned_spans = Vec::new();
                self.phase = Phase::PlanReady;
                Ok(true)
            }
            Phase::PlanReady => Ok(false),
            Phase::EmitLeaves => {
                if self.emit == self.spans.len() {
                    self.spans.clear();
                    self.emit = 0;
                    self.start_range_level_or_root()?;
                    return Ok(false);
                }
                let span = self.spans[self.emit];
                let entries = &self.entries[span.start..span.end];
                let bytes = encode_leaf(entries)?;
                if bytes.len() != span.bytes {
                    return Err(invalid("Tree v2 emitted leaf differs from its plan"));
                }
                let hash = hash_bytes(&bytes);
                let serialized_bytes = u32::try_from(bytes.len())
                    .map_err(|_| invalid("Tree v2 leaf length does not fit its descriptor"))?;
                let range = RangeRef {
                    min_path: copy_path_with_exact_request(&entries[0].path)?,
                    max_path: copy_path_with_exact_request(&entries[entries.len() - 1].path)?,
                    hash,
                    file_count: entries.len() as u64,
                    serialized_bytes,
                    height: 0,
                };
                self.store.insert_chunk(hash, bytes);
                self.ranges.push(range);
                self.range_memory.push_vector(
                    V2RangeVector::Ranges,
                    &self.ranges,
                    self.ranges.last().unwrap(),
                );
                self.record_emitted_node(span.bytes)?;
                self.emit += 1;
                Ok(true)
            }
            Phase::ValidateRanges => {
                if self.cursor == self.ranges.len() {
                    self.cursor = 0;
                    self.segment_start = 0;
                    self.segment_bytes = INTERNAL_HEADER_BYTES;
                    self.spans.clear();
                    self.phase = Phase::SegmentRanges;
                    return Ok(false);
                }
                let current = &self.ranges[self.cursor];
                if current.height != self.range_height {
                    return Err(invalid("Tree v2 range height is inconsistent"));
                }
                validate_range(current)?;
                if self.cursor > 0 {
                    let previous = &self.ranges[self.cursor - 1];
                    if previous.max_path >= current.min_path
                        || previous.max_path >= current.max_path
                    {
                        return Err(invalid("Tree v2 ranges overlap or are unordered"));
                    }
                }
                self.cursor += 1;
                Ok(true)
            }
            Phase::SegmentRanges => {
                if self.cursor == self.ranges.len() {
                    if self.segment_start < self.ranges.len() {
                        self.spans.push(Span {
                            start: self.segment_start,
                            end: self.ranges.len(),
                            bytes: self.segment_bytes,
                        });
                    }
                    self.next_ranges.clear();
                    self.emit = 0;
                    self.phase = Phase::EmitParents;
                    return Ok(false);
                }
                let index = self.cursor;
                let length = range_record_len(&self.ranges[index]);
                self.segment_record(
                    index,
                    length,
                    INTERNAL_HEADER_BYTES,
                    MIN_INTERNAL_CHILDREN,
                    MAX_INTERNAL_CHILDREN,
                    0,
                    INTERNAL_ANCHOR_MASK,
                    gear_hash(self.ranges[index].max_path.as_bytes()),
                )?;
                self.cursor += 1;
                Ok(true)
            }
            Phase::EmitParents => {
                if self.emit == self.spans.len() {
                    self.spans.clear();
                    self.emit = 0;
                    self.phase = Phase::RetireRanges;
                    return Ok(false);
                }
                let span = self.spans[self.emit];
                let expected = self
                    .planned_internal_spans
                    .get(self.emitted_internal_spans)
                    .ok_or_else(|| invalid("Tree v2 emitted an unplanned internal node"))?;
                if expected.span != span || expected.child_height != self.range_height {
                    return Err(invalid(
                        "Tree v2 internal segmentation differs from its plan",
                    ));
                }
                let (bytes, hash, serialized_bytes, file_count, height) = {
                    let children = &self.ranges[span.start..span.end];
                    let bytes = encode_internal(self.range_height, children)?;
                    if bytes.len() != span.bytes {
                        return Err(invalid(
                            "Tree v2 emitted internal node differs from its plan",
                        ));
                    }
                    let hash = hash_bytes(&bytes);
                    let serialized_bytes = u32::try_from(bytes.len()).map_err(|_| {
                        invalid("Tree v2 internal length does not fit its descriptor")
                    })?;
                    let file_count = children.iter().try_fold(0u64, |sum, child| {
                        sum.checked_add(child.file_count)
                            .ok_or_else(|| invalid("Tree v2 file count overflow"))
                    })?;
                    (
                        bytes,
                        hash,
                        serialized_bytes,
                        file_count,
                        self.range_height + 1,
                    )
                };
                self.store.insert_chunk(hash, bytes);
                // The encoded segment is immutable and no later phase reads
                // its boundary paths. Move their allocations into the parent;
                // the child descriptors remain real, empty String owners until
                // bounded RetireRanges removes each row.
                let min_memory = StringMemory::of(&self.ranges[span.start].min_path);
                let min_path = std::mem::take(&mut self.ranges[span.start].min_path);
                self.range_memory.replace_vector_endpoint(
                    V2RangeVector::Ranges,
                    min_memory,
                    &self.ranges[span.start].min_path,
                );
                let last = span.end - 1;
                let max_memory = StringMemory::of(&self.ranges[last].max_path);
                let max_path = std::mem::take(&mut self.ranges[last].max_path);
                self.range_memory.replace_vector_endpoint(
                    V2RangeVector::Ranges,
                    max_memory,
                    &self.ranges[last].max_path,
                );
                let range = RangeRef {
                    min_path,
                    max_path,
                    hash,
                    file_count,
                    serialized_bytes,
                    height,
                };
                self.next_ranges.push(range);
                self.range_memory.push_vector(
                    V2RangeVector::NextRanges,
                    &self.next_ranges,
                    self.next_ranges.last().unwrap(),
                );
                self.record_emitted_node(span.bytes)?;
                self.emitted_internal_spans += 1;
                self.emit += 1;
                Ok(true)
            }
            Phase::RetireRanges => {
                if let Some(range) = self.ranges.pop() {
                    self.range_memory
                        .remove_vector(V2RangeVector::Ranges, &range);
                    return Ok(true);
                }
                if self.ranges.capacity() != 0 {
                    let allocation = std::mem::take(&mut self.ranges);
                    self.range_memory.release_vector(V2RangeVector::Ranges);
                    drop(allocation);
                    return Ok(true);
                }
                self.ranges = std::mem::take(&mut self.next_ranges);
                self.range_memory
                    .move_vector(V2RangeVector::NextRanges, V2RangeVector::Ranges);
                self.start_range_level_or_root()?;
                Ok(false)
            }
            Phase::Root => {
                let child = match self.ranges.len() {
                    0 => None,
                    1 => {
                        let range = self.ranges.pop().unwrap();
                        self.range_memory
                            .remove_vector(V2RangeVector::Ranges, &range);
                        Some(range)
                    }
                    _ => return Err(invalid("Tree v2 replacement root level is incomplete")),
                };
                let allocation = std::mem::take(&mut self.ranges);
                self.range_memory.release_vector(V2RangeVector::Ranges);
                drop(allocation);
                let tree = TreeV2Root {
                    version: TREE_VERSION,
                    total_files: self.entries.len() as u64,
                    child,
                };
                let root = RootNodeV2::new(self.vault_id.clone(), self.device_id.clone(), tree)?;
                if root.serialized_length()? != self.plan.stored_root_bytes as usize {
                    return Err(invalid("Tree v2 emitted root differs from its plan"));
                }
                self.root = Some(root);
                self.closure = Some(ClosureValidation::new(
                    &self.root.as_ref().unwrap().tree,
                    &mut self.range_memory,
                )?);
                self.phase = Phase::Closure;
                Ok(true)
            }
            Phase::Closure => {
                let closure = self
                    .closure
                    .as_mut()
                    .expect("closure phase must own its validation cursor");
                if closure.tick(&self.store, &mut self.range_memory)? {
                    return Ok(true);
                }
                let closure = self
                    .closure
                    .take()
                    .expect("completed closure cursor must remain owned");
                self.retiring_descriptors = closure.descriptors.into_iter();
                self.phase = Phase::CleanupClosure;
                Ok(false)
            }
            Phase::CleanupClosure => {
                if let Some((_, range)) = self.retiring_descriptors.next() {
                    self.range_memory.remove_descriptor(&range);
                    return Ok(true);
                }
                self.phase = Phase::CleanupEntries;
                Ok(false)
            }
            Phase::CleanupEntries => {
                if let Some(entry) = self.entries.pop() {
                    self.entry_memory.retire_entry(&entry);
                    return Ok(true);
                }
                if self.entries_owned {
                    self.entries = Vec::new();
                    self.entries_owned = false;
                    self.entry_memory.release_backing();
                    return Ok(true);
                }
                self.phase = Phase::Ready;
                Ok(false)
            }
            Phase::Ready => Ok(false),
            Phase::Failed => Err(invalid("replacement rebuild is in a failed state")),
            Phase::Retiring => Err(invalid("replacement rebuild retirement has started")),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn segment_record(
        &mut self,
        index: usize,
        length: usize,
        header_bytes: usize,
        min_records: usize,
        max_records: usize,
        min_bytes: usize,
        anchor_mask: u64,
        anchor_hash: u64,
    ) -> Result<(), ChunkError> {
        if header_bytes.saturating_add(length) > MAX_NODE_BYTES {
            return Err(invalid("one Tree v2 record exceeds the node cap"));
        }
        if index > self.segment_start
            && (index - self.segment_start >= max_records
                || self.segment_bytes.saturating_add(length) > MAX_NODE_BYTES)
        {
            self.spans.push(Span {
                start: self.segment_start,
                end: index,
                bytes: self.segment_bytes,
            });
            self.segment_start = index;
            self.segment_bytes = header_bytes;
        }
        self.segment_bytes = self
            .segment_bytes
            .checked_add(length)
            .ok_or_else(|| invalid("Tree v2 segment length overflow"))?;
        let records = index - self.segment_start + 1;
        let natural = records >= min_records
            && self.segment_bytes >= min_bytes
            && anchor_hash & anchor_mask == 0;
        let forced = records >= max_records || self.segment_bytes == MAX_NODE_BYTES;
        if natural || forced {
            self.spans.push(Span {
                start: self.segment_start,
                end: index + 1,
                bytes: self.segment_bytes,
            });
            self.segment_start = index + 1;
            self.segment_bytes = header_bytes;
        }
        Ok(())
    }

    fn planned_range_record_len(&self, range: PlannedRange) -> Result<usize, ChunkError> {
        let min_path = self
            .entries
            .get(range.min_entry)
            .ok_or_else(|| invalid("Tree v2 planned minimum entry is missing"))?
            .path
            .len();
        let max_path = self
            .entries
            .get(range.max_entry)
            .ok_or_else(|| invalid("Tree v2 planned maximum entry is missing"))?
            .path
            .len();
        48usize
            .checked_add(min_path)
            .and_then(|value| value.checked_add(max_path))
            .ok_or_else(|| invalid("Tree v2 planned range length overflow"))
    }

    fn segment_planned_record(
        &mut self,
        index: usize,
        length: usize,
        anchor_hash: u64,
    ) -> Result<(), ChunkError> {
        if INTERNAL_HEADER_BYTES.saturating_add(length) > MAX_NODE_BYTES {
            return Err(invalid("one Tree v2 planned range exceeds the node cap"));
        }
        if index > self.segment_start
            && (index - self.segment_start >= MAX_INTERNAL_CHILDREN
                || self.segment_bytes.saturating_add(length) > MAX_NODE_BYTES)
        {
            self.planned_spans.push(Span {
                start: self.segment_start,
                end: index,
                bytes: self.segment_bytes,
            });
            self.segment_start = index;
            self.segment_bytes = INTERNAL_HEADER_BYTES;
        }
        self.segment_bytes = self
            .segment_bytes
            .checked_add(length)
            .ok_or_else(|| invalid("Tree v2 planned segment length overflow"))?;
        let records = index - self.segment_start + 1;
        let natural = records >= MIN_INTERNAL_CHILDREN && anchor_hash & INTERNAL_ANCHOR_MASK == 0;
        let forced = records >= MAX_INTERNAL_CHILDREN || self.segment_bytes == MAX_NODE_BYTES;
        if natural || forced {
            self.planned_spans.push(Span {
                start: self.segment_start,
                end: index + 1,
                bytes: self.segment_bytes,
            });
            self.segment_start = index + 1;
            self.segment_bytes = INTERNAL_HEADER_BYTES;
        }
        Ok(())
    }

    fn start_planned_range_level_or_root(&mut self) -> Result<(), ChunkError> {
        if self.planned_ranges.len() <= 1 {
            self.phase = Phase::PlanRoot;
            return Ok(());
        }
        self.planned_height = self.planned_ranges[0].height;
        if self.planned_height >= MAX_TREE_DEPTH {
            return Err(invalid("Tree v2 exceeds the depth limit"));
        }
        self.cursor = 0;
        self.phase = Phase::PlanValidateRanges;
        Ok(())
    }

    fn endpoint_pair_bytes(min_path: &str, max_path: &str) -> Result<u64, ChunkError> {
        let min_path = u64::try_from(min_path.len())
            .map_err(|_| invalid("Tree v2 planned endpoint byte overflow"))?;
        let max_path = u64::try_from(max_path.len())
            .map_err(|_| invalid("Tree v2 planned endpoint byte overflow"))?;
        min_path
            .checked_add(max_path)
            .ok_or_else(|| invalid("Tree v2 planned endpoint byte overflow"))
    }

    fn build_output_memory_plan_v1(
        node_payload_bytes: u64,
        descriptor_endpoint_bytes: u64,
        root_endpoint_bytes: u64,
    ) -> Result<ReplacementOutputMemoryPlanV1, ChunkError> {
        let endpoint_peak = descriptor_endpoint_bytes
            .checked_add(root_endpoint_bytes)
            .ok_or_else(|| invalid("Tree v2 planned endpoint byte overflow"))?;
        let peak_admission_bytes = node_payload_bytes
            .checked_add(endpoint_peak)
            .ok_or_else(|| invalid("Tree v2 output memory plan byte overflow"))?;
        let resident_admission_bytes = node_payload_bytes
            .checked_add(root_endpoint_bytes)
            .ok_or_else(|| invalid("Tree v2 output memory plan byte overflow"))?;
        Ok(ReplacementOutputMemoryPlanV1 {
            node_payload_bytes,
            range_endpoint_peak_requested_bytes: endpoint_peak,
            range_endpoint_resident_requested_bytes: root_endpoint_bytes,
            peak_admission_bytes,
            resident_admission_bytes,
        })
    }

    fn add_planned_range_endpoint_bytes(
        &mut self,
        min_entry: usize,
        max_entry: usize,
    ) -> Result<(), ChunkError> {
        let bytes = Self::endpoint_pair_bytes(
            &self.entries[min_entry].path,
            &self.entries[max_entry].path,
        )?;
        self.planned_range_endpoint_requested_bytes = self
            .planned_range_endpoint_requested_bytes
            .checked_add(bytes)
            .ok_or_else(|| invalid("Tree v2 planned endpoint byte overflow"))?;
        Ok(())
    }

    fn record_emitted_node(&mut self, bytes: usize) -> Result<(), ChunkError> {
        self.emitted_nodes = self
            .emitted_nodes
            .checked_add(1)
            .ok_or_else(|| invalid("Tree v2 emitted node count overflow"))?;
        self.emitted_payload_bytes = self
            .emitted_payload_bytes
            .checked_add(
                u64::try_from(bytes)
                    .map_err(|_| invalid("Tree v2 emitted payload byte overflow"))?,
            )
            .ok_or_else(|| invalid("Tree v2 emitted payload byte overflow"))?;
        Ok(())
    }

    fn start_range_level_or_root(&mut self) -> Result<(), ChunkError> {
        if self.ranges.len() <= 1 {
            if self.emitted_nodes != self.plan.node_count
                || self.emitted_payload_bytes != self.plan.node_payload_bytes
                || self.emitted_internal_spans != self.planned_internal_spans.len()
            {
                return Err(invalid("Tree v2 emitted graph differs from its plan"));
            }
            self.planned_internal_spans.clear();
            self.phase = Phase::Root;
            return Ok(());
        }
        self.range_height = self.ranges[0].height;
        if self.range_height >= MAX_TREE_DEPTH {
            return Err(invalid("Tree v2 exceeds the depth limit"));
        }
        self.cursor = 0;
        self.phase = Phase::ValidateRanges;
        Ok(())
    }
}

fn invalid(message: impl Into<String>) -> ChunkError {
    ChunkError::Deserialize(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn entry(path: impl Into<String>, revision: u64) -> FileEntry {
        let path = path.into();
        FileEntry::new(
            path.clone(),
            hash_bytes(format!("{path}:{revision}").as_bytes()),
            revision,
            revision.wrapping_mul(17),
        )
    }

    fn drain(mut builder: ReplacementRebuildV2, budget: usize) -> TransactionalTreeV2 {
        let mut completed = 0usize;
        while !builder.is_ready() {
            let atomic_steps = builder.atomic_steps;
            let progress = builder.step(budget).unwrap();
            assert!(progress.units <= budget);
            assert!(progress.completed >= completed);
            assert!(progress.phase.len() <= 64);
            assert!(progress.done || progress.units > 0);
            assert!(builder.atomic_steps - atomic_steps <= 1);
            completed = progress.completed;
            if builder.phase == Phase::PlanReady {
                let plan = builder.plan().unwrap();
                assert_eq!(builder.store.len(), 0);
                assert_eq!(builder.chunk_memory(), ChunkStoreMemory::default());
                builder.resume_after_plan(plan.node_payload_bytes).unwrap();
            }
        }
        assert!(builder.entries.is_empty());
        assert!(builder.closure.is_none());
        assert_eq!(builder.retiring_descriptors.len(), 0);
        builder.finish().unwrap()
    }

    fn chunks(tree: &TransactionalTreeV2) -> BTreeMap<FileHash, Vec<u8>> {
        tree.resident_store_for_test()
            .all_chunks()
            .into_iter()
            .collect()
    }

    fn output_endpoint_oracle(tree: &TransactionalTreeV2) -> (u64, u64) {
        let root = tree.committed_root().unwrap();
        let Some(child) = root.tree.child.clone() else {
            return (0, 0);
        };
        let root_endpoint_bytes = (child.min_path.len() + child.max_path.len()) as u64;
        let chunks = chunks(tree);
        let mut pending = vec![child];
        let mut descriptor_endpoint_bytes = 0u64;
        while let Some(range) = pending.pop() {
            descriptor_endpoint_bytes += (range.min_path.len() + range.max_path.len()) as u64;
            let bytes = chunks.get(&range.hash).unwrap();
            match validate_loaded_node(&range, bytes).unwrap() {
                V2Node::Leaf(_) => {}
                V2Node::Internal { children, .. } => pending.extend(children),
            }
        }
        (descriptor_endpoint_bytes, root_endpoint_bytes)
    }

    fn planning_components(
        memory: ReplacementV2PlanningMemory,
    ) -> [crate::tree_work_memory::VecBackingMemory; 6] {
        [
            memory.spans,
            memory.leaf_spans,
            memory.planned_ranges,
            memory.next_planned_ranges,
            memory.planned_spans,
            memory.planned_internal_spans,
        ]
    }

    fn range_paths<'a>(ranges: impl IntoIterator<Item = &'a RangeRef>) -> StringMemory {
        let mut paths = StringMemory::default();
        for range in ranges {
            paths.add(&range.min_path);
            paths.add(&range.max_path);
        }
        paths
    }

    fn range_vec_memory(values: &Vec<RangeRef>) -> crate::tree_work_memory::VecBackingMemory {
        if values.capacity() == 0 {
            Default::default()
        } else {
            crate::tree_work_memory::VecBackingMemory::of(values)
        }
    }

    fn assert_active_range_memory_exact(builder: &ReplacementRebuildV2) {
        assert!(builder.retirement.is_none());
        let memory = builder.range_memory();
        assert_eq!(memory.ranges, range_vec_memory(&builder.ranges));
        assert_eq!(memory.next_ranges, range_vec_memory(&builder.next_ranges));
        let mut vector_paths = range_paths(&builder.ranges);
        vector_paths.add_memory(range_paths(&builder.next_ranges));
        let mut descriptor_paths = StringMemory::default();
        let descriptor_ranges;
        if let Some(closure) = &builder.closure {
            assert_eq!(memory.closure_pending, range_vec_memory(&closure.pending));
            vector_paths.add_memory(range_paths(&closure.pending));
            if let Some(expanding) = &closure.expanding {
                assert_eq!(memory.closure_expanding.length_slots, expanding.len());
                assert_eq!(memory.closure_expanding.owners, 1);
                assert_eq!(
                    memory.closure_expanding.slot_size_bytes,
                    std::mem::size_of::<RangeRef>()
                );
                vector_paths.add_memory(range_paths(expanding.as_slice()));
            } else {
                assert_eq!(memory.closure_expanding, Default::default());
            }
            descriptor_ranges = closure.descriptors.len();
            descriptor_paths = range_paths(closure.descriptors.values());
        } else {
            assert_eq!(memory.closure_pending, Default::default());
            assert_eq!(memory.closure_expanding, Default::default());
            descriptor_ranges = builder.retiring_descriptors.len();
        }
        assert_eq!(memory.vector_paths, vector_paths);
        assert_eq!(memory.descriptor_ranges, descriptor_ranges);
        if builder.closure.is_some() {
            assert_eq!(memory.descriptor_paths, descriptor_paths);
        } else {
            assert_eq!(memory.descriptor_paths.strings, descriptor_ranges * 2);
        }
        assert!(memory.counters_valid);
    }

    fn assert_active_planning_memory_exact(builder: &ReplacementRebuildV2) {
        assert!(builder.retirement.is_none());
        assert_eq!(
            builder.planning_memory(),
            ReplacementV2PlanningMemory::new(
                &builder.spans,
                &builder.leaf_spans,
                &builder.planned_ranges,
                &builder.next_planned_ranges,
                &builder.planned_spans,
                &builder.planned_internal_spans,
            )
        );
    }

    fn assert_retiring_planning_memory_exact(builder: &ReplacementRebuildV2) {
        let retirement = builder
            .retirement
            .as_ref()
            .expect("replacement planning owner was not transferred");
        let memory = retirement.planning_memory();
        let observed = planning_components(memory);
        let remaining = [
            retirement.spans.as_ref().map(ExactSizeIterator::len),
            retirement.leaf_spans.as_ref().map(ExactSizeIterator::len),
            retirement
                .planned_ranges
                .as_ref()
                .map(ExactSizeIterator::len),
            retirement
                .next_planned_ranges
                .as_ref()
                .map(ExactSizeIterator::len),
            retirement
                .planned_spans
                .as_ref()
                .map(ExactSizeIterator::len),
            retirement
                .planned_internal_spans
                .as_ref()
                .map(ExactSizeIterator::len),
        ];
        for (component, remaining) in observed.into_iter().zip(remaining) {
            match remaining {
                Some(length) => {
                    assert_eq!(component.owners, 1);
                    assert_eq!(component.length_slots, length);
                    assert!(component.length_slots <= component.capacity_slots);
                    assert!(component.counters_valid);
                }
                None => assert_eq!(component, Default::default()),
            }
        }
        assert!(memory.counters_valid);
    }

    fn retire(builder: &mut ReplacementRebuildV2) -> usize {
        let mut units = 0usize;
        while builder.retire_one() {
            units += 1;
            assert!(units < 2_000_000, "replacement retirement did not converge");
        }
        assert_eq!(builder.phase, Phase::Retiring);
        assert!(!builder.is_ready());
        assert!(!builder.retire_one());
        assert!(builder.step(1).is_err());
        assert!(builder.take_ready().is_err());
        units
    }

    fn advance_to_phase(builder: &mut ReplacementRebuildV2, target: Phase) {
        let mut units = 0usize;
        while builder.phase != target {
            assert_ne!(builder.phase, Phase::Ready, "target phase was skipped");
            if builder.phase == Phase::PlanReady {
                let bytes = builder.plan().unwrap().node_payload_bytes;
                builder.resume_after_plan(bytes).unwrap();
                continue;
            }
            builder.tick().unwrap();
            units += 1;
            assert!(units < 2_000_000, "phase advance did not converge");
        }
    }

    async fn assert_differential(rows: Vec<FileEntry>, budget: usize) {
        let mut oracle = TransactionalTreeV2::new("vault", "device");
        oracle.rebuild(rows.clone()).await.unwrap();
        let actual = drain(ReplacementRebuildV2::new(rows, "vault", "device"), budget);
        assert_eq!(actual.committed_root(), oracle.committed_root());
        assert_eq!(
            actual.committed_root().unwrap().serialize().unwrap(),
            oracle.committed_root().unwrap().serialize().unwrap()
        );
        assert_eq!(chunks(&actual), chunks(&oracle));
    }

    #[tokio::test]
    async fn differential_budgets_unicode_empty_and_large_graphs() {
        for budget in [1, 256] {
            assert_differential(Vec::new(), budget).await;
            assert_differential(
                vec![
                    entry("\u{10000}.md", 1),
                    entry("z/last.md", 2),
                    entry("\u{e000}.md", 3),
                    entry("a/first.md", 4),
                ],
                budget,
            )
            .await;
            let count = if budget == 1 { 1_337 } else { 25_000 };
            let rows = (0..count)
                .rev()
                .map(|index| entry(format!("wide/{index:08}.md"), index as u64))
                .collect();
            assert_differential(rows, budget).await;
        }
    }

    #[test]
    fn internal_parent_moves_boundary_endpoint_allocations_and_retires_empty_donors() {
        let rows = (0..5_000)
            .rev()
            .map(|index| entry(format!("parents/é/{index:08}.md"), index as u64))
            .collect();
        let mut builder = ReplacementRebuildV2::new(rows, "vault", "device");
        advance_to_phase(&mut builder, Phase::EmitParents);
        assert!(builder.spans.len() >= 1);
        let span = builder.spans[0];
        assert!(span.start < span.end);
        let min_ptr = builder.ranges[span.start].min_path.as_ptr();
        let min_capacity = builder.ranges[span.start].min_path.capacity();
        let max_ptr = builder.ranges[span.end - 1].max_path.as_ptr();
        let max_capacity = builder.ranges[span.end - 1].max_path.capacity();

        assert!(builder.tick().unwrap());
        let parent = builder.next_ranges.last().unwrap();
        assert_eq!(parent.min_path.as_ptr(), min_ptr);
        assert_eq!(parent.min_path.capacity(), min_capacity);
        assert_eq!(parent.max_path.as_ptr(), max_ptr);
        assert_eq!(parent.max_path.capacity(), max_capacity);
        assert!(builder.ranges[span.start].min_path.is_empty());
        assert!(builder.ranges[span.end - 1].max_path.is_empty());
        assert_active_range_memory_exact(&builder);

        // Cancellation after a partial parent level must account and destroy
        // both the moved endpoints and the real empty donor String values.
        assert!(retire(&mut builder) > 0);
        assert_eq!(builder.range_memory(), ReplacementV2RangesMemory::default());
    }

    #[test]
    fn range_memory_is_exact_and_closure_edges_move_without_string_clones() {
        let rows = (0..5_000)
            .rev()
            .map(|index| entry(format!("ranges/é/{index:08}.md"), index as u64))
            .collect();
        let mut builder = ReplacementRebuildV2::new(rows, "vault", "device");
        let mut saw_ranges = false;
        let mut saw_next_ranges = false;
        while builder.phase != Phase::Closure {
            assert_active_range_memory_exact(&builder);
            let memory = builder.range_memory();
            saw_ranges |= memory.ranges.length_slots > 0;
            saw_next_ranges |= memory.next_ranges.length_slots > 0;
            if builder.phase == Phase::PlanReady {
                let bytes = builder.plan().unwrap().node_payload_bytes;
                builder.resume_after_plan(bytes).unwrap();
            } else {
                builder.tick().unwrap();
            }
        }
        assert_active_range_memory_exact(&builder);
        assert!(saw_ranges && saw_next_ranges);
        assert_eq!(builder.range_memory().ranges, Default::default());
        assert_eq!(builder.range_memory().closure_pending.length_slots, 1);
        assert_eq!(
            builder.metadata_memory().unwrap().committed.strings.strings,
            4
        );

        // Validate the root node. Its decoded children now belong to the
        // expansion iterator, while the root descriptor moved into the map.
        assert!(builder.tick().unwrap());
        assert_active_range_memory_exact(&builder);
        let closure = builder.closure.as_ref().unwrap();
        let expanding = closure
            .expanding
            .as_ref()
            .expect("multi-leaf root must expose an internal expansion");
        assert!(expanding.len() > 1);
        let first = &expanding.as_slice()[0];
        let min_pointer = first.min_path.as_ptr();
        let max_pointer = first.max_path.as_ptr();
        let min_capacity = first.min_path.capacity();
        let max_capacity = first.max_path.capacity();
        let paths_before = builder.range_memory().vector_paths;

        // One edge moves out of IntoIter and into pending. The String
        // allocations and capacities must be the exact same owners.
        assert!(builder.tick().unwrap());
        assert_active_range_memory_exact(&builder);
        let moved = builder.closure.as_ref().unwrap().pending.last().unwrap();
        assert_eq!(moved.min_path.as_ptr(), min_pointer);
        assert_eq!(moved.max_path.as_ptr(), max_pointer);
        assert_eq!(moved.min_path.capacity(), min_capacity);
        assert_eq!(moved.max_path.capacity(), max_capacity);
        assert_eq!(builder.range_memory().vector_paths, paths_before);

        while !builder.is_ready() {
            assert_active_range_memory_exact(&builder);
            builder.tick().unwrap();
        }
        assert_active_range_memory_exact(&builder);
        assert!(builder.range_memory().owners_empty());
        assert_eq!(builder.range_memory(), ReplacementV2RangesMemory::default());
    }

    #[test]
    fn partial_range_owners_transfer_exactly_and_retire_cooperatively() {
        let rows = (0..5_000)
            .rev()
            .map(|index| entry(format!("cancel/{index:08}.md"), index as u64))
            .collect();
        let mut builder = ReplacementRebuildV2::new(rows, "vault", "device");
        advance_to_phase(&mut builder, Phase::Closure);
        assert!(builder.tick().unwrap());
        assert!(builder.tick().unwrap());
        assert_active_range_memory_exact(&builder);
        let before = builder.range_memory();
        let metadata_before = builder.metadata_memory().unwrap();

        assert!(builder.retire_one());
        assert_eq!(builder.range_memory(), before);
        assert_eq!(builder.metadata_memory().unwrap(), metadata_before);
        assert!(builder.retirement.is_some());

        let mut previous_strings = before
            .vector_paths
            .strings
            .checked_add(before.descriptor_paths.strings)
            .unwrap();
        let mut units = 1usize;
        while builder.retire_one() {
            units += 1;
            let memory = builder.range_memory();
            let strings = memory
                .vector_paths
                .strings
                .checked_add(memory.descriptor_paths.strings)
                .unwrap();
            assert!(strings <= previous_strings);
            assert!(previous_strings - strings <= 2);
            assert!(memory.counters_valid);
            previous_strings = strings;
            assert!(units < 2_000_000);
        }
        assert_eq!(builder.range_memory(), ReplacementV2RangesMemory::default());
        assert!(!builder.is_ready());
    }

    #[test]
    fn root_retirement_metadata_invalidity_stays_sticky_through_cleanup() {
        let child = RangeRef {
            min_path: "é/first.md".to_owned(),
            max_path: "𐀀/last.md".to_owned(),
            hash: hash_bytes(b"root-retirement-child"),
            file_count: 2,
            serialized_bytes: 64,
            height: 0,
        };
        let root = RootNodeV2::new(
            "vault".to_owned(),
            "device".to_owned(),
            TreeV2Root {
                version: TREE_VERSION,
                total_files: 2,
                child: Some(child),
            },
        )
        .unwrap();
        let mut retirement = RootBuildRetirement::new(root);
        retirement.metadata.counters_valid = false;
        retirement.metadata.strings.counters_valid = false;
        let mut units = 0;
        while retirement.retire_one() {
            units += 1;
            assert!(!retirement.metadata_memory().counters_valid);
            assert!(!retirement.metadata_memory().strings.counters_valid);
        }
        assert_eq!(units, 3);
        assert_eq!(retirement.metadata_memory().roots, 0);
        assert_eq!(retirement.metadata_memory().strings.strings, 0);
        assert!(!retirement.metadata_memory().counters_valid);
        assert!(!retirement.metadata_memory().strings.counters_valid);
    }

    #[test]
    fn empty_device_string_transfers_and_retires_as_its_own_units() {
        let mut builder = ReplacementRebuildV2::new(Vec::new(), "vault", "");
        advance_to_phase(&mut builder, Phase::Ready);
        let before = builder.metadata_memory().unwrap();
        assert_eq!(before.committed.strings.strings, 2);
        assert_eq!(before.tree_ids.strings, 2);
        assert_eq!(before.committed.strings.length_bytes, 5);
        assert_eq!(before.tree_ids.length_bytes, 5);

        let mut previous = before;
        let mut saw_empty_builder_device = false;
        let mut units = 0usize;
        while builder.retire_one() {
            units += 1;
            let current = builder.metadata_memory().unwrap();
            let previous_strings = previous.committed.strings.strings + previous.tree_ids.strings;
            let current_strings = current.committed.strings.strings + current.tree_ids.strings;
            assert!(current_strings <= previous_strings);
            assert!(previous_strings - current_strings <= 1);
            saw_empty_builder_device |= previous.tree_ids.strings == 1
                && previous.tree_ids.length_bytes == 0
                && previous.tree_ids.capacity_bytes == 0
                && current.tree_ids.strings == 0;
            previous = current;
            assert!(units < 128);
        }
        assert!(saw_empty_builder_device);
        assert_eq!(previous, TreeMetadataMemory::default());
    }

    #[tokio::test]
    async fn path_only_boundaries_ignore_metadata() {
        let paths: Vec<_> = (0..4_096)
            .map(|index| format!("stable/{index:08}.md"))
            .collect();
        let first = drain(
            ReplacementRebuildV2::new(
                paths
                    .iter()
                    .enumerate()
                    .map(|(index, path)| entry(path, index as u64))
                    .collect(),
                "vault",
                "device",
            ),
            37,
        );
        let second = drain(
            ReplacementRebuildV2::new(
                paths
                    .iter()
                    .enumerate()
                    .map(|(index, path)| entry(path, 100_000 + index as u64))
                    .collect(),
                "vault",
                "device",
            ),
            19,
        );
        let first_ranges = crate::tree_v2::leaf_ranges(
            first.resident_store_for_test(),
            &first.committed_root().unwrap().tree,
        )
        .await
        .unwrap();
        let second_ranges = crate::tree_v2::leaf_ranges(
            second.resident_store_for_test(),
            &second.committed_root().unwrap().tree,
        )
        .await
        .unwrap();
        assert_eq!(
            first_ranges
                .iter()
                .map(|range| (&range.min_path, &range.max_path, range.file_count))
                .collect::<Vec<_>>(),
            second_ranges
                .iter()
                .map(|range| (&range.min_path, &range.max_path, range.file_count))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn exact_plan_stops_before_encoding_and_matches_emitted_graph() {
        for budget in [1, 256] {
            for boundary in ["natural-anchor", "byte-cap"] {
                let rows: Vec<_> = if boundary == "natural-anchor" {
                    let filler = "n".repeat(240);
                    let anchor = (0..100_000)
                        .map(|salt| format!("planned/{:08}-{filler}-{salt:05}.md", 127))
                        .find(|path| gear_hash(path.as_bytes()) & LEAF_ANCHOR_MASK == 0)
                        .expect("bounded fixture must find a natural leaf anchor");
                    (0..512)
                        .rev()
                        .map(|index| {
                            let path = if index == 127 {
                                anchor.clone()
                            } else {
                                format!("planned/{index:08}-{filler}-fixed.md")
                            };
                            entry(path, index)
                        })
                        .collect()
                } else {
                    let filler = "c".repeat(3_970);
                    (0..2_304)
                        .rev()
                        .map(|index| entry(format!("planned/{index:08}-{filler}.md"), index))
                        .collect()
                };
                let mut builder = ReplacementRebuildV2::new(rows, "vault", "device");
                while builder.phase != Phase::PlanReady {
                    let progress = builder.step(budget).unwrap();
                    assert!(progress.units > 0);
                    assert!(!progress.done);
                }
                if boundary == "natural-anchor" {
                    assert_eq!(builder.leaf_spans[0].end, MIN_LEAF_ENTRIES);
                } else {
                    assert!(builder.leaf_spans[0].end < MIN_LEAF_ENTRIES);
                    let planned = builder.planned_internal_spans[0];
                    assert!(planned.span.end - planned.span.start < MAX_INTERNAL_CHILDREN);
                    let next = builder.leaf_spans[planned.span.end];
                    let next_record = 48
                        + builder.entries[next.start].path.len()
                        + builder.entries[next.end - 1].path.len();
                    assert!(planned.span.bytes + next_record > MAX_NODE_BYTES);
                }
                let plan = builder.plan().unwrap();
                let output_plan = builder.output_memory_plan_v1().unwrap();
                assert!(plan.node_count > 1);
                assert_eq!(plan.node_count, plan.leaf_count + plan.internal_count);
                assert!(plan.node_payload_bytes >= plan.max_node_bytes as u64);
                assert!(plan.stored_root_bytes >= 64);
                assert_eq!(output_plan.node_payload_bytes, plan.node_payload_bytes);
                assert_eq!(
                    output_plan.peak_admission_bytes,
                    output_plan.node_payload_bytes
                        + output_plan.range_endpoint_peak_requested_bytes
                );
                assert_eq!(
                    output_plan.resident_admission_bytes,
                    output_plan.node_payload_bytes
                        + output_plan.range_endpoint_resident_requested_bytes
                );
                assert_eq!(builder.store.len(), 0);
                assert_eq!(builder.chunk_memory(), ChunkStoreMemory::default());

                let paused = builder.step(budget).unwrap();
                assert_eq!(paused.phase, "plan ready");
                assert_eq!(paused.units, 0);
                assert!(builder
                    .resume_after_plan(plan.node_payload_bytes + 1)
                    .is_err());
                assert_eq!(builder.plan().unwrap(), plan);
                assert_eq!(builder.store.len(), 0);

                for witness in [
                    (
                        output_plan.node_payload_bytes + 1,
                        output_plan.range_endpoint_peak_requested_bytes,
                        output_plan.range_endpoint_resident_requested_bytes,
                    ),
                    (
                        output_plan.node_payload_bytes,
                        output_plan.range_endpoint_peak_requested_bytes + 1,
                        output_plan.range_endpoint_resident_requested_bytes,
                    ),
                    (
                        output_plan.node_payload_bytes,
                        output_plan.range_endpoint_peak_requested_bytes,
                        output_plan.range_endpoint_resident_requested_bytes + 1,
                    ),
                ] {
                    assert!(builder
                        .resume_after_output_memory_plan_v1(witness.0, witness.1, witness.2)
                        .is_err());
                    assert_eq!(builder.output_memory_plan_v1().unwrap(), output_plan);
                    assert_eq!(builder.store.len(), 0);
                }

                builder
                    .resume_after_output_memory_plan_v1(
                        output_plan.node_payload_bytes,
                        output_plan.range_endpoint_peak_requested_bytes,
                        output_plan.range_endpoint_resident_requested_bytes,
                    )
                    .unwrap();
                while !builder.is_ready() {
                    builder.step(budget).unwrap();
                }
                let tree = builder.finish().unwrap();
                let (descriptor_endpoint_bytes, root_endpoint_bytes) =
                    output_endpoint_oracle(&tree);
                assert_eq!(
                    output_plan.range_endpoint_peak_requested_bytes,
                    descriptor_endpoint_bytes + root_endpoint_bytes
                );
                assert_eq!(
                    output_plan.range_endpoint_resident_requested_bytes,
                    root_endpoint_bytes
                );
                let chunks = tree.resident_store_for_test().all_chunks();
                assert_eq!(chunks.len() as u64, plan.node_count);
                assert_eq!(
                    chunks
                        .iter()
                        .map(|(_, bytes)| bytes.len() as u64)
                        .sum::<u64>(),
                    plan.node_payload_bytes
                );
                assert_eq!(
                    chunks.iter().map(|(_, bytes)| bytes.len()).max().unwrap() as u32,
                    plan.max_node_bytes
                );
                assert_eq!(
                    chunks
                        .iter()
                        .filter(|(_, bytes)| bytes.starts_with(b"OVL2"))
                        .count() as u64,
                    plan.leaf_count
                );
                assert_eq!(
                    chunks
                        .iter()
                        .filter(|(_, bytes)| bytes.starts_with(b"OVI2"))
                        .count() as u64,
                    plan.internal_count
                );
                assert_eq!(
                    tree.committed_root().unwrap().serialize().unwrap().len() as u32,
                    plan.stored_root_bytes
                );
            }
        }
    }

    #[test]
    fn output_memory_plan_handles_empty_tree_and_checked_overflow() {
        let mut builder = ReplacementRebuildV2::new(Vec::new(), "vault", "device");
        while builder.phase != Phase::PlanReady {
            builder.step(1).unwrap();
        }
        let output_plan = builder.output_memory_plan_v1().unwrap();
        assert_eq!(output_plan, ReplacementOutputMemoryPlanV1::default());
        builder.resume_after_output_memory_plan_v1(0, 0, 0).unwrap();
        while !builder.is_ready() {
            builder.step(1).unwrap();
        }
        let tree = builder.finish().unwrap();
        assert_eq!(output_endpoint_oracle(&tree), (0, 0));

        assert!(ReplacementRebuildV2::build_output_memory_plan_v1(0, u64::MAX, 1).is_err());
        assert!(ReplacementRebuildV2::build_output_memory_plan_v1(u64::MAX, 1, 0).is_err());
        assert!(ReplacementRebuildV2::build_output_memory_plan_v1(u64::MAX, 0, 1).is_err());
    }

    #[test]
    fn internal_emission_checks_each_planned_span_before_codec_allocation() {
        let rows = (0..5_000)
            .rev()
            .map(|index| entry(format!("planned/{index:08}.md"), index))
            .collect();
        let mut builder = ReplacementRebuildV2::new(rows, "vault", "device");
        while builder.phase != Phase::PlanReady {
            builder.step(256).unwrap();
        }
        let plan = builder.plan().unwrap();
        assert!(!builder.planned_internal_spans.is_empty());
        builder.resume_after_plan(plan.node_payload_bytes).unwrap();
        advance_to_phase(&mut builder, Phase::EmitParents);
        let span = builder.spans[0];
        let min_ptr = builder.ranges[span.start].min_path.as_ptr();
        let max_ptr = builder.ranges[span.end - 1].max_path.as_ptr();
        builder.planned_internal_spans[0].span.end -= 1;
        let error = builder.step(1).unwrap_err();
        assert!(error
            .to_string()
            .contains("internal segmentation differs from its plan"));
        assert_eq!(builder.store.len(), plan.leaf_count as usize);
        assert_eq!(builder.ranges[span.start].min_path.as_ptr(), min_ptr);
        assert_eq!(builder.ranges[span.end - 1].max_path.as_ptr(), max_ptr);
        assert!(builder.next_ranges.is_empty());
        assert_active_range_memory_exact(&builder);
    }

    #[test]
    fn metadata_changes_do_not_change_v2_payload_plan() {
        let paths: Vec<_> = (0..4_096)
            .map(|index| format!("stable/{index:08}.md"))
            .collect();
        let planned = |revision_base: u64| {
            let mut builder = ReplacementRebuildV2::new(
                paths
                    .iter()
                    .enumerate()
                    .rev()
                    .map(|(index, path)| entry(path, revision_base + index as u64))
                    .collect(),
                "vault",
                "device",
            );
            while builder.phase != Phase::PlanReady {
                builder.step(256).unwrap();
            }
            builder.plan().unwrap()
        };
        assert_eq!(planned(0), planned(100_000));
    }

    #[tokio::test]
    async fn duplicate_failure_is_terminal_and_private() {
        let mut live = TransactionalTreeV2::new("live", "device");
        live.rebuild(vec![entry("old.md", 1)]).await.unwrap();
        let live_root = live.committed_root().unwrap().serialize().unwrap();
        let live_chunks = chunks(&live);

        let mut builder = ReplacementRebuildV2::new(
            vec![entry("same.md", 1), entry("same.md", 2)],
            "vault",
            "device",
        );
        let error = loop {
            match builder.step(1) {
                Ok(progress) => assert!(!progress.done),
                Err(error) => break error,
            }
        };
        assert!(error.to_string().contains("strictly ordered"));
        assert!(!builder.is_ready());
        assert!(builder.step(1).is_err());
        assert_eq!(
            live.committed_root().unwrap().serialize().unwrap(),
            live_root
        );
        assert_eq!(chunks(&live), live_chunks);

        let partial = ReplacementRebuildV2::new(
            (0..2_000)
                .map(|index| entry(format!("drop/{index:08}.md"), index))
                .collect(),
            "vault",
            "device",
        );
        drop(partial);
        assert_eq!(
            live.committed_root().unwrap().serialize().unwrap(),
            live_root
        );
        assert_eq!(chunks(&live), live_chunks);
    }

    #[test]
    fn retirement_covers_every_private_builder_phase() {
        let targets = [
            Phase::Sort,
            Phase::ValidateEntries,
            Phase::SegmentLeaves,
            Phase::PlanLeafDescriptors,
            Phase::PlanValidateRanges,
            Phase::PlanSegmentRanges,
            Phase::PlanEmitParents,
            Phase::PlanRetireRanges,
            Phase::PlanRoot,
            Phase::PlanReady,
            Phase::EmitLeaves,
            Phase::ValidateRanges,
            Phase::SegmentRanges,
            Phase::EmitParents,
            Phase::RetireRanges,
            Phase::Root,
            Phase::Closure,
            Phase::CleanupClosure,
            Phase::CleanupEntries,
            Phase::Ready,
        ];
        for target in targets {
            let rows = (0..2_000)
                .rev()
                .map(|index| entry(format!("phase/{index:08}.md"), index))
                .collect();
            let mut builder = ReplacementRebuildV2::new(rows, "vault", "device");
            advance_to_phase(&mut builder, target);
            assert_active_planning_memory_exact(&builder);
            assert!(retire(&mut builder) > 0, "no retirement work in {target:?}");
            assert_eq!(
                builder.planning_memory(),
                ReplacementV2PlanningMemory::default()
            );
        }
    }

    #[test]
    fn replacement_v2_planning_backing_is_exact_through_every_planning_phase() {
        let rows = (0..25_000)
            .rev()
            .map(|index| entry(format!("planning/{index:08}.md"), index))
            .collect();
        let mut builder = ReplacementRebuildV2::new(rows, "vault", "device");
        let mut saw_populated = [false; 6];
        let mut steps = 0usize;
        while builder.phase != Phase::PlanReady {
            assert_active_planning_memory_exact(&builder);
            for (seen, component) in saw_populated
                .iter_mut()
                .zip(planning_components(builder.planning_memory()))
            {
                *seen |= component.length_slots > 0 && component.capacity_slots > 0;
            }
            let progress = builder.step(1).unwrap();
            assert_eq!(progress.units, 1);
            steps += 1;
            assert!(steps < 2_000_000, "V2 planning did not converge");
        }
        assert_active_planning_memory_exact(&builder);
        assert_eq!(
            saw_populated, [true; 6],
            "fixture did not exercise every V2 planning Vec"
        );

        let ready = builder.planning_memory();
        assert_eq!(ready.planned_ranges.capacity_slots, 0);
        assert_eq!(ready.next_planned_ranges.capacity_slots, 0);
        assert_eq!(ready.planned_spans.capacity_slots, 0);
        assert_eq!(ready.planned_ranges.owners, 1);
        assert_eq!(ready.next_planned_ranges.owners, 1);
        assert_eq!(ready.planned_spans.owners, 1);
        assert!(ready.leaf_spans.capacity_slots > 0);
        assert!(ready.planned_internal_spans.capacity_slots > 0);
    }

    #[test]
    fn replacement_v2_planning_backing_transfers_and_releases_one_unit_at_a_time() {
        let rows = (0..5_000)
            .rev()
            .map(|index| entry(format!("planning-retire/{index:08}.md"), index))
            .collect();
        let mut builder = ReplacementRebuildV2::new(rows, "vault", "device");
        advance_to_phase(&mut builder, Phase::PlanEmitParents);
        let captured = builder.planning_memory();
        assert!(planning_components(captured)
            .iter()
            .any(|component| component.length_slots > 0));

        assert!(builder.retire_one());
        assert_eq!(builder.planning_memory(), captured);
        assert_retiring_planning_memory_exact(&builder);

        let mut previous = captured;
        let mut saw_drained_backing = false;
        let mut saw_explicit_release = false;
        let mut units = 1usize;
        while builder.retire_one() {
            let current = builder.planning_memory();
            assert_retiring_planning_memory_exact(&builder);
            let before = planning_components(previous);
            let after = planning_components(current);
            let before_lengths: usize = before.iter().map(|value| value.length_slots).sum();
            let after_lengths: usize = after.iter().map(|value| value.length_slots).sum();
            let before_owners: usize = before.iter().map(|value| value.owners).sum();
            let after_owners: usize = after.iter().map(|value| value.owners).sum();
            assert!(after_lengths <= before_lengths);
            assert!(after_owners <= before_owners);
            assert!(before_lengths - after_lengths + before_owners - after_owners <= 1);
            for (old, new) in before.into_iter().zip(after) {
                if old.owners == 1 && new.owners == 1 {
                    assert_eq!(new.capacity_slots, old.capacity_slots);
                    assert_eq!(new.backing_capacity_bytes, old.backing_capacity_bytes);
                }
                if old.owners == 1 && new.owners == 0 {
                    assert_eq!(old.length_slots, 0);
                }
                saw_drained_backing |=
                    new.owners == 1 && new.length_slots == 0 && new.capacity_slots > 0;
                saw_explicit_release |= old.owners == 1 && new.owners == 0;
            }
            previous = current;
            units += 1;
            assert!(units < 100_000, "V2 planning retirement did not converge");
        }
        assert!(saw_drained_backing);
        assert!(saw_explicit_release);
        assert_eq!(
            builder.planning_memory(),
            ReplacementV2PlanningMemory::default()
        );
    }

    #[test]
    fn replacement_v2_sort_backing_moves_into_retirement_without_double_counting() {
        let mut rows = Vec::with_capacity(1_017);
        rows.extend(
            (0..1_000)
                .rev()
                .map(|index| entry(format!("sort-meter/{index:08}.md"), index)),
        );
        let capacity = rows.capacity();
        let mut builder = ReplacementRebuildV2::new(rows, "vault", "device");
        let initial = builder.sort_memory();
        assert_eq!(initial.entries.sorters, 1);
        assert_eq!(initial.children.sorters, 0);
        assert_eq!(initial.entries.values.length_slots, 1_000);
        assert_eq!(initial.entries.values.capacity_slots, capacity);
        assert!(initial.entries.source_indices.capacity_slots >= 1_000);
        assert!(initial.entries.target_indices.capacity_slots >= 1_000);

        builder.step(17).unwrap();
        let indexed = builder.sort_memory();
        assert_eq!(indexed.entries.source_indices.length_slots, 17);
        assert_eq!(indexed.entries.target_indices.length_slots, 17);
        assert_eq!(indexed.entries.values, initial.entries.values);

        assert!(builder.retire_one());
        let retiring = builder.sort_memory();
        assert_eq!(retiring.entries.values.length_slots, 999);
        assert_eq!(
            retiring.entries.values.capacity_slots,
            initial.entries.values.capacity_slots
        );
        assert!(retiring.counters_valid);
    }

    #[test]
    fn post_sort_entries_and_paths_are_exact_until_bounded_release() {
        let mut rows = Vec::with_capacity(17);
        let mut spare_path = entry("zeta/🧪.md", 1);
        spare_path.path.reserve(257);
        rows.extend([
            spare_path,
            entry("alpha/é.md", 2),
            entry("middle/長い名前.md", 3),
        ]);
        let expected = ReplacementInputMemory::new(&rows);
        assert!(expected.entries.capacity_slots > expected.entries.length_slots);
        assert!(expected.paths.capacity_bytes > expected.paths.length_bytes);

        let mut builder = ReplacementRebuildV2::new(rows, "vault", "device");
        assert_eq!(builder.post_sort_entry_memory(), None);
        advance_to_phase(&mut builder, Phase::ValidateEntries);
        assert_eq!(builder.post_sort_entry_memory(), Some(expected));

        advance_to_phase(&mut builder, Phase::CleanupEntries);
        while !builder.entries.is_empty() {
            assert_eq!(
                builder.post_sort_entry_memory(),
                Some(ReplacementInputMemory::new(&builder.entries))
            );
            assert!(builder.tick().unwrap());
        }
        let drained = builder.post_sort_entry_memory().unwrap();
        assert_eq!(drained.entries.owners, 1);
        assert_eq!(drained.entries.length_slots, 0);
        assert_eq!(
            drained.entries.capacity_slots,
            expected.entries.capacity_slots
        );
        assert_eq!(drained.paths, StringMemory::default());

        assert!(builder.tick().unwrap());
        assert_eq!(
            builder.post_sort_entry_memory(),
            Some(ReplacementInputMemory::default())
        );
        assert_eq!(builder.phase, Phase::CleanupEntries);
        assert!(!builder.tick().unwrap());
        assert_eq!(builder.phase, Phase::Ready);
    }

    #[test]
    fn post_sort_entry_meter_transfers_to_retirement_and_does_not_go_stale() {
        let mut first = entry("zeta/🧪.md", 1);
        first.path.reserve(113);
        let mut second = entry("alpha/é.md", 2);
        second.path.reserve(29);
        let third = entry("middle/長い名前.md", 3);
        let rows = vec![first, second, third];
        let mut retirement_order: Vec<_> = rows
            .iter()
            .map(|entry| (entry.path.clone(), entry.path.len(), entry.path.capacity()))
            .collect();
        retirement_order.sort_by(|left, right| left.0.cmp(&right.0));
        let mut expected = ReplacementInputMemory::new(&rows);

        let mut sorted = ReplacementRebuildV2::new(rows, "vault", "device");
        advance_to_phase(&mut sorted, Phase::ValidateEntries);
        assert_eq!(sorted.post_sort_entry_memory(), Some(expected));
        for (_, length, capacity) in retirement_order {
            assert!(sorted.retire_one());
            expected.entries.length_slots -= 1;
            expected.paths.strings -= 1;
            expected.paths.length_bytes -= u64::try_from(length).unwrap();
            expected.paths.capacity_bytes -= u64::try_from(capacity).unwrap();
            assert_eq!(sorted.post_sort_entry_memory(), Some(expected));
        }
        assert_eq!(expected.entries.length_slots, 0);
        assert!(expected.entries.capacity_slots > 0);
        assert_eq!(expected.paths, StringMemory::default());
        assert!(sorted.retire_one());
        assert_eq!(
            sorted.post_sort_entry_memory(),
            Some(ReplacementInputMemory::default())
        );

        let rows: Vec<_> = (0..257)
            .rev()
            .map(|index| entry(format!("retire-sort/{index:08}.md"), index))
            .collect();
        let mut sorting = ReplacementRebuildV2::new(rows, "vault", "device");
        sorting.step(7).unwrap();
        assert_eq!(sorting.post_sort_entry_memory(), None);
        let mut saw_sorter_backing_release = false;
        while sorting.post_sort_entry_memory().is_none() {
            let before_sort = sorting.sort_memory();
            let before_planning = sorting.planning_memory();
            assert!(sorting.retire_one());
            let after_sort = sorting.sort_memory();
            if before_sort.entries.sorters == 1 && after_sort.entries.sorters == 0 {
                assert_eq!(sorting.planning_memory(), before_planning);
                saw_sorter_backing_release = true;
            }
            if sorting.post_sort_entry_memory().is_none() {
                assert_eq!(sorting.post_sort_entry_memory(), None);
            }
        }
        assert!(saw_sorter_backing_release);
        assert_eq!(
            sorting.post_sort_entry_memory(),
            Some(ReplacementInputMemory::default())
        );
    }

    #[test]
    fn partial_large_sort_retires_incrementally_and_cannot_resume() {
        let count = 25_000usize;
        let rows = (0..count)
            .rev()
            .map(|index| entry(format!("sort/{index:08}.md"), index as u64))
            .collect();
        let mut builder = ReplacementRebuildV2::new(rows, "vault", "device");
        assert_eq!(builder.step(17).unwrap().phase, "sort");
        let units = retire(&mut builder);
        // Each original row, the two Copy-index backing arrays, the drained
        // values backing, then six planning Vec owners and vault/device
        // strings. No per-integer cleanup delay or T bulk drop.
        assert_eq!(units, count + 11);
    }

    #[tokio::test]
    async fn take_ready_preserves_errors_and_moves_canonical_result_once() {
        let rows: Vec<_> = (0..1_337)
            .rev()
            .map(|index| entry(format!("take/{index:08}.md"), index))
            .collect();
        let mut builder = ReplacementRebuildV2::new(rows.clone(), "vault", "device");
        assert!(builder.take_ready().is_err());
        while !builder.is_ready() {
            builder.step(256).unwrap();
            if builder.phase == Phase::PlanReady {
                let bytes = builder.plan().unwrap().node_payload_bytes;
                builder.resume_after_plan(bytes).unwrap();
            }
        }
        let root = builder.root.take().unwrap();
        let resident = builder.store.len();
        assert!(builder.take_ready().is_err());
        assert_eq!(builder.phase, Phase::Ready);
        assert_eq!(builder.store.len(), resident);
        assert_eq!(builder.vault_id, "vault");
        builder.root = Some(root);
        let actual = builder.take_ready().unwrap();
        assert_eq!(builder.phase, Phase::Retiring);
        assert!(builder.take_ready().is_err());
        // The result moved out, while six now-empty planning Vec owners and
        // the two now-empty identity String owners are still released
        // cooperatively rather than in the publishing turn.
        assert_eq!(retire(&mut builder), 8);

        let mut oracle = TransactionalTreeV2::new("vault", "device");
        oracle.rebuild(rows).await.unwrap();
        assert_eq!(actual.committed_root(), oracle.committed_root());
        assert_eq!(chunks(&actual), chunks(&oracle));
    }

    #[test]
    fn invalid_budget_poison_is_not_ready() {
        let mut builder = ReplacementRebuildV2::new(Vec::new(), "vault", "device");
        assert!(builder.step(0).is_err());
        assert!(!builder.is_ready());
        assert!(builder.step(1).is_err());
        assert!(builder.finish().is_err());
    }

    #[tokio::test]
    async fn private_closure_faults_fail_closed_without_touching_live_tree() {
        let mut live = TransactionalTreeV2::new("live", "device");
        live.rebuild(vec![entry("live.md", 1)]).await.unwrap();
        let live_root = live.committed_root().unwrap().serialize().unwrap();
        let live_chunks = chunks(&live);
        let rows = || {
            (0..2_000)
                .rev()
                .map(|index| entry(format!("fault/{index:08}.md"), index))
                .collect()
        };
        let advance = |builder: &mut ReplacementRebuildV2| {
            while builder.phase != Phase::Closure {
                let progress = builder.step(256).unwrap();
                assert!(!progress.done, "builder skipped its private closure phase");
                if builder.phase == Phase::PlanReady {
                    let bytes = builder.plan().unwrap().node_payload_bytes;
                    builder.resume_after_plan(bytes).unwrap();
                }
            }
            assert!(builder.root.is_some());
            assert!(!builder.store.is_empty());
        };
        let reject = |builder: &mut ReplacementRebuildV2| {
            loop {
                match builder.step(256) {
                    Ok(progress) => assert!(!progress.done),
                    Err(_) => break,
                }
            }
            assert_eq!(builder.phase, Phase::Failed);
            assert!(!builder.is_ready());
            assert!(builder.step(1).is_err());
            assert!(builder.root.is_some());
            assert!(!builder.store.is_empty());
        };

        let mut corrupt = ReplacementRebuildV2::new(rows(), "vault", "device");
        advance(&mut corrupt);
        let leaf = corrupt
            .store
            .all_chunks()
            .into_iter()
            .find(|(_, bytes)| bytes.get(..4) == Some(b"OVL2"))
            .unwrap()
            .0;
        corrupt.store.insert_chunk(leaf, b"corrupt".to_vec());
        reject(&mut corrupt);
        assert!(retire(&mut corrupt) > 0);

        let mut missing = ReplacementRebuildV2::new(rows(), "vault", "device");
        advance(&mut missing);
        let leaf = missing
            .store
            .all_chunks()
            .into_iter()
            .find(|(_, bytes)| bytes.get(..4) == Some(b"OVL2"))
            .unwrap()
            .0;
        use crate::store::ChunkStore;
        missing.store.delete(&leaf).await.unwrap();
        reject(&mut missing);
        assert!(retire(&mut missing) > 0);

        let mut conflicting = ReplacementRebuildV2::new(rows(), "vault", "device");
        advance(&mut conflicting);
        let root = &conflicting.root.as_ref().unwrap().tree;
        let leaf = crate::tree_v2::leaf_ranges(&conflicting.store, root)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let mut different = leaf.clone();
        different.max_path.push('z');
        let closure = conflicting.closure.as_mut().unwrap();
        // LIFO: validate the honest descriptor first, then observe the same
        // content hash under a different edge descriptor.
        closure.pending.push(different);
        closure.pending.push(leaf);
        reject(&mut conflicting);
        assert!(retire(&mut conflicting) > 0);

        assert_eq!(
            live.committed_root().unwrap().serialize().unwrap(),
            live_root
        );
        assert_eq!(chunks(&live), live_chunks);
    }

    #[test]
    fn path_cap_failure_is_terminal() {
        let exact = format!("{}.md", "x".repeat(crate::tree_v2::MAX_PATH_BYTES - 3));
        let tree = drain(
            ReplacementRebuildV2::new(vec![entry(exact, 1)], "vault", "device"),
            1,
        );
        assert_eq!(tree.committed_root().unwrap().total_files(), 1);

        let too_long = format!("{}.md", "x".repeat(crate::tree_v2::MAX_PATH_BYTES - 2));
        let mut builder = ReplacementRebuildV2::new(vec![entry(too_long, 1)], "vault", "device");
        while builder.step(256).is_ok() {}
        assert!(!builder.is_ready());
        assert!(builder.step(1).is_err());
        assert!(builder.finish().is_err());
    }
}
