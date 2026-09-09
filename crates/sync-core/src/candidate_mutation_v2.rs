//! Owner-local, resumable equivalent of one `tree_v2::update_tree` call.
//!
//! The caller must keep the same resident store and candidate exclusively
//! owned until finish or drop. Steps only read that store: encoded objects
//! remain in a private overlay. Finish checks the exact captured semantic
//! root and promotes the complete verified overlay before returning its new
//! root. It does not change transaction history, commit/ACK state, or the
//! transaction's all-resident chunk baseline.
//!
//! A unit is one operation/entry/range, one bounded node decode/encode/hash,
//! or one phase transition. Native allocation, dropping a cancelled cursor,
//! and the final atomic overlay verification/promotion are synchronous
//! residuals, not a wall-clock slice or a native-heap/RSS budget guarantee.

use crate::chunk::{ChunkError, FileEntry};
use crate::hash::{hash_bytes, hash_to_hex, FileHash};
use crate::store::MemoryChunkStore;
use crate::tree_v2::{
    self, copy_path_with_exact_request, copy_range_with_exact_request, RangeRef, TreeV2Root,
    UpdateStats, V2Node, INTERNAL_ANCHOR_MASK, INTERNAL_HEADER_BYTES, LEAF_ANCHOR_MASK,
    LEAF_HEADER_BYTES, MAX_INTERNAL_CHILDREN, MAX_LEAF_ENTRIES, MAX_LOADED_ENTRIES, MAX_NODE_BYTES,
    MAX_TREE_DEPTH, MAX_VISITED_NODES, MIN_INTERNAL_CHILDREN, MIN_LEAF_BYTES, MIN_LEAF_ENTRIES,
    TREE_VERSION,
};
use std::collections::{hash_map, BTreeMap, HashMap};

const MAX_STEP_UNITS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Upserts,
    Deletions,
    Collect,
    Select,
    Merge,
    Resynchronize,
    AppendLeaf,
    AppendEntries,
    PlanAssemble,
    PlanValidateRanges,
    PlanSegmentRanges,
    PlanEmitParents,
    PlanRetireRanges,
    PlanRoot,
    PlanReady,
    EmitLeaves,
    Assemble,
    BeginLevel,
    SegmentLevel,
    EmitInternal,
    RetireLevel,
    Cleanup,
    Ready,
    Failed,
    Retiring,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::Upserts => "upserts",
            Self::Deletions => "deletions",
            Self::Collect => "collect",
            Self::Select => "select",
            Self::Merge => "merge",
            Self::Resynchronize => "resynchronize",
            Self::AppendLeaf => "append-leaf",
            Self::AppendEntries => "append-entries",
            Self::PlanAssemble => "plan-assemble",
            Self::PlanValidateRanges => "plan-validate-ranges",
            Self::PlanSegmentRanges => "plan-segment-ranges",
            Self::PlanEmitParents => "plan-emit-parents",
            Self::PlanRetireRanges => "plan-retire-ranges",
            Self::PlanRoot => "plan-root",
            Self::PlanReady => "plan ready",
            Self::EmitLeaves => "emit-leaves",
            Self::Assemble => "assemble",
            Self::BeginLevel => "begin-level",
            Self::SegmentLevel => "segment-level",
            Self::EmitInternal => "emit-internal",
            Self::RetireLevel => "retire-level",
            Self::Cleanup => "cleanup",
            Self::Ready => "ready",
            Self::Failed => "failed",
            Self::Retiring => "retiring",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetirementPhase {
    Changed,
    Deleted,
    Operations,
    Pending,
    Leaves,
    Loaded,
    Replacement,
    ReplacementRefs,
    Segments,
    PlannedRanges,
    NextPlannedRanges,
    PlannedSpans,
    PlannedInternalSpans,
    Ranges,
    Parents,
    Staged,
    Captured,
    Next,
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V2MutationProgress {
    pub done: bool,
    pub units: usize,
    pub completed: usize,
    pub phase: &'static str,
    pub staged_chunks: usize,
    pub staged_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
struct Span {
    start: usize,
    end: usize,
    bytes: usize,
}

#[derive(Debug, Clone, Copy)]
enum PlannedPath {
    Replacement(usize),
    LeafMin(usize),
    LeafMax(usize),
}

#[derive(Debug, Clone, Copy)]
struct PlannedRange {
    min_path: PlannedPath,
    max_path: PlannedPath,
    file_count: u64,
    serialized_bytes: u32,
    height: u16,
}

#[derive(Debug, Clone, Copy)]
struct PlannedInternalSpan {
    span: Span,
    child_height: u16,
}

/// Versioned conservative admission plan for the output allocations requested
/// after one V2 candidate mutation crosses its pre-codec barrier. Node bytes
/// count every canonical codec request, including content already resident;
/// endpoint bytes count exact String allocation requests. Planning scratch,
/// decoded/input owners, map buckets and total native/WASM heap are excluded.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CandidateMutationOutputMemoryPlanV1 {
    pub node_payload_bytes: u64,
    pub range_endpoint_peak_requested_bytes: u64,
    pub range_endpoint_resident_requested_bytes: u64,
    pub peak_admission_bytes: u64,
    pub resident_admission_bytes: u64,
}

/// Exact immutable output retained by a ready mutation before publication.
/// Staged node bytes include only the private hash-deduplicated overlay which
/// finish will promote; byte-identical resident objects are omitted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CandidateMutationOutputMemoryReadyV1 {
    pub staged_node_payload_bytes: u64,
    pub range_endpoint_resident_requested_bytes: u64,
    pub resident_admission_bytes: u64,
}

/// An unfinished canonical tail is retained, not finalized and repeatedly
/// rescanned whenever another old leaf is needed for resynchronization.
struct Segmenter {
    start: usize,
    bytes: usize,
    header: usize,
    minimum: usize,
    maximum: usize,
    minimum_bytes: usize,
    anchor_mask: u64,
}

impl Segmenter {
    fn leaves() -> Self {
        Self {
            start: 0,
            bytes: LEAF_HEADER_BYTES,
            header: LEAF_HEADER_BYTES,
            minimum: MIN_LEAF_ENTRIES,
            maximum: MAX_LEAF_ENTRIES,
            minimum_bytes: MIN_LEAF_BYTES,
            anchor_mask: LEAF_ANCHOR_MASK,
        }
    }

    fn internal() -> Self {
        Self {
            start: 0,
            bytes: INTERNAL_HEADER_BYTES,
            header: INTERNAL_HEADER_BYTES,
            minimum: MIN_INTERNAL_CHILDREN,
            maximum: MAX_INTERNAL_CHILDREN,
            minimum_bytes: 0,
            anchor_mask: INTERNAL_ANCHOR_MASK,
        }
    }

    fn record(
        &mut self,
        index: usize,
        length: usize,
        key: &[u8],
        output: &mut Vec<Span>,
    ) -> Result<(), ChunkError> {
        self.record_with_anchor(index, length, tree_v2::gear_hash(key), output)
    }

    fn record_with_anchor(
        &mut self,
        index: usize,
        length: usize,
        anchor_hash: u64,
        output: &mut Vec<Span>,
    ) -> Result<(), ChunkError> {
        if self.header.saturating_add(length) > MAX_NODE_BYTES {
            return Err(invalid("one Tree v2 record exceeds the node cap"));
        }
        if index > self.start
            && (index - self.start >= self.maximum
                || self.bytes.saturating_add(length) > MAX_NODE_BYTES)
        {
            output.push(Span {
                start: self.start,
                end: index,
                bytes: self.bytes,
            });
            self.start = index;
            self.bytes = self.header;
        }
        self.bytes = self
            .bytes
            .checked_add(length)
            .ok_or_else(|| invalid("Tree v2 segment length overflow"))?;
        let records = index - self.start + 1;
        let natural = records >= self.minimum
            && self.bytes >= self.minimum_bytes
            && anchor_hash & self.anchor_mask == 0;
        if natural || records >= self.maximum || self.bytes == MAX_NODE_BYTES {
            output.push(Span {
                start: self.start,
                end: index + 1,
                bytes: self.bytes,
            });
            self.start = index + 1;
            self.bytes = self.header;
        }
        Ok(())
    }

    fn finish_tail(&self, length: usize, output: &mut Vec<Span>) {
        if self.start < length {
            output.push(Span {
                start: self.start,
                end: length,
                bytes: self.bytes,
            });
        }
    }
}

pub(crate) struct V2CandidateMutation {
    captured: TreeV2Root,
    next: Option<TreeV2Root>,
    phase: Phase,
    completed: usize,
    changed: std::vec::IntoIter<FileEntry>,
    deleted: std::vec::IntoIter<String>,
    operations: BTreeMap<String, Option<FileEntry>>,
    pending: Vec<RangeRef>,
    leaves: Vec<RangeRef>,
    leaf_total: u64,
    visited_nodes: usize,
    loaded_entries: usize,
    empty_base: bool,
    start: usize,
    end: usize,
    read_leaf: usize,
    loaded: std::vec::IntoIter<FileEntry>,
    replacement: Vec<FileEntry>,
    replacement_refs: Vec<RangeRef>,
    segmenter: Segmenter,
    segments: Vec<Span>,
    planned_ranges: Vec<PlannedRange>,
    next_planned_ranges: Vec<PlannedRange>,
    planned_spans: Vec<Span>,
    planned_internal_spans: Vec<PlannedInternalSpan>,
    planned_height: u16,
    planned_node_payload_bytes: u64,
    planned_endpoint_requested_bytes: u64,
    output_memory_plan: CandidateMutationOutputMemoryPlanV1,
    emitted_node_payload_bytes: u64,
    emitted_internal_spans: usize,
    index: usize,
    ranges: Vec<RangeRef>,
    parents: Vec<RangeRef>,
    total_files: u64,
    staged: HashMap<FileHash, Vec<u8>>,
    retiring_staged: Option<hash_map::IntoIter<FileHash, Vec<u8>>>,
    retirement_phase: RetirementPhase,
    staged_bytes: usize,
    stats: UpdateStats,
}

impl V2CandidateMutation {
    pub(crate) fn new(
        root: TreeV2Root,
        changed: Vec<FileEntry>,
        deleted: Vec<String>,
    ) -> Result<Self, ChunkError> {
        tree_v2::validate_root_shape(&root)?;
        let no_operations = changed.is_empty() && deleted.is_empty();
        Ok(Self {
            next: no_operations.then(|| root.clone()),
            pending: root.child.iter().cloned().collect(),
            captured: root,
            phase: if no_operations {
                Phase::Ready
            } else {
                Phase::Upserts
            },
            completed: 0,
            changed: changed.into_iter(),
            deleted: deleted.into_iter(),
            operations: BTreeMap::new(),
            leaves: Vec::new(),
            leaf_total: 0,
            visited_nodes: 0,
            loaded_entries: 0,
            empty_base: false,
            start: 0,
            end: 0,
            read_leaf: 0,
            loaded: Vec::new().into_iter(),
            replacement: Vec::new(),
            replacement_refs: Vec::new(),
            segmenter: Segmenter::leaves(),
            segments: Vec::new(),
            planned_ranges: Vec::new(),
            next_planned_ranges: Vec::new(),
            planned_spans: Vec::new(),
            planned_internal_spans: Vec::new(),
            planned_height: 0,
            planned_node_payload_bytes: 0,
            planned_endpoint_requested_bytes: 0,
            output_memory_plan: CandidateMutationOutputMemoryPlanV1::default(),
            emitted_node_payload_bytes: 0,
            emitted_internal_spans: 0,
            index: 0,
            ranges: Vec::new(),
            parents: Vec::new(),
            total_files: 0,
            staged: HashMap::new(),
            retiring_staged: None,
            retirement_phase: RetirementPhase::Changed,
            staged_bytes: 0,
            stats: UpdateStats::default(),
        })
    }

    pub(crate) fn progress(&self, units: usize) -> V2MutationProgress {
        V2MutationProgress {
            done: self.phase == Phase::Ready,
            units,
            completed: self.completed,
            phase: self.phase.name(),
            staged_chunks: self.staged.len(),
            staged_bytes: self.staged_bytes,
        }
    }

    pub(crate) fn step(
        &mut self,
        store: &MemoryChunkStore,
        max_units: usize,
    ) -> Result<V2MutationProgress, ChunkError> {
        if max_units == 0 || max_units > MAX_STEP_UNITS {
            return Err(invalid("invalid Tree v2 mutation step budget"));
        }
        if self.phase == Phase::Failed {
            return Err(invalid("Tree v2 mutation has failed"));
        }
        if self.phase == Phase::Retiring {
            return Err(invalid("Tree v2 mutation retirement has started"));
        }
        let mut units = 0;
        if self.phase == Phase::PlanReady {
            return Ok(self.progress(0));
        }
        while units < max_units && self.phase != Phase::Ready && self.phase != Phase::PlanReady {
            if let Err(error) = self.step_one(store) {
                self.phase = Phase::Failed;
                return Err(error);
            }
            units += 1;
            self.completed = match self.completed.checked_add(1) {
                Some(completed) => completed,
                None => {
                    self.phase = Phase::Failed;
                    return Err(invalid("Tree v2 mutation progress overflow"));
                }
            };
        }
        Ok(self.progress(units))
    }

    pub(crate) fn output_memory_plan_v1(
        &self,
    ) -> Result<CandidateMutationOutputMemoryPlanV1, ChunkError> {
        if self.phase != Phase::PlanReady {
            return Err(invalid(
                "Tree v2 candidate mutation output memory plan is not ready",
            ));
        }
        Ok(self.output_memory_plan)
    }

    pub(crate) fn resume_after_output_memory_plan_v1(
        &mut self,
        expected_node_payload_bytes: u64,
        expected_range_endpoint_peak_requested_bytes: u64,
        expected_range_endpoint_resident_requested_bytes: u64,
    ) -> Result<(), ChunkError> {
        if self.phase != Phase::PlanReady {
            return Err(invalid(
                "Tree v2 candidate mutation output memory plan is not ready",
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
                "Tree v2 candidate mutation output memory plan witness changed",
            ));
        }
        self.index = 0;
        self.phase = Phase::EmitLeaves;
        Ok(())
    }

    pub(crate) fn output_memory_ready_v1(
        &self,
    ) -> Result<CandidateMutationOutputMemoryReadyV1, ChunkError> {
        if self.phase != Phase::Ready {
            return Err(invalid(
                "Tree v2 candidate mutation output memory is not ready",
            ));
        }
        let staged_node_payload_bytes = u64::try_from(self.staged_bytes)
            .map_err(|_| invalid("Tree v2 candidate mutation ready byte overflow"))?;
        let endpoint_bytes = self
            .output_memory_plan
            .range_endpoint_resident_requested_bytes;
        let resident_admission_bytes = staged_node_payload_bytes
            .checked_add(endpoint_bytes)
            .ok_or_else(|| invalid("Tree v2 candidate mutation ready byte overflow"))?;
        Ok(CandidateMutationOutputMemoryReadyV1 {
            staged_node_payload_bytes,
            range_endpoint_resident_requested_bytes: endpoint_bytes,
            resident_admission_bytes,
        })
    }

    /// No host yield occurs between the exact candidate witness check,
    /// verified overlay promotion and return. Rehashing/collision validation,
    /// the resident HashMap reserve and promotion loop are synchronous final
    /// residuals, not covered by the step or output-byte budget. The owner
    /// must publish the returned root/history without another await or
    /// fallible operation.
    pub(crate) fn finish(
        &mut self,
        store: &MemoryChunkStore,
        current: &TreeV2Root,
    ) -> Result<(TreeV2Root, UpdateStats), ChunkError> {
        if self.phase != Phase::Ready {
            self.phase = Phase::Failed;
            return Err(invalid("Tree v2 mutation is incomplete or failed"));
        }
        // Finish is deliberately one-shot. Every fallible witness/hash/
        // collision/allocation preflight retains the private overlay for
        // cooperative retirement; only the subsequent infallible promotion
        // consumes it. Poisoning first prevents any publication retry.
        self.phase = Phase::Failed;
        if current != &self.captured {
            return Err(invalid("Tree v2 mutation candidate changed"));
        }
        let next = self
            .next
            .as_ref()
            .ok_or_else(|| invalid("Tree v2 mutation result is missing"))?;
        tree_v2::validate_root_shape(next)?;
        store.prepare_candidate_chunk_promotion(&self.staged)?;
        store.promote_prepared_candidate_chunks(std::mem::take(&mut self.staged));
        Ok((self.next.take().unwrap(), self.stats))
    }

    fn step_one(&mut self, store: &MemoryChunkStore) -> Result<(), ChunkError> {
        match self.phase {
            Phase::Upserts => {
                if let Some(entry) = self.changed.next() {
                    tree_v2::validate_path(&entry.path)?;
                    self.operations.insert(entry.path.clone(), Some(entry));
                } else {
                    self.phase = Phase::Deletions;
                }
            }
            Phase::Deletions => {
                if let Some(path) = self.deleted.next() {
                    tree_v2::validate_path(&path)?;
                    // All deletions precede upserts semantically; the last
                    // upsert for a duplicate path has already won above.
                    self.operations.entry(path).or_insert(None);
                } else {
                    self.phase = Phase::Collect;
                }
            }
            Phase::Collect => self.collect_one(store)?,
            Phase::Select => self.select_span()?,
            Phase::Merge => self.merge_one(store)?,
            Phase::Resynchronize => self.resynchronize(),
            Phase::AppendLeaf => {
                self.end += 1;
                self.load_leaf(store, self.end)?;
                self.phase = Phase::AppendEntries;
            }
            Phase::AppendEntries => {
                if let Some(entry) = self.loaded.next() {
                    self.append_entry(entry)?;
                } else {
                    self.phase = Phase::Resynchronize;
                }
            }
            Phase::PlanAssemble => self.plan_assemble_one()?,
            Phase::PlanValidateRanges => self.plan_validate_range()?,
            Phase::PlanSegmentRanges => self.plan_segment_range()?,
            Phase::PlanEmitParents => self.plan_emit_parent()?,
            Phase::PlanRetireRanges => {
                if self.planned_ranges.pop().is_none() {
                    self.planned_ranges = std::mem::take(&mut self.next_planned_ranges);
                    self.start_planned_level_or_root()?;
                }
            }
            Phase::PlanRoot => self.plan_root()?,
            Phase::PlanReady => return Ok(()),
            Phase::EmitLeaves => self.emit_leaf(store)?,
            Phase::Assemble => self.assemble_one()?,
            Phase::BeginLevel => self.begin_level()?,
            Phase::SegmentLevel => self.segment_level()?,
            Phase::EmitInternal => self.emit_internal(store)?,
            Phase::RetireLevel => {
                if self.ranges.pop().is_none() {
                    self.ranges = std::mem::take(&mut self.parents);
                    self.phase = Phase::BeginLevel;
                }
            }
            Phase::Cleanup => {
                // The retained work arrays contain owned path strings. Drop
                // only a bounded number per unit, not a full vault at finish.
                let entry = self.replacement.pop();
                let leaf = self.leaves.pop();
                let replacement = self.replacement_refs.pop();
                if entry.is_none() && leaf.is_none() && replacement.is_none() {
                    self.phase = Phase::Ready;
                }
            }
            Phase::Ready | Phase::Failed | Phase::Retiring => {
                return Err(invalid("invalid Tree v2 mutation phase"))
            }
        }
        Ok(())
    }

    fn visit(&mut self, height: u16) -> Result<(), ChunkError> {
        self.visited_nodes = self.visited_nodes.saturating_add(1);
        if height > MAX_TREE_DEPTH || self.visited_nodes > MAX_VISITED_NODES {
            return Err(invalid("Tree v2 mutation exceeds traversal limits"));
        }
        Ok(())
    }

    fn collect_one(&mut self, store: &MemoryChunkStore) -> Result<(), ChunkError> {
        let Some(range) = self.pending.pop() else {
            if self.leaf_total != self.captured.total_files {
                return Err(invalid("Tree v2 root total does not match its ranges"));
            }
            self.phase = Phase::Select;
            return Ok(());
        };
        self.visit(range.height)?;
        tree_v2::validate_range(&range)?;
        if range.height == 0 {
            validate_next_range(self.leaves.last(), &range, 0)?;
            self.leaf_total = self
                .leaf_total
                .checked_add(range.file_count)
                .ok_or_else(|| invalid("Tree v2 file count overflow"))?;
            self.leaves.push(range);
        } else {
            let bytes = load_bytes(store, &range)?;
            let V2Node::Internal { children, .. } = tree_v2::validate_loaded_node(&range, &bytes)?
            else {
                return Err(invalid("Tree v2 internal descriptor names a leaf"));
            };
            if self.pending.len().saturating_add(children.len()) > MAX_VISITED_NODES {
                return Err(invalid("Tree v2 mutation pending node limit"));
            }
            // A decoded internal node has at most MAX_INTERNAL_CHILDREN.
            self.pending.extend(children.into_iter().rev());
        }
        Ok(())
    }

    fn select_span(&mut self) -> Result<(), ChunkError> {
        let first = self
            .operations
            .first_key_value()
            .ok_or_else(|| invalid("Tree v2 mutation operation set is empty"))?
            .0;
        let last = self.operations.last_key_value().unwrap().0;
        self.empty_base = self.leaves.is_empty();
        if !self.empty_base {
            self.start = self
                .leaves
                .partition_point(|range| &range.max_path < first)
                .min(self.leaves.len() - 1);
            self.end = self
                .leaves
                .partition_point(|range| &range.max_path < last)
                .min(self.leaves.len() - 1)
                .max(self.start);
            self.read_leaf = self.start;
        }
        self.phase = Phase::Merge;
        Ok(())
    }

    fn load_leaf(&mut self, store: &MemoryChunkStore, index: usize) -> Result<(), ChunkError> {
        self.visit(0)?;
        let range = &self.leaves[index];
        let bytes = load_bytes(store, range)?;
        let V2Node::Leaf(entries) = tree_v2::validate_loaded_node(range, &bytes)? else {
            return Err(invalid("Tree v2 leaf descriptor names an internal node"));
        };
        self.loaded_entries = self.loaded_entries.saturating_add(entries.len());
        if self.loaded_entries > MAX_LOADED_ENTRIES {
            return Err(invalid("Tree v2 exceeds the entry limit"));
        }
        self.stats.leaves_loaded += 1;
        self.stats.entries_loaded += entries.len() as u64;
        self.loaded = entries.into_iter();
        Ok(())
    }

    fn merge_one(&mut self, store: &MemoryChunkStore) -> Result<(), ChunkError> {
        if self.loaded.len() == 0 && !self.empty_base && self.read_leaf <= self.end {
            self.load_leaf(store, self.read_leaf)?;
            self.read_leaf += 1;
            return Ok(());
        }
        match (
            self.loaded.as_slice().first(),
            self.operations.first_key_value(),
        ) {
            (Some(entry), Some((path, _))) => match entry.path.cmp(path) {
                std::cmp::Ordering::Less => {
                    let entry = self.loaded.next().unwrap();
                    self.append_entry(entry)?;
                }
                std::cmp::Ordering::Equal => {
                    self.loaded.next();
                    if let Some(entry) = self.operations.pop_first().unwrap().1 {
                        self.append_entry(entry)?;
                    }
                }
                std::cmp::Ordering::Greater => {
                    if let Some(entry) = self.operations.pop_first().unwrap().1 {
                        self.append_entry(entry)?;
                    }
                }
            },
            (Some(_), None) => {
                let entry = self.loaded.next().unwrap();
                self.append_entry(entry)?;
            }
            (None, Some(_)) => {
                if let Some(entry) = self.operations.pop_first().unwrap().1 {
                    self.append_entry(entry)?;
                }
            }
            (None, None) => self.phase = Phase::Resynchronize,
        }
        Ok(())
    }

    fn append_entry(&mut self, entry: FileEntry) -> Result<(), ChunkError> {
        tree_v2::validate_path(&entry.path)?;
        if self.replacement.len() >= MAX_LOADED_ENTRIES
            || self
                .replacement
                .last()
                .is_some_and(|previous| previous.path >= entry.path)
        {
            return Err(invalid(
                "Tree v2 replacement entries exceed limits or are unordered",
            ));
        }
        self.segmenter.record(
            self.replacement.len(),
            tree_v2::leaf_record_len(&entry),
            entry.path.as_bytes(),
            &mut self.segments,
        )?;
        self.replacement.push(entry);
        Ok(())
    }

    fn resynchronize(&mut self) {
        let reached_end = self.empty_base || self.end + 1 == self.leaves.len();
        let stable_old_end = !self.empty_base
            && self.segmenter.start == self.replacement.len()
            && !self.segments.is_empty()
            && self
                .replacement
                .last()
                .is_some_and(|entry| entry.path == self.leaves[self.end].max_path);
        if reached_end || stable_old_end {
            self.stats.reached_tree_end = reached_end;
            if !self.empty_base {
                self.stats.resync_leaves_scanned = (self.end - self.start + 1) as u64;
            }
            self.segmenter
                .finish_tail(self.replacement.len(), &mut self.segments);
            self.stats.replacement_leaves = self.segments.len() as u64;
            self.index = 0;
            self.phase = Phase::PlanAssemble;
        } else {
            self.phase = Phase::AppendLeaf;
        }
    }

    fn planned_path(&self, path: PlannedPath) -> Result<&str, ChunkError> {
        match path {
            PlannedPath::Replacement(index) => {
                self.replacement.get(index).map(|entry| entry.path.as_str())
            }
            PlannedPath::LeafMin(index) => {
                self.leaves.get(index).map(|range| range.min_path.as_str())
            }
            PlannedPath::LeafMax(index) => {
                self.leaves.get(index).map(|range| range.max_path.as_str())
            }
        }
        .ok_or_else(|| invalid("Tree v2 candidate mutation planned endpoint is missing"))
    }

    fn endpoint_pair_bytes(
        &self,
        min_path: PlannedPath,
        max_path: PlannedPath,
    ) -> Result<u64, ChunkError> {
        let min = u64::try_from(self.planned_path(min_path)?.len())
            .map_err(|_| invalid("Tree v2 candidate mutation endpoint byte overflow"))?;
        let max = u64::try_from(self.planned_path(max_path)?.len())
            .map_err(|_| invalid("Tree v2 candidate mutation endpoint byte overflow"))?;
        min.checked_add(max)
            .ok_or_else(|| invalid("Tree v2 candidate mutation endpoint byte overflow"))
    }

    fn add_planned_node_bytes(&mut self, bytes: usize) -> Result<(), ChunkError> {
        let bytes = u64::try_from(bytes)
            .map_err(|_| invalid("Tree v2 candidate mutation planned byte overflow"))?;
        self.planned_node_payload_bytes = self
            .planned_node_payload_bytes
            .checked_add(bytes)
            .ok_or_else(|| invalid("Tree v2 candidate mutation planned byte overflow"))?;
        Ok(())
    }

    fn add_planned_endpoint_bytes(
        &mut self,
        min_path: PlannedPath,
        max_path: PlannedPath,
    ) -> Result<(), ChunkError> {
        let bytes = self.endpoint_pair_bytes(min_path, max_path)?;
        self.planned_endpoint_requested_bytes = self
            .planned_endpoint_requested_bytes
            .checked_add(bytes)
            .ok_or_else(|| invalid("Tree v2 candidate mutation endpoint byte overflow"))?;
        Ok(())
    }

    fn plan_assemble_one(&mut self) -> Result<(), ChunkError> {
        let prefix = if self.empty_base { 0 } else { self.start };
        let replacements_end = prefix
            .checked_add(self.segments.len())
            .ok_or_else(|| invalid("Tree v2 candidate mutation plan length overflow"))?;
        let suffix_start = if self.empty_base { 0 } else { self.end + 1 };
        let total = replacements_end
            .checked_add(self.leaves.len().saturating_sub(suffix_start))
            .ok_or_else(|| invalid("Tree v2 candidate mutation plan length overflow"))?;
        if self.index == total {
            self.index = 0;
            self.start_planned_level_or_root()?;
            return Ok(());
        }
        let planned = if self.index < prefix {
            let range = &self.leaves[self.index];
            PlannedRange {
                min_path: PlannedPath::LeafMin(self.index),
                max_path: PlannedPath::LeafMax(self.index),
                file_count: range.file_count,
                serialized_bytes: range.serialized_bytes,
                height: 0,
            }
        } else if self.index < replacements_end {
            let span = self.segments[self.index - prefix];
            let serialized_bytes = u32::try_from(span.bytes).map_err(|_| {
                invalid("Tree v2 candidate mutation planned leaf length does not fit")
            })?;
            let min_path = PlannedPath::Replacement(span.start);
            let max_path = PlannedPath::Replacement(span.end - 1);
            // Emission first creates a replacement descriptor; assembly then
            // copies that descriptor into the final leaf level.
            self.add_planned_endpoint_bytes(min_path, max_path)?;
            self.add_planned_node_bytes(span.bytes)?;
            PlannedRange {
                min_path,
                max_path,
                file_count: (span.end - span.start) as u64,
                serialized_bytes,
                height: 0,
            }
        } else {
            let leaf = suffix_start + self.index - replacements_end;
            let range = &self.leaves[leaf];
            PlannedRange {
                min_path: PlannedPath::LeafMin(leaf),
                max_path: PlannedPath::LeafMax(leaf),
                file_count: range.file_count,
                serialized_bytes: range.serialized_bytes,
                height: 0,
            }
        };
        self.add_planned_endpoint_bytes(planned.min_path, planned.max_path)?;
        self.planned_ranges.push(planned);
        self.index += 1;
        Ok(())
    }

    fn start_planned_level_or_root(&mut self) -> Result<(), ChunkError> {
        if self.planned_ranges.len() <= 1 {
            self.phase = Phase::PlanRoot;
            return Ok(());
        }
        self.planned_height = self.planned_ranges[0].height;
        if self.planned_height >= MAX_TREE_DEPTH {
            return Err(invalid("Tree v2 exceeds the depth limit"));
        }
        self.segmenter = Segmenter::internal();
        self.planned_spans.clear();
        self.index = 0;
        self.phase = Phase::PlanValidateRanges;
        Ok(())
    }

    fn plan_validate_range(&mut self) -> Result<(), ChunkError> {
        let Some(range) = self.planned_ranges.get(self.index).copied() else {
            self.index = 0;
            self.phase = Phase::PlanSegmentRanges;
            return Ok(());
        };
        let min = self.planned_path(range.min_path)?;
        let max = self.planned_path(range.max_path)?;
        if min.is_empty()
            || max.is_empty()
            || min > max
            || range.height != self.planned_height
            || range.file_count == 0
            || range.serialized_bytes == 0
            || range.serialized_bytes as usize > MAX_NODE_BYTES
        {
            return Err(invalid(
                "Tree v2 candidate mutation planned range is inconsistent",
            ));
        }
        if self.index > 0 {
            let previous = self.planned_ranges[self.index - 1];
            if self.planned_path(previous.max_path)? >= min {
                return Err(invalid("Tree v2 candidate mutation planned ranges overlap"));
            }
        }
        self.index += 1;
        Ok(())
    }

    fn planned_range_record_len(&self, range: PlannedRange) -> Result<usize, ChunkError> {
        let min = self.planned_path(range.min_path)?.len();
        let max = self.planned_path(range.max_path)?.len();
        2usize
            .checked_add(2)
            .and_then(|sum| sum.checked_add(min))
            .and_then(|sum| sum.checked_add(max))
            .and_then(|sum| sum.checked_add(32 + 8 + 4))
            .ok_or_else(|| invalid("Tree v2 candidate mutation planned range length overflow"))
    }

    fn plan_segment_range(&mut self) -> Result<(), ChunkError> {
        let Some(range) = self.planned_ranges.get(self.index).copied() else {
            self.segmenter
                .finish_tail(self.planned_ranges.len(), &mut self.planned_spans);
            self.next_planned_ranges.clear();
            self.index = 0;
            self.phase = Phase::PlanEmitParents;
            return Ok(());
        };
        let length = self.planned_range_record_len(range)?;
        let anchor_hash = tree_v2::gear_hash(self.planned_path(range.max_path)?.as_bytes());
        self.segmenter.record_with_anchor(
            self.index,
            length,
            anchor_hash,
            &mut self.planned_spans,
        )?;
        self.index += 1;
        Ok(())
    }

    fn plan_emit_parent(&mut self) -> Result<(), ChunkError> {
        let Some(span) = self.planned_spans.get(self.index).copied() else {
            self.index = 0;
            self.phase = Phase::PlanRetireRanges;
            return Ok(());
        };
        let children = &self.planned_ranges[span.start..span.end];
        let file_count = children.iter().try_fold(0u64, |sum, child| {
            sum.checked_add(child.file_count)
                .ok_or_else(|| invalid("Tree v2 file count overflow"))
        })?;
        let serialized_bytes = u32::try_from(span.bytes).map_err(|_| {
            invalid("Tree v2 candidate mutation planned internal length does not fit")
        })?;
        let first = children[0];
        let last = children[children.len() - 1];
        self.add_planned_node_bytes(span.bytes)?;
        self.planned_internal_spans.push(PlannedInternalSpan {
            span,
            child_height: self.planned_height,
        });
        self.next_planned_ranges.push(PlannedRange {
            min_path: first.min_path,
            max_path: last.max_path,
            file_count,
            serialized_bytes,
            height: self.planned_height + 1,
        });
        self.index += 1;
        Ok(())
    }

    fn plan_root(&mut self) -> Result<(), ChunkError> {
        let root_endpoint_bytes = match self.planned_ranges.as_slice() {
            [] => 0,
            [root] => self.endpoint_pair_bytes(root.min_path, root.max_path)?,
            _ => {
                return Err(invalid(
                    "Tree v2 candidate mutation planned root is incomplete",
                ))
            }
        };
        self.output_memory_plan = Self::build_output_memory_plan_v1(
            self.planned_node_payload_bytes,
            self.planned_endpoint_requested_bytes,
            root_endpoint_bytes,
        )?;
        self.planned_ranges.clear();
        self.next_planned_ranges.clear();
        self.planned_spans.clear();
        self.phase = Phase::PlanReady;
        Ok(())
    }

    fn build_output_memory_plan_v1(
        node_payload_bytes: u64,
        endpoint_peak_bytes: u64,
        root_endpoint_bytes: u64,
    ) -> Result<CandidateMutationOutputMemoryPlanV1, ChunkError> {
        let peak_admission_bytes = node_payload_bytes
            .checked_add(endpoint_peak_bytes)
            .ok_or_else(|| invalid("Tree v2 candidate mutation output plan byte overflow"))?;
        let resident_admission_bytes = node_payload_bytes
            .checked_add(root_endpoint_bytes)
            .ok_or_else(|| invalid("Tree v2 candidate mutation output plan byte overflow"))?;
        Ok(CandidateMutationOutputMemoryPlanV1 {
            node_payload_bytes,
            range_endpoint_peak_requested_bytes: endpoint_peak_bytes,
            range_endpoint_resident_requested_bytes: root_endpoint_bytes,
            peak_admission_bytes,
            resident_admission_bytes,
        })
    }

    fn emit_leaf(&mut self, store: &MemoryChunkStore) -> Result<(), ChunkError> {
        let Some(span) = self.segments.get(self.index).copied() else {
            self.index = 0;
            self.phase = Phase::Assemble;
            return Ok(());
        };
        let entries = &self.replacement[span.start..span.end];
        let bytes = tree_v2::encode_leaf(entries)?;
        if bytes.len() != span.bytes {
            return Err(invalid(
                "Tree v2 candidate mutation emitted leaf differs from its plan",
            ));
        }
        let range = RangeRef {
            min_path: copy_path_with_exact_request(&entries[0].path)?,
            max_path: copy_path_with_exact_request(&entries[entries.len() - 1].path)?,
            hash: hash_bytes(&bytes),
            file_count: entries.len() as u64,
            serialized_bytes: bytes.len() as u32,
            height: 0,
        };
        self.stage(store, range.hash, bytes)?;
        self.add_emitted_node_bytes(span.bytes)?;
        self.replacement_refs.push(range);
        self.index += 1;
        Ok(())
    }

    fn assemble_one(&mut self) -> Result<(), ChunkError> {
        let prefix = if self.empty_base { 0 } else { self.start };
        let replacements_end = prefix + self.replacement_refs.len();
        let suffix_start = if self.empty_base { 0 } else { self.end + 1 };
        let total = replacements_end + self.leaves.len() - suffix_start;
        if self.index == total {
            self.phase = Phase::BeginLevel;
            return Ok(());
        }
        let range = if self.index < prefix {
            copy_range_with_exact_request(&self.leaves[self.index])?
        } else if self.index < replacements_end {
            copy_range_with_exact_request(&self.replacement_refs[self.index - prefix])?
        } else {
            copy_range_with_exact_request(
                &self.leaves[suffix_start + self.index - replacements_end],
            )?
        };
        validate_next_range(self.ranges.last(), &range, 0)?;
        self.total_files = self
            .total_files
            .checked_add(range.file_count)
            .ok_or_else(|| invalid("Tree v2 file count overflow"))?;
        self.ranges.push(range);
        self.index += 1;
        Ok(())
    }

    fn begin_level(&mut self) -> Result<(), ChunkError> {
        if self.ranges.len() <= 1 {
            if self.emitted_internal_spans != self.planned_internal_spans.len()
                || self.emitted_node_payload_bytes != self.output_memory_plan.node_payload_bytes
            {
                return Err(invalid(
                    "Tree v2 candidate mutation output differs from its plan",
                ));
            }
            let next = TreeV2Root {
                version: TREE_VERSION,
                total_files: self.total_files,
                child: self.ranges.pop(),
            };
            tree_v2::validate_root_shape(&next)?;
            let endpoint_bytes = next.child.as_ref().map_or(Ok(0), |range| {
                let min = u64::try_from(range.min_path.len())
                    .map_err(|_| invalid("Tree v2 candidate mutation endpoint byte overflow"))?;
                let max = u64::try_from(range.max_path.len())
                    .map_err(|_| invalid("Tree v2 candidate mutation endpoint byte overflow"))?;
                min.checked_add(max)
                    .ok_or_else(|| invalid("Tree v2 candidate mutation endpoint byte overflow"))
            })?;
            if endpoint_bytes
                != self
                    .output_memory_plan
                    .range_endpoint_resident_requested_bytes
            {
                return Err(invalid(
                    "Tree v2 candidate mutation root differs from its plan",
                ));
            }
            self.next = Some(next);
            self.phase = Phase::Cleanup;
        } else {
            if self.ranges[0].height >= MAX_TREE_DEPTH {
                return Err(invalid("Tree v2 exceeds the depth limit"));
            }
            self.segmenter = Segmenter::internal();
            self.segments.clear();
            self.index = 0;
            self.phase = Phase::SegmentLevel;
        }
        Ok(())
    }

    fn segment_level(&mut self) -> Result<(), ChunkError> {
        if let Some(range) = self.ranges.get(self.index) {
            validate_next_range(
                self.index.checked_sub(1).map(|index| &self.ranges[index]),
                range,
                self.ranges[0].height,
            )?;
            self.segmenter.record(
                self.index,
                tree_v2::range_record_len(range),
                range.max_path.as_bytes(),
                &mut self.segments,
            )?;
            self.index += 1;
        } else {
            self.segmenter
                .finish_tail(self.ranges.len(), &mut self.segments);
            self.index = 0;
            self.phase = Phase::EmitInternal;
        }
        Ok(())
    }

    fn emit_internal(&mut self, store: &MemoryChunkStore) -> Result<(), ChunkError> {
        let Some(span) = self.segments.get(self.index).copied() else {
            self.phase = Phase::RetireLevel;
            return Ok(());
        };
        let planned = self
            .planned_internal_spans
            .get(self.emitted_internal_spans)
            .copied()
            .ok_or_else(|| invalid("Tree v2 candidate mutation emitted an unplanned node"))?;
        if planned.span.start != span.start
            || planned.span.end != span.end
            || planned.span.bytes != span.bytes
            || planned.child_height != self.ranges[span.start].height
        {
            return Err(invalid(
                "Tree v2 candidate mutation internal segmentation differs from its plan",
            ));
        }
        let (bytes, hash, file_count, serialized_bytes, height) = {
            let children = &self.ranges[span.start..span.end];
            let bytes = tree_v2::encode_internal(children[0].height, children)?;
            if bytes.len() != span.bytes {
                return Err(invalid(
                    "Tree v2 candidate mutation emitted internal differs from its plan",
                ));
            }
            let hash = hash_bytes(&bytes);
            let file_count = children.iter().try_fold(0u64, |sum, child| {
                sum.checked_add(child.file_count)
                    .ok_or_else(|| invalid("Tree v2 file count overflow"))
            })?;
            let serialized_bytes = bytes.len() as u32;
            (
                bytes,
                hash,
                file_count,
                serialized_bytes,
                children[0].height + 1,
            )
        };
        self.stage(store, hash, bytes)?;
        self.add_emitted_node_bytes(span.bytes)?;
        // Every child belongs only to this private level and each segment is
        // encoded exactly once. After successful staging its two boundary
        // allocations can become the parent without cloning.
        let min_path = std::mem::take(&mut self.ranges[span.start].min_path);
        let max_path = std::mem::take(&mut self.ranges[span.end - 1].max_path);
        let range = RangeRef {
            min_path,
            max_path,
            hash,
            file_count,
            serialized_bytes,
            height,
        };
        self.parents.push(range);
        self.emitted_internal_spans += 1;
        if !self.empty_base {
            self.stats.internal_nodes_emitted += 1;
        }
        self.index += 1;
        Ok(())
    }

    fn stage(
        &mut self,
        store: &MemoryChunkStore,
        hash: FileHash,
        bytes: Vec<u8>,
    ) -> Result<(), ChunkError> {
        if let Some(matches) = store.resident_chunk_matches(&hash, &bytes) {
            if matches {
                return Ok(());
            }
            return Err(invalid("Tree v2 mutation resident chunk collision"));
        }
        if let Some(existing) = self.staged.get(&hash) {
            if *existing != bytes {
                return Err(invalid("Tree v2 mutation staged chunk collision"));
            }
            return Ok(());
        }
        if self.staged.len() >= MAX_VISITED_NODES {
            return Err(invalid("Tree v2 mutation staged node limit"));
        }
        self.staged_bytes = self
            .staged_bytes
            .checked_add(bytes.len())
            .ok_or_else(|| invalid("Tree v2 mutation staged byte overflow"))?;
        self.staged.insert(hash, bytes);
        Ok(())
    }

    fn add_emitted_node_bytes(&mut self, bytes: usize) -> Result<(), ChunkError> {
        let bytes = u64::try_from(bytes)
            .map_err(|_| invalid("Tree v2 candidate mutation emitted byte overflow"))?;
        self.emitted_node_payload_bytes = self
            .emitted_node_payload_bytes
            .checked_add(bytes)
            .ok_or_else(|| invalid("Tree v2 candidate mutation emitted byte overflow"))?;
        if self.emitted_node_payload_bytes > self.output_memory_plan.node_payload_bytes {
            return Err(invalid(
                "Tree v2 candidate mutation emitted bytes exceed its plan",
            ));
        }
        Ok(())
    }

    /// Irreversibly release one bounded owned record/descriptor/node buffer or
    /// one exhausted container backing allocation. The initial conversion is
    /// O(1); no input-sized temporary collection or nested bulk destructor is
    /// created. Hash-table/B-tree node deallocation and one bounded Vec/String
    /// allocator free remain synchronous native primitives.
    pub(crate) fn retire_one(&mut self) -> bool {
        if self.phase != Phase::Retiring {
            self.phase = Phase::Retiring;
            self.retirement_phase = RetirementPhase::Changed;
            self.retiring_staged = Some(std::mem::take(&mut self.staged).into_iter());
        }
        loop {
            match self.retirement_phase {
                RetirementPhase::Changed => {
                    if let Some(row) = self.changed.next() {
                        drop(row);
                        return true;
                    }
                    self.changed = Vec::new().into_iter();
                    self.retirement_phase = RetirementPhase::Deleted;
                    return true;
                }
                RetirementPhase::Deleted => {
                    if let Some(path) = self.deleted.next() {
                        drop(path);
                        return true;
                    }
                    self.deleted = Vec::new().into_iter();
                    self.retirement_phase = RetirementPhase::Operations;
                    return true;
                }
                RetirementPhase::Operations => {
                    if let Some(row) = self.operations.pop_first() {
                        drop(row);
                        return true;
                    }
                    self.retirement_phase = RetirementPhase::Pending;
                }
                RetirementPhase::Pending => {
                    if let Some(range) = self.pending.pop() {
                        drop(range);
                        return true;
                    }
                    if release_vec_backing(&mut self.pending) {
                        self.retirement_phase = RetirementPhase::Leaves;
                        return true;
                    }
                    self.retirement_phase = RetirementPhase::Leaves;
                }
                RetirementPhase::Leaves => {
                    if let Some(range) = self.leaves.pop() {
                        drop(range);
                        return true;
                    }
                    if release_vec_backing(&mut self.leaves) {
                        self.retirement_phase = RetirementPhase::Loaded;
                        return true;
                    }
                    self.retirement_phase = RetirementPhase::Loaded;
                }
                RetirementPhase::Loaded => {
                    if let Some(row) = self.loaded.next() {
                        drop(row);
                        return true;
                    }
                    self.loaded = Vec::new().into_iter();
                    self.retirement_phase = RetirementPhase::Replacement;
                    return true;
                }
                RetirementPhase::Replacement => {
                    if let Some(row) = self.replacement.pop() {
                        drop(row);
                        return true;
                    }
                    if release_vec_backing(&mut self.replacement) {
                        self.retirement_phase = RetirementPhase::ReplacementRefs;
                        return true;
                    }
                    self.retirement_phase = RetirementPhase::ReplacementRefs;
                }
                RetirementPhase::ReplacementRefs => {
                    if let Some(range) = self.replacement_refs.pop() {
                        drop(range);
                        return true;
                    }
                    if release_vec_backing(&mut self.replacement_refs) {
                        self.retirement_phase = RetirementPhase::Segments;
                        return true;
                    }
                    self.retirement_phase = RetirementPhase::Segments;
                }
                RetirementPhase::Segments => {
                    self.segments.clear();
                    if release_vec_backing(&mut self.segments) {
                        self.retirement_phase = RetirementPhase::PlannedRanges;
                        return true;
                    }
                    self.retirement_phase = RetirementPhase::PlannedRanges;
                }
                RetirementPhase::PlannedRanges => {
                    self.planned_ranges.clear();
                    if release_vec_backing(&mut self.planned_ranges) {
                        self.retirement_phase = RetirementPhase::NextPlannedRanges;
                        return true;
                    }
                    self.retirement_phase = RetirementPhase::NextPlannedRanges;
                }
                RetirementPhase::NextPlannedRanges => {
                    self.next_planned_ranges.clear();
                    if release_vec_backing(&mut self.next_planned_ranges) {
                        self.retirement_phase = RetirementPhase::PlannedSpans;
                        return true;
                    }
                    self.retirement_phase = RetirementPhase::PlannedSpans;
                }
                RetirementPhase::PlannedSpans => {
                    self.planned_spans.clear();
                    if release_vec_backing(&mut self.planned_spans) {
                        self.retirement_phase = RetirementPhase::PlannedInternalSpans;
                        return true;
                    }
                    self.retirement_phase = RetirementPhase::PlannedInternalSpans;
                }
                RetirementPhase::PlannedInternalSpans => {
                    self.planned_internal_spans.clear();
                    if release_vec_backing(&mut self.planned_internal_spans) {
                        self.retirement_phase = RetirementPhase::Ranges;
                        return true;
                    }
                    self.retirement_phase = RetirementPhase::Ranges;
                }
                RetirementPhase::Ranges => {
                    if let Some(range) = self.ranges.pop() {
                        drop(range);
                        return true;
                    }
                    if release_vec_backing(&mut self.ranges) {
                        self.retirement_phase = RetirementPhase::Parents;
                        return true;
                    }
                    self.retirement_phase = RetirementPhase::Parents;
                }
                RetirementPhase::Parents => {
                    if let Some(range) = self.parents.pop() {
                        drop(range);
                        return true;
                    }
                    if release_vec_backing(&mut self.parents) {
                        self.retirement_phase = RetirementPhase::Staged;
                        return true;
                    }
                    self.retirement_phase = RetirementPhase::Staged;
                }
                RetirementPhase::Staged => {
                    let staged = self
                        .retiring_staged
                        .as_mut()
                        .expect("retiring mutation must own its staged iterator");
                    if let Some(row) = staged.next() {
                        drop(row);
                        return true;
                    }
                    self.retiring_staged = None;
                    self.retirement_phase = RetirementPhase::Captured;
                    return true;
                }
                RetirementPhase::Captured => {
                    if let Some(range) = self.captured.child.take() {
                        drop(range);
                        return true;
                    }
                    self.retirement_phase = RetirementPhase::Next;
                }
                RetirementPhase::Next => {
                    if let Some(root) = &mut self.next {
                        if let Some(range) = root.child.take() {
                            drop(range);
                            return true;
                        }
                    }
                    self.next = None;
                    self.retirement_phase = RetirementPhase::Done;
                }
                RetirementPhase::Done => return false,
            }
        }
    }
}

fn release_vec_backing<T>(values: &mut Vec<T>) -> bool {
    debug_assert!(values.is_empty());
    if values.capacity() == 0 {
        return false;
    }
    *values = Vec::new();
    true
}

fn load_bytes(store: &MemoryChunkStore, range: &RangeRef) -> Result<Vec<u8>, ChunkError> {
    // Refuse malformed/native oversized buffers before get_chunk clones them.
    if store.chunk_byte_length(&range.hash) != Some(range.serialized_bytes as usize) {
        return Err(invalid(
            "Tree v2 mutation child is missing or has the wrong length",
        ));
    }
    store
        .get_chunk(&range.hash)
        .ok_or_else(|| ChunkError::NotFound(hash_to_hex(&range.hash)))
}

fn validate_next_range(
    previous: Option<&RangeRef>,
    range: &RangeRef,
    height: u16,
) -> Result<(), ChunkError> {
    tree_v2::validate_range(range)?;
    if range.height != height
        || previous.is_some_and(|previous| {
            previous.max_path >= range.min_path || previous.max_path >= range.max_path
        })
    {
        return Err(invalid(
            "Tree v2 ranges overlap or have inconsistent heights",
        ));
    }
    Ok(())
}

fn invalid(message: &str) -> ChunkError {
    ChunkError::Deserialize(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::ChunkStore;

    fn entry(index: usize, revision: u8) -> FileEntry {
        FileEntry {
            path: format!("folder/note-{index:06}.md"),
            hash: hash_bytes(&[revision]),
            mtime_ms: u64::from(revision),
            size_bytes: u64::from(revision) + 1,
        }
    }

    fn snapshot(store: &MemoryChunkStore) -> BTreeMap<FileHash, Vec<u8>> {
        store.all_chunks().into_iter().collect()
    }

    fn duplicate_store(store: &MemoryChunkStore) -> MemoryChunkStore {
        let copy = MemoryChunkStore::new();
        for (hash, bytes) in store.all_chunks() {
            copy.insert_chunk(hash, bytes);
        }
        copy
    }

    fn finish_steps(job: &mut V2CandidateMutation, store: &MemoryChunkStore, budget: usize) {
        let mut previous = job.progress(0).completed;
        for _ in 0..1_000_000 {
            let progress = job.step(store, budget).unwrap();
            assert!(progress.units <= budget);
            assert_eq!(progress.completed, previous + progress.units);
            previous = progress.completed;
            if progress.done {
                return;
            }
            if progress.phase == "plan ready" {
                let plan = job.output_memory_plan_v1().unwrap();
                job.resume_after_output_memory_plan_v1(
                    plan.node_payload_bytes,
                    plan.range_endpoint_peak_requested_bytes,
                    plan.range_endpoint_resident_requested_bytes,
                )
                .unwrap();
            } else {
                assert!(progress.units > 0);
            }
        }
        panic!("mutation never became ready");
    }

    async fn compare(
        entries: Vec<FileEntry>,
        changed: Vec<FileEntry>,
        deleted: Vec<String>,
        budget: usize,
    ) -> (TreeV2Root, UpdateStats) {
        let store = MemoryChunkStore::new();
        let root = tree_v2::build_tree(&store, entries).await.unwrap();
        let oracle_store = duplicate_store(&store);
        let (oracle_root, oracle_stats) =
            tree_v2::update_tree(&oracle_store, &root, &changed, &deleted)
                .await
                .unwrap();
        let initial = snapshot(&store);
        let mut job = V2CandidateMutation::new(root.clone(), changed, deleted).unwrap();
        finish_steps(&mut job, &store, budget);
        assert_eq!(snapshot(&store), initial, "steps published staged bytes");
        let (next, stats) = job.finish(&store, &root).unwrap();
        assert_eq!(next, oracle_root, "canonical semantic root differs");
        assert_eq!(next.hash(), oracle_root.hash());
        assert_eq!(stats, oracle_stats, "localized update statistics differ");
        assert_eq!(
            snapshot(&store),
            snapshot(&oracle_store),
            "emitted immutable objects differ"
        );
        tree_v2::reachable_hashes(&store, &next).await.unwrap();
        (next, stats)
    }

    #[tokio::test]
    async fn matches_legacy_normalization_empty_and_deletion_semantics() {
        for budget in [1, 3, 256] {
            compare(Vec::new(), Vec::new(), Vec::new(), budget).await;
            compare(Vec::new(), Vec::new(), vec![entry(1, 0).path], budget).await;
            compare(
                Vec::new(),
                vec![entry(1, 1), entry(0, 2), entry(1, 3)],
                vec![entry(1, 0).path],
                budget,
            )
            .await;
            compare(
                (0..8).map(|i| entry(i, 0)).collect(),
                Vec::new(),
                (0..8).map(|i| entry(i, 0).path).collect(),
                budget,
            )
            .await;
            compare(
                (0..8).map(|i| entry(i, 0)).collect(),
                vec![entry(7, 1), entry(0, 2), entry(7, 3)],
                vec![entry(7, 0).path, entry(3, 0).path, entry(3, 0).path],
                budget,
            )
            .await;
            compare(
                (0..8).map(|i| entry(i, 0)).collect(),
                vec![entry(0, 0)],
                vec![entry(99, 0).path],
                budget,
            )
            .await;
        }
        let (_, empty_stats) = compare(
            Vec::new(),
            (0..2_000).map(|i| entry(i, 1)).collect(),
            Vec::new(),
            256,
        )
        .await;
        assert_eq!(
            empty_stats.internal_nodes_emitted, 0,
            "legacy empty-tree branch deliberately does not count internal emissions"
        );
        assert!(empty_stats.replacement_leaves > 1);
    }

    #[tokio::test]
    async fn localized_resynchronization_and_multi_leaf_spans_match() {
        let original: Vec<_> = (0..8_000).map(|i| entry(i, 0)).collect();
        for budget in [1, 256] {
            let (_, content_stats) =
                compare(original.clone(), vec![entry(3_000, 1)], Vec::new(), budget).await;
            assert!(content_stats.leaves_loaded > 0);
            assert!(content_stats.leaves_loaded < 10);
            assert!(!content_stats.reached_tree_end);
            compare(
                original.clone(),
                vec![entry(1, 1), entry(6_000, 2)],
                (200..500).map(|i| entry(i, 0).path).collect(),
                budget,
            )
            .await;
            compare(
                original.clone(),
                vec![
                    FileEntry {
                        path: "aaa.md".into(),
                        ..entry(0, 1)
                    },
                    FileEntry {
                        path: "zzz.md".into(),
                        ..entry(0, 2)
                    },
                ],
                Vec::new(),
                budget,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn byte_forced_leaf_and_internal_boundaries_match() {
        let entries: Vec<_> = (0..3_000)
            .map(|index| FileEntry {
                path: format!("{index:06}-{}.md", "x".repeat(3_900)),
                ..entry(index, 0)
            })
            .collect();
        let changed = vec![FileEntry {
            hash: hash_bytes(b"changed"),
            ..entries[1_000].clone()
        }];
        let (next, _) = compare(entries, changed, Vec::new(), 256).await;
        assert!(
            next.height() >= 2,
            "fixture did not exercise multiple internal levels"
        );
    }

    #[tokio::test]
    async fn deterministic_mixed_changes_match_all_roots_chunks_and_stats() {
        let mut seed = 0x9183u64;
        let mut random = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for case in 0..24 {
            let count = 800 + random() as usize % 800;
            let mut changed = Vec::new();
            let mut deleted = Vec::new();
            for _ in 0..24 {
                let index = random() as usize % (count + 100);
                if random() & 1 == 0 {
                    changed.push(entry(index, 1 + (random() % 250) as u8));
                } else {
                    deleted.push(entry(index, 0).path);
                }
            }
            compare(
                (0..count).map(|i| entry(i, 0)).collect(),
                changed,
                deleted,
                if case % 2 == 0 { 1 } else { 256 },
            )
            .await;
        }
    }

    #[tokio::test]
    async fn internal_parent_reuses_private_boundary_endpoint_allocations() {
        let store = MemoryChunkStore::new();
        let root = tree_v2::build_tree(&store, (0..5_000).map(|index| entry(index, 0)).collect())
            .await
            .unwrap();
        let initial = snapshot(&store);
        let mut job =
            V2CandidateMutation::new(root.clone(), vec![entry(2_500, 7)], Vec::new()).unwrap();
        for _ in 0..1_000_000 {
            if job.phase == Phase::EmitInternal {
                break;
            }
            let progress = job.step(&store, 1).unwrap();
            if progress.phase == "plan ready" {
                let plan = job.output_memory_plan_v1().unwrap();
                job.resume_after_output_memory_plan_v1(
                    plan.node_payload_bytes,
                    plan.range_endpoint_peak_requested_bytes,
                    plan.range_endpoint_resident_requested_bytes,
                )
                .unwrap();
            }
            assert!(
                !progress.done,
                "fixture reached ready without an internal level"
            );
        }
        assert_eq!(job.phase, Phase::EmitInternal);
        let span = job.segments[0];
        let min_ptr = job.ranges[span.start].min_path.as_ptr();
        let min_capacity = job.ranges[span.start].min_path.capacity();
        let max_ptr = job.ranges[span.end - 1].max_path.as_ptr();
        let max_capacity = job.ranges[span.end - 1].max_path.capacity();

        job.step(&store, 1).unwrap();
        let parent = job.parents.last().unwrap();
        assert_eq!(parent.min_path.as_ptr(), min_ptr);
        assert_eq!(parent.min_path.capacity(), min_capacity);
        assert_eq!(parent.max_path.as_ptr(), max_ptr);
        assert_eq!(parent.max_path.capacity(), max_capacity);
        assert!(job.ranges[span.start].min_path.is_empty());
        assert!(job.ranges[span.end - 1].max_path.is_empty());
        assert_eq!(
            snapshot(&store),
            initial,
            "private parent leaked into resident store"
        );

        finish_steps(&mut job, &store, 1);
        let (next, _) = job.finish(&store, &root).unwrap();
        tree_v2::reachable_hashes(&store, &next).await.unwrap();
    }

    #[tokio::test]
    async fn failed_parent_stage_does_not_consume_boundary_endpoints() {
        let store = MemoryChunkStore::new();
        let root = tree_v2::build_tree(&store, (0..5_000).map(|index| entry(index, 0)).collect())
            .await
            .unwrap();
        let mut job = V2CandidateMutation::new(root, vec![entry(2_500, 8)], Vec::new()).unwrap();
        for _ in 0..1_000_000 {
            if job.phase == Phase::EmitInternal {
                break;
            }
            let progress = job.step(&store, 1).unwrap();
            if progress.phase == "plan ready" {
                let plan = job.output_memory_plan_v1().unwrap();
                job.resume_after_output_memory_plan_v1(
                    plan.node_payload_bytes,
                    plan.range_endpoint_peak_requested_bytes,
                    plan.range_endpoint_resident_requested_bytes,
                )
                .unwrap();
            }
        }
        assert_eq!(job.phase, Phase::EmitInternal);
        let span = job.segments[0];
        let expected = tree_v2::encode_internal(
            job.ranges[span.start].height,
            &job.ranges[span.start..span.end],
        )
        .unwrap();
        let hash = hash_bytes(&expected);
        let min_ptr = job.ranges[span.start].min_path.as_ptr();
        let max_ptr = job.ranges[span.end - 1].max_path.as_ptr();
        store.insert_chunk(hash, vec![0; expected.len() + 1]);

        assert!(job.step(&store, 1).is_err());
        assert_eq!(job.ranges[span.start].min_path.as_ptr(), min_ptr);
        assert_eq!(job.ranges[span.end - 1].max_path.as_ptr(), max_ptr);
        assert!(!job.ranges[span.start].min_path.is_empty());
        assert!(!job.ranges[span.end - 1].max_path.is_empty());
        assert!(job.parents.is_empty());
    }

    #[tokio::test]
    async fn cancellation_after_actual_emit_never_changes_resident_state() {
        let store = MemoryChunkStore::new();
        let root = tree_v2::build_tree(&store, (0..2_000).map(|i| entry(i, 0)).collect())
            .await
            .unwrap();
        let initial = snapshot(&store);
        let mut job =
            V2CandidateMutation::new(root.clone(), vec![entry(1_000, 2)], Vec::new()).unwrap();
        for _ in 0..100_000 {
            let progress = job.step(&store, 1).unwrap();
            if progress.phase == "plan ready" {
                let plan = job.output_memory_plan_v1().unwrap();
                job.resume_after_output_memory_plan_v1(
                    plan.node_payload_bytes,
                    plan.range_endpoint_peak_requested_bytes,
                    plan.range_endpoint_resident_requested_bytes,
                )
                .unwrap();
            }
            if progress.staged_chunks > 0 {
                assert!(progress.staged_bytes > 0);
                assert!(!progress.done);
                assert_eq!(snapshot(&store), initial);
                drop(job);
                assert_eq!(snapshot(&store), initial);
                tree_v2::reachable_hashes(&store, &root).await.unwrap();
                return;
            }
        }
        panic!("cancellation fixture never emitted a staged leaf");
    }

    #[tokio::test]
    async fn invalid_source_or_input_poison_without_promotion() {
        let store = MemoryChunkStore::new();
        let root = tree_v2::build_tree(&store, vec![entry(0, 0)])
            .await
            .unwrap();
        let initial = snapshot(&store);
        let mut bad = V2CandidateMutation::new(
            root.clone(),
            vec![FileEntry {
                path: "../unsafe".into(),
                ..entry(0, 1)
            }],
            Vec::new(),
        )
        .unwrap();
        assert!(bad.step(&store, 1).is_err());
        assert!(bad.step(&store, 1).is_err());
        assert!(bad.finish(&store, &root).is_err());
        assert_eq!(snapshot(&store), initial);

        let child = root.child.as_ref().unwrap();
        store.insert_chunk(child.hash, vec![0; child.serialized_bytes as usize]);
        let corrupt = snapshot(&store);
        let mut bad =
            V2CandidateMutation::new(root.clone(), vec![entry(0, 1)], Vec::new()).unwrap();
        let failure = loop {
            match bad.step(&store, 1) {
                Ok(progress) => assert!(!progress.done),
                Err(error) => break error,
            }
        };
        assert!(failure.to_string().contains("content-address"));
        assert!(bad.finish(&store, &root).is_err());
        assert_eq!(
            snapshot(&store),
            corrupt,
            "failed read repaired or removed resident bytes"
        );
    }

    #[tokio::test]
    async fn incomplete_stale_candidate_and_late_collision_refuse_atomic_finish() {
        let store = MemoryChunkStore::new();
        let root = tree_v2::build_tree(&store, (0..2_000).map(|i| entry(i, 0)).collect())
            .await
            .unwrap();
        let initial = snapshot(&store);
        let mut job =
            V2CandidateMutation::new(root.clone(), vec![entry(1_000, 2)], Vec::new()).unwrap();
        assert!(job.finish(&store, &root).is_err());
        assert_eq!(snapshot(&store), initial);

        let mut job =
            V2CandidateMutation::new(root.clone(), vec![entry(1_000, 2)], Vec::new()).unwrap();
        finish_steps(&mut job, &store, 1);
        let foreign = TreeV2Root {
            version: TREE_VERSION,
            total_files: 0,
            child: None,
        };
        assert!(job.finish(&store, &foreign).is_err());
        assert_eq!(snapshot(&store), initial);

        let mut job =
            V2CandidateMutation::new(root.clone(), vec![entry(1_000, 2)], Vec::new()).unwrap();
        finish_steps(&mut job, &store, 1);
        assert!(
            job.staged.len() > 1,
            "fixture needs more than one possible publication"
        );
        let staged_before = job.staged.len();
        let collision = *job.staged.keys().next().unwrap();
        store.insert_chunk(collision, b"conflicting resident object".to_vec());
        let before_finish = snapshot(&store);
        assert!(job.finish(&store, &root).is_err());
        assert_eq!(
            snapshot(&store),
            before_finish,
            "failed finish partially promoted overlay"
        );
        assert_eq!(
            job.staged.len(),
            staged_before,
            "failed promotion discarded its cooperatively retired owner"
        );
        assert!(job.finish(&store, &root).is_err());
        assert_eq!(
            snapshot(&store),
            before_finish,
            "a repeated finish published an overlay lost by the first failure"
        );
    }

    #[tokio::test]
    async fn resident_identical_orphan_is_reused_and_bad_budgets_do_not_consume_work() {
        let store = MemoryChunkStore::new();
        let root = tree_v2::build_tree(&store, vec![entry(0, 0)])
            .await
            .unwrap();
        let future = MemoryChunkStore::new();
        let expected = tree_v2::build_tree(&future, vec![entry(0, 2)])
            .await
            .unwrap();
        for (hash, bytes) in future.all_chunks() {
            store.put(hash, bytes).await.unwrap();
        }
        let before = snapshot(&store);
        let mut job =
            V2CandidateMutation::new(root.clone(), vec![entry(0, 2)], Vec::new()).unwrap();
        assert!(job.step(&store, 0).is_err());
        assert!(job.step(&store, 257).is_err());
        assert_eq!(job.progress(0).completed, 0);
        finish_steps(&mut job, &store, 1);
        assert_eq!(
            job.output_memory_ready_v1()
                .unwrap()
                .staged_node_payload_bytes,
            0,
            "resident-equal output retained a redundant private node buffer"
        );
        let (next, _) = job.finish(&store, &root).unwrap();
        assert_eq!(next, expected);
        assert_eq!(
            snapshot(&store),
            before,
            "identical resident orphan was overwritten"
        );
    }

    #[tokio::test]
    async fn output_memory_plan_is_a_strict_pre_codec_barrier_and_ready_is_exact() {
        let store = MemoryChunkStore::new();
        let root = tree_v2::build_tree(&store, (0..5_000).map(|index| entry(index, 0)).collect())
            .await
            .unwrap();
        let initial = snapshot(&store);
        let mut job = V2CandidateMutation::new(
            root.clone(),
            vec![entry(2_500, 9), entry(4_999, 7)],
            vec![entry(100, 0).path],
        )
        .unwrap();

        for _ in 0..1_000_000 {
            let progress = job.step(&store, 256).unwrap();
            assert!(!progress.done);
            assert_eq!(snapshot(&store), initial);
            assert_eq!((progress.staged_chunks, progress.staged_bytes), (0, 0));
            if progress.phase == "plan ready" {
                break;
            }
            assert!(progress.units > 0);
        }
        assert_eq!(job.phase, Phase::PlanReady);
        let plan = job.output_memory_plan_v1().unwrap();
        assert!(plan.node_payload_bytes > 0);
        assert!(plan.range_endpoint_peak_requested_bytes > 0);
        assert!(
            plan.range_endpoint_peak_requested_bytes
                >= plan.range_endpoint_resident_requested_bytes
        );
        assert_eq!(
            plan.peak_admission_bytes,
            plan.node_payload_bytes + plan.range_endpoint_peak_requested_bytes
        );
        assert_eq!(
            plan.resident_admission_bytes,
            plan.node_payload_bytes + plan.range_endpoint_resident_requested_bytes
        );
        assert_eq!(job.step(&store, 256).unwrap().units, 0);
        assert!(job.output_memory_ready_v1().is_err());

        for witness in [
            (
                plan.node_payload_bytes + 1,
                plan.range_endpoint_peak_requested_bytes,
                plan.range_endpoint_resident_requested_bytes,
            ),
            (
                plan.node_payload_bytes,
                plan.range_endpoint_peak_requested_bytes + 1,
                plan.range_endpoint_resident_requested_bytes,
            ),
            (
                plan.node_payload_bytes,
                plan.range_endpoint_peak_requested_bytes,
                plan.range_endpoint_resident_requested_bytes + 1,
            ),
        ] {
            assert!(job
                .resume_after_output_memory_plan_v1(witness.0, witness.1, witness.2)
                .is_err());
            assert_eq!(job.phase, Phase::PlanReady);
            assert_eq!(job.output_memory_plan_v1().unwrap(), plan);
            assert_eq!(snapshot(&store), initial);
        }

        job.resume_after_output_memory_plan_v1(
            plan.node_payload_bytes,
            plan.range_endpoint_peak_requested_bytes,
            plan.range_endpoint_resident_requested_bytes,
        )
        .unwrap();
        assert!(job.output_memory_plan_v1().is_err());
        finish_steps(&mut job, &store, 256);
        assert_eq!(job.emitted_node_payload_bytes, plan.node_payload_bytes);
        let ready = job.output_memory_ready_v1().unwrap();
        assert_eq!(ready.staged_node_payload_bytes, job.staged_bytes as u64);
        assert!(ready.staged_node_payload_bytes <= plan.node_payload_bytes);
        assert_eq!(
            ready.range_endpoint_resident_requested_bytes,
            plan.range_endpoint_resident_requested_bytes
        );
        assert_eq!(
            ready.resident_admission_bytes,
            ready.staged_node_payload_bytes + ready.range_endpoint_resident_requested_bytes
        );
        let next = job.next.as_ref().unwrap();
        let child = next.child.as_ref().unwrap();
        assert!(child.min_path.capacity() >= child.min_path.len());
        assert!(child.max_path.capacity() >= child.max_path.len());
        assert_eq!(
            (child.min_path.len() + child.max_path.len()) as u64,
            ready.range_endpoint_resident_requested_bytes
        );
        assert_eq!(snapshot(&store), initial);
        job.finish(&store, &root).unwrap();
    }

    #[test]
    fn output_memory_plan_checked_totals_refuse_overflow() {
        assert!(V2CandidateMutation::build_output_memory_plan_v1(u64::MAX, 1, 0).is_err());
        assert!(V2CandidateMutation::build_output_memory_plan_v1(u64::MAX, 0, 1).is_err());
        let plan = V2CandidateMutation::build_output_memory_plan_v1(3, 5, 2).unwrap();
        assert_eq!(plan.peak_admission_bytes, 8);
        assert_eq!(plan.resident_admission_bytes, 5);
    }

    #[tokio::test]
    async fn retirement_is_irreversible_and_releases_staged_nodes_one_per_unit() {
        let store = MemoryChunkStore::new();
        let root = tree_v2::build_tree(&store, (0..5_000).map(|index| entry(index, 0)).collect())
            .await
            .unwrap();
        let initial = snapshot(&store);
        let mut ready =
            V2CandidateMutation::new(root.clone(), vec![entry(2_500, 9)], Vec::new()).unwrap();
        finish_steps(&mut ready, &store, 256);
        assert!(!ready.staged.is_empty());

        for _ in 0..1_000_000 {
            if ready.retirement_phase == RetirementPhase::Staged {
                break;
            }
            assert!(ready.retire_one());
        }
        assert_eq!(ready.retirement_phase, RetirementPhase::Staged);
        let before_nodes = ready.retiring_staged.as_ref().unwrap().len();
        assert!(before_nodes > 0);
        assert!(ready.retire_one());
        assert_eq!(
            ready.retiring_staged.as_ref().unwrap().len(),
            before_nodes - 1
        );
        assert!(ready.step(&store, 1).is_err());
        while ready.retire_one() {}
        assert!(!ready.retire_one());
        assert_eq!(snapshot(&store), initial);

        let mut partial = V2CandidateMutation::new(
            root.clone(),
            (0..25_000).map(|index| entry(index, 2)).collect(),
            Vec::new(),
        )
        .unwrap();
        partial.step(&store, 1).unwrap();
        let mut units = 0usize;
        while partial.retire_one() {
            units += 1;
            assert!(units < 100_000);
        }
        assert!(units >= 25_000, "input rows were not retired cooperatively");
        assert!(partial.step(&store, 1).is_err());
        assert_eq!(snapshot(&store), initial);

        let mut failed = V2CandidateMutation::new(
            root,
            vec![FileEntry {
                path: "../invalid".into(),
                ..entry(0, 4)
            }],
            Vec::new(),
        )
        .unwrap();
        assert!(failed.step(&store, 1).is_err());
        assert!(failed.retire_one());
        while failed.retire_one() {}
        assert_eq!(snapshot(&store), initial);
    }
}
