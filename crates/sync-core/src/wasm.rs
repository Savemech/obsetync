#![cfg(feature = "wasm")]

use crate::candidate_mutation_v1::CandidateMutationV1;
use crate::candidate_mutation_v2::{
    CandidateMutationOutputMemoryPlanV1, CandidateMutationOutputMemoryReadyV1, V2CandidateMutation,
    V2MutationProgress,
};
use crate::chunk::{ChunkError, FileEntry, RootNode};
use crate::hash::{hash_bytes, hash_to_hex, hex_to_hash, FileHash};
use crate::replacement_rebuild::{
    ReplacementBuildProgress, ReplacementOutputMemoryPlanV1, SortWorkspace,
};
use crate::replacement_rebuild_v1::ReplacementRebuildV1;
use crate::replacement_rebuild_v2::{ReplacementBuildPlanV2, ReplacementRebuildV2};
use crate::root_export_v1::{
    RootExportV1, ROOT_EXPORT_MAX_ATOMIC_BYTES, ROOT_EXPORT_MAX_STEP_BYTES,
    ROOT_EXPORT_MAX_STEP_UNITS, ROOT_EXPORT_PART_BYTES,
};
use crate::store::ChunkStoreMemory;
use crate::transactional_tree::{
    CandidateOpenMemoryPlanV1, ChunkGcStats, ReachabilityCursor, TransactionalTree,
    TreeRetirementV1,
};
use crate::transactional_tree_v2::{
    RootRetirementV2, TransactionalTreeV2, TreeRetirementV2, V2StableOutputMemoryV1,
};
use crate::tree_metadata::{OwnedRootV1, StringMemory, TreeMetadataMemory};
use crate::tree_v2::{RootNodeV2, V2ReachabilityCursor};
use crate::tree_work_memory::{
    ReplacementInputMemory, ReplacementSortMemory, ReplacementV1EntriesMemory,
    ReplacementV1GraphMemory, ReplacementV2PlanningMemory, ReplacementV2RangesMemory,
};
use crate::versioned_root::{VersionedRoot, TREE_V1, TREE_V2};
use serde::Serialize;
use wasm_bindgen::prelude::*;

/// Serialize a Rust value to a plain JS object (not a JS Map).
/// serde-wasm-bindgen 0.4+ serializes maps as JS Map by default, which breaks
/// property access syntax (obj.field) — this forces plain objects instead.
fn to_js(value: &impl Serialize) -> JsValue {
    let ser = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
    value.serialize(&ser).unwrap_or(JsValue::NULL)
}

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}

/// One module's allocator observation, not a per-tree/admission/RSS budget.
/// Integer counters which cannot be represented exactly by JS are null and
/// invalidate the report, never silently rounded. Encoding itself may affect
/// subsequent allocation counters; the raw snapshot is nonallocating.
#[derive(Serialize)]
struct WasmMemoryReport {
    schema: u8,
    scope: &'static str,
    enabled: bool,
    counters_valid: bool,
    precision_lost: bool,
    live_requested_bytes: Option<u64>,
    peak_requested_bytes: Option<u64>,
    live_allocations: Option<u64>,
    successful_allocations: Option<u64>,
    successful_reallocations: Option<u64>,
    allocation_failures: Option<u64>,
    reallocation_failures: Option<u64>,
    linear_memory_bytes: Option<u64>,
}

impl From<crate::wasm_memory::WasmMemorySnapshot> for WasmMemoryReport {
    fn from(snapshot: crate::wasm_memory::WasmMemorySnapshot) -> Self {
        const MAX_SAFE_INTEGER: u64 = (1u64 << 53) - 1;
        let exact = |value| (value <= MAX_SAFE_INTEGER).then_some(value);
        let counters = [
            snapshot.live_requested_bytes,
            snapshot.peak_requested_bytes,
            snapshot.live_allocations,
            snapshot.successful_allocations,
            snapshot.successful_reallocations,
            snapshot.allocation_failures,
            snapshot.reallocation_failures,
        ];
        let precision_lost = counters.iter().any(|value| exact(*value).is_none())
            || snapshot
                .linear_memory_bytes
                .is_some_and(|value| exact(value).is_none());
        Self {
            schema: 1,
            scope: "wasm-instance",
            enabled: snapshot.enabled,
            counters_valid: snapshot.counters_valid && !precision_lost,
            precision_lost,
            live_requested_bytes: exact(snapshot.live_requested_bytes),
            peak_requested_bytes: exact(snapshot.peak_requested_bytes),
            live_allocations: exact(snapshot.live_allocations),
            successful_allocations: exact(snapshot.successful_allocations),
            successful_reallocations: exact(snapshot.successful_reallocations),
            allocation_failures: exact(snapshot.allocation_failures),
            reallocation_failures: exact(snapshot.reallocation_failures),
            linear_memory_bytes: snapshot.linear_memory_bytes.and_then(exact),
        }
    }
}

#[wasm_bindgen]
pub fn wasm_memory_snapshot() -> JsValue {
    // This ABI uses explicit null for unavailable/unsafe integers. Keep the
    // legacy tree serializers' undefined behavior unchanged.
    let report = WasmMemoryReport::from(crate::wasm_memory::snapshot());
    let serializer = serde_wasm_bindgen::Serializer::new()
        .serialize_maps_as_objects(true)
        .serialize_missing_as_null(true);
    report.serialize(&serializer).unwrap_or(JsValue::NULL)
}

#[cfg(test)]
mod memory_report_tests {
    use super::WasmMemoryReport;

    #[test]
    fn memory_report_never_rounds_out_of_range_counters_or_hides_invalidity() {
        let mut raw = crate::wasm_memory::snapshot();
        let disabled = WasmMemoryReport::from(raw);
        assert!(!disabled.enabled);
        assert!(disabled.counters_valid);
        assert!(!disabled.precision_lost);
        assert_eq!(disabled.live_requested_bytes, Some(0));
        raw.enabled = true;
        raw.successful_allocations = (1u64 << 53) - 1;
        assert!(WasmMemoryReport::from(raw).counters_valid);
        raw.successful_allocations += 1;
        let overflow = WasmMemoryReport::from(raw);
        assert!(overflow.precision_lost);
        assert!(!overflow.counters_valid);
        assert_eq!(overflow.successful_allocations, None);
        assert_eq!(overflow.live_requested_bytes, Some(0));
        raw.successful_allocations = 0;
        raw.counters_valid = false;
        let invalid = WasmMemoryReport::from(raw);
        assert!(!invalid.counters_valid);
        assert!(!invalid.precision_lost);
        raw.counters_valid = true;
        raw.linear_memory_bytes = Some(u64::MAX);
        let linear_overflow = WasmMemoryReport::from(raw);
        assert!(linear_overflow.precision_lost);
        assert!(!linear_overflow.counters_valid);
        assert_eq!(linear_overflow.linear_memory_bytes, None);
    }
}

/// Hash raw bytes using Blake3. Returns hex string.
/// This is the hot path — called per-file from Web Workers.
#[wasm_bindgen]
pub fn wasm_hash(data: &[u8]) -> String {
    hash_to_hex(&hash_bytes(data))
}

/// Holds the local Merkle tree state in WASM memory.
/// Used by the plugin's push path to incrementally update the tree.
#[wasm_bindgen]
pub struct WasmTree {
    inner: WasmTreeInner,
    vault_id: String,
    device_id: String,
    // Monotonic identity of committed-state replacement within this exact
    // wrapper. Kept outside `inner` because load/rebuild replace that value.
    committed_revision: u64,
    // Separate owner-local witness: provisional jobs do not change a visible
    // candidate until their successful finish publishes it.
    candidate_revision: u64,
    tree_job: Option<WasmTreeJob>,
    next_tree_job_token: u32,
    last_completed_reachability_retirement: Option<CompletedReachabilityRetirement>,
}

const MAX_SAFE_COMMITTED_REVISION: u64 = (1u64 << 53) - 1;
const MAX_SAFE_CANDIDATE_REVISION: u64 = (1u64 << 53) - 1;

enum WasmTreeInner {
    V1(TransactionalTree),
    V2(TransactionalTreeV2),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WasmTreeJobPurpose {
    BeginCandidate,
    CandidateChunks,
    CandidateMutation,
    ReplacementRebuild,
    TreeChunkExport,
    RootExport,
    Retirement,
}

/// Owns abandoned private input/builds or the previous live graph after a
/// successful replacement. It occupies the normal exclusive job slot until
/// fully drained: no unbounded retirement queue or overlapping graph build.
struct TreeRetirement {
    reachability: Option<ReachabilityRetirement>,
    candidate_chunk_plan: Option<CandidateChunkSortCursor>,
    prepared_candidate_v1: Option<OwnedRootV1>,
    prepared_candidate_v2: Option<RootRetirementV2>,
    reachability_origin: bool,
    mutation_v1: Option<CandidateMutationV1>,
    mutation_v1_changed: Option<Vec<FileEntry>>,
    mutation_v1_deleted: Option<Vec<String>>,
    mutation_v2: Option<V2CandidateMutation>,
    mutation_origin: bool,
    input: Option<std::vec::IntoIter<FileEntry>>,
    input_memory: Option<ReplacementInputMemory>,
    builder_v1: Option<ReplacementRebuildV1>,
    builder_v2: Option<ReplacementRebuildV2>,
    tree_v1: Option<TreeRetirementV1>,
    tree_v2: Option<TreeRetirementV2>,
    completed: usize,
    done: bool,
    last_progress: Option<TreeRetirementProgress>,
}

#[derive(Clone, Copy)]
struct CompletedReachabilityRetirement {
    token: u32,
    completed: usize,
}

enum ReachabilityRetirement {
    V1(ReachabilityCursor),
    V2(V2ReachabilityCursor),
}

impl ReachabilityRetirement {
    fn retire_one(&mut self) -> bool {
        match self {
            Self::V1(cursor) => cursor.retire_one(),
            Self::V2(cursor) => cursor.retire_one(),
        }
    }
}

impl TreeRetirement {
    fn has_unmeasured_private(&self) -> bool {
        self.reachability.is_some()
            || self.candidate_chunk_plan.is_some()
            || self.mutation_v1.is_some()
            || self.mutation_v1_changed.is_some()
            || self.mutation_v1_deleted.is_some()
            || self.mutation_v2.is_some()
            || self.prepared_candidate_v1.is_some()
            || self.prepared_candidate_v2.is_some()
    }

    fn chunk_memory(&self) -> ChunkStoreMemory {
        self.builder_v1
            .as_ref()
            .map_or_else(
                ChunkStoreMemory::default,
                ReplacementRebuildV1::chunk_memory,
            )
            .combine(self.builder_v2.as_ref().map_or_else(
                ChunkStoreMemory::default,
                ReplacementRebuildV2::chunk_memory,
            ))
            .combine(
                self.tree_v1
                    .as_ref()
                    .map_or_else(ChunkStoreMemory::default, TreeRetirementV1::chunk_memory),
            )
            .combine(
                self.tree_v2
                    .as_ref()
                    .map_or_else(ChunkStoreMemory::default, TreeRetirementV2::chunk_memory),
            )
    }

    /// Known root/identity/baseline owners only. A partial builder can own
    /// provisional metadata which this component ABI intentionally reports as
    /// incomplete rather than treating it as zero.
    fn metadata_memory(&self) -> (TreeMetadataMemory, bool, bool) {
        let mut summary = TreeMetadataMemory::default();
        let mut has_known = false;
        // Even after the last row is yielded, Vec::IntoIter retains its
        // backing allocation until the iterator itself is dropped.
        let mut incomplete = self.input.is_some()
            || self.reachability.is_some()
            || self.candidate_chunk_plan.is_some()
            || self.mutation_v1.is_some()
            || self.mutation_v1_changed.is_some()
            || self.mutation_v1_deleted.is_some()
            || self.mutation_v2.is_some();
        if let Some(cursor) = &self.builder_v1 {
            match cursor.metadata_memory() {
                Some(memory) => {
                    summary = summary.combine(memory);
                    has_known = true;
                }
                None => incomplete = true,
            }
        }
        if let Some(cursor) = &self.builder_v2 {
            match cursor.metadata_memory() {
                Some(memory) => {
                    summary = summary.combine(memory);
                    has_known = true;
                }
                None => incomplete = true,
            }
        }
        if let Some(cursor) = &self.tree_v1 {
            summary = summary.combine(cursor.metadata_memory());
            has_known = true;
        }
        if let Some(cursor) = &self.tree_v2 {
            summary = summary.combine(cursor.metadata_memory());
            has_known = true;
        }
        if let Some(root) = &self.prepared_candidate_v1 {
            summary = summary.combine(TreeMetadataMemory::new(
                crate::tree_metadata::RootMetadataMemory::default(),
                root.metadata(),
                StringMemory::default(),
                crate::tree_metadata::CandidateBaselineMemory::default(),
            ));
            has_known = true;
        }
        if let Some(root) = &self.prepared_candidate_v2 {
            summary = summary.combine(TreeMetadataMemory::new(
                crate::tree_metadata::RootMetadataMemory::default(),
                root.metadata_memory(),
                StringMemory::default(),
                crate::tree_metadata::CandidateBaselineMemory::default(),
            ));
            has_known = true;
        }
        (summary, has_known, incomplete)
    }

    fn replacement_input_memory(&self) -> Option<ReplacementInputMemory> {
        if self.input.is_some() {
            return self.input_memory;
        }
        if self.builder_v1.is_some() || self.builder_v2.is_some() {
            return None;
        }
        Some(ReplacementInputMemory::default())
    }

    fn replacement_sort_memory(&self) -> ReplacementSortMemory {
        if let Some(cursor) = &self.builder_v1 {
            return cursor.sort_memory();
        }
        if let Some(cursor) = &self.builder_v2 {
            return cursor.sort_memory();
        }
        ReplacementSortMemory::default()
    }

    fn replacement_v1_entries_memory(&self) -> ReplacementV1EntriesMemory {
        self.builder_v1.as_ref().map_or_else(
            ReplacementV1EntriesMemory::default,
            ReplacementRebuildV1::entry_memory,
        )
    }

    fn replacement_v1_graph_memory(&self) -> ReplacementV1GraphMemory {
        self.builder_v1.as_ref().map_or_else(
            ReplacementV1GraphMemory::default,
            ReplacementRebuildV1::graph_memory,
        )
    }

    fn replacement_v2_planning_memory(&self) -> ReplacementV2PlanningMemory {
        self.builder_v2.as_ref().map_or_else(
            ReplacementV2PlanningMemory::default,
            ReplacementRebuildV2::planning_memory,
        )
    }

    fn replacement_v2_ranges_memory(&self) -> ReplacementV2RangesMemory {
        self.builder_v2.as_ref().map_or_else(
            ReplacementV2RangesMemory::default,
            ReplacementRebuildV2::range_memory,
        )
    }

    fn replacement_v2_post_sort_memory(&self) -> Option<ReplacementInputMemory> {
        self.builder_v2
            .as_ref()
            .map_or(Some(ReplacementInputMemory::default()), |cursor| {
                cursor.post_sort_entry_memory()
            })
    }

    fn from_replacement(job: WasmTreeJob, previous: Option<WasmTreeInner>) -> Self {
        let mut result = Self {
            reachability: None,
            candidate_chunk_plan: None,
            prepared_candidate_v1: None,
            prepared_candidate_v2: None,
            reachability_origin: false,
            mutation_v1: None,
            mutation_v1_changed: None,
            mutation_v1_deleted: None,
            mutation_v2: None,
            mutation_origin: false,
            input: None,
            input_memory: None,
            builder_v1: None,
            builder_v2: None,
            tree_v1: None,
            tree_v2: None,
            completed: 0,
            done: false,
            last_progress: None,
        };
        match job {
            WasmTreeJob::ReplacementRebuild {
                entries,
                input_memory,
                ..
            } => {
                result.input = Some(entries.into_iter());
                result.input_memory = Some(input_memory);
            }
            WasmTreeJob::ReplacementBuildV1 { cursor, .. } => result.builder_v1 = Some(cursor),
            WasmTreeJob::ReplacementBuildV2 { cursor, .. } => result.builder_v2 = Some(cursor),
            _ => unreachable!("replacement job checked before retirement ownership transfer"),
        }
        match previous {
            Some(WasmTreeInner::V1(tree)) => result.tree_v1 = Some(tree.into_retirement()),
            Some(WasmTreeInner::V2(tree)) => result.tree_v2 = Some(tree.into_retirement()),
            None => {}
        }
        result
    }

    fn from_reachability(job: WasmTreeJob) -> Self {
        let (reachability, candidate_chunk_plan, prepared_candidate_v1, prepared_candidate_v2) =
            match job {
                WasmTreeJob::V1 {
                    cursor,
                    prepared_candidate,
                    ..
                } => (
                    ReachabilityRetirement::V1(cursor),
                    None,
                    prepared_candidate,
                    None,
                ),
                WasmTreeJob::V2 {
                    cursor,
                    prepared_candidate,
                    ..
                } => (
                    ReachabilityRetirement::V2(cursor),
                    None,
                    None,
                    prepared_candidate.map(RootRetirementV2::from_root),
                ),
                WasmTreeJob::CandidateChunkSort {
                    reachability,
                    cursor,
                    ..
                } => (reachability, Some(cursor), None, None),
                _ => unreachable!("reachability job checked before retirement ownership transfer"),
            };
        Self {
            reachability: Some(reachability),
            candidate_chunk_plan,
            prepared_candidate_v1,
            prepared_candidate_v2,
            reachability_origin: true,
            mutation_v1: None,
            mutation_v1_changed: None,
            mutation_v1_deleted: None,
            mutation_v2: None,
            mutation_origin: false,
            input: None,
            input_memory: None,
            builder_v1: None,
            builder_v2: None,
            tree_v1: None,
            tree_v2: None,
            completed: 0,
            done: false,
            last_progress: None,
        }
    }

    fn from_candidate_mutation(job: WasmTreeJob) -> Self {
        let (mutation_v1, mutation_v1_changed, mutation_v1_deleted, mutation_v2) = match job {
            WasmTreeJob::MutationV1 {
                cursor,
                changed,
                deleted,
                ..
            } => (cursor, changed, deleted, None),
            WasmTreeJob::MutationV2 { cursor, .. } => (None, None, None, Some(cursor)),
            WasmTreeJob::MutationNoop { .. } => (None, None, None, None),
            _ => unreachable!("candidate mutation checked before retirement transfer"),
        };
        Self {
            reachability: None,
            candidate_chunk_plan: None,
            prepared_candidate_v1: None,
            prepared_candidate_v2: None,
            reachability_origin: false,
            mutation_v1,
            mutation_v1_changed,
            mutation_v1_deleted,
            mutation_v2,
            mutation_origin: true,
            input: None,
            input_memory: None,
            builder_v1: None,
            builder_v2: None,
            tree_v1: None,
            tree_v2: None,
            completed: 0,
            done: false,
            last_progress: None,
        }
    }

    fn retire_one(&mut self) -> bool {
        if let Some(cursor) = &mut self.reachability {
            if cursor.retire_one() {
                return true;
            }
            self.reachability = None;
        }
        if let Some(cursor) = &mut self.candidate_chunk_plan {
            if cursor.retire_one() {
                return true;
            }
            self.candidate_chunk_plan = None;
        }
        if let Some(root) = &mut self.prepared_candidate_v1 {
            if root.retire_child() || root.retire_vault() || root.retire_device() {
                return true;
            }
            self.prepared_candidate_v1 = None;
        }
        if let Some(root) = &mut self.prepared_candidate_v2 {
            if root.retire_one() {
                return true;
            }
            self.prepared_candidate_v2 = None;
        }
        if let Some(cursor) = &mut self.mutation_v1 {
            if cursor.retire_one() {
                return true;
            }
            self.mutation_v1 = None;
        }
        if let Some(changed) = &mut self.mutation_v1_changed {
            if let Some(entry) = changed.pop() {
                drop(entry);
                return true;
            }
            self.mutation_v1_changed = None;
            return true;
        }
        if let Some(deleted) = &mut self.mutation_v1_deleted {
            if let Some(path) = deleted.pop() {
                drop(path);
                return true;
            }
            self.mutation_v1_deleted = None;
            return true;
        }
        if let Some(cursor) = &mut self.mutation_v2 {
            if cursor.retire_one() {
                return true;
            }
            self.mutation_v2 = None;
        }
        if let Some(input) = &mut self.input {
            if let Some(entry) = input.next() {
                self.input_memory
                    .as_mut()
                    .expect("replacement input meter must follow its iterator")
                    .retire_entry(&entry);
                return true;
            }
            // Drop the exhausted iterator and its original Vec allocation as
            // one explicit unit before reporting that owner as measured-empty.
            self.input = None;
            let mut memory = self
                .input_memory
                .take()
                .expect("replacement input meter must follow its iterator");
            memory.release_backing();
            debug_assert_eq!(memory, ReplacementInputMemory::default());
            return true;
        }
        if let Some(cursor) = &mut self.builder_v1 {
            if cursor.retire_one() {
                return true;
            }
            self.builder_v1 = None;
        }
        if let Some(cursor) = &mut self.builder_v2 {
            if cursor.retire_one() {
                return true;
            }
            self.builder_v2 = None;
        }
        if let Some(cursor) = &mut self.tree_v1 {
            if cursor.retire_one() {
                return true;
            }
            self.tree_v1 = None;
        }
        if let Some(cursor) = &mut self.tree_v2 {
            if cursor.retire_one() {
                return true;
            }
            self.tree_v2 = None;
        }
        false
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
struct TreeRetirementProgress {
    done: bool,
    units: usize,
    completed: usize,
}

/// Authority-bearing only as the direct return from the matching atomic
/// commit/abort method. Host code must still bind it to its own tree/ledger
/// capability; this plain object is not itself an accounting token.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct V2OutputSettlementReportV1 {
    schema: u8,
    scope: &'static str,
    outcome: &'static str,
    tree_version: u32,
    before: u64,
    reachable: u64,
    removed: u64,
    after: u64,
    bytes_removed: u64,
    committed_revision: u64,
    candidate_revision: u64,
    counters_valid: bool,
    node_payload_bytes: Option<u64>,
    range_endpoint_resident_requested_bytes: Option<u64>,
    resident_admission_bytes: Option<u64>,
}

impl V2OutputSettlementReportV1 {
    fn new(
        outcome: &'static str,
        stats: ChunkGcStats,
        memory: V2StableOutputMemoryV1,
        committed_revision: u64,
        candidate_revision: u64,
    ) -> Self {
        let exact = memory.counters_valid.then_some(());
        Self {
            schema: 1,
            scope: "v2-stable-tree-output",
            outcome,
            tree_version: TREE_V2,
            before: stats.before,
            reachable: stats.reachable,
            removed: stats.removed,
            after: stats.after,
            bytes_removed: stats.bytes_removed,
            committed_revision,
            candidate_revision,
            counters_valid: memory.counters_valid,
            node_payload_bytes: exact.map(|()| memory.node_payload_bytes),
            range_endpoint_resident_requested_bytes: exact
                .map(|()| memory.range_endpoint_resident_requested_bytes),
            resident_admission_bytes: exact.map(|()| memory.resident_admission_bytes),
        }
    }
}

#[derive(Debug, Serialize)]
struct TreeChunkMemoryReport {
    schema: u8,
    scope: &'static str,
    resident: ChunkStoreMemory,
    replacement: ChunkStoreMemory,
    retiring: ChunkStoreMemory,
    other_private_jobs_unmeasured: bool,
}

#[derive(Debug, Serialize)]
struct TreeMetadataMemoryReport {
    schema: u8,
    scope: &'static str,
    wrapper_ids: StringMemory,
    resident: TreeMetadataMemory,
    replacement: Option<TreeMetadataMemory>,
    retiring: Option<TreeMetadataMemory>,
    other_private_jobs_unmeasured: bool,
}

#[derive(Debug, Serialize)]
struct ReplacementInputMemoryReport {
    schema: u8,
    scope: &'static str,
    replacement: Option<ReplacementInputMemory>,
    retiring: Option<ReplacementInputMemory>,
    other_input_owners_unmeasured: bool,
}

#[derive(Debug, Serialize)]
struct ReplacementSortMemoryReport {
    schema: u8,
    scope: &'static str,
    replacement: Option<ReplacementSortMemory>,
    retiring: Option<ReplacementSortMemory>,
    other_sort_owners_unmeasured: bool,
}

#[derive(Debug, Serialize)]
struct ReplacementV1EntriesMemoryReport {
    schema: u8,
    scope: &'static str,
    replacement: Option<ReplacementV1EntriesMemory>,
    retiring: Option<ReplacementV1EntriesMemory>,
    other_entry_owners_unmeasured: bool,
}

#[derive(Debug, Serialize)]
struct ReplacementV1GraphMemoryReport {
    schema: u8,
    scope: &'static str,
    replacement: Option<ReplacementV1GraphMemory>,
    retiring: Option<ReplacementV1GraphMemory>,
    other_graph_owners_unmeasured: bool,
}

#[derive(Debug, Serialize)]
struct ReplacementV2PlanningMemoryReport {
    schema: u8,
    scope: &'static str,
    replacement: Option<ReplacementV2PlanningMemory>,
    retiring: Option<ReplacementV2PlanningMemory>,
    other_planning_owners_unmeasured: bool,
}

#[derive(Debug, Serialize)]
struct ReplacementV2RangesMemoryReport {
    schema: u8,
    scope: &'static str,
    replacement: Option<ReplacementV2RangesMemory>,
    retiring: Option<ReplacementV2RangesMemory>,
    other_range_owners_unmeasured: bool,
}

#[derive(Debug, Serialize)]
struct ReplacementV2PostSortMemoryReport {
    schema: u8,
    scope: &'static str,
    replacement: Option<ReplacementInputMemory>,
    retiring: Option<ReplacementInputMemory>,
    other_post_sort_owners_unmeasured: bool,
}

#[derive(Clone, Copy)]
enum RootExportScope {
    Candidate,
    Committed,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RootExportV2Phase {
    Plan,
    Planned,
    Build,
    Built,
    Sealed,
}

struct RootExportV2 {
    phase: RootExportV2Phase,
    max_length: usize,
    arena_limit: usize,
    hash: FileHash,
    bytes: Vec<u8>,
    offset: usize,
    completed: u64,
    processed: u64,
}

impl RootExportV2 {
    fn with_arena_limit(arena_limit: usize) -> Result<Self, ChunkError> {
        if arena_limit == 0 || arena_limit > u32::MAX as usize {
            return Err(tree_job_error("invalid root export arena limit"));
        }
        Ok(Self {
            phase: RootExportV2Phase::Plan,
            max_length: 0,
            arena_limit,
            hash: [0; 32],
            bytes: Vec::new(),
            offset: 0,
            completed: 0,
            processed: 0,
        })
    }

    fn step(&mut self, root: &RootNodeV2) -> Result<WasmRootExportProgress, ChunkError> {
        let units: u64 = match self.phase {
            RootExportV2Phase::Plan => {
                self.max_length = root.serialized_length()?;
                if self.max_length > self.arena_limit {
                    return Err(tree_job_error("root export arena exceeds the caller limit"));
                }
                // One descriptor, bounded by the existing 16 KiB codec cap.
                self.hash = root.hash();
                self.phase = RootExportV2Phase::Planned;
                1
            }
            RootExportV2Phase::Build => {
                let bytes = root.serialize()?;
                if bytes.len() != self.max_length || bytes.capacity() > self.max_length {
                    return Err(tree_job_error("v2 root export exceeded admission"));
                }
                self.bytes = bytes;
                self.phase = RootExportV2Phase::Built;
                1
            }
            RootExportV2Phase::Planned | RootExportV2Phase::Built => 0,
            RootExportV2Phase::Sealed => {
                return Err(tree_job_error("root export is sealed for page reads"))
            }
        };
        let bytes = if units == 0 {
            0
        } else {
            // The bounded v2 codec may validate/hash/encode its <=16 KiB
            // descriptor more than once. Charge one full 64 KiB scheduling
            // quantum instead of claiming exact native memory traffic.
            ROOT_EXPORT_PART_BYTES as u64
        };
        self.completed = self
            .completed
            .checked_add(units)
            .ok_or_else(|| tree_job_error("root export completed-unit overflow"))?;
        self.processed = self
            .processed
            .checked_add(bytes)
            .ok_or_else(|| tree_job_error("root export processed-byte overflow"))?;
        Ok(WasmRootExportProgress {
            done: true,
            units: units as f64,
            bytes: bytes as f64,
            completed: self.completed as f64,
            processed: self.processed as f64,
        })
    }
}

enum WasmTreeJob {
    Retirement {
        token: u32,
        cursor: TreeRetirement,
    },
    V1 {
        token: u32,
        purpose: WasmTreeJobPurpose,
        cursor: ReachabilityCursor,
        next_root_child: usize,
        prepared_candidate: Option<OwnedRootV1>,
    },
    V2 {
        token: u32,
        purpose: WasmTreeJobPurpose,
        cursor: V2ReachabilityCursor,
        root_seeded: bool,
        prepared_candidate: Option<RootNodeV2>,
    },
    CandidateChunkSort {
        token: u32,
        reachability: ReachabilityRetirement,
        cursor: CandidateChunkSortCursor,
    },
    MutationV1 {
        token: u32,
        cursor: Option<CandidateMutationV1>,
        changed: Option<Vec<FileEntry>>,
        deleted: Option<Vec<String>>,
        output_plan: CandidateMutationOutputMemoryPlanV1,
        output_resumed: bool,
    },
    MutationV2 {
        token: u32,
        cursor: V2CandidateMutation,
    },
    MutationNoop {
        token: u32,
    },
    ReplacementRebuild {
        token: u32,
        version: u32,
        expected_entries: usize,
        expected_bytes: usize,
        received_bytes: usize,
        entries: Vec<FileEntry>,
        input_memory: ReplacementInputMemory,
    },
    ReplacementBuildV1 {
        token: u32,
        cursor: ReplacementRebuildV1,
    },
    ReplacementBuildV2 {
        token: u32,
        cursor: ReplacementRebuildV2,
    },
    ChunkExport {
        token: u32,
        hash: FileHash,
        length: u32,
        offset: u32,
    },
    RootExportV1 {
        token: u32,
        scope: RootExportScope,
        cursor: RootExportV1,
    },
    RootExportV2 {
        token: u32,
        scope: RootExportScope,
        cursor: RootExportV2,
    },
}

impl WasmTreeJob {
    fn token(&self) -> u32 {
        match self {
            Self::V1 { token, .. }
            | Self::V2 { token, .. }
            | Self::CandidateChunkSort { token, .. }
            | Self::MutationV1 { token, .. }
            | Self::MutationV2 { token, .. }
            | Self::MutationNoop { token }
            | Self::ReplacementRebuild { token, .. }
            | Self::ReplacementBuildV1 { token, .. }
            | Self::ReplacementBuildV2 { token, .. }
            | Self::ChunkExport { token, .. }
            | Self::RootExportV1 { token, .. }
            | Self::RootExportV2 { token, .. } => *token,
            Self::Retirement { token, .. } => *token,
        }
    }

    fn purpose(&self) -> WasmTreeJobPurpose {
        match self {
            Self::V1 { purpose, .. } | Self::V2 { purpose, .. } => *purpose,
            Self::CandidateChunkSort { .. } => WasmTreeJobPurpose::CandidateChunks,
            Self::MutationV1 { .. } | Self::MutationV2 { .. } | Self::MutationNoop { .. } => {
                WasmTreeJobPurpose::CandidateMutation
            }
            Self::ReplacementRebuild { .. }
            | Self::ReplacementBuildV1 { .. }
            | Self::ReplacementBuildV2 { .. } => WasmTreeJobPurpose::ReplacementRebuild,
            Self::ChunkExport { .. } => WasmTreeJobPurpose::TreeChunkExport,
            Self::RootExportV1 { .. } | Self::RootExportV2 { .. } => WasmTreeJobPurpose::RootExport,
            Self::Retirement { .. } => WasmTreeJobPurpose::Retirement,
        }
    }

    fn ready(&self) -> bool {
        match self {
            Self::Retirement { .. } => false,
            Self::V1 { cursor, .. } => cursor.progress(0).done,
            Self::V2 { cursor, .. } => cursor.progress(0).done,
            Self::CandidateChunkSort { cursor, .. } => {
                cursor.phase == CandidateChunkSortPhase::Ready
            }
            Self::MutationV1 { cursor, .. } => {
                cursor.as_ref().is_some_and(|cursor| cursor.progress().done)
            }
            Self::MutationV2 { cursor, .. } => cursor.progress(0).done,
            Self::MutationNoop { .. } => true,
            Self::ReplacementRebuild {
                expected_entries,
                expected_bytes,
                received_bytes,
                entries,
                ..
            } => entries.len() == *expected_entries && received_bytes == expected_bytes,
            Self::ReplacementBuildV1 { cursor, .. } => cursor.is_ready(),
            Self::ReplacementBuildV2 { cursor, .. } => cursor.is_ready(),
            Self::ChunkExport { length, offset, .. } => offset == length,
            Self::RootExportV1 { cursor, .. } => cursor.at_eof(),
            Self::RootExportV2 { cursor, .. } => {
                cursor.phase == RootExportV2Phase::Sealed && cursor.offset == cursor.bytes.len()
            }
        }
    }

    fn root_export_sealed(&self) -> bool {
        match self {
            Self::RootExportV1 { cursor, .. } => cursor.sealed(),
            Self::RootExportV2 { cursor, .. } => cursor.phase == RootExportV2Phase::Sealed,
            _ => false,
        }
    }
}

#[derive(Serialize)]
struct WasmTreeJobProgress {
    done: bool,
    units: f64,
    completed: f64,
    remaining: f64,
    reachable: f64,
}

#[derive(Serialize)]
struct WasmRootExportProgress {
    done: bool,
    units: f64,
    bytes: f64,
    completed: f64,
    processed: f64,
}

#[derive(Serialize)]
struct WasmCandidateChunkPlan {
    all: Vec<String>,
    fresh: Vec<String>,
}

const CANDIDATE_CHUNK_PLAN_HASH_BYTES: usize = std::mem::size_of::<FileHash>();
const CANDIDATE_CHUNK_PLAN_RADIX_PASSES: usize = CANDIDATE_CHUNK_PLAN_HASH_BYTES;
const CANDIDATE_CHUNK_PLAN_PAGE_MAX_HASHES: u32 = 256;
const CANDIDATE_CHUNK_SORT_MAX_STEP_UNITS: u32 = 4_096;
const CANDIDATE_CHUNK_PLAN_WORK_UNITS_PER_HASH: u64 =
    2 + 2 * CANDIDATE_CHUNK_PLAN_RADIX_PASSES as u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateChunkSortPhase {
    Collect,
    InitializeScratch,
    CountByte,
    ScatterByte,
    Ready,
}

impl CandidateChunkSortPhase {
    fn label(self) -> &'static str {
        match self {
            Self::Collect => "collect",
            Self::InitializeScratch => "initialize-scratch",
            Self::CountByte => "count-byte",
            Self::ScatterByte => "scatter-byte",
            Self::Ready => "ready",
        }
    }
}

/// Same-token continuation of one validated candidate reachability traversal.
/// It owns only explicitly admitted contiguous hash workspaces; the drained
/// HashSet backing allocation and allocator metadata are intentionally outside
/// that component admission scope.
struct CandidateChunkSortCursor {
    input: Option<std::collections::hash_set::IntoIter<FileHash>>,
    source: Vec<FileHash>,
    scratch: Vec<FileHash>,
    phase: CandidateChunkSortPhase,
    byte: usize,
    index: usize,
    counts: [usize; 256],
    offsets: [usize; 256],
    hash_count: usize,
    completed: u64,
    total: u64,
    page_offset: usize,
    retirement_phase: u8,
}

impl CandidateChunkSortCursor {
    fn try_new(hash_count: usize) -> Result<Self, ChunkError> {
        let total = (hash_count as u64)
            .checked_mul(CANDIDATE_CHUNK_PLAN_WORK_UNITS_PER_HASH)
            .filter(|value| *value <= MAX_SAFE_COMMITTED_REVISION)
            .ok_or_else(|| tree_job_error("candidate chunk sort progress space exhausted"))?;
        let mut source = Vec::new();
        source
            .try_reserve_exact(hash_count)
            .map_err(|_| tree_job_error("candidate chunk sort source allocation failed"))?;
        let mut scratch = Vec::new();
        scratch
            .try_reserve_exact(hash_count)
            .map_err(|_| tree_job_error("candidate chunk sort scratch allocation failed"))?;
        Ok(Self {
            input: None,
            source,
            scratch,
            phase: if hash_count == 0 {
                CandidateChunkSortPhase::Ready
            } else {
                CandidateChunkSortPhase::Collect
            },
            byte: CANDIDATE_CHUNK_PLAN_RADIX_PASSES.saturating_sub(1),
            index: 0,
            counts: [0; 256],
            offsets: [0; 256],
            hash_count,
            completed: 0,
            total,
            page_offset: 0,
            retirement_phase: 0,
        })
    }

    fn install_input(&mut self, reachable: std::collections::HashSet<FileHash>) {
        assert_eq!(reachable.len(), self.hash_count);
        if self.hash_count == 0 {
            drop(reachable);
        } else {
            self.input = Some(reachable.into_iter());
        }
    }

    fn step(&mut self, max_units: usize) -> Result<CandidateChunkSortProgressV1, ChunkError> {
        if max_units == 0 || max_units > CANDIDATE_CHUNK_SORT_MAX_STEP_UNITS as usize {
            return Err(tree_job_error("invalid candidate chunk sort step budget"));
        }
        if self.phase == CandidateChunkSortPhase::Ready {
            return Ok(self.progress(0));
        }
        let start = self.completed;
        while self.completed - start < max_units as u64 {
            match self.phase {
                CandidateChunkSortPhase::Collect => {
                    let hash = self
                        .input
                        .as_mut()
                        .and_then(Iterator::next)
                        .ok_or_else(|| tree_job_error("candidate chunk sort input ended early"))?;
                    self.source.push(hash);
                    self.completed += 1;
                    if self.source.len() == self.hash_count {
                        self.input = None;
                        self.phase = CandidateChunkSortPhase::InitializeScratch;
                    }
                }
                CandidateChunkSortPhase::InitializeScratch => {
                    self.scratch.push([0; CANDIDATE_CHUNK_PLAN_HASH_BYTES]);
                    self.completed += 1;
                    if self.scratch.len() == self.hash_count {
                        self.index = 0;
                        self.phase = CandidateChunkSortPhase::CountByte;
                    }
                }
                CandidateChunkSortPhase::CountByte => {
                    let bucket = self.source[self.index][self.byte] as usize;
                    self.counts[bucket] += 1;
                    self.index += 1;
                    self.completed += 1;
                    if self.index == self.hash_count {
                        let mut next = 0usize;
                        for (count, offset) in self.counts.iter().zip(self.offsets.iter_mut()) {
                            *offset = next;
                            next = next.checked_add(*count).ok_or_else(|| {
                                tree_job_error("candidate chunk radix offset overflow")
                            })?;
                        }
                        if next != self.hash_count {
                            return Err(tree_job_error("candidate chunk radix count mismatch"));
                        }
                        self.index = 0;
                        self.phase = CandidateChunkSortPhase::ScatterByte;
                    }
                }
                CandidateChunkSortPhase::ScatterByte => {
                    let hash = self.source[self.index];
                    let bucket = hash[self.byte] as usize;
                    let target = self.offsets[bucket];
                    self.scratch[target] = hash;
                    self.offsets[bucket] += 1;
                    self.index += 1;
                    self.completed += 1;
                    if self.index == self.hash_count {
                        std::mem::swap(&mut self.source, &mut self.scratch);
                        if self.byte == 0 {
                            self.phase = CandidateChunkSortPhase::Ready;
                        } else {
                            self.byte -= 1;
                            self.index = 0;
                            self.counts.fill(0);
                            self.offsets.fill(0);
                            self.phase = CandidateChunkSortPhase::CountByte;
                        }
                    }
                }
                CandidateChunkSortPhase::Ready => break,
            }
        }
        if self.completed > self.total {
            return Err(tree_job_error(
                "candidate chunk sort progress exceeded its plan",
            ));
        }
        Ok(self.progress((self.completed - start) as usize))
    }

    fn progress(&self, units: usize) -> CandidateChunkSortProgressV1 {
        CandidateChunkSortProgressV1 {
            schema: 1,
            scope: "candidate-chunk-plan-sort",
            done: self.phase == CandidateChunkSortPhase::Ready,
            units,
            completed: self.completed,
            remaining: self.total - self.completed,
            all_count: self.hash_count,
            phase: self.phase.label(),
        }
    }

    fn retire_one(&mut self) -> bool {
        loop {
            match self.retirement_phase {
                0 => {
                    if let Some(input) = &mut self.input {
                        if input.next().is_some() {
                            return true;
                        }
                        self.input = None;
                        return true;
                    }
                    self.retirement_phase = 1;
                }
                1 => {
                    if self.source.pop().is_some() {
                        return true;
                    }
                    self.retirement_phase = 2;
                    if self.source.capacity() > 0 {
                        self.source = Vec::new();
                        return true;
                    }
                }
                2 => {
                    if self.scratch.pop().is_some() {
                        return true;
                    }
                    self.retirement_phase = 3;
                    if self.scratch.capacity() > 0 {
                        self.scratch = Vec::new();
                        return true;
                    }
                }
                _ => return false,
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CandidateChunkSortMemoryPlanV1 {
    schema: u8,
    scope: &'static str,
    hash_count: usize,
    hash_size_bytes: usize,
    source_hashes_requested_bytes: u64,
    scratch_hashes_requested_bytes: u64,
    peak_admission_bytes: u64,
    reachable_set_unmeasured: bool,
    page_output_unmeasured: bool,
    sort_strategy: &'static str,
    page_max_hashes: u32,
}

impl CandidateChunkSortMemoryPlanV1 {
    fn for_hashes(hash_count: usize) -> Result<Self, ChunkError> {
        let bytes = (hash_count as u64)
            .checked_mul(CANDIDATE_CHUNK_PLAN_HASH_BYTES as u64)
            .ok_or_else(|| tree_job_error("candidate chunk sort byte count overflow"))?;
        let peak = bytes
            .checked_mul(2)
            .filter(|value| *value <= MAX_SAFE_COMMITTED_REVISION)
            .ok_or_else(|| tree_job_error("candidate chunk sort workspace byte count overflow"))?;
        Ok(Self {
            schema: 1,
            scope: "candidate-chunk-plan-sort-workspace",
            hash_count,
            hash_size_bytes: CANDIDATE_CHUNK_PLAN_HASH_BYTES,
            source_hashes_requested_bytes: bytes,
            scratch_hashes_requested_bytes: bytes,
            peak_admission_bytes: peak,
            reachable_set_unmeasured: true,
            page_output_unmeasured: true,
            sort_strategy: "stable-lsd-radix-v1",
            page_max_hashes: CANDIDATE_CHUNK_PLAN_PAGE_MAX_HASHES,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CandidateChunkSortProgressV1 {
    schema: u8,
    scope: &'static str,
    done: bool,
    units: usize,
    completed: u64,
    remaining: u64,
    all_count: usize,
    phase: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CandidateChunkPlanInfoV1 {
    schema: u8,
    scope: &'static str,
    all_count: usize,
    page_max_hashes: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CandidateChunkPlanPageV1 {
    schema: u8,
    scope: &'static str,
    offset: usize,
    next_offset: usize,
    done: bool,
    all: Vec<String>,
    fresh: Vec<String>,
}

#[derive(Debug, Serialize)]
struct WasmTreeChunkExport {
    token: u32,
    length: u32,
}

#[derive(Debug, Serialize)]
struct WasmRootExportWorkset {
    max_length: u32,
    offset_bytes: u32,
}

#[derive(Debug, Serialize)]
struct WasmRootExportInfo {
    length: u32,
    version: u32,
    hash: String,
}

/// Only the two indirect-sort index allocation requests. Input rows, nested
/// paths, IDs, other builder buffers and allocator metadata are not included.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReplacementRebuildSortMemoryPlanV1 {
    schema: u8,
    scope: &'static str,
    entry_count: usize,
    index_size_bytes: usize,
    source_index_requested_bytes: u64,
    target_index_requested_bytes: u64,
    peak_admission_bytes: u64,
}

impl ReplacementRebuildSortMemoryPlanV1 {
    fn for_entries(entry_count: usize) -> Result<Self, ChunkError> {
        if entry_count > MAX_REPLACEMENT_REBUILD_ENTRIES {
            return Err(tree_job_error(
                "replacement sort entry count exceeds its bound",
            ));
        }
        let index_size_bytes = std::mem::size_of::<usize>();
        let index_bytes = (entry_count as u64)
            .checked_mul(index_size_bytes as u64)
            .ok_or_else(|| tree_job_error("replacement sort index byte count overflow"))?;
        let peak_admission_bytes = index_bytes
            .checked_mul(2)
            .filter(|bytes| *bytes <= (1u64 << 53) - 1)
            .ok_or_else(|| tree_job_error("replacement sort workspace byte count overflow"))?;
        Ok(Self {
            schema: 1,
            scope: "v2-replacement-sort-indices",
            entry_count,
            index_size_bytes,
            source_index_requested_bytes: index_bytes,
            target_index_requested_bytes: index_bytes,
            peak_admission_bytes,
        })
    }
}

const MAX_TREE_JOB_STEP_UNITS: u32 = 256;
const MAX_CANDIDATE_MUTATION_ROWS: usize = 65_536;
const MAX_CANDIDATE_MUTATION_JSON_BYTES: usize = 32 * 1024 * 1024;
const MAX_REPLACEMENT_REBUILD_ENTRIES: usize = 65_536;
const MAX_REPLACEMENT_REBUILD_FEED_ENTRIES: usize = 256;
const MAX_REPLACEMENT_REBUILD_FEED_BYTES: usize = 256 * 1024;
const MAX_REPLACEMENT_REBUILD_JSON_BYTES: usize = 8 * 1024 * 1024;

fn safe_integer_u64(value: f64) -> Option<u64> {
    const MAX_SAFE_INTEGER: f64 = ((1u64 << 53) - 1) as f64;
    (value.is_finite() && value >= 0.0 && value <= MAX_SAFE_INTEGER && value.fract() == 0.0)
        .then_some(value as u64)
}

fn tree_job_error(message: impl Into<String>) -> ChunkError {
    ChunkError::Deserialize(message.into())
}

fn candidate_v1_output_memory_plan(
    tree: &TransactionalTree,
    changed: &[FileEntry],
    deleted: &[String],
) -> Result<CandidateMutationOutputMemoryPlanV1, ChunkError> {
    const MAX_SAFE_INTEGER: u64 = (1u64 << 53) - 1;
    let memory = tree.chunk_memory();
    if !memory.counters_valid {
        return Err(tree_job_error(
            "Tree v1 resident chunk counters are invalid",
        ));
    }
    let path_bytes = changed
        .iter()
        .map(|entry| entry.path.len())
        .chain(deleted.iter().map(String::len))
        .try_fold(0u64, |total, bytes| {
            u64::try_from(bytes)
                .ok()
                .and_then(|bytes| total.checked_add(bytes))
        })
        .ok_or_else(|| tree_job_error("Tree v1 candidate path byte count overflow"))?;
    let rows = u64::try_from(changed.len().saturating_add(deleted.len()))
        .map_err(|_| tree_job_error("Tree v1 candidate row count overflow"))?;
    let root = tree
        .candidate_root()
        .ok_or_else(|| tree_job_error("no active candidate"))?;
    let mut current_root_request = root
        .vault_id
        .len()
        .checked_add(root.device_id.len())
        .and_then(|bytes| {
            root.children
                .len()
                .checked_mul(std::mem::size_of::<(String, FileHash)>())
                .and_then(|backing| bytes.checked_add(backing))
        })
        .ok_or_else(|| tree_job_error("Tree v1 candidate root byte count overflow"))?;
    for child in &root.children {
        current_root_request = current_root_request
            .checked_add(child.0.len())
            .ok_or_else(|| tree_job_error("Tree v1 candidate root byte count overflow"))?;
    }
    let current_root_request = u64::try_from(current_root_request)
        .map_err(|_| tree_job_error("Tree v1 candidate root byte count overflow"))?;
    // Rebuilding affected V1 prefixes cannot retain more old canonical node
    // payload than the complete resident store. Additional source paths and a
    // fixed per-row FlatBuffer/topology allowance cover inserts and boundary
    // reshaping. The Ready report checks actual private payload against this
    // witness before publication.
    let node_payload_bytes = memory
        .payload_bytes
        .checked_mul(2)
        .and_then(|bytes| {
            path_bytes
                .checked_mul(4)
                .and_then(|extra| bytes.checked_add(extra))
        })
        .and_then(|bytes| {
            rows.checked_mul(512)
                .and_then(|extra| bytes.checked_add(extra))
        })
        .and_then(|bytes| bytes.checked_add(128 * 1024))
        .filter(|bytes| *bytes <= MAX_SAFE_INTEGER)
        .ok_or_else(|| tree_job_error("Tree v1 candidate output byte count overflow"))?;
    // The exact admitted clone of the old root remains live while assembly
    // grows the replacement children Vec. The resident bound covers that new
    // root; the peak additionally covers the old cloned root allocation.
    let root_resident_requested_bytes = current_root_request
        .checked_mul(2)
        .and_then(|bytes| {
            path_bytes
                .checked_mul(4)
                .and_then(|extra| bytes.checked_add(extra))
        })
        .and_then(|bytes| {
            rows.checked_mul(256)
                .and_then(|extra| bytes.checked_add(extra))
        })
        .and_then(|bytes| bytes.checked_add(128 * 1024))
        .filter(|bytes| *bytes <= MAX_SAFE_INTEGER)
        .ok_or_else(|| tree_job_error("Tree v1 candidate root output byte count overflow"))?;
    let root_peak_requested_bytes = current_root_request
        .checked_add(root_resident_requested_bytes)
        .filter(|bytes| *bytes <= MAX_SAFE_INTEGER)
        .ok_or_else(|| tree_job_error("Tree v1 candidate root peak byte count overflow"))?;
    let peak_admission_bytes = node_payload_bytes
        .checked_add(root_peak_requested_bytes)
        .filter(|bytes| *bytes <= MAX_SAFE_INTEGER)
        .ok_or_else(|| tree_job_error("Tree v1 candidate peak byte count overflow"))?;
    let resident_admission_bytes = node_payload_bytes
        .checked_add(root_resident_requested_bytes)
        .filter(|bytes| *bytes <= MAX_SAFE_INTEGER)
        .ok_or_else(|| tree_job_error("Tree v1 candidate resident byte count overflow"))?;
    Ok(CandidateMutationOutputMemoryPlanV1 {
        node_payload_bytes,
        range_endpoint_peak_requested_bytes: root_peak_requested_bytes,
        range_endpoint_resident_requested_bytes: root_resident_requested_bytes,
        peak_admission_bytes,
        resident_admission_bytes,
    })
}

fn build_replacement_tree(
    version: u32,
    vault_id: &str,
    device_id: &str,
    entries: Vec<FileEntry>,
) -> Result<WasmTreeInner, ChunkError> {
    let mut replacement = match version {
        TREE_V1 => WasmTreeInner::V1(TransactionalTree::new(vault_id, device_id)),
        TREE_V2 => WasmTreeInner::V2(TransactionalTreeV2::new(vault_id, device_id)),
        other => return Err(tree_job_error(format!("unsupported tree version {other}"))),
    };
    match &mut replacement {
        WasmTreeInner::V1(tree) => run_local(tree.rebuild(entries))?,
        WasmTreeInner::V2(tree) => run_local(tree.rebuild(entries))?,
    }
    Ok(replacement)
}

fn step_tree_job_inner(
    job: &mut WasmTreeJob,
    inner: &WasmTreeInner,
    max_units: usize,
) -> Result<WasmTreeJobProgress, ChunkError> {
    match (job, inner) {
        (
            WasmTreeJob::MutationV1 {
                cursor,
                changed,
                deleted,
                output_resumed,
                ..
            },
            WasmTreeInner::V1(tree),
        ) => {
            // The generic ABI predates output admission. Preserve it by
            // acknowledging the conservative native witness internally;
            // opt-in hosts use the dedicated family and see PlanReady.
            if !*output_resumed {
                let root = tree.prepare_candidate_mutation_root()?;
                let changed = changed
                    .take()
                    .ok_or_else(|| tree_job_error("candidate mutation lost its update input"))?;
                let deleted = deleted
                    .take()
                    .ok_or_else(|| tree_job_error("candidate mutation lost its delete input"))?;
                *cursor = Some(CandidateMutationV1::new(root, changed, deleted));
                *output_resumed = true;
            }
            let progress = tree.step_candidate_mutation(
                cursor
                    .as_mut()
                    .ok_or_else(|| tree_job_error("candidate mutation output was not resumed"))?,
                max_units,
            )?;
            Ok(WasmTreeJobProgress {
                done: progress.done,
                units: progress.units as f64,
                completed: progress.completed as f64,
                remaining: if progress.done { 0.0 } else { 1.0 },
                reachable: progress.staged_chunks as f64,
            })
        }
        (WasmTreeJob::MutationV2 { cursor, .. }, WasmTreeInner::V2(tree)) => {
            let mut progress = tree.step_candidate_mutation(cursor, max_units)?;
            // The legacy generic step family predates host-side candidate
            // output admission. Preserve that ABI by crossing the new native
            // barrier internally; opt-in hosts use the dedicated versioned
            // step/plan/resume family below and receive the actual phase.
            if progress.phase == "plan ready" {
                let plan = cursor.output_memory_plan_v1()?;
                cursor.resume_after_output_memory_plan_v1(
                    plan.node_payload_bytes,
                    plan.range_endpoint_peak_requested_bytes,
                    plan.range_endpoint_resident_requested_bytes,
                )?;
                if progress.units == 0 {
                    progress = tree.step_candidate_mutation(cursor, max_units)?;
                }
            }
            Ok(WasmTreeJobProgress {
                done: progress.done,
                units: progress.units as f64,
                completed: progress.completed as f64,
                remaining: if progress.done { 0.0 } else { 1.0 },
                reachable: progress.staged_chunks as f64,
            })
        }
        (WasmTreeJob::MutationNoop { .. }, _) => Ok(WasmTreeJobProgress {
            done: true,
            units: 0.0,
            completed: 0.0,
            remaining: 0.0,
            reachable: 0.0,
        }),
        (
            WasmTreeJob::V1 {
                purpose,
                cursor,
                next_root_child,
                ..
            },
            WasmTreeInner::V1(tree),
        ) => {
            let root = match purpose {
                WasmTreeJobPurpose::BeginCandidate => tree
                    .committed_root()
                    .ok_or_else(|| tree_job_error("no committed root"))?,
                WasmTreeJobPurpose::CandidateChunks => tree
                    .candidate_root()
                    .ok_or_else(|| tree_job_error("no active candidate"))?,
                WasmTreeJobPurpose::CandidateMutation
                | WasmTreeJobPurpose::ReplacementRebuild
                | WasmTreeJobPurpose::Retirement
                | WasmTreeJobPurpose::TreeChunkExport
                | WasmTreeJobPurpose::RootExport => {
                    return Err(tree_job_error("tree job purpose changed"));
                }
            };
            let mut units = 0usize;
            while units < max_units && *next_root_child < root.children.len() {
                cursor.seed_root_child(root.children[*next_root_child].1)?;
                *next_root_child += 1;
                units += 1;
            }
            if *next_root_child == root.children.len() {
                cursor.finish_seeding();
            }
            if units < max_units && !cursor.progress(0).done {
                let progress = tree.step_candidate_validation(cursor, max_units - units)?;
                units += progress.units;
            }
            let progress = cursor.progress(units);
            Ok(WasmTreeJobProgress {
                done: *next_root_child == root.children.len() && progress.done,
                units: units as f64,
                completed: progress.completed as f64,
                remaining: (progress.remaining + root.children.len() - *next_root_child) as f64,
                reachable: progress.reachable as f64,
            })
        }
        (
            WasmTreeJob::V2 {
                purpose,
                cursor,
                root_seeded,
                ..
            },
            WasmTreeInner::V2(tree),
        ) => {
            let root = match purpose {
                WasmTreeJobPurpose::BeginCandidate => tree
                    .committed_root()
                    .ok_or_else(|| tree_job_error("no committed root"))?,
                WasmTreeJobPurpose::CandidateChunks => tree
                    .candidate_root()
                    .ok_or_else(|| tree_job_error("no active candidate"))?,
                WasmTreeJobPurpose::CandidateMutation
                | WasmTreeJobPurpose::ReplacementRebuild
                | WasmTreeJobPurpose::Retirement
                | WasmTreeJobPurpose::TreeChunkExport
                | WasmTreeJobPurpose::RootExport => {
                    return Err(tree_job_error("tree job purpose changed"));
                }
            };
            let mut units = 0usize;
            if !*root_seeded && units < max_units {
                if let Some(range) = root.tree.child.clone() {
                    cursor.seed_root_child(range)?;
                    units += 1;
                }
                *root_seeded = true;
                cursor.finish_seeding();
            }
            if units < max_units && !cursor.progress(0).done {
                let progress =
                    run_local(tree.step_candidate_validation(cursor, max_units - units))?;
                units += progress.units;
            }
            let progress = cursor.progress(units);
            Ok(WasmTreeJobProgress {
                done: *root_seeded && progress.done,
                units: units as f64,
                completed: progress.completed as f64,
                remaining: (progress.remaining + usize::from(!*root_seeded)) as f64,
                reachable: progress.reachable as f64,
            })
        }
        _ => Err(tree_job_error("tree job format changed")),
    }
}

fn step_root_export_job_inner(
    job: &mut WasmTreeJob,
    inner: &WasmTreeInner,
    max_units: usize,
    max_bytes: usize,
) -> Result<WasmRootExportProgress, ChunkError> {
    match (job, inner) {
        (WasmTreeJob::RootExportV1 { scope, cursor, .. }, WasmTreeInner::V1(tree)) => {
            let root = match scope {
                RootExportScope::Candidate => tree.candidate_root(),
                RootExportScope::Committed => tree.committed_root(),
            }
            .ok_or_else(|| tree_job_error("root export source is missing"))?;
            let progress = cursor.step(root, max_units, max_bytes)?;
            Ok(WasmRootExportProgress {
                done: progress.done,
                units: progress.units as f64,
                bytes: progress.bytes as f64,
                completed: progress.completed as f64,
                processed: progress.processed as f64,
            })
        }
        (WasmTreeJob::RootExportV2 { scope, cursor, .. }, WasmTreeInner::V2(tree)) => {
            let root = match scope {
                RootExportScope::Candidate => tree.candidate_root(),
                RootExportScope::Committed => tree.committed_root(),
            }
            .ok_or_else(|| tree_job_error("root export source is missing"))?;
            cursor.step(root)
        }
        _ => Err(tree_job_error("root export job format changed")),
    }
}

// These native helpers are shared by the bindings and unit tests. Keeping
// errors as ChunkError here also permits native tests without JS host imports.
impl WasmTree {
    fn ensure_tree_job_idle_native(&self) -> Result<(), ChunkError> {
        if self.tree_job.is_some() {
            return Err(tree_job_error("WASM tree has an active job"));
        }
        Ok(())
    }

    fn next_committed_revision_native(&self) -> Result<u64, ChunkError> {
        self.committed_revision
            .checked_add(1)
            .filter(|next| *next <= MAX_SAFE_COMMITTED_REVISION)
            .ok_or_else(|| tree_job_error("WASM committed revision space exhausted"))
    }

    fn mutate_committed_native<T>(
        &mut self,
        mutate: impl FnOnce(&mut WasmTreeInner) -> Result<T, ChunkError>,
    ) -> Result<T, ChunkError> {
        // Reserve the next exact JS witness before the closure can touch live
        // committed state. Exhaustion therefore never exposes a new root under
        // an old or wrapped revision.
        let next_revision = self.next_committed_revision_native()?;
        // Every successful caller replaces committed state and clears any
        // pre-existing candidate. Reserve BOTH witnesses before touching it.
        let next_candidate = self.next_candidate_revision_native(self.has_candidate())?;
        let result = mutate(&mut self.inner)?;
        self.committed_revision = next_revision;
        self.candidate_revision = next_candidate;
        Ok(result)
    }

    fn next_candidate_revision_native(&self, changes: bool) -> Result<u64, ChunkError> {
        if !changes {
            return Ok(self.candidate_revision);
        }
        self.candidate_revision
            .checked_add(1)
            .filter(|next| *next <= MAX_SAFE_CANDIDATE_REVISION)
            .ok_or_else(|| tree_job_error("WASM candidate revision space exhausted"))
    }

    fn mutate_candidate_native<T>(
        &mut self,
        changes: bool,
        mutate: impl FnOnce(&mut WasmTreeInner, &mut Option<WasmTreeJob>) -> Result<T, ChunkError>,
    ) -> Result<T, ChunkError> {
        // In particular, a ready job stays owned on revision exhaustion: no
        // cursor is consumed and the host can still cancel the matching token.
        let next_revision = self.next_candidate_revision_native(changes)?;
        let result = mutate(&mut self.inner, &mut self.tree_job)?;
        self.candidate_revision = next_revision;
        Ok(result)
    }

    fn commit_candidate_output_settlement_v1_native(
        &mut self,
    ) -> Result<V2OutputSettlementReportV1, ChunkError> {
        self.ensure_tree_job_idle_native()?;
        let (stats, memory) = self.mutate_committed_native(|inner| match inner {
            WasmTreeInner::V1(_) => Err(tree_job_error(
                "V2 output settlement is unavailable for Tree v1",
            )),
            WasmTreeInner::V2(tree) => run_local(tree.commit_candidate_output_settlement_v1()),
        })?;
        Ok(V2OutputSettlementReportV1::new(
            "commit",
            stats,
            memory,
            self.committed_revision,
            self.candidate_revision,
        ))
    }

    fn abort_candidate_output_settlement_v1_native(
        &mut self,
    ) -> Result<V2OutputSettlementReportV1, ChunkError> {
        self.ensure_tree_job_idle_native()?;
        let (stats, memory) = self.mutate_candidate_native(true, |inner, _| match inner {
            WasmTreeInner::V1(_) => Err(tree_job_error(
                "V2 output settlement is unavailable for Tree v1",
            )),
            WasmTreeInner::V2(tree) => run_local(tree.abort_candidate_output_settlement_v1()),
        })?;
        Ok(V2OutputSettlementReportV1::new(
            "abort",
            stats,
            memory,
            self.committed_revision,
            self.candidate_revision,
        ))
    }

    fn ready_candidate_job_native(
        &self,
        token: u32,
        purpose: WasmTreeJobPurpose,
    ) -> Result<(), ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        if job.purpose() != purpose {
            return Err(tree_job_error("WASM tree job finish kind mismatch"));
        }
        if !job.ready() {
            return Err(tree_job_error("WASM tree job is incomplete"));
        }
        Ok(())
    }

    fn candidate_chunks_sort_memory_plan_v1_native(
        &self,
        token: u32,
    ) -> Result<CandidateChunkSortMemoryPlanV1, ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        let hash_count = match job {
            WasmTreeJob::V1 {
                purpose: WasmTreeJobPurpose::CandidateChunks,
                cursor,
                ..
            } if cursor.progress(0).done => cursor.progress(0).reachable,
            WasmTreeJob::V2 {
                purpose: WasmTreeJobPurpose::CandidateChunks,
                cursor,
                ..
            } if cursor.progress(0).done => cursor.progress(0).reachable,
            WasmTreeJob::V1 {
                purpose: WasmTreeJobPurpose::CandidateChunks,
                ..
            }
            | WasmTreeJob::V2 {
                purpose: WasmTreeJobPurpose::CandidateChunks,
                ..
            } => return Err(tree_job_error("WASM tree job is incomplete")),
            _ => return Err(tree_job_error("WASM tree job finish kind mismatch")),
        };
        CandidateChunkSortMemoryPlanV1::for_hashes(hash_count)
    }

    fn resume_candidate_chunks_sort_memory_v1_native(
        &mut self,
        token: u32,
        source_bytes: f64,
        scratch_bytes: f64,
    ) -> Result<(), ChunkError> {
        let (Some(source_bytes), Some(scratch_bytes)) = (
            safe_integer_u64(source_bytes),
            safe_integer_u64(scratch_bytes),
        ) else {
            return Err(tree_job_error(
                "candidate chunk sort witnesses must be safe integers",
            ));
        };
        let plan = self.candidate_chunks_sort_memory_plan_v1_native(token)?;
        if source_bytes != plan.source_hashes_requested_bytes
            || scratch_bytes != plan.scratch_hashes_requested_bytes
        {
            return Err(tree_job_error(
                "candidate chunk sort memory witness mismatch",
            ));
        }
        // Allocate both exact-request workspaces before consuming the ready
        // traversal. Allocation failure therefore leaves that job retryable.
        let mut sort = CandidateChunkSortCursor::try_new(plan.hash_count)?;
        let mut job = self.tree_job.take().unwrap();
        let reachable = match &mut job {
            WasmTreeJob::V1 { cursor, .. } => cursor.take_reachable(),
            WasmTreeJob::V2 { cursor, .. } => cursor.take_reachable(),
            _ => unreachable!("candidate chunk traversal checked before resume"),
        };
        let reachable = match reachable {
            Ok(reachable) => reachable,
            Err(error) => {
                self.tree_job = Some(job);
                return Err(error);
            }
        };
        let reachability = match job {
            WasmTreeJob::V1 { cursor, .. } => ReachabilityRetirement::V1(cursor),
            WasmTreeJob::V2 { cursor, .. } => ReachabilityRetirement::V2(cursor),
            _ => unreachable!("candidate chunk traversal checked before resume"),
        };
        sort.install_input(reachable);
        self.tree_job = Some(WasmTreeJob::CandidateChunkSort {
            token,
            reachability,
            cursor: sort,
        });
        Ok(())
    }

    fn step_candidate_chunks_sort_v1_native(
        &mut self,
        token: u32,
        max_units: u32,
    ) -> Result<CandidateChunkSortProgressV1, ChunkError> {
        let job = self
            .tree_job
            .as_mut()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        let WasmTreeJob::CandidateChunkSort { cursor, .. } = job else {
            return Err(tree_job_error("WASM candidate chunk sort kind mismatch"));
        };
        cursor.step(max_units as usize)
    }

    fn candidate_chunks_plan_info_v1_native(
        &self,
        token: u32,
    ) -> Result<CandidateChunkPlanInfoV1, ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        let WasmTreeJob::CandidateChunkSort { cursor, .. } = job else {
            return Err(tree_job_error("WASM candidate chunk sort kind mismatch"));
        };
        if cursor.phase != CandidateChunkSortPhase::Ready {
            return Err(tree_job_error("candidate chunk sort is incomplete"));
        }
        Ok(CandidateChunkPlanInfoV1 {
            schema: 1,
            scope: "candidate-chunk-plan-pages",
            all_count: cursor.hash_count,
            page_max_hashes: CANDIDATE_CHUNK_PLAN_PAGE_MAX_HASHES,
        })
    }

    fn read_candidate_chunks_page_v1_native(
        &mut self,
        token: u32,
        expected_offset: usize,
        max_hashes: u32,
    ) -> Result<CandidateChunkPlanPageV1, ChunkError> {
        if max_hashes == 0 || max_hashes > CANDIDATE_CHUNK_PLAN_PAGE_MAX_HASHES {
            return Err(tree_job_error("invalid candidate chunk page budget"));
        }
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        let WasmTreeJob::CandidateChunkSort { cursor, .. } = job else {
            return Err(tree_job_error("WASM candidate chunk sort kind mismatch"));
        };
        if cursor.phase != CandidateChunkSortPhase::Ready {
            return Err(tree_job_error("candidate chunk sort is incomplete"));
        }
        if cursor.page_offset != expected_offset {
            return Err(tree_job_error("candidate chunk page offset mismatch"));
        }
        let end = expected_offset
            .saturating_add(max_hashes as usize)
            .min(cursor.hash_count);
        let hashes = &cursor.source[expected_offset..end];
        let mut all = Vec::new();
        all.try_reserve_exact(hashes.len())
            .map_err(|_| tree_job_error("candidate chunk page allocation failed"))?;
        let mut fresh = Vec::new();
        fresh
            .try_reserve_exact(hashes.len())
            .map_err(|_| tree_job_error("candidate fresh page allocation failed"))?;
        for hash in hashes {
            let hex = hash_to_hex(hash);
            all.push(hex.clone());
            let is_fresh = match &self.inner {
                WasmTreeInner::V1(tree) => tree.candidate_chunk_is_fresh(hash)?,
                WasmTreeInner::V2(tree) => tree.candidate_chunk_is_fresh(hash)?,
            };
            if is_fresh {
                fresh.push(hex);
            }
        }
        let job = self.tree_job.as_mut().unwrap();
        let WasmTreeJob::CandidateChunkSort { cursor, .. } = job else {
            unreachable!()
        };
        cursor.page_offset = end;
        Ok(CandidateChunkPlanPageV1 {
            schema: 1,
            scope: "candidate-chunk-plan-page",
            offset: expected_offset,
            next_offset: end,
            done: end == cursor.hash_count,
            all,
            fresh,
        })
    }

    fn finish_candidate_chunks_plan_v1_native(&mut self, token: u32) -> Result<(), ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        let WasmTreeJob::CandidateChunkSort { cursor, .. } = job else {
            return Err(tree_job_error("WASM candidate chunk sort kind mismatch"));
        };
        if cursor.phase != CandidateChunkSortPhase::Ready || cursor.page_offset != cursor.hash_count
        {
            return Err(tree_job_error("candidate chunk plan pages are incomplete"));
        }
        let job = self.tree_job.take().unwrap();
        self.tree_job = Some(WasmTreeJob::Retirement {
            token,
            cursor: TreeRetirement::from_reachability(job),
        });
        Ok(())
    }

    fn finish_candidate_job_native(&mut self, token: u32) -> Result<usize, ChunkError> {
        self.ready_candidate_job_native(token, WasmTreeJobPurpose::BeginCandidate)?;
        let next_revision = self.next_candidate_revision_native(true)?;
        self.ensure_prepared_candidate_job_native(token)?;
        let reachable = match self.tree_job.as_ref().unwrap() {
            WasmTreeJob::V1 { cursor, .. } => cursor.progress(0).reachable,
            WasmTreeJob::V2 { cursor, .. } => cursor.progress(0).reachable,
            _ => unreachable!("job purpose checked before candidate finish"),
        };
        match (&mut self.inner, self.tree_job.as_mut().unwrap()) {
            (
                WasmTreeInner::V1(tree),
                WasmTreeJob::V1 {
                    prepared_candidate, ..
                },
            ) => tree.open_prepared_candidate(prepared_candidate)?,
            (
                WasmTreeInner::V2(tree),
                WasmTreeJob::V2 {
                    prepared_candidate, ..
                },
            ) => tree.open_prepared_candidate(prepared_candidate)?,
            _ => return Err(tree_job_error("tree job format changed")),
        }
        // Legacy destruction remains synchronous, but no allocation or
        // fallible root/baseline preparation occurs after publication.
        self.tree_job = None;
        self.candidate_revision = next_revision;
        Ok(reachable)
    }

    fn finish_candidate_job_deferred_native(&mut self, token: u32) -> Result<usize, ChunkError> {
        self.ready_candidate_job_native(token, WasmTreeJobPurpose::BeginCandidate)?;
        // Keep the validated cursor installed until every fallible publication
        // preflight has passed. After open succeeds, ownership transfer and the
        // revision write are infallible field moves under the same token.
        let next_revision = self.next_candidate_revision_native(true)?;
        self.ensure_prepared_candidate_job_native(token)?;
        let reachable = match self.tree_job.as_ref().unwrap() {
            WasmTreeJob::V1 { cursor, .. } => cursor.progress(0).reachable,
            WasmTreeJob::V2 { cursor, .. } => cursor.progress(0).reachable,
            _ => unreachable!("job purpose checked before deferred candidate finish"),
        };
        match (&mut self.inner, self.tree_job.as_mut().unwrap()) {
            (
                WasmTreeInner::V1(tree),
                WasmTreeJob::V1 {
                    prepared_candidate, ..
                },
            ) => tree.open_prepared_candidate(prepared_candidate),
            (
                WasmTreeInner::V2(tree),
                WasmTreeJob::V2 {
                    prepared_candidate, ..
                },
            ) => tree.open_prepared_candidate(prepared_candidate),
            _ => Err(tree_job_error("tree job format changed")),
        }?;
        let job = self.tree_job.take().unwrap();
        self.tree_job = Some(WasmTreeJob::Retirement {
            token,
            cursor: TreeRetirement::from_reachability(job),
        });
        self.candidate_revision = next_revision;
        Ok(reachable)
    }

    fn candidate_open_memory_plan_v1_native(
        &self,
        token: u32,
    ) -> Result<CandidateOpenMemoryPlanV1, ChunkError> {
        self.ready_candidate_job_native(token, WasmTreeJobPurpose::BeginCandidate)?;
        match (&self.tree_job, &self.inner) {
            (
                Some(WasmTreeJob::V1 {
                    prepared_candidate: None,
                    ..
                }),
                WasmTreeInner::V1(tree),
            ) => tree.candidate_open_memory_plan(),
            (
                Some(WasmTreeJob::V2 {
                    prepared_candidate: None,
                    ..
                }),
                WasmTreeInner::V2(tree),
            ) => tree.candidate_open_memory_plan(),
            _ => Err(tree_job_error(
                "candidate open memory plan requires unprepared ready input",
            )),
        }
    }

    fn prepare_candidate_job_native(&mut self, token: u32) -> Result<(), ChunkError> {
        self.candidate_open_memory_plan_v1_native(token)?;
        match (&self.inner, self.tree_job.as_mut().unwrap()) {
            (
                WasmTreeInner::V1(tree),
                WasmTreeJob::V1 {
                    prepared_candidate, ..
                },
            ) => *prepared_candidate = Some(tree.prepare_candidate_root()?),
            (
                WasmTreeInner::V2(tree),
                WasmTreeJob::V2 {
                    prepared_candidate, ..
                },
            ) => *prepared_candidate = Some(tree.prepare_candidate_root()?),
            _ => return Err(tree_job_error("tree job format changed")),
        }
        Ok(())
    }

    #[cfg(test)]
    fn prepare_candidate_job_v2_with(
        &mut self,
        token: u32,
        prepare: impl FnOnce(&TransactionalTreeV2) -> Result<RootNodeV2, ChunkError>,
    ) -> Result<(), ChunkError> {
        self.candidate_open_memory_plan_v1_native(token)?;
        let WasmTreeInner::V2(tree) = &self.inner else {
            return Err(tree_job_error(
                "Tree v2 candidate preparation requires Tree v2",
            ));
        };
        let root = prepare(tree)?;
        let Some(WasmTreeJob::V2 {
            prepared_candidate, ..
        }) = &mut self.tree_job
        else {
            return Err(tree_job_error("Tree v2 candidate job kind mismatch"));
        };
        *prepared_candidate = Some(root);
        Ok(())
    }

    fn ensure_prepared_candidate_job_native(&mut self, token: u32) -> Result<(), ChunkError> {
        if matches!(
            (&self.tree_job, &self.inner),
            (
                Some(WasmTreeJob::V1 {
                    prepared_candidate: Some(_),
                    ..
                }),
                WasmTreeInner::V1(_),
            ) | (
                Some(WasmTreeJob::V2 {
                    prepared_candidate: Some(_),
                    ..
                }),
                WasmTreeInner::V2(_),
            )
        ) {
            return Ok(());
        }
        self.prepare_candidate_job_native(token)
    }

    fn resume_candidate_open_memory_v1_native(
        &mut self,
        token: u32,
        identity: f64,
        endpoint: f64,
        total: f64,
    ) -> Result<(), ChunkError> {
        let (Some(identity), Some(endpoint), Some(total)) = (
            safe_integer_u64(identity),
            safe_integer_u64(endpoint),
            safe_integer_u64(total),
        ) else {
            return Err(tree_job_error(
                "candidate open witnesses must be safe integers",
            ));
        };
        let plan = self.candidate_open_memory_plan_v1_native(token)?;
        if (identity, endpoint, total)
            != (
                plan.root_identity_requested_bytes,
                plan.root_endpoint_requested_bytes,
                plan.root_string_requested_bytes,
            )
        {
            return Err(tree_job_error("candidate open memory witness mismatch"));
        }
        self.prepare_candidate_job_native(token)
    }

    fn finish_candidate_chunks_deferred_native(
        &mut self,
        token: u32,
    ) -> Result<WasmCandidateChunkPlan, ChunkError> {
        if matches!(
            self.tree_job.as_ref(),
            Some(WasmTreeJob::CandidateChunkSort { .. })
        ) {
            return Err(tree_job_error(
                "candidate chunk sort requires its versioned page API",
            ));
        }
        self.ready_candidate_job_native(token, WasmTreeJobPurpose::CandidateChunks)?;
        let reachable = match self.tree_job.as_mut().unwrap() {
            WasmTreeJob::V1 { cursor, .. } => cursor.take_reachable(),
            WasmTreeJob::V2 { cursor, .. } => cursor.take_reachable(),
            _ => unreachable!("job purpose checked before deferred chunk finish"),
        }?;
        // Sorting and hex output are still explicit synchronous residuals. The
        // much larger V2 descriptor owner remains installed throughout and is
        // moved to cooperative retirement even when plan construction fails.
        let plan = match &self.inner {
            WasmTreeInner::V1(tree) => tree.candidate_chunk_plan_from_reachable(reachable),
            WasmTreeInner::V2(tree) => tree.candidate_chunk_plan_from_reachable(reachable),
        };
        let job = self.tree_job.take().unwrap();
        self.tree_job = Some(WasmTreeJob::Retirement {
            token,
            cursor: TreeRetirement::from_reachability(job),
        });
        let (all, fresh) = plan?;
        Ok(WasmCandidateChunkPlan {
            all: hex_hashes(all),
            fresh: hex_hashes(fresh),
        })
    }

    fn finish_candidate_mutation_native(&mut self, token: u32) -> Result<(), ChunkError> {
        self.ready_candidate_job_native(token, WasmTreeJobPurpose::CandidateMutation)?;
        if !self.has_candidate() {
            return Err(tree_job_error("no active candidate"));
        }
        let changes = !matches!(self.tree_job, Some(WasmTreeJob::MutationNoop { .. }));
        self.mutate_candidate_native(changes, |inner, slot| {
            let mut job = slot.take().unwrap();
            let result = match (&mut job, inner) {
                (WasmTreeJob::MutationV1 { cursor, .. }, WasmTreeInner::V1(tree)) => tree
                    .finish_candidate_mutation(cursor.as_mut().ok_or_else(|| {
                        tree_job_error("candidate mutation output was not resumed")
                    })?),
                (WasmTreeJob::MutationV2 { cursor, .. }, WasmTreeInner::V2(tree)) => {
                    tree.finish_candidate_mutation(cursor)
                }
                (WasmTreeJob::MutationNoop { .. }, _) => Ok(()),
                _ => Err(tree_job_error("tree job format changed")),
            };
            if result.is_err() {
                *slot = Some(job);
            }
            result
        })
    }

    fn finish_candidate_mutation_deferred_native(&mut self, token: u32) -> Result<(), ChunkError> {
        self.ready_candidate_job_native(token, WasmTreeJobPurpose::CandidateMutation)?;
        if !self.has_candidate() {
            return Err(tree_job_error("no active candidate"));
        }
        let changes = !matches!(self.tree_job, Some(WasmTreeJob::MutationNoop { .. }));
        let next_revision = self.next_candidate_revision_native(changes)?;
        if !matches!(
            (self.tree_job.as_ref().unwrap(), &self.inner),
            (
                WasmTreeJob::MutationV1 {
                    output_resumed: true,
                    ..
                },
                WasmTreeInner::V1(_)
            ) | (WasmTreeJob::MutationV2 { .. }, WasmTreeInner::V2(_))
                | (WasmTreeJob::MutationNoop { .. }, _)
        ) {
            return Err(tree_job_error(
                "candidate mutation output admission was not resumed",
            ));
        }

        let mut job = self.tree_job.take().unwrap();
        let result =
            match (&mut job, &mut self.inner) {
                (WasmTreeJob::MutationV1 { cursor, .. }, WasmTreeInner::V1(tree)) => tree
                    .finish_candidate_mutation(cursor.as_mut().ok_or_else(|| {
                        tree_job_error("candidate mutation output was not resumed")
                    })?),
                (WasmTreeJob::MutationV2 { cursor, .. }, WasmTreeInner::V2(tree)) => {
                    tree.finish_candidate_mutation(cursor)
                }
                (WasmTreeJob::MutationNoop { .. }, _) => Ok(()),
                _ => Err(tree_job_error("tree job format changed")),
            };
        if let Err(error) = result {
            self.tree_job = Some(job);
            return Err(error);
        }
        self.tree_job = Some(WasmTreeJob::Retirement {
            token,
            cursor: TreeRetirement::from_candidate_mutation(job),
        });
        self.candidate_revision = next_revision;
        Ok(())
    }

    fn step_candidate_mutation_output_memory_v1_native(
        &mut self,
        token: u32,
        max_units: u32,
    ) -> Result<V2MutationProgress, ChunkError> {
        if max_units == 0 || max_units > MAX_TREE_JOB_STEP_UNITS {
            return Err(tree_job_error(
                "invalid candidate mutation output-memory step budget",
            ));
        }
        let job = self
            .tree_job
            .as_mut()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        match (job, &self.inner) {
            (WasmTreeJob::MutationV2 { cursor, .. }, WasmTreeInner::V2(tree)) => {
                tree.step_candidate_mutation(cursor, max_units as usize)
            }
            (WasmTreeJob::MutationNoop { .. }, _) => Ok(V2MutationProgress {
                done: true,
                units: 0,
                completed: 0,
                phase: "ready",
                staged_chunks: 0,
                staged_bytes: 0,
            }),
            (
                WasmTreeJob::MutationV1 {
                    cursor,
                    output_resumed: false,
                    ..
                },
                WasmTreeInner::V1(_),
            ) => Ok(V2MutationProgress {
                done: false,
                units: 0,
                completed: cursor
                    .as_ref()
                    .map_or(0, |cursor| cursor.progress().completed),
                phase: "plan ready",
                staged_chunks: 0,
                staged_bytes: 0,
            }),
            (
                WasmTreeJob::MutationV1 {
                    cursor,
                    output_resumed: true,
                    ..
                },
                WasmTreeInner::V1(tree),
            ) => {
                let cursor = cursor
                    .as_mut()
                    .ok_or_else(|| tree_job_error("candidate mutation output was not resumed"))?;
                let progress = tree.step_candidate_mutation(cursor, max_units as usize)?;
                Ok(V2MutationProgress {
                    done: progress.done,
                    units: progress.units,
                    completed: progress.completed,
                    phase: if progress.done { "ready" } else { "build" },
                    staged_chunks: progress.staged_chunks,
                    staged_bytes: progress.staged_bytes,
                })
            }
            _ => Err(tree_job_error(
                "WASM tree job candidate mutation output-memory kind mismatch",
            )),
        }
    }

    fn candidate_mutation_output_memory_plan_v1_native(
        &self,
        token: u32,
    ) -> Result<CandidateMutationOutputMemoryPlanV1, ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        match job {
            WasmTreeJob::MutationV2 { cursor, .. } => cursor.output_memory_plan_v1(),
            WasmTreeJob::MutationV1 {
                output_plan,
                output_resumed: false,
                ..
            } => Ok(*output_plan),
            WasmTreeJob::MutationV1 { .. } | WasmTreeJob::MutationNoop { .. } => Err(
                tree_job_error("candidate mutation output memory plan is not ready"),
            ),
            _ => Err(tree_job_error(
                "WASM tree job candidate mutation output-memory kind mismatch",
            )),
        }
    }

    fn resume_candidate_mutation_output_memory_v1_native(
        &mut self,
        token: u32,
        expected_node_payload_bytes: u64,
        expected_range_endpoint_peak_requested_bytes: u64,
        expected_range_endpoint_resident_requested_bytes: u64,
    ) -> Result<(), ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        match job {
            WasmTreeJob::MutationV2 { cursor, .. } => {
                let plan = cursor.output_memory_plan_v1()?;
                if expected_node_payload_bytes != plan.node_payload_bytes
                    || expected_range_endpoint_peak_requested_bytes
                        != plan.range_endpoint_peak_requested_bytes
                    || expected_range_endpoint_resident_requested_bytes
                        != plan.range_endpoint_resident_requested_bytes
                {
                    return Err(tree_job_error(
                        "Tree v2 candidate output memory witness changed",
                    ));
                }
                Ok(())
            }
            WasmTreeJob::MutationV1 {
                output_plan,
                output_resumed,
                ..
            } => {
                if *output_resumed
                    || expected_node_payload_bytes != output_plan.node_payload_bytes
                    || expected_range_endpoint_peak_requested_bytes
                        != output_plan.range_endpoint_peak_requested_bytes
                    || expected_range_endpoint_resident_requested_bytes
                        != output_plan.range_endpoint_resident_requested_bytes
                {
                    return Err(tree_job_error(
                        "Tree v1 candidate output memory witness changed",
                    ));
                }
                Ok(())
            }
            WasmTreeJob::MutationNoop { .. } => Err(tree_job_error(
                "no-op candidate mutation has no output memory plan",
            )),
            _ => Err(tree_job_error(
                "WASM tree job candidate mutation output-memory kind mismatch",
            )),
        }?;
        if matches!(self.tree_job, Some(WasmTreeJob::MutationV1 { .. })) {
            let root = match &self.inner {
                WasmTreeInner::V1(tree) => tree.prepare_candidate_mutation_root()?,
                _ => return Err(tree_job_error("candidate mutation tree format changed")),
            };
            let Some(WasmTreeJob::MutationV1 {
                cursor,
                changed,
                deleted,
                output_resumed,
                ..
            }) = self.tree_job.as_mut()
            else {
                unreachable!("Tree v1 mutation kind checked above")
            };
            let changed = changed
                .take()
                .ok_or_else(|| tree_job_error("candidate mutation lost its update input"))?;
            let deleted = deleted
                .take()
                .ok_or_else(|| tree_job_error("candidate mutation lost its delete input"))?;
            *cursor = Some(CandidateMutationV1::new(root, changed, deleted));
            *output_resumed = true;
        } else if let Some(WasmTreeJob::MutationV2 { cursor, .. }) = self.tree_job.as_mut() {
            cursor.resume_after_output_memory_plan_v1(
                expected_node_payload_bytes,
                expected_range_endpoint_peak_requested_bytes,
                expected_range_endpoint_resident_requested_bytes,
            )?;
        }
        Ok(())
    }

    fn candidate_mutation_output_memory_ready_v1_native(
        &self,
        token: u32,
    ) -> Result<CandidateMutationOutputMemoryReadyV1, ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        match job {
            WasmTreeJob::MutationV2 { cursor, .. } => cursor.output_memory_ready_v1(),
            WasmTreeJob::MutationV1 {
                cursor,
                output_plan,
                output_resumed: true,
                ..
            } => {
                let cursor = cursor
                    .as_ref()
                    .ok_or_else(|| tree_job_error("candidate mutation output was not resumed"))?;
                let progress = cursor.progress();
                let staged = progress.staged_bytes as u64;
                let root = cursor.root_requested_bytes()?;
                let resident = staged
                    .checked_add(root)
                    .ok_or_else(|| tree_job_error("Tree v1 candidate ready byte count overflow"))?;
                if !progress.done
                    || staged > output_plan.node_payload_bytes
                    || root > output_plan.range_endpoint_resident_requested_bytes
                    || resident > output_plan.resident_admission_bytes
                {
                    return Err(tree_job_error(
                        "Tree v1 candidate output memory is not ready",
                    ));
                }
                Ok(CandidateMutationOutputMemoryReadyV1 {
                    staged_node_payload_bytes: staged,
                    range_endpoint_resident_requested_bytes: root,
                    resident_admission_bytes: resident,
                })
            }
            WasmTreeJob::MutationNoop { .. } => Err(tree_job_error(
                "no-op candidate mutation has no output memory readiness",
            )),
            WasmTreeJob::MutationV1 { .. } => Err(tree_job_error(
                "Tree v1 candidate output memory is not ready",
            )),
            _ => Err(tree_job_error(
                "WASM tree job candidate mutation output-memory kind mismatch",
            )),
        }
    }

    fn begin_replacement_rebuild_native(
        &mut self,
        version: u32,
        expected_entries: usize,
        expected_bytes: usize,
    ) -> Result<u32, ChunkError> {
        self.ensure_tree_job_idle_native()?;
        if version != TREE_V1 && version != TREE_V2 {
            return Err(tree_job_error(format!(
                "unsupported tree version {version}"
            )));
        }
        if expected_entries > MAX_REPLACEMENT_REBUILD_ENTRIES {
            return Err(tree_job_error(
                "replacement rebuild entry count exceeds its bound",
            ));
        }
        if !(2..=MAX_REPLACEMENT_REBUILD_JSON_BYTES).contains(&expected_bytes)
            || (expected_entries == 0) != (expected_bytes == 2)
        {
            return Err(tree_job_error(
                "replacement rebuild JSON byte count is invalid",
            ));
        }
        let token = self.next_tree_job_token;
        let next = token
            .checked_add(1)
            .ok_or_else(|| tree_job_error("WASM tree job token space exhausted"))?;
        let mut entries = Vec::new();
        // Reserve the declared cardinality once, without geometric growth.
        // No accepted feed can exceed this capacity or trigger reallocation.
        // This bounds entry slots, not allocator overhead or native heap RSS.
        entries
            .try_reserve_exact(expected_entries)
            .map_err(|_| tree_job_error("replacement rebuild entry allocation failed"))?;
        self.tree_job = Some(WasmTreeJob::ReplacementRebuild {
            token,
            version,
            expected_entries,
            expected_bytes,
            received_bytes: 2,
            input_memory: ReplacementInputMemory::new(&entries),
            entries,
        });
        self.next_tree_job_token = next;
        Ok(token)
    }

    fn chunk_memory_native(&self) -> TreeChunkMemoryReport {
        let resident = match &self.inner {
            WasmTreeInner::V1(tree) => tree.chunk_memory(),
            WasmTreeInner::V2(tree) => tree.chunk_memory(),
        };
        let mut report = TreeChunkMemoryReport {
            schema: 1,
            scope: "tree-chunk-stores",
            resident,
            replacement: ChunkStoreMemory::default(),
            retiring: ChunkStoreMemory::default(),
            other_private_jobs_unmeasured: false,
        };
        match &self.tree_job {
            Some(WasmTreeJob::ReplacementBuildV1 { cursor, .. }) => {
                report.replacement = cursor.chunk_memory()
            }
            Some(WasmTreeJob::ReplacementBuildV2 { cursor, .. }) => {
                report.replacement = cursor.chunk_memory()
            }
            Some(WasmTreeJob::Retirement { cursor, .. }) => {
                report.retiring = cursor.chunk_memory();
                report.other_private_jobs_unmeasured = cursor.has_unmeasured_private();
            }
            None | Some(WasmTreeJob::ReplacementRebuild { .. }) => {}
            // Mutation overlays/export/traversal worksets are NOT covered by
            // these replacement/store counters. Never silently report their
            // absence as complete private-heap accounting.
            Some(_) => report.other_private_jobs_unmeasured = true,
        }
        report
    }

    fn metadata_memory_native(&self) -> TreeMetadataMemoryReport {
        let resident = match &self.inner {
            WasmTreeInner::V1(tree) => tree.metadata_memory(),
            WasmTreeInner::V2(tree) => tree.metadata_memory(),
        };
        let mut report = TreeMetadataMemoryReport {
            schema: 1,
            scope: "tree-metadata",
            wrapper_ids: StringMemory::of(&self.vault_id)
                .combine(StringMemory::of(&self.device_id)),
            resident,
            replacement: Some(TreeMetadataMemory::default()),
            retiring: Some(TreeMetadataMemory::default()),
            other_private_jobs_unmeasured: false,
        };
        match &self.tree_job {
            Some(WasmTreeJob::ReplacementBuildV1 { cursor, .. }) => {
                report.replacement = cursor.metadata_memory();
                report.other_private_jobs_unmeasured = report.replacement.is_none();
            }
            Some(WasmTreeJob::ReplacementBuildV2 { cursor, .. }) => {
                report.replacement = cursor.metadata_memory();
                report.other_private_jobs_unmeasured = report.replacement.is_none();
            }
            Some(WasmTreeJob::Retirement { cursor, .. }) => {
                let (known, has_known, incomplete) = cursor.metadata_memory();
                report.retiring = if has_known {
                    Some(known)
                } else if incomplete {
                    None
                } else {
                    Some(TreeMetadataMemory::default())
                };
                report.other_private_jobs_unmeasured = incomplete;
            }
            None => {}
            Some(WasmTreeJob::ReplacementRebuild { .. }) => {
                report.replacement = None;
                report.other_private_jobs_unmeasured = true;
            }
            // Candidate mutation and traversal/export cursors can own cloned
            // roots/ranges or other metadata worksets. Resident accounting is
            // still exact, but the private component is intentionally partial.
            Some(_) => report.other_private_jobs_unmeasured = true,
        }
        report
    }

    fn replacement_input_memory_native(&self) -> ReplacementInputMemoryReport {
        let mut report = ReplacementInputMemoryReport {
            schema: 1,
            scope: "replacement-input",
            replacement: Some(ReplacementInputMemory::default()),
            retiring: Some(ReplacementInputMemory::default()),
            other_input_owners_unmeasured: false,
        };
        match &self.tree_job {
            Some(WasmTreeJob::ReplacementRebuild { input_memory, .. }) => {
                report.replacement = Some(*input_memory);
            }
            Some(WasmTreeJob::ReplacementBuildV1 { .. })
            | Some(WasmTreeJob::ReplacementBuildV2 { .. }) => {
                report.replacement = None;
                report.other_input_owners_unmeasured = true;
            }
            Some(WasmTreeJob::Retirement { cursor, .. }) => {
                report.retiring = cursor.replacement_input_memory();
                report.other_input_owners_unmeasured = report.retiring.is_none();
            }
            None | Some(_) => {}
        }
        report
    }

    fn replacement_sort_memory_native(&self) -> ReplacementSortMemoryReport {
        let mut report = ReplacementSortMemoryReport {
            schema: 1,
            scope: "replacement-sort",
            replacement: Some(ReplacementSortMemory::default()),
            retiring: Some(ReplacementSortMemory::default()),
            other_sort_owners_unmeasured: false,
        };
        match &self.tree_job {
            Some(WasmTreeJob::ReplacementBuildV1 { cursor, .. }) => {
                report.replacement = Some(cursor.sort_memory());
            }
            Some(WasmTreeJob::ReplacementBuildV2 { cursor, .. }) => {
                report.replacement = Some(cursor.sort_memory());
            }
            Some(WasmTreeJob::Retirement { cursor, .. }) => {
                report.retiring = Some(cursor.replacement_sort_memory());
            }
            // Candidate mutation has separate sorting worksets which this
            // replacement-only component does not yet instrument.
            Some(WasmTreeJob::MutationV1 { .. }) | Some(WasmTreeJob::MutationV2 { .. }) => {
                report.replacement = None;
                report.other_sort_owners_unmeasured = true;
            }
            None | Some(_) => {}
        }
        report
    }

    fn replacement_v1_entries_memory_native(&self) -> ReplacementV1EntriesMemoryReport {
        let mut report = ReplacementV1EntriesMemoryReport {
            schema: 1,
            scope: "replacement-v1-entries",
            replacement: Some(ReplacementV1EntriesMemory::default()),
            retiring: Some(ReplacementV1EntriesMemory::default()),
            other_entry_owners_unmeasured: false,
        };
        match &self.tree_job {
            Some(WasmTreeJob::ReplacementBuildV1 { cursor, .. }) => {
                report.replacement = Some(cursor.entry_memory());
            }
            Some(WasmTreeJob::Retirement { cursor, .. }) => {
                report.retiring = Some(cursor.replacement_v1_entries_memory());
            }
            None | Some(_) => {}
        }
        report
    }

    fn replacement_v1_graph_memory_native(&self) -> ReplacementV1GraphMemoryReport {
        let mut report = ReplacementV1GraphMemoryReport {
            schema: 1,
            scope: "replacement-v1-graph",
            replacement: Some(ReplacementV1GraphMemory::default()),
            retiring: Some(ReplacementV1GraphMemory::default()),
            other_graph_owners_unmeasured: false,
        };
        match &self.tree_job {
            Some(WasmTreeJob::ReplacementBuildV1 { cursor, .. }) => {
                report.replacement = Some(cursor.graph_memory());
            }
            Some(WasmTreeJob::Retirement { cursor, .. }) => {
                report.retiring = Some(cursor.replacement_v1_graph_memory());
            }
            None | Some(_) => {}
        }
        report
    }

    fn replacement_v2_planning_memory_native(&self) -> ReplacementV2PlanningMemoryReport {
        let mut report = ReplacementV2PlanningMemoryReport {
            schema: 1,
            scope: "replacement-v2-planning",
            replacement: Some(ReplacementV2PlanningMemory::default()),
            retiring: Some(ReplacementV2PlanningMemory::default()),
            other_planning_owners_unmeasured: false,
        };
        match &self.tree_job {
            Some(WasmTreeJob::ReplacementBuildV2 { cursor, .. }) => {
                report.replacement = Some(cursor.planning_memory());
            }
            Some(WasmTreeJob::Retirement { cursor, .. }) => {
                report.retiring = Some(cursor.replacement_v2_planning_memory());
            }
            None | Some(_) => {}
        }
        report
    }

    fn replacement_v2_ranges_memory_native(&self) -> ReplacementV2RangesMemoryReport {
        let mut report = ReplacementV2RangesMemoryReport {
            schema: 1,
            scope: "replacement-v2-ranges",
            replacement: Some(ReplacementV2RangesMemory::default()),
            retiring: Some(ReplacementV2RangesMemory::default()),
            // Candidate mutation and the general reachability cursor retain
            // RangeRef owners which this replacement-only ABI does not cover.
            other_range_owners_unmeasured: true,
        };
        match &self.tree_job {
            Some(WasmTreeJob::ReplacementBuildV2 { cursor, .. }) => {
                report.replacement = Some(cursor.range_memory());
            }
            Some(WasmTreeJob::Retirement { cursor, .. }) => {
                report.retiring = Some(cursor.replacement_v2_ranges_memory());
            }
            None | Some(_) => {}
        }
        report
    }

    fn replacement_v2_post_sort_memory_native(&self) -> ReplacementV2PostSortMemoryReport {
        let mut report = ReplacementV2PostSortMemoryReport {
            schema: 1,
            scope: "replacement-v2-post-sort",
            replacement: Some(ReplacementInputMemory::default()),
            retiring: Some(ReplacementInputMemory::default()),
            other_post_sort_owners_unmeasured: false,
        };
        match &self.tree_job {
            Some(WasmTreeJob::ReplacementBuildV2 { cursor, .. }) => {
                report.replacement = cursor.post_sort_entry_memory();
                report.other_post_sort_owners_unmeasured = report.replacement.is_none();
            }
            Some(WasmTreeJob::Retirement { cursor, .. }) => {
                report.retiring = cursor.replacement_v2_post_sort_memory();
                report.other_post_sort_owners_unmeasured = report.retiring.is_none();
            }
            None | Some(_) => {}
        }
        report
    }

    fn append_replacement_rebuild_native(
        &mut self,
        token: u32,
        expected_offset_entries: usize,
        page_json: &str,
    ) -> Result<usize, ChunkError> {
        let job = self
            .tree_job
            .as_mut()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        let WasmTreeJob::ReplacementRebuild {
            expected_entries,
            expected_bytes,
            received_bytes,
            entries,
            input_memory,
            ..
        } = job
        else {
            return Err(tree_job_error("WASM tree job kind mismatch"));
        };
        if expected_offset_entries != entries.len() {
            return Err(tree_job_error("replacement rebuild feed offset mismatch"));
        }
        // Check source bytes before parsing. The host must also check before
        // wasm-bindgen copies its string; this native check cannot undo that
        // initial crossing allocation. Exact outer brackets make the complete
        // logical JSON accounting independent of feed partitioning.
        if page_json.len() > MAX_REPLACEMENT_REBUILD_FEED_BYTES
            || !page_json.starts_with('[')
            || !page_json.ends_with(']')
        {
            return Err(tree_job_error(
                "replacement rebuild feed JSON bound is invalid",
            ));
        }
        let next_bytes = received_bytes
            .checked_add(page_json.len() - 2)
            .and_then(|bytes| bytes.checked_add(usize::from(!entries.is_empty())))
            .filter(|bytes| *bytes <= *expected_bytes)
            .ok_or_else(|| tree_job_error("replacement rebuild received too many JSON bytes"))?;
        let batch = parse_entry_page_native(page_json)?;
        if batch.is_empty() || batch.len() > MAX_REPLACEMENT_REBUILD_FEED_ENTRIES {
            return Err(tree_job_error(
                "replacement rebuild feed entry count is invalid",
            ));
        }
        let received = entries
            .len()
            .checked_add(batch.len())
            .ok_or_else(|| tree_job_error("replacement rebuild entry count overflow"))?;
        if received > *expected_entries {
            return Err(tree_job_error(
                "replacement rebuild received too many entries",
            ));
        }
        let paths = ReplacementInputMemory::paths(&batch);
        entries.extend(batch);
        input_memory.accepted(entries, paths);
        *received_bytes = next_bytes;
        Ok(received)
    }

    fn replacement_rebuild_sort_memory_plan_v1_native(
        &self,
        token: u32,
    ) -> Result<ReplacementRebuildSortMemoryPlanV1, ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        let WasmTreeJob::ReplacementRebuild {
            version, entries, ..
        } = job
        else {
            return Err(tree_job_error("WASM tree job sort-plan kind mismatch"));
        };
        if *version != TREE_V2 {
            return Err(tree_job_error(
                "Tree v1 replacement has no sort-index memory plan",
            ));
        }
        if !job.ready() {
            return Err(tree_job_error("replacement rebuild input is incomplete"));
        }
        ReplacementRebuildSortMemoryPlanV1::for_entries(entries.len())
    }

    fn start_replacement_rebuild_sort_memory_v1_native(
        &mut self,
        token: u32,
        expected_source_index_bytes: f64,
        expected_target_index_bytes: f64,
    ) -> Result<(), ChunkError> {
        let (Some(source_bytes), Some(target_bytes)) = (
            safe_integer_u64(expected_source_index_bytes),
            safe_integer_u64(expected_target_index_bytes),
        ) else {
            return Err(tree_job_error(
                "replacement sort memory witnesses must be safe integers",
            ));
        };
        self.start_replacement_rebuild_with_workspace_native(
            token,
            Some((source_bytes, target_bytes)),
            SortWorkspace::try_new,
        )
    }

    fn start_replacement_rebuild_native(&mut self, token: u32) -> Result<(), ChunkError> {
        // Legacy V2 callers gain recoverable allocation of the same two
        // buffers, but this compatibility entrypoint does not assert admission.
        self.start_replacement_rebuild_with_workspace_native(token, None, SortWorkspace::try_new)
    }

    fn start_replacement_rebuild_with_workspace_native(
        &mut self,
        token: u32,
        expected_indices: Option<(u64, u64)>,
        allocate_workspace: impl FnOnce(usize) -> Result<SortWorkspace, ChunkError>,
    ) -> Result<(), ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        if !matches!(job, WasmTreeJob::ReplacementRebuild { .. }) {
            return Err(tree_job_error("WASM tree job start kind mismatch"));
        }
        if !job.ready() {
            return Err(tree_job_error("replacement rebuild input is incomplete"));
        }
        if let Some((expected_source, expected_target)) = expected_indices {
            let plan = self.replacement_rebuild_sort_memory_plan_v1_native(token)?;
            if expected_source != plan.source_index_requested_bytes
                || expected_target != plan.target_index_requested_bytes
            {
                return Err(tree_job_error("replacement sort memory witness mismatch"));
            }
        }
        let WasmTreeJob::ReplacementRebuild {
            version, entries, ..
        } = job
        else {
            unreachable!("input kind checked before start")
        };
        // Reserve both empty index buffers BEFORE moving any input. Either
        // reserve may fail; the local first buffer then drops and the same job
        // remains available for retry or deferred cancellation. ID clones are
        // also prepared before take, but their ordinary allocation/OOM behavior
        // is not a recoverable-allocation promise or part of the index plan.
        let prepared_v2 = if *version == TREE_V2 {
            let workspace = allocate_workspace(entries.len())?;
            if !workspace.accepts(entries.len()) {
                return Err(tree_job_error("replacement sort workspace is invalid"));
            }
            Some((workspace, self.vault_id.clone(), self.device_id.clone()))
        } else {
            None
        };
        let WasmTreeJob::ReplacementRebuild {
            version,
            expected_bytes,
            entries,
            input_memory,
            ..
        } = self.tree_job.take().unwrap()
        else {
            unreachable!("input kind checked before start")
        };
        // Only move the admitted input. Construction and validation happen
        // under subsequent bounded steps, never against the live graph.
        self.tree_job = Some(match version {
            TREE_V1 => WasmTreeJob::ReplacementBuildV1 {
                token,
                cursor: ReplacementRebuildV1::new_metered_with_output_plan(
                    entries,
                    input_memory,
                    &self.vault_id,
                    &self.device_id,
                    expected_bytes,
                ),
            },
            TREE_V2 => {
                let (workspace, vault_id, device_id) =
                    prepared_v2.expect("V2 owners prepared before taking input");
                WasmTreeJob::ReplacementBuildV2 {
                    token,
                    cursor: ReplacementRebuildV2::new_metered_with_workspace(
                        entries,
                        input_memory,
                        workspace,
                        vault_id,
                        device_id,
                    ),
                }
            }
            _ => unreachable!("version validated at input begin"),
        });
        Ok(())
    }

    fn step_replacement_rebuild_native(
        &mut self,
        token: u32,
        max_units: u32,
    ) -> Result<ReplacementBuildProgress, ChunkError> {
        if max_units == 0 || max_units > MAX_TREE_JOB_STEP_UNITS {
            return Err(tree_job_error("invalid replacement rebuild step budget"));
        }
        let vault_id = self.vault_id.as_str();
        let device_id = self.device_id.as_str();
        let job = self
            .tree_job
            .as_mut()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        // Errors keep the private (failed) owner available for explicit
        // cancel. API misuse never discards somebody else's job.
        match job {
            WasmTreeJob::ReplacementBuildV1 { cursor, .. } => {
                // Additive compatibility: the old V1 start/step ABI predates
                // output admission and must keep making progress. New hosts
                // use the dedicated step below to observe PlanReady.
                let mut progress = cursor.step(max_units as usize)?;
                if progress.phase == "plan ready" {
                    let plan = cursor.output_memory_plan_v1()?;
                    cursor.resume_after_output_memory_plan_v1(
                        plan.node_payload_bytes,
                        plan.range_endpoint_peak_requested_bytes,
                        plan.range_endpoint_resident_requested_bytes,
                        vault_id,
                        device_id,
                    )?;
                    progress = cursor.step(max_units as usize)?;
                }
                Ok(progress)
            }
            WasmTreeJob::ReplacementBuildV2 { cursor, .. } => cursor.step(max_units as usize),
            _ => Err(tree_job_error("WASM tree job step kind mismatch")),
        }
    }

    fn step_replacement_rebuild_output_memory_v1_native(
        &mut self,
        token: u32,
        max_units: u32,
    ) -> Result<ReplacementBuildProgress, ChunkError> {
        if max_units == 0 || max_units > MAX_TREE_JOB_STEP_UNITS {
            return Err(tree_job_error("invalid replacement rebuild step budget"));
        }
        let job = self
            .tree_job
            .as_mut()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        match job {
            WasmTreeJob::ReplacementBuildV1 { cursor, .. } => cursor.step(max_units as usize),
            WasmTreeJob::ReplacementBuildV2 { cursor, .. } => cursor.step(max_units as usize),
            _ => Err(tree_job_error("WASM tree job step kind mismatch")),
        }
    }

    fn replacement_rebuild_plan_native(
        &self,
        token: u32,
    ) -> Result<ReplacementBuildPlanV2, ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        match job {
            WasmTreeJob::ReplacementBuildV2 { cursor, .. } => cursor.plan(),
            WasmTreeJob::ReplacementBuildV1 { .. } => Err(tree_job_error(
                "Tree v1 replacement has no node payload plan",
            )),
            _ => Err(tree_job_error("WASM tree job plan kind mismatch")),
        }
    }

    fn replacement_rebuild_output_memory_plan_v1_native(
        &self,
        token: u32,
    ) -> Result<ReplacementOutputMemoryPlanV1, ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        match job {
            WasmTreeJob::ReplacementBuildV1 { cursor, .. } => cursor.output_memory_plan_v1(),
            WasmTreeJob::ReplacementBuildV2 { cursor, .. } => cursor.output_memory_plan_v1(),
            _ => Err(tree_job_error(
                "WASM tree job output memory plan kind mismatch",
            )),
        }
    }

    fn resume_replacement_rebuild_native(
        &mut self,
        token: u32,
        expected_node_payload_bytes: u64,
    ) -> Result<(), ChunkError> {
        let job = self
            .tree_job
            .as_mut()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        match job {
            WasmTreeJob::ReplacementBuildV2 { cursor, .. } => {
                cursor.resume_after_plan(expected_node_payload_bytes)
            }
            WasmTreeJob::ReplacementBuildV1 { .. } => Err(tree_job_error(
                "Tree v1 replacement has no node payload plan",
            )),
            _ => Err(tree_job_error("WASM tree job resume kind mismatch")),
        }
    }

    fn resume_replacement_rebuild_output_memory_v1_native(
        &mut self,
        token: u32,
        expected_node_payload_bytes: u64,
        expected_range_endpoint_peak_requested_bytes: u64,
        expected_range_endpoint_resident_requested_bytes: u64,
    ) -> Result<(), ChunkError> {
        let vault_id = self.vault_id.as_str();
        let device_id = self.device_id.as_str();
        let job = self
            .tree_job
            .as_mut()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        match job {
            WasmTreeJob::ReplacementBuildV1 { cursor, .. } => cursor
                .resume_after_output_memory_plan_v1(
                    expected_node_payload_bytes,
                    expected_range_endpoint_peak_requested_bytes,
                    expected_range_endpoint_resident_requested_bytes,
                    vault_id,
                    device_id,
                ),
            WasmTreeJob::ReplacementBuildV2 { cursor, .. } => cursor
                .resume_after_output_memory_plan_v1(
                    expected_node_payload_bytes,
                    expected_range_endpoint_peak_requested_bytes,
                    expected_range_endpoint_resident_requested_bytes,
                ),
            _ => Err(tree_job_error(
                "WASM tree job output memory resume kind mismatch",
            )),
        }
    }

    fn finish_replacement_rebuild_native(&mut self, token: u32) -> Result<(), ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        if job.purpose() != WasmTreeJobPurpose::ReplacementRebuild {
            return Err(tree_job_error("WASM tree job finish kind mismatch"));
        }
        if !job.ready() {
            return Err(tree_job_error("WASM tree job is incomplete"));
        }

        // A correct ready finish consumes the private input owner even when a
        // later preflight/build fails. The live graph and both revisions remain
        // unchanged until every replacement operation has succeeded.
        let job = self.tree_job.take().unwrap();
        let next_committed = self.next_committed_revision_native()?;
        let next_candidate = self.next_candidate_revision_native(self.has_candidate())?;
        let replacement = match job {
            WasmTreeJob::ReplacementRebuild {
                version, entries, ..
            } => build_replacement_tree(version, &self.vault_id, &self.device_id, entries)?,
            WasmTreeJob::ReplacementBuildV1 { cursor, .. } => WasmTreeInner::V1(cursor.finish()?),
            WasmTreeJob::ReplacementBuildV2 { cursor, .. } => WasmTreeInner::V2(cursor.finish()?),
            _ => unreachable!("replacement rebuild purpose checked before finish"),
        };
        // The verified replacement is moved once. Destruction of the old
        // resident graph remains synchronous and separately unaccounted.
        self.inner = replacement;
        self.committed_revision = next_committed;
        self.candidate_revision = next_candidate;
        Ok(())
    }

    fn cancel_replacement_rebuild_deferred_native(&mut self, token: u32) -> Result<(), ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        if job.purpose() != WasmTreeJobPurpose::ReplacementRebuild {
            return Err(tree_job_error("WASM tree job retirement kind mismatch"));
        }
        let job = self.tree_job.take().unwrap();
        self.tree_job = Some(WasmTreeJob::Retirement {
            token,
            cursor: TreeRetirement::from_replacement(job, None),
        });
        Ok(())
    }

    fn finish_replacement_rebuild_deferred_native(&mut self, token: u32) -> Result<(), ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        if !matches!(
            job,
            WasmTreeJob::ReplacementBuildV1 { .. } | WasmTreeJob::ReplacementBuildV2 { .. }
        ) {
            return Err(tree_job_error(
                "deferred finish requires a started replacement build",
            ));
        }
        if !job.ready() {
            return Err(tree_job_error("WASM tree job is incomplete"));
        }
        // Unlike the legacy atomic finish, EVERY error leaves a cancellable
        // owner. Preflight and take_ready never destroy the private graph.
        let next_committed = self.next_committed_revision_native()?;
        let next_candidate = self.next_candidate_revision_native(self.has_candidate())?;
        let replacement = match self.tree_job.as_mut().unwrap() {
            WasmTreeJob::ReplacementBuildV1 { cursor, .. } => {
                WasmTreeInner::V1(cursor.take_ready()?)
            }
            WasmTreeJob::ReplacementBuildV2 { cursor, .. } => {
                WasmTreeInner::V2(cursor.take_ready()?)
            }
            _ => unreachable!("started builder checked before finish"),
        };
        let previous = std::mem::replace(&mut self.inner, replacement);
        let job = self.tree_job.take().unwrap();
        self.tree_job = Some(WasmTreeJob::Retirement {
            token,
            cursor: TreeRetirement::from_replacement(job, Some(previous)),
        });
        self.committed_revision = next_committed;
        self.candidate_revision = next_candidate;
        Ok(())
    }

    fn step_tree_retirement_native(
        &mut self,
        token: u32,
        max_units: u32,
    ) -> Result<TreeRetirementProgress, ChunkError> {
        if max_units == 0 || max_units > MAX_TREE_JOB_STEP_UNITS {
            return Err(tree_job_error("invalid tree retirement step budget"));
        }
        let job = self
            .tree_job
            .as_mut()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        let WasmTreeJob::Retirement { cursor, .. } = job else {
            return Err(tree_job_error("WASM tree job retirement kind mismatch"));
        };
        if cursor.reachability_origin {
            return Err(tree_job_error(
                "reachability retirement requires its replay-safe step API",
            ));
        }
        cursor
            .completed
            .checked_add(max_units as usize)
            .filter(|value| *value as u64 <= MAX_SAFE_COMMITTED_REVISION)
            .ok_or_else(|| tree_job_error("tree retirement progress space exhausted"))?;
        let mut units = 0;
        let mut done = false;
        while units < max_units as usize {
            if !cursor.retire_one() {
                done = true;
                break;
            }
            units += 1;
        }
        cursor.completed += units;
        cursor.done = done;
        let progress = TreeRetirementProgress {
            done,
            units,
            completed: cursor.completed,
        };
        if done {
            // No populated rows, descriptors or node buffers remain. Releasing
            // empty backing allocations is still an indivisible allocator op.
            self.tree_job = None;
        }
        Ok(progress)
    }

    fn step_reachability_retirement_native(
        &mut self,
        token: u32,
        expected_completed: usize,
        max_units: u32,
    ) -> Result<TreeRetirementProgress, ChunkError> {
        if max_units == 0 || max_units > MAX_TREE_JOB_STEP_UNITS {
            return Err(tree_job_error("invalid tree retirement step budget"));
        }
        let job = self
            .tree_job
            .as_mut()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        let WasmTreeJob::Retirement { cursor, .. } = job else {
            return Err(tree_job_error("WASM tree job retirement kind mismatch"));
        };
        if !cursor.reachability_origin {
            return Err(tree_job_error("WASM reachability retirement kind mismatch"));
        }
        if let Some(previous) = cursor.last_progress {
            let previous_start = previous.completed.saturating_sub(previous.units);
            if expected_completed == previous_start {
                if previous.units > max_units as usize {
                    return Err(tree_job_error(
                        "replayed retirement exceeds the step budget",
                    ));
                }
                return Ok(previous);
            }
            if expected_completed != previous.completed {
                return Err(tree_job_error("tree retirement progress mismatch"));
            }
            if previous.done {
                return Err(tree_job_error(
                    "tree retirement requires finish acknowledgement",
                ));
            }
            cursor.last_progress = None;
        }
        if expected_completed != cursor.completed {
            return Err(tree_job_error("tree retirement progress mismatch"));
        }
        cursor
            .completed
            .checked_add(max_units as usize)
            .filter(|value| *value as u64 <= MAX_SAFE_COMMITTED_REVISION)
            .ok_or_else(|| tree_job_error("tree retirement progress space exhausted"))?;
        let mut units = 0usize;
        let mut done = false;
        while units < max_units as usize {
            if !cursor.retire_one() {
                done = true;
                break;
            }
            units += 1;
        }
        cursor.completed += units;
        cursor.done = done;
        let progress = TreeRetirementProgress {
            done,
            units,
            completed: cursor.completed,
        };
        cursor.last_progress = Some(progress);
        Ok(progress)
    }

    fn finish_reachability_retirement_native(
        &mut self,
        token: u32,
        expected_completed: usize,
    ) -> Result<(), ChunkError> {
        if self.tree_job.is_none() {
            return match self.last_completed_reachability_retirement {
                Some(completed)
                    if completed.token == token && completed.completed == expected_completed =>
                {
                    Ok(())
                }
                _ => Err(tree_job_error(
                    "no matching completed reachability retirement",
                )),
            };
        }
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        let WasmTreeJob::Retirement { cursor, .. } = job else {
            return Err(tree_job_error("WASM tree job retirement kind mismatch"));
        };
        if !cursor.reachability_origin
            || !cursor.done
            || cursor.completed != expected_completed
            || !matches!(cursor.last_progress, Some(progress) if progress.done && progress.completed == expected_completed)
        {
            return Err(tree_job_error("reachability retirement is incomplete"));
        }
        self.tree_job = None;
        self.last_completed_reachability_retirement = Some(CompletedReachabilityRetirement {
            token,
            completed: expected_completed,
        });
        Ok(())
    }

    #[cfg(test)]
    fn begin_root_export_inner(&mut self, scope: RootExportScope) -> Result<u32, ChunkError> {
        self.begin_root_export_with_limit_inner(scope, u32::MAX)
    }

    fn begin_root_export_with_limit_inner(
        &mut self,
        scope: RootExportScope,
        max_arena_bytes: u32,
    ) -> Result<u32, ChunkError> {
        if max_arena_bytes == 0 {
            return Err(tree_job_error("invalid root export arena limit"));
        }
        self.ensure_tree_job_idle_native()?;
        let has_root = match &self.inner {
            WasmTreeInner::V1(tree) => match scope {
                RootExportScope::Candidate => tree.candidate_root().is_some(),
                RootExportScope::Committed => tree.committed_root().is_some(),
            },
            WasmTreeInner::V2(tree) => match scope {
                RootExportScope::Candidate => tree.candidate_root().is_some(),
                RootExportScope::Committed => tree.committed_root().is_some(),
            },
        };
        if !has_root {
            return Err(tree_job_error("requested root export scope is absent"));
        }
        let token = self.next_tree_job_token;
        let next = token
            .checked_add(1)
            .ok_or_else(|| tree_job_error("WASM tree job token space exhausted"))?;
        self.tree_job = Some(match &self.inner {
            WasmTreeInner::V1(_) => WasmTreeJob::RootExportV1 {
                token,
                scope,
                cursor: RootExportV1::with_arena_limit(max_arena_bytes as usize)?,
            },
            WasmTreeInner::V2(_) => WasmTreeJob::RootExportV2 {
                token,
                scope,
                cursor: RootExportV2::with_arena_limit(max_arena_bytes as usize)?,
            },
        });
        self.next_tree_job_token = next;
        Ok(token)
    }

    fn root_export_job(&self, token: u32) -> Result<&WasmTreeJob, ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        if job.purpose() != WasmTreeJobPurpose::RootExport {
            return Err(tree_job_error("WASM tree job kind mismatch"));
        }
        Ok(job)
    }

    fn root_export_workset_inner(&self, token: u32) -> Result<WasmRootExportWorkset, ChunkError> {
        let (max_length, offset_bytes) = match self.root_export_job(token)? {
            WasmTreeJob::RootExportV1 { cursor, .. } => {
                let workset = cursor.workset()?;
                (workset.max_length, workset.offset_bytes)
            }
            WasmTreeJob::RootExportV2 { cursor, .. } => {
                if cursor.phase != RootExportV2Phase::Planned {
                    return Err(tree_job_error(
                        "root export Plan is not ready for admission",
                    ));
                }
                (cursor.max_length, 0)
            }
            _ => unreachable!(),
        };
        Ok(WasmRootExportWorkset {
            max_length: u32::try_from(max_length)
                .map_err(|_| tree_job_error("root export exceeds ABI length"))?,
            offset_bytes: u32::try_from(offset_bytes)
                .map_err(|_| tree_job_error("root export exceeds ABI offset bytes"))?,
        })
    }

    fn start_root_export_build_inner(&mut self, token: u32) -> Result<(), ChunkError> {
        self.root_export_job(token)?;
        match self.tree_job.as_mut().unwrap() {
            WasmTreeJob::RootExportV1 { cursor, .. } => cursor.start_build(),
            WasmTreeJob::RootExportV2 { cursor, .. } => {
                if cursor.phase != RootExportV2Phase::Planned {
                    return Err(tree_job_error(
                        "root export Plan is not ready for admission",
                    ));
                }
                cursor.phase = RootExportV2Phase::Build;
                cursor.completed = 0;
                cursor.processed = 0;
                Ok(())
            }
            _ => unreachable!(),
        }
    }

    fn root_export_info_inner(&mut self, token: u32) -> Result<WasmRootExportInfo, ChunkError> {
        self.root_export_job(token)?;
        let (length, version, hash) = match self.tree_job.as_mut().unwrap() {
            WasmTreeJob::RootExportV1 { cursor, .. } => {
                let (length, hash) = cursor.seal()?;
                (length, TREE_V1, hash)
            }
            WasmTreeJob::RootExportV2 { cursor, .. } => {
                if !matches!(
                    cursor.phase,
                    RootExportV2Phase::Built | RootExportV2Phase::Sealed
                ) {
                    return Err(tree_job_error("root export Build is not ready"));
                }
                cursor.phase = RootExportV2Phase::Sealed;
                (cursor.bytes.len(), TREE_V2, cursor.hash)
            }
            _ => unreachable!(),
        };
        Ok(WasmRootExportInfo {
            length: u32::try_from(length)
                .map_err(|_| tree_job_error("root export exceeds ABI length"))?,
            version,
            hash: hash_to_hex(&hash),
        })
    }

    fn read_root_export_inner(
        &mut self,
        token: u32,
        expected_offset: u32,
        max_bytes: u32,
    ) -> Result<Vec<u8>, ChunkError> {
        self.root_export_job(token)?;
        match self.tree_job.as_mut().unwrap() {
            WasmTreeJob::RootExportV1 { cursor, .. } => {
                cursor.read(expected_offset as usize, max_bytes as usize)
            }
            WasmTreeJob::RootExportV2 { cursor, .. } => {
                if cursor.phase != RootExportV2Phase::Sealed
                    || cursor.offset != expected_offset as usize
                    || !(1..=ROOT_EXPORT_PART_BYTES).contains(&(max_bytes as usize))
                {
                    return Err(tree_job_error("invalid root export page read"));
                }
                let end = (cursor.offset + max_bytes as usize).min(cursor.bytes.len());
                let bytes = cursor.bytes[cursor.offset..end].to_vec();
                cursor.offset = end;
                Ok(bytes)
            }
            _ => unreachable!(),
        }
    }

    fn finish_root_export_inner(&mut self, token: u32) -> Result<(), ChunkError> {
        if !self.root_export_job(token)?.ready() {
            return Err(tree_job_error("root export has not reached exact EOF"));
        }
        self.tree_job = None;
        Ok(())
    }

    fn begin_tree_chunk_export_inner(
        &mut self,
        hash_hex: &str,
    ) -> Result<WasmTreeChunkExport, ChunkError> {
        if self.tree_job.is_some() {
            return Err(tree_job_error("WASM tree has an active job"));
        }
        let has_root = match &self.inner {
            WasmTreeInner::V1(tree) => tree.candidate_root().or(tree.committed_root()).is_some(),
            WasmTreeInner::V2(tree) => tree.candidate_root().or(tree.committed_root()).is_some(),
        };
        if !has_root {
            return Err(tree_job_error("no current tree root"));
        }
        if hash_hex.len() != 64 {
            return Err(tree_job_error("invalid chunk export hash"));
        }
        let hash =
            hex_to_hash(hash_hex).map_err(|_| tree_job_error("invalid chunk export hash"))?;
        let length = match &self.inner {
            WasmTreeInner::V1(tree) => tree.chunk_byte_length(&hash),
            WasmTreeInner::V2(tree) => tree.chunk_byte_length(&hash),
        }
        .ok_or_else(|| tree_job_error("chunk export source is missing"))?;
        let length = u32::try_from(length)
            .map_err(|_| tree_job_error("chunk export length exceeds this ABI"))?;
        let token = self.next_tree_job_token;
        let next = token
            .checked_add(1)
            .ok_or_else(|| tree_job_error("WASM tree job token space exhausted"))?;
        self.tree_job = Some(WasmTreeJob::ChunkExport {
            token,
            hash,
            length,
            offset: 0,
        });
        self.next_tree_job_token = next;
        Ok(WasmTreeChunkExport { token, length })
    }

    fn read_tree_chunk_export_inner(
        &mut self,
        token: u32,
        expected_offset: u32,
        max_bytes: u32,
    ) -> Result<Vec<u8>, ChunkError> {
        let job = self
            .tree_job
            .as_mut()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        let WasmTreeJob::ChunkExport {
            hash,
            length,
            offset,
            ..
        } = job
        else {
            return Err(tree_job_error("WASM tree job read kind mismatch"));
        };
        if expected_offset != *offset {
            return Err(tree_job_error("chunk export offset mismatch"));
        }
        let bytes = match &self.inner {
            WasmTreeInner::V1(tree) => {
                tree.copy_chunk_range(hash, *length as usize, *offset as usize, max_bytes as usize)
            }
            WasmTreeInner::V2(tree) => {
                tree.copy_chunk_range(hash, *length as usize, *offset as usize, max_bytes as usize)
            }
        }?;
        let next = offset
            .checked_add(bytes.len() as u32)
            .ok_or_else(|| tree_job_error("chunk export offset overflow"))?;
        *offset = next;
        Ok(bytes)
    }

    fn finish_tree_chunk_export_inner(&mut self, token: u32) -> Result<(), ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        if job.purpose() != WasmTreeJobPurpose::TreeChunkExport {
            return Err(tree_job_error("WASM tree job finish kind mismatch"));
        }
        if !job.ready() {
            return Err(tree_job_error("WASM tree job is incomplete"));
        }
        self.tree_job = None;
        Ok(())
    }

    fn step_tree_job_native(
        &mut self,
        token: u32,
        max_units: u32,
    ) -> Result<WasmTreeJobProgress, ChunkError> {
        if max_units == 0 || max_units > MAX_TREE_JOB_STEP_UNITS {
            return Err(tree_job_error("invalid WASM tree job step budget"));
        }
        let mut job = self
            .tree_job
            .take()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            self.tree_job = Some(job);
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        if matches!(
            &job,
            WasmTreeJob::V1 {
                prepared_candidate: Some(_),
                ..
            } | WasmTreeJob::V2 {
                prepared_candidate: Some(_),
                ..
            }
        ) {
            self.tree_job = Some(job);
            return Err(tree_job_error(
                "prepared candidate requires finish or deferred cancellation",
            ));
        }
        if job.purpose() == WasmTreeJobPurpose::TreeChunkExport {
            self.tree_job = Some(job);
            return Err(tree_job_error("chunk export requires the page read API"));
        }
        if job.purpose() == WasmTreeJobPurpose::RootExport {
            self.tree_job = Some(job);
            return Err(tree_job_error(
                "root export requires the byte-budgeted step API",
            ));
        }
        if job.purpose() == WasmTreeJobPurpose::ReplacementRebuild {
            self.tree_job = Some(job);
            return Err(tree_job_error(
                "replacement rebuild requires the input feed API",
            ));
        }
        if job.purpose() == WasmTreeJobPurpose::Retirement {
            self.tree_job = Some(job);
            return Err(tree_job_error(
                "tree retirement requires the retirement step API",
            ));
        }
        if matches!(&job, WasmTreeJob::CandidateChunkSort { .. }) {
            self.tree_job = Some(job);
            return Err(tree_job_error(
                "candidate chunk sort requires its versioned step API",
            ));
        }
        if job.root_export_sealed() {
            self.tree_job = Some(job);
            return Err(tree_job_error("root export is sealed for page reads"));
        }
        match step_tree_job_inner(&mut job, &self.inner, max_units as usize) {
            Ok(progress) => {
                self.tree_job = Some(job);
                Ok(progress)
            }
            Err(error) => Err(error),
        }
    }

    fn step_reachability_job_deferred_native(
        &mut self,
        token: u32,
        max_units: u32,
    ) -> Result<WasmTreeJobProgress, ChunkError> {
        if max_units == 0 || max_units > MAX_TREE_JOB_STEP_UNITS {
            return Err(tree_job_error("invalid WASM tree job step budget"));
        }
        let mut job = self
            .tree_job
            .take()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            self.tree_job = Some(job);
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        if !matches!(job, WasmTreeJob::V1 { .. } | WasmTreeJob::V2 { .. }) {
            self.tree_job = Some(job);
            return Err(tree_job_error("WASM tree job reachability kind mismatch"));
        }
        if matches!(
            &job,
            WasmTreeJob::V1 {
                prepared_candidate: Some(_),
                ..
            } | WasmTreeJob::V2 {
                prepared_candidate: Some(_),
                ..
            }
        ) {
            self.tree_job = Some(job);
            return Err(tree_job_error(
                "prepared candidate requires finish or deferred cancellation",
            ));
        }
        match step_tree_job_inner(&mut job, &self.inner, max_units as usize) {
            Ok(progress) => {
                self.tree_job = Some(job);
                Ok(progress)
            }
            Err(error) => {
                self.tree_job = Some(WasmTreeJob::Retirement {
                    token,
                    cursor: TreeRetirement::from_reachability(job),
                });
                Err(error)
            }
        }
    }

    fn step_root_export_job_native(
        &mut self,
        token: u32,
        max_units: u32,
        max_bytes: u32,
    ) -> Result<WasmRootExportProgress, ChunkError> {
        if max_units == 0 || max_units as usize > ROOT_EXPORT_MAX_STEP_UNITS {
            return Err(tree_job_error("invalid root export operation budget"));
        }
        if max_bytes == 0 || max_bytes as usize > ROOT_EXPORT_MAX_STEP_BYTES {
            return Err(tree_job_error("invalid root export byte budget"));
        }
        let mut job = self
            .tree_job
            .take()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            self.tree_job = Some(job);
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        if job.purpose() != WasmTreeJobPurpose::RootExport {
            self.tree_job = Some(job);
            return Err(tree_job_error("WASM tree job kind mismatch"));
        }
        if job.root_export_sealed() {
            self.tree_job = Some(job);
            return Err(tree_job_error("root export is sealed for page reads"));
        }
        match step_root_export_job_inner(
            &mut job,
            &self.inner,
            max_units as usize,
            max_bytes as usize,
        ) {
            Ok(progress) => {
                debug_assert!(
                    progress.bytes <= max_bytes as f64
                        || (progress.units == 1.0
                            && progress.bytes <= ROOT_EXPORT_MAX_ATOMIC_BYTES as f64)
                );
                self.tree_job = Some(job);
                Ok(progress)
            }
            Err(error) => Err(error),
        }
    }

    fn cancel_tree_job_inner(&mut self, token: u32) -> Result<(), ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        if job.purpose() == WasmTreeJobPurpose::Retirement {
            return Err(tree_job_error(
                "tree retirement must be drained, not synchronously cancelled",
            ));
        }
        if matches!(
            job,
            WasmTreeJob::V1 {
                prepared_candidate: Some(_),
                ..
            } | WasmTreeJob::V2 {
                prepared_candidate: Some(_),
                ..
            }
        ) {
            return Err(tree_job_error(
                "prepared candidate requires deferred cancellation",
            ));
        }
        if matches!(job, WasmTreeJob::CandidateChunkSort { .. }) {
            return Err(tree_job_error(
                "candidate chunk sort requires deferred cancellation",
            ));
        }
        self.tree_job = None;
        Ok(())
    }

    fn cancel_candidate_mutation_deferred_native(&mut self, token: u32) -> Result<(), ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        if let WasmTreeJob::Retirement { cursor, .. } = job {
            return if cursor.mutation_origin {
                Ok(())
            } else {
                Err(tree_job_error(
                    "WASM tree job candidate mutation retirement kind mismatch",
                ))
            };
        }
        if !matches!(
            (job, &self.inner),
            (WasmTreeJob::MutationV1 { .. }, WasmTreeInner::V1(_))
                | (WasmTreeJob::MutationV2 { .. }, WasmTreeInner::V2(_))
                | (WasmTreeJob::MutationNoop { .. }, _)
        ) {
            return Err(tree_job_error(
                "WASM tree job candidate mutation output kind mismatch",
            ));
        }
        let job = self.tree_job.take().unwrap();
        self.tree_job = Some(WasmTreeJob::Retirement {
            token,
            cursor: TreeRetirement::from_candidate_mutation(job),
        });
        Ok(())
    }

    fn cancel_reachability_job_deferred_native(&mut self, token: u32) -> Result<(), ChunkError> {
        let job = self
            .tree_job
            .as_ref()
            .ok_or_else(|| tree_job_error("no active WASM tree job"))?;
        if job.token() != token {
            return Err(tree_job_error("WASM tree job token mismatch"));
        }
        if let WasmTreeJob::Retirement { cursor, .. } = job {
            return if cursor.reachability_origin {
                Ok(())
            } else {
                Err(tree_job_error("WASM tree job reachability kind mismatch"))
            };
        }
        if !matches!(
            job,
            WasmTreeJob::V1 { .. }
                | WasmTreeJob::V2 { .. }
                | WasmTreeJob::CandidateChunkSort { .. }
        ) {
            return Err(tree_job_error("WASM tree job reachability kind mismatch"));
        }
        let job = self.tree_job.take().unwrap();
        self.tree_job = Some(WasmTreeJob::Retirement {
            token,
            cursor: TreeRetirement::from_reachability(job),
        });
        Ok(())
    }
}

#[wasm_bindgen]
impl WasmTree {
    #[wasm_bindgen(constructor)]
    pub fn new(vault_id: &str, device_id: &str) -> Self {
        Self {
            inner: WasmTreeInner::V1(TransactionalTree::new(vault_id, device_id)),
            vault_id: vault_id.to_owned(),
            device_id: device_id.to_owned(),
            committed_revision: 0,
            candidate_revision: 0,
            tree_job: None,
            next_tree_job_token: 1,
            last_completed_reachability_retirement: None,
        }
    }

    fn ensure_tree_job_idle(&self) -> Result<(), JsValue> {
        self.ensure_tree_job_idle_native()
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Select the empty tree's format after authenticated capability
    /// negotiation. Switching a populated graph is forbidden; callers rebuild
    /// it from sync-base so v1/v2 node bytes can never share one transaction.
    pub fn set_tree_version(&mut self, version: u32) -> Result<(), JsValue> {
        self.ensure_tree_job_idle()?;
        if self.root_hash_hex().is_some() || self.has_candidate() {
            return Err(JsValue::from_str(
                "cannot switch the format of a populated WASM tree",
            ));
        }
        let replacement = match version {
            TREE_V1 => WasmTreeInner::V1(TransactionalTree::new(&self.vault_id, &self.device_id)),
            TREE_V2 => WasmTreeInner::V2(TransactionalTreeV2::new(&self.vault_id, &self.device_id)),
            other => {
                return Err(JsValue::from_str(&format!(
                    "unsupported tree version {other}"
                )))
            }
        };
        self.mutate_committed_native(|inner| {
            *inner = replacement;
            Ok(())
        })
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    pub fn tree_version(&self) -> u32 {
        match &self.inner {
            WasmTreeInner::V1(_) => TREE_V1,
            WasmTreeInner::V2(_) => TREE_V2,
        }
    }

    /// O(1) ABA-resistant witness for the committed root/graph owned by this
    /// exact WasmTree instance. Candidate-only work does not change it.
    pub fn committed_revision(&self) -> f64 {
        self.committed_revision as f64
    }

    /// O(1) witness for the visible candidate owned by this exact wrapper.
    /// Successful open, nonempty update/delete (even semantically unchanged),
    /// mutation publish, abort, commit, or replacement clearing a candidate
    /// advances it. Empty batches/MutationNoop, provisional steps/cancel, and
    /// rejected transitions leave it unchanged. This is not a wire identity.
    pub fn candidate_revision(&self) -> f64 {
        self.candidate_revision as f64
    }

    /// O(1) component observation across live/private/retiring node stores.
    /// These buffers already belong to wasm_memory_snapshot's module total.
    /// Root metadata, hash-table storage, sort/input/closure and other private
    /// job buffers are not a covered heap budget or an admission permission.
    pub fn chunk_memory_snapshot(&self) -> JsValue {
        to_js(&self.chunk_memory_native())
    }

    /// O(1) root/identity/candidate-baseline component observation. Requested
    /// String/Vec capacities and logical HashSet slots are a subset of this
    /// WASM instance, not allocator usable bytes, total heap/RSS, or quota.
    pub fn metadata_memory_snapshot(&self) -> JsValue {
        let serializer = serde_wasm_bindgen::Serializer::new()
            .serialize_maps_as_objects(true)
            .serialize_missing_as_null(true);
        self.metadata_memory_native()
            .serialize(&serializer)
            .unwrap_or(JsValue::NULL)
    }

    /// Exact accepted replacement-input Vec backing and path String buffers.
    /// Once a builder redistributes those rows, coverage becomes explicit null
    /// until later component slices account its other containers.
    pub fn replacement_input_memory_snapshot(&self) -> JsValue {
        let serializer = serde_wasm_bindgen::Serializer::new()
            .serialize_maps_as_objects(true)
            .serialize_missing_as_null(true);
        self.replacement_input_memory_native()
            .serialize(&serializer)
            .unwrap_or(JsValue::NULL)
    }

    /// Exact top-level values/source/target Vec backing for active replacement
    /// indirect sorters. Nested row strings and other builder worksets are not
    /// included; candidate-mutation sort coverage remains explicit null.
    pub fn replacement_sort_memory_snapshot(&self) -> JsValue {
        let serializer = serde_wasm_bindgen::Serializer::new()
            .serialize_maps_as_objects(true)
            .serialize_missing_as_null(true);
        self.replacement_sort_memory_native()
            .serialize(&serializer)
            .unwrap_or(JsValue::NULL)
    }

    /// Exact retained V1 replacement FileEntry Vec backing and nested path
    /// String buffers. Active sorter Vec backing remains exclusively in the
    /// replacement-sort component; only its logical row count appears here.
    pub fn replacement_v1_entries_memory_snapshot(&self) -> JsValue {
        let serializer = serde_wasm_bindgen::Serializer::new()
            .serialize_maps_as_objects(true)
            .serialize_missing_as_null(true);
        self.replacement_v1_entries_memory_native()
            .serialize(&serializer)
            .unwrap_or(JsValue::NULL)
    }

    /// Exact V1 assembly Vec backing and owned String buffers before the
    /// provisional root moves into metadata accounting. BTree/HashSet buckets,
    /// reachability closure, codecs, allocator metadata and sorter backing are
    /// deliberately excluded from this additive component.
    pub fn replacement_v1_graph_memory_snapshot(&self) -> JsValue {
        let serializer = serde_wasm_bindgen::Serializer::new()
            .serialize_maps_as_objects(true)
            .serialize_missing_as_null(true);
        self.replacement_v1_graph_memory_native()
            .serialize(&serializer)
            .unwrap_or(JsValue::NULL)
    }

    /// Exact top-level Vec backing for the six POD descriptor arrays retained
    /// by a V2 replacement planner/emitter. Entries, range strings, closure
    /// maps and codec scratch remain outside this additive component.
    pub fn replacement_v2_planning_memory_snapshot(&self) -> JsValue {
        let serializer = serde_wasm_bindgen::Serializer::new()
            .serialize_maps_as_objects(true)
            .serialize_missing_as_null(true);
        self.replacement_v2_planning_memory_native()
            .serialize(&serializer)
            .unwrap_or(JsValue::NULL)
    }

    /// Exact retained backing and endpoint String allocations for V2
    /// replacement range vectors and closure descriptors. Root endpoints are
    /// exclusively reported by metadata; map buckets, node decode scratch and
    /// non-replacement range owners remain outside this additive component.
    pub fn replacement_v2_ranges_memory_snapshot(&self) -> JsValue {
        let serializer = serde_wasm_bindgen::Serializer::new()
            .serialize_maps_as_objects(true)
            .serialize_missing_as_null(true);
        self.replacement_v2_ranges_memory_native()
            .serialize(&serializer)
            .unwrap_or(JsValue::NULL)
    }

    /// Exact V2 replacement entry Vec backing and nested path buffers after
    /// the indirect sorter has returned its values. The owner is explicit
    /// null during sorting so this component cannot double-count sort backing.
    pub fn replacement_v2_post_sort_memory_snapshot(&self) -> JsValue {
        let serializer = serde_wasm_bindgen::Serializer::new()
            .serialize_maps_as_objects(true)
            .serialize_missing_as_null(true);
        self.replacement_v2_post_sort_memory_native()
            .serialize(&serializer)
            .unwrap_or(JsValue::NULL)
    }

    /// Load a root from serialized bytes (received from server or local cache).
    pub fn load_root(&mut self, root_bytes: &[u8]) -> Result<(), JsValue> {
        self.ensure_tree_job_idle()?;
        let root = VersionedRoot::deserialize(root_bytes)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        if root.vault_id() != self.vault_id {
            return Err(JsValue::from_str(
                "root vault id does not match the WASM tree",
            ));
        }

        // Root bytes live separately from the byte-addressed leaf/internal
        // store: RootNode::hash() is semantic and intentionally excludes its
        // history metadata, so it is not a Blake3 address for root_bytes.
        let replacement = match root {
            VersionedRoot::V1(root) => {
                let mut tree = TransactionalTree::new(&self.vault_id, &self.device_id);
                tree.load_root_without_chunks(root);
                WasmTreeInner::V1(tree)
            }
            VersionedRoot::V2(root) => {
                let mut tree = TransactionalTreeV2::new(&self.vault_id, &self.device_id);
                tree.load_root_without_chunks(root);
                WasmTreeInner::V2(tree)
            }
        };
        self.mutate_committed_native(|inner| {
            *inner = replacement;
            Ok(())
        })
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Get the current root hash as hex string.
    pub fn root_hash_hex(&self) -> Option<String> {
        match &self.inner {
            WasmTreeInner::V1(tree) => tree.committed_root_hash(),
            WasmTreeInner::V2(tree) => tree.committed_root_hash(),
        }
        .map(|hash| hash_to_hex(&hash))
    }

    /// Get the serialized root bytes for upload to server.
    pub fn root_bytes(&self) -> Result<Option<Vec<u8>>, JsValue> {
        match &self.inner {
            WasmTreeInner::V1(tree) => Ok(tree.committed_root().map(RootNode::serialize)),
            WasmTreeInner::V2(tree) => tree
                .committed_root()
                .map(|root| root.serialize())
                .transpose()
                .map_err(|error| JsValue::from_str(&error.to_string())),
        }
    }

    /// Get total file count in the tree.
    pub fn total_files(&self) -> f64 {
        match &self.inner {
            WasmTreeInner::V1(tree) => tree.committed_root().map(|root| root.total_files as f64),
            WasmTreeInner::V2(tree) => tree.committed_root().map(|root| root.total_files() as f64),
        }
        .unwrap_or(0.0)
    }

    /// Start an owner-local, non-mutating validation job for candidate open.
    /// The committed graph remains authoritative until finish_candidate_job.
    pub fn begin_candidate_job(&mut self) -> Result<u32, JsValue> {
        self.ensure_tree_job_idle()?;
        if self.has_candidate() {
            return Err(JsValue::from_str("candidate already active"));
        }
        let token = self.next_tree_job_token;
        let next = token
            .checked_add(1)
            .ok_or_else(|| JsValue::from_str("WASM tree job token space exhausted"))?;
        let job = match &self.inner {
            WasmTreeInner::V1(tree) => {
                if tree.committed_root().is_none() {
                    return Err(JsValue::from_str("no committed root"));
                }
                WasmTreeJob::V1 {
                    token,
                    purpose: WasmTreeJobPurpose::BeginCandidate,
                    cursor: ReachabilityCursor::new(),
                    next_root_child: 0,
                    prepared_candidate: None,
                }
            }
            WasmTreeInner::V2(tree) => {
                let root = tree
                    .committed_root()
                    .ok_or_else(|| JsValue::from_str("no committed root"))?;
                WasmTreeJob::V2 {
                    token,
                    purpose: WasmTreeJobPurpose::BeginCandidate,
                    cursor: V2ReachabilityCursor::new(&root.tree)
                        .map_err(|error| JsValue::from_str(&error.to_string()))?,
                    root_seeded: false,
                    prepared_candidate: None,
                }
            }
        };
        self.next_tree_job_token = next;
        self.tree_job = Some(job);
        Ok(token)
    }

    /// Start one traversal of the current candidate for both the complete
    /// reachable set and the transaction-fresh subset. The candidate and its
    /// original all-resident baseline stay immutable until finish.
    pub fn begin_candidate_chunks_job(&mut self) -> Result<u32, JsValue> {
        self.ensure_tree_job_idle()?;
        if !self.has_candidate() {
            return Err(JsValue::from_str("no active candidate"));
        }
        let token = self.next_tree_job_token;
        let next = token
            .checked_add(1)
            .ok_or_else(|| JsValue::from_str("WASM tree job token space exhausted"))?;
        let job = match &self.inner {
            WasmTreeInner::V1(tree) => {
                if tree.candidate_root().is_none() {
                    return Err(JsValue::from_str("no active candidate"));
                }
                WasmTreeJob::V1 {
                    token,
                    purpose: WasmTreeJobPurpose::CandidateChunks,
                    cursor: ReachabilityCursor::new(),
                    next_root_child: 0,
                    prepared_candidate: None,
                }
            }
            WasmTreeInner::V2(tree) => {
                let root = tree
                    .candidate_root()
                    .ok_or_else(|| JsValue::from_str("no active candidate"))?;
                WasmTreeJob::V2 {
                    token,
                    purpose: WasmTreeJobPurpose::CandidateChunks,
                    cursor: V2ReachabilityCursor::new(&root.tree)
                        .map_err(|error| JsValue::from_str(&error.to_string()))?,
                    root_seeded: false,
                    prepared_candidate: None,
                }
            }
        };
        self.next_tree_job_token = next;
        self.tree_job = Some(job);
        Ok(token)
    }

    /// Start exactly one candidate upsert operation. Parsing is bounded before
    /// any owner-local job is installed; the candidate remains unchanged until
    /// finish_candidate_mutation_job.
    pub fn begin_candidate_update_job(&mut self, entries_json: &str) -> Result<u32, JsValue> {
        self.ensure_candidate_mutation_admission(entries_json)?;
        let entries = parse_entries(entries_json)?;
        if entries.len() > MAX_CANDIDATE_MUTATION_ROWS {
            return Err(JsValue::from_str("candidate mutation has too many rows"));
        }
        self.install_candidate_mutation_job(entries, Vec::new())
    }

    /// Start exactly one candidate delete operation.
    pub fn begin_candidate_delete_job(&mut self, paths_json: &str) -> Result<u32, JsValue> {
        self.ensure_candidate_mutation_admission(paths_json)?;
        let paths = parse_paths(paths_json)?;
        if paths.len() > MAX_CANDIDATE_MUTATION_ROWS {
            return Err(JsValue::from_str("candidate mutation has too many rows"));
        }
        self.install_candidate_mutation_job(Vec::new(), paths)
    }

    fn ensure_candidate_mutation_admission(&self, input: &str) -> Result<(), JsValue> {
        self.ensure_tree_job_idle()?;
        if !self.has_candidate() {
            return Err(JsValue::from_str("no active candidate"));
        }
        if input.len() > MAX_CANDIDATE_MUTATION_JSON_BYTES {
            return Err(JsValue::from_str("candidate mutation JSON is too large"));
        }
        Ok(())
    }

    fn install_candidate_mutation_job(
        &mut self,
        changed: Vec<FileEntry>,
        deleted: Vec<String>,
    ) -> Result<u32, JsValue> {
        let token = self.next_tree_job_token;
        let next = token
            .checked_add(1)
            .ok_or_else(|| JsValue::from_str("WASM tree job token space exhausted"))?;
        let job = match &self.inner {
            WasmTreeInner::V1(tree) => {
                let output_plan = candidate_v1_output_memory_plan(tree, &changed, &deleted)
                    .map_err(|error| JsValue::from_str(&error.to_string()))?;
                if changed.is_empty() && deleted.is_empty() {
                    WasmTreeJob::MutationNoop { token }
                } else {
                    // Parsed inputs are already admitted by the caller. Do not
                    // clone the resident candidate root or allocate output
                    // until the host reserves and resumes the plan below.
                    WasmTreeJob::MutationV1 {
                        token,
                        cursor: None,
                        changed: Some(changed),
                        deleted: Some(deleted),
                        output_plan,
                        output_resumed: false,
                    }
                }
            }
            WasmTreeInner::V2(tree) => match tree
                .begin_candidate_mutation(changed, deleted)
                .map_err(|error| JsValue::from_str(&error.to_string()))?
            {
                Some(cursor) => WasmTreeJob::MutationV2 { token, cursor },
                None => WasmTreeJob::MutationNoop { token },
            },
        };
        self.next_tree_job_token = next;
        self.tree_job = Some(job);
        Ok(token)
    }

    /// Start a private, bounded input owner for an atomic committed-tree
    /// replacement. Feeding pages never changes the live graph or revisions.
    /// Start/step enable private cooperative construction; omitting start keeps
    /// the older synchronous-build finish behavior.
    pub fn begin_replacement_rebuild_job(
        &mut self,
        version: u32,
        expected_entries: u32,
        expected_json_bytes: u32,
    ) -> Result<u32, JsValue> {
        self.begin_replacement_rebuild_native(
            version,
            expected_entries as usize,
            expected_json_bytes as usize,
        )
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Parse and retain one non-empty page of at most 256 entries / 256 KiB.
    /// Invalid input, token or cardinality leaves the existing owner intact.
    /// Returns the cumulative accepted entry count.
    pub fn append_replacement_rebuild_job(
        &mut self,
        token: u32,
        expected_offset_entries: u32,
        page_json: &str,
    ) -> Result<f64, JsValue> {
        self.append_replacement_rebuild_native(token, expected_offset_entries as usize, page_json)
            .map(|received| received as f64)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Seal complete input into a private resumable builder. Without this
    /// optional call, finish preserves the older atomic-build compatibility.
    pub fn start_replacement_rebuild_job(&mut self, token: u32) -> Result<(), JsValue> {
        self.start_replacement_rebuild_native(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Read the complete V2 input's two index-buffer allocation requests before
    /// starting its sorter. This is not an input/builder/total-heap estimate.
    pub fn replacement_rebuild_sort_memory_plan_v1_job(
        &self,
        token: u32,
    ) -> Result<JsValue, JsValue> {
        let plan = self
            .replacement_rebuild_sort_memory_plan_v1_native(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        Ok(to_js(&plan))
    }

    /// Validate both admitted byte witnesses, then reserve both empty index
    /// buffers before moving the input. Reserve errors retain the same input
    /// job for retry/cancel; the host owns any admission lease separately.
    pub fn start_replacement_rebuild_sort_memory_v1_job(
        &mut self,
        token: u32,
        expected_source_index_bytes: f64,
        expected_target_index_bytes: f64,
    ) -> Result<(), JsValue> {
        self.start_replacement_rebuild_sort_memory_v1_native(
            token,
            expected_source_index_bytes,
            expected_target_index_bytes,
        )
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Advance private sorting/build/closure validation. A node codec/hash is
    /// still indivisible; no visible tree or revision changes during steps.
    pub fn step_replacement_rebuild_job(
        &mut self,
        token: u32,
        max_units: u32,
    ) -> Result<JsValue, JsValue> {
        let progress = self
            .step_replacement_rebuild_native(token, max_units)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        Ok(to_js(&serde_json::json!({
            "done": progress.done, "units": progress.units as f64,
            "completed": progress.completed as f64, "phase": progress.phase,
        })))
    }

    /// Output-admission-aware replacement step. Unlike the legacy generic
    /// method, this stops at the V1/V2 `plan ready` barrier.
    pub fn step_replacement_rebuild_output_memory_v1_job(
        &mut self,
        token: u32,
        max_units: u32,
    ) -> Result<JsValue, JsValue> {
        let progress = self
            .step_replacement_rebuild_output_memory_v1_native(token, max_units)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        Ok(to_js(&serde_json::json!({
            "done": progress.done, "units": progress.units as f64,
            "completed": progress.completed as f64, "phase": progress.phase,
        })))
    }

    /// Return the exact logical node payload plan at the V2 pre-encode
    /// boundary. The values exclude allocator capacity, planning scratch,
    /// metadata descriptors, the root owner and total WASM/RSS usage.
    pub fn replacement_rebuild_plan_job(&self, token: u32) -> Result<JsValue, JsValue> {
        let plan = self
            .replacement_rebuild_plan_native(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        Ok(to_js(&serde_json::json!({
            "nodeCount": plan.node_count as f64,
            "leafCount": plan.leaf_count as f64,
            "internalCount": plan.internal_count as f64,
            "nodePayloadBytes": plan.node_payload_bytes as f64,
            "maxNodeBytes": plan.max_node_bytes as f64,
            "storedRootBytes": plan.stored_root_bytes as f64,
        })))
    }

    /// Return the versioned conservative output-memory plan at the same V2
    /// pre-encode boundary. Endpoint fields are exact allocation requests for
    /// RangeRef Strings; the admission totals remain a scoped subset of heap.
    pub fn replacement_rebuild_output_memory_plan_v1_job(
        &self,
        token: u32,
    ) -> Result<JsValue, JsValue> {
        let plan = self
            .replacement_rebuild_output_memory_plan_v1_native(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let scope = match self.tree_job.as_ref() {
            Some(WasmTreeJob::ReplacementBuildV1 { token: owner, .. }) if *owner == token => {
                "v1-replacement-output"
            }
            Some(WasmTreeJob::ReplacementBuildV2 { token: owner, .. }) if *owner == token => {
                "v2-replacement-output"
            }
            _ => {
                return Err(JsValue::from_str(
                    "WASM tree job output memory kind mismatch",
                ))
            }
        };
        Ok(to_js(&serde_json::json!({
            "schema": 1,
            "scope": scope,
            "nodePayloadBytes": plan.node_payload_bytes as f64,
            "rangeEndpointPeakRequestedBytes": plan.range_endpoint_peak_requested_bytes as f64,
            "rangeEndpointResidentRequestedBytes": plan.range_endpoint_resident_requested_bytes as f64,
            "peakAdmissionBytes": plan.peak_admission_bytes as f64,
            "residentAdmissionBytes": plan.resident_admission_bytes as f64,
        })))
    }

    /// Acknowledge the exact plan witness after host-side preflight. A stale
    /// or rounded byte value cannot cross into the first node codec. The host
    /// decides separately whether that preflight includes a memory quota.
    pub fn resume_replacement_rebuild_job(
        &mut self,
        token: u32,
        expected_node_payload_bytes: f64,
    ) -> Result<(), JsValue> {
        const MAX_SAFE_INTEGER: f64 = ((1u64 << 53) - 1) as f64;
        if !expected_node_payload_bytes.is_finite()
            || expected_node_payload_bytes < 0.0
            || expected_node_payload_bytes > MAX_SAFE_INTEGER
            || expected_node_payload_bytes.fract() != 0.0
        {
            return Err(JsValue::from_str(
                "Tree v2 replacement plan bytes must be a safe integer",
            ));
        }
        self.resume_replacement_rebuild_native(token, expected_node_payload_bytes as u64)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Resume only when every independent component of the version-1 output
    /// memory plan still matches the host's admitted witness.
    pub fn resume_replacement_rebuild_output_memory_v1_job(
        &mut self,
        token: u32,
        expected_node_payload_bytes: f64,
        expected_range_endpoint_peak_requested_bytes: f64,
        expected_range_endpoint_resident_requested_bytes: f64,
    ) -> Result<(), JsValue> {
        fn safe_u64(value: f64) -> Option<u64> {
            const MAX_SAFE_INTEGER: f64 = ((1u64 << 53) - 1) as f64;
            (value.is_finite() && value >= 0.0 && value <= MAX_SAFE_INTEGER && value.fract() == 0.0)
                .then_some(value as u64)
        }

        let node_payload_bytes = safe_u64(expected_node_payload_bytes);
        let endpoint_peak_bytes = safe_u64(expected_range_endpoint_peak_requested_bytes);
        let endpoint_resident_bytes = safe_u64(expected_range_endpoint_resident_requested_bytes);
        let (Some(node_payload_bytes), Some(endpoint_peak_bytes), Some(endpoint_resident_bytes)) = (
            node_payload_bytes,
            endpoint_peak_bytes,
            endpoint_resident_bytes,
        ) else {
            return Err(JsValue::from_str(
                "replacement output memory witness must contain safe integers",
            ));
        };
        self.resume_replacement_rebuild_output_memory_v1_native(
            token,
            node_payload_bytes,
            endpoint_peak_bytes,
            endpoint_resident_bytes,
        )
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Consume complete private input or a ready verified builder, then publish
    /// the replacement with a single final swap. A ready invocation consumes
    /// the job even if revision preflight or private construction fails; the
    /// old live graph/revisions remain unchanged on every failure.
    pub fn finish_replacement_rebuild_job(&mut self, token: u32) -> Result<(), JsValue> {
        self.finish_replacement_rebuild_native(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Abandon input or a private builder without running its full destructor.
    /// The same token now exclusively owns retirement until step reports done.
    pub fn cancel_replacement_rebuild_job_deferred(&mut self, token: u32) -> Result<(), JsValue> {
        self.cancel_replacement_rebuild_deferred_native(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Publish a ready stepped replacement once, retaining its former live
    /// graph and builder scratch for bounded retirement under the same token.
    /// Errors leave the original build job available for deferred cancellation.
    pub fn finish_replacement_rebuild_job_deferred(&mut self, token: u32) -> Result<(), JsValue> {
        self.finish_replacement_rebuild_deferred_native(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Destroy at most max_units owned rows/descriptors/node buffers. This is
    /// a work-count bound, not a byte, deallocation-time or WASM RSS guarantee.
    /// Successful done consumes the token; misuse leaves retirement intact.
    pub fn step_tree_retirement(&mut self, token: u32, max_units: u32) -> Result<JsValue, JsValue> {
        let progress = self
            .step_tree_retirement_native(token, max_units)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        Ok(to_js(&serde_json::json!({
            "done": progress.done, "units": progress.units as f64,
            "completed": progress.completed as f64,
        })))
    }

    /// Replay-safe retirement step for the additive reachability family.
    /// Repeating a lost response with the same expected_completed returns the
    /// cached progress without destroying another owner. A completed cursor
    /// keeps the exclusive slot until finish acknowledges that exact count.
    pub fn step_reachability_retirement(
        &mut self,
        token: u32,
        expected_completed: u32,
        max_units: u32,
    ) -> Result<JsValue, JsValue> {
        self.step_reachability_retirement_native(token, expected_completed as usize, max_units)
            .map(|progress| to_js(&progress))
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Release an empty completed reachability owner. The last completed token
    /// is retained as an idempotency tombstone, so a lost JS return can retry.
    pub fn finish_reachability_retirement(
        &mut self,
        token: u32,
        expected_completed: u32,
    ) -> Result<(), JsValue> {
        self.finish_reachability_retirement_native(token, expected_completed as usize)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Pin one resident index object of the current tree for page export. A
    /// candidate is preferred when active; otherwise the committed tree is
    /// retained. The caller supplies a hash from its previously validated plan;
    /// this is not a new reachability proof and does not traverse the graph.
    /// The response is a plain JS object with exactly { token, length }.
    pub fn begin_tree_chunk_export_job(&mut self, hash_hex: &str) -> Result<JsValue, JsValue> {
        let info = self
            .begin_tree_chunk_export_inner(hash_hex)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
        match info.serialize(&serializer) {
            Ok(value) => Ok(value),
            Err(_) => {
                // The caller has not received its token. Release only the
                // metadata-only job created synchronously by this call.
                self.tree_job = None;
                Err(JsValue::from_str(
                    "could not encode chunk export descriptor",
                ))
            }
        }
    }

    /// Pin precisely the candidate root; unlike the index export API this
    /// never falls back to committed state. Begin performs no root clone,
    /// root hash or root serialization. The arena ceiling rejects impossible
    /// work incrementally during Plan. Step Plan before asking for workset.
    pub fn begin_candidate_root_export_job(
        &mut self,
        max_arena_bytes: u32,
    ) -> Result<u32, JsValue> {
        self.begin_root_export_with_limit_inner(RootExportScope::Candidate, max_arena_bytes)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Pin precisely the committed root, even if a candidate is also active.
    pub fn begin_committed_root_export_job(
        &mut self,
        max_arena_bytes: u32,
    ) -> Result<u32, JsValue> {
        self.begin_root_export_with_limit_inner(RootExportScope::Committed, max_arena_bytes)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Plan-ready only: plain { max_length, offset_bytes }. This describes
    /// the retained native arena plus child offsets, not total process memory.
    /// The host must separately admit its final JS buffer and page bridge.
    pub fn root_export_workset(&self, token: u32) -> Result<JsValue, JsValue> {
        let value = self
            .root_export_workset_inner(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        value
            .serialize(&serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true))
            .map_err(|_| JsValue::from_str("could not encode root export workset"))
    }

    /// Caller admission boundary. Starts Build and resets its progress. V1
    /// reserves without filling; subsequent steps initialize bounded pages.
    /// Allocation failure retains the Planned owner for cancel or retry.
    pub fn start_root_export_build_job(&mut self, token: u32) -> Result<(), JsValue> {
        self.start_root_export_build_inner(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Build-ready only: seal the retained output, returning plain
    /// { length, version, hash }. Hash is the semantic tree identity, not the
    /// digest of these bytes. Repeated info calls do not rewind page reads.
    pub fn root_export_info(&mut self, token: u32) -> Result<JsValue, JsValue> {
        let value = self
            .root_export_info_inner(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        value
            .serialize(&serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true))
            .map_err(|_| JsValue::from_str("could not encode root export info"))
    }

    /// Detached next part, <=64 KiB, with exact offset checking. Native read
    /// completion advances the offset; host bridge-copy failure requires
    /// cancel rather than assuming the completed read can be replayed.
    pub fn read_root_export_job(
        &mut self,
        token: u32,
        expected_offset: u32,
        max_bytes: u32,
    ) -> Result<Vec<u8>, JsValue> {
        self.read_root_export_inner(token, expected_offset, max_bytes)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Release at exact EOF only. Never commits a tree, sweeps chunks or ACKs.
    pub fn finish_root_export_job(&mut self, token: u32) -> Result<(), JsValue> {
        self.finish_root_export_inner(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Return the next exact part, at most 64 KiB. The generated Vec<u8>
    /// binding returns a detached JS copy, never a view into growing WASM
    /// memory. Rejected native reads retain ownership and the previous offset.
    /// If the host cannot allocate its bridge copy, it must cancel the token;
    /// it must not assume that the completed native read can be replayed.
    pub fn read_tree_chunk_export_job(
        &mut self,
        token: u32,
        expected_offset: u32,
        max_bytes: u32,
    ) -> Result<Vec<u8>, JsValue> {
        self.read_tree_chunk_export_inner(token, expected_offset, max_bytes)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Release export ownership only at exact EOF. This does not commit,
    /// serialize a root, acknowledge work or change any resident object.
    pub fn finish_tree_chunk_export_job(&mut self, token: u32) -> Result<(), JsValue> {
        self.finish_tree_chunk_export_inner(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Run bounded graph traversal or candidate-mutation primitives. An
    /// execution error drops that provisional job; invalid budgets/tokens and
    /// chunk/root-export misuse retain ownership. Root export has a dedicated
    /// byte-budgeted step API below. This never opens a candidate or sweeps
    /// the store.
    pub fn step_tree_job(&mut self, token: u32, max_units: u32) -> Result<JsValue, JsValue> {
        self.step_tree_job_native(token, max_units)
            .map(|progress| to_js(&progress))
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Opt in to the candidate-mutation output admission contract. Unlike
    /// the legacy generic step family, this method stops at `plan ready` and
    /// exposes that phase without crossing the first node-codec boundary.
    pub fn step_candidate_mutation_output_memory_v1_job(
        &mut self,
        token: u32,
        max_units: u32,
    ) -> Result<JsValue, JsValue> {
        let progress = self
            .step_candidate_mutation_output_memory_v1_native(token, max_units)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        Ok(to_js(&serde_json::json!({
            "done": progress.done,
            "units": progress.units as f64,
            "completed": progress.completed as f64,
            "remaining": if progress.done { 0.0 } else { 1.0 },
            "reachable": progress.staged_chunks as f64,
            "phase": progress.phase,
        })))
    }

    /// Return the versioned conservative allocation-request plan at the
    /// candidate mutation's pre-codec barrier.
    pub fn candidate_mutation_output_memory_plan_v1_job(
        &self,
        token: u32,
    ) -> Result<JsValue, JsValue> {
        let plan = self
            .candidate_mutation_output_memory_plan_v1_native(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let scope = match (&self.tree_job, &self.inner) {
            (Some(WasmTreeJob::MutationV1 { token: owner, .. }), WasmTreeInner::V1(_))
                if *owner == token =>
            {
                "v1-candidate-mutation-output"
            }
            (Some(WasmTreeJob::MutationV2 { token: owner, .. }), WasmTreeInner::V2(_))
                if *owner == token =>
            {
                "v2-candidate-mutation-output"
            }
            _ => {
                return Err(JsValue::from_str(
                    "candidate mutation output plan kind mismatch",
                ))
            }
        };
        Ok(to_js(&serde_json::json!({
            "schema": 1,
            "scope": scope,
            "nodePayloadBytes": plan.node_payload_bytes as f64,
            "rangeEndpointPeakRequestedBytes": plan.range_endpoint_peak_requested_bytes as f64,
            "rangeEndpointResidentRequestedBytes": plan.range_endpoint_resident_requested_bytes as f64,
            "peakAdmissionBytes": plan.peak_admission_bytes as f64,
            "residentAdmissionBytes": plan.resident_admission_bytes as f64,
        })))
    }

    /// Resume only when every independently planned byte component exactly
    /// matches the host's admitted witness. Validation failures preserve the
    /// same owner at the barrier.
    pub fn resume_candidate_mutation_output_memory_v1_job(
        &mut self,
        token: u32,
        expected_node_payload_bytes: f64,
        expected_range_endpoint_peak_requested_bytes: f64,
        expected_range_endpoint_resident_requested_bytes: f64,
    ) -> Result<(), JsValue> {
        let node_payload_bytes = safe_integer_u64(expected_node_payload_bytes);
        let endpoint_peak_bytes = safe_integer_u64(expected_range_endpoint_peak_requested_bytes);
        let endpoint_resident_bytes =
            safe_integer_u64(expected_range_endpoint_resident_requested_bytes);
        let (Some(node_payload_bytes), Some(endpoint_peak_bytes), Some(endpoint_resident_bytes)) = (
            node_payload_bytes,
            endpoint_peak_bytes,
            endpoint_resident_bytes,
        ) else {
            return Err(JsValue::from_str(
                "candidate mutation output memory witness must contain safe integers",
            ));
        };
        self.resume_candidate_mutation_output_memory_v1_native(
            token,
            node_payload_bytes,
            endpoint_peak_bytes,
            endpoint_resident_bytes,
        )
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Report the exact private overlay and root-endpoint bytes retained by a
    /// ready mutation. This remains query-only until atomic publication.
    pub fn candidate_mutation_output_memory_ready_v1_job(
        &self,
        token: u32,
    ) -> Result<JsValue, JsValue> {
        let ready = self
            .candidate_mutation_output_memory_ready_v1_native(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let scope = match (&self.tree_job, &self.inner) {
            (Some(WasmTreeJob::MutationV1 { token: owner, .. }), WasmTreeInner::V1(_))
                if *owner == token =>
            {
                "v1-candidate-mutation-output"
            }
            (Some(WasmTreeJob::MutationV2 { token: owner, .. }), WasmTreeInner::V2(_))
                if *owner == token =>
            {
                "v2-candidate-mutation-output"
            }
            (Some(WasmTreeJob::MutationNoop { token: owner }), WasmTreeInner::V1(_))
                if *owner == token =>
            {
                "v1-candidate-mutation-output"
            }
            (Some(WasmTreeJob::MutationNoop { token: owner }), WasmTreeInner::V2(_))
                if *owner == token =>
            {
                "v2-candidate-mutation-output"
            }
            _ => {
                return Err(JsValue::from_str(
                    "candidate mutation output Ready kind mismatch",
                ))
            }
        };
        Ok(to_js(&serde_json::json!({
            "schema": 1,
            "scope": scope,
            "stagedNodePayloadBytes": ready.staged_node_payload_bytes as f64,
            "rangeEndpointResidentRequestedBytes": ready.range_endpoint_resident_requested_bytes as f64,
            "residentAdmissionBytes": ready.resident_admission_bytes as f64,
        })))
    }

    /// Opt-in reachability traversal contract. Execution failures preserve the
    /// cursor as same-token retirement; validation misuse leaves the active
    /// traversal unchanged. Hosts selecting this export must also use the
    /// matching deferred cancel/finish family and drain every retirement.
    pub fn step_reachability_job_deferred(
        &mut self,
        token: u32,
        max_units: u32,
    ) -> Result<JsValue, JsValue> {
        self.step_reachability_job_deferred_native(token, max_units)
            .map(|progress| to_js(&progress))
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Run root serialization primitives under independent operation and
    /// scheduled-byte guards. `bytes` is conservative work accounting, not a
    /// measurement of allocator traffic, heap/RSS, or elapsed time. A single
    /// bounded atomic primitive may exceed a smaller byte budget; it is then
    /// the only unit executed by that call. Plan and Build counters reset.
    pub fn step_root_export_job(
        &mut self,
        token: u32,
        max_units: u32,
        max_bytes: u32,
    ) -> Result<JsValue, JsValue> {
        self.step_root_export_job_native(token, max_units, max_bytes)
            .map(|progress| to_js(&progress))
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Inspect the exact root clone requests for a ready, unprepared candidate
    /// job. No root clone or resident-key snapshot is allocated.
    pub fn candidate_open_memory_plan_v1_job(&self, token: u32) -> Result<JsValue, JsValue> {
        self.candidate_open_memory_plan_v1_native(token)
            .map(|plan| to_js(&plan))
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Prepare exact-request root owners without publishing a candidate.
    /// Its all-resident baseline is a scalar insertion-generation cut, not a
    /// HashSet snapshot. Failed preparation keeps the ready job for retry/cancel.
    pub fn resume_candidate_open_memory_v1_job(
        &mut self,
        token: u32,
        expected_identity_bytes: f64,
        expected_endpoint_bytes: f64,
        expected_root_string_bytes: f64,
    ) -> Result<(), JsValue> {
        self.resume_candidate_open_memory_v1_native(
            token,
            expected_identity_bytes,
            expected_endpoint_bytes,
            expected_root_string_bytes,
        )
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Publish a validated candidate. V2 moves a prepared root (or prepares it
    /// recoverably here) and opens an O(1) all-resident generation baseline.
    /// Legacy cursor destruction remains synchronous.
    pub fn finish_candidate_job(&mut self, token: u32) -> Result<f64, JsValue> {
        self.finish_candidate_job_native(token)
            .map(|reachable| reachable as f64)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Publish a validated candidate, then retain the traversal cursor under
    /// the same token for cooperative destruction. V2 moves its prepared root
    /// and opens an O(1) baseline; without prior resume its exact-request root
    /// preparation remains synchronous. V1 keeps its legacy preparation.
    pub fn finish_candidate_job_deferred(&mut self, token: u32) -> Result<f64, JsValue> {
        self.finish_candidate_job_deferred_native(token)
            .map(|reachable| reachable as f64)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Finish one validated candidate traversal and return sorted full/fresh
    /// hash arrays without walking the graph a second time.
    pub fn finish_candidate_chunks_job(&mut self, token: u32) -> Result<JsValue, JsValue> {
        if matches!(
            self.tree_job.as_ref(),
            Some(WasmTreeJob::CandidateChunkSort { .. })
        ) {
            return Err(JsValue::from_str(
                "candidate chunk sort requires its versioned page API",
            ));
        }
        let job = self
            .tree_job
            .take()
            .ok_or_else(|| JsValue::from_str("no active WASM tree job"))?;
        if job.token() != token {
            self.tree_job = Some(job);
            return Err(JsValue::from_str("WASM tree job token mismatch"));
        }
        if job.purpose() != WasmTreeJobPurpose::CandidateChunks {
            self.tree_job = Some(job);
            return Err(JsValue::from_str("WASM tree job finish kind mismatch"));
        }
        if !job.ready() {
            self.tree_job = Some(job);
            return Err(JsValue::from_str("WASM tree job is incomplete"));
        }
        let reachable = match job {
            WasmTreeJob::V1 { cursor, .. } => cursor.finish(),
            WasmTreeJob::V2 { cursor, .. } => cursor.finish(),
            _ => unreachable!("job purpose checked before chunk finish"),
        }
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let (all, fresh) = match &self.inner {
            WasmTreeInner::V1(tree) => tree.candidate_chunk_plan_from_reachable(reachable),
            WasmTreeInner::V2(tree) => tree.candidate_chunk_plan_from_reachable(reachable),
        }
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
        Ok(to_js(&WasmCandidateChunkPlan {
            all: hex_hashes(all),
            fresh: hex_hashes(fresh),
        }))
    }

    /// Preserve the descriptor cursor for same-token retirement after the
    /// exact legacy chunk plan is produced. Sorting, hex conversion and output
    /// serialization are intentionally still synchronous residuals.
    pub fn finish_candidate_chunks_job_deferred(&mut self, token: u32) -> Result<JsValue, JsValue> {
        self.finish_candidate_chunks_deferred_native(token)
            .map(|plan| to_js(&plan))
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Exact requested-byte admission for the two contiguous radix-sort
    /// workspaces. The existing reachable HashSet, allocator metadata and
    /// bounded page output are explicitly outside this component scope.
    pub fn candidate_chunks_sort_memory_plan_v1_job(&self, token: u32) -> Result<JsValue, JsValue> {
        self.candidate_chunks_sort_memory_plan_v1_native(token)
            .map(|plan| to_js(&plan))
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Validate the exact admission witnesses and convert the ready traversal
    /// into a same-token cooperative radix-sort cursor. Allocation failure
    /// leaves the traversal retryable.
    pub fn resume_candidate_chunks_sort_memory_v1_job(
        &mut self,
        token: u32,
        expected_source_bytes: f64,
        expected_scratch_bytes: f64,
    ) -> Result<(), JsValue> {
        self.resume_candidate_chunks_sort_memory_v1_native(
            token,
            expected_source_bytes,
            expected_scratch_bytes,
        )
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Advance stable LSD radix sorting by at most 4096 hash-local units.
    pub fn step_candidate_chunks_sort_v1_job(
        &mut self,
        token: u32,
        max_units: u32,
    ) -> Result<JsValue, JsValue> {
        self.step_candidate_chunks_sort_v1_native(token, max_units)
            .map(|progress| to_js(&progress))
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Describe a completed sorted plan before bounded page reads begin.
    pub fn candidate_chunks_plan_info_v1_job(&self, token: u32) -> Result<JsValue, JsValue> {
        self.candidate_chunks_plan_info_v1_native(token)
            .map(|info| to_js(&info))
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Hex-encode one exact sequential page. `fresh` is the page-local sorted
    /// subset relative to the pinned all-resident candidate-open baseline.
    pub fn read_candidate_chunks_page_v1_job(
        &mut self,
        token: u32,
        expected_offset: f64,
        max_hashes: u32,
    ) -> Result<JsValue, JsValue> {
        let expected_offset = safe_integer_u64(expected_offset)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| {
                JsValue::from_str("candidate chunk page offset must be a safe integer")
            })?;
        self.read_candidate_chunks_page_v1_native(token, expected_offset, max_hashes)
            .map(|page| to_js(&page))
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Require exact EOF, then move traversal and sort owners to same-token
    /// replay-safe reachability retirement.
    pub fn finish_candidate_chunks_plan_v1_job(&mut self, token: u32) -> Result<(), JsValue> {
        self.finish_candidate_chunks_plan_v1_native(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Atomically promote one fully stepped candidate mutation. Finish errors
    /// retain the job token so the host can run its cleanup contract.
    pub fn finish_candidate_mutation_job(&mut self, token: u32) -> Result<(), JsValue> {
        self.finish_candidate_mutation_native(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Publish a ready V2 candidate mutation, then keep every residual cursor
    /// owner under the same token for cooperative `step_tree_retirement`.
    /// Finish failures preserve the original job for deferred cancellation.
    pub fn finish_candidate_mutation_job_deferred(&mut self, token: u32) -> Result<(), JsValue> {
        self.finish_candidate_mutation_deferred_native(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Convert an unfinished/failed/ready V2 candidate mutation to same-token
    /// cooperative retirement without changing candidate state or revision.
    /// Repeating this exact transition is idempotent.
    pub fn cancel_candidate_mutation_job_deferred(&mut self, token: u32) -> Result<(), JsValue> {
        self.cancel_candidate_mutation_deferred_native(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Drop only the matching provisional job. Roots, chunks and candidate
    /// state are unchanged.
    pub fn cancel_tree_job(&mut self, token: u32) -> Result<(), JsValue> {
        self.cancel_tree_job_inner(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Move a candidate-open/chunk reachability cursor to same-token bounded
    /// retirement. Repeating this exact transition is idempotent; wrong-token
    /// and non-reachability jobs remain untouched.
    pub fn cancel_reachability_job_deferred(&mut self, token: u32) -> Result<(), JsValue> {
        self.cancel_reachability_job_deferred_native(token)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Begin an isolated candidate rooted at the committed graph.
    pub fn begin_candidate(&mut self) -> Result<(), JsValue> {
        self.ensure_tree_job_idle()?;
        self.mutate_candidate_native(true, |inner, _| match inner {
            WasmTreeInner::V1(tree) => tree.begin_candidate(),
            WasmTreeInner::V2(tree) => run_local(tree.begin_candidate()),
        })
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    pub fn has_candidate(&self) -> bool {
        match &self.inner {
            WasmTreeInner::V1(tree) => tree.has_candidate(),
            WasmTreeInner::V2(tree) => tree.has_candidate(),
        }
    }

    pub fn candidate_root_hash_hex(&self) -> Option<String> {
        match &self.inner {
            WasmTreeInner::V1(tree) => tree.candidate_root_hash(),
            WasmTreeInner::V2(tree) => tree.candidate_root_hash(),
        }
        .map(|hash| hash_to_hex(&hash))
    }

    pub fn candidate_root_bytes(&self) -> Result<Option<Vec<u8>>, JsValue> {
        match &self.inner {
            WasmTreeInner::V1(tree) => Ok(tree.candidate_root().map(RootNode::serialize)),
            WasmTreeInner::V2(tree) => tree
                .candidate_root()
                .map(|root| root.serialize())
                .transpose()
                .map_err(|error| JsValue::from_str(&error.to_string())),
        }
    }

    pub fn candidate_total_files(&self) -> f64 {
        match &self.inner {
            WasmTreeInner::V1(tree) => tree.candidate_root().map(|root| root.total_files as f64),
            WasmTreeInner::V2(tree) => tree.candidate_root().map(|root| root.total_files() as f64),
        }
        .unwrap_or(0.0)
    }

    /// Atomically promote a V2 candidate, sweep unreachable node buffers and
    /// return the exact stable requested-output total under the post-commit
    /// wrapper revisions. A null/malformed serialized value is not permission
    /// for the host to release accounting, but the commit remains successful.
    pub fn commit_candidate_output_settlement_v1(&mut self) -> Result<JsValue, JsValue> {
        self.commit_candidate_output_settlement_v1_native()
            .map(|report| to_js(&report))
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Atomically abort a V2 candidate, sweep its unreachable node buffers and
    /// return the exact stable requested-output total under the post-abort
    /// candidate revision. Legacy Tree v1 keeps the old abort ABI only.
    pub fn abort_candidate_output_settlement_v1(&mut self) -> Result<JsValue, JsValue> {
        self.abort_candidate_output_settlement_v1_native()
            .map(|report| to_js(&report))
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Promote the candidate only after the server has accepted its root.
    pub fn commit_candidate(&mut self) -> Result<JsValue, JsValue> {
        self.ensure_tree_job_idle()?;
        let stats = self
            .mutate_committed_native(|inner| match inner {
                WasmTreeInner::V1(tree) => tree.commit_candidate(),
                WasmTreeInner::V2(tree) => run_local(tree.commit_candidate()),
            })
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        Ok(to_js(&stats))
    }

    /// Discard a failed candidate and retain only the committed graph.
    pub fn abort_candidate(&mut self) -> Result<JsValue, JsValue> {
        self.ensure_tree_job_idle()?;
        self.mutate_candidate_native(true, |inner, _| match inner {
            WasmTreeInner::V1(tree) => tree.abort_candidate(),
            WasmTreeInner::V2(tree) => run_local(tree.abort_candidate()),
        })
        .map(|stats| to_js(&stats))
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Update a single file entry in the tree.
    pub fn update_entry(
        &mut self,
        path: &str,
        hash_hex: &str,
        mtime_ms: f64,
        size: f64,
    ) -> Result<(), JsValue> {
        self.ensure_tree_job_idle()?;
        let file_hash = hex_to_hash(hash_hex)
            .map_err(|error| JsValue::from_str(&format!("invalid hash: {error}")))?;
        let entry = FileEntry::new(path.to_owned(), file_hash, mtime_ms as u64, size as u64);
        self.mutate_committed_native(|inner| match inner {
            WasmTreeInner::V1(tree) => run_local(tree.apply_committed(&[entry], &[])),
            WasmTreeInner::V2(tree) => run_local(tree.apply_committed(&[entry], &[])),
        })
        .map(|_| ())
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Delete a file entry from the tree.
    pub fn delete_entry(&mut self, path: &str) -> Result<(), JsValue> {
        self.ensure_tree_job_idle()?;
        self.mutate_committed_native(|inner| match inner {
            WasmTreeInner::V1(tree) => run_local(tree.apply_committed(&[], &[path.to_owned()])),
            WasmTreeInner::V2(tree) => run_local(tree.apply_committed(&[], &[path.to_owned()])),
        })
        .map(|_| ())
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Apply a batch of upserts in ONE update_tree call.
    ///
    /// Calling update_entry N times for N files in the same directory causes N
    /// separate update_tree invocations, each reloading + rebuilding the same
    /// leaf chunk: O(N × prefix_size). This method passes all N entries at once
    /// so update_tree groups them by prefix and rebuilds each prefix only once:
    /// O(N + prefix_size).
    ///
    /// Input: [{ "path": "...", "hash": "hex", "mtime_ms": u64, "size": u64 }, ...]
    pub fn update_batch(&mut self, entries_json: &str) -> Result<(), JsValue> {
        self.ensure_tree_job_idle()?;
        let entries = parse_entries(entries_json)?;
        if entries.is_empty() {
            return Ok(());
        }
        self.mutate_committed_native(|inner| match inner {
            WasmTreeInner::V1(tree) => run_local(tree.apply_committed(&entries, &[])),
            WasmTreeInner::V2(tree) => run_local(tree.apply_committed(&entries, &[])),
        })
        .map(|_| ())
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Delete a batch of paths in ONE update_tree call.
    /// Same O(N × prefix_size) → O(N + prefix_size) win as update_batch.
    ///
    /// Input: ["path/a.md", "path/b.md", ...]
    pub fn delete_batch(&mut self, paths_json: &str) -> Result<(), JsValue> {
        self.ensure_tree_job_idle()?;
        let paths = parse_paths(paths_json)?;

        if paths.is_empty() {
            return Ok(());
        }

        self.mutate_committed_native(|inner| match inner {
            WasmTreeInner::V1(tree) => run_local(tree.apply_committed(&[], &paths)),
            WasmTreeInner::V2(tree) => run_local(tree.apply_committed(&[], &paths)),
        })
        .map(|_| ())
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    pub fn candidate_update_batch(&mut self, entries_json: &str) -> Result<(), JsValue> {
        self.ensure_tree_job_idle()?;
        let entries = parse_entries(entries_json)?;
        self.mutate_candidate_native(!entries.is_empty(), |inner, _| match inner {
            WasmTreeInner::V1(tree) => run_local(tree.apply_candidate(&entries, &[])),
            WasmTreeInner::V2(tree) => run_local(tree.apply_candidate(&entries, &[])),
        })
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    pub fn candidate_delete_batch(&mut self, paths_json: &str) -> Result<(), JsValue> {
        self.ensure_tree_job_idle()?;
        let paths = parse_paths(paths_json)?;
        self.mutate_candidate_native(!paths.is_empty(), |inner, _| match inner {
            WasmTreeInner::V1(tree) => run_local(tree.apply_candidate(&[], &paths)),
            WasmTreeInner::V2(tree) => run_local(tree.apply_candidate(&[], &paths)),
        })
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Build a tree from scratch given a JSON array of file entries.
    ///
    /// Input: [{ "path": "...", "hash": "hex", "mtime_ms": u64, "size": u64 }, ...]
    pub fn build_from_entries(&mut self, entries_json: &str) -> Result<(), JsValue> {
        self.ensure_tree_job_idle()?;
        let entries = parse_entries(entries_json)?;
        self.mutate_committed_native(|inner| match inner {
            WasmTreeInner::V1(tree) => run_local(tree.rebuild(entries)),
            WasmTreeInner::V2(tree) => run_local(tree.rebuild(entries)),
        })
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Atomically rebuild the semantic sync-base snapshot in a negotiated
    /// tree format. Unlike `set_tree_version`, this is deliberately valid for
    /// a populated tree: a live server-side v1/v2 transition must replace the
    /// whole immutable graph without exposing an empty or half-built state.
    pub fn rebuild_from_entries_in_version(
        &mut self,
        version: u32,
        entries_json: &str,
    ) -> Result<(), JsValue> {
        self.ensure_tree_job_idle()?;
        let entries = parse_entries(entries_json)?;
        let replacement = build_replacement_tree(version, &self.vault_id, &self.device_id, entries)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        self.mutate_committed_native(|inner| {
            *inner = replacement;
            Ok(())
        })
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }
}

/// Get the serialized bytes of a chunk from the WASM tree's internal store.
/// Called by the plugin to upload individual chunks to the server.
#[wasm_bindgen]
pub fn wasm_tree_get_chunk(tree: &WasmTree, hash_hex: &str) -> Option<Vec<u8>> {
    let hash = hex_to_hash(hash_hex).ok()?;
    match &tree.inner {
        WasmTreeInner::V1(inner) => inner.chunk_bytes(&hash),
        WasmTreeInner::V2(inner) => inner.chunk_bytes(&hash),
    }
}

/// Inspect a resident index chunk before reserving upload memory. This reads
/// only the stored Vec length: it does not clone, serialize or export payload
/// bytes. Invalid/missing hashes return undefined in JS; an empty chunk is 0.
#[wasm_bindgen]
pub fn wasm_tree_chunk_byte_length(tree: &WasmTree, hash_hex: &str) -> Option<u32> {
    let hash = hex_to_hash(hash_hex).ok()?;
    let length = match &tree.inner {
        WasmTreeInner::V1(inner) => inner.chunk_byte_length(&hash),
        WasmTreeInner::V2(inner) => inner.chunk_byte_length(&hash),
    }?;
    u32::try_from(length).ok()
}

#[cfg(test)]
mod root_export_tests {
    use super::*;
    use crate::store::MemoryChunkStore;
    use std::collections::BTreeMap;

    fn store(tree: &WasmTree) -> &MemoryChunkStore {
        match &tree.inner {
            WasmTreeInner::V1(inner) => inner.resident_store_for_test(),
            WasmTreeInner::V2(inner) => inner.resident_store_for_test(),
        }
    }

    fn stable_v2_output_oracle(tree: &WasmTree) -> (u64, u64, u64) {
        let payload = store(tree)
            .all_chunks()
            .into_iter()
            .map(|(_, bytes)| bytes.len() as u64)
            .sum::<u64>();
        let endpoint = match &tree.inner {
            WasmTreeInner::V2(inner) => inner
                .committed_root()
                .and_then(|root| root.tree.child.as_ref())
                .map_or(0, |child| {
                    (child.min_path.len() + child.max_path.len()) as u64
                }),
            WasmTreeInner::V1(_) => panic!("V2 stable output oracle received Tree v1"),
        };
        (payload, endpoint, payload + endpoint)
    }

    fn state(
        tree: &WasmTree,
    ) -> (
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        BTreeMap<FileHash, Vec<u8>>,
    ) {
        (
            tree.root_bytes().unwrap(),
            tree.candidate_root_bytes().unwrap(),
            store(tree).all_chunks().into_iter().collect(),
        )
    }

    fn fixture(version: u32, candidate: bool) -> WasmTree {
        let mut tree = WasmTree::new("root-export", "device");
        let entries: Vec<_> = (0u32..12)
            .map(|index| {
                FileEntry::new(
                    format!("dir{index}/note.md"),
                    hash_bytes(&index.to_le_bytes()),
                    index as u64,
                    8,
                )
            })
            .collect();
        let changed = vec![FileEntry::new(
            "dir0/changed.md".into(),
            hash_bytes(b"new bytes"),
            99,
            9,
        )];
        if version == TREE_V2 {
            tree.inner = WasmTreeInner::V2(TransactionalTreeV2::new("root-export", "device"));
        }
        match &mut tree.inner {
            WasmTreeInner::V1(inner) => {
                run_local(inner.rebuild(entries)).unwrap();
                if candidate {
                    inner.begin_candidate().unwrap();
                    run_local(inner.apply_candidate(&changed, &["dir1/note.md".into()])).unwrap();
                }
            }
            WasmTreeInner::V2(inner) => {
                run_local(inner.rebuild(entries)).unwrap();
                if candidate {
                    run_local(inner.begin_candidate()).unwrap();
                    run_local(inner.apply_candidate(&changed, &["dir1/note.md".into()])).unwrap();
                }
            }
        }
        tree
    }

    fn drain(tree: &mut WasmTree, token: u32, budget: u32) -> usize {
        let mut completed = None;
        let mut processed = None;
        let mut units = 0;
        loop {
            let progress = tree
                .step_root_export_job_native(token, budget, ROOT_EXPORT_MAX_STEP_BYTES as u32)
                .unwrap();
            assert!(progress.units <= budget as f64);
            if let Some(previous) = completed {
                assert_eq!(progress.completed, previous + progress.units);
            }
            if let Some(previous) = processed {
                assert_eq!(progress.processed, previous + progress.bytes);
            }
            completed = Some(progress.completed);
            processed = Some(progress.processed);
            units += progress.units as usize;
            assert!(tree.ensure_tree_job_idle_native().is_err());
            assert!(progress.done || progress.units > 0.0);
            if progress.done {
                return units;
            }
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct RebuildSnapshot {
        committed: Option<Vec<u8>>,
        candidate: Option<Vec<u8>>,
        resident: BTreeMap<FileHash, Vec<u8>>,
        fresh_candidate: Vec<FileHash>,
        committed_revision: u64,
        candidate_revision: u64,
        version: u32,
    }

    fn rebuild_snapshot(tree: &WasmTree) -> RebuildSnapshot {
        let (committed, candidate, resident) = state(tree);
        let fresh_candidate = if tree.has_candidate() {
            match &tree.inner {
                WasmTreeInner::V1(inner) => inner.new_candidate_chunk_hashes().unwrap(),
                WasmTreeInner::V2(inner) => run_local(inner.new_candidate_chunk_hashes()).unwrap(),
            }
        } else {
            Vec::new()
        };
        RebuildSnapshot {
            committed,
            candidate,
            resident,
            fresh_candidate,
            committed_revision: tree.committed_revision,
            candidate_revision: tree.candidate_revision,
            version: tree.tree_version(),
        }
    }

    fn rebuild_page(rows: &[FileEntry]) -> String {
        serde_json::to_string(
            &rows
                .iter()
                .map(|row| {
                    serde_json::json!({
                        "path": row.path, "hash": hash_to_hex(&row.hash),
                        "mtime_ms": row.mtime_ms, "size": row.size_bytes,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    fn rebuild_row(path: &str, value: u8) -> FileEntry {
        FileEntry::new(
            path.into(),
            hash_bytes(&[value]),
            value as u64,
            value as u64,
        )
    }

    fn rebuild_input(tree: &WasmTree) -> (u32, usize, usize, Vec<FileEntry>, usize, usize) {
        let Some(WasmTreeJob::ReplacementRebuild {
            token,
            expected_entries,
            received_bytes,
            entries,
            ..
        }) = &tree.tree_job
        else {
            panic!("replacement input owner was lost")
        };
        (
            *token,
            *expected_entries,
            *received_bytes,
            entries.clone(),
            entries.capacity(),
            entries.as_ptr() as usize,
        )
    }

    fn start_test_replacement(tree: &mut WasmTree, version: u32, rows: &[FileEntry]) -> u32 {
        let json = rebuild_page(rows);
        let token = tree
            .begin_replacement_rebuild_native(version, rows.len(), json.len())
            .unwrap();
        for (index, page) in rows.chunks(256).enumerate() {
            tree.append_replacement_rebuild_native(token, index * 256, &rebuild_page(page))
                .unwrap();
        }
        tree.start_replacement_rebuild_native(token).unwrap();
        token
    }

    fn resume_test_replacement_plan(
        tree: &mut WasmTree,
        token: u32,
        progress: ReplacementBuildProgress,
    ) {
        if progress.phase != "plan ready" {
            return;
        }
        let plan = tree
            .replacement_rebuild_output_memory_plan_v1_native(token)
            .unwrap();
        if matches!(tree.tree_job, Some(WasmTreeJob::ReplacementBuildV2 { .. })) {
            let node = tree.replacement_rebuild_plan_native(token).unwrap();
            assert_eq!(plan.node_payload_bytes, node.node_payload_bytes);
        }
        assert_eq!(
            tree.chunk_memory_native().replacement,
            ChunkStoreMemory::default()
        );
        tree.resume_replacement_rebuild_output_memory_v1_native(
            token,
            plan.node_payload_bytes,
            plan.range_endpoint_peak_requested_bytes,
            plan.range_endpoint_resident_requested_bytes,
        )
        .unwrap();
    }

    fn finish_test_replacement_steps(tree: &mut WasmTree, token: u32, budget: u32) {
        finish_test_replacement_steps_from(tree, token, budget, 0);
    }

    fn finish_test_replacement_steps_from(
        tree: &mut WasmTree,
        token: u32,
        budget: u32,
        mut completed: usize,
    ) {
        let before = rebuild_snapshot(tree);
        for _ in 0..1_000_000 {
            let progress = tree.step_replacement_rebuild_native(token, budget).unwrap();
            assert!(progress.units <= budget as usize);
            assert!(progress.done || progress.units > 0 || progress.phase == "plan ready");
            completed += progress.units;
            assert_eq!(progress.completed, completed);
            assert_eq!(rebuild_snapshot(tree), before);
            resume_test_replacement_plan(tree, token, progress);
            if progress.done {
                let ready = tree.step_replacement_rebuild_native(token, budget).unwrap();
                assert!(ready.done);
                assert_eq!((ready.units, ready.completed), (0, completed));
                return;
            }
        }
        panic!("replacement builder did not reach ready");
    }

    #[test]
    fn replacement_output_plan_is_token_scoped_and_required_before_encoding() {
        let mut tree = fixture(TREE_V1, true);
        let before = rebuild_snapshot(&tree);
        let rows: Vec<_> = (0..2_000)
            .rev()
            .map(|index| rebuild_row(&format!("plan/{index:05}.md"), (index % 255) as u8))
            .collect();
        let token = start_test_replacement(&mut tree, TREE_V2, &rows);
        let progress = loop {
            let progress = tree.step_replacement_rebuild_native(token, 256).unwrap();
            assert!(!progress.done);
            assert_eq!(rebuild_snapshot(&tree), before);
            if progress.phase == "plan ready" {
                break progress;
            }
        };
        assert!(progress.units > 0);
        assert_eq!(
            tree.chunk_memory_native().replacement,
            ChunkStoreMemory::default()
        );
        assert!(tree.replacement_rebuild_plan_native(token + 1).is_err());
        assert!(tree
            .replacement_rebuild_output_memory_plan_v1_native(token + 1)
            .is_err());
        assert!(tree
            .resume_replacement_rebuild_native(token + 1, 0)
            .is_err());
        let plan = tree.replacement_rebuild_plan_native(token).unwrap();
        let output_plan = tree
            .replacement_rebuild_output_memory_plan_v1_native(token)
            .unwrap();
        assert_eq!(plan.node_count, plan.leaf_count + plan.internal_count);
        assert!(plan.node_payload_bytes > 0);
        assert_eq!(output_plan.node_payload_bytes, plan.node_payload_bytes);
        assert!(tree
            .resume_replacement_rebuild_native(token, plan.node_payload_bytes + 1)
            .is_err());
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
            assert!(tree
                .resume_replacement_rebuild_output_memory_v1_native(
                    token, witness.0, witness.1, witness.2,
                )
                .is_err());
            assert_eq!(
                tree.replacement_rebuild_output_memory_plan_v1_native(token)
                    .unwrap(),
                output_plan
            );
        }
        let paused = tree
            .step_replacement_rebuild_output_memory_v1_native(token, 256)
            .unwrap();
        assert_eq!((paused.units, paused.phase), (0, "plan ready"));
        assert_eq!(
            tree.chunk_memory_native().replacement,
            ChunkStoreMemory::default()
        );
        tree.resume_replacement_rebuild_output_memory_v1_native(
            token,
            output_plan.node_payload_bytes,
            output_plan.range_endpoint_peak_requested_bytes,
            output_plan.range_endpoint_resident_requested_bytes,
        )
        .unwrap();
        assert!(tree.replacement_rebuild_plan_native(token).is_err());
        assert!(tree
            .replacement_rebuild_output_memory_plan_v1_native(token)
            .is_err());
        finish_test_replacement_steps_from(&mut tree, token, 256, progress.completed);
        tree.cancel_replacement_rebuild_deferred_native(token)
            .unwrap();
        drain_test_retirement(&mut tree, token, 256);
        assert_eq!(rebuild_snapshot(&tree), before);

        let v1 = start_test_replacement(&mut tree, TREE_V1, &[]);
        assert!(tree.replacement_rebuild_plan_native(v1).is_err());
        let v1_plan = tree
            .replacement_rebuild_output_memory_plan_v1_native(v1)
            .unwrap();
        assert!(v1_plan.peak_admission_bytes > 0);
        assert_eq!(
            v1_plan.peak_admission_bytes,
            v1_plan.node_payload_bytes + v1_plan.range_endpoint_peak_requested_bytes,
        );
        let paused = tree
            .step_replacement_rebuild_output_memory_v1_native(v1, 1)
            .unwrap();
        assert_eq!(
            (paused.done, paused.units, paused.phase),
            (false, 0, "plan ready")
        );
        assert!(tree.resume_replacement_rebuild_native(v1, 0).is_err());
        assert!(tree
            .resume_replacement_rebuild_output_memory_v1_native(v1, 0, 0, 0)
            .is_err());
        assert_eq!(
            tree.replacement_rebuild_output_memory_plan_v1_native(v1)
                .unwrap(),
            v1_plan,
        );
        tree.resume_replacement_rebuild_output_memory_v1_native(
            v1,
            v1_plan.node_payload_bytes,
            v1_plan.range_endpoint_peak_requested_bytes,
            v1_plan.range_endpoint_resident_requested_bytes,
        )
        .unwrap();
        assert!(tree
            .replacement_rebuild_output_memory_plan_v1_native(v1)
            .is_err());
        tree.cancel_replacement_rebuild_deferred_native(v1).unwrap();
        drain_test_retirement(&mut tree, v1, 256);

        // The pre-admission ABI remains additive: callers which only know the
        // original V1 step method cannot spin forever at a zero-unit barrier.
        let legacy = start_test_replacement(&mut tree, TREE_V1, &[rebuild_row("legacy-v1.md", 9)]);
        let first = tree.step_replacement_rebuild_native(legacy, 1).unwrap();
        assert_eq!(first.units, 1);
        assert_ne!(first.phase, "plan ready");
        finish_test_replacement_steps_from(&mut tree, legacy, 256, first.completed);
        tree.cancel_replacement_rebuild_deferred_native(legacy)
            .unwrap();
        drain_test_retirement(&mut tree, legacy, 256);
    }

    #[test]
    fn chunk_memory_components_follow_private_publish_and_actual_retirement_owners() {
        for previous_version in [TREE_V1, TREE_V2] {
            for version in [TREE_V1, TREE_V2] {
                for outcome in ["partial-cancel", "ready-cancel", "publish"] {
                    let mut tree = fixture(previous_version, true);
                    let before = rebuild_snapshot(&tree);
                    let initial = tree.chunk_memory_native();
                    assert_eq!(initial.resident, store(&tree).memory_summary());
                    assert!(initial.resident.chunks > 0);
                    assert_eq!(initial.replacement, ChunkStoreMemory::default());
                    assert_eq!(initial.retiring, ChunkStoreMemory::default());
                    let rows: Vec<_> = (0..1033)
                        .rev()
                        .map(|index| {
                            rebuild_row(&format!("replacement/{index:05}.md"), (index % 255) as u8)
                        })
                        .collect();
                    let token = start_test_replacement(&mut tree, version, &rows);
                    assert_eq!(
                        tree.chunk_memory_native().replacement,
                        ChunkStoreMemory::default()
                    );
                    let mut completed = 0;
                    loop {
                        let progress = tree.step_replacement_rebuild_native(token, 256).unwrap();
                        completed += progress.units;
                        assert!(completed < 100_000);
                        let sample = tree.chunk_memory_native();
                        assert_eq!(sample.resident, initial.resident);
                        assert!(sample.replacement.counters_valid);
                        assert!(
                            sample.replacement.buffer_capacity_bytes
                                >= sample.replacement.payload_bytes
                        );
                        assert_eq!(sample.retiring, ChunkStoreMemory::default());
                        assert!(!sample.other_private_jobs_unmeasured);
                        resume_test_replacement_plan(&mut tree, token, progress);
                        if outcome == "partial-cancel" && sample.replacement.chunks > 0 {
                            assert!(!progress.done);
                            break;
                        }
                        if progress.done {
                            break;
                        }
                    }
                    let prepared = tree.chunk_memory_native().replacement;
                    assert!(prepared.payload_bytes > 0);
                    let retiring;
                    if outcome == "publish" {
                        tree.finish_replacement_rebuild_deferred_native(token)
                            .unwrap();
                        assert_eq!(tree.chunk_memory_native().resident, prepared);
                        retiring = initial.resident;
                    } else {
                        tree.cancel_replacement_rebuild_deferred_native(token)
                            .unwrap();
                        assert_eq!(tree.chunk_memory_native().resident, initial.resident);
                        retiring = prepared;
                    }
                    let pending = tree.chunk_memory_native();
                    assert_eq!(pending.replacement, ChunkStoreMemory::default());
                    assert_eq!(
                        pending.retiring, retiring,
                        "handoff lost or double-counted live node owners"
                    );
                    let mut last = retiring;
                    let mut steps = 0;
                    loop {
                        let progress = tree.step_tree_retirement_native(token, 1).unwrap();
                        let sample = tree.chunk_memory_native();
                        assert_eq!(sample.resident, pending.resident);
                        assert!(sample.retiring.counters_valid);
                        assert!(sample.retiring.chunks <= last.chunks);
                        assert!(sample.retiring.payload_bytes <= last.payload_bytes);
                        assert!(
                            sample.retiring.buffer_capacity_bytes <= last.buffer_capacity_bytes
                        );
                        last = sample.retiring;
                        steps += 1;
                        assert!(steps < 25_000);
                        if progress.done {
                            break;
                        }
                    }
                    assert_eq!(last, ChunkStoreMemory::default());
                    if outcome != "publish" {
                        assert_eq!(rebuild_snapshot(&tree), before);
                    }
                }
            }
        }
    }

    #[test]
    fn metadata_components_follow_cross_format_publish_and_retirement_owners() {
        for previous_version in [TREE_V1, TREE_V2] {
            for version in [TREE_V1, TREE_V2] {
                let mut tree = fixture(previous_version, true);
                let initial = tree.metadata_memory_native();
                let expected_resident = match &tree.inner {
                    WasmTreeInner::V1(inner) => inner.metadata_memory(),
                    WasmTreeInner::V2(inner) => inner.metadata_memory(),
                };
                assert_eq!(initial.resident, expected_resident);
                assert_eq!(initial.wrapper_ids.strings, 2);
                assert_eq!(initial.replacement, Some(TreeMetadataMemory::default()));
                assert_eq!(initial.retiring, Some(TreeMetadataMemory::default()));
                assert!(!initial.other_private_jobs_unmeasured);

                let rows: Vec<_> = (0..1_033)
                    .rev()
                    .map(|index| {
                        rebuild_row(&format!("metadata/{index:05}.md"), (index % 255) as u8)
                    })
                    .collect();
                let token = start_test_replacement(&mut tree, version, &rows);
                let first = tree.metadata_memory_native();
                assert_eq!(first.resident, initial.resident);
                assert_eq!(first.wrapper_ids, initial.wrapper_ids);
                assert!(first.replacement.is_none());
                assert!(first.other_private_jobs_unmeasured);

                finish_test_replacement_steps(&mut tree, token, 256);
                let ready = tree.metadata_memory_native();
                let prepared = ready
                    .replacement
                    .expect("ready replacement metadata owner must be exact");
                assert!(prepared.committed.roots > 0);
                assert!(prepared.tree_ids.strings >= 2);
                assert_eq!(ready.resident, initial.resident);
                assert_eq!(ready.wrapper_ids, initial.wrapper_ids);

                tree.finish_replacement_rebuild_deferred_native(token)
                    .unwrap();
                let published = tree.metadata_memory_native();
                assert_eq!(published.resident, prepared);
                assert_eq!(published.retiring, Some(initial.resident));
                assert_eq!(published.wrapper_ids, initial.wrapper_ids);

                let mut saw_exact_retirement = !published.other_private_jobs_unmeasured;
                let mut steps = 0usize;
                loop {
                    let progress = tree.step_tree_retirement_native(token, 1).unwrap();
                    let sample = tree.metadata_memory_native();
                    assert_eq!(sample.resident, prepared);
                    assert_eq!(sample.wrapper_ids, initial.wrapper_ids);
                    if !sample.other_private_jobs_unmeasured {
                        saw_exact_retirement = true;
                    }
                    steps += 1;
                    assert!(steps < 30_000, "metadata retirement did not converge");
                    if progress.done {
                        assert_eq!(sample.retiring, Some(TreeMetadataMemory::default()));
                        assert!(!sample.other_private_jobs_unmeasured);
                        break;
                    }
                }
                assert!(saw_exact_retirement);
            }
        }
    }

    #[test]
    fn metadata_snapshot_keeps_cancelled_input_unmeasured_until_backing_drop() {
        for version in [TREE_V1, TREE_V2] {
            let mut tree = fixture(version, true);
            let rows = [rebuild_row("input/a.md", 1), rebuild_row("input/b.md", 2)];
            let json = rebuild_page(&rows);
            let token = tree
                .begin_replacement_rebuild_native(version, rows.len(), json.len())
                .unwrap();
            tree.append_replacement_rebuild_native(token, 0, &json)
                .unwrap();

            let active = tree.metadata_memory_native();
            assert!(active.replacement.is_none());
            assert!(active.other_private_jobs_unmeasured);
            tree.cancel_replacement_rebuild_deferred_native(token)
                .unwrap();

            let pending = tree.metadata_memory_native();
            assert!(pending.retiring.is_none());
            assert!(pending.other_private_jobs_unmeasured);
            for _ in &rows {
                let progress = tree.step_tree_retirement_native(token, 1).unwrap();
                assert!(!progress.done);
                let sample = tree.metadata_memory_native();
                assert!(sample.retiring.is_none());
                assert!(sample.other_private_jobs_unmeasured);
            }

            let released = tree.step_tree_retirement_native(token, 1).unwrap();
            assert!(!released.done);
            let measured = tree.metadata_memory_native();
            assert_eq!(measured.retiring, Some(TreeMetadataMemory::default()));
            assert!(!measured.other_private_jobs_unmeasured);

            assert!(tree.step_tree_retirement_native(token, 1).unwrap().done);
            let drained = tree.metadata_memory_native();
            assert_eq!(drained.retiring, Some(TreeMetadataMemory::default()));
            assert!(!drained.other_private_jobs_unmeasured);
        }
    }

    #[test]
    fn replacement_input_components_transfer_and_retire_without_fabricated_zero() {
        for version in [TREE_V1, TREE_V2] {
            let rows = [
                rebuild_row("é/one.md", 1),
                rebuild_row("𐀀/two.md", 2),
                rebuild_row("plain.md", 3),
            ];
            let json = rebuild_page(&rows);
            let mut tree = fixture(version, true);
            let token = tree
                .begin_replacement_rebuild_native(version, rows.len(), json.len())
                .unwrap();
            let empty = tree.replacement_input_memory_native();
            let initial = empty.replacement.unwrap();
            assert_eq!(initial.entries.owners, 1);
            assert_eq!(initial.entries.length_slots, 0);
            assert!(initial.entries.capacity_slots >= rows.len());
            assert_eq!(initial.paths, StringMemory::default());
            assert!(!empty.other_input_owners_unmeasured);

            tree.append_replacement_rebuild_native(token, 0, &json)
                .unwrap();
            let accepted = tree.replacement_input_memory_native();
            let mut expected = accepted.replacement.unwrap();
            let WasmTreeJob::ReplacementRebuild { entries, .. } = tree.tree_job.as_ref().unwrap()
            else {
                unreachable!()
            };
            assert_eq!(expected, ReplacementInputMemory::new(entries));
            assert_eq!(
                expected.entries.backing_capacity_bytes,
                (expected.entries.capacity_slots * std::mem::size_of::<FileEntry>()) as u64
            );
            for (wrong_token, offset, page) in [
                (token + 1, rows.len(), "[]"),
                (token, 0, "[]"),
                (token, rows.len(), "[malformed]"),
            ] {
                assert!(tree
                    .append_replacement_rebuild_native(wrong_token, offset, page)
                    .is_err());
                assert_eq!(
                    tree.replacement_input_memory_native().replacement,
                    Some(expected)
                );
            }

            tree.cancel_replacement_rebuild_deferred_native(token)
                .unwrap();
            let transferred = tree.replacement_input_memory_native();
            assert_eq!(
                transferred.replacement,
                Some(ReplacementInputMemory::default())
            );
            assert_eq!(transferred.retiring, Some(expected));
            assert!(!transferred.other_input_owners_unmeasured);
            for row in &rows {
                assert!(!tree.step_tree_retirement_native(token, 1).unwrap().done);
                expected.retire_entry(row);
                let sample = tree.replacement_input_memory_native();
                assert_eq!(sample.retiring, Some(expected));
                assert!(!sample.other_input_owners_unmeasured);
            }
            assert_eq!(expected.entries.length_slots, 0);
            assert!(expected.entries.capacity_slots >= rows.len());
            assert!(!tree.step_tree_retirement_native(token, 1).unwrap().done);
            assert_eq!(
                tree.replacement_input_memory_native().retiring,
                Some(ReplacementInputMemory::default())
            );
            assert!(tree.step_tree_retirement_native(token, 1).unwrap().done);

            let started = tree
                .begin_replacement_rebuild_native(version, rows.len(), json.len())
                .unwrap();
            tree.append_replacement_rebuild_native(started, 0, &json)
                .unwrap();
            tree.start_replacement_rebuild_native(started).unwrap();
            let building = tree.replacement_input_memory_native();
            assert!(building.replacement.is_none());
            assert!(building.other_input_owners_unmeasured);
            tree.cancel_replacement_rebuild_deferred_native(started)
                .unwrap();
            let retiring = tree.replacement_input_memory_native();
            assert!(retiring.retiring.is_none());
            assert!(retiring.other_input_owners_unmeasured);
            drain_test_retirement(&mut tree, started, 256);
            let drained = tree.replacement_input_memory_native();
            assert_eq!(drained.retiring, Some(ReplacementInputMemory::default()));
            assert!(!drained.other_input_owners_unmeasured);

            let partial = tree
                .begin_replacement_rebuild_native(version, rows.len(), json.len())
                .unwrap();
            let first_page = rebuild_page(&rows[..1]);
            tree.append_replacement_rebuild_native(partial, 0, &first_page)
                .unwrap();
            let partial_accepted = tree.replacement_input_memory_native();
            let mut partial_expected = partial_accepted.replacement.unwrap();
            assert_eq!(partial_expected.entries.length_slots, 1);
            assert!(partial_expected.entries.capacity_slots >= rows.len());
            assert_eq!(partial_expected.paths.strings, 1);
            tree.cancel_replacement_rebuild_deferred_native(partial)
                .unwrap();
            let partial_transferred = tree.replacement_input_memory_native();
            assert_eq!(
                partial_transferred.replacement,
                Some(ReplacementInputMemory::default())
            );
            assert_eq!(partial_transferred.retiring, Some(partial_expected));
            assert!(!tree.step_tree_retirement_native(partial, 1).unwrap().done);
            partial_expected.retire_entry(&rows[0]);
            assert_eq!(
                tree.replacement_input_memory_native().retiring,
                Some(partial_expected)
            );
            assert!(!tree.step_tree_retirement_native(partial, 1).unwrap().done);
            assert_eq!(
                tree.replacement_input_memory_native().retiring,
                Some(ReplacementInputMemory::default())
            );
            assert!(tree.step_tree_retirement_native(partial, 1).unwrap().done);

            let zero = tree
                .begin_replacement_rebuild_native(version, 0, 2)
                .unwrap();
            let zero_active = tree.replacement_input_memory_native();
            let zero_owner = zero_active.replacement.unwrap();
            assert_eq!(zero_owner.entries.owners, 1);
            assert_eq!(zero_owner.entries.length_slots, 0);
            assert_eq!(zero_owner.entries.capacity_slots, 0);
            assert_eq!(zero_owner.paths, StringMemory::default());
            tree.cancel_replacement_rebuild_deferred_native(zero)
                .unwrap();
            let zero_transferred = tree.replacement_input_memory_native();
            assert_eq!(zero_transferred.retiring, Some(zero_owner));
            assert!(!tree.step_tree_retirement_native(zero, 1).unwrap().done);
            assert_eq!(
                tree.replacement_input_memory_native().retiring,
                Some(ReplacementInputMemory::default())
            );
            assert!(tree.step_tree_retirement_native(zero, 1).unwrap().done);
        }
    }

    fn v1_entry_count(memory: ReplacementV1EntriesMemory) -> usize {
        memory.input.length_slots
            + memory.groups.length_slots
            + memory.sorting_rows
            + memory.rows.length_slots
            + memory.leaf.length_slots
            + memory.retiring_rows.length_slots
    }

    fn v1_graph_requested_bytes(memory: ReplacementV1GraphMemory) -> u64 {
        [
            memory.group_keys.capacity_bytes,
            memory.prefix.capacity_bytes,
            memory.leaf_hashes.backing_capacity_bytes,
            memory.hashes.backing_capacity_bytes,
            memory.children.backing_capacity_bytes,
            memory.retiring_children.backing_capacity_bytes,
            memory.child_labels.capacity_bytes,
            memory.root_children.backing_capacity_bytes,
            memory.root_child_labels.capacity_bytes,
            memory.identities.capacity_bytes,
        ]
        .into_iter()
        .sum()
    }

    fn assert_v1_graph_relations(memory: ReplacementV1GraphMemory) {
        assert!(memory.counters_valid);
        assert!(memory.prefix.strings <= 1);
        assert!(memory.identities.strings <= 2);
        assert_eq!(
            memory.children.length_slots
                + memory.sorting_children
                + memory.retiring_children.length_slots,
            memory.child_labels.strings
        );
        assert_eq!(
            memory.root_children.length_slots,
            memory.root_child_labels.strings
        );
        for strings in [
            memory.group_keys,
            memory.prefix,
            memory.child_labels,
            memory.root_child_labels,
            memory.identities,
        ] {
            assert!(strings.counters_valid);
            assert!(strings.length_bytes <= strings.capacity_bytes);
        }
    }

    #[test]
    fn replacement_v1_entry_components_redistribute_transfer_and_retire_exactly() {
        let rows = [
            rebuild_row("é/a.md", 1),
            rebuild_row("𐀀/b.md", 2),
            rebuild_row("other/c.md", 3),
            rebuild_row("root.md", 4),
        ];
        let json = rebuild_page(&rows);
        let mut tree = fixture(TREE_V1, true);
        let token = tree
            .begin_replacement_rebuild_native(TREE_V1, rows.len(), json.len())
            .unwrap();
        tree.append_replacement_rebuild_native(token, 0, &json)
            .unwrap();
        let accepted = tree.replacement_input_memory_native().replacement.unwrap();
        let before_start = tree.replacement_v1_entries_memory_native();
        assert_eq!(
            before_start.replacement,
            Some(ReplacementV1EntriesMemory::default())
        );
        assert!(!before_start.other_entry_owners_unmeasured);

        tree.start_replacement_rebuild_native(token).unwrap();
        let started = tree
            .replacement_v1_entries_memory_native()
            .replacement
            .unwrap();
        assert_eq!(started.input, accepted.entries);
        assert_eq!(started.paths, accepted.paths);
        assert_eq!(v1_entry_count(started), rows.len());
        assert!(started.counters_valid);

        let mut saw_group_aggregate = false;
        let mut saw_sort = false;
        for _ in 0..64 {
            let progress = tree.step_replacement_rebuild_native(token, 1).unwrap();
            resume_test_replacement_plan(&mut tree, token, progress);
            let memory = tree
                .replacement_v1_entries_memory_native()
                .replacement
                .unwrap();
            assert!(memory.counters_valid);
            assert_eq!(v1_entry_count(memory), memory.paths.strings);
            saw_group_aggregate |= memory.groups.owners > 1;
            let sort = tree.replacement_sort_memory_native().replacement.unwrap();
            if sort.entries.sorters == 1 {
                saw_sort = true;
                assert_eq!(memory.sorting_rows, sort.entries.values.length_slots);
                break;
            }
            assert!(!progress.done);
        }
        assert!(saw_group_aggregate && saw_sort);
        let active = tree
            .replacement_v1_entries_memory_native()
            .replacement
            .unwrap();
        tree.cancel_replacement_rebuild_deferred_native(token)
            .unwrap();
        let transferred = tree.replacement_v1_entries_memory_native();
        assert_eq!(
            transferred.replacement,
            Some(ReplacementV1EntriesMemory::default())
        );
        assert_eq!(transferred.retiring, Some(active));

        let mut previous_paths = active.paths.strings;
        let mut saw_non_row_unit = false;
        loop {
            let progress = tree.step_tree_retirement_native(token, 1).unwrap();
            let memory = tree
                .replacement_v1_entries_memory_native()
                .retiring
                .unwrap();
            assert!(memory.counters_valid);
            assert_eq!(v1_entry_count(memory), memory.paths.strings);
            assert!(memory.paths.strings <= previous_paths);
            saw_non_row_unit |= memory.paths.strings == previous_paths && !progress.done;
            previous_paths = memory.paths.strings;
            if progress.done {
                assert_eq!(memory, ReplacementV1EntriesMemory::default());
                break;
            }
        }
        assert!(
            saw_non_row_unit,
            "backing/index owners did not get separate units"
        );

        let complete = tree
            .begin_replacement_rebuild_native(TREE_V1, rows.len(), json.len())
            .unwrap();
        tree.append_replacement_rebuild_native(complete, 0, &json)
            .unwrap();
        tree.start_replacement_rebuild_native(complete).unwrap();
        finish_test_replacement_steps(&mut tree, complete, 256);
        assert_eq!(
            tree.replacement_v1_entries_memory_native().replacement,
            Some(ReplacementV1EntriesMemory::default())
        );
        tree.finish_replacement_rebuild_deferred_native(complete)
            .unwrap();
        assert_eq!(
            tree.replacement_v1_entries_memory_native().retiring,
            Some(ReplacementV1EntriesMemory::default())
        );
        drain_test_retirement(&mut tree, complete, 256);
    }

    #[test]
    fn replacement_v1_graph_component_transfers_to_retirement_and_metadata_without_overlap() {
        let mut rows = Vec::with_capacity(1_008);
        for index in (0..1_001).rev() {
            rows.push(rebuild_row(
                &format!("a/note-{index:04}.md"),
                (index % 255) as u8,
            ));
        }
        for index in (0..7).rev() {
            rows.push(rebuild_row(
                &format!("é/note-{index}.md"),
                (index + 31) as u8,
            ));
        }

        let mut tree = fixture(TREE_V1, true);
        let token = start_test_replacement(&mut tree, TREE_V1, &rows);
        let initial = tree.replacement_v1_graph_memory_native();
        let initial_graph = initial.replacement.unwrap();
        assert_eq!(initial_graph.identities.strings, 0);
        assert_eq!(initial.retiring, Some(ReplacementV1GraphMemory::default()));
        assert!(!initial.other_graph_owners_unmeasured);
        assert_v1_graph_relations(initial_graph);

        let mut active = initial_graph;
        let mut saw_group_keys = false;
        let mut saw_leaf_hashes = false;
        let mut saw_child_sort = false;
        for _ in 0..100_000 {
            let progress = tree.step_replacement_rebuild_native(token, 1).unwrap();
            resume_test_replacement_plan(&mut tree, token, progress);
            active = tree
                .replacement_v1_graph_memory_native()
                .replacement
                .unwrap();
            assert_eq!(active.identities.strings, 2);
            assert_v1_graph_relations(active);
            saw_group_keys |= active.group_keys.strings > 1;
            saw_leaf_hashes |= active.leaf_hashes.length_slots > 1;
            let sort = tree.replacement_sort_memory_native().replacement.unwrap();
            if active.sorting_children > 0 {
                assert_eq!(sort.children.values.length_slots, active.sorting_children);
                saw_child_sort = true;
                break;
            }
            assert!(!progress.done);
        }
        assert!(saw_group_keys && saw_leaf_hashes && saw_child_sort);

        tree.cancel_replacement_rebuild_deferred_native(token)
            .unwrap();
        let transferred = tree.replacement_v1_graph_memory_native();
        assert_eq!(
            transferred.replacement,
            Some(ReplacementV1GraphMemory::default())
        );
        assert_eq!(transferred.retiring, Some(active));
        let mut previous_bytes = v1_graph_requested_bytes(active);
        loop {
            let progress = tree.step_tree_retirement_native(token, 1).unwrap();
            let memory = tree.replacement_v1_graph_memory_native().retiring.unwrap();
            assert_v1_graph_relations(memory);
            let bytes = v1_graph_requested_bytes(memory);
            assert!(bytes <= previous_bytes);
            previous_bytes = bytes;
            if progress.done {
                assert_eq!(memory, ReplacementV1GraphMemory::default());
                break;
            }
        }

        let complete = start_test_replacement(&mut tree, TREE_V1, &rows);
        let mut before_transfer = None;
        let mut saw_metadata_transfer = false;
        for _ in 0..1_000_000 {
            let graph = tree
                .replacement_v1_graph_memory_native()
                .replacement
                .unwrap();
            assert_v1_graph_relations(graph);
            if graph.root_children.length_slots > 0 {
                before_transfer = Some(graph);
            }
            let progress = tree.step_replacement_rebuild_native(complete, 1).unwrap();
            resume_test_replacement_plan(&mut tree, complete, progress);
            let after = tree
                .replacement_v1_graph_memory_native()
                .replacement
                .unwrap();
            if after == ReplacementV1GraphMemory::default()
                && tree.metadata_memory_native().replacement.is_some()
                && before_transfer.is_some()
            {
                let before = before_transfer.unwrap();
                let metadata = tree.metadata_memory_native().replacement.unwrap().committed;
                assert_eq!(
                    metadata.v1_children_length,
                    before.root_children.length_slots
                );
                assert_eq!(
                    metadata.v1_children_backing_capacity_bytes,
                    before.root_children.backing_capacity_bytes
                );
                assert_eq!(
                    metadata.strings.capacity_bytes,
                    before.root_child_labels.capacity_bytes + before.identities.capacity_bytes
                );
                saw_metadata_transfer = true;
            }
            if progress.done {
                break;
            }
        }
        assert!(saw_metadata_transfer);
        assert_eq!(
            tree.replacement_v1_graph_memory_native().replacement,
            Some(ReplacementV1GraphMemory::default())
        );
        tree.finish_replacement_rebuild_deferred_native(complete)
            .unwrap();
        assert_eq!(
            tree.replacement_v1_graph_memory_native().retiring,
            Some(ReplacementV1GraphMemory::default())
        );
        drain_test_retirement(&mut tree, complete, 256);
    }

    #[test]
    fn replacement_sort_components_transfer_and_release_each_backing_owner() {
        for version in [TREE_V1, TREE_V2] {
            let rows = [
                rebuild_row("notes/three.md", 3),
                rebuild_row("notes/two.md", 2),
                rebuild_row("notes/one.md", 1),
            ];
            let json = rebuild_page(&rows);
            let mut tree = fixture(version, true);
            let token = tree
                .begin_replacement_rebuild_native(version, rows.len(), json.len())
                .unwrap();
            tree.append_replacement_rebuild_native(token, 0, &json)
                .unwrap();
            tree.start_replacement_rebuild_native(token).unwrap();

            let mut active = tree.replacement_sort_memory_native().replacement.unwrap();
            let mut steps = 0;
            while active.entries.sorters == 0 {
                let progress = tree.step_replacement_rebuild_native(token, 1).unwrap();
                resume_test_replacement_plan(&mut tree, token, progress);
                assert!(!progress.done);
                active = tree.replacement_sort_memory_native().replacement.unwrap();
                steps += 1;
                assert!(steps < 32, "entry sorter was not reached");
            }
            assert_eq!(active.entries.sorters, 1);
            assert_eq!(active.children.sorters, 0);
            assert_eq!(active.entries.values.length_slots, rows.len());
            assert_eq!(
                active.entries.values.slot_size_bytes,
                std::mem::size_of::<FileEntry>()
            );
            assert!(active.entries.source_indices.capacity_slots >= rows.len());
            assert!(active.entries.target_indices.capacity_slots >= rows.len());

            tree.cancel_replacement_rebuild_deferred_native(token)
                .unwrap();
            let transferred = tree.replacement_sort_memory_native();
            assert_eq!(
                transferred.replacement,
                Some(ReplacementSortMemory::default())
            );
            assert_eq!(transferred.retiring, Some(active));
            assert!(!transferred.other_sort_owners_unmeasured);

            let original_capacity = active.entries.values.capacity_slots;
            let mut saw_empty_values_with_backing = false;
            let mut saw_one_index_backing = false;
            let mut iterations = 0;
            loop {
                let progress = tree.step_tree_retirement_native(token, 1).unwrap();
                let report = tree.replacement_sort_memory_native();
                assert_eq!(report.replacement, Some(ReplacementSortMemory::default()));
                let retiring = report.retiring.unwrap();
                if retiring.entries.sorters == 1 {
                    assert!(
                        retiring.entries.values.length_slots <= active.entries.values.length_slots
                    );
                    if retiring.entries.values.length_slots == 0 {
                        assert_eq!(retiring.entries.values.capacity_slots, original_capacity);
                        saw_empty_values_with_backing = true;
                    }
                    let index_backings =
                        usize::from(retiring.entries.source_indices.capacity_slots != 0)
                            + usize::from(retiring.entries.target_indices.capacity_slots != 0);
                    saw_one_index_backing |= index_backings == 1;
                    active = retiring;
                } else {
                    assert_eq!(retiring, ReplacementSortMemory::default());
                }
                iterations += 1;
                assert!(iterations < 128, "sort retirement did not converge");
                if progress.done {
                    break;
                }
            }
            assert!(saw_empty_values_with_backing);
            assert!(saw_one_index_backing);
        }
    }

    #[test]
    fn replacement_v2_planning_components_transfer_and_release_each_backing_owner() {
        let components = |memory: ReplacementV2PlanningMemory| {
            [
                memory.spans,
                memory.leaf_spans,
                memory.planned_ranges,
                memory.next_planned_ranges,
                memory.planned_spans,
                memory.planned_internal_spans,
            ]
        };

        let mut tree = fixture(TREE_V1, true);
        let idle = tree.replacement_v2_planning_memory_native();
        assert_eq!(
            idle.replacement,
            Some(ReplacementV2PlanningMemory::default())
        );
        assert_eq!(idle.retiring, Some(ReplacementV2PlanningMemory::default()));
        assert!(!idle.other_planning_owners_unmeasured);

        let v1_rows = [rebuild_row("v1/two.md", 2), rebuild_row("v1/one.md", 1)];
        let v1 = start_test_replacement(&mut tree, TREE_V1, &v1_rows);
        assert_eq!(
            tree.replacement_v2_planning_memory_native().replacement,
            Some(ReplacementV2PlanningMemory::default())
        );
        tree.cancel_replacement_rebuild_deferred_native(v1).unwrap();
        assert_eq!(
            tree.replacement_v2_planning_memory_native().retiring,
            Some(ReplacementV2PlanningMemory::default())
        );
        drain_test_retirement(&mut tree, v1, 256);

        let rows: Vec<_> = (0..2_000)
            .rev()
            .map(|index| rebuild_row(&format!("planning/{index:08}.md"), (index % 255) as u8))
            .collect();
        let json = rebuild_page(&rows);
        let token = tree
            .begin_replacement_rebuild_native(TREE_V2, rows.len(), json.len())
            .unwrap();
        assert_eq!(
            tree.replacement_v2_planning_memory_native().replacement,
            Some(ReplacementV2PlanningMemory::default())
        );
        for (index, page) in rows.chunks(256).enumerate() {
            tree.append_replacement_rebuild_native(token, index * 256, &rebuild_page(page))
                .unwrap();
        }
        tree.start_replacement_rebuild_native(token).unwrap();

        let initial = tree
            .replacement_v2_planning_memory_native()
            .replacement
            .unwrap();
        for component in components(initial) {
            assert_eq!(component.owners, 1);
            assert_eq!(component.length_slots, 0);
            assert_eq!(component.capacity_slots, 0);
            assert!(component.counters_valid);
        }

        let captured = loop {
            let progress = tree.step_replacement_rebuild_native(token, 1).unwrap();
            assert!(!progress.done);
            let sample = tree
                .replacement_v2_planning_memory_native()
                .replacement
                .unwrap();
            if sample.next_planned_ranges.length_slots > 0
                && sample.planned_internal_spans.length_slots > 0
            {
                break sample;
            }
        };
        assert!(captured.leaf_spans.length_slots > 0);
        assert!(captured.planned_ranges.length_slots > 0);
        assert!(captured.planned_spans.length_slots > 0);

        tree.cancel_replacement_rebuild_deferred_native(token)
            .unwrap();
        let transferred = tree.replacement_v2_planning_memory_native();
        assert_eq!(
            transferred.replacement,
            Some(ReplacementV2PlanningMemory::default())
        );
        assert_eq!(transferred.retiring, Some(captured));
        assert!(!transferred.other_planning_owners_unmeasured);

        let mut previous = captured;
        let mut saw_drained_backing = false;
        let mut saw_explicit_release = false;
        let mut steps = 0usize;
        loop {
            let progress = tree.step_tree_retirement_native(token, 1).unwrap();
            let report = tree.replacement_v2_planning_memory_native();
            assert_eq!(
                report.replacement,
                Some(ReplacementV2PlanningMemory::default())
            );
            assert!(!report.other_planning_owners_unmeasured);
            let current = report.retiring.unwrap();
            let before = components(previous);
            let after = components(current);
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
            steps += 1;
            assert!(steps < 20_000, "V2 planning retirement did not converge");
            if progress.done {
                assert_eq!(current, ReplacementV2PlanningMemory::default());
                break;
            }
        }
        assert!(saw_drained_backing);
        assert!(saw_explicit_release);
    }

    #[test]
    fn replacement_v2_planning_snapshot_survives_failure_and_publish_retirement() {
        let duplicate = rebuild_row("duplicate.md", 1);
        let mut failed = fixture(TREE_V2, true);
        let failed_token =
            start_test_replacement(&mut failed, TREE_V2, &[duplicate.clone(), duplicate]);
        loop {
            if failed
                .step_replacement_rebuild_native(failed_token, 1)
                .is_err()
            {
                break;
            }
        }
        let failed_active = failed
            .replacement_v2_planning_memory_native()
            .replacement
            .unwrap();
        assert!(failed_active.counters_valid);
        failed
            .cancel_replacement_rebuild_deferred_native(failed_token)
            .unwrap();
        assert_eq!(
            failed.replacement_v2_planning_memory_native().retiring,
            Some(failed_active)
        );
        drain_test_retirement(&mut failed, failed_token, 256);
        assert_eq!(
            failed.replacement_v2_planning_memory_native().retiring,
            Some(ReplacementV2PlanningMemory::default())
        );

        let rows: Vec<_> = (0..1_337)
            .rev()
            .map(|index| rebuild_row(&format!("published/{index:08}.md"), (index % 255) as u8))
            .collect();
        let mut published = fixture(TREE_V1, true);
        let token = start_test_replacement(&mut published, TREE_V2, &rows);
        finish_test_replacement_steps(&mut published, token, 256);
        let ready = published
            .replacement_v2_planning_memory_native()
            .replacement
            .unwrap();
        assert!(ready.counters_valid);
        assert!(
            ready.leaf_spans.capacity_slots > 0 || ready.planned_internal_spans.capacity_slots > 0
        );
        published
            .finish_replacement_rebuild_deferred_native(token)
            .unwrap();
        let transferred = published.replacement_v2_planning_memory_native();
        assert_eq!(
            transferred.replacement,
            Some(ReplacementV2PlanningMemory::default())
        );
        assert_eq!(transferred.retiring, Some(ready));
        drain_test_retirement(&mut published, token, 256);
        assert_eq!(
            published.replacement_v2_planning_memory_native().retiring,
            Some(ReplacementV2PlanningMemory::default())
        );
    }

    #[test]
    fn replacement_v2_ranges_snapshot_covers_lifecycle_and_retirement() {
        let components = |memory: ReplacementV2RangesMemory| {
            [
                memory.ranges,
                memory.next_ranges,
                memory.closure_pending,
                memory.closure_expanding,
            ]
        };
        let string_count = |memory: ReplacementV2RangesMemory| {
            memory.vector_paths.strings + memory.descriptor_paths.strings
        };

        let mut tree = fixture(TREE_V1, true);
        let idle = tree.replacement_v2_ranges_memory_native();
        assert_eq!(idle.schema, 1);
        assert_eq!(idle.scope, "replacement-v2-ranges");
        assert_eq!(idle.replacement, Some(ReplacementV2RangesMemory::default()));
        assert_eq!(idle.retiring, Some(ReplacementV2RangesMemory::default()));
        assert!(idle.other_range_owners_unmeasured);

        let rows: Vec<_> = (0..5_000)
            .rev()
            .map(|index| rebuild_row(&format!("ranges/é/{index:08}.md"), (index % 255) as u8))
            .collect();
        let token = start_test_replacement(&mut tree, TREE_V2, &rows);
        let mut saw = [false; 5];
        loop {
            let report = tree.replacement_v2_ranges_memory_native();
            let memory = report.replacement.unwrap();
            assert!(memory.counters_valid);
            saw[0] |= memory.ranges.length_slots > 0;
            saw[1] |= memory.next_ranges.length_slots > 0;
            saw[2] |= memory.closure_pending.length_slots > 0;
            saw[3] |= memory.closure_expanding.length_slots > 0;
            saw[4] |= memory.descriptor_ranges > 0;
            let progress = tree.step_replacement_rebuild_native(token, 1).unwrap();
            resume_test_replacement_plan(&mut tree, token, progress);
            if progress.done {
                break;
            }
        }
        assert!(saw.into_iter().all(|value| value));
        assert_eq!(
            tree.replacement_v2_ranges_memory_native().replacement,
            Some(ReplacementV2RangesMemory::default())
        );
        tree.cancel_replacement_rebuild_deferred_native(token)
            .unwrap();
        drain_test_retirement(&mut tree, token, 256);

        let cancel = start_test_replacement(&mut tree, TREE_V2, &rows);
        let captured = loop {
            let progress = tree.step_replacement_rebuild_native(cancel, 1).unwrap();
            resume_test_replacement_plan(&mut tree, cancel, progress);
            let memory = tree
                .replacement_v2_ranges_memory_native()
                .replacement
                .unwrap();
            if memory.closure_expanding.length_slots > 0 && memory.descriptor_ranges > 0 {
                break memory;
            }
            assert!(!progress.done);
        };
        tree.cancel_replacement_rebuild_deferred_native(cancel)
            .unwrap();
        let transferred = tree.replacement_v2_ranges_memory_native();
        assert_eq!(
            transferred.replacement,
            Some(ReplacementV2RangesMemory::default())
        );
        assert_eq!(transferred.retiring, Some(captured));

        let mut previous = captured;
        let mut saw_empty_backing = false;
        let mut saw_release = false;
        for _ in 0..100_000 {
            let progress = tree.step_tree_retirement_native(cancel, 1).unwrap();
            let report = tree.replacement_v2_ranges_memory_native();
            let current = report.retiring.unwrap();
            assert!(current.counters_valid);
            assert!(string_count(current) <= string_count(previous));
            assert!(string_count(previous) - string_count(current) <= 2);
            for (before, after) in components(previous).into_iter().zip(components(current)) {
                if before.owners == 1 && after.owners == 1 {
                    assert_eq!(before.capacity_slots, after.capacity_slots);
                    assert_eq!(before.backing_capacity_bytes, after.backing_capacity_bytes);
                }
                saw_empty_backing |=
                    after.owners == 1 && after.length_slots == 0 && after.capacity_slots > 0;
                saw_release |= before.owners == 1 && after.owners == 0;
            }
            previous = current;
            if progress.done {
                assert_eq!(current, ReplacementV2RangesMemory::default());
                assert!(saw_empty_backing);
                assert!(saw_release);
                return;
            }
        }
        panic!("V2 range retirement did not converge");
    }

    #[test]
    fn replacement_v2_post_sort_entries_transfer_fail_and_retire_exactly() {
        let rows = [
            rebuild_row("zeta/🧪.md", 3),
            rebuild_row("alpha/é.md", 1),
            rebuild_row("middle/長い名前.md", 2),
        ];
        let json = rebuild_page(&rows);
        let mut tree = fixture(TREE_V1, true);

        let idle = tree.replacement_v2_post_sort_memory_native();
        assert_eq!(idle.replacement, Some(ReplacementInputMemory::default()));
        assert_eq!(idle.retiring, Some(ReplacementInputMemory::default()));
        assert!(!idle.other_post_sort_owners_unmeasured);

        let token = tree
            .begin_replacement_rebuild_native(TREE_V2, rows.len(), json.len())
            .unwrap();
        tree.append_replacement_rebuild_native(token, 0, &json)
            .unwrap();
        let accepted = tree.replacement_input_memory_native().replacement.unwrap();
        assert_eq!(
            tree.replacement_v2_post_sort_memory_native().replacement,
            Some(ReplacementInputMemory::default())
        );
        tree.start_replacement_rebuild_native(token).unwrap();

        let sorting = tree.replacement_v2_post_sort_memory_native();
        assert!(sorting.replacement.is_none());
        assert!(sorting.other_post_sort_owners_unmeasured);
        let exact = loop {
            let progress = tree.step_replacement_rebuild_native(token, 1).unwrap();
            let report = tree.replacement_v2_post_sort_memory_native();
            if let Some(memory) = report.replacement {
                assert!(!report.other_post_sort_owners_unmeasured);
                break memory;
            }
            assert!(report.other_post_sort_owners_unmeasured);
            assert!(!progress.done);
        };
        assert_eq!(exact, accepted);

        tree.cancel_replacement_rebuild_deferred_native(token)
            .unwrap();
        let transferred = tree.replacement_v2_post_sort_memory_native();
        assert_eq!(
            transferred.replacement,
            Some(ReplacementInputMemory::default())
        );
        assert_eq!(transferred.retiring, Some(accepted));
        assert!(!transferred.other_post_sort_owners_unmeasured);

        let first = tree.step_tree_retirement_native(token, 1).unwrap();
        assert!(!first.done);
        let after_one = tree
            .replacement_v2_post_sort_memory_native()
            .retiring
            .unwrap();
        assert_eq!(
            after_one.entries.length_slots,
            accepted.entries.length_slots - 1
        );
        assert_eq!(
            after_one.entries.capacity_slots,
            accepted.entries.capacity_slots
        );
        assert_eq!(after_one.paths.strings, accepted.paths.strings - 1);
        assert!(after_one.paths.length_bytes < accepted.paths.length_bytes);
        assert!(after_one.paths.capacity_bytes < accepted.paths.capacity_bytes);

        let mut saw_drained_backing = false;
        loop {
            let progress = tree.step_tree_retirement_native(token, 1).unwrap();
            let report = tree.replacement_v2_post_sort_memory_native();
            assert!(!report.other_post_sort_owners_unmeasured);
            let memory = report.retiring.unwrap();
            saw_drained_backing |= memory.entries.owners == 1
                && memory.entries.length_slots == 0
                && memory.entries.capacity_slots == accepted.entries.capacity_slots;
            if progress.done {
                assert_eq!(memory, ReplacementInputMemory::default());
                break;
            }
        }
        assert!(saw_drained_backing);

        let duplicate = rebuild_row("duplicate.md", 7);
        let failed_token =
            start_test_replacement(&mut tree, TREE_V2, &[duplicate.clone(), duplicate]);
        assert!(tree
            .replacement_v2_post_sort_memory_native()
            .replacement
            .is_none());
        loop {
            if tree
                .step_replacement_rebuild_native(failed_token, 1)
                .is_err()
            {
                break;
            }
        }
        let failed = tree.replacement_v2_post_sort_memory_native();
        let failed_memory = failed.replacement.unwrap();
        assert_eq!(failed_memory.entries.length_slots, 2);
        assert_eq!(failed_memory.paths.strings, 2);
        assert!(!failed.other_post_sort_owners_unmeasured);
        tree.cancel_replacement_rebuild_deferred_native(failed_token)
            .unwrap();
        assert_eq!(
            tree.replacement_v2_post_sort_memory_native().retiring,
            Some(failed_memory)
        );
        drain_test_retirement(&mut tree, failed_token, 256);
    }

    #[test]
    fn replacement_v2_post_sort_stays_unmeasured_during_sort_retirement() {
        let rows: Vec<_> = (0..257)
            .rev()
            .map(|index| rebuild_row(&format!("sort-cancel/{index:04}.md"), index as u8))
            .collect();
        let mut tree = fixture(TREE_V1, true);
        let token = start_test_replacement(&mut tree, TREE_V2, &rows);
        tree.step_replacement_rebuild_native(token, 7).unwrap();
        tree.cancel_replacement_rebuild_deferred_native(token)
            .unwrap();

        let mut saw_unmeasured = false;
        loop {
            let report = tree.replacement_v2_post_sort_memory_native();
            if report.retiring.is_none() {
                saw_unmeasured = true;
                assert!(report.other_post_sort_owners_unmeasured);
            } else {
                assert_eq!(report.retiring, Some(ReplacementInputMemory::default()));
                assert!(!report.other_post_sort_owners_unmeasured);
            }
            let progress = tree.step_tree_retirement_native(token, 1).unwrap();
            if progress.done {
                break;
            }
        }
        assert!(saw_unmeasured);
        let drained = tree.replacement_v2_post_sort_memory_native();
        assert_eq!(drained.retiring, Some(ReplacementInputMemory::default()));
        assert!(!drained.other_post_sort_owners_unmeasured);
    }

    #[test]
    fn metadata_snapshot_marks_candidate_job_private_owners_unmeasured() {
        for version in [TREE_V1, TREE_V2] {
            let mut tree = fixture(version, true);
            let resident = tree.metadata_memory_native().resident;
            let token = tree
                .install_candidate_mutation_job(
                    vec![FileEntry::new(
                        "private/change.md".into(),
                        hash_bytes(b"private"),
                        77,
                        7,
                    )],
                    Vec::new(),
                )
                .unwrap();
            let active = tree.metadata_memory_native();
            assert_eq!(active.resident, resident);
            assert!(active.other_private_jobs_unmeasured);
            assert_eq!(active.replacement, Some(TreeMetadataMemory::default()));
            tree.cancel_tree_job_inner(token).unwrap();
            let cancelled = tree.metadata_memory_native();
            assert_eq!(cancelled.resident, resident);
            assert!(!cancelled.other_private_jobs_unmeasured);
        }
    }

    #[test]
    fn chunk_memory_marks_other_job_worksets_unmeasured_without_losing_resident_bytes() {
        for version in [TREE_V1, TREE_V2] {
            let mut tree = fixture(version, true);
            let initial = tree.chunk_memory_native().resident;
            let token = tree.begin_candidate_chunks_job().unwrap();
            let sample = tree.chunk_memory_native();
            assert!(sample.other_private_jobs_unmeasured);
            assert_eq!(sample.resident, initial);
            tree.cancel_tree_job_inner(token).unwrap();
            assert!(!tree.chunk_memory_native().other_private_jobs_unmeasured);
        }
    }

    #[test]
    fn invalid_v2_identity_fails_before_first_node_allocation() {
        // Tree v1 accepts this identity. Tree v2 must reject it during the dry
        // plan, while its private node store is still empty.
        let vault_id = "v".repeat(1025);
        let mut tree = WasmTree::new(&vault_id, "device");
        let initial_rows: Vec<_> = (0..12)
            .map(|index| rebuild_row(&format!("resident/{index:02}.md"), index))
            .collect();
        let WasmTreeInner::V1(inner) = &mut tree.inner else {
            unreachable!("constructor starts with Tree v1")
        };
        run_local(inner.rebuild(initial_rows)).unwrap();
        let before = rebuild_snapshot(&tree);
        let resident = tree.chunk_memory_native().resident;
        assert!(resident.chunks > 0);

        let rows: Vec<_> = (0..1033)
            .rev()
            .map(|index| rebuild_row(&format!("replacement/{index:05}.md"), (index % 255) as u8))
            .collect();
        let token = start_test_replacement(&mut tree, TREE_V2, &rows);
        let failed = loop {
            match tree.step_replacement_rebuild_native(token, 256) {
                Ok(progress) => {
                    assert!(!progress.done);
                    let report = tree.chunk_memory_native();
                    assert_eq!(report.resident, resident);
                    assert_eq!(report.retiring, ChunkStoreMemory::default());
                    assert!(!report.other_private_jobs_unmeasured);
                    resume_test_replacement_plan(&mut tree, token, progress);
                }
                Err(_) => break tree.chunk_memory_native(),
            }
        };
        assert_eq!(failed.resident, resident);
        assert_eq!(failed.replacement, ChunkStoreMemory::default());
        assert_eq!(failed.retiring, ChunkStoreMemory::default());
        assert!(!failed.other_private_jobs_unmeasured);
        assert!(tree.step_replacement_rebuild_native(token, 1).is_err());
        let failed_again = tree.chunk_memory_native();
        assert_eq!(failed_again.resident, failed.resident);
        assert_eq!(failed_again.replacement, failed.replacement);
        assert_eq!(failed_again.retiring, failed.retiring);
        assert_eq!(
            failed_again.other_private_jobs_unmeasured,
            failed.other_private_jobs_unmeasured
        );

        tree.cancel_replacement_rebuild_deferred_native(token)
            .unwrap();
        let pending = tree.chunk_memory_native();
        assert_eq!(pending.resident, resident);
        assert_eq!(pending.replacement, ChunkStoreMemory::default());
        assert_eq!(pending.retiring, ChunkStoreMemory::default());
        assert!(!pending.other_private_jobs_unmeasured);
        assert_eq!(rebuild_snapshot(&tree), before);

        let mut previous = pending.retiring;
        loop {
            let progress = tree.step_tree_retirement_native(token, 1).unwrap();
            let report = tree.chunk_memory_native();
            assert_eq!(report.resident, resident);
            assert!(report.retiring.chunks <= previous.chunks);
            assert!(report.retiring.payload_bytes <= previous.payload_bytes);
            assert!(report.retiring.buffer_capacity_bytes <= previous.buffer_capacity_bytes);
            previous = report.retiring;
            if progress.done {
                break;
            }
        }
        assert_eq!(previous, ChunkStoreMemory::default());
        assert_eq!(rebuild_snapshot(&tree), before);
    }

    #[test]
    fn replacement_sort_memory_plan_is_exact_for_complete_v2_input_only() {
        for count in [0, 1, MAX_REPLACEMENT_REBUILD_ENTRIES] {
            // A V1 resident must not make a declared V2 replacement take the
            // V1 compatibility path. Keep its active candidate live throughout.
            let mut tree = fixture(TREE_V1, true);
            let rows: Vec<_> = (0..count)
                .rev()
                .map(|index| rebuild_row(&format!("s/{index:05}"), 1))
                .collect();
            let json = rebuild_page(&rows);
            let before = rebuild_snapshot(&tree);
            let token = tree
                .begin_replacement_rebuild_native(TREE_V2, count, json.len())
                .unwrap();
            for (page, entries) in rows.chunks(256).enumerate() {
                tree.append_replacement_rebuild_native(token, page * 256, &rebuild_page(entries))
                    .unwrap();
            }
            let input = rebuild_input(&tree);
            let meter = tree.replacement_input_memory_native().replacement;
            let plan = tree
                .replacement_rebuild_sort_memory_plan_v1_native(token)
                .unwrap();
            let bytes = (count * std::mem::size_of::<usize>()) as u64;
            assert_eq!(
                serde_json::to_value(plan).unwrap(),
                serde_json::json!({
                    "schema": 1, "scope": "v2-replacement-sort-indices",
                    "entryCount": count, "indexSizeBytes": std::mem::size_of::<usize>(),
                    "sourceIndexRequestedBytes": bytes, "targetIndexRequestedBytes": bytes,
                    "peakAdmissionBytes": bytes * 2,
                })
            );
            assert_eq!(
                tree.replacement_rebuild_sort_memory_plan_v1_native(token)
                    .unwrap(),
                plan
            );
            assert_eq!(rebuild_input(&tree), input);
            assert_eq!(tree.replacement_input_memory_native().replacement, meter);
            assert_eq!(
                tree.replacement_sort_memory_native().replacement,
                Some(ReplacementSortMemory::default())
            );
            assert_eq!(
                tree.chunk_memory_native().replacement,
                ChunkStoreMemory::default()
            );
            tree.start_replacement_rebuild_sort_memory_v1_native(token, bytes as f64, bytes as f64)
                .unwrap();
            let sort = tree
                .replacement_sort_memory_native()
                .replacement
                .unwrap()
                .entries;
            assert_eq!(sort.values.length_slots, count);
            assert_eq!(sort.values.capacity_slots, input.4);
            for indices in [sort.source_indices, sort.target_indices] {
                assert_eq!(indices.length_slots, 0);
                assert!(indices.capacity_slots >= count);
                assert_eq!(indices.slot_size_bytes, std::mem::size_of::<usize>());
                assert_eq!(
                    indices.backing_capacity_bytes,
                    (indices.capacity_slots * std::mem::size_of::<usize>()) as u64
                );
            }
            assert_eq!(rebuild_snapshot(&tree), before);
            assert!(tree
                .replacement_rebuild_sort_memory_plan_v1_native(token)
                .is_err());
            tree.cancel_replacement_rebuild_deferred_native(token)
                .unwrap();
            drain_test_retirement(&mut tree, token, 256);
            assert_eq!(rebuild_snapshot(&tree), before);
            assert_eq!(
                tree.replacement_sort_memory_native().retiring,
                Some(ReplacementSortMemory::default())
            );
        }
        assert!(ReplacementRebuildSortMemoryPlanV1::for_entries(
            MAX_REPLACEMENT_REBUILD_ENTRIES + 1
        )
        .is_err());
    }

    #[test]
    fn replacement_sort_memory_rejects_witnesses_and_phase_misuse_before_allocation() {
        let mut tree = fixture(TREE_V2, true);
        let rows = [
            rebuild_row("unicode/\u{e000}.md", 1),
            rebuild_row("z.md", 2),
        ];
        let json = rebuild_page(&rows);
        let before = rebuild_snapshot(&tree);
        let token = tree
            .begin_replacement_rebuild_native(TREE_V2, rows.len(), json.len())
            .unwrap();
        tree.append_replacement_rebuild_native(token, 0, &rebuild_page(&rows[..1]))
            .unwrap();
        let incomplete = rebuild_input(&tree);
        assert!(tree
            .replacement_rebuild_sort_memory_plan_v1_native(token)
            .is_err());
        assert!(tree
            .start_replacement_rebuild_sort_memory_v1_native(token, 0.0, 0.0)
            .is_err());
        assert_eq!(rebuild_input(&tree), incomplete);
        tree.append_replacement_rebuild_native(token, 1, &rebuild_page(&rows[1..]))
            .unwrap();
        let input = rebuild_input(&tree);
        let plan = tree
            .replacement_rebuild_sort_memory_plan_v1_native(token)
            .unwrap();
        let bytes = plan.source_index_requested_bytes as f64;
        for bad in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -1.0,
            0.5,
            (1u64 << 53) as f64,
            bytes - 1.0,
            bytes + 1.0,
        ] {
            for (source, target) in [(bad, bytes), (bytes, bad)] {
                assert!(tree
                    .start_replacement_rebuild_sort_memory_v1_native(token, source, target)
                    .is_err());
                assert_eq!(rebuild_input(&tree), input);
                assert_eq!(rebuild_snapshot(&tree), before);
            }
        }
        for wrong in [0, token + 1] {
            assert!(tree
                .replacement_rebuild_sort_memory_plan_v1_native(wrong)
                .is_err());
            assert!(tree
                .start_replacement_rebuild_sort_memory_v1_native(wrong, bytes, bytes)
                .is_err());
            assert_eq!(rebuild_input(&tree), input);
        }
        assert!(tree
            .start_replacement_rebuild_with_workspace_native(
                token,
                Some((
                    plan.source_index_requested_bytes + 1,
                    plan.target_index_requested_bytes
                )),
                |_| panic!("wrong witness reached allocation"),
            )
            .is_err());
        tree.start_replacement_rebuild_sort_memory_v1_native(token, bytes, bytes)
            .unwrap();
        assert!(tree
            .start_replacement_rebuild_sort_memory_v1_native(token, bytes, bytes)
            .is_err());
        tree.cancel_replacement_rebuild_deferred_native(token)
            .unwrap();
        assert!(tree
            .replacement_rebuild_sort_memory_plan_v1_native(token)
            .is_err());
        assert!(tree
            .start_replacement_rebuild_sort_memory_v1_native(token, bytes, bytes)
            .is_err());
        drain_test_retirement(&mut tree, token, 1);
        assert!(tree
            .start_replacement_rebuild_sort_memory_v1_native(token, bytes, bytes)
            .is_err());
        assert_eq!(rebuild_snapshot(&tree), before);

        let v1 = tree
            .begin_replacement_rebuild_native(TREE_V1, 0, 2)
            .unwrap();
        assert!(tree
            .replacement_rebuild_sort_memory_plan_v1_native(v1)
            .is_err());
        assert!(tree
            .start_replacement_rebuild_with_workspace_native(v1, Some((0, 0)), |_| panic!(
                "V1 sort plan reached allocation"
            ))
            .is_err());
        tree.start_replacement_rebuild_with_workspace_native(v1, None, |_| {
            panic!("legacy V1 allocated V2 workspace")
        })
        .unwrap();
        finish_test_replacement_steps(&mut tree, v1, 256);
        tree.cancel_replacement_rebuild_deferred_native(v1).unwrap();
        drain_test_retirement(&mut tree, v1, 1);
        assert_eq!(rebuild_snapshot(&tree), before);
    }

    #[test]
    fn replacement_sort_memory_allocation_failures_retain_exact_input_for_retry_or_cancel() {
        for witnessed in [false, true] {
            for failed_reserve in [1, 2] {
                for retry in [false, true] {
                    let mut tree = fixture(TREE_V1, true);
                    let rows = [
                        rebuild_row("\u{e000}.md", 1),
                        rebuild_row("\u{10000}.md", 2),
                    ];
                    let json = rebuild_page(&rows);
                    let before = rebuild_snapshot(&tree);
                    let token = tree
                        .begin_replacement_rebuild_native(TREE_V2, rows.len(), json.len())
                        .unwrap();
                    tree.append_replacement_rebuild_native(token, 0, &json)
                        .unwrap();
                    let input = rebuild_input(&tree);
                    let input_memory = tree.replacement_input_memory_native().replacement;
                    let next_token = tree.next_tree_job_token;
                    let plan = tree
                        .replacement_rebuild_sort_memory_plan_v1_native(token)
                        .unwrap();
                    let witnesses = witnessed.then_some((
                        plan.source_index_requested_bytes,
                        plan.target_index_requested_bytes,
                    ));
                    let mut calls = 0;
                    let mut requested_counts = Vec::new();
                    let result = tree.start_replacement_rebuild_with_workspace_native(
                        token,
                        witnesses,
                        |count| {
                            assert_eq!(count, rows.len());
                            SortWorkspace::try_new_with_reserve(count, |values, needed| {
                                calls += 1;
                                requested_counts.push(needed);
                                assert_eq!(needed, rows.len());
                                assert!(values.is_empty());
                                assert_eq!(values.capacity(), 0);
                                // A real TryReserveError without allocating a huge
                                // fixture or introducing a process-global failpoint.
                                values.try_reserve_exact(if calls == failed_reserve {
                                    usize::MAX
                                } else {
                                    needed
                                })
                            })
                        },
                    );
                    assert!(result.is_err());
                    assert_eq!(calls, failed_reserve);
                    assert_eq!(requested_counts, vec![rows.len(); failed_reserve]);
                    assert_eq!(tree.next_tree_job_token, next_token);
                    assert_eq!(rebuild_input(&tree), input);
                    assert_eq!(
                        tree.replacement_input_memory_native().replacement,
                        input_memory
                    );
                    assert_eq!(
                        tree.replacement_sort_memory_native().replacement,
                        Some(ReplacementSortMemory::default())
                    );
                    assert_eq!(
                        tree.chunk_memory_native().replacement,
                        ChunkStoreMemory::default()
                    );
                    assert_eq!(rebuild_snapshot(&tree), before);
                    assert_eq!(
                        tree.replacement_rebuild_sort_memory_plan_v1_native(token)
                            .unwrap(),
                        plan
                    );
                    if retry {
                        let mut successful_requests = Vec::new();
                        tree.start_replacement_rebuild_with_workspace_native(
                            token,
                            witnesses,
                            |count| {
                                assert_eq!(count, rows.len());
                                SortWorkspace::try_new_with_reserve(count, |values, needed| {
                                    successful_requests.push(needed);
                                    assert_eq!(needed, rows.len());
                                    values.try_reserve_exact(needed)
                                })
                            },
                        )
                        .unwrap();
                        assert_eq!(successful_requests, [rows.len(), rows.len()]);
                        finish_test_replacement_steps(&mut tree, token, 256);
                        tree.finish_replacement_rebuild_deferred_native(token)
                            .unwrap();
                        drain_test_retirement(&mut tree, token, 256);
                        let mut oracle = fixture(TREE_V2, false);
                        oracle.inner = build_replacement_tree(
                            TREE_V2,
                            &tree.vault_id,
                            &tree.device_id,
                            rows.to_vec(),
                        )
                        .unwrap();
                        assert_eq!(state(&tree), state(&oracle));
                        assert_eq!(tree.committed_revision, before.committed_revision + 1);
                        assert_eq!(tree.candidate_revision, before.candidate_revision + 1);
                    } else {
                        tree.cancel_replacement_rebuild_deferred_native(token)
                            .unwrap();
                        drain_test_retirement(&mut tree, token, 1);
                        assert_eq!(rebuild_snapshot(&tree), before);
                    }
                    assert!(tree.tree_job.is_none());
                }
            }
        }
    }

    #[test]
    fn replacement_build_phase_fences_and_cancel_preserve_live_graph() {
        for version in [TREE_V1, TREE_V2] {
            for ready in [false, true] {
                let mut tree = fixture(version, true);
                let before = rebuild_snapshot(&tree);
                let rows = [rebuild_row("z.md", 1), rebuild_row("a/first.md", 2)];
                let token = start_test_replacement(&mut tree, version, &rows);
                for wrong in [token + 1, 0] {
                    assert!(tree.start_replacement_rebuild_native(wrong).is_err());
                    assert!(tree.step_replacement_rebuild_native(wrong, 1).is_err());
                    assert!(tree.finish_replacement_rebuild_native(wrong).is_err());
                }
                assert!(tree.start_replacement_rebuild_native(token).is_err());
                assert!(tree
                    .append_replacement_rebuild_native(token, 2, "[]")
                    .is_err());
                assert!(tree.finish_replacement_rebuild_native(token).is_err());
                assert!(tree.step_tree_job_native(token, 1).is_err());
                for bad_budget in [0, 257, u32::MAX] {
                    assert!(tree
                        .step_replacement_rebuild_native(token, bad_budget)
                        .is_err());
                }
                assert!(tree.ensure_tree_job_idle_native().is_err());
                if ready {
                    finish_test_replacement_steps(&mut tree, token, 1);
                } else {
                    tree.step_replacement_rebuild_native(token, 1).unwrap();
                }
                assert_eq!(rebuild_snapshot(&tree), before);
                tree.cancel_tree_job_inner(token).unwrap();
                assert_eq!(rebuild_snapshot(&tree), before);
                assert!(tree.step_replacement_rebuild_native(token, 1).is_err());
                assert!(tree.tree_job.is_none());
            }
        }
    }

    #[test]
    fn replacement_build_step_failure_retains_owner_but_ready_finish_is_terminal() {
        let mut tree = fixture(TREE_V1, true);
        let before = rebuild_snapshot(&tree);
        let duplicate = rebuild_row("same.md", 1);
        let token = start_test_replacement(&mut tree, TREE_V2, &[duplicate.clone(), duplicate]);
        let mut failed = false;
        for _ in 0..100 {
            match tree.step_replacement_rebuild_native(token, 256) {
                Ok(progress) => assert!(!progress.done),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        assert!(failed);
        assert_eq!(rebuild_snapshot(&tree), before);
        assert!(tree.step_replacement_rebuild_native(token, 1).is_err());
        assert!(tree.finish_replacement_rebuild_native(token).is_err());
        assert_eq!(tree.tree_job.as_ref().unwrap().token(), token);
        tree.cancel_tree_job_inner(token).unwrap();
        assert_eq!(rebuild_snapshot(&tree), before);

        for version in [TREE_V1, TREE_V2] {
            for candidate_overflow in [false, true] {
                let mut tree = fixture(version, true);
                let token = start_test_replacement(&mut tree, version, &[]);
                finish_test_replacement_steps(&mut tree, token, 256);
                if candidate_overflow {
                    tree.candidate_revision = MAX_SAFE_CANDIDATE_REVISION;
                } else {
                    tree.committed_revision = MAX_SAFE_COMMITTED_REVISION;
                }
                let before = rebuild_snapshot(&tree);
                assert!(tree.finish_replacement_rebuild_native(token).is_err());
                assert!(tree.tree_job.is_none());
                assert_eq!(rebuild_snapshot(&tree), before);
            }
        }
    }

    #[test]
    fn replacement_build_start_requires_complete_input_and_success_swaps_once() {
        for version in [TREE_V1, TREE_V2] {
            let mut tree = fixture(if version == TREE_V1 { TREE_V2 } else { TREE_V1 }, true);
            let rows = [rebuild_row("new.md", 1)];
            let json = rebuild_page(&rows);
            let token = tree
                .begin_replacement_rebuild_native(version, 1, json.len())
                .unwrap();
            let before = rebuild_snapshot(&tree);
            let input = rebuild_input(&tree);
            assert!(tree.start_replacement_rebuild_native(token).is_err());
            assert!(tree.step_replacement_rebuild_native(token, 1).is_err());
            assert_eq!(rebuild_input(&tree), input);
            tree.append_replacement_rebuild_native(token, 0, &json)
                .unwrap();
            tree.start_replacement_rebuild_native(token).unwrap();
            finish_test_replacement_steps(&mut tree, token, 256);
            tree.finish_replacement_rebuild_native(token).unwrap();
            assert_eq!(tree.tree_version(), version);
            assert_eq!(tree.total_files(), 1.0);
            assert!(!tree.has_candidate());
            assert_eq!(tree.committed_revision, before.committed_revision + 1);
            assert_eq!(tree.candidate_revision, before.candidate_revision + 1);
            assert!(tree.tree_job.is_none());
            assert!(tree.finish_replacement_rebuild_native(token).is_err());
        }
    }

    fn drain_test_retirement(tree: &mut WasmTree, token: u32, budget: u32) -> usize {
        let before = rebuild_snapshot(tree);
        let mut completed = 0;
        let mut turns = 0;
        loop {
            assert!(tree.ensure_tree_job_idle_native().is_err());
            assert!(tree
                .begin_replacement_rebuild_native(TREE_V1, 0, 2)
                .is_err());
            assert!(tree.cancel_tree_job_inner(token).is_err());
            assert!(tree.step_tree_job_native(token, 1).is_err());
            assert!(tree.step_replacement_rebuild_native(token, 1).is_err());
            assert!(tree.finish_replacement_rebuild_native(token).is_err());
            assert!(tree
                .finish_replacement_rebuild_deferred_native(token)
                .is_err());
            assert!(tree
                .cancel_replacement_rebuild_deferred_native(token)
                .is_err());
            for wrong in [0, token + 1] {
                assert!(tree.step_tree_retirement_native(wrong, budget).is_err());
            }
            for invalid in [0, 257] {
                assert!(tree.step_tree_retirement_native(token, invalid).is_err());
            }
            let result = tree.step_tree_retirement_native(token, budget).unwrap();
            assert!(result.units <= budget as usize);
            assert!(result.done || result.units > 0);
            assert_eq!(result.completed, completed + result.units);
            completed = result.completed;
            turns += 1;
            assert_eq!(
                rebuild_snapshot(tree),
                before,
                "retirement must not mutate live graph"
            );
            if result.done {
                break;
            }
            assert!(turns < 100_000);
        }
        assert!(tree.ensure_tree_job_idle_native().is_ok());
        assert!(tree.step_tree_retirement_native(token, budget).is_err());
        turns
    }

    #[test]
    fn deferred_replacement_cancel_retires_input_partial_failed_and_ready_owners() {
        for version in [TREE_V1, TREE_V2] {
            for phase in ["input", "partial", "ready", "failed"] {
                if phase == "failed" && version == TREE_V1 {
                    continue;
                }
                let mut tree = fixture(version, true);
                let mut rows: Vec<_> = (0usize..1_003)
                    .rev()
                    .map(|index| rebuild_row(&format!("many/{index:04}.md"), (index % 255) as u8))
                    .collect();
                if phase == "failed" {
                    rows.push(rows[0].clone());
                }
                let json = rebuild_page(&rows);
                let token = tree
                    .begin_replacement_rebuild_native(version, rows.len(), json.len())
                    .unwrap();
                let before = rebuild_snapshot(&tree);
                let mut offset = 0;
                for page in rows.chunks(256) {
                    offset = tree
                        .append_replacement_rebuild_native(token, offset, &rebuild_page(page))
                        .unwrap();
                }
                assert!(tree.step_tree_retirement_native(token, 1).is_err());
                if phase != "input" {
                    tree.start_replacement_rebuild_native(token).unwrap();
                    if phase == "ready" {
                        finish_test_replacement_steps(&mut tree, token, 256);
                    } else if phase == "failed" {
                        loop {
                            match tree.step_replacement_rebuild_native(token, 256) {
                                Ok(progress) => assert!(!progress.done),
                                Err(_) => break,
                            }
                        }
                    } else {
                        tree.step_replacement_rebuild_native(token, 256).unwrap();
                    }
                }
                assert!(tree
                    .cancel_replacement_rebuild_deferred_native(token + 1)
                    .is_err());
                tree.cancel_replacement_rebuild_deferred_native(token)
                    .unwrap();
                assert_eq!(rebuild_snapshot(&tree), before);
                let turns = drain_test_retirement(&mut tree, token, 7);
                assert!(
                    turns > 1,
                    "populated retirement should cross a host boundary: {phase}"
                );
                let replacement = tree
                    .begin_replacement_rebuild_native(version, 0, 2)
                    .unwrap();
                assert!(tree.step_tree_retirement_native(token, 1).is_err());
                assert_eq!(tree.tree_job.as_ref().unwrap().token(), replacement);
                tree.cancel_tree_job_inner(replacement).unwrap();
            }
        }
    }

    #[test]
    fn deferred_replacement_finish_swaps_once_and_retires_both_previous_formats() {
        for previous_version in [TREE_V1, TREE_V2] {
            for version in [TREE_V1, TREE_V2] {
                let mut tree = fixture(previous_version, true);
                let before = rebuild_snapshot(&tree);
                let token = tree
                    .begin_replacement_rebuild_native(version, 0, 2)
                    .unwrap();
                // The deferred finish never hides a legacy synchronous build.
                assert!(tree
                    .finish_replacement_rebuild_deferred_native(token)
                    .is_err());
                assert_eq!(rebuild_snapshot(&tree), before);
                tree.start_replacement_rebuild_native(token).unwrap();
                assert!(tree
                    .finish_replacement_rebuild_deferred_native(token)
                    .is_err());
                finish_test_replacement_steps(&mut tree, token, 256);
                tree.finish_replacement_rebuild_deferred_native(token)
                    .unwrap();
                assert_eq!(tree.tree_version(), version);
                assert_eq!(tree.total_files(), 0.0);
                assert!(!tree.has_candidate());
                assert_eq!(tree.committed_revision, before.committed_revision + 1);
                assert_eq!(tree.candidate_revision, before.candidate_revision + 1);
                assert!(store(&tree).is_empty());
                assert!(drain_test_retirement(&mut tree, token, 1) > 10);
            }
        }
    }

    #[test]
    fn deferred_finish_exhaustion_retains_private_owner_for_cooperative_cancel() {
        for version in [TREE_V1, TREE_V2] {
            for candidate_exhaustion in [false, true] {
                let mut tree = fixture(version, true);
                if candidate_exhaustion {
                    tree.candidate_revision = MAX_SAFE_CANDIDATE_REVISION;
                } else {
                    tree.committed_revision = MAX_SAFE_COMMITTED_REVISION;
                }
                let before = rebuild_snapshot(&tree);
                let token = tree
                    .begin_replacement_rebuild_native(version, 0, 2)
                    .unwrap();
                tree.start_replacement_rebuild_native(token).unwrap();
                finish_test_replacement_steps(&mut tree, token, 256);
                assert!(tree
                    .finish_replacement_rebuild_deferred_native(token)
                    .is_err());
                assert!(tree.tree_job.as_ref().unwrap().ready());
                assert_eq!(rebuild_snapshot(&tree), before);
                tree.cancel_replacement_rebuild_deferred_native(token)
                    .unwrap();
                drain_test_retirement(&mut tree, token, 256);
            }
        }
    }

    #[test]
    fn replacement_rebuild_pages_preserve_live_state_and_match_legacy_graphs() {
        for version in [TREE_V1, TREE_V2] {
            for page_size in [1, 256] {
                // Native ordering must not assume JS UTF-16 order. Root-level
                // files also belong to prefix "", ahead of directory groups.
                let mut rows = vec![
                    rebuild_row("\u{10000}.md", 1),
                    rebuild_row("\u{e000}.md", 2),
                    rebuild_row("z.md", 3),
                    rebuild_row("a/first.md", 4),
                ];
                for index in (0..if page_size == 1 { 7 } else { 1_003 }).rev() {
                    rows.push(rebuild_row(&format!("many/{index:04}.md"), 5));
                }
                let json = rebuild_page(&rows);
                let initial_version = if version == TREE_V1 { TREE_V2 } else { TREE_V1 };
                let mut tree = fixture(initial_version, true);
                tree.committed_revision = 23;
                tree.candidate_revision = 41;
                store(&tree).insert_chunk(hash_bytes(b"orphan"), b"orphan".to_vec());
                let before = rebuild_snapshot(&tree);
                let token = tree
                    .begin_replacement_rebuild_native(version, rows.len(), json.len())
                    .unwrap();
                let initial_input = rebuild_input(&tree);
                assert!(initial_input.4 >= rows.len());
                let started = crate::tree::now_ms();
                let mut accepted = 0;
                for page in rows.chunks(page_size) {
                    accepted = tree
                        .append_replacement_rebuild_native(token, accepted, &rebuild_page(page))
                        .unwrap();
                    assert_eq!(rebuild_snapshot(&tree), before);
                    let input = rebuild_input(&tree);
                    assert_eq!((input.4, input.5), (initial_input.4, initial_input.5));
                }
                assert_eq!(accepted, rows.len());
                assert_eq!(rebuild_input(&tree).2, json.len());
                tree.finish_replacement_rebuild_native(token).unwrap();
                assert!(tree.tree_job.is_none());
                assert!(!tree.has_candidate());
                assert_eq!((tree.committed_revision, tree.candidate_revision), (24, 42));
                assert_eq!(tree.tree_version(), version);
                assert_eq!(tree.total_files(), rows.len() as f64);

                // Independent existing synchronous builder is the oracle.
                // Native clock is not patched: only v1 wall-clock metadata is
                // normalized, with actual timestamp separately bounded here.
                let mut oracle = fixture(version, false);
                match &mut oracle.inner {
                    WasmTreeInner::V1(inner) => run_local(inner.rebuild(rows.clone())).unwrap(),
                    WasmTreeInner::V2(inner) => run_local(inner.rebuild(rows.clone())).unwrap(),
                }
                let actual_root = tree.root_bytes().unwrap().unwrap();
                let expected_root = match &oracle.inner {
                    WasmTreeInner::V1(inner) => {
                        let actual = RootNode::deserialize(&actual_root).unwrap();
                        assert!((started..=crate::tree::now_ms()).contains(&actual.created_ms));
                        assert_eq!(actual.parent_hash, None);
                        let mut expected = inner.committed_root().unwrap().clone();
                        expected.created_ms = actual.created_ms;
                        expected.serialize()
                    }
                    WasmTreeInner::V2(inner) => {
                        assert_eq!(inner.committed_root().unwrap().parent_hash, None);
                        inner.committed_root().unwrap().serialize().unwrap()
                    }
                };
                assert_eq!(actual_root, expected_root);
                assert_eq!(
                    tree.root_hash_hex().unwrap(),
                    oracle.root_hash_hex().unwrap()
                );
                assert_eq!(state(&tree).2, state(&oracle).2);
                assert!(tree.finish_replacement_rebuild_native(token).is_err());
            }
        }
    }

    #[test]
    fn replacement_rebuild_rejected_feeds_and_cancel_retain_exact_owner() {
        let rows = [rebuild_row("\u{e000}.md", 1), rebuild_row("other.md", 2)];
        let first = rebuild_page(&rows[..1]);
        for version in [TREE_V1, TREE_V2] {
            let mut tree = fixture(version, true);
            let before = rebuild_snapshot(&tree);
            let token = tree
                .begin_replacement_rebuild_native(version, 2, MAX_REPLACEMENT_REBUILD_JSON_BYTES)
                .unwrap();
            tree.append_replacement_rebuild_native(token, 0, &first)
                .unwrap();
            let input = rebuild_input(&tree);
            let too_many = rebuild_page(&vec![rebuild_row("x.md", 3); 257]);
            let too_large = format!("[{}]", " ".repeat(MAX_REPLACEMENT_REBUILD_FEED_BYTES - 1));
            let invalid_hash = rebuild_page(&rows[1..]).replace(&hash_to_hex(&rows[1].hash), "bad");
            let invalid_integer = rebuild_page(&rows[1..]).replace("\"size\":2", "\"size\":-1");
            for (wrong_token, offset, page) in [
                (token + 1, 1, first.as_str()),
                (token, 0, first.as_str()),
                (token, 2, first.as_str()),
                (token, 1, "[]"),
                (token, 1, "[ ]"),
                (token, 1, "[{}]"),
                (token, 1, "[true]"),
                (token, 1, "["),
                (token, 1, " []"),
                (token, 1, "[] "),
                (token, 1, too_many.as_str()),
                (token, 1, too_large.as_str()),
                (token, 1, invalid_hash.as_str()),
                (token, 1, invalid_integer.as_str()),
            ] {
                assert!(tree
                    .append_replacement_rebuild_native(wrong_token, offset, page)
                    .is_err());
                assert_eq!(rebuild_input(&tree), input);
                assert_eq!(rebuild_snapshot(&tree), before);
            }
            assert!(tree.finish_replacement_rebuild_native(token).is_err());
            assert_eq!(rebuild_input(&tree), input);
            assert!(tree
                .begin_replacement_rebuild_native(version, 0, 2)
                .is_err());
            assert!(tree.cancel_tree_job_inner(token + 1).is_err());
            assert_eq!(rebuild_input(&tree), input);
            tree.cancel_tree_job_inner(token).unwrap();
            assert_eq!(rebuild_snapshot(&tree), before);
            let next = tree
                .begin_replacement_rebuild_native(version, 0, 2)
                .unwrap();
            assert!(next > token);
            assert!(tree
                .append_replacement_rebuild_native(token, 0, &first)
                .is_err());
            assert_eq!(tree.tree_job.as_ref().unwrap().token(), next);
            tree.cancel_tree_job_inner(next).unwrap();
        }
    }

    #[test]
    fn replacement_rebuild_count_byte_and_token_bounds_precede_state_changes() {
        for version in [TREE_V1, TREE_V2] {
            let mut tree = fixture(version, true);
            let before = rebuild_snapshot(&tree);
            for (format, count, bytes) in [
                (0, 0, 2),
                (3, 0, 2),
                (version, 65_537, 1_000),
                (version, 0, 0),
                (version, 0, 1),
                (version, 0, 3),
                (version, 1, 2),
                (version, 1, MAX_REPLACEMENT_REBUILD_JSON_BYTES + 1),
            ] {
                assert!(tree
                    .begin_replacement_rebuild_native(format, count, bytes)
                    .is_err());
                assert!(tree.tree_job.is_none());
                assert_eq!(tree.next_tree_job_token, 1);
                assert_eq!(rebuild_snapshot(&tree), before);
            }
            tree.next_tree_job_token = u32::MAX;
            assert!(tree
                .begin_replacement_rebuild_native(version, 0, 2)
                .is_err());
            assert!(tree.tree_job.is_none());
            tree.next_tree_job_token = 1;
            let token = tree
                .begin_replacement_rebuild_native(
                    version,
                    65_536,
                    MAX_REPLACEMENT_REBUILD_JSON_BYTES,
                )
                .unwrap();
            assert!(rebuild_input(&tree).4 >= 65_536);
            // Row count bound is independently tested with ample byte quota.
            let many = rebuild_page(&vec![rebuild_row("same.md", 1); 257]);
            let input = rebuild_input(&tree);
            assert!(tree
                .append_replacement_rebuild_native(token, 0, &many)
                .is_err());
            assert_eq!(rebuild_input(&tree), input);
            tree.cancel_tree_job_inner(token).unwrap();

            let one = rebuild_page(&[rebuild_row("one.md", 2)]);
            let two = rebuild_page(&[rebuild_row("one.md", 2), rebuild_row("two.md", 3)]);
            // Count overflow is independent of bytes; byte under/overrun does
            // not silently make a count-complete owner ready.
            let token = tree
                .begin_replacement_rebuild_native(version, 1, two.len())
                .unwrap();
            assert!(tree
                .append_replacement_rebuild_native(token, 0, &two)
                .is_err());
            assert_eq!(rebuild_input(&tree).3.len(), 0);
            tree.append_replacement_rebuild_native(token, 0, &one)
                .unwrap();
            assert!(tree.finish_replacement_rebuild_native(token).is_err());
            assert_eq!(rebuild_input(&tree).3.len(), 1);
            tree.cancel_tree_job_inner(token).unwrap();
            let token = tree
                .begin_replacement_rebuild_native(version, 1, one.len() - 1)
                .unwrap();
            assert!(tree
                .append_replacement_rebuild_native(token, 0, &one)
                .is_err());
            assert_eq!(rebuild_input(&tree).2, 2);
            tree.cancel_tree_job_inner(token).unwrap();

            // Inclusive page-byte bound, then inclusive complete 8 MiB JSON
            // bound, using valid whitespace without retaining huge paths.
            let token = tree
                .begin_replacement_rebuild_native(version, 33, MAX_REPLACEMENT_REBUILD_JSON_BYTES)
                .unwrap();
            for index in 0..33 {
                let length = if index < 31 {
                    MAX_REPLACEMENT_REBUILD_FEED_BYTES
                } else if index == 31 {
                    262_000
                } else {
                    176
                };
                assert!(length >= one.len());
                let page = format!(
                    "[{}{}]",
                    &one[1..one.len() - 1],
                    " ".repeat(length - one.len())
                );
                assert_eq!(page.len(), length);
                assert_eq!(
                    tree.append_replacement_rebuild_native(token, index, &page)
                        .unwrap(),
                    index + 1
                );
            }
            assert_eq!(rebuild_input(&tree).2, MAX_REPLACEMENT_REBUILD_JSON_BYTES);
            assert!(tree.tree_job.as_ref().unwrap().ready());
            assert!(tree
                .append_replacement_rebuild_native(token, 33, &one)
                .is_err());
            tree.cancel_tree_job_inner(token).unwrap();
            assert_eq!(rebuild_snapshot(&tree), before);
        }
    }

    #[test]
    fn replacement_rebuild_wrong_job_apis_preserve_owner_and_fences() {
        for version in [TREE_V1, TREE_V2] {
            let mut tree = fixture(version, true);
            let before = rebuild_snapshot(&tree);
            let token = tree
                .begin_replacement_rebuild_native(version, 0, 2)
                .unwrap();
            let input = rebuild_input(&tree);
            assert!(tree.step_tree_job_native(token, 1).is_err());
            assert!(tree.step_tree_job_native(token + 1, 1).is_err());
            assert!(tree.step_root_export_job_native(token, 1, 1).is_err());
            assert!(tree.finish_candidate_job_native(token).is_err());
            assert!(tree.finish_candidate_mutation_native(token).is_err());
            assert!(tree.finish_tree_chunk_export_inner(token).is_err());
            assert!(tree.finish_root_export_inner(token).is_err());
            assert!(tree.read_tree_chunk_export_inner(token, 0, 1).is_err());
            assert!(tree.read_root_export_inner(token, 0, 1).is_err());
            assert!(tree.root_export_workset_inner(token).is_err());
            assert!(tree.start_root_export_build_inner(token).is_err());
            assert!(tree.root_export_info_inner(token).is_err());
            assert!(tree.ensure_tree_job_idle_native().is_err());
            assert_eq!(rebuild_input(&tree), input);
            assert_eq!(rebuild_snapshot(&tree), before);
            tree.cancel_tree_job_inner(token).unwrap();

            let other = tree
                .begin_root_export_inner(RootExportScope::Candidate)
                .unwrap();
            assert!(tree
                .append_replacement_rebuild_native(other, 0, "[]")
                .is_err());
            assert!(tree.finish_replacement_rebuild_native(other).is_err());
            assert_eq!(tree.tree_job.as_ref().unwrap().token(), other);
            tree.cancel_tree_job_inner(other).unwrap();
            assert_eq!(rebuild_snapshot(&tree), before);
        }
    }

    #[test]
    fn replacement_rebuild_ready_finish_is_terminal_on_overflow_or_private_failure() {
        for version in [TREE_V1, TREE_V2] {
            for candidate in [false, true] {
                let mut tree = fixture(version, candidate);
                let before = rebuild_snapshot(&tree);
                let token = tree
                    .begin_replacement_rebuild_native(version, 0, 2)
                    .unwrap();
                tree.finish_replacement_rebuild_native(token).unwrap();
                assert_eq!(tree.committed_revision, before.committed_revision + 1);
                assert_eq!(
                    tree.candidate_revision,
                    before.candidate_revision + u64::from(candidate)
                );
                assert!(!tree.has_candidate());
                assert_eq!(tree.total_files(), 0.0);
                assert!(store(&tree).is_empty());
            }
            for overflow in ["committed", "candidate"] {
                let mut tree = fixture(version, true);
                if overflow == "committed" {
                    tree.committed_revision = MAX_SAFE_COMMITTED_REVISION;
                } else {
                    tree.candidate_revision = MAX_SAFE_CANDIDATE_REVISION;
                }
                let before = rebuild_snapshot(&tree);
                let token = tree
                    .begin_replacement_rebuild_native(version, 0, 2)
                    .unwrap();
                assert!(tree.finish_replacement_rebuild_native(token).is_err());
                assert!(
                    tree.tree_job.is_none(),
                    "ready finish must drop on {overflow} exhaustion"
                );
                assert_eq!(rebuild_snapshot(&tree), before);
                assert!(tree.cancel_tree_job_inner(token).is_err());
            }
            // A v2 duplicate fails inside actual private build, after valid
            // feed/ready and revision preflight, with either old tree format.
            let mut tree = fixture(version, true);
            let before = rebuild_snapshot(&tree);
            let duplicates = [rebuild_row("same.md", 1), rebuild_row("same.md", 2)];
            let page = rebuild_page(&duplicates);
            let token = tree
                .begin_replacement_rebuild_native(TREE_V2, 2, page.len())
                .unwrap();
            tree.append_replacement_rebuild_native(token, 0, &page)
                .unwrap();
            assert!(tree.tree_job.as_ref().unwrap().ready());
            assert!(tree.finish_replacement_rebuild_native(token).is_err());
            assert!(tree.tree_job.is_none());
            assert_eq!(rebuild_snapshot(&tree), before);
            // Legacy v1 deliberately retains duplicate rows in stable input
            // order; bounded feed must not introduce last-wins normalization.
            let token = tree
                .begin_replacement_rebuild_native(TREE_V1, 2, page.len())
                .unwrap();
            tree.append_replacement_rebuild_native(token, 0, &page)
                .unwrap();
            tree.finish_replacement_rebuild_native(token).unwrap();
            assert_eq!(tree.total_files(), 2.0);
            let leaf =
                crate::chunk::LeafChunk::deserialize(&store(&tree).all_chunks()[0].1).unwrap();
            assert_eq!(leaf.entries, duplicates);
        }
    }

    #[test]
    fn committed_revision_tracks_all_wrapper_replacements_without_candidate_noise() {
        let row = |path: &str, byte: u8| {
            format!(
                r#"[{{"path":"{path}","hash":"{}","mtime_ms":{byte},"size":1}}]"#,
                hash_to_hex(&hash_bytes(&[byte]))
            )
        };
        for version in [TREE_V1, TREE_V2] {
            let mut tree = WasmTree::new("revision", "device");
            assert_eq!(tree.committed_revision(), 0.0);
            tree.set_tree_version(version).unwrap();
            assert_eq!(tree.committed_revision(), 1.0);

            tree.build_from_entries(&row("a.md", 1)).unwrap();
            let root_a = tree.root_bytes().unwrap().unwrap();
            let hash_a = tree.root_hash_hex().unwrap();
            let mut expected = 2.0;
            assert_eq!(tree.committed_revision(), expected);

            tree.update_entry("a.md", &hash_to_hex(&hash_bytes(&[2])), 2.0, 1.0)
                .unwrap();
            expected += 1.0;
            assert_eq!(tree.committed_revision(), expected);
            assert_ne!(tree.root_hash_hex().unwrap(), hash_a);

            tree.update_batch(&row("b.md", 3)).unwrap();
            expected += 1.0;
            assert_eq!(tree.committed_revision(), expected);
            tree.delete_entry("b.md").unwrap();
            expected += 1.0;
            assert_eq!(tree.committed_revision(), expected);
            tree.delete_batch(r#"["absent.md"]"#).unwrap();
            expected += 1.0;
            assert_eq!(tree.committed_revision(), expected);
            tree.update_batch("[]").unwrap();
            tree.delete_batch("[]").unwrap();
            assert_eq!(tree.committed_revision(), expected);

            // Exact same bytes still replace graph/history ownership and must
            // invalidate the old witness. Loading A after B also covers ABA.
            tree.load_root(&root_a).unwrap();
            expected += 1.0;
            assert_eq!(tree.committed_revision(), expected);
            assert_eq!(tree.root_hash_hex().unwrap(), hash_a);
            tree.load_root(&root_a).unwrap();
            expected += 1.0;
            assert_eq!(tree.committed_revision(), expected);

            tree.rebuild_from_entries_in_version(version, &row("a.md", 1))
                .unwrap();
            expected += 1.0;
            assert_eq!(tree.committed_revision(), expected);
            tree.begin_candidate().unwrap();
            tree.candidate_update_batch(&row("candidate.md", 9))
                .unwrap();
            assert_eq!(tree.committed_revision(), expected);
            match &mut tree.inner {
                WasmTreeInner::V1(inner) => {
                    inner.abort_candidate().unwrap();
                }
                WasmTreeInner::V2(inner) => {
                    run_local(inner.abort_candidate()).unwrap();
                }
            }
            assert_eq!(tree.committed_revision(), expected);

            let token = tree
                .begin_root_export_inner(RootExportScope::Committed)
                .unwrap();
            tree.step_root_export_job_native(token, 1, 1).unwrap();
            tree.cancel_tree_job_inner(token).unwrap();
            assert_eq!(tree.committed_revision(), expected);

            let before = state(&tree);
            tree.committed_revision = MAX_SAFE_COMMITTED_REVISION;
            let mut mutation_ran = false;
            assert!(tree
                .mutate_committed_native(|_| {
                    mutation_ran = true;
                    Ok(())
                })
                .is_err());
            assert!(
                !mutation_ran,
                "exhaustion entered the live mutation closure"
            );
            assert_eq!(state(&tree), before);
            assert_eq!(
                tree.committed_revision(),
                MAX_SAFE_COMMITTED_REVISION as f64
            );
        }

        // commit_candidate uses the preflighted wrapper helper before it
        // converts stats for JS. Keep that ordering explicit in source because
        // serde-wasm-bindgen values cannot be exercised by native cargo tests.
        let source = include_str!("wasm.rs");
        let commit = &source[source.find("pub fn commit_candidate(").unwrap()..];
        let commit = &commit[..commit.find("pub fn abort_candidate(").unwrap()];
        let advanced = commit.find(".mutate_committed_native(").unwrap();
        let returned = commit.find("Ok(to_js(&stats))").unwrap();
        assert!(advanced < returned);
        for name in [
            "set_tree_version",
            "load_root",
            "commit_candidate",
            "update_entry",
            "delete_entry",
            "update_batch",
            "delete_batch",
            "build_from_entries",
            "rebuild_from_entries_in_version",
        ] {
            let signature = format!("pub fn {name}(");
            let tail = &source[source.find(&signature).unwrap()..];
            let end = tail[1..]
                .find("\n    pub fn ")
                .map(|offset| offset + 1)
                .unwrap_or(tail.len());
            assert!(
                tail[..end].contains(".mutate_committed_native("),
                "committed wrapper bypassed revision guard: {name}"
            );
        }
    }

    fn abort_candidate_without_js_stats(tree: &mut WasmTree) -> Result<(), ChunkError> {
        tree.mutate_candidate_native(true, |inner, _| match inner {
            WasmTreeInner::V1(inner) => inner.abort_candidate(),
            WasmTreeInner::V2(inner) => run_local(inner.abort_candidate()),
        })
        .map(|_| ())
    }

    fn commit_candidate_without_js_stats(tree: &mut WasmTree) -> Result<(), ChunkError> {
        tree.mutate_committed_native(|inner| match inner {
            WasmTreeInner::V1(inner) => inner.commit_candidate(),
            WasmTreeInner::V2(inner) => run_local(inner.commit_candidate()),
        })
        .map(|_| ())
    }

    fn drain_candidate_revision_job(tree: &mut WasmTree, token: u32) {
        let revision = tree.candidate_revision();
        for _ in 0..10_000 {
            let progress = tree.step_tree_job_native(token, 1).unwrap();
            assert_eq!(
                tree.candidate_revision(),
                revision,
                "provisional step changed candidate witness"
            );
            if progress.done {
                return;
            }
        }
        panic!("bounded candidate revision fixture did not finish");
    }

    fn drain_reachability_job_deferred(tree: &mut WasmTree, token: u32) -> WasmTreeJobProgress {
        for _ in 0..10_000 {
            let progress = tree
                .step_reachability_job_deferred_native(token, 1)
                .unwrap();
            if progress.done {
                return progress;
            }
        }
        panic!("bounded deferred reachability fixture did not finish");
    }

    fn drain_reachability_retirement(tree: &mut WasmTree, token: u32, budget: u32) -> usize {
        let mut completed = 0usize;
        for _ in 0..100_000 {
            let progress = tree
                .step_reachability_retirement_native(token, completed, budget)
                .unwrap();
            let replayed = tree
                .step_reachability_retirement_native(token, completed, budget)
                .unwrap();
            assert_eq!(replayed.done, progress.done);
            assert_eq!(replayed.units, progress.units);
            assert_eq!(replayed.completed, progress.completed);
            assert!(tree
                .step_reachability_retirement_native(
                    token,
                    progress.completed.saturating_add(1),
                    budget,
                )
                .is_err());
            completed = progress.completed;
            if progress.done {
                tree.cancel_reachability_job_deferred_native(token).unwrap();
                assert!(tree.step_tree_retirement_native(token, budget).is_err());
                assert!(tree
                    .finish_reachability_retirement_native(token, completed.saturating_add(1))
                    .is_err());
                tree.finish_reachability_retirement_native(token, completed)
                    .unwrap();
                tree.finish_reachability_retirement_native(token, completed)
                    .unwrap();
                assert!(tree
                    .finish_reachability_retirement_native(token, completed.saturating_add(1))
                    .is_err());
                return completed;
            }
        }
        panic!("bounded reachability retirement did not converge");
    }

    #[test]
    fn deferred_reachability_cancel_and_execution_error_keep_same_token_until_drain() {
        for version in [TREE_V1, TREE_V2] {
            let mut cancelled = fixture(version, false);
            let before = state(&cancelled);
            let token = cancelled.begin_candidate_job().unwrap();
            assert!(cancelled
                .step_reachability_job_deferred_native(token, 0)
                .is_err());
            assert_eq!(cancelled.tree_job.as_ref().unwrap().token(), token);
            let partial = cancelled
                .step_reachability_job_deferred_native(token, 1)
                .unwrap();
            assert!(!partial.done);
            assert!(cancelled
                .cancel_reachability_job_deferred_native(token + 1)
                .is_err());
            cancelled
                .cancel_reachability_job_deferred_native(token)
                .unwrap();
            cancelled
                .cancel_reachability_job_deferred_native(token)
                .unwrap();
            assert_eq!(cancelled.tree_job.as_ref().unwrap().token(), token);
            assert!(cancelled.ensure_tree_job_idle_native().is_err());
            drain_reachability_retirement(&mut cancelled, token, 1);
            assert!(cancelled.step_tree_retirement_native(token, 1).is_err());
            assert_eq!(state(&cancelled), before);
            assert_eq!(
                (
                    cancelled.committed_revision(),
                    cancelled.candidate_revision()
                ),
                (0.0, 0.0)
            );

            let mut failed = fixture(version, false);
            let missing = match &failed.inner {
                WasmTreeInner::V1(inner) => inner.committed_root().unwrap().children[0].1,
                WasmTreeInner::V2(inner) => {
                    inner
                        .committed_root()
                        .unwrap()
                        .tree
                        .child
                        .as_ref()
                        .unwrap()
                        .hash
                }
            };
            run_local(crate::store::ChunkStore::delete(store(&failed), &missing)).unwrap();
            let before = state(&failed);
            let token = failed.begin_candidate_job().unwrap();
            let mut saw_error = false;
            for _ in 0..10_000 {
                match failed.step_reachability_job_deferred_native(token, 1) {
                    Ok(progress) => assert!(!progress.done),
                    Err(_) => {
                        saw_error = true;
                        break;
                    }
                }
            }
            assert!(saw_error);
            assert!(
                matches!(failed.tree_job, Some(WasmTreeJob::Retirement { token: owner, .. }) if owner == token)
            );
            failed
                .cancel_reachability_job_deferred_native(token)
                .unwrap();
            drain_reachability_retirement(&mut failed, token, 256);
            assert_eq!(state(&failed), before);
            assert_eq!(
                (failed.committed_revision(), failed.candidate_revision()),
                (0.0, 0.0)
            );
        }
    }

    #[test]
    fn deferred_candidate_finishes_publish_once_then_retire_cursor() {
        for version in [TREE_V1, TREE_V2] {
            let mut tree = fixture(version, false);
            let token = tree.begin_candidate_job().unwrap();
            let ready = drain_reachability_job_deferred(&mut tree, token);
            let reachable = tree.finish_candidate_job_deferred_native(token).unwrap();
            assert_eq!(reachable as f64, ready.reachable);
            assert!(tree.has_candidate());
            assert_eq!(
                (tree.committed_revision(), tree.candidate_revision()),
                (0.0, 1.0)
            );
            assert!(
                matches!(tree.tree_job, Some(WasmTreeJob::Retirement { token: owner, .. }) if owner == token)
            );
            assert!(tree.ensure_tree_job_idle_native().is_err());
            assert!(tree.metadata_memory_native().other_private_jobs_unmeasured);
            drain_reachability_retirement(&mut tree, token, 1);
            assert!(tree.has_candidate());
            assert!(!tree.metadata_memory_native().other_private_jobs_unmeasured);
            assert_eq!(
                (tree.committed_revision(), tree.candidate_revision()),
                (0.0, 1.0)
            );

            let mut exhausted = fixture(version, false);
            exhausted.candidate_revision = MAX_SAFE_CANDIDATE_REVISION;
            let before = state(&exhausted);
            let token = exhausted.begin_candidate_job().unwrap();
            drain_reachability_job_deferred(&mut exhausted, token);
            assert!(exhausted
                .finish_candidate_job_deferred_native(token)
                .is_err());
            assert!(
                matches!(exhausted.tree_job, Some(WasmTreeJob::V1 { token: owner, .. } | WasmTreeJob::V2 { token: owner, .. }) if owner == token)
            );
            assert!(!exhausted.has_candidate());
            assert_eq!(state(&exhausted), before);
            exhausted
                .cancel_reachability_job_deferred_native(token)
                .unwrap();
            drain_reachability_retirement(&mut exhausted, token, 256);
        }
    }

    #[test]
    fn candidate_open_memory_prepares_without_publication_and_finishes_or_retires_exact_owner() {
        for version in [TREE_V1, TREE_V2] {
            for cancel in [false, true] {
                let mut tree = fixture(version, false);
                store(&tree)
                    .insert_chunk(hash_bytes(b"resident orphan"), b"resident orphan".to_vec());
                let before = rebuild_snapshot(&tree);
                let token = tree.begin_candidate_job().unwrap();
                assert!(tree.candidate_open_memory_plan_v1_native(token).is_err());
                let ready = drain_reachability_job_deferred(&mut tree, token);
                let plan = tree.candidate_open_memory_plan_v1_native(token).unwrap();
                let (scope, identity, endpoint, string_count) = match &tree.inner {
                    WasmTreeInner::V1(inner) => {
                        let root = inner.committed_root().unwrap();
                        let identity = (root.vault_id.len() + root.device_id.len()) as u64;
                        let children = (root.children.len()
                            * std::mem::size_of::<(String, FileHash)>())
                            as u64;
                        let labels = root
                            .children
                            .iter()
                            .map(|(prefix, _)| prefix.len() as u64)
                            .sum::<u64>();
                        (
                            "v1-candidate-open-root",
                            identity,
                            children + labels,
                            root.children.len() + 2,
                        )
                    }
                    WasmTreeInner::V2(inner) => {
                        let root = inner.committed_root().unwrap();
                        let identity = (root.vault_id.len() + root.device_id.len()) as u64;
                        let endpoint = root.tree.child.as_ref().map_or(0, |child| {
                            (child.min_path.len() + child.max_path.len()) as u64
                        });
                        (
                            "v2-candidate-open-root",
                            identity,
                            endpoint,
                            if root.tree.child.is_some() { 4 } else { 2 },
                        )
                    }
                };
                assert_eq!(
                    serde_json::to_value(plan).unwrap(),
                    serde_json::json!({
                        "schema":1,"scope":scope,
                        "residentChunkCount":store(&tree).len(),"rootStringCount":string_count,
                        "rootIdentityRequestedBytes":identity,"rootEndpointRequestedBytes":endpoint,
                        "rootStringRequestedBytes":identity+endpoint,"baselineKeySnapshotRequestedBytes":0,
                        "peakAdmissionBytes":identity+endpoint,"baselineStrategy":"insertion-generation-v1",
                    })
                );
                assert!(plan.resident_chunk_count as f64 > ready.reachable);
                let args = [
                    identity as f64,
                    endpoint as f64,
                    (identity + endpoint) as f64,
                ];
                for field in 0..3 {
                    for bad in [
                        args[field] - 1.0,
                        args[field] + 1.0,
                        -1.0,
                        0.5,
                        f64::NAN,
                        f64::INFINITY,
                        (1u64 << 53) as f64,
                    ] {
                        let mut invalid = args;
                        invalid[field] = bad;
                        assert!(tree
                            .resume_candidate_open_memory_v1_native(
                                token, invalid[0], invalid[1], invalid[2]
                            )
                            .is_err());
                        assert_eq!(
                            tree.candidate_open_memory_plan_v1_native(token).unwrap(),
                            plan
                        );
                        assert_eq!(rebuild_snapshot(&tree), before);
                    }
                }
                for wrong in [0, token + 1] {
                    assert!(tree.candidate_open_memory_plan_v1_native(wrong).is_err());
                    assert!(tree
                        .resume_candidate_open_memory_v1_native(wrong, args[0], args[1], args[2])
                        .is_err());
                }
                tree.resume_candidate_open_memory_v1_native(token, args[0], args[1], args[2])
                    .unwrap();
                assert_eq!(rebuild_snapshot(&tree), before);
                assert!(!tree.has_candidate());
                assert!(tree.cancel_tree_job_inner(token).is_err());
                assert!(tree.step_tree_job_native(token, 1).is_err());
                assert!(tree
                    .step_reachability_job_deferred_native(token, 1)
                    .is_err());
                assert!(tree.candidate_open_memory_plan_v1_native(token).is_err());
                assert!(tree
                    .resume_candidate_open_memory_v1_native(token, args[0], args[1], args[2])
                    .is_err());
                let prepared_bytes = match &tree.tree_job {
                    Some(WasmTreeJob::V1 {
                        prepared_candidate: Some(prepared),
                        ..
                    }) => prepared.value().serialize(),
                    Some(WasmTreeJob::V2 {
                        prepared_candidate: Some(prepared),
                        ..
                    }) => prepared.serialize().unwrap(),
                    _ => panic!("prepared owner missing"),
                };
                assert_eq!(prepared_bytes, before.committed.clone().unwrap());
                if cancel {
                    tree.cancel_reachability_job_deferred_native(token).unwrap();
                    assert!(
                        matches!(
                            (&tree.tree_job, version),
                            (Some(WasmTreeJob::Retirement { cursor, .. }), TREE_V1)
                                if cursor.prepared_candidate_v1.is_some()
                        ) || matches!(
                            (&tree.tree_job, version),
                            (Some(WasmTreeJob::Retirement { cursor, .. }), TREE_V2)
                                if cursor.prepared_candidate_v2.is_some()
                        )
                    );
                    drain_reachability_retirement(&mut tree, token, 1);
                    assert_eq!(rebuild_snapshot(&tree), before);
                } else {
                    assert_eq!(
                        tree.finish_candidate_job_deferred_native(token).unwrap() as f64,
                        ready.reachable
                    );
                    assert_eq!(tree.candidate_revision, before.candidate_revision + 1);
                    assert_eq!(tree.committed_revision, before.committed_revision);
                    assert!(
                        matches!(&tree.tree_job, Some(WasmTreeJob::Retirement { cursor, .. })
                        if cursor.prepared_candidate_v1.is_none()
                            && cursor.prepared_candidate_v2.is_none())
                    );
                    drain_reachability_retirement(&mut tree, token, 1);
                    assert_eq!(
                        tree.metadata_memory_native().resident.candidate_baseline,
                        crate::tree_metadata::CandidateBaselineMemory::scalar()
                    );
                }
                assert!(tree.candidate_open_memory_plan_v1_native(token).is_err());
            }
        }
    }

    #[test]
    fn candidate_open_memory_copy_failures_and_late_overflow_keep_ready_owners() {
        for failure in 1..=4 {
            let mut tree = fixture(TREE_V2, false);
            let before = rebuild_snapshot(&tree);
            let token = tree.begin_candidate_job().unwrap();
            drain_reachability_job_deferred(&mut tree, token);
            let plan = tree.candidate_open_memory_plan_v1_native(token).unwrap();
            let mut calls = 0;
            assert!(tree
                .prepare_candidate_job_v2_with(token, |inner| inner
                    .committed_root()
                    .unwrap()
                    .try_clone_with_strings(|source| {
                        calls += 1;
                        let mut copy = String::new();
                        copy.try_reserve_exact(if calls == failure {
                            usize::MAX
                        } else {
                            source.len()
                        })
                        .map_err(|_| {
                            tree_job_error("injected candidate root allocation failure")
                        })?;
                        copy.push_str(source);
                        Ok(copy)
                    }))
                .is_err());
            assert_eq!(calls, failure);
            assert_eq!(rebuild_snapshot(&tree), before);
            assert_eq!(
                tree.candidate_open_memory_plan_v1_native(token).unwrap(),
                plan
            );
            tree.resume_candidate_open_memory_v1_native(
                token,
                plan.root_identity_requested_bytes as f64,
                plan.root_endpoint_requested_bytes as f64,
                plan.root_string_requested_bytes as f64,
            )
            .unwrap();
            if failure % 2 == 0 {
                tree.candidate_revision = MAX_SAFE_CANDIDATE_REVISION;
            } else {
                store(&tree).set_insertion_generation_for_test(u64::MAX);
            }
            let before_failure = rebuild_snapshot(&tree);
            assert!(tree.finish_candidate_job_deferred_native(token).is_err());
            assert_eq!(rebuild_snapshot(&tree), before_failure);
            assert!(matches!(
                &tree.tree_job,
                Some(WasmTreeJob::V2 {
                    prepared_candidate: Some(_),
                    ..
                })
            ));
            tree.cancel_reachability_job_deferred_native(token).unwrap();
            drain_reachability_retirement(&mut tree, token, 1);
            assert_eq!(rebuild_snapshot(&tree), before_failure);
        }

        let mut v1 = fixture(TREE_V1, false);
        let before = rebuild_snapshot(&v1);
        let token = v1.begin_candidate_job().unwrap();
        drain_reachability_job_deferred(&mut v1, token);
        let plan = v1.candidate_open_memory_plan_v1_native(token).unwrap();
        v1.resume_candidate_open_memory_v1_native(
            token,
            plan.root_identity_requested_bytes as f64,
            plan.root_endpoint_requested_bytes as f64,
            plan.root_string_requested_bytes as f64,
        )
        .unwrap();
        store(&v1).set_insertion_generation_for_test(u64::MAX);
        assert!(v1.finish_candidate_job_deferred_native(token).is_err());
        assert_eq!(rebuild_snapshot(&v1), before);
        assert!(matches!(
            &v1.tree_job,
            Some(WasmTreeJob::V1 {
                prepared_candidate: Some(_),
                ..
            })
        ));
        v1.cancel_reachability_job_deferred_native(token).unwrap();
        drain_reachability_retirement(&mut v1, token, 1);
        assert_eq!(rebuild_snapshot(&v1), before);
    }

    #[test]
    fn deferred_candidate_chunk_finish_matches_legacy_plan_before_retirement() {
        for version in [TREE_V1, TREE_V2] {
            let legacy = fixture(version, true);
            let legacy_plan = match &legacy.inner {
                WasmTreeInner::V1(inner) => (
                    inner.candidate_chunk_hashes().unwrap(),
                    inner.new_candidate_chunk_hashes().unwrap(),
                ),
                WasmTreeInner::V2(inner) => (
                    run_local(inner.candidate_chunk_hashes()).unwrap(),
                    run_local(inner.new_candidate_chunk_hashes()).unwrap(),
                ),
            };

            let mut tree = fixture(version, true);
            let token = tree.begin_candidate_chunks_job().unwrap();
            drain_reachability_job_deferred(&mut tree, token);
            let plan = tree.finish_candidate_chunks_deferred_native(token).unwrap();
            assert_eq!(plan.all, hex_hashes(legacy_plan.0));
            assert_eq!(plan.fresh, hex_hashes(legacy_plan.1));
            assert!(tree.has_candidate());
            assert!(
                matches!(tree.tree_job, Some(WasmTreeJob::Retirement { token: owner, .. }) if owner == token)
            );
            drain_reachability_retirement(&mut tree, token, 256);
            assert!(tree.has_candidate());
        }
    }

    #[test]
    fn candidate_chunk_radix_cursor_is_exact_bounded_and_canonical() {
        let hashes = [
            [0xff; 32],
            [0; 32],
            {
                let mut hash = [0; 32];
                hash[0] = 1;
                hash[31] = 9;
                hash
            },
            {
                let mut hash = [0; 32];
                hash[0] = 1;
                hash[31] = 2;
                hash
            },
        ];
        let reachable = hashes.into_iter().collect::<std::collections::HashSet<_>>();
        let plan = CandidateChunkSortMemoryPlanV1::for_hashes(reachable.len()).unwrap();
        assert_eq!(plan.schema, 1);
        assert_eq!(plan.scope, "candidate-chunk-plan-sort-workspace");
        assert_eq!(plan.hash_size_bytes, 32);
        assert_eq!(plan.source_hashes_requested_bytes, 4 * 32);
        assert_eq!(plan.scratch_hashes_requested_bytes, 4 * 32);
        assert_eq!(plan.peak_admission_bytes, 8 * 32);
        assert!(plan.reachable_set_unmeasured && plan.page_output_unmeasured);
        assert_eq!(plan.sort_strategy, "stable-lsd-radix-v1");
        assert_eq!(plan.page_max_hashes, 256);

        let mut cursor = CandidateChunkSortCursor::try_new(reachable.len()).unwrap();
        cursor.install_input(reachable);
        for budget in [0, CANDIDATE_CHUNK_SORT_MAX_STEP_UNITS as usize + 1] {
            assert!(cursor.step(budget).is_err());
            assert_eq!(cursor.completed, 0);
        }
        let total = CANDIDATE_CHUNK_PLAN_WORK_UNITS_PER_HASH * hashes.len() as u64;
        let mut completed = 0u64;
        let mut phases = std::collections::HashSet::new();
        loop {
            let progress = cursor.step(1).unwrap();
            assert_eq!(progress.schema, 1);
            assert_eq!(progress.scope, "candidate-chunk-plan-sort");
            assert_eq!(progress.completed, completed + progress.units as u64);
            assert_eq!(progress.completed + progress.remaining, total);
            assert_eq!(progress.all_count, hashes.len());
            assert_eq!(progress.units, 1);
            phases.insert(progress.phase);
            completed = progress.completed;
            if progress.done {
                break;
            }
        }
        assert_eq!(completed, total);
        assert_eq!(cursor.source, {
            let mut expected = hashes.to_vec();
            expected.sort_unstable();
            expected
        });
        assert!(phases.contains("collect"));
        assert!(phases.contains("initialize-scratch"));
        assert!(phases.contains("count-byte") || phases.contains("scatter-byte"));
        let replay = cursor.step(1).unwrap();
        assert!(replay.done);
        assert_eq!((replay.units, replay.remaining), (0, 0));

        let mut empty = CandidateChunkSortCursor::try_new(0).unwrap();
        empty.install_input(std::collections::HashSet::new());
        let ready = empty.step(1).unwrap();
        assert!(ready.done);
        assert_eq!((ready.units, ready.completed, ready.remaining), (0, 0, 0));
    }

    #[test]
    fn candidate_chunk_paged_plan_matches_legacy_and_retires_for_v1_v2_and_empty() {
        for version in [TREE_V1, TREE_V2] {
            let mut tree = fixture(version, true);
            let before = state(&tree);
            let (expected_all, expected_fresh) = match &tree.inner {
                WasmTreeInner::V1(inner) => (
                    inner.candidate_chunk_hashes().unwrap(),
                    inner.new_candidate_chunk_hashes().unwrap(),
                ),
                WasmTreeInner::V2(inner) => (
                    run_local(inner.candidate_chunk_hashes()).unwrap(),
                    run_local(inner.new_candidate_chunk_hashes()).unwrap(),
                ),
            };
            let token = tree.begin_candidate_chunks_job().unwrap();
            drain_reachability_job_deferred(&mut tree, token);
            let plan = tree
                .candidate_chunks_sort_memory_plan_v1_native(token)
                .unwrap();
            assert_eq!(plan.hash_count, expected_all.len());
            assert_eq!(
                plan.source_hashes_requested_bytes,
                expected_all.len() as u64 * 32
            );
            assert!(tree
                .resume_candidate_chunks_sort_memory_v1_native(
                    token,
                    plan.source_hashes_requested_bytes as f64 + 1.0,
                    plan.scratch_hashes_requested_bytes as f64,
                )
                .is_err());
            assert_eq!(
                tree.candidate_chunks_sort_memory_plan_v1_native(token)
                    .unwrap(),
                plan
            );
            tree.resume_candidate_chunks_sort_memory_v1_native(
                token,
                plan.source_hashes_requested_bytes as f64,
                plan.scratch_hashes_requested_bytes as f64,
            )
            .unwrap();
            assert!(tree.cancel_tree_job_inner(token).is_err());
            assert!(tree.step_tree_job_native(token, 1).is_err());
            assert!(tree.finish_candidate_chunks_deferred_native(token).is_err());
            assert!(tree
                .step_candidate_chunks_sort_v1_native(token + 1, 1)
                .is_err());
            assert!(tree.step_candidate_chunks_sort_v1_native(token, 0).is_err());
            assert!(tree
                .step_candidate_chunks_sort_v1_native(
                    token,
                    CANDIDATE_CHUNK_SORT_MAX_STEP_UNITS + 1,
                )
                .is_err());
            assert!(tree.candidate_chunks_plan_info_v1_native(token).is_err());
            let mut completed = 0u64;
            loop {
                let progress = tree
                    .step_candidate_chunks_sort_v1_native(token, 11)
                    .unwrap();
                assert_eq!(progress.completed, completed + progress.units as u64);
                assert_eq!(
                    progress.completed + progress.remaining,
                    expected_all.len() as u64 * CANDIDATE_CHUNK_PLAN_WORK_UNITS_PER_HASH
                );
                completed = progress.completed;
                if progress.done {
                    break;
                }
            }
            let info = tree.candidate_chunks_plan_info_v1_native(token).unwrap();
            assert_eq!(
                (
                    info.schema,
                    info.scope,
                    info.all_count,
                    info.page_max_hashes
                ),
                (1, "candidate-chunk-plan-pages", expected_all.len(), 256)
            );
            assert!(tree.finish_candidate_chunks_plan_v1_native(token).is_err());
            assert!(tree
                .read_candidate_chunks_page_v1_native(token + 1, 0, 1)
                .is_err());
            assert!(tree
                .read_candidate_chunks_page_v1_native(token, 1, 1)
                .is_err());
            for budget in [0, CANDIDATE_CHUNK_PLAN_PAGE_MAX_HASHES + 1] {
                assert!(tree
                    .read_candidate_chunks_page_v1_native(token, 0, budget)
                    .is_err());
            }
            let mut offset = 0usize;
            let mut actual_all = Vec::new();
            let mut actual_fresh = Vec::new();
            while offset < info.all_count {
                let page = tree
                    .read_candidate_chunks_page_v1_native(token, offset, 1)
                    .unwrap();
                assert_eq!(page.offset, offset);
                assert_eq!(page.all.len(), page.next_offset - page.offset);
                assert!(page.fresh.iter().all(|hash| page.all.contains(hash)));
                offset = page.next_offset;
                actual_all.extend(page.all);
                actual_fresh.extend(page.fresh);
            }
            assert_eq!(actual_all, hex_hashes(expected_all));
            assert_eq!(actual_fresh, hex_hashes(expected_fresh));
            tree.finish_candidate_chunks_plan_v1_native(token).unwrap();
            assert_eq!(state(&tree), before);
            assert!(matches!(
                tree.tree_job,
                Some(WasmTreeJob::Retirement { token: owner, .. }) if owner == token
            ));
            drain_reachability_retirement(&mut tree, token, 256);
            assert_eq!(state(&tree), before);

            let mut empty = WasmTree::new("empty", "device");
            if version == TREE_V2 {
                empty.inner = WasmTreeInner::V2(TransactionalTreeV2::new("empty", "device"));
            }
            match &mut empty.inner {
                WasmTreeInner::V1(inner) => {
                    run_local(inner.rebuild(Vec::new())).unwrap();
                    inner.begin_candidate().unwrap();
                }
                WasmTreeInner::V2(inner) => {
                    run_local(inner.rebuild(Vec::new())).unwrap();
                    run_local(inner.begin_candidate()).unwrap();
                }
            }
            let token = empty.begin_candidate_chunks_job().unwrap();
            drain_reachability_job_deferred(&mut empty, token);
            let plan = empty
                .candidate_chunks_sort_memory_plan_v1_native(token)
                .unwrap();
            assert_eq!((plan.hash_count, plan.peak_admission_bytes), (0, 0));
            empty
                .resume_candidate_chunks_sort_memory_v1_native(token, 0.0, 0.0)
                .unwrap();
            let ready = empty
                .step_candidate_chunks_sort_v1_native(token, 1)
                .unwrap();
            assert!(ready.done);
            assert_eq!((ready.units, ready.completed, ready.remaining), (0, 0, 0));
            let info = empty.candidate_chunks_plan_info_v1_native(token).unwrap();
            assert_eq!(info.all_count, 0);
            empty.finish_candidate_chunks_plan_v1_native(token).unwrap();
            drain_reachability_retirement(&mut empty, token, 1);
            assert!(empty.has_candidate());

            let mut cancelled = fixture(version, true);
            let cancelled_before = state(&cancelled);
            let token = cancelled.begin_candidate_chunks_job().unwrap();
            drain_reachability_job_deferred(&mut cancelled, token);
            let plan = cancelled
                .candidate_chunks_sort_memory_plan_v1_native(token)
                .unwrap();
            cancelled
                .resume_candidate_chunks_sort_memory_v1_native(
                    token,
                    plan.source_hashes_requested_bytes as f64,
                    plan.scratch_hashes_requested_bytes as f64,
                )
                .unwrap();
            cancelled
                .step_candidate_chunks_sort_v1_native(token, 1)
                .unwrap();
            assert!(cancelled
                .cancel_reachability_job_deferred_native(token + 1)
                .is_err());
            cancelled
                .cancel_reachability_job_deferred_native(token)
                .unwrap();
            cancelled
                .cancel_reachability_job_deferred_native(token)
                .unwrap();
            assert_eq!(state(&cancelled), cancelled_before);
            drain_reachability_retirement(&mut cancelled, token, 1);
            assert_eq!(state(&cancelled), cancelled_before);
        }
    }

    #[test]
    fn v2_output_settlement_is_atomic_exact_and_revision_bound() {
        let mut aborted = fixture(TREE_V2, true);
        let report = aborted
            .abort_candidate_output_settlement_v1_native()
            .unwrap();
        let (payload, endpoint, resident) = stable_v2_output_oracle(&aborted);
        assert_eq!(
            (report.schema, report.scope, report.outcome),
            (1, "v2-stable-tree-output", "abort")
        );
        assert_eq!(report.tree_version, TREE_V2);
        assert_eq!(report.after, report.reachable);
        assert_eq!(report.removed, report.before - report.after);
        assert!(report.counters_valid);
        assert_eq!(report.node_payload_bytes, Some(payload));
        assert_eq!(
            report.range_endpoint_resident_requested_bytes,
            Some(endpoint)
        );
        assert_eq!(report.resident_admission_bytes, Some(resident));
        assert_eq!(
            (report.committed_revision, report.candidate_revision),
            (0, 1)
        );
        assert!(!aborted.has_candidate());

        let mut committed = fixture(TREE_V2, true);
        let candidate = committed.candidate_root_hash_hex().unwrap();
        let report = committed
            .commit_candidate_output_settlement_v1_native()
            .unwrap();
        let (payload, endpoint, resident) = stable_v2_output_oracle(&committed);
        assert_eq!(
            (report.scope, report.outcome),
            ("v2-stable-tree-output", "commit")
        );
        assert_eq!(report.after, report.reachable);
        assert_eq!(report.removed, report.before - report.after);
        assert_eq!(report.node_payload_bytes, Some(payload));
        assert_eq!(
            report.range_endpoint_resident_requested_bytes,
            Some(endpoint)
        );
        assert_eq!(report.resident_admission_bytes, Some(resident));
        assert_eq!(
            (report.committed_revision, report.candidate_revision),
            (1, 1)
        );
        assert_eq!(committed.root_hash_hex().unwrap(), candidate);
        assert!(!committed.has_candidate());

        let mut v1 = fixture(TREE_V1, true);
        let v1_before = state(&v1);
        assert!(v1.commit_candidate_output_settlement_v1_native().is_err());
        assert!(v1.abort_candidate_output_settlement_v1_native().is_err());
        assert_eq!(state(&v1), v1_before);
        assert_eq!(
            (v1.committed_revision(), v1.candidate_revision()),
            (0.0, 0.0)
        );

        for outcome in ["commit", "abort"] {
            let mut active_job = fixture(TREE_V2, true);
            let token = active_job.begin_candidate_chunks_job().unwrap();
            let active_before = state(&active_job);
            let result = match outcome {
                "commit" => active_job.commit_candidate_output_settlement_v1_native(),
                "abort" => active_job.abort_candidate_output_settlement_v1_native(),
                _ => unreachable!(),
            };
            assert!(result.is_err());
            assert_eq!(state(&active_job), active_before);
            assert_eq!(active_job.tree_job.as_ref().unwrap().token(), token);
            active_job.cancel_tree_job_inner(token).unwrap();
        }

        let mut exhausted = fixture(TREE_V2, true);
        exhausted.committed_revision = MAX_SAFE_COMMITTED_REVISION;
        let exhausted_before = state(&exhausted);
        assert!(exhausted
            .commit_candidate_output_settlement_v1_native()
            .is_err());
        assert_eq!(state(&exhausted), exhausted_before);
        assert!(exhausted.has_candidate());

        for outcome in ["commit", "abort"] {
            let mut exhausted = fixture(TREE_V2, true);
            exhausted.candidate_revision = MAX_SAFE_CANDIDATE_REVISION;
            let exhausted_before = state(&exhausted);
            let result = match outcome {
                "commit" => exhausted.commit_candidate_output_settlement_v1_native(),
                "abort" => exhausted.abort_candidate_output_settlement_v1_native(),
                _ => unreachable!(),
            };
            assert!(result.is_err());
            assert_eq!(state(&exhausted), exhausted_before);
            assert!(exhausted.has_candidate());
        }
    }

    #[test]
    fn candidate_revision_tracks_visible_transitions_and_empty_noops() {
        for version in [TREE_V1, TREE_V2] {
            let mut tree = fixture(version, false);
            assert_eq!(tree.candidate_revision(), 0.0);
            tree.begin_candidate().unwrap();
            assert_eq!(tree.candidate_revision(), 1.0);
            let before = state(&tree);
            tree.candidate_update_batch("[]").unwrap();
            tree.candidate_delete_batch("[]").unwrap();
            assert_eq!(state(&tree), before);
            assert_eq!(tree.candidate_revision(), 1.0);

            // Missing delete and identical updates still install a candidate
            // root, regardless of whether its semantic hash changes.
            tree.candidate_delete_batch(r#"["absent.md"]"#).unwrap();
            assert_eq!(tree.candidate_revision(), 2.0);
            let rows = format!(
                r#"[{{"path":"same.md","hash":"{}","mtime_ms":1,"size":1}}]"#,
                hash_to_hex(&hash_bytes(b"same"))
            );
            tree.candidate_update_batch(&rows).unwrap();
            assert_eq!(tree.candidate_revision(), 3.0);
            let hash = tree.candidate_root_hash_hex();
            tree.candidate_update_batch(&rows).unwrap();
            assert_eq!(tree.candidate_revision(), 4.0);
            assert_eq!(tree.candidate_root_hash_hex(), hash);
            assert_eq!(tree.committed_revision(), 0.0);

            abort_candidate_without_js_stats(&mut tree).unwrap();
            assert_eq!(tree.candidate_revision(), 5.0);
            assert!(!tree.has_candidate());
            tree.begin_candidate().unwrap();
            assert_eq!(tree.candidate_revision(), 6.0);
            commit_candidate_without_js_stats(&mut tree).unwrap();
            assert_eq!(tree.candidate_revision(), 7.0);
            assert_eq!(tree.committed_revision(), 1.0);
            assert!(!tree.has_candidate());

            let before = state(&tree);
            assert!(abort_candidate_without_js_stats(&mut tree).is_err());
            assert!(commit_candidate_without_js_stats(&mut tree).is_err());
            assert!(
                tree.mutate_candidate_native(false, |inner, _| match inner {
                    WasmTreeInner::V1(inner) => run_local(inner.apply_candidate(&[], &[])),
                    WasmTreeInner::V2(inner) => run_local(inner.apply_candidate(&[], &[])),
                })
                .is_err(),
                "empty operation must not accept an absent candidate"
            );
            assert_eq!(state(&tree), before);
            assert_eq!(
                (tree.candidate_revision(), tree.committed_revision()),
                (7.0, 1.0)
            );
        }
    }

    #[test]
    fn candidate_revision_jobs_publish_once_and_preserve_owner_on_errors() {
        for version in [TREE_V1, TREE_V2] {
            let mut tree = fixture(version, false);
            let token = tree.begin_candidate_job().unwrap();
            assert_eq!(tree.candidate_revision(), 0.0);
            assert!(tree.finish_candidate_job_native(token + 1).is_err());
            assert!(tree.finish_candidate_mutation_native(token).is_err());
            assert!(tree.finish_candidate_job_native(token).is_err());
            drain_candidate_revision_job(&mut tree, token);
            tree.finish_candidate_job_native(token).unwrap();
            assert_eq!(tree.candidate_revision(), 1.0);
            assert!(tree.has_candidate());
            assert!(tree.finish_candidate_job_native(token).is_err());

            let before = state(&tree);
            let cancelled = tree
                .begin_candidate_delete_job(r#"["dir1/note.md"]"#)
                .unwrap();
            drain_candidate_revision_job(&mut tree, cancelled);
            tree.cancel_tree_job_inner(cancelled).unwrap();
            assert_eq!(state(&tree), before);
            assert_eq!(tree.candidate_revision(), 1.0);
            for (payload, expected) in [("[]", 1.0), (r#"["absent.md"]"#, 2.0)] {
                let token = tree.begin_candidate_delete_job(payload).unwrap();
                drain_candidate_revision_job(&mut tree, token);
                assert!(tree.finish_candidate_job_native(token).is_err());
                assert!(tree.finish_candidate_mutation_native(token + 1).is_err());
                assert_eq!(tree.tree_job.as_ref().unwrap().token(), token);
                tree.finish_candidate_mutation_job(token).unwrap();
                assert_eq!(tree.candidate_revision(), expected);
                assert!(tree.finish_candidate_mutation_native(token).is_err());
                assert_eq!(tree.candidate_revision(), expected);
            }
            let rows = format!(
                r#"[{{"path":"job.md","hash":"{}","mtime_ms":2,"size":1}}]"#,
                hash_to_hex(&hash_bytes(b"job"))
            );
            let token = tree.begin_candidate_update_job(&rows).unwrap();
            drain_candidate_revision_job(&mut tree, token);
            tree.finish_candidate_mutation_job(token).unwrap();
            assert_eq!(tree.candidate_revision(), 3.0);
            assert_eq!(tree.committed_revision(), 0.0);
        }
    }

    #[test]
    fn v2_candidate_output_plan_is_token_scoped_strict_and_publish_atomic() {
        let mut tree = WasmTree::new("candidate-plan", "device");
        tree.inner = WasmTreeInner::V2(TransactionalTreeV2::new("candidate-plan", "device"));
        let entries: Vec<_> = (0..5_000)
            .map(|index| rebuild_row(&format!("candidate/{index:05}.md"), 1))
            .collect();
        match &mut tree.inner {
            WasmTreeInner::V2(inner) => {
                run_local(inner.rebuild(entries)).unwrap();
                run_local(inner.begin_candidate()).unwrap();
            }
            WasmTreeInner::V1(_) => unreachable!(),
        }
        let before = rebuild_snapshot(&tree);
        let rows = rebuild_page(&[rebuild_row("candidate/02500.md", 9)]);
        let token = tree.begin_candidate_update_job(&rows).unwrap();

        assert!(tree
            .candidate_mutation_output_memory_plan_v1_native(token)
            .is_err());
        assert!(tree
            .candidate_mutation_output_memory_ready_v1_native(token)
            .is_err());
        for wrong in [token + 1, 0] {
            assert!(tree
                .step_candidate_mutation_output_memory_v1_native(wrong, 1)
                .is_err());
            assert!(tree
                .candidate_mutation_output_memory_plan_v1_native(wrong)
                .is_err());
            assert!(tree
                .resume_candidate_mutation_output_memory_v1_native(wrong, 0, 0, 0)
                .is_err());
            assert!(tree
                .candidate_mutation_output_memory_ready_v1_native(wrong)
                .is_err());
            assert_eq!(tree.tree_job.as_ref().unwrap().token(), token);
        }
        for budget in [0, MAX_TREE_JOB_STEP_UNITS + 1] {
            assert!(tree
                .step_candidate_mutation_output_memory_v1_native(token, budget)
                .is_err());
            assert_eq!(tree.tree_job.as_ref().unwrap().token(), token);
        }

        let progress = loop {
            let progress = tree
                .step_candidate_mutation_output_memory_v1_native(token, 256)
                .unwrap();
            assert_eq!(rebuild_snapshot(&tree), before);
            assert!(!progress.done);
            if progress.phase == "plan ready" {
                break progress;
            }
            assert!(progress.units > 0);
        };
        assert!(progress.units > 0);
        let plan = tree
            .candidate_mutation_output_memory_plan_v1_native(token)
            .unwrap();
        assert!(plan.node_payload_bytes > 0);
        assert_eq!(
            plan.peak_admission_bytes,
            plan.node_payload_bytes + plan.range_endpoint_peak_requested_bytes
        );
        assert_eq!(
            plan.resident_admission_bytes,
            plan.node_payload_bytes + plan.range_endpoint_resident_requested_bytes
        );
        assert_eq!(
            tree.step_candidate_mutation_output_memory_v1_native(token, 256)
                .unwrap()
                .units,
            0
        );
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
            assert!(tree
                .resume_candidate_mutation_output_memory_v1_native(
                    token, witness.0, witness.1, witness.2,
                )
                .is_err());
            assert_eq!(
                tree.candidate_mutation_output_memory_plan_v1_native(token)
                    .unwrap(),
                plan
            );
            assert_eq!(rebuild_snapshot(&tree), before);
        }
        tree.resume_candidate_mutation_output_memory_v1_native(
            token,
            plan.node_payload_bytes,
            plan.range_endpoint_peak_requested_bytes,
            plan.range_endpoint_resident_requested_bytes,
        )
        .unwrap();
        assert!(tree
            .candidate_mutation_output_memory_plan_v1_native(token)
            .is_err());
        loop {
            let progress = tree
                .step_candidate_mutation_output_memory_v1_native(token, 256)
                .unwrap();
            assert_eq!(rebuild_snapshot(&tree), before);
            if progress.done {
                break;
            }
            assert!(progress.units > 0);
        }
        let ready = tree
            .candidate_mutation_output_memory_ready_v1_native(token)
            .unwrap();
        assert!(ready.staged_node_payload_bytes <= plan.node_payload_bytes);
        assert_eq!(
            ready.range_endpoint_resident_requested_bytes,
            plan.range_endpoint_resident_requested_bytes
        );
        assert_eq!(
            ready.resident_admission_bytes,
            ready.staged_node_payload_bytes + ready.range_endpoint_resident_requested_bytes
        );
        assert_eq!(rebuild_snapshot(&tree), before);
        tree.finish_candidate_mutation_deferred_native(token)
            .unwrap();
        assert_eq!(
            tree.candidate_revision(),
            before.candidate_revision as f64 + 1.0
        );
        assert_ne!(tree.candidate_root_bytes().unwrap(), before.candidate);
        assert!(tree
            .candidate_mutation_output_memory_ready_v1_native(token)
            .is_err());
        tree.cancel_candidate_mutation_deferred_native(token)
            .unwrap();
        assert!(drain_test_retirement(&mut tree, token, 1) > 1);
    }

    #[test]
    fn candidate_output_plan_family_admits_v1_and_legacy_step_still_completes_both() {
        let rows = rebuild_page(&[rebuild_row("candidate-plan.md", 3)]);
        let mut v1 = fixture(TREE_V1, true);
        let v1_token = v1.begin_candidate_update_job(&rows).unwrap();
        let paused = v1
            .step_candidate_mutation_output_memory_v1_native(v1_token, 1)
            .unwrap();
        assert_eq!(
            (paused.done, paused.units, paused.phase),
            (false, 0, "plan ready")
        );
        let plan = v1
            .candidate_mutation_output_memory_plan_v1_native(v1_token)
            .unwrap();
        assert!(matches!(
            v1.tree_job.as_ref(),
            Some(WasmTreeJob::MutationV1 {
                cursor: None,
                changed: Some(_),
                deleted: Some(_),
                output_resumed: false,
                ..
            })
        ));
        assert!(plan.range_endpoint_peak_requested_bytes > 0);
        assert!(plan.range_endpoint_resident_requested_bytes > 0);
        assert_eq!(
            plan.peak_admission_bytes,
            plan.node_payload_bytes + plan.range_endpoint_peak_requested_bytes
        );
        assert!(v1
            .resume_candidate_mutation_output_memory_v1_native(
                v1_token,
                plan.node_payload_bytes + 1,
                plan.range_endpoint_peak_requested_bytes,
                plan.range_endpoint_resident_requested_bytes,
            )
            .is_err());
        assert_eq!(v1.tree_job.as_ref().unwrap().token(), v1_token);
        v1.resume_candidate_mutation_output_memory_v1_native(
            v1_token,
            plan.node_payload_bytes,
            plan.range_endpoint_peak_requested_bytes,
            plan.range_endpoint_resident_requested_bytes,
        )
        .unwrap();
        loop {
            let progress = v1
                .step_candidate_mutation_output_memory_v1_native(v1_token, 256)
                .unwrap();
            if progress.done {
                break;
            }
        }
        let ready = v1
            .candidate_mutation_output_memory_ready_v1_native(v1_token)
            .unwrap();
        assert!(ready.staged_node_payload_bytes <= plan.node_payload_bytes);
        assert!(ready.range_endpoint_resident_requested_bytes > 0);
        assert!(
            ready.range_endpoint_resident_requested_bytes
                <= plan.range_endpoint_resident_requested_bytes
        );
        assert_eq!(
            ready.resident_admission_bytes,
            ready.staged_node_payload_bytes + ready.range_endpoint_resident_requested_bytes
        );
        v1.finish_candidate_mutation_deferred_native(v1_token)
            .unwrap();
        drain_test_retirement(&mut v1, v1_token, 256);

        let v1_noop = v1.begin_candidate_update_job("[]").unwrap();
        let noop = v1
            .step_candidate_mutation_output_memory_v1_native(v1_noop, 1)
            .unwrap();
        assert!(noop.done);
        assert!(v1
            .candidate_mutation_output_memory_plan_v1_native(v1_noop)
            .is_err());
        v1.finish_candidate_mutation_deferred_native(v1_noop)
            .unwrap();
        drain_test_retirement(&mut v1, v1_noop, 1);

        let mut v2 = fixture(TREE_V2, true);
        let v2_token = v2.begin_candidate_update_job(&rows).unwrap();
        drain_candidate_revision_job(&mut v2, v2_token);
        v2.finish_candidate_mutation_native(v2_token).unwrap();
    }

    #[test]
    fn v2_candidate_deferred_cancel_and_noop_keep_exact_state_and_token() {
        let rows = rebuild_page(&[rebuild_row("candidate-plan.md", 5)]);
        let mut cancelled = fixture(TREE_V2, true);
        let before = rebuild_snapshot(&cancelled);
        let token = cancelled.begin_candidate_update_job(&rows).unwrap();
        cancelled
            .step_candidate_mutation_output_memory_v1_native(token, 1)
            .unwrap();
        assert!(cancelled
            .cancel_candidate_mutation_deferred_native(token + 1)
            .is_err());
        assert_eq!(cancelled.tree_job.as_ref().unwrap().token(), token);
        cancelled
            .cancel_candidate_mutation_deferred_native(token)
            .unwrap();
        cancelled
            .cancel_candidate_mutation_deferred_native(token)
            .unwrap();
        assert!(cancelled.cancel_tree_job_inner(token).is_err());
        assert!(cancelled
            .finish_candidate_mutation_deferred_native(token)
            .is_err());
        assert!(cancelled
            .cancel_reachability_job_deferred_native(token)
            .is_err());
        assert!(cancelled
            .step_reachability_retirement_native(token, 0, 1)
            .is_err());
        drain_test_retirement(&mut cancelled, token, 1);
        assert_eq!(rebuild_snapshot(&cancelled), before);

        for cancel_phase in ["plan", "ready"] {
            let mut cancelled = fixture(TREE_V2, true);
            let before = rebuild_snapshot(&cancelled);
            let token = cancelled.begin_candidate_update_job(&rows).unwrap();
            let plan = loop {
                let progress = cancelled
                    .step_candidate_mutation_output_memory_v1_native(token, 1)
                    .unwrap();
                if progress.phase == "plan ready" {
                    break cancelled
                        .candidate_mutation_output_memory_plan_v1_native(token)
                        .unwrap();
                }
            };
            if cancel_phase == "ready" {
                cancelled
                    .resume_candidate_mutation_output_memory_v1_native(
                        token,
                        plan.node_payload_bytes,
                        plan.range_endpoint_peak_requested_bytes,
                        plan.range_endpoint_resident_requested_bytes,
                    )
                    .unwrap();
                loop {
                    let progress = cancelled
                        .step_candidate_mutation_output_memory_v1_native(token, 1)
                        .unwrap();
                    if progress.done {
                        break;
                    }
                }
                cancelled
                    .candidate_mutation_output_memory_ready_v1_native(token)
                    .unwrap();
            }
            cancelled
                .cancel_candidate_mutation_deferred_native(token)
                .unwrap();
            drain_test_retirement(&mut cancelled, token, 1);
            assert_eq!(rebuild_snapshot(&cancelled), before);
        }

        let mut noop = fixture(TREE_V2, true);
        let before = rebuild_snapshot(&noop);
        let token = noop.begin_candidate_update_job("[]").unwrap();
        let progress = noop
            .step_candidate_mutation_output_memory_v1_native(token, 256)
            .unwrap();
        assert_eq!(
            (
                progress.done,
                progress.units,
                progress.completed,
                progress.phase,
                progress.staged_chunks,
                progress.staged_bytes,
            ),
            (true, 0, 0, "ready", 0, 0)
        );
        assert!(noop
            .candidate_mutation_output_memory_plan_v1_native(token)
            .is_err());
        assert!(noop
            .candidate_mutation_output_memory_ready_v1_native(token)
            .is_err());
        noop.finish_candidate_mutation_deferred_native(token)
            .unwrap();
        assert_eq!(rebuild_snapshot(&noop), before);
        drain_test_retirement(&mut noop, token, 256);
        assert_eq!(rebuild_snapshot(&noop), before);

        let mut failed = fixture(TREE_V2, true);
        let before = rebuild_snapshot(&failed);
        let invalid = rebuild_page(&[FileEntry {
            path: "../invalid".into(),
            ..rebuild_row("valid.md", 7)
        }]);
        let token = failed.begin_candidate_update_job(&invalid).unwrap();
        assert!(failed
            .step_candidate_mutation_output_memory_v1_native(token, 1)
            .is_err());
        assert_eq!(failed.tree_job.as_ref().unwrap().token(), token);
        failed
            .cancel_candidate_mutation_deferred_native(token)
            .unwrap();
        drain_test_retirement(&mut failed, token, 1);
        assert_eq!(rebuild_snapshot(&failed), before);
    }

    #[test]
    fn candidate_revision_committed_replacements_clear_only_existing_owners() {
        for version in [TREE_V1, TREE_V2] {
            for replacement in 0..3 {
                let mut tree = fixture(version, false);
                let root = tree.root_bytes().unwrap().unwrap();
                tree.begin_candidate().unwrap();
                let before = state(&tree);
                // Direct committed apply refuses to overwrite an active
                // candidate and must not advance either witness on failure.
                assert!(tree
                    .mutate_committed_native(|inner| match inner {
                        WasmTreeInner::V1(inner) => run_local(inner.apply_committed(&[], &[])),
                        WasmTreeInner::V2(inner) => run_local(inner.apply_committed(&[], &[])),
                    })
                    .is_err());
                assert_eq!(state(&tree), before);
                assert_eq!(
                    (tree.candidate_revision(), tree.committed_revision()),
                    (1.0, 0.0)
                );
                match replacement {
                    0 => tree.load_root(&root).unwrap(),
                    1 => tree.build_from_entries("[]").unwrap(),
                    _ => tree
                        .rebuild_from_entries_in_version(
                            if version == TREE_V1 { TREE_V2 } else { TREE_V1 },
                            "[]",
                        )
                        .unwrap(),
                }
                assert!(!tree.has_candidate());
                assert_eq!(
                    (tree.candidate_revision(), tree.committed_revision()),
                    (2.0, 1.0)
                );
                tree.build_from_entries("[]").unwrap();
                tree.update_entry(
                    "committed.md",
                    &hash_to_hex(&hash_bytes(b"committed")),
                    1.0,
                    1.0,
                )
                .unwrap();
                assert_eq!(
                    tree.candidate_revision(),
                    2.0,
                    "internal committed transaction created no external candidate owner"
                );
                assert!(!tree.has_candidate());
            }
        }
    }

    #[test]
    fn candidate_revision_exhaustion_rejects_before_live_mutation_or_job_consumption() {
        for version in [TREE_V1, TREE_V2] {
            let mut tree = fixture(version, false);
            tree.candidate_revision = MAX_SAFE_CANDIDATE_REVISION;
            let before = state(&tree);
            let mut entered = false;
            assert!(tree
                .mutate_candidate_native(true, |inner, _| {
                    entered = true;
                    match inner {
                        WasmTreeInner::V1(inner) => inner.begin_candidate(),
                        WasmTreeInner::V2(inner) => run_local(inner.begin_candidate()),
                    }
                })
                .is_err());
            assert!(!entered);
            assert_eq!(state(&tree), before);
            let token = tree.begin_candidate_job().unwrap();
            drain_candidate_revision_job(&mut tree, token);
            assert!(tree.finish_candidate_job_native(token).is_err());
            assert_eq!(tree.tree_job.as_ref().unwrap().token(), token);
            assert_eq!(state(&tree), before);
            tree.cancel_tree_job_inner(token).unwrap();

            tree.candidate_revision = MAX_SAFE_CANDIDATE_REVISION - 1;
            tree.begin_candidate().unwrap();
            assert_eq!(
                tree.candidate_revision(),
                MAX_SAFE_CANDIDATE_REVISION as f64
            );
            let before = state(&tree);
            let committed = tree.committed_revision();
            assert!(abort_candidate_without_js_stats(&mut tree).is_err());
            assert!(commit_candidate_without_js_stats(&mut tree).is_err());
            assert!(tree
                .mutate_committed_native(|inner| match inner {
                    WasmTreeInner::V1(inner) => run_local(inner.rebuild(Vec::new())),
                    WasmTreeInner::V2(inner) => run_local(inner.rebuild(Vec::new())),
                })
                .is_err());
            assert_eq!(tree.committed_revision(), committed);
            assert_eq!(state(&tree), before);
            tree.candidate_update_batch("[]").unwrap();
            let noop = tree.begin_candidate_update_job("[]").unwrap();
            drain_candidate_revision_job(&mut tree, noop);
            tree.finish_candidate_mutation_native(noop).unwrap();
            assert_eq!(state(&tree), before);

            let token = tree
                .begin_candidate_delete_job(r#"["dir1/note.md"]"#)
                .unwrap();
            drain_candidate_revision_job(&mut tree, token);
            assert!(tree.finish_candidate_mutation_native(token).is_err());
            assert_eq!(tree.tree_job.as_ref().unwrap().token(), token);
            assert_eq!(state(&tree), before);
            tree.cancel_tree_job_inner(token).unwrap();
            assert_eq!(
                tree.candidate_revision(),
                MAX_SAFE_CANDIDATE_REVISION as f64
            );
        }
    }

    #[test]
    fn root_export_jobs_exact_legacy_bytes_for_both_scopes_and_formats() {
        for version in [TREE_V1, TREE_V2] {
            for scope in [RootExportScope::Candidate, RootExportScope::Committed] {
                for page in [1, 63 * 1024, 64 * 1024] {
                    let mut tree = fixture(version, true);
                    let before = state(&tree);
                    assert_ne!(before.0, before.1);
                    let (expected, hash) = match scope {
                        RootExportScope::Candidate => (
                            before.1.as_ref().unwrap(),
                            tree.candidate_root_hash_hex().unwrap(),
                        ),
                        RootExportScope::Committed => {
                            (before.0.as_ref().unwrap(), tree.root_hash_hex().unwrap())
                        }
                    };
                    let token = tree.begin_root_export_inner(scope).unwrap();
                    assert!(tree.root_export_workset_inner(token).is_err());
                    assert!(tree.root_export_info_inner(token).is_err());
                    assert!(tree.start_root_export_build_inner(token).is_err());
                    assert!(tree.read_root_export_inner(token, 0, 1).is_err());
                    assert!(tree.finish_root_export_inner(token).is_err());
                    assert!(drain(&mut tree, token, 1) > 0);
                    let workset = tree.root_export_workset_inner(token).unwrap();
                    assert!(workset.max_length >= expected.len() as u32);
                    if version == TREE_V2 {
                        assert!(workset.max_length <= 16 * 1024);
                        assert_eq!(workset.offset_bytes, 0);
                    }
                    assert!(tree.root_export_info_inner(token).is_err());
                    tree.start_root_export_build_inner(token).unwrap();
                    assert!(tree.start_root_export_build_inner(token).is_err());
                    assert!(tree.root_export_workset_inner(token).is_err());
                    assert!(drain(&mut tree, token, 7) > 0);
                    assert!(tree.read_root_export_inner(token, 0, 1).is_err());
                    let info = tree.root_export_info_inner(token).unwrap();
                    assert_eq!(info.version, version);
                    assert_eq!(info.hash, hash);
                    assert_eq!(info.length as usize, expected.len());
                    assert!(tree.step_root_export_job_native(token, 1, 1).is_err());
                    assert!(tree.finish_root_export_inner(token).is_err());
                    let mut output = Vec::new();
                    while output.len() < expected.len() {
                        let bytes = tree
                            .read_root_export_inner(token, output.len() as u32, page)
                            .unwrap();
                        assert!(bytes.len() <= page as usize);
                        output.extend_from_slice(&bytes);
                    }
                    assert_eq!(&output, expected);
                    assert!(tree
                        .read_root_export_inner(token, info.length, page)
                        .unwrap()
                        .is_empty());
                    tree.finish_root_export_inner(token).unwrap();
                    assert!(tree.finish_root_export_inner(token).is_err());
                    assert!(tree.ensure_tree_job_idle_native().is_ok());
                    assert_eq!(state(&tree), before);
                    output[0] ^= 1; // Returned pages do not alias native root bytes.
                    assert_eq!(state(&tree), before);
                }
            }
        }
    }

    #[test]
    fn root_export_jobs_wrong_tokens_kinds_offsets_and_reentrant_info_retain_owner() {
        for version in [TREE_V1, TREE_V2] {
            let mut tree = fixture(version, true);
            let token = tree
                .begin_root_export_inner(RootExportScope::Candidate)
                .unwrap();
            for wrong in [0, token + 1] {
                assert!(tree.step_tree_job_native(wrong, 1).is_err());
                assert!(tree.step_root_export_job_native(wrong, 1, 1).is_err());
                assert!(tree.root_export_workset_inner(wrong).is_err());
                assert!(tree.start_root_export_build_inner(wrong).is_err());
                assert!(tree.root_export_info_inner(wrong).is_err());
                assert!(tree.read_root_export_inner(wrong, 0, 1).is_err());
                assert!(tree.finish_root_export_inner(wrong).is_err());
                assert!(tree.cancel_tree_job_inner(wrong).is_err());
                assert_eq!(tree.tree_job.as_ref().unwrap().token(), token);
            }
            for budget in [0, 257] {
                assert!(tree.step_tree_job_native(token, budget).is_err());
            }
            assert!(tree.step_tree_job_native(token, 1).is_err());
            for budget in [0, ROOT_EXPORT_MAX_STEP_UNITS as u32 + 1] {
                assert!(tree.step_root_export_job_native(token, budget, 1).is_err());
            }
            for budget in [0, ROOT_EXPORT_MAX_STEP_BYTES as u32 + 1] {
                assert!(tree.step_root_export_job_native(token, 1, budget).is_err());
            }
            assert!(tree.finish_tree_chunk_export_inner(token).is_err());
            assert!(tree.read_tree_chunk_export_inner(token, 0, 1).is_err());
            assert!(tree
                .begin_root_export_inner(RootExportScope::Committed)
                .is_err());
            assert!(tree
                .begin_tree_chunk_export_inner(&"00".repeat(32))
                .is_err());
            drain(&mut tree, token, 256);
            tree.start_root_export_build_inner(token).unwrap();
            drain(&mut tree, token, 256);
            let info = tree.root_export_info_inner(token).unwrap();
            for (offset, budget) in [(1, 1), (0, 0), (0, 65537)] {
                assert!(tree.read_root_export_inner(token, offset, budget).is_err());
            }
            let first = tree.read_root_export_inner(token, 0, 1).unwrap();
            let again = tree.root_export_info_inner(token).unwrap();
            assert_eq!(again.length, info.length);
            assert!(tree.read_root_export_inner(token, 0, 1).is_err());
            assert!(tree.finish_root_export_inner(token).is_err());
            tree.cancel_tree_job_inner(token).unwrap();
            assert!(tree.read_root_export_inner(token, 1, 1).is_err());
            assert!(tree.finish_root_export_inner(token).is_err());
            let replacement = tree
                .begin_root_export_inner(RootExportScope::Committed)
                .unwrap();
            assert!(replacement > token);
            assert!(tree.cancel_tree_job_inner(token).is_err());
            tree.cancel_tree_job_inner(replacement).unwrap();
            assert_eq!(first.len(), 1);
            // A different ready job is not consumed by any root-export API.
            tree.tree_job = Some(WasmTreeJob::MutationNoop { token: 500 });
            assert!(tree.root_export_workset_inner(500).is_err());
            assert!(tree.start_root_export_build_inner(500).is_err());
            assert!(tree.root_export_info_inner(500).is_err());
            assert!(tree.read_root_export_inner(500, 0, 1).is_err());
            assert!(tree.finish_root_export_inner(500).is_err());
            assert!(tree.tree_job.as_ref().unwrap().ready());
            tree.cancel_tree_job_inner(500).unwrap();
        }
    }

    #[test]
    fn root_export_jobs_cancel_each_phase_and_partial_page_leave_roots_and_store_unchanged() {
        for version in [TREE_V1, TREE_V2] {
            for stage in 0..7 {
                let mut tree = fixture(version, true);
                let before = state(&tree);
                let token = tree
                    .begin_root_export_inner(RootExportScope::Candidate)
                    .unwrap();
                if stage >= 1 {
                    tree.step_root_export_job_native(token, 1, 1).unwrap();
                }
                if stage >= 2 {
                    drain(&mut tree, token, 256);
                }
                if stage >= 3 {
                    tree.start_root_export_build_inner(token).unwrap();
                }
                if stage >= 4 {
                    tree.step_root_export_job_native(token, 1, 1).unwrap();
                }
                if stage >= 5 {
                    drain(&mut tree, token, 256);
                    tree.root_export_info_inner(token).unwrap();
                }
                if stage >= 6 {
                    tree.read_root_export_inner(token, 0, 1).unwrap();
                }
                assert!(tree.ensure_tree_job_idle_native().is_err());
                tree.cancel_tree_job_inner(token).unwrap();
                assert!(tree.ensure_tree_job_idle_native().is_ok());
                assert_eq!(state(&tree), before);
            }
        }
    }

    #[test]
    fn root_export_jobs_scope_absence_token_exhaustion_and_execution_failure() {
        let mut empty = WasmTree::new("v", "d");
        for scope in [RootExportScope::Candidate, RootExportScope::Committed] {
            assert!(empty.begin_root_export_inner(scope).is_err());
            assert_eq!(empty.next_tree_job_token, 1);
            assert!(empty.tree_job.is_none());
        }
        for version in [TREE_V1, TREE_V2] {
            let mut tree = fixture(version, false);
            assert!(tree
                .begin_root_export_inner(RootExportScope::Candidate)
                .is_err());
            tree.next_tree_job_token = u32::MAX;
            assert!(tree
                .begin_root_export_inner(RootExportScope::Committed)
                .is_err());
            assert!(tree.tree_job.is_none());
            tree.next_tree_job_token = 5;
            let token = tree
                .begin_root_export_inner(RootExportScope::Committed)
                .unwrap();
            // Private test-only violation of the owner invariant. Execution
            // failure drops the provisional cursor, not a fabricated result.
            tree.inner = match version {
                TREE_V1 => WasmTreeInner::V1(TransactionalTree::new("root-export", "device")),
                _ => WasmTreeInner::V2(TransactionalTreeV2::new("root-export", "device")),
            };
            assert!(tree.step_root_export_job_native(token, 1, 1).is_err());
            assert!(tree.tree_job.is_none());
            assert!(tree.root_export_info_inner(token).is_err());
        }
        let mut tree = WasmTree::new("v", "d");
        let WasmTreeInner::V1(inner) = &mut tree.inner else {
            unreachable!()
        };
        inner.load_root_without_chunks(RootNode {
            vault_id: "v".into(),
            device_id: "d".into(),
            version: 1,
            created_ms: 0,
            total_files: 1,
            parent_hash: None,
            children: vec![("x".repeat(ROOT_EXPORT_PART_BYTES + 1), [0; 32])],
        });
        let before = state(&tree);
        let token = tree
            .begin_root_export_inner(RootExportScope::Committed)
            .unwrap();
        assert!(tree
            .step_root_export_job_native(token, 256, ROOT_EXPORT_MAX_STEP_BYTES as u32)
            .is_err());
        assert!(tree.tree_job.is_none());
        assert_eq!(state(&tree), before);
    }

    #[test]
    fn root_export_jobs_all_existing_mutators_keep_the_shared_idle_fence() {
        // Native JsValue error construction is unavailable; actual JS-host
        // rejection belongs to rebuilt-WASM parity. Assert the production
        // entrypoint wiring alongside the native predicate/state tests above.
        let source = include_str!("wasm.rs");
        let admission = &source[source
            .find("fn ensure_candidate_mutation_admission(")
            .unwrap()..];
        assert!(admission[admission.find('{').unwrap() + 1..]
            .trim_start()
            .starts_with("self.ensure_tree_job_idle()?;"));
        for name in [
            "set_tree_version",
            "load_root",
            "begin_candidate_job",
            "begin_candidate_chunks_job",
            "begin_candidate_update_job",
            "begin_candidate_delete_job",
            "begin_candidate",
            "commit_candidate",
            "abort_candidate",
            "update_entry",
            "delete_entry",
            "update_batch",
            "delete_batch",
            "candidate_update_batch",
            "candidate_delete_batch",
            "build_from_entries",
            "rebuild_from_entries_in_version",
        ] {
            let signature = format!("pub fn {name}(");
            let tail = &source[source.find(&signature).unwrap()..];
            let body = &tail[tail.find('{').unwrap() + 1..];
            let expected = if matches!(
                name,
                "begin_candidate_update_job" | "begin_candidate_delete_job"
            ) {
                "self.ensure_candidate_mutation_admission("
            } else {
                "self.ensure_tree_job_idle()?;"
            };
            assert!(
                body.trim_start().starts_with(expected),
                "missing idle fence: {name}"
            );
        }
    }

    #[test]
    fn root_export_jobs_v2_empty_and_max_descriptor_use_bounded_atomic_codec() {
        use crate::tree_v2::{RangeRef, TreeV2Root, MAX_PATH_BYTES};
        for populated in [false, true] {
            let root = RootNodeV2 {
                vault_id: "v".repeat(1024),
                device_id: "d".repeat(1024),
                created_ms: u64::MAX,
                parent_hash: Some(hash_bytes(b"parent")),
                tree: TreeV2Root {
                    version: TREE_V2,
                    total_files: u64::from(populated),
                    child: populated.then(|| RangeRef {
                        min_path: "a".repeat(MAX_PATH_BYTES),
                        max_path: "a".repeat(MAX_PATH_BYTES),
                        hash: hash_bytes(b"range"),
                        file_count: 1,
                        serialized_bytes: 200,
                        height: 0,
                    }),
                },
            };
            let expected = root.serialize().unwrap();
            let hash = hash_to_hex(&root.hash());
            let mut inner = TransactionalTreeV2::new(&root.vault_id, &root.device_id);
            inner.load_root_without_chunks(root);
            let mut tree = WasmTree::new("unused", "unused");
            tree.inner = WasmTreeInner::V2(inner);
            let token = tree
                .begin_root_export_inner(RootExportScope::Committed)
                .unwrap();
            let planned = tree.step_root_export_job_native(token, 1, 1).unwrap();
            assert!(planned.done);
            assert_eq!(planned.units, 1.0);
            let workset = tree.root_export_workset_inner(token).unwrap();
            assert_eq!(workset.max_length as usize, expected.len());
            assert!(workset.max_length <= 16 * 1024);
            assert_eq!(workset.offset_bytes, 0);
            tree.start_root_export_build_inner(token).unwrap();
            let built = tree.step_root_export_job_native(token, 1, 1).unwrap();
            assert!(built.done);
            assert_eq!(built.units, 1.0);
            assert_eq!(built.completed, 1.0);
            let info = tree.root_export_info_inner(token).unwrap();
            assert_eq!(info.hash, hash);
            assert_eq!(
                tree.read_root_export_inner(token, 0, 65536).unwrap(),
                expected
            );
            tree.finish_root_export_inner(token).unwrap();
        }
    }
}

#[cfg(test)]
mod chunk_export_tests {
    use super::*;
    use crate::store::{ChunkStore, MemoryChunkStore, MAX_CHUNK_EXPORT_PART_BYTES};
    use std::collections::BTreeMap;

    fn store(tree: &WasmTree) -> &MemoryChunkStore {
        match &tree.inner {
            WasmTreeInner::V1(inner) => inner.resident_store_for_test(),
            WasmTreeInner::V2(inner) => inner.resident_store_for_test(),
        }
    }

    fn state(
        tree: &WasmTree,
    ) -> (
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        BTreeMap<FileHash, Vec<u8>>,
    ) {
        (
            tree.root_bytes().unwrap(),
            tree.candidate_root_bytes().unwrap(),
            store(tree).all_chunks().into_iter().collect(),
        )
    }

    fn fixture(version: u32, candidate: bool) -> (WasmTree, FileHash, Vec<u8>) {
        let mut tree = WasmTree::new("export-vault", "export-device");
        let entries: Vec<_> = (0usize..1_300)
            .map(|index| {
                FileEntry::new(
                    format!("notes/{index:06}-{}.md", "path".repeat(16)),
                    hash_bytes(&index.to_le_bytes()),
                    index as u64,
                    123,
                )
            })
            .collect();
        match version {
            TREE_V1 => {
                let WasmTreeInner::V1(inner) = &mut tree.inner else {
                    unreachable!()
                };
                run_local(inner.rebuild(entries)).unwrap();
                if candidate {
                    inner.begin_candidate().unwrap();
                }
            }
            TREE_V2 => {
                let mut inner = TransactionalTreeV2::new("export-vault", "export-device");
                run_local(inner.rebuild(entries)).unwrap();
                if candidate {
                    run_local(inner.begin_candidate()).unwrap();
                }
                tree.inner = WasmTreeInner::V2(inner);
            }
            _ => unreachable!(),
        }
        let (hash, bytes) = store(&tree)
            .all_chunks()
            .into_iter()
            .max_by_key(|(_, bytes)| bytes.len())
            .unwrap();
        (tree, hash, bytes)
    }

    fn offset(tree: &WasmTree, token: u32) -> u32 {
        let job = tree.tree_job.as_ref().expect("export owner was lost");
        assert_eq!(job.token(), token);
        let WasmTreeJob::ChunkExport { offset, .. } = job else {
            panic!("export purpose changed")
        };
        *offset
    }

    #[test]
    fn chunk_export_exact_pages_preserve_both_formats_and_scopes() {
        for version in [TREE_V1, TREE_V2] {
            for candidate in [false, true] {
                let (mut tree, hash, expected) = fixture(version, candidate);
                let before = state(&tree);
                for budget in [1, 63 * 1024, 64 * 1024] {
                    let info = tree
                        .begin_tree_chunk_export_inner(&hash_to_hex(&hash))
                        .unwrap();
                    assert_eq!(info.length as usize, expected.len());
                    assert_eq!(
                        tree.has_candidate(),
                        candidate,
                        "begin opened a candidate as a side effect"
                    );
                    let mut actual = Vec::new();
                    while actual.len() < expected.len() {
                        let part = tree
                            .read_tree_chunk_export_inner(info.token, actual.len() as u32, budget)
                            .unwrap();
                        assert!(!part.is_empty());
                        assert!(part.len() <= budget as usize);
                        assert_eq!(part.capacity(), part.len());
                        actual.extend_from_slice(&part);
                    }
                    assert_eq!(actual, expected);
                    assert_eq!(hash_bytes(&actual), hash);
                    assert!(tree
                        .read_tree_chunk_export_inner(info.token, info.length, budget)
                        .unwrap()
                        .is_empty());
                    assert_eq!(offset(&tree, info.token), info.length);
                    tree.finish_tree_chunk_export_inner(info.token).unwrap();
                    assert!(tree.tree_job.is_none());
                    assert_eq!(state(&tree), before);
                }
            }
        }
    }

    #[test]
    fn chunk_export_invalid_calls_retain_owner_and_exact_offset() {
        for version in [TREE_V1, TREE_V2] {
            let (mut tree, hash, expected) = fixture(version, true);
            let before = state(&tree);
            let info = tree
                .begin_tree_chunk_export_inner(&hash_to_hex(&hash))
                .unwrap();
            assert!(tree
                .begin_tree_chunk_export_inner(&hash_to_hex(&hash))
                .is_err());
            for (token, at, budget) in [
                (info.token + 1, 0, 1),
                (info.token, 1, 1),
                (info.token, u32::MAX, 1),
                (info.token, 0, 0),
                (info.token, 0, 65_537),
                (info.token, 0, u32::MAX),
            ] {
                assert!(tree
                    .read_tree_chunk_export_inner(token, at, budget)
                    .is_err());
                assert_eq!(offset(&tree, info.token), 0);
            }
            for (token, budget) in [
                (info.token, 0),
                (info.token, 257),
                (info.token + 1, 1),
                (info.token, 1),
            ] {
                assert!(tree.step_tree_job_native(token, budget).is_err());
                assert_eq!(
                    offset(&tree, info.token),
                    0,
                    "step_tree_job consumed export ownership"
                );
            }
            assert!(tree.finish_tree_chunk_export_inner(info.token).is_err());
            assert!(tree.finish_tree_chunk_export_inner(info.token + 1).is_err());
            assert!(tree.cancel_tree_job_inner(info.token + 1).is_err());
            assert_eq!(offset(&tree, info.token), 0);
            assert_eq!(
                tree.read_tree_chunk_export_inner(info.token, 0, 17)
                    .unwrap(),
                expected[..17]
            );
            assert!(tree
                .read_tree_chunk_export_inner(info.token, 0, 17)
                .is_err());
            assert_eq!(offset(&tree, info.token), 17);
            assert_eq!(state(&tree), before);
            tree.cancel_tree_job_inner(info.token).unwrap();
            let replacement = tree
                .begin_tree_chunk_export_inner(&hash_to_hex(&hash))
                .unwrap();
            assert_ne!(replacement.token, info.token);
            assert!(tree.read_tree_chunk_export_inner(info.token, 0, 1).is_err());
            assert!(tree.finish_tree_chunk_export_inner(info.token).is_err());
            assert!(tree.cancel_tree_job_inner(info.token).is_err());
            assert_eq!(offset(&tree, replacement.token), 0);
            tree.cancel_tree_job_inner(replacement.token).unwrap();
            assert_eq!(state(&tree), before);
        }
    }

    #[test]
    fn chunk_export_ready_cancel_and_success_are_single_owner_boundaries() {
        for version in [TREE_V1, TREE_V2] {
            for candidate in [false, true] {
                let (mut tree, hash, expected) = fixture(version, candidate);
                let before = state(&tree);
                for cancel in [true, false] {
                    let info = tree
                        .begin_tree_chunk_export_inner(&hash_to_hex(&hash))
                        .unwrap();
                    while offset(&tree, info.token) < info.length {
                        let at = offset(&tree, info.token);
                        tree.read_tree_chunk_export_inner(
                            info.token,
                            at,
                            MAX_CHUNK_EXPORT_PART_BYTES as u32,
                        )
                        .unwrap();
                    }
                    assert!(tree.tree_job.as_ref().unwrap().ready());
                    assert!(tree.step_tree_job_native(info.token, 1).is_err());
                    assert_eq!(offset(&tree, info.token), expected.len() as u32);
                    assert!(tree.finish_tree_chunk_export_inner(info.token + 1).is_err());
                    if cancel {
                        tree.cancel_tree_job_inner(info.token).unwrap();
                    } else {
                        tree.finish_tree_chunk_export_inner(info.token).unwrap();
                    }
                    assert!(tree.finish_tree_chunk_export_inner(info.token).is_err());
                    assert!(tree
                        .read_tree_chunk_export_inner(info.token, info.length, 1)
                        .is_err());
                    assert!(tree.cancel_tree_job_inner(info.token).is_err());
                    assert_eq!(state(&tree), before);
                }
            }
        }
    }

    #[test]
    fn chunk_export_wrong_purpose_does_not_consume_a_ready_mutation() {
        let mut tree = WasmTree::new("v", "d");
        tree.tree_job = Some(WasmTreeJob::MutationNoop { token: 41 });
        assert!(tree.finish_tree_chunk_export_inner(41).is_err());
        assert!(tree.read_tree_chunk_export_inner(41, 0, 1).is_err());
        let progress = tree.step_tree_job_native(41, 1).unwrap();
        assert!(progress.done);
        assert_eq!(progress.units, 0.0);
        assert_eq!(tree.tree_job.as_ref().unwrap().token(), 41);
        tree.cancel_tree_job_inner(41).unwrap();
    }

    #[test]
    fn chunk_export_source_failures_preserve_owner_for_cancel_without_repair() {
        for version in [TREE_V1, TREE_V2] {
            for remove in [false, true] {
                // V2 pins all resident keys while a candidate exists. Exercise
                // missing-source export on committed state; replacement faults
                // still exercise an active candidate and its unchanged roots.
                let (mut tree, hash, original) = fixture(version, !remove);
                let roots = (
                    tree.root_bytes().unwrap(),
                    tree.candidate_root_bytes().unwrap(),
                );
                let info = tree
                    .begin_tree_chunk_export_inner(&hash_to_hex(&hash))
                    .unwrap();
                tree.read_tree_chunk_export_inner(info.token, 0, 17)
                    .unwrap();
                if remove {
                    run_local(store(&tree).delete(&hash)).unwrap();
                } else {
                    store(&tree).insert_chunk(hash, b"replaced-length".to_vec());
                }
                let fault_state: BTreeMap<_, _> = store(&tree).all_chunks().into_iter().collect();
                for _ in 0..2 {
                    assert!(tree
                        .read_tree_chunk_export_inner(info.token, 17, 64 * 1024)
                        .is_err());
                    assert_eq!(offset(&tree, info.token), 17);
                }
                assert!(tree.finish_tree_chunk_export_inner(info.token).is_err());
                tree.cancel_tree_job_inner(info.token).unwrap();
                assert_eq!(
                    store(&tree)
                        .all_chunks()
                        .into_iter()
                        .collect::<BTreeMap<_, _>>(),
                    fault_state
                );
                assert_eq!(
                    (
                        tree.root_bytes().unwrap(),
                        tree.candidate_root_bytes().unwrap()
                    ),
                    roots
                );
                store(&tree).insert_chunk(hash, original);
                let replacement = tree
                    .begin_tree_chunk_export_inner(&hash_to_hex(&hash))
                    .unwrap();
                assert!(replacement.token > info.token);
                tree.cancel_tree_job_inner(replacement.token).unwrap();
            }
        }
    }

    #[test]
    fn chunk_export_admission_empty_source_and_detached_page_lifetime() {
        let mut empty = WasmTree::new("v", "d");
        assert!(empty
            .begin_tree_chunk_export_inner(&"00".repeat(32))
            .is_err());
        assert!(empty.tree_job.is_none());
        assert_eq!(empty.next_tree_job_token, 1);
        for version in [TREE_V1, TREE_V2] {
            let (mut tree, hash, bytes) = fixture(version, false);
            for invalid in [
                "invalid".to_owned(),
                "00".repeat(32),
                "ab".repeat(33),
                "z".repeat(64),
            ] {
                assert!(tree.begin_tree_chunk_export_inner(&invalid).is_err());
                assert!(tree.tree_job.is_none());
                assert_eq!(tree.next_tree_job_token, 1);
            }
            let info = tree
                .begin_tree_chunk_export_inner(&hash_to_hex(&hash).to_uppercase())
                .unwrap();
            let mut part = tree.read_tree_chunk_export_inner(info.token, 0, 7).unwrap();
            part[0] ^= 255;
            assert_eq!(store(&tree).get_chunk(&hash).unwrap(), bytes);
            tree.cancel_tree_job_inner(info.token).unwrap();
            let empty_hash = hash_bytes(b"");
            // A resident orphan is intentional: begin is NOT a reachability
            // oracle. Production supplies a previously validated plan hash.
            store(&tree).insert_chunk(empty_hash, Vec::new());
            let zero = tree
                .begin_tree_chunk_export_inner(&hash_to_hex(&empty_hash))
                .unwrap();
            assert_eq!(zero.length, 0);
            assert!(tree
                .read_tree_chunk_export_inner(zero.token, 0, 1)
                .unwrap()
                .is_empty());
            tree.finish_tree_chunk_export_inner(zero.token).unwrap();
            tree.next_tree_job_token = u32::MAX;
            assert!(tree
                .begin_tree_chunk_export_inner(&hash_to_hex(&hash))
                .is_err());
            assert!(tree.tree_job.is_none());
            drop(tree);
            assert_eq!(part[0], bytes[0] ^ 255);
        }
    }
}

#[cfg(test)]
mod chunk_byte_length_tests {
    use super::*;

    #[test]
    fn wasm_tree_chunk_byte_length_matches_v1_and_v2_payloads() {
        for version in [TREE_V1, TREE_V2] {
            let mut tree = WasmTree::new("vault", "device");
            if version == TREE_V2 {
                tree.inner = WasmTreeInner::V2(TransactionalTreeV2::new("vault", "device"));
            }
            let entries = vec![FileEntry::new("note.md".into(), hash_bytes(b"note"), 1, 4)];
            let hashes = match &mut tree.inner {
                WasmTreeInner::V1(inner) => {
                    run_local(inner.rebuild(entries)).unwrap();
                    inner.committed_chunk_hashes().unwrap()
                }
                WasmTreeInner::V2(inner) => {
                    run_local(inner.rebuild(entries)).unwrap();
                    run_local(inner.committed_chunk_hashes()).unwrap()
                }
            };
            assert!(!hashes.is_empty());
            for hash in hashes {
                let hex = hash_to_hex(&hash);
                let length = wasm_tree_chunk_byte_length(&tree, &hex).unwrap();
                let bytes = wasm_tree_get_chunk(&tree, &hex).unwrap();
                assert_eq!(length as usize, bytes.len());
                assert_eq!(hash_bytes(&bytes), hash);
                assert_eq!(
                    wasm_tree_chunk_byte_length(&tree, &hex.to_uppercase()),
                    Some(length)
                );
            }
            assert_eq!(wasm_tree_chunk_byte_length(&tree, "invalid"), None);
            assert_eq!(wasm_tree_chunk_byte_length(&tree, &"00".repeat(32)), None);
        }
    }
}

/// Return hex hashes of all byte-addressed index chunks (LeafChunk/InternalNode)
/// held in the WASM tree's in-memory store.
/// The plugin calls this before putRoot to upload any chunks the server is missing.
#[wasm_bindgen]
pub fn wasm_tree_chunk_hashes(tree: &WasmTree) -> Result<Vec<String>, JsValue> {
    if tree.has_candidate() {
        wasm_tree_candidate_chunk_hashes(tree)
    } else {
        wasm_tree_committed_chunk_hashes(tree)
    }
}

#[wasm_bindgen]
pub fn wasm_tree_committed_chunk_hashes(tree: &WasmTree) -> Result<Vec<String>, JsValue> {
    match &tree.inner {
        WasmTreeInner::V1(inner) => inner.committed_chunk_hashes(),
        WasmTreeInner::V2(inner) => run_local(inner.committed_chunk_hashes()),
    }
    .map(hex_hashes)
    .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[wasm_bindgen]
pub fn wasm_tree_candidate_chunk_hashes(tree: &WasmTree) -> Result<Vec<String>, JsValue> {
    match &tree.inner {
        WasmTreeInner::V1(inner) => inner.candidate_chunk_hashes(),
        WasmTreeInner::V2(inner) => run_local(inner.candidate_chunk_hashes()),
    }
    .map(hex_hashes)
    .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[wasm_bindgen]
pub fn wasm_tree_new_candidate_chunk_hashes(tree: &WasmTree) -> Result<Vec<String>, JsValue> {
    match &tree.inner {
        WasmTreeInner::V1(inner) => inner.new_candidate_chunk_hashes(),
        WasmTreeInner::V2(inner) => run_local(inner.new_candidate_chunk_hashes()),
    }
    .map(hex_hashes)
    .map_err(|error| JsValue::from_str(&error.to_string()))
}

/// Parse the root hash from cached root bytes without loading the tree structure.
/// Used on startup: we need the hash for X-Parent-Root but must NOT load the root
/// into the WASM tree — load_root only stores the root node itself, not its children
/// (LeafChunk/InternalNode), so update_entry would fail with "chunk not found" for
/// any directory prefix that already has entries. The tree always bootstraps fresh
/// from sync-base on first push, which correctly populates the full MemoryChunkStore.
#[wasm_bindgen]
pub fn wasm_root_hash_from_bytes(bytes: &[u8]) -> Option<String> {
    let root = VersionedRoot::deserialize(bytes).ok()?;
    Some(hash_to_hex(&root.hash()))
}

#[wasm_bindgen]
pub fn wasm_root_version_from_bytes(bytes: &[u8]) -> Option<u32> {
    Some(VersionedRoot::deserialize(bytes).ok()?.version())
}

/// Run FastCDC sub-file chunking on a large file.
/// Returns JSON: { "file_hash": "hex", "total_size": u64, "chunks": [{ "hash": "hex", "offset": u64, "size": u32 }] }
#[wasm_bindgen]
pub fn wasm_chunk_file(data: &[u8]) -> JsValue {
    let chunked = crate::fastcdc_chunker::chunk_file(data);
    let result = serde_json::json!({
        "file_hash": hash_to_hex(&chunked.manifest.file_hash),
        "total_size": chunked.manifest.total_size,
        "chunks": chunked.manifest.chunks.iter().map(|c| {
            serde_json::json!({
                "hash": hash_to_hex(&c.hash),
                "offset": c.offset,
                "size": c.size,
            })
        }).collect::<Vec<_>>(),
    });
    to_js(&result)
}

/// Streaming FastCDC planner for memory-constrained clients. Feed small JS
/// views with update(); WASM retains a bounded ~4 MiB window and finish() returns only
/// the manifest, avoiding a second whole-file copy in WASM linear memory.
#[wasm_bindgen]
pub struct WasmChunker {
    inner: crate::fastcdc_chunker::StreamingChunker,
}

#[wasm_bindgen]
impl WasmChunker {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: crate::fastcdc_chunker::StreamingChunker::new(),
        }
    }

    pub fn update(&mut self, bytes: &[u8]) -> Result<(), JsValue> {
        self.inner.update(bytes).map_err(JsValue::from_str)
    }

    pub fn finish(&mut self) -> Result<JsValue, JsValue> {
        let manifest = self.inner.finish().map_err(JsValue::from_str)?;
        let result = serde_json::json!({
            "file_hash": hash_to_hex(&manifest.file_hash),
            "total_size": manifest.total_size,
            "chunks": manifest.chunks.iter().map(|chunk| {
                serde_json::json!({
                    "hash": hash_to_hex(&chunk.hash),
                    "offset": chunk.offset,
                    "size": chunk.size,
                })
            }).collect::<Vec<_>>(),
        });
        Ok(to_js(&result))
    }
}

/// Get a specific sub-file chunk's bytes after calling wasm_chunk_file.
/// This avoids sending all chunk data over the WASM bridge at once.
#[wasm_bindgen]
pub fn wasm_get_file_chunk(data: &[u8], offset: u32, size: u32) -> Vec<u8> {
    let start = offset as usize;
    let end = start + size as usize;
    if end <= data.len() {
        data[start..end].to_vec()
    } else {
        vec![]
    }
}

/// Check if a file should use sub-file chunking based on size.
#[wasm_bindgen]
pub fn wasm_should_chunk(size: u32) -> bool {
    crate::fastcdc_chunker::should_chunk(size as u64)
}

/// Hash N files in ONE WASM call — no JS re-entry between files.
///
/// The cost of crossing the JS↔WASM boundary is paid once regardless of N.
/// Rust iterates the file slices internally, keeping the hot path in native code.
///
/// data    — concatenated bytes of all files back-to-back
/// offsets — byte offset in `data` where each file starts (Uint32Array on JS side)
/// sizes   — byte length of each file (Uint32Array on JS side)
///
/// Returns a JS Array of hex strings, one per file, same order as offsets/sizes.
#[wasm_bindgen]
pub fn wasm_hash_batch(data: &[u8], offsets: &[u32], sizes: &[u32]) -> Vec<String> {
    offsets
        .iter()
        .zip(sizes.iter())
        .map(|(&off, &sz)| {
            let start = off as usize;
            let end = start + sz as usize;
            let slice = data.get(start..end).unwrap_or(&[]);
            hash_to_hex(&hash_bytes(slice))
        })
        .collect()
}

/// Streaming Blake3 hasher — feed the file in 64 KB chunks.
///
/// WASM linear memory grows to the largest single `&[u8]` slice it receives and
/// never shrinks back. Calling `wasm_hash(entireFile)` on a 500 MB PDF grows the
/// WASM heap to 500 MB for the entire session. Using this Hasher with 64 KB
/// chunks keeps the WASM heap bounded to ~64 KB per file.
///
/// Usage (TypeScript):
///   const h = new wasm.Hasher();
///   for each chunk: h.update(chunk);   // chunk ≤ 64 KB
///   const hex = h.finalize();
///   h.free();
#[wasm_bindgen]
pub struct Hasher {
    inner: blake3::Hasher,
}

#[wasm_bindgen]
impl Hasher {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: blake3::Hasher::new(),
        }
    }

    /// Feed the next chunk of file bytes. Call repeatedly until all bytes fed.
    pub fn update(&mut self, chunk: &[u8]) {
        self.inner.update(chunk);
    }

    /// Feed bytes into the running whole-file hash and return this slice's
    /// independent Blake3 address in the same JS→WASM copy. Pull uses this to
    /// validate a manifest chunk and the ordered full stream simultaneously.
    pub fn update_and_hash(&mut self, chunk: &[u8]) -> String {
        self.inner.update(chunk);
        hash_to_hex(&hash_bytes(chunk))
    }

    /// Return the final Blake3 hex hash. Non-consuming — safe to call once.
    pub fn finalize(&self) -> String {
        hash_to_hex(self.inner.finalize().as_bytes())
    }
}

// --- Internal helpers ---

#[derive(serde::Deserialize)]
struct RawEntry {
    path: String,
    hash: String,
    mtime_ms: u64,
    size: u64,
}

fn parse_entries_native(entries_json: &str) -> Result<Vec<FileEntry>, ChunkError> {
    let raw: Vec<RawEntry> = serde_json::from_str(entries_json)
        .map_err(|error| tree_job_error(format!("invalid entries JSON: {error}")))?;
    convert_raw_entries(raw)
}

/// Decode at most one admitted page, refusing the next element before its
/// fields/strings are materialized. The byte cap alone could otherwise admit
/// thousands of RawEntry allocations before the post-parse row check. The
/// legacy whole-tree decoder deliberately retains its existing semantics.
fn parse_entry_page_native(entries_json: &str) -> Result<Vec<FileEntry>, ChunkError> {
    use serde::de::{DeserializeSeed, Deserializer, Error, SeqAccess, Visitor};
    use std::cell::Cell;

    struct RejectExtra<'a>(&'a Cell<bool>);
    impl<'de> DeserializeSeed<'de> for RejectExtra<'_> {
        type Value = ();
        fn deserialize<D: Deserializer<'de>>(self, _: D) -> Result<(), D::Error> {
            self.0.set(true);
            Err(D::Error::custom(
                "replacement rebuild page has too many rows",
            ))
        }
    }
    struct Page<'a>(&'a Cell<bool>);
    impl<'de> Visitor<'de> for Page<'_> {
        type Value = Vec<RawEntry>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a bounded replacement entry page")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
            // No untrusted size_hint or geometric row-buffer growth.
            let mut rows = Vec::with_capacity(MAX_REPLACEMENT_REBUILD_FEED_ENTRIES);
            while rows.len() < MAX_REPLACEMENT_REBUILD_FEED_ENTRIES {
                match sequence.next_element::<RawEntry>()? {
                    Some(row) => rows.push(row),
                    None => return Ok(rows),
                }
            }
            let _: Option<()> = sequence.next_element_seed(RejectExtra(self.0))?;
            Ok(rows)
        }
    }
    let exceeded = Cell::new(false);
    let mut decoder = serde_json::Deserializer::from_str(entries_json);
    let raw = decoder.deserialize_seq(Page(&exceeded)).map_err(|error| {
        if exceeded.get() {
            tree_job_error("replacement rebuild feed entry count is invalid")
        } else {
            tree_job_error(format!("invalid entries JSON: {error}"))
        }
    })?;
    decoder
        .end()
        .map_err(|error| tree_job_error(format!("invalid entries JSON: {error}")))?;
    convert_raw_entries(raw)
}

fn convert_raw_entries(raw: Vec<RawEntry>) -> Result<Vec<FileEntry>, ChunkError> {
    raw.into_iter()
        .map(|entry| {
            let hash = hex_to_hash(&entry.hash)
                .map_err(|error| tree_job_error(format!("bad hash for {}: {error}", entry.path)))?;
            Ok(FileEntry::new(entry.path, hash, entry.mtime_ms, entry.size))
        })
        .collect()
}

#[cfg(test)]
mod entry_page_tests {
    use super::*;

    fn row(index: usize) -> serde_json::Value {
        serde_json::json!({"path":format!("é/𐀀/{index}.md"), "hash":"a1".repeat(32),
            "mtime_ms":index as u64, "size":index as u64 + 9})
    }

    #[test]
    fn bounded_entry_pages_preserve_exact_legacy_rows_and_errors() {
        for count in [0, 1, 17, MAX_REPLACEMENT_REBUILD_FEED_ENTRIES] {
            let input = serde_json::to_string(&(0..count).map(row).collect::<Vec<_>>()).unwrap();
            assert_eq!(
                parse_entry_page_native(&input).unwrap(),
                parse_entries_native(&input).unwrap()
            );
        }
        for input in [
            "[",
            "[{}]",
            "null",
            "{}",
            "[] []",
            "[true]",
            "[1]",
            "[\"row\"]",
        ] {
            assert!(
                parse_entry_page_native(input).is_err(),
                "accepted invalid page: {input}"
            );
        }
        let mut bad_hash = row(0);
        bad_hash["hash"] = "not-a-hash".into();
        let input = serde_json::to_string(&vec![bad_hash]).unwrap();
        assert_eq!(
            parse_entry_page_native(&input).unwrap_err().to_string(),
            parse_entries_native(&input).unwrap_err().to_string()
        );
    }

    #[test]
    fn page_row_limit_precedes_materializing_extra_row_even_if_its_fields_are_malformed() {
        let mut input = serde_json::to_string(&(0..256).map(row).collect::<Vec<_>>()).unwrap();
        input.pop();
        // The extra row is intentionally not valid RawEntry/JSON. Rejection
        // by the row seed, not an error parsing its fields, proves the page
        // limit is enforced before decoding the 257th record.
        input.push_str(", {\"path\": [malformed]");
        assert_eq!(
            parse_entry_page_native(&input).unwrap_err().to_string(),
            tree_job_error("replacement rebuild feed entry count is invalid").to_string()
        );
        let huge = serde_json::to_string(&(0..1800).map(row).collect::<Vec<_>>()).unwrap();
        assert_eq!(
            parse_entry_page_native(&huge).unwrap_err().to_string(),
            tree_job_error("replacement rebuild feed entry count is invalid").to_string()
        );
    }
}

fn parse_entries(entries_json: &str) -> Result<Vec<FileEntry>, JsValue> {
    // Preserve the existing binding's parse-error text while sharing the
    // actual decoder with bounded native feeds and their non-JS tests.
    parse_entries_native(entries_json).map_err(|error| match error {
        ChunkError::Deserialize(message) => JsValue::from_str(&message),
        other => JsValue::from_str(&other.to_string()),
    })
}

fn parse_paths(paths_json: &str) -> Result<Vec<String>, JsValue> {
    serde_json::from_str(paths_json)
        .map_err(|error| JsValue::from_str(&format!("invalid paths JSON: {error}")))
}

fn hex_hashes(hashes: Vec<crate::hash::FileHash>) -> Vec<String> {
    hashes.into_iter().map(|hash| hash_to_hex(&hash)).collect()
}

/// Run a !Send future synchronously. Works in WASM because WASM is single-threaded
/// and MemoryChunkStore operations resolve immediately (no actual I/O).
fn run_local<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    // In WASM, we can poll the future to completion since all I/O is synchronous
    // (MemoryChunkStore has no real async). Use a simple executor.
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    fn dummy_raw_waker() -> RawWaker {
        fn no_op(_: *const ()) {}
        fn clone(data: *const ()) -> RawWaker {
            RawWaker::new(data, &VTABLE)
        }
        const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
        RawWaker::new(std::ptr::null(), &VTABLE)
    }

    let waker = unsafe { Waker::from_raw(dummy_raw_waker()) };
    let mut cx = Context::from_waker(&waker);
    let mut future = Box::pin(future);

    match future.as_mut().poll(&mut cx) {
        Poll::Ready(value) => value,
        Poll::Pending => {
            // MemoryChunkStore should never return Pending.
            panic!("WASM async operation returned Pending — this should not happen with MemoryChunkStore");
        }
    }
}
