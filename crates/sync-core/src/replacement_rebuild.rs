//! Shared private-rebuild scheduling primitives. These count explicit work,
//! not wall-clock time or allocator/GC/RSS. A node codec remains indivisible.

pub(crate) use crate::candidate_mutation_v1::{Sort as StableSort, SortWorkspace};

/// Versioned conservative admission plan for immutable replacement output.
/// Node bytes cover retained canonical chunks. The endpoint fields cover the
/// format-specific retained root metadata (RangeRef strings for V2; root and
/// identity strings plus root-child backing for V1). Codec scratch, allocator
/// metadata, map buckets, linear memory and RSS remain outside this scope.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReplacementOutputMemoryPlanV1 {
    pub node_payload_bytes: u64,
    pub range_endpoint_peak_requested_bytes: u64,
    pub range_endpoint_resident_requested_bytes: u64,
    pub peak_admission_bytes: u64,
    pub resident_admission_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReplacementBuildProgress {
    pub done: bool,
    pub units: usize,
    pub completed: usize,
    pub phase: &'static str,
}
