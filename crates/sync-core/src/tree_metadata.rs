//! Exact owned String/Vec components, not total heap or an admission policy.
//!
//! A String is counted even when empty; length and capacity describe different
//! things. Hash-set capacity is only reported element capacity, never bucket
//! allocation bytes. Inline struct/enum headers, allocator overhead, graph
//! buffers, temporary codec/sort allocations and RSS are not included here.
//! Measurements do not allocate. V1's initial measurement walks only root
//! children; metered construction, snapshots and retirement updates are O(1).

use crate::chunk::RootNode;
use crate::hash::FileHash;
use crate::tree_v2::RootNodeV2;
#[cfg(test)]
use std::collections::HashSet;

fn add_usize(value: &mut usize, amount: usize, valid: &mut bool) {
    match value.checked_add(amount) {
        Some(next) => *value = next,
        None => {
            *value = usize::MAX;
            *valid = false;
        }
    }
}

fn subtract_usize(value: &mut usize, amount: usize, valid: &mut bool) {
    match value.checked_sub(amount) {
        Some(next) => *value = next,
        None => {
            *value = 0;
            *valid = false;
        }
    }
}

fn add_u64(value: &mut u64, amount: u64, valid: &mut bool) {
    match value.checked_add(amount) {
        Some(next) => *value = next,
        None => {
            *value = u64::MAX;
            *valid = false;
        }
    }
}

fn subtract_u64(value: &mut u64, amount: u64, valid: &mut bool) {
    match value.checked_sub(amount) {
        Some(next) => *value = next,
        None => {
            *value = 0;
            *valid = false;
        }
    }
}

fn bytes(value: usize, valid: &mut bool) -> u64 {
    match u64::try_from(value) {
        Ok(value) => value,
        Err(_) => {
            *valid = false;
            u64::MAX
        }
    }
}

fn backing_bytes(slots: usize, slot_bytes: usize, valid: &mut bool) -> u64 {
    let slots = bytes(slots, valid);
    let slot_bytes = bytes(slot_bytes, valid);
    match slots.checked_mul(slot_bytes) {
        Some(value) => value,
        None => {
            *valid = false;
            u64::MAX
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct StringMemory {
    pub strings: usize,
    pub length_bytes: u64,
    pub capacity_bytes: u64,
    pub counters_valid: bool,
}

impl Default for StringMemory {
    fn default() -> Self {
        Self {
            strings: 0,
            length_bytes: 0,
            capacity_bytes: 0,
            counters_valid: true,
        }
    }
}

impl StringMemory {
    pub(crate) fn of(value: &String) -> Self {
        let mut valid = true;
        Self {
            strings: 1,
            length_bytes: bytes(value.len(), &mut valid),
            capacity_bytes: bytes(value.capacity(), &mut valid),
            counters_valid: valid,
        }
    }

    pub(crate) fn combine(mut self, other: Self) -> Self {
        self.add_memory(other);
        self
    }

    pub(crate) fn add(&mut self, value: &String) {
        self.add_memory(Self::of(value));
    }

    pub(crate) fn subtract(&mut self, value: &String) {
        self.subtract_memory(Self::of(value));
    }

    pub(crate) fn add_memory(&mut self, other: Self) {
        self.counters_valid &= other.counters_valid;
        add_usize(&mut self.strings, other.strings, &mut self.counters_valid);
        add_u64(
            &mut self.length_bytes,
            other.length_bytes,
            &mut self.counters_valid,
        );
        add_u64(
            &mut self.capacity_bytes,
            other.capacity_bytes,
            &mut self.counters_valid,
        );
    }

    pub(crate) fn subtract_memory(&mut self, other: Self) {
        self.counters_valid &= other.counters_valid;
        subtract_usize(&mut self.strings, other.strings, &mut self.counters_valid);
        subtract_u64(
            &mut self.length_bytes,
            other.length_bytes,
            &mut self.counters_valid,
        );
        subtract_u64(
            &mut self.capacity_bytes,
            other.capacity_bytes,
            &mut self.counters_valid,
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct RootMetadataMemory {
    pub roots: usize,
    pub strings: StringMemory,
    pub v1_children_length: usize,
    pub v1_children_capacity: usize,
    pub v1_children_backing_capacity_bytes: u64,
    pub counters_valid: bool,
}

impl Default for RootMetadataMemory {
    fn default() -> Self {
        Self {
            roots: 0,
            strings: StringMemory::default(),
            v1_children_length: 0,
            v1_children_capacity: 0,
            v1_children_backing_capacity_bytes: 0,
            counters_valid: true,
        }
    }
}

impl RootMetadataMemory {
    /// One root-metadata pass, not a graph traversal. Do not call this from an
    /// O(1) snapshot or a ready-only native finish.
    pub(crate) fn from_v1(root: &RootNode) -> Self {
        let mut children = StringMemory::default();
        for (prefix, _) in &root.children {
            children.add(prefix);
        }
        Self::from_v1_metered(root, children)
    }

    /// `children` must be the producer's measurement of the actual strings
    /// moved into this exact Vec. Scalars are checked without a path walk;
    /// this is not validation of an untrusted caller-supplied byte estimate.
    pub(crate) fn from_v1_metered(root: &RootNode, mut children: StringMemory) -> Self {
        children.counters_valid &= children.strings == root.children.len()
            && children.length_bytes <= children.capacity_bytes;
        let strings = children
            .combine(StringMemory::of(&root.vault_id))
            .combine(StringMemory::of(&root.device_id));
        let mut valid = strings.counters_valid;
        let capacity_bytes = backing_bytes(
            root.children.capacity(),
            std::mem::size_of::<(String, FileHash)>(),
            &mut valid,
        );
        Self {
            roots: 1,
            strings,
            v1_children_length: root.children.len(),
            v1_children_capacity: root.children.capacity(),
            v1_children_backing_capacity_bytes: capacity_bytes,
            counters_valid: valid,
        }
    }

    pub(crate) fn from_v2(root: &RootNodeV2) -> Self {
        let mut strings =
            StringMemory::of(&root.vault_id).combine(StringMemory::of(&root.device_id));
        if let Some(child) = &root.tree.child {
            strings.add(&child.min_path);
            strings.add(&child.max_path);
        }
        Self {
            roots: 1,
            strings,
            counters_valid: strings.counters_valid,
            ..Self::default()
        }
    }

    pub(crate) fn combine(mut self, other: Self) -> Self {
        self.add(other);
        self
    }

    pub(crate) fn add(&mut self, other: Self) {
        self.counters_valid &= other.counters_valid;
        self.strings.add_memory(other.strings);
        self.counters_valid &= self.strings.counters_valid;
        add_usize(&mut self.roots, other.roots, &mut self.counters_valid);
        add_usize(
            &mut self.v1_children_length,
            other.v1_children_length,
            &mut self.counters_valid,
        );
        add_usize(
            &mut self.v1_children_capacity,
            other.v1_children_capacity,
            &mut self.counters_valid,
        );
        add_u64(
            &mut self.v1_children_backing_capacity_bytes,
            other.v1_children_backing_capacity_bytes,
            &mut self.counters_valid,
        );
    }

    #[cfg(test)]
    pub(crate) fn subtract(&mut self, other: Self) {
        self.counters_valid &= other.counters_valid;
        self.strings.subtract_memory(other.strings);
        self.counters_valid &= self.strings.counters_valid;
        subtract_usize(&mut self.roots, other.roots, &mut self.counters_valid);
        subtract_usize(
            &mut self.v1_children_length,
            other.v1_children_length,
            &mut self.counters_valid,
        );
        subtract_usize(
            &mut self.v1_children_capacity,
            other.v1_children_capacity,
            &mut self.counters_valid,
        );
        subtract_u64(
            &mut self.v1_children_backing_capacity_bytes,
            other.v1_children_backing_capacity_bytes,
            &mut self.counters_valid,
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct CandidateBaselineMemory {
    pub present: bool,
    pub hashes: usize,
    /// HashSet's reported element capacity, not bucket allocation size. Keep
    /// this observation with an IntoIter until its backing is actually freed.
    pub capacity_slots: usize,
    /// Logical key bytes only, not an additional allocation to add to buckets.
    pub logical_hash_bytes: u64,
    pub counters_valid: bool,
}

impl Default for CandidateBaselineMemory {
    fn default() -> Self {
        Self {
            present: false,
            hashes: 0,
            capacity_slots: 0,
            logical_hash_bytes: 0,
            counters_valid: true,
        }
    }
}

impl CandidateBaselineMemory {
    /// Insertion-generation candidate cuts own one scalar, not a key container.
    pub(crate) fn scalar() -> Self {
        Self {
            present: true,
            ..Self::default()
        }
    }

    #[cfg(test)]
    pub(crate) fn from_set(value: &HashSet<FileHash>) -> Self {
        let mut valid = true;
        let logical_hash_bytes =
            backing_bytes(value.len(), std::mem::size_of::<FileHash>(), &mut valid);
        Self {
            present: true,
            hashes: value.len(),
            capacity_slots: value.capacity(),
            logical_hash_bytes,
            counters_valid: valid,
        }
    }

    /// Aggregate disjoint owners. `present` means at least one owner, not a
    /// container count. Capacity sums remain logical observations only.
    pub(crate) fn combine(mut self, other: Self) -> Self {
        self.present |= other.present;
        self.counters_valid &= other.counters_valid;
        add_usize(&mut self.hashes, other.hashes, &mut self.counters_valid);
        add_usize(
            &mut self.capacity_slots,
            other.capacity_slots,
            &mut self.counters_valid,
        );
        add_u64(
            &mut self.logical_hash_bytes,
            other.logical_hash_bytes,
            &mut self.counters_valid,
        );
        self
    }

    /// Call only after an iterator actually yields a key. Empty iteration
    /// does not authorize dropping `present` or the retained backing capacity.
    #[cfg(test)]
    pub(crate) fn retire_hash(&mut self) {
        self.counters_valid &= self.present;
        subtract_usize(&mut self.hashes, 1, &mut self.counters_valid);
        subtract_u64(
            &mut self.logical_hash_bytes,
            std::mem::size_of::<FileHash>() as u64,
            &mut self.counters_valid,
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct TreeMetadataMemory {
    pub committed: RootMetadataMemory,
    pub candidate: RootMetadataMemory,
    pub tree_ids: StringMemory,
    pub candidate_baseline: CandidateBaselineMemory,
    pub counters_valid: bool,
}

impl Default for TreeMetadataMemory {
    fn default() -> Self {
        Self::new(
            RootMetadataMemory::default(),
            RootMetadataMemory::default(),
            StringMemory::default(),
            CandidateBaselineMemory::default(),
        )
    }
}

impl TreeMetadataMemory {
    pub(crate) fn new(
        committed: RootMetadataMemory,
        candidate: RootMetadataMemory,
        tree_ids: StringMemory,
        candidate_baseline: CandidateBaselineMemory,
    ) -> Self {
        Self {
            counters_valid: committed.counters_valid
                && committed.strings.counters_valid
                && candidate.counters_valid
                && candidate.strings.counters_valid
                && tree_ids.counters_valid
                && candidate_baseline.counters_valid,
            committed,
            candidate,
            tree_ids,
            candidate_baseline,
        }
    }

    pub(crate) fn combine(self, other: Self) -> Self {
        let mut value = Self::new(
            self.committed.combine(other.committed),
            self.candidate.combine(other.candidate),
            self.tree_ids.combine(other.tree_ids),
            self.candidate_baseline.combine(other.candidate_baseline),
        );
        value.counters_valid &= self.counters_valid && other.counters_valid;
        value
    }
}

/// Root and its meter move together. No mutable root escape: callers replacing
/// dynamic fields must install a newly measured owner, not reuse this cache.
#[derive(Debug)]
pub(crate) struct OwnedRootV1 {
    value: RootNode,
    metadata: RootMetadataMemory,
}

impl OwnedRootV1 {
    pub(crate) fn measured(value: RootNode) -> Self {
        let metadata = RootMetadataMemory::from_v1(&value);
        Self { value, metadata }
    }

    pub(crate) fn metered(value: RootNode, child_strings: StringMemory) -> Self {
        let metadata = RootMetadataMemory::from_v1_metered(&value, child_strings);
        Self { value, metadata }
    }

    pub(crate) fn value(&self) -> &RootNode {
        &self.value
    }
    pub(crate) fn metadata(&self) -> RootMetadataMemory {
        self.metadata
    }

    #[cfg(test)]
    pub(crate) fn into_parts(self) -> (RootNode, RootMetadataMemory) {
        (self.value, self.metadata)
    }

    /// Measure the new allocations while cloning. Source spare capacity and
    /// cached counters are never copied as proof of destination ownership.
    #[cfg(test)]
    pub(crate) fn measured_clone(&self) -> Self {
        let mut child_strings = StringMemory::default();
        let mut children = Vec::with_capacity(self.value.children.len());
        for (prefix, hash) in &self.value.children {
            let prefix = prefix.clone();
            child_strings.add(&prefix);
            children.push((prefix, *hash));
        }
        Self::metered(
            RootNode {
                vault_id: self.value.vault_id.clone(),
                created_ms: self.value.created_ms,
                version: self.value.version,
                children,
                total_files: self.value.total_files,
                parent_hash: self.value.parent_hash,
                device_id: self.value.device_id.clone(),
            },
            child_strings,
        )
    }

    pub(crate) fn retire_child(&mut self) -> bool {
        let Some((prefix, _)) = self.value.children.pop() else {
            return false;
        };
        self.metadata.strings.subtract(&prefix);
        subtract_usize(
            &mut self.metadata.v1_children_length,
            1,
            &mut self.metadata.counters_valid,
        );
        self.metadata.counters_valid &= self.metadata.strings.counters_valid;
        // Vec capacity remains owned even after the final child is dropped.
        true
    }

    pub(crate) fn retire_vault(&mut self) -> bool {
        Self::retire_identity(&mut self.value.vault_id, &mut self.metadata)
    }

    pub(crate) fn retire_device(&mut self) -> bool {
        Self::retire_identity(&mut self.value.device_id, &mut self.metadata)
    }

    fn retire_identity(value: &mut String, metadata: &mut RootMetadataMemory) -> bool {
        if value.capacity() == 0 {
            return false;
        }
        let old = std::mem::take(value);
        metadata.strings.subtract(&old);
        // Unlike Option<String> retirement, the root still contains the new
        // empty String value until the root itself is dropped.
        metadata.strings.add(value);
        metadata.counters_valid &= metadata.strings.counters_valid;
        true
    }

    /// False means only empty root fields/backing remain. The outer owner must
    /// then drop this wrapper; do not zero its metadata before that actual drop.
    #[cfg(test)]
    pub(crate) fn retire_one(&mut self) -> bool {
        self.retire_child() || self.retire_vault() || self.retire_device()
    }
}

/// Immutable candidate-history ownership, measured without collecting keys.
#[derive(Debug)]
#[cfg(test)]
pub(crate) struct OwnedBaseline {
    value: HashSet<FileHash>,
    metadata: CandidateBaselineMemory,
}

#[cfg(test)]
impl OwnedBaseline {
    pub(crate) fn new(value: HashSet<FileHash>) -> Self {
        let metadata = CandidateBaselineMemory::from_set(&value);
        Self { value, metadata }
    }

    pub(crate) fn value(&self) -> &HashSet<FileHash> {
        &self.value
    }
    pub(crate) fn metadata(&self) -> CandidateBaselineMemory {
        self.metadata
    }

    pub(crate) fn into_parts(self) -> (HashSet<FileHash>, CandidateBaselineMemory) {
        (self.value, self.metadata)
    }

    #[cfg(test)]
    pub(crate) fn measured_clone(&self) -> Self {
        Self::new(self.value.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree_v2::{RangeRef, TreeV2Root};

    fn string(value: &str, capacity: usize) -> String {
        let mut result = String::with_capacity(capacity);
        result.push_str(value);
        result
    }

    fn root() -> RootNode {
        let mut children = Vec::with_capacity(29);
        children.push((string("", 41), [1; 32]));
        children.push((string("é/", 127), [2; 32]));
        RootNode {
            vault_id: string("vault", 97),
            created_ms: 123,
            version: 1,
            children,
            total_files: 13,
            parent_hash: Some([9; 32]),
            device_id: string("", 53),
        }
    }

    #[test]
    fn strings_measure_actual_empty_and_spare_buffers_and_disjoint_owners() {
        let a = string("é", 37);
        let empty = string("", 29);
        let zero = String::new();
        let mut meter = StringMemory::default();
        for value in [&a, &empty, &zero] {
            meter.add(value);
        }
        assert_eq!(meter.strings, 3);
        assert_eq!(meter.length_bytes, a.len() as u64);
        assert_eq!(
            meter.capacity_bytes,
            (a.capacity() + empty.capacity()) as u64
        );
        assert!(meter.counters_valid);
        assert_eq!(StringMemory::of(&zero).strings, 1);
        let duplicate = StringMemory::of(&a).combine(StringMemory::of(&a));
        assert_eq!(duplicate.capacity_bytes, 2 * a.capacity() as u64);
        for value in [&a, &empty, &zero] {
            meter.subtract(value);
        }
        assert_eq!(meter, StringMemory::default());
    }

    #[test]
    fn arithmetic_failures_saturate_and_remain_invalid() {
        let mut maximum = StringMemory {
            strings: usize::MAX,
            length_bytes: u64::MAX,
            capacity_bytes: u64::MAX,
            counters_valid: true,
        };
        maximum.add(&"x".to_owned());
        assert_eq!(maximum.strings, usize::MAX);
        assert_eq!(maximum.capacity_bytes, u64::MAX);
        assert!(!maximum.counters_valid);
        maximum.subtract(&"x".to_owned());
        maximum.add_memory(StringMemory::default());
        assert!(!maximum.counters_valid);
        let mut missing = StringMemory::default();
        missing.subtract(&"x".to_owned());
        assert_eq!(
            (
                missing.strings,
                missing.length_bytes,
                missing.capacity_bytes
            ),
            (0, 0, 0)
        );
        missing.add(&"x".to_owned());
        assert!(!missing.counters_valid);
        let mut valid = true;
        let product = (usize::MAX as u128) * (usize::MAX as u128);
        assert_eq!(
            backing_bytes(usize::MAX, usize::MAX, &mut valid),
            product.min(u64::MAX as u128) as u64
        );
        assert_eq!(valid, product <= u64::MAX as u128);
    }

    #[test]
    fn v1_measured_metered_clone_and_move_keep_actual_capacity_and_canonical_bytes() {
        let root = root();
        let wire = root.serialize();
        let children_pointer = root.children.as_ptr();
        let mut children = StringMemory::default();
        for (prefix, _) in &root.children {
            children.add(prefix);
        }
        let expected = RootMetadataMemory::from_v1(&root);
        assert_eq!(expected.strings.strings, 4);
        assert_eq!(
            expected.v1_children_backing_capacity_bytes,
            (root.children.capacity() * std::mem::size_of::<(String, FileHash)>()) as u64
        );
        let owner = OwnedRootV1::metered(root, children);
        assert_eq!(owner.metadata(), expected);
        assert_eq!(owner.value().children.as_ptr(), children_pointer);
        let cloned = owner.measured_clone();
        assert_eq!(cloned.value().serialize(), wire);
        assert_eq!(
            cloned.metadata(),
            RootMetadataMemory::from_v1(cloned.value())
        );
        assert_ne!(
            cloned.value().children.as_ptr(),
            owner.value().children.as_ptr()
        );
        for ((copy, _), (source, _)) in cloned.value().children.iter().zip(&owner.value().children)
        {
            if !copy.is_empty() {
                assert_ne!(copy.as_ptr(), source.as_ptr());
            }
        }
        let (moved, moved_metadata) = owner.into_parts();
        assert_eq!(moved.children.as_ptr(), children_pointer);
        assert_eq!(moved_metadata, expected);
        assert_eq!(moved.serialize(), wire);
    }

    #[test]
    fn invalid_producer_meter_does_not_change_root_or_become_valid_during_retirement() {
        let root = root();
        let wire = root.serialize();
        let mut owner = OwnedRootV1::metered(root, StringMemory::default());
        assert!(!owner.metadata().counters_valid);
        assert!(!owner.metadata().strings.counters_valid);
        assert_eq!(owner.value().serialize(), wire);
        while owner.retire_one() {
            assert!(!owner.metadata().counters_valid);
        }
        assert!(!owner.metadata().counters_valid);
    }

    #[test]
    fn v1_retirement_counts_empty_identity_values_and_retained_vector_backing() {
        let mut owner = OwnedRootV1::measured(root());
        let capacity = owner.metadata().v1_children_backing_capacity_bytes;
        assert!(capacity > 0);
        let mut steps = 0;
        while owner.retire_one() {
            steps += 1;
            assert_eq!(owner.metadata(), RootMetadataMemory::from_v1(owner.value()));
            assert_eq!(
                owner.metadata().v1_children_backing_capacity_bytes,
                capacity
            );
        }
        assert_eq!(steps, 4); // two children and two allocated IDs (one empty)
        let final_metadata = owner.metadata();
        assert_eq!(final_metadata.roots, 1);
        assert_eq!(final_metadata.strings.strings, 2);
        assert_eq!(
            (
                final_metadata.strings.length_bytes,
                final_metadata.strings.capacity_bytes
            ),
            (0, 0)
        );
        assert_eq!(final_metadata.v1_children_length, 0);
        assert!(final_metadata.v1_children_capacity > 0);
        assert!(!owner.retire_one());
    }

    #[test]
    fn v2_measurement_has_only_actual_string_buffers_and_no_v1_vector() {
        let root = RootNodeV2 {
            vault_id: string("vault", 41),
            device_id: string("", 67),
            created_ms: 0,
            parent_hash: Some([9; 32]),
            tree: TreeV2Root {
                version: 2,
                total_files: 1,
                child: Some(RangeRef {
                    min_path: string("same.md", 31),
                    max_path: string("same.md", 71),
                    hash: [2; 32],
                    file_count: 1,
                    serialized_bytes: 57,
                    height: 0,
                }),
            },
        };
        let metadata = RootMetadataMemory::from_v2(&root);
        let range = root.tree.child.as_ref().unwrap();
        assert_eq!(metadata.roots, 1);
        assert_eq!(metadata.strings.strings, 4);
        assert_eq!(
            metadata.strings.capacity_bytes,
            (root.vault_id.capacity()
                + root.device_id.capacity()
                + range.min_path.capacity()
                + range.max_path.capacity()) as u64
        );
        assert_eq!(
            (
                metadata.v1_children_length,
                metadata.v1_children_capacity,
                metadata.v1_children_backing_capacity_bytes
            ),
            (0, 0, 0)
        );
        let mut empty = root;
        empty.tree.child = None;
        assert_eq!(RootMetadataMemory::from_v2(&empty).strings.strings, 2);
    }

    #[test]
    fn baseline_move_retains_logical_capacity_until_owner_drop() {
        let mut hashes = HashSet::with_capacity(37);
        hashes.insert([1; 32]);
        hashes.insert([2; 32]);
        let expected = CandidateBaselineMemory::from_set(&hashes);
        let owner = OwnedBaseline::new(hashes);
        assert!(owner.value().contains(&[1; 32]));
        assert_eq!(owner.metadata(), expected);
        let cloned = owner.measured_clone();
        assert_eq!(
            cloned.metadata(),
            CandidateBaselineMemory::from_set(cloned.value())
        );
        let (value, mut meter) = owner.into_parts();
        let mut iter = value.into_iter();
        while iter.next().is_some() {
            meter.retire_hash();
            assert!(meter.counters_valid);
            assert_eq!(meter.hashes, iter.len());
            assert_eq!(meter.capacity_slots, expected.capacity_slots);
        }
        assert_eq!(meter.logical_hash_bytes, 0);
        assert!(meter.present && meter.capacity_slots > 0);
        meter.retire_hash();
        assert!(!meter.counters_valid);
        let empty = HashSet::with_capacity(13);
        let empty = CandidateBaselineMemory::from_set(&empty);
        assert!(empty.present && empty.capacity_slots >= 13);
        assert_ne!(empty, CandidateBaselineMemory::default());
    }

    #[test]
    fn root_and_tree_aggregates_are_disjoint_checked_and_path_free() {
        let root = root();
        let metadata = RootMetadataMemory::from_v1(&root);
        let mut total = metadata.combine(metadata);
        assert_eq!(total.roots, 2);
        assert_eq!(
            total.strings.capacity_bytes,
            2 * metadata.strings.capacity_bytes
        );
        total.subtract(metadata);
        assert_eq!(total, metadata);
        total.subtract(metadata);
        assert_eq!(total, RootMetadataMemory::default());
        total.subtract(metadata);
        assert!(!total.counters_valid);
        let tree = TreeMetadataMemory::new(
            metadata,
            metadata,
            StringMemory::of(&root.vault_id),
            CandidateBaselineMemory::from_set(&HashSet::new()),
        );
        let combined = tree.combine(tree);
        assert_eq!(combined.committed.roots, 2);
        assert_eq!(combined.candidate.roots, 2);
        assert!(combined.counters_valid && combined.candidate_baseline.present);
        let json = serde_json::to_value(combined).unwrap();
        assert_eq!(json["committed"]["strings"]["strings"], 8);
        assert!(!serde_json::to_string(&json).unwrap().contains("vault"));
        let mut invalid = tree;
        invalid.counters_valid = false;
        assert!(
            !invalid
                .combine(TreeMetadataMemory::default())
                .counters_valid
        );
        let mut broken = metadata;
        broken.strings.counters_valid = false;
        assert!(
            !TreeMetadataMemory::new(
                broken,
                RootMetadataMemory::default(),
                StringMemory::default(),
                CandidateBaselineMemory::default()
            )
            .counters_valid
        );
    }
}
