//! Exact additive components for private tree worksets.
//!
//! These counters describe requested Vec backing and owned String buffers only.
//! They exclude inline headers, allocator metadata/buckets, serde/wasm-bindgen
//! scratch, codec buffers, the rest of a builder, linear-memory high-water and
//! process RSS. Measurements are ownership diagnostics, never admission.

use crate::chunk::FileEntry;
use crate::tree_metadata::StringMemory;
use crate::tree_v2::RangeRef;

fn as_u64(value: usize, valid: &mut bool) -> u64 {
    match u64::try_from(value) {
        Ok(value) => value,
        Err(_) => {
            *valid = false;
            u64::MAX
        }
    }
}

fn backing_bytes(slots: usize, slot_bytes: usize, valid: &mut bool) -> u64 {
    match as_u64(slots, valid).checked_mul(as_u64(slot_bytes, valid)) {
        Some(value) => value,
        None => {
            *valid = false;
            u64::MAX
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct VecBackingMemory {
    pub owners: usize,
    pub length_slots: usize,
    pub capacity_slots: usize,
    pub slot_size_bytes: usize,
    pub backing_capacity_bytes: u64,
    pub counters_valid: bool,
}

impl Default for VecBackingMemory {
    fn default() -> Self {
        Self {
            owners: 0,
            length_slots: 0,
            capacity_slots: 0,
            slot_size_bytes: 0,
            backing_capacity_bytes: 0,
            counters_valid: true,
        }
    }
}

impl VecBackingMemory {
    pub(crate) fn of<T>(values: &Vec<T>) -> Self {
        let mut valid = values.len() <= values.capacity();
        Self {
            owners: 1,
            length_slots: values.len(),
            capacity_slots: values.capacity(),
            slot_size_bytes: std::mem::size_of::<T>(),
            backing_capacity_bytes: backing_bytes(
                values.capacity(),
                std::mem::size_of::<T>(),
                &mut valid,
            ),
            counters_valid: valid,
        }
    }

    /// A plain Vec field with no allocation is outside backing-memory scope.
    /// Once capacity exists, its owner remains visible after length reaches 0.
    fn allocated<T>(values: &Vec<T>) -> Self {
        if values.capacity() == 0 {
            Self::default()
        } else {
            Self::of(values)
        }
    }

    fn refresh<T>(&mut self, values: &Vec<T>) {
        let next = Self::of(values);
        self.counters_valid &= next.counters_valid;
        self.owners = next.owners;
        self.length_slots = next.length_slots;
        self.capacity_slots = next.capacity_slots;
        self.slot_size_bytes = next.slot_size_bytes;
        self.backing_capacity_bytes = next.backing_capacity_bytes;
    }

    fn remove_slot(&mut self) {
        match self.length_slots.checked_sub(1) {
            Some(next) => self.length_slots = next,
            None => {
                self.length_slots = 0;
                self.counters_valid = false;
            }
        }
    }

    fn owners_empty(&self) -> bool {
        self.owners == 0
            && self.length_slots == 0
            && self.capacity_slots == 0
            && self.slot_size_bytes == 0
            && self.backing_capacity_bytes == 0
    }
}

/// Exact aggregate of top-level Vec backing allocations with the same slot
/// type. Unlike `VecBackingMemory`, more than one owner can be live at once.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct VecBackingAggregateMemory {
    pub owners: usize,
    pub length_slots: usize,
    pub capacity_slots: usize,
    pub slot_size_bytes: usize,
    pub backing_capacity_bytes: u64,
    pub counters_valid: bool,
}

impl Default for VecBackingAggregateMemory {
    fn default() -> Self {
        Self {
            owners: 0,
            length_slots: 0,
            capacity_slots: 0,
            slot_size_bytes: 0,
            backing_capacity_bytes: 0,
            counters_valid: true,
        }
    }
}

impl VecBackingAggregateMemory {
    fn add_component(&mut self, component: VecBackingMemory) {
        self.counters_valid &= component.counters_valid;
        if self.owners == 0 {
            self.slot_size_bytes = component.slot_size_bytes;
        } else {
            self.counters_valid &= self.slot_size_bytes == component.slot_size_bytes;
        }
        self.owners = match self.owners.checked_add(1) {
            Some(value) => value,
            None => {
                self.counters_valid = false;
                usize::MAX
            }
        };
        self.length_slots = match self.length_slots.checked_add(component.length_slots) {
            Some(value) => value,
            None => {
                self.counters_valid = false;
                usize::MAX
            }
        };
        self.capacity_slots = match self.capacity_slots.checked_add(component.capacity_slots) {
            Some(value) => value,
            None => {
                self.counters_valid = false;
                usize::MAX
            }
        };
        self.backing_capacity_bytes = match self
            .backing_capacity_bytes
            .checked_add(component.backing_capacity_bytes)
        {
            Some(value) => value,
            None => {
                self.counters_valid = false;
                u64::MAX
            }
        };
        self.check();
    }

    fn add<T>(&mut self, values: &Vec<T>) {
        self.add_component(VecBackingMemory::of(values));
    }

    fn remove_component(&mut self, component: VecBackingMemory) {
        self.counters_valid &= component.counters_valid
            && self.owners > 0
            && self.slot_size_bytes == component.slot_size_bytes;
        self.owners = self.owners.checked_sub(1).unwrap_or_else(|| {
            self.counters_valid = false;
            0
        });
        self.length_slots = self
            .length_slots
            .checked_sub(component.length_slots)
            .unwrap_or_else(|| {
                self.counters_valid = false;
                0
            });
        self.capacity_slots = self
            .capacity_slots
            .checked_sub(component.capacity_slots)
            .unwrap_or_else(|| {
                self.counters_valid = false;
                0
            });
        self.backing_capacity_bytes = self
            .backing_capacity_bytes
            .checked_sub(component.backing_capacity_bytes)
            .unwrap_or_else(|| {
                self.counters_valid = false;
                0
            });
        if self.owners == 0 {
            self.counters_valid &= self.length_slots == 0
                && self.capacity_slots == 0
                && self.backing_capacity_bytes == 0;
            self.slot_size_bytes = 0;
        }
        self.check();
    }

    fn remove<T>(&mut self, values: &Vec<T>) {
        self.remove_component(VecBackingMemory::of(values));
    }

    fn replace(&mut self, before: VecBackingMemory, after: VecBackingMemory) {
        self.remove_component(before);
        self.add_component(after);
    }

    fn check(&mut self) {
        self.counters_valid &= self.length_slots <= self.capacity_slots
            && ((self.owners == 0
                && self.slot_size_bytes == 0
                && self.length_slots == 0
                && self.capacity_slots == 0
                && self.backing_capacity_bytes == 0)
                || (self.owners > 0
                    && self.backing_capacity_bytes
                        == backing_bytes(
                            self.capacity_slots,
                            self.slot_size_bytes,
                            &mut self.counters_valid,
                        )));
    }
}

/// Exact retained ownership for original FileEntry rows while a private Tree
/// v1 replacement redistributes them. The active sorter contributes only its
/// logical row count here: its values Vec is already owned by the sort ABI.
/// Prefix keys, generated child labels, codec scratch and allocator metadata
/// are deliberately outside this additive component.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ReplacementV1EntriesMemory {
    pub input: VecBackingMemory,
    pub groups: VecBackingAggregateMemory,
    pub sorting_rows: usize,
    pub rows: VecBackingMemory,
    pub leaf: VecBackingMemory,
    pub retiring_rows: VecBackingMemory,
    pub paths: StringMemory,
    pub counters_valid: bool,
}

impl Default for ReplacementV1EntriesMemory {
    fn default() -> Self {
        Self {
            input: VecBackingMemory::default(),
            groups: VecBackingAggregateMemory::default(),
            sorting_rows: 0,
            rows: VecBackingMemory::default(),
            leaf: VecBackingMemory::default(),
            retiring_rows: VecBackingMemory::default(),
            paths: StringMemory::default(),
            counters_valid: true,
        }
    }
}

impl ReplacementV1EntriesMemory {
    pub(crate) fn from_input(input: ReplacementInputMemory) -> Self {
        let mut result = Self {
            input: input.entries,
            paths: input.paths,
            counters_valid: input.counters_valid,
            ..Self::default()
        };
        result.check();
        result
    }

    pub(crate) fn input_to_new_group(&mut self, group: &Vec<FileEntry>) {
        self.input.remove_slot();
        self.groups.add(group);
        self.check();
    }

    pub(crate) fn input_to_existing_group(
        &mut self,
        before: VecBackingMemory,
        after: VecBackingMemory,
    ) {
        self.input.remove_slot();
        self.groups.replace(before, after);
        self.check();
    }

    pub(crate) fn release_input(&mut self) {
        self.counters_valid &= self.input.length_slots == 0;
        self.input = VecBackingMemory::default();
        self.check();
    }

    pub(crate) fn group_to_sort(&mut self, group: &Vec<FileEntry>) {
        self.groups.remove(group);
        self.sorting_rows = match self.sorting_rows.checked_add(group.len()) {
            Some(value) => value,
            None => {
                self.counters_valid = false;
                usize::MAX
            }
        };
        self.check();
    }

    pub(crate) fn sort_to_rows(&mut self, rows: &Vec<FileEntry>) {
        self.counters_valid &= self.sorting_rows == rows.len() && self.rows.owners == 0;
        self.sorting_rows = 0;
        self.rows = VecBackingMemory::of(rows);
        self.check();
    }

    pub(crate) fn rows_to_leaf(&mut self, leaf_was_owned: bool, leaf: &Vec<FileEntry>) {
        self.rows.remove_slot();
        self.counters_valid &= leaf_was_owned == (self.leaf.owners == 1);
        self.leaf.refresh(leaf);
        self.check();
    }

    pub(crate) fn release_rows(&mut self) {
        self.counters_valid &= self.rows.length_slots == 0;
        self.rows = VecBackingMemory::default();
        self.check();
    }

    pub(crate) fn leaf_to_retiring_rows(&mut self, rows: &Vec<FileEntry>) {
        self.counters_valid &=
            self.leaf == VecBackingMemory::of(rows) && self.retiring_rows.owners == 0;
        self.leaf = VecBackingMemory::default();
        self.retiring_rows = VecBackingMemory::of(rows);
        self.check();
    }

    pub(crate) fn retire_input(&mut self, entry: &FileEntry) {
        self.input.remove_slot();
        self.paths.subtract(&entry.path);
        self.check();
    }

    pub(crate) fn retire_group_entry(
        &mut self,
        before: VecBackingMemory,
        after: VecBackingMemory,
        entry: &FileEntry,
    ) {
        self.groups.replace(before, after);
        self.paths.subtract(&entry.path);
        self.check();
    }

    pub(crate) fn release_group(&mut self, group: &Vec<FileEntry>) {
        self.counters_valid &= group.is_empty();
        self.groups.remove(group);
        self.check();
    }

    pub(crate) fn retire_sorting_row(&mut self, entry: &FileEntry) {
        self.sorting_rows = self.sorting_rows.checked_sub(1).unwrap_or_else(|| {
            self.counters_valid = false;
            0
        });
        self.paths.subtract(&entry.path);
        self.check();
    }

    pub(crate) fn retire_row(&mut self, entry: &FileEntry) {
        self.rows.remove_slot();
        self.paths.subtract(&entry.path);
        self.check();
    }

    pub(crate) fn retire_leaf_row(&mut self, entry: &FileEntry) {
        self.leaf.remove_slot();
        self.paths.subtract(&entry.path);
        self.check();
    }

    pub(crate) fn release_leaf(&mut self) {
        self.counters_valid &= self.leaf.length_slots == 0;
        self.leaf = VecBackingMemory::default();
        self.check();
    }

    pub(crate) fn retire_encoded_row(&mut self, entry: &FileEntry) {
        self.retiring_rows.remove_slot();
        self.paths.subtract(&entry.path);
        self.check();
    }

    pub(crate) fn release_retiring_rows(&mut self) {
        self.counters_valid &= self.retiring_rows.length_slots == 0;
        self.retiring_rows = VecBackingMemory::default();
        self.check();
    }

    fn check(&mut self) {
        self.counters_valid &= self.input.counters_valid
            && self.groups.counters_valid
            && self.rows.counters_valid
            && self.leaf.counters_valid
            && self.retiring_rows.counters_valid
            && self.paths.counters_valid
            && self.paths.length_bytes <= self.paths.capacity_bytes;
        let lengths = [
            self.input.length_slots,
            self.groups.length_slots,
            self.sorting_rows,
            self.rows.length_slots,
            self.leaf.length_slots,
            self.retiring_rows.length_slots,
        ];
        let mut total = 0usize;
        for length in lengths {
            total = match total.checked_add(length) {
                Some(value) => value,
                None => {
                    self.counters_valid = false;
                    usize::MAX
                }
            };
        }
        self.counters_valid &= total == self.paths.strings;
    }
}

/// Exact retained assembly allocations for a Tree v1 replacement before its
/// provisional root moves into the existing metadata component. BTree nodes,
/// reachability closure, codecs and allocator metadata are excluded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ReplacementV1GraphMemory {
    pub group_keys: StringMemory,
    pub prefix: StringMemory,
    pub leaf_hashes: VecBackingMemory,
    pub hashes: VecBackingMemory,
    pub children: VecBackingMemory,
    pub sorting_children: usize,
    pub retiring_children: VecBackingMemory,
    pub child_labels: StringMemory,
    pub root_children: VecBackingMemory,
    pub root_child_labels: StringMemory,
    pub identities: StringMemory,
    pub counters_valid: bool,
}

impl Default for ReplacementV1GraphMemory {
    fn default() -> Self {
        Self {
            group_keys: StringMemory::default(),
            prefix: StringMemory::default(),
            leaf_hashes: VecBackingMemory::default(),
            hashes: VecBackingMemory::default(),
            children: VecBackingMemory::default(),
            sorting_children: 0,
            retiring_children: VecBackingMemory::default(),
            child_labels: StringMemory::default(),
            root_children: VecBackingMemory::default(),
            root_child_labels: StringMemory::default(),
            identities: StringMemory::default(),
            counters_valid: true,
        }
    }
}

impl ReplacementV1GraphMemory {
    pub(crate) fn new(vault_id: &String, device_id: &String) -> Self {
        let mut result = Self::default();
        result.identities.add(vault_id);
        result.identities.add(device_id);
        result.check();
        result
    }

    pub(crate) fn add_group_key(&mut self, key: &String) {
        self.group_keys.add(key);
        self.check();
    }

    pub(crate) fn group_key_to_prefix(&mut self, key: &String) {
        self.counters_valid &= self.prefix.strings == 0;
        self.group_keys.subtract(key);
        self.prefix.add(key);
        self.check();
    }

    pub(crate) fn retire_group_key(&mut self, key: &String) {
        self.group_keys.subtract(key);
        self.check();
    }

    pub(crate) fn retire_prefix(&mut self, prefix: &String) {
        self.prefix.subtract(prefix);
        self.check();
    }

    pub(crate) fn refresh_leaf_hashes<T>(&mut self, hashes: &Vec<T>) {
        self.leaf_hashes = VecBackingMemory::allocated(hashes);
        self.check();
    }

    pub(crate) fn leaf_hashes_to_hashes(&mut self) {
        self.counters_valid &= self.hashes.owners == 0;
        self.hashes = self.leaf_hashes;
        self.leaf_hashes = VecBackingMemory::default();
        self.check();
    }

    pub(crate) fn retire_hash(&mut self) {
        self.hashes.remove_slot();
        self.check();
    }

    pub(crate) fn release_hashes(&mut self) {
        self.counters_valid &= self.hashes.length_slots == 0;
        self.hashes = VecBackingMemory::default();
        self.check();
    }

    pub(crate) fn release_leaf_hashes(&mut self) {
        self.counters_valid &= self.leaf_hashes.length_slots == 0;
        self.leaf_hashes = VecBackingMemory::default();
        self.check();
    }

    pub(crate) fn push_child<T>(&mut self, label: &String, children: &Vec<T>) {
        self.child_labels.add(label);
        self.children = VecBackingMemory::allocated(children);
        self.check();
    }

    pub(crate) fn children_to_sort(&mut self) {
        self.counters_valid &= self.sorting_children == 0;
        self.sorting_children = self.children.length_slots;
        self.children = VecBackingMemory::default();
        self.check();
    }

    pub(crate) fn retire_sorting_child(&mut self, label: &String) {
        self.sorting_children = self.sorting_children.checked_sub(1).unwrap_or_else(|| {
            self.counters_valid = false;
            0
        });
        self.child_labels.subtract(label);
        self.check();
    }

    pub(crate) fn sort_to_children<T>(&mut self, children: &Vec<T>) {
        self.counters_valid &= self.sorting_children == children.len();
        self.sorting_children = 0;
        self.children = VecBackingMemory::allocated(children);
        self.check();
    }

    pub(crate) fn children_to_retiring(&mut self) {
        self.counters_valid &= self.retiring_children.owners == 0;
        self.retiring_children = self.children;
        self.children = VecBackingMemory::default();
        self.check();
    }

    pub(crate) fn retire_child_from_vec<T>(&mut self, label: &String, children: &Vec<T>) {
        self.child_labels.subtract(label);
        self.children = VecBackingMemory::allocated(children);
        self.check();
    }

    pub(crate) fn release_children(&mut self) {
        self.counters_valid &= self.children.length_slots == 0 && self.child_labels.strings == 0;
        self.children = VecBackingMemory::default();
        self.check();
    }

    pub(crate) fn retire_child_from_iter(&mut self, label: &String) {
        self.retiring_children.remove_slot();
        self.child_labels.subtract(label);
        self.check();
    }

    pub(crate) fn release_retiring_children(&mut self) {
        self.counters_valid &=
            self.retiring_children.length_slots == 0 && self.child_labels.strings == 0;
        self.retiring_children = VecBackingMemory::default();
        self.check();
    }

    pub(crate) fn prefix_to_root_child<T>(&mut self, prefix: &String, children: &Vec<T>) {
        self.prefix.subtract(prefix);
        self.root_child_labels.add(prefix);
        self.root_children = VecBackingMemory::allocated(children);
        self.check();
    }

    pub(crate) fn retire_root_child<T>(&mut self, label: &String, children: &Vec<T>) {
        self.root_child_labels.subtract(label);
        self.root_children = VecBackingMemory::allocated(children);
        self.check();
    }

    pub(crate) fn release_root_children(&mut self) {
        self.counters_valid &=
            self.root_children.length_slots == 0 && self.root_child_labels.strings == 0;
        self.root_children = VecBackingMemory::default();
        self.check();
    }

    pub(crate) fn retire_identity(&mut self, value: &String) {
        self.identities.subtract(value);
        self.check();
    }

    /// Root Vec/Strings and identities have moved into OwnedRootV1, whose
    /// existing metadata sidecar becomes their sole reporter immediately.
    pub(crate) fn root_to_metadata(&mut self) -> StringMemory {
        self.counters_valid &= self.group_keys.strings == 0
            && self.prefix.strings == 0
            && self.leaf_hashes.owners == 0
            && self.hashes.owners == 0
            && self.children.owners == 0
            && self.sorting_children == 0
            && self.retiring_children.owners == 0
            && self.child_labels.strings == 0
            && self.root_children.length_slots == self.root_child_labels.strings
            && self.identities.strings == 2;
        self.root_children = VecBackingMemory::default();
        let root_child_labels = std::mem::take(&mut self.root_child_labels);
        self.identities = StringMemory::default();
        self.check();
        root_child_labels
    }

    /// Physical-ownership emptiness only. Counter validity is intentionally
    /// ignored so diagnostics can never keep cleanup alive or make it fail.
    pub(crate) fn owners_empty(&self) -> bool {
        let strings_empty = |value: StringMemory| {
            value.strings == 0 && value.length_bytes == 0 && value.capacity_bytes == 0
        };
        strings_empty(self.group_keys)
            && strings_empty(self.prefix)
            && self.leaf_hashes.owners_empty()
            && self.hashes.owners_empty()
            && self.children.owners_empty()
            && self.sorting_children == 0
            && self.retiring_children.owners_empty()
            && strings_empty(self.child_labels)
            && self.root_children.owners_empty()
            && strings_empty(self.root_child_labels)
            && strings_empty(self.identities)
    }

    fn check(&mut self) {
        self.counters_valid &= self.group_keys.counters_valid
            && self.prefix.counters_valid
            && self.leaf_hashes.counters_valid
            && self.hashes.counters_valid
            && self.children.counters_valid
            && self.retiring_children.counters_valid
            && self.child_labels.counters_valid
            && self.root_children.counters_valid
            && self.root_child_labels.counters_valid
            && self.identities.counters_valid
            && self.prefix.strings <= 1
            && self.identities.strings <= 2
            && self.group_keys.length_bytes <= self.group_keys.capacity_bytes
            && self.prefix.length_bytes <= self.prefix.capacity_bytes
            && self.child_labels.length_bytes <= self.child_labels.capacity_bytes
            && self.root_child_labels.length_bytes <= self.root_child_labels.capacity_bytes
            && self.identities.length_bytes <= self.identities.capacity_bytes;
        let child_rows = self
            .children
            .length_slots
            .checked_add(self.sorting_children)
            .and_then(|value| value.checked_add(self.retiring_children.length_slots));
        match child_rows {
            Some(value) => self.counters_valid &= value == self.child_labels.strings,
            None => self.counters_valid = false,
        }
        self.counters_valid &= self.root_children.length_slots == self.root_child_labels.strings;
    }
}

/// A physical RangeRef Vec owner. Iterator retirement keeps the same location
/// and backing meter until its explicit release, even after its last row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum V2RangeVector {
    Ranges,
    NextRanges,
    ClosurePending,
    ClosureExpanding,
}

/// Exact retained RangeRef vector backing and nested endpoint Strings. Map
/// descriptors contribute their two String allocations only: descriptor count
/// is logical, never a claim about HashMap buckets, headers, or inline hashes.
/// Root-owned RangeRef values, decoded temporary nodes and allocator overhead
/// are outside this component. Every transition is O(1), nonallocating, and
/// advisory; invalid accounting must never control native owner cleanup.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ReplacementV2RangesMemory {
    pub ranges: VecBackingMemory,
    pub next_ranges: VecBackingMemory,
    pub closure_pending: VecBackingMemory,
    pub closure_expanding: VecBackingMemory,
    pub vector_paths: StringMemory,
    pub descriptor_ranges: usize,
    pub descriptor_paths: StringMemory,
    pub counters_valid: bool,
}

impl Default for ReplacementV2RangesMemory {
    fn default() -> Self {
        Self {
            ranges: VecBackingMemory::default(),
            next_ranges: VecBackingMemory::default(),
            closure_pending: VecBackingMemory::default(),
            closure_expanding: VecBackingMemory::default(),
            vector_paths: StringMemory::default(),
            descriptor_ranges: 0,
            descriptor_paths: StringMemory::default(),
            counters_valid: true,
        }
    }
}

impl ReplacementV2RangesMemory {
    /// Measure the actual destination Strings, including empty values. A
    /// RangeRef clone need not preserve either source String's spare capacity.
    pub(crate) fn endpoints(range: &RangeRef) -> StringMemory {
        StringMemory::of(&range.min_path).combine(StringMemory::of(&range.max_path))
    }

    pub(crate) fn vector(&self, owner: V2RangeVector) -> VecBackingMemory {
        match owner {
            V2RangeVector::Ranges => self.ranges,
            V2RangeVector::NextRanges => self.next_ranges,
            V2RangeVector::ClosurePending => self.closure_pending,
            V2RangeVector::ClosureExpanding => self.closure_expanding,
        }
    }

    fn vector_mut(&mut self, owner: V2RangeVector) -> &mut VecBackingMemory {
        match owner {
            V2RangeVector::Ranges => &mut self.ranges,
            V2RangeVector::NextRanges => &mut self.next_ranges,
            V2RangeVector::ClosurePending => &mut self.closure_pending,
            V2RangeVector::ClosureExpanding => &mut self.closure_expanding,
        }
    }

    /// Install an actual Vec and a meter accumulated while its rows were
    /// constructed. No endpoint traversal runs here. The previous location
    /// must be absent; an exhausted but retained allocation needs release first.
    /// Capacity-zero fields are absent; an empty allocated Vec remains owned.
    pub(crate) fn adopt_vector(
        &mut self,
        owner: V2RangeVector,
        ranges: &Vec<RangeRef>,
        paths: StringMemory,
    ) {
        if self.vector(owner).owners != 0 {
            self.counters_valid = false;
            return;
        }
        self.counters_valid &= ranges.len().checked_mul(2) == Some(paths.strings);
        *self.vector_mut(owner) = VecBackingMemory::allocated(ranges);
        self.vector_paths.add_memory(paths);
        self.check();
    }

    /// After one newly owned row was pushed, refresh actual Vec capacity and
    /// add that row's actual endpoints. Clones are measured at the destination.
    pub(crate) fn push_vector(
        &mut self,
        owner: V2RangeVector,
        ranges: &Vec<RangeRef>,
        added: &RangeRef,
    ) {
        let previous = self.vector(owner);
        self.counters_valid &= previous.length_slots.checked_add(1) == Some(ranges.len());
        let mut next = VecBackingMemory::allocated(ranges);
        next.counters_valid &= previous.counters_valid;
        *self.vector_mut(owner) = next;
        self.vector_paths.add_memory(Self::endpoints(added));
        self.check();
    }

    /// After pop/IntoIter::next yields a row and before its endpoints leave
    /// this component. This never releases or reconstructs iterator backing.
    pub(crate) fn remove_vector(&mut self, owner: V2RangeVector, removed: &RangeRef) {
        self.vector_mut(owner).remove_slot();
        self.vector_paths.subtract_memory(Self::endpoints(removed));
        self.check();
    }

    /// One endpoint allocation moved out of an existing row while the
    /// moved-from empty String value remains in the same vector slot. This
    /// updates only the advisory sidecar; the caller performs the native move
    /// first and separately accounts the destination owner.
    pub(crate) fn replace_vector_endpoint(
        &mut self,
        owner: V2RangeVector,
        previous: StringMemory,
        current: &String,
    ) {
        self.counters_valid &= self.vector(owner).owners == 1
            && self.vector(owner).length_slots > 0
            && previous.strings == 1;
        self.vector_paths.subtract_memory(previous);
        self.vector_paths.add(current);
        self.check();
    }

    /// Whole-owner move, including spare capacity, without cloning endpoints.
    /// Call only after the native ownership move; overlapping locations fail
    /// closed without overwriting either existing meter.
    pub(crate) fn move_vector(&mut self, from: V2RangeVector, to: V2RangeVector) {
        if from == to || self.vector(to).owners != 0 {
            self.counters_valid = false;
            return;
        }
        // Moving a capacity-zero Vec transfers no allocation or endpoints.
        if self.vector(from).owners == 0 {
            self.check();
            return;
        }
        if self.vector(from).owners != 1 {
            self.counters_valid = false;
            return;
        }
        let moved = std::mem::take(self.vector_mut(from));
        *self.vector_mut(to) = moved;
        self.check();
    }

    /// The exhausted Vec/IntoIter has actually been dropped. Rows must have
    /// been removed individually before releasing this retained backing.
    pub(crate) fn release_vector(&mut self, owner: V2RangeVector) {
        let previous = self.vector(owner);
        self.counters_valid &=
            previous.owners_empty() || (previous.owners == 1 && previous.length_slots == 0);
        self.counters_valid &= previous.counters_valid;
        *self.vector_mut(owner) = VecBackingMemory::default();
        self.check();
    }

    /// A distinct actual descriptor now owns these endpoints. A move from a
    /// vector uses remove_vector first; a clone uses its new String capacities.
    pub(crate) fn add_descriptor(&mut self, added: &RangeRef) {
        self.descriptor_ranges = self.descriptor_ranges.checked_add(1).unwrap_or_else(|| {
            self.counters_valid = false;
            usize::MAX
        });
        self.descriptor_paths.add_memory(Self::endpoints(added));
        self.check();
    }

    /// Before a map/IntoIter descriptor is dropped or leaves this component.
    /// Map backing is intentionally neither released nor estimated here.
    pub(crate) fn remove_descriptor(&mut self, removed: &RangeRef) {
        self.descriptor_ranges = self.descriptor_ranges.checked_sub(1).unwrap_or_else(|| {
            self.counters_valid = false;
            0
        });
        self.descriptor_paths
            .subtract_memory(Self::endpoints(removed));
        self.check();
    }

    /// Only for a real replacement of an existing map value. Counts remain
    /// unchanged; the previous and replacement endpoint allocations may differ.
    #[cfg(test)]
    pub(crate) fn replace_descriptor(&mut self, previous: &RangeRef, replacement: &RangeRef) {
        self.counters_valid &= self.descriptor_ranges > 0;
        self.descriptor_paths
            .subtract_memory(Self::endpoints(previous));
        self.descriptor_paths
            .add_memory(Self::endpoints(replacement));
        self.check();
    }

    /// Numerical emptiness, independent of sticky validity, for diagnostics.
    /// This is not proof that a caller actually released its native allocations.
    #[cfg(test)]
    pub(crate) fn owners_empty(&self) -> bool {
        let strings_empty = |value: StringMemory| {
            value.strings == 0 && value.length_bytes == 0 && value.capacity_bytes == 0
        };
        self.ranges.owners_empty()
            && self.next_ranges.owners_empty()
            && self.closure_pending.owners_empty()
            && self.closure_expanding.owners_empty()
            && strings_empty(self.vector_paths)
            && self.descriptor_ranges == 0
            && strings_empty(self.descriptor_paths)
    }

    fn check(&mut self) {
        let vectors = [
            self.ranges,
            self.next_ranges,
            self.closure_pending,
            self.closure_expanding,
        ];
        let mut valid = self.counters_valid;
        let mut ranges = Some(0usize);
        for vector in vectors {
            valid &= vector.counters_valid && vector.length_slots <= vector.capacity_slots;
            let component_valid = match vector.owners {
                0 => vector.owners_empty(),
                1 => {
                    let bytes =
                        backing_bytes(vector.capacity_slots, vector.slot_size_bytes, &mut valid);
                    vector.capacity_slots > 0
                        && vector.slot_size_bytes == std::mem::size_of::<RangeRef>()
                        && vector.backing_capacity_bytes == bytes
                }
                _ => false,
            };
            valid &= component_valid;
            ranges = ranges.and_then(|count| count.checked_add(vector.length_slots));
        }
        valid &= ranges.and_then(|count| count.checked_mul(2)) == Some(self.vector_paths.strings);
        valid &= self.descriptor_ranges.checked_mul(2) == Some(self.descriptor_paths.strings);
        for paths in [self.vector_paths, self.descriptor_paths] {
            valid &= paths.counters_valid && paths.length_bytes <= paths.capacity_bytes;
            valid &= paths.strings != 0 || (paths.length_bytes == 0 && paths.capacity_bytes == 0);
        }
        self.counters_valid = valid;
    }
}

#[cfg(test)]
mod v2_ranges_memory_tests {
    use super::*;

    fn path(text: &str, spare: usize) -> String {
        let mut value = String::with_capacity(text.len() + spare);
        value.push_str(text);
        value
    }

    fn range(index: usize) -> RangeRef {
        RangeRef {
            min_path: path(&format!("é/{index:03}"), 17 + index),
            max_path: path(&format!("𐀀/{index:03}"), 29 + index),
            hash: [index as u8; 32],
            file_count: 1,
            serialized_bytes: 99,
            height: 0,
        }
    }

    fn paths(ranges: &[RangeRef]) -> StringMemory {
        let mut memory = StringMemory::default();
        for range in ranges {
            memory.add(&range.min_path);
            memory.add(&range.max_path);
        }
        memory
    }

    #[test]
    fn ranges_actual_capacities_unicode_empty_endpoints_and_schema() {
        let mut rows = Vec::with_capacity(13);
        rows.push(range(0));
        let mut empty = range(1);
        empty.min_path = path("", 7);
        empty.max_path = String::new();
        rows.push(empty);
        let endpoints = paths(&rows);
        let mut memory = ReplacementV2RangesMemory::default();
        memory.adopt_vector(V2RangeVector::Ranges, &rows, endpoints);
        assert_eq!(memory.ranges, VecBackingMemory::of(&rows));
        assert_eq!(memory.vector_paths, endpoints);
        assert_eq!(memory.vector_paths.strings, 4);
        assert_eq!(
            memory.vector_paths.length_bytes,
            (rows[0].min_path.len() + rows[0].max_path.len()) as u64
        );
        assert!(memory.vector_paths.capacity_bytes > memory.vector_paths.length_bytes);
        assert!(memory.counters_valid);

        let json = serde_json::to_value(memory).unwrap();
        let object = json.as_object().unwrap();
        let names: Vec<_> = object.keys().map(String::as_str).collect();
        assert_eq!(
            names,
            [
                "closure_expanding",
                "closure_pending",
                "counters_valid",
                "descriptor_paths",
                "descriptor_ranges",
                "next_ranges",
                "ranges",
                "vector_paths"
            ]
        );
        assert!(!json.to_string().contains("é/000"));
    }

    #[test]
    fn ranges_push_refreshes_growth_and_moves_original_iterator_capacity() {
        let mut rows = Vec::with_capacity(1);
        let mut memory = ReplacementV2RangesMemory::default();
        memory.adopt_vector(V2RangeVector::NextRanges, &rows, StringMemory::default());
        let initial_capacity = rows.capacity();
        for index in 0..9 {
            rows.push(range(index));
            memory.push_vector(V2RangeVector::NextRanges, &rows, rows.last().unwrap());
            assert_eq!(memory.next_ranges, VecBackingMemory::of(&rows));
            assert_eq!(memory.vector_paths, paths(&rows));
            assert!(memory.counters_valid);
        }
        assert!(rows.capacity() > initial_capacity);
        let backing = memory.next_ranges;
        let expected_paths = memory.vector_paths;
        let mut retired = rows.into_iter();
        memory.move_vector(V2RangeVector::NextRanges, V2RangeVector::Ranges);
        assert_eq!(memory.next_ranges, VecBackingMemory::default());
        assert_eq!(memory.ranges, backing);
        assert_eq!(memory.vector_paths, expected_paths);

        while let Some(value) = retired.next() {
            memory.remove_vector(V2RangeVector::Ranges, &value);
            drop(value);
            assert_eq!(memory.ranges.length_slots, retired.len());
            assert_eq!(memory.ranges.capacity_slots, backing.capacity_slots);
            assert_eq!(
                memory.ranges.backing_capacity_bytes,
                backing.backing_capacity_bytes
            );
            assert_eq!(memory.vector_paths, paths(retired.as_slice()));
            assert!(memory.counters_valid);
        }
        assert_eq!(memory.ranges.owners, 1);
        assert!(!memory.owners_empty());
        drop(retired);
        memory.release_vector(V2RangeVector::Ranges);
        assert_eq!(memory, ReplacementV2RangesMemory::default());
    }

    #[test]
    fn ranges_clone_and_descriptor_transfer_count_distinct_actual_strings() {
        let original = range(0);
        let source_paths = ReplacementV2RangesMemory::endpoints(&original);
        let cloned = original.clone();
        let clone_paths = ReplacementV2RangesMemory::endpoints(&cloned);
        assert_ne!(original.min_path.as_ptr(), cloned.min_path.as_ptr());
        assert_ne!(original.max_path.as_ptr(), cloned.max_path.as_ptr());
        assert_ne!(
            source_paths.capacity_bytes, clone_paths.capacity_bytes,
            "fixture must distinguish copied source capacity from actual clone capacity"
        );

        let mut memory = ReplacementV2RangesMemory::default();
        let mut ranges = vec![original];
        memory.adopt_vector(V2RangeVector::Ranges, &ranges, source_paths);
        let mut pending = vec![cloned];
        memory.adopt_vector(V2RangeVector::ClosurePending, &pending, clone_paths);
        assert_eq!(memory.vector_paths, source_paths.combine(clone_paths));

        let descriptor = pending.pop().unwrap();
        memory.remove_vector(V2RangeVector::ClosurePending, &descriptor);
        memory.add_descriptor(&descriptor);
        assert_eq!(memory.vector_paths, source_paths);
        assert_eq!(memory.descriptor_paths, clone_paths);
        assert_eq!(memory.descriptor_ranges, 1);

        let replacement = range(9);
        let replacement_paths = ReplacementV2RangesMemory::endpoints(&replacement);
        memory.replace_descriptor(&descriptor, &replacement);
        drop(descriptor);
        assert_eq!(memory.descriptor_paths, replacement_paths);
        assert_eq!(memory.descriptor_ranges, 1);
        memory.remove_descriptor(&replacement);
        drop(replacement);
        drop(pending);
        memory.release_vector(V2RangeVector::ClosurePending);
        let value = ranges.pop().unwrap();
        memory.remove_vector(V2RangeVector::Ranges, &value);
        drop(value);
        drop(ranges);
        memory.release_vector(V2RangeVector::Ranges);
        assert_eq!(memory, ReplacementV2RangesMemory::default());
    }

    #[test]
    fn ranges_all_four_owners_and_empty_backing_remain_distinct() {
        let owners = [
            V2RangeVector::Ranges,
            V2RangeVector::NextRanges,
            V2RangeVector::ClosurePending,
            V2RangeVector::ClosureExpanding,
        ];
        let mut memory = ReplacementV2RangesMemory::default();
        let vectors: Vec<Vec<RangeRef>> = owners
            .iter()
            .enumerate()
            .map(|(index, _)| Vec::with_capacity(index * 3))
            .collect();
        for (owner, vector) in owners.into_iter().zip(&vectors) {
            memory.adopt_vector(owner, vector, StringMemory::default());
            assert_eq!(memory.vector(owner), VecBackingMemory::allocated(vector));
            assert!(memory.counters_valid);
        }
        assert!(!memory.owners_empty());
        for (owner, vector) in owners.into_iter().zip(vectors) {
            drop(vector);
            memory.release_vector(owner);
            assert_eq!(memory.vector(owner), VecBackingMemory::default());
        }
        assert_eq!(memory, ReplacementV2RangesMemory::default());
    }

    #[test]
    fn ranges_unallocated_fields_move_and_release_without_inventing_owners() {
        let mut memory = ReplacementV2RangesMemory::default();
        let rows: Vec<RangeRef> = Vec::new();
        memory.adopt_vector(V2RangeVector::Ranges, &rows, StringMemory::default());
        assert_eq!(memory, ReplacementV2RangesMemory::default());
        memory.move_vector(V2RangeVector::Ranges, V2RangeVector::NextRanges);
        assert_eq!(memory, ReplacementV2RangesMemory::default());
        drop(rows);
        memory.release_vector(V2RangeVector::NextRanges);
        assert_eq!(memory, ReplacementV2RangesMemory::default());
    }

    #[test]
    fn ranges_overlapping_and_malformed_transfers_fail_without_forgetting_owners() {
        let first = vec![range(0)];
        let second = vec![range(1)];
        let mut memory = ReplacementV2RangesMemory::default();
        memory.adopt_vector(V2RangeVector::Ranges, &first, paths(&first));
        memory.adopt_vector(V2RangeVector::NextRanges, &second, paths(&second));
        let before = memory;
        memory.move_vector(V2RangeVector::Ranges, V2RangeVector::NextRanges);
        assert!(!memory.counters_valid);
        assert_eq!(memory.ranges, before.ranges);
        assert_eq!(memory.next_ranges, before.next_ranges);
        assert_eq!(memory.vector_paths, before.vector_paths);
        memory.adopt_vector(V2RangeVector::Ranges, &second, paths(&second));
        assert_eq!(memory.vector_paths, before.vector_paths);

        let mut bad_meter = ReplacementV2RangesMemory::default();
        bad_meter.adopt_vector(
            V2RangeVector::ClosureExpanding,
            &first,
            StringMemory::default(),
        );
        assert!(!bad_meter.counters_valid);
        assert_eq!(bad_meter.closure_expanding, VecBackingMemory::of(&first));

        let mut skipped_push = ReplacementV2RangesMemory::default();
        let two = vec![range(2), range(3)];
        skipped_push.push_vector(V2RangeVector::ClosurePending, &two, &two[1]);
        assert!(!skipped_push.counters_valid);
    }

    #[test]
    fn ranges_overflow_underflow_and_nested_invalidity_are_sticky() {
        let value = range(0);
        let mut underflow = ReplacementV2RangesMemory::default();
        underflow.remove_descriptor(&value);
        assert!(!underflow.counters_valid);
        assert!(!underflow.descriptor_paths.counters_valid);
        underflow.add_descriptor(&value);
        underflow.remove_descriptor(&value);
        assert!(!underflow.counters_valid);
        assert!(underflow.owners_empty());

        let mut overflow = ReplacementV2RangesMemory {
            descriptor_ranges: usize::MAX,
            ..ReplacementV2RangesMemory::default()
        };
        overflow.add_descriptor(&value);
        assert_eq!(overflow.descriptor_ranges, usize::MAX);
        assert!(!overflow.counters_valid);

        let mut multiplication = ReplacementV2RangesMemory {
            descriptor_ranges: usize::MAX / 2 + 1,
            ..ReplacementV2RangesMemory::default()
        };
        multiplication.check();
        assert!(!multiplication.counters_valid);

        let mut sum_overflow = ReplacementV2RangesMemory::default();
        sum_overflow.ranges = VecBackingMemory {
            owners: 1,
            length_slots: usize::MAX,
            capacity_slots: usize::MAX,
            slot_size_bytes: std::mem::size_of::<RangeRef>(),
            backing_capacity_bytes: u64::MAX,
            counters_valid: true,
        };
        sum_overflow.next_ranges = sum_overflow.ranges;
        sum_overflow.check();
        assert!(!sum_overflow.counters_valid);

        let mut rows = vec![range(1)];
        let mut invalid = ReplacementV2RangesMemory::default();
        invalid.adopt_vector(V2RangeVector::Ranges, &rows, paths(&rows));
        invalid.ranges.counters_valid = false;
        invalid.vector_paths.counters_valid = false;
        let row = rows.pop().unwrap();
        invalid.remove_vector(V2RangeVector::Ranges, &row);
        drop(row);
        drop(rows);
        invalid.release_vector(V2RangeVector::Ranges);
        assert!(!invalid.counters_valid);
        assert!(!invalid.vector_paths.counters_valid);
        assert!(invalid.owners_empty());
    }
}

/// Exact top-level Vec backing owned by one active indirect stable sorter.
/// Nested allocations inside `T` are deliberately outside this component.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct IndirectSortMemory {
    pub sorters: usize,
    pub values: VecBackingMemory,
    pub source_indices: VecBackingMemory,
    pub target_indices: VecBackingMemory,
    pub counters_valid: bool,
}

impl Default for IndirectSortMemory {
    fn default() -> Self {
        Self {
            sorters: 0,
            values: VecBackingMemory::default(),
            source_indices: VecBackingMemory::default(),
            target_indices: VecBackingMemory::default(),
            counters_valid: true,
        }
    }
}

impl IndirectSortMemory {
    pub(crate) fn new<T>(values: &Vec<T>, source: &Vec<usize>, target: &Vec<usize>) -> Self {
        let values = VecBackingMemory::of(values);
        let source_indices = VecBackingMemory::of(source);
        let target_indices = VecBackingMemory::of(target);
        Self {
            sorters: 1,
            values,
            source_indices,
            target_indices,
            counters_valid: values.counters_valid
                && source_indices.counters_valid
                && target_indices.counters_valid,
        }
    }
}

/// Replacement builders have at most one entry sorter and one child-label
/// sorter, and never run them concurrently. They remain separate because their
/// native slot sizes differ and must not be inferred by the host.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ReplacementSortMemory {
    pub entries: IndirectSortMemory,
    pub children: IndirectSortMemory,
    pub counters_valid: bool,
}

impl Default for ReplacementSortMemory {
    fn default() -> Self {
        Self {
            entries: IndirectSortMemory::default(),
            children: IndirectSortMemory::default(),
            counters_valid: true,
        }
    }
}

impl ReplacementSortMemory {
    pub(crate) fn entries(entries: IndirectSortMemory) -> Self {
        Self {
            counters_valid: entries.counters_valid,
            entries,
            ..Self::default()
        }
    }

    pub(crate) fn children(children: IndirectSortMemory) -> Self {
        Self {
            counters_valid: children.counters_valid,
            children,
            ..Self::default()
        }
    }
}

/// Exact backing for the six POD descriptor Vecs used by the V2 replacement
/// planner/emitter. File entries, RangeRef strings, closure sets/maps and codec
/// scratch are intentionally outside this additive component.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ReplacementV2PlanningMemory {
    pub spans: VecBackingMemory,
    pub leaf_spans: VecBackingMemory,
    pub planned_ranges: VecBackingMemory,
    pub next_planned_ranges: VecBackingMemory,
    pub planned_spans: VecBackingMemory,
    pub planned_internal_spans: VecBackingMemory,
    pub counters_valid: bool,
}

impl Default for ReplacementV2PlanningMemory {
    fn default() -> Self {
        Self {
            spans: VecBackingMemory::default(),
            leaf_spans: VecBackingMemory::default(),
            planned_ranges: VecBackingMemory::default(),
            next_planned_ranges: VecBackingMemory::default(),
            planned_spans: VecBackingMemory::default(),
            planned_internal_spans: VecBackingMemory::default(),
            counters_valid: true,
        }
    }
}

impl ReplacementV2PlanningMemory {
    pub(crate) fn new<A, B, C, D, E, F>(
        spans: &Vec<A>,
        leaf_spans: &Vec<B>,
        planned_ranges: &Vec<C>,
        next_planned_ranges: &Vec<D>,
        planned_spans: &Vec<E>,
        planned_internal_spans: &Vec<F>,
    ) -> Self {
        let mut result = Self {
            spans: VecBackingMemory::of(spans),
            leaf_spans: VecBackingMemory::of(leaf_spans),
            planned_ranges: VecBackingMemory::of(planned_ranges),
            next_planned_ranges: VecBackingMemory::of(next_planned_ranges),
            planned_spans: VecBackingMemory::of(planned_spans),
            planned_internal_spans: VecBackingMemory::of(planned_internal_spans),
            counters_valid: true,
        };
        result.check();
        result
    }

    pub(crate) fn retire_spans(&mut self) {
        self.spans.remove_slot();
        self.check();
    }

    pub(crate) fn release_spans(&mut self) {
        self.counters_valid &= self.spans.length_slots == 0;
        self.spans = VecBackingMemory::default();
        self.check();
    }

    pub(crate) fn retire_leaf_spans(&mut self) {
        self.leaf_spans.remove_slot();
        self.check();
    }

    pub(crate) fn release_leaf_spans(&mut self) {
        self.counters_valid &= self.leaf_spans.length_slots == 0;
        self.leaf_spans = VecBackingMemory::default();
        self.check();
    }

    pub(crate) fn retire_planned_range(&mut self) {
        self.planned_ranges.remove_slot();
        self.check();
    }

    pub(crate) fn release_planned_ranges(&mut self) {
        self.counters_valid &= self.planned_ranges.length_slots == 0;
        self.planned_ranges = VecBackingMemory::default();
        self.check();
    }

    pub(crate) fn retire_next_planned_range(&mut self) {
        self.next_planned_ranges.remove_slot();
        self.check();
    }

    pub(crate) fn release_next_planned_ranges(&mut self) {
        self.counters_valid &= self.next_planned_ranges.length_slots == 0;
        self.next_planned_ranges = VecBackingMemory::default();
        self.check();
    }

    pub(crate) fn retire_planned_span(&mut self) {
        self.planned_spans.remove_slot();
        self.check();
    }

    pub(crate) fn release_planned_spans(&mut self) {
        self.counters_valid &= self.planned_spans.length_slots == 0;
        self.planned_spans = VecBackingMemory::default();
        self.check();
    }

    pub(crate) fn retire_planned_internal_span(&mut self) {
        self.planned_internal_spans.remove_slot();
        self.check();
    }

    pub(crate) fn release_planned_internal_spans(&mut self) {
        self.counters_valid &= self.planned_internal_spans.length_slots == 0;
        self.planned_internal_spans = VecBackingMemory::default();
        self.check();
    }

    fn check(&mut self) {
        self.counters_valid &= [
            self.spans,
            self.leaf_spans,
            self.planned_ranges,
            self.next_planned_ranges,
            self.planned_spans,
            self.planned_internal_spans,
        ]
        .iter()
        .all(|component| {
            component.counters_valid && component.length_slots <= component.capacity_slots
        });
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ReplacementInputMemory {
    pub entries: VecBackingMemory,
    pub paths: StringMemory,
    pub counters_valid: bool,
}

impl Default for ReplacementInputMemory {
    fn default() -> Self {
        Self {
            entries: VecBackingMemory::default(),
            paths: StringMemory::default(),
            counters_valid: true,
        }
    }
}

impl ReplacementInputMemory {
    pub(crate) fn new(entries: &Vec<FileEntry>) -> Self {
        let mut paths = StringMemory::default();
        for entry in entries {
            paths.add(&entry.path);
        }
        let entries = VecBackingMemory::of(entries);
        let mut result = Self {
            entries,
            paths,
            counters_valid: true,
        };
        result.check();
        result
    }

    pub(crate) fn paths(entries: &[FileEntry]) -> StringMemory {
        let mut paths = StringMemory::default();
        for entry in entries {
            paths.add(&entry.path);
        }
        paths
    }

    /// Call only after these exact entries were moved into `owner`.
    pub(crate) fn accepted(&mut self, owner: &Vec<FileEntry>, paths: StringMemory) {
        self.entries.refresh(owner);
        self.paths.add_memory(paths);
        self.check();
    }

    /// Refresh the top-level owner after these exact entries were moved
    /// without cloning or dropping any nested path String.
    pub(crate) fn transferred(&mut self, owner: &Vec<FileEntry>) {
        self.entries.refresh(owner);
        self.check();
    }

    /// Update before the yielded FileEntry and its path String are dropped.
    pub(crate) fn retire_entry(&mut self, entry: &FileEntry) {
        self.entries.remove_slot();
        self.paths.subtract(&entry.path);
        self.check();
    }

    /// The exhausted entry owner has just been dropped, releasing its Vec backing.
    pub(crate) fn release_backing(&mut self) {
        self.counters_valid &= self.entries.length_slots == 0 && self.paths.strings == 0;
        self.entries = VecBackingMemory::default();
        self.check();
    }

    fn check(&mut self) {
        self.counters_valid &= self.entries.counters_valid
            && self.paths.counters_valid
            && self.entries.length_slots == self.paths.strings
            && self.entries.length_slots <= self.entries.capacity_slots
            && self.paths.length_bytes <= self.paths.capacity_bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::hash_bytes;

    fn entry(path: &str, value: u64) -> FileEntry {
        FileEntry::new(
            path.to_owned(),
            hash_bytes(&value.to_le_bytes()),
            value,
            value + 1,
        )
    }

    #[test]
    fn input_components_follow_actual_capacity_unicode_and_retirement() {
        let mut rows = Vec::with_capacity(17);
        rows.push(entry("é/𐀀.md", 1));
        rows.push(entry("plain.md", 2));
        let mut memory = ReplacementInputMemory::new(&rows);
        assert_eq!(memory.entries.owners, 1);
        assert_eq!(memory.entries.length_slots, 2);
        assert_eq!(memory.entries.capacity_slots, 17);
        assert_eq!(
            memory.entries.slot_size_bytes,
            std::mem::size_of::<FileEntry>()
        );
        assert_eq!(
            memory.entries.backing_capacity_bytes,
            (17 * std::mem::size_of::<FileEntry>()) as u64
        );
        assert_eq!(memory.paths.strings, 2);
        assert_eq!(
            memory.paths.length_bytes,
            rows.iter().map(|row| row.path.len() as u64).sum::<u64>()
        );
        assert_eq!(
            memory.paths.capacity_bytes,
            rows.iter()
                .map(|row| row.path.capacity() as u64)
                .sum::<u64>()
        );

        let mut rows = rows.into_iter();
        for remaining in [1, 0] {
            let row = rows.next().unwrap();
            memory.retire_entry(&row);
            drop(row);
            assert_eq!(memory.entries.length_slots, remaining);
            assert_eq!(memory.paths.strings, remaining);
            assert_eq!(memory.entries.capacity_slots, 17);
        }
        drop(rows);
        memory.release_backing();
        assert_eq!(memory, ReplacementInputMemory::default());
    }

    #[test]
    fn accepted_pages_use_moved_path_capacities_without_recounting_old_rows() {
        let mut owner = Vec::with_capacity(4);
        let mut memory = ReplacementInputMemory::new(&owner);
        let first = vec![entry("one.md", 1), entry("é/two.md", 2)];
        let first_paths = ReplacementInputMemory::paths(&first);
        owner.extend(first);
        memory.accepted(&owner, first_paths);
        let first_snapshot = memory;

        let second = vec![entry("𐀀/three.md", 3)];
        let second_paths = ReplacementInputMemory::paths(&second);
        owner.extend(second);
        memory.accepted(&owner, second_paths);
        assert_eq!(memory.entries.length_slots, 3);
        assert_eq!(memory.paths.strings, 3);
        assert_eq!(
            memory.paths.length_bytes,
            first_snapshot.paths.length_bytes + owner[2].path.len() as u64
        );
        assert!(memory.counters_valid);
    }

    #[test]
    fn aggregate_vec_arithmetic_fails_closed_and_stays_invalid() {
        let rows = vec![entry("one.md", 1)];

        let mut underflow = VecBackingAggregateMemory::default();
        underflow.remove(&rows);
        assert!(!underflow.counters_valid);
        assert_eq!(underflow.owners, 0);
        underflow.add(&rows);
        underflow.remove(&rows);
        assert!(
            !underflow.counters_valid,
            "later valid transitions must not heal an invalid aggregate"
        );

        let mut overflow = VecBackingAggregateMemory {
            owners: usize::MAX,
            length_slots: usize::MAX,
            capacity_slots: usize::MAX,
            slot_size_bytes: std::mem::size_of::<FileEntry>(),
            backing_capacity_bytes: u64::MAX,
            counters_valid: true,
        };
        overflow.add(&rows);
        assert!(!overflow.counters_valid);
        assert_eq!(overflow.owners, usize::MAX);
        assert_eq!(overflow.length_slots, usize::MAX);
        assert_eq!(overflow.capacity_slots, usize::MAX);
        assert_eq!(overflow.backing_capacity_bytes, u64::MAX);
        overflow.remove(&rows);
        assert!(
            !overflow.counters_valid,
            "removing a real owner must not heal overflowed counters"
        );
    }
}
