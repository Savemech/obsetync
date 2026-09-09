//! One logical v1 update, with private immutable-chunk staging. This is a CPU
//! continuation, not a durable transaction or a wall-clock/heap guarantee.
//! Input root ownership and exclusion of concurrent tree mutations belong to
//! the caller. Allocation, one node codec/hash, and final promotion/drop remain
//! synchronous primitives. No resident bytes change before `finish`.

use crate::chunk::{ChunkError, FileEntry, InternalNode, LeafChunk, RootNode};
use crate::hash::{hash_bytes, hash_to_hex, FileHash, IncrementalHasher};
use crate::store::MemoryChunkStore;
use crate::tree::TARGET_CHUNK_ENTRIES;
use crate::tree_work_memory::IndirectSortMemory;
use std::collections::{btree_map, BTreeMap, HashMap};
use std::iter::Peekable;
use std::vec::IntoIter;

const MAX_VISITED_NODES: usize = 1_000_000;
const MAX_LOADED_ENTRIES: usize = 10_000_000;
type Children = Vec<(String, FileHash)>;
type Operations = (Vec<FileEntry>, Vec<String>);

fn invalid(message: &str) -> ChunkError {
    ChunkError::Deserialize(message.into())
}
fn prefix(path: &str) -> String {
    prefix_slice(path).to_owned()
}
fn prefix_slice(path: &str) -> &str {
    path.find('/').map_or("", |index| &path[..=index])
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SortPhase {
    Indexing,
    Merging,
    Inverting,
    Permuting,
    Ready,
    Retiring,
}

/// The two Copy-only index buffers, prepared without taking any input rows.
/// Requests cover exactly `len * size_of::<usize>()` per buffer, not allocator
/// metadata or the original values/nested allocations. A failed second reserve
/// drops the first empty buffer and leaves the caller's input owner untouched.
pub(crate) struct SortWorkspace {
    source: Vec<usize>,
    target: Vec<usize>,
}

impl SortWorkspace {
    pub(crate) fn try_new(len: usize) -> Result<Self, ChunkError> {
        Self::try_new_with_reserve(len, |values, count| values.try_reserve_exact(count))
    }

    // A local reserve port permits deterministic first/second allocation
    // failure tests without changing the process-global allocator.
    pub(crate) fn try_new_with_reserve(
        len: usize,
        mut reserve: impl FnMut(&mut Vec<usize>, usize) -> Result<(), std::collections::TryReserveError>,
    ) -> Result<Self, ChunkError> {
        let mut source = Vec::new();
        let mut target = Vec::new();
        reserve(&mut source, len)
            .map_err(|_| invalid("replacement sort source-index allocation failed"))?;
        reserve(&mut target, len)
            .map_err(|_| invalid("replacement sort target-index allocation failed"))?;
        let workspace = Self { source, target };
        if !workspace.accepts(len) {
            return Err(invalid("replacement sort workspace is invalid"));
        }
        Ok(workspace)
    }

    pub(crate) fn accepts(&self, len: usize) -> bool {
        self.source.is_empty()
            && self.target.is_empty()
            && self.source.capacity() >= len
            && self.target.capacity() >= len
    }
}

/// Stable indirect bottom-up merge sort. Owns the original record array once
/// and two usize index arrays, not additional full-record merge/output arrays.
/// Each tick initializes indices, emits one merge index, inverts one index, or
/// swaps/advances one permutation position. The constructor reserves both
/// index arrays for all inputs without filling them. Allocation and comparisons are still
/// native primitives, not wall-clock or admission guarantees.
pub(crate) struct Sort<T> {
    values: Vec<T>,
    source: Vec<usize>,
    target: Vec<usize>,
    input_sorted: bool,
    width: usize,
    start: usize,
    left: usize,
    right: usize,
    middle: usize,
    end: usize,
    output: usize,
    position: usize,
    phase: SortPhase,
}
impl<T: Ord> Sort<T> {
    pub(crate) fn new(values: Vec<T>) -> Self {
        let len = values.len();
        Self::with_workspace(
            values,
            SortWorkspace {
                source: Vec::with_capacity(len),
                target: Vec::with_capacity(len),
            },
        )
    }

    /// Move prevalidated index buffers into the sorter without allocating or
    /// copying any rows. Callers must validate capacity before releasing their
    /// previous input owner; this assertion is an internal invariant only.
    pub(crate) fn with_workspace(values: Vec<T>, workspace: SortWorkspace) -> Self {
        assert!(
            workspace.accepts(values.len()),
            "invalid stable-sort workspace"
        );
        Self {
            values,
            source: workspace.source,
            target: workspace.target,
            input_sorted: true,
            width: 1,
            start: 0,
            left: 0,
            right: 0,
            middle: 0,
            end: 0,
            output: 0,
            position: 0,
            phase: SortPhase::Indexing,
        }
    }
    /// True means one index/permutation unit; false means complete. At most
    /// one T comparison or one T swap occurs in a unit. Equal keys retain their
    /// original input order. No T is cloned, dropped, or reallocated by sorting.
    pub(crate) fn tick(&mut self) -> bool {
        assert!(
            self.phase != SortPhase::Retiring,
            "retiring stable sort cannot resume"
        );
        let len = self.values.len();
        loop {
            match self.phase {
                SortPhase::Indexing => {
                    if self.source.len() < len {
                        let index = self.source.len();
                        if index > 0 && self.values[index - 1] > self.values[index] {
                            self.input_sorted = false;
                        }
                        self.source.push(index);
                        self.target.push(0);
                        return true;
                    }
                    // Canonical tree leaves are already ordered. Retain the
                    // bounded adjacent validation above, but do not spend an
                    // additional O(N log N) merge/permutation pass for every
                    // small mutation of the same large prefix.
                    if self.input_sorted {
                        self.phase = SortPhase::Ready;
                        continue;
                    }
                    self.phase = SortPhase::Merging;
                }
                SortPhase::Merging => {
                    if self.width >= len {
                        self.phase = SortPhase::Inverting;
                        continue;
                    }
                    if self.output == self.end {
                        if self.start == len {
                            std::mem::swap(&mut self.source, &mut self.target);
                            self.width = self.width.saturating_mul(2);
                            self.start = 0;
                            self.output = 0;
                            self.end = 0;
                            continue;
                        }
                        self.left = self.start;
                        self.middle = self.start.saturating_add(self.width).min(len);
                        self.right = self.middle;
                        self.end = self
                            .start
                            .saturating_add(self.width.saturating_mul(2))
                            .min(len);
                        self.output = self.start;
                        self.start = self.end;
                    }
                    let from = if self.right == self.end
                        || (self.left < self.middle
                            && self.values[self.source[self.left]]
                                <= self.values[self.source[self.right]])
                    {
                        let value = self.left;
                        self.left += 1;
                        value
                    } else {
                        let value = self.right;
                        self.right += 1;
                        value
                    };
                    self.target[self.output] = self.source[from];
                    self.output += 1;
                    return true;
                }
                SortPhase::Inverting => {
                    if self.position < len {
                        // source[destination] = original slot. Invert into the
                        // now-spare merge array before moving any full records.
                        self.target[self.source[self.position]] = self.position;
                        self.position += 1;
                        return true;
                    }
                    self.position = 0;
                    self.phase = SortPhase::Permuting;
                }
                SortPhase::Permuting => {
                    if self.position == len {
                        self.phase = SortPhase::Ready;
                        continue;
                    }
                    let destination = self.target[self.position];
                    if destination == self.position {
                        self.position += 1;
                    } else {
                        // Move the record and its destination together. This
                        // fixes destination permanently; revisit position on
                        // the next tick until its cycle is exhausted.
                        self.values.swap(self.position, destination);
                        self.target.swap(self.position, destination);
                    }
                    return true;
                }
                SortPhase::Ready => return false,
                SortPhase::Retiring => unreachable!(),
            }
        }
    }
    pub(crate) fn finish(self) -> Vec<T> {
        assert!(self.phase == SortPhase::Ready, "stable sort is not ready");
        self.values
    }

    /// O(1) top-level backing ownership. This intentionally excludes any
    /// String/Vec allocations nested inside T and allocator metadata.
    pub(crate) fn memory(&self) -> IndirectSortMemory {
        IndirectSortMemory::new(&self.values, &self.source, &self.target)
    }

    #[cfg(test)]
    pub(crate) fn values_for_test(&self) -> &[T] {
        &self.values
    }

    /// Irreversibly retire one original row or one Copy-index backing array.
    /// At most one T is dropped per true result. Index arrays have no element
    /// destructors; popping every integer would postpone freeing their bytes
    /// without dividing meaningful work. Allocator deallocation is still an
    /// atomic native primitive, not a wall-clock bound.
    pub(crate) fn retire_one(&mut self) -> bool {
        self.retire_one_with(|_| {})
    }

    /// Variant used by owners which meter allocations nested inside `T`.
    /// The observer runs after the value leaves the Vec but before it drops;
    /// it is never called for either Copy-only index allocation.
    pub(crate) fn retire_one_with(&mut self, mut before_drop: impl FnMut(&T)) -> bool {
        self.phase = SortPhase::Retiring;
        if let Some(value) = self.values.pop() {
            before_drop(&value);
            drop(value);
            return true;
        }
        if self.source.capacity() != 0 {
            self.source = Vec::new();
            return true;
        }
        if self.target.capacity() != 0 {
            self.target = Vec::new();
            return true;
        }
        false
    }
}

#[cfg(test)]
mod sort_retirement_tests {
    use super::{Sort, SortWorkspace};
    use std::cell::Cell;
    use std::rc::Rc;

    #[test]
    fn already_sorted_input_finishes_after_one_bounded_validation_per_row() {
        for count in [0, 1, 257, 25_000] {
            let values: Vec<_> = (0..count).collect();
            let pointer = values.as_ptr();
            let capacity = values.capacity();
            let mut sort = Sort::new(values);
            let mut units = 0;
            while sort.tick() {
                units += 1;
            }
            assert_eq!(units, count);
            let values = sort.finish();
            assert_eq!(values.as_ptr(), pointer);
            assert_eq!(values.capacity(), capacity);
            assert!(values.into_iter().eq(0..count));
        }
    }

    #[test]
    fn prepared_sort_workspace_moves_both_buffers_without_row_or_index_reallocation() {
        for count in [0, 1, 257, 25_000] {
            let mut values = Vec::with_capacity(count + 17);
            values.extend((0..count).rev());
            let values_pointer = values.as_ptr();
            let values_capacity = values.capacity();
            let workspace = SortWorkspace::try_new(count).unwrap();
            assert!(workspace.source.is_empty() && workspace.target.is_empty());
            let buffers = [
                (workspace.source.as_ptr(), workspace.source.capacity()),
                (workspace.target.as_ptr(), workspace.target.capacity()),
            ];
            let mut sort = Sort::with_workspace(values, workspace);
            while sort.tick() {
                assert_eq!(sort.values.as_ptr(), values_pointer);
                assert_eq!(sort.values.capacity(), values_capacity);
                assert!(buffers.contains(&(sort.source.as_ptr(), sort.source.capacity())));
                assert!(buffers.contains(&(sort.target.as_ptr(), sort.target.capacity())));
            }
            let values = sort.finish();
            assert_eq!(values.as_ptr(), values_pointer);
            assert_eq!(values.capacity(), values_capacity);
            assert!(values.into_iter().eq(0..count));
        }
    }

    struct Tracked {
        key: usize,
        ordinal: usize,
        drops: Rc<Cell<usize>>,
        comparisons: Rc<Cell<usize>>,
    }
    impl Drop for Tracked {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }
    impl PartialEq for Tracked {
        fn eq(&self, other: &Self) -> bool {
            self.key == other.key
        }
    }
    impl Eq for Tracked {}
    impl PartialOrd for Tracked {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Ord for Tracked {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.comparisons.set(self.comparisons.get() + 1);
            self.key.cmp(&other.key)
        }
    }

    #[test]
    fn indirect_sort_is_stable_bounded_and_preserves_original_allocation() {
        for count in (0..75).chain([1_000, 25_000]) {
            for shape in 0..5 {
                let drops = Rc::new(Cell::new(0));
                let comparisons = Rc::new(Cell::new(0));
                // Spare capacity must be preserved too: no new full-record
                // output array is permitted even for empty input.
                let mut values = Vec::with_capacity(count + 17);
                for ordinal in 0..count {
                    let key = match shape {
                        0 => ordinal,
                        1 => count - ordinal,
                        2 => 0,
                        3 => (ordinal * 19) % 11,
                        _ => (ordinal + 1) % count,
                    };
                    values.push(Tracked {
                        key,
                        ordinal,
                        drops: drops.clone(),
                        comparisons: comparisons.clone(),
                    });
                }
                let mut expected: Vec<_> = values.iter().map(|v| (v.key, v.ordinal)).collect();
                expected.sort_by_key(|v| v.0);
                let pointer = values.as_ptr();
                let capacity = values.capacity();
                let mut sort = Sort::new(values);
                assert!(sort.source.is_empty() && sort.target.is_empty());
                let initial_memory = sort.memory();
                assert_eq!(initial_memory.sorters, 1);
                assert_eq!(initial_memory.values.length_slots, count);
                assert_eq!(initial_memory.values.capacity_slots, capacity);
                assert_eq!(
                    initial_memory.values.backing_capacity_bytes,
                    (capacity * std::mem::size_of::<Tracked>()) as u64
                );
                assert!(initial_memory.source_indices.capacity_slots >= count);
                assert!(initial_memory.target_indices.capacity_slots >= count);
                let index_allocations = [
                    (sort.source.as_ptr(), sort.source.capacity()),
                    (sort.target.as_ptr(), sort.target.capacity()),
                ];
                assert!(index_allocations
                    .iter()
                    .all(|(_, capacity)| *capacity >= count));
                let mut units = 0;
                loop {
                    let before = comparisons.get();
                    let more = sort.tick();
                    assert!(comparisons.get() - before <= 1);
                    assert_eq!(sort.values.as_ptr(), pointer);
                    assert_eq!(sort.values.capacity(), capacity);
                    let memory = sort.memory();
                    assert_eq!(memory.values.length_slots, sort.values.len());
                    assert_eq!(memory.values.capacity_slots, sort.values.capacity());
                    assert_eq!(memory.source_indices.length_slots, sort.source.len());
                    assert_eq!(memory.source_indices.capacity_slots, sort.source.capacity());
                    assert_eq!(memory.target_indices.length_slots, sort.target.len());
                    assert_eq!(memory.target_indices.capacity_slots, sort.target.capacity());
                    assert!(memory.counters_valid);
                    assert!(
                        index_allocations.contains(&(sort.source.as_ptr(), sort.source.capacity()))
                    );
                    assert!(
                        index_allocations.contains(&(sort.target.as_ptr(), sort.target.capacity()))
                    );
                    assert_eq!(drops.get(), 0);
                    if !more {
                        break;
                    }
                    units += 1;
                    assert!(units <= count * 32, "permutation cycle did not converge");
                }
                assert!(!sort.tick(), "ready is idempotent");
                let sorted = sort.finish();
                assert_eq!(sorted.as_ptr(), pointer);
                assert_eq!(sorted.capacity(), capacity);
                assert_eq!(
                    sorted
                        .iter()
                        .map(|v| (v.key, v.ordinal))
                        .collect::<Vec<_>>(),
                    expected
                );
                drop(sorted);
                assert_eq!(drops.get(), count);
            }
        }
    }

    #[test]
    fn stable_sort_retirement_25k_partial_merge_drops_at_most_one_owner_per_unit() {
        const COUNT: usize = 25_000;
        for advance in [
            0,
            12_500,
            25_000,
            25_123,
            74_999,
            175_000,
            400_001,
            425_001,
            440_001,
            usize::MAX,
        ] {
            let drops = Rc::new(Cell::new(0));
            let comparisons = Rc::new(Cell::new(0));
            let values = (0..COUNT)
                .rev()
                .map(|key| Tracked {
                    key,
                    ordinal: key,
                    drops: drops.clone(),
                    comparisons: comparisons.clone(),
                })
                .collect();
            let mut sort = Sort::new(values);
            let mut steps = 0;
            while steps < advance && sort.tick() {
                steps += 1;
            }
            assert_eq!(
                drops.get(),
                0,
                "sort lost or prematurely dropped an input owner"
            );
            let expected = sort.values.len()
                + usize::from(sort.source.capacity() != 0)
                + usize::from(sort.target.capacity() != 0);
            let mut units = 0;
            loop {
                let before = drops.get();
                let before_memory = sort.memory();
                if !sort.retire_one() {
                    break;
                }
                let after_memory = sort.memory();
                assert!(
                    drops.get() - before <= 1,
                    "retirement dropped multiple T owners"
                );
                if before_memory.values.length_slots != 0 {
                    assert_eq!(
                        after_memory.values.length_slots + 1,
                        before_memory.values.length_slots
                    );
                    assert_eq!(
                        after_memory.values.capacity_slots, before_memory.values.capacity_slots,
                        "retiring a value released the original Vec backing"
                    );
                } else {
                    let released = usize::from(
                        after_memory.source_indices.capacity_slots
                            < before_memory.source_indices.capacity_slots,
                    ) + usize::from(
                        after_memory.target_indices.capacity_slots
                            < before_memory.target_indices.capacity_slots,
                    );
                    assert_eq!(released, 1, "one unit must release one index backing");
                }
                units += 1;
            }
            assert_eq!(units, expected, "cleanup hid or invented owner work");
            assert_eq!(drops.get(), COUNT);
            assert!(!sort.retire_one());
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sort.tick())).is_err()
            );
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sort.finish())).is_err()
            );
            assert_eq!(
                drops.get(),
                COUNT,
                "drained backing drop revisited populated values"
            );
        }
    }
}

struct Load {
    pending: Vec<FileHash>,
    children: Option<LoadChildren>,
    moving: Option<IntoIter<FileEntry>>,
    entries: Vec<FileEntry>,
    visited: usize,
}

/// Canonical V1 wide-prefix nodes label their leaf chunks `prefix0`,
/// `prefix1`, ... but serialize those labels lexicographically. Restore the
/// numeric chunk order in place before the existing LIFO traversal. No second
/// child array or path owner is allocated; malformed/noncanonical labels fall
/// back to the original traversal and the downstream row sorter remains the
/// final ordering authority.
enum LoadChildren {
    Canonicalize { rows: Children, position: usize },
    Drain(IntoIter<(String, FileHash)>),
}

fn canonical_child_index(label: &str, prefix: &str, child_count: usize) -> Option<usize> {
    const USIZE_DECIMAL_DIGITS: usize = if usize::BITS == 64 { 20 } else { 10 };
    let suffix = label.strip_prefix(prefix)?;
    if suffix.is_empty()
        || (suffix.len() > 1 && suffix.as_bytes()[0] == b'0')
        || suffix.len() > USIZE_DECIMAL_DIGITS
    {
        return None;
    }
    let mut value = 0usize;
    for byte in suffix.bytes() {
        let digit = byte.checked_sub(b'0').filter(|digit| *digit <= 9)? as usize;
        value = value.checked_mul(10)?.checked_add(digit)?;
    }
    (value < child_count).then_some(value)
}

impl LoadChildren {
    /// One validation, swap, phase transition, or child handoff per unit.
    /// A canonical permutation is restored without allocation. Any duplicate,
    /// missing, out-of-range, or noncanonical label abandons the optimization
    /// but retains every child for the checked row-sort fallback.
    fn tick(&mut self, prefix: &str, pending: &mut Vec<FileHash>) -> bool {
        match self {
            Self::Canonicalize { rows, position } => {
                if *position == rows.len() {
                    let rows = std::mem::take(rows);
                    *self = Self::Drain(rows.into_iter());
                    return true;
                }
                let Some(target) = canonical_child_index(&rows[*position].0, prefix, rows.len())
                else {
                    let rows = std::mem::take(rows);
                    *self = Self::Drain(rows.into_iter());
                    return true;
                };
                if target == *position {
                    *position += 1;
                    return true;
                }
                // Positions below the cursor are already exact. A reference
                // to one of them, or a second label for an already-correct
                // target, proves this is not the canonical permutation.
                if target < *position
                    || canonical_child_index(&rows[target].0, prefix, rows.len()) == Some(target)
                {
                    let rows = std::mem::take(rows);
                    *self = Self::Drain(rows.into_iter());
                    return true;
                }
                rows.swap(*position, target);
                true
            }
            Self::Drain(rows) => {
                let Some((_, hash)) = rows.next_back() else {
                    return false;
                };
                pending.push(hash);
                true
            }
        }
    }

    fn retire_one(&mut self) -> bool {
        match self {
            Self::Canonicalize { rows, .. } => {
                if let Some(row) = rows.pop() {
                    drop(row);
                    true
                } else if rows.capacity() != 0 {
                    drop(std::mem::take(rows));
                    true
                } else {
                    false
                }
            }
            Self::Drain(rows) => {
                if let Some(row) = rows.next() {
                    drop(row);
                    true
                } else {
                    false
                }
            }
        }
    }
}

#[cfg(test)]
mod load_children_tests {
    use super::{canonical_child_index, hash_bytes, LoadChildren};

    fn child(label: String, index: usize) -> (String, [u8; 32]) {
        (label, hash_bytes(&index.to_le_bytes()))
    }

    #[test]
    fn canonical_wide_children_are_reordered_in_place_and_handed_off_numerically() {
        const COUNT: usize = 103;
        let prefix = "wide/";
        let mut rows: Vec<_> = (0..COUNT)
            .map(|index| child(format!("{prefix}{index}"), index))
            .collect();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        let pointer = rows.as_ptr();
        let capacity = rows.capacity();
        let expected: Vec<_> = (0..COUNT)
            .map(|index| hash_bytes(&index.to_le_bytes()))
            .collect();
        let mut children = LoadChildren::Canonicalize { rows, position: 0 };
        let mut pending = Vec::new();
        let mut units = 0;
        while children.tick(prefix, &mut pending) {
            units += 1;
            match &children {
                LoadChildren::Canonicalize { rows, .. } => {
                    assert_eq!(rows.as_ptr(), pointer);
                    assert_eq!(rows.capacity(), capacity);
                }
                LoadChildren::Drain(rows) if !rows.as_slice().is_empty() => {
                    assert_eq!(rows.as_slice().as_ptr(), pointer);
                }
                LoadChildren::Drain(_) => {}
            }
            assert!(units <= COUNT * 3 + 1, "child permutation did not converge");
        }
        let actual: Vec<_> = std::iter::from_fn(|| pending.pop()).collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn malformed_child_labels_fall_back_without_losing_an_owner() {
        let prefix = "wide/";
        let rows = vec![
            child("wide/1".into(), 0),
            child("wide/1".into(), 1),
            child("wide/02".into(), 2),
            child("other/3".into(), 3),
        ];
        let mut expected: Vec<_> = rows.iter().map(|row| row.1).collect();
        expected.sort();
        let mut children = LoadChildren::Canonicalize { rows, position: 0 };
        let mut pending = Vec::new();
        let mut units = 0;
        while children.tick(prefix, &mut pending) {
            units += 1;
            assert!(units <= 16, "malformed fallback did not converge");
        }
        pending.sort();
        assert_eq!(pending, expected);
        assert_eq!(canonical_child_index("wide/00", prefix, 1), None);
        assert_eq!(canonical_child_index("wide/1", prefix, 1), None);
        assert_eq!(canonical_child_index("not-wide/0", prefix, 1), None);
    }

    #[test]
    fn partial_child_permutation_retires_one_owned_row_or_backing_per_unit() {
        const COUNT: usize = 257;
        let prefix = "wide/";
        let mut rows: Vec<_> = (0..COUNT)
            .map(|index| child(format!("{prefix}{index}"), index))
            .collect();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        let mut children = LoadChildren::Canonicalize { rows, position: 0 };
        let mut pending = Vec::new();
        for _ in 0..37 {
            assert!(children.tick(prefix, &mut pending));
        }
        assert!(matches!(children, LoadChildren::Canonicalize { .. }));
        assert!(pending.is_empty());
        let mut units = 0;
        while children.retire_one() {
            units += 1;
        }
        assert_eq!(units, COUNT + 1, "retirement hid child owners in one unit");
        assert!(!children.retire_one());
    }
}

enum State {
    Upsert(FileEntry),
    Delete(String),
}
impl State {
    fn path(&self) -> &str {
        match self {
            Self::Upsert(row) => &row.path,
            Self::Delete(path) => path,
        }
    }
}
enum PrefixPhase {
    AppendProbe(AppendProbe),
    AppendAdds(IntoIter<FileEntry>),
    Load(Load),
    SortExisting(Sort<FileEntry>),
    SortAdds(Sort<FileEntry>),
    SortDeletes(Sort<String>),
    NormalizeAdds(IntoIter<FileEntry>),
    NormalizeDeletes(IntoIter<String>),
    States {
        adds: Peekable<IntoIter<FileEntry>>,
        deletes: Peekable<IntoIter<String>>,
    },
    Merge {
        existing: Peekable<IntoIter<FileEntry>>,
        states: Peekable<IntoIter<State>>,
    },
    Leaves(usize),
    InternalChildren(IntoIter<FileHash>, Children),
    Internal(Children),
    Retire(IntoIter<FileEntry>, Option<FileHash>),
    Done(Option<FileHash>),
    Transition,
}

enum AppendProbe {
    Read(FileHash),
    Canonicalize {
        rows: Children,
        position: usize,
    },
    Extract {
        rows: IntoIter<(String, FileHash)>,
        hashes: Vec<FileHash>,
    },
    ReadTail(Vec<FileHash>),
    ValidateTail {
        hashes: Vec<FileHash>,
        entries: Vec<FileEntry>,
        position: usize,
    },
    Search {
        hashes: Vec<FileHash>,
        lower: usize,
        upper: usize,
    },
    ValidateShape {
        hashes: Vec<FileHash>,
        index: usize,
        previous_last: Option<String>,
    },
    LoadUpdates {
        hashes: Vec<FileHash>,
        index: usize,
        adds: Peekable<IntoIter<FileEntry>>,
        pending: Vec<PendingUpdateLeaf>,
    },
    ValidateUpdateLeaf {
        hashes: Vec<FileHash>,
        index: usize,
        adds: Peekable<IntoIter<FileEntry>>,
        pending: Vec<PendingUpdateLeaf>,
        entries: Vec<FileEntry>,
        position: usize,
    },
    ApplyUpdateLeaf {
        hashes: Vec<FileHash>,
        index: usize,
        adds: Peekable<IntoIter<FileEntry>>,
        pending: Vec<PendingUpdateLeaf>,
        leaf: PendingUpdateLeaf,
    },
    EmitUpdates {
        hashes: Vec<FileHash>,
        pending: IntoIter<PendingUpdateLeaf>,
        retiring: Option<PendingUpdateLeaf>,
    },
    FallbackRetire {
        entries: Vec<IntoIter<FileEntry>>,
        adds: Vec<FileEntry>,
    },
}

struct PendingUpdateLeaf {
    index: usize,
    entries: Vec<FileEntry>,
    replacements: Vec<(usize, FileEntry)>,
}

fn retire_hashes(hashes: &mut Vec<FileHash>) -> bool {
    if hashes.pop().is_some() {
        true
    } else if hashes.capacity() != 0 {
        drop(std::mem::take(hashes));
        true
    } else {
        false
    }
}

fn retire_pending_leaf(leaf: &mut PendingUpdateLeaf) -> bool {
    if let Some(entry) = leaf.entries.pop() {
        drop(entry);
        true
    } else if leaf.entries.capacity() != 0 {
        drop(std::mem::take(&mut leaf.entries));
        true
    } else if let Some(replacement) = leaf.replacements.pop() {
        drop(replacement);
        true
    } else if leaf.replacements.capacity() != 0 {
        drop(std::mem::take(&mut leaf.replacements));
        true
    } else {
        false
    }
}

impl AppendProbe {
    fn retire_one(&mut self) -> bool {
        match self {
            Self::Read(_) => false,
            Self::Canonicalize { rows, .. } => {
                if let Some(row) = rows.pop() {
                    drop(row);
                    true
                } else if rows.capacity() != 0 {
                    drop(std::mem::take(rows));
                    true
                } else {
                    false
                }
            }
            Self::Extract { rows, hashes } => {
                if let Some(row) = rows.next() {
                    drop(row);
                    true
                } else if hashes.pop().is_some() {
                    true
                } else if hashes.capacity() != 0 {
                    drop(std::mem::take(hashes));
                    true
                } else {
                    false
                }
            }
            Self::ReadTail(hashes) => {
                if hashes.pop().is_some() {
                    true
                } else if hashes.capacity() != 0 {
                    drop(std::mem::take(hashes));
                    true
                } else {
                    false
                }
            }
            Self::ValidateTail {
                hashes, entries, ..
            } => {
                if let Some(entry) = entries.pop() {
                    drop(entry);
                    true
                } else if entries.capacity() != 0 {
                    drop(std::mem::take(entries));
                    true
                } else if hashes.pop().is_some() {
                    true
                } else if hashes.capacity() != 0 {
                    drop(std::mem::take(hashes));
                    true
                } else {
                    false
                }
            }
            Self::Search { hashes, .. } => retire_hashes(hashes),
            Self::ValidateShape {
                hashes,
                previous_last,
                ..
            } => {
                if previous_last.take().is_some() {
                    true
                } else {
                    retire_hashes(hashes)
                }
            }
            Self::LoadUpdates {
                hashes,
                adds,
                pending,
                ..
            } => retire_update_owners(hashes, adds, pending),
            Self::ValidateUpdateLeaf {
                hashes,
                adds,
                pending,
                entries,
                ..
            } => {
                if let Some(entry) = entries.pop() {
                    drop(entry);
                    true
                } else if entries.capacity() != 0 {
                    drop(std::mem::take(entries));
                    true
                } else {
                    retire_update_owners(hashes, adds, pending)
                }
            }
            Self::ApplyUpdateLeaf {
                hashes,
                adds,
                pending,
                leaf,
                ..
            } => {
                if retire_pending_leaf(leaf) {
                    true
                } else {
                    retire_update_owners(hashes, adds, pending)
                }
            }
            Self::EmitUpdates {
                hashes,
                pending,
                retiring,
            } => {
                if let Some(leaf) = retiring.as_mut() {
                    if !retire_pending_leaf(leaf) {
                        retiring.take();
                    }
                    true
                } else if let Some(leaf) = pending.next() {
                    *retiring = Some(leaf);
                    true
                } else {
                    retire_hashes(hashes)
                }
            }
            Self::FallbackRetire { entries, adds } => {
                if entries
                    .last()
                    .is_some_and(|rows| rows.as_slice().is_empty())
                {
                    entries.pop();
                    return true;
                }
                if let Some(entry) = entries.last_mut().and_then(Iterator::next) {
                    drop(entry);
                    true
                } else if let Some(entry) = adds.pop() {
                    drop(entry);
                    true
                } else if adds.capacity() != 0 {
                    drop(std::mem::take(adds));
                    true
                } else {
                    false
                }
            }
        }
    }
}

fn retire_update_owners(
    hashes: &mut Vec<FileHash>,
    adds: &mut Peekable<IntoIter<FileEntry>>,
    pending: &mut Vec<PendingUpdateLeaf>,
) -> bool {
    if let Some(entry) = adds.next() {
        drop(entry);
        return true;
    }
    if let Some(leaf) = pending.last_mut() {
        if retire_pending_leaf(leaf) {
            return true;
        }
        pending.pop();
        return true;
    }
    retire_hashes(hashes)
}

struct PrefixMutation {
    prefix: String,
    previous: Option<FileHash>,
    phase: PrefixPhase,
    adds: Vec<FileEntry>,
    deletes: Vec<String>,
    existing: Vec<FileEntry>,
    normalized_adds: Vec<FileEntry>,
    normalized_deletes: Vec<String>,
    states: Vec<State>,
    merged: Vec<FileEntry>,
    leaves: Vec<FileHash>,
    old_count: u64,
    new_count: u64,
}
impl PrefixMutation {
    fn new(
        prefix: String,
        adds: Vec<FileEntry>,
        deletes: Vec<String>,
        previous: Option<FileHash>,
    ) -> Self {
        let append_probe = previous.filter(|_| {
            deletes.is_empty()
                && !adds.is_empty()
                && adds.windows(2).all(|rows| rows[0].path < rows[1].path)
        });
        Self {
            prefix,
            previous,
            adds,
            deletes,
            existing: Vec::new(),
            normalized_adds: Vec::new(),
            normalized_deletes: Vec::new(),
            states: Vec::new(),
            merged: Vec::new(),
            leaves: Vec::new(),
            old_count: 0,
            new_count: 0,
            phase: append_probe.map_or_else(
                || PrefixPhase::Load(Self::full_load(previous)),
                |hash| PrefixPhase::AppendProbe(AppendProbe::Read(hash)),
            ),
        }
    }

    fn full_load(previous: Option<FileHash>) -> Load {
        Load {
            pending: previous.into_iter().collect(),
            children: None,
            moving: None,
            entries: Vec::new(),
            visited: 0,
        }
    }

    fn append_fallback(&mut self) {
        self.phase = PrefixPhase::Load(Self::full_load(self.previous));
    }

    fn child_fallback(&mut self, rows: Children) {
        self.phase = PrefixPhase::Load(Load {
            pending: Vec::new(),
            children: Some(LoadChildren::Drain(rows.into_iter())),
            moving: None,
            entries: Vec::new(),
            visited: 0,
        });
    }

    fn update_fallback(
        &mut self,
        mut pending: Vec<PendingUpdateLeaf>,
        current: Option<PendingUpdateLeaf>,
        remaining: Peekable<IntoIter<FileEntry>>,
    ) {
        if let Some(current) = current {
            pending.push(current);
        }
        let mut retired = Vec::with_capacity(pending.len());
        let mut adds = Vec::new();
        for mut leaf in pending {
            for (index, previous) in leaf.replacements.drain(..) {
                adds.push(std::mem::replace(&mut leaf.entries[index], previous));
            }
            retired.push(leaf.entries.into_iter());
        }
        adds.extend(remaining);
        self.phase = PrefixPhase::AppendProbe(AppendProbe::FallbackRetire {
            entries: retired,
            adds,
        });
    }
    fn tick(
        &mut self,
        store: &MemoryChunkStore,
        overlay: &mut HashMap<FileHash, Vec<u8>>,
        staged_bytes: &mut usize,
    ) -> Result<bool, ChunkError> {
        loop {
            let phase = std::mem::replace(&mut self.phase, PrefixPhase::Transition);
            match phase {
                PrefixPhase::AppendProbe(probe) => match probe {
                    AppendProbe::Read(hash) => {
                        let bytes = overlay
                            .get(&hash)
                            .cloned()
                            .or_else(|| store.get_chunk(&hash))
                            .ok_or_else(|| ChunkError::NotFound(hash_to_hex(&hash)))?;
                        if hash_bytes(&bytes) != hash {
                            return Err(invalid("index object failed content-address validation"));
                        }
                        if let Ok(leaf) = LeafChunk::deserialize(&bytes) {
                            self.phase = PrefixPhase::AppendProbe(AppendProbe::ValidateTail {
                                hashes: vec![hash],
                                entries: leaf.entries,
                                position: 0,
                            });
                        } else if let Ok(node) = InternalNode::deserialize(&bytes) {
                            self.phase = PrefixPhase::AppendProbe(AppendProbe::Canonicalize {
                                rows: node.children,
                                position: 0,
                            });
                        } else {
                            return Err(invalid("could not parse as LeafChunk or InternalNode"));
                        }
                        return Ok(true);
                    }
                    AppendProbe::Canonicalize {
                        mut rows,
                        mut position,
                    } => {
                        if position == rows.len() {
                            let capacity = rows.len();
                            self.phase = PrefixPhase::AppendProbe(AppendProbe::Extract {
                                rows: rows.into_iter(),
                                hashes: Vec::with_capacity(capacity),
                            });
                            return Ok(true);
                        }
                        let Some(target) =
                            canonical_child_index(&rows[position].0, &self.prefix, rows.len())
                        else {
                            self.child_fallback(rows);
                            return Ok(true);
                        };
                        if target == position {
                            position += 1;
                        } else if target < position
                            || canonical_child_index(&rows[target].0, &self.prefix, rows.len())
                                == Some(target)
                        {
                            self.child_fallback(rows);
                            return Ok(true);
                        } else {
                            rows.swap(position, target);
                        }
                        self.phase =
                            PrefixPhase::AppendProbe(AppendProbe::Canonicalize { rows, position });
                        return Ok(true);
                    }
                    AppendProbe::Extract {
                        mut rows,
                        mut hashes,
                    } => {
                        if let Some((label, hash)) = rows.next() {
                            drop(label);
                            hashes.push(hash);
                            self.phase =
                                PrefixPhase::AppendProbe(AppendProbe::Extract { rows, hashes });
                            return Ok(true);
                        }
                        if hashes.is_empty() {
                            self.append_fallback();
                        } else {
                            self.phase = PrefixPhase::AppendProbe(AppendProbe::ValidateShape {
                                hashes,
                                index: 0,
                                previous_last: None,
                            });
                        }
                        return Ok(true);
                    }
                    AppendProbe::ReadTail(hashes) => {
                        let hash = *hashes.last().expect("empty children fall back before tail");
                        let bytes = overlay
                            .get(&hash)
                            .cloned()
                            .or_else(|| store.get_chunk(&hash))
                            .ok_or_else(|| ChunkError::NotFound(hash_to_hex(&hash)))?;
                        if hash_bytes(&bytes) != hash {
                            return Err(invalid("index object failed content-address validation"));
                        }
                        let Ok(leaf) = LeafChunk::deserialize(&bytes) else {
                            self.append_fallback();
                            return Ok(true);
                        };
                        self.phase = PrefixPhase::AppendProbe(AppendProbe::ValidateTail {
                            hashes,
                            entries: leaf.entries,
                            position: 0,
                        });
                        return Ok(true);
                    }
                    AppendProbe::ValidateTail {
                        mut hashes,
                        entries,
                        mut position,
                    } => {
                        if entries.is_empty() || entries.len() > TARGET_CHUNK_ENTRIES {
                            let remaining = std::mem::take(&mut self.adds).into_iter().peekable();
                            self.update_fallback(
                                Vec::new(),
                                Some(PendingUpdateLeaf {
                                    index: 0,
                                    entries,
                                    replacements: Vec::new(),
                                }),
                                remaining,
                            );
                            return Ok(true);
                        }
                        if position < entries.len() {
                            if prefix_slice(&entries[position].path) != self.prefix
                                || (position > 0
                                    && entries[position - 1].path >= entries[position].path)
                            {
                                let remaining =
                                    std::mem::take(&mut self.adds).into_iter().peekable();
                                self.update_fallback(
                                    Vec::new(),
                                    Some(PendingUpdateLeaf {
                                        index: 0,
                                        entries,
                                        replacements: Vec::new(),
                                    }),
                                    remaining,
                                );
                            } else {
                                position += 1;
                                self.phase = PrefixPhase::AppendProbe(AppendProbe::ValidateTail {
                                    hashes,
                                    entries,
                                    position,
                                });
                            }
                            return Ok(true);
                        }
                        let first_add =
                            &self.adds.first().expect("append probe requires input").path;
                        if first_add <= &entries.last().unwrap().path {
                            if first_add < &entries[0].path {
                                let remaining =
                                    std::mem::take(&mut self.adds).into_iter().peekable();
                                self.update_fallback(
                                    Vec::new(),
                                    Some(PendingUpdateLeaf {
                                        index: 0,
                                        entries,
                                        replacements: Vec::new(),
                                    }),
                                    remaining,
                                );
                                return Ok(true);
                            }
                            self.phase = PrefixPhase::AppendProbe(AppendProbe::ApplyUpdateLeaf {
                                hashes,
                                index: 0,
                                adds: std::mem::take(&mut self.adds).into_iter().peekable(),
                                pending: Vec::new(),
                                leaf: PendingUpdateLeaf {
                                    index: 0,
                                    entries,
                                    replacements: Vec::new(),
                                },
                            });
                            return Ok(true);
                        }
                        hashes.pop();
                        self.old_count = (hashes.len() as u64)
                            .checked_mul(TARGET_CHUNK_ENTRIES as u64)
                            .and_then(|count| count.checked_add(entries.len() as u64))
                            .ok_or_else(|| invalid("candidate mutation file count overflow"))?;
                        self.new_count = self
                            .old_count
                            .checked_add(self.adds.len() as u64)
                            .ok_or_else(|| invalid("candidate mutation file count overflow"))?;
                        self.leaves = hashes;
                        self.merged = entries;
                        self.phase =
                            PrefixPhase::AppendAdds(std::mem::take(&mut self.adds).into_iter());
                        return Ok(true);
                    }
                    AppendProbe::ValidateShape {
                        hashes,
                        mut index,
                        previous_last,
                    } => {
                        if index == hashes.len() {
                            let upper = hashes.len();
                            self.phase = PrefixPhase::AppendProbe(AppendProbe::Search {
                                hashes,
                                lower: 0,
                                upper,
                            });
                            return Ok(true);
                        }
                        let hash = hashes[index];
                        let bytes = overlay
                            .get(&hash)
                            .cloned()
                            .or_else(|| store.get_chunk(&hash))
                            .ok_or_else(|| ChunkError::NotFound(hash_to_hex(&hash)))?;
                        if hash_bytes(&bytes) != hash {
                            return Err(invalid("index object failed content-address validation"));
                        }
                        let exact_entries =
                            (index + 1 < hashes.len()).then_some(TARGET_CHUNK_ENTRIES);
                        let Ok(Some((first, last))) = LeafChunk::validated_path_bounds(
                            &bytes,
                            &self.prefix,
                            exact_entries,
                            TARGET_CHUNK_ENTRIES,
                        ) else {
                            self.append_fallback();
                            return Ok(true);
                        };
                        if previous_last
                            .as_deref()
                            .is_some_and(|previous| previous >= first)
                        {
                            self.append_fallback();
                            return Ok(true);
                        }
                        let mut next_last = String::new();
                        next_last
                            .try_reserve_exact(last.len())
                            .map_err(|_| invalid("leaf boundary allocation failed"))?;
                        next_last.push_str(last);
                        index += 1;
                        self.phase = PrefixPhase::AppendProbe(AppendProbe::ValidateShape {
                            hashes,
                            index,
                            previous_last: Some(next_last),
                        });
                        return Ok(true);
                    }
                    AppendProbe::Search {
                        hashes,
                        mut lower,
                        mut upper,
                    } => {
                        if lower == upper {
                            if lower == hashes.len() {
                                self.phase =
                                    PrefixPhase::AppendProbe(AppendProbe::ReadTail(hashes));
                            } else {
                                self.append_fallback();
                            }
                            return Ok(true);
                        }
                        let middle = lower + (upper - lower) / 2;
                        let hash = hashes[middle];
                        let bytes = overlay
                            .get(&hash)
                            .cloned()
                            .or_else(|| store.get_chunk(&hash))
                            .ok_or_else(|| ChunkError::NotFound(hash_to_hex(&hash)))?;
                        if hash_bytes(&bytes) != hash {
                            return Err(invalid("index object failed content-address validation"));
                        }
                        let exact_entries =
                            (middle + 1 < hashes.len()).then_some(TARGET_CHUNK_ENTRIES);
                        let Ok(Some((first, last))) = LeafChunk::validated_path_bounds(
                            &bytes,
                            &self.prefix,
                            exact_entries,
                            TARGET_CHUNK_ENTRIES,
                        ) else {
                            self.append_fallback();
                            return Ok(true);
                        };
                        let path = &self
                            .adds
                            .first()
                            .expect("ordered probe requires input")
                            .path;
                        if path.as_str() < first {
                            upper = middle;
                            self.phase = PrefixPhase::AppendProbe(AppendProbe::Search {
                                hashes,
                                lower,
                                upper,
                            });
                        } else if path.as_str() > last {
                            lower = middle + 1;
                            self.phase = PrefixPhase::AppendProbe(AppendProbe::Search {
                                hashes,
                                lower,
                                upper,
                            });
                        } else {
                            self.phase = PrefixPhase::AppendProbe(AppendProbe::LoadUpdates {
                                hashes,
                                index: middle,
                                adds: std::mem::take(&mut self.adds).into_iter().peekable(),
                                pending: Vec::new(),
                            });
                        }
                        return Ok(true);
                    }
                    AppendProbe::LoadUpdates {
                        hashes,
                        mut index,
                        mut adds,
                        pending,
                    } => {
                        let Some(path) = adds.peek().map(|row| row.path.as_str()) else {
                            self.phase = PrefixPhase::AppendProbe(AppendProbe::EmitUpdates {
                                hashes,
                                pending: pending.into_iter(),
                                retiring: None,
                            });
                            return Ok(true);
                        };
                        if index >= hashes.len() {
                            self.update_fallback(pending, None, adds);
                            return Ok(true);
                        }
                        let hash = hashes[index];
                        let bytes = overlay
                            .get(&hash)
                            .cloned()
                            .or_else(|| store.get_chunk(&hash))
                            .ok_or_else(|| ChunkError::NotFound(hash_to_hex(&hash)))?;
                        if hash_bytes(&bytes) != hash {
                            return Err(invalid("index object failed content-address validation"));
                        }
                        let exact_entries =
                            (index + 1 < hashes.len()).then_some(TARGET_CHUNK_ENTRIES);
                        let Ok(Some((first, last))) = LeafChunk::validated_path_bounds(
                            &bytes,
                            &self.prefix,
                            exact_entries,
                            TARGET_CHUNK_ENTRIES,
                        ) else {
                            self.update_fallback(pending, None, adds);
                            return Ok(true);
                        };
                        if path > last {
                            index += 1;
                            self.phase = PrefixPhase::AppendProbe(AppendProbe::LoadUpdates {
                                hashes,
                                index,
                                adds,
                                pending,
                            });
                            return Ok(true);
                        }
                        if path < first {
                            self.update_fallback(pending, None, adds);
                            return Ok(true);
                        }
                        let leaf = LeafChunk::deserialize(&bytes).map_err(|_| {
                            invalid("candidate update leaf failed canonical deserialization")
                        })?;
                        self.phase = PrefixPhase::AppendProbe(AppendProbe::ValidateUpdateLeaf {
                            hashes,
                            index,
                            adds,
                            pending,
                            entries: leaf.entries,
                            position: 1,
                        });
                        return Ok(true);
                    }
                    AppendProbe::ValidateUpdateLeaf {
                        hashes,
                        index,
                        adds,
                        pending,
                        entries,
                        mut position,
                    } => {
                        let expected_full = index + 1 < hashes.len();
                        if entries.is_empty()
                            || entries.len() > TARGET_CHUNK_ENTRIES
                            || (expected_full && entries.len() != TARGET_CHUNK_ENTRIES)
                        {
                            self.update_fallback(
                                pending,
                                Some(PendingUpdateLeaf {
                                    index,
                                    entries,
                                    replacements: Vec::new(),
                                }),
                                adds,
                            );
                            return Ok(true);
                        }
                        if position < entries.len() {
                            if entries[position - 1].path >= entries[position].path {
                                self.update_fallback(
                                    pending,
                                    Some(PendingUpdateLeaf {
                                        index,
                                        entries,
                                        replacements: Vec::new(),
                                    }),
                                    adds,
                                );
                            } else {
                                position += 1;
                                self.phase =
                                    PrefixPhase::AppendProbe(AppendProbe::ValidateUpdateLeaf {
                                        hashes,
                                        index,
                                        adds,
                                        pending,
                                        entries,
                                        position,
                                    });
                            }
                            return Ok(true);
                        }
                        self.phase = PrefixPhase::AppendProbe(AppendProbe::ApplyUpdateLeaf {
                            hashes,
                            index,
                            adds,
                            pending,
                            leaf: PendingUpdateLeaf {
                                index,
                                entries,
                                replacements: Vec::new(),
                            },
                        });
                        return Ok(true);
                    }
                    AppendProbe::ApplyUpdateLeaf {
                        hashes,
                        index,
                        mut adds,
                        mut pending,
                        mut leaf,
                    } => {
                        let Some(path) = adds.peek().map(|row| row.path.as_str()) else {
                            pending.push(leaf);
                            self.phase = PrefixPhase::AppendProbe(AppendProbe::EmitUpdates {
                                hashes,
                                pending: pending.into_iter(),
                                retiring: None,
                            });
                            return Ok(true);
                        };
                        if path > leaf.entries.last().unwrap().path.as_str() {
                            pending.push(leaf);
                            self.phase = PrefixPhase::AppendProbe(AppendProbe::LoadUpdates {
                                hashes,
                                index: index + 1,
                                adds,
                                pending,
                            });
                            return Ok(true);
                        }
                        let Ok(position) = leaf
                            .entries
                            .binary_search_by(|entry| entry.path.as_str().cmp(path))
                        else {
                            self.update_fallback(pending, Some(leaf), adds);
                            return Ok(true);
                        };
                        let row = adds.next().expect("peeked update disappeared");
                        let previous = std::mem::replace(&mut leaf.entries[position], row);
                        leaf.replacements.push((position, previous));
                        self.phase = PrefixPhase::AppendProbe(AppendProbe::ApplyUpdateLeaf {
                            hashes,
                            index,
                            adds,
                            pending,
                            leaf,
                        });
                        return Ok(true);
                    }
                    AppendProbe::EmitUpdates {
                        mut hashes,
                        mut pending,
                        mut retiring,
                    } => {
                        if let Some(leaf) = &mut retiring {
                            if retire_pending_leaf(leaf) {
                                self.phase = PrefixPhase::AppendProbe(AppendProbe::EmitUpdates {
                                    hashes,
                                    pending,
                                    retiring,
                                });
                                return Ok(true);
                            }
                            retiring.take();
                        }
                        if let Some(leaf) = pending.next() {
                            let bytes = LeafChunk::serialize_entries(&leaf.entries);
                            hashes[leaf.index] = stage(overlay, staged_bytes, bytes)?;
                            self.phase = PrefixPhase::AppendProbe(AppendProbe::EmitUpdates {
                                hashes,
                                pending,
                                retiring: Some(leaf),
                            });
                            return Ok(true);
                        }
                        let hash = if hashes.len() == 1 {
                            hashes[0]
                        } else {
                            let children = hashes
                                .into_iter()
                                .enumerate()
                                .map(|(index, hash)| (format!("{}{}", self.prefix, index), hash))
                                .collect();
                            stage(
                                overlay,
                                staged_bytes,
                                InternalNode::new(children).serialize(),
                            )?
                        };
                        self.phase = PrefixPhase::Done(Some(hash));
                        return Ok(true);
                    }
                    AppendProbe::FallbackRetire { mut entries, adds } => {
                        if let Some(rows) = entries.last_mut() {
                            if let Some(entry) = rows.next() {
                                drop(entry);
                            } else {
                                entries.pop();
                            }
                            self.phase = PrefixPhase::AppendProbe(AppendProbe::FallbackRetire {
                                entries,
                                adds,
                            });
                            return Ok(true);
                        }
                        self.adds = adds;
                        self.append_fallback();
                        return Ok(true);
                    }
                },
                PrefixPhase::AppendAdds(mut rows) => {
                    if let Some(row) = rows.next() {
                        self.merged.push(row);
                        self.phase = PrefixPhase::AppendAdds(rows);
                        return Ok(true);
                    }
                    self.phase = PrefixPhase::Leaves(0);
                }
                PrefixPhase::Load(mut load) => {
                    if let Some(moving) = &mut load.moving {
                        if let Some(entry) = moving.next() {
                            load.entries.push(entry);
                            self.phase = PrefixPhase::Load(load);
                            return Ok(true);
                        }
                        load.moving = None;
                    }
                    if let Some(children) = &mut load.children {
                        if children.tick(&self.prefix, &mut load.pending) {
                            self.phase = PrefixPhase::Load(load);
                            return Ok(true);
                        }
                        load.children = None;
                    }
                    if let Some(hash) = load.pending.pop() {
                        load.visited += 1;
                        if load.visited > MAX_VISITED_NODES {
                            return Err(invalid("tree has too many nodes"));
                        }
                        let bytes = overlay
                            .get(&hash)
                            .cloned()
                            .or_else(|| store.get_chunk(&hash))
                            .ok_or_else(|| ChunkError::NotFound(hash_to_hex(&hash)))?;
                        if hash_bytes(&bytes) != hash {
                            return Err(invalid("index object failed content-address validation"));
                        }
                        if let Ok(leaf) = LeafChunk::deserialize(&bytes) {
                            if load.entries.len().saturating_add(leaf.entries.len())
                                > MAX_LOADED_ENTRIES
                            {
                                return Err(invalid("tree has too many entries"));
                            }
                            load.moving = Some(leaf.entries.into_iter());
                        } else if let Ok(node) = InternalNode::deserialize(&bytes) {
                            load.children = Some(LoadChildren::Canonicalize {
                                rows: node.children,
                                position: 0,
                            });
                        } else {
                            return Err(invalid("could not parse as LeafChunk or InternalNode"));
                        }
                        self.phase = PrefixPhase::Load(load);
                        return Ok(true);
                    }
                    self.old_count = load.entries.len() as u64;
                    self.phase = PrefixPhase::SortExisting(Sort::new(load.entries));
                }
                PrefixPhase::SortExisting(mut sort) => {
                    if sort.tick() {
                        self.phase = PrefixPhase::SortExisting(sort);
                        return Ok(true);
                    }
                    self.existing = sort.finish();
                    self.phase = PrefixPhase::SortAdds(Sort::new(std::mem::take(&mut self.adds)));
                }
                PrefixPhase::SortAdds(mut sort) => {
                    if sort.tick() {
                        self.phase = PrefixPhase::SortAdds(sort);
                        return Ok(true);
                    }
                    self.phase = PrefixPhase::NormalizeAdds(sort.finish().into_iter());
                }
                PrefixPhase::NormalizeAdds(mut rows) => {
                    if let Some(row) = rows.next() {
                        if self
                            .normalized_adds
                            .last()
                            .is_some_and(|old| old.path == row.path)
                        {
                            *self.normalized_adds.last_mut().unwrap() = row;
                        } else {
                            self.normalized_adds.push(row);
                        }
                        self.phase = PrefixPhase::NormalizeAdds(rows);
                        return Ok(true);
                    }
                    self.phase =
                        PrefixPhase::SortDeletes(Sort::new(std::mem::take(&mut self.deletes)));
                }
                PrefixPhase::SortDeletes(mut sort) => {
                    if sort.tick() {
                        self.phase = PrefixPhase::SortDeletes(sort);
                        return Ok(true);
                    }
                    self.phase = PrefixPhase::NormalizeDeletes(sort.finish().into_iter());
                }
                PrefixPhase::NormalizeDeletes(mut rows) => {
                    if let Some(row) = rows.next() {
                        if self.normalized_deletes.last() != Some(&row) {
                            self.normalized_deletes.push(row);
                        }
                        self.phase = PrefixPhase::NormalizeDeletes(rows);
                        return Ok(true);
                    }
                    self.phase = PrefixPhase::States {
                        adds: std::mem::take(&mut self.normalized_adds)
                            .into_iter()
                            .peekable(),
                        deletes: std::mem::take(&mut self.normalized_deletes)
                            .into_iter()
                            .peekable(),
                    };
                }
                PrefixPhase::States {
                    mut adds,
                    mut deletes,
                } => {
                    let take_add = match (adds.peek(), deletes.peek()) {
                        (Some(a), Some(d)) => a.path <= *d,
                        (Some(_), None) => true,
                        (None, Some(_)) => false,
                        (None, None) => {
                            self.phase = PrefixPhase::Merge {
                                existing: std::mem::take(&mut self.existing).into_iter().peekable(),
                                states: std::mem::take(&mut self.states).into_iter().peekable(),
                            };
                            continue;
                        }
                    };
                    if take_add {
                        let entry = adds.next().unwrap();
                        if deletes.peek() == Some(&entry.path) {
                            deletes.next();
                        }
                        self.states.push(State::Upsert(entry));
                    } else {
                        self.states.push(State::Delete(deletes.next().unwrap()));
                    }
                    self.phase = PrefixPhase::States { adds, deletes };
                    return Ok(true);
                }
                PrefixPhase::Merge {
                    mut existing,
                    mut states,
                } => {
                    let take_existing = match (existing.peek(), states.peek()) {
                        (Some(entry), Some(state)) => entry.path.as_str() < state.path(),
                        (Some(_), None) => true,
                        (None, Some(_)) => false,
                        (None, None) => {
                            self.new_count = self.merged.len() as u64;
                            self.phase = PrefixPhase::Leaves(0);
                            continue;
                        }
                    };
                    if take_existing {
                        self.merged.push(existing.next().unwrap());
                    } else {
                        let state = states.next().unwrap();
                        if existing
                            .peek()
                            .is_some_and(|entry| entry.path == state.path())
                        {
                            existing.next();
                        }
                        if let State::Upsert(entry) = state {
                            self.merged.push(entry);
                        }
                    }
                    self.phase = PrefixPhase::Merge { existing, states };
                    return Ok(true);
                }
                PrefixPhase::Leaves(offset) => {
                    if offset < self.merged.len() {
                        let end = (offset + TARGET_CHUNK_ENTRIES).min(self.merged.len());
                        let bytes = LeafChunk::new(self.merged[offset..end].to_vec()).serialize();
                        self.leaves.push(stage(overlay, staged_bytes, bytes)?);
                        self.phase = PrefixPhase::Leaves(end);
                        return Ok(true);
                    }
                    if self.leaves.len() <= 1 {
                        self.phase = PrefixPhase::Retire(
                            std::mem::take(&mut self.merged).into_iter(),
                            self.leaves.first().copied(),
                        );
                    } else {
                        self.phase = PrefixPhase::InternalChildren(
                            std::mem::take(&mut self.leaves).into_iter(),
                            Vec::new(),
                        );
                    }
                }
                PrefixPhase::InternalChildren(mut hashes, mut children) => {
                    if let Some(hash) = hashes.next() {
                        children.push((format!("{}{}", self.prefix, children.len()), hash));
                        self.phase = PrefixPhase::InternalChildren(hashes, children);
                        return Ok(true);
                    }
                    self.phase = PrefixPhase::Internal(children);
                }
                PrefixPhase::Internal(children) => {
                    let bytes = InternalNode::new(children).serialize();
                    let hash = stage(overlay, staged_bytes, bytes)?;
                    self.phase = PrefixPhase::Retire(
                        std::mem::take(&mut self.merged).into_iter(),
                        Some(hash),
                    );
                    return Ok(true);
                }
                PrefixPhase::Retire(mut entries, hash) => {
                    if entries.next().is_some() {
                        self.phase = PrefixPhase::Retire(entries, hash);
                        return Ok(true);
                    }
                    self.phase = PrefixPhase::Done(hash);
                }
                PrefixPhase::Done(hash) => {
                    self.phase = PrefixPhase::Done(hash);
                    return Ok(false);
                }
                PrefixPhase::Transition => return Err(invalid("candidate mutation cursor failed")),
            }
        }
    }

    /// Retire at most one populated record (or one already-drained backing)
    /// from the active prefix. Cancellation must not hide a many-thousand-row
    /// destructor behind the outer `Option<PrefixMutation>`.
    fn retire_one(&mut self) -> bool {
        let mut phase_drained = false;
        let did_work = match &mut self.phase {
            PrefixPhase::AppendProbe(probe) => {
                if probe.retire_one() {
                    true
                } else {
                    phase_drained = true;
                    false
                }
            }
            PrefixPhase::AppendAdds(rows) => {
                if let Some(row) = rows.next() {
                    drop(row);
                    true
                } else {
                    phase_drained = true;
                    false
                }
            }
            PrefixPhase::Load(load) => {
                if let Some(rows) = &mut load.moving {
                    if let Some(row) = rows.next() {
                        drop(row);
                        true
                    } else {
                        load.moving = None;
                        true
                    }
                } else if let Some(children) = &mut load.children {
                    if children.retire_one() {
                        true
                    } else {
                        load.children = None;
                        true
                    }
                } else if load.pending.pop().is_some() {
                    true
                } else if load.pending.capacity() != 0 {
                    drop(std::mem::take(&mut load.pending));
                    true
                } else if let Some(row) = load.entries.pop() {
                    drop(row);
                    true
                } else if load.entries.capacity() != 0 {
                    drop(std::mem::take(&mut load.entries));
                    true
                } else {
                    phase_drained = true;
                    false
                }
            }
            PrefixPhase::SortExisting(sort) | PrefixPhase::SortAdds(sort) => {
                if sort.retire_one() {
                    true
                } else {
                    phase_drained = true;
                    false
                }
            }
            PrefixPhase::SortDeletes(sort) => {
                if sort.retire_one() {
                    true
                } else {
                    phase_drained = true;
                    false
                }
            }
            PrefixPhase::NormalizeAdds(rows) | PrefixPhase::Retire(rows, _) => {
                if let Some(row) = rows.next() {
                    drop(row);
                    true
                } else {
                    phase_drained = true;
                    false
                }
            }
            PrefixPhase::NormalizeDeletes(rows) => {
                if let Some(path) = rows.next() {
                    drop(path);
                    true
                } else {
                    phase_drained = true;
                    false
                }
            }
            PrefixPhase::States { adds, deletes } => {
                if let Some(row) = adds.next() {
                    drop(row);
                    true
                } else if let Some(path) = deletes.next() {
                    drop(path);
                    true
                } else {
                    phase_drained = true;
                    false
                }
            }
            PrefixPhase::Merge { existing, states } => {
                if let Some(row) = existing.next() {
                    drop(row);
                    true
                } else if let Some(state) = states.next() {
                    drop(state);
                    true
                } else {
                    phase_drained = true;
                    false
                }
            }
            PrefixPhase::InternalChildren(hashes, children) => {
                if hashes.next().is_some() {
                    true
                } else if let Some(child) = children.pop() {
                    drop(child);
                    true
                } else if children.capacity() != 0 {
                    drop(std::mem::take(children));
                    true
                } else {
                    phase_drained = true;
                    false
                }
            }
            PrefixPhase::Internal(children) => {
                if let Some(child) = children.pop() {
                    drop(child);
                    true
                } else if children.capacity() != 0 {
                    drop(std::mem::take(children));
                    true
                } else {
                    phase_drained = true;
                    false
                }
            }
            PrefixPhase::Leaves(_) | PrefixPhase::Done(_) | PrefixPhase::Transition => {
                phase_drained = true;
                false
            }
        };
        if did_work {
            return true;
        }
        if phase_drained && !matches!(self.phase, PrefixPhase::Transition) {
            // Every populated element above was drained separately. Dropping
            // the now-empty iterator/sorter shell is one cleanup unit.
            self.phase = PrefixPhase::Transition;
            return true;
        }

        if let Some(row) = self.adds.pop() {
            drop(row);
            return true;
        }
        if let Some(path) = self.deletes.pop() {
            drop(path);
            return true;
        }
        if let Some(row) = self.existing.pop() {
            drop(row);
            return true;
        }
        if let Some(row) = self.normalized_adds.pop() {
            drop(row);
            return true;
        }
        if let Some(path) = self.normalized_deletes.pop() {
            drop(path);
            return true;
        }
        if let Some(state) = self.states.pop() {
            drop(state);
            return true;
        }
        if let Some(row) = self.merged.pop() {
            drop(row);
            return true;
        }
        if self.leaves.pop().is_some() {
            return true;
        }
        if !self.prefix.is_empty() {
            drop(std::mem::take(&mut self.prefix));
            return true;
        }
        false
    }
}

fn stage(
    overlay: &mut HashMap<FileHash, Vec<u8>>,
    bytes_total: &mut usize,
    bytes: Vec<u8>,
) -> Result<FileHash, ChunkError> {
    let hash = hash_bytes(&bytes);
    if let Some(previous) = overlay.get(&hash) {
        if previous != &bytes {
            return Err(invalid("staged chunk collision"));
        }
        return Ok(hash);
    }
    *bytes_total = bytes_total
        .checked_add(bytes.len())
        .ok_or_else(|| invalid("staged chunk byte count overflow"))?;
    overlay.insert(hash, bytes);
    Ok(hash)
}

struct RootAssembly {
    old: Peekable<IntoIter<(String, FileHash)>>,
    replacements: Peekable<btree_map::IntoIter<String, Option<FileHash>>>,
    children: Children,
    parent: IncrementalHasher,
    retirement_phase: u8,
}
impl RootAssembly {
    fn take_old(&mut self) -> (String, FileHash) {
        let row = self.old.next().unwrap();
        self.parent.update_str(&row.0).update(&row.1);
        row
    }
    fn tick(&mut self) -> bool {
        let old_first = match (self.old.peek(), self.replacements.peek()) {
            (Some(old), Some(new)) => old.0 < new.0,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => return false,
        };
        if old_first {
            let row = self.take_old();
            self.children.push(row);
        } else {
            let (path, hash) = self.replacements.next().unwrap();
            if self.old.peek().is_some_and(|old| old.0 == path) {
                self.take_old();
            }
            if let Some(hash) = hash {
                self.children.push((path, hash));
            }
        }
        true
    }

    fn retire_one(&mut self) -> bool {
        if self.retirement_phase == 0 {
            if let Some(row) = self.old.next() {
                drop(row);
                return true;
            }
            self.old = Vec::new().into_iter().peekable();
            self.retirement_phase = 1;
            return true;
        }
        if self.retirement_phase == 1 {
            if let Some(row) = self.replacements.next() {
                drop(row);
                return true;
            }
            self.replacements = BTreeMap::new().into_iter().peekable();
            self.retirement_phase = 2;
            return true;
        }
        if let Some(row) = self.children.pop() {
            drop(row);
            return true;
        }
        if self.children.capacity() != 0 {
            drop(std::mem::take(&mut self.children));
            return true;
        }
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CandidateMutationProgress {
    pub units: usize,
    pub completed: usize,
    pub staged_chunks: usize,
    pub staged_bytes: usize,
    pub done: bool,
}

/// Caller supplies an owned clone of the current, validated canonical root.
/// Exactly one logical update is represented, even for nonempty no-op edits.
pub(crate) struct CandidateMutationV1 {
    root: Option<RootNode>,
    changed: IntoIter<FileEntry>,
    deleted: IntoIter<String>,
    groups: BTreeMap<String, Operations>,
    grouping: bool,
    active: Option<PrefixMutation>,
    replacements: BTreeMap<String, Option<FileHash>>,
    assembly: Option<RootAssembly>,
    overlay: HashMap<FileHash, Vec<u8>>,
    staged_bytes: usize,
    completed: usize,
    ready: bool,
    failed: bool,
}
impl CandidateMutationV1 {
    pub(crate) fn new(root: RootNode, changed: Vec<FileEntry>, deleted: Vec<String>) -> Self {
        Self {
            root: Some(root),
            changed: changed.into_iter(),
            deleted: deleted.into_iter(),
            groups: BTreeMap::new(),
            grouping: true,
            active: None,
            replacements: BTreeMap::new(),
            assembly: None,
            overlay: HashMap::new(),
            staged_bytes: 0,
            completed: 0,
            ready: false,
            failed: false,
        }
    }
    pub(crate) fn progress(&self) -> CandidateMutationProgress {
        CandidateMutationProgress {
            units: 0,
            completed: self.completed,
            staged_chunks: self.overlay.len(),
            staged_bytes: self.staged_bytes,
            done: self.ready && !self.failed,
        }
    }

    /// Retire one directly owned input/output record or one drained backing.
    /// Nested prefix/root assembly owners are themselves cooperatively
    /// drained, so cancellation never bulk-drops their row collections.
    pub(crate) fn retire_one(&mut self) -> bool {
        if let Some(entry) = self.changed.next() {
            drop(entry);
            return true;
        }
        if let Some(path) = self.deleted.next() {
            drop(path);
            return true;
        }
        if let Some(mut group) = self.groups.first_entry() {
            if let Some(entry) = group.get_mut().0.pop() {
                drop(entry);
                return true;
            }
            if let Some(path) = group.get_mut().1.pop() {
                drop(path);
                return true;
            }
            let (prefix, operations) = group.remove_entry();
            drop((prefix, operations));
            return true;
        }
        if let Some(active) = &mut self.active {
            if active.retire_one() {
                return true;
            }
            self.active = None;
            return true;
        }
        if let Some(assembly) = &mut self.assembly {
            if assembly.retire_one() {
                return true;
            }
            self.assembly = None;
            return true;
        }
        if let Some((prefix, hash)) = self.replacements.pop_first() {
            drop((prefix, hash));
            return true;
        }
        if let Some(hash) = self.overlay.keys().next().copied() {
            self.overlay.remove(&hash);
            return true;
        }
        if let Some(root) = &mut self.root {
            if let Some(child) = root.children.pop() {
                drop(child);
                return true;
            }
            if !root.vault_id.is_empty() {
                drop(std::mem::take(&mut root.vault_id));
                return true;
            }
            if !root.device_id.is_empty() {
                drop(std::mem::take(&mut root.device_id));
                return true;
            }
            self.root = None;
            return true;
        }
        false
    }

    pub(crate) fn root_requested_bytes(&self) -> Result<u64, ChunkError> {
        let root = self
            .root
            .as_ref()
            .ok_or_else(|| invalid("candidate mutation lost its root owner"))?;
        let string_bytes = root
            .children
            .iter()
            .try_fold(
                root.vault_id
                    .capacity()
                    .saturating_add(root.device_id.capacity()),
                |sum, row| sum.checked_add(row.0.capacity()),
            )
            .ok_or_else(|| invalid("candidate root string byte count overflow"))?;
        let backing = root
            .children
            .capacity()
            .checked_mul(std::mem::size_of::<(String, FileHash)>())
            .ok_or_else(|| invalid("candidate root backing byte count overflow"))?;
        u64::try_from(
            string_bytes
                .checked_add(backing)
                .ok_or_else(|| invalid("candidate root requested byte count overflow"))?,
        )
        .map_err(|_| invalid("candidate root requested byte count overflow"))
    }

    fn validating_canonical_leaf_shape(&self) -> bool {
        matches!(
            self.active.as_ref().map(|prefix| &prefix.phase),
            Some(PrefixPhase::AppendProbe(AppendProbe::ValidateShape { .. }))
        )
    }

    pub(crate) fn step(
        &mut self,
        store: &MemoryChunkStore,
        max_units: usize,
    ) -> Result<CandidateMutationProgress, ChunkError> {
        if self.failed {
            return Err(invalid("candidate mutation cursor failed"));
        }
        if max_units == 0 {
            return Err(invalid("candidate mutation budget must be positive"));
        }
        let mut units = 0;
        while units < max_units && !self.ready {
            let validating_leaf = self.validating_canonical_leaf_shape();
            match self.tick(store) {
                Ok(true) => {
                    units += 1;
                    self.completed += 1;
                    // A borrowed validation scans at most one fixed-size leaf,
                    // but multiple such native scans must never accumulate in
                    // one renderer turn even when the surrounding mutation
                    // quantum is much larger.
                    if validating_leaf {
                        break;
                    }
                }
                Ok(false) => {}
                Err(error) => {
                    self.failed = true;
                    return Err(error);
                }
            }
        }
        Ok(CandidateMutationProgress {
            units,
            ..self.progress()
        })
    }
    fn tick(&mut self, store: &MemoryChunkStore) -> Result<bool, ChunkError> {
        if self.grouping {
            if let Some(row) = self.changed.next() {
                self.groups
                    .entry(prefix(&row.path))
                    .or_default()
                    .0
                    .push(row);
                return Ok(true);
            }
            if let Some(path) = self.deleted.next() {
                self.groups.entry(prefix(&path)).or_default().1.push(path);
                return Ok(true);
            }
            self.grouping = false;
        }
        if let Some(active) = &mut self.active {
            if active.tick(store, &mut self.overlay, &mut self.staged_bytes)? {
                return Ok(true);
            }
            let PrefixPhase::Done(hash) = active.phase else {
                unreachable!()
            };
            let root = self
                .root
                .as_mut()
                .ok_or_else(|| invalid("candidate mutation lost its root owner"))?;
            root.total_files = root
                .total_files
                .checked_sub(active.old_count)
                .and_then(|count| count.checked_add(active.new_count))
                .ok_or_else(|| invalid("candidate mutation file count overflow"))?;
            self.replacements.insert(active.prefix.clone(), hash);
            self.active = None;
            return Ok(true);
        }
        if let Some((path, (adds, deletes))) = self.groups.pop_first() {
            let previous = self
                .root
                .as_ref()
                .ok_or_else(|| invalid("candidate mutation lost its root owner"))?
                .children
                .binary_search_by(|row| row.0.cmp(&path))
                .ok()
                .map(|index| {
                    self.root
                        .as_ref()
                        .expect("candidate root checked above")
                        .children[index]
                        .1
                });
            self.active = Some(PrefixMutation::new(path, adds, deletes, previous));
            return Ok(true);
        }
        if self.assembly.is_none() {
            self.assembly = Some(RootAssembly {
                old: std::mem::take(
                    &mut self
                        .root
                        .as_mut()
                        .ok_or_else(|| invalid("candidate mutation lost its root owner"))?
                        .children,
                )
                .into_iter()
                .peekable(),
                replacements: std::mem::take(&mut self.replacements)
                    .into_iter()
                    .peekable(),
                children: Vec::new(),
                parent: IncrementalHasher::new(),
                retirement_phase: 0,
            });
        }
        if self.assembly.as_mut().unwrap().tick() {
            return Ok(true);
        }
        let assembly = self.assembly.take().unwrap();
        let root = self
            .root
            .as_mut()
            .ok_or_else(|| invalid("candidate mutation lost its root owner"))?;
        root.children = assembly.children;
        root.parent_hash = Some(assembly.parent.finalize());
        self.ready = true;
        Ok(false)
    }
    /// Clock is sampled exactly once AFTER promotion (the legacy puts→clock
    /// order), never at start/per step. Callers use an infallible native clock.
    /// Promotion validates all staged objects/collisions before any insert.
    pub(crate) fn finish_with_clock(
        &mut self,
        store: &MemoryChunkStore,
        clock: impl FnOnce() -> u64,
    ) -> Result<RootNode, ChunkError> {
        if self.failed || !self.ready {
            self.failed = true;
            return Err(invalid("candidate mutation is not ready"));
        }
        // A finish attempt is one-shot even when promotion fails. Promotion
        // consumes the private overlay, so retaining a ready cursor would let
        // a retry publish a root whose referenced chunks were never inserted.
        // The WASM owner retains the poisoned job only so it can be cancelled.
        self.failed = true;
        store.promote_verified_chunks(std::mem::take(&mut self.overlay))?;
        let root = self
            .root
            .as_mut()
            .ok_or_else(|| invalid("candidate mutation lost its root owner"))?;
        root.created_ms = clock();
        self.root
            .take()
            .ok_or_else(|| invalid("candidate mutation lost its root owner"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::ChunkStore;
    use crate::tree::{build_tree, update_tree};
    use std::cell::Cell;

    fn entry(path: &str, revision: u64) -> FileEntry {
        FileEntry::new(
            path.into(),
            hash_bytes(format!("{path}:{revision}").as_bytes()),
            revision,
            revision + 7,
        )
    }
    fn chunks(store: &MemoryChunkStore) -> Vec<(FileHash, Vec<u8>)> {
        let mut chunks = store.all_chunks();
        chunks.sort_by_key(|row| row.0);
        chunks
    }
    fn copy_store(source: &MemoryChunkStore) -> MemoryChunkStore {
        let target = MemoryChunkStore::new();
        for (hash, bytes) in source.all_chunks() {
            target.insert_chunk(hash, bytes);
        }
        target
    }
    fn drain(cursor: &mut CandidateMutationV1, store: &MemoryChunkStore, units: usize) -> usize {
        let resident = store.len();
        let mut completed = 0;
        let mut calls = 0;
        while !cursor.progress().done {
            let progress = cursor.step(store, units).unwrap();
            calls += 1;
            assert!(progress.units <= units);
            assert!(progress.done || progress.units > 0);
            assert_eq!(progress.completed, completed + progress.units);
            assert_eq!(store.len(), resident, "step published a resident object");
            completed = progress.completed;
            assert!(calls < 10_000_000, "cursor stopped making bounded progress");
        }
        completed
    }
    async fn parity(
        initial: Vec<FileEntry>,
        changed: Vec<FileEntry>,
        deleted: Vec<String>,
        units: usize,
    ) -> usize {
        let store = MemoryChunkStore::new();
        let root = build_tree(&store, initial, "vault", "device")
            .await
            .unwrap();
        let before = chunks(&store);
        let oracle_store = copy_store(&store);
        let mut expected = update_tree(&oracle_store, &root, &changed, &deleted)
            .await
            .unwrap();
        expected.created_ms = 42_424;
        let mut cursor = CandidateMutationV1::new(root.clone(), changed, deleted);
        let completed = drain(&mut cursor, &store, units);
        assert_eq!(chunks(&store), before, "ready state changed resident bytes");
        let staged: Vec<_> = cursor.overlay.keys().copied().collect();
        let clock_reads = Cell::new(0);
        let actual = cursor
            .finish_with_clock(&store, || {
                clock_reads.set(clock_reads.get() + 1);
                for hash in &staged {
                    assert!(store.get_chunk(hash).is_some(), "clock preceded promotion");
                }
                42_424
            })
            .unwrap();
        assert!(
            cursor.root.is_none(),
            "finish cloned instead of moving the root owner"
        );
        assert_eq!(clock_reads.get(), 1);
        assert_eq!(
            actual.serialize(),
            expected.serialize(),
            "root/history bytes differ"
        );
        assert_eq!(actual.hash(), expected.hash());
        assert_eq!(actual.parent_hash, Some(root.hash()));
        assert_eq!(actual.total_files, expected.total_files);
        assert_eq!(
            chunks(&store),
            chunks(&oracle_store),
            "exact staged chunks differ from legacy apply"
        );
        completed
    }

    async fn malformed_layout_parity(
        leaves: Vec<Vec<FileEntry>>,
        changed: Vec<FileEntry>,
    ) -> usize {
        let store = MemoryChunkStore::new();
        let total_files = leaves.iter().map(Vec::len).sum::<usize>() as u64;
        let mut children = Vec::new();
        for (index, entries) in leaves.into_iter().enumerate() {
            let bytes = LeafChunk { entries }.serialize();
            let hash = hash_bytes(&bytes);
            store.put(hash, bytes).await.unwrap();
            children.push((format!("wide/{index}"), hash));
        }
        let bytes = InternalNode::new(children).serialize();
        let hash = hash_bytes(&bytes);
        store.put(hash, bytes).await.unwrap();
        let root = RootNode {
            vault_id: "vault".into(),
            device_id: "device".into(),
            created_ms: 1,
            version: 1,
            children: vec![("wide/".into(), hash)],
            total_files,
            parent_hash: None,
        };
        let before = chunks(&store);
        let oracle_store = copy_store(&store);
        // An absent delete disables the ordered fixed-count probe without
        // changing the final path set, giving an exact oracle for the original
        // full-load/sort/rebuild mutation path even for malformed layouts.
        let mut oracle = CandidateMutationV1::new(
            root.clone(),
            changed.clone(),
            vec!["wide/\u{10ffff}-absent".into()],
        );
        drain(&mut oracle, &oracle_store, 17);
        let expected = oracle.finish_with_clock(&oracle_store, || 42_424).unwrap();
        let mut cursor = CandidateMutationV1::new(root, changed, Vec::new());
        let completed = drain(&mut cursor, &store, 17);
        assert_eq!(chunks(&store), before, "fallback published before finish");
        let actual = cursor.finish_with_clock(&store, || 42_424).unwrap();
        assert_eq!(actual.serialize(), expected.serialize());
        assert_eq!(actual.total_files, expected.total_files);
        assert_eq!(chunks(&store), chunks(&oracle_store));
        completed
    }

    #[test]
    fn cancellation_drains_large_prefix_and_root_assembly_one_record_per_unit() {
        const COUNT: usize = 25_000;
        let root = RootNode {
            vault_id: "vault".into(),
            device_id: "device".into(),
            created_ms: 1,
            version: 1,
            children: Vec::new(),
            total_files: 0,
            parent_hash: None,
        };
        let adds = (0..COUNT)
            .map(|index| entry(&format!("wide/{index:05}.md"), index as u64))
            .collect();
        let mut prefix = CandidateMutationV1::new(root.clone(), Vec::new(), Vec::new());
        prefix.active = Some(PrefixMutation::new("wide/".into(), adds, Vec::new(), None));
        let mut units = 0;
        while prefix.retire_one() {
            units += 1;
        }
        assert!(
            units >= COUNT,
            "prefix owner was bulk-dropped in one retirement unit"
        );

        let old = (0..COUNT)
            .map(|index| (format!("p-{index:05}/"), hash_bytes(&index.to_le_bytes())))
            .collect::<Vec<_>>();
        let mut assembly = CandidateMutationV1::new(root, Vec::new(), Vec::new());
        assembly.assembly = Some(RootAssembly {
            old: old.into_iter().peekable(),
            replacements: BTreeMap::new().into_iter().peekable(),
            children: Vec::new(),
            parent: IncrementalHasher::new(),
            retirement_phase: 0,
        });
        let mut units = 0;
        while assembly.retire_one() {
            units += 1;
        }
        assert!(
            units >= COUNT,
            "root assembly was bulk-dropped in one retirement unit"
        );
    }

    #[test]
    fn sort_is_stable_for_all_small_lengths() {
        for len in 0..75 {
            let values: Vec<_> = (0..len)
                .map(|index| entry(&format!("{:02}.md", (index * 19) % 11), index as u64))
                .collect();
            let mut expected = values.clone();
            expected.sort();
            let mut sort = Sort::new(values);
            let mut units = 0;
            while sort.tick() {
                units += 1;
                assert!(units < 20_000);
            }
            assert_eq!(sort.finish(), expected);
        }
    }

    #[tokio::test]
    async fn exact_legacy_parity_for_duplicates_precedence_and_prefix_changes() {
        for units in [1, 7, 256] {
            parity(
                vec![
                    entry("a.md", 1),
                    entry("one/a.md", 1),
                    entry("last/only.md", 1),
                ],
                vec![
                    entry("one/a.md", 3),
                    entry("z/new.md", 4),
                    entry("one/a.md", 5),
                    entry("é/中.md", 6),
                ],
                vec![
                    "one/a.md".into(),
                    "last/only.md".into(),
                    "last/only.md".into(),
                    "missing.md".into(),
                ],
                units,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn exact_legacy_parity_for_empty_and_nonempty_noop_history() {
        for units in [1, 256] {
            parity(Vec::new(), Vec::new(), Vec::new(), units).await;
            parity(vec![entry("one.md", 1)], Vec::new(), Vec::new(), units).await;
            parity(
                vec![entry("one.md", 1)],
                vec![entry("one.md", 1)],
                Vec::new(),
                units,
            )
            .await;
            parity(
                vec![entry("one.md", 1)],
                Vec::new(),
                vec!["absent.md".into()],
                units,
            )
            .await;
            parity(Vec::new(), vec![entry("a/new.md", 7)], Vec::new(), units).await;
            parity(
                vec![entry("last.md", 1)],
                Vec::new(),
                vec!["last.md".into()],
                units,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn wide_prefix_over_ten_leaves_keeps_exact_chunk_labels_and_bytes() {
        const COUNT: usize = 25_000;
        let initial: Vec<_> = (0..COUNT)
            .map(|index| entry(&format!("wide/{index:05}.md"), index as u64))
            .collect();
        let changed = vec![
            entry("wide/00001.md", 99_999),
            entry("wide/09999.md", 88_888),
            entry("other/new.md", 77_777),
        ];
        let deleted = vec![
            "wide/00000.md".into(),
            "wide/01000.md".into(),
            "wide/24999.md".into(),
        ];
        let completed = parity(initial, changed, deleted, 256).await;
        assert!(
            completed <= COUNT * 8 + 4_096,
            "wide-prefix mutation did not retain linear native work: {completed} units"
        );
    }

    #[tokio::test]
    async fn repeated_ordered_existing_updates_reuse_untouched_wide_leaves() {
        const COUNT: usize = 25_000;
        const BATCH: usize = 256;
        let path = |index: usize| format!("notes/p-{:03}/n-{index:05}.md", index / BATCH);
        let store = MemoryChunkStore::new();
        let initial: Vec<_> = (0..COUNT)
            .map(|index| entry(&path(index), index as u64))
            .collect();
        let mut root = build_tree(&store, initial, "vault", "device")
            .await
            .unwrap();
        let mut completed = 0usize;
        for first in (0..COUNT).step_by(BATCH) {
            let changed = (first..(first + BATCH).min(COUNT))
                .map(|index| entry(&path(index), 100_000 + index as u64))
                .collect();
            let mut cursor = CandidateMutationV1::new(root, changed, Vec::new());
            completed += drain(&mut cursor, &store, 256);
            root = cursor.finish_with_clock(&store, || 42_424).unwrap();
        }

        let expected_store = MemoryChunkStore::new();
        let expected = build_tree(
            &expected_store,
            (0..COUNT)
                .map(|index| entry(&path(index), 100_000 + index as u64))
                .collect(),
            "vault",
            "device",
        )
        .await
        .unwrap();
        assert_eq!(root.children, expected.children);
        assert_eq!(root.hash(), expected.hash());
        assert_eq!(root.total_files, COUNT as u64);
        assert!(
            completed <= 350_000,
            "ordered existing updates revisited the full prefix: {completed} units"
        );
    }

    #[test]
    fn canonical_shape_validation_returns_after_one_leaf_per_external_step() {
        const LEAVES: usize = 65;
        let store = MemoryChunkStore::new();
        let mut hashes = Vec::with_capacity(LEAVES);
        for leaf_index in 0..LEAVES {
            let first = leaf_index * TARGET_CHUNK_ENTRIES;
            let entries = (first..first + TARGET_CHUNK_ENTRIES)
                .map(|index| entry(&format!("wide/{index:05}.md"), index as u64))
                .collect();
            let bytes = LeafChunk::new(entries).serialize();
            let hash = hash_bytes(&bytes);
            store.insert_chunk(hash, bytes);
            hashes.push(hash);
        }
        let root = RootNode {
            vault_id: "vault".into(),
            device_id: "device".into(),
            created_ms: 1,
            version: 1,
            children: Vec::new(),
            total_files: (LEAVES * TARGET_CHUNK_ENTRIES) as u64,
            parent_hash: None,
        };
        let mut prefix = PrefixMutation::new(
            "wide/".into(),
            vec![entry("wide/00001.md", 99_999)],
            Vec::new(),
            None,
        );
        prefix.phase = PrefixPhase::AppendProbe(AppendProbe::ValidateShape {
            hashes,
            index: 0,
            previous_last: None,
        });
        let mut cursor = CandidateMutationV1::new(root, Vec::new(), Vec::new());
        cursor.grouping = false;
        cursor.active = Some(prefix);

        for expected in 1..=2 {
            let before = cursor.progress().completed;
            let progress = cursor.step(&store, 64).unwrap();
            assert_eq!(progress.units, 1);
            assert_eq!(progress.completed, before + 1);
            assert!(!progress.done);
            let PrefixPhase::AppendProbe(AppendProbe::ValidateShape { index, hashes, .. }) =
                &cursor.active.as_ref().unwrap().phase
            else {
                panic!("shape validation escaped its external-step barrier");
            };
            assert_eq!(*index, expected);
            assert_eq!(hashes.len(), LEAVES);
        }
    }

    #[tokio::test]
    async fn ordered_fixed_count_probe_falls_back_when_any_path_is_new() {
        let initial: Vec<_> = (0..2_500)
            .map(|index| entry(&format!("wide/{index:05}.md"), index))
            .collect();
        let changed = vec![
            entry("wide/00001.md", 90_001),
            entry("wide/00500-new.md", 90_002),
            entry("wide/01234.md", 90_003),
        ];
        parity(initial, changed, Vec::new(), 17).await;
    }

    #[tokio::test]
    async fn malformed_wide_child_labels_fall_back_to_exact_legacy_ordering() {
        let store = MemoryChunkStore::new();
        let mut children = Vec::new();
        for index in 0..13 {
            let leaf = LeafChunk::new(vec![entry(&format!("wide/{index:05}.md"), index)]);
            let bytes = leaf.serialize();
            let hash = hash_bytes(&bytes);
            store.put(hash, bytes).await.unwrap();
            // Duplicate and non-numeric labels deliberately defeat the
            // canonical numeric permutation without losing any child owner.
            let label = match index {
                0 | 1 => "wide/0".to_string(),
                2 => "wide/not-a-number".to_string(),
                _ => format!("wide/{index}"),
            };
            children.push((label, hash));
        }
        let node = InternalNode::new(children);
        let bytes = node.serialize();
        let hash = hash_bytes(&bytes);
        store.put(hash, bytes).await.unwrap();
        let root = RootNode {
            vault_id: "vault".into(),
            device_id: "device".into(),
            created_ms: 1,
            version: 1,
            children: vec![("wide/".into(), hash)],
            total_files: 13,
            parent_hash: None,
        };
        let changed = vec![entry("wide/00007.md", 99_999)];
        let oracle_store = copy_store(&store);
        let mut expected = update_tree(&oracle_store, &root, &changed, &[])
            .await
            .unwrap();
        expected.created_ms = 42_424;
        let mut cursor = CandidateMutationV1::new(root, changed, Vec::new());
        drain(&mut cursor, &store, 1);
        let actual = cursor.finish_with_clock(&store, || 42_424).unwrap();
        assert_eq!(actual.serialize(), expected.serialize());
        assert_eq!(chunks(&store), chunks(&oracle_store));
    }

    #[tokio::test]
    async fn malformed_leaf_boundaries_fail_safe_to_exact_legacy_rebuild() {
        let range = |start: usize, end: usize| {
            (start..end)
                .map(|index| entry(&format!("wide/{index:05}.md"), index as u64))
                .collect::<Vec<_>>()
        };

        let mut unsorted_tail = range(1_000, 1_003);
        unsorted_tail.swap(0, 2);
        malformed_layout_parity(
            vec![range(0, 1_000), unsorted_tail],
            vec![entry("wide/02000.md", 90_001)],
        )
        .await;

        malformed_layout_parity(
            vec![range(1_000, 2_000), range(0, 1_000)],
            vec![entry("wide/00001.md", 90_002)],
        )
        .await;

        let mut overlapping = range(999, 1_002);
        overlapping.insert(0, entry("wide/00999.md", 77_777));
        malformed_layout_parity(
            vec![range(0, 1_000), overlapping],
            vec![entry("wide/01000.md", 90_003)],
        )
        .await;

        malformed_layout_parity(
            vec![
                range(0, 1_000),
                vec![entry("wide/01000.md", 1), entry("z/cross-prefix.md", 2)],
            ],
            vec![entry("wide/01000.md", 90_004)],
        )
        .await;

        malformed_layout_parity(
            vec![range(0, 999), range(999, 1_002)],
            vec![entry("wide/01000.md", 90_005)],
        )
        .await;
    }

    #[tokio::test]
    async fn seeded_operation_corpora_match_legacy() {
        let mut state = 0x89ab_cdefu64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for round in 0..24 {
            let initial: Vec<_> = (0..80)
                .map(|index| entry(&format!("d{}/{index:03}.md", index % 4), 1))
                .collect();
            let changed: Vec<_> = (0..37)
                .map(|index| {
                    let path = next() % 100;
                    entry(&format!("d{}/{path:03}.md", path % 4), index + 2)
                })
                .collect();
            let deleted = (0..29)
                .map(|_| {
                    let path = next() % 100;
                    format!("d{}/{path:03}.md", path % 4)
                })
                .collect();
            parity(
                initial,
                changed,
                deleted,
                if round % 2 == 0 { 1 } else { 17 },
            )
            .await;
        }
    }

    #[tokio::test]
    async fn work_and_output_do_not_depend_on_step_budget() {
        let initial: Vec<_> = (0..2_507)
            .map(|index| entry(&format!("dir/{index:05}.md"), index))
            .collect();
        let original = MemoryChunkStore::new();
        let root = build_tree(&original, initial, "vault", "device")
            .await
            .unwrap();
        let mut expected = None;
        for budget in [1, 2, 17, 256, usize::MAX] {
            let store = copy_store(&original);
            let mut cursor = CandidateMutationV1::new(
                root.clone(),
                vec![entry("dir/00011.md", 8_888), entry("z/new.md", 7_777)],
                vec!["dir/00222.md".into()],
            );
            let units = drain(&mut cursor, &store, budget);
            let progress = cursor.progress();
            assert_eq!(cursor.step(&store, budget).unwrap().units, 0);
            let result = cursor.finish_with_clock(&store, || 17).unwrap();
            let actual = (
                units,
                progress.staged_chunks,
                progress.staged_bytes,
                result.serialize(),
                chunks(&store),
            );
            if let Some(expected) = &expected {
                assert_eq!(&actual, expected);
            } else {
                expected = Some(actual);
            }
        }
    }

    #[test]
    fn staging_reuses_identical_bytes_and_refuses_a_collision() {
        let mut overlay = HashMap::new();
        let mut bytes = 0;
        let value = b"staged bytes".to_vec();
        let hash = hash_bytes(&value);
        assert_eq!(
            stage(&mut overlay, &mut bytes, value.clone()).unwrap(),
            hash
        );
        assert_eq!(
            stage(&mut overlay, &mut bytes, value.clone()).unwrap(),
            hash
        );
        assert_eq!(overlay.len(), 1);
        assert_eq!(bytes, value.len());
        overlay.insert(hash, b"corruption under existing hash".to_vec());
        let before = overlay.clone();
        assert!(stage(&mut overlay, &mut bytes, value.clone()).is_err());
        assert_eq!(overlay, before);
        assert_eq!(bytes, value.len());
    }

    #[tokio::test]
    async fn cancellation_and_incomplete_finish_never_publish() {
        let store = MemoryChunkStore::new();
        let initial: Vec<_> = (0..2_001)
            .map(|index| entry(&format!("wide/{index:05}.md"), index))
            .collect();
        let root = build_tree(&store, initial, "vault", "device")
            .await
            .unwrap();
        let before = chunks(&store);
        for until in [0, 1, 17, 1_000, 10_000, 60_000] {
            let mut cursor = CandidateMutationV1::new(
                root.clone(),
                vec![entry("wide/00000.md", 9_999)],
                Vec::new(),
            );
            for _ in 0..until {
                if cursor.progress().done {
                    break;
                }
                cursor.step(&store, 1).unwrap();
            }
            drop(cursor);
            assert_eq!(chunks(&store), before);
        }
        let clock = Cell::new(0);
        let mut cursor = CandidateMutationV1::new(root, vec![entry("new.md", 2)], Vec::new());
        assert!(cursor.step(&store, 0).is_err());
        assert_eq!(cursor.progress().completed, 0);
        assert!(cursor
            .finish_with_clock(&store, || {
                clock.set(1);
                9
            })
            .is_err());
        assert_eq!(clock.get(), 0);
        assert_eq!(chunks(&store), before);
    }

    #[tokio::test]
    async fn failed_load_poisons_cursor_and_preserves_every_resident_object() {
        for missing in [true, false] {
            let store = MemoryChunkStore::new();
            let root = build_tree(&store, vec![entry("a.md", 1)], "vault", "device")
                .await
                .unwrap();
            let hash = root.children[0].1;
            if missing {
                store.delete(&hash).await.unwrap();
            } else {
                store.insert_chunk(hash, b"corrupted bytes".to_vec());
            }
            let before = chunks(&store);
            let mut cursor = CandidateMutationV1::new(root, vec![entry("a.md", 2)], Vec::new());
            let error = loop {
                match cursor.step(&store, 1) {
                    Err(error) => break error,
                    Ok(progress) => assert!(!progress.done),
                }
            };
            assert!(matches!(
                error,
                ChunkError::NotFound(_) | ChunkError::Deserialize(_)
            ));
            assert!(!cursor.progress().done);
            assert!(cursor.step(&store, 1).is_err());
            assert!(cursor
                .finish_with_clock(&store, || panic!("failed cursor sampled clock"))
                .is_err());
            assert_eq!(chunks(&store), before);
        }
    }

    #[tokio::test]
    async fn final_hash_or_resident_collision_failure_is_atomic_and_does_not_sample_clock() {
        for corrupt_staged in [true, false] {
            let store = MemoryChunkStore::new();
            let root = build_tree(&store, vec![entry("a.md", 1)], "vault", "device")
                .await
                .unwrap();
            let mut cursor = CandidateMutationV1::new(
                root,
                vec![entry("a.md", 2), entry("new/b.md", 3)],
                Vec::new(),
            );
            drain(&mut cursor, &store, 17);
            assert!(cursor.overlay.len() >= 2);
            let hash = *cursor.overlay.keys().next().unwrap();
            if corrupt_staged {
                cursor.overlay.insert(hash, b"wrong staged bytes".to_vec());
            } else {
                store.insert_chunk(hash, b"wrong resident bytes".to_vec());
            }
            let before = chunks(&store);
            assert!(cursor
                .finish_with_clock(&store, || panic!("failed promotion sampled clock"))
                .is_err());
            assert_eq!(
                chunks(&store),
                before,
                "promotion partly inserted before detecting failure"
            );
            assert!(cursor
                .finish_with_clock(&store, || panic!("poisoned cursor sampled clock"))
                .is_err());
            assert_eq!(
                chunks(&store),
                before,
                "a repeated finish published an overlay lost by the first failure"
            );
        }
    }
}
