//! Owner-local continuation for the *existing* v1 FlatBuffer codec.
//!
//! The owner must pin the source root for this cursor's entire lifetime. Plan
//! neither clones the root nor allocates output/offset buffers. Build starts
//! only after the caller admits the reported arena and offsets. Native reserve,
//! allocator bookkeeping and drop remain atomic; this is not an RSS or time
//! guarantee. Each string is capped at 64 KiB (a new export-API restriction,
//! not a change to the legacy codec). Dedicated root steps have independent
//! operation and charged-byte guards. Cheap primitives carry a 256-byte floor;
//! the largest atomic primitive is one capped string plus schema allowance.

use crate::chunk::{ChunkError, RootNode};
use crate::hash::{FileHash, IncrementalHasher};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use sync_schema::sync_chunk;

pub(crate) const ROOT_EXPORT_PART_BYTES: usize = 64 * 1024;
pub(crate) const ROOT_EXPORT_MAX_ATOMIC_BYTES: usize = ROOT_EXPORT_PART_BYTES + 128;
pub(crate) const ROOT_EXPORT_MAX_STEP_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const ROOT_EXPORT_MAX_STEP_UNITS: usize = 4096;
const ROOT_EXPORT_MIN_CHARGE_BYTES: usize = 256;
// Below the pinned FlatBuffers allocator's 2 GiB ceiling; also fits this ABI.
const MAX_ARENA_BYTES: usize = (1usize << 31) - 8;

fn invalid(message: &str) -> ChunkError {
    ChunkError::Deserialize(message.into())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    PlanVault,
    PlanDevice,
    PlanPrefix,
    PlanHash,
    PlanFinish,
    Planned,
    Fill,
    Vault,
    Device,
    RootHash,
    ParentHash,
    Prefix,
    Child,
    VectorStart,
    VectorRow,
    Finish,
    Built,
    Sealed,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RootExportWorkset {
    pub max_length: usize,
    pub offset_bytes: usize,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RootExportProgress {
    pub done: bool,
    pub units: usize,
    pub bytes: usize,
    pub completed: u64,
    pub processed: u64,
}

pub(crate) struct RootExportV1 {
    phase: Phase,
    next_child: usize,
    child_count: usize,
    completed: u64,
    processed: u64,
    max_length: usize,
    arena_limit: usize,
    hasher: Option<IncrementalHasher>,
    hash: Option<FileHash>,
    arena: Vec<u8>,
    offsets: Vec<u32>,
    builder: Option<FlatBufferBuilder<'static>>,
    vault: u32,
    device: u32,
    root_hash: u32,
    parent_hash: Option<u32>,
    prefix: u32,
    output_start: usize,
    read_offset: usize,
}

impl RootExportV1 {
    #[cfg(test)]
    pub fn new() -> Self {
        Self::with_arena_limit(MAX_ARENA_BYTES).expect("codec maximum accepts the root header")
    }

    pub fn with_arena_limit(arena_limit: usize) -> Result<Self, ChunkError> {
        if arena_limit < 512 {
            return Err(invalid("invalid root export arena limit"));
        }
        let arena_limit = arena_limit.min(MAX_ARENA_BYTES);
        Ok(Self {
            phase: Phase::PlanVault,
            next_child: 0,
            child_count: 0,
            completed: 0,
            processed: 0,
            // A schema upper bound, not the legacy builder's initial capacity:
            // <=512 fixed bytes for two string headers/padding, hash vectors,
            // the root/envelope fields/vtables, vector header and final align.
            // Each child adds prefix.len()+128: string <=len+8, hash <=39,
            // ChildRef <=24, its final vector offset 4, with spare alignment.
            // We do not rely on vtable dedup to satisfy this bound.
            max_length: 512,
            arena_limit,
            hasher: Some(IncrementalHasher::new()),
            hash: None,
            arena: Vec::new(),
            offsets: Vec::new(),
            builder: None,
            vault: 0,
            device: 0,
            root_hash: 0,
            parent_hash: None,
            prefix: 0,
            output_start: 0,
            read_offset: 0,
        })
    }

    pub fn progress(&self, units: usize, bytes: usize) -> RootExportProgress {
        RootExportProgress {
            done: matches!(self.phase, Phase::Planned | Phase::Built | Phase::Sealed),
            units,
            bytes,
            completed: self.completed,
            processed: self.processed,
        }
    }

    pub fn sealed(&self) -> bool {
        self.phase == Phase::Sealed
    }

    pub fn workset(&self) -> Result<RootExportWorkset, ChunkError> {
        if self.phase != Phase::Planned {
            return Err(invalid("root export Plan is not ready for admission"));
        }
        Ok(RootExportWorkset {
            max_length: self.max_length,
            offset_bytes: self.child_count * std::mem::size_of::<u32>(),
        })
    }

    pub fn start_build(&mut self) -> Result<(), ChunkError> {
        let workset = self.workset()?;
        // Reserve without filling. Failure leaves this Planned owner intact;
        // partially successful local reservations are dropped, never exposed.
        let mut arena = Vec::new();
        arena
            .try_reserve_exact(workset.max_length)
            .map_err(|_| invalid("root export arena reservation failed"))?;
        let mut offsets = Vec::new();
        offsets
            .try_reserve_exact(self.child_count)
            .map_err(|_| invalid("root export offsets reservation failed"))?;
        if arena.capacity() > workset.max_length || offsets.capacity() > self.child_count {
            return Err(invalid("root export reservation exceeded admission"));
        }
        self.arena = arena;
        self.offsets = offsets;
        self.next_child = 0;
        self.completed = 0;
        self.processed = 0;
        self.phase = Phase::Fill;
        Ok(())
    }

    fn charge(&mut self, bytes: usize) -> Result<(), ChunkError> {
        let next = self
            .max_length
            .checked_add(bytes)
            .filter(|value| *value <= self.arena_limit)
            .ok_or_else(|| invalid("root export arena exceeds the codec/ABI limit"))?;
        self.max_length = next;
        Ok(())
    }

    fn check_string(value: &str) -> Result<(), ChunkError> {
        if value.len() > ROOT_EXPORT_PART_BYTES {
            return Err(invalid("root export string exceeds 64 KiB"));
        }
        Ok(())
    }

    fn room(&self, bytes: usize) -> Result<(), ChunkError> {
        let used = self.builder.as_ref().unwrap().unfinished_data().len();
        if used
            .checked_add(bytes)
            .is_none_or(|value| value > self.max_length)
        {
            return Err(invalid("root export schema exceeded the admitted arena"));
        }
        Ok(())
    }

    pub fn step(
        &mut self,
        root: &RootNode,
        max_units: usize,
        max_bytes: usize,
    ) -> Result<RootExportProgress, ChunkError> {
        if max_units == 0 || max_units > ROOT_EXPORT_MAX_STEP_UNITS {
            return Err(invalid("invalid root export step budget"));
        }
        if max_bytes == 0 || max_bytes > ROOT_EXPORT_MAX_STEP_BYTES {
            return Err(invalid("invalid root export byte budget"));
        }
        if self.sealed() {
            return Err(invalid("root export is sealed for page reads"));
        }
        let mut units = 0;
        let mut bytes = 0usize;
        while units < max_units && !self.progress(0, 0).done {
            let charge = self.next_charge(root)?;
            if charge == 0 || charge > ROOT_EXPORT_MAX_ATOMIC_BYTES {
                return Err(invalid("root export primitive exceeds its atomic bound"));
            }
            let next_bytes = bytes
                .checked_add(charge)
                .ok_or_else(|| invalid("root export step charge overflow"))?;
            // A budget smaller than one atomic primitive still makes progress,
            // but that primitive is the only work performed by this call.
            if units > 0 && next_bytes > max_bytes {
                break;
            }
            self.tick(root)?;
            units += 1;
            bytes = next_bytes;
            self.completed = self
                .completed
                .checked_add(1)
                .ok_or_else(|| invalid("root export completed-unit overflow"))?;
            self.processed = self
                .processed
                .checked_add(charge as u64)
                .ok_or_else(|| invalid("root export processed-byte overflow"))?;
            if bytes >= max_bytes {
                break;
            }
        }
        Ok(self.progress(units, bytes))
    }

    fn next_charge(&self, root: &RootNode) -> Result<usize, ChunkError> {
        let charge = match self.phase {
            Phase::PlanVault => {
                Self::check_string(&root.vault_id)?;
                root.vault_id.len()
            }
            Phase::PlanDevice => {
                Self::check_string(&root.device_id)?;
                root.device_id.len()
            }
            Phase::PlanPrefix => {
                let prefix = &root.children[self.next_child].0;
                Self::check_string(prefix)?;
                prefix
                    .len()
                    .checked_add(128)
                    .ok_or_else(|| invalid("root export prefix charge overflow"))?
            }
            Phase::PlanHash => ROOT_EXPORT_MIN_CHARGE_BYTES,
            Phase::PlanFinish => 2048,
            Phase::Fill => (self.max_length - self.arena.len()).min(ROOT_EXPORT_PART_BYTES),
            Phase::Vault => {
                Self::check_string(&root.vault_id)?;
                root.vault_id.len() + 8
            }
            Phase::Device => {
                Self::check_string(&root.device_id)?;
                root.device_id.len() + 8
            }
            Phase::RootHash | Phase::ParentHash | Phase::VectorStart => {
                ROOT_EXPORT_MIN_CHARGE_BYTES
            }
            Phase::Prefix => {
                let prefix = &root.children[self.next_child].0;
                Self::check_string(prefix)?;
                prefix.len() + 8
            }
            Phase::Child => 128,
            Phase::VectorRow => ROOT_EXPORT_MIN_CHARGE_BYTES,
            Phase::Finish => 512,
            Phase::Planned | Phase::Built | Phase::Sealed => {
                return Err(invalid("root export phase is not executable"))
            }
        };
        Ok(charge.max(ROOT_EXPORT_MIN_CHARGE_BYTES))
    }

    fn tick(&mut self, root: &RootNode) -> Result<(), ChunkError> {
        match self.phase {
            Phase::PlanVault => {
                Self::check_string(&root.vault_id)?;
                self.charge(root.vault_id.len())?;
                self.child_count = root.children.len();
                // Check vector length/bytes before copying or allocating rows.
                self.child_count
                    .checked_mul(128)
                    .filter(|value| *value <= MAX_ARENA_BYTES)
                    .ok_or_else(|| invalid("root export child count exceeds the codec limit"))?;
                self.phase = Phase::PlanDevice;
            }
            Phase::PlanDevice => {
                Self::check_string(&root.device_id)?;
                self.charge(root.device_id.len())?;
                self.phase = if self.child_count == 0 {
                    Phase::PlanFinish
                } else {
                    Phase::PlanPrefix
                };
            }
            Phase::PlanPrefix => {
                let (prefix, _) = &root.children[self.next_child];
                Self::check_string(prefix)?;
                self.charge(prefix.len() + 128)?;
                self.hasher.as_mut().unwrap().update_str(prefix);
                self.phase = Phase::PlanHash;
            }
            Phase::PlanHash => {
                self.hasher
                    .as_mut()
                    .unwrap()
                    .update(&root.children[self.next_child].1);
                self.next_child += 1;
                self.phase = if self.next_child == self.child_count {
                    Phase::PlanFinish
                } else {
                    Phase::PlanPrefix
                };
            }
            Phase::PlanFinish => {
                self.hash = Some(self.hasher.take().unwrap().finalize());
                self.phase = Phase::Planned;
            }
            Phase::Fill => {
                let next = (self.arena.len() + ROOT_EXPORT_PART_BYTES).min(self.max_length);
                self.arena.resize(next, 0);
                if next == self.max_length {
                    self.builder =
                        Some(FlatBufferBuilder::from_vec(std::mem::take(&mut self.arena)));
                    self.phase = Phase::Vault;
                }
            }
            Phase::Vault => {
                Self::check_string(&root.vault_id)?;
                self.room(root.vault_id.len() + 8)?;
                self.vault = self
                    .builder
                    .as_mut()
                    .unwrap()
                    .create_string(&root.vault_id)
                    .value();
                self.phase = Phase::Device;
            }
            Phase::Device => {
                Self::check_string(&root.device_id)?;
                self.room(root.device_id.len() + 8)?;
                self.device = self
                    .builder
                    .as_mut()
                    .unwrap()
                    .create_string(&root.device_id)
                    .value();
                self.phase = Phase::RootHash;
            }
            Phase::RootHash => {
                self.room(40)?;
                self.root_hash = self
                    .builder
                    .as_mut()
                    .unwrap()
                    .create_vector(&self.hash.unwrap())
                    .value();
                self.phase = Phase::ParentHash;
            }
            Phase::ParentHash => {
                self.room(40)?;
                self.parent_hash = root
                    .parent_hash
                    .as_ref()
                    .map(|hash| self.builder.as_mut().unwrap().create_vector(hash).value());
                self.phase = if self.child_count == 0 {
                    Phase::VectorStart
                } else {
                    Phase::Prefix
                };
            }
            Phase::Prefix => {
                let prefix = &root.children[self.next_child].0;
                Self::check_string(prefix)?;
                self.room(prefix.len() + 8)?;
                self.prefix = self.builder.as_mut().unwrap().create_string(prefix).value();
                self.phase = Phase::Child;
            }
            Phase::Child => {
                self.room(72)?;
                let builder = self.builder.as_mut().unwrap();
                let hash = builder.create_vector(&root.children[self.next_child].1);
                let child = sync_chunk::ChildRef::create(
                    builder,
                    &sync_chunk::ChildRefArgs {
                        prefix: Some(WIPOffset::new(self.prefix)),
                        hash: Some(hash),
                    },
                );
                // Capacity was admitted/reserved before any build work.
                self.offsets.push(child.value());
                self.next_child += 1;
                self.phase = if self.next_child == self.child_count {
                    Phase::VectorStart
                } else {
                    Phase::Prefix
                };
            }
            Phase::VectorStart => {
                self.room(self.child_count * 4 + 8)?;
                self.builder
                    .as_mut()
                    .unwrap()
                    .start_vector::<WIPOffset<sync_chunk::ChildRef>>(self.child_count);
                self.next_child = self.child_count;
                self.phase = if self.next_child == 0 {
                    Phase::Finish
                } else {
                    Phase::VectorRow
                };
            }
            Phase::VectorRow => {
                self.next_child -= 1;
                self.builder
                    .as_mut()
                    .unwrap()
                    .push(WIPOffset::<sync_chunk::ChildRef>::new(
                        self.offsets[self.next_child],
                    ));
                if self.next_child == 0 {
                    self.phase = Phase::Finish;
                }
            }
            Phase::Finish => {
                self.room(256)?;
                let builder = self.builder.as_mut().unwrap();
                let children =
                    builder.end_vector::<WIPOffset<sync_chunk::ChildRef>>(self.child_count);
                let root = sync_chunk::RootNode::create(
                    builder,
                    &sync_chunk::RootNodeArgs {
                        version: root.version,
                        vault_id: Some(WIPOffset::new(self.vault)),
                        created_ms: root.created_ms,
                        total_files: root.total_files,
                        children: Some(WIPOffset::new(children.value())),
                        root_hash: Some(WIPOffset::new(self.root_hash)),
                        parent_hash: self.parent_hash.map(WIPOffset::new),
                        device_id: Some(WIPOffset::new(self.device)),
                    },
                );
                let envelope = sync_chunk::ChunkEnvelope::create(
                    builder,
                    &sync_chunk::ChunkEnvelopeArgs {
                        node_type: sync_chunk::NodeType::RootNode,
                        node: Some(root.as_union_value()),
                    },
                );
                builder.finish(envelope, None);
                let (arena, start) = self.builder.take().unwrap().collapse();
                if arena.len() != self.max_length || arena.capacity() > self.max_length {
                    return Err(invalid("root export builder grew beyond admission"));
                }
                self.arena = arena;
                self.output_start = start;
                self.phase = Phase::Built;
            }
            Phase::Planned | Phase::Built | Phase::Sealed => unreachable!(),
        }
        Ok(())
    }

    /// Seal without copying the complete output or changing the tree.
    pub fn seal(&mut self) -> Result<(usize, FileHash), ChunkError> {
        if !matches!(self.phase, Phase::Built | Phase::Sealed) {
            return Err(invalid("root export Build is not ready"));
        }
        self.phase = Phase::Sealed;
        Ok((self.arena.len() - self.output_start, self.hash.unwrap()))
    }

    pub fn read(
        &mut self,
        expected_offset: usize,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ChunkError> {
        if !self.sealed()
            || expected_offset != self.read_offset
            || !(1..=ROOT_EXPORT_PART_BYTES).contains(&max_bytes)
        {
            return Err(invalid("invalid root export page read"));
        }
        let length = self.arena.len() - self.output_start;
        let end = (self.read_offset + max_bytes).min(length);
        let bytes =
            self.arena[self.output_start + self.read_offset..self.output_start + end].to_vec();
        self.read_offset = end;
        Ok(bytes)
    }

    pub fn at_eof(&self) -> bool {
        self.sealed() && self.read_offset == self.arena.len() - self.output_start
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::hash_bytes;

    fn root(count: usize) -> RootNode {
        RootNode {
            vault_id: "vault/\u{1f642}\0".into(),
            device_id: "device\0é".into(),
            created_ms: 123456,
            version: 1,
            total_files: count as u64,
            parent_hash: Some(hash_bytes(b"parent")),
            children: (0..count)
                .map(|index| {
                    (
                        format!("folder/{:05}/", count - index),
                        hash_bytes(&index.to_le_bytes()),
                    )
                })
                .collect(),
        }
    }

    fn drain(cursor: &mut RootExportV1, root: &RootNode, budget: usize) -> usize {
        let mut total = 0;
        loop {
            let before = cursor.progress(0, 0).completed;
            let progress = cursor
                .step(root, budget, ROOT_EXPORT_MAX_STEP_BYTES)
                .unwrap();
            assert!(progress.units <= budget);
            assert_eq!(progress.completed, before + progress.units as u64);
            assert!(progress.bytes > 0);
            assert!(progress.processed >= progress.bytes as u64);
            assert!(progress.done || progress.units > 0);
            total += progress.units;
            if progress.done {
                return total;
            }
        }
    }

    fn export(root: &RootNode, budget: usize, page: usize) -> Vec<u8> {
        let mut cursor = RootExportV1::new();
        assert_eq!(cursor.arena.capacity(), 0);
        assert_eq!(cursor.offsets.capacity(), 0);
        assert!(cursor.hash.is_none());
        assert_eq!(
            drain(&mut cursor, root, budget),
            3 + 2 * root.children.len()
        );
        assert_eq!(cursor.arena.capacity(), 0);
        assert_eq!(cursor.offsets.capacity(), 0);
        assert!(cursor.builder.is_none());
        let workset = cursor.workset().unwrap();
        assert_eq!(workset.offset_bytes, root.children.len() * 4);
        cursor.start_build().unwrap();
        assert_eq!(cursor.arena.len(), 0);
        assert_eq!(cursor.offsets.len(), 0);
        assert_eq!(cursor.progress(0, 0).completed, 0);
        let total = drain(&mut cursor, root, budget);
        assert_eq!(
            total,
            workset.max_length.div_ceil(ROOT_EXPORT_PART_BYTES) + 3 * root.children.len() + 6
        );
        assert_eq!(cursor.arena.len(), workset.max_length);
        assert!(cursor.arena.capacity() <= workset.max_length);
        assert!(cursor.offsets.capacity() * 4 <= workset.offset_bytes);
        let (length, hash) = cursor.seal().unwrap();
        assert_eq!(hash, root.hash());
        assert!(length <= workset.max_length);
        let mut output = Vec::new();
        while output.len() < length {
            let part = cursor.read(output.len(), page).unwrap();
            assert!(part.len() <= page);
            output.extend_from_slice(&part);
        }
        assert!(cursor.at_eof());
        assert!(cursor.read(length, page).unwrap().is_empty());
        output
    }

    #[test]
    fn root_export_v1_differential_legacy_bytes_and_metadata() {
        for count in [0, 1, 2, 31, 256] {
            for metadata in 0..3 {
                let mut value = root(count);
                if metadata == 0 {
                    value.vault_id.clear();
                    value.device_id.clear();
                    value.created_ms = 0;
                    value.total_files = 0;
                    value.parent_hash = None;
                } else if metadata == 2 {
                    value.created_ms = u64::MAX;
                    value.total_files = u64::MAX;
                }
                // Deliberately preserve duplicates and noncanonical order.
                if count > 1 {
                    value.children[1].0 = value.children[0].0.clone();
                }
                let expected = value.serialize();
                for budget in [1, 7, 256] {
                    assert_eq!(export(&value, budget, 63 * 1024), expected);
                }
            }
        }
    }

    #[test]
    fn root_export_v1_alignment_unicode_and_maximum_strings() {
        for length in (0..=32).chain([63, 64, 127, 128, 1023, ROOT_EXPORT_PART_BYTES]) {
            let mut value = root(3);
            value.vault_id = "v".repeat(length);
            value.device_id = "d".repeat(length);
            value.children[0].0 = "é".repeat(length / 2);
            value.children[1].0 = "\0".repeat(length);
            value.children[2].0 = "x".repeat(length);
            assert_eq!(export(&value, 1, ROOT_EXPORT_PART_BYTES), value.serialize());
        }
    }

    #[test]
    fn root_export_v1_wide_25000_has_exact_work_and_no_hidden_vector_copy() {
        let value = root(25_000);
        let expected = value.serialize();
        assert!(expected.len() > ROOT_EXPORT_PART_BYTES);
        assert_eq!(export(&value, 256, ROOT_EXPORT_PART_BYTES), expected);
        // Include every-byte paging on a smaller but nonempty FlatBuffer.
        assert_eq!(export(&root(2), 1, 1), root(2).serialize());
    }

    #[test]
    fn root_export_v1_dual_budget_groups_cheap_work_and_bounds_each_turn() {
        fn phase(
            cursor: &mut RootExportV1,
            root: &RootNode,
            max_units: usize,
            max_bytes: usize,
        ) -> usize {
            let mut calls = 0;
            let mut completed = 0u64;
            let mut processed = 0u64;
            loop {
                let progress = cursor.step(root, max_units, max_bytes).unwrap();
                assert!(progress.units <= max_units);
                assert!(
                    progress.bytes <= max_bytes
                        || (progress.units == 1 && progress.bytes <= ROOT_EXPORT_MAX_ATOMIC_BYTES)
                );
                assert_eq!(progress.completed, completed + progress.units as u64);
                assert_eq!(progress.processed, processed + progress.bytes as u64);
                assert!(progress.done || (progress.units > 0 && progress.bytes > 0));
                completed = progress.completed;
                processed = progress.processed;
                calls += 1;
                if progress.done {
                    return calls;
                }
            }
        }

        let value = root(25_000);
        let mut cursor = RootExportV1::new();
        let plan_calls = phase(&mut cursor, &value, 4096, 256 * 1024);
        assert!(plan_calls < 64, "byte budget did not group cheap Plan work");
        cursor.start_build().unwrap();
        assert_eq!(cursor.progress(0, 0).completed, 0);
        assert_eq!(cursor.progress(0, 0).processed, 0);
        let build_calls = phase(&mut cursor, &value, 4096, 256 * 1024);
        assert!(
            build_calls < 100,
            "byte budget did not group cheap Build work"
        );
        assert!(plan_calls + build_calls < 150);

        let mut atomic = root(1);
        atomic.children[0].0 = "x".repeat(ROOT_EXPORT_PART_BYTES);
        let mut cursor = RootExportV1::new();
        let first = cursor.step(&atomic, 4096, 1).unwrap();
        assert_eq!(first.units, 1);
        assert!(first.bytes <= ROOT_EXPORT_MAX_ATOMIC_BYTES);
        let mut saw_oversized_atomic = first.bytes > 1;
        while !cursor.progress(0, 0).done {
            let progress = cursor.step(&atomic, 4096, 1).unwrap();
            assert_eq!(progress.units, 1);
            assert!(progress.bytes <= ROOT_EXPORT_MAX_ATOMIC_BYTES);
            saw_oversized_atomic |= progress.bytes > 1;
        }
        assert!(saw_oversized_atomic);
    }

    #[test]
    fn root_export_v1_rejects_unbounded_strings_before_arena_admission() {
        assert!(RootExportV1::with_arena_limit(511).is_err());
        for field in 0..3 {
            let mut value = root(2);
            let oversized = "x".repeat(ROOT_EXPORT_PART_BYTES + 1);
            match field {
                0 => value.vault_id = oversized,
                1 => value.device_id = oversized,
                _ => value.children[1].0 = oversized,
            }
            // This is intentionally still accepted by the legacy serializer.
            assert!(!value.serialize().is_empty());
            let mut cursor = RootExportV1::new();
            assert!(cursor
                .step(&value, 256, ROOT_EXPORT_MAX_STEP_BYTES)
                .is_err());
            assert!(cursor.workset().is_err());
            assert_eq!(cursor.arena.capacity(), 0);
            assert_eq!(cursor.offsets.capacity(), 0);
            assert!(cursor.builder.is_none());
        }
        let mut cursor = RootExportV1::new();
        cursor.max_length = MAX_ARENA_BYTES;
        assert!(cursor.charge(1).is_err());
        assert_eq!(cursor.arena.capacity(), 0);

        let value = root(25_000);
        let mut capped = RootExportV1::with_arena_limit(32 * 1024).unwrap();
        let mut calls = 0;
        loop {
            calls += 1;
            if capped.step(&value, 4096, 256 * 1024).is_err() {
                break;
            }
        }
        assert!(
            calls <= 2,
            "impossible arena was not rejected during early Plan"
        );
        assert_eq!(capped.arena.capacity(), 0);
        assert_eq!(capped.offsets.capacity(), 0);
        assert!(capped.builder.is_none());
    }

    #[test]
    fn root_export_v1_phase_errors_and_cancel_at_every_unit_preserve_source() {
        let value = root(7);
        let expected = value.serialize();
        let mut cursor = RootExportV1::new();
        assert!(cursor.start_build().is_err());
        assert!(cursor.seal().is_err());
        assert!(cursor.read(0, 1).is_err());
        assert!(cursor.step(&value, 0, 1).is_err());
        assert!(cursor
            .step(&value, ROOT_EXPORT_MAX_STEP_UNITS + 1, 1)
            .is_err());
        assert!(cursor.step(&value, 1, 0).is_err());
        assert!(cursor
            .step(&value, 1, ROOT_EXPORT_MAX_STEP_BYTES + 1)
            .is_err());
        assert_eq!(cursor.progress(0, 0).completed, 0);
        let plan_units = drain(&mut cursor, &value, 1);
        let workset = cursor.workset().unwrap();
        assert_eq!(
            cursor
                .step(&value, 256, ROOT_EXPORT_MAX_STEP_BYTES)
                .unwrap()
                .units,
            0
        );
        cursor.start_build().unwrap();
        assert!(cursor.start_build().is_err());
        assert!(cursor.workset().is_err());
        let build_units = drain(&mut cursor, &value, 1);
        assert!(cursor.read(0, 1).is_err()); // Build-ready is not sealed.
        let (length, _) = cursor.seal().unwrap();
        assert!(cursor.step(&value, 1, 1).is_err());
        for (at, budget) in [(1, 1), (0, 0), (0, ROOT_EXPORT_PART_BYTES + 1)] {
            assert!(cursor.read(at, budget).is_err());
        }
        assert_eq!(cursor.read(0, 1).unwrap(), expected[..1]);
        cursor.seal().unwrap(); // Does not rewind a partial export.
        assert!(cursor.read(0, 1).is_err());
        assert!(!cursor.at_eof());
        assert_eq!(
            cursor.read(1, ROOT_EXPORT_PART_BYTES).unwrap(),
            expected[1..]
        );
        assert!(cursor.at_eof());
        assert_eq!(length, expected.len());
        assert_eq!(workset.offset_bytes, 28);
        for cancelled_after in 0..=plan_units + build_units {
            let mut cursor = RootExportV1::new();
            for index in 0..cancelled_after {
                if index == plan_units {
                    cursor.start_build().unwrap();
                }
                cursor.step(&value, 1, ROOT_EXPORT_MAX_STEP_BYTES).unwrap();
            }
            drop(cursor);
            assert_eq!(value.serialize(), expected);
        }
    }
}
