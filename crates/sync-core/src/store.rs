use crate::chunk::ChunkError;
use crate::hash::{hash_bytes, hash_to_hex, FileHash};
use std::collections::{hash_map, HashMap, HashSet};
use std::io::Write;

/// Abstract byte-addressed store for Merkle index data (LeafChunk/InternalNode).
/// Desktop uses DiskChunkStore. Server uses its own filesystem impl.
/// iOS WASM uses a JS-backed impl via wasm-bindgen.
#[async_trait::async_trait(?Send)] // ?Send because WASM is single-threaded
pub trait ChunkStore {
    async fn has(&self, hash: &FileHash) -> bool;
    async fn get(&self, hash: &FileHash) -> Result<Vec<u8>, ChunkError>;
    async fn put(&self, hash: FileHash, data: Vec<u8>) -> Result<(), ChunkError>;
    async fn delete(&self, hash: &FileHash) -> Result<(), ChunkError>;
}

/// Disk-based chunk store.
/// Layout: `<base>/<first2hex>/<remaining60hex>`
pub struct DiskChunkStore {
    base: std::path::PathBuf,
}

impl DiskChunkStore {
    pub fn new(base: impl Into<std::path::PathBuf>) -> Self {
        Self { base: base.into() }
    }

    fn chunk_path(&self, hash: &FileHash) -> std::path::PathBuf {
        let hex = hash_to_hex(hash);
        self.base.join(&hex[..2]).join(&hex[2..])
    }

    fn read_verified(&self, hash: &FileHash) -> Result<Vec<u8>, ChunkError> {
        let path = self.chunk_path(hash);
        let data = match std::fs::read(&path) {
            Ok(data) => data,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(ChunkError::NotFound(hash_to_hex(hash)));
            }
            Err(error) => return Err(ChunkError::Io(error)),
        };
        if hash_bytes(&data) != *hash {
            return Err(ChunkError::Deserialize(format!(
                "index object {} failed content-address validation",
                hash_to_hex(hash),
            )));
        }
        Ok(data)
    }
}

#[async_trait::async_trait(?Send)]
impl ChunkStore for DiskChunkStore {
    async fn has(&self, hash: &FileHash) -> bool {
        self.read_verified(hash).is_ok()
    }

    async fn get(&self, hash: &FileHash) -> Result<Vec<u8>, ChunkError> {
        self.read_verified(hash)
    }

    async fn put(&self, hash: FileHash, data: Vec<u8>) -> Result<(), ChunkError> {
        if hash_bytes(&data) != hash {
            return Err(ChunkError::Deserialize(format!(
                "refusing index bytes that do not match {}",
                hash_to_hex(&hash),
            )));
        }
        let path = self.chunk_path(&hash);
        atomic_write(&path, &data)?;
        Ok(())
    }

    async fn delete(&self, hash: &FileHash) -> Result<(), ChunkError> {
        let path = self.chunk_path(hash);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }
}

/// Durable same-directory promotion for native index nodes. A merge or admin
/// rebuild must never expose a half-written object under its final hash, and a
/// previously corrupt object must remain repairable by a deterministic put.
pub(crate) fn atomic_write(path: &std::path::Path, data: &[u8]) -> Result<(), std::io::Error> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "chunk path has no parent")
    })?;
    std::fs::create_dir_all(parent)?;
    if std::fs::read(path).is_ok_and(|existing| existing == data) {
        return Ok(());
    }

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let mut opened = None;
    for attempt in 0..32u8 {
        let candidate = path.with_extension(format!(
            "tmp-{}-{nonce:032x}-{attempt:02x}",
            std::process::id(),
        ));
        match std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&candidate)
        {
            Ok(file) => {
                opened = Some((candidate, file));
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    let (tmp, mut file) = opened.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not allocate a unique chunk temp file",
        )
    })?;
    if let Err(error) = file.write_all(data).and_then(|_| file.sync_all()) {
        let _ = std::fs::remove_file(&tmp);
        return Err(error);
    }
    drop(file);

    if let Err(rename_error) = std::fs::rename(&tmp, path) {
        if std::fs::read(path).is_ok_and(|existing| existing == data) {
            let _ = std::fs::remove_file(&tmp);
            return Ok(());
        }

        // Windows cannot rename over an existing destination. Keep the old
        // object recoverable until the new one has reached the final name.
        let mut backup_name = tmp.as_os_str().to_os_string();
        backup_name.push(".backup");
        let backup = std::path::PathBuf::from(backup_name);
        let had_existing = path.exists();
        if had_existing {
            if let Err(error) = std::fs::rename(path, &backup) {
                let _ = std::fs::remove_file(&tmp);
                return Err(error);
            }
        }
        if let Err(error) = std::fs::rename(&tmp, path) {
            if had_existing {
                let _ = std::fs::rename(&backup, path);
            }
            let _ = std::fs::remove_file(&tmp);
            return Err(std::io::Error::new(
                error.kind(),
                format!("chunk promotion failed after rename error {rename_error}: {error}"),
            ));
        }
        if had_existing {
            let _ = std::fs::remove_file(&backup);
        }
    }

    #[cfg(unix)]
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

/// O(1) component ownership, not total heap, map bucket bytes, or RSS.
/// Byte counts describe only Vec buffers currently owned by this container.
/// If counters_valid is false, the saturated byte counts are NOT exact.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ChunkStoreMemory {
    pub chunks: usize,
    /// HashMap's reported element capacity, not its bucket allocation size.
    /// Tombstones can change this report without shrinking native backing.
    /// A retirement cursor retains the capacity observed before the move.
    pub map_capacity: usize,
    pub payload_bytes: u64,
    pub buffer_capacity_bytes: u64,
    pub counters_valid: bool,
}

impl Default for ChunkStoreMemory {
    fn default() -> Self {
        Self {
            chunks: 0,
            map_capacity: 0,
            payload_bytes: 0,
            buffer_capacity_bytes: 0,
            counters_valid: true,
        }
    }
}

impl ChunkStoreMemory {
    /// Add disjoint container owners, without deduplicating hashes: equal
    /// content in separate stores still owns separate allocations. Summed map
    /// capacities remain logical element capacities, never bucket bytes.
    pub(crate) fn combine(self, other: Self) -> Self {
        let chunks = self.chunks.checked_add(other.chunks);
        let map_capacity = self.map_capacity.checked_add(other.map_capacity);
        let payload_bytes = self.payload_bytes.checked_add(other.payload_bytes);
        let buffer_capacity_bytes = self
            .buffer_capacity_bytes
            .checked_add(other.buffer_capacity_bytes);
        Self {
            counters_valid: self.counters_valid
                && other.counters_valid
                && chunks.is_some()
                && map_capacity.is_some()
                && payload_bytes.is_some()
                && buffer_capacity_bytes.is_some(),
            chunks: chunks.unwrap_or(usize::MAX),
            map_capacity: map_capacity.unwrap_or(usize::MAX),
            payload_bytes: payload_bytes.unwrap_or(u64::MAX),
            buffer_capacity_bytes: buffer_capacity_bytes.unwrap_or(u64::MAX),
        }
    }
}

#[derive(Debug)]
struct BufferMemory {
    payload_bytes: u64,
    buffer_capacity_bytes: u64,
    counters_valid: bool,
}

impl Default for BufferMemory {
    fn default() -> Self {
        Self {
            payload_bytes: 0,
            buffer_capacity_bytes: 0,
            counters_valid: true,
        }
    }
}

impl BufferMemory {
    fn add(&mut self, length: usize, capacity: usize) {
        Self::add_count(&mut self.payload_bytes, length, &mut self.counters_valid);
        Self::add_count(
            &mut self.buffer_capacity_bytes,
            capacity,
            &mut self.counters_valid,
        );
    }

    fn remove(&mut self, bytes: &Vec<u8>) {
        Self::subtract_count(
            &mut self.payload_bytes,
            bytes.len(),
            &mut self.counters_valid,
        );
        Self::subtract_count(
            &mut self.buffer_capacity_bytes,
            bytes.capacity(),
            &mut self.counters_valid,
        );
    }

    fn add_count(value: &mut u64, amount: usize, valid: &mut bool) {
        match u64::try_from(amount)
            .ok()
            .and_then(|amount| value.checked_add(amount))
        {
            Some(next) => *value = next,
            None => {
                *value = u64::MAX;
                *valid = false;
            }
        }
    }

    fn subtract_count(value: &mut u64, amount: usize, valid: &mut bool) {
        match u64::try_from(amount)
            .ok()
            .and_then(|amount| value.checked_sub(amount))
        {
            Some(next) => *value = next,
            None => {
                *value = 0;
                *valid = false;
            }
        }
    }

    fn summary(&self, chunks: usize, map_capacity: usize) -> ChunkStoreMemory {
        ChunkStoreMemory {
            chunks,
            map_capacity,
            payload_bytes: self.payload_bytes,
            buffer_capacity_bytes: self.buffer_capacity_bytes,
            counters_valid: self.counters_valid,
        }
    }
}

#[derive(Default)]
struct MemoryChunkState {
    map: HashMap<FileHash, StoredChunk>,
    memory: BufferMemory,
    insertion_generation: u64,
    active_baseline: Option<u64>,
}

struct StoredChunk {
    bytes: Vec<u8>,
    birth_generation: u64,
}

impl std::ops::Deref for StoredChunk {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

/// In-memory immutable-node storage. Map and component counters share one
/// RefCell borrow: readers never observe a partially updated summary.
pub struct MemoryChunkStore {
    data: std::cell::RefCell<MemoryChunkState>,
}

/// O(1) ownership transfer of the original map allocation. next() removes one
/// buffer from this component's accounting, but does not free the returned Vec:
/// the caller owns it until its explicit drop. Bucket scanning and the final
/// backing allocation release remain native, non-wall-clock-bounded residuals.
pub(crate) struct ChunkRetirement {
    chunks: hash_map::IntoIter<FileHash, StoredChunk>,
    map_capacity: usize,
    memory: BufferMemory,
}

impl ChunkRetirement {
    pub(crate) fn memory_summary(&self) -> ChunkStoreMemory {
        self.memory.summary(self.chunks.len(), self.map_capacity)
    }
}

impl Iterator for ChunkRetirement {
    type Item = (FileHash, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        let chunk = self.chunks.next()?;
        self.memory.remove(&chunk.1);
        Some((chunk.0, chunk.1.bytes))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.chunks.size_hint()
    }
}

impl ExactSizeIterator for ChunkRetirement {
    fn len(&self) -> usize {
        self.chunks.len()
    }
}

impl std::iter::FusedIterator for ChunkRetirement {}

/// Export-copy limit, not a limit on immutable object size or tree format.
pub(crate) const MAX_CHUNK_EXPORT_PART_BYTES: usize = 64 * 1024;

impl Default for MemoryChunkStore {
    fn default() -> Self {
        Self {
            data: std::cell::RefCell::new(MemoryChunkState::default()),
        }
    }
}

impl MemoryChunkStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.data.borrow().map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.borrow().map.is_empty()
    }

    pub fn memory_summary(&self) -> ChunkStoreMemory {
        let state = self.data.borrow();
        state.memory.summary(state.map.len(), state.map.capacity())
    }

    /// An owner-local cut over ALL resident keys. No key snapshot is allocated.
    /// The generation advances only when a candidate actually opens, never on
    /// an individual insert; overflow therefore precedes publication.
    pub(crate) fn candidate_baseline_preflight(&self) -> Result<u64, ChunkError> {
        let state = self.data.borrow();
        if state.active_baseline.is_some() {
            return Err(ChunkError::Deserialize(
                "candidate baseline already pinned".into(),
            ));
        }
        state.insertion_generation.checked_add(1).ok_or_else(|| {
            ChunkError::Deserialize("candidate baseline generation exhausted".into())
        })?;
        Ok(state.insertion_generation)
    }

    pub(crate) fn open_candidate_baseline(&self, expected: u64) -> Result<(), ChunkError> {
        let mut state = self.data.borrow_mut();
        if state.active_baseline.is_some() || state.insertion_generation != expected {
            return Err(ChunkError::Deserialize(
                "candidate baseline generation changed".into(),
            ));
        }
        let next = expected.checked_add(1).ok_or_else(|| {
            ChunkError::Deserialize("candidate baseline generation exhausted".into())
        })?;
        state.active_baseline = Some(expected);
        state.insertion_generation = next;
        Ok(())
    }

    /// Only terminal candidate close may unpin immediately before its already
    /// validated, infallible sweep. Removing/reinserting a pre-cut key earlier
    /// would otherwise change the established all-resident baseline semantics.
    pub(crate) fn close_candidate_baseline(&self, expected: u64) -> Result<(), ChunkError> {
        let mut state = self.data.borrow_mut();
        if state.active_baseline != Some(expected) {
            return Err(ChunkError::Deserialize(
                "candidate baseline pin mismatch".into(),
            ));
        }
        state.active_baseline = None;
        Ok(())
    }

    pub(crate) fn is_fresh_since(
        &self,
        hash: &FileHash,
        baseline: u64,
    ) -> Result<bool, ChunkError> {
        let state = self.data.borrow();
        if state.active_baseline != Some(baseline) {
            return Err(ChunkError::Deserialize(
                "candidate baseline is not pinned".into(),
            ));
        }
        state
            .map
            .get(hash)
            .map(|chunk| chunk.birth_generation > baseline)
            .ok_or_else(|| ChunkError::NotFound(hash_to_hex(hash)))
    }

    #[cfg(test)]
    pub(crate) fn set_insertion_generation_for_test(&self, value: u64) {
        self.data.borrow_mut().insertion_generation = value;
    }

    #[cfg(test)]
    pub(crate) fn set_payload_memory_counter_for_test(&self, payload_bytes: u64, valid: bool) {
        let mut state = self.data.borrow_mut();
        state.memory.payload_bytes = payload_bytes;
        state.memory.counters_valid = valid;
    }

    /// Move the private store into a single-pass retirement cursor without
    /// copying buffers or collecting keys. Each next() owns one node buffer;
    /// hash-table bucket scanning and final backing allocation deallocation
    /// remain allocator-level residuals, not a wall-clock guarantee.
    pub(crate) fn into_chunks(self) -> ChunkRetirement {
        let MemoryChunkState { map, memory, .. } = self.data.into_inner();
        ChunkRetirement {
            map_capacity: map.capacity(),
            chunks: map.into_iter(),
            memory,
        }
    }

    pub fn insert_chunk(&self, hash: FileHash, bytes: Vec<u8>) {
        let length = bytes.len();
        let capacity = bytes.capacity();
        let mut state = self.data.borrow_mut();
        let birth_generation = state
            .map
            .get(&hash)
            .map_or(state.insertion_generation, |chunk| chunk.birth_generation);
        if let Some(previous) = state.map.insert(
            hash,
            StoredChunk {
                bytes,
                birth_generation,
            },
        ) {
            state.memory.remove(&previous);
        }
        state.memory.add(length, capacity);
    }

    pub fn get_chunk(&self, hash: &FileHash) -> Option<Vec<u8>> {
        self.data
            .borrow()
            .map
            .get(hash)
            .map(|chunk| chunk.bytes.clone())
    }

    /// Inspect the resident buffer without cloning or serializing its bytes.
    pub fn chunk_byte_length(&self, hash: &FileHash) -> Option<usize> {
        self.data
            .borrow()
            .map
            .get(hash)
            .map(|chunk| chunk.bytes.len())
    }

    /// Compare one resident object in place. The borrow remains local and no
    /// node-sized buffer is cloned merely to detect a content-addressed
    /// collision before a private overlay accepts its bytes.
    pub(crate) fn resident_chunk_matches(&self, hash: &FileHash, bytes: &[u8]) -> Option<bool> {
        self.data
            .borrow()
            .map
            .get(hash)
            .map(|resident| resident.as_slice() == bytes)
    }

    /// Copy only the requested part of an exclusively owned resident object.
    /// The captured length fences missing/replaced-size buffers; same-length
    /// out-of-band mutation is forbidden by the owner's lifetime contract.
    /// No RefCell borrow or view into resident memory escapes this call.
    pub(crate) fn copy_chunk_range(
        &self,
        hash: &FileHash,
        captured_length: usize,
        offset: usize,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ChunkError> {
        if max_bytes == 0 || max_bytes > MAX_CHUNK_EXPORT_PART_BYTES {
            return Err(ChunkError::Deserialize(
                "invalid chunk export part budget".into(),
            ));
        }
        let remaining = captured_length.checked_sub(offset).ok_or_else(|| {
            ChunkError::Deserialize("chunk export offset exceeds captured length".into())
        })?;
        let count = remaining.min(max_bytes);
        let end = offset
            .checked_add(count)
            .ok_or_else(|| ChunkError::Deserialize("chunk export offset overflow".into()))?;
        let resident = self.data.borrow();
        let bytes = resident
            .map
            .get(hash)
            .ok_or_else(|| ChunkError::Deserialize("chunk export source is missing".into()))?;
        if bytes.len() != captured_length {
            return Err(ChunkError::Deserialize(
                "chunk export source length changed".into(),
            ));
        }
        // Deliberately slice BEFORE copying; get_chunk() would clone the whole
        // object and defeat both the part bound and the native memory owner.
        Ok(bytes[offset..end].to_vec())
    }

    pub fn all_chunks(&self) -> Vec<(FileHash, Vec<u8>)> {
        self.data
            .borrow()
            .map
            .iter()
            .map(|(k, v)| (*k, v.bytes.clone()))
            .collect()
    }

    pub fn all_chunk_hashes(&self) -> Vec<FileHash> {
        self.data.borrow().map.keys().copied().collect()
    }

    /// Atomically promote a job-local content-addressed overlay. Every staged
    /// object and every resident collision is validated before the first
    /// insert, so an error cannot expose a partially promoted mutation.
    pub(crate) fn promote_verified_chunks(
        &self,
        staged: std::collections::HashMap<FileHash, Vec<u8>>,
    ) -> Result<(), ChunkError> {
        for (hash, bytes) in &staged {
            if hash_bytes(bytes) != *hash {
                return Err(ChunkError::Deserialize(format!(
                    "refusing staged index bytes that do not match {}",
                    hash_to_hex(hash),
                )));
            }
        }

        let mut state = self.data.borrow_mut();
        let MemoryChunkState {
            map: resident,
            memory,
            insertion_generation,
            ..
        } = &mut *state;
        for (hash, bytes) in &staged {
            if resident
                .get(hash)
                .is_some_and(|existing| &existing.bytes != bytes)
            {
                return Err(ChunkError::Deserialize(format!(
                    "resident index object {} has conflicting bytes",
                    hash_to_hex(hash),
                )));
            }
        }
        let missing = staged
            .keys()
            .filter(|hash| !resident.contains_key(*hash))
            .count();
        resident.try_reserve(missing).map_err(|_| {
            ChunkError::Deserialize("could not reserve staged index promotion".into())
        })?;
        for (hash, bytes) in staged {
            if let hash_map::Entry::Vacant(entry) = resident.entry(hash) {
                let length = bytes.len();
                let capacity = bytes.capacity();
                entry.insert(StoredChunk {
                    bytes,
                    birth_generation: *insertion_generation,
                });
                memory.add(length, capacity);
            }
        }
        Ok(())
    }

    /// Validate and reserve an exclusively owned candidate overlay without
    /// consuming it. Candidate staging already omits resident-equal objects,
    /// so any resident appearance here is a late ownership/collision change.
    /// Every fallible operation precedes the caller's final move/promotion.
    pub(crate) fn prepare_candidate_chunk_promotion(
        &self,
        staged: &std::collections::HashMap<FileHash, Vec<u8>>,
    ) -> Result<(), ChunkError> {
        for (hash, bytes) in staged {
            if hash_bytes(bytes) != *hash {
                return Err(ChunkError::Deserialize(format!(
                    "refusing staged candidate bytes that do not match {}",
                    hash_to_hex(hash),
                )));
            }
        }
        let mut state = self.data.borrow_mut();
        if let Some(hash) = staged.keys().find(|hash| state.map.contains_key(*hash)) {
            return Err(ChunkError::Deserialize(format!(
                "resident candidate object {} appeared after staging",
                hash_to_hex(hash),
            )));
        }
        state.map.try_reserve(staged.len()).map_err(|_| {
            ChunkError::Deserialize("could not reserve candidate chunk promotion".into())
        })?;
        Ok(())
    }

    /// Promote an overlay after `prepare_candidate_chunk_promotion` under the
    /// same exclusive tree-owner turn. Capacity and collisions are already
    /// fenced, so this phase contains no fallible operation or buffer copy.
    pub(crate) fn promote_prepared_candidate_chunks(
        &self,
        staged: std::collections::HashMap<FileHash, Vec<u8>>,
    ) {
        let mut state = self.data.borrow_mut();
        let MemoryChunkState {
            map,
            memory,
            insertion_generation,
            ..
        } = &mut *state;
        for (hash, bytes) in staged {
            let length = bytes.len();
            let capacity = bytes.capacity();
            let birth_generation = map
                .get(&hash)
                .map_or(*insertion_generation, |chunk| chunk.birth_generation);
            let previous = map.insert(
                hash,
                StoredChunk {
                    bytes,
                    birth_generation,
                },
            );
            debug_assert!(previous.is_none());
            if let Some(previous) = previous {
                memory.remove(&previous);
            }
            memory.add(length, capacity);
        }
    }

    /// Retain exactly the marked graph after reachability has been validated.
    /// The caller must finish the complete mark phase before invoking this;
    /// keeping mutation here as one RefCell borrow prevents a partial sweep.
    pub(crate) fn retain_chunks(&self, reachable: &HashSet<FileHash>) -> (usize, usize, u64) {
        let mut state = self.data.borrow_mut();
        // This internal non-fallible sweep is only called after full graph
        // validation and terminal unpin. Fail before touching any key if a
        // future caller violates that ownership protocol.
        assert!(
            state.active_baseline.is_none(),
            "cannot sweep a pinned candidate baseline"
        );
        let MemoryChunkState {
            map: data, memory, ..
        } = &mut *state;
        let before = data.len();
        let mut bytes_removed = 0u64;
        data.retain(|hash, bytes| {
            let keep = reachable.contains(hash);
            if !keep {
                bytes_removed = bytes_removed.saturating_add(bytes.len() as u64);
                memory.remove(bytes);
            }
            keep
        });
        (before, data.len(), bytes_removed)
    }
}

#[async_trait::async_trait(?Send)]
impl ChunkStore for MemoryChunkStore {
    async fn has(&self, hash: &FileHash) -> bool {
        self.data.borrow().map.contains_key(hash)
    }

    async fn get(&self, hash: &FileHash) -> Result<Vec<u8>, ChunkError> {
        self.data
            .borrow()
            .map
            .get(hash)
            .map(|chunk| chunk.bytes.clone())
            .ok_or_else(|| ChunkError::NotFound(hash_to_hex(hash)))
    }

    async fn put(&self, hash: FileHash, data: Vec<u8>) -> Result<(), ChunkError> {
        self.insert_chunk(hash, data);
        Ok(())
    }

    async fn delete(&self, hash: &FileHash) -> Result<(), ChunkError> {
        let mut state = self.data.borrow_mut();
        if state.active_baseline.is_some() {
            return Err(ChunkError::Deserialize(
                "cannot delete with a pinned candidate baseline".into(),
            ));
        }
        if let Some(bytes) = state.map.remove(hash) {
            state.memory.remove(&bytes);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::hash_bytes;

    #[tokio::test]
    async fn candidate_generation_baseline_matches_all_key_oracle_and_pins_removals() {
        let store = MemoryChunkStore::new();
        let old = hash_bytes(b"old");
        let orphan = hash_bytes(b"orphan");
        store.insert_chunk(old, b"old".to_vec());
        store.insert_chunk(orphan, b"orphan".to_vec());
        for turn in 0..20u8 {
            let baseline: HashSet<_> = store.all_chunk_hashes().into_iter().collect();
            let cut = store.candidate_baseline_preflight().unwrap();
            assert_eq!(cut, turn as u64);
            store.open_candidate_baseline(cut).unwrap();
            assert!(store.open_candidate_baseline(cut).is_err());
            assert!(store.candidate_baseline_preflight().is_err());
            // Replacement, including an altered buffer under the same key,
            // never changes historical membership in the baseline.
            store.insert_chunk(old, vec![turn]);
            let fresh = hash_bytes(&[turn, 1]);
            store.put(fresh, vec![turn, 1]).await.unwrap();
            let promoted = hash_bytes(&[turn, 2]);
            let existing_bytes = store.get_chunk(&orphan).unwrap();
            store
                .promote_verified_chunks(HashMap::from([
                    (orphan, existing_bytes),
                    (promoted, vec![turn, 2]),
                ]))
                .unwrap();
            let prepared = hash_bytes(&[turn, 3]);
            let staged = HashMap::from([(prepared, vec![turn, 3])]);
            store.prepare_candidate_chunk_promotion(&staged).unwrap();
            store.promote_prepared_candidate_chunks(staged);
            for hash in store.all_chunk_hashes() {
                assert_eq!(
                    store.is_fresh_since(&hash, cut).unwrap(),
                    !baseline.contains(&hash)
                );
            }
            let before = store.memory_summary();
            let keys: HashSet<_> = store.all_chunk_hashes().into_iter().collect();
            assert!(store.delete(&old).await.is_err());
            assert!(store.close_candidate_baseline(cut + 1).is_err());
            let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                store.retain_chunks(&HashSet::new())
            }));
            assert!(rejected.is_err());
            assert_eq!(store.memory_summary(), before);
            assert_eq!(
                store.all_chunk_hashes().into_iter().collect::<HashSet<_>>(),
                keys
            );
            store.close_candidate_baseline(cut).unwrap();
            assert!(store.is_fresh_since(&old, cut).is_err());
            store.retain_chunks(&HashSet::from([old, orphan, fresh]));
            assert_memory_oracle(&store);
        }
        // Removal/reinsertion BETWEEN cuts is permitted and becomes part of
        // the next baseline; a key absent at that cut is fresh if reinserted.
        store.delete(&old).await.unwrap();
        store.insert_chunk(old, b"old".to_vec());
        let cut = store.candidate_baseline_preflight().unwrap();
        store.open_candidate_baseline(cut).unwrap();
        assert!(!store.is_fresh_since(&old, cut).unwrap());
        store.close_candidate_baseline(cut).unwrap();
        store.delete(&old).await.unwrap();
        let cut = store.candidate_baseline_preflight().unwrap();
        store.open_candidate_baseline(cut).unwrap();
        store.insert_chunk(old, b"old".to_vec());
        assert!(store.is_fresh_since(&old, cut).unwrap());
    }

    #[test]
    fn candidate_generation_overflow_and_promotion_error_are_atomic() {
        let store = MemoryChunkStore::new();
        let hash = hash_bytes(b"old");
        store.insert_chunk(hash, b"old".to_vec());
        store.set_insertion_generation_for_test(u64::MAX);
        let before = store.memory_summary();
        assert!(store.candidate_baseline_preflight().is_err());
        assert!(store.open_candidate_baseline(u64::MAX).is_err());
        assert_eq!(store.data.borrow().active_baseline, None);
        assert_eq!(store.data.borrow().insertion_generation, u64::MAX);
        assert_eq!(store.memory_summary(), before);
        store.set_insertion_generation_for_test(7);
        let cut = store.candidate_baseline_preflight().unwrap();
        store.open_candidate_baseline(cut).unwrap();
        let fresh = hash_bytes(b"new");
        assert!(store
            .promote_verified_chunks(HashMap::from([
                (fresh, b"new".to_vec()),
                (hash, b"bad collision".to_vec()),
            ]))
            .is_err());
        assert!(!store.data.borrow().map.contains_key(&fresh));
        assert!(!store.is_fresh_since(&hash, cut).unwrap());
        assert_eq!(store.memory_summary(), before);
    }

    fn assert_memory_oracle(store: &MemoryChunkStore) -> ChunkStoreMemory {
        let expected = {
            let state = store.data.borrow();
            ChunkStoreMemory {
                chunks: state.map.len(),
                map_capacity: state.map.capacity(),
                payload_bytes: state.map.values().map(|bytes| bytes.len() as u64).sum(),
                buffer_capacity_bytes: state
                    .map
                    .values()
                    .map(|bytes| bytes.capacity() as u64)
                    .sum(),
                counters_valid: true,
            }
        };
        assert_eq!(store.memory_summary(), expected);
        assert_eq!(
            store.memory_summary(),
            expected,
            "reporting mutated ownership"
        );
        expected
    }

    fn spare(bytes: &[u8], capacity: usize) -> Vec<u8> {
        let mut output = Vec::with_capacity(capacity.max(bytes.len()));
        output.extend_from_slice(bytes);
        output
    }

    #[test]
    fn memory_component_combine_counts_identical_content_in_separate_owners() {
        let resident = MemoryChunkStore::new();
        let replacement = MemoryChunkStore::new();
        let content = b"same immutable node, independent buffers";
        let hash = hash_bytes(content);
        resident.insert_chunk(hash, spare(content, 1024));
        replacement.insert_chunk(hash, spare(content, 4096));
        let left = assert_memory_oracle(&resident);
        let right = assert_memory_oracle(&replacement);
        let total = left.combine(right);
        assert_eq!(total.chunks, 2);
        assert_eq!(total.payload_bytes, content.len() as u64 * 2);
        assert_eq!(
            total.buffer_capacity_bytes,
            left.buffer_capacity_bytes + right.buffer_capacity_bytes
        );
        assert_eq!(total.map_capacity, left.map_capacity + right.map_capacity);
        assert!(total.counters_valid);
        assert_eq!(total, right.combine(left));
        assert_eq!(total.combine(ChunkStoreMemory::default()), total);
        assert_eq!(resident.memory_summary(), left);
        assert_eq!(replacement.memory_summary(), right);

        let mut retiring = replacement.into_chunks();
        assert_eq!(left.combine(retiring.memory_summary()), total);
        drop(retiring.next().unwrap());
        let remaining = left.combine(retiring.memory_summary());
        assert_eq!(remaining.chunks, left.chunks);
        assert_eq!(remaining.payload_bytes, left.payload_bytes);
        assert_eq!(remaining.buffer_capacity_bytes, left.buffer_capacity_bytes);
        assert_eq!(remaining.map_capacity, total.map_capacity);
    }

    #[test]
    fn memory_component_combine_overflow_is_independently_invalid_and_sticky() {
        let unit = ChunkStoreMemory {
            chunks: 1,
            map_capacity: 1,
            payload_bytes: 1,
            buffer_capacity_bytes: 1,
            counters_valid: true,
        };
        for component in 0..4 {
            let mut at_limit = unit;
            match component {
                0 => at_limit.chunks = usize::MAX,
                1 => at_limit.map_capacity = usize::MAX,
                2 => at_limit.payload_bytes = u64::MAX,
                _ => at_limit.buffer_capacity_bytes = u64::MAX,
            }
            let overflowed = at_limit.combine(unit);
            assert!(!overflowed.counters_valid);
            assert_eq!(overflowed, unit.combine(at_limit));
            assert_eq!(overflowed.combine(ChunkStoreMemory::default()), overflowed);
            assert_eq!(
                overflowed.chunks,
                if component == 0 { usize::MAX } else { 2 }
            );
            assert_eq!(
                overflowed.map_capacity,
                if component == 1 { usize::MAX } else { 2 }
            );
            assert_eq!(
                overflowed.payload_bytes,
                if component == 2 { u64::MAX } else { 2 }
            );
            assert_eq!(
                overflowed.buffer_capacity_bytes,
                if component == 3 { u64::MAX } else { 2 }
            );
        }
        let invalid = ChunkStoreMemory {
            counters_valid: false,
            ..unit
        };
        assert!(!invalid.combine(unit).counters_valid);
        assert!(!unit.combine(invalid).counters_valid);
        assert_eq!(invalid.combine(unit).payload_bytes, 2);
    }

    #[tokio::test]
    async fn memory_component_insert_replace_put_delete_and_spare_capacity() {
        let store = MemoryChunkStore::new();
        assert_eq!(
            assert_memory_oracle(&store),
            ChunkStoreMemory {
                chunks: 0,
                map_capacity: 0,
                payload_bytes: 0,
                buffer_capacity_bytes: 0,
                counters_valid: true,
            }
        );
        let hash = hash_bytes(b"owner");
        let original = spare(b"abc", 8192);
        let original_capacity = original.capacity() as u64;
        store.insert_chunk(hash, original);
        let first = assert_memory_oracle(&store);
        assert_eq!(first.payload_bytes, 3);
        assert_eq!(first.buffer_capacity_bytes, original_capacity);
        let detached = store.get_chunk(&hash).unwrap();
        assert_eq!(
            store.memory_summary(),
            first,
            "detached copies are not store-owned"
        );
        drop(detached);

        store.insert_chunk(hash, spare(b"replacement", 32));
        assert_memory_oracle(&store);
        let empty_allocated = Vec::with_capacity(1024 * 1024);
        let empty_capacity = empty_allocated.capacity() as u64;
        store.put(hash, empty_allocated).await.unwrap();
        let empty = assert_memory_oracle(&store);
        assert_eq!(empty.chunks, 1);
        assert_eq!(empty.payload_bytes, 0);
        assert_eq!(empty.buffer_capacity_bytes, empty_capacity);
        store.delete(&hash_bytes(b"missing")).await.unwrap();
        assert_eq!(store.memory_summary(), empty);
        store.put(hash, Vec::new()).await.unwrap();
        assert_eq!(assert_memory_oracle(&store).buffer_capacity_bytes, 0);
        store.delete(&hash).await.unwrap();
        let removed = assert_memory_oracle(&store);
        assert_eq!(removed.chunks, 0);
        assert!(
            removed.map_capacity > 0,
            "delete unexpectedly discarded map backing"
        );
    }

    #[test]
    fn memory_component_promotion_counts_only_new_owners_and_failure_changes_nothing() {
        let store = MemoryChunkStore::new();
        let resident = spare(b"resident", 4096);
        let resident_hash = hash_bytes(&resident);
        let resident_pointer = resident.as_ptr();
        let resident_capacity = resident.capacity();
        store.insert_chunk(resident_hash, resident);
        let before = assert_memory_oracle(&store);
        store.promote_verified_chunks(HashMap::new()).unwrap();
        assert_eq!(store.memory_summary(), before);
        store
            .promote_verified_chunks(HashMap::from([(
                resident_hash,
                spare(b"resident", 1024 * 1024),
            )]))
            .unwrap();
        assert_eq!(
            store.memory_summary(),
            before,
            "duplicate replaced/charged its spare buffer"
        );
        assert_eq!(
            store.data.borrow().map[&resident_hash].as_ptr(),
            resident_pointer
        );

        let fresh = spare(b"fresh", 8192);
        let fresh_hash = hash_bytes(&fresh);
        let fresh_capacity = fresh.capacity();
        store
            .promote_verified_chunks(HashMap::from([
                (resident_hash, spare(b"resident", 65536)),
                (fresh_hash, fresh),
            ]))
            .unwrap();
        let promoted = assert_memory_oracle(&store);
        assert_eq!(
            promoted.buffer_capacity_bytes,
            (resident_capacity + fresh_capacity) as u64
        );

        let another = b"another".to_vec();
        let another_hash = hash_bytes(&another);
        assert!(store
            .promote_verified_chunks(HashMap::from([
                (another_hash, another.clone()),
                (hash_bytes(b"wrong hash"), b"wrong bytes".to_vec()),
            ]))
            .is_err());
        assert_eq!(assert_memory_oracle(&store), promoted);
        assert_eq!(store.get_chunk(&another_hash), None);
        store.insert_chunk(fresh_hash, spare(b"corrupt collision", 16384));
        let corrupt = assert_memory_oracle(&store);
        assert!(store
            .promote_verified_chunks(HashMap::from([
                (another_hash, another),
                (fresh_hash, b"fresh".to_vec()),
            ]))
            .is_err());
        assert_eq!(assert_memory_oracle(&store), corrupt);
        assert_eq!(store.get_chunk(&another_hash), None);
    }

    #[tokio::test]
    async fn memory_component_random_mutations_match_full_scan_oracle() {
        let store = MemoryChunkStore::new();
        let mut random = 0x3157_2468_abcd_u64;
        for index in 0..1500usize {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            let hash = hash_bytes(&(random % 127).to_le_bytes());
            let length = ((random >> 8) % 67) as usize;
            let capacity = length + ((random >> 16) % 257) as usize;
            let bytes = spare(&vec![(random >> 32) as u8; length], capacity);
            match index % 7 {
                0 | 1 => store.insert_chunk(hash, bytes),
                2 => store.put(hash, bytes).await.unwrap(),
                3 => store.delete(&hash).await.unwrap(),
                4 => {
                    let reachable: HashSet<_> = store
                        .all_chunk_hashes()
                        .into_iter()
                        .filter(|value| random.wrapping_add(u64::from(value[0])) % 3 != 0)
                        .collect();
                    let before = store.memory_summary();
                    let expected_removed: u64 = store
                        .data
                        .borrow()
                        .map
                        .iter()
                        .filter(|(hash, _)| !reachable.contains(*hash))
                        .map(|(_, bytes)| bytes.len() as u64)
                        .sum();
                    let (old, kept, removed) = store.retain_chunks(&reachable);
                    assert_eq!(old, before.chunks);
                    assert_eq!(kept, reachable.len());
                    assert_eq!(removed, expected_removed);
                }
                5 => {
                    let valid = hash_bytes(&bytes);
                    store
                        .promote_verified_chunks(HashMap::from([(valid, bytes)]))
                        .unwrap();
                }
                _ => {
                    let _ = store.get_chunk(&hash);
                }
            }
            assert_memory_oracle(&store);
        }
        store.retain_chunks(&HashSet::new());
        let empty = assert_memory_oracle(&store);
        assert_eq!(empty.chunks, 0);
        assert_eq!(empty.payload_bytes, 0);
        assert_eq!(empty.buffer_capacity_bytes, 0);
        assert!(
            empty.map_capacity > 0,
            "retain must not shrink-to-fit its backing allocation"
        );
    }

    #[test]
    fn memory_component_retirement_transfers_each_original_buffer_and_retains_map_capacity() {
        let store = MemoryChunkStore::new();
        let mut remaining = HashMap::new();
        for index in 0..257usize {
            let hash = hash_bytes(&index.to_le_bytes());
            let length = index % 31;
            let bytes = spare(&vec![index as u8; length], 128 + index);
            remaining.insert(
                hash,
                (bytes.as_ptr(), length, bytes.capacity(), index as u8),
            );
            store.insert_chunk(hash, bytes);
            assert_memory_oracle(&store);
        }
        let before = store.memory_summary();
        let mut retiring = store.into_chunks();
        assert_eq!(retiring.memory_summary(), before);
        let mut outgoing = Vec::new();
        while let Some((hash, bytes)) = retiring.next() {
            let (pointer, length, capacity, fill) = remaining.remove(&hash).unwrap();
            assert_eq!(bytes.as_ptr(), pointer, "retirement cloned a node buffer");
            assert_eq!(bytes.len(), length);
            assert_eq!(bytes.capacity(), capacity);
            assert!(bytes.iter().all(|byte| *byte == fill));
            let expected = ChunkStoreMemory {
                chunks: remaining.len(),
                map_capacity: before.map_capacity,
                payload_bytes: remaining
                    .values()
                    .map(|(_, length, _, _)| *length as u64)
                    .sum(),
                buffer_capacity_bytes: remaining
                    .values()
                    .map(|(_, _, capacity, _)| *capacity as u64)
                    .sum(),
                counters_valid: true,
            };
            assert_eq!(retiring.memory_summary(), expected);
            assert_eq!(retiring.len(), remaining.len());
            assert_eq!(
                retiring.size_hint(),
                (remaining.len(), Some(remaining.len()))
            );
            outgoing.push((bytes, pointer, fill));
        }
        assert_eq!(outgoing.len(), before.chunks);
        let drained = retiring.memory_summary();
        assert_eq!(drained.payload_bytes, 0);
        assert_eq!(drained.buffer_capacity_bytes, 0);
        assert_eq!(drained.map_capacity, before.map_capacity);
        assert!(retiring.next().is_none());
        assert_eq!(retiring.memory_summary(), drained);
        drop(retiring); // Only the map backing remains; outgoing buffers still have real owners.
        for (mut bytes, pointer, fill) in outgoing {
            assert_eq!(bytes.as_ptr(), pointer);
            if bytes.is_empty() {
                bytes.push(fill);
            }
            assert_eq!(bytes[0], fill);
            drop(bytes); // Native deterministic Vec drop, never a GC-based ownership assertion.
        }
    }

    #[test]
    fn memory_component_partial_and_empty_retirement_preserve_outgoing_ownership() {
        let store = MemoryChunkStore::new();
        for index in 0..64u64 {
            store.insert_chunk(
                hash_bytes(&index.to_le_bytes()),
                spare(&[index as u8], 1024),
            );
        }
        let mut retiring = store.into_chunks();
        let (_, first) = retiring.next().unwrap();
        let pointer = first.as_ptr();
        assert_eq!(retiring.memory_summary().chunks, 63);
        drop(retiring); // Drops only the still-owned 63 entries and the backing map.
        assert_eq!(first.as_ptr(), pointer);
        assert_eq!(first.len(), 1);
        assert!(first[0] < 64);
        drop(first);

        let store = MemoryChunkStore::new();
        store.insert_chunk(hash_bytes(b"empty"), Vec::with_capacity(8192));
        assert_eq!(store.memory_summary().buffer_capacity_bytes, 8192);
        store.retain_chunks(&HashSet::new());
        let before = assert_memory_oracle(&store);
        assert!(before.map_capacity > 0);
        let mut retiring = store.into_chunks();
        assert_eq!(retiring.memory_summary(), before);
        assert_eq!(retiring.len(), 0);
        assert!(retiring.next().is_none());
        assert_eq!(retiring.memory_summary(), before);
        drop(retiring);
        let mut empty = MemoryChunkStore::new().into_chunks();
        assert_eq!(empty.memory_summary().map_capacity, 0);
        assert_eq!(empty.len(), 0);
        assert!(empty.next().is_none());
    }

    #[tokio::test]
    async fn memory_component_overflow_and_underflow_are_sticky_through_retirement() {
        let store = MemoryChunkStore::new();
        {
            let mut state = store.data.borrow_mut();
            state.memory.payload_bytes = u64::MAX;
            state.memory.buffer_capacity_bytes = u64::MAX;
        }
        let hash = hash_bytes(b"overflow");
        store.insert_chunk(hash, vec![1]);
        let overflowed = store.memory_summary();
        assert!(!overflowed.counters_valid);
        assert_eq!(overflowed.payload_bytes, u64::MAX);
        assert_eq!(overflowed.buffer_capacity_bytes, u64::MAX);
        store.delete(&hash).await.unwrap();
        assert!(!store.memory_summary().counters_valid);
        store.insert_chunk(hash, Vec::new());
        let mut retiring = store.into_chunks();
        assert!(!retiring.memory_summary().counters_valid);
        drop(retiring.next().unwrap());
        assert!(!retiring.memory_summary().counters_valid);
        assert!(retiring.next().is_none());

        let store = MemoryChunkStore::new();
        store.insert_chunk(hash, spare(b"underflow", 8192));
        {
            let mut state = store.data.borrow_mut();
            state.memory.payload_bytes = 0;
            state.memory.buffer_capacity_bytes = 0;
        }
        store.retain_chunks(&HashSet::new());
        let underflowed = store.memory_summary();
        assert!(!underflowed.counters_valid);
        assert_eq!(underflowed.payload_bytes, 0);
        assert_eq!(underflowed.buffer_capacity_bytes, 0);
        store.put(hash, vec![9]).await.unwrap();
        assert!(!store.memory_summary().counters_valid);
        let before = store.memory_summary();
        assert!(store
            .promote_verified_chunks(HashMap::from([(hash_bytes(b"not these bytes"), vec![8]),]))
            .is_err());
        assert_eq!(store.memory_summary(), before);
    }

    #[test]
    fn memory_component_each_byte_counter_independently_marks_arithmetic_failure() {
        for capacity_only in [false, true] {
            let mut overflow = BufferMemory::default();
            if capacity_only {
                overflow.buffer_capacity_bytes = u64::MAX;
            } else {
                overflow.payload_bytes = u64::MAX;
            }
            overflow.add(1, 1);
            assert!(!overflow.counters_valid);
            overflow.remove(&vec![1]);
            overflow.add(0, 0);
            assert!(
                !overflow.counters_valid,
                "later valid arithmetic repaired an invalid counter"
            );

            let bytes = spare(b"x", 32);
            let mut underflow = BufferMemory::default();
            underflow.add(bytes.len(), bytes.capacity());
            if capacity_only {
                underflow.buffer_capacity_bytes = 0;
            } else {
                underflow.payload_bytes = 0;
            }
            underflow.remove(&bytes);
            assert!(!underflow.counters_valid);
            assert_eq!(underflow.payload_bytes, 0);
            assert_eq!(underflow.buffer_capacity_bytes, 0);
            underflow.add(bytes.len(), bytes.capacity());
            assert!(!underflow.counters_valid);
        }
    }

    #[test]
    fn chunk_export_range_exact_parts_and_detached_bounded_allocation() {
        let store = MemoryChunkStore::new();
        let bytes: Vec<u8> = (0..2 * MAX_CHUNK_EXPORT_PART_BYTES + 17)
            .map(|index| (index.wrapping_mul(131) ^ (index >> 5)) as u8)
            .collect();
        let hash = hash_bytes(&bytes);
        store.insert_chunk(hash, bytes.clone());
        for budget in [1, 63 * 1024, MAX_CHUNK_EXPORT_PART_BYTES] {
            let mut actual = Vec::new();
            while actual.len() < bytes.len() {
                let part = store
                    .copy_chunk_range(&hash, bytes.len(), actual.len(), budget)
                    .unwrap();
                assert!(!part.is_empty());
                assert!(part.len() <= budget);
                assert_eq!(
                    part.capacity(),
                    part.len(),
                    "range retained a whole-source backing allocation"
                );
                actual.extend_from_slice(&part);
            }
            assert_eq!(actual, bytes);
            assert!(store
                .copy_chunk_range(&hash, bytes.len(), bytes.len(), budget)
                .unwrap()
                .is_empty());
        }
        let mut part = store.copy_chunk_range(&hash, bytes.len(), 5, 1).unwrap();
        part[0] ^= 255;
        assert_eq!(store.get_chunk(&hash).unwrap(), bytes);
        // No borrow/view survives the read: replacement can occur immediately.
        store.insert_chunk(hash, vec![7]);
        assert_eq!(part[0], bytes[5] ^ 255);
    }

    #[test]
    fn chunk_export_range_refuses_invalid_budget_length_and_offsets() {
        let store = MemoryChunkStore::new();
        let hash = hash_bytes(b"source");
        store.insert_chunk(hash, b"source".to_vec());
        for budget in [0, MAX_CHUNK_EXPORT_PART_BYTES + 1, usize::MAX] {
            assert!(store.copy_chunk_range(&hash, 6, 0, budget).is_err());
        }
        for offset in [7, usize::MAX] {
            assert!(store.copy_chunk_range(&hash, 6, offset, 1).is_err());
        }
        assert!(store.copy_chunk_range(&hash, 5, 0, 1).is_err());
        assert!(store.copy_chunk_range(&hash, 7, 0, 1).is_err());
        assert!(store
            .copy_chunk_range(&hash, usize::MAX, usize::MAX, 1)
            .is_err());
        assert!(store
            .copy_chunk_range(&hash_bytes(b"missing"), 0, 0, 1)
            .is_err());
        let empty = hash_bytes(b"");
        store.insert_chunk(empty, Vec::new());
        assert!(store.copy_chunk_range(&empty, 0, 0, 1).unwrap().is_empty());
        assert_eq!(store.get_chunk(&hash).unwrap(), b"source");
    }

    #[tokio::test]
    async fn chunk_export_range_detects_removed_or_replaced_length_without_repair() {
        let store = MemoryChunkStore::new();
        let hash = hash_bytes(b"source");
        store.insert_chunk(hash, b"source".to_vec());
        assert_eq!(store.copy_chunk_range(&hash, 6, 0, 2).unwrap(), b"so");
        store.insert_chunk(hash, b"short".to_vec());
        assert!(store.copy_chunk_range(&hash, 6, 2, 2).is_err());
        assert_eq!(store.get_chunk(&hash).unwrap(), b"short");
        store.delete(&hash).await.unwrap();
        assert!(store.copy_chunk_range(&hash, 6, 2, 2).is_err());
        assert_eq!(store.get_chunk(&hash), None);
    }

    #[tokio::test]
    async fn memory_store_put_get() {
        let store = MemoryChunkStore::new();
        let hash = hash_bytes(b"test data");
        let data = b"test data".to_vec();

        store.put(hash, data.clone()).await.unwrap();
        assert!(store.has(&hash).await);

        let retrieved = store.get(&hash).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn memory_store_not_found() {
        let store = MemoryChunkStore::new();
        let hash = hash_bytes(b"nonexistent");
        assert!(!store.has(&hash).await);
        assert!(store.get(&hash).await.is_err());
    }

    #[tokio::test]
    async fn memory_store_delete() {
        let store = MemoryChunkStore::new();
        let hash = hash_bytes(b"to delete");
        store.put(hash, b"data".to_vec()).await.unwrap();
        assert!(store.has(&hash).await);

        store.delete(&hash).await.unwrap();
        assert!(!store.has(&hash).await);
    }

    #[tokio::test]
    async fn disk_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = DiskChunkStore::new(dir.path());
        let data = b"disk test data".to_vec();
        let hash = hash_bytes(&data);

        store.put(hash, data.clone()).await.unwrap();
        assert!(store.has(&hash).await);

        let retrieved = store.get(&hash).await.unwrap();
        assert_eq!(retrieved, data);

        store.delete(&hash).await.unwrap();
        assert!(!store.has(&hash).await);
    }

    #[tokio::test]
    async fn memory_store_default_is_empty() {
        let store = MemoryChunkStore::default();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }

    #[tokio::test]
    async fn memory_store_len_tracks_entries() {
        let store = MemoryChunkStore::new();
        assert!(store.is_empty());
        store.put(hash_bytes(b"a"), vec![1, 2, 3]).await.unwrap();
        store.put(hash_bytes(b"b"), vec![4, 5, 6]).await.unwrap();
        assert!(!store.is_empty());
        assert_eq!(store.len(), 2);
    }

    #[tokio::test]
    async fn memory_store_insert_get_helpers() {
        let store = MemoryChunkStore::new();
        let h = hash_bytes(b"helper");
        store.insert_chunk(h, vec![9, 9, 9]);
        assert_eq!(store.get_chunk(&h), Some(vec![9, 9, 9]));
        assert_eq!(store.get_chunk(&hash_bytes(b"missing")), None);
    }

    #[test]
    fn memory_store_overlay_promotion_is_validated_before_insertion() {
        let store = MemoryChunkStore::new();
        let resident_bytes = b"resident".to_vec();
        let resident_hash = hash_bytes(&resident_bytes);
        store.insert_chunk(resident_hash, resident_bytes.clone());

        let fresh_bytes = b"fresh".to_vec();
        let fresh_hash = hash_bytes(&fresh_bytes);
        let invalid_hash = hash_bytes(b"another object");
        let invalid = std::collections::HashMap::from([
            (fresh_hash, fresh_bytes.clone()),
            (invalid_hash, b"wrong bytes".to_vec()),
        ]);
        assert!(store.promote_verified_chunks(invalid).is_err());
        assert_eq!(store.get_chunk(&fresh_hash), None);
        assert_eq!(store.get_chunk(&resident_hash), Some(resident_bytes));

        store.insert_chunk(fresh_hash, b"corrupt resident collision".to_vec());
        let later_bytes = b"later".to_vec();
        let later_hash = hash_bytes(&later_bytes);
        let collision =
            std::collections::HashMap::from([(fresh_hash, fresh_bytes), (later_hash, later_bytes)]);
        assert!(store.promote_verified_chunks(collision).is_err());
        assert_eq!(store.get_chunk(&later_hash), None);
        assert_eq!(
            store.get_chunk(&fresh_hash),
            Some(b"corrupt resident collision".to_vec())
        );
    }

    #[test]
    fn memory_store_chunk_byte_length_distinguishes_empty_and_missing() {
        let store = MemoryChunkStore::new();
        let hash = hash_bytes(b"sized");
        assert_eq!(store.chunk_byte_length(&hash), None);
        store.insert_chunk(hash, Vec::new());
        assert_eq!(store.chunk_byte_length(&hash), Some(0));
        store.insert_chunk(hash, vec![42; 4096]);
        for _ in 0..100 {
            assert_eq!(store.chunk_byte_length(&hash), Some(4096));
        }
        assert_eq!(store.get_chunk(&hash), Some(vec![42; 4096]));
        assert_eq!(store.len(), 1);
    }

    #[tokio::test]
    async fn memory_store_all_chunks_lists_everything() {
        let store = MemoryChunkStore::new();
        let h1 = hash_bytes(b"a");
        let h2 = hash_bytes(b"b");
        store.insert_chunk(h1, vec![1]);
        store.insert_chunk(h2, vec![2]);

        let mut hashes = store.all_chunk_hashes();
        hashes.sort();
        let mut expected = vec![h1, h2];
        expected.sort();
        assert_eq!(hashes, expected);

        let all = store.all_chunks();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn memory_store_put_overwrites() {
        let store = MemoryChunkStore::new();
        let h = hash_bytes(b"k");
        store.put(h, vec![1]).await.unwrap();
        store.put(h, vec![2, 2]).await.unwrap();
        assert_eq!(store.get(&h).await.unwrap(), vec![2, 2]);
        assert_eq!(store.len(), 1);
    }

    #[tokio::test]
    async fn memory_store_delete_missing_is_ok() {
        let store = MemoryChunkStore::new();
        store.delete(&hash_bytes(b"never")).await.unwrap();
    }

    #[tokio::test]
    async fn disk_store_get_missing_returns_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let store = DiskChunkStore::new(dir.path());
        let err = store.get(&hash_bytes(b"absent")).await.unwrap_err();
        assert!(matches!(err, ChunkError::NotFound(_)));
        assert!(!store.has(&hash_bytes(b"absent")).await);
    }

    #[tokio::test]
    async fn disk_store_path_layout_uses_first_two_hex() {
        let dir = tempfile::tempdir().unwrap();
        let store = DiskChunkStore::new(dir.path());
        let data = b"layout".to_vec();
        let h = hash_bytes(&data);
        store.put(h, data).await.unwrap();
        // The expected on-disk path puts the first two hex chars in a sub-dir.
        let hex = hash_to_hex(&h);
        let expected = dir.path().join(&hex[..2]).join(&hex[2..]);
        assert!(expected.exists(), "expected blob at {:?}", expected);
    }

    #[tokio::test]
    async fn disk_store_rejects_and_repairs_a_corrupt_address() {
        let dir = tempfile::tempdir().unwrap();
        let store = DiskChunkStore::new(dir.path());
        let data = b"valid index node".to_vec();
        let hash = hash_bytes(&data);
        store.put(hash, data.clone()).await.unwrap();

        std::fs::write(store.chunk_path(&hash), b"same path, wrong bytes").unwrap();
        assert!(!store.has(&hash).await);
        assert!(matches!(
            store.get(&hash).await.unwrap_err(),
            ChunkError::Deserialize(_)
        ));

        store.put(hash, data.clone()).await.unwrap();
        assert_eq!(store.get(&hash).await.unwrap(), data);
        assert_eq!(
            std::fs::read_dir(store.chunk_path(&hash).parent().unwrap())
                .unwrap()
                .count(),
            1,
        );
    }

    #[tokio::test]
    async fn disk_store_delete_missing_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let store = DiskChunkStore::new(dir.path());
        // deleting a hash that was never written must not error.
        store.delete(&hash_bytes(b"ghost")).await.unwrap();
    }
}
