//! Indexed append-only storage for immutable sync objects.
//!
//! Every record carries a cheap CRC32C framing checksum while BLAKE3 remains
//! the semantic content address. Sealed segments have a sorted, checksummed
//! sidecar, so normal startup reads index metadata only. The one active
//! segment is scanned; an incomplete final record is truncated to the last
//! complete boundary, while corruption with intact framing is hidden rather
//! than returned.

use crate::perf::ServerPerfCounters;
use crate::storage::{read_blob_measured, StorageLayout};
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use sync_core::hash::{hash_bytes, hex_to_hash, FileHash};

const SEGMENT_MAGIC: &[u8; 4] = b"OSG1";
const SEGMENT_VERSION: u8 = 1;
const SEGMENT_HEADER_BYTES: usize = 32;
const RECORD_MAGIC: &[u8; 4] = b"OSR1";
const RECORD_VERSION: u8 = 1;
const RECORD_HEADER_BYTES: usize = 56;
const RECORD_TRAILER_BYTES: usize = 4;
const INDEX_MAGIC: &[u8; 4] = b"OSI1";
const INDEX_VERSION: u8 = 1;
const INDEX_HEADER_BYTES: usize = 48;
const INDEX_ENTRY_BYTES: usize = 64;
const INDEX_TRAILER_BYTES: usize = 32;

const SEGMENT_TARGET_BYTES: u64 = 256 * 1024 * 1024;
const SEGMENT_OBJECT_TARGET: usize = 262_144;
const MAX_SEGMENT_BYTES: u64 = 384 * 1024 * 1024;
const MAX_SEGMENT_OBJECTS: usize = SEGMENT_OBJECT_TARGET + 4096;
pub(crate) const MAX_PACK_OBJECT_BYTES: usize = 64 * 1024 * 1024;
const MAX_SIDECAR_BYTES: u64 = INDEX_HEADER_BYTES as u64
    + MAX_SEGMENT_OBJECTS as u64 * INDEX_ENTRY_BYTES as u64
    + INDEX_TRAILER_BYTES as u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum PackedObjectKind {
    Content = 0,
    ContentChunk = 1,
    IndexChunk = 2,
    Manifest = 3,
}

impl TryFrom<u8> for PackedObjectKind {
    type Error = std::io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Content),
            1 => Ok(Self::ContentChunk),
            2 => Ok(Self::IndexChunk),
            3 => Ok(Self::Manifest),
            _ => Err(invalid_data("unknown packed object kind")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PackedObject {
    pub kind: PackedObjectKind,
    pub hash: FileHash,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ObjectKey {
    kind: PackedObjectKind,
    hash: FileHash,
}

impl ObjectKey {
    fn new(kind: PackedObjectKind, hash: FileHash) -> Self {
        Self { kind, hash }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordLocation {
    segment_id: u64,
    record_offset: u64,
    plain_len: u64,
    stored_len: u64,
    record_crc32c: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IndexEntry {
    key: ObjectKey,
    location: RecordLocation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackReadError {
    Corrupt(String),
    Io(String),
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScrubStats {
    pub checked: u64,
    pub corrupted: u64,
}

impl std::fmt::Display for PackReadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Corrupt(message) => write!(formatter, "corrupt packed object: {message}"),
            Self::Io(message) => write!(formatter, "packed object I/O error: {message}"),
        }
    }
}

impl std::error::Error for PackReadError {}

#[derive(Clone)]
pub struct PackStore {
    inner: Arc<PackStoreInner>,
}

struct PackStoreInner {
    layout: StorageLayout,
    index: RwLock<HashMap<ObjectKey, RecordLocation>>,
    perf: Arc<ServerPerfCounters>,
}

pub(crate) struct ActiveSegment {
    id: u64,
    file: std::fs::File,
    len: u64,
    entries: Vec<IndexEntry>,
}

pub(crate) struct OpenedPackStore {
    pub store: PackStore,
    pub active: ActiveSegment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum CommitFault {
    None,
    AfterAppendBeforeSync,
    AfterSyncBeforePublish,
    AfterPublishBeforeReply,
}

impl PackStore {
    pub(crate) fn open(
        layout: StorageLayout,
        perf: Arc<ServerPerfCounters>,
    ) -> Result<OpenedPackStore, std::io::Error> {
        layout.init_directories()?;
        let store = Self {
            inner: Arc::new(PackStoreInner {
                layout: layout.clone(),
                index: RwLock::new(HashMap::new()),
                perf,
            }),
        };
        let mut segment_ids = list_segment_ids(&layout)?;
        segment_ids.sort_unstable();
        segment_ids.dedup();
        let mut active = None;

        for (position, segment_id) in segment_ids.iter().copied().enumerate() {
            let is_last = position + 1 == segment_ids.len();
            let pack_path = layout.object_segment_path(segment_id);
            let index_path = layout.object_segment_index_path(segment_id);
            let pack_len = std::fs::metadata(&pack_path)?.len();

            if is_last && !index_path.exists() && pack_len < SEGMENT_HEADER_BYTES as u64 {
                initialize_segment_file(&pack_path, segment_id)?;
                active = Some(open_active_segment(&pack_path, segment_id, Vec::new())?);
                continue;
            }
            validate_segment_header(&pack_path, segment_id)?;

            if index_path.exists() {
                let entries = match load_sidecar(&index_path, segment_id, pack_len) {
                    Ok(entries) => entries,
                    Err(_) => {
                        let scan = scan_segment(&pack_path, segment_id, false, store.perf())?;
                        write_sidecar(&layout, segment_id, scan.segment_len, &scan.entries)?;
                        scan.entries
                    }
                };
                store.publish_entries(&entries)?;
                continue;
            }

            if is_last {
                let scan = scan_segment(&pack_path, segment_id, true, store.perf())?;
                store.publish_entries(&scan.entries)?;
                active = Some(open_active_segment(&pack_path, segment_id, scan.entries)?);
            } else {
                let scan = scan_segment(&pack_path, segment_id, false, store.perf())?;
                write_sidecar(&layout, segment_id, scan.segment_len, &scan.entries)?;
                store.publish_entries(&scan.entries)?;
            }
        }

        let active = match active {
            Some(active) => active,
            None => {
                let next_id = segment_ids
                    .last()
                    .copied()
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or_else(|| invalid_data("segment id overflow"))?;
                create_active_segment(&layout, next_id)?
            }
        };
        Ok(OpenedPackStore { store, active })
    }

    pub fn contains(&self, kind: PackedObjectKind, hash: &FileHash) -> bool {
        let key = ObjectKey::new(kind, *hash);
        let hit = self
            .inner
            .index
            .read()
            .is_ok_and(|index| index.contains_key(&key));
        self.perf().record_index_lookup(hit);
        if hit {
            return true;
        }
        read_loose_verified(&self.inner.layout, kind, hash, self.perf()).is_some()
    }

    pub fn read(
        &self,
        kind: PackedObjectKind,
        hash: &FileHash,
    ) -> Result<Option<Vec<u8>>, PackReadError> {
        let key = ObjectKey::new(kind, *hash);
        let location = self
            .inner
            .index
            .read()
            .map_err(|_| PackReadError::Io("pack index lock poisoned".into()))?
            .get(&key)
            .copied();
        self.perf().record_index_lookup(location.is_some());

        if let Some(location) = location {
            match self.read_location(key, location) {
                Ok(bytes) => return Ok(Some(bytes)),
                Err(error) => {
                    self.hide_location(key, location);
                    if let Some(bytes) =
                        read_loose_verified(&self.inner.layout, kind, hash, self.perf())
                    {
                        return Ok(Some(bytes));
                    }
                    return Err(error);
                }
            }
        }
        Ok(read_loose_verified(
            &self.inner.layout,
            kind,
            hash,
            self.perf(),
        ))
    }

    pub(crate) fn read_pack_only(
        &self,
        kind: PackedObjectKind,
        hash: &FileHash,
    ) -> Result<Option<Vec<u8>>, PackReadError> {
        let key = ObjectKey::new(kind, *hash);
        let location = self
            .inner
            .index
            .read()
            .map_err(|_| PackReadError::Io("pack index lock poisoned".into()))?
            .get(&key)
            .copied();
        let Some(location) = location else {
            return Ok(None);
        };
        match self.read_location(key, location) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) => {
                self.hide_location(key, location);
                Err(error)
            }
        }
    }

    pub(crate) fn indexed(&self, kind: PackedObjectKind, hash: &FileHash) -> bool {
        self.inner
            .index
            .read()
            .is_ok_and(|index| index.contains_key(&ObjectKey::new(kind, *hash)))
    }

    pub(crate) fn layout(&self) -> &StorageLayout {
        &self.inner.layout
    }

    pub(crate) fn scrub_once(&self) -> ScrubStats {
        let entries: Vec<(ObjectKey, RecordLocation)> = self
            .inner
            .index
            .read()
            .map(|index| {
                index
                    .iter()
                    .map(|(key, location)| (*key, *location))
                    .collect()
            })
            .unwrap_or_default();
        let mut stats = ScrubStats::default();
        for (position, (key, location)) in entries.into_iter().enumerate() {
            stats.checked += 1;
            if self.read_location(key, location).is_err() {
                stats.corrupted += 1;
                self.hide_location(key, location);
            }
            if position % 256 == 255 {
                std::thread::yield_now();
            }
        }
        stats
    }

    pub(crate) fn append_group(
        &self,
        active: &mut ActiveSegment,
        objects: &[&PackedObject],
        fault: CommitFault,
    ) -> Result<(), std::io::Error> {
        if objects.is_empty() {
            return Ok(());
        }
        for object in objects {
            validate_object(object).map_err(invalid_data)?;
        }

        let group_start = active.len;
        active.file.seek(SeekFrom::Start(group_start))?;
        let write_started = Instant::now();
        let mut output = std::io::BufWriter::with_capacity(256 * 1024, &mut active.file);
        let mut entries = Vec::with_capacity(objects.len());
        let mut offset = group_start;
        for object in objects {
            let (entry, next_offset) = write_record(&mut output, active.id, offset, object)?;
            entries.push(entry);
            offset = next_offset;
        }
        output.flush()?;
        drop(output);
        let write_elapsed = write_started.elapsed();
        active.len = offset;

        if active.len > MAX_SEGMENT_BYTES {
            rollback_segment(active, group_start)?;
            return Err(invalid_data("active segment exceeds hard byte limit"));
        }
        if active.entries.len().saturating_add(entries.len()) > MAX_SEGMENT_OBJECTS {
            rollback_segment(active, group_start)?;
            return Err(invalid_data("active segment exceeds hard object limit"));
        }
        if fault == CommitFault::AfterAppendBeforeSync {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "injected crash before segment fdatasync",
            ));
        }

        let durability_started = Instant::now();
        active.file.sync_data()?;
        let durability_elapsed = durability_started.elapsed();
        let payload_bytes = objects.iter().try_fold(0u64, |total, object| {
            let len = u64::try_from(object.bytes.len())
                .map_err(|_| invalid_data("object length overflow"))?;
            total
                .checked_add(len)
                .ok_or_else(|| invalid_data("group payload length overflow"))
        })?;
        self.perf().record_pack_commit(
            objects.len() as u64,
            payload_bytes,
            write_elapsed,
            durability_elapsed,
        );
        if fault == CommitFault::AfterSyncBeforePublish {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "injected crash after segment fdatasync",
            ));
        }

        self.publish_entries(&entries)?;
        active.entries.extend(entries);
        if fault == CommitFault::AfterPublishBeforeReply {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "injected response loss after index publication",
            ));
        }

        if active.len >= SEGMENT_TARGET_BYTES || active.entries.len() >= SEGMENT_OBJECT_TARGET {
            self.seal_active(active)?;
        }
        Ok(())
    }

    pub(crate) fn seal_active(&self, active: &mut ActiveSegment) -> Result<(), std::io::Error> {
        let entries = self.current_entries_for_segment(active.id)?;
        write_sidecar(&self.inner.layout, active.id, active.len, &entries)?;
        let next_id = active
            .id
            .checked_add(1)
            .ok_or_else(|| invalid_data("segment id overflow"))?;
        *active = create_active_segment(&self.inner.layout, next_id)?;
        Ok(())
    }

    fn current_entries_for_segment(
        &self,
        segment_id: u64,
    ) -> Result<Vec<IndexEntry>, std::io::Error> {
        let index = self
            .inner
            .index
            .read()
            .map_err(|_| invalid_data("pack index lock poisoned"))?;
        Ok(index
            .iter()
            .filter_map(|(key, location)| {
                (location.segment_id == segment_id).then_some(IndexEntry {
                    key: *key,
                    location: *location,
                })
            })
            .collect())
    }

    fn publish_entries(&self, entries: &[IndexEntry]) -> Result<(), std::io::Error> {
        let mut index = self
            .inner
            .index
            .write()
            .map_err(|_| invalid_data("pack index lock poisoned"))?;
        for entry in entries {
            index.insert(entry.key, entry.location);
        }
        Ok(())
    }

    fn hide_location(&self, key: ObjectKey, location: RecordLocation) {
        let mut hidden = false;
        if let Ok(mut index) = self.inner.index.write() {
            if index.get(&key) == Some(&location) {
                index.remove(&key);
                hidden = true;
            }
        }
        if !hidden {
            return;
        }

        // A sealed sidecar is a reconstructible cache, not the source of
        // truth. Removing it makes the visibility loss survive restart: the
        // next open scans the segment, omits the corrupt record, and writes a
        // clean sidecar. The active segment has no sidecar, so this is a no-op
        // there.
        let sidecar = self
            .inner
            .layout
            .object_segment_index_path(location.segment_id);
        match std::fs::remove_file(&sidecar) {
            Ok(()) => {
                if let Err(error) =
                    sync_directory(self.inner.layout.object_segments_dir().as_path())
                {
                    tracing::warn!(
                        segment_id = location.segment_id,
                        %error,
                        "failed to sync persistent pack-corruption quarantine"
                    );
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => tracing::warn!(
                segment_id = location.segment_id,
                %error,
                "failed to remove sidecar for corrupt pack segment"
            ),
        }
    }

    fn read_location(
        &self,
        key: ObjectKey,
        location: RecordLocation,
    ) -> Result<Vec<u8>, PackReadError> {
        let path = self.inner.layout.object_segment_path(location.segment_id);
        let read_started = Instant::now();
        let mut file =
            std::fs::File::open(&path).map_err(|error| PackReadError::Io(error.to_string()))?;
        file.seek(SeekFrom::Start(location.record_offset))
            .map_err(|error| PackReadError::Io(error.to_string()))?;
        let mut header = [0u8; RECORD_HEADER_BYTES];
        file.read_exact(&mut header)
            .map_err(|error| PackReadError::Corrupt(error.to_string()))?;
        let decoded = decode_record_header(&header)
            .map_err(|error| PackReadError::Corrupt(error.to_string()))?;
        if decoded.key != key
            || decoded.plain_len != location.plain_len
            || decoded.stored_len != location.stored_len
        {
            return Err(PackReadError::Corrupt(
                "record header disagrees with index".into(),
            ));
        }
        let stored_len = usize::try_from(decoded.stored_len)
            .map_err(|_| PackReadError::Corrupt("stored length overflow".into()))?;
        if stored_len > MAX_PACK_OBJECT_BYTES {
            return Err(PackReadError::Corrupt(
                "stored length exceeds object cap".into(),
            ));
        }
        let mut bytes = vec![0u8; stored_len];
        file.read_exact(&mut bytes)
            .map_err(|error| PackReadError::Corrupt(error.to_string()))?;
        let mut trailer = [0u8; RECORD_TRAILER_BYTES];
        file.read_exact(&mut trailer)
            .map_err(|error| PackReadError::Corrupt(error.to_string()))?;
        let stored_crc = u32::from_le_bytes(trailer);
        let read_elapsed = read_started.elapsed();
        let verify_started = Instant::now();
        let mut crc = Crc32c::new();
        crc.update(&header);
        crc.update(&bytes);
        let valid = stored_crc == location.record_crc32c
            && stored_crc == crc.finalize()
            && validate_bytes(key.kind, &key.hash, &bytes).is_ok();
        let verify_elapsed = verify_started.elapsed();
        self.perf()
            .record_pack_read(bytes.len() as u64, read_elapsed, verify_elapsed, valid);
        if !valid {
            return Err(PackReadError::Corrupt(
                "record checksum or content address mismatch".into(),
            ));
        }
        Ok(bytes)
    }

    fn perf(&self) -> &ServerPerfCounters {
        self.inner.perf.as_ref()
    }
}

struct ScannedSegment {
    entries: Vec<IndexEntry>,
    segment_len: u64,
}

#[derive(Debug, Clone, Copy)]
struct DecodedRecordHeader {
    key: ObjectKey,
    plain_len: u64,
    stored_len: u64,
}

fn list_segment_ids(layout: &StorageLayout) -> Result<Vec<u64>, std::io::Error> {
    let mut ids = Vec::new();
    for entry in std::fs::read_dir(layout.object_segments_dir())? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(stem) = name.strip_suffix(".pack") else {
            continue;
        };
        if stem.len() == 16
            && stem
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            if let Ok(id) = u64::from_str_radix(stem, 16) {
                ids.push(id);
            }
        }
    }
    Ok(ids)
}

fn create_active_segment(
    layout: &StorageLayout,
    segment_id: u64,
) -> Result<ActiveSegment, std::io::Error> {
    let path = layout.object_segment_path(segment_id);
    let header = segment_header(segment_id);
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    file.write_all(&header)?;
    file.sync_all()?;
    sync_directory(layout.object_segments_dir().as_path())?;
    Ok(ActiveSegment {
        id: segment_id,
        file,
        len: SEGMENT_HEADER_BYTES as u64,
        entries: Vec::new(),
    })
}

fn initialize_segment_file(path: &Path, segment_id: u64) -> Result<(), std::io::Error> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)?;
    file.write_all(&segment_header(segment_id))?;
    file.sync_all()?;
    if let Some(parent) = path.parent() {
        sync_directory(parent)?;
    }
    Ok(())
}

fn open_active_segment(
    path: &Path,
    segment_id: u64,
    entries: Vec<IndexEntry>,
) -> Result<ActiveSegment, std::io::Error> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    let len = file.metadata()?.len();
    Ok(ActiveSegment {
        id: segment_id,
        file,
        len,
        entries,
    })
}

fn segment_header(segment_id: u64) -> [u8; SEGMENT_HEADER_BYTES] {
    let mut header = [0u8; SEGMENT_HEADER_BYTES];
    header[..4].copy_from_slice(SEGMENT_MAGIC);
    header[4] = SEGMENT_VERSION;
    header[8..16].copy_from_slice(&segment_id.to_le_bytes());
    let crc = crc32c(&header[..24]);
    header[24..28].copy_from_slice(&crc.to_le_bytes());
    header
}

fn validate_segment_header(path: &Path, segment_id: u64) -> Result<(), std::io::Error> {
    let mut file = std::fs::File::open(path)?;
    let mut header = [0u8; SEGMENT_HEADER_BYTES];
    file.read_exact(&mut header)?;
    if header[..4] != *SEGMENT_MAGIC
        || header[4] != SEGMENT_VERSION
        || header[5..8] != [0, 0, 0]
        || header[16..24] != [0; 8]
        || header[28..32] != [0; 4]
    {
        return Err(invalid_data("invalid segment header"));
    }
    if u64::from_le_bytes(header[8..16].try_into().unwrap()) != segment_id {
        return Err(invalid_data("segment id does not match filename"));
    }
    let stored_crc = u32::from_le_bytes(header[24..28].try_into().unwrap());
    if stored_crc != crc32c(&header[..24]) {
        return Err(invalid_data("segment header checksum mismatch"));
    }
    Ok(())
}

fn write_record(
    output: &mut impl Write,
    segment_id: u64,
    offset: u64,
    object: &PackedObject,
) -> Result<(IndexEntry, u64), std::io::Error> {
    let length =
        u64::try_from(object.bytes.len()).map_err(|_| invalid_data("object length overflow"))?;
    let mut header = [0u8; RECORD_HEADER_BYTES];
    header[..4].copy_from_slice(RECORD_MAGIC);
    header[4] = RECORD_VERSION;
    header[5] = object.kind as u8;
    header[8..40].copy_from_slice(&object.hash);
    header[40..48].copy_from_slice(&length.to_le_bytes());
    header[48..56].copy_from_slice(&length.to_le_bytes());
    let mut crc = Crc32c::new();
    crc.update(&header);
    crc.update(&object.bytes);
    let record_crc32c = crc.finalize();
    let raw_len = (RECORD_HEADER_BYTES as u64)
        .checked_add(length)
        .and_then(|value| value.checked_add(RECORD_TRAILER_BYTES as u64))
        .ok_or_else(|| invalid_data("record length overflow"))?;
    let padding = padding_for(raw_len);

    output.write_all(&header)?;
    output.write_all(&object.bytes)?;
    output.write_all(&record_crc32c.to_le_bytes())?;
    output.write_all(&[0u8; 7][..padding as usize])?;
    let next_offset = offset
        .checked_add(raw_len)
        .and_then(|value| value.checked_add(padding))
        .ok_or_else(|| invalid_data("record offset overflow"))?;
    Ok((
        IndexEntry {
            key: ObjectKey::new(object.kind, object.hash),
            location: RecordLocation {
                segment_id,
                record_offset: offset,
                plain_len: length,
                stored_len: length,
                record_crc32c,
            },
        },
        next_offset,
    ))
}

fn decode_record_header(
    header: &[u8; RECORD_HEADER_BYTES],
) -> Result<DecodedRecordHeader, std::io::Error> {
    if header[..4] != *RECORD_MAGIC || header[4] != RECORD_VERSION || header[6..8] != [0, 0] {
        return Err(invalid_data("invalid record header"));
    }
    let kind = PackedObjectKind::try_from(header[5])?;
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&header[8..40]);
    let plain_len = u64::from_le_bytes(header[40..48].try_into().unwrap());
    let stored_len = u64::from_le_bytes(header[48..56].try_into().unwrap());
    if plain_len != stored_len {
        return Err(invalid_data("compressed records are not implemented"));
    }
    if stored_len > MAX_PACK_OBJECT_BYTES as u64 {
        return Err(invalid_data("record exceeds object byte limit"));
    }
    Ok(DecodedRecordHeader {
        key: ObjectKey::new(kind, hash),
        plain_len,
        stored_len,
    })
}

fn scan_segment(
    path: &Path,
    segment_id: u64,
    allow_tail_truncate: bool,
    perf: &ServerPerfCounters,
) -> Result<ScannedSegment, std::io::Error> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(allow_tail_truncate)
        .open(path)?;
    let file_len = file.metadata()?.len();
    if file_len > MAX_SEGMENT_BYTES {
        return Err(invalid_data("segment exceeds hard byte limit"));
    }
    let mut offset = SEGMENT_HEADER_BYTES as u64;
    let mut entries = Vec::new();
    let mut records_seen = 0usize;

    while offset < file_len {
        records_seen = records_seen
            .checked_add(1)
            .ok_or_else(|| invalid_data("segment record count overflow"))?;
        if records_seen > MAX_SEGMENT_OBJECTS {
            return Err(invalid_data("segment exceeds hard object limit"));
        }
        let remaining = file_len - offset;
        if remaining < RECORD_HEADER_BYTES as u64 {
            if allow_tail_truncate {
                truncate_segment_tail(&mut file, offset)?;
                return Ok(ScannedSegment {
                    entries,
                    segment_len: offset,
                });
            }
            return Err(invalid_data("sealed segment has a truncated record header"));
        }
        file.seek(SeekFrom::Start(offset))?;
        let mut header = [0u8; RECORD_HEADER_BYTES];
        file.read_exact(&mut header)?;
        let decoded = decode_record_header(&header)?;
        let raw_len = (RECORD_HEADER_BYTES as u64)
            .checked_add(decoded.stored_len)
            .and_then(|value| value.checked_add(RECORD_TRAILER_BYTES as u64))
            .ok_or_else(|| invalid_data("record length overflow"))?;
        let padding = padding_for(raw_len);
        let next_offset = offset
            .checked_add(raw_len)
            .and_then(|value| value.checked_add(padding))
            .ok_or_else(|| invalid_data("record offset overflow"))?;
        if next_offset > file_len {
            if allow_tail_truncate {
                truncate_segment_tail(&mut file, offset)?;
                return Ok(ScannedSegment {
                    entries,
                    segment_len: offset,
                });
            }
            return Err(invalid_data("sealed segment has a truncated record"));
        }

        let stored_len = usize::try_from(decoded.stored_len)
            .map_err(|_| invalid_data("stored length overflow"))?;
        let mut bytes = vec![0u8; stored_len];
        file.read_exact(&mut bytes)?;
        let mut trailer = [0u8; RECORD_TRAILER_BYTES];
        file.read_exact(&mut trailer)?;
        let stored_crc = u32::from_le_bytes(trailer);
        let mut padding_bytes = [0u8; 7];
        file.read_exact(&mut padding_bytes[..padding as usize])?;
        let hash_started = Instant::now();
        let mut crc = Crc32c::new();
        crc.update(&header);
        crc.update(&bytes);
        let valid = stored_crc == crc.finalize()
            && padding_bytes[..padding as usize]
                .iter()
                .all(|byte| *byte == 0)
            && validate_bytes(decoded.key.kind, &decoded.key.hash, &bytes).is_ok();
        perf.record_hash_check(decoded.plain_len, hash_started.elapsed(), valid);
        if valid {
            entries.push(IndexEntry {
                key: decoded.key,
                location: RecordLocation {
                    segment_id,
                    record_offset: offset,
                    plain_len: decoded.plain_len,
                    stored_len: decoded.stored_len,
                    record_crc32c: stored_crc,
                },
            });
        }
        offset = next_offset;
    }
    Ok(ScannedSegment {
        entries,
        segment_len: offset,
    })
}

fn rollback_segment(active: &mut ActiveSegment, offset: u64) -> Result<(), std::io::Error> {
    active.file.set_len(offset)?;
    active.file.seek(SeekFrom::Start(offset))?;
    active.file.sync_data()?;
    active.len = offset;
    Ok(())
}

fn truncate_segment_tail(file: &mut std::fs::File, offset: u64) -> Result<(), std::io::Error> {
    file.set_len(offset)?;
    file.sync_data()?;
    file.seek(SeekFrom::Start(offset))?;
    Ok(())
}

fn padding_for(raw_len: u64) -> u64 {
    (8 - (raw_len % 8)) % 8
}

fn write_sidecar(
    layout: &StorageLayout,
    segment_id: u64,
    segment_len: u64,
    entries: &[IndexEntry],
) -> Result<(), std::io::Error> {
    let mut by_key = HashMap::with_capacity(entries.len());
    for entry in entries {
        by_key.insert(entry.key, *entry);
    }
    let mut canonical: Vec<IndexEntry> = by_key.into_values().collect();
    canonical.sort_unstable_by_key(|entry| entry.key);
    if canonical.len() > MAX_SEGMENT_OBJECTS {
        return Err(invalid_data("sidecar exceeds hard object limit"));
    }
    let count =
        u64::try_from(canonical.len()).map_err(|_| invalid_data("sidecar count overflow"))?;
    let entries_len = count
        .checked_mul(INDEX_ENTRY_BYTES as u64)
        .ok_or_else(|| invalid_data("sidecar byte length overflow"))?;
    let mut header = [0u8; INDEX_HEADER_BYTES];
    header[..4].copy_from_slice(INDEX_MAGIC);
    header[4] = INDEX_VERSION;
    header[8..16].copy_from_slice(&segment_id.to_le_bytes());
    header[16..24].copy_from_slice(&segment_len.to_le_bytes());
    header[24..32].copy_from_slice(&count.to_le_bytes());
    header[32..40].copy_from_slice(&entries_len.to_le_bytes());

    let final_path = layout.object_segment_index_path(segment_id);
    let temp_path = final_path.with_extension(format!(
        "idx-tmp-{}-{:016x}",
        std::process::id(),
        rand::random::<u64>()
    ));
    let result = (|| -> Result<(), std::io::Error> {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)?;
        let mut output = std::io::BufWriter::with_capacity(256 * 1024, &mut file);
        let mut checksum = blake3::Hasher::new();
        output.write_all(&header)?;
        checksum.update(&header);
        for entry in canonical {
            let encoded = encode_index_entry(entry);
            output.write_all(&encoded)?;
            checksum.update(&encoded);
        }
        output.write_all(checksum.finalize().as_bytes())?;
        output.flush()?;
        drop(output);
        file.sync_all()?;
        drop(file);
        replace_file(&temp_path, &final_path)?;
        sync_directory(layout.object_segments_dir().as_path())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp_path);
    }
    result
}

fn load_sidecar(
    path: &Path,
    segment_id: u64,
    segment_len: u64,
) -> Result<Vec<IndexEntry>, std::io::Error> {
    let file_len = std::fs::metadata(path)?.len();
    if file_len > MAX_SIDECAR_BYTES {
        return Err(invalid_data("sidecar exceeds hard byte limit"));
    }
    let bytes = std::fs::read(path)?;
    if bytes.len() < INDEX_HEADER_BYTES + INDEX_TRAILER_BYTES {
        return Err(invalid_data("sidecar is truncated"));
    }
    let header = &bytes[..INDEX_HEADER_BYTES];
    if header[..4] != *INDEX_MAGIC
        || header[4] != INDEX_VERSION
        || header[5..8] != [0, 0, 0]
        || header[40..48] != [0; 8]
    {
        return Err(invalid_data("invalid sidecar header"));
    }
    if u64::from_le_bytes(header[8..16].try_into().unwrap()) != segment_id
        || u64::from_le_bytes(header[16..24].try_into().unwrap()) != segment_len
    {
        return Err(invalid_data("sidecar does not match segment"));
    }
    let count_u64 = u64::from_le_bytes(header[24..32].try_into().unwrap());
    let count = usize::try_from(count_u64).map_err(|_| invalid_data("sidecar count overflow"))?;
    if count > MAX_SEGMENT_OBJECTS {
        return Err(invalid_data("sidecar exceeds hard object limit"));
    }
    let entries_len_u64 = u64::from_le_bytes(header[32..40].try_into().unwrap());
    let expected_entries_len = count_u64
        .checked_mul(INDEX_ENTRY_BYTES as u64)
        .ok_or_else(|| invalid_data("sidecar byte length overflow"))?;
    if entries_len_u64 != expected_entries_len {
        return Err(invalid_data("sidecar count/length mismatch"));
    }
    let entries_len = usize::try_from(entries_len_u64)
        .map_err(|_| invalid_data("sidecar byte length overflow"))?;
    let expected_len = INDEX_HEADER_BYTES
        .checked_add(entries_len)
        .and_then(|value| value.checked_add(INDEX_TRAILER_BYTES))
        .ok_or_else(|| invalid_data("sidecar total length overflow"))?;
    if bytes.len() != expected_len {
        return Err(invalid_data("sidecar file length mismatch"));
    }
    let trailer_start = INDEX_HEADER_BYTES + entries_len;
    let mut checksum = blake3::Hasher::new();
    checksum.update(&bytes[..trailer_start]);
    if checksum.finalize().as_bytes() != &bytes[trailer_start..] {
        return Err(invalid_data("sidecar checksum mismatch"));
    }

    let mut entries = Vec::with_capacity(count);
    let mut previous = None;
    for chunk in bytes[INDEX_HEADER_BYTES..trailer_start].chunks_exact(INDEX_ENTRY_BYTES) {
        let entry = decode_index_entry(chunk, segment_id, segment_len)?;
        if previous.is_some_and(|key| key >= entry.key) {
            return Err(invalid_data("sidecar entries are not strictly sorted"));
        }
        previous = Some(entry.key);
        entries.push(entry);
    }
    Ok(entries)
}

fn encode_index_entry(entry: IndexEntry) -> [u8; INDEX_ENTRY_BYTES] {
    let mut encoded = [0u8; INDEX_ENTRY_BYTES];
    encoded[0] = entry.key.kind as u8;
    encoded[1] = 1;
    encoded[4..36].copy_from_slice(&entry.key.hash);
    encoded[36..44].copy_from_slice(&entry.location.record_offset.to_le_bytes());
    encoded[44..52].copy_from_slice(&entry.location.plain_len.to_le_bytes());
    encoded[52..60].copy_from_slice(&entry.location.stored_len.to_le_bytes());
    encoded[60..64].copy_from_slice(&entry.location.record_crc32c.to_le_bytes());
    encoded
}

fn decode_index_entry(
    bytes: &[u8],
    segment_id: u64,
    segment_len: u64,
) -> Result<IndexEntry, std::io::Error> {
    if bytes.len() != INDEX_ENTRY_BYTES || bytes[1] != 1 || bytes[2..4] != [0, 0] {
        return Err(invalid_data("invalid sidecar entry"));
    }
    let kind = PackedObjectKind::try_from(bytes[0])?;
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&bytes[4..36]);
    let record_offset = u64::from_le_bytes(bytes[36..44].try_into().unwrap());
    let plain_len = u64::from_le_bytes(bytes[44..52].try_into().unwrap());
    let stored_len = u64::from_le_bytes(bytes[52..60].try_into().unwrap());
    let record_crc32c = u32::from_le_bytes(bytes[60..64].try_into().unwrap());
    if record_offset < SEGMENT_HEADER_BYTES as u64
        || record_offset >= segment_len
        || plain_len != stored_len
        || stored_len > MAX_PACK_OBJECT_BYTES as u64
    {
        return Err(invalid_data("sidecar entry is out of bounds"));
    }
    let record_end = record_offset
        .checked_add(RECORD_HEADER_BYTES as u64)
        .and_then(|value| value.checked_add(stored_len))
        .and_then(|value| value.checked_add(RECORD_TRAILER_BYTES as u64))
        .and_then(|value| {
            value.checked_add(padding_for(
                RECORD_HEADER_BYTES as u64 + stored_len + RECORD_TRAILER_BYTES as u64,
            ))
        })
        .ok_or_else(|| invalid_data("sidecar record bound overflow"))?;
    if record_end > segment_len {
        return Err(invalid_data("sidecar entry crosses segment end"));
    }
    Ok(IndexEntry {
        key: ObjectKey::new(kind, hash),
        location: RecordLocation {
            segment_id,
            record_offset,
            plain_len,
            stored_len,
            record_crc32c,
        },
    })
}

fn replace_file(temp: &Path, final_path: &Path) -> Result<(), std::io::Error> {
    if std::fs::rename(temp, final_path).is_ok() {
        return Ok(());
    }
    let backup = final_path.with_extension(format!(
        "idx-backup-{}-{:016x}",
        std::process::id(),
        rand::random::<u64>()
    ));
    let had_existing = final_path.exists();
    if had_existing {
        std::fs::rename(final_path, &backup)?;
    }
    if let Err(error) = std::fs::rename(temp, final_path) {
        if had_existing {
            let _ = std::fs::rename(&backup, final_path);
        }
        let _ = std::fs::remove_file(temp);
        return Err(error);
    }
    if had_existing {
        let _ = std::fs::remove_file(backup);
    }
    Ok(())
}

fn read_loose_verified(
    layout: &StorageLayout,
    kind: PackedObjectKind,
    hash: &FileHash,
    perf: &ServerPerfCounters,
) -> Option<Vec<u8>> {
    let path = loose_path(layout, kind, hash);
    let bytes = read_blob_measured(&path, perf)?;
    let started = Instant::now();
    let valid = validate_bytes(kind, hash, &bytes).is_ok();
    perf.record_hash_check(bytes.len() as u64, started.elapsed(), valid);
    valid.then_some(bytes)
}

pub(crate) fn loose_path(
    layout: &StorageLayout,
    kind: PackedObjectKind,
    hash: &FileHash,
) -> PathBuf {
    match kind {
        PackedObjectKind::Content => layout.content_blob_path(hash),
        PackedObjectKind::ContentChunk => layout.content_chunk_path(hash),
        PackedObjectKind::IndexChunk => layout.index_path(hash),
        PackedObjectKind::Manifest => layout.content_manifest_path(hash),
    }
}

pub(crate) fn validate_object(object: &PackedObject) -> Result<(), String> {
    validate_bytes(object.kind, &object.hash, &object.bytes)
}

fn validate_bytes(kind: PackedObjectKind, hash: &FileHash, bytes: &[u8]) -> Result<(), String> {
    if bytes.len() > MAX_PACK_OBJECT_BYTES {
        return Err("object exceeds pack byte limit".into());
    }
    if kind == PackedObjectKind::Manifest {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|_| "manifest is not valid JSON".to_string())?;
        let declared = value
            .get("file_hash")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "manifest has no file_hash".to_string())?;
        let declared =
            hex_to_hash(declared).map_err(|_| "manifest file_hash is invalid".to_string())?;
        if declared != *hash {
            return Err("manifest file_hash does not match its address".into());
        }
    } else if hash_bytes(bytes) != *hash {
        return Err("plaintext BLAKE3 does not match its address".into());
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        std::fs::File::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct Crc32c {
    state: u32,
}

impl Crc32c {
    fn new() -> Self {
        Self { state: !0 }
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            let index = ((self.state ^ u32::from(*byte)) & 0xff) as usize;
            self.state = CRC32C_TABLE[index] ^ (self.state >> 8);
        }
    }

    fn finalize(self) -> u32 {
        !self.state
    }
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = Crc32c::new();
    crc.update(bytes);
    crc.finalize()
}

const CRC32C_TABLE: [u32; 256] = crc32c_table();

const fn crc32c_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut index = 0usize;
    while index < table.len() {
        let mut value = index as u32;
        let mut bit = 0;
        while bit < 8 {
            value = if value & 1 == 1 {
                0x82f6_3b78 ^ (value >> 1)
            } else {
                value >> 1
            };
            bit += 1;
        }
        table[index] = value;
        index += 1;
    }
    table
}

fn invalid_data(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn content(seed: u64, size: usize) -> PackedObject {
        let mut bytes = vec![0u8; size];
        bytes[..8].copy_from_slice(&seed.to_le_bytes());
        let hash = hash_bytes(&bytes);
        PackedObject {
            kind: PackedObjectKind::Content,
            hash,
            bytes,
        }
    }

    fn open_temp() -> (tempfile::TempDir, Arc<ServerPerfCounters>, OpenedPackStore) {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        let perf = Arc::new(ServerPerfCounters::default());
        let opened = PackStore::open(layout, Arc::clone(&perf)).unwrap();
        (dir, perf, opened)
    }

    #[test]
    fn crc32c_matches_castagnoli_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn append_publish_read_and_pack_check_round_trip() {
        let (_dir, perf, mut opened) = open_temp();
        let object = content(1, 4096);
        opened
            .store
            .append_group(&mut opened.active, &[&object], CommitFault::None)
            .unwrap();

        assert!(opened.store.contains(object.kind, &object.hash));
        assert_eq!(
            opened.store.read(object.kind, &object.hash).unwrap(),
            Some(object.bytes)
        );
        let storage = perf.snapshot().storage;
        assert_eq!(storage.pack_appends, 1);
        assert_eq!(storage.fdatasyncs, 1);
        assert_eq!(storage.pack_reads, 1);
        assert_eq!(storage.loose_reads, 0);
    }

    #[test]
    fn fdatasynced_unpublished_record_is_recovered_from_active_tail() {
        let (dir, _perf, mut opened) = open_temp();
        let object = content(2, 2048);
        let error = opened
            .store
            .append_group(
                &mut opened.active,
                &[&object],
                CommitFault::AfterSyncBeforePublish,
            )
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
        assert!(!opened.store.indexed(object.kind, &object.hash));
        drop(opened);

        let recovered = PackStore::open(
            StorageLayout::new(dir.path()),
            Arc::new(ServerPerfCounters::default()),
        )
        .unwrap();
        assert_eq!(
            recovered.store.read(object.kind, &object.hash).unwrap(),
            Some(object.bytes)
        );
    }

    #[test]
    fn torn_active_record_is_truncated_to_previous_boundary() {
        let (dir, _perf, mut opened) = open_temp();
        let object = content(3, 2048);
        opened
            .store
            .append_group(
                &mut opened.active,
                &[&object],
                CommitFault::AfterAppendBeforeSync,
            )
            .unwrap_err();
        let path = StorageLayout::new(dir.path()).object_segment_path(1);
        let length = std::fs::metadata(&path).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(length - 7)
            .unwrap();
        drop(opened);

        let recovered = PackStore::open(
            StorageLayout::new(dir.path()),
            Arc::new(ServerPerfCounters::default()),
        )
        .unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            SEGMENT_HEADER_BYTES as u64
        );
        assert!(!recovered.store.contains(object.kind, &object.hash));
    }

    #[test]
    fn recovery_skips_framed_corruption_and_keeps_later_valid_records() {
        let (dir, _perf, mut opened) = open_temp();
        let first = content(31, 2048);
        let second = content(32, 4096);
        opened
            .store
            .append_group(&mut opened.active, &[&first, &second], CommitFault::None)
            .unwrap();
        let path = StorageLayout::new(dir.path()).object_segment_path(1);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        file.seek(SeekFrom::Start(
            SEGMENT_HEADER_BYTES as u64 + RECORD_HEADER_BYTES as u64 + 17,
        ))
        .unwrap();
        file.write_all(&[0xa5]).unwrap();
        file.sync_data().unwrap();
        drop(file);
        drop(opened);

        let recovered = PackStore::open(
            StorageLayout::new(dir.path()),
            Arc::new(ServerPerfCounters::default()),
        )
        .unwrap();
        assert!(!recovered.store.contains(first.kind, &first.hash));
        assert_eq!(
            recovered.store.read(second.kind, &second.hash).unwrap(),
            Some(second.bytes)
        );
    }

    #[test]
    fn sealed_sidecar_starts_without_rehashing_segment_payload() {
        let (dir, _perf, mut opened) = open_temp();
        let object = content(4, 8192);
        opened
            .store
            .append_group(&mut opened.active, &[&object], CommitFault::None)
            .unwrap();
        opened.store.seal_active(&mut opened.active).unwrap();
        drop(opened);

        let restart_perf = Arc::new(ServerPerfCounters::default());
        let recovered =
            PackStore::open(StorageLayout::new(dir.path()), Arc::clone(&restart_perf)).unwrap();
        assert!(recovered.store.contains(object.kind, &object.hash));
        let storage = restart_perf.snapshot().storage;
        assert_eq!(storage.bytes_rehashed, 0);
        assert_eq!(storage.pack_reads, 0);
        assert_eq!(storage.index_hits, 1);
    }

    #[test]
    fn corrupt_sidecar_is_rebuilt_from_verified_segment_records() {
        let (dir, _perf, mut opened) = open_temp();
        let object = content(5, 4096);
        opened
            .store
            .append_group(&mut opened.active, &[&object], CommitFault::None)
            .unwrap();
        opened.store.seal_active(&mut opened.active).unwrap();
        drop(opened);
        let layout = StorageLayout::new(dir.path());
        let index_path = layout.object_segment_index_path(1);
        let mut sidecar = std::fs::read(&index_path).unwrap();
        sidecar[INDEX_HEADER_BYTES + 7] ^= 0x80;
        std::fs::write(&index_path, sidecar).unwrap();

        let recovered = PackStore::open(layout, Arc::new(ServerPerfCounters::default())).unwrap();
        assert_eq!(
            recovered.store.read(object.kind, &object.hash).unwrap(),
            Some(object.bytes)
        );
    }

    #[test]
    fn corrupt_payload_is_hidden_and_never_returned() {
        let (dir, perf, mut opened) = open_temp();
        let object = content(6, 4096);
        opened
            .store
            .append_group(&mut opened.active, &[&object], CommitFault::None)
            .unwrap();
        let path = StorageLayout::new(dir.path()).object_segment_path(1);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        file.seek(SeekFrom::Start(
            SEGMENT_HEADER_BYTES as u64 + RECORD_HEADER_BYTES as u64 + 9,
        ))
        .unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_data().unwrap();

        assert!(matches!(
            opened.store.read(object.kind, &object.hash),
            Err(PackReadError::Corrupt(_))
        ));
        assert!(!opened.store.contains(object.kind, &object.hash));
        assert_eq!(perf.snapshot().storage.corrupted_records, 1);
    }

    #[test]
    fn sealed_index_never_turns_corrupt_payload_into_readable_content() {
        let (dir, _perf, mut opened) = open_temp();
        let object = content(60, 4096);
        opened
            .store
            .append_group(&mut opened.active, &[&object], CommitFault::None)
            .unwrap();
        opened.store.seal_active(&mut opened.active).unwrap();
        drop(opened);

        let layout = StorageLayout::new(dir.path());
        let path = layout.object_segment_path(1);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        file.seek(SeekFrom::Start(
            SEGMENT_HEADER_BYTES as u64 + RECORD_HEADER_BYTES as u64 + 23,
        ))
        .unwrap();
        file.write_all(&[0x5a]).unwrap();
        file.sync_data().unwrap();
        drop(file);

        let recovered = PackStore::open(layout, Arc::new(ServerPerfCounters::default())).unwrap();
        assert!(recovered.store.contains(object.kind, &object.hash));
        assert!(matches!(
            recovered.store.read(object.kind, &object.hash),
            Err(PackReadError::Corrupt(_))
        ));
        assert!(!recovered.store.contains(object.kind, &object.hash));
        assert!(!StorageLayout::new(dir.path())
            .object_segment_index_path(1)
            .exists());
        drop(recovered);

        let rebuilt = PackStore::open(
            StorageLayout::new(dir.path()),
            Arc::new(ServerPerfCounters::default()),
        )
        .unwrap();
        assert!(!rebuilt.store.contains(object.kind, &object.hash));
    }

    #[test]
    fn background_scrub_hides_corruption_before_the_next_check() {
        let (dir, _perf, mut opened) = open_temp();
        let object = content(61, 4096);
        opened
            .store
            .append_group(&mut opened.active, &[&object], CommitFault::None)
            .unwrap();
        let path = StorageLayout::new(dir.path()).object_segment_path(1);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        file.seek(SeekFrom::Start(
            SEGMENT_HEADER_BYTES as u64 + RECORD_HEADER_BYTES as u64 + 11,
        ))
        .unwrap();
        file.write_all(&[0xee]).unwrap();
        file.sync_data().unwrap();

        let stats = opened.store.scrub_once();
        assert_eq!(stats.checked, 1);
        assert_eq!(stats.corrupted, 1);
        assert!(!opened.store.contains(object.kind, &object.hash));
    }

    #[test]
    fn oversized_sidecar_is_rejected_before_allocating_its_contents() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("oversized.idx");
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(MAX_SIDECAR_BYTES + 1).unwrap();
        let error = load_sidecar(&path, 1, SEGMENT_HEADER_BYTES as u64).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn response_loss_after_publication_is_idempotently_visible() {
        let (_dir, _perf, mut opened) = open_temp();
        let object = content(7, 1024);
        opened
            .store
            .append_group(
                &mut opened.active,
                &[&object],
                CommitFault::AfterPublishBeforeReply,
            )
            .unwrap_err();
        assert!(opened.store.indexed(object.kind, &object.hash));
        assert_eq!(
            opened.store.read(object.kind, &object.hash).unwrap(),
            Some(object.bytes)
        );
    }
}
