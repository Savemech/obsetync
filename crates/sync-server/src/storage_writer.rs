//! Bounded dedicated writer for immutable content-addressed objects.
//!
//! The writer is the only append owner for the active pack segment. API
//! callers reserve queue bytes before enqueueing, objects are hash-verified
//! and deduplicated, and a whole group becomes visible only after the segment
//! crosses one `fdatasync` barrier.

use crate::pack_store::{
    validate_object, ActiveSegment, CommitFault, OpenedPackStore, PackReadError, PackStore,
};
use crate::perf::ServerPerfCounters;
use crate::storage::StorageLayout;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sync_core::hash::FileHash;
use sync_core::hash::{hash_to_hex, hex_to_hash};
use tokio::sync::oneshot;

pub use crate::pack_store::{PackedObject as DurableObject, PackedObjectKind as StorageObjectKind};

const CHANNEL_MESSAGES: usize = 64;
const QUEUE_BYTES: u64 = 64 * 1024 * 1024;
const GROUP_TARGET_BYTES: u64 = 16 * 1024 * 1024;
const GROUP_LATENCY: Duration = Duration::from_millis(5);
const GROUP_OBJECT_TARGET: usize = 4096;
const MAX_BATCH_OBJECTS: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreOutcome {
    Stored,
    AlreadyPresent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    Busy,
    Closed,
    InvalidObject(String),
    Io(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => formatter.write_str("storage writer queue is full"),
            Self::Closed => formatter.write_str("storage writer is unavailable"),
            Self::InvalidObject(message) => write!(formatter, "invalid object: {message}"),
            Self::Io(message) => write!(formatter, "storage writer I/O error: {message}"),
        }
    }
}

impl std::error::Error for StoreError {}

pub type StoreResult = Result<StoreOutcome, StoreError>;

#[derive(Clone)]
pub struct StorageWriter {
    inner: Arc<WriterHandle>,
}

struct WriterHandle {
    sender: SyncSender<WriterMessage>,
    queued_bytes: AtomicU64,
    perf: Arc<ServerPerfCounters>,
    store: PackStore,
}

struct QueueReservation {
    handle: Arc<WriterHandle>,
    bytes: u64,
    objects: u64,
}

impl Drop for QueueReservation {
    fn drop(&mut self) {
        self.handle
            .queued_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
        self.handle.perf.record_writer_queue_remove(self.objects);
    }
}

struct WriterMessage {
    objects: Vec<DurableObject>,
    reply: oneshot::Sender<Vec<StoreResult>>,
    reservation: QueueReservation,
}

struct FlatObject {
    message_index: usize,
    object_index: usize,
    object: DurableObject,
}

struct CommitReport {
    results: Vec<StoreResult>,
    fatal: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LooseImportStats {
    pub scanned: usize,
    pub imported: usize,
    pub invalid: usize,
    pub retained: usize,
}

struct LooseCandidate {
    kind: StorageObjectKind,
    hash: FileHash,
    path: PathBuf,
    bytes: Vec<u8>,
}

impl StorageWriter {
    /// Open sidecar indexes and recover the active segment synchronously
    /// before serving, then hand the active append handle to one OS thread.
    pub fn start(
        layout: StorageLayout,
        perf: Arc<ServerPerfCounters>,
    ) -> Result<Self, std::io::Error> {
        let OpenedPackStore { store, mut active } = PackStore::open(layout, Arc::clone(&perf))?;
        migrate_legacy_journal(store.layout(), &store, &mut active)?;
        let (sender, receiver) = mpsc::sync_channel(CHANNEL_MESSAGES);
        let inner = Arc::new(WriterHandle {
            sender,
            queued_bytes: AtomicU64::new(0),
            perf,
            store: store.clone(),
        });
        std::thread::Builder::new()
            .name("obsetync-storage-writer".into())
            .spawn(move || writer_loop(receiver, active, store))?;
        let writer = Self { inner };
        #[cfg(not(test))]
        {
            writer.start_loose_importer();
            writer.start_scrubber();
        }
        Ok(writer)
    }

    /// Enqueue one logical API batch without waiting for memory. Refusing a
    /// full byte/message queue immediately keeps authenticated request bodies
    /// from accumulating behind a slow disk.
    pub async fn store_batch(
        &self,
        objects: Vec<DurableObject>,
    ) -> Result<Vec<StoreResult>, StoreError> {
        if objects.is_empty() {
            return Ok(Vec::new());
        }
        if objects.len() > MAX_BATCH_OBJECTS {
            return Err(StoreError::InvalidObject(format!(
                "batch has {} objects; maximum is {MAX_BATCH_OBJECTS}",
                objects.len()
            )));
        }
        let bytes = objects.iter().try_fold(0u64, |total, object| {
            let object_bytes = u64::try_from(object.bytes.len())
                .map_err(|_| StoreError::InvalidObject("object length overflow".into()))?;
            total
                .checked_add(object_bytes.max(1))
                .ok_or_else(|| StoreError::InvalidObject("batch byte length overflow".into()))
        })?;
        let reservation = self.reserve(bytes, objects.len() as u64)?;
        let (reply, receive) = oneshot::channel();
        let message = WriterMessage {
            objects,
            reply,
            reservation,
        };
        match self.inner.sender.try_send(message) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => return Err(StoreError::Busy),
            Err(TrySendError::Disconnected(_)) => return Err(StoreError::Closed),
        }
        receive.await.map_err(|_| StoreError::Closed)
    }

    pub fn contains(&self, kind: StorageObjectKind, hash: &FileHash) -> bool {
        self.inner.store.contains(kind, hash)
    }

    pub fn read(
        &self,
        kind: StorageObjectKind,
        hash: &FileHash,
    ) -> Result<Option<Vec<u8>>, PackReadError> {
        self.inner.store.read(kind, hash)
    }

    #[cfg(test)]
    pub(crate) fn pack_store(&self) -> PackStore {
        self.inner.store.clone()
    }

    #[cfg(not(test))]
    fn start_loose_importer(&self) {
        let writer = self.clone();
        let _ = std::thread::Builder::new()
            .name("obsetync-loose-importer".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        tracing::error!(%error, "loose importer runtime failed");
                        return;
                    }
                };
                match runtime.block_on(writer.import_loose_objects(None)) {
                    Ok(stats) if stats.scanned > 0 => tracing::info!(
                        scanned = stats.scanned,
                        imported = stats.imported,
                        invalid = stats.invalid,
                        retained = stats.retained,
                        "loose object migration pass finished"
                    ),
                    Ok(_) => {}
                    Err(error) => tracing::warn!(%error, "loose object migration paused"),
                }
            });
    }

    #[cfg(not(test))]
    fn start_scrubber(&self) {
        let handle = Arc::downgrade(&self.inner);
        let _ = std::thread::Builder::new()
            .name("obsetync-pack-scrubber".into())
            .spawn(move || loop {
                std::thread::sleep(Duration::from_secs(6 * 60 * 60));
                let Some(handle) = handle.upgrade() else {
                    break;
                };
                let stats = handle.store.scrub_once();
                if stats.corrupted > 0 {
                    tracing::error!(
                        checked = stats.checked,
                        corrupted = stats.corrupted,
                        "pack scrub hid corrupted object records"
                    );
                } else {
                    tracing::info!(checked = stats.checked, "pack scrub finished");
                }
                drop(handle);
            });
    }

    pub(crate) async fn import_loose_objects(
        &self,
        max_objects: Option<usize>,
    ) -> Result<LooseImportStats, StoreError> {
        let layout = self.inner.store.layout().clone();
        let roots = [
            (layout.base.join("index"), StorageObjectKind::IndexChunk),
            (layout.base.join("content"), StorageObjectKind::Content),
            (
                layout.base.join("content/chunks"),
                StorageObjectKind::ContentChunk,
            ),
            (
                layout.base.join("content/manifests"),
                StorageObjectKind::Manifest,
            ),
        ];
        let mut stats = LooseImportStats::default();
        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;

        'roots: for (root, kind) in roots {
            let prefixes = sorted_directory_entries(&root).map_err(|error| {
                StoreError::Io(format!("read loose root {}: {error}", root.display()))
            })?;
            for prefix in prefixes {
                let Some(prefix_name) = prefix.file_name().to_str().map(str::to_owned) else {
                    continue;
                };
                if prefix_name.len() != 2
                    || !is_lower_hex(&prefix_name)
                    || !prefix
                        .file_type()
                        .map_err(|error| StoreError::Io(error.to_string()))?
                        .is_dir()
                {
                    continue;
                }
                let files = sorted_directory_entries(&prefix.path())
                    .map_err(|error| StoreError::Io(error.to_string()))?;
                for file in files {
                    if max_objects.is_some_and(|limit| stats.scanned >= limit) {
                        break 'roots;
                    }
                    if !file
                        .file_type()
                        .map_err(|error| StoreError::Io(error.to_string()))?
                        .is_file()
                    {
                        continue;
                    }
                    let Some(file_name) = file.file_name().to_str().map(str::to_owned) else {
                        continue;
                    };
                    if file_name.len() != 62 || !is_lower_hex(&file_name) {
                        continue;
                    }
                    let hash = match hex_to_hash(&format!("{prefix_name}{file_name}")) {
                        Ok(hash) => hash,
                        Err(_) => continue,
                    };
                    let path = file.path();
                    let bytes = match read_loose_bounded(&path) {
                        Ok(Some(bytes)) => bytes,
                        Ok(None) => {
                            stats.invalid += 1;
                            stats.scanned += 1;
                            continue;
                        }
                        Err(error) => {
                            tracing::warn!(path = %path.display(), %error, "loose importer read failed");
                            stats.retained += 1;
                            stats.scanned += 1;
                            continue;
                        }
                    };
                    stats.scanned += 1;
                    if !batch.is_empty()
                        && (batch.len() >= 256
                            || batch_bytes.saturating_add(bytes.len()) > 8 * 1024 * 1024)
                    {
                        self.flush_loose_batch(&mut batch, &mut stats).await?;
                        batch_bytes = 0;
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    batch_bytes = batch_bytes.saturating_add(bytes.len());
                    batch.push(LooseCandidate {
                        kind,
                        hash,
                        path,
                        bytes,
                    });
                }
            }
        }
        self.flush_loose_batch(&mut batch, &mut stats).await?;
        Ok(stats)
    }

    async fn flush_loose_batch(
        &self,
        candidates: &mut Vec<LooseCandidate>,
        stats: &mut LooseImportStats,
    ) -> Result<(), StoreError> {
        if candidates.is_empty() {
            return Ok(());
        }
        let mut pending = std::mem::take(candidates);
        let objects = pending
            .iter_mut()
            .map(|candidate| DurableObject {
                kind: candidate.kind,
                hash: candidate.hash,
                bytes: std::mem::take(&mut candidate.bytes),
            })
            .collect();
        let results = self.store_batch(objects).await?;
        if results.len() != pending.len() {
            return Err(StoreError::Closed);
        }
        let mut synced_parents = HashSet::new();
        for (candidate, result) in pending.into_iter().zip(results) {
            match result {
                Ok(_) => match self
                    .inner
                    .store
                    .read_pack_only(candidate.kind, &candidate.hash)
                {
                    Ok(Some(_)) => match std::fs::remove_file(&candidate.path) {
                        Ok(()) => {
                            stats.imported += 1;
                            if let Some(parent) = candidate.path.parent() {
                                synced_parents.insert(parent.to_path_buf());
                            }
                        }
                        Err(error) => {
                            tracing::warn!(path = %candidate.path.display(), %error, "loose importer delete failed");
                            stats.retained += 1;
                        }
                    },
                    Ok(None) | Err(_) => stats.retained += 1,
                },
                Err(StoreError::InvalidObject(_)) => stats.invalid += 1,
                Err(_) => stats.retained += 1,
            }
        }
        for parent in synced_parents {
            sync_directory(&parent).map_err(|error| StoreError::Io(error.to_string()))?;
        }
        Ok(())
    }

    fn reserve(&self, bytes: u64, objects: u64) -> Result<QueueReservation, StoreError> {
        if bytes > QUEUE_BYTES {
            return Err(StoreError::Busy);
        }
        let mut current = self.inner.queued_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return Err(StoreError::Busy);
            };
            if next > QUEUE_BYTES {
                return Err(StoreError::Busy);
            }
            match self.inner.queued_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        self.inner.perf.record_writer_queue_add(objects);
        Ok(QueueReservation {
            handle: Arc::clone(&self.inner),
            bytes,
            objects,
        })
    }
}

fn sorted_directory_entries(path: &Path) -> Result<Vec<std::fs::DirEntry>, std::io::Error> {
    let mut entries = std::fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_unstable_by_key(std::fs::DirEntry::file_name);
    Ok(entries)
}

/// Read at most one byte beyond the object cap. Metadata is only a fast
/// rejection: the bounded reader remains authoritative if the file grows
/// between stat and read.
fn read_loose_bounded(path: &Path) -> Result<Option<Vec<u8>>, std::io::Error> {
    let file = std::fs::File::open(path)?;
    if file.metadata()?.len() > crate::pack_store::MAX_PACK_OBJECT_BYTES as u64 {
        return Ok(None);
    }
    let limit = (crate::pack_store::MAX_PACK_OBJECT_BYTES as u64)
        .checked_add(1)
        .ok_or_else(|| invalid_data("loose object read limit overflow"))?;
    let mut bytes = Vec::new();
    file.take(limit).read_to_end(&mut bytes)?;
    if bytes.len() > crate::pack_store::MAX_PACK_OBJECT_BYTES {
        return Ok(None);
    }
    Ok(Some(bytes))
}

fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
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

fn writer_loop(receiver: Receiver<WriterMessage>, mut active: ActiveSegment, store: PackStore) {
    while let Ok(first) = receiver.recv() {
        let deadline = Instant::now() + GROUP_LATENCY;
        let mut messages = vec![first];
        let mut bytes = message_bytes(&messages[0]);
        let mut objects = messages[0].objects.len();
        let mut disconnected = false;
        // A binary bulk request is already a useful durability group and must
        // not pay an artificial latency timer. Singleton legacy requests wait
        // briefly so concurrent callers can share one barrier.
        let coalesce_singletons = objects == 1;

        while coalesce_singletons && bytes < GROUP_TARGET_BYTES && objects < GROUP_OBJECT_TARGET {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match receiver.recv_timeout(remaining) {
                Ok(message) => {
                    bytes = bytes.saturating_add(message_bytes(&message));
                    objects = objects.saturating_add(message.objects.len());
                    messages.push(message);
                }
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }

        let result_lengths: Vec<usize> = messages
            .iter()
            .map(|message| message.objects.len())
            .collect();
        let mut flat = Vec::with_capacity(result_lengths.iter().sum());
        for (message_index, message) in messages.iter_mut().enumerate() {
            for (object_index, object) in
                std::mem::take(&mut message.objects).into_iter().enumerate()
            {
                flat.push(FlatObject {
                    message_index,
                    object_index,
                    object,
                });
            }
        }
        let report = commit_objects(&store, &mut active, &flat);
        let mut separated: Vec<Vec<StoreResult>> = result_lengths
            .into_iter()
            .map(|length| vec![Err(StoreError::Closed); length])
            .collect();
        for (flat_object, result) in flat.iter().zip(report.results) {
            separated[flat_object.message_index][flat_object.object_index] = result;
        }
        for (message, results) in messages.into_iter().zip(separated) {
            let WriterMessage {
                reply, reservation, ..
            } = message;
            drop(reservation);
            let _ = reply.send(results);
        }
        if report.fatal || disconnected {
            break;
        }
    }
}

fn message_bytes(message: &WriterMessage) -> u64 {
    message
        .objects
        .iter()
        .map(|object| u64::try_from(object.bytes.len()).unwrap_or(u64::MAX).max(1))
        .sum()
}

fn commit_objects(
    store: &PackStore,
    active: &mut ActiveSegment,
    objects: &[FlatObject],
) -> CommitReport {
    let mut results = vec![Err(StoreError::Closed); objects.len()];
    let mut primary_by_key: HashMap<(StorageObjectKind, FileHash), usize> = HashMap::new();
    let mut pending = Vec::<usize>::new();
    let mut aliases = Vec::<(usize, usize)>::new();

    for (index, flat) in objects.iter().enumerate() {
        if let Err(error) = validate_object(&flat.object) {
            results[index] = Err(StoreError::InvalidObject(error));
            continue;
        }
        if store.indexed(flat.object.kind, &flat.object.hash) {
            results[index] = Ok(StoreOutcome::AlreadyPresent);
            continue;
        }
        let key = (flat.object.kind, flat.object.hash);
        if let Some(primary) = primary_by_key.get(&key).copied() {
            aliases.push((index, primary));
            continue;
        }
        primary_by_key.insert(key, index);
        pending.push(index);
    }

    if pending.is_empty() {
        return CommitReport {
            results,
            fatal: false,
        };
    }
    let pending_objects: Vec<&DurableObject> = pending
        .iter()
        .map(|index| &objects[*index].object)
        .collect();
    if let Err(error) = store.append_group(active, &pending_objects, CommitFault::None) {
        let error = StoreError::Io(error.to_string());
        for index in &pending {
            results[*index] = Err(error.clone());
        }
        for (alias, _) in &aliases {
            results[*alias] = Err(error.clone());
        }
        return CommitReport {
            results,
            fatal: true,
        };
    }

    for index in pending {
        results[index] = Ok(StoreOutcome::Stored);
    }
    for (alias, primary) in aliases {
        results[alias] = match &results[primary] {
            Ok(_) => Ok(StoreOutcome::AlreadyPresent),
            Err(error) => Err(error.clone()),
        };
    }
    CommitReport {
        results,
        fatal: false,
    }
}

const LEGACY_MAGIC: &[u8; 4] = b"OWG1";
const LEGACY_VERSION: u8 = 1;
const LEGACY_GROUP_HEADER_BYTES: usize = 20;
const LEGACY_RECORD_HEADER_BYTES: usize = 44;
const LEGACY_GROUP_TRAILER_BYTES: usize = 32;
const LEGACY_MAX_GROUP_OBJECTS: usize = 8192;
const LEGACY_MAX_GROUP_BYTES: usize = 80 * 1024 * 1024;

/// Slice 6 used a group journal plus loose mirrors. Import it before the API
/// opens because an acknowledged record may exist only in the journal after a
/// crash between fdatasync and mirror publication.
fn migrate_legacy_journal(
    layout: &StorageLayout,
    store: &PackStore,
    active: &mut ActiveSegment,
) -> Result<(), std::io::Error> {
    let path = layout.storage_writer_journal_path();
    if !path.exists() {
        return Ok(());
    }
    let mut journal = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)?;
    let file_len = journal.metadata()?.len();
    let mut offset = 0u64;
    while offset < file_len {
        journal.seek(SeekFrom::Start(offset))?;
        let mut header = [0u8; LEGACY_GROUP_HEADER_BYTES];
        let header_read = read_up_to(&mut journal, &mut header)?;
        if header_read < LEGACY_GROUP_HEADER_BYTES {
            truncate_legacy_tail(&mut journal, offset)?;
            break;
        }
        if header[..4] != *LEGACY_MAGIC || header[4] != LEGACY_VERSION || header[5..8] != [0, 0, 0]
        {
            return Err(invalid_data("legacy storage journal header is corrupt"));
        }
        let count = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
        let payload_len_u64 = u64::from_le_bytes(header[12..20].try_into().unwrap());
        let payload_len = usize::try_from(payload_len_u64)
            .map_err(|_| invalid_data("legacy journal payload length overflow"))?;
        if count > LEGACY_MAX_GROUP_OBJECTS || payload_len > LEGACY_MAX_GROUP_BYTES {
            return Err(invalid_data("legacy journal group exceeds limits"));
        }
        let total_len = (LEGACY_GROUP_HEADER_BYTES as u64)
            .checked_add(payload_len_u64)
            .and_then(|value| value.checked_add(LEGACY_GROUP_TRAILER_BYTES as u64))
            .ok_or_else(|| invalid_data("legacy journal group length overflow"))?;
        let group_end = offset
            .checked_add(total_len)
            .ok_or_else(|| invalid_data("legacy journal offset overflow"))?;
        if group_end > file_len {
            truncate_legacy_tail(&mut journal, offset)?;
            break;
        }

        let mut payload = vec![0u8; payload_len];
        journal.read_exact(&mut payload)?;
        let mut trailer = [0u8; LEGACY_GROUP_TRAILER_BYTES];
        journal.read_exact(&mut trailer)?;
        let mut checksum = blake3::Hasher::new();
        checksum.update(&header);
        checksum.update(&payload);
        if checksum.finalize().as_bytes() != &trailer {
            return Err(invalid_data("legacy storage journal checksum mismatch"));
        }
        let objects = decode_legacy_payload(count, &payload)?;
        let missing: Vec<&DurableObject> = objects
            .iter()
            .filter(|object| !store.indexed(object.kind, &object.hash))
            .collect();
        store.append_group(active, &missing, CommitFault::None)?;
        offset = group_end;
    }
    drop(journal);

    let mut migrated = path.with_file_name("loose-groups-v1.migrated");
    if migrated.exists() {
        migrated = path.with_file_name(format!(
            "loose-groups-v1.migrated-{:016x}",
            rand::random::<u64>()
        ));
    }
    std::fs::rename(&path, &migrated)?;
    if let Some(parent) = path.parent() {
        sync_directory(parent)?;
    }
    Ok(())
}

fn decode_legacy_payload(
    count: usize,
    payload: &[u8],
) -> Result<Vec<DurableObject>, std::io::Error> {
    let minimum = count
        .checked_mul(LEGACY_RECORD_HEADER_BYTES)
        .ok_or_else(|| invalid_data("legacy record headers overflow"))?;
    if payload.len() < minimum {
        return Err(invalid_data("legacy record headers are truncated"));
    }
    let mut cursor = 0usize;
    let mut objects = Vec::with_capacity(count);
    for _ in 0..count {
        let header_end = cursor
            .checked_add(LEGACY_RECORD_HEADER_BYTES)
            .ok_or_else(|| invalid_data("legacy record cursor overflow"))?;
        let header = payload
            .get(cursor..header_end)
            .ok_or_else(|| invalid_data("legacy record header is truncated"))?;
        let kind = StorageObjectKind::try_from(header[0])?;
        if header[1..4] != [0, 0, 0] {
            return Err(invalid_data("legacy record flags are unsupported"));
        }
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&header[4..36]);
        let data_len_u64 = u64::from_le_bytes(header[36..44].try_into().unwrap());
        let data_len = usize::try_from(data_len_u64)
            .map_err(|_| invalid_data("legacy object length overflow"))?;
        if data_len > crate::pack_store::MAX_PACK_OBJECT_BYTES {
            return Err(invalid_data("legacy object exceeds byte limit"));
        }
        let data_end = header_end
            .checked_add(data_len)
            .ok_or_else(|| invalid_data("legacy object cursor overflow"))?;
        let bytes = payload
            .get(header_end..data_end)
            .ok_or_else(|| invalid_data("legacy object bytes are truncated"))?
            .to_vec();
        objects.push(DurableObject { kind, hash, bytes });
        cursor = data_end;
    }
    if cursor != payload.len() {
        return Err(invalid_data("legacy group has trailing payload bytes"));
    }
    Ok(objects)
}

fn read_up_to(file: &mut std::fs::File, output: &mut [u8]) -> Result<usize, std::io::Error> {
    let mut filled = 0usize;
    while filled < output.len() {
        match file.read(&mut output[filled..])? {
            0 => break,
            read => filled += read,
        }
    }
    Ok(filled)
}

fn truncate_legacy_tail(journal: &mut std::fs::File, offset: u64) -> Result<(), std::io::Error> {
    journal.set_len(offset)?;
    journal.sync_data()?;
    journal.seek(SeekFrom::Start(offset))?;
    Ok(())
}

fn invalid_data(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

fn chunk_error(error: impl std::fmt::Display) -> sync_core::chunk::ChunkError {
    sync_core::chunk::ChunkError::Io(std::io::Error::other(error.to_string()))
}

#[async_trait::async_trait(?Send)]
impl sync_core::store::ChunkStore for StorageWriter {
    async fn has(&self, hash: &FileHash) -> bool {
        self.contains(StorageObjectKind::IndexChunk, hash)
    }

    async fn get(&self, hash: &FileHash) -> Result<Vec<u8>, sync_core::chunk::ChunkError> {
        self.read(StorageObjectKind::IndexChunk, hash)
            .map_err(chunk_error)?
            .ok_or_else(|| sync_core::chunk::ChunkError::NotFound(hash_to_hex(hash)))
    }

    async fn put(&self, hash: FileHash, data: Vec<u8>) -> Result<(), sync_core::chunk::ChunkError> {
        let mut results = self
            .store_batch(vec![DurableObject {
                kind: StorageObjectKind::IndexChunk,
                hash,
                bytes: data,
            }])
            .await
            .map_err(chunk_error)?;
        results
            .pop()
            .ok_or_else(|| chunk_error("storage writer returned no result"))?
            .map(|_| ())
            .map_err(chunk_error)
    }

    async fn delete(&self, _hash: &FileHash) -> Result<(), sync_core::chunk::ChunkError> {
        Err(sync_core::chunk::ChunkError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "packed index deletion requires compaction",
        )))
    }
}

#[derive(serde::Deserialize)]
struct StoredWireManifest {
    file_hash: String,
    total_size: u64,
    chunks: Vec<StoredWireChunk>,
}

#[derive(serde::Deserialize)]
struct StoredWireChunk {
    hash: String,
    offset: u64,
    size: u32,
}

#[async_trait::async_trait(?Send)]
impl sync_core::content_store::ContentStore for StorageWriter {
    async fn has(&self, hash: &FileHash) -> bool {
        self.contains(StorageObjectKind::Content, hash)
    }

    async fn get(&self, hash: &FileHash) -> Result<Vec<u8>, sync_core::chunk::ChunkError> {
        self.read(StorageObjectKind::Content, hash)
            .map_err(chunk_error)?
            .ok_or_else(|| sync_core::chunk::ChunkError::NotFound(hash_to_hex(hash)))
    }

    async fn put(&self, hash: FileHash, data: Vec<u8>) -> Result<(), sync_core::chunk::ChunkError> {
        let mut results = self
            .store_batch(vec![DurableObject {
                kind: StorageObjectKind::Content,
                hash,
                bytes: data,
            }])
            .await
            .map_err(chunk_error)?;
        results
            .pop()
            .ok_or_else(|| chunk_error("storage writer returned no result"))?
            .map(|_| ())
            .map_err(chunk_error)
    }

    async fn has_manifest(&self, file_hash: &FileHash) -> bool {
        self.contains(StorageObjectKind::Manifest, file_hash)
    }

    async fn get_manifest(
        &self,
        file_hash: &FileHash,
    ) -> Result<sync_core::content_store::FileManifest, sync_core::chunk::ChunkError> {
        let bytes = self
            .read(StorageObjectKind::Manifest, file_hash)
            .map_err(chunk_error)?
            .ok_or_else(|| sync_core::chunk::ChunkError::NotFound(hash_to_hex(file_hash)))?;
        let stored: StoredWireManifest = serde_json::from_slice(&bytes)
            .map_err(|error| sync_core::chunk::ChunkError::Deserialize(error.to_string()))?;
        let declared = hex_to_hash(&stored.file_hash).map_err(|_| {
            sync_core::chunk::ChunkError::Deserialize("invalid manifest file_hash".into())
        })?;
        if declared != *file_hash {
            return Err(sync_core::chunk::ChunkError::Deserialize(
                "manifest file_hash does not match its address".into(),
            ));
        }
        let chunks = stored
            .chunks
            .into_iter()
            .map(|chunk| {
                Ok(sync_core::content_store::ChunkRef {
                    hash: hex_to_hash(&chunk.hash).map_err(|_| {
                        sync_core::chunk::ChunkError::Deserialize(
                            "invalid manifest chunk hash".into(),
                        )
                    })?,
                    offset: chunk.offset,
                    size: chunk.size,
                })
            })
            .collect::<Result<Vec<_>, sync_core::chunk::ChunkError>>()?;
        Ok(sync_core::content_store::FileManifest {
            file_hash: declared,
            total_size: stored.total_size,
            chunks,
        })
    }

    async fn put_manifest(
        &self,
        manifest: sync_core::content_store::FileManifest,
    ) -> Result<(), sync_core::chunk::ChunkError> {
        let bytes = serde_json::to_vec(&serde_json::json!({
            "file_hash": hash_to_hex(&manifest.file_hash),
            "total_size": manifest.total_size,
            "chunks": manifest.chunks.iter().map(|chunk| serde_json::json!({
                "hash": hash_to_hex(&chunk.hash),
                "offset": chunk.offset,
                "size": chunk.size,
            })).collect::<Vec<_>>(),
        }))
        .map_err(|error| sync_core::chunk::ChunkError::Deserialize(error.to_string()))?;
        let mut results = self
            .store_batch(vec![DurableObject {
                kind: StorageObjectKind::Manifest,
                hash: manifest.file_hash,
                bytes,
            }])
            .await
            .map_err(chunk_error)?;
        results
            .pop()
            .ok_or_else(|| chunk_error("storage writer returned no result"))?
            .map(|_| ())
            .map_err(chunk_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sync_core::hash::hash_bytes;
    use tempfile::tempdir;

    fn content(seed: usize, size: usize) -> DurableObject {
        let mut bytes = vec![0u8; size];
        bytes[..std::mem::size_of::<usize>()].copy_from_slice(&seed.to_le_bytes());
        let hash = hash_bytes(&bytes);
        DurableObject {
            kind: StorageObjectKind::Content,
            hash,
            bytes,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn one_bulk_batch_uses_one_fdatasync_and_retry_is_idempotent() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let perf = Arc::new(ServerPerfCounters::default());
        let writer = StorageWriter::start(layout, Arc::clone(&perf)).unwrap();
        let objects: Vec<DurableObject> = (0..256).map(|index| content(index, 1024)).collect();
        let hashes: Vec<FileHash> = objects.iter().map(|object| object.hash).collect();

        let first = writer.store_batch(objects).await.unwrap();
        assert!(first
            .iter()
            .all(|result| *result == Ok(StoreOutcome::Stored)));
        let snapshot = perf.snapshot();
        assert_eq!(snapshot.storage.pack_appends, 256);
        assert_eq!(snapshot.storage.fdatasyncs, 1);
        assert_eq!(snapshot.storage.loose_writes, 0);
        assert_eq!(snapshot.storage.writer_queue_depth, 0);
        assert_eq!(snapshot.storage.writer_queue_peak, 256);
        for hash in &hashes {
            assert!(writer.contains(StorageObjectKind::Content, hash));
            assert!(writer
                .read(StorageObjectKind::Content, hash)
                .unwrap()
                .is_some());
        }

        let retry: Vec<DurableObject> = hashes
            .iter()
            .enumerate()
            .map(|(index, _)| content(index, 1024))
            .collect();
        let second = writer.store_batch(retry).await.unwrap();
        assert!(second
            .iter()
            .all(|result| *result == Ok(StoreOutcome::AlreadyPresent)));
        assert_eq!(perf.snapshot().storage.fdatasyncs, 1);
    }

    #[test]
    fn invalid_hash_is_rejected_before_segment_append() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let perf = Arc::new(ServerPerfCounters::default());
        let OpenedPackStore { store, mut active } =
            PackStore::open(layout, Arc::clone(&perf)).unwrap();
        let object = DurableObject {
            kind: StorageObjectKind::ContentChunk,
            hash: hash_bytes(b"different"),
            bytes: b"payload".to_vec(),
        };
        let report = commit_objects(
            &store,
            &mut active,
            &[FlatObject {
                message_index: 0,
                object_index: 0,
                object,
            }],
        );
        assert!(matches!(
            report.results[0],
            Err(StoreError::InvalidObject(_))
        ));
        assert_eq!(perf.snapshot().storage.fdatasyncs, 0);
    }

    #[test]
    fn byte_queue_rejects_work_above_the_hard_cap_without_reserving_capacity() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let perf = Arc::new(ServerPerfCounters::default());
        let writer = StorageWriter::start(layout, Arc::clone(&perf)).unwrap();

        assert!(matches!(
            writer.reserve(QUEUE_BYTES + 1, 1),
            Err(StoreError::Busy)
        ));
        let snapshot = perf.snapshot();
        assert_eq!(snapshot.storage.writer_queue_depth, 0);
        assert_eq!(snapshot.storage.writer_queue_peak, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn loose_import_is_bounded_resumable_and_deletes_only_verified_copies() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let first = content(41, 2048);
        let second = content(42, 4096);
        let first_path = layout.content_blob_path(&first.hash);
        let second_path = layout.content_blob_path(&second.hash);
        write_loose(&first_path, &first.bytes);
        write_loose(&second_path, &second.bytes);
        let writer =
            StorageWriter::start(layout.clone(), Arc::new(ServerPerfCounters::default())).unwrap();

        let first_pass = writer.import_loose_objects(Some(1)).await.unwrap();
        assert_eq!(first_pass.scanned, 1);
        assert_eq!(first_pass.imported, 1);
        assert_eq!(
            usize::from(first_path.exists()) + usize::from(second_path.exists()),
            1
        );

        let second_pass = writer.import_loose_objects(None).await.unwrap();
        assert_eq!(second_pass.scanned, 1);
        assert_eq!(second_pass.imported, 1);
        assert!(!first_path.exists());
        assert!(!second_path.exists());
        assert_eq!(
            writer
                .read(StorageObjectKind::Content, &first.hash)
                .unwrap(),
            Some(first.bytes)
        );
        assert_eq!(
            writer
                .read(StorageObjectKind::Content, &second.hash)
                .unwrap(),
            Some(second.bytes)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn corrupt_loose_object_is_retained_and_never_indexed() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let hash = hash_bytes(b"declared bytes");
        let path = layout.content_blob_path(&hash);
        write_loose(&path, b"corrupt bytes");
        let writer = StorageWriter::start(layout, Arc::new(ServerPerfCounters::default())).unwrap();

        let stats = writer.import_loose_objects(None).await.unwrap();
        assert_eq!(stats.scanned, 1);
        assert_eq!(stats.invalid, 1);
        assert!(path.exists());
        assert!(!writer
            .pack_store()
            .indexed(StorageObjectKind::Content, &hash));
        assert!(!writer.contains(StorageObjectKind::Content, &hash));
    }

    #[test]
    fn oversized_loose_object_is_rejected_from_metadata_before_reading() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("oversized");
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(crate::pack_store::MAX_PACK_OBJECT_BYTES as u64 + 1)
            .unwrap();

        assert!(read_loose_bounded(&path).unwrap().is_none());
    }

    #[tokio::test]
    async fn manifest_adapter_preserves_wire_hashes_and_chunk_metadata() {
        use sync_core::content_store::ContentStore;

        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        let writer = StorageWriter::start(layout, Arc::new(ServerPerfCounters::default())).unwrap();
        let manifest = sync_core::content_store::FileManifest {
            file_hash: hash_bytes(b"whole file"),
            total_size: 1234,
            chunks: vec![sync_core::content_store::ChunkRef {
                hash: hash_bytes(b"chunk"),
                offset: 17,
                size: 123,
            }],
        };

        writer.put_manifest(manifest.clone()).await.unwrap();
        let restored = writer.get_manifest(&manifest.file_hash).await.unwrap();
        assert_eq!(restored.file_hash, manifest.file_hash);
        assert_eq!(restored.total_size, manifest.total_size);
        assert_eq!(restored.chunks.len(), 1);
        assert_eq!(restored.chunks[0].hash, manifest.chunks[0].hash);
        assert_eq!(restored.chunks[0].offset, manifest.chunks[0].offset);
        assert_eq!(restored.chunks[0].size, manifest.chunks[0].size);
    }

    #[test]
    fn legacy_group_journal_is_imported_before_writer_start_returns() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let object = content(51, 4096);
        let mut legacy = encode_legacy_group(std::slice::from_ref(&object));
        legacy.extend_from_slice(b"OWG");
        std::fs::write(layout.storage_writer_journal_path(), legacy).unwrap();
        let perf = Arc::new(ServerPerfCounters::default());

        let writer = StorageWriter::start(layout.clone(), Arc::clone(&perf)).unwrap();
        assert_eq!(
            writer
                .read(StorageObjectKind::Content, &object.hash)
                .unwrap(),
            Some(object.bytes)
        );
        assert!(!layout.storage_writer_journal_path().exists());
        assert!(layout
            .storage_writer_journal_path()
            .with_file_name("loose-groups-v1.migrated")
            .exists());
        assert_eq!(perf.snapshot().storage.fdatasyncs, 1);
    }

    #[test]
    fn corrupt_complete_legacy_group_fails_startup_closed() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let object = content(52, 1024);
        let mut legacy = encode_legacy_group(&[object]);
        let last = legacy.len() - 1;
        legacy[last] ^= 0x80;
        std::fs::write(layout.storage_writer_journal_path(), legacy).unwrap();

        let result = StorageWriter::start(layout.clone(), Arc::new(ServerPerfCounters::default()));
        assert!(result.is_err());
        assert!(layout.storage_writer_journal_path().exists());
    }

    /// Full W1 evidence run for indexed segments. Payload construction,
    /// validation, append, CRC32C, BLAKE3, and all durability waits are inside
    /// the measured interval; no loose-file fan-out occurs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "full 870.4 MB W1 indexed-segment benchmark"]
    async fn benchmark_w1_segment_storage() {
        const FILES: usize = 100_000;
        const BATCH: usize = 256;
        const EXPECTED_BYTES: u64 = 870_400_000;
        const EXPECTED_GROUPS: u64 = 391;

        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let perf = Arc::new(ServerPerfCounters::default());
        let writer = StorageWriter::start(layout.clone(), Arc::clone(&perf)).unwrap();
        let started = Instant::now();
        let mut written = 0usize;

        while written < FILES {
            let end = (written + BATCH).min(FILES);
            let mut objects = Vec::with_capacity(end - written);
            for index in written..end {
                let size = (index % 16 + 1) * 1024;
                objects.push(content(index, size));
            }
            let results = writer.store_batch(objects).await.unwrap();
            assert!(results
                .iter()
                .all(|result| *result == Ok(StoreOutcome::Stored)));
            written = end;
        }

        let elapsed = started.elapsed();
        let storage = perf.snapshot().storage;
        assert_eq!(storage.pack_appends, FILES as u64);
        assert_eq!(storage.bytes_written, EXPECTED_BYTES);
        assert_eq!(storage.fdatasyncs, EXPECTED_GROUPS);
        assert_eq!(storage.loose_writes, 0);
        assert_eq!(storage.writer_queue_depth, 0);
        assert_eq!(storage.writer_queue_peak, BATCH as u64);
        assert_eq!(count_files(&layout.object_segments_dir(), "pack"), 4);
        assert_eq!(count_files(&layout.object_segments_dir(), "idx"), 3);
        drop(writer);

        // Sealed segments load from small sidecars. Only the bounded active
        // tail may be scanned and rehashed during restart.
        let restart_perf = Arc::new(ServerPerfCounters::default());
        let restart_started = Instant::now();
        let reopened = PackStore::open(layout.clone(), Arc::clone(&restart_perf)).unwrap();
        let restart_elapsed = restart_started.elapsed();
        let restart_storage = restart_perf.snapshot().storage;
        assert!(restart_storage.bytes_rehashed <= 272 * 1024 * 1024);
        let sample = content(FILES - 1, ((FILES - 1) % 16 + 1) * 1024);
        assert!(reopened.store.contains(sample.kind, &sample.hash));
        println!(
            "W1_SEGMENT_STORAGE elapsed_ms={:.3} files={} bytes={} fdatasyncs={} files_per_second={:.3} mib_per_second={:.3} restart_ms={:.3} restart_rehashed_bytes={}",
            elapsed.as_secs_f64() * 1_000.0,
            FILES,
            EXPECTED_BYTES,
            storage.fdatasyncs,
            FILES as f64 / elapsed.as_secs_f64(),
            EXPECTED_BYTES as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
            restart_elapsed.as_secs_f64() * 1_000.0,
            restart_storage.bytes_rehashed,
        );
    }

    fn count_files(directory: &std::path::Path, extension: &str) -> usize {
        std::fs::read_dir(directory)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|value| value == extension)
            })
            .count()
    }

    fn write_loose(path: &Path, bytes: &[u8]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }

    fn encode_legacy_group(objects: &[DurableObject]) -> Vec<u8> {
        let mut payload = Vec::new();
        for object in objects {
            let mut header = [0u8; LEGACY_RECORD_HEADER_BYTES];
            header[0] = object.kind as u8;
            header[4..36].copy_from_slice(&object.hash);
            header[36..44].copy_from_slice(&(object.bytes.len() as u64).to_le_bytes());
            payload.extend_from_slice(&header);
            payload.extend_from_slice(&object.bytes);
        }
        let mut header = [0u8; LEGACY_GROUP_HEADER_BYTES];
        header[..4].copy_from_slice(LEGACY_MAGIC);
        header[4] = LEGACY_VERSION;
        header[8..12].copy_from_slice(&(objects.len() as u32).to_le_bytes());
        header[12..20].copy_from_slice(&(payload.len() as u64).to_le_bytes());
        let mut checksum = blake3::Hasher::new();
        checksum.update(&header);
        checksum.update(&payload);
        let mut encoded = Vec::with_capacity(
            LEGACY_GROUP_HEADER_BYTES + payload.len() + LEGACY_GROUP_TRAILER_BYTES,
        );
        encoded.extend_from_slice(&header);
        encoded.extend_from_slice(&payload);
        encoded.extend_from_slice(checksum.finalize().as_bytes());
        encoded
    }
}
