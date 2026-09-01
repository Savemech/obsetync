//! Bounded dedicated writer for immutable content-addressed objects.
//!
//! Slice 6 deliberately keeps the established loose-object read backend. A
//! checksummed append-only journal is the durability source of truth: one
//! `fdatasync` commits a whole group, then atomic loose mirrors are published
//! without per-object fsync. On restart the journal recreates any mirror that
//! was lost between the group ACK and a crash/power loss.

use crate::perf::ServerPerfCounters;
use crate::storage::{materialize_journaled_blob, StorageLayout};
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sync_core::hash::{hash_bytes, hex_to_hash, FileHash};
use tokio::sync::oneshot;

const JOURNAL_MAGIC: &[u8; 4] = b"OWG1";
const JOURNAL_VERSION: u8 = 1;
const GROUP_HEADER_BYTES: usize = 20;
const RECORD_HEADER_BYTES: usize = 44;
const GROUP_TRAILER_BYTES: usize = 32;

const CHANNEL_MESSAGES: usize = 64;
const QUEUE_BYTES: u64 = 64 * 1024 * 1024;
const GROUP_TARGET_BYTES: u64 = 16 * 1024 * 1024;
const GROUP_LATENCY: Duration = Duration::from_millis(5);
const GROUP_OBJECT_TARGET: usize = 4096;
const MAX_BATCH_OBJECTS: usize = 4096;
const MAX_RECOVERY_GROUP_OBJECTS: usize = MAX_BATCH_OBJECTS * 2;
const MAX_OBJECT_BYTES: usize = 64 * 1024 * 1024;
const MAX_RECOVERY_GROUP_BYTES: usize = 80 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum StorageObjectKind {
    Content = 0,
    ContentChunk = 1,
    IndexChunk = 2,
    Manifest = 3,
}

impl TryFrom<u8> for StorageObjectKind {
    type Error = std::io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Content),
            1 => Ok(Self::ContentChunk),
            2 => Ok(Self::IndexChunk),
            3 => Ok(Self::Manifest),
            _ => Err(invalid_data("unknown storage object kind")),
        }
    }
}

#[derive(Debug)]
pub struct DurableObject {
    pub kind: StorageObjectKind,
    pub hash: FileHash,
    pub bytes: Vec<u8>,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
enum FaultPoint {
    None,
    AfterAppendBeforeSync,
    AfterSyncBeforeMaterialize,
    AfterFirstMaterialize,
}

impl StorageWriter {
    /// Recover the journal synchronously before serving requests, then start
    /// the one blocking writer thread. Startup fails closed if a committed
    /// group is corrupt or cannot be materialized.
    pub fn start(
        layout: StorageLayout,
        perf: Arc<ServerPerfCounters>,
    ) -> Result<Self, std::io::Error> {
        let journal = open_and_recover(&layout, perf.as_ref())?;
        let (sender, receiver) = mpsc::sync_channel(CHANNEL_MESSAGES);
        let inner = Arc::new(WriterHandle {
            sender,
            queued_bytes: AtomicU64::new(0),
            perf: Arc::clone(&perf),
        });
        let thread_layout = layout;
        std::thread::Builder::new()
            .name("obsetync-storage-writer".into())
            .spawn(move || writer_loop(receiver, journal, thread_layout, perf))?;
        Ok(Self { inner })
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

fn writer_loop(
    receiver: Receiver<WriterMessage>,
    mut journal: std::fs::File,
    layout: StorageLayout,
    perf: Arc<ServerPerfCounters>,
) {
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

        let result_lengths: Vec<usize> = messages.iter().map(|m| m.objects.len()).collect();
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
        let report = commit_objects(
            &mut journal,
            &layout,
            &flat,
            perf.as_ref(),
            FaultPoint::None,
        );
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
        .map(|object| (object.bytes.len() as u64).max(1))
        .sum()
}

fn commit_objects(
    journal: &mut std::fs::File,
    layout: &StorageLayout,
    objects: &[FlatObject],
    perf: &ServerPerfCounters,
    fault: FaultPoint,
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
        let path = object_path(layout, flat.object.kind, &flat.object.hash);
        if std::fs::read(&path).is_ok_and(|existing| existing == flat.object.bytes) {
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

    let group_start = match journal.seek(SeekFrom::End(0)) {
        Ok(offset) => offset,
        Err(error) => {
            set_pending_error(&mut results, &pending, &aliases, io_error(error));
            return CommitReport {
                results,
                fatal: true,
            };
        }
    };
    let write_started = Instant::now();
    let append = append_group(journal, objects, &pending);
    let write_elapsed = write_started.elapsed();
    let payload_bytes = pending
        .iter()
        .map(|index| objects[*index].object.bytes.len() as u64)
        .sum();
    if let Err(error) = append {
        let _ = rollback_tail(journal, group_start);
        set_pending_error(&mut results, &pending, &aliases, io_error(error));
        return CommitReport {
            results,
            fatal: true,
        };
    }

    if fault == FaultPoint::AfterAppendBeforeSync {
        set_pending_error(
            &mut results,
            &pending,
            &aliases,
            StoreError::Io("injected crash before fdatasync".into()),
        );
        return CommitReport {
            results,
            fatal: false,
        };
    }

    let durability_started = Instant::now();
    if let Err(error) = journal.sync_data() {
        set_pending_error(&mut results, &pending, &aliases, io_error(error));
        return CommitReport {
            results,
            fatal: true,
        };
    }
    let durability_elapsed = durability_started.elapsed();
    perf.record_pack_commit(
        pending.len() as u64,
        payload_bytes,
        write_elapsed,
        durability_elapsed,
    );

    if fault == FaultPoint::AfterSyncBeforeMaterialize {
        set_pending_error(
            &mut results,
            &pending,
            &aliases,
            StoreError::Io("injected crash after fdatasync".into()),
        );
        return CommitReport {
            results,
            fatal: false,
        };
    }

    for (position, index) in pending.iter().copied().enumerate() {
        let object = &objects[index].object;
        let path = object_path(layout, object.kind, &object.hash);
        results[index] = materialize_journaled_blob(&path, &object.bytes)
            .map(|()| StoreOutcome::Stored)
            .map_err(io_error);
        if position == 0 && fault == FaultPoint::AfterFirstMaterialize {
            set_pending_error(
                &mut results,
                &pending,
                &aliases,
                StoreError::Io("injected crash during loose publication".into()),
            );
            break;
        }
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

fn set_pending_error(
    results: &mut [StoreResult],
    pending: &[usize],
    aliases: &[(usize, usize)],
    error: StoreError,
) {
    for index in pending {
        results[*index] = Err(error.clone());
    }
    for (alias, _) in aliases {
        results[*alias] = Err(error.clone());
    }
}

fn append_group(
    journal: &mut std::fs::File,
    objects: &[FlatObject],
    pending: &[usize],
) -> Result<(), std::io::Error> {
    let count = u32::try_from(pending.len()).map_err(|_| invalid_data("group count overflow"))?;
    let payload_len = pending.iter().try_fold(0u64, |total, index| {
        let data_len = u64::try_from(objects[*index].object.bytes.len())
            .map_err(|_| invalid_data("object length overflow"))?;
        total
            .checked_add(RECORD_HEADER_BYTES as u64)
            .and_then(|value| value.checked_add(data_len))
            .ok_or_else(|| invalid_data("group payload length overflow"))
    })?;
    let payload_len_usize =
        usize::try_from(payload_len).map_err(|_| invalid_data("group payload length overflow"))?;
    if payload_len_usize > MAX_RECOVERY_GROUP_BYTES {
        return Err(invalid_data("group exceeds recovery byte limit"));
    }

    let mut header = [0u8; GROUP_HEADER_BYTES];
    header[..4].copy_from_slice(JOURNAL_MAGIC);
    header[4] = JOURNAL_VERSION;
    header[8..12].copy_from_slice(&count.to_le_bytes());
    header[12..20].copy_from_slice(&payload_len.to_le_bytes());
    let mut checksum = blake3::Hasher::new();
    let mut output = std::io::BufWriter::with_capacity(256 * 1024, journal);
    output.write_all(&header)?;
    checksum.update(&header);

    for index in pending {
        let object = &objects[*index].object;
        let mut record = [0u8; RECORD_HEADER_BYTES];
        record[0] = object.kind as u8;
        record[4..36].copy_from_slice(&object.hash);
        let data_len = u64::try_from(object.bytes.len())
            .map_err(|_| invalid_data("object length overflow"))?;
        record[36..44].copy_from_slice(&data_len.to_le_bytes());
        output.write_all(&record)?;
        output.write_all(&object.bytes)?;
        checksum.update(&record);
        checksum.update(&object.bytes);
    }
    output.write_all(checksum.finalize().as_bytes())?;
    output.flush()
}

fn rollback_tail(journal: &mut std::fs::File, offset: u64) -> Result<(), std::io::Error> {
    journal.set_len(offset)?;
    journal.seek(SeekFrom::Start(offset))?;
    journal.sync_data()
}

fn open_and_recover(
    layout: &StorageLayout,
    perf: &ServerPerfCounters,
) -> Result<std::fs::File, std::io::Error> {
    let path = layout.storage_writer_journal_path();
    let parent = path
        .parent()
        .ok_or_else(|| invalid_data("storage writer journal has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let existed = path.exists();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&path)?;
    if !existed {
        file.sync_all()?;
        std::fs::File::open(parent)?.sync_all()?;
    }
    recover_journal(layout, &mut file, perf)?;
    file.seek(SeekFrom::End(0))?;
    Ok(file)
}

fn recover_journal(
    layout: &StorageLayout,
    journal: &mut std::fs::File,
    _perf: &ServerPerfCounters,
) -> Result<(), std::io::Error> {
    let file_len = journal.metadata()?.len();
    let mut offset = 0u64;
    while offset < file_len {
        journal.seek(SeekFrom::Start(offset))?;
        let mut header = [0u8; GROUP_HEADER_BYTES];
        let header_read = read_up_to(journal, &mut header)?;
        if header_read < GROUP_HEADER_BYTES {
            truncate_recovery_tail(journal, offset)?;
            return Ok(());
        }
        if &header[..4] != JOURNAL_MAGIC
            || header[4] != JOURNAL_VERSION
            || header[5..8] != [0, 0, 0]
        {
            return Err(invalid_data("storage writer journal header is corrupt"));
        }
        let count = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
        let payload_len_u64 = u64::from_le_bytes(header[12..20].try_into().unwrap());
        let payload_len = usize::try_from(payload_len_u64)
            .map_err(|_| invalid_data("journal payload length overflow"))?;
        if count > MAX_RECOVERY_GROUP_OBJECTS || payload_len > MAX_RECOVERY_GROUP_BYTES {
            return Err(invalid_data("storage writer journal group exceeds limits"));
        }
        let total_len = (GROUP_HEADER_BYTES as u64)
            .checked_add(payload_len_u64)
            .and_then(|value| value.checked_add(GROUP_TRAILER_BYTES as u64))
            .ok_or_else(|| invalid_data("journal group length overflow"))?;
        let group_end = offset
            .checked_add(total_len)
            .ok_or_else(|| invalid_data("journal offset overflow"))?;
        if group_end > file_len {
            truncate_recovery_tail(journal, offset)?;
            return Ok(());
        }

        let mut payload = vec![0u8; payload_len];
        journal.read_exact(&mut payload)?;
        let mut trailer = [0u8; GROUP_TRAILER_BYTES];
        journal.read_exact(&mut trailer)?;
        let mut checksum = blake3::Hasher::new();
        checksum.update(&header);
        checksum.update(&payload);
        if checksum.finalize().as_bytes() != &trailer {
            return Err(invalid_data("storage writer journal checksum mismatch"));
        }
        let recovered = decode_group_payload(count, &payload)?;
        for object in recovered {
            validate_object(&object).map_err(invalid_data)?;
            let path = object_path(layout, object.kind, &object.hash);
            materialize_journaled_blob(&path, &object.bytes)?;
        }
        offset = group_end;
    }
    Ok(())
}

fn decode_group_payload(
    count: usize,
    payload: &[u8],
) -> Result<Vec<DurableObject>, std::io::Error> {
    let minimum = count
        .checked_mul(RECORD_HEADER_BYTES)
        .ok_or_else(|| invalid_data("journal record headers overflow"))?;
    if payload.len() < minimum {
        return Err(invalid_data("journal record headers are truncated"));
    }
    let mut cursor = 0usize;
    let mut objects = Vec::with_capacity(count);
    for _ in 0..count {
        let header_end = cursor
            .checked_add(RECORD_HEADER_BYTES)
            .ok_or_else(|| invalid_data("journal record cursor overflow"))?;
        let header = payload
            .get(cursor..header_end)
            .ok_or_else(|| invalid_data("journal record header is truncated"))?;
        let kind = StorageObjectKind::try_from(header[0])?;
        if header[1..4] != [0, 0, 0] {
            return Err(invalid_data("journal record flags are unsupported"));
        }
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&header[4..36]);
        let data_len_u64 = u64::from_le_bytes(header[36..44].try_into().unwrap());
        let data_len = usize::try_from(data_len_u64)
            .map_err(|_| invalid_data("journal object length overflow"))?;
        if data_len > MAX_OBJECT_BYTES {
            return Err(invalid_data("journal object exceeds byte limit"));
        }
        let data_end = header_end
            .checked_add(data_len)
            .ok_or_else(|| invalid_data("journal object cursor overflow"))?;
        let bytes = payload
            .get(header_end..data_end)
            .ok_or_else(|| invalid_data("journal object bytes are truncated"))?
            .to_vec();
        objects.push(DurableObject { kind, hash, bytes });
        cursor = data_end;
    }
    if cursor != payload.len() {
        return Err(invalid_data("journal group has trailing payload bytes"));
    }
    Ok(objects)
}

fn truncate_recovery_tail(journal: &mut std::fs::File, offset: u64) -> Result<(), std::io::Error> {
    journal.set_len(offset)?;
    journal.sync_data()?;
    journal.seek(SeekFrom::Start(offset))?;
    Ok(())
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

fn validate_object(object: &DurableObject) -> Result<(), String> {
    if object.bytes.len() > MAX_OBJECT_BYTES {
        return Err("object exceeds storage writer byte limit".into());
    }
    if object.kind == StorageObjectKind::Manifest {
        let value: serde_json::Value = serde_json::from_slice(&object.bytes)
            .map_err(|_| "manifest is not valid JSON".to_string())?;
        let declared = value
            .get("file_hash")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "manifest has no file_hash".to_string())?;
        let declared =
            hex_to_hash(declared).map_err(|_| "manifest file_hash is invalid".to_string())?;
        if declared != object.hash {
            return Err("manifest file_hash does not match its address".into());
        }
    } else if hash_bytes(&object.bytes) != object.hash {
        return Err("plaintext BLAKE3 does not match its address".into());
    }
    Ok(())
}

fn object_path(layout: &StorageLayout, kind: StorageObjectKind, hash: &FileHash) -> PathBuf {
    match kind {
        StorageObjectKind::Content => layout.content_blob_path(hash),
        StorageObjectKind::ContentChunk => layout.content_chunk_path(hash),
        StorageObjectKind::IndexChunk => layout.index_path(hash),
        StorageObjectKind::Manifest => layout.content_manifest_path(hash),
    }
}

fn io_error(error: std::io::Error) -> StoreError {
    StoreError::Io(error.to_string())
}

fn invalid_data(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn flat(object: DurableObject) -> FlatObject {
        FlatObject {
            message_index: 0,
            object_index: 0,
            object,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn one_bulk_batch_uses_one_fdatasync_and_retry_is_idempotent() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let perf = Arc::new(ServerPerfCounters::default());
        let writer = StorageWriter::start(layout.clone(), Arc::clone(&perf)).unwrap();
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
        assert!(hashes
            .iter()
            .all(|hash| layout.content_blob_path(hash).exists()));

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
    fn fdatasynced_group_recovers_when_process_stops_before_publication() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let perf = ServerPerfCounters::default();
        let object = content(7, 4096);
        let hash = object.hash;
        let mut journal = open_and_recover(&layout, &perf).unwrap();
        let report = commit_objects(
            &mut journal,
            &layout,
            &[flat(object)],
            &perf,
            FaultPoint::AfterSyncBeforeMaterialize,
        );
        assert!(report.results[0].is_err(), "no ACK crosses the fault point");
        assert!(!layout.content_blob_path(&hash).exists());
        drop(journal);

        drop(open_and_recover(&layout, &perf).unwrap());
        assert_eq!(
            hash_bytes(&std::fs::read(layout.content_blob_path(&hash)).unwrap()),
            hash
        );
    }

    #[test]
    fn fdatasynced_group_recovers_every_object_after_partial_publication() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let perf = ServerPerfCounters::default();
        let objects: Vec<DurableObject> = (0..3).map(|index| content(index, 4096)).collect();
        let hashes: Vec<FileHash> = objects.iter().map(|object| object.hash).collect();
        let flat_objects: Vec<FlatObject> = objects.into_iter().map(flat).collect();
        let mut journal = open_and_recover(&layout, &perf).unwrap();

        let report = commit_objects(
            &mut journal,
            &layout,
            &flat_objects,
            &perf,
            FaultPoint::AfterFirstMaterialize,
        );
        assert!(report.results.iter().all(Result::is_err));
        assert!(layout.content_blob_path(&hashes[0]).exists());
        assert!(!layout.content_blob_path(&hashes[1]).exists());
        assert!(!layout.content_blob_path(&hashes[2]).exists());
        drop(journal);

        drop(open_and_recover(&layout, &perf).unwrap());
        for hash in hashes {
            assert_eq!(
                hash_bytes(&std::fs::read(layout.content_blob_path(&hash)).unwrap()),
                hash
            );
        }
    }

    #[test]
    fn torn_unacked_tail_is_discarded_at_last_complete_group() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let perf = ServerPerfCounters::default();
        let object = content(11, 2048);
        let hash = object.hash;
        let mut journal = open_and_recover(&layout, &perf).unwrap();
        let report = commit_objects(
            &mut journal,
            &layout,
            &[flat(object)],
            &perf,
            FaultPoint::AfterAppendBeforeSync,
        );
        assert!(report.results[0].is_err());
        let length = journal.metadata().unwrap().len();
        assert!(length > 8);
        journal.set_len(length - 7).unwrap();
        drop(journal);

        let recovered = open_and_recover(&layout, &perf).unwrap();
        assert_eq!(recovered.metadata().unwrap().len(), 0);
        assert!(!layout.content_blob_path(&hash).exists());
    }

    #[test]
    fn torn_tail_keeps_every_earlier_complete_group() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let perf = ServerPerfCounters::default();
        let committed = content(21, 2048);
        let committed_hash = committed.hash;
        let torn = content(22, 2048);
        let torn_hash = torn.hash;
        let mut journal = open_and_recover(&layout, &perf).unwrap();

        let committed_report = commit_objects(
            &mut journal,
            &layout,
            &[flat(committed)],
            &perf,
            FaultPoint::None,
        );
        assert_eq!(committed_report.results[0], Ok(StoreOutcome::Stored));
        let committed_length = journal.metadata().unwrap().len();
        let torn_report = commit_objects(
            &mut journal,
            &layout,
            &[flat(torn)],
            &perf,
            FaultPoint::AfterAppendBeforeSync,
        );
        assert!(torn_report.results[0].is_err());
        let length = journal.metadata().unwrap().len();
        journal.set_len(length - 7).unwrap();
        std::fs::remove_file(layout.content_blob_path(&committed_hash)).unwrap();
        drop(journal);

        let recovered = open_and_recover(&layout, &perf).unwrap();
        assert_eq!(recovered.metadata().unwrap().len(), committed_length);
        assert!(layout.content_blob_path(&committed_hash).exists());
        assert!(!layout.content_blob_path(&torn_hash).exists());
    }

    #[test]
    fn lost_ack_after_publication_is_idempotent_across_restart() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let perf = ServerPerfCounters::default();
        let original = content(31, 4096);
        let retry = content(31, 4096);
        let mut journal = open_and_recover(&layout, &perf).unwrap();

        let first = commit_objects(
            &mut journal,
            &layout,
            &[flat(original)],
            &perf,
            FaultPoint::None,
        );
        assert_eq!(first.results[0], Ok(StoreOutcome::Stored));
        let journal_length = journal.metadata().unwrap().len();
        drop(journal);

        let mut recovered = open_and_recover(&layout, &perf).unwrap();
        let second = commit_objects(
            &mut recovered,
            &layout,
            &[flat(retry)],
            &perf,
            FaultPoint::None,
        );
        assert_eq!(second.results[0], Ok(StoreOutcome::AlreadyPresent));
        assert_eq!(recovered.metadata().unwrap().len(), journal_length);
        assert_eq!(perf.snapshot().storage.fdatasyncs, 1);
    }

    #[test]
    fn checksum_corruption_fails_startup_closed() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let perf = ServerPerfCounters::default();
        let object = content(13, 1024);
        let hash = object.hash;
        let mut journal = open_and_recover(&layout, &perf).unwrap();
        let report = commit_objects(
            &mut journal,
            &layout,
            &[flat(object)],
            &perf,
            FaultPoint::None,
        );
        assert_eq!(report.results[0], Ok(StoreOutcome::Stored));
        std::fs::remove_file(layout.content_blob_path(&hash)).unwrap();
        journal
            .seek(SeekFrom::Start(GROUP_HEADER_BYTES as u64 + 5))
            .unwrap();
        journal.write_all(&[0xff]).unwrap();
        journal.sync_data().unwrap();
        drop(journal);

        let error = open_and_recover(&layout, &perf).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(!layout.content_blob_path(&hash).exists());
    }

    #[test]
    fn invalid_hash_is_rejected_before_journal_append() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let perf = ServerPerfCounters::default();
        let object = DurableObject {
            kind: StorageObjectKind::ContentChunk,
            hash: hash_bytes(b"different"),
            bytes: b"payload".to_vec(),
        };
        let mut journal = open_and_recover(&layout, &perf).unwrap();
        let report = commit_objects(
            &mut journal,
            &layout,
            &[flat(object)],
            &perf,
            FaultPoint::None,
        );
        assert!(matches!(
            report.results[0],
            Err(StoreError::InvalidObject(_))
        ));
        assert_eq!(journal.metadata().unwrap().len(), 0);
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

    /// Full W1 evidence run. It exercises the production writer with the same
    /// 100,000-file, deterministic 1..16 KiB distribution as the performance
    /// harness and proves that durability barriers scale with bulk groups.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "full 870.4 MB W1 storage benchmark"]
    async fn benchmark_w1_group_commit() {
        const FILES: usize = 100_000;
        const BATCH: usize = 256;
        const EXPECTED_BYTES: u64 = 870_400_000;
        const EXPECTED_GROUPS: u64 = 391;

        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let perf = Arc::new(ServerPerfCounters::default());
        let writer = StorageWriter::start(layout, Arc::clone(&perf)).unwrap();
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
        println!(
            "W1_GROUP_COMMIT elapsed_ms={:.3} files={} bytes={} fdatasyncs={} files_per_second={:.3} mib_per_second={:.3}",
            elapsed.as_secs_f64() * 1_000.0,
            FILES,
            EXPECTED_BYTES,
            storage.fdatasyncs,
            FILES as f64 / elapsed.as_secs_f64(),
            EXPECTED_BYTES as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
        );
    }
}
