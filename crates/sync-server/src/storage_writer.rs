//! Bounded dedicated writer for immutable content-addressed objects.
//!
//! The writer is the only append owner for the active pack segment. API
//! callers reserve queue bytes before enqueueing, objects are hash-verified
//! and deduplicated, and a whole group becomes visible only after the segment
//! crosses one `fdatasync` barrier.

use crate::blocking_io::{BlockingError, BlockingPool};
use crate::pack_store::{
    validate_object, ActiveSegment, CommitFault, OpenedPackStore, PackReadError, PackStore,
};
use crate::perf::ServerPerfCounters;
use crate::storage::StorageLayout;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use sync_core::hash::FileHash;
use sync_core::hash::{hash_to_hex, hex_to_hash};
use tokio::sync::oneshot;

pub use crate::pack_store::{PackedObject as DurableObject, PackedObjectKind as StorageObjectKind};

const CHANNEL_MESSAGES: usize = 64;
const QUEUE_BYTES: u64 = 64 * 1024 * 1024;
// Bulk cannot consume the final eight message slots / eight MiB. Those are
// available to bounded control/index work even while object uploads are noisy.
const BULK_QUEUE_MESSAGES: usize = 56;
const BULK_QUEUE_BYTES: u64 = 56 * 1024 * 1024;
const MAX_ACTIVE_OWNERS: usize = 64;
const OWNER_QUEUE_MESSAGES: usize = 24;
const OWNER_QUEUE_BYTES: u64 = QUEUE_BYTES;
const OWNER_BULK_MESSAGES: usize = 16;
const OWNER_BULK_BYTES: u64 = 16 * 1024 * 1024;
const OWNER_CONTROL_MESSAGES: usize = 8;
const OWNER_CONTROL_BYTES: u64 = 8 * 1024 * 1024;
const MAX_OWNER_KEY_BYTES: usize = 128;
const MAX_CONTROL_BURST: usize = 8;
const BULK_AGING: Duration = Duration::from_millis(25);
const GROUP_TARGET_BYTES: u64 = 16 * 1024 * 1024;
const GROUP_LATENCY: Duration = Duration::from_millis(5);
const GROUP_OBJECT_TARGET: usize = 4096;
const MAX_BATCH_OBJECTS: usize = 4096;
const READ_TASK_PARALLELISM: usize = 4;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriterClass {
    Control,
    Bulk,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum WriterOwner {
    Internal,
    Authenticated(Arc<str>),
}

impl WriterOwner {
    fn authenticated(value: &str) -> Result<Self, StoreError> {
        if value.is_empty() || value.len() > MAX_OWNER_KEY_BYTES {
            return Err(StoreError::InvalidObject(
                "storage writer owner key is invalid".into(),
            ));
        }
        Ok(Self::Authenticated(Arc::from(value)))
    }

    fn internal() -> Self {
        Self::Internal
    }

    fn is_internal(&self) -> bool {
        matches!(self, Self::Internal)
    }
}

#[derive(Clone)]
pub struct StorageWriter {
    inner: Arc<WriterHandle>,
}

struct WriterHandle {
    sender: SyncSender<WriterMessage>,
    queue: Mutex<QueueState>,
    perf: Arc<ServerPerfCounters>,
    store: PackStore,
    read_pool: BlockingPool,
}

struct QueueReservation {
    handle: Arc<WriterHandle>,
    owner: WriterOwner,
    class: WriterClass,
    bytes: u64,
    objects: u64,
}

/// A non-waiting owner/global reservation acquired before an authenticated
/// request enters blocking validation. It can become exactly one writer
/// message without a second admission race; dropping it releases every cap.
pub(crate) struct WriterAdmission {
    reservation: Option<QueueReservation>,
}

impl Drop for QueueReservation {
    fn drop(&mut self) {
        self.handle
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .release(&self.owner, self.class, self.bytes);
        self.handle.perf.record_writer_queue_remove(self.objects);
    }
}

impl QueueReservation {
    fn record_objects(&mut self, objects: u64) {
        debug_assert_eq!(self.objects, 0);
        self.objects = objects;
        self.handle.perf.record_writer_queue_add(objects);
    }
}

struct WriterMessage {
    objects: Vec<DurableObject>,
    reply: oneshot::Sender<Vec<StoreResult>>,
    reservation: QueueReservation,
    enqueued_at: Instant,
}

#[derive(Debug, Default, Clone, Copy)]
struct OwnerUsage {
    messages: usize,
    bytes: u64,
    bulk_messages: usize,
    bulk_bytes: u64,
}

#[derive(Debug, Default)]
struct QueueState {
    messages: usize,
    bytes: u64,
    bulk_messages: usize,
    bulk_bytes: u64,
    owners: HashMap<WriterOwner, OwnerUsage>,
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
            queue: Mutex::new(QueueState::default()),
            perf,
            store: store.clone(),
            read_pool: BlockingPool::new("storage reads", READ_TASK_PARALLELISM),
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
        self.store_batch_owned(WriterOwner::internal(), WriterClass::Control, objects)
            .await
    }

    /// Enqueue authenticated API work under a bounded fairness owner. The
    /// owner key is scheduling-only and is never persisted or returned.
    pub(crate) async fn store_batch_for(
        &self,
        owner: &str,
        class: WriterClass,
        objects: Vec<DurableObject>,
    ) -> Result<Vec<StoreResult>, StoreError> {
        let owner = WriterOwner::authenticated(owner)?;
        self.store_batch_owned(owner, class, objects).await
    }

    async fn store_batch_owned(
        &self,
        owner: WriterOwner,
        class: WriterClass,
        objects: Vec<DurableObject>,
    ) -> Result<Vec<StoreResult>, StoreError> {
        if objects.is_empty() {
            return Ok(Vec::new());
        }
        let bytes = batch_reservation_bytes(&objects)?;
        let admission = self.admit_batch_owned(owner, class, bytes)?;
        self.store_admitted(admission, objects).await
    }

    /// Reserve preprocessing ownership synchronously, before a body can wait
    /// on the bounded blocking pool. `bytes` is the caller's conservative
    /// charged byte workset (for HTTP, the body plus its payload copy), not a
    /// post-parse stored-size estimate.
    pub(crate) fn admit_batch_for(
        &self,
        owner: &str,
        class: WriterClass,
        bytes: usize,
    ) -> Result<WriterAdmission, StoreError> {
        let owner = WriterOwner::authenticated(owner)?;
        let bytes = u64::try_from(bytes)
            .map_err(|_| StoreError::InvalidObject("request length overflow".into()))?;
        self.admit_batch_owned(owner, class, bytes.max(1))
    }

    fn admit_batch_owned(
        &self,
        owner: WriterOwner,
        class: WriterClass,
        bytes: u64,
    ) -> Result<WriterAdmission, StoreError> {
        Ok(WriterAdmission {
            reservation: Some(self.reserve_for(owner, class, bytes, 0)?),
        })
    }

    /// Convert a preprocessing reservation into one durable writer message.
    /// Actual stored bytes may be smaller than the admitted preprocessing
    /// workset, never larger. Every error drops the same reservation exactly
    /// once.
    pub(crate) async fn store_admitted(
        &self,
        mut admission: WriterAdmission,
        objects: Vec<DurableObject>,
    ) -> Result<Vec<StoreResult>, StoreError> {
        if objects.is_empty() {
            return Ok(Vec::new());
        }
        let bytes = batch_reservation_bytes(&objects)?;
        let mut reservation = admission
            .reservation
            .take()
            .expect("writer admission must own a reservation");
        if !Arc::ptr_eq(&reservation.handle, &self.inner) {
            return Err(StoreError::InvalidObject(
                "storage writer admission owner changed".into(),
            ));
        }
        if bytes > reservation.bytes {
            return Err(StoreError::InvalidObject(
                "prepared batch exceeds its request admission".into(),
            ));
        }
        reservation.record_objects(objects.len() as u64);
        let (reply, receive) = oneshot::channel();
        let message = WriterMessage {
            objects,
            reply,
            reservation,
            enqueued_at: Instant::now(),
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

    /// Run one coarse storage read/scan operation on the bounded blocking
    /// pool. The permit is acquired before Tokio creates a blocking task.
    pub async fn run_blocking<F, T>(&self, operation: F) -> Result<T, BlockingError>
    where
        F: FnOnce(StorageWriter) -> T + Send + 'static,
        T: Send + 'static,
    {
        let writer = self.clone();
        self.inner.read_pool.run(move || operation(writer)).await
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

    #[cfg(test)]
    fn reserve(&self, bytes: u64, objects: u64) -> Result<QueueReservation, StoreError> {
        self.reserve_for(
            WriterOwner::internal(),
            WriterClass::Control,
            bytes,
            objects,
        )
    }

    fn reserve_for(
        &self,
        owner: WriterOwner,
        class: WriterClass,
        bytes: u64,
        objects: u64,
    ) -> Result<QueueReservation, StoreError> {
        self.inner
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .reserve(&owner, class, bytes)?;
        let mut reservation = QueueReservation {
            handle: Arc::clone(&self.inner),
            owner,
            class,
            bytes,
            objects: 0,
        };
        reservation.record_objects(objects);
        Ok(reservation)
    }
}

fn batch_reservation_bytes(objects: &[DurableObject]) -> Result<u64, StoreError> {
    if objects.is_empty() {
        return Ok(1);
    }
    if objects.len() > MAX_BATCH_OBJECTS {
        return Err(StoreError::InvalidObject(format!(
            "batch has {} objects; maximum is {MAX_BATCH_OBJECTS}",
            objects.len()
        )));
    }
    objects.iter().try_fold(0u64, |total, object| {
        let object_bytes = u64::try_from(object.bytes.len())
            .map_err(|_| StoreError::InvalidObject("object length overflow".into()))?;
        total
            .checked_add(object_bytes.max(1))
            .ok_or_else(|| StoreError::InvalidObject("batch byte length overflow".into()))
    })
}

impl QueueState {
    fn reserve(
        &mut self,
        owner: &WriterOwner,
        class: WriterClass,
        bytes: u64,
    ) -> Result<(), StoreError> {
        let next_messages = self.messages.checked_add(1).ok_or(StoreError::Busy)?;
        let next_bytes = self.bytes.checked_add(bytes).ok_or(StoreError::Busy)?;
        if next_messages > CHANNEL_MESSAGES || next_bytes > QUEUE_BYTES {
            return Err(StoreError::Busy);
        }
        if !self.owners.contains_key(owner) && self.owners.len() >= MAX_ACTIVE_OWNERS {
            return Err(StoreError::Busy);
        }
        let current = self.owners.get(owner).copied().unwrap_or_default();
        if current.messages >= OWNER_QUEUE_MESSAGES
            || current
                .bytes
                .checked_add(bytes)
                .map_or(true, |value| value > OWNER_QUEUE_BYTES)
        {
            return Err(StoreError::Busy);
        }
        if class == WriterClass::Bulk {
            let next_bulk_messages = self.bulk_messages.checked_add(1).ok_or(StoreError::Busy)?;
            let next_bulk_bytes = self.bulk_bytes.checked_add(bytes).ok_or(StoreError::Busy)?;
            let next_owner_bulk_bytes = current
                .bulk_bytes
                .checked_add(bytes)
                .ok_or(StoreError::Busy)?;
            if next_bulk_messages > BULK_QUEUE_MESSAGES
                || next_bulk_bytes > BULK_QUEUE_BYTES
                || current.bulk_messages >= OWNER_BULK_MESSAGES
                || next_owner_bulk_bytes > OWNER_BULK_BYTES
            {
                return Err(StoreError::Busy);
            }
        } else {
            let control_messages = current.messages - current.bulk_messages;
            let control_bytes = current.bytes - current.bulk_bytes;
            let owner_control_bytes = if owner.is_internal() {
                QUEUE_BYTES
            } else {
                OWNER_CONTROL_BYTES
            };
            if control_messages >= OWNER_CONTROL_MESSAGES
                || control_bytes
                    .checked_add(bytes)
                    .map_or(true, |value| value > owner_control_bytes)
            {
                return Err(StoreError::Busy);
            }
        }

        self.messages = next_messages;
        self.bytes = next_bytes;
        if class == WriterClass::Bulk {
            self.bulk_messages += 1;
            self.bulk_bytes += bytes;
        }
        let usage = self.owners.entry(owner.clone()).or_default();
        usage.messages += 1;
        usage.bytes += bytes;
        if class == WriterClass::Bulk {
            usage.bulk_messages += 1;
            usage.bulk_bytes += bytes;
        }
        Ok(())
    }

    fn release(&mut self, owner: &WriterOwner, class: WriterClass, bytes: u64) {
        self.messages = self
            .messages
            .checked_sub(1)
            .expect("writer message underflow");
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .expect("writer byte underflow");
        if class == WriterClass::Bulk {
            self.bulk_messages = self
                .bulk_messages
                .checked_sub(1)
                .expect("writer bulk message underflow");
            self.bulk_bytes = self
                .bulk_bytes
                .checked_sub(bytes)
                .expect("writer bulk byte underflow");
        }
        let remove = {
            let usage = self
                .owners
                .get_mut(owner)
                .expect("writer owner reservation missing");
            usage.messages = usage
                .messages
                .checked_sub(1)
                .expect("owner message underflow");
            usage.bytes = usage
                .bytes
                .checked_sub(bytes)
                .expect("owner byte underflow");
            if class == WriterClass::Bulk {
                usage.bulk_messages = usage
                    .bulk_messages
                    .checked_sub(1)
                    .expect("owner bulk message underflow");
                usage.bulk_bytes = usage
                    .bulk_bytes
                    .checked_sub(bytes)
                    .expect("owner bulk byte underflow");
            }
            usage.messages == 0
        };
        if remove {
            self.owners.remove(owner);
        }
    }
}

struct Scheduled<T> {
    enqueued_at: Instant,
    value: T,
}

struct OwnerQueues<T> {
    control: VecDeque<Scheduled<T>>,
    bulk: VecDeque<Scheduled<T>>,
}

impl<T> Default for OwnerQueues<T> {
    fn default() -> Self {
        Self {
            control: VecDeque::new(),
            bulk: VecDeque::new(),
        }
    }
}

struct FairScheduler<T> {
    owners: HashMap<WriterOwner, OwnerQueues<T>>,
    control_order: VecDeque<WriterOwner>,
    bulk_order: VecDeque<WriterOwner>,
    control_burst: usize,
    len: usize,
}

impl<T> Default for FairScheduler<T> {
    fn default() -> Self {
        Self {
            owners: HashMap::new(),
            control_order: VecDeque::new(),
            bulk_order: VecDeque::new(),
            control_burst: 0,
            len: 0,
        }
    }
}

impl<T> FairScheduler<T> {
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn push(&mut self, owner: WriterOwner, class: WriterClass, enqueued_at: Instant, value: T) {
        let queues = self.owners.entry(owner.clone()).or_default();
        let queue = match class {
            WriterClass::Control => &mut queues.control,
            WriterClass::Bulk => &mut queues.bulk,
        };
        if queue.is_empty() {
            match class {
                WriterClass::Control => self.control_order.push_back(owner),
                WriterClass::Bulk => self.bulk_order.push_back(owner),
            }
        }
        queue.push_back(Scheduled { enqueued_at, value });
        self.len += 1;
        debug_assert!(self.owners.len() <= MAX_ACTIVE_OWNERS);
        debug_assert!(self.len <= CHANNEL_MESSAGES);
    }

    fn pop(&mut self, now: Instant) -> Option<T> {
        let has_control = !self.control_order.is_empty();
        let has_bulk = !self.bulk_order.is_empty();
        let class = match (has_control, has_bulk) {
            (false, false) => return None,
            (true, false) => WriterClass::Control,
            (false, true) => WriterClass::Bulk,
            (true, true) if self.bulk_should_run(now) => WriterClass::Bulk,
            (true, true) => WriterClass::Control,
        };
        self.pop_selected(class)
    }

    fn pop_control_for_group(&mut self, now: Instant) -> Option<T> {
        if self.control_order.is_empty() || self.bulk_should_run(now) {
            return None;
        }
        self.pop_selected(WriterClass::Control)
    }

    fn bulk_should_run(&self, now: Instant) -> bool {
        !self.bulk_order.is_empty()
            && (self.control_burst >= MAX_CONTROL_BURST
                || (self.control_burst > 0 && self.bulk_is_aged(now)))
    }

    fn pop_selected(&mut self, class: WriterClass) -> Option<T> {
        let value = self.pop_class(class)?;
        if class == WriterClass::Control {
            self.control_burst = self.control_burst.saturating_add(1);
        } else {
            self.control_burst = 0;
        }
        Some(value)
    }

    fn bulk_is_aged(&self, now: Instant) -> bool {
        self.bulk_order.iter().any(|owner| {
            self.owners
                .get(owner)
                .and_then(|queues| queues.bulk.front())
                .is_some_and(|item| now.saturating_duration_since(item.enqueued_at) >= BULK_AGING)
        })
    }

    fn pop_class(&mut self, class: WriterClass) -> Option<T> {
        let owner = match class {
            WriterClass::Control => self.control_order.pop_front()?,
            WriterClass::Bulk => self.bulk_order.pop_front()?,
        };
        let (scheduled, class_has_more, owner_is_empty) = {
            let queues = self
                .owners
                .get_mut(&owner)
                .expect("scheduled writer owner missing");
            let queue = match class {
                WriterClass::Control => &mut queues.control,
                WriterClass::Bulk => &mut queues.bulk,
            };
            let scheduled = queue.pop_front().expect("scheduled writer class empty");
            (
                scheduled,
                !queue.is_empty(),
                queues.control.is_empty() && queues.bulk.is_empty(),
            )
        };
        if class_has_more {
            match class {
                WriterClass::Control => self.control_order.push_back(owner.clone()),
                WriterClass::Bulk => self.bulk_order.push_back(owner.clone()),
            }
        }
        if owner_is_empty {
            self.owners.remove(&owner);
        }
        self.len = self.len.checked_sub(1).expect("writer scheduler underflow");
        Some(scheduled.value)
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
    let mut scheduler = FairScheduler::default();
    let mut receiver_open = true;
    loop {
        if scheduler.is_empty() {
            if !receiver_open {
                break;
            }
            match receiver.recv() {
                Ok(message) => schedule_writer_message(&mut scheduler, message),
                Err(_) => break,
            }
        }
        drain_writer_channel(&receiver, &mut scheduler, &mut receiver_open);
        let Some(first) = scheduler.pop(Instant::now()) else {
            continue;
        };
        let deadline = Instant::now() + GROUP_LATENCY;
        let mut messages = vec![first];
        let mut bytes = message_bytes(&messages[0]);
        let mut objects = messages[0].objects.len();
        let control_group = messages[0].reservation.class == WriterClass::Control;
        // A binary bulk request is already a useful durability group and must
        // not pay an artificial latency timer. Singleton legacy requests wait
        // briefly so concurrent callers can share one barrier.
        let coalesce_singletons = objects == 1;

        while coalesce_singletons && bytes < GROUP_TARGET_BYTES && objects < GROUP_OBJECT_TARGET {
            drain_writer_channel(&receiver, &mut scheduler, &mut receiver_open);
            let now = Instant::now();
            let next = if control_group {
                scheduler.pop_control_for_group(now)
            } else {
                scheduler.pop(now)
            };
            if let Some(message) = next {
                bytes = bytes.saturating_add(message_bytes(&message));
                objects = objects.saturating_add(message.objects.len());
                messages.push(message);
                continue;
            }
            if !scheduler.is_empty() {
                // A queued bulk message must not lengthen the durability path
                // of a control-first group. Commit control now, then serve the
                // bulk owner according to burst/aging fairness.
                break;
            }
            if !receiver_open {
                break;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match receiver.recv_timeout(remaining) {
                Ok(message) => schedule_writer_message(&mut scheduler, message),
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => {
                    receiver_open = false;
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
        if report.fatal {
            break;
        }
    }
}

fn schedule_writer_message(scheduler: &mut FairScheduler<WriterMessage>, message: WriterMessage) {
    scheduler.push(
        message.reservation.owner.clone(),
        message.reservation.class,
        message.enqueued_at,
        message,
    );
}

fn drain_writer_channel(
    receiver: &Receiver<WriterMessage>,
    scheduler: &mut FairScheduler<WriterMessage>,
    receiver_open: &mut bool,
) {
    if !*receiver_open {
        return;
    }
    loop {
        match receiver.try_recv() {
            Ok(message) => schedule_writer_message(scheduler, message),
            Err(TryRecvError::Empty) => return,
            Err(TryRecvError::Disconnected) => {
                *receiver_open = false;
                return;
            }
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

    #[test]
    fn fair_scheduler_is_owner_round_robin_control_bounded_and_aging_aware() {
        #[derive(Debug, PartialEq, Eq)]
        enum Work {
            Control(usize),
            Bulk(usize),
        }

        let now = Instant::now();
        let noisy = WriterOwner::authenticated("noisy").unwrap();
        let neighbor = WriterOwner::authenticated("neighbor").unwrap();
        let mut scheduler = FairScheduler::default();
        for index in 0..4 {
            scheduler.push(noisy.clone(), WriterClass::Bulk, now, Work::Bulk(index));
        }
        scheduler.push(neighbor.clone(), WriterClass::Bulk, now, Work::Bulk(100));
        assert_eq!(scheduler.pop(now), Some(Work::Bulk(0)));
        assert_eq!(scheduler.pop(now), Some(Work::Bulk(100)));
        assert_eq!(scheduler.pop(now), Some(Work::Bulk(1)));

        let mut bounded = FairScheduler::default();
        bounded.push(noisy.clone(), WriterClass::Bulk, now, Work::Bulk(200));
        for index in 0..=MAX_CONTROL_BURST {
            bounded.push(
                neighbor.clone(),
                WriterClass::Control,
                now,
                Work::Control(index),
            );
        }
        for index in 0..MAX_CONTROL_BURST {
            assert_eq!(bounded.pop(now), Some(Work::Control(index)));
        }
        assert_eq!(bounded.pop(now), Some(Work::Bulk(200)));
        assert_eq!(bounded.pop(now), Some(Work::Control(MAX_CONTROL_BURST)));

        let mut aged = FairScheduler::default();
        aged.push(
            noisy,
            WriterClass::Bulk,
            now.checked_sub(BULK_AGING).unwrap(),
            Work::Bulk(300),
        );
        aged.push(neighbor, WriterClass::Control, now, Work::Control(300));
        assert_eq!(aged.pop(now), Some(Work::Control(300)));
        assert_eq!(aged.pop(now), Some(Work::Bulk(300)));
        assert!(aged.is_empty());
    }

    #[test]
    fn admission_reserves_control_and_enforces_global_owner_caps_with_cleanup() {
        const MIB: u64 = 1024 * 1024;
        let noisy = WriterOwner::authenticated("noisy").unwrap();
        let mut owner_limited = QueueState::default();
        for _ in 0..OWNER_BULK_MESSAGES {
            owner_limited
                .reserve(&noisy, WriterClass::Bulk, MIB)
                .unwrap();
        }
        assert!(matches!(
            owner_limited.reserve(&noisy, WriterClass::Bulk, 1),
            Err(StoreError::Busy)
        ));
        owner_limited
            .reserve(&noisy, WriterClass::Control, 1)
            .unwrap();
        owner_limited.release(&noisy, WriterClass::Control, 1);
        for _ in 0..OWNER_BULK_MESSAGES {
            owner_limited.release(&noisy, WriterClass::Bulk, MIB);
        }
        assert_eq!(owner_limited.messages, 0);
        assert_eq!(owner_limited.bytes, 0);
        assert!(owner_limited.owners.is_empty());

        let byte_limited = WriterOwner::authenticated("byte-limited").unwrap();
        owner_limited
            .reserve(&byte_limited, WriterClass::Bulk, OWNER_BULK_BYTES)
            .unwrap();
        assert!(matches!(
            owner_limited.reserve(&byte_limited, WriterClass::Bulk, 1),
            Err(StoreError::Busy)
        ));
        owner_limited.release(&byte_limited, WriterClass::Bulk, OWNER_BULK_BYTES);
        assert!(owner_limited.owners.is_empty());

        let control_limited = WriterOwner::authenticated("control-limited").unwrap();
        owner_limited
            .reserve(&control_limited, WriterClass::Control, OWNER_CONTROL_BYTES)
            .unwrap();
        assert!(matches!(
            owner_limited.reserve(&control_limited, WriterClass::Control, 1),
            Err(StoreError::Busy)
        ));
        owner_limited.release(&control_limited, WriterClass::Control, OWNER_CONTROL_BYTES);
        assert!(owner_limited.owners.is_empty());

        let mut saturated = QueueState::default();
        let mut bulk_owners = Vec::new();
        for index in 0..BULK_QUEUE_MESSAGES {
            let owner = WriterOwner::authenticated(&format!("bulk-{}", index / 8)).unwrap();
            saturated.reserve(&owner, WriterClass::Bulk, MIB).unwrap();
            bulk_owners.push(owner);
        }
        let extra_bulk = WriterOwner::authenticated("extra-bulk").unwrap();
        assert!(matches!(
            saturated.reserve(&extra_bulk, WriterClass::Bulk, 1),
            Err(StoreError::Busy)
        ));
        let mut control_owners = Vec::new();
        for index in 0..(CHANNEL_MESSAGES - BULK_QUEUE_MESSAGES) {
            let owner = WriterOwner::authenticated(&format!("control-{index}")).unwrap();
            saturated.reserve(&owner, WriterClass::Control, 1).unwrap();
            control_owners.push(owner);
        }
        assert_eq!(saturated.messages, CHANNEL_MESSAGES);
        assert_eq!(saturated.bulk_messages, BULK_QUEUE_MESSAGES);
        assert!(saturated.bytes <= QUEUE_BYTES);
        assert!(saturated.owners.len() <= MAX_ACTIVE_OWNERS);
        assert!(matches!(
            saturated.reserve(
                &WriterOwner::authenticated("sixty-fifth").unwrap(),
                WriterClass::Control,
                1,
            ),
            Err(StoreError::Busy)
        ));
        for owner in bulk_owners {
            saturated.release(&owner, WriterClass::Bulk, MIB);
        }
        for owner in control_owners {
            saturated.release(&owner, WriterClass::Control, 1);
        }
        assert!(saturated.owners.is_empty());

        let exact = WriterOwner::internal();
        saturated
            .reserve(&exact, WriterClass::Control, QUEUE_BYTES)
            .unwrap();
        assert!(matches!(
            saturated.reserve(&extra_bulk, WriterClass::Bulk, 1),
            Err(StoreError::Busy)
        ));
        saturated.release(&exact, WriterClass::Control, QUEUE_BYTES);
        assert_eq!(saturated.messages, 0);
        assert_eq!(saturated.bytes, 0);
        assert!(saturated.owners.is_empty());
    }

    fn paused_writer(
        layout: StorageLayout,
        perf: Arc<ServerPerfCounters>,
    ) -> (
        StorageWriter,
        Receiver<WriterMessage>,
        ActiveSegment,
        PackStore,
    ) {
        let OpenedPackStore { store, active } = PackStore::open(layout, Arc::clone(&perf)).unwrap();
        let (sender, receiver) = mpsc::sync_channel(CHANNEL_MESSAGES);
        let inner = Arc::new(WriterHandle {
            sender,
            queue: Mutex::new(QueueState::default()),
            perf,
            store: store.clone(),
            read_pool: BlockingPool::new("paused storage reads", READ_TASK_PARALLELISM),
        });
        (StorageWriter { inner }, receiver, active, store)
    }

    #[test]
    fn preprocessing_admission_is_bounded_before_enqueue_and_drop_releases_it() {
        const MIB: usize = 1024 * 1024;
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let perf = Arc::new(ServerPerfCounters::default());
        let (writer, receiver, _active, _store) = paused_writer(layout, Arc::clone(&perf));

        let mut admissions = Vec::new();
        for _ in 0..OWNER_BULK_MESSAGES {
            admissions.push(
                writer
                    .admit_batch_for("preprocessing-device", WriterClass::Bulk, MIB)
                    .unwrap(),
            );
        }
        assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(
            writer.admit_batch_for("preprocessing-device", WriterClass::Bulk, 1),
            Err(StoreError::Busy)
        ));
        {
            let queue = writer
                .inner
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(queue.messages, OWNER_BULK_MESSAGES);
            assert_eq!(queue.bytes, OWNER_BULK_BYTES);
            assert_eq!(queue.owners.len(), 1);
        }

        drop(admissions);
        let queue = writer
            .inner
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(queue.messages, 0);
        assert_eq!(queue.bytes, 0);
        assert!(queue.owners.is_empty());
        assert_eq!(perf.snapshot().storage.writer_queue_depth, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn preprocessing_admission_transfers_once_and_rejects_expansion_cleanly() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let perf = Arc::new(ServerPerfCounters::default());
        let (writer, receiver, _active, _store) = paused_writer(layout, Arc::clone(&perf));

        let too_small = writer
            .admit_batch_for("expanding-device", WriterClass::Bulk, 1024)
            .unwrap();
        let error = writer
            .store_admitted(too_small, vec![content(71, 2048)])
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::InvalidObject(_)));
        assert_eq!(
            writer
                .inner
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .messages,
            0
        );

        let admission = writer
            .admit_batch_for("transfer-device", WriterClass::Bulk, 4096)
            .unwrap();
        let task_writer = writer.clone();
        let task = tokio::spawn(async move {
            task_writer
                .store_admitted(admission, vec![content(72, 4096)])
                .await
        });
        let message = receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        {
            let queue = writer
                .inner
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(queue.messages, 1);
            assert_eq!(queue.bytes, 4096);
        }
        assert_eq!(perf.snapshot().storage.writer_queue_depth, 1);

        drop(message);
        assert!(matches!(task.await.unwrap(), Err(StoreError::Closed)));
        let queue = writer
            .inner
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(queue.messages, 0);
        assert_eq!(queue.bytes, 0);
        assert!(queue.owners.is_empty());
        assert_eq!(perf.snapshot().storage.writer_queue_depth, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_waiter_keeps_write_owned_until_durable_commit() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let perf = Arc::new(ServerPerfCounters::default());
        let (writer, receiver, active, store) = paused_writer(layout, Arc::clone(&perf));
        let object = content(70, 4096);
        let hash = object.hash;
        let task_writer = writer.clone();
        let waiter = tokio::spawn(async move {
            task_writer
                .store_batch_for("cancelled-device", WriterClass::Bulk, vec![object])
                .await
        });
        for _ in 0..100 {
            if writer
                .inner
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .messages
                == 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            writer
                .inner
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .messages,
            1
        );
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert_eq!(
            writer
                .inner
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .messages,
            1
        );

        let thread_store = store.clone();
        let thread = std::thread::spawn(move || writer_loop(receiver, active, thread_store));
        for _ in 0..100 {
            if store.contains(StorageObjectKind::Content, &hash)
                && writer
                    .inner
                    .queue
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .messages
                    == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(store.contains(StorageObjectKind::Content, &hash));
        assert_eq!(perf.snapshot().storage.fdatasyncs, 1);
        assert_eq!(
            writer
                .inner
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .messages,
            0
        );
        drop(writer);
        thread.join().unwrap();
    }

    #[tokio::test]
    async fn closed_writer_releases_owner_and_global_reservations() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        let perf = Arc::new(ServerPerfCounters::default());
        let (writer, receiver, active, _store) = paused_writer(layout, Arc::clone(&perf));
        drop(receiver);
        drop(active);
        let result = writer
            .store_batch_for("closed-device", WriterClass::Bulk, vec![content(71, 1024)])
            .await;
        assert!(matches!(result, Err(StoreError::Closed)));
        let queue = writer
            .inner
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(queue.messages, 0);
        assert_eq!(queue.bytes, 0);
        assert!(queue.owners.is_empty());
        assert_eq!(perf.snapshot().storage.writer_queue_depth, 0);
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
