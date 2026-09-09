//! Versioned, dependency-free durability boundary for live documents.
//!
//! This module deliberately has no HTTP, WebSocket, editor, or CRDT codec
//! policy.  A caller must first open an exact `(vault, doc_id, epoch, path)`
//! session.  Accepted updates are immutable records and the durable head is
//! the acknowledgement boundary. A deterministic materializer consumes the
//! bounded snapshot/suffix view and prepares an epoch-bound file revision.

use crate::storage::StorageLayout;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

pub const DOCUMENT_SCHEMA: u32 = 1;
pub const MAX_DOCUMENT_PATH_BYTES: usize = 4 * 1024;
pub const MAX_DOCUMENT_UPDATE_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_DOCUMENT_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_DOCUMENT_LIVE_OPS: u64 = 4_096;
pub const MAX_DOCUMENT_LIVE_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_DOCUMENT_CLIENTS: usize = 32;
pub const MAX_DOCUMENT_READ_OPS: usize = 256;
pub const MAX_DOCUMENT_READ_BYTES: usize = 4 * 1024 * 1024;

const STORE_DIR: &str = "live-doc-v1";
const OP_MAGIC: &[u8; 4] = b"ODO1";
const SNAPSHOT_MAGIC: &[u8; 4] = b"ODS1";
const RECORD_HEADER_BYTES: usize = 44;
const CHECKSUM_BYTES: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocumentError {
    Invalid(&'static str),
    NotFound,
    BaseConflict,
    StaleSession,
    LeaseHeld,
    LeaseMismatch,
    Sequence { expected: u64, actual: u64 },
    MutationMismatch,
    Capacity(&'static str),
    Generation { expected: u64, actual: u64 },
    Corrupt(String),
    Io(String),
}

impl std::fmt::Display for DocumentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) => write!(f, "invalid live-document request: {message}"),
            Self::NotFound => f.write_str("live document not found"),
            Self::BaseConflict => f.write_str("live document activation base differs"),
            Self::StaleSession => f.write_str("live document session is stale"),
            Self::LeaseHeld => {
                f.write_str("live document already has a writer; fork conflict instead")
            }
            Self::LeaseMismatch => f.write_str("live document writer lease is stale"),
            Self::Sequence { expected, actual } => {
                write!(f, "live document sequence {actual}; expected {expected}")
            }
            Self::MutationMismatch => {
                f.write_str("live document sequence was reused with different bytes")
            }
            Self::Capacity(message) => write!(f, "live document capacity reached: {message}"),
            Self::Generation { expected, actual } => {
                write!(f, "live document generation {actual}; expected {expected}")
            }
            Self::Corrupt(message) => write!(f, "live document store is corrupt: {message}"),
            Self::Io(message) => write!(f, "live document I/O failed: {message}"),
        }
    }
}

impl std::error::Error for DocumentError {}

impl From<std::io::Error> for DocumentError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DocumentIdentity {
    pub schema: u32,
    pub vault_id: String,
    pub doc_id: String,
    pub epoch: String,
    pub path: String,
    pub base_content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Activation {
    pub identity: DocumentIdentity,
    pub created: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Binding {
    schema: u32,
    kind: String,
    identity: DocumentIdentity,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ClientCursor {
    client_id: String,
    sequence: u64,
    watermark: u64,
    digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SnapshotRef {
    pub generation: u64,
    pub watermark: u64,
    pub bytes: u64,
    pub content_hash: String,
    file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileState {
    pub watermark: u64,
    pub content_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WriterLease {
    pub lease_id: String,
    pub client_id: String,
    pub base_revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PreparedFileRevision {
    pub transaction_id: String,
    pub lease_id: String,
    pub revision: u64,
    pub watermark: u64,
    pub content_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct DocumentHead {
    schema: u32,
    kind: String,
    identity: DocumentIdentity,
    generation: u64,
    watermark: u64,
    live_ops: u64,
    live_bytes: u64,
    snapshot: Option<SnapshotRef>,
    file_revision: u64,
    file: FileState,
    lease: Option<WriterLease>,
    prepared_file: Option<PreparedFileRevision>,
    clients: Vec<ClientCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentStatus {
    pub identity: DocumentIdentity,
    pub generation: u64,
    pub watermark: u64,
    pub snapshot: Option<SnapshotRef>,
    pub file_revision: u64,
    pub file: FileState,
    pub lease: Option<WriterLease>,
    pub prepared_file: Option<PreparedFileRevision>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendAck {
    pub epoch: String,
    pub client_sequence: u64,
    pub watermark: u64,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableUpdate {
    pub watermark: u64,
    pub client_id: String,
    pub client_sequence: u64,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePage {
    pub from_watermark: u64,
    pub next_watermark: u64,
    pub durable_watermark: u64,
    pub done: bool,
    pub updates: Vec<DurableUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotBytes {
    pub reference: SnapshotRef,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRevisionWitness {
    pub doc_id: String,
    pub epoch: String,
    pub watermark: u64,
    pub content_hash: String,
    pub lease_id: String,
    pub revision: u64,
    pub transaction_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileWriteFence {
    FileMode,
    AuthorizedLeaseRevision,
    ActiveDocument(DocumentIdentity),
}

#[derive(Debug, Clone, Copy)]
struct Limits {
    update_bytes: usize,
    snapshot_bytes: usize,
    live_ops: u64,
    live_bytes: u64,
    clients: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            update_bytes: MAX_DOCUMENT_UPDATE_BYTES,
            snapshot_bytes: MAX_DOCUMENT_SNAPSHOT_BYTES,
            live_ops: MAX_DOCUMENT_LIVE_OPS,
            live_bytes: MAX_DOCUMENT_LIVE_BYTES,
            clients: MAX_DOCUMENT_CLIENTS,
        }
    }
}

#[derive(Clone)]
pub struct DocumentStore {
    layout: StorageLayout,
    locks: Arc<Mutex<HashMap<String, Weak<Mutex<()>>>>>,
    limits: Limits,
}

#[derive(Clone)]
pub struct DocumentSession {
    store: DocumentStore,
    identity: DocumentIdentity,
}

impl DocumentStore {
    pub fn new(layout: StorageLayout) -> Self {
        Self {
            layout,
            locks: Arc::new(Mutex::new(HashMap::new())),
            limits: Limits::default(),
        }
    }

    #[cfg(test)]
    fn with_limits(layout: StorageLayout, limits: Limits) -> Self {
        Self {
            layout,
            locks: Arc::new(Mutex::new(HashMap::new())),
            limits,
        }
    }

    /// Create exactly one registration epoch for a path. Concurrent callers with
    /// the same base join the existing identity; a different base must be
    /// reconciled before activation and is never imported a second time.
    pub fn activate(
        &self,
        vault_id: &str,
        path: &str,
        base_content_hash: &str,
    ) -> Result<Activation, DocumentError> {
        validate_vault(vault_id)?;
        validate_path(path)?;
        validate_hash(base_content_hash)?;
        let key = format!("binding:{}:{}", vault_key(vault_id), path_key(path));
        let lock = self.lock_for(&key);
        let _held = lock
            .lock()
            .map_err(|_| DocumentError::Corrupt("binding lock poisoned".into()))?;
        if let Some(binding) = self.load_binding(vault_id, path)? {
            if binding.identity.base_content_hash != base_content_hash {
                return Err(DocumentError::BaseConflict);
            }
            self.load_head(&binding.identity)?;
            return Ok(Activation {
                identity: binding.identity,
                created: false,
            });
        }

        let identity = DocumentIdentity {
            schema: DOCUMENT_SCHEMA,
            vault_id: vault_id.to_owned(),
            doc_id: random_id(),
            epoch: random_id(),
            path: path.to_owned(),
            base_content_hash: base_content_hash.to_owned(),
        };
        let head = DocumentHead {
            schema: DOCUMENT_SCHEMA,
            kind: "live-document-head".into(),
            identity: identity.clone(),
            generation: 1,
            watermark: 0,
            live_ops: 0,
            live_bytes: 0,
            snapshot: None,
            file_revision: 0,
            file: FileState {
                watermark: 0,
                content_hash: base_content_hash.to_owned(),
            },
            lease: None,
            prepared_file: None,
            clients: Vec::new(),
        };
        let doc_dir = self.doc_dir(&identity);
        create_dir_durable(&doc_dir)?;
        create_dir_durable(&doc_dir.join("ops"))?;
        create_dir_durable(&doc_dir.join("snapshots"))?;
        atomic_json(&self.head_path(&identity), &head)?;
        let binding = Binding {
            schema: DOCUMENT_SCHEMA,
            kind: "live-document-binding".into(),
            identity: identity.clone(),
        };
        atomic_json(&self.binding_path(vault_id, path), &binding)?;
        Ok(Activation {
            identity,
            created: true,
        })
    }

    pub fn open(&self, identity: &DocumentIdentity) -> Result<DocumentSession, DocumentError> {
        validate_identity(identity)?;
        let binding = self
            .load_binding(&identity.vault_id, &identity.path)?
            .ok_or(DocumentError::NotFound)?;
        if &binding.identity != identity {
            return Err(DocumentError::StaleSession);
        }
        let head = self.load_head(identity)?;
        if head.identity != *identity {
            return Err(DocumentError::StaleSession);
        }
        Ok(DocumentSession {
            store: self.clone(),
            identity: identity.clone(),
        })
    }

    /// A file-mode writer has no witness and is rejected for an active doc.
    /// A materializer is admitted only for the exact prepared file tuple.
    pub fn file_write_fence(
        &self,
        vault_id: &str,
        path: &str,
        witness: Option<&FileRevisionWitness>,
    ) -> Result<FileWriteFence, DocumentError> {
        validate_vault(vault_id)?;
        validate_path(path)?;
        let Some(binding) = self.load_binding(vault_id, path)? else {
            return Ok(FileWriteFence::FileMode);
        };
        let head = self.load_head(&binding.identity)?;
        let authorized =
            witness
                .zip(head.prepared_file.as_ref())
                .is_some_and(|(candidate, prepared)| {
                    candidate.doc_id == head.identity.doc_id
                        && candidate.epoch == head.identity.epoch
                        && candidate.lease_id == prepared.lease_id
                        && candidate.revision == prepared.revision
                        && candidate.watermark == prepared.watermark
                        && candidate.content_hash == prepared.content_hash
                        && candidate.transaction_id == prepared.transaction_id
                });
        Ok(if authorized {
            FileWriteFence::AuthorizedLeaseRevision
        } else if head.lease.is_none()
            && head.prepared_file.is_none()
            && head.file.watermark == head.watermark
        {
            FileWriteFence::FileMode
        } else {
            FileWriteFence::ActiveDocument(head.identity)
        })
    }

    fn lock_for(&self, key: &str) -> Arc<Mutex<()>> {
        let mut locks = self.locks.lock().expect("document lock registry poisoned");
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(key.to_owned(), Arc::downgrade(&lock));
        lock
    }

    fn vault_dir(&self, vault_id: &str) -> PathBuf {
        self.layout.base.join(STORE_DIR).join(vault_key(vault_id))
    }
    fn binding_path(&self, vault_id: &str, path: &str) -> PathBuf {
        self.vault_dir(vault_id)
            .join("bindings")
            .join(format!("{}.json", path_key(path)))
    }
    fn doc_dir(&self, identity: &DocumentIdentity) -> PathBuf {
        self.vault_dir(&identity.vault_id)
            .join("docs")
            .join(&identity.doc_id)
    }
    fn head_path(&self, identity: &DocumentIdentity) -> PathBuf {
        self.doc_dir(identity).join("head.json")
    }
    fn op_path(&self, identity: &DocumentIdentity, watermark: u64) -> PathBuf {
        self.doc_dir(identity)
            .join("ops")
            .join(format!("op-{watermark:020}.bin"))
    }
    fn snapshot_path(&self, identity: &DocumentIdentity, file: &str) -> PathBuf {
        self.doc_dir(identity).join("snapshots").join(file)
    }

    fn load_binding(&self, vault_id: &str, path: &str) -> Result<Option<Binding>, DocumentError> {
        let Some(binding): Option<Binding> =
            read_json_optional(&self.binding_path(vault_id, path))?
        else {
            return Ok(None);
        };
        if binding.schema != DOCUMENT_SCHEMA
            || binding.kind != "live-document-binding"
            || binding.identity.vault_id != vault_id
            || binding.identity.path != path
        {
            return Err(DocumentError::Corrupt("binding identity mismatch".into()));
        }
        validate_identity(&binding.identity)?;
        Ok(Some(binding))
    }

    fn load_head(&self, identity: &DocumentIdentity) -> Result<DocumentHead, DocumentError> {
        let head: DocumentHead = read_json(&self.head_path(identity))?;
        validate_head(&head)?;
        if head.identity != *identity {
            return Err(DocumentError::StaleSession);
        }
        Ok(head)
    }
}

impl DocumentSession {
    pub fn status(&self) -> Result<DocumentStatus, DocumentError> {
        let head = self.store.load_head(&self.identity)?;
        Ok(status(&head))
    }

    /// Acquire the only writer slot for this file revision. Reopening from the
    /// same client returns the durable token; another client must preserve its
    /// content as a fork/conflict instead of silently merging it.
    pub fn acquire_writer(
        &self,
        client_id: &str,
        expected_file_revision: u64,
    ) -> Result<WriterLease, DocumentError> {
        validate_id(client_id, "client id")?;
        let lock = self
            .store
            .lock_for(&format!("doc:{}", self.identity.doc_id));
        let _held = lock
            .lock()
            .map_err(|_| DocumentError::Corrupt("document lock poisoned".into()))?;
        let mut head = self.store.load_head(&self.identity)?;
        if expected_file_revision != head.file_revision() {
            return Err(DocumentError::Generation {
                expected: expected_file_revision,
                actual: head.file_revision(),
            });
        }
        if let Some(lease) = &head.lease {
            return if lease.client_id == client_id {
                Ok(lease.clone())
            } else {
                Err(DocumentError::LeaseHeld)
            };
        }
        if head.prepared_file.is_some() || head.file.watermark != head.watermark {
            return Err(DocumentError::LeaseHeld);
        }
        let lease = WriterLease {
            lease_id: random_id(),
            client_id: client_id.to_owned(),
            base_revision: head.file_revision(),
        };
        head.lease = Some(lease.clone());
        head.generation = head
            .generation
            .checked_add(1)
            .ok_or(DocumentError::Capacity("generation"))?;
        atomic_json(&self.store.head_path(&self.identity), &head)?;
        Ok(lease)
    }

    /// Accept one client-sequenced update. A client may have only one
    /// unacknowledged request per document: replay of its last sequence is
    /// idempotent, while gaps and byte-changing reuse fail closed.
    pub fn append(
        &self,
        client_id: &str,
        lease_id: &str,
        client_sequence: u64,
        update: &[u8],
    ) -> Result<AppendAck, DocumentError> {
        validate_id(client_id, "client id")?;
        validate_id(lease_id, "lease id")?;
        if client_sequence == 0 {
            return Err(DocumentError::Invalid("client sequence must be positive"));
        }
        if update.is_empty() {
            return Err(DocumentError::Invalid("empty update"));
        }
        if update.len() > self.store.limits.update_bytes {
            return Err(DocumentError::Capacity("update bytes"));
        }
        let lock = self
            .store
            .lock_for(&format!("doc:{}", self.identity.doc_id));
        let _held = lock
            .lock()
            .map_err(|_| DocumentError::Corrupt("document lock poisoned".into()))?;
        let mut head = self.store.load_head(&self.identity)?;
        if !head
            .lease
            .as_ref()
            .is_some_and(|lease| lease.client_id == client_id && lease.lease_id == lease_id)
        {
            return Err(DocumentError::LeaseMismatch);
        }
        let digest = mutation_digest(&self.identity, client_id, client_sequence, update);
        if let Some(cursor) = head
            .clients
            .iter()
            .find(|cursor| cursor.client_id == client_id)
        {
            if client_sequence == cursor.sequence {
                if cursor.digest != digest {
                    return Err(DocumentError::MutationMismatch);
                }
                return Ok(AppendAck {
                    epoch: self.identity.epoch.clone(),
                    client_sequence,
                    watermark: cursor.watermark,
                    duplicate: true,
                });
            }
            let expected = cursor
                .sequence
                .checked_add(1)
                .ok_or(DocumentError::Capacity("client sequence"))?;
            if client_sequence != expected {
                return Err(DocumentError::Sequence {
                    expected,
                    actual: client_sequence,
                });
            }
        } else {
            if client_sequence != 1 {
                return Err(DocumentError::Sequence {
                    expected: 1,
                    actual: client_sequence,
                });
            }
            if head.clients.len() >= self.store.limits.clients {
                return Err(DocumentError::Capacity("client cardinality"));
            }
        }
        if head.live_ops >= self.store.limits.live_ops {
            return Err(DocumentError::Capacity(
                "live operation count; compact first",
            ));
        }
        let update_len =
            u64::try_from(update.len()).map_err(|_| DocumentError::Capacity("update length"))?;
        let live_bytes = head
            .live_bytes
            .checked_add(update_len)
            .ok_or(DocumentError::Capacity("live bytes"))?;
        if live_bytes > self.store.limits.live_bytes {
            return Err(DocumentError::Capacity(
                "live operation bytes; compact first",
            ));
        }
        let watermark = head
            .watermark
            .checked_add(1)
            .ok_or(DocumentError::Capacity("watermark"))?;
        let record = encode_record(OP_MAGIC, watermark, client_sequence, client_id, update)?;
        let op_path = self.store.op_path(&self.identity, watermark);
        if op_path.exists() {
            let orphan = read_record(&op_path, OP_MAGIC, self.store.limits.update_bytes)?;
            if orphan.watermark != watermark
                || orphan.client_sequence != client_sequence
                || orphan.client_id != client_id
                || orphan.bytes != update
            {
                return Err(DocumentError::Corrupt(
                    "conflicting orphan operation".into(),
                ));
            }
        } else {
            atomic_bytes(&op_path, &record)?;
        }
        head.watermark = watermark;
        head.live_ops += 1;
        head.live_bytes = live_bytes;
        head.generation = head
            .generation
            .checked_add(1)
            .ok_or(DocumentError::Capacity("generation"))?;
        if let Some(cursor) = head
            .clients
            .iter_mut()
            .find(|cursor| cursor.client_id == client_id)
        {
            cursor.sequence = client_sequence;
            cursor.watermark = watermark;
            cursor.digest = digest;
        } else {
            head.clients.push(ClientCursor {
                client_id: client_id.to_owned(),
                sequence: client_sequence,
                watermark,
                digest,
            });
            head.clients
                .sort_unstable_by(|a, b| a.client_id.cmp(&b.client_id));
        }
        atomic_json(&self.store.head_path(&self.identity), &head)?;
        Ok(AppendAck {
            epoch: self.identity.epoch.clone(),
            client_sequence,
            watermark,
            duplicate: false,
        })
    }

    pub fn read_updates(
        &self,
        after_watermark: u64,
        max_ops: usize,
        max_bytes: usize,
    ) -> Result<UpdatePage, DocumentError> {
        if max_ops == 0 || max_ops > MAX_DOCUMENT_READ_OPS {
            return Err(DocumentError::Invalid("read op limit"));
        }
        if max_bytes == 0 || max_bytes > MAX_DOCUMENT_READ_BYTES {
            return Err(DocumentError::Invalid("read byte limit"));
        }
        let head = self.store.load_head(&self.identity)?;
        let snapshot_watermark = head
            .snapshot
            .as_ref()
            .map_or(0, |snapshot| snapshot.watermark);
        if after_watermark < snapshot_watermark {
            return Err(DocumentError::Invalid("snapshot required before suffix"));
        }
        if after_watermark > head.watermark {
            return Err(DocumentError::Invalid(
                "read cursor exceeds durable watermark",
            ));
        }
        let mut updates = Vec::new();
        let mut bytes = 0usize;
        let mut cursor = after_watermark;
        while cursor < head.watermark && updates.len() < max_ops {
            let next = cursor + 1;
            let record = read_record(
                &self.store.op_path(&self.identity, next),
                OP_MAGIC,
                self.store.limits.update_bytes,
            )?;
            if record.watermark != next {
                return Err(DocumentError::Corrupt(
                    "operation watermark mismatch".into(),
                ));
            }
            let prospective = bytes
                .checked_add(record.bytes.len())
                .ok_or(DocumentError::Capacity("read page bytes"))?;
            if prospective > max_bytes {
                if updates.is_empty() {
                    return Err(DocumentError::Capacity("single update exceeds read page"));
                }
                break;
            }
            bytes = prospective;
            cursor = next;
            updates.push(DurableUpdate {
                watermark: next,
                client_id: record.client_id,
                client_sequence: record.client_sequence,
                bytes: record.bytes,
            });
        }
        Ok(UpdatePage {
            from_watermark: after_watermark,
            next_watermark: cursor,
            durable_watermark: head.watermark,
            done: cursor == head.watermark,
            updates,
        })
    }

    pub fn read_snapshot(&self) -> Result<Option<SnapshotBytes>, DocumentError> {
        let head = self.store.load_head(&self.identity)?;
        let Some(reference) = head.snapshot else {
            return Ok(None);
        };
        let record = read_record(
            &self.store.snapshot_path(&self.identity, &reference.file),
            SNAPSHOT_MAGIC,
            self.store.limits.snapshot_bytes,
        )?;
        if record.watermark != reference.watermark
            || record.client_sequence != reference.generation
            || record.client_id != "00000000000000000000000000000000"
            || record.bytes.len() as u64 != reference.bytes
            || content_hash(&record.bytes) != reference.content_hash
        {
            return Err(DocumentError::Corrupt("snapshot witness mismatch".into()));
        }
        Ok(Some(SnapshotBytes {
            reference,
            bytes: record.bytes,
        }))
    }

    /// Publish a snapshot covering an exact prefix. The lock preserves all
    /// concurrently accepted suffix records; stale materializers cannot
    /// replace a newer head because generation is a CAS witness.
    pub fn compact(
        &self,
        lease_id: &str,
        expected_generation: u64,
        covered_watermark: u64,
        snapshot: &[u8],
    ) -> Result<DocumentStatus, DocumentError> {
        validate_id(lease_id, "lease id")?;
        if snapshot.is_empty() {
            return Err(DocumentError::Invalid("empty snapshot"));
        }
        if snapshot.len() > self.store.limits.snapshot_bytes {
            return Err(DocumentError::Capacity("snapshot bytes"));
        }
        let lock = self
            .store
            .lock_for(&format!("doc:{}", self.identity.doc_id));
        let _held = lock
            .lock()
            .map_err(|_| DocumentError::Corrupt("document lock poisoned".into()))?;
        let mut head = self.store.load_head(&self.identity)?;
        if !head
            .lease
            .as_ref()
            .is_some_and(|lease| lease.lease_id == lease_id)
        {
            return Err(DocumentError::LeaseMismatch);
        }
        if head.generation != expected_generation {
            return Err(DocumentError::Generation {
                expected: expected_generation,
                actual: head.generation,
            });
        }
        let previous = head.snapshot.as_ref().map_or(0, |value| value.watermark);
        if covered_watermark < previous || covered_watermark > head.watermark {
            return Err(DocumentError::Invalid(
                "snapshot watermark does not cover a durable prefix",
            ));
        }
        let generation = head
            .generation
            .checked_add(1)
            .ok_or(DocumentError::Capacity("generation"))?;
        let file = format!("snapshot-{generation:020}-{covered_watermark:020}.bin");
        let record = encode_record(
            SNAPSHOT_MAGIC,
            covered_watermark,
            generation,
            "00000000000000000000000000000000",
            snapshot,
        )?;
        atomic_bytes(&self.store.snapshot_path(&self.identity, &file), &record)?;
        let mut removed_ops = 0u64;
        let mut removed_bytes = 0u64;
        for watermark in (previous + 1)..=covered_watermark {
            let op = read_record(
                &self.store.op_path(&self.identity, watermark),
                OP_MAGIC,
                self.store.limits.update_bytes,
            )?;
            removed_ops = removed_ops
                .checked_add(1)
                .ok_or(DocumentError::Corrupt("operation count overflow".into()))?;
            removed_bytes = removed_bytes
                .checked_add(op.bytes.len() as u64)
                .ok_or(DocumentError::Corrupt("operation bytes overflow".into()))?;
        }
        head.generation = generation;
        head.live_ops = head
            .live_ops
            .checked_sub(removed_ops)
            .ok_or(DocumentError::Corrupt(
                "live operation count underflow".into(),
            ))?;
        head.live_bytes =
            head.live_bytes
                .checked_sub(removed_bytes)
                .ok_or(DocumentError::Corrupt(
                    "live operation bytes underflow".into(),
                ))?;
        let old_snapshot = head.snapshot.replace(SnapshotRef {
            generation,
            watermark: covered_watermark,
            bytes: snapshot.len() as u64,
            content_hash: content_hash(snapshot),
            file: file.clone(),
        });
        atomic_json(&self.store.head_path(&self.identity), &head)?;
        // Publication above is authoritative. Cleanup is best-effort and a
        // crash merely leaves unreachable immutable files for maintenance.
        for watermark in (previous + 1)..=covered_watermark {
            let _ = fs::remove_file(self.store.op_path(&self.identity, watermark));
        }
        if let Some(old) = old_snapshot {
            let _ = fs::remove_file(self.store.snapshot_path(&self.identity, &old.file));
        }
        Ok(status(&head))
    }

    /// Persist an exact root candidate before publication. The root guard can
    /// authorize only this witness; acceptance is confirmed separately so a
    /// lost HTTP/root ACK can replay without inventing a file revision.
    pub fn prepare_file_revision(
        &self,
        lease_id: &str,
        expected_generation: u64,
        watermark: u64,
        projected_content_hash: &str,
    ) -> Result<FileRevisionWitness, DocumentError> {
        validate_id(lease_id, "lease id")?;
        validate_hash(projected_content_hash)?;
        let lock = self
            .store
            .lock_for(&format!("doc:{}", self.identity.doc_id));
        let _held = lock
            .lock()
            .map_err(|_| DocumentError::Corrupt("document lock poisoned".into()))?;
        let mut head = self.store.load_head(&self.identity)?;
        if !head
            .lease
            .as_ref()
            .is_some_and(|lease| lease.lease_id == lease_id)
        {
            return Err(DocumentError::LeaseMismatch);
        }
        if let Some(prepared) = &head.prepared_file {
            if prepared.lease_id == lease_id
                && prepared.watermark == watermark
                && prepared.content_hash == projected_content_hash
            {
                return Ok(FileRevisionWitness {
                    doc_id: self.identity.doc_id.clone(),
                    epoch: self.identity.epoch.clone(),
                    watermark,
                    content_hash: projected_content_hash.to_owned(),
                    lease_id: lease_id.to_owned(),
                    revision: prepared.revision,
                    transaction_id: prepared.transaction_id.clone(),
                });
            }
            return Err(DocumentError::LeaseHeld);
        }
        if head.generation != expected_generation {
            return Err(DocumentError::Generation {
                expected: expected_generation,
                actual: head.generation,
            });
        }
        if watermark < head.file.watermark || watermark > head.watermark {
            return Err(DocumentError::Invalid(
                "file watermark is not a durable monotonic prefix",
            ));
        }
        let revision = head
            .file_revision()
            .checked_add(1)
            .ok_or(DocumentError::Capacity("file revision"))?;
        let transaction_id = random_id();
        let prepared = PreparedFileRevision {
            transaction_id: transaction_id.clone(),
            lease_id: lease_id.to_owned(),
            revision,
            watermark,
            content_hash: projected_content_hash.to_owned(),
        };
        head.generation = head
            .generation
            .checked_add(1)
            .ok_or(DocumentError::Capacity("generation"))?;
        head.prepared_file = Some(prepared);
        atomic_json(&self.store.head_path(&self.identity), &head)?;
        Ok(FileRevisionWitness {
            doc_id: self.identity.doc_id.clone(),
            epoch: self.identity.epoch.clone(),
            watermark,
            content_hash: projected_content_hash.to_owned(),
            lease_id: lease_id.to_owned(),
            revision,
            transaction_id,
        })
    }

    /// Confirm only after the epoch-bound candidate root has a durable
    /// accepted outcome. Replaying the same confirmation is idempotent.
    pub fn confirm_file_revision(
        &self,
        witness: &FileRevisionWitness,
    ) -> Result<DocumentStatus, DocumentError> {
        let lock = self
            .store
            .lock_for(&format!("doc:{}", self.identity.doc_id));
        let _held = lock
            .lock()
            .map_err(|_| DocumentError::Corrupt("document lock poisoned".into()))?;
        let mut head = self.store.load_head(&self.identity)?;
        if head.file_revision() == witness.revision
            && head.file.watermark == witness.watermark
            && head.file.content_hash == witness.content_hash
            && head.prepared_file.is_none()
        {
            return Ok(status(&head));
        }
        let exact = head.prepared_file.as_ref().is_some_and(|prepared| {
            witness.doc_id == head.identity.doc_id
                && witness.epoch == head.identity.epoch
                && witness.lease_id == prepared.lease_id
                && witness.revision == prepared.revision
                && witness.watermark == prepared.watermark
                && witness.content_hash == prepared.content_hash
                && witness.transaction_id == prepared.transaction_id
        });
        if !exact {
            return Err(DocumentError::LeaseMismatch);
        }
        head.file = FileState {
            watermark: witness.watermark,
            content_hash: witness.content_hash.clone(),
        };
        head.file_revision = witness.revision;
        head.prepared_file = None;
        head.generation = head
            .generation
            .checked_add(1)
            .ok_or(DocumentError::Capacity("generation"))?;
        atomic_json(&self.store.head_path(&self.identity), &head)?;
        Ok(status(&head))
    }

    /// Release is allowed only after every durable micro-batch has become the
    /// authoritative file revision and no root candidate is in flight.
    pub fn release_writer(&self, lease_id: &str) -> Result<DocumentStatus, DocumentError> {
        validate_id(lease_id, "lease id")?;
        let lock = self
            .store
            .lock_for(&format!("doc:{}", self.identity.doc_id));
        let _held = lock
            .lock()
            .map_err(|_| DocumentError::Corrupt("document lock poisoned".into()))?;
        let mut head = self.store.load_head(&self.identity)?;
        if !head
            .lease
            .as_ref()
            .is_some_and(|lease| lease.lease_id == lease_id)
        {
            return Err(DocumentError::LeaseMismatch);
        }
        if head.prepared_file.is_some() || head.file.watermark != head.watermark {
            return Err(DocumentError::Invalid("writer has unmaterialized updates"));
        }
        head.lease = None;
        head.generation = head
            .generation
            .checked_add(1)
            .ok_or(DocumentError::Capacity("generation"))?;
        atomic_json(&self.store.head_path(&self.identity), &head)?;
        Ok(status(&head))
    }
}

fn status(head: &DocumentHead) -> DocumentStatus {
    DocumentStatus {
        identity: head.identity.clone(),
        generation: head.generation,
        watermark: head.watermark,
        snapshot: head.snapshot.clone(),
        file_revision: head.file_revision(),
        file: head.file.clone(),
        lease: head.lease.clone(),
        prepared_file: head.prepared_file.clone(),
    }
}

impl DocumentHead {
    fn file_revision(&self) -> u64 {
        self.file_revision
    }
}

struct DecodedRecord {
    watermark: u64,
    client_sequence: u64,
    client_id: String,
    bytes: Vec<u8>,
}

fn encode_record(
    magic: &[u8; 4],
    watermark: u64,
    client_sequence: u64,
    client_id: &str,
    bytes: &[u8],
) -> Result<Vec<u8>, DocumentError> {
    validate_id(client_id, "record client id")?;
    let client = hex::decode(client_id).map_err(|_| DocumentError::Invalid("record client id"))?;
    let length = u32::try_from(bytes.len()).map_err(|_| DocumentError::Capacity("record bytes"))?;
    let mut out = Vec::with_capacity(RECORD_HEADER_BYTES + bytes.len() + CHECKSUM_BYTES);
    out.extend_from_slice(magic);
    out.push(DOCUMENT_SCHEMA as u8);
    out.extend_from_slice(&[0, 0, 0]);
    out.extend_from_slice(&watermark.to_le_bytes());
    out.extend_from_slice(&client_sequence.to_le_bytes());
    out.extend_from_slice(&client);
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(bytes);
    let checksum = blake3::hash(&out);
    out.extend_from_slice(checksum.as_bytes());
    Ok(out)
}

fn read_record(
    path: &Path,
    magic: &[u8; 4],
    max_bytes: usize,
) -> Result<DecodedRecord, DocumentError> {
    let file = File::open(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            DocumentError::Corrupt(format!("missing record {}", path.display()))
        } else {
            error.into()
        }
    })?;
    let cap = RECORD_HEADER_BYTES
        .checked_add(max_bytes)
        .and_then(|v| v.checked_add(CHECKSUM_BYTES))
        .ok_or(DocumentError::Capacity("record read cap"))?;
    if file.metadata()?.len() > cap as u64 {
        return Err(DocumentError::Corrupt("record exceeds limit".into()));
    }
    let mut encoded = Vec::new();
    file.take((cap + 1) as u64).read_to_end(&mut encoded)?;
    if encoded.len() < RECORD_HEADER_BYTES + CHECKSUM_BYTES
        || &encoded[..4] != magic
        || encoded[4] != DOCUMENT_SCHEMA as u8
        || encoded[5..8] != [0, 0, 0]
    {
        return Err(DocumentError::Corrupt("record header".into()));
    }
    let length = u32::from_le_bytes(encoded[40..44].try_into().unwrap()) as usize;
    let expected = RECORD_HEADER_BYTES
        .checked_add(length)
        .and_then(|v| v.checked_add(CHECKSUM_BYTES))
        .ok_or(DocumentError::Corrupt("record length overflow".into()))?;
    if length > max_bytes || encoded.len() != expected {
        return Err(DocumentError::Corrupt("record length mismatch".into()));
    }
    let checksum_at = expected - CHECKSUM_BYTES;
    if blake3::hash(&encoded[..checksum_at]).as_bytes() != &encoded[checksum_at..] {
        return Err(DocumentError::Corrupt("record checksum".into()));
    }
    Ok(DecodedRecord {
        watermark: u64::from_le_bytes(encoded[8..16].try_into().unwrap()),
        client_sequence: u64::from_le_bytes(encoded[16..24].try_into().unwrap()),
        client_id: hex::encode(&encoded[24..40]),
        bytes: encoded[44..checksum_at].to_vec(),
    })
}

fn validate_head(head: &DocumentHead) -> Result<(), DocumentError> {
    if head.schema != DOCUMENT_SCHEMA || head.kind != "live-document-head" {
        return Err(DocumentError::Corrupt("head schema/kind".into()));
    }
    validate_identity(&head.identity)?;
    if head.generation == 0
        || head.file.watermark > head.watermark
        || !is_hash(&head.file.content_hash)
        || head.clients.len() > MAX_DOCUMENT_CLIENTS
    {
        return Err(DocumentError::Corrupt("head bounds".into()));
    }
    if let Some(lease) = &head.lease {
        validate_id(&lease.lease_id, "lease id")
            .map_err(|e| DocumentError::Corrupt(e.to_string()))?;
        validate_id(&lease.client_id, "lease client")
            .map_err(|e| DocumentError::Corrupt(e.to_string()))?;
    }
    if let Some(prepared) = &head.prepared_file {
        if !head
            .lease
            .as_ref()
            .is_some_and(|lease| lease.lease_id == prepared.lease_id)
            || prepared.revision != head.file_revision.checked_add(1).unwrap_or(u64::MAX)
            || prepared.watermark < head.file.watermark
            || prepared.watermark > head.watermark
            || !is_hash(&prepared.content_hash)
        {
            return Err(DocumentError::Corrupt("prepared file revision".into()));
        }
        validate_id(&prepared.transaction_id, "file transaction")
            .map_err(|e| DocumentError::Corrupt(e.to_string()))?;
    }
    let snapshot_watermark = head
        .snapshot
        .as_ref()
        .map_or(0, |snapshot| snapshot.watermark);
    if snapshot_watermark > head.watermark || head.live_ops != head.watermark - snapshot_watermark {
        return Err(DocumentError::Corrupt("head live range".into()));
    }
    let mut previous = None;
    for cursor in &head.clients {
        validate_id(&cursor.client_id, "client cursor")
            .map_err(|e| DocumentError::Corrupt(e.to_string()))?;
        if cursor.sequence == 0
            || cursor.watermark == 0
            || cursor.watermark > head.watermark
            || !is_hash(&cursor.digest)
            || previous.is_some_and(|value: &str| value >= cursor.client_id.as_str())
        {
            return Err(DocumentError::Corrupt("client cursor".into()));
        }
        previous = Some(cursor.client_id.as_str());
    }
    if let Some(snapshot) = &head.snapshot {
        if snapshot.generation == 0
            || snapshot.bytes > MAX_DOCUMENT_SNAPSHOT_BYTES as u64
            || !is_hash(&snapshot.content_hash)
            || !safe_file(&snapshot.file)
        {
            return Err(DocumentError::Corrupt("snapshot reference".into()));
        }
    }
    Ok(())
}

fn validate_identity(identity: &DocumentIdentity) -> Result<(), DocumentError> {
    if identity.schema != DOCUMENT_SCHEMA {
        return Err(DocumentError::Invalid("document schema"));
    }
    validate_vault(&identity.vault_id)?;
    validate_path(&identity.path)?;
    validate_id(&identity.doc_id, "document id")?;
    validate_id(&identity.epoch, "document epoch")?;
    validate_hash(&identity.base_content_hash)
}
fn validate_vault(value: &str) -> Result<(), DocumentError> {
    if value.is_empty() || value.len() > 256 {
        Err(DocumentError::Invalid("vault id"))
    } else {
        Ok(())
    }
}
fn validate_path(value: &str) -> Result<(), DocumentError> {
    if value.is_empty() || value.len() > MAX_DOCUMENT_PATH_BYTES || value.contains('\0') {
        Err(DocumentError::Invalid("document path"))
    } else {
        Ok(())
    }
}
fn validate_id(value: &str, label: &'static str) -> Result<(), DocumentError> {
    if value.len() == 32
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(DocumentError::Invalid(label))
    }
}
fn validate_hash(value: &str) -> Result<(), DocumentError> {
    if is_hash(value) {
        Ok(())
    } else {
        Err(DocumentError::Invalid("content hash"))
    }
}
fn is_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'))
}
fn safe_file(value: &str) -> bool {
    !value.is_empty()
        && !value.contains('/')
        && !value.contains('\\')
        && value != "."
        && value != ".."
}
fn random_id() -> String {
    hex::encode(rand::random::<[u8; 16]>())
}
fn vault_key(value: &str) -> String {
    content_hash(value.as_bytes())
}
fn path_key(value: &str) -> String {
    content_hash(value.as_bytes())
}
fn content_hash(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}
fn mutation_digest(
    identity: &DocumentIdentity,
    client: &str,
    sequence: u64,
    update: &[u8],
) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(identity.doc_id.as_bytes());
    hash.update(identity.epoch.as_bytes());
    hash.update(client.as_bytes());
    hash.update(&sequence.to_le_bytes());
    hash.update(update);
    hash.finalize().to_hex().to_string()
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, DocumentError> {
    read_json_optional(path)?.ok_or(DocumentError::NotFound)
}
fn read_json_optional<T: for<'de> Deserialize<'de>>(
    path: &Path,
) -> Result<Option<T>, DocumentError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    serde_json::from_reader(file)
        .map(Some)
        .map_err(|error| DocumentError::Corrupt(format!("{}: {error}", path.display())))
}

fn atomic_json(path: &Path, value: &impl Serialize) -> Result<(), DocumentError> {
    let bytes =
        serde_json::to_vec(value).map_err(|error| DocumentError::Corrupt(error.to_string()))?;
    atomic_bytes(path, &bytes)
}

fn atomic_bytes(path: &Path, bytes: &[u8]) -> Result<(), DocumentError> {
    let parent = path
        .parent()
        .ok_or(DocumentError::Invalid("storage path parent"))?;
    create_dir_durable(parent)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(DocumentError::Invalid("storage filename"))?;
    let temp = parent.join(format!(".{file_name}.tmp-{}", random_id()));
    let result = (|| -> Result<(), DocumentError> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        sync_directory(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn create_dir_durable(path: &Path) -> Result<(), DocumentError> {
    if path.exists() {
        return Ok(());
    }
    fs::create_dir_all(path)?;
    sync_directory(path)?;
    if let Some(parent) = path.parent() {
        sync_directory(parent)?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), DocumentError> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn fixture() -> (tempfile::TempDir, DocumentStore) {
        let temp = tempdir().unwrap();
        let layout = StorageLayout::new(temp.path());
        layout.init_directories().unwrap();
        (temp, DocumentStore::new(layout))
    }
    fn hash(byte: u8) -> String {
        format!("{byte:02x}").repeat(32)
    }
    const CLIENT_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const CLIENT_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    #[test]
    fn activation_is_single_identity_and_file_mode_is_fenced() {
        let (_temp, store) = fixture();
        let first = store.activate("vault", "notes/live.md", &hash(1)).unwrap();
        assert!(first.created);
        let joined = store.activate("vault", "notes/live.md", &hash(1)).unwrap();
        assert!(!joined.created);
        assert_eq!(joined.identity, first.identity);
        assert_eq!(
            store
                .activate("vault", "notes/live.md", &hash(2))
                .unwrap_err(),
            DocumentError::BaseConflict
        );
        assert!(matches!(
            store
                .file_write_fence("vault", "ordinary.md", None)
                .unwrap(),
            FileWriteFence::FileMode
        ));
        let session = store.open(&first.identity).unwrap();
        assert!(matches!(
            store
                .file_write_fence("vault", "notes/live.md", None)
                .unwrap(),
            FileWriteFence::FileMode
        ));
        let lease = session.acquire_writer(CLIENT_A, 0).unwrap();
        assert!(matches!(
            store
                .file_write_fence("vault", "notes/live.md", None)
                .unwrap(),
            FileWriteFence::ActiveDocument(_)
        ));
        assert_eq!(session.acquire_writer(CLIENT_A, 0).unwrap(), lease);
        assert_eq!(
            session.acquire_writer(CLIENT_B, 0).unwrap_err(),
            DocumentError::LeaseHeld
        );
        let mut stale = first.identity.clone();
        stale.epoch = "cccccccccccccccccccccccccccccccc".into();
        assert!(matches!(
            store.open(&stale),
            Err(DocumentError::StaleSession)
        ));
    }

    #[test]
    fn append_is_durable_sequenced_idempotent_and_bounded() {
        let (temp, store) = fixture();
        let identity = store
            .activate("vault", "live.md", &hash(1))
            .unwrap()
            .identity;
        let session = store.open(&identity).unwrap();
        let lease = session.acquire_writer(CLIENT_A, 0).unwrap();
        let first = session
            .append(CLIENT_A, &lease.lease_id, 1, b"one")
            .unwrap();
        assert_eq!(first.watermark, 1);
        assert!(!first.duplicate);
        let retry = session
            .append(CLIENT_A, &lease.lease_id, 1, b"one")
            .unwrap();
        assert_eq!(retry.watermark, 1);
        assert!(retry.duplicate);
        assert_eq!(
            session
                .append(CLIENT_A, &lease.lease_id, 1, b"different")
                .unwrap_err(),
            DocumentError::MutationMismatch
        );
        assert!(matches!(
            session.append(CLIENT_A, &lease.lease_id, 3, b"gap"),
            Err(DocumentError::Sequence {
                expected: 2,
                actual: 3
            })
        ));
        assert_eq!(
            session
                .append(CLIENT_B, &lease.lease_id, 1, b"two")
                .unwrap_err(),
            DocumentError::LeaseMismatch
        );
        session
            .append(CLIENT_A, &lease.lease_id, 2, b"two")
            .unwrap();
        let page = session.read_updates(0, 2, 6).unwrap();
        assert_eq!(
            page.updates
                .iter()
                .map(|op| op.bytes.as_slice())
                .collect::<Vec<_>>(),
            vec![b"one".as_slice(), b"two".as_slice()]
        );
        assert!(page.done);

        let restarted = DocumentStore::new(StorageLayout::new(temp.path()));
        let restored = restarted.open(&identity).unwrap();
        assert!(
            restored
                .append(CLIENT_A, &lease.lease_id, 2, b"two")
                .unwrap()
                .duplicate
        );
        assert_eq!(restored.status().unwrap().watermark, 2);
    }

    #[test]
    fn compact_cas_preserves_suffix_and_file_revision_is_monotonic() {
        let (temp, store) = fixture();
        let identity = store
            .activate("vault", "live.md", &hash(1))
            .unwrap()
            .identity;
        let session = store.open(&identity).unwrap();
        let lease = session.acquire_writer(CLIENT_A, 0).unwrap();
        session
            .append(CLIENT_A, &lease.lease_id, 1, b"one")
            .unwrap();
        session
            .append(CLIENT_A, &lease.lease_id, 2, b"two")
            .unwrap();
        session
            .append(CLIENT_A, &lease.lease_id, 3, b"three")
            .unwrap();
        let generation = session.status().unwrap().generation;
        let compacted = session
            .compact(&lease.lease_id, generation, 2, b"snapshot-through-two")
            .unwrap();
        assert_eq!(compacted.snapshot.as_ref().unwrap().watermark, 2);
        assert_eq!(
            session.read_snapshot().unwrap().unwrap().bytes,
            b"snapshot-through-two"
        );
        let suffix = session.read_updates(2, 10, 1024).unwrap();
        assert_eq!(suffix.updates.len(), 1);
        assert_eq!(suffix.updates[0].bytes, b"three");
        assert!(matches!(
            session.compact(&lease.lease_id, generation, 3, b"stale"),
            Err(DocumentError::Generation { .. })
        ));

        let projected_hash = hash(9);
        let witness = session
            .prepare_file_revision(&lease.lease_id, compacted.generation, 2, &projected_hash)
            .unwrap();
        assert_eq!(
            session
                .prepare_file_revision(&lease.lease_id, compacted.generation, 2, &projected_hash,)
                .unwrap(),
            witness,
            "lost prepare response did not replay the same transaction",
        );
        assert_eq!(
            session
                .prepare_file_revision(&lease.lease_id, compacted.generation, 2, &hash(8))
                .unwrap_err(),
            DocumentError::LeaseHeld,
        );
        assert_eq!(
            store
                .file_write_fence("vault", "live.md", Some(&witness))
                .unwrap(),
            FileWriteFence::AuthorizedLeaseRevision
        );
        let confirmed = session.confirm_file_revision(&witness).unwrap();
        assert_eq!(confirmed.file_revision, 1);
        assert_eq!(confirmed.file.watermark, 2);
        let status = session.status().unwrap();
        assert!(matches!(
            session.prepare_file_revision(&lease.lease_id, status.generation, 1, &hash(8)),
            Err(DocumentError::Invalid(_))
        ));
        assert!(matches!(
            session.release_writer(&lease.lease_id),
            Err(DocumentError::Invalid(_))
        ));
        let witness = session
            .prepare_file_revision(&lease.lease_id, status.generation, 3, &hash(7))
            .unwrap();
        session.confirm_file_revision(&witness).unwrap();
        session.release_writer(&lease.lease_id).unwrap();
        assert!(matches!(
            store.file_write_fence("vault", "live.md", None).unwrap(),
            FileWriteFence::FileMode
        ));

        let restored = DocumentStore::new(StorageLayout::new(temp.path()))
            .open(&identity)
            .unwrap();
        assert_eq!(
            restored.read_snapshot().unwrap().unwrap().bytes,
            b"snapshot-through-two"
        );
        assert_eq!(
            restored.read_updates(2, 10, 1024).unwrap().updates[0].bytes,
            b"three"
        );
    }

    #[test]
    fn configured_caps_refuse_before_mutating_the_head() {
        let temp = tempdir().unwrap();
        let layout = StorageLayout::new(temp.path());
        layout.init_directories().unwrap();
        let store = DocumentStore::with_limits(
            layout,
            Limits {
                update_bytes: 4,
                snapshot_bytes: 8,
                live_ops: 1,
                live_bytes: 4,
                clients: 1,
            },
        );
        let session = store
            .open(&store.activate("v", "a.md", &hash(1)).unwrap().identity)
            .unwrap();
        let lease = session.acquire_writer(CLIENT_A, 0).unwrap();
        assert!(matches!(
            session.append(CLIENT_A, &lease.lease_id, 1, b"12345"),
            Err(DocumentError::Capacity(_))
        ));
        session
            .append(CLIENT_A, &lease.lease_id, 1, b"1234")
            .unwrap();
        assert!(matches!(
            session.append(CLIENT_A, &lease.lease_id, 2, b"x"),
            Err(DocumentError::Capacity(_))
        ));
        assert!(matches!(
            session.append(CLIENT_B, &lease.lease_id, 1, b"x"),
            Err(DocumentError::LeaseMismatch)
        ));
        assert_eq!(session.status().unwrap().watermark, 1);
    }

    #[test]
    fn corrupt_record_is_never_skipped_as_a_valid_suffix() {
        let (_temp, store) = fixture();
        let identity = store.activate("v", "a.md", &hash(1)).unwrap().identity;
        let session = store.open(&identity).unwrap();
        let lease = session.acquire_writer(CLIENT_A, 0).unwrap();
        session
            .append(CLIENT_A, &lease.lease_id, 1, b"one")
            .unwrap();
        let path = store.op_path(&identity, 1);
        let mut bytes = fs::read(&path).unwrap();
        bytes[44] ^= 0xff;
        fs::write(path, bytes).unwrap();
        assert!(matches!(
            session.read_updates(0, 1, 1024),
            Err(DocumentError::Corrupt(_))
        ));
    }
}
