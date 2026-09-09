use crate::perf::ServerPerfCounters;
use crate::root_head::{self, RootHead, RootReceipt, MAX_ROOT_HEAD_BYTES, MAX_ROOT_SEQUENCE};
use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use sync_core::hash::{hash_to_hex, FileHash};
use sync_core::versioned_root::VersionedRoot;

#[cfg(test)]
use sync_core::chunk::RootNode;

/// Manages the filesystem layout for the server's data directory.
#[derive(Debug, Clone)]
pub struct StorageLayout {
    pub base: PathBuf,
}

impl StorageLayout {
    pub fn new(base: impl Into<PathBuf>) -> Self {
        Self { base: base.into() }
    }

    /// Create the full directory tree on first init.
    pub fn init_directories(&self) -> Result<(), std::io::Error> {
        let dirs = [
            "ca",
            "server",
            "devices",
            "devices/tokens",
            "enrollments",
            "vaults",
            "index",
            "content",
            "content/manifests",
            "content/chunks",
            "storage-writer",
            "objects-v1/segments",
        ];
        for dir in &dirs {
            std::fs::create_dir_all(self.base.join(dir))?;
        }
        Ok(())
    }

    // --- Index chunks ---

    pub fn index_path(&self, hash: &FileHash) -> PathBuf {
        let hex = hash_to_hex(hash);
        self.base.join("index").join(&hex[..2]).join(&hex[2..])
    }

    // --- Content (small files, whole blobs) ---

    pub fn content_blob_path(&self, hash: &FileHash) -> PathBuf {
        let hex = hash_to_hex(hash);
        self.base.join("content").join(&hex[..2]).join(&hex[2..])
    }

    pub fn content_manifest_path(&self, hash: &FileHash) -> PathBuf {
        let hex = hash_to_hex(hash);
        self.base
            .join("content/manifests")
            .join(&hex[..2])
            .join(&hex[2..])
    }

    pub fn content_chunk_path(&self, hash: &FileHash) -> PathBuf {
        let hex = hash_to_hex(hash);
        self.base
            .join("content/chunks")
            .join(&hex[..2])
            .join(&hex[2..])
    }

    /// Transitional group-commit journal used by the loose-object writer.
    /// The journal is the durability source of truth; loose files remain the
    /// current read/index backend until pack storage replaces them.
    pub fn storage_writer_journal_path(&self) -> PathBuf {
        self.base.join("storage-writer/loose-groups-v1.log")
    }

    /// Immutable-object pack storage. Segment names are fixed-width lowercase
    /// hex so directory order is generation order on every supported host.
    pub fn object_segments_dir(&self) -> PathBuf {
        self.base.join("objects-v1/segments")
    }

    pub fn object_segment_path(&self, segment_id: u64) -> PathBuf {
        self.object_segments_dir()
            .join(format!("{segment_id:016x}.pack"))
    }

    pub fn object_segment_index_path(&self, segment_id: u64) -> PathBuf {
        self.object_segments_dir()
            .join(format!("{segment_id:016x}.idx"))
    }

    /// Durable HTTP-v2 anti-replay window for one enrolled device.
    pub fn device_sequence_path(&self, device_id: &str) -> PathBuf {
        self.base
            .join("devices")
            .join(storage_component(device_id))
            .join("seq-window")
    }

    // --- Vaults ---

    pub fn vault_dir(&self, vault_id: &str) -> PathBuf {
        self.base.join("vaults").join(storage_component(vault_id))
    }

    pub fn vault_current_path(&self, vault_id: &str) -> PathBuf {
        self.vault_dir(vault_id).join("current")
    }

    pub fn vault_roots_dir(&self, vault_id: &str) -> PathBuf {
        self.vault_dir(vault_id).join("roots")
    }

    pub fn vault_root_path(&self, vault_id: &str, hash: &FileHash) -> PathBuf {
        let hex = hash_to_hex(hash);
        self.vault_roots_dir(vault_id).join(format!("{}.bin", hex))
    }

    pub fn ensure_vault(&self, vault_id: &str) -> Result<(), std::io::Error> {
        ensure_durable_directories(&self.vault_roots_dir(vault_id)).map(|_| ())
    }

    // --- CRDT logs (Ph4: live co-editing) ---

    /// Append-only log of Yjs update blobs for one hot note. `note_hash_hex`
    /// is a hash of the note path (computed by the caller) so arbitrary note
    /// paths can't traverse the filesystem.
    pub fn crdt_log_path(&self, vault_id: &str, note_hash_hex: &str) -> PathBuf {
        self.base
            .join("crdt")
            .join(storage_component(vault_id))
            .join(format!("{}.log", storage_component(note_hash_hex)))
    }

    // --- Devices ---

    pub fn device_dir(&self, fingerprint: &str) -> PathBuf {
        self.base
            .join("devices")
            .join(storage_component(fingerprint))
    }

    /// Path for the bearer-token → fingerprint index entry.
    pub fn token_path(&self, token: &str) -> PathBuf {
        self.base
            .join("devices")
            .join("tokens")
            .join(storage_component(token))
    }

    // --- Enrollments ---

    pub fn enrollment_path(&self, code: &str) -> PathBuf {
        self.base
            .join("enrollments")
            .join(format!("{}.json", storage_component(code)))
    }
}

/// Keep normal human/hex identifiers readable, while mapping every unsafe
/// path component (slashes, `..`, control bytes, excessive length) to a stable
/// content-derived name inside the intended namespace. This is defense in
/// depth for every route/admin parameter that eventually reaches storage.
fn storage_component(value: &str) -> String {
    let safe = !value.is_empty()
        && value.len() <= 128
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if safe {
        value.to_owned()
    } else {
        // `~` is deliberately outside the accepted safe alphabet. Therefore
        // no literal identifier can equal the encoded name of another value;
        // the mapping is collision-resistant up to the content hash itself.
        format!(
            "~{}",
            hash_to_hex(&sync_core::hash::hash_bytes(value.as_bytes()))
        )
    }
}

/// Read/write vault root state.
pub struct VaultStore {
    layout: StorageLayout,
    perf: Option<Arc<ServerPerfCounters>>,
}

impl VaultStore {
    #[cfg(test)]
    pub fn new(layout: StorageLayout) -> Self {
        Self { layout, perf: None }
    }

    pub fn with_perf(layout: StorageLayout, perf: Arc<ServerPerfCounters>) -> Self {
        Self {
            layout,
            perf: Some(perf),
        }
    }

    /// Compatibility-only lossy reader. Mutation and authoritative API callers
    /// must use try_get_current_root: corrupt/inaccessible is never empty.
    #[cfg(test)]
    pub fn get_current_root(&self, vault_id: &str) -> Option<FileHash> {
        self.try_get_current_root(vault_id).ok().flatten()
    }

    /// Only actual absence, without an orphan temporary head, is an empty vault.
    pub fn try_get_current_root(&self, vault_id: &str) -> io::Result<Option<FileHash>> {
        Ok(self
            .read_current_head(vault_id)?
            .and_then(|loaded| loaded.head.root_hash))
    }

    /// Return a validated historical outcome only after a durability barrier.
    /// A prior rename may have succeeded despite a directory-sync/response error.
    /// The per-vault mutation lock must cover receipt lookup and fresh publish.
    pub fn get_root_receipt(
        &self,
        vault_id: &str,
        device_id: &str,
    ) -> io::Result<Option<RootReceipt>> {
        root_head::validate_device_id(device_id)?;
        let Some(mut loaded) = self.read_current_head(vault_id)? else {
            return Ok(None);
        };
        let Some(receipt) = loaded.head.receipts.remove(device_id) else {
            return Ok(None);
        };
        root_head_checkpoint("before_replay_file_sync")?;
        loaded.file.sync_all()?;
        root_head_checkpoint("after_replay_file_sync")?;
        root_head_checkpoint("before_replay_dir_sync")?;
        sync_directory_ancestors(&self.layout.vault_dir(vault_id))?;
        root_head_checkpoint("after_replay_dir_sync")?;
        Ok(Some(receipt))
    }

    /// Atomic legacy/admin root replacement, preserving every device receipt.
    /// Once migrated, this method can never flatten the richer authority file.
    pub fn set_current_root(&self, vault_id: &str, hash: &FileHash) -> Result<(), std::io::Error> {
        let receipts = self
            .read_current_head(vault_id)?
            .map(|loaded| loaded.head.receipts)
            .unwrap_or_default();
        self.publish_current_head(
            vault_id,
            &RootHead {
                root_hash: Some(*hash),
                receipts,
            },
        )
    }

    /// Root and outcome have exactly one publication boundary. The caller owns
    /// the per-vault mutex and has already durably written referenced objects and
    /// root history. This defensive sequence check never re-executes a duplicate.
    pub fn set_current_root_with_receipt(
        &self,
        vault_id: &str,
        hash: &FileHash,
        device_id: &str,
        receipt: RootReceipt,
    ) -> io::Result<()> {
        let head = self.prepare_root_receipt(vault_id, hash, device_id, &receipt)?;
        self.publish_current_head(vault_id, &head)
    }

    /// Consume a stream operation without changing the selected root. Even an
    /// initially empty vault retains this checksummed terminal receipt so a
    /// later exact commit cannot revive the cancelled operation.
    pub fn cancel_root_with_receipt(
        &self,
        vault_id: &str,
        device_id: &str,
        receipt: RootReceipt,
    ) -> io::Result<()> {
        if !receipt.is_cancelled() {
            return Err(root_head_input_error(root_head_error(
                "cancellation requires a cancelled receipt",
            )));
        }
        let head = self.prepare_receipt_head(vault_id, device_id, &receipt)?;
        self.publish_current_head(vault_id, &head)
    }

    /// Read-only deterministic admission before consuming one-use approval.
    /// Call under the same vault lock as guard evaluation and final publication;
    /// final publication repeats these checks and never trusts a prior preview.
    pub fn validate_root_receipt(
        &self,
        vault_id: &str,
        hash: &FileHash,
        device_id: &str,
        receipt: &RootReceipt,
    ) -> io::Result<()> {
        let head = self.prepare_root_receipt(vault_id, hash, device_id, receipt)?;
        root_head::encode(vault_id, &head).map_err(root_head_input_error)?;
        Ok(())
    }

    fn prepare_root_receipt(
        &self,
        vault_id: &str,
        hash: &FileHash,
        device_id: &str,
        receipt: &RootReceipt,
    ) -> io::Result<RootHead> {
        if receipt
            .result
            .get("root_hash")
            .and_then(|value| value.as_str())
            != Some(hash_to_hex(hash).as_str())
        {
            return Err(root_head_input_error(root_head_error(
                "published root and receipt result disagree",
            )));
        }
        let mut head = self.prepare_receipt_head(vault_id, device_id, receipt)?;
        head.root_hash = Some(*hash);
        Ok(head)
    }

    fn prepare_receipt_head(
        &self,
        vault_id: &str,
        device_id: &str,
        receipt: &RootReceipt,
    ) -> io::Result<RootHead> {
        root_head::validate_device_id(device_id).map_err(root_head_input_error)?;
        receipt.validate().map_err(root_head_input_error)?;
        let mut head = self
            .read_current_head(vault_id)?
            .map(|loaded| loaded.head)
            .unwrap_or_else(|| RootHead {
                root_hash: None,
                receipts: BTreeMap::new(),
            });
        let previous = head.receipts.get(device_id).map_or(0, |last| last.sequence);
        if previous == MAX_ROOT_SEQUENCE || receipt.sequence != previous + 1 {
            return Err(root_head_input_error(root_head_error(
                "root receipt sequence is not the next operation",
            )));
        }
        head.receipts.insert(device_id.to_owned(), receipt.clone());
        Ok(head)
    }

    fn read_current_head(&self, vault_id: &str) -> io::Result<Option<LoadedRootHead>> {
        let started = Instant::now();
        let loaded = read_root_head(&self.layout.vault_current_path(vault_id), vault_id)?;
        if let (Some(perf), Some(loaded)) = (&self.perf, &loaded) {
            perf.record_loose_read(loaded.bytes as u64, started.elapsed());
        }
        Ok(loaded)
    }

    fn publish_current_head(&self, vault_id: &str, head: &RootHead) -> io::Result<()> {
        // All receipt/device/head ceilings and serialization finish before any
        // directory, temporary file or authoritative namespace is changed.
        let bytes = root_head::encode(vault_id, head).map_err(root_head_input_error)?;
        let directory_started = Instant::now();
        let created_dir_syncs = ensure_durable_directories(&self.layout.vault_roots_dir(vault_id))?;
        let directory_elapsed = directory_started.elapsed();
        let path = self.layout.vault_current_path(vault_id);
        let tmp = path.with_extension("tmp");
        // A valid selected current remains authoritative after a pre-rename
        // crash. Under the external vault lock, only this known temporary name
        // may be overwritten; orphan temporaries are never promoted on read.
        match std::fs::symlink_metadata(&tmp) {
            Ok(metadata) if !metadata.is_file() => {
                return Err(root_head_error("root temporary is not a regular file"))
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let write_started = Instant::now();
        root_head_checkpoint("before_temp_open")?;
        let mut file = std::fs::File::create(&tmp)?;
        root_head_checkpoint("after_temp_open")?;
        file.write_all(&bytes)?;
        root_head_checkpoint("after_temp_write")?;
        let write_elapsed = write_started.elapsed();
        let durability_started = Instant::now();
        file.sync_all()?;
        root_head_checkpoint("after_temp_sync")?;
        drop(file);
        root_head_checkpoint("before_rename")?;
        std::fs::rename(&tmp, &path)?;
        root_head_checkpoint("after_rename")?;
        root_head_checkpoint("before_head_dir_sync")?;
        let synced_dirs =
            sync_directory_ancestors(path.parent().expect("current root has parent"))?;
        root_head_checkpoint("after_head_dir_sync")?;
        if let Some(perf) = &self.perf {
            perf.record_loose_write(
                bytes.len() as u64,
                write_elapsed,
                directory_elapsed + durability_started.elapsed(),
                created_dir_syncs + 1 + synced_dirs,
            );
        }
        Ok(())
    }

    /// Store a root node's bytes in the vault's root history.
    pub fn store_root(
        &self,
        vault_id: &str,
        hash: &FileHash,
        data: &[u8],
    ) -> Result<(), std::io::Error> {
        if data.len() > MAX_ROOT_HEAD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "root history object exceeds bounded root response capacity",
            ));
        }
        self.layout.ensure_vault(vault_id)?;
        let path = self.layout.vault_root_path(vault_id, hash);

        // Root identity deliberately commits only to the ordered child-hash
        // set, not created_ms/device/parent metadata. History is therefore a
        // set of unique states: once valid metadata has been recorded for a
        // semantic root, a replay of the same state must not rewrite it.
        let read_started = Instant::now();
        match std::fs::File::open(&path) {
            Ok(mut file) => {
                let metadata = file.metadata()?;
                if !metadata.is_file() || metadata.len() > MAX_ROOT_HEAD_BYTES as u64 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "existing root history object exceeds bounded capacity",
                    ));
                }
                let mut existing = Vec::with_capacity(metadata.len() as usize);
                (&mut file)
                    .take(MAX_ROOT_HEAD_BYTES as u64 + 1)
                    .read_to_end(&mut existing)?;
                if existing.len() > MAX_ROOT_HEAD_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "existing root history object grew past bounded capacity",
                    ));
                }
                if let Some(perf) = &self.perf {
                    perf.record_loose_read(existing.len() as u64, read_started.elapsed());
                }
                if VersionedRoot::deserialize(&existing)
                    .is_ok_and(|root| root.vault_id() == vault_id && root.hash() == *hash)
                {
                    // A previous history rename can be visible after its
                    // directory sync failed. Sync the SAME descriptor whose
                    // bytes were validated, then its namespace before reuse.
                    root_head_checkpoint("before_history_reuse_file_sync")?;
                    file.sync_all()?;
                    root_head_checkpoint("after_history_reuse_file_sync")?;
                    root_head_checkpoint("before_history_reuse_dir_sync")?;
                    sync_directory_ancestors(&self.layout.vault_roots_dir(vault_id))?;
                    root_head_checkpoint("after_history_reuse_dir_sync")?;
                    return Ok(());
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        write_blob_with_optional_perf(&path, data, self.perf.as_deref())
    }

    /// Load a root node's bytes from history.
    pub fn get_root(&self, vault_id: &str, hash: &FileHash) -> Option<Vec<u8>> {
        let path = self.layout.vault_root_path(vault_id, hash);
        read_blob_bounded_with_optional_perf(&path, MAX_ROOT_HEAD_BYTES, self.perf.as_deref())
    }

    #[cfg(test)]
    pub fn get_current_version(&self, vault_id: &str) -> Option<u32> {
        let hash = self.get_current_root(vault_id)?;
        let bytes = self.get_root(vault_id, &hash)?;
        VersionedRoot::deserialize(&bytes)
            .ok()
            .map(|root| root.version())
    }

    /// Check if a vault exists (has at least one root).
    #[allow(dead_code)]
    pub fn vault_exists(&self, vault_id: &str) -> bool {
        self.layout.vault_dir(vault_id).exists()
    }
}

struct LoadedRootHead {
    head: RootHead,
    file: std::fs::File,
    bytes: usize,
}

fn root_head_error(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[derive(Debug)]
struct RootPublicationRefused(io::Error);

impl std::fmt::Display for RootPublicationRefused {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "root publication refused: {}", self.0)
    }
}

impl std::error::Error for RootPublicationRefused {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

/// Only deterministic input/capacity refusal is safe to label rejected. A
/// native EINVAL can occur after rename and must remain an ambiguous IO error.
pub fn is_root_publication_refusal(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|inner| inner.is::<RootPublicationRefused>())
}

fn root_head_input_error(error: io::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, RootPublicationRefused(error))
}

fn read_root_head(path: &Path, vault_id: &str) -> io::Result<Option<LoadedRootHead>> {
    root_head_checkpoint("before_head_read")?;
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.is_file() => {
            return Err(root_head_error("root head is not a regular file"))
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            // A first-publish crash may leave a complete or partial temp. It is
            // not accepted authority and cannot be mistaken for an empty vault.
            return match std::fs::symlink_metadata(path.with_extension("tmp")) {
                Ok(_) => Err(root_head_error(
                    "missing root head has an orphan temporary; recovery required",
                )),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(error),
            };
        }
        Err(error) => return Err(error),
    }
    // If open now fails, including NotFound after a successful stat, the read
    // was not a proven absence. Do not turn a namespace race into an empty root.
    let mut file = std::fs::File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > MAX_ROOT_HEAD_BYTES as u64 {
        return Err(root_head_error("invalid or oversized root head file"));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    (&mut file)
        .take(MAX_ROOT_HEAD_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    let head = root_head::decode(vault_id, &bytes)?;
    root_head_checkpoint("after_head_read")?;
    Ok(Some(LoadedRootHead {
        head,
        file,
        bytes: bytes.len(),
    }))
}

/// mkdir is not durable merely because the child file is fsynced. Sync each new
/// directory and its parent; the final root publication also re-syncs ancestors
/// to cover a retry following an ambiguous earlier mkdir/directory-sync error.
fn ensure_durable_directories(path: &Path) -> io::Result<u64> {
    let path = if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    };
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => return Ok(0),
        Ok(_) => return Err(root_head_error("vault directory is not a directory")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let parent = path
        .parent()
        .ok_or_else(|| root_head_error("vault directory has no parent"))?;
    let ancestor_syncs = ensure_durable_directories(parent)?;
    root_head_checkpoint("before_mkdir")?;
    match std::fs::create_dir(path) {
        Ok(()) => {}
        Err(error)
            if error.kind() == io::ErrorKind::AlreadyExists
                && std::fs::metadata(path)?.is_dir() => {}
        Err(error) => return Err(error),
    }
    root_head_checkpoint("after_mkdir")?;
    std::fs::File::open(path)?.sync_all()?;
    root_head_checkpoint("after_mkdir_sync")?;
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    std::fs::File::open(parent)?.sync_all()?;
    root_head_checkpoint("after_mkdir_parent_sync")?;
    Ok(ancestor_syncs + 2)
}

fn sync_directory_ancestors(path: &Path) -> io::Result<u64> {
    let mut count = 0;
    for ancestor in path.ancestors() {
        let ancestor = if ancestor.as_os_str().is_empty() {
            Path::new(".")
        } else {
            ancestor
        };
        std::fs::File::open(ancestor)?.sync_all()?;
        count += 1;
    }
    Ok(count)
}

#[cfg(not(test))]
fn root_head_checkpoint(_boundary: &'static str) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
thread_local! {
    static ROOT_HEAD_FAULT: std::cell::RefCell<Option<&'static str>> = const { std::cell::RefCell::new(None) };
    static ROOT_HEAD_PROCESS_EXIT: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn root_head_checkpoint(boundary: &'static str) -> io::Result<()> {
    ROOT_HEAD_PROCESS_EXIT.with(|selected| {
        if selected.get() == Some(boundary) {
            // Test child only: exit without Rust stack unwinding/destructors.
            // This preserves the kernel cache and is not a power-loss model.
            std::process::exit(73);
        }
    });
    ROOT_HEAD_FAULT.with(|fault| {
        let mut fault = fault.borrow_mut();
        if *fault == Some(boundary) {
            *fault = None;
            Err(io::Error::other("injected root head IO failure"))
        } else {
            Ok(())
        }
    })
}

/// Helper: read a content-addressed blob from a path.
#[cfg(test)]
pub fn read_blob(path: &Path) -> Option<Vec<u8>> {
    read_blob_with_optional_perf(path, None)
}

pub fn read_blob_measured(path: &Path, perf: &ServerPerfCounters) -> Option<Vec<u8>> {
    read_blob_with_optional_perf(path, Some(perf))
}

fn read_blob_bounded_with_optional_perf(
    path: &Path,
    max_bytes: usize,
    perf: Option<&ServerPerfCounters>,
) -> Option<Vec<u8>> {
    let started = Instant::now();
    let mut file = std::fs::File::open(path).ok()?;
    let metadata = file.metadata().ok()?;
    if !metadata.is_file() || metadata.len() > max_bytes as u64 {
        return None;
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    (&mut file)
        .take(max_bytes.saturating_add(1) as u64)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() > max_bytes {
        return None;
    }
    if let Some(perf) = perf {
        perf.record_loose_read(bytes.len() as u64, started.elapsed());
    }
    Some(bytes)
}

fn read_blob_with_optional_perf(path: &Path, perf: Option<&ServerPerfCounters>) -> Option<Vec<u8>> {
    let started = Instant::now();
    let bytes = std::fs::read(path).ok()?;
    if let Some(perf) = perf {
        perf.record_loose_read(bytes.len() as u64, started.elapsed());
    }
    Some(bytes)
}

/// Write an immutable/content-addressed object without ever exposing a partial
/// final file. The temp file lives in the same directory so rename is an
/// atomic namespace operation; file and directory are synced before success.
#[cfg(test)]
pub fn write_blob(path: &Path, data: &[u8]) -> Result<(), std::io::Error> {
    write_blob_with_optional_perf(path, data, None)
}

#[cfg(test)]
pub fn write_blob_measured(
    path: &Path,
    data: &[u8],
    perf: &ServerPerfCounters,
) -> Result<(), std::io::Error> {
    write_blob_with_optional_perf(path, data, Some(perf))
}

fn write_blob_with_optional_perf(
    path: &Path,
    data: &[u8],
    perf: Option<&ServerPerfCounters>,
) -> Result<(), std::io::Error> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "blob path has no parent")
    })?;
    std::fs::create_dir_all(parent)?;

    // Concurrent writers of the same address are common and harmless. Avoid
    // replacing a complete identical object (especially on Windows, where
    // rename-over-existing is not portable).
    let existing_started = Instant::now();
    if let Ok(existing) = std::fs::read(path) {
        if let Some(perf) = perf {
            perf.record_loose_read(existing.len() as u64, existing_started.elapsed());
        }
        if existing == data {
            return Ok(());
        }
    }

    let tmp = path.with_extension(format!(
        "tmp-{}-{:016x}",
        std::process::id(),
        rand::random::<u64>()
    ));
    let write_started = Instant::now();
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp)?;
    if let Err(error) = file.write_all(data) {
        let _ = std::fs::remove_file(&tmp);
        return Err(error);
    }
    let write_elapsed = write_started.elapsed();
    let durability_started = Instant::now();
    if let Err(error) = file.sync_all() {
        let _ = std::fs::remove_file(&tmp);
        return Err(error);
    }
    let mut durability_elapsed = durability_started.elapsed();
    drop(file);

    if let Err(rename_error) = std::fs::rename(&tmp, path) {
        // Another writer may have won while our temp was being synced.
        if std::fs::read(path).is_ok_and(|existing| existing == data) {
            let _ = std::fs::remove_file(&tmp);
            if let Some(perf) = perf {
                perf.record_loose_write(data.len() as u64, write_elapsed, durability_elapsed, 1);
            }
            return Ok(());
        }

        // Windows cannot replace an existing destination with rename(). Use a
        // recoverable old→backup, temp→final sequence for the rare corrupt or
        // different existing object.
        let backup = path.with_extension(format!(
            "backup-{}-{:016x}",
            std::process::id(),
            rand::random::<u64>()
        ));
        let had_existing = path.exists();
        if had_existing {
            std::fs::rename(path, &backup)?;
        }
        if let Err(error) = std::fs::rename(&tmp, path) {
            if had_existing {
                let _ = std::fs::rename(&backup, path);
            }
            let _ = std::fs::remove_file(&tmp);
            return Err(std::io::Error::new(
                error.kind(),
                format!("blob promotion failed after rename error {rename_error}: {error}"),
            ));
        }
        if had_existing {
            let _ = std::fs::remove_file(&backup);
        }
    }
    root_head_checkpoint("after_blob_rename")?;
    let parent_sync_started = Instant::now();
    std::fs::File::open(parent)?.sync_all()?;
    durability_elapsed += parent_sync_started.elapsed();
    if let Some(perf) = perf {
        perf.record_loose_write(data.len() as u64, write_elapsed, durability_elapsed, 2);
    }
    Ok(())
}

/// Materialize a loose-object mirror after its bytes are already durable in
/// the storage-writer journal. Atomic promotion still prevents readers from
/// observing partial data, but an additional per-file fsync would defeat
/// group commit and is unnecessary: startup recovery can recreate this mirror
/// from the fdatasync'ed journal after any crash or power loss.
#[cfg(test)]
pub(crate) fn materialize_journaled_blob(path: &Path, data: &[u8]) -> Result<(), std::io::Error> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "blob path has no parent")
    })?;
    std::fs::create_dir_all(parent)?;
    if std::fs::read(path).is_ok_and(|existing| existing == data) {
        return Ok(());
    }

    let tmp = path.with_extension(format!(
        "journal-tmp-{}-{:016x}",
        std::process::id(),
        rand::random::<u64>()
    ));
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp)?;
    if let Err(error) = file.write_all(data) {
        let _ = std::fs::remove_file(&tmp);
        return Err(error);
    }
    drop(file);

    if let Err(rename_error) = std::fs::rename(&tmp, path) {
        if std::fs::read(path).is_ok_and(|existing| existing == data) {
            let _ = std::fs::remove_file(&tmp);
            return Ok(());
        }
        let backup = path.with_extension(format!(
            "journal-backup-{}-{:016x}",
            std::process::id(),
            rand::random::<u64>()
        ));
        let had_existing = path.exists();
        if had_existing {
            std::fs::rename(path, &backup)?;
        }
        if let Err(error) = std::fs::rename(&tmp, path) {
            if had_existing {
                let _ = std::fs::rename(&backup, path);
            }
            let _ = std::fs::remove_file(&tmp);
            return Err(std::io::Error::new(
                error.kind(),
                format!(
                    "journaled blob promotion failed after rename error {rename_error}: {error}"
                ),
            ));
        }
        if had_existing {
            let _ = std::fs::remove_file(&backup);
        }
    }
    Ok(())
}

/// Helper: check if a content-addressed blob exists.
#[cfg(test)]
pub fn blob_exists(path: &Path) -> bool {
    path.exists()
}

/// Verify that an object still matches the content address encoded by its
/// path. Check endpoints use this for the small set of objects a changed
/// batch wants to reuse, allowing pre-atomic-write partials or disk damage to
/// be repaired by a normal re-upload instead of becoming permanently stuck.
#[cfg(test)]
pub fn blob_matches_hash(path: &Path, expected: &FileHash) -> bool {
    std::fs::read(path).is_ok_and(|bytes| sync_core::hash::hash_bytes(&bytes) == *expected)
}

#[cfg(test)]
pub fn blob_matches_hash_measured(
    path: &Path,
    expected: &FileHash,
    perf: &ServerPerfCounters,
) -> bool {
    let Some(bytes) = read_blob_measured(path, perf) else {
        perf.record_index_lookup(false);
        return false;
    };
    perf.record_index_lookup(true);
    let started = Instant::now();
    let matched = sync_core::hash::hash_bytes(&bytes) == *expected;
    perf.record_hash_check(bytes.len() as u64, started.elapsed(), matched);
    matched
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::perf::ServerPerfCounters;
    use sync_core::hash::hash_bytes;
    use tempfile::tempdir;

    fn outcome_receipt(sequence: u64, root: &FileHash) -> RootReceipt {
        RootReceipt {
            sequence,
            mutation_id: format!("{sequence:032x}"),
            request_hash: "b".repeat(64),
            result: serde_json::json!({"accepted":true,"root_hash":hash_to_hex(root)}),
        }
    }

    fn cancellation_receipt(sequence: u64) -> RootReceipt {
        RootReceipt {
            sequence,
            mutation_id: format!("{sequence:032x}"),
            request_hash: "b".repeat(64),
            result: serde_json::json!({"cancelled":true}),
        }
    }

    #[test]
    fn root_history_storage_rejects_and_does_not_read_past_response_bound() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        let store = VaultStore::new(layout.clone());
        let hash = hash_bytes(b"oversized-root");
        let oversized = vec![0; MAX_ROOT_HEAD_BYTES + 1];
        let error = store.store_root("v", &hash, &oversized).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);

        layout.ensure_vault("v").unwrap();
        let path = layout.vault_root_path("v", &hash);
        let file = std::fs::File::create(path).unwrap();
        file.set_len((MAX_ROOT_HEAD_BYTES + 1) as u64).unwrap();
        assert!(store.get_root("v", &hash).is_none());
    }

    #[test]
    fn root_cancellation_preserves_empty_or_existing_root_and_other_streams_across_restart() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        let store = VaultStore::new(layout.clone());
        store
            .cancel_root_with_receipt("v", "a", cancellation_receipt(1))
            .unwrap();
        assert!(layout.vault_current_path("v").is_file());
        assert_eq!(store.try_get_current_root("v").unwrap(), None);
        let restarted = VaultStore::new(layout.clone());
        assert_eq!(
            restarted.get_root_receipt("v", "a").unwrap(),
            Some(cancellation_receipt(1))
        );
        let root = hash_bytes(b"later-root");
        restarted.set_current_root("v", &root).unwrap();
        assert_eq!(
            restarted.get_root_receipt("v", "a").unwrap(),
            Some(cancellation_receipt(1))
        );
        restarted
            .set_current_root_with_receipt("v", &root, "b", outcome_receipt(1, &root))
            .unwrap();
        restarted
            .cancel_root_with_receipt("v", "a", cancellation_receipt(2))
            .unwrap();
        assert_eq!(restarted.try_get_current_root("v").unwrap(), Some(root));
        assert_eq!(
            restarted.get_root_receipt("v", "b").unwrap(),
            Some(outcome_receipt(1, &root))
        );
        assert!(restarted
            .set_current_root_with_receipt("v", &root, "a", outcome_receipt(2, &root))
            .is_err());
        let later = hash_bytes(b"next-root");
        restarted
            .set_current_root_with_receipt("v", &later, "a", outcome_receipt(3, &later))
            .unwrap();
        assert_eq!(restarted.try_get_current_root("v").unwrap(), Some(later));
        assert!(restarted
            .cancel_root_with_receipt("v", "a", cancellation_receipt(3))
            .is_err());
        assert_eq!(
            restarted.get_root_receipt("v", "a").unwrap(),
            Some(outcome_receipt(3, &later))
        );
    }

    #[test]
    fn root_cancellation_capacity_and_malformed_receipts_never_modify_authority() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        let store = VaultStore::new(layout.clone());
        let root = hash_bytes(b"root");
        let error = store
            .cancel_root_with_receipt("v", "a", outcome_receipt(1, &root))
            .unwrap_err();
        assert!(is_root_publication_refusal(&error));
        assert!(!layout.vault_dir("v").exists());
        layout.ensure_vault("v").unwrap();
        let head = RootHead {
            root_hash: None,
            receipts: (0..root_head::MAX_ROOT_RECEIPT_DEVICES)
                .map(|i| (format!("d{i}"), cancellation_receipt(1)))
                .collect(),
        };
        let bytes = root_head::encode("v", &head).unwrap();
        std::fs::write(layout.vault_current_path("v"), &bytes).unwrap();
        assert!(is_root_publication_refusal(
            &store
                .cancel_root_with_receipt("v", "new", cancellation_receipt(1))
                .unwrap_err()
        ));
        assert_eq!(
            std::fs::read(layout.vault_current_path("v")).unwrap(),
            bytes
        );
        assert!(!layout
            .vault_current_path("v")
            .with_extension("tmp")
            .exists());
    }

    fn arm_root_fault(boundary: &'static str) {
        ROOT_HEAD_FAULT.with(|fault| {
            assert!(fault.borrow().is_none(), "unconsumed prior fault");
            *fault.borrow_mut() = Some(boundary);
        });
    }

    fn assert_root_fault_consumed() {
        ROOT_HEAD_FAULT
            .with(|fault| assert!(fault.borrow().is_none(), "fault boundary not reached"));
    }

    const PROCESS_MODE_ENV: &str = "OBSETYNC_TEST_ROOT_HEAD_PROCESS_MODE";
    const PROCESS_FIXTURE_ENV: &str = "OBSETYNC_TEST_ROOT_HEAD_PROCESS_FIXTURE";
    const PROCESS_BOUNDARY_ENV: &str = "OBSETYNC_TEST_ROOT_HEAD_PROCESS_BOUNDARY";
    const PROCESS_FIXTURE_MARKER: &[u8] = b"obsetync-root-head-process-fixture-v1";
    const PROCESS_CUTS: [&str; 6] = [
        "after_temp_open", // The existing checkpoint immediately before write_all.
        "after_temp_write",
        "after_temp_sync",
        "before_rename",
        "after_rename",
        "after_head_dir_sync",
    ];

    /// Invoked only by the parent tests below using an exact test name. During
    /// an ordinary suite this helper returns without claiming a child run.
    #[test]
    fn root_head_process_child() {
        let Some(mode) = std::env::var_os(PROCESS_MODE_ENV) else {
            return;
        };
        let mode = mode.to_str().expect("test process mode must be UTF-8");
        assert!(matches!(
            mode,
            "replace" | "first" | "cancel-replace" | "cancel-first"
        ));
        let fixture = PathBuf::from(
            std::env::var_os(PROCESS_FIXTURE_ENV).expect("parent-owned fixture path missing"),
        );
        assert!(fixture.is_absolute() && fixture.is_dir());
        assert!(fixture
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("obsetync-root-head-process-"));
        assert_eq!(
            std::fs::read(fixture.join("process-fixture")).unwrap(),
            PROCESS_FIXTURE_MARKER
        );
        let selected = std::env::var(PROCESS_BOUNDARY_ENV).expect("test process boundary missing");
        let boundary = PROCESS_CUTS
            .into_iter()
            .find(|cut| *cut == selected)
            .expect("unknown test process boundary");
        let store = VaultStore::new(StorageLayout::new(&fixture));
        let sequence = if matches!(mode, "replace" | "cancel-replace") {
            assert_eq!(
                store.try_get_current_root("v").unwrap(),
                Some(hash_bytes(b"process-old"))
            );
            assert_eq!(
                store.get_root_receipt("v", "a").unwrap(),
                Some(outcome_receipt(1, &hash_bytes(b"process-old")))
            );
            2
        } else {
            assert_eq!(store.try_get_current_root("v").unwrap(), None);
            1
        };
        ROOT_HEAD_PROCESS_EXIT.with(|selected| selected.set(Some(boundary)));
        let new = hash_bytes(b"process-new");
        if mode.starts_with("cancel-") {
            store
                .cancel_root_with_receipt("v", "a", cancellation_receipt(sequence))
                .expect("child must exit at selected cancellation publication boundary");
        } else {
            store
                .set_current_root_with_receipt("v", &new, "a", outcome_receipt(sequence, &new))
                .expect("child must exit at the selected native publication boundary");
        }
        panic!("selected process-exit boundary was not reached");
    }

    fn run_root_head_process_child(fixture: &Path, boundary: &str, mode: &str) {
        // Command-local environment avoids mutating global process state while
        // Rust's other tests run concurrently. The child never owns TempDir and
        // never removes the fixture; cleanup remains with the parent alone.
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("storage::tests::root_head_process_child")
            .arg("--nocapture")
            .env(PROCESS_MODE_ENV, mode)
            .env(PROCESS_FIXTURE_ENV, fixture)
            .env(PROCESS_BOUNDARY_ENV, boundary)
            .output()
            .expect("failed to spawn native root-head test child");
        assert_eq!(
            output.status.code(),
            Some(73),
            "child did not reach process::exit at {mode}/{boundary}: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn root_head_native_process_replacement_reopens_exact_whole_cuts() {
        let mut actual_child_runs = 0;
        for boundary in PROCESS_CUTS {
            let fixture = tempfile::Builder::new()
                .prefix("obsetync-root-head-process-")
                .tempdir()
                .unwrap();
            std::fs::write(
                fixture.path().join("process-fixture"),
                PROCESS_FIXTURE_MARKER,
            )
            .unwrap();
            let layout = StorageLayout::new(fixture.path());
            let old = hash_bytes(b"process-old");
            let new = hash_bytes(b"process-new");
            {
                let seed = VaultStore::new(layout.clone());
                seed.set_current_root_with_receipt("v", &old, "a", outcome_receipt(1, &old))
                    .unwrap();
            }
            run_root_head_process_child(fixture.path(), boundary, "replace");
            actual_child_runs += 1;

            // Cold storage object, no child heap/cache/destructor continuation.
            // The OS page cache is deliberately still alive: no power-loss claim.
            let reopened = VaultStore::new(layout.clone());
            let promoted = matches!(boundary, "after_rename" | "after_head_dir_sync");
            assert_eq!(
                (
                    reopened.try_get_current_root("v").unwrap().unwrap(),
                    reopened.get_root_receipt("v", "a").unwrap().unwrap()
                ),
                if promoted {
                    (new, outcome_receipt(2, &new))
                } else {
                    (old, outcome_receipt(1, &old))
                },
                "mixed selected root/receipt after native process exit at {boundary}"
            );
            let tmp = layout.vault_current_path("v").with_extension("tmp");
            if promoted {
                assert!(!tmp.exists());
            } else {
                let bytes = std::fs::read(&tmp).unwrap();
                if boundary == "after_temp_open" {
                    assert!(bytes.is_empty());
                } else {
                    let prepared = root_head::decode("v", &bytes).unwrap();
                    assert_eq!(prepared.root_hash, Some(new));
                    assert_eq!(prepared.receipts.get("a"), Some(&outcome_receipt(2, &new)));
                }
                // A complete newer temp is still not selected authority.
                assert_eq!(reopened.try_get_current_root("v").unwrap(), Some(old));
                assert_eq!(std::fs::read(&tmp).unwrap(), bytes);
            }
        }
        assert_eq!(
            actual_child_runs, 6,
            "native child coverage must not silently skip"
        );
    }

    #[test]
    fn root_head_native_process_first_publish_never_promotes_orphan() {
        let mut actual_child_runs = 0;
        for boundary in PROCESS_CUTS {
            let fixture = tempfile::Builder::new()
                .prefix("obsetync-root-head-process-")
                .tempdir()
                .unwrap();
            std::fs::write(
                fixture.path().join("process-fixture"),
                PROCESS_FIXTURE_MARKER,
            )
            .unwrap();
            let layout = StorageLayout::new(fixture.path());
            let new = hash_bytes(b"process-new");
            run_root_head_process_child(fixture.path(), boundary, "first");
            actual_child_runs += 1;

            let reopened = VaultStore::new(layout.clone());
            let path = layout.vault_current_path("v");
            let tmp = path.with_extension("tmp");
            if matches!(boundary, "after_rename" | "after_head_dir_sync") {
                assert_eq!(reopened.try_get_current_root("v").unwrap(), Some(new));
                assert_eq!(
                    reopened.get_root_receipt("v", "a").unwrap(),
                    Some(outcome_receipt(1, &new))
                );
                assert!(!tmp.exists());
            } else {
                let bytes = std::fs::read(&tmp).unwrap();
                if boundary == "after_temp_open" {
                    assert!(bytes.is_empty());
                } else {
                    let prepared = root_head::decode("v", &bytes).unwrap();
                    assert_eq!(prepared.root_hash, Some(new));
                    assert_eq!(prepared.receipts.get("a"), Some(&outcome_receipt(1, &new)));
                }
                assert!(reopened.try_get_current_root("v").is_err());
                assert!(reopened.get_root_receipt("v", "a").is_err());
                assert!(
                    !path.exists(),
                    "orphan was promoted after native process exit at {boundary}"
                );
                assert_eq!(std::fs::read(&tmp).unwrap(), bytes);
            }
        }
        assert_eq!(
            actual_child_runs, 6,
            "native child coverage must not silently skip"
        );
    }

    #[test]
    fn root_head_native_process_cancellation_reopens_whole_terminal_cuts() {
        let mut actual_child_runs = 0;
        for existing in [false, true] {
            for boundary in PROCESS_CUTS {
                let fixture = tempfile::Builder::new()
                    .prefix("obsetync-root-head-process-")
                    .tempdir()
                    .unwrap();
                std::fs::write(
                    fixture.path().join("process-fixture"),
                    PROCESS_FIXTURE_MARKER,
                )
                .unwrap();
                let layout = StorageLayout::new(fixture.path());
                let old = hash_bytes(b"process-old");
                if existing {
                    VaultStore::new(layout.clone())
                        .set_current_root_with_receipt("v", &old, "a", outcome_receipt(1, &old))
                        .unwrap();
                }
                run_root_head_process_child(
                    fixture.path(),
                    boundary,
                    if existing {
                        "cancel-replace"
                    } else {
                        "cancel-first"
                    },
                );
                actual_child_runs += 1;
                let reopened = VaultStore::new(layout.clone());
                let promoted = matches!(boundary, "after_rename" | "after_head_dir_sync");
                let path = layout.vault_current_path("v");
                if promoted {
                    assert_eq!(
                        reopened.try_get_current_root("v").unwrap(),
                        existing.then_some(old)
                    );
                    assert_eq!(
                        reopened.get_root_receipt("v", "a").unwrap(),
                        Some(cancellation_receipt(if existing { 2 } else { 1 }))
                    );
                    assert!(path.exists());
                    assert!(!path.with_extension("tmp").exists());
                } else if existing {
                    assert_eq!(reopened.try_get_current_root("v").unwrap(), Some(old));
                    assert_eq!(
                        reopened.get_root_receipt("v", "a").unwrap(),
                        Some(outcome_receipt(1, &old))
                    );
                } else {
                    assert!(reopened.try_get_current_root("v").is_err());
                    assert!(reopened.get_root_receipt("v", "a").is_err());
                    assert!(!path.exists());
                }
                if !promoted {
                    let bytes = std::fs::read(path.with_extension("tmp")).unwrap();
                    if boundary == "after_temp_open" {
                        assert!(bytes.is_empty());
                    } else {
                        let prepared = root_head::decode("v", &bytes).unwrap();
                        assert_eq!(prepared.root_hash, existing.then_some(old));
                        assert_eq!(
                            prepared.receipts.get("a"),
                            Some(&cancellation_receipt(if existing { 2 } else { 1 }))
                        );
                    }
                }
            }
        }
        assert_eq!(
            actual_child_runs, 12,
            "native cancellation child coverage must not silently skip"
        );
    }

    #[test]
    fn publication_refusal_marker_never_classifies_native_invalid_input_as_rejected() {
        let refusal = root_head_input_error(root_head_error("capacity exceeded"));
        assert_eq!(refusal.kind(), io::ErrorKind::InvalidInput);
        assert!(is_root_publication_refusal(&refusal));
        assert!(!is_root_publication_refusal(&io::Error::from_raw_os_error(
            22
        )));
        assert!(!is_root_publication_refusal(&io::Error::new(
            io::ErrorKind::InvalidInput,
            "native failure"
        )));
        assert!(!is_root_publication_refusal(&root_head_error(
            "corrupt persisted bytes"
        )));
    }

    #[test]
    fn admission_preview_is_read_only_and_cannot_authorize_a_stale_sequence() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        let store = VaultStore::new(layout.clone());
        let first = hash_bytes(b"first");
        let second = hash_bytes(b"second");
        store
            .validate_root_receipt("v", &first, "a", &outcome_receipt(1, &first))
            .unwrap();
        assert!(!layout.vault_dir("v").exists());
        store
            .set_current_root_with_receipt("v", &first, "a", outcome_receipt(1, &first))
            .unwrap();
        let before = std::fs::read(layout.vault_current_path("v")).unwrap();
        store
            .validate_root_receipt("v", &second, "a", &outcome_receipt(2, &second))
            .unwrap();
        assert_eq!(
            std::fs::read(layout.vault_current_path("v")).unwrap(),
            before
        );
        assert!(!layout
            .vault_current_path("v")
            .with_extension("tmp")
            .exists());
        store
            .set_current_root_with_receipt("v", &first, "a", outcome_receipt(2, &first))
            .unwrap();
        let error = store
            .set_current_root_with_receipt("v", &second, "a", outcome_receipt(2, &second))
            .unwrap_err();
        assert!(is_root_publication_refusal(&error));
        assert_eq!(store.try_get_current_root("v").unwrap(), Some(first));
        assert_eq!(
            store.get_root_receipt("v", "a").unwrap(),
            Some(outcome_receipt(2, &first))
        );
    }

    #[test]
    fn root_receipts_survive_restart_other_devices_and_legacy_admin_replacements() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        let store = VaultStore::new(layout.clone());
        let first = hash_bytes(b"first");
        let second = hash_bytes(b"second");
        let third = hash_bytes(b"third");
        store.set_current_root("v", &first).unwrap();
        assert_eq!(
            std::fs::read(layout.vault_current_path("v")).unwrap().len(),
            64
        );
        store
            .set_current_root_with_receipt("v", &second, "a", outcome_receipt(1, &second))
            .unwrap();
        store
            .set_current_root_with_receipt("v", &third, "b", outcome_receipt(1, &third))
            .unwrap();
        store.set_current_root("v", &first).unwrap();

        let restarted = VaultStore::new(layout.clone());
        assert_eq!(restarted.try_get_current_root("v").unwrap(), Some(first));
        assert_eq!(
            restarted.get_root_receipt("v", "a").unwrap(),
            Some(outcome_receipt(1, &second))
        );
        assert_eq!(
            restarted.get_root_receipt("v", "b").unwrap(),
            Some(outcome_receipt(1, &third))
        );
        assert_eq!(restarted.get_root_receipt("v", "missing").unwrap(), None);
        assert!(std::fs::read(layout.vault_current_path("v"))
            .unwrap()
            .starts_with(b"OBSETYNC_ROOT_HEAD_V1\n"));
        restarted
            .set_current_root_with_receipt("v", &third, "a", outcome_receipt(2, &third))
            .unwrap();
        assert_eq!(
            restarted.get_root_receipt("v", "b").unwrap(),
            Some(outcome_receipt(1, &third))
        );
    }

    #[test]
    fn receipt_input_refusals_do_not_change_authority_or_allocate_first_vault() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        let store = VaultStore::new(layout.clone());
        let h = hash_bytes(b"root");
        for (device, mut receipt) in [
            ("../bad", outcome_receipt(1, &h)),
            ("a", outcome_receipt(0, &h)),
            ("a", outcome_receipt(2, &h)),
        ] {
            if device == "../bad" {
                receipt.sequence = 1;
            }
            assert_eq!(
                store
                    .set_current_root_with_receipt("v", &h, device, receipt)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
            assert!(!layout.vault_dir("v").exists());
        }
        let mut huge = outcome_receipt(1, &h);
        huge.result = serde_json::json!({"merged":true,"root_hash":hash_to_hex(&h),"auto_resolved":0,"text_merged":0,"conflicts":[{
            "path":"x".repeat(root_head::MAX_ROOT_RECEIPT_BYTES),"base_hash":"a".repeat(64),"side_a_hash":"b".repeat(64),"side_b_hash":"c".repeat(64)
        }]});
        assert_eq!(
            store
                .set_current_root_with_receipt("v", &h, "a", huge)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(!layout.vault_dir("v").exists());
        store
            .set_current_root_with_receipt("v", &h, "a", outcome_receipt(1, &h))
            .unwrap();
        let before = std::fs::read(layout.vault_current_path("v")).unwrap();
        for sequence in [0, 1, 3, MAX_ROOT_SEQUENCE + 1] {
            assert_eq!(
                store
                    .set_current_root_with_receipt("v", &h, "a", outcome_receipt(sequence, &h))
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(
                std::fs::read(layout.vault_current_path("v")).unwrap(),
                before
            );
        }
        let other = hash_bytes(b"other");
        assert!(store
            .set_current_root_with_receipt("v", &other, "a", outcome_receipt(2, &h))
            .is_err());
        assert_eq!(
            std::fs::read(layout.vault_current_path("v")).unwrap(),
            before
        );
        assert!(!layout
            .vault_current_path("v")
            .with_extension("tmp")
            .exists());
    }

    #[test]
    fn receipt_device_and_aggregate_caps_refuse_before_head_publication() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.ensure_vault("v").unwrap();
        let store = VaultStore::new(layout.clone());
        let h = hash_bytes(b"root");
        let head = RootHead {
            root_hash: Some(h),
            receipts: (0..root_head::MAX_ROOT_RECEIPT_DEVICES)
                .map(|i| (format!("d{i}"), outcome_receipt(1, &h)))
                .collect(),
        };
        let bytes = root_head::encode("v", &head).unwrap();
        std::fs::write(layout.vault_current_path("v"), &bytes).unwrap();
        assert_eq!(
            store
                .set_current_root_with_receipt("v", &h, "new", outcome_receipt(1, &h))
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            std::fs::read(layout.vault_current_path("v")).unwrap(),
            bytes
        );
        let mut large = outcome_receipt(1, &h);
        large.result = serde_json::json!({"merged":true,"root_hash":hash_to_hex(&h),"auto_resolved":0,"text_merged":0,"conflicts":[{
            "path":"x".repeat(60*1024),"base_hash":"a".repeat(64),"side_a_hash":"b".repeat(64),"side_b_hash":"c".repeat(64)
        }]});
        let mut large_head = RootHead {
            root_hash: Some(h),
            receipts: BTreeMap::new(),
        };
        let mut last_valid = Vec::new();
        for i in 0..128 {
            large_head.receipts.insert(format!("d{i}"), large.clone());
            match root_head::encode("v", &large_head) {
                Ok(bytes) => last_valid = bytes,
                Err(_) => break,
            }
        }
        assert!(!last_valid.is_empty());
        std::fs::write(layout.vault_current_path("v"), &last_valid).unwrap();
        assert_eq!(
            store
                .set_current_root_with_receipt("v", &h, "new", large)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            std::fs::read(layout.vault_current_path("v")).unwrap(),
            last_valid
        );
        assert!(!layout
            .vault_current_path("v")
            .with_extension("tmp")
            .exists());
    }

    #[test]
    fn maximum_sequence_is_readable_but_cannot_wrap_or_restart() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.ensure_vault("v").unwrap();
        let h = hash_bytes(b"root");
        let head = RootHead {
            root_hash: Some(h),
            receipts: BTreeMap::from([("a".into(), outcome_receipt(MAX_ROOT_SEQUENCE - 1, &h))]),
        };
        std::fs::write(
            layout.vault_current_path("v"),
            root_head::encode("v", &head).unwrap(),
        )
        .unwrap();
        let store = VaultStore::new(layout.clone());
        store
            .set_current_root_with_receipt("v", &h, "a", outcome_receipt(MAX_ROOT_SEQUENCE, &h))
            .unwrap();
        let restarted = VaultStore::new(layout);
        assert_eq!(
            restarted
                .get_root_receipt("v", "a")
                .unwrap()
                .unwrap()
                .sequence,
            MAX_ROOT_SEQUENCE
        );
        for sequence in [1, MAX_ROOT_SEQUENCE, MAX_ROOT_SEQUENCE + 1] {
            assert!(restarted
                .set_current_root_with_receipt("v", &h, "a", outcome_receipt(sequence, &h))
                .is_err());
        }
    }

    #[test]
    fn strict_root_reads_never_downgrade_corruption_scope_or_io_failure_to_empty() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.ensure_vault("v").unwrap();
        let path = layout.vault_current_path("v");
        let store = VaultStore::new(layout.clone());
        let h = hash_bytes(b"root");
        assert_eq!(store.try_get_current_root("v").unwrap(), None);
        let foreign = RootHead {
            root_hash: Some(h),
            receipts: BTreeMap::from([("a".into(), outcome_receipt(1, &h))]),
        };
        for bytes in [
            b"corrupt".to_vec(),
            vec![b'x'; MAX_ROOT_HEAD_BYTES + 1],
            root_head::encode("other", &foreign).unwrap(),
        ] {
            std::fs::write(&path, &bytes).unwrap();
            assert_eq!(
                store.try_get_current_root("v").unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
            assert!(store.get_root_receipt("v", "a").is_err());
            assert_eq!(
                store.set_current_root("v", &h).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
            assert_eq!(
                store
                    .set_current_root_with_receipt("v", &h, "a", outcome_receipt(1, &h))
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidData
            );
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
        }
        // The old lossy Option method remains compatibility-only.
        assert!(store.get_current_root("v").is_none());
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(store.try_get_current_root("v").is_err());
        std::fs::remove_dir(&path).unwrap();
        arm_root_fault("before_head_read");
        assert_eq!(
            store.try_get_current_root("v").unwrap_err().kind(),
            io::ErrorKind::Other
        );
        assert_root_fault_consumed();
    }

    #[test]
    fn orphan_temporary_is_not_promoted_but_does_not_hide_valid_current() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.ensure_vault("v").unwrap();
        let path = layout.vault_current_path("v");
        let tmp = path.with_extension("tmp");
        let store = VaultStore::new(layout.clone());
        let h = hash_bytes(b"root");
        let prepared = RootHead {
            root_hash: Some(h),
            receipts: BTreeMap::from([("a".into(), outcome_receipt(1, &h))]),
        };
        for bytes in [
            b"partial".to_vec(),
            root_head::encode("v", &prepared).unwrap(),
        ] {
            std::fs::write(&tmp, bytes).unwrap();
            assert!(store.try_get_current_root("v").is_err());
            assert!(store.get_root_receipt("v", "a").is_err());
            assert!(store
                .set_current_root_with_receipt("v", &h, "a", outcome_receipt(1, &h))
                .is_err());
            assert!(!path.exists());
        }
        // An already selected old head, not the orphan, is authoritative.
        std::fs::write(&path, hash_to_hex(&h)).unwrap();
        assert_eq!(store.try_get_current_root("v").unwrap(), Some(h));
        assert_eq!(store.get_root_receipt("v", "a").unwrap(), None);
        store
            .set_current_root_with_receipt("v", &h, "a", outcome_receipt(1, &h))
            .unwrap();
        assert!(!tmp.exists());
        assert_eq!(
            store.get_root_receipt("v", "a").unwrap(),
            Some(outcome_receipt(1, &h))
        );
    }

    #[test]
    fn every_head_publication_fault_selects_one_whole_root_receipt_cut_after_restart() {
        for boundary in [
            "before_head_read",
            "after_head_read",
            "before_temp_open",
            "after_temp_open",
            "after_temp_write",
            "after_temp_sync",
            "before_rename",
            "after_rename",
            "before_head_dir_sync",
            "after_head_dir_sync",
        ] {
            let dir = tempdir().unwrap();
            let layout = StorageLayout::new(dir.path());
            let store = VaultStore::new(layout.clone());
            let old = hash_bytes(b"old");
            let new = hash_bytes(b"new");
            store
                .set_current_root_with_receipt("v", &old, "a", outcome_receipt(1, &old))
                .unwrap();
            arm_root_fault(boundary);
            assert!(
                store
                    .set_current_root_with_receipt("v", &new, "a", outcome_receipt(2, &new))
                    .is_err(),
                "{boundary}"
            );
            assert_root_fault_consumed();
            let restarted = VaultStore::new(layout.clone());
            let root = restarted.try_get_current_root("v").unwrap().unwrap();
            let receipt = restarted.get_root_receipt("v", "a").unwrap().unwrap();
            let promoted = matches!(
                boundary,
                "after_rename" | "before_head_dir_sync" | "after_head_dir_sync"
            );
            assert_eq!(
                (root, receipt),
                if promoted {
                    (new, outcome_receipt(2, &new))
                } else {
                    (old, outcome_receipt(1, &old))
                },
                "{boundary}"
            );
            if !promoted {
                // Valid current plus orphan temp permits an explicit retry,
                // which overwrites the known temporary under the vault lock.
                restarted
                    .set_current_root_with_receipt("v", &new, "a", outcome_receipt(2, &new))
                    .unwrap();
            } else {
                assert!(restarted
                    .set_current_root_with_receipt("v", &new, "a", outcome_receipt(2, &new))
                    .is_err());
            }
            assert_eq!(
                restarted.get_root_receipt("v", "a").unwrap(),
                Some(outcome_receipt(2, &new))
            );
        }
    }

    #[test]
    fn first_head_faults_never_promote_or_acknowledge_orphan_temporary() {
        for boundary in [
            "before_temp_open",
            "after_temp_open",
            "after_temp_write",
            "after_temp_sync",
            "before_rename",
            "after_rename",
        ] {
            let dir = tempdir().unwrap();
            let layout = StorageLayout::new(dir.path());
            let store = VaultStore::new(layout.clone());
            let h = hash_bytes(b"root");
            arm_root_fault(boundary);
            assert!(store
                .set_current_root_with_receipt("v", &h, "a", outcome_receipt(1, &h))
                .is_err());
            assert_root_fault_consumed();
            let restarted = VaultStore::new(layout.clone());
            match boundary {
                "before_temp_open" => {
                    assert_eq!(restarted.try_get_current_root("v").unwrap(), None)
                }
                "after_rename" => assert_eq!(
                    restarted.get_root_receipt("v", "a").unwrap(),
                    Some(outcome_receipt(1, &h))
                ),
                _ => {
                    assert!(restarted.get_root_receipt("v", "a").is_err());
                    assert!(!layout.vault_current_path("v").exists());
                }
            }
        }
    }

    #[test]
    fn accepted_receipt_replay_requires_file_and_directory_barriers_after_ambiguous_write() {
        for boundary in [
            "before_replay_file_sync",
            "after_replay_file_sync",
            "before_replay_dir_sync",
            "after_replay_dir_sync",
        ] {
            let dir = tempdir().unwrap();
            let layout = StorageLayout::new(dir.path());
            let store = VaultStore::new(layout.clone());
            let h = hash_bytes(b"root");
            arm_root_fault("after_rename");
            assert!(store
                .set_current_root_with_receipt("v", &h, "a", outcome_receipt(1, &h))
                .is_err());
            assert_root_fault_consumed();
            let restarted = VaultStore::new(layout);
            arm_root_fault(boundary);
            assert!(restarted.get_root_receipt("v", "a").is_err(), "{boundary}");
            assert_root_fault_consumed();
            assert_eq!(
                restarted.get_root_receipt("v", "a").unwrap(),
                Some(outcome_receipt(1, &h))
            );
        }
    }

    #[test]
    fn new_directory_faults_refuse_ack_and_retry_syncs_the_complete_ancestry() {
        for boundary in [
            "before_mkdir",
            "after_mkdir",
            "after_mkdir_sync",
            "after_mkdir_parent_sync",
        ] {
            let dir = tempdir().unwrap();
            let layout = StorageLayout::new(dir.path().join("new/deeper"));
            let store = VaultStore::new(layout.clone());
            let h = hash_bytes(b"root");
            arm_root_fault(boundary);
            assert!(store
                .set_current_root_with_receipt("v", &h, "a", outcome_receipt(1, &h))
                .is_err());
            assert_root_fault_consumed();
            assert_eq!(store.try_get_current_root("v").unwrap(), None);
            store
                .set_current_root_with_receipt("v", &h, "a", outcome_receipt(1, &h))
                .unwrap();
            assert_eq!(
                store.get_root_receipt("v", "a").unwrap(),
                Some(outcome_receipt(1, &h))
            );
        }
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path().join("new/deeper"));
        let perf = Arc::new(ServerPerfCounters::default());
        let store = VaultStore::with_perf(layout.clone(), perf.clone());
        let h = hash_bytes(b"root");
        store
            .set_current_root_with_receipt("v", &h, "a", outcome_receipt(1, &h))
            .unwrap();
        // Five newly created dirs, each synced along with its parent, then
        // the head file plus every ancestor of the selected current entry.
        let expected_syncs = 5 * 2 + 1 + layout.vault_dir("v").ancestors().count() as u64;
        assert_eq!(perf.snapshot().storage.fdatasyncs, expected_syncs);
    }

    #[test]
    fn reused_history_after_ambiguous_rename_needs_its_own_durability_barrier() {
        for boundary in [
            "before_history_reuse_file_sync",
            "after_history_reuse_file_sync",
            "before_history_reuse_dir_sync",
            "after_history_reuse_dir_sync",
        ] {
            let dir = tempdir().unwrap();
            let layout = StorageLayout::new(dir.path());
            let store = VaultStore::new(layout.clone());
            let root = RootNode {
                vault_id: "v".into(),
                created_ms: 1,
                version: 1,
                children: vec![],
                total_files: 0,
                parent_hash: None,
                device_id: "a".into(),
            };
            let hash = root.hash();
            arm_root_fault("after_blob_rename");
            assert!(store.store_root("v", &hash, &root.serialize()).is_err());
            assert_root_fault_consumed();
            assert!(layout.vault_root_path("v", &hash).exists());
            assert_eq!(store.try_get_current_root("v").unwrap(), None);
            arm_root_fault(boundary);
            let attempt = (|| -> io::Result<()> {
                store.store_root("v", &hash, &root.serialize())?;
                store.set_current_root_with_receipt("v", &hash, "a", outcome_receipt(1, &hash))
            })();
            assert!(attempt.is_err());
            assert_root_fault_consumed();
            assert_eq!(store.try_get_current_root("v").unwrap(), None);
            // History without current is legal first-publication staging. Its
            // presence alone was never evidence that the operation committed.
            store.store_root("v", &hash, &root.serialize()).unwrap();
            store
                .set_current_root_with_receipt("v", &hash, "a", outcome_receipt(1, &hash))
                .unwrap();
            assert_eq!(
                store.get_root_receipt("v", "a").unwrap(),
                Some(outcome_receipt(1, &hash))
            );
        }
    }

    #[test]
    fn measured_blob_io_updates_aggregate_counters() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("objects/blob");
        let perf = ServerPerfCounters::default();
        let expected = hash_bytes(b"measured");

        write_blob_measured(&path, b"measured", &perf).unwrap();
        assert_eq!(read_blob_measured(&path, &perf).unwrap(), b"measured");
        assert!(blob_matches_hash_measured(&path, &expected, &perf));

        let snapshot = perf.snapshot();
        assert_eq!(snapshot.storage.loose_writes, 1);
        assert_eq!(snapshot.storage.bytes_written, 8);
        assert_eq!(snapshot.storage.fdatasyncs, 2);
        assert_eq!(snapshot.storage.loose_reads, 2);
        assert_eq!(snapshot.storage.bytes_read, 16);
        assert_eq!(snapshot.storage.bytes_rehashed, 8);
        assert_eq!(snapshot.storage.corrupted_records, 0);
    }

    #[test]
    fn init_directories_creates_full_layout() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        for sub in [
            "ca",
            "server",
            "devices",
            "devices/tokens",
            "enrollments",
            "vaults",
            "index",
            "content",
            "content/manifests",
            "content/chunks",
            "storage-writer",
        ] {
            assert!(
                dir.path().join(sub).is_dir(),
                "{} should exist after init_directories",
                sub
            );
        }
    }

    #[test]
    fn init_directories_is_idempotent() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        // Calling twice must not error.
        layout.init_directories().unwrap();
    }

    #[test]
    fn paths_use_two_char_sharding() {
        let layout = StorageLayout::new("/base");
        let h = hash_bytes(b"x");
        let hex = sync_core::hash::hash_to_hex(&h);

        assert_eq!(
            layout.index_path(&h),
            PathBuf::from(format!("/base/index/{}/{}", &hex[..2], &hex[2..]))
        );
        assert_eq!(
            layout.content_blob_path(&h),
            PathBuf::from(format!("/base/content/{}/{}", &hex[..2], &hex[2..]))
        );
        assert_eq!(
            layout.content_manifest_path(&h),
            PathBuf::from(format!(
                "/base/content/manifests/{}/{}",
                &hex[..2],
                &hex[2..]
            ))
        );
        assert_eq!(
            layout.content_chunk_path(&h),
            PathBuf::from(format!("/base/content/chunks/{}/{}", &hex[..2], &hex[2..]))
        );
        assert_eq!(
            layout.storage_writer_journal_path(),
            PathBuf::from("/base/storage-writer/loose-groups-v1.log")
        );
    }

    #[test]
    fn journaled_materialization_is_atomic_and_replaceable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("objects/blob");
        materialize_journaled_blob(&path, b"first").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"first");
        materialize_journaled_blob(&path, b"first").unwrap();
        materialize_journaled_blob(&path, b"replacement").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
        assert_eq!(
            std::fs::read_dir(path.parent().unwrap()).unwrap().count(),
            1
        );
    }

    #[test]
    fn vault_paths_namespaced_by_vault_id() {
        let layout = StorageLayout::new("/d");
        assert_eq!(layout.vault_dir("v1"), PathBuf::from("/d/vaults/v1"));
        assert_eq!(
            layout.vault_current_path("v1"),
            PathBuf::from("/d/vaults/v1/current")
        );
        assert_eq!(
            layout.vault_roots_dir("v1"),
            PathBuf::from("/d/vaults/v1/roots")
        );

        let h = hash_bytes(b"r");
        let hex = sync_core::hash::hash_to_hex(&h);
        assert_eq!(
            layout.vault_root_path("v1", &h),
            PathBuf::from(format!("/d/vaults/v1/roots/{}.bin", hex))
        );
    }

    #[test]
    fn device_and_token_and_enrollment_paths() {
        let layout = StorageLayout::new("/d");
        assert_eq!(layout.device_dir("abc"), PathBuf::from("/d/devices/abc"));
        assert_eq!(
            layout.token_path("tok"),
            PathBuf::from("/d/devices/tokens/tok")
        );
        assert_eq!(
            layout.enrollment_path("AXBR-7742"),
            PathBuf::from("/d/enrollments/AXBR-7742.json")
        );
    }

    #[test]
    fn untrusted_identifiers_cannot_escape_their_storage_namespaces() {
        let layout = StorageLayout::new("/data");
        assert!(layout.vault_dir("../../server").starts_with("/data/vaults"));
        assert!(layout
            .token_path("/etc/passwd")
            .starts_with("/data/devices/tokens"));
        assert!(layout.device_dir("..").starts_with("/data/devices"));
        assert!(layout
            .enrollment_path("../secret")
            .starts_with("/data/enrollments"));
        for path in [
            layout.vault_dir("../../server"),
            layout.token_path("/etc/passwd"),
            layout.device_dir(".."),
        ] {
            assert!(!path.components().any(|part| part.as_os_str() == ".."));
        }

        let encoded_unsafe = storage_component("../collision");
        assert_ne!(
            storage_component(&encoded_unsafe),
            encoded_unsafe,
            "a literal safe identifier must not alias an escaped unsafe one",
        );
    }

    #[test]
    fn ensure_vault_creates_roots_dir() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.ensure_vault("v1").unwrap();
        assert!(layout.vault_roots_dir("v1").is_dir());
        // Calling twice is idempotent.
        layout.ensure_vault("v1").unwrap();
    }

    #[test]
    fn vault_store_set_and_get_current_root() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        let store = VaultStore::new(layout.clone());

        let h = hash_bytes(b"root1");
        store.set_current_root("v1", &h).unwrap();
        assert_eq!(store.get_current_root("v1"), Some(h));
    }

    #[test]
    fn vault_store_get_current_root_missing_returns_none() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        let store = VaultStore::new(layout);
        assert!(store.get_current_root("never").is_none());
    }

    #[test]
    fn vault_store_set_current_root_overwrites_atomically() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        let store = VaultStore::new(layout);

        let h1 = hash_bytes(b"a");
        let h2 = hash_bytes(b"b");
        store.set_current_root("v", &h1).unwrap();
        store.set_current_root("v", &h2).unwrap();
        assert_eq!(store.get_current_root("v"), Some(h2));
    }

    #[test]
    fn vault_store_root_history_roundtrip() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        let store = VaultStore::new(layout);
        let h = hash_bytes(b"root-bytes");
        store.store_root("v", &h, b"the bytes").unwrap();
        assert_eq!(store.get_root("v", &h).unwrap(), b"the bytes".to_vec());
    }

    #[test]
    fn vault_store_does_not_rewrite_metadata_for_the_same_semantic_root() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        let store = VaultStore::new(layout);
        let first = RootNode {
            vault_id: "v".into(),
            created_ms: 1,
            version: 1,
            children: vec![],
            total_files: 0,
            parent_hash: None,
            device_id: "first-device".into(),
        };
        let second = RootNode {
            created_ms: 2,
            parent_hash: Some(hash_bytes(b"forged-parent")),
            device_id: "second-device".into(),
            ..first.clone()
        };
        let hash = first.hash();
        assert_eq!(hash, second.hash());

        store.store_root("v", &hash, &first.serialize()).unwrap();
        store.store_root("v", &hash, &second.serialize()).unwrap();

        let stored = RootNode::deserialize(&store.get_root("v", &hash).unwrap()).unwrap();
        assert_eq!(stored.created_ms, 1);
        assert_eq!(stored.device_id, "first-device");
        assert_eq!(stored.parent_hash, None);
    }

    #[test]
    fn vault_store_get_root_missing_returns_none() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        let store = VaultStore::new(layout);
        assert!(store.get_root("v", &hash_bytes(b"missing")).is_none());
    }

    #[test]
    fn vault_store_exists_after_root_stored() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        let store = VaultStore::new(layout);
        assert!(!store.vault_exists("v"));
        store.store_root("v", &hash_bytes(b"r"), b"x").unwrap();
        assert!(store.vault_exists("v"));
    }

    #[test]
    fn read_write_blob_helpers_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nested/sub/blob");
        write_blob(&path, b"hello").unwrap();
        assert!(blob_exists(&path));
        assert_eq!(read_blob(&path).unwrap(), b"hello".to_vec());
    }

    #[test]
    fn write_blob_replaces_an_incomplete_existing_object() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("objects/blob");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"partial").unwrap();
        write_blob(&path, b"complete payload").unwrap();
        assert_eq!(read_blob(&path).unwrap(), b"complete payload".to_vec());
        assert_eq!(
            std::fs::read_dir(path.parent().unwrap()).unwrap().count(),
            1
        );
        assert!(blob_matches_hash(&path, &hash_bytes(b"complete payload")));
        assert!(!blob_matches_hash(&path, &hash_bytes(b"other")));
    }

    #[test]
    fn read_blob_missing_returns_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nope");
        assert!(read_blob(&path).is_none());
        assert!(!blob_exists(&path));
    }

    #[test]
    fn write_blob_creates_parent_directories() {
        let dir = tempdir().unwrap();
        let deep = dir.path().join("a/b/c/d/e/file");
        write_blob(&deep, b"x").unwrap();
        assert!(deep.exists());
    }

    #[test]
    fn vault_store_recovers_from_garbled_current_file() {
        // current file contains garbage hex — get_current_root must return None,
        // never panic.
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.ensure_vault("v").unwrap();
        std::fs::write(layout.vault_current_path("v"), "not-hex-content").unwrap();
        let store = VaultStore::new(layout);
        assert!(store.get_current_root("v").is_none());
    }
}
