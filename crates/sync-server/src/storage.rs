use crate::perf::ServerPerfCounters;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use sync_core::chunk::RootNode;
use sync_core::hash::{hash_to_hex, hex_to_hash, FileHash};

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
        std::fs::create_dir_all(self.vault_roots_dir(vault_id))
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

    /// Get the current root hash for a vault. Returns None if vault doesn't exist.
    pub fn get_current_root(&self, vault_id: &str) -> Option<FileHash> {
        let path = self.layout.vault_current_path(vault_id);
        let started = Instant::now();
        let hex = std::fs::read_to_string(&path).ok()?;
        if let Some(perf) = &self.perf {
            perf.record_loose_read(hex.len() as u64, started.elapsed());
        }
        hex_to_hash(hex.trim()).ok()
    }

    /// Set the current root hash for a vault (atomic write via rename).
    pub fn set_current_root(&self, vault_id: &str, hash: &FileHash) -> Result<(), std::io::Error> {
        self.layout.ensure_vault(vault_id)?;
        let path = self.layout.vault_current_path(vault_id);
        let tmp = path.with_extension("tmp");
        let write_started = Instant::now();
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(hash_to_hex(hash).as_bytes())?;
        let write_elapsed = write_started.elapsed();
        let durability_started = Instant::now();
        file.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        std::fs::File::open(path.parent().expect("current root has parent"))?.sync_all()?;
        if let Some(perf) = &self.perf {
            perf.record_loose_write(64, write_elapsed, durability_started.elapsed(), 2);
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
        self.layout.ensure_vault(vault_id)?;
        let path = self.layout.vault_root_path(vault_id, hash);

        // Root identity deliberately commits only to the ordered child-hash
        // set, not created_ms/device/parent metadata. History is therefore a
        // set of unique states: once valid metadata has been recorded for a
        // semantic root, a replay of the same state must not rewrite it.
        if read_blob_with_optional_perf(&path, self.perf.as_deref()).is_some_and(|existing| {
            RootNode::deserialize(&existing)
                .is_ok_and(|root| root.vault_id == vault_id && root.hash() == *hash)
        }) {
            return Ok(());
        }
        write_blob_with_optional_perf(&path, data, self.perf.as_deref())
    }

    /// Load a root node's bytes from history.
    pub fn get_root(&self, vault_id: &str, hash: &FileHash) -> Option<Vec<u8>> {
        let path = self.layout.vault_root_path(vault_id, hash);
        read_blob_with_optional_perf(&path, self.perf.as_deref())
    }

    /// Check if a vault exists (has at least one root).
    #[allow(dead_code)]
    pub fn vault_exists(&self, vault_id: &str) -> bool {
        self.layout.vault_dir(vault_id).exists()
    }
}

/// Helper: read a content-addressed blob from a path.
#[cfg(test)]
pub fn read_blob(path: &Path) -> Option<Vec<u8>> {
    read_blob_with_optional_perf(path, None)
}

pub fn read_blob_measured(path: &Path, perf: &ServerPerfCounters) -> Option<Vec<u8>> {
    read_blob_with_optional_perf(path, Some(perf))
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

pub fn blob_exists_measured(path: &Path, perf: &ServerPerfCounters) -> bool {
    let exists = path.exists();
    perf.record_index_lookup(exists);
    exists
}

/// Verify that an object still matches the content address encoded by its
/// path. Check endpoints use this for the small set of objects a changed
/// batch wants to reuse, allowing pre-atomic-write partials or disk damage to
/// be repaired by a normal re-upload instead of becoming permanently stuck.
#[cfg(test)]
pub fn blob_matches_hash(path: &Path, expected: &FileHash) -> bool {
    std::fs::read(path).is_ok_and(|bytes| sync_core::hash::hash_bytes(&bytes) == *expected)
}

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
