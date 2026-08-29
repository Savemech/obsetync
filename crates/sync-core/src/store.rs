use crate::chunk::ChunkError;
use crate::hash::{hash_bytes, hash_to_hex, FileHash};
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
/// Layout: <base>/<first2hex>/<remaining60hex>
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

/// In-memory chunk store for testing.
pub struct MemoryChunkStore {
    data: std::cell::RefCell<std::collections::HashMap<FileHash, Vec<u8>>>,
}

impl Default for MemoryChunkStore {
    fn default() -> Self {
        Self {
            data: std::cell::RefCell::new(std::collections::HashMap::new()),
        }
    }
}

impl MemoryChunkStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.data.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.borrow().is_empty()
    }

    pub fn insert_chunk(&self, hash: FileHash, bytes: Vec<u8>) {
        self.data.borrow_mut().insert(hash, bytes);
    }

    pub fn get_chunk(&self, hash: &FileHash) -> Option<Vec<u8>> {
        self.data.borrow().get(hash).cloned()
    }

    pub fn all_chunks(&self) -> Vec<(FileHash, Vec<u8>)> {
        self.data
            .borrow()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    pub fn all_chunk_hashes(&self) -> Vec<FileHash> {
        self.data.borrow().keys().copied().collect()
    }
}

#[async_trait::async_trait(?Send)]
impl ChunkStore for MemoryChunkStore {
    async fn has(&self, hash: &FileHash) -> bool {
        self.data.borrow().contains_key(hash)
    }

    async fn get(&self, hash: &FileHash) -> Result<Vec<u8>, ChunkError> {
        self.data
            .borrow()
            .get(hash)
            .cloned()
            .ok_or_else(|| ChunkError::NotFound(hash_to_hex(hash)))
    }

    async fn put(&self, hash: FileHash, data: Vec<u8>) -> Result<(), ChunkError> {
        self.data.borrow_mut().insert(hash, data);
        Ok(())
    }

    async fn delete(&self, hash: &FileHash) -> Result<(), ChunkError> {
        self.data.borrow_mut().remove(hash);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::hash_bytes;

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
