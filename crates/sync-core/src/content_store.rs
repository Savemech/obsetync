use crate::chunk::ChunkError;
use crate::hash::{FileHash, hash_to_hex};

/// Store for actual file content (the bytes of vault files).
/// Separate from ChunkStore which handles index data.
///
/// Small files (<1MB): stored as whole blobs via put/get.
/// Large files (>=1MB): chunked via FastCDC, stored as manifest + sub-file chunks.
/// See D-001 and D-008.
#[async_trait::async_trait(?Send)]
pub trait ContentStore {
    /// Check if a content blob exists.
    async fn has(&self, hash: &FileHash) -> bool;

    /// Get a content blob (small file or sub-file chunk).
    async fn get(&self, hash: &FileHash) -> Result<Vec<u8>, ChunkError>;

    /// Store a content blob.
    async fn put(&self, hash: FileHash, data: Vec<u8>) -> Result<(), ChunkError>;

    /// Check if a file manifest exists.
    async fn has_manifest(&self, file_hash: &FileHash) -> bool;

    /// Get a file manifest (for large files).
    async fn get_manifest(&self, file_hash: &FileHash) -> Result<FileManifest, ChunkError>;

    /// Store a file manifest.
    async fn put_manifest(&self, manifest: FileManifest) -> Result<(), ChunkError>;
}

/// Manifest for a large file, split into content-defined chunks.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileManifest {
    /// Blake3 hash of the full file content.
    pub file_hash: FileHash,
    /// Total file size in bytes.
    pub total_size: u64,
    /// Ordered list of chunks. Concatenation reconstructs the file.
    pub chunks: Vec<ChunkRef>,
}

/// Reference to a sub-file chunk within a manifest.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkRef {
    /// Blake3 hash of this chunk's bytes.
    pub hash: FileHash,
    /// Byte offset within the original file.
    pub offset: u64,
    /// Chunk size in bytes.
    pub size: u32,
}

/// Disk-based content store.
/// Layout:
///   <base>/<first2hex>/<rest>           — small files / sub-file chunks
///   <base>/manifests/<first2hex>/<rest>  — file manifests (JSON)
pub struct DiskContentStore {
    base: std::path::PathBuf,
}

impl DiskContentStore {
    pub fn new(base: impl Into<std::path::PathBuf>) -> Self {
        Self { base: base.into() }
    }

    fn blob_path(&self, hash: &FileHash) -> std::path::PathBuf {
        let hex = hash_to_hex(hash);
        self.base.join(&hex[..2]).join(&hex[2..])
    }

    fn manifest_path(&self, file_hash: &FileHash) -> std::path::PathBuf {
        let hex = hash_to_hex(file_hash);
        self.base
            .join("manifests")
            .join(&hex[..2])
            .join(&hex[2..])
    }
}

#[async_trait::async_trait(?Send)]
impl ContentStore for DiskContentStore {
    async fn has(&self, hash: &FileHash) -> bool {
        self.blob_path(hash).exists()
    }

    async fn get(&self, hash: &FileHash) -> Result<Vec<u8>, ChunkError> {
        let path = self.blob_path(hash);
        std::fs::read(&path).map_err(|_| ChunkError::NotFound(hash_to_hex(hash)))
    }

    async fn put(&self, hash: FileHash, data: Vec<u8>) -> Result<(), ChunkError> {
        let path = self.blob_path(&hash);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, &data)?;
        Ok(())
    }

    async fn has_manifest(&self, file_hash: &FileHash) -> bool {
        self.manifest_path(file_hash).exists()
    }

    async fn get_manifest(&self, file_hash: &FileHash) -> Result<FileManifest, ChunkError> {
        let path = self.manifest_path(file_hash);
        let data =
            std::fs::read(&path).map_err(|_| ChunkError::NotFound(hash_to_hex(file_hash)))?;
        serde_json::from_slice(&data)
            .map_err(|e| ChunkError::Deserialize(format!("manifest: {}", e)))
    }

    async fn put_manifest(&self, manifest: FileManifest) -> Result<(), ChunkError> {
        let path = self.manifest_path(&manifest.file_hash);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let data = serde_json::to_vec(&manifest)
            .map_err(|e| ChunkError::Deserialize(format!("manifest serialize: {}", e)))?;
        std::fs::write(&path, &data)?;
        Ok(())
    }
}

/// In-memory content store for testing.
pub struct MemoryContentStore {
    blobs: std::cell::RefCell<std::collections::HashMap<FileHash, Vec<u8>>>,
    manifests: std::cell::RefCell<std::collections::HashMap<FileHash, FileManifest>>,
}

impl Default for MemoryContentStore {
    fn default() -> Self {
        Self {
            blobs: std::cell::RefCell::new(std::collections::HashMap::new()),
            manifests: std::cell::RefCell::new(std::collections::HashMap::new()),
        }
    }
}

impl MemoryContentStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait(?Send)]
impl ContentStore for MemoryContentStore {
    async fn has(&self, hash: &FileHash) -> bool {
        self.blobs.borrow().contains_key(hash)
    }

    async fn get(&self, hash: &FileHash) -> Result<Vec<u8>, ChunkError> {
        self.blobs
            .borrow()
            .get(hash)
            .cloned()
            .ok_or_else(|| ChunkError::NotFound(hash_to_hex(hash)))
    }

    async fn put(&self, hash: FileHash, data: Vec<u8>) -> Result<(), ChunkError> {
        self.blobs.borrow_mut().insert(hash, data);
        Ok(())
    }

    async fn has_manifest(&self, file_hash: &FileHash) -> bool {
        self.manifests.borrow().contains_key(file_hash)
    }

    async fn get_manifest(&self, file_hash: &FileHash) -> Result<FileManifest, ChunkError> {
        self.manifests
            .borrow()
            .get(file_hash)
            .cloned()
            .ok_or_else(|| ChunkError::NotFound(hash_to_hex(file_hash)))
    }

    async fn put_manifest(&self, manifest: FileManifest) -> Result<(), ChunkError> {
        self.manifests.borrow_mut().insert(manifest.file_hash, manifest);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::hash_bytes;

    #[tokio::test]
    async fn memory_content_store_blob() {
        let store = MemoryContentStore::new();
        let hash = hash_bytes(b"file content");
        let data = b"file content".to_vec();

        store.put(hash, data.clone()).await.unwrap();
        assert!(store.has(&hash).await);
        assert_eq!(store.get(&hash).await.unwrap(), data);
    }

    #[tokio::test]
    async fn memory_content_store_manifest() {
        let store = MemoryContentStore::new();
        let manifest = FileManifest {
            file_hash: hash_bytes(b"big file"),
            total_size: 150_000_000,
            chunks: vec![
                ChunkRef {
                    hash: hash_bytes(b"chunk1"),
                    offset: 0,
                    size: 262144,
                },
                ChunkRef {
                    hash: hash_bytes(b"chunk2"),
                    offset: 262144,
                    size: 262144,
                },
            ],
        };

        store.put_manifest(manifest.clone()).await.unwrap();
        assert!(store.has_manifest(&manifest.file_hash).await);

        let retrieved = store.get_manifest(&manifest.file_hash).await.unwrap();
        assert_eq!(retrieved.total_size, 150_000_000);
        assert_eq!(retrieved.chunks.len(), 2);
    }

    #[tokio::test]
    async fn disk_content_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = DiskContentStore::new(dir.path());
        let hash = hash_bytes(b"disk content");
        let data = b"disk content bytes".to_vec();

        store.put(hash, data.clone()).await.unwrap();
        assert!(store.has(&hash).await);
        assert_eq!(store.get(&hash).await.unwrap(), data);
    }

    #[tokio::test]
    async fn disk_content_store_manifest_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = DiskContentStore::new(dir.path());
        let manifest = FileManifest {
            file_hash: hash_bytes(b"big disk file"),
            total_size: 1000,
            chunks: vec![ChunkRef {
                hash: hash_bytes(b"c1"),
                offset: 0,
                size: 1000,
            }],
        };

        store.put_manifest(manifest.clone()).await.unwrap();
        assert!(store.has_manifest(&manifest.file_hash).await);

        let retrieved = store.get_manifest(&manifest.file_hash).await.unwrap();
        assert_eq!(retrieved.total_size, 1000);
    }
}
