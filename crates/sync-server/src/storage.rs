use std::path::{Path, PathBuf};
use sync_core::hash::{FileHash, hash_to_hex, hex_to_hash};

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

    // --- Vaults ---

    pub fn vault_dir(&self, vault_id: &str) -> PathBuf {
        self.base.join("vaults").join(vault_id)
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

    // --- Devices ---

    pub fn device_dir(&self, fingerprint: &str) -> PathBuf {
        self.base.join("devices").join(fingerprint)
    }

    /// Path for the bearer-token → fingerprint index entry.
    pub fn token_path(&self, token: &str) -> PathBuf {
        self.base.join("devices").join("tokens").join(token)
    }

    // --- Enrollments ---

    pub fn enrollment_path(&self, code: &str) -> PathBuf {
        self.base.join("enrollments").join(format!("{}.json", code))
    }
}

/// Read/write vault root state.
pub struct VaultStore {
    layout: StorageLayout,
}

impl VaultStore {
    pub fn new(layout: StorageLayout) -> Self {
        Self { layout }
    }

    /// Get the current root hash for a vault. Returns None if vault doesn't exist.
    pub fn get_current_root(&self, vault_id: &str) -> Option<FileHash> {
        let path = self.layout.vault_current_path(vault_id);
        let hex = std::fs::read_to_string(&path).ok()?;
        hex_to_hash(hex.trim()).ok()
    }

    /// Set the current root hash for a vault (atomic write via rename).
    pub fn set_current_root(
        &self,
        vault_id: &str,
        hash: &FileHash,
    ) -> Result<(), std::io::Error> {
        self.layout.ensure_vault(vault_id)?;
        let path = self.layout.vault_current_path(vault_id);
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, hash_to_hex(hash))?;
        std::fs::rename(&tmp, &path)?;
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
        std::fs::write(&path, data)?;
        Ok(())
    }

    /// Load a root node's bytes from history.
    pub fn get_root(&self, vault_id: &str, hash: &FileHash) -> Option<Vec<u8>> {
        let path = self.layout.vault_root_path(vault_id, hash);
        std::fs::read(&path).ok()
    }

    /// Check if a vault exists (has at least one root).
    #[allow(dead_code)]
    pub fn vault_exists(&self, vault_id: &str) -> bool {
        self.layout.vault_dir(vault_id).exists()
    }
}

/// Helper: read a content-addressed blob from a path.
pub fn read_blob(path: &Path) -> Option<Vec<u8>> {
    std::fs::read(path).ok()
}

/// Helper: write a content-addressed blob to a path, creating parent dirs.
pub fn write_blob(path: &Path, data: &[u8]) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, data)?;
    Ok(())
}

/// Helper: check if a content-addressed blob exists.
pub fn blob_exists(path: &Path) -> bool {
    path.exists()
}
