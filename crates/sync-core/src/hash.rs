/// 32-byte Blake3 hash used throughout the system.
pub type FileHash = [u8; 32];

/// Hash raw bytes (file content, chunk payload, etc.).
pub fn hash_bytes(data: &[u8]) -> FileHash {
    *blake3::hash(data).as_bytes()
}

/// Hash file content — alias for clarity at call sites.
pub fn hash_content(data: &[u8]) -> FileHash {
    hash_bytes(data)
}

/// Incremental hasher for building chunk hashes from multiple fields.
pub struct IncrementalHasher {
    inner: blake3::Hasher,
}

impl IncrementalHasher {
    pub fn new() -> Self {
        Self {
            inner: blake3::Hasher::new(),
        }
    }

    pub fn update(&mut self, data: &[u8]) -> &mut Self {
        self.inner.update(data);
        self
    }

    pub fn update_str(&mut self, s: &str) -> &mut Self {
        self.inner.update(s.as_bytes());
        self
    }

    pub fn update_u64(&mut self, v: u64) -> &mut Self {
        self.inner.update(&v.to_le_bytes());
        self
    }

    pub fn finalize(self) -> FileHash {
        *self.inner.finalize().as_bytes()
    }
}

impl Default for IncrementalHasher {
    fn default() -> Self {
        Self::new()
    }
}

/// Zero hash — used as a sentinel for "no hash" / empty state.
pub const ZERO_HASH: FileHash = [0u8; 32];

/// Format a hash as lowercase hex string.
pub fn hash_to_hex(hash: &FileHash) -> String {
    hex::encode(hash)
}

/// Parse a hex string into a FileHash.
pub fn hex_to_hash(s: &str) -> Result<FileHash, hex::FromHexError> {
    let bytes = hex::decode(s)?;
    let mut hash = [0u8; 32];
    if bytes.len() != 32 {
        return Err(hex::FromHexError::InvalidStringLength);
    }
    hash.copy_from_slice(&bytes);
    Ok(hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_deterministic() {
        let data = b"hello world";
        let h1 = hash_bytes(data);
        let h2 = hash_bytes(data);
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_different_inputs() {
        let h1 = hash_bytes(b"hello");
        let h2 = hash_bytes(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn hex_roundtrip() {
        let hash = hash_bytes(b"test");
        let hex_str = hash_to_hex(&hash);
        let parsed = hex_to_hash(&hex_str).unwrap();
        assert_eq!(hash, parsed);
    }

    #[test]
    fn incremental_hasher() {
        let mut h = IncrementalHasher::new();
        h.update_str("notes/jan.md");
        h.update(&hash_bytes(b"content"));
        h.update_u64(1720000000000);
        let result = h.finalize();
        assert_ne!(result, ZERO_HASH);
    }
}
