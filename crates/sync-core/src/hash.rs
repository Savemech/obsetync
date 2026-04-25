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

    #[test]
    fn incremental_hasher_default_eq_new() {
        let a = IncrementalHasher::new().finalize();
        let b = IncrementalHasher::default().finalize();
        assert_eq!(a, b);
    }

    #[test]
    fn incremental_hasher_order_matters() {
        let mut a = IncrementalHasher::new();
        a.update_str("foo");
        a.update_str("bar");
        let mut b = IncrementalHasher::new();
        b.update_str("bar");
        b.update_str("foo");
        assert_ne!(a.finalize(), b.finalize());
    }

    #[test]
    fn incremental_hasher_u64_le_encoding() {
        // update_u64 uses to_le_bytes; equivalent to update with the same 8 bytes.
        let mut a = IncrementalHasher::new();
        a.update_u64(0x0102030405060708);
        let mut b = IncrementalHasher::new();
        b.update(&0x0102030405060708u64.to_le_bytes());
        assert_eq!(a.finalize(), b.finalize());
    }

    #[test]
    fn hash_content_is_alias_for_hash_bytes() {
        let data = b"alias check";
        assert_eq!(hash_content(data), hash_bytes(data));
    }

    #[test]
    fn zero_hash_is_all_zeros() {
        assert_eq!(ZERO_HASH, [0u8; 32]);
        assert_ne!(hash_bytes(b""), ZERO_HASH);
    }

    #[test]
    fn hex_to_hash_rejects_wrong_length() {
        // 31-byte hex (62 chars) — must fail with InvalidStringLength.
        let short = "ab".repeat(31);
        assert!(hex_to_hash(&short).is_err());

        let long = "ab".repeat(33);
        assert!(hex_to_hash(&long).is_err());
    }

    #[test]
    fn hex_to_hash_rejects_non_hex() {
        // 64 chars but 'z' is not hex.
        let bad = "z".repeat(64);
        assert!(hex_to_hash(&bad).is_err());
    }

    #[test]
    fn hash_to_hex_returns_64_lowercase_chars() {
        let hex = hash_to_hex(&hash_bytes(b"x"));
        assert_eq!(hex.len(), 64);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    #[test]
    fn empty_input_hash_is_stable() {
        // Blake3 of empty input is fixed; check it does not equal ZERO_HASH
        // and is repeatable across calls.
        let h1 = hash_bytes(b"");
        let h2 = hash_bytes(b"");
        assert_eq!(h1, h2);
        assert_ne!(h1, ZERO_HASH);
    }
}
