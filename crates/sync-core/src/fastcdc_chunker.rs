use crate::content_store::{ChunkRef, FileManifest};
use crate::hash::{hash_bytes, FileHash};
use fastcdc::v2020::FastCDC;

/// Files below this size are stored as whole blobs — no chunking overhead.
pub const FILE_CHUNK_THRESHOLD: u64 = 1_048_576; // 1MB

/// Minimum sub-file chunk size.
const MIN_CHUNK: u32 = 64 * 1024; // 64KB

/// Average sub-file chunk size.
const AVG_CHUNK: u32 = 256 * 1024; // 256KB

/// Maximum sub-file chunk size.
const MAX_CHUNK: u32 = 1024 * 1024; // 1MB

/// Result of chunking a file.
pub struct ChunkedFile {
    /// The manifest describing how to reassemble the file.
    pub manifest: FileManifest,
    /// The raw chunk data, paired with their hashes.
    /// Only includes chunks that need to be stored/uploaded.
    pub chunk_data: Vec<(FileHash, Vec<u8>)>,
}

/// Split a large file into content-defined chunks using FastCDC.
///
/// Returns a manifest (ordered chunk list) and the raw chunk bytes.
/// Chunks are content-addressed — identical chunks across files are deduped.
pub fn chunk_file(data: &[u8]) -> ChunkedFile {
    let file_hash = hash_bytes(data);

    let mut chunks = Vec::new();
    let mut chunk_data = Vec::new();

    for chunk in FastCDC::new(data, MIN_CHUNK, AVG_CHUNK, MAX_CHUNK) {
        let chunk_bytes = &data[chunk.offset..chunk.offset + chunk.length];
        let chunk_hash = hash_bytes(chunk_bytes);

        chunks.push(ChunkRef {
            hash: chunk_hash,
            offset: chunk.offset as u64,
            size: chunk.length as u32,
        });
        chunk_data.push((chunk_hash, chunk_bytes.to_vec()));
    }

    ChunkedFile {
        manifest: FileManifest {
            file_hash,
            total_size: data.len() as u64,
            chunks,
        },
        chunk_data,
    }
}

/// Reassemble a file from its chunks.
/// Chunks must be provided in the order specified by the manifest.
pub fn reassemble_file(manifest: &FileManifest, chunks: &[(FileHash, Vec<u8>)]) -> Option<Vec<u8>> {
    // Chunks are provided in manifest order — iterate in parallel.
    if manifest.chunks.len() != chunks.len() {
        return None;
    }

    let mut result = Vec::with_capacity(manifest.total_size as usize);

    for (chunk_ref, (chunk_hash, chunk_data)) in manifest.chunks.iter().zip(chunks.iter()) {
        if chunk_hash != &chunk_ref.hash || chunk_data.len() != chunk_ref.size as usize {
            return None;
        }
        result.extend_from_slice(chunk_data);
    }

    // Verify the reassembled file hash matches.
    if hash_bytes(&result) != manifest.file_hash {
        return None;
    }

    Some(result)
}

/// Check if a file should be sub-chunked based on its size.
pub fn should_chunk(size: u64) -> bool {
    size >= FILE_CHUNK_THRESHOLD
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_file_no_chunk() {
        assert!(!should_chunk(100));
        assert!(!should_chunk(1_048_575));
        assert!(should_chunk(1_048_576));
        assert!(should_chunk(10_000_000));
    }

    #[test]
    fn chunk_and_reassemble() {
        // Create a file large enough to produce multiple chunks.
        // At 256KB avg, we need >256KB to get at least 1 chunk.
        // Use 2MB to get ~8 chunks.
        let data: Vec<u8> = (0..2_000_000u32).flat_map(|i| i.to_le_bytes()).collect();

        let chunked = chunk_file(&data);

        assert!(
            chunked.manifest.chunks.len() > 1,
            "expected multiple chunks"
        );
        assert_eq!(chunked.manifest.total_size, data.len() as u64);
        assert_eq!(chunked.manifest.file_hash, hash_bytes(&data));

        // Every chunk in manifest has corresponding data.
        assert_eq!(chunked.manifest.chunks.len(), chunked.chunk_data.len());

        // Reassemble and verify.
        let reassembled = reassemble_file(&chunked.manifest, &chunked.chunk_data);
        assert!(reassembled.is_some());
        assert_eq!(reassembled.unwrap(), data);
    }

    #[test]
    fn chunk_deterministic() {
        let data: Vec<u8> = (0..500_000u32).flat_map(|i| i.to_le_bytes()).collect();
        let c1 = chunk_file(&data);
        let c2 = chunk_file(&data);

        assert_eq!(c1.manifest.chunks.len(), c2.manifest.chunks.len());
        for (a, b) in c1.manifest.chunks.iter().zip(c2.manifest.chunks.iter()) {
            assert_eq!(a.hash, b.hash);
            assert_eq!(a.offset, b.offset);
            assert_eq!(a.size, b.size);
        }
    }

    #[test]
    fn small_edit_changes_few_chunks() {
        // Create a large file.
        let mut data: Vec<u8> = (0..2_000_000u32).flat_map(|i| i.to_le_bytes()).collect();
        let original = chunk_file(&data);

        // Modify a small region near the middle.
        for i in 1_000_000..1_000_100 {
            data[i] = 0xFF;
        }
        let modified = chunk_file(&data);

        // Most chunks should be identical.
        let original_hashes: std::collections::HashSet<_> =
            original.manifest.chunks.iter().map(|c| c.hash).collect();
        let modified_hashes: std::collections::HashSet<_> =
            modified.manifest.chunks.iter().map(|c| c.hash).collect();

        let shared = original_hashes.intersection(&modified_hashes).count();
        let changed = modified_hashes.len() - shared;

        assert!(
            changed <= 3,
            "expected at most 3 changed chunks, got {} changed out of {} total",
            changed,
            modified_hashes.len()
        );
    }
}
