use crate::content_store::{ChunkRef, FileManifest};
use crate::hash::{hash_bytes, FileHash};
use fastcdc::v2020::FastCDC;

/// Files below this size are stored as whole blobs — no chunking overhead.
pub const FILE_CHUNK_THRESHOLD: u64 = 1_048_576; // 1MB

/// Minimum sub-file chunk size.
const MIN_CHUNK: u32 = 256 * 1024; // 256KB

/// Average sub-file chunk size.
const AVG_CHUNK: u32 = 1024 * 1024; // 1MB

/// Maximum sub-file chunk size.
const MAX_CHUNK: u32 = 4 * 1024 * 1024; // 4MB

/// Incremental FastCDC planner. At most MAX_CHUNK + one caller feed remains
/// buffered; full-file hashing is incremental. Chunk bytes stay in the caller
/// (the plugin can slice its source buffer only for hashes the server needs).
pub struct StreamingChunker {
    pending: Vec<u8>,
    pending_start: usize,
    chunks: Vec<ChunkRef>,
    file_hasher: blake3::Hasher,
    total_size: u64,
    emitted_size: u64,
    finished: bool,
}

impl Default for StreamingChunker {
    fn default() -> Self {
        Self {
            pending: Vec::with_capacity(MAX_CHUNK as usize),
            pending_start: 0,
            chunks: Vec::new(),
            file_hasher: blake3::Hasher::new(),
            total_size: 0,
            emitted_size: 0,
            finished: false,
        }
    }
}

impl StreamingChunker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, bytes: &[u8]) -> Result<(), &'static str> {
        if self.finished {
            return Err("chunker already finished");
        }
        self.file_hasher.update(bytes);
        self.total_size = self
            .total_size
            .checked_add(bytes.len() as u64)
            .ok_or("file size overflow")?;
        self.pending.extend_from_slice(bytes);

        // Once MAX_CHUNK bytes are available, FastCDC's first boundary cannot
        // depend on future input: it either finds a cut earlier or cuts at the
        // configured maximum. Keep the remainder for the next feed.
        while self.pending.len() - self.pending_start >= MAX_CHUNK as usize {
            let first = FastCDC::new(
                &self.pending[self.pending_start..],
                MIN_CHUNK,
                AVG_CHUNK,
                MAX_CHUNK,
            )
            .next()
            .ok_or("FastCDC produced no chunk")?;
            self.emit_prefix(first.length);
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<FileManifest, &'static str> {
        if self.finished {
            return Err("chunker already finished");
        }
        while self.pending_start < self.pending.len() {
            let first = FastCDC::new(
                &self.pending[self.pending_start..],
                MIN_CHUNK,
                AVG_CHUNK,
                MAX_CHUNK,
            )
            .next()
            .ok_or("FastCDC produced no final chunk")?;
            self.emit_prefix(first.length);
        }
        self.pending.clear();
        self.pending_start = 0;
        self.finished = true;
        Ok(FileManifest {
            file_hash: *self.file_hasher.finalize().as_bytes(),
            total_size: self.total_size,
            chunks: std::mem::take(&mut self.chunks),
        })
    }

    fn emit_prefix(&mut self, length: usize) {
        let end = self.pending_start + length;
        let hash = hash_bytes(&self.pending[self.pending_start..end]);
        self.chunks.push(ChunkRef {
            hash,
            offset: self.emitted_size,
            size: length as u32,
        });
        self.emitted_size += length as u64;
        self.pending_start = end;
        // Avoid shifting the remainder on every chunk. Compact only after a
        // full maximum-chunk worth of consumed prefix has accumulated.
        if self.pending_start >= MAX_CHUNK as usize {
            self.pending.drain(..self.pending_start);
            self.pending_start = 0;
        }
    }
}

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
        // Use 8MB to get several chunks with the 1MB target size.
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
    fn streaming_chunker_matches_one_shot_across_feed_sizes() {
        let data: Vec<u8> = (0..1_750_003u32)
            .map(|i| ((i.wrapping_mul(31) ^ (i >> 7)) & 0xff) as u8)
            .collect();
        let expected = chunk_file(&data).manifest;

        for feed_size in [1, 65_537, 262_147] {
            let mut streaming = StreamingChunker::new();
            for part in data.chunks(feed_size) {
                streaming.update(part).unwrap();
            }
            let actual = streaming.finish().unwrap();
            assert_eq!(actual.file_hash, expected.file_hash);
            assert_eq!(actual.total_size, expected.total_size);
            assert_eq!(actual.chunks.len(), expected.chunks.len());
            for (actual, expected) in actual.chunks.iter().zip(&expected.chunks) {
                assert_eq!(actual.hash, expected.hash);
                assert_eq!(actual.offset, expected.offset);
                assert_eq!(actual.size, expected.size);
            }
        }
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
    fn empty_file_chunks_to_zero_chunks() {
        let chunked = chunk_file(&[]);
        assert!(chunked.manifest.chunks.is_empty());
        assert!(chunked.chunk_data.is_empty());
        assert_eq!(chunked.manifest.total_size, 0);
        assert_eq!(chunked.manifest.file_hash, hash_bytes(&[]));
    }

    #[test]
    fn empty_file_reassembles_to_empty() {
        let chunked = chunk_file(&[]);
        let out = reassemble_file(&chunked.manifest, &chunked.chunk_data);
        assert_eq!(out, Some(vec![]));
    }

    #[test]
    fn reassemble_rejects_chunk_count_mismatch() {
        let data: Vec<u8> = (0..500_000u32).flat_map(|i| i.to_le_bytes()).collect();
        let chunked = chunk_file(&data);
        // Drop one chunk — reassembly must refuse rather than silently truncate.
        let mut bad = chunked.chunk_data.clone();
        bad.pop();
        assert!(reassemble_file(&chunked.manifest, &bad).is_none());
    }

    #[test]
    fn reassemble_rejects_wrong_chunk_hash() {
        let data: Vec<u8> = (0..500_000u32).flat_map(|i| i.to_le_bytes()).collect();
        let chunked = chunk_file(&data);
        let mut bad = chunked.chunk_data.clone();
        // Replace the first chunk's hash with something that doesn't match its bytes.
        bad[0].0 = hash_bytes(b"definitely not the right hash");
        assert!(reassemble_file(&chunked.manifest, &bad).is_none());
    }

    #[test]
    fn reassemble_rejects_wrong_chunk_size() {
        let data: Vec<u8> = (0..500_000u32).flat_map(|i| i.to_le_bytes()).collect();
        let chunked = chunk_file(&data);
        let mut bad_manifest = chunked.manifest.clone();
        // Tamper with a recorded size — reassemble cross-checks chunk_data.len().
        bad_manifest.chunks[0].size += 1;
        assert!(reassemble_file(&bad_manifest, &chunked.chunk_data).is_none());
    }

    #[test]
    fn should_chunk_threshold_constant() {
        assert_eq!(FILE_CHUNK_THRESHOLD, 1_048_576);
        assert!(should_chunk(FILE_CHUNK_THRESHOLD));
        assert!(!should_chunk(FILE_CHUNK_THRESHOLD - 1));
        assert!(!should_chunk(0));
    }

    #[test]
    fn chunk_offsets_are_contiguous() {
        let data: Vec<u8> = (0..1_500_000u32).flat_map(|i| i.to_le_bytes()).collect();
        let chunked = chunk_file(&data);
        let mut expected_offset: u64 = 0;
        for (chunk, (_, bytes)) in chunked
            .manifest
            .chunks
            .iter()
            .zip(chunked.chunk_data.iter())
        {
            assert_eq!(chunk.offset, expected_offset);
            assert_eq!(chunk.size as usize, bytes.len());
            expected_offset += chunk.size as u64;
        }
        assert_eq!(expected_offset, chunked.manifest.total_size);
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
