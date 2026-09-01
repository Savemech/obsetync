#![cfg(feature = "wasm")]

use crate::chunk::{FileEntry, RootNode};
use crate::hash::{hash_bytes, hash_to_hex, hex_to_hash};
use crate::transactional_tree::TransactionalTree;
use crate::transactional_tree_v2::TransactionalTreeV2;
use crate::versioned_root::{VersionedRoot, TREE_V1, TREE_V2};
use serde::Serialize;
use wasm_bindgen::prelude::*;

/// Serialize a Rust value to a plain JS object (not a JS Map).
/// serde-wasm-bindgen 0.4+ serializes maps as JS Map by default, which breaks
/// property access syntax (obj.field) — this forces plain objects instead.
fn to_js(value: &impl Serialize) -> JsValue {
    let ser = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
    value.serialize(&ser).unwrap_or(JsValue::NULL)
}

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}

/// Hash raw bytes using Blake3. Returns hex string.
/// This is the hot path — called per-file from Web Workers.
#[wasm_bindgen]
pub fn wasm_hash(data: &[u8]) -> String {
    hash_to_hex(&hash_bytes(data))
}

/// Holds the local Merkle tree state in WASM memory.
/// Used by the plugin's push path to incrementally update the tree.
#[wasm_bindgen]
pub struct WasmTree {
    inner: WasmTreeInner,
    vault_id: String,
    device_id: String,
}

enum WasmTreeInner {
    V1(TransactionalTree),
    V2(TransactionalTreeV2),
}

#[wasm_bindgen]
impl WasmTree {
    #[wasm_bindgen(constructor)]
    pub fn new(vault_id: &str, device_id: &str) -> Self {
        Self {
            inner: WasmTreeInner::V1(TransactionalTree::new(vault_id, device_id)),
            vault_id: vault_id.to_owned(),
            device_id: device_id.to_owned(),
        }
    }

    /// Select the empty tree's format after authenticated capability
    /// negotiation. Switching a populated graph is forbidden; callers rebuild
    /// it from sync-base so v1/v2 node bytes can never share one transaction.
    pub fn set_tree_version(&mut self, version: u32) -> Result<(), JsValue> {
        if self.root_hash_hex().is_some() || self.has_candidate() {
            return Err(JsValue::from_str(
                "cannot switch the format of a populated WASM tree",
            ));
        }
        self.inner = match version {
            TREE_V1 => WasmTreeInner::V1(TransactionalTree::new(&self.vault_id, &self.device_id)),
            TREE_V2 => WasmTreeInner::V2(TransactionalTreeV2::new(&self.vault_id, &self.device_id)),
            other => {
                return Err(JsValue::from_str(&format!(
                    "unsupported tree version {other}"
                )))
            }
        };
        Ok(())
    }

    pub fn tree_version(&self) -> u32 {
        match &self.inner {
            WasmTreeInner::V1(_) => TREE_V1,
            WasmTreeInner::V2(_) => TREE_V2,
        }
    }

    /// Load a root from serialized bytes (received from server or local cache).
    pub fn load_root(&mut self, root_bytes: &[u8]) -> Result<(), JsValue> {
        let root = VersionedRoot::deserialize(root_bytes)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        if root.vault_id() != self.vault_id {
            return Err(JsValue::from_str(
                "root vault id does not match the WASM tree",
            ));
        }

        // Root bytes live separately from the byte-addressed leaf/internal
        // store: RootNode::hash() is semantic and intentionally excludes its
        // history metadata, so it is not a Blake3 address for root_bytes.
        self.inner = match root {
            VersionedRoot::V1(root) => {
                let mut tree = TransactionalTree::new(&self.vault_id, &self.device_id);
                tree.load_root_without_chunks(root);
                WasmTreeInner::V1(tree)
            }
            VersionedRoot::V2(root) => {
                let mut tree = TransactionalTreeV2::new(&self.vault_id, &self.device_id);
                tree.load_root_without_chunks(root);
                WasmTreeInner::V2(tree)
            }
        };
        Ok(())
    }

    /// Get the current root hash as hex string.
    pub fn root_hash_hex(&self) -> Option<String> {
        match &self.inner {
            WasmTreeInner::V1(tree) => tree.committed_root_hash(),
            WasmTreeInner::V2(tree) => tree.committed_root_hash(),
        }
        .map(|hash| hash_to_hex(&hash))
    }

    /// Get the serialized root bytes for upload to server.
    pub fn root_bytes(&self) -> Result<Option<Vec<u8>>, JsValue> {
        match &self.inner {
            WasmTreeInner::V1(tree) => Ok(tree.committed_root().map(RootNode::serialize)),
            WasmTreeInner::V2(tree) => tree
                .committed_root()
                .map(|root| root.serialize())
                .transpose()
                .map_err(|error| JsValue::from_str(&error.to_string())),
        }
    }

    /// Get total file count in the tree.
    pub fn total_files(&self) -> f64 {
        match &self.inner {
            WasmTreeInner::V1(tree) => tree.committed_root().map(|root| root.total_files as f64),
            WasmTreeInner::V2(tree) => tree.committed_root().map(|root| root.total_files() as f64),
        }
        .unwrap_or(0.0)
    }

    /// Begin an isolated candidate rooted at the committed graph.
    pub fn begin_candidate(&mut self) -> Result<(), JsValue> {
        match &mut self.inner {
            WasmTreeInner::V1(tree) => tree.begin_candidate(),
            WasmTreeInner::V2(tree) => run_local(tree.begin_candidate()),
        }
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    pub fn has_candidate(&self) -> bool {
        match &self.inner {
            WasmTreeInner::V1(tree) => tree.has_candidate(),
            WasmTreeInner::V2(tree) => tree.has_candidate(),
        }
    }

    pub fn candidate_root_hash_hex(&self) -> Option<String> {
        match &self.inner {
            WasmTreeInner::V1(tree) => tree.candidate_root_hash(),
            WasmTreeInner::V2(tree) => tree.candidate_root_hash(),
        }
        .map(|hash| hash_to_hex(&hash))
    }

    pub fn candidate_root_bytes(&self) -> Result<Option<Vec<u8>>, JsValue> {
        match &self.inner {
            WasmTreeInner::V1(tree) => Ok(tree.candidate_root().map(RootNode::serialize)),
            WasmTreeInner::V2(tree) => tree
                .candidate_root()
                .map(|root| root.serialize())
                .transpose()
                .map_err(|error| JsValue::from_str(&error.to_string())),
        }
    }

    pub fn candidate_total_files(&self) -> f64 {
        match &self.inner {
            WasmTreeInner::V1(tree) => tree.candidate_root().map(|root| root.total_files as f64),
            WasmTreeInner::V2(tree) => tree.candidate_root().map(|root| root.total_files() as f64),
        }
        .unwrap_or(0.0)
    }

    /// Promote the candidate only after the server has accepted its root.
    pub fn commit_candidate(&mut self) -> Result<JsValue, JsValue> {
        match &mut self.inner {
            WasmTreeInner::V1(tree) => tree.commit_candidate(),
            WasmTreeInner::V2(tree) => run_local(tree.commit_candidate()),
        }
        .map(|stats| to_js(&stats))
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Discard a failed candidate and retain only the committed graph.
    pub fn abort_candidate(&mut self) -> Result<JsValue, JsValue> {
        match &mut self.inner {
            WasmTreeInner::V1(tree) => tree.abort_candidate(),
            WasmTreeInner::V2(tree) => run_local(tree.abort_candidate()),
        }
        .map(|stats| to_js(&stats))
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Update a single file entry in the tree.
    pub fn update_entry(
        &mut self,
        path: &str,
        hash_hex: &str,
        mtime_ms: f64,
        size: f64,
    ) -> Result<(), JsValue> {
        let file_hash = hex_to_hash(hash_hex)
            .map_err(|error| JsValue::from_str(&format!("invalid hash: {error}")))?;
        let entry = FileEntry::new(path.to_owned(), file_hash, mtime_ms as u64, size as u64);
        match &mut self.inner {
            WasmTreeInner::V1(tree) => run_local(tree.apply_committed(&[entry], &[])),
            WasmTreeInner::V2(tree) => run_local(tree.apply_committed(&[entry], &[])),
        }
        .map(|_| ())
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Delete a file entry from the tree.
    pub fn delete_entry(&mut self, path: &str) -> Result<(), JsValue> {
        match &mut self.inner {
            WasmTreeInner::V1(tree) => run_local(tree.apply_committed(&[], &[path.to_owned()])),
            WasmTreeInner::V2(tree) => run_local(tree.apply_committed(&[], &[path.to_owned()])),
        }
        .map(|_| ())
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Apply a batch of upserts in ONE update_tree call.
    ///
    /// Calling update_entry N times for N files in the same directory causes N
    /// separate update_tree invocations, each reloading + rebuilding the same
    /// leaf chunk: O(N × prefix_size). This method passes all N entries at once
    /// so update_tree groups them by prefix and rebuilds each prefix only once:
    /// O(N + prefix_size).
    ///
    /// Input: [{ "path": "...", "hash": "hex", "mtime_ms": u64, "size": u64 }, ...]
    pub fn update_batch(&mut self, entries_json: &str) -> Result<(), JsValue> {
        let entries = parse_entries(entries_json)?;
        if entries.is_empty() {
            return Ok(());
        }
        match &mut self.inner {
            WasmTreeInner::V1(tree) => run_local(tree.apply_committed(&entries, &[])),
            WasmTreeInner::V2(tree) => run_local(tree.apply_committed(&entries, &[])),
        }
        .map(|_| ())
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Delete a batch of paths in ONE update_tree call.
    /// Same O(N × prefix_size) → O(N + prefix_size) win as update_batch.
    ///
    /// Input: ["path/a.md", "path/b.md", ...]
    pub fn delete_batch(&mut self, paths_json: &str) -> Result<(), JsValue> {
        let paths = parse_paths(paths_json)?;

        if paths.is_empty() {
            return Ok(());
        }

        match &mut self.inner {
            WasmTreeInner::V1(tree) => run_local(tree.apply_committed(&[], &paths)),
            WasmTreeInner::V2(tree) => run_local(tree.apply_committed(&[], &paths)),
        }
        .map(|_| ())
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    pub fn candidate_update_batch(&mut self, entries_json: &str) -> Result<(), JsValue> {
        let entries = parse_entries(entries_json)?;
        match &mut self.inner {
            WasmTreeInner::V1(tree) => run_local(tree.apply_candidate(&entries, &[])),
            WasmTreeInner::V2(tree) => run_local(tree.apply_candidate(&entries, &[])),
        }
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    pub fn candidate_delete_batch(&mut self, paths_json: &str) -> Result<(), JsValue> {
        let paths = parse_paths(paths_json)?;
        match &mut self.inner {
            WasmTreeInner::V1(tree) => run_local(tree.apply_candidate(&[], &paths)),
            WasmTreeInner::V2(tree) => run_local(tree.apply_candidate(&[], &paths)),
        }
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Build a tree from scratch given a JSON array of file entries.
    ///
    /// Input: [{ "path": "...", "hash": "hex", "mtime_ms": u64, "size": u64 }, ...]
    pub fn build_from_entries(&mut self, entries_json: &str) -> Result<(), JsValue> {
        let entries = parse_entries(entries_json)?;
        match &mut self.inner {
            WasmTreeInner::V1(tree) => run_local(tree.rebuild(entries)),
            WasmTreeInner::V2(tree) => run_local(tree.rebuild(entries)),
        }
        .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Atomically rebuild the semantic sync-base snapshot in a negotiated
    /// tree format. Unlike `set_tree_version`, this is deliberately valid for
    /// a populated tree: a live server-side v1/v2 transition must replace the
    /// whole immutable graph without exposing an empty or half-built state.
    pub fn rebuild_from_entries_in_version(
        &mut self,
        version: u32,
        entries_json: &str,
    ) -> Result<(), JsValue> {
        let entries = parse_entries(entries_json)?;
        let mut replacement = match version {
            TREE_V1 => WasmTreeInner::V1(TransactionalTree::new(&self.vault_id, &self.device_id)),
            TREE_V2 => WasmTreeInner::V2(TransactionalTreeV2::new(&self.vault_id, &self.device_id)),
            other => {
                return Err(JsValue::from_str(&format!(
                    "unsupported tree version {other}"
                )))
            }
        };
        match &mut replacement {
            WasmTreeInner::V1(tree) => run_local(tree.rebuild(entries)),
            WasmTreeInner::V2(tree) => run_local(tree.rebuild(entries)),
        }
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
        self.inner = replacement;
        Ok(())
    }
}

/// Get the serialized bytes of a chunk from the WASM tree's internal store.
/// Called by the plugin to upload individual chunks to the server.
#[wasm_bindgen]
pub fn wasm_tree_get_chunk(tree: &WasmTree, hash_hex: &str) -> Option<Vec<u8>> {
    let hash = hex_to_hash(hash_hex).ok()?;
    match &tree.inner {
        WasmTreeInner::V1(inner) => inner.chunk_bytes(&hash),
        WasmTreeInner::V2(inner) => inner.chunk_bytes(&hash),
    }
}

/// Return hex hashes of all byte-addressed index chunks (LeafChunk/InternalNode)
/// held in the WASM tree's in-memory store.
/// The plugin calls this before putRoot to upload any chunks the server is missing.
#[wasm_bindgen]
pub fn wasm_tree_chunk_hashes(tree: &WasmTree) -> Result<Vec<String>, JsValue> {
    if tree.has_candidate() {
        wasm_tree_candidate_chunk_hashes(tree)
    } else {
        wasm_tree_committed_chunk_hashes(tree)
    }
}

#[wasm_bindgen]
pub fn wasm_tree_committed_chunk_hashes(tree: &WasmTree) -> Result<Vec<String>, JsValue> {
    match &tree.inner {
        WasmTreeInner::V1(inner) => inner.committed_chunk_hashes(),
        WasmTreeInner::V2(inner) => run_local(inner.committed_chunk_hashes()),
    }
    .map(hex_hashes)
    .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[wasm_bindgen]
pub fn wasm_tree_candidate_chunk_hashes(tree: &WasmTree) -> Result<Vec<String>, JsValue> {
    match &tree.inner {
        WasmTreeInner::V1(inner) => inner.candidate_chunk_hashes(),
        WasmTreeInner::V2(inner) => run_local(inner.candidate_chunk_hashes()),
    }
    .map(hex_hashes)
    .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[wasm_bindgen]
pub fn wasm_tree_new_candidate_chunk_hashes(tree: &WasmTree) -> Result<Vec<String>, JsValue> {
    match &tree.inner {
        WasmTreeInner::V1(inner) => inner.new_candidate_chunk_hashes(),
        WasmTreeInner::V2(inner) => run_local(inner.new_candidate_chunk_hashes()),
    }
    .map(hex_hashes)
    .map_err(|error| JsValue::from_str(&error.to_string()))
}

/// Parse the root hash from cached root bytes without loading the tree structure.
/// Used on startup: we need the hash for X-Parent-Root but must NOT load the root
/// into the WASM tree — load_root only stores the root node itself, not its children
/// (LeafChunk/InternalNode), so update_entry would fail with "chunk not found" for
/// any directory prefix that already has entries. The tree always bootstraps fresh
/// from sync-base on first push, which correctly populates the full MemoryChunkStore.
#[wasm_bindgen]
pub fn wasm_root_hash_from_bytes(bytes: &[u8]) -> Option<String> {
    let root = VersionedRoot::deserialize(bytes).ok()?;
    Some(hash_to_hex(&root.hash()))
}

#[wasm_bindgen]
pub fn wasm_root_version_from_bytes(bytes: &[u8]) -> Option<u32> {
    Some(VersionedRoot::deserialize(bytes).ok()?.version())
}

/// Run FastCDC sub-file chunking on a large file.
/// Returns JSON: { "file_hash": "hex", "total_size": u64, "chunks": [{ "hash": "hex", "offset": u64, "size": u32 }] }
#[wasm_bindgen]
pub fn wasm_chunk_file(data: &[u8]) -> JsValue {
    let chunked = crate::fastcdc_chunker::chunk_file(data);
    let result = serde_json::json!({
        "file_hash": hash_to_hex(&chunked.manifest.file_hash),
        "total_size": chunked.manifest.total_size,
        "chunks": chunked.manifest.chunks.iter().map(|c| {
            serde_json::json!({
                "hash": hash_to_hex(&c.hash),
                "offset": c.offset,
                "size": c.size,
            })
        }).collect::<Vec<_>>(),
    });
    to_js(&result)
}

/// Streaming FastCDC planner for memory-constrained clients. Feed small JS
/// views with update(); WASM retains a bounded ~4 MiB window and finish() returns only
/// the manifest, avoiding a second whole-file copy in WASM linear memory.
#[wasm_bindgen]
pub struct WasmChunker {
    inner: crate::fastcdc_chunker::StreamingChunker,
}

#[wasm_bindgen]
impl WasmChunker {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: crate::fastcdc_chunker::StreamingChunker::new(),
        }
    }

    pub fn update(&mut self, bytes: &[u8]) -> Result<(), JsValue> {
        self.inner.update(bytes).map_err(JsValue::from_str)
    }

    pub fn finish(&mut self) -> Result<JsValue, JsValue> {
        let manifest = self.inner.finish().map_err(JsValue::from_str)?;
        let result = serde_json::json!({
            "file_hash": hash_to_hex(&manifest.file_hash),
            "total_size": manifest.total_size,
            "chunks": manifest.chunks.iter().map(|chunk| {
                serde_json::json!({
                    "hash": hash_to_hex(&chunk.hash),
                    "offset": chunk.offset,
                    "size": chunk.size,
                })
            }).collect::<Vec<_>>(),
        });
        Ok(to_js(&result))
    }
}

/// Get a specific sub-file chunk's bytes after calling wasm_chunk_file.
/// This avoids sending all chunk data over the WASM bridge at once.
#[wasm_bindgen]
pub fn wasm_get_file_chunk(data: &[u8], offset: u32, size: u32) -> Vec<u8> {
    let start = offset as usize;
    let end = start + size as usize;
    if end <= data.len() {
        data[start..end].to_vec()
    } else {
        vec![]
    }
}

/// Check if a file should use sub-file chunking based on size.
#[wasm_bindgen]
pub fn wasm_should_chunk(size: u32) -> bool {
    crate::fastcdc_chunker::should_chunk(size as u64)
}

/// Hash N files in ONE WASM call — no JS re-entry between files.
///
/// The cost of crossing the JS↔WASM boundary is paid once regardless of N.
/// Rust iterates the file slices internally, keeping the hot path in native code.
///
/// data    — concatenated bytes of all files back-to-back
/// offsets — byte offset in `data` where each file starts (Uint32Array on JS side)
/// sizes   — byte length of each file (Uint32Array on JS side)
///
/// Returns a JS Array of hex strings, one per file, same order as offsets/sizes.
#[wasm_bindgen]
pub fn wasm_hash_batch(data: &[u8], offsets: &[u32], sizes: &[u32]) -> Vec<String> {
    offsets
        .iter()
        .zip(sizes.iter())
        .map(|(&off, &sz)| {
            let start = off as usize;
            let end = start + sz as usize;
            let slice = data.get(start..end).unwrap_or(&[]);
            hash_to_hex(&hash_bytes(slice))
        })
        .collect()
}

/// Streaming Blake3 hasher — feed the file in 64 KB chunks.
///
/// WASM linear memory grows to the largest single `&[u8]` slice it receives and
/// never shrinks back. Calling `wasm_hash(entireFile)` on a 500 MB PDF grows the
/// WASM heap to 500 MB for the entire session. Using this Hasher with 64 KB
/// chunks keeps the WASM heap bounded to ~64 KB per file.
///
/// Usage (TypeScript):
///   const h = new wasm.Hasher();
///   for each chunk: h.update(chunk);   // chunk ≤ 64 KB
///   const hex = h.finalize();
///   h.free();
#[wasm_bindgen]
pub struct Hasher {
    inner: blake3::Hasher,
}

#[wasm_bindgen]
impl Hasher {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: blake3::Hasher::new(),
        }
    }

    /// Feed the next chunk of file bytes. Call repeatedly until all bytes fed.
    pub fn update(&mut self, chunk: &[u8]) {
        self.inner.update(chunk);
    }

    /// Feed bytes into the running whole-file hash and return this slice's
    /// independent Blake3 address in the same JS→WASM copy. Pull uses this to
    /// validate a manifest chunk and the ordered full stream simultaneously.
    pub fn update_and_hash(&mut self, chunk: &[u8]) -> String {
        self.inner.update(chunk);
        hash_to_hex(&hash_bytes(chunk))
    }

    /// Return the final Blake3 hex hash. Non-consuming — safe to call once.
    pub fn finalize(&self) -> String {
        hash_to_hex(self.inner.finalize().as_bytes())
    }
}

// --- Internal helpers ---

#[derive(serde::Deserialize)]
struct RawEntry {
    path: String,
    hash: String,
    mtime_ms: u64,
    size: u64,
}

fn parse_entries(entries_json: &str) -> Result<Vec<FileEntry>, JsValue> {
    let raw: Vec<RawEntry> = serde_json::from_str(entries_json)
        .map_err(|error| JsValue::from_str(&format!("invalid entries JSON: {error}")))?;
    raw.into_iter()
        .map(|entry| {
            let hash = hex_to_hash(&entry.hash).map_err(|error| {
                JsValue::from_str(&format!("bad hash for {}: {error}", entry.path))
            })?;
            Ok(FileEntry::new(entry.path, hash, entry.mtime_ms, entry.size))
        })
        .collect()
}

fn parse_paths(paths_json: &str) -> Result<Vec<String>, JsValue> {
    serde_json::from_str(paths_json)
        .map_err(|error| JsValue::from_str(&format!("invalid paths JSON: {error}")))
}

fn hex_hashes(hashes: Vec<crate::hash::FileHash>) -> Vec<String> {
    hashes.into_iter().map(|hash| hash_to_hex(&hash)).collect()
}

/// Run a !Send future synchronously. Works in WASM because WASM is single-threaded
/// and MemoryChunkStore operations resolve immediately (no actual I/O).
fn run_local<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    // In WASM, we can poll the future to completion since all I/O is synchronous
    // (MemoryChunkStore has no real async). Use a simple executor.
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    fn dummy_raw_waker() -> RawWaker {
        fn no_op(_: *const ()) {}
        fn clone(data: *const ()) -> RawWaker {
            RawWaker::new(data, &VTABLE)
        }
        const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
        RawWaker::new(std::ptr::null(), &VTABLE)
    }

    let waker = unsafe { Waker::from_raw(dummy_raw_waker()) };
    let mut cx = Context::from_waker(&waker);
    let mut future = Box::pin(future);

    match future.as_mut().poll(&mut cx) {
        Poll::Ready(value) => value,
        Poll::Pending => {
            // MemoryChunkStore should never return Pending.
            panic!("WASM async operation returned Pending — this should not happen with MemoryChunkStore");
        }
    }
}
