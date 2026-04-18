#![cfg(feature = "wasm")]

use wasm_bindgen::prelude::*;
use serde::Serialize;
use crate::chunk::{FileEntry, RootNode};
use crate::hash::{hash_bytes, hash_to_hex, hex_to_hash};
use crate::store::MemoryChunkStore;

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
    root: Option<RootNode>,
    store: MemoryChunkStore,
    vault_id: String,
    device_id: String,
}

#[wasm_bindgen]
impl WasmTree {
    #[wasm_bindgen(constructor)]
    pub fn new(vault_id: &str, device_id: &str) -> Self {
        Self {
            root: None,
            store: MemoryChunkStore::new(),
            vault_id: vault_id.to_string(),
            device_id: device_id.to_string(),
        }
    }

    /// Load a root from serialized bytes (received from server or local cache).
    pub fn load_root(&mut self, root_bytes: &[u8]) -> Result<(), JsValue> {
        let root = RootNode::deserialize(root_bytes)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        // Store the root bytes in the memory store so tree operations can find it.
        let hash = root.hash();
        self.store.insert_chunk(hash, root_bytes.to_vec());

        self.root = Some(root);
        Ok(())
    }

    /// Get the current root hash as hex string.
    pub fn root_hash_hex(&self) -> Option<String> {
        self.root.as_ref().map(|r| hash_to_hex(&r.hash()))
    }

    /// Get the serialized root bytes for upload to server.
    pub fn root_bytes(&self) -> Option<Vec<u8>> {
        self.root.as_ref().map(|r| r.serialize())
    }

    /// Get total file count in the tree.
    pub fn total_files(&self) -> f64 {
        self.root.as_ref().map(|r| r.total_files as f64).unwrap_or(0.0)
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
            .map_err(|e| JsValue::from_str(&format!("invalid hash: {}", e)))?;

        let entry = FileEntry::new(path.to_string(), file_hash, mtime_ms as u64, size as u64);

        let root = self
            .root
            .as_ref()
            .ok_or_else(|| JsValue::from_str("no root loaded — call load_root or build_from_entries first"))?;

        // Run the async update_tree in a blocking context (WASM is single-threaded,
        // async is cooperative, MemoryChunkStore resolves immediately).
        let new_root = run_local(async {
            crate::tree::update_tree(&self.store, root, &[entry], &[]).await
        })
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

        self.root = Some(new_root);
        Ok(())
    }

    /// Delete a file entry from the tree.
    pub fn delete_entry(&mut self, path: &str) -> Result<(), JsValue> {
        let root = self
            .root
            .as_ref()
            .ok_or_else(|| JsValue::from_str("no root loaded"))?;

        let new_root = run_local(async {
            crate::tree::update_tree(&self.store, root, &[], &[path.to_string()]).await
        })
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

        self.root = Some(new_root);
        Ok(())
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
        let raw: Vec<RawEntry> = serde_json::from_str(entries_json)
            .map_err(|e| JsValue::from_str(&format!("invalid entries JSON: {}", e)))?;

        if raw.is_empty() { return Ok(()); }

        let entries: Result<Vec<FileEntry>, JsValue> = raw
            .into_iter()
            .map(|e| {
                let hash = hex_to_hash(&e.hash)
                    .map_err(|err| JsValue::from_str(&format!("bad hash for {}: {}", e.path, err)))?;
                Ok(FileEntry::new(e.path, hash, e.mtime_ms, e.size))
            })
            .collect();
        let entries = entries?;

        let root = self.root.as_ref()
            .ok_or_else(|| JsValue::from_str("no root — call build_from_entries first"))?;

        let new_root = run_local(async {
            crate::tree::update_tree(&self.store, root, &entries, &[]).await
        })
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

        self.root = Some(new_root);
        Ok(())
    }

    /// Delete a batch of paths in ONE update_tree call.
    /// Same O(N × prefix_size) → O(N + prefix_size) win as update_batch.
    ///
    /// Input: ["path/a.md", "path/b.md", ...]
    pub fn delete_batch(&mut self, paths_json: &str) -> Result<(), JsValue> {
        let paths: Vec<String> = serde_json::from_str(paths_json)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        if paths.is_empty() { return Ok(()); }

        let root = self.root.as_ref()
            .ok_or_else(|| JsValue::from_str("no root — call build_from_entries first"))?;

        let new_root = run_local(async {
            crate::tree::update_tree(&self.store, root, &[], &paths).await
        })
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

        self.root = Some(new_root);
        Ok(())
    }

    /// Build a tree from scratch given a JSON array of file entries.
    ///
    /// Input: [{ "path": "...", "hash": "hex", "mtime_ms": u64, "size": u64 }, ...]
    pub fn build_from_entries(&mut self, entries_json: &str) -> Result<(), JsValue> {
        let raw_entries: Vec<RawEntry> = serde_json::from_str(entries_json)
            .map_err(|e| JsValue::from_str(&format!("invalid entries JSON: {}", e)))?;

        let entries: Result<Vec<FileEntry>, JsValue> = raw_entries
            .into_iter()
            .map(|e| -> Result<FileEntry, JsValue> {
                let hash = hex_to_hash(&e.hash)
                    .map_err(|err| JsValue::from_str(&format!("bad hash for {}: {}", e.path, err)))?;
                Ok(FileEntry::new(e.path, hash, e.mtime_ms, e.size))
            })
            .collect();
        let entries = entries?;

        let root = run_local(async {
            crate::tree::build_tree(&self.store, entries, &self.vault_id, &self.device_id).await
        })
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

        self.root = Some(root);
        Ok(())
    }
}

/// Get the serialized bytes of a chunk from the WASM tree's internal store.
/// Called by the plugin to upload individual chunks to the server.
#[wasm_bindgen]
pub fn wasm_tree_get_chunk(tree: &WasmTree, hash_hex: &str) -> Option<Vec<u8>> {
    let hash = hex_to_hash(hash_hex).ok()?;
    tree.store.get_chunk(&hash)
}

/// Return hex hashes of all index chunks (LeafChunk, InternalNode, RootNode)
/// held in the WASM tree's in-memory store.
/// The plugin calls this before putRoot to upload any chunks the server is missing.
#[wasm_bindgen]
pub fn wasm_tree_chunk_hashes(tree: &WasmTree) -> Vec<String> {
    tree.store.all_chunk_hashes()
        .into_iter()
        .map(|hash| hash_to_hex(&hash))
        .collect()
}

/// Parse the root hash from cached root bytes without loading the tree structure.
/// Used on startup: we need the hash for X-Parent-Root but must NOT load the root
/// into the WASM tree — load_root only stores the root node itself, not its children
/// (LeafChunk/InternalNode), so update_entry would fail with "chunk not found" for
/// any directory prefix that already has entries. The tree always bootstraps fresh
/// from sync-base on first push, which correctly populates the full MemoryChunkStore.
#[wasm_bindgen]
pub fn wasm_root_hash_from_bytes(bytes: &[u8]) -> Option<String> {
    let root = RootNode::deserialize(bytes).ok()?;
    Some(hash_to_hex(&root.hash()))
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
            let end   = start + sz as usize;
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
        Self { inner: blake3::Hasher::new() }
    }

    /// Feed the next chunk of file bytes. Call repeatedly until all bytes fed.
    pub fn update(&mut self, chunk: &[u8]) {
        self.inner.update(chunk);
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

    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(val) => return val,
            Poll::Pending => {
                // MemoryChunkStore should never return Pending.
                panic!("WASM async operation returned Pending — this should not happen with MemoryChunkStore");
            }
        }
    }
}
