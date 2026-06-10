import { ObsetyncApi } from "./api";
import { PlatformIO } from "./platform";
import { ObsetyncSyncBase } from "./sync-base";

/** Streaming Blake3 hasher — feed in 64 KB chunks, call finalize(), then free(). */
export interface WasmHasher {
    update(chunk: Uint8Array): void;
    finalize(): string;
    free(): void;
}

export interface WasmModule {
    wasm_hash(data: Uint8Array): string;
    wasm_should_chunk(size: number): boolean;
    wasm_chunk_file(data: Uint8Array): any;
    wasm_get_file_chunk(data: Uint8Array, offset: number, size: number): Uint8Array;
    wasm_tree_get_chunk(tree: any, hash: string): Uint8Array | null;
    wasm_tree_chunk_hashes(tree: any): string[];
    wasm_root_hash_from_bytes(bytes: Uint8Array): string | undefined;
    /** Streaming Blake3 hasher. Peak WASM heap = chunk size (64 KB), not file size. */
    Hasher: new () => WasmHasher;
    /**
     * Hash N files in one WASM call. data = concatenated bytes of all files.
     * offsets[i] = byte offset where file i starts. sizes[i] = byte length of file i.
     * Returns hex hashes, one per file. ONE WASM boundary crossing for the whole group.
     */
    wasm_hash_batch(data: Uint8Array, offsets: Uint32Array, sizes: Uint32Array): string[];
    WasmTree: {
        new (vaultId: string, deviceId: string): WasmTree;
    };
}

export interface WasmTree {
    load_root(rootBytes: Uint8Array): void;
    root_hash_hex(): string | null;
    root_bytes(): Uint8Array | null;
    total_files(): number;
    update_entry(path: string, hash: string, mtime: number, size: number): void;
    delete_entry(path: string): void;
    build_from_entries(entriesJson: string): void;
    /** Upsert N entries in ONE update_tree call. JSON: [{path,hash,mtime_ms,size},...] */
    update_batch(entriesJson: string): void;
    /** Delete N paths in ONE update_tree call. JSON: ["path/a.md","path/b.md",...] */
    delete_batch(pathsJson: string): void;
}

export interface FileChange {
    action: "created" | "modified" | "deleted";
    path: string;
    hash?: string;
    data?: Uint8Array; // only populated for single-file vault events
    mtime?: number;
    size?: number;
}

/**
 * Per-file state held while a push batch is in flight. A named class (vs an
 * object literal) so DevTools heap snapshots attribute these — and any
 * `largeData` blobs they retain — to obsetync: filter the Constructor column
 * by "Obsetync" and read Retained Size.
 */
class ObsetyncBatchFile {
    constructor(
        public change: FileChange,
        public size: number,
        public mtime: number,
        /** kept only for large files (needed for chunk upload) */
        public chunkInfo?: any,
        /** kept only for large files through upload */
        public largeData?: Uint8Array,
    ) {}
}

// Max files held in memory at once. Keeps RSS bounded even for 10k-file vaults.
const STREAM_BATCH = 50;
// Concurrent reads within a batch. 4 is the sweet spot: amortises IPC latency for
// small files without inflating peak WASM heap (8 concurrent 200 MB PDFs = 1.6 GB).
const READ_CONCURRENCY = 4;

/**
 * Push path — streams through changes in batches so peak memory is
 * STREAM_BATCH × avg_file_size, not total_vault_size.
 *
 * Per batch:
 *   A. Read + hash (parallel reads; wasm_hash_batch for small unknown files per group)
 *   B. Batch-check hashes against server (2 requests)
 *   C. Upload only what's missing; collect tree updates
 *   → data released at end of batch
 *
 * After all batches:
 *   D. tree.update_batch — ONE update_tree call for all N upserts → O(N+prefix)
 *   E. Upload index chunks + push root
 *
 * Root push and sync-base save happen once after all batches.
 */
export async function push(
    api: ObsetyncApi,
    io: PlatformIO,
    syncBase: ObsetyncSyncBase,
    wasm: WasmModule,
    tree: WasmTree,
    vaultId: string,
    changes: FileChange[],
    /** The root hash the server currently holds (from the last pull or push result).
     *  Used as X-Parent-Root so the server can fast-forward or merge correctly.
     *  Must be captured BEFORE tree updates — never the new root we're about to push. */
    serverRootHash: string | null,
    onProgress?: (msg: string) => void
): Promise<{ newRootHash: string | null; conflicts: any[] }> {
    if (changes.length === 0) {
        return { newRootHash: tree.root_hash_hex(), conflicts: [] };
    }

    const total = changes.length;
    console.log(`[obsetync] pushing ${total} changes`);

    // Bootstrap tree from sync-base on first push.
    // build_from_entries is O(n log n) in WASM — one call vs N×prefix with a loop.
    if (!tree.root_hash_hex()) {
        const paths = syncBase.allPaths();
        if (paths.length === 0) {
            tree.build_from_entries("[]");
        } else {
            const entries = paths.map(p => {
                const e = syncBase.getEntry(p)!;
                return { path: p, hash: e.hash, mtime_ms: e.mtime, size: e.size };
            });
            tree.build_from_entries(JSON.stringify(entries));
        }
    }

    const deleted    = changes.filter(c => c.action === "deleted");
    const nonDeleted = changes.filter(c => c.action !== "deleted");

    console.log(
        `[obsetync] push: ${changes.length} changes (${nonDeleted.length} upsert, ` +
        `${deleted.length} delete), tree bootstrapped=${!!tree.root_hash_hex()}, ` +
        `sync-base size=${syncBase.allPaths().length}`
    );

    // ONE delete_batch call for all deletions → O(N+prefix) not O(N×prefix).
    if (deleted.length > 0) {
        tree.delete_batch(JSON.stringify(deleted.map(c => c.path)));
        for (const change of deleted) syncBase.removeEntry(change.path);
    }

    let processed = deleted.length;
    let uploadedBytes = 0;
    const startTime = Date.now();
    // Collected here; applied in ONE update_batch call after all content batches.
    const allTreeUpdates: { path: string; hash: string; mtime_ms: number; size: number }[] = [];

    // Stream through non-deleted files in batches.
    for (let batchStart = 0; batchStart < nonDeleted.length; batchStart += STREAM_BATCH) {
        // Yield before each batch so Electron's audio/render callbacks can run.
        await yieldToUI();
        const batchChanges = nonDeleted.slice(batchStart, batchStart + STREAM_BATCH);

        // ------------------------------------------------------------------
        // A. Hash resolution — parallel reads, wasm_hash_batch per group.
        //
        // Small file with known hash + size: skip read entirely (lazy-read in C).
        //   For incremental syncs the server already has most content → zero reads.
        //
        // Large file (wasm_should_chunk): must read now for FastCDC.
        //   wasm_chunk_file computes the hash internally.
        //
        // Small file without known hash: read in parallel, then wasm_hash_batch
        //   for the whole sub-group — ONE WASM boundary crossing regardless of N.
        // ------------------------------------------------------------------
        const batchFiles: ObsetyncBatchFile[] = [];

        for (let i = 0; i < batchChanges.length; i += READ_CONCURRENCY) {
            const group = batchChanges.slice(i, i + READ_CONCURRENCY);

            // Partition: known-hash small files skip reading.
            const skipRead: FileChange[] = [];
            const needRead: FileChange[] = [];
            for (const c of group) {
                if (c.hash && c.size !== undefined && !wasm.wasm_should_chunk(c.size)) {
                    skipRead.push(c);
                } else {
                    needRead.push(c);
                }
            }

            for (const c of skipRead) {
                batchFiles.push(new ObsetyncBatchFile(c, c.size!, c.mtime ?? Date.now()));
            }

            if (needRead.length === 0) continue;

            // Read files in parallel.
            const reads = await Promise.all(needRead.map(async c => ({
                change: c,
                data: c.data ?? await io.readFile(c.path),
            })));

            // Large files — wasm_chunk_file hashes internally.
            for (const { change, data } of reads.filter(r => wasm.wasm_should_chunk(r.data.length))) {
                const chunkInfo = wasm.wasm_chunk_file(data);
                change.hash = chunkInfo.file_hash;
                batchFiles.push(new ObsetyncBatchFile(
                    change,
                    data.length,
                    change.mtime ?? Date.now(),
                    chunkInfo,
                    data,
                ));
            }

            // Small files — batch hash unknown-hash ones in ONE wasm_hash_batch call.
            const smallReads = reads.filter(r => !wasm.wasm_should_chunk(r.data.length));
            if (smallReads.length === 0) continue;

            // Known-hash small files that were forced to read (preloaded change.data).
            for (const { change, data } of smallReads.filter(r => r.change.hash)) {
                batchFiles.push(new ObsetyncBatchFile(change, data.length, change.mtime ?? Date.now()));
            }

            const unknownSmall = smallReads.filter(r => !r.change.hash);
            if (unknownSmall.length === 0) continue;

            // Concatenate all unknown-hash small files → ONE WASM boundary crossing.
            const totalBytes = unknownSmall.reduce((s, r) => s + r.data.length, 0);
            const flat    = new Uint8Array(totalBytes);
            const offsets = new Uint32Array(unknownSmall.length);
            const sizes   = new Uint32Array(unknownSmall.length);
            let off = 0;
            for (let j = 0; j < unknownSmall.length; j++) {
                flat.set(unknownSmall[j].data, off);
                offsets[j] = off;
                sizes[j]   = unknownSmall[j].data.length;
                off       += unknownSmall[j].data.length;
            }
            const hashes = wasm.wasm_hash_batch(flat, offsets, sizes);
            // flat, offsets, sizes go out of scope → GC-eligible.

            for (let j = 0; j < unknownSmall.length; j++) {
                unknownSmall[j].change.hash = hashes[j];
                batchFiles.push(new ObsetyncBatchFile(
                    unknownSmall[j].change,
                    unknownSmall[j].data.length,
                    unknownSmall[j].change.mtime ?? Date.now(),
                ));
            }
        }

        // ------------------------------------------------------------------
        // B. Two batch-check requests for this batch.
        // ------------------------------------------------------------------
        const smallHashes = batchFiles
            .filter(f => !f.chunkInfo)
            .map(f => f.change.hash!);

        const allChunkHashes: string[] = batchFiles
            .filter(f => f.chunkInfo)
            .flatMap(f => (f.chunkInfo.chunks as any[]).map((c: any) => c.hash));

        const [neededSmall, neededChunks] = await Promise.all([
            smallHashes.length > 0    ? api.checkContent(smallHashes)             : Promise.resolve([]),
            allChunkHashes.length > 0 ? api.checkContentChunks(allChunkHashes)    : Promise.resolve([]),
        ]);

        const neededSmallSet  = new Set(neededSmall);
        const neededChunksSet = new Set(neededChunks);

        // ------------------------------------------------------------------
        // C. Upload missing content. Collect tree updates.
        // ------------------------------------------------------------------
        for (const { change, size, mtime, chunkInfo, largeData } of batchFiles) {
            if (chunkInfo && largeData) {
                let fileBytesUploaded = 0;
                for (const chunk of chunkInfo.chunks as any[]) {
                    if (neededChunksSet.has(chunk.hash)) {
                        const chunkData = wasm.wasm_get_file_chunk(largeData, chunk.offset, chunk.size);
                        await api.putContentChunk(chunk.hash, chunkData);
                        fileBytesUploaded += chunkData.length;
                    }
                }
                await api.putManifest(change.hash!, {
                    file_hash:  change.hash!,
                    total_size: chunkInfo.total_size,
                    chunks:     chunkInfo.chunks,
                });
                uploadedBytes += fileBytesUploaded;
            } else if (change.hash && neededSmallSet.has(change.hash)) {
                // Re-read the file — only done for the small fraction the server needs.
                const data = change.data ?? (await io.readFile(change.path));
                await api.putContent(change.hash, data);
                uploadedBytes += data.length;
            }

            processed++;
            onProgress?.(`↑ ${processed}/${total} ${throughput(processed, uploadedBytes, startTime)}`);

            // Queue tree update — applied in ONE update_batch after all batches.
            allTreeUpdates.push({ path: change.path, hash: change.hash!, mtime_ms: mtime, size });
            // syncBase is fast in-memory — update per-file is fine.
            syncBase.setEntry(change.path, change.hash!, mtime, size);
        }

        // batchFiles goes out of scope — large file blobs are GC-eligible now.
    }

    // ------------------------------------------------------------------
    // D. Apply ALL tree updates in ONE update_tree call.
    //    O(N + prefix_size) vs O(N × prefix_size) with per-file update_entry.
    // ------------------------------------------------------------------
    const beforeRoot = tree.root_hash_hex();
    const beforeFiles = tree.total_files();
    if (allTreeUpdates.length > 0) {
        tree.update_batch(JSON.stringify(allTreeUpdates));
    }
    const afterRoot = tree.root_hash_hex();
    const afterFiles = tree.total_files();
    // Diagnostic: if batch > 0 but root didn't move, or file count didn't grow
    // by the expected delta, update_batch silently dropped entries and we want
    // to know NOW instead of watching devices mysteriously fail to sync.
    console.log(
        `[obsetync] tree update: files ${beforeFiles} → ${afterFiles}, ` +
        `root ${(beforeRoot ?? "(empty)").slice(0, 16)} → ${(afterRoot ?? "(empty)").slice(0, 16)}, ` +
        `batch=${allTreeUpdates.length} deletes=${deleted.length}`
    );
    if (allTreeUpdates.length > 0 && beforeRoot === afterRoot) {
        console.warn(
            `[obsetync] update_batch didn't move root despite ${allTreeUpdates.length} entries — ` +
            `first 3: ${JSON.stringify(allTreeUpdates.slice(0, 3))}`
        );
    }

    // Upload index chunks (LeafChunk, InternalNode) accumulated in MemoryChunkStore.
    // Server needs these to walk the tree during merge/diff.
    const chunkHashes = wasm.wasm_tree_chunk_hashes(tree);
    if (chunkHashes.length > 0) {
        onProgress?.(`↑ checking ${chunkHashes.length} index chunks...`);
        const neededChunks = await api.checkChunks(chunkHashes);
        if (neededChunks.length > 0) {
            onProgress?.(`↑ uploading ${neededChunks.length} index chunks...`);
            await Promise.all(neededChunks.map(hash => {
                const bytes = wasm.wasm_tree_get_chunk(tree, hash);
                return bytes ? api.putChunk(hash, bytes) : Promise.resolve();
            }));
        }
    }

    // Push root once after all batches.
    // parentHash = what the server had BEFORE this push (for fast-forward detection).
    const parentHash = serverRootHash ?? "";
    const rootBytes  = tree.root_bytes();
    if (!rootBytes) {
        console.warn(`[obsetync] push: tree.root_bytes() returned null — tree uninitialised?`);
        return { newRootHash: null, conflicts: [] };
    }
    console.log(
        `[obsetync] putRoot → parent=${parentHash ? parentHash.slice(0,16) : "(empty)"} ` +
        `new=${afterRoot?.slice(0,16)} bytes=${rootBytes.length}`
    );

    onProgress?.("↑ pushing root...");
    const result = await api.putRoot(vaultId, rootBytes, parentHash);

    syncBase.setLastSyncTimestamp(Date.now());
    await syncBase.save();

    return {
        newRootHash: result.root_hash,
        conflicts:   result.conflicts ?? [],
    };
}

/** Yield control back to the JS event loop so Obsidian stays responsive. */
const yieldToUI = () => new Promise<void>(r => setTimeout(r, 0));

/**
 * Hash file bytes via the streaming WASM Hasher — feeds data in 64 KB slices.
 * WASM linear memory grows to the largest single slice (64 KB), never the full
 * file size. Use this instead of wasm_hash() for anything > 64 KB.
 */
export function streamingHash(wasm: WasmModule, data: Uint8Array): string {
    const CHUNK = 65536;
    const hasher = new wasm.Hasher();
    try {
        for (let off = 0; off < data.length; off += CHUNK) {
            hasher.update(data.subarray(off, off + CHUNK));
        }
        return hasher.finalize();
    } finally {
        hasher.free();
    }
}

/**
 * Stream-hash a file directly from disk using Node.js fs (Electron/desktop only).
 * Reads in 64 KB chunks — peak memory per file ≈ 64 KB regardless of file size.
 * Falls back to readFile + streamingHash on mobile (no Node.js fs).
 *
 * This is the nproc-ready path: each Web Worker calls this independently,
 * giving true parallel hashing across cores with zero data crossing thread boundaries.
 */
export async function hashFileStreaming(
    path: string,
    io: PlatformIO,
    wasm: WasmModule,
): Promise<string> {
    const absPath = io.getAbsolutePath(path);
    if (absPath) {
        const fs = (globalThis as any).require?.('fs') as typeof import('fs') | undefined;
        if (fs?.createReadStream) {
            const hasher = new wasm.Hasher();
            try {
                await new Promise<void>((resolve, reject) => {
                    fs.createReadStream(absPath, { highWaterMark: 65536 })
                        .on('data', (chunk: Buffer | string) => {
                            const buf = chunk as Buffer;
                            hasher.update(new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength));
                        })
                        .on('end', resolve)
                        .on('error', reject);
                });
                return hasher.finalize();
            } finally {
                hasher.free();
            }
        }
    }
    // Mobile / no-fs fallback.
    const data = await io.readFile(path);
    return streamingHash(wasm, data);
}

function throughput(files: number, bytes: number, startMs: number): string {
    const secs = Math.max((Date.now() - startMs) / 1000, 0.1);
    const fps  = (files / secs).toFixed(1);
    const bps  = bytes / secs;
    let bpsFmt: string;
    if (bps >= 1_048_576)  bpsFmt = `${(bps / 1_048_576).toFixed(1)} MB/s`;
    else if (bps >= 1024)  bpsFmt = `${(bps / 1024).toFixed(0)} KB/s`;
    else if (bytes > 0)    bpsFmt = `${bps.toFixed(0)} B/s`;
    else                   bpsFmt = "";
    return bpsFmt ? `· ${fps} f/s · ${bpsFmt}` : `· ${fps} f/s`;
}
