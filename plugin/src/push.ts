import type { ObsetyncApi } from "./api";
import {
    BulkObjectKind,
    type BulkUploadRecord,
} from "./bulk-codec";
import { PlatformIO } from "./platform";
import { ObsetyncSyncBase } from "./sync-base";
import type { PerfOperation } from "./perf-trace";
import {
    getHashTuning,
    observeHashFeedback,
    planByteBoundedBatches,
    type HashTuning,
} from "./hash-runtime";
import {
    HashWorkerFileDriftError,
    HashWorkerPoolError,
    type DesktopHashWorkerPool,
} from "./desktop-hash-workers";
import {
    uploadDesktopMissingRanges,
    type DesktopRangeSource,
} from "./desktop-ranged-upload";

/** Streaming Blake3 hasher — feed bounded slices, call finalize(), then free(). */
export interface WasmHasher {
    update(chunk: Uint8Array): void;
    update_and_hash(chunk: Uint8Array): string;
    finalize(): string;
    free(): void;
}

export interface WasmChunker {
    update(chunk: Uint8Array): void;
    finish(): any;
    free(): void;
}

export interface WasmModule {
    wasm_hash(data: Uint8Array): string;
    wasm_should_chunk(size: number): boolean;
    wasm_chunk_file(data: Uint8Array): any;
    wasm_get_file_chunk(data: Uint8Array, offset: number, size: number): Uint8Array;
    wasm_tree_get_chunk(tree: any, hash: string): Uint8Array | null;
    wasm_tree_chunk_hashes(tree: any): string[];
    wasm_tree_committed_chunk_hashes(tree: any): string[];
    wasm_tree_candidate_chunk_hashes(tree: any): string[];
    wasm_tree_new_candidate_chunk_hashes(tree: any): string[];
    wasm_root_hash_from_bytes(bytes: Uint8Array): string | undefined;
    wasm_root_version_from_bytes(bytes: Uint8Array): number | undefined;
    /** Streaming Blake3 hasher. Peak WASM heap = feed size, not file size. */
    Hasher: new () => WasmHasher;
    WasmChunker: new () => WasmChunker;
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
    set_tree_version(version: number): void;
    tree_version(): number;
    rebuild_from_entries_in_version(version: number, entriesJson: string): void;
    load_root(rootBytes: Uint8Array): void;
    root_hash_hex(): string | null;
    root_bytes(): Uint8Array | null;
    total_files(): number;
    begin_candidate(): void;
    has_candidate(): boolean;
    candidate_root_hash_hex(): string | null;
    candidate_root_bytes(): Uint8Array | null;
    candidate_total_files(): number;
    candidate_update_batch(entriesJson: string): void;
    candidate_delete_batch(pathsJson: string): void;
    commit_candidate(): WasmTreeGcStats;
    abort_candidate(): WasmTreeGcStats;
    update_entry(path: string, hash: string, mtime: number, size: number): void;
    delete_entry(path: string): void;
    build_from_entries(entriesJson: string): void;
    /** Upsert N entries in ONE update_tree call. JSON: [{path,hash,mtime_ms,size},...] */
    update_batch(entriesJson: string): void;
    /** Delete N paths in ONE update_tree call. JSON: ["path/a.md","path/b.md",...] */
    delete_batch(pathsJson: string): void;
}

export interface WasmTreeGcStats {
    before: number;
    reachable: number;
    removed: number;
    after: number;
    bytes_removed: number;
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
        /** exact pass-1 source identity for desktop ranged pass 2 */
        public rangedSource?: DesktopRangeSource,
    ) {}
}

interface ObsetyncRead {
    change: FileChange;
    data: Uint8Array;
}

function hashUnknownSmallReads(
    wasm: WasmModule,
    reads: ObsetyncRead[],
    tuning: HashTuning,
    perf?: PerfOperation,
): ObsetyncBatchFile[] {
    const result: ObsetyncBatchFile[] = [];
    const batches = planByteBoundedBatches(reads, (read) => read.data.length, {
        maxFiles: tuning.maxBatchFiles,
        maxBytes: tuning.maxBatchBytes,
        maxSingleBytes: tuning.maxSingleBatchFileBytes,
        maxHoldMs: tuning.maxBatchHoldMs,
    });
    const residentBytes = reads.reduce((sum, read) => sum + read.data.length, 0);

    for (const batch of batches) {
        const totalBytes = batch.reduce((sum, read) => sum + read.data.length, 0);
        perf?.observePeakBatchBytes(residentBytes + totalBytes);
        const flat = new Uint8Array(totalBytes);
        const offsets = new Uint32Array(batch.length);
        const sizes = new Uint32Array(batch.length);
        let offset = 0;
        for (let index = 0; index < batch.length; index++) {
            flat.set(batch[index].data, offset);
            offsets[index] = offset;
            sizes[index] = batch[index].data.length;
            offset += batch[index].data.length;
        }
        const endHash = perf?.phase("hash");
        const hashStarted = monotonicNow();
        let hashes: string[];
        try {
            hashes = wasm.wasm_hash_batch(flat, offsets, sizes);
        } finally {
            observeHashStep(totalBytes, hashStarted);
            endHash?.();
        }
        for (let index = 0; index < batch.length; index++) {
            const read = batch[index];
            read.change.hash = hashes[index];
            result.push(new ObsetyncBatchFile(
                read.change,
                read.data.length,
                read.change.mtime ?? Date.now(),
            ));
        }
    }
    return result;
}

/**
 * Push path — streams through byte- and count-bounded batches so peak memory
 * follows the platform budget, not total vault size.
 *
 * Per batch:
 *   A. Read + hash (parallel reads; wasm_hash_batch for small unknown files per group)
 *   B. Batch-check hashes against server (2 requests)
 *   C. Upload only what's missing; collect tree updates
 *   → data released at end of batch
 *
 * After all batches:
 *   D. tree.candidate_update_batch — one update_tree call for all N upserts
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
    /** The server root this device's tree state was DERIVED from — i.e. the
     *  last root the tree verifiably reconciled with (engine.treeBaseRoot),
     *  NOT merely the last root observed on the server. The server uses it
     *  as the merge base, so lying here (e.g. sending a freshly-polled
     *  current root while the tree is stale) turns a divergence into a
     *  "fast-forward" that reverts the whole vault — incident 2026-07-13. */
    baseRootHash: string | null,
    onProgress?: (msg: string) => void,
    perf?: PerfOperation,
    hashWorkers?: DesktopHashWorkerPool | null,
    beforeHeavyBatch?: () => Promise<void>,
): Promise<{ newRootHash: string | null; conflicts: any[] }> {
    perf?.setWorkload({ filesTotal: changes.length });
    if (changes.length === 0) {
        perf?.setWorkload({
            bytesTotal: 0,
            filesNeeded: 0,
            bytesNeeded: 0,
        });
        return { newRootHash: tree.root_hash_hex(), conflicts: [] };
    }

    // Fail terminal enrollment/transport mismatches before hashing thousands
    // of files or touching the candidate tree. Authentication is still
    // checked by the first real request; this resolves the local v2 channel.
    const endPreflight = perf?.phase("check");
    try {
        await api.ensureTransportReady(perf);
    } finally {
        endPreflight?.();
    }

    const total = changes.length;
    console.log(`[obsetync] pushing ${total} changes`);

    // Bootstrap is the only push path allowed to enumerate the complete
    // sync-base. Every normal incremental push starts from a cheap committed
    // root pointer and never retains O(vault_size) JS rollback objects.
    let bootstrapped = false;
    if (!tree.root_hash_hex()) {
        const endEnumerate = perf?.phase("enumerate");
        let baseEntries: Array<{ path: string; hash: string; mtime_ms: number; size: number }>;
        try {
            baseEntries = syncBase.allPaths().map((path) => {
                const entry = syncBase.getEntry(path)!;
                return {
                    path,
                    hash: entry.hash,
                    mtime_ms: syncBase.getTreeMtime(path) ?? entry.mtime,
                    size: entry.size,
                };
            });
        } finally {
            endEnumerate?.();
        }
        const endTree = perf?.phase("tree_update");
        try {
            tree.build_from_entries(JSON.stringify(baseEntries));
            bootstrapped = true;
        } finally {
            endTree?.();
        }
    }

    const committedChunks = wasm.wasm_tree_committed_chunk_hashes(tree);
    perf?.setWasmChunks({ before: committedChunks.length });
    tree.begin_candidate();
    let candidateOpen = true;

    try {

    const deleted    = changes.filter(c => c.action === "deleted");
    const nonDeleted = changes.filter(c => c.action !== "deleted");

    console.log(
        `[obsetync] push: ${changes.length} changes (${nonDeleted.length} upsert, ` +
        `${deleted.length} delete), bootstrap=${bootstrapped}, ` +
        `sync-base size=${syncBase.entryCount()}`
    );

    // One candidate delete call for all deletions: O(N+prefix), not O(N×prefix).
    if (deleted.length > 0) {
        const endTree = perf?.phase("tree_update");
        try {
            tree.candidate_delete_batch(JSON.stringify(deleted.map(c => c.path)));
        } finally {
            endTree?.();
        }
    }

    let processed = deleted.length;
    perf?.increment({ filesCompleted: deleted.length });
    let uploadedBytes = 0;
    let neededFiles = 0;
    let neededBytes = 0;
    const startTime = Date.now();
    // Collected here; applied in ONE update_batch call after all content batches.
    const allTreeUpdates: { path: string; hash: string; mtime_ms: number; size: number }[] = [];

    // Stream through byte- and count-bounded batches. Large or size-unknown
    // files are singletons, while small files may share one WASM hash call
    // without allowing retained JS buffers to exceed the platform budget.
    const tuning = getHashTuning();
    let workerFallbackWarned = false;
    const streamBatches = planByteBoundedBatches(nonDeleted, (change) => {
        if (change.size === undefined || wasm.wasm_should_chunk(change.size)) {
            return tuning.maxSingleBatchFileBytes + 1;
        }
        return change.size;
    }, {
        maxFiles: tuning.maxBatchFiles,
        maxBytes: tuning.maxBatchBytes,
        maxSingleBytes: tuning.maxSingleBatchFileBytes,
        maxHoldMs: tuning.maxBatchHoldMs,
    });
    for (const batchChanges of streamBatches) {
        await beforeHeavyBatch?.();
        // Yield before each batch so Electron's audio/render callbacks can run.
        await yieldToUI();

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
        const unknownSmallReads: ObsetyncRead[] = [];

        for (let i = 0; i < batchChanges.length; i += tuning.readConcurrency) {
            const group = batchChanges.slice(i, i + tuning.readConcurrency);

            // Partition: known-hash small files skip reading.
            const skipRead: FileChange[] = [];
            const needRead: FileChange[] = [];
            const workerEligible: Array<{ change: FileChange; absolutePath: string }> = [];
            for (const c of group) {
                if (c.hash && c.size !== undefined && !wasm.wasm_should_chunk(c.size)) {
                    skipRead.push(c);
                } else {
                    const absolutePath = c.data === undefined && c.size !== undefined &&
                        c.mtime !== undefined ? io.getAbsolutePath(c.path) : null;
                    if (hashWorkers && absolutePath) {
                        workerEligible.push({ change: c, absolutePath });
                    } else {
                        needRead.push(c);
                    }
                }
            }

            for (const c of skipRead) {
                batchFiles.push(new ObsetyncBatchFile(c, c.size!, c.mtime ?? Date.now()));
            }

            if (workerEligible.length > 0) {
                const workerResults = await Promise.all(workerEligible.map(async (candidate) => {
                    try {
                        const result = await hashWorkers!.run({
                            absolutePath: candidate.absolutePath,
                            expectedSize: candidate.change.size!,
                            expectedMtime: candidate.change.mtime!,
                            mode: wasm.wasm_should_chunk(candidate.change.size!)
                                ? "manifest"
                                : "hash",
                            feedBytes: tuning.feedBytes,
                        });
                        return { candidate, result };
                    } catch (error) {
                        if (
                            error instanceof HashWorkerFileDriftError ||
                            (error instanceof HashWorkerPoolError && error.code === "CLOSED") ||
                            (error as Error)?.name === "AbortError"
                        ) {
                            throw error;
                        }
                        if (!workerFallbackWarned) {
                            workerFallbackWarned = true;
                            console.warn(
                                "[obsetync] hash worker unavailable during push; " +
                                "using renderer fallback:",
                                error,
                            );
                        }
                        needRead.push(candidate.change);
                        return null;
                    }
                }));
                perf?.observePeakBatchBytes(workerEligible.length * tuning.feedBytes);
                for (const row of workerResults) {
                    if (!row) continue;
                    const { change } = row.candidate;
                    perf?.addPhase("read", row.result.read_ms);
                    if (row.result.mode === "hash") {
                        perf?.addPhase("hash", row.result.hash_ms);
                        change.hash = row.result.hash;
                        batchFiles.push(new ObsetyncBatchFile(
                            change,
                            row.result.size,
                            change.mtime!,
                        ));
                    } else {
                        perf?.addPhase("fastcdc", row.result.hash_ms);
                        change.hash = row.result.manifest.file_hash;
                        batchFiles.push(new ObsetyncBatchFile(
                            change,
                            row.result.size,
                            change.mtime!,
                            row.result.manifest,
                            undefined,
                            {
                                absolutePath: row.candidate.absolutePath,
                                fingerprint: row.result.fingerprint,
                            },
                        ));
                    }
                }
            }

            if (needRead.length === 0) continue;

            // Read files in parallel.
            const endRead = perf?.phase("read");
            let reads: Array<{ change: FileChange; data: Uint8Array }>;
            try {
                reads = await Promise.all(needRead.map(async c => ({
                    change: c,
                    data: c.data ?? await io.readFile(c.path),
                })));
            } finally {
                endRead?.();
            }
            const residentReadBytes = reads.reduce((sum, row) => sum + row.data.length, 0);
            perf?.observePeakBatchBytes(residentReadBytes);

            // Large files — wasm_chunk_file hashes internally.
            for (const { change, data } of reads.filter(r => wasm.wasm_should_chunk(r.data.length))) {
                const endChunk = perf?.phase("fastcdc");
                let chunkInfo: any;
                try {
                    chunkInfo = await chunkFileStreaming(wasm, data);
                } finally {
                    endChunk?.();
                }
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

            unknownSmallReads.push(...smallReads.filter(r => !r.change.hash));
        }

        // Read concurrency and WASM call size are independent budgets. Keep
        // reads parallel, then hash the accumulated small-file payload in
        // byte-bounded calls so neither bridge overhead nor retained memory
        // grows with the number of files in this stream batch.
        batchFiles.push(...hashUnknownSmallReads(wasm, unknownSmallReads, tuning, perf));
        unknownSmallReads.length = 0;

        // ------------------------------------------------------------------
        // B. Two batch-check requests for this batch.
        // ------------------------------------------------------------------
        const smallHashes = batchFiles
            .filter(f => !f.chunkInfo)
            .map(f => f.change.hash!);

        const allChunkHashes: string[] = batchFiles
            .filter(f => f.chunkInfo)
            .flatMap(f => (f.chunkInfo.chunks as any[]).map((c: any) => c.hash));

        const endCheck = perf?.phase("check");
        let neededSmall: string[];
        let neededChunks: string[];
        try {
            [neededSmall, neededChunks] = await Promise.all([
                smallHashes.length > 0
                    ? api.checkContent(smallHashes, perf)
                    : Promise.resolve([]),
                allChunkHashes.length > 0
                    ? api.checkContentChunks(allChunkHashes, perf)
                    : Promise.resolve([]),
            ]);
        } finally {
            endCheck?.();
        }

        const neededSmallSet  = new Set(neededSmall);
        const neededChunksSet = new Set(neededChunks);
        for (const file of batchFiles) {
            if (file.chunkInfo) {
                let missingForFile = 0;
                for (const chunk of file.chunkInfo.chunks as any[]) {
                    if (neededChunksSet.has(chunk.hash)) missingForFile += chunk.size;
                }
                if (missingForFile > 0) {
                    neededFiles++;
                    neededBytes += missingForFile;
                }
            } else if (file.change.hash && neededSmallSet.has(file.change.hash)) {
                neededFiles++;
                neededBytes += file.size;
            }
        }

        // ------------------------------------------------------------------
        // C. Prepare missing content, then ACK one or more byte-bounded packs.
        // ------------------------------------------------------------------
        const uploadRecords: BulkUploadRecord[] = [];
        const queuedContent = new Set<string>();
        const queuedContentChunks = new Set<string>();
        const queuedManifests = new Set<string>();
        let batchContentBytes = 0;
        for (const {
            change,
            size,
            mtime,
            chunkInfo,
            largeData,
            rangedSource,
        } of batchFiles) {
            if (chunkInfo) {
                const missingChunks: Array<{ hash: string; offset: number; size: number }> = [];
                for (const chunk of chunkInfo.chunks as Array<{
                    hash: string;
                    offset: number;
                    size: number;
                }>) {
                    if (
                        neededChunksSet.has(chunk.hash) &&
                        !queuedContentChunks.has(chunk.hash)
                    ) {
                        queuedContentChunks.add(chunk.hash);
                        missingChunks.push(chunk);
                    }
                }
                const uploadManifest = !queuedManifests.has(change.hash!);
                if (uploadManifest) queuedManifests.add(change.hash!);
                const makeManifestRecord = (): BulkUploadRecord => ({
                    kind: BulkObjectKind.Manifest,
                    hash: change.hash!,
                    data: new TextEncoder().encode(JSON.stringify({
                        file_hash: change.hash!,
                        total_size: chunkInfo.total_size,
                        chunks: chunkInfo.chunks,
                    })),
                });

                if (rangedSource) {
                    const putRangedRecords = async (
                        records: readonly BulkUploadRecord[],
                    ): Promise<void> => {
                        await beforeHeavyBatch?.();
                        const endUpload = perf?.phase("upload");
                        try {
                            await api.putObjects(records, perf);
                        } finally {
                            endUpload?.();
                        }
                        const bytes = records.reduce(
                            (sum, record) => sum + record.data.byteLength,
                            0,
                        );
                        uploadedBytes += bytes;
                        perf?.increment({ bytesTransferred: bytes });
                    };
                    const ranged = await uploadDesktopMissingRanges(
                        rangedSource,
                        missingChunks,
                        putRangedRecords,
                        async () => {
                            if (!uploadManifest) return;
                            await beforeHeavyBatch?.();
                            const manifestRecord = makeManifestRecord();
                            perf?.observePeakBatchBytes(manifestRecord.data.byteLength);
                            const endUpload = perf?.phase("upload");
                            try {
                                await api.putObjects([manifestRecord], perf);
                            } finally {
                                endUpload?.();
                            }
                        },
                    );
                    perf?.addPhase("read", ranged.readMs);
                    perf?.observePeakBatchBytes(ranged.peakBufferedBytes);
                    continue;
                }

                let content = largeData;
                if (missingChunks.length > 0 && !content) {
                    const before = await io.stat(change.path);
                    if (
                        !before || before.size !== size ||
                        Math.abs(before.mtime - mtime) > 1
                    ) {
                        throw new HashWorkerFileDriftError(
                            "file changed between manifest planning and upload",
                        );
                    }
                    const endRead = perf?.phase("read");
                    try {
                        content = await io.readFile(change.path);
                    } finally {
                        endRead?.();
                    }
                    const after = await io.stat(change.path);
                    if (
                        content.length !== size ||
                        !after || after.size !== size ||
                        Math.abs(after.mtime - mtime) > 1
                    ) {
                        throw new HashWorkerFileDriftError(
                            "file changed while loading planned chunks",
                        );
                    }
                    perf?.observePeakBatchBytes(content.length);
                }
                for (const chunk of missingChunks) {
                    const chunkData = content!.subarray(
                        chunk.offset,
                        chunk.offset + chunk.size,
                    );
                    uploadRecords.push({
                        kind: BulkObjectKind.ContentChunk,
                        hash: chunk.hash,
                        data: chunkData,
                    });
                    batchContentBytes += chunkData.length;
                }
                if (uploadManifest) uploadRecords.push(makeManifestRecord());
            } else if (
                change.hash &&
                neededSmallSet.has(change.hash) &&
                !queuedContent.has(change.hash)
            ) {
                // Re-read the file — only done for the small fraction the server needs.
                let data = change.data;
                if (!data) {
                    const endRead = perf?.phase("read");
                    try {
                        data = await io.readFile(change.path);
                    } finally {
                        endRead?.();
                    }
                }
                perf?.observePeakBatchBytes(data.length);
                queuedContent.add(change.hash);
                uploadRecords.push({
                    kind: BulkObjectKind.Content,
                    hash: change.hash,
                    data,
                });
                batchContentBytes += data.length;
            }

            // Tree/progress state is appended below only after every object
            // in this batch has a successful stored/already-present ACK.
        }

        if (uploadRecords.length > 0) {
            await beforeHeavyBatch?.();
            const endUpload = perf?.phase("upload");
            try {
                await api.putObjects(uploadRecords, perf);
            } finally {
                endUpload?.();
            }
            uploadedBytes += batchContentBytes;
            perf?.increment({ bytesTransferred: batchContentBytes });
        }

        for (const { change, size, mtime } of batchFiles) {
            processed++;
            perf?.increment({ filesCompleted: 1 });
            onProgress?.(`↑ ${processed}/${total} ${throughput(processed, uploadedBytes, startTime)}`);

            // Queue tree update — applied in ONE update_batch after all batches.
            allTreeUpdates.push({ path: change.path, hash: change.hash!, mtime_ms: mtime, size });
        }

        // batchFiles goes out of scope — large file blobs are GC-eligible now.
    }

    // ------------------------------------------------------------------
    // D. Apply all tree updates to the candidate in one update_tree call.
    //    O(N + prefix_size) vs O(N × prefix_size) with per-file update_entry.
    // ------------------------------------------------------------------
    const beforeRoot = tree.root_hash_hex();
    const beforeFiles = tree.total_files();
    if (allTreeUpdates.length > 0) {
        const endTree = perf?.phase("tree_update");
        try {
            tree.candidate_update_batch(JSON.stringify(allTreeUpdates));
        } finally {
            endTree?.();
        }
    }
    perf?.setWorkload({
        bytesTotal: allTreeUpdates.reduce((sum, update) => sum + update.size, 0),
        filesNeeded: neededFiles,
        bytesNeeded: neededBytes,
    });
    const afterRoot = tree.candidate_root_hash_hex();
    const afterFiles = tree.candidate_total_files();
    // Diagnostic: if batch > 0 but root didn't move, or file count didn't grow
    // by the expected delta, candidate_update_batch silently dropped entries and we want
    // to know NOW instead of watching devices mysteriously fail to sync.
    console.log(
        `[obsetync] tree update: files ${beforeFiles} → ${afterFiles}, ` +
        `root ${(beforeRoot ?? "(empty)").slice(0, 16)} → ${(afterRoot ?? "(empty)").slice(0, 16)}, ` +
        `batch=${allTreeUpdates.length} deletes=${deleted.length}`
    );
    if (allTreeUpdates.length > 0 && beforeRoot === afterRoot) {
        console.warn(
            `[obsetync] candidate_update_batch didn't move root despite ` +
            `${allTreeUpdates.length} entries — ` +
            `first 3: ${JSON.stringify(allTreeUpdates.slice(0, 3))}`
        );
    }

    // Upload index chunks (LeafChunk, InternalNode) accumulated in MemoryChunkStore.
    // Server needs these to walk the tree during merge/diff.
    const candidateChunkHashes = wasm.wasm_tree_candidate_chunk_hashes(tree);
    const chunkHashes = bootstrapped
        ? candidateChunkHashes
        : wasm.wasm_tree_new_candidate_chunk_hashes(tree);
    perf?.setWasmChunks({ reachable: candidateChunkHashes.length });
    if (chunkHashes.length > 0) {
        onProgress?.(`↑ checking ${chunkHashes.length} index chunks...`);
        const endCheck = perf?.phase("check");
        let neededChunks: string[];
        try {
            neededChunks = await api.checkChunks(chunkHashes, perf);
        } finally {
            endCheck?.();
        }
        if (neededChunks.length > 0) {
            onProgress?.(`↑ uploading ${neededChunks.length} index chunks...`);
            const records: BulkUploadRecord[] = [];
            for (const hash of neededChunks) {
                const bytes = wasm.wasm_tree_get_chunk(tree, hash);
                if (bytes) {
                    records.push({ kind: BulkObjectKind.IndexChunk, hash, data: bytes });
                }
            }
            const endIndexUpload = perf?.phase("tree_index_upload");
            try {
                await beforeHeavyBatch?.();
                await api.putObjects(records, perf);
            } finally {
                endIndexUpload?.();
            }
            await yieldToUI();
        }
    }

    // Push root once after all batches.
    // parentHash = the base this tree state descends from. An honest base
    // lets the server fast-forward when we're truly current and pick the
    // correct three-way-merge base when we're not.
    const parentHash = baseRootHash ?? "";
    const rootBytes  = tree.candidate_root_bytes();
    if (!rootBytes) {
        throw new Error("push: candidate_root_bytes() returned null — candidate uninitialised");
    }
    console.log(
        `[obsetync] putRoot → parent=${parentHash ? parentHash.slice(0,16) : "(empty)"} ` +
        `new=${afterRoot?.slice(0,16)} bytes=${rootBytes.length}`
    );

    onProgress?.("↑ pushing root...");
    const endRootCommit = perf?.phase("root_commit");
    let result: Awaited<ReturnType<ObsetyncApi["putRoot"]>>;
    try {
        result = await api.putRoot(vaultId, rootBytes, parentHash, perf);
    } finally {
        endRootCommit?.();
    }

    // Server acceptance is the transaction boundary. Only now can the local
    // committed pointer advance and old immutable chunks be swept.
    const gcStats = tree.commit_candidate();
    candidateOpen = false;
    perf?.setWasmChunks({ after: gcStats.after });

    // Commit local metadata only after the server accepted this root. A
    // failed check/upload/root request therefore leaves sync-base untouched.
    for (const change of deleted) syncBase.removeEntry(change.path);
    for (const update of allTreeUpdates) {
        syncBase.setEntry(update.path, update.hash, update.mtime_ms, update.size);
    }

    syncBase.setLastSyncTimestamp(Date.now());
    const endCheckpoint = perf?.phase("checkpoint");
    try {
        await syncBase.save();
    } finally {
        endCheckpoint?.();
    }

    return {
        newRootHash: result.root_hash,
        conflicts:   result.conflicts ?? [],
    };
    } catch (error) {
        if (candidateOpen && tree.has_candidate()) {
            try {
                const endTree = perf?.phase("tree_update");
                try {
                    const gcStats = tree.abort_candidate();
                    perf?.setWasmChunks({ after: gcStats.after });
                } finally {
                    endTree?.();
                }
                console.warn("[obsetync] failed push aborted candidate tree");
            } catch (abortError) {
                console.error("[obsetync] failed to abort candidate tree:", abortError);
            }
        }
        throw error;
    }
}

/** Yield control back to the JS event loop so Obsidian stays responsive. */
const yieldToUI = () => new Promise<void>(r => window.setTimeout(r, 0));

const monotonicNow = (): number => globalThis.performance?.now?.() ?? Date.now();

function memoryPressureDetected(): boolean {
    const memory = (globalThis.performance as any)?.memory;
    return Number.isFinite(memory?.usedJSHeapSize) &&
        Number.isFinite(memory?.jsHeapSizeLimit) &&
        memory.jsHeapSizeLimit > 0 &&
        memory.usedJSHeapSize / memory.jsHeapSizeLimit >= 0.85;
}

function observeHashStep(bytes: number, startedAt: number): number {
    const durationMs = Math.max(0, monotonicNow() - startedAt);
    // A synchronous WASM call occupies the renderer for its full duration,
    // so that duration is also a conservative event-loop-lag observation.
    observeHashFeedback({
        bytes,
        durationMs,
        eventLoopLagMs: durationMs,
        memoryPressure: memoryPressureDetected(),
    });
    return durationMs;
}

/**
 * Hash file bytes via the streaming WASM Hasher. Feed size is selected for the
 * active platform and adapts between 64 KiB and its platform cap. WASM linear
 * memory therefore follows the feed budget, never the full file size.
 */
export function streamingHash(wasm: WasmModule, data: Uint8Array): string {
    const hasher = new wasm.Hasher();
    try {
        let offset = 0;
        while (offset < data.length) {
            const feedBytes = getHashTuning().feedBytes;
            const end = Math.min(data.length, offset + feedBytes);
            const started = monotonicNow();
            hasher.update(data.subarray(offset, end));
            observeHashStep(end - offset, started);
            offset = end;
        }
        return hasher.finalize();
    } finally {
        hasher.free();
    }
}

/** Plan FastCDC chunks through small WASM bridge slices. The source buffer is
 *  necessarily whole-file on Obsidian mobile, but WASM never receives a
 *  second whole-file copy and retains a bounded ~4 MiB window. */
export async function chunkFileStreaming(wasm: WasmModule, data: Uint8Array): Promise<any> {
    const chunker = new wasm.WasmChunker();
    try {
        let offset = 0;
        let lastYieldAt = monotonicNow();
        while (offset < data.length) {
            const feedBytes = getHashTuning().feedBytes;
            const end = Math.min(data.length, offset + feedBytes);
            const started = monotonicNow();
            chunker.update(data.subarray(offset, end));
            observeHashStep(end - offset, started);
            offset = end;
            const afterStep = monotonicNow();
            if (afterStep - lastYieldAt >= getHashTuning().yieldBudgetMs) {
                await yieldToUI();
                lastYieldAt = monotonicNow();
            }
        }
        return chunker.finish();
    } finally {
        chunker.free();
    }
}

/**
 * Stream-hash a file directly from disk using Node.js fs (Electron/desktop only).
 * Uses the adaptive platform feed — peak read memory follows that bounded feed
 * regardless of file size.
 * Falls back to readFile + streamingHash on mobile (no Node.js fs).
 *
 * This is the nproc-ready path: each Web Worker calls this independently,
 * giving true parallel hashing across cores with zero data crossing thread boundaries.
 */
export async function hashFileStreaming(
    path: string,
    io: PlatformIO,
    wasm: WasmModule,
    perf?: PerfOperation,
    perfWeight = 1,
): Promise<string> {
    const absPath = io.getAbsolutePath(path);
    if (absPath) {
        const fs = (globalThis as any).require?.('fs') as typeof import('fs') | undefined;
        if (fs?.createReadStream) {
            const hasher = new wasm.Hasher();
            const started = perf ? monotonicNow() : 0;
            let hashMs = 0;
            try {
                await new Promise<void>((resolve, reject) => {
                    fs.createReadStream(absPath, { highWaterMark: getHashTuning().feedBytes })
                        .on('data', (chunk: Buffer | string) => {
                            const buf = chunk as Buffer;
                            const hashStarted = monotonicNow();
                            hasher.update(new Uint8Array(
                                buf.buffer,
                                buf.byteOffset,
                                buf.byteLength,
                            ));
                            hashMs += observeHashStep(buf.byteLength, hashStarted);
                        })
                        .on('end', resolve)
                        .on('error', reject);
                });
                const finalizeStarted = perf ? monotonicNow() : 0;
                const result = hasher.finalize();
                if (perf) {
                    hashMs += Math.max(0, monotonicNow() - finalizeStarted);
                    const totalMs = Math.max(0, monotonicNow() - started);
                    perf.addPhase("hash", hashMs * perfWeight);
                    perf.addPhase("read", Math.max(0, totalMs - hashMs) * perfWeight);
                }
                return result;
            } finally {
                hasher.free();
            }
        }
    }
    // Mobile / no-fs fallback.
    const readStarted = perf ? monotonicNow() : 0;
    const data = await io.readFile(path);
    if (perf) perf.addPhase("read", Math.max(0, monotonicNow() - readStarted) * perfWeight);
    const hashStarted = perf ? monotonicNow() : 0;
    const result = streamingHash(wasm, data);
    if (perf) perf.addPhase("hash", Math.max(0, monotonicNow() - hashStarted) * perfWeight);
    return result;
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
