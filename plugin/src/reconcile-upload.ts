import type { ObsetyncApi } from "./api";
import { adaptiveBatches } from "./adaptive-batches";
import { BulkObjectKind, type BulkUploadRecord } from "./bulk-codec";
import { HashWorkerFileDriftError, HashWorkerPoolError, waitForHashWorkerCleanup,
    type DesktopHashWorkerPool } from "./desktop-hash-workers";
import { reconcileDesktopLargeFile } from "./desktop-reconcile-upload";
import { openQualifiedDesktopRangeSource, type DesktopRangeReaderOpener,
    type QualifiedDesktopRangeSource } from "./desktop-ranged-upload";
import { getHashTuning, type HashTuning } from "./hash-runtime";
import { MAX_FASTCDC_CHUNK_BYTES } from "./hash-worker-protocol";
import type { PerfOperation, PerfPhase } from "./perf-trace";
import type { PlatformIO } from "./platform";
import { allSettledBounded, validateManifest } from "./pull";
import { chunkFileStreaming, runDesktopRendererRangePass, type WasmModule } from "./push";
import { planPushMemory, pushGroupingByteLimit, PUSH_CHUNKER_WORK_BYTES } from "./push-memory";
import type { ReconcileContentSource } from "./reconcile-content";
import { selectReconcileMissingRanges } from "./reconcile-content";
import { ResourceBudgetClosedError, ResourceBudgetOversizedError, type ResourceBudget } from "./resource-budget";
import { reserveTransientScope, reserveTransientWorkset, transientMemorySnapshot,
    type TransientWorkScope } from "./transient-memory";
import { throwIfWorkAborted, yieldWork } from "./work-scheduler";

type RepairApi = Pick<ObsetyncApi, "putObjects" | "checkContentChunks">;
type RepairBudget = Pick<ResourceBudget, "reserve" | "snapshot">;
const sharedBudget: RepairBudget = { reserve: reserveTransientWorkset, snapshot: transientMemorySnapshot };

export interface ReconcileRepairStats {
    uploaded: number;
    bytes: number;
    neededBytes: number;
    /** All incomplete objects; the following counters are diagnostic subsets. */
    deferred: number;
    drifted: number;
    readErrors: number;
    uploadErrors: number;
}

export interface ReconcileUploadOptions {
    signal?: AbortSignal;
    perf?: PerfOperation;
    beforeHeavyBatch?: () => Promise<void>;
    onProgress?: (checked: number, stats: Readonly<ReconcileRepairStats>) => void;
    /** Isolated deterministic test/embedding seams; production shares one pool. */
    budget?: RepairBudget;
    tuning?: () => HashTuning;
    openReader?: DesktopRangeReaderOpener;
    qualifyReader?: (absolutePath: string, expected: { size: number; mtime: number }) =>
        Promise<QualifiedDesktopRangeSource>;
}

const emptyStats = (): ReconcileRepairStats => ({
    uploaded: 0, bytes: 0, neededBytes: 0, deferred: 0, drifted: 0, readErrors: 0, uploadErrors: 0,
});

/** Never trust an unexpected server hash as an instruction to read/upload an
 * unrelated source. Canonicalize and deduplicate only members of this check. */
export function validatedReconcileMissing(requested: readonly string[], missing: readonly string[]): string[] {
    const allowed = new Set(requested.map(hash => hash.toLowerCase()));
    const selected = new Set<string>();
    for (const hash of missing) {
        if (typeof hash !== "string" || !/^[0-9a-f]{64}$/i.test(hash) || !allowed.has(hash.toLowerCase())) {
            throw new Error("reconcile check returned an unexpected content hash");
        }
        selected.add(hash.toLowerCase());
    }
    return [...selected];
}

function rethrowTerminal(error: unknown, signal?: AbortSignal): void {
    throwIfWorkAborted(signal);
    if ((error as Error)?.name === "AbortError" || error instanceof ResourceBudgetClosedError ||
        (error instanceof HashWorkerPoolError && error.code === "CLOSED")) throw error;
}

function recordFailure(stats: ReconcileRepairStats, error: unknown, stage: "read" | "upload", count = 1): void {
    stats.deferred += count;
    if (error instanceof HashWorkerFileDriftError) stats.drifted += count;
    else if (!(error instanceof ResourceBudgetOversizedError)) {
        if (stage === "read") stats.readErrors += count;
        else stats.uploadErrors += count;
    }
}

async function phase<T>(perf: PerfOperation | undefined, name: PerfPhase, work: () => Promise<T>): Promise<T> {
    const end = perf?.phase(name);
    try { return await work(); }
    finally { end?.(); }
}

async function permit(options: ReconcileUploadOptions): Promise<void> {
    throwIfWorkAborted(options.signal);
    await options.beforeHeavyBatch?.();
    await yieldWork({ signal: options.signal, perf: options.perf });
}

/** Includes the actual native read, with a fresh size fence after a queued
 * admission. Post-read checks detect drift, not an initial native allocator cap. */
async function readSource(source: ReconcileContentSource, io: PlatformIO,
    memory: TransientWorkScope, options: ReconcileUploadOptions): Promise<Uint8Array> {
    return memory.track(() => phase(options.perf, "read", async () => {
        throwIfWorkAborted(options.signal);
        const before = await io.stat(source.path);
        if (!before || before.size !== source.size) throw new HashWorkerFileDriftError("reconcile source changed before read");
        throwIfWorkAborted(options.signal);
        const data = await io.readFile(source.path);
        throwIfWorkAborted(options.signal);
        if (data.byteLength !== before.size || data.buffer.byteLength > before.size) {
            throw new HashWorkerFileDriftError("reconcile source exceeded admitted size");
        }
        const after = await io.stat(source.path);
        if (!after || after.size !== before.size || Math.abs(after.mtime - before.mtime) > 1) {
            throw new HashWorkerFileDriftError("reconcile source changed during read");
        }
        return data;
    }));
}

async function hashSource(wasm: WasmModule, data: Uint8Array, feedCeiling: number,
    options: ReconcileUploadOptions): Promise<string> {
    return phase(options.perf, "hash", async () => {
        const hasher = new wasm.Hasher();
        try {
            let yieldedAt = Date.now();
            for (let offset = 0; offset < data.byteLength;) {
                throwIfWorkAborted(options.signal);
                const tuning = (options.tuning ?? getHashTuning)();
                const end = Math.min(data.byteLength, offset + Math.min(tuning.feedBytes, feedCeiling));
                hasher.update(data.subarray(offset, end));
                offset = end;
                if (Date.now() - yieldedAt >= tuning.yieldBudgetMs) {
                    await yieldWork({ perf: options.perf, signal: options.signal });
                    yieldedAt = Date.now();
                }
            }
            throwIfWorkAborted(options.signal);
            return hasher.finalize().toLowerCase();
        } finally { hasher.free(); }
    });
}

/** Repair only explicitly missing content; never mutate a path, sync base,
 * journal, or root. A failed/oversized object does not stop unrelated objects. */
export async function repairSmallContent(api: RepairApi, io: PlatformIO, wasm: WasmModule,
    sources: readonly ReconcileContentSource[], options: ReconcileUploadOptions = {}): Promise<ReconcileRepairStats> {
    const budget = options.budget ?? sharedBudget;
    const tuningNow = options.tuning ?? getHashTuning;
    const indexed = new Map<string, ReconcileContentSource>();
    for (const source of sources) {
        const hash = source.hash.toLowerCase();
        if (!indexed.has(hash)) indexed.set(hash, { ...source, hash });
    }
    const unique = [...indexed.values()];
    const stats = emptyStats();
    stats.neededBytes = unique.reduce((sum, source) => sum + source.size, 0);
    const batches = adaptiveBatches(unique, source => source.size, () => {
        const tuning = tuningNow();
        return { maxFiles: tuning.maxBatchFiles, maxBytes: pushGroupingByteLimit(tuning, budget.snapshot().capacityBytes),
            maxSingleBytes: tuning.maxSingleBatchFileBytes, maxHoldMs: tuning.maxBatchHoldMs };
    });
    const split: ReconcileContentSource[][] = [];
    let checked = 0;
    for (;;) {
        // Read current profile AFTER the cooperative gate, not before waiting.
        await permit(options);
        const batch = split.pop() ?? batches.next().value;
        if (!batch) break;
        const tuning = tuningNow();
        const plan = planPushMemory(batch.map(source => ({ size: source.size, chunked: false, ranged: false })), tuning);
        const capacity = budget.snapshot().capacityBytes;
        if (plan.totalBytes > capacity) {
            if (batch.length > 1) {
                const middle = Math.ceil(batch.length / 2);
                split.push(batch.slice(middle), batch.slice(0, middle));
            } else {
                recordFailure(stats, new ResourceBudgetOversizedError(plan.totalBytes, capacity), "read");
                checked++;
                options.onProgress?.(checked, stats);
            }
            continue;
        }
        let memory: TransientWorkScope;
        try { memory = await reserveTransientScope(plan, { signal: options.signal, budget }); }
        catch (error) {
            rethrowTerminal(error, options.signal);
            if (error instanceof ResourceBudgetOversizedError) { split.push(batch); continue; }
            throw error;
        }
        const records: BulkUploadRecord[] = [];
        try {
            const prepared = await allSettledBounded(batch, tuning.readConcurrency, async source => {
                const data = await readSource(source, io, memory, options);
                if (await hashSource(wasm, data, tuning.maxFeedBytes, options) !== source.hash) {
                    throw new HashWorkerFileDriftError("reconcile content differs from committed hash");
                }
                return { kind: BulkObjectKind.Content, hash: source.hash, data } satisfies BulkUploadRecord;
            });
            // Every native sibling has settled before an abort can close the owner.
            throwIfWorkAborted(options.signal);
            for (const result of prepared) {
                if (result.status === "fulfilled") records.push(result.value);
                else { rethrowTerminal(result.reason, options.signal); recordFailure(stats, result.reason, "read"); }
            }
            if (records.length) {
                const resident = records.reduce((sum, record) => sum + record.data.byteLength, 0);
                options.perf?.observePeakBatchBytes(resident);
                try {
                    await phase(options.perf, "upload", () => api.putObjects(records, options.perf, memory));
                    throwIfWorkAborted(options.signal);
                    stats.uploaded += records.length;
                    stats.bytes += resident;
                    options.perf?.increment({ filesCompleted: records.length, bytesTransferred: resident });
                } catch (error) {
                    rethrowTerminal(error, options.signal);
                    recordFailure(stats, error, "upload", records.length);
                }
            }
        } finally { records.length = 0; memory.close(); }
        checked += batch.length;
        options.onProgress?.(checked, stats);
    }
    return stats;
}

async function checkChunkHashes(api: RepairApi, hashes: readonly string[], options: ReconcileUploadOptions,
    memory: TransientWorkScope): Promise<string[]> {
    const unique = [...new Set(hashes)];
    const missing: string[] = [];
    for (let cursor = 0; cursor < unique.length; cursor += 1000) {
        throwIfWorkAborted(options.signal);
        const batch = unique.slice(cursor, cursor + 1000);
        const response = await phase(options.perf, "check", () =>
            api.checkContentChunks(batch, options.perf, options.signal, memory));
        missing.push(...validatedReconcileMissing(batch, response));
    }
    return missing;
}

export async function repairLargeContent(api: RepairApi, io: PlatformIO, wasm: WasmModule,
    source: ReconcileContentSource, worker?: Pick<DesktopHashWorkerPool, "run">,
    options: ReconcileUploadOptions = {}): Promise<ReconcileRepairStats> {
    const stats = emptyStats();
    const budget = options.budget ?? sharedBudget;
    const tuningNow = options.tuning ?? getHashTuning;
    let allowWorker = !!worker;
    let stage: "read" | "upload" = "read";
    for (;;) {
        await permit(options);
        const tuning = tuningNow();
        const absolutePath = io.getAbsolutePath(source.path);
        const ranged = !!absolutePath;
        const workerBacked = ranged && allowWorker && !!worker;
        let plan = planPushMemory([{ size: source.size, chunked: true, ranged, worker: workerBacked }], tuning);
        const capacity = budget.snapshot().capacityBytes;
        if (ranged && plan.totalBytes > capacity) {
            plan = planPushMemory([{ size: source.size, chunked: true, ranged: true, worker: workerBacked }],
                tuning, MAX_FASTCDC_CHUNK_BYTES);
        }
        if (plan.totalBytes > capacity) {
            recordFailure(stats, new ResourceBudgetOversizedError(plan.totalBytes, capacity), "read");
            return stats;
        }
        let memory: TransientWorkScope;
        try { memory = await reserveTransientScope(plan, { signal: options.signal, budget }); }
        catch (error) {
            rethrowTerminal(error, options.signal);
            if (error instanceof ResourceBudgetOversizedError) continue;
            throw error;
        }
        let data: Uint8Array | undefined;
        const records: BulkUploadRecord[] = [];
        try {
            if (ranged) {
                const stat = await io.stat(source.path);
                if (!stat || stat.size !== source.size) throw new HashWorkerFileDriftError("reconcile source changed before worker read");
                const endWorker = options.perf?.phase("prepare_batch");
                let workerPending = true;
                try {
                    const manifestRunner: Pick<DesktopHashWorkerPool, "run"> = workerBacked ? worker! : {
                        run: async (job, signal) => {
                            const qualified = await (options.qualifyReader ?? openQualifiedDesktopRangeSource)(
                                job.absolutePath,
                                { size: job.expectedSize, mtime: job.expectedMtime },
                            );
                            try {
                                return await runDesktopRendererRangePass(
                                    wasm,
                                    qualified.source,
                                    qualified.reader,
                                    job.mode,
                                    options.perf,
                                    signal,
                                    Math.min(job.feedBytes, tuning.maxFeedBytes),
                                );
                            } finally {
                                await qualified.reader.close();
                            }
                        },
                    };
                    const result = await reconcileDesktopLargeFile(manifestRunner, {
                        absolutePath: absolutePath!, expectedHash: source.hash.toLowerCase(), expectedSize: source.size,
                        expectedMtime: stat.mtime, feedBytes: Math.min(tuning.feedBytes, tuning.maxFeedBytes),
                    }, hashes => {
                        workerPending = false;
                        stage = "upload";
                        return checkChunkHashes(api, hashes, options, memory);
                    },
                    async batch => {
                        workerPending = false;
                        stage = "upload";
                        await permit(options);
                        await phase(options.perf, "upload", () => api.putObjects(batch, options.perf, memory));
                        const transferred = batch.reduce((sum, record) => sum +
                            (record.kind === BulkObjectKind.ContentChunk ? record.data.byteLength : 0), 0);
                        stats.bytes += transferred;
                        options.perf?.increment({ bytesTransferred: transferred });
                    }, options.signal, options.openReader,
                    { memory, maxBufferedBytes: plan.rangeQueueBytes,
                        beforeRead: () => { workerPending = false; stage = "read"; return permit(options); },
                        onNeededBytes: needed => { stats.neededBytes = needed; } });
                    options.perf?.addPhase("read", result.workerReadMs);
                    options.perf?.addPhase("fastcdc", result.workerHashMs);
                    if (result.status === "drifted") {
                        recordFailure(stats, new HashWorkerFileDriftError(), "read");
                        return stats;
                    }
                    stats.uploaded = 1;
                    stats.bytes = result.uploadedBytes;
                    stats.neededBytes = result.neededBytes;
                    options.perf?.addPhase("read", result.rangeReadMs);
                    options.perf?.observePeakBatchBytes(result.peakBufferedBytes);
                    options.perf?.increment({ filesCompleted: 1 });
                    return stats;
                } catch (error) {
                    rethrowTerminal(error, options.signal);
                    // A confirmed worker capability failure retries through the
                    // same bounded native range path in the renderer. Do not
                    // reinterpret source drift or upload errors as capability loss.
                    if (workerBacked && workerPending && stage === "read" &&
                        !(error instanceof HashWorkerFileDriftError) &&
                        await waitForHashWorkerCleanup(error)) {
                        throwIfWorkAborted(options.signal);
                        allowWorker = false;
                        // finally closes the drained worker scope. The next
                        // iteration re-admits a renderer range pass from scratch.
                        continue;
                    }
                    throw error;
                } finally { endWorker?.(); }
            }

            data = await readSource(source, io, memory, options);
            options.perf?.observePeakBatchBytes(data.byteLength);
            const info = await phase(options.perf, "fastcdc", () => memory.run(PUSH_CHUNKER_WORK_BYTES,
                () => chunkFileStreaming(wasm, data!, options.perf, options.signal, tuning.maxFeedBytes),
                { signal: options.signal }));
            if (info.file_hash?.toLowerCase() !== source.hash.toLowerCase()) {
                throw new HashWorkerFileDriftError("reconcile manifest differs from committed hash");
            }
            const manifest = validateManifest(info, source.hash, source.size);
            stage = "upload";
            const missing = await checkChunkHashes(api, manifest.chunks.map(chunk => chunk.hash), options, memory);
            const ranges = selectReconcileMissingRanges(manifest.chunks, missing);
            stats.neededBytes = ranges.reduce((sum, chunk) => sum + chunk.size, 0);
            for (const chunk of ranges) records.push({ kind: BulkObjectKind.ContentChunk, hash: chunk.hash,
                data: data.subarray(chunk.offset, chunk.offset + chunk.size) });
            const encoded = new TextEncoder().encode(JSON.stringify(manifest));
            if (encoded.byteLength > plan.manifestBytes) throw new ResourceBudgetOversizedError(encoded.byteLength, plan.manifestBytes);
            records.push({ kind: BulkObjectKind.Manifest, hash: source.hash.toLowerCase(), data: encoded });
            await permit(options);
            await phase(options.perf, "upload", () => api.putObjects(records, options.perf, memory));
            throwIfWorkAborted(options.signal);
            stats.uploaded = 1;
            stats.bytes = stats.neededBytes;
            options.perf?.increment({ filesCompleted: 1, bytesTransferred: stats.bytes });
            return stats;
        } catch (error) {
            rethrowTerminal(error, options.signal);
            recordFailure(stats, error, stage);
            return stats;
        } finally { records.length = 0; data = undefined; memory.close(); }
    }
}
