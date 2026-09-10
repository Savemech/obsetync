import { repairSmallContent, repairLargeContent, validatedReconcileMissing } from "./reconcile-upload";
import { ResourceBudget } from "./resource-budget";
import { hashTuningForRuntime } from "./hash-runtime";
import { planPushMemory } from "./push-memory";
import { BulkObjectKind } from "./bulk-codec";
import { DesktopHashWorkerPool } from "./desktop-hash-workers";
import type { HashWorkerRequest, HashWorkerResponse } from "./hash-worker-protocol";

let assertions = 0;
const check = (condition: unknown, message: string): void => {
    assertions++;
    if (!condition) throw new Error(message);
};
const hash = (byte: number) => byte.toString(16).padStart(2, "0").repeat(32);
const tuning = () => ({ ...hashTuningForRuntime("desktop"), maxFeedBytes: 64 * 1024, feedBytes: 64 * 1024 });
const options = (budget: ResourceBudget) => ({ budget, tuning });
function deferred<T = void>() {
    let resolve!: (value: T) => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<T>((done, fail) => { resolve = done; reject = fail; });
    return { promise, resolve, reject };
}
const flush = async () => { for (let i = 0; i < 20; i++) await Promise.resolve(); };
const wasm = {
    Hasher: class {
        private byte = 0;
        update(data: Uint8Array): void { this.byte = data[0]; }
        finalize(): string { return hash(this.byte); }
        free(): void {}
    },
    WasmChunker: class {
        private length = 0;
        update(data: Uint8Array): void {
            check(data.byteLength <= 64 * 1024, "fallback feed grew beyond admitted ceiling");
            this.length += data.byteLength;
        }
        finish() {
            return { file_hash: hash(1), total_size: this.length,
                chunks: [{ hash: hash(2), offset: 0, size: this.length }] };
        }
        free(): void {}
    },
} as any;

async function smallRepairValidatesAndIsolatesFailures(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 32 * 1024 * 1024 });
    let uploads = 0;
    const reads: string[] = [];
    const sources = [1, 2, 3].map(byte => ({ path: `source-${byte}`, hash: hash(byte), size: 1 }));
    const io = {
        stat: async () => ({ size: 1, mtime: 1 }),
        readFile: async (path: string) => {
            check(budget.snapshot().usedBytes > 0, "small native source read preceded admission");
            reads.push(path);
            if (path === "source-3") throw new Error("native read unavailable");
            return new Uint8Array([path === "source-1" ? 1 : 9]);
        },
    } as any;
    const api = {
        putObjects: async (records: any[], _perf: unknown, memory: any) => {
            uploads++;
            check(!!memory && budget.snapshot().usedBytes === memory.snapshot().totalBytes,
                "small upload did not receive complete source ownership");
            check(records.length === 1 && records[0].hash === hash(1), "drifted/failed source reached server");
            await memory.run(memory.snapshot().work.capacityBytes, async () => {
                check(budget.snapshot().usedBytes > 0, "nested crypto double-counted/released parent");
            });
        },
    } as any;
    const stats = await repairSmallContent(api, io, wasm, [...sources, sources[0]], options(budget));
    check(stats.uploaded === 1 && uploads === 1 && reads.length === 3, "duplicate content was repaired twice");
    check(stats.deferred === 2 && stats.drifted === 1 && stats.readErrors === 1, "incomplete repair lost reason counts");
    check(budget.snapshot().usedBytes === 0, "small repair leaked source ownership");
    check(validatedReconcileMissing([hash(1)], [hash(1).toUpperCase(), hash(1)]).length === 1,
        "missing bitmap duplicates were not canonicalized");
    let rejected = false;
    try { validatedReconcileMissing([hash(1)], [hash(2)]); } catch { rejected = true; }
    check(rejected, "unexpected missing hash could select an unrelated source");
}

async function abortWaitsForEveryNativeSibling(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 32 * 1024 * 1024 });
    const native = deferred();
    const entered = deferred();
    const controller = new AbortController();
    let uploads = 0;
    const io = {
        stat: async () => ({ size: 1, mtime: 1 }),
        readFile: async (path: string) => {
            if (path === "failed") throw new Error("read failure");
            entered.resolve();
            await native.promise;
            return new Uint8Array([1]);
        },
    } as any;
    const promise = repairSmallContent({ putObjects: async () => { uploads++; } } as any, io, wasm,
        [{ path: "waiting", hash: hash(1), size: 1 }, { path: "failed", hash: hash(2), size: 1 }],
        { ...options(budget), signal: controller.signal });
    const outcome = promise.then(() => undefined, error => error);
    await entered.promise;
    controller.abort();
    await flush();
    check(budget.snapshot().usedBytes > 0, "abort released unfinished native sibling");
    native.resolve();
    check((await outcome)?.name === "AbortError", "reconcile swallowed cancellation as a skipped file");
    check(uploads === 0 && budget.snapshot().usedBytes === 0, "cancelled repair uploaded or leaked memory");
}

async function queuedShrinkReplansBeforeAnyRead(): Promise<void> {
    const sourceBytes = 100 * 1024;
    const singleton = planPushMemory([{ size: sourceBytes, chunked: false, ranged: false }], tuning());
    const budget = new ResourceBudget({ capacityBytes: singleton.totalBytes * 3 });
    const blocker = await budget.reserve(budget.snapshot().capacityBytes);
    const queued = deferred();
    let calls = 0;
    let reads = 0;
    const observedBudget = {
        snapshot: () => budget.snapshot(),
        reserve: (bytes: number, opts: any) => {
            calls++;
            const reservation = budget.reserve(bytes, opts);
            if (calls === 1) queued.resolve();
            return reservation;
        },
    };
    const io = {
        stat: async () => ({ size: sourceBytes, mtime: 1 }),
        readFile: async () => { reads++; return new Uint8Array(sourceBytes).fill(1); },
    } as any;
    let uploadCalls = 0;
    const promise = repairSmallContent({ putObjects: async (records: any[]) => {
        uploadCalls++;
        check(records.length === 1, "queued policy shrink did not split old batch");
    } } as any, io, wasm,
    [{ path: "first", hash: hash(1), size: sourceBytes }, { path: "second", hash: hash(2), size: sourceBytes }],
    { tuning, budget: observedBudget });
    await queued.promise;
    check(reads === 0, "queued repair already allocated source buffers");
    budget.setCapacity(singleton.totalBytes);
    blocker.release();
    const stats = await promise;
    check(calls >= 3 && reads === 2 && uploadCalls === 1 && stats.drifted === 1,
        "shrunk repair lost replanning/hash verification");
    check(budget.snapshot().usedBytes === 0, "queued shrink leaked repair reservation");
}

async function oversizeAndSourceGrowthDoNotReadUnboundedBytes(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 8 * 1024 * 1024 });
    let reads = 0;
    const io = {
        stat: async (path: string) => ({ size: path === "grew-before" ? 2 : 1, mtime: 1 }),
        readFile: async () => { reads++; return new Uint8Array([1, 2]); },
    } as any;
    let uploaded = 0;
    const stats = await repairSmallContent({ putObjects: async () => { uploaded++; } } as any, io, wasm,
        [
            { path: "oversized", hash: hash(3), size: 100 * 1024 * 1024 },
            { path: "grew-before", hash: hash(2), size: 1 },
            { path: "grew-during", hash: hash(1), size: 1 },
        ], options(budget));
    check(reads === 1 && uploaded === 0, "oversize/stale stat crossed native read admission");
    check(stats.deferred === 3 && stats.drifted === 2, "source growth did not preserve incomplete counts");
    check(budget.snapshot().usedBytes === 0, "growth detection leaked ownership");
}

class FakeWorker {
    readonly posted: HashWorkerRequest[] = [];
    readonly entered = deferred();
    termination = deferred<number>();
    private listeners = new Map<string, Array<(value: any) => void>>();
    on(event: string, listener: (value: any) => void): this {
        this.listeners.set(event, [...(this.listeners.get(event) ?? []), listener]); return this;
    }
    emit(event: string, value: unknown): void { this.listeners.get(event)?.forEach(listener => listener(value)); }
    postMessage(message: HashWorkerRequest): void {
        this.posted.push(message);
        if (message.type === "job") this.entered.resolve();
    }
    terminate(): Promise<number> { return this.termination.promise; }
}

async function fallbackWaitsForNativeTerminationAndReadmission(): Promise<void> {
    const size = 1024 * 1024;
    const budget = new ResourceBudget({ capacityBytes: 32 * 1024 * 1024 });
    const worker = new FakeWorker();
    const pool = new DesktopHashWorkerPool(() => worker as any, 1, 1);
    worker.emit("message", { type: "ready", wasm_mode: "simd" } satisfies HashWorkerResponse);
    let rendererReads = 0;
    let wholeReads = 0;
    let workerTerminated = false;
    let seenScopeSize = 0;
    const rangedPlan = planPushMemory([{ size, chunked: true, ranged: true, worker: false }], tuning());
    const io = {
        getAbsolutePath: () => "/fixture/source",
        stat: async () => ({ size, mtime: 1 }),
        readFile: async () => {
            wholeReads++;
            throw new Error("worker fallback read the whole source");
        },
    } as any;
    const api = {
        checkContentChunks: async () => [],
        putObjects: async (records: any[], _perf: unknown, memory: any) => {
            seenScopeSize = memory.snapshot().totalBytes;
            check(records.length === 1 && records[0].kind === BulkObjectKind.Manifest,
                "fallback uploaded already-present content chunks");
        },
    } as any;
    const rangeOptions = {
        ...options(budget),
        qualifyReader: async (absolutePath: string, expected: { size: number; mtime: number }) => {
            check(workerTerminated, "renderer range fallback raced native worker termination");
            return {
                source: { absolutePath, fingerprint: { ...expected, ctime: 1, device: 1, inode: 1 } },
                reader: {
                    verify: async () => {},
                    read: async (_offset: number, requested: number) => {
                        rendererReads++;
                        check(requested <= tuning().maxFeedBytes, "renderer fallback exceeded admitted feed ceiling");
                        return new Uint8Array(requested).fill(1);
                    },
                    close: async () => {},
                },
            };
        },
        openReader: async () => ({ verify: async () => {}, read: async () => new Uint8Array(0), close: async () => {} }),
    };
    const repairing = repairLargeContent(api, io, wasm,
        { path: "source", hash: hash(1), size }, pool, rangeOptions);
    await worker.entered.promise;
    check(worker.posted[0].type === "job" && worker.posted[0].feed_bytes <= tuning().maxFeedBytes,
        "worker feed was not pinned before source read");
    worker.emit("error", new Error("worker crashed"));
    await flush();
    check(rendererReads === 0 && wholeReads === 0 && budget.snapshot().usedBytes > 0,
        "fallback raced native worker termination");
    workerTerminated = true;
    worker.termination.resolve(0);
    const stats = await repairing;
    check(rendererReads > 1 && wholeReads === 0 && seenScopeSize === rangedPlan.totalBytes && stats.uploaded === 1,
        `confirmed worker termination failed to re-admit bounded fallback: ${JSON.stringify({ rendererReads, wholeReads, seenScopeSize, expected: rangedPlan.totalBytes, stats })}`);
    check(budget.snapshot().usedBytes === 0, "fallback leaked worker/source admission");
    pool.close();
}

async function absentWorkerUsesBoundedRendererRanges(): Promise<void> {
    const size = 4 * 1024 * 1024;
    const rangedPlan = planPushMemory([{ size, chunked: true, ranged: true, worker: false }], tuning());
    const wholePlan = planPushMemory([{ size, chunked: true, ranged: false }], tuning());
    check(rangedPlan.totalBytes < wholePlan.totalBytes, "range regression fixture does not exclude whole-source fallback");
    const budget = new ResourceBudget({ capacityBytes: rangedPlan.totalBytes });
    let rangeReads = 0;
    let wholeReads = 0;
    let manifests = 0;
    const io = {
        getAbsolutePath: () => "/fixture/large",
        stat: async () => ({ size, mtime: 1 }),
        readFile: async () => { wholeReads++; throw new Error("unbounded whole source read"); },
    } as any;
    const stats = await repairLargeContent({
        checkContentChunks: async () => [],
        putObjects: async (records: any[]) => {
            if (records.some(record => record.kind === BulkObjectKind.Manifest)) manifests++;
        },
    } as any, io, wasm, { path: "large", hash: hash(1), size }, undefined, {
        ...options(budget),
        qualifyReader: async (absolutePath, expected) => ({
            source: { absolutePath, fingerprint: { ...expected, ctime: 1, device: 1, inode: 1 } },
            reader: {
                verify: async () => {},
                read: async (_offset, requested) => {
                    rangeReads++;
                    check(requested <= tuning().maxFeedBytes, "renderer repair exceeded feed ceiling");
                    return new Uint8Array(requested).fill(1);
                },
                close: async () => {},
            },
        }),
        openReader: async () => ({ verify: async () => {}, read: async () => new Uint8Array(0), close: async () => {} }),
    });
    check(rangeReads === size / tuning().maxFeedBytes && wholeReads === 0 && manifests === 1 &&
        stats.deferred === 0 && stats.uploaded === 1,
    "absent worker did not complete through bounded renderer ranges");
    check(budget.snapshot().usedBytes === 0, "renderer range fallback leaked drained worker scope");
}

async function unconfirmedWorkerTerminationNeverStartsFallback(): Promise<void> {
    const size = 1024 * 1024;
    const budget = new ResourceBudget({ capacityBytes: 32 * 1024 * 1024 });
    const worker = new FakeWorker();
    const pool = new DesktopHashWorkerPool(() => worker as any, 1, 1);
    worker.emit("message", { type: "ready", wasm_mode: "simd" });
    let reads = 0;
    const io = {
        getAbsolutePath: () => "/fixture/source",
        stat: async () => ({ size, mtime: 1 }),
        readFile: async () => { reads++; return new Uint8Array(size); },
    } as any;
    const repairing = repairLargeContent({} as any, io, wasm,
        { path: "source", hash: hash(1), size }, pool, options(budget));
    await worker.entered.promise;
    worker.emit("error", new Error("native worker crash"));
    worker.termination.reject(new Error("native termination failed"));
    const stats = await repairing;
    check(stats.deferred === 1 && reads === 0, "unconfirmed native termination triggered fallback allocation");
    check(budget.snapshot().usedBytes > 0, "failed termination released potentially live worker buffers");
    worker.emit("exit", 1);
    await flush();
    check(budget.snapshot().usedBytes === 0, "eventual native exit failed to drain closed repair owner");
    pool.close();
}

async function rangedQueueAndNativeCloseStayOwned(): Promise<void> {
    const chunkBytes = 4 * 1024 * 1024;
    const size = chunkBytes * 3;
    const budget = new ResourceBudget({ capacityBytes: 32 * 1024 * 1024 });
    const chunks = [2, 3, 4].map((byte, index) => ({ hash: hash(byte), offset: index * chunkBytes, size: chunkBytes }));
    let uploads = 0;
    let reads = 0;
    let closed = false;
    const worker = {
        run: async (_job: unknown, _signal: unknown, lifetime: unknown) => {
            check(!!lifetime && budget.snapshot().usedBytes > 0, "manifest worker lacked native lifetime owner");
            return { mode: "manifest", manifest: { file_hash: hash(1), total_size: size, chunks },
                fingerprint: { size, mtime: 1, ctime: 1, device: 1, inode: 1 }, read_ms: 1, hash_ms: 1 };
        },
    } as any;
    const api = {
        checkContentChunks: async (hashes: string[]) => hashes,
        putObjects: async (records: any[], _perf: unknown, memory: any) => {
            check(budget.snapshot().usedBytes === memory.snapshot().totalBytes, "ranged upload lost parent source scope");
            if (records[0].kind === BulkObjectKind.ContentChunk) {
                uploads++;
                check(records.length === 1 && records[0].data.byteLength === chunkBytes,
                    "32MiB admission allowed default8MiB range queue");
            }
        },
    } as any;
    const stats = await repairLargeContent(api, {
        getAbsolutePath: () => "/fixture/ranged",
        stat: async () => ({ size, mtime: 1 }),
        readFile: async () => { throw new Error("ranged repair read the whole source"); },
    } as any, wasm, { path: "ranged", hash: hash(1), size }, worker, {
        ...options(budget),
        openReader: async () => ({
            verify: async () => {},
            read: async (_offset, requested) => {
                check(budget.snapshot().usedBytes > 0 && uploads === reads,
                    "range read raced admission or retained an ACKed queue");
                reads++;
                return new Uint8Array(requested);
            },
            close: async () => { check(budget.snapshot().usedBytes > 0, "native close outlived range ownership"); closed = true; },
        }),
    });
    check(stats.uploaded === 1 && stats.bytes === size && uploads === 3 && reads === 3 && closed,
        "bounded ranges did not preserve content-before-manifest upload");
    check(budget.snapshot().usedBytes === 0, "ranged native close leaked complete admission");
}

async function abortDuringRangeReadWaitsForNativeClose(): Promise<void> {
    const size = 1024 * 1024;
    const budget = new ResourceBudget({ capacityBytes: 32 * 1024 * 1024 });
    const read = deferred();
    const enteredRead = deferred();
    const close = deferred();
    const enteredClose = deferred();
    const controller = new AbortController();
    let uploads = 0;
    const worker = { run: async () => ({ mode: "manifest",
        manifest: { file_hash: hash(1), total_size: size, chunks: [{ hash: hash(2), offset: 0, size }] },
        fingerprint: { size, mtime: 1, ctime: 1, device: 1, inode: 1 }, read_ms: 1, hash_ms: 1 }) } as any;
    const promise = repairLargeContent({
        checkContentChunks: async () => [hash(2)],
        putObjects: async () => { uploads++; },
    } as any, { getAbsolutePath: () => "/fixture/ranged", stat: async () => ({ size, mtime: 1 }) } as any,
    wasm, { path: "ranged", hash: hash(1), size }, worker, {
        ...options(budget), signal: controller.signal,
        openReader: async () => ({
            verify: async () => {},
            read: async () => { enteredRead.resolve(); await read.promise; return new Uint8Array(size); },
            close: async () => { enteredClose.resolve(); await close.promise; },
        }),
    });
    const outcome = promise.then(() => undefined, error => error);
    await enteredRead.promise;
    controller.abort();
    check(budget.snapshot().usedBytes > 0, "abort released native range read");
    read.resolve();
    await enteredClose.promise;
    check(budget.snapshot().usedBytes > 0, "native reader close escaped cancelled scope");
    close.resolve();
    check((await outcome)?.name === "AbortError" && uploads === 0, "range cancellation was swallowed or uploaded");
    check(budget.snapshot().usedBytes === 0, "cancelled range close leaked owner");
}

// A regression that leaves every promise pending must fail, not silently let
// Node exit successfully with an unresolved ownership/cancellation test.
const watchdog = setTimeout(() => { throw new Error("reconcile ownership test did not settle"); }, 10_000);
void smallRepairValidatesAndIsolatesFailures()
    .then(abortWaitsForEveryNativeSibling)
    .then(queuedShrinkReplansBeforeAnyRead)
    .then(oversizeAndSourceGrowthDoNotReadUnboundedBytes)
    .then(fallbackWaitsForNativeTerminationAndReadmission)
    .then(absentWorkerUsesBoundedRendererRanges)
    .then(unconfirmedWorkerTerminationNeverStartsFallback)
    .then(rangedQueueAndNativeCloseStayOwned)
    .then(abortDuringRangeReadWaitsForNativeClose)
    .then(() => console.log(`reconcile-upload.test: ${assertions} assertions passed`))
    .catch(error => { console.error(error); process.exitCode = 1; })
    .finally(() => clearTimeout(watchdog));
