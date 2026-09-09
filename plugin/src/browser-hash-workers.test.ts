import assert from "node:assert/strict";
import {
    BrowserHashWorkerError,
    BrowserHashWorkerPool,
    createBrowserHashWorkerPool,
    type BrowserHashWorkerPoolOptions,
    type BrowserHashWorkerHost,
    type BrowserWorkerLike,
} from "./browser-hash-workers";
import {
    BROWSER_HASH_WORKER_SCHEMA,
    type BrowserHashWorkerRequest,
    type BrowserHashWorkerResponse,
} from "./browser-hash-worker-protocol";

class FakeBrowserWorker implements BrowserWorkerLike {
    onmessage: ((event: MessageEvent<BrowserHashWorkerResponse>) => void) | null = null;
    onerror: ((event: ErrorEvent) => void) | null = null;
    onmessageerror: ((event: MessageEvent) => void) | null = null;
    readonly posted: BrowserHashWorkerRequest[] = [];
    terminated = false;

    postMessage(message: BrowserHashWorkerRequest, transfer: Transferable[]): void {
        this.posted.push(structuredClone(message, { transfer }) as BrowserHashWorkerRequest);
    }

    emit(message: BrowserHashWorkerResponse, transfer: Transferable[] = []): void {
        const delivered = structuredClone(message, { transfer }) as BrowserHashWorkerResponse;
        this.onmessage?.({ data: delivered } as MessageEvent<BrowserHashWorkerResponse>);
    }

    crash(): void { this.onerror?.({} as ErrorEvent); }
    terminate(): void { this.terminated = true; }
}

class NonTransferWorker extends FakeBrowserWorker {
    override postMessage(message: BrowserHashWorkerRequest): void {
        this.posted.push(message);
    }
}

function fakeHost(worker: FakeBrowserWorker, available = true): BrowserHashWorkerHost & { revoked: string[] } {
    const revoked: string[] = [];
    return {
        revoked,
        available: () => available,
        createUrl: () => "blob:worker",
        revokeUrl: url => { revoked.push(url); },
        createWorker: () => worker,
    };
}

const poolOptions = (host: BrowserHashWorkerHost,
    extra: Partial<BrowserHashWorkerPoolOptions> = {}): BrowserHashWorkerPoolOptions => ({
    host,
    scalarWasm: Uint8Array.from([0, 97, 115, 109, 1, 0, 0, 0]).buffer,
    simdWasm: Uint8Array.from([0, 97, 115, 109, 1, 0, 0, 0]).buffer,
    ...extra,
});

function initFrom(worker: FakeBrowserWorker) {
    const init = worker.posted.find(message => message.type === "init");
    assert(init?.type === "init");
    return init;
}

async function qualify(pool: BrowserHashWorkerPool, worker: FakeBrowserWorker, mode: "scalar" | "simd" = "simd") {
    const init = initFrom(worker);
    worker.emit({ schema: BROWSER_HASH_WORKER_SCHEMA, type: "qualified",
        generation: init.generation, qualification: init.qualification,
        scalarWasm: init.scalarWasm, simdWasm: init.simdWasm, wasmMode: mode,
        wasmHeapBytes: 1024 * 1024 },
    [init.qualification, init.scalarWasm, init.simdWasm]);
    await pool.ready();
}

function hashJobFrom(worker: FakeBrowserWorker, index = 0) {
    const job = worker.posted.filter(message => message.type === "hash")[index];
    assert(job?.type === "hash");
    return job;
}

async function qualifiedTransferAndGenerationFence(): Promise<void> {
    const worker = new FakeBrowserWorker();
    const host = fakeHost(worker);
    const pool = new BrowserHashWorkerPool("worker source", poolOptions(host));
    const init = initFrom(worker);
    assert.equal(init.qualification.byteLength, 64 * 1024);
    await qualify(pool, worker);
    assert.deepEqual(pool.stats(), { state: "ready", wasmMode: "simd", active: 0, queued: 0,
        ownedBytes: 0, maxOwnedBytes: 16 * 1024 * 1024, sourceChars: 13,
        wasmHeapBytes: 1024 * 1024, quarantinedPayloadBytes: 0, fallbackSafe: true,
        generation: pool.generation });

    const original = new Uint8Array([1, 2, 3, 4]);
    const result = pool.run(original, { feedBytes: 64 * 1024 });
    assert.equal(original.byteLength, 0, "production input was copied instead of transferred");
    const job = hashJobFrom(worker);
    const listener = worker.onmessage!;
    listener({ data: { schema: BROWSER_HASH_WORKER_SCHEMA, type: "result",
        generation: pool.generation + 1, jobId: job.jobId, bytes: new ArrayBuffer(4),
        hash: "f".repeat(64), hashMs: 1, wasmHeapBytes: 1024 * 1024 } } as MessageEvent<any>);
    assert.equal(pool.stats().active, 1, "stale generation settled active work");
    worker.emit({ schema: BROWSER_HASH_WORKER_SCHEMA, type: "result", generation: pool.generation,
        jobId: job.jobId, bytes: job.bytes, hash: "a".repeat(64), hashMs: 2,
        wasmHeapBytes: 1024 * 1024 }, [job.bytes]);
    const completed = await result;
    assert.equal(completed.hash, "a".repeat(64));
    assert.deepEqual([...completed.returnedBytes], [1, 2, 3, 4]);
    assert.equal(pool.diagnostics().completedJobs, 1);
    pool.close();
    assert(worker.terminated && host.revoked.length === 1);
}

async function abortReturnsOwnershipAndQueueIsBounded(): Promise<void> {
    const worker = new FakeBrowserWorker();
    const pool = new BrowserHashWorkerPool("source", poolOptions(fakeHost(worker), { maxQueuedJobs: 2 }));
    await qualify(pool, worker, "scalar");
    const abort = new AbortController();
    const active = pool.run(new Uint8Array([9, 8]), { feedBytes: 64 * 1024, signal: abort.signal });
    const activeJob = hashJobFrom(worker);
    const queuedAbort = new AbortController();
    const queued = pool.run(new Uint8Array([7]), { feedBytes: 64 * 1024, signal: queuedAbort.signal });
    await assert.rejects(pool.run(new Uint8Array([6]), { feedBytes: 64 * 1024 }),
        (error: unknown) => error instanceof BrowserHashWorkerError && error.code === "UNAVAILABLE");
    queuedAbort.abort();
    await assert.rejects(queued, { name: "AbortError" });
    abort.abort();
    assert(worker.posted.some(message => message.type === "cancel" && message.jobId === activeJob.jobId));
    worker.emit({ schema: BROWSER_HASH_WORKER_SCHEMA, type: "cancelled", generation: pool.generation,
        jobId: activeJob.jobId, bytes: activeJob.bytes }, [activeJob.bytes]);
    await assert.rejects(active, { name: "AbortError" });
    assert.equal(pool.stats().ownedBytes, 0);
    assert.equal(pool.diagnostics().cancelledJobs, 2);
    pool.close();
}

async function crashDisablesGenerationAndRejectsOwnedWork(): Promise<void> {
    const worker = new FakeBrowserWorker();
    const pool = new BrowserHashWorkerPool("source", poolOptions(fakeHost(worker)));
    await qualify(pool, worker);
    const active = pool.run(new Uint8Array([1]), { feedBytes: 64 * 1024 });
    worker.crash();
    await assert.rejects(active,
        (error: unknown) => error instanceof BrowserHashWorkerError && error.code === "CRASH" &&
            error.fallbackSafe === false && error.unconfirmedReleaseBytes >= 1);
    assert.equal(pool.stats().state, "unavailable");
    assert.equal(pool.stats().ownedBytes, 0);
    assert.equal(pool.stats().quarantinedPayloadBytes, 1);
    assert.equal(pool.stats().fallbackSafe, false);
    assert.equal(pool.diagnostics().cleanupEvidence, "shutdown-unconfirmed");
    await assert.rejects(pool.run(new Uint8Array([2]), { feedBytes: 64 * 1024 }),
        (error: unknown) => error instanceof BrowserHashWorkerError && error.code === "UNAVAILABLE");
    assert(worker.terminated);
}

async function unsupportedAndFailedQualificationNeverAdvertiseReady(): Promise<void> {
    const worker = new FakeBrowserWorker();
    const diagnostics: string[] = [];
    assert.equal(createBrowserHashWorkerPool("source", {
        ...poolOptions(fakeHost(worker, false)), onDiagnostic: event => diagnostics.push(event.reason),
    }), null);
    assert.deepEqual(diagnostics, ["UNAVAILABLE"]);

    const failedWorker = new FakeBrowserWorker();
    const pool = new BrowserHashWorkerPool("source", poolOptions(fakeHost(failedWorker)));
    const init = initFrom(failedWorker);
    const corrupt = init.qualification;
    new Uint8Array(corrupt)[10] ^= 1;
    failedWorker.emit({ schema: BROWSER_HASH_WORKER_SCHEMA, type: "qualified",
        generation: pool.generation, qualification: corrupt, scalarWasm: init.scalarWasm,
        simdWasm: init.simdWasm, wasmMode: "simd", wasmHeapBytes: 1024 * 1024 },
    [corrupt, init.scalarWasm, init.simdWasm]);
    await assert.rejects(pool.ready(),
        (error: unknown) => error instanceof BrowserHashWorkerError && error.code === "QUALIFICATION_FAILED");
    assert.equal(pool.stats().state, "unavailable");
    assert(failedWorker.terminated);

    const noTransfer = new NonTransferWorker();
    const noTransferPool = new BrowserHashWorkerPool("source", poolOptions(fakeHost(noTransfer)));
    await assert.rejects(noTransferPool.ready(),
        (error: unknown) => error instanceof BrowserHashWorkerError && error.code === "TRANSFER_UNSUPPORTED");
    assert.equal(noTransferPool.stats().state, "unavailable");
}

async function byteAdmissionAndProtocolFaultAreFailClosed(): Promise<void> {
    const worker = new FakeBrowserWorker();
    const pool = new BrowserHashWorkerPool("source", poolOptions(fakeHost(worker), {
        maxOwnedBytes: 8 * 1024 * 1024,
    }));
    await qualify(pool, worker);
    const full = pool.run(new Uint8Array(8 * 1024 * 1024), { feedBytes: 64 * 1024 });
    await assert.rejects(pool.run(new Uint8Array(1), { feedBytes: 64 * 1024 }),
        (error: unknown) => error instanceof BrowserHashWorkerError && error.code === "UNAVAILABLE");
    const job = hashJobFrom(worker);
    worker.emit({ schema: BROWSER_HASH_WORKER_SCHEMA, type: "result", generation: pool.generation,
        jobId: job.jobId, bytes: job.bytes, hash: "not-a-hash", hashMs: 1,
        wasmHeapBytes: 1024 * 1024 }, [job.bytes]);
    await assert.rejects(full,
        (error: unknown) => error instanceof BrowserHashWorkerError && error.code === "PROTOCOL");
    assert.equal(pool.stats().state, "unavailable");
    assert.equal(pool.stats().ownedBytes, 0);
}

async function hungJobTimesOutAndRetiresWorker(): Promise<void> {
    const worker = new FakeBrowserWorker();
    const pool = new BrowserHashWorkerPool("source", poolOptions(fakeHost(worker), {
        jobTimeoutMs: 5,
    }));
    await qualify(pool, worker);
    const hung = pool.run(new Uint8Array([1, 2]), { feedBytes: 64 * 1024 });
    await assert.rejects(hung,
        (error: unknown) => error instanceof BrowserHashWorkerError && error.code === "JOB_TIMEOUT" &&
            error.fallbackSafe === false);
    assert.equal(pool.stats().state, "unavailable");
    assert.equal(pool.stats().quarantinedPayloadBytes, 2);
    assert(worker.terminated);
}

async function hungCancellationUsesShortRetirementBound(): Promise<void> {
    const worker = new FakeBrowserWorker();
    const pool = new BrowserHashWorkerPool("source", poolOptions(fakeHost(worker), {
        jobTimeoutMs: 1000, cancelTimeoutMs: 5,
    }));
    await qualify(pool, worker);
    const abort = new AbortController();
    const hung = pool.run(new Uint8Array([1, 2]), { feedBytes: 64 * 1024, signal: abort.signal });
    abort.abort();
    await assert.rejects(hung, { name: "AbortError" });
    assert.equal(pool.stats().state, "unavailable");
    assert.equal(pool.stats().fallbackSafe, false,
        "cancel timeout falsely authorized a same-scope renderer fallback");
    assert.equal(pool.stats().quarantinedPayloadBytes, 2);
    assert(worker.terminated);
}

async function main(): Promise<void> {
    await qualifiedTransferAndGenerationFence();
    await abortReturnsOwnershipAndQueueIsBounded();
    await crashDisablesGenerationAndRejectsOwnedWork();
    await unsupportedAndFailedQualificationNeverAdvertiseReady();
    await byteAdmissionAndProtocolFaultAreFailClosed();
    await hungJobTimesOutAndRetiresWorker();
    await hungCancellationUsesShortRetirementBound();
    console.log("browser hash worker tests passed");
}

void main().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
