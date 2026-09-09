import {
    DesktopHashWorkerPool,
    HashWorkerFileDriftError,
    HashWorkerPoolError,
    createDesktopHashWorkerPool,
    desktopHashWorkerCount,
    waitForHashWorkerCleanup,
    type HashWorkerDiagnostic,
    type HashWorkerPoolLifecycleOptions,
} from "./desktop-hash-workers";
import type {
    HashWorkerRequest,
    HashWorkerResponse,
} from "./hash-worker-protocol";
import { MAX_FASTCDC_CHUNK_BYTES } from "./hash-worker-protocol";
import { ResourceBudget } from "./resource-budget";
import { reserveTransientScope } from "./transient-memory";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

class FakeWorker {
    readonly posted: HashWorkerRequest[] = [];
    terminated = false;
    terminationResult: Promise<number> | number = 0;
    private readonly messages: Array<(message: HashWorkerResponse) => void> = [];
    private readonly errors: Array<(error: Error) => void> = [];
    private readonly exits: Array<(code: number) => void> = [];

    postMessage(message: HashWorkerRequest): void {
        this.posted.push(message);
    }

    on(event: "message" | "error" | "exit", listener: any): FakeWorker {
        if (event === "message") this.messages.push(listener);
        else if (event === "error") this.errors.push(listener);
        else this.exits.push(listener);
        return this;
    }

    terminate(): Promise<number> | number {
        this.terminated = true;
        return this.terminationResult;
    }

    emit(message: HashWorkerResponse): void {
        for (const listener of this.messages) listener(message);
    }

    fail(error: Error): void {
        for (const listener of this.errors) listener(error);
    }

    exit(code = 0): void {
        for (const listener of this.exits) listener(code);
    }
}

const jobFrom = (worker: FakeWorker, index = 0) => {
    const job = worker.posted.filter((message) => message.type === "job")[index];
    if (!job || job.type !== "job") throw new Error("expected posted worker job");
    return job;
};

const fingerprintFor = (job: ReturnType<typeof jobFrom>) => ({
    size: job.expected_size,
    mtime: job.expected_mtime,
    ctime: job.expected_mtime + 1,
    device: 7,
    inode: 11,
});

const hashResult = (job: ReturnType<typeof jobFrom>) => ({
    type: "result" as const,
    job_id: job.job_id,
    mode: "hash" as const,
    hash: "a".repeat(64),
    size: job.expected_size,
    mtime: job.expected_mtime,
    fingerprint: fingerprintFor(job),
    read_ms: 2,
    hash_ms: 3,
});

async function schedulerIsBoundedAndMetadataOnly(): Promise<void> {
    check(desktopHashWorkerCount("darwin", "arm64", 8) === 4, "M1 count differs");
    check(desktopHashWorkerCount("win32", "arm64", 8) === 2, "Snapdragon count differs");
    check(desktopHashWorkerCount("linux", "x64", 32) === 4, "x86 count differs");
    check(desktopHashWorkerCount("linux", "x64", 4) === 3, "all cores were claimed");
    check(desktopHashWorkerCount("linux", "x64", 1) === 1, "single-core fallback differs");

    const workers: FakeWorker[] = [];
    const pool = new DesktopHashWorkerPool(() => {
        const worker = new FakeWorker();
        workers.push(worker);
        return worker as any;
    }, 1, 1);
    workers[0].emit({ type: "ready", wasm_mode: "simd" });

    const first = pool.run({
        absolutePath: "/vault/first.md",
        expectedSize: 10,
        expectedMtime: 20,
        mode: "hash",
        feedBytes: 64 * 1024,
    });
    const firstJob = jobFrom(workers[0]);
    check(firstJob.absolute_path === "/vault/first.md", "absolute path missing");
    check(firstJob.expected_size === 10 && firstJob.expected_mtime === 20, "stat missing");
    check(!Object.values(firstJob).some((value) => value instanceof Uint8Array), "bytes crossed boundary");

    const queuedAbort = new AbortController();
    const queued = pool.run({
        absolutePath: "/vault/queued.md",
        expectedSize: 11,
        expectedMtime: 21,
        mode: "hash",
        feedBytes: 64 * 1024,
    }, queuedAbort.signal);
    const overflow = pool.run({
        absolutePath: "/vault/overflow.md",
        expectedSize: 12,
        expectedMtime: 22,
        mode: "hash",
        feedBytes: 64 * 1024,
    });
    await overflow.then(
        () => check(false, "queue overflow resolved"),
        (error) => check(error instanceof HashWorkerPoolError && error.code === "QUEUE_FULL", "queue bound missing"),
    );
    queuedAbort.abort();
    await queued.then(
        () => check(false, "queued cancellation resolved"),
        (error) => check(error.name === "AbortError", "queued cancellation mismatch"),
    );

    workers[0].emit(hashResult(firstJob));
    const result = await first;
    check(result.mode === "hash" && result.hash === "a".repeat(64), "hash result lost");
    check(pool.stats().active === 0 && pool.stats().queued === 0, "pool did not drain");

    const manifestPromise = pool.run({
        absolutePath: "/vault/large.bin",
        expectedSize: 10,
        expectedMtime: 30,
        mode: "manifest",
        feedBytes: 64 * 1024,
    });
    const manifestJob = jobFrom(workers[0], 1);
    workers[0].emit({
        type: "result",
        job_id: manifestJob.job_id,
        mode: "manifest",
        manifest: {
            file_hash: "b".repeat(64),
            total_size: 10,
            chunks: [{ hash: "c".repeat(64), offset: 0, size: 10 }],
        },
        size: 10,
        mtime: 30,
        fingerprint: fingerprintFor(manifestJob),
        read_ms: 2,
        hash_ms: 3,
    });
    const manifest = await manifestPromise;
    check(manifest.mode === "manifest", "manifest result lost its mode");
    check(manifest.mode === "manifest" && manifest.manifest.chunks.length === 1, "manifest metadata lost");
    await pool.close();
    check(workers[0].terminated, "worker was not terminated");
}

async function activeCancellationAndDriftAreTyped(): Promise<void> {
    const worker = new FakeWorker();
    const pool = new DesktopHashWorkerPool(() => worker as any, 1, 2);
    worker.emit({ type: "ready", wasm_mode: "simd" });

    const controller = new AbortController();
    const active = pool.run({
        absolutePath: "/vault/cancel.md",
        expectedSize: 1,
        expectedMtime: 2,
        mode: "hash",
        feedBytes: 64 * 1024,
    }, controller.signal);
    const activeJob = jobFrom(worker);
    controller.abort();
    check(
        worker.posted.some((message) => message.type === "cancel" && message.job_id === activeJob.job_id),
        "active cancel message missing",
    );
    worker.emit(hashResult(activeJob));
    await active.then(
        () => check(false, "active cancellation resolved"),
        (error) => check(error.name === "AbortError", "active cancellation mismatch"),
    );

    const drifted = pool.run({
        absolutePath: "/vault/drift.md",
        expectedSize: 3,
        expectedMtime: 4,
        mode: "hash",
        feedBytes: 64 * 1024,
    });
    const driftJob = jobFrom(worker, 1);
    worker.emit({
        type: "error",
        job_id: driftJob.job_id,
        code: "FILE_DRIFT",
        message: "file metadata changed while hashing",
    });
    await drifted.then(
        () => check(false, "drift resolved"),
        (error) => check(error instanceof HashWorkerFileDriftError, "drift error lost type"),
    );
    await pool.close();
}

async function crashedWorkerRestartsAndBinaryResponseIsRejected(): Promise<void> {
    const workers: FakeWorker[] = [];
    const pool = new DesktopHashWorkerPool(() => {
        const worker = new FakeWorker();
        workers.push(worker);
        return worker as any;
    }, 1, 2);
    workers[0].emit({ type: "ready", wasm_mode: "simd" });
    const failed = pool.run({
        absolutePath: "/vault/fail.md",
        expectedSize: 5,
        expectedMtime: 6,
        mode: "hash",
        feedBytes: 64 * 1024,
    });
    workers[0].fail(new Error("injected worker crash"));
    await failed.then(
        () => check(false, "crashed worker job resolved"),
        (error) => check(error.message === "injected worker crash", "worker crash was hidden"),
    );
    check(workers.length === 2, "worker was not restarted");
    workers[1].emit({ type: "ready", wasm_mode: "simd" });

    const invalid = pool.run({
        absolutePath: "/vault/binary.md",
        expectedSize: 7,
        expectedMtime: 8,
        mode: "hash",
        feedBytes: 64 * 1024,
    });
    const invalidJob = jobFrom(workers[1]);
    workers[1].emit({
        ...hashResult(invalidJob),
        leaked: new Uint8Array([1]),
    } as any);
    await invalid.then(
        () => check(false, "binary worker response resolved"),
        (error) => check(error instanceof HashWorkerPoolError && error.code === "PROTOCOL", "binary response accepted"),
    );
    await pool.close();
}

async function hostileWorkerMetadataIsRejected(): Promise<void> {
    {
        const worker = new FakeWorker();
        const pool = new DesktopHashWorkerPool(() => worker as any, 1, 1);
        worker.emit({ type: "ready", wasm_mode: "simd" });
        const pending = pool.run({
            absolutePath: "/vault/bad-fingerprint.md",
            expectedSize: 9,
            expectedMtime: 10,
            mode: "hash",
            feedBytes: 64 * 1024,
        });
        const job = jobFrom(worker);
        worker.emit({
            ...hashResult(job),
            fingerprint: { ...fingerprintFor(job), inode: -1 },
        });
        await pending.then(
            () => check(false, "invalid fingerprint resolved"),
            (error) => check(
                error instanceof HashWorkerPoolError && error.code === "PROTOCOL",
                "invalid fingerprint was accepted",
            ),
        );
        await pool.close();
    }

    {
        const worker = new FakeWorker();
        const pool = new DesktopHashWorkerPool(() => worker as any, 1, 1);
        worker.emit({ type: "ready", wasm_mode: "simd" });
        const size = MAX_FASTCDC_CHUNK_BYTES + 1;
        const pending = pool.run({
            absolutePath: "/vault/oversized-chunk.bin",
            expectedSize: size,
            expectedMtime: 12,
            mode: "manifest",
            feedBytes: 64 * 1024,
        });
        const job = jobFrom(worker);
        worker.emit({
            type: "result",
            job_id: job.job_id,
            mode: "manifest",
            manifest: {
                file_hash: "b".repeat(64),
                total_size: size,
                chunks: [{ hash: "c".repeat(64), offset: 0, size }],
            },
            size,
            mtime: job.expected_mtime,
            fingerprint: fingerprintFor(job),
            read_ms: 1,
            hash_ms: 1,
        });
        await pending.then(
            () => check(false, "oversized manifest chunk resolved"),
            (error) => check(
                error instanceof HashWorkerPoolError && error.code === "PROTOCOL",
                "oversized manifest chunk was accepted",
            ),
        );
        await pool.close();
    }
}

async function workerLimitGrowsLazilyAndShrinksAfterActiveJobs(): Promise<void> {
    const workers: FakeWorker[] = [];
    const pool = new DesktopHashWorkerPool(() => {
        const worker = new FakeWorker();
        workers.push(worker);
        return worker as any;
    }, 3, 12, 1);
    check(workers.length === 1, "inactive capacity was spawned eagerly");
    check(pool.stats().capacity === 3 && pool.stats().limit === 1, "initial limit missing");
    workers[0].emit({ type: "ready", wasm_mode: "simd" });

    const jobs = [0, 1, 2].map((index) => pool.run({
        absolutePath: `/vault/dynamic-${index}.md`,
        expectedSize: index + 1,
        expectedMtime: index + 10,
        mode: "hash",
        feedBytes: 64 * 1024,
    }));
    pool.setActiveWorkerLimit(3);
    check(workers.length === 3, "worker growth did not spawn lazily");
    workers[1].emit({ type: "ready", wasm_mode: "simd" });
    workers[2].emit({ type: "ready", wasm_mode: "simd" });
    check(pool.stats().active === 3 && pool.stats().queued === 0, "grown pool did not drain queue");

    const posted = workers.map((worker) => jobFrom(worker));
    pool.setActiveWorkerLimit(1);
    check(pool.stats().limit === 1, "reduced worker limit was not applied");
    check(!workers[1].terminated && !workers[2].terminated, "active workers were killed mid-job");
    workers[1].emit(hashResult(posted[1]));
    workers[2].emit(hashResult(posted[2]));
    check(workers[1].terminated && workers[2].terminated, "retiring workers kept resources");
    workers[0].emit(hashResult(posted[0]));
    await Promise.all(jobs);
    check(pool.stats().workers === 1, "retired workers remained live");

    pool.setActiveWorkerLimit(2);
    await Promise.resolve(); // Regrowth waits for the retired thread to terminate.
    check(workers.length === 4, "regrowth did not replace a retired worker");
    workers[3].emit({ type: "ready", wasm_mode: "simd" });
    check(pool.stats().ready === 2, "regrown pool did not become ready");
    let rejected = false;
    try {
        pool.setActiveWorkerLimit(4);
    } catch (error) {
        rejected = error instanceof RangeError;
    }
    check(rejected, "worker limit above capacity was accepted");
    await pool.close();
}

async function failedGrowthRollsBackTheWorkerSet(): Promise<void> {
    const workers: FakeWorker[] = [];
    const pool = new DesktopHashWorkerPool((index) => {
        if (index === 2) throw new Error("injected worker construction failure");
        const worker = new FakeWorker();
        workers.push(worker);
        return worker as any;
    }, 3, 12, 1);
    workers[0].emit({ type: "ready", wasm_mode: "simd" });
    let rejected = false;
    try {
        pool.setActiveWorkerLimit(3);
    } catch (error) {
        rejected = (error as Error).message === "injected worker construction failure";
    }
    check(rejected, "failed worker growth was hidden");
    check(pool.stats().limit === 1, "failed growth changed the active limit");
    check(pool.stats().workers === 1, "failed growth leaked a live worker");
    check(workers[1].terminated, "partially-created worker survived rollback");
    await pool.close();
}

const smallInput = {
    absolutePath: "/private/vault/note.md",
    expectedSize: 1,
    expectedMtime: 2,
    mode: "hash" as const,
    feedBytes: 64 * 1024,
};

function manualTimers() {
    const callbacks = new Map<number, () => void>();
    let nextId = 0;
    const delays: number[] = [];
    const timer: NonNullable<HashWorkerPoolLifecycleOptions["timer"]> = {
        schedule(callback, delayMs) {
            const id = ++nextId;
            callbacks.set(id, callback);
            delays.push(delayMs);
            return id;
        },
        cancel(handle) { callbacks.delete(handle as number); },
    };
    return { timer, callbacks, delays };
}

async function startupFailuresAreTypedBoundedAndFailFast(): Promise<void> {
    for (const failure of ["fatal", "scalar", "error", "timeout"] as const) {
        const diagnostics: HashWorkerDiagnostic[] = [];
        const timers = manualTimers();
        const workers: FakeWorker[] = [];
        const pool = new DesktopHashWorkerPool(() => {
            const worker = new FakeWorker();
            workers.push(worker);
            return worker;
        }, 1, 2, 1, { onDiagnostic: (event) => diagnostics.push(event), timer: timers.timer });
        const pending = pool.run(smallInput).then(
            () => check(false, `${failure}: startup failure resolved a job`),
            (error) => check(error.code === "UNAVAILABLE", `${failure}: startup queue did not fail unavailable`),
        );
        if (failure === "fatal") workers[0].emit({ type: "fatal", message: "/private/vault/secret.js: startup failed" });
        else if (failure === "scalar") workers[0].emit({ type: "ready", wasm_mode: "scalar" });
        else if (failure === "error") workers[0].fail(new Error("/private/vault/token=secret"));
        else {
            check(timers.delays[0] === 15_000, "startup timeout changed unexpectedly");
            [...timers.callbacks.values()][0]();
        }
        check(pool.stats().queued === 0, `${failure}: unavailable startup retained queued jobs`);
        await pending;
        const reason = failure === "scalar" ? "NO_SIMD"
            : failure === "timeout" ? "WORKER_START_TIMEOUT" : "WORKER_INIT";
        check(pool.diagnostics().lastFailure?.reason === reason, `${failure}: diagnostic reason lost`);
        check(pool.diagnostics().startupAttempts === 1, `${failure}: startup retries churned`);
        check(!JSON.stringify(diagnostics).includes("private"), `${failure}: diagnostic leaked a path`);
        check(!JSON.stringify(diagnostics).includes("secret"), `${failure}: diagnostic leaked raw error data`);
        const late = pool.run(smallInput);
        check(pool.stats().queued === 0, `${failure}: future jobs hang after all workers died`);
        await late.then(
            () => check(false, `${failure}: dead pool accepted future job`),
            (error) => check(error.code === "UNAVAILABLE", `${failure}: future job lost unavailable classification`),
        );
        check(workers.length === 1, `${failure}: later run restarted a disabled worker`);
        check(timers.callbacks.size === 0, `${failure}: startup timer leaked`);
        await pool.close();
        check(pool.diagnostics().closed, "closed pool diagnostics look like a running unavailable pool");
    }
}

async function recoveryWaitsForTerminationAndBudgetSurvivesRegrowth(): Promise<void> {
    const slots: FakeWorker[][] = [[], []];
    const diagnostics: HashWorkerDiagnostic[] = [];
    const pool = new DesktopHashWorkerPool((index) => {
        const worker = new FakeWorker();
        slots[index].push(worker);
        return worker;
    }, 2, 4, 2, { onDiagnostic: (event) => diagnostics.push(event) });
    slots[0][0].emit({ type: "ready", wasm_mode: "simd" });
    slots[1][0].emit({ type: "ready", wasm_mode: "simd" });

    let release!: (code: number) => void;
    slots[1][0].terminationResult = new Promise<number>((resolve) => { release = resolve; });
    slots[1][0].fail(new Error("runtime crash"));
    check(slots[1].length === 1, "restart overlapped the previous worker heap");
    check(pool.stats().restarting === 1, "termination wait was hidden from diagnostics");
    release(0);
    await Promise.resolve();
    await Promise.resolve();
    check(slots[1].length === 2, "terminated runtime worker did not restart");

    for (let restart = 1; restart <= 3; restart++) {
        const worker = slots[1][restart];
        worker.emit({ type: "ready", wasm_mode: "simd" });
        worker.fail(new Error("repeated runtime crash"));
        await Promise.resolve();
        await Promise.resolve();
    }
    check(slots[1].length === 4, "runtime restart limit was not three per slot");
    check(pool.diagnostics().restartCount === 3, "restart counter includes non-restarts");
    check(diagnostics.some((event) => event.reason === "RESTART_LIMIT"), "exhausted restart budget was not diagnosed");
    pool.setActiveWorkerLimit(1);
    for (let attempt = 0; attempt < 3; attempt++) {
        let rejected = false;
        try { pool.setActiveWorkerLimit(2); } catch (error) {
            rejected = error instanceof HashWorkerPoolError && error.code === "UNAVAILABLE";
        }
        check(rejected, "governor growth revived an exhausted slot");
    }
    check(slots[1].length === 4, "growth reset the lifetime failure budget");
    const pending = pool.run(smallInput);
    slots[0][0].emit(hashResult(jobFrom(slots[0][0])));
    await pending;
    check(pool.stats().ready === 1, "one exhausted slot disabled healthy peers");
    await pool.close();
}

async function retiredRegrowthAndCloseDoNotOverlapWorkers(): Promise<void> {
    const slots: FakeWorker[][] = [[], []];
    const pool = new DesktopHashWorkerPool((index) => {
        const worker = new FakeWorker();
        slots[index].push(worker);
        return worker;
    }, 2, 4);
    slots[0][0].emit({ type: "ready", wasm_mode: "simd" });
    slots[1][0].emit({ type: "ready", wasm_mode: "simd" });
    let release!: (code: number) => void;
    slots[1][0].terminationResult = new Promise<number>((resolve) => { release = resolve; });
    pool.setActiveWorkerLimit(1);
    pool.setActiveWorkerLimit(2);
    check(slots[1].length === 1, "governor regrowth overlapped a terminating worker");
    const closing = pool.close();
    release(0);
    await closing;
    await Promise.resolve();
    check(slots[1].length === 1, "pending regrowth resurrected a closed pool");
    check(pool.stats().restarting === 0, "close left a planned restart");

    const worker = new FakeWorker();
    const failedTermination = new DesktopHashWorkerPool(() => worker, 1);
    worker.emit({ type: "ready", wasm_mode: "simd" });
    worker.terminationResult = Promise.reject(new Error("private termination error"));
    worker.fail(new Error("runtime crash"));
    await Promise.resolve();
    await Promise.resolve();
    check(failedTermination.diagnostics().lastFailure?.reason === "WORKER_TERMINATION_FAILED", "termination rejection lost its reason");
    check(failedTermination.stats().restarting === 0, "termination failure left recovery pending");
    await failedTermination.close();
}

async function terminationTimeoutCannotHangFutureJobsOrSpawnAnotherHeap(): Promise<void> {
    const timers = manualTimers();
    const workers: FakeWorker[] = [];
    const pool = new DesktopHashWorkerPool(() => {
        const worker = new FakeWorker();
        workers.push(worker);
        return worker;
    }, 1, 2, 1, { timer: timers.timer });
    workers[0].emit({ type: "ready", wasm_mode: "simd" });
    check(timers.callbacks.size === 0, "ready handshake did not clear startup timeout");
    let release!: (code: number) => void;
    workers[0].terminationResult = new Promise<number>((resolve) => { release = resolve; });
    workers[0].fail(new Error("runtime crash with unresponsive termination"));
    const pending = pool.run(smallInput).then(
        () => check(false, "termination timeout unexpectedly ran queued work"),
        (error) => check(error.code === "UNAVAILABLE", "termination timeout did not unblock queue"),
    );
    check(timers.delays.at(-1) === 5_000, "termination wait was not bounded");
    [...timers.callbacks.values()][0]();
    await pending;
    check(pool.diagnostics().lastFailure?.reason === "WORKER_TERMINATION_TIMEOUT", "termination timeout reason missing");
    check(workers.length === 1, "timed-out termination created an overlapping worker heap");
    check(pool.stats().restarting === 0, "timed-out termination left a pending restart");
    await pool.close();
    release(0); // Late completion must not undo the failed lifecycle decision.
    await Promise.resolve();
    await Promise.resolve();
    check(workers.length === 1, "late termination resurrected an unavailable worker");
    check(timers.callbacks.size === 0, "termination timeout timer leaked");
}

async function factoryReportsCapabilitiesWithoutGlobalProcessOrRawErrors(): Promise<void> {
    const events: HashWorkerDiagnostic[] = [];
    const constructed: FakeWorker[] = [];
    const sources: string[] = [];
    class RuntimeWorker extends FakeWorker {
        constructor(source: string) {
            super();
            constructed.push(this);
            sources.push(source);
        }
    }
    const runtimeRequire = (id: string): any => {
        if (id === "node:worker_threads") return { Worker: RuntimeWorker };
        if (id === "node:os") return {
            platform: () => "win32", arch: () => "arm64", availableParallelism: () => 8,
        };
        throw new Error(`unexpected runtime dependency: ${id}`);
    };
    const pool = createDesktopHashWorkerPool("inline worker", {
        runtimeRequire, onDiagnostic: (event) => events.push(event),
    });
    check(pool !== null, "injected Node capabilities failed to create workers");
    check(pool!.stats().limit === 2, "factory used ambient process instead of runtime os/platform");
    check(constructed.length === 2 && sources.every((source) => source === "inline worker"), "worker source or lazy capacity changed");
    constructed.forEach((worker) => worker.emit({ type: "ready", wasm_mode: "simd" }));
    check(events.filter((event) => event.kind === "ready").length === 2, "working capability did not report ready");
    const snapshot = pool!.diagnostics();
    snapshot.startupAttempts = -1;
    check(pool!.diagnostics().startupAttempts === 2, "diagnostic snapshot exposed mutable counters");
    await pool!.close();

    const expectUnavailable = (
        reason: HashWorkerDiagnostic["reason"],
        source: string,
        loader: ((id: string) => any) | null,
        maxWorkers?: number,
    ) => {
        events.length = 0;
        const missing = createDesktopHashWorkerPool(source, {
            runtimeRequire: loader, maxWorkers, onDiagnostic: (event) => events.push(event),
        });
        check(missing === null, `${reason}: unsupported capability created a pool`);
        check(events.at(-1)?.reason === reason, `${reason}: fallback reason was swallowed`);
        check(!JSON.stringify(events).includes("private"), `${reason}: raw failure path leaked`);
    };
    expectUnavailable("WORKER_SOURCE_MISSING", "", runtimeRequire);
    expectUnavailable("NODE_REQUIRE_UNAVAILABLE", "inline", null);
    expectUnavailable("WORKER_THREADS_UNAVAILABLE", "inline", () => { throw new Error("/private/source.js"); });
    expectUnavailable("WORKER_THREADS_UNAVAILABLE", "inline", () => ({ Worker: null }));
    expectUnavailable("OS_INFO_UNAVAILABLE", "inline", (id) => {
        if (id === "node:worker_threads") return { Worker: RuntimeWorker };
        throw new Error("/private/denied-os.js");
    });
    expectUnavailable("INVALID_CONFIGURATION", "inline", runtimeRequire, 9);
    let constructorCalls = 0;
    expectUnavailable("WORKER_CONSTRUCTOR", "inline", (id) => {
        if (id === "node:worker_threads") return { Worker: class {
            constructor() { constructorCalls++; throw new Error("/private/worker.js eval denied"); }
        } };
        return runtimeRequire(id);
    });
    check(constructorCalls === 1, "deterministic constructor failure retried synchronously");
    const safeObserver = createDesktopHashWorkerPool("inline", {
        runtimeRequire, initialWorkers: 1, onDiagnostic: () => { throw new Error("observer failed"); },
    });
    check(safeObserver !== null, "diagnostic observer exception broke worker creation");
    await safeObserver!.close();
}

async function realNodeStartupAndAsyncFailureAreObserved(): Promise<void> {
    const events: HashWorkerDiagnostic[] = [];
    let ready!: () => void;
    const started = new Promise<void>((resolve) => { ready = resolve; });
    const pool = createDesktopHashWorkerPool(
        'require("node:worker_threads").parentPort.postMessage({type:"ready",wasm_mode:"simd"});' +
        'require("node:worker_threads").parentPort.on("message", () => {});',
        { initialWorkers: 1, maxWorkers: 1, onDiagnostic: (event) => {
            events.push(event);
            if (event.kind === "ready") ready();
        } },
    );
    check(pool !== null, "real Node worker constructor unavailable in unit-test runtime");
    await started;
    check(pool!.stats().ready === 1, "real ready handshake was not accepted");
    await pool!.close();

    const failureEvents: HashWorkerDiagnostic[] = [];
    const broken = createDesktopHashWorkerPool('throw new Error("/private/worker-secret.js");', {
        initialWorkers: 1, maxWorkers: 1, onDiagnostic: (event) => failureEvents.push(event),
    });
    check(broken !== null, "async worker failure was incorrectly reported as constructor failure");
    await broken!.run(smallInput).then(
        () => check(false, "startup exception resolved a real worker job"),
        (error) => check(error.code === "UNAVAILABLE", "real startup exception did not reject queue"),
    );
    check(broken!.diagnostics().startupAttempts === 1, "real startup exception spawned repeated workers");
    check(broken!.diagnostics().lastFailure?.reason === "WORKER_INIT", "real startup exception lost its reason");
    check(!JSON.stringify(failureEvents).includes("private"), "real worker error leaked path in diagnostics");
    await broken!.close();
}

async function flushOwnershipMicrotasks(): Promise<void> {
    for (let step = 0; step < 8; step++) await Promise.resolve();
}

async function failedJobsRetainAdmissionUntilNativeTermination(): Promise<void> {
    for (const failure of ["crash", "protocol", "dispatch"] as const) {
        const workers: FakeWorker[] = [];
        const pool = new DesktopHashWorkerPool(() => {
            const worker = new FakeWorker();
            workers.push(worker);
            return worker;
        }, 1, 2);
        const worker = workers[0];
        worker.emit({ type: "ready", wasm_mode: "simd" });
        let terminate!: (code: number) => void;
        worker.terminationResult = new Promise<number>((resolve) => { terminate = resolve; });
        if (failure === "dispatch") worker.postMessage = () => { throw new Error("dispatch failed"); };
        const budget = new ResourceBudget({ capacityBytes: 100 });
        const scope = await reserveTransientScope({ ownerBytes: 60, workBytes: 40 }, { budget });
        const result = pool.run(smallInput, undefined, scope).then(
            () => { throw new Error(`${failure}: failed job resolved`); },
            (error: Error) => error,
        );
        if (failure === "crash") worker.fail(new Error("injected crash"));
        if (failure === "protocol") worker.emit({ ...hashResult(jobFrom(worker)), leaked: new Uint8Array(1) } as any);
        const error = await result;
        check(error instanceof Error, `${failure}: caller did not receive a prompt typed failure`);
        scope.close();
        check(budget.snapshot().usedBytes === 100, `${failure}: native job quota refunded before termination`);
        check(scope.snapshot().trackedWork === 1, `${failure}: actual native lifetime was not retained`);
        let cleanupDone = false;
        const cleanup = waitForHashWorkerCleanup(error).then((confirmed) => { cleanupDone = true; return confirmed; });
        await flushOwnershipMicrotasks();
        check(!cleanupDone, `${failure}: renderer fallback was allowed before native cleanup`);
        terminate(0);
        check(await cleanup, `${failure}: confirmed termination did not allow fallback`);
        await flushOwnershipMicrotasks();
        check(budget.snapshot().usedBytes === 0, `${failure}: confirmed termination leaked admission`);
        check(scope.snapshot().parentReleased, `${failure}: drained parent did not release`);
        await pool.close();
    }
    check(await waitForHashWorkerCleanup(new HashWorkerFileDriftError()), "ordinary settled task error incorrectly required termination");
    check(await waitForHashWorkerCleanup(null), "unrelated failure incorrectly required termination");
}

async function childQuotaAndCloseRetainNativeWork(): Promise<void> {
    const worker = new FakeWorker();
    const pool = new DesktopHashWorkerPool(() => worker, 1);
    worker.emit({ type: "ready", wasm_mode: "simd" });
    let terminate!: (code: number) => void;
    worker.terminationResult = new Promise<number>((resolve) => { terminate = resolve; });
    const budget = new ResourceBudget({ capacityBytes: 100 });
    const scope = await reserveTransientScope({ ownerBytes: 60, workBytes: 40 }, { budget });
    const result = scope.run(40, (context) => pool.run(smallInput, undefined, context)).then(
        () => { throw new Error("closed worker job resolved"); },
        (error: Error) => error,
    );
    await flushOwnershipMicrotasks();
    check(worker.posted.some((message) => message.type === "job"), "child-owned worker never dispatched");
    const closing = pool.close();
    const error = await result;
    check(error instanceof HashWorkerPoolError && error.code === "CLOSED", "close lost typed caller failure");
    scope.close();
    check(scope.snapshot().work.usedBytes === 40, "close refunded child quota still used by native work");
    check(budget.snapshot().usedBytes === 100, "close refunded parent quota still used by native work");
    terminate(0);
    check(await waitForHashWorkerCleanup(error), "close cleanup was not confirmed");
    await closing;
    await flushOwnershipMicrotasks();
    check(scope.snapshot().work.usedBytes === 0 && budget.snapshot().usedBytes === 0,
        "native close did not release child and parent together");
}

async function failedTerminationNeverFakesNativeRelease(): Promise<void> {
    for (const failure of ["timeout", "reject", "throw"] as const) {
        const timers = manualTimers();
        const worker = new FakeWorker();
        const pool = new DesktopHashWorkerPool(() => worker, 1, 2, 1, { timer: timers.timer });
        worker.emit({ type: "ready", wasm_mode: "simd" });
        let terminate!: (code: number) => void;
        worker.terminationResult = failure === "reject"
            ? Promise.reject(new Error("termination rejected"))
            : new Promise<number>((resolve) => { terminate = resolve; });
        if (failure === "throw") worker.terminate = () => { throw new Error("termination threw"); };
        const budget = new ResourceBudget({ capacityBytes: 100 });
        const scope = await reserveTransientScope({ ownerBytes: 60, workBytes: 40 }, { budget });
        const result = pool.run(smallInput, undefined, scope).catch((error: Error) => error);
        worker.fail(new Error("native cleanup uncertain"));
        const error = await result;
        scope.close();
        if (failure === "timeout") [...timers.callbacks.values()][0]();
        check(!await waitForHashWorkerCleanup(error), `${failure}: unavailable termination allowed renderer reuse`);
        await flushOwnershipMicrotasks();
        check(budget.snapshot().usedBytes === 100, `${failure}: failed termination falsely released job buffers`);
        await pool.close();
        check(budget.snapshot().usedBytes === 100, `${failure}: pool close falsely confirmed native cleanup`);
        worker.exit();
        await flushOwnershipMicrotasks();
        check(budget.snapshot().usedBytes === 0, `${failure}: late native exit did not release held admission`);
        check(await waitForHashWorkerCleanup(error), `${failure}: late exit was not authoritative`);
        if (failure === "timeout") terminate(0);
        check(timers.callbacks.size === 0, `${failure}: native cleanup timer leaked`);
    }
}

async function normalRepliesAndQueuedCancellationReleaseExactlyTheirJobs(): Promise<void> {
    const worker = new FakeWorker();
    const pool = new DesktopHashWorkerPool(() => worker, 1, 2);
    worker.emit({ type: "ready", wasm_mode: "simd" });
    const budget = new ResourceBudget({ capacityBytes: 100 });
    const scope = await reserveTransientScope({ ownerBytes: 60, workBytes: 40 }, { budget });
    const active = pool.run(smallInput, undefined, scope);
    const abort = new AbortController();
    const queued = pool.run(smallInput, abort.signal, scope).catch((error: Error) => error);
    check(scope.snapshot().trackedWork === 1, "queued metadata-only job retained a native allowance");
    abort.abort();
    const error = await queued;
    check(error instanceof Error && error.name === "AbortError", "queued cancellation did not settle");
    check(await waitForHashWorkerCleanup(error), "never-started queued cancellation waits for native cleanup");
    worker.emit(hashResult(jobFrom(worker)));
    await active;
    await flushOwnershipMicrotasks();
    check(scope.snapshot().trackedWork === 0, "normal reply did not release native lifetime");
    scope.close();
    check(budget.snapshot().usedBytes === 0, "successful job retained the parent after close");

    const rejected = await pool.run(smallInput, undefined, scope).catch((failure: Error) => failure);
    check(rejected instanceof Error, "closed tracker accepted native allocation");
    check(worker.posted.filter((message) => message.type === "job").length === 1,
        "closed tracker dispatched a job before admission");
    await pool.close();
}

async function nativeDrainOutlivesBoundedTerminationFailure(): Promise<void> {
    for (const outcome of ["timeout-exit", "timeout-completion", "rejection", "throw"] as const) {
        const timers = manualTimers();
        const worker = new FakeWorker();
        const pool = new DesktopHashWorkerPool(() => worker, 1, 2, 1, { timer: timers.timer });
        worker.emit({ type: "ready", wasm_mode: "simd" });
        let complete!: (code: number) => void;
        worker.terminationResult = outcome === "rejection" ? Promise.reject(new Error("terminate rejected"))
            : new Promise<number>(resolve => { complete = resolve; });
        if (outcome === "throw") worker.terminate = () => { throw new Error("terminate threw"); };
        const closing = pool.close();
        const drain = pool.closeAndDrainNative();
        let drained = false;
        void drain.then(() => { drained = true; });
        check(drain === pool.closeAndDrainNative(), `${outcome}: native drain identity changed while pending`);
        if (outcome.startsWith("timeout")) [...timers.callbacks.values()].forEach(callback => callback());
        await closing;
        await flushOwnershipMicrotasks();
        check(!drained, `${outcome}: bounded close falsely completed actual native drain`);
        check(pool.diagnostics().nativeOwnedWorkers === 1 && !pool.diagnostics().nativeDrained,
            `${outcome}: uncertain native ownership was dropped from diagnostics`);
        if (outcome === "timeout-completion") complete(0);
        else worker.exit();
        await drain;
        check(pool.diagnostics().nativeOwnedWorkers === 0 && pool.diagnostics().nativeDrained,
            `${outcome}: actual native completion did not release drain`);
        check(drain === pool.closeAndDrainNative(), `${outcome}: completed native drain identity changed`);
        if (outcome === "timeout-exit") complete(0); // A second authoritative signal is idempotent.
        await flushOwnershipMicrotasks();
        check(pool.diagnostics().nativeOwnedWorkers === 0, `${outcome}: duplicate native confirmation underflowed owners`);
        check(timers.callbacks.size === 0, `${outcome}: native drain introduced/leaked a timer`);
        pool.setActiveWorkerLimit(1);
        await pool.run(smallInput).then(() => check(false, "closed drain accepted a new job"),
            error => check(error.code === "CLOSED", "drained pool lost closed admission"));
    }
}

async function nativeDrainWaitsForEveryRetiredAndStartingWorker(): Promise<void> {
    const timers = manualTimers();
    const workers: FakeWorker[] = [];
    const completions: Array<(code: number) => void> = [];
    const pool = new DesktopHashWorkerPool(() => {
        const worker = new FakeWorker();
        worker.terminationResult = new Promise<number>(resolve => { completions.push(resolve); });
        workers.push(worker);
        return worker;
    }, 3, 4, 3, { timer: timers.timer });
    workers[0].emit({ type: "ready", wasm_mode: "simd" });
    workers[2].emit({ type: "ready", wasm_mode: "simd" });
    // Worker 1 is still starting; worker 2 is retired before pool close.
    pool.setActiveWorkerLimit(2);
    check(workers[2].terminated, "retired slot never began native termination");
    const drain = pool.closeAndDrainNative();
    let drained = false;
    void drain.then(() => { drained = true; });
    [...timers.callbacks.values()].forEach(callback => callback());
    await flushOwnershipMicrotasks();
    await pool.close();
    check(!drained && pool.diagnostics().nativeOwnedWorkers === 3, "bounded close lost starting/retired owners");
    workers[1].exit();
    await flushOwnershipMicrotasks();
    check(!drained && pool.diagnostics().nativeOwnedWorkers === 2, "one peer exit completed all-worker drain");
    completions[2](0);
    await flushOwnershipMicrotasks();
    check(!drained && pool.diagnostics().nativeOwnedWorkers === 1, "retired-slot completion released a healthy peer");
    workers[0].exit();
    await drain;
    check(pool.diagnostics().nativeDrained, "all workers exited but pool did not drain");
    completions[0](0); completions[1](0);
    await flushOwnershipMicrotasks();
    check(workers.length === 3, "native completion spawned work after close");
    check(timers.callbacks.size === 0, "multi-worker native drain leaked timers");
}

async function nativeDrainIncludesReplacedSlotsAndEmptyClosedPool(): Promise<void> {
    const workers: FakeWorker[] = [];
    const pool = new DesktopHashWorkerPool(() => { const worker = new FakeWorker(); workers.push(worker); return worker; }, 1);
    workers[0].emit({ type: "ready", wasm_mode: "simd" });
    workers[0].exit();
    await flushOwnershipMicrotasks();
    check(workers.length === 2, "native-exit recovery did not create replacement fixture");
    check(pool.diagnostics().nativeOwnedWorkers === 1, "confirmed replaced slot remained retained as a native owner");
    workers[1].emit({ type: "fatal", message: "fixture startup failed" });
    await flushOwnershipMicrotasks();
    check(pool.diagnostics().nativeOwnedWorkers === 0, "failed startup's actual termination was not confirmed");
    await pool.close();
    const drain = pool.closeAndDrainNative();
    await drain;
    check(drain === pool.closeAndDrainNative(), "already empty/closed pool does not have stable native drain");
    check(pool.diagnostics().nativeDrained, "empty closed pool was not drained");
}

async function constructorFailureHandsOffActualNativeLifetime(): Promise<void> {
    for (const listener of ["message", "exit"] as const) {
        let worker!: FakeWorker;
        let complete!: (code: number) => void;
        let nativeDrain: Promise<void> | undefined;
        class BrokenListenerWorker extends FakeWorker {
            constructor() {
                super(); worker = this;
                this.terminationResult = new Promise<number>(resolve => { complete = resolve; });
            }
            override on(event: "message" | "error" | "exit", handler: any): FakeWorker {
                if (event === listener) throw new Error("fixture listener registration failed");
                return super.on(event, handler);
            }
        }
        const pool = createDesktopHashWorkerPool("fixture source", { initialWorkers: 1, maxWorkers: 1,
            runtimeRequire: id => id === "node:worker_threads" ? { Worker: BrokenListenerWorker }
                : { platform: () => "linux", arch: () => "x64", availableParallelism: () => 2 },
            onConstructionCleanup: promise => { nativeDrain = promise; },
        });
        check(pool === null, `${listener}: failed construction returned a usable pool`);
        check(nativeDrain instanceof Promise, `${listener}: null factory dropped the actual native cleanup lifetime`);
        let drained = false;
        void nativeDrain!.then(() => { drained = true; });
        await flushOwnershipMicrotasks();
        check(!drained, `${listener}: constructor rollback was confused with native termination`);
        worker.exit();
        await flushOwnershipMicrotasks();
        if (listener === "message") check(drained, "exit listener was not installed before failing message listener");
        else check(!drained, "failed exit listener pretended to observe native exit");
        complete(0);
        await nativeDrain;
    }
    const worker = new FakeWorker();
    let complete!: (code: number) => void;
    worker.terminationResult = new Promise<number>(resolve => { complete = resolve; });
    const original = new Error("second constructor failed");
    let drain!: Promise<void>;
    let caught: unknown;
    try {
        new DesktopHashWorkerPool(index => { if (index === 1) throw original; return worker; }, 2, 4, 2, {
            onConstructionCleanup: promise => { drain = promise; throw new Error("observer must not mask constructor error"); },
        });
    } catch (error) { caught = error; }
    check(caught === original, "construction-cleanup observer hid the original constructor failure");
    check(drain instanceof Promise && worker.terminated, "partial constructor did not retain/terminate its earlier worker");
    let drained = false;
    void drain.then(() => { drained = true; });
    await flushOwnershipMicrotasks();
    check(!drained, "partial constructor cleanup resolved before actual earlier worker exit");
    worker.exit();
    await drain;
    complete(0);
}

async function nativeDrainProtectsReentrantConstruction(): Promise<void> {
    const workers: FakeWorker[] = [];
    let pool!: DesktopHashWorkerPool;
    let drain!: Promise<void>;
    let complete!: (code: number) => void;
    pool = new DesktopHashWorkerPool(index => {
        const worker = new FakeWorker();
        if (index === 1) {
            drain = pool.closeAndDrainNative();
            check(pool.diagnostics().nativeSpawnsInProgress === 1, "factory call lacks a synchronous native owner");
            worker.terminationResult = new Promise<number>(resolve => { complete = resolve; });
        }
        workers.push(worker);
        return worker;
    }, 2, 4, 1);
    workers[0].emit({ type: "ready", wasm_mode: "simd" });
    pool.setActiveWorkerLimit(2);
    let drained = false;
    void drain.then(() => { drained = true; });
    await flushOwnershipMicrotasks();
    check(!drained && workers[1].terminated, "reentrant close lost the factory's not-yet-returned native worker");
    complete(0);
    await drain;
    check(pool.diagnostics().nativeDrained, "reentrant factory worker did not release native drain");

    let calls = 0;
    let stopping!: DesktopHashWorkerPool;
    stopping = new DesktopHashWorkerPool(() => { calls++; return new FakeWorker(); }, 2, 4, 1, {
        onDiagnostic: event => { if (event.reason === "STARTING" && event.slot === 1) void stopping.closeAndDrainNative(); },
    });
    let rejected = false;
    try { stopping.setActiveWorkerLimit(2); } catch (error) { rejected = error instanceof HashWorkerPoolError && error.code === "CLOSED"; }
    await stopping.closeAndDrainNative();
    check(rejected && calls === 1, "diagnostic reentrancy constructed a new worker after native close");
}

let workerTestsCompleted = false;
process.once("beforeExit", () => {
    if (!workerTestsCompleted) {
        console.error("desktop hash worker tests did not reach actual drain completion");
        process.exitCode = 1;
    }
});

void schedulerIsBoundedAndMetadataOnly()
    .then(activeCancellationAndDriftAreTyped)
    .then(crashedWorkerRestartsAndBinaryResponseIsRejected)
    .then(hostileWorkerMetadataIsRejected)
    .then(workerLimitGrowsLazilyAndShrinksAfterActiveJobs)
    .then(failedGrowthRollsBackTheWorkerSet)
    .then(startupFailuresAreTypedBoundedAndFailFast)
    .then(recoveryWaitsForTerminationAndBudgetSurvivesRegrowth)
    .then(retiredRegrowthAndCloseDoNotOverlapWorkers)
    .then(terminationTimeoutCannotHangFutureJobsOrSpawnAnotherHeap)
    .then(factoryReportsCapabilitiesWithoutGlobalProcessOrRawErrors)
    .then(realNodeStartupAndAsyncFailureAreObserved)
    .then(failedJobsRetainAdmissionUntilNativeTermination)
    .then(childQuotaAndCloseRetainNativeWork)
    .then(failedTerminationNeverFakesNativeRelease)
    .then(normalRepliesAndQueuedCancellationReleaseExactlyTheirJobs)
    .then(nativeDrainOutlivesBoundedTerminationFailure)
    .then(nativeDrainWaitsForEveryRetiredAndStartingWorker)
    .then(nativeDrainIncludesReplacedSlotsAndEmptyClosedPool)
    .then(constructorFailureHandsOffActualNativeLifetime)
    .then(nativeDrainProtectsReentrantConstruction)
    .then(() => {
        workerTestsCompleted = true;
        console.log(`desktop-hash-workers.test: ${assertions} assertions passed`);
    })
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
