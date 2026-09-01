import {
    DesktopHashWorkerPool,
    HashWorkerFileDriftError,
    HashWorkerPoolError,
    desktopHashWorkerCount,
} from "./desktop-hash-workers";
import type {
    HashWorkerRequest,
    HashWorkerResponse,
} from "./hash-worker-protocol";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

class FakeWorker {
    readonly posted: HashWorkerRequest[] = [];
    terminated = false;
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

    terminate(): number {
        this.terminated = true;
        return 0;
    }

    emit(message: HashWorkerResponse): void {
        for (const listener of this.messages) listener(message);
    }

    fail(error: Error): void {
        for (const listener of this.errors) listener(error);
    }
}

const jobFrom = (worker: FakeWorker, index = 0) => {
    const job = worker.posted.filter((message) => message.type === "job")[index];
    if (!job || job.type !== "job") throw new Error("expected posted worker job");
    return job;
};

const hashResult = (job: ReturnType<typeof jobFrom>) => ({
    type: "result" as const,
    job_id: job.job_id,
    mode: "hash" as const,
    hash: "a".repeat(64),
    size: job.expected_size,
    mtime: job.expected_mtime,
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

void schedulerIsBoundedAndMetadataOnly()
    .then(activeCancellationAndDriftAreTyped)
    .then(crashedWorkerRestartsAndBinaryResponseIsRejected)
    .then(() => console.log(`desktop-hash-workers.test: ${assertions} assertions passed`))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
