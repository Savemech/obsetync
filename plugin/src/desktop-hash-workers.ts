import type {
    HashWorkerErrorCode,
    HashWorkerJob,
    HashWorkerMode,
    HashWorkerRequest,
    HashWorkerResponse,
    HashWorkerResult,
} from "./hash-worker-protocol";

export interface HashWorkerInput {
    absolutePath: string;
    expectedSize: number;
    expectedMtime: number;
    mode: HashWorkerMode;
    feedBytes: number;
}

export interface HashWorkerPoolStats {
    workers: number;
    ready: number;
    active: number;
    queued: number;
    wasmMode: "simd" | "starting" | "unavailable";
}

interface WorkerLike {
    postMessage(message: HashWorkerRequest): void;
    on(event: "message", listener: (message: HashWorkerResponse) => void): WorkerLike;
    on(event: "error", listener: (error: Error) => void): WorkerLike;
    on(event: "exit", listener: (code: number) => void): WorkerLike;
    terminate(): Promise<number> | number;
}

export type HashWorkerFactory = (index: number) => WorkerLike;

export class HashWorkerPoolError extends Error {
    constructor(message: string, readonly code: string) {
        super(message);
        this.name = "HashWorkerPoolError";
    }
}

export class HashWorkerFileDriftError extends HashWorkerPoolError {
    constructor(message = "file changed while hashing") {
        super(message, "FILE_DRIFT");
        this.name = "HashWorkerFileDriftError";
    }
}

interface PendingJob {
    request: HashWorkerJob;
    resolve: (value: HashWorkerResult) => void;
    reject: (error: Error) => void;
    signal?: AbortSignal;
    abortListener?: () => void;
    abortRequested: boolean;
}

interface WorkerSlot {
    index: number;
    worker: WorkerLike;
    ready: boolean;
    dead: boolean;
    restarts: number;
    current: PendingJob | null;
    startupTimer: ReturnType<typeof setTimeout> | null;
}

const MAX_RESTARTS_PER_SLOT = 3;
const WORKER_START_TIMEOUT_MS = 15_000;

function abortError(): Error {
    const error = new Error("hash worker job aborted");
    error.name = "AbortError";
    return error;
}

function containsBinary(value: unknown, seen = new Set<object>()): boolean {
    if (
        value instanceof ArrayBuffer ||
        ArrayBuffer.isView(value) ||
        (typeof Buffer !== "undefined" && Buffer.isBuffer(value))
    ) {
        return true;
    }
    if (value === null || typeof value !== "object") return false;
    if (seen.has(value)) return false;
    seen.add(value);
    for (const nested of Object.values(value as Record<string, unknown>)) {
        if (containsBinary(nested, seen)) return true;
    }
    return false;
}

function workerFailure(code: HashWorkerErrorCode, message: string): Error {
    if (code === "FILE_DRIFT") return new HashWorkerFileDriftError(message);
    if (code === "CANCELLED") return abortError();
    return new HashWorkerPoolError(message, code);
}

function validHash(value: unknown): value is string {
    return typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
}

function validResult(message: HashWorkerResult, request: HashWorkerJob): boolean {
    if (
        message.size !== request.expected_size ||
        !Number.isFinite(message.mtime) ||
        Math.abs(message.mtime - request.expected_mtime) > 1 ||
        !Number.isFinite(message.read_ms) || message.read_ms < 0 ||
        !Number.isFinite(message.hash_ms) || message.hash_ms < 0
    ) {
        return false;
    }
    if (message.mode === "hash") return validHash(message.hash);
    const manifest = message.manifest;
    if (
        !manifest || !validHash(manifest.file_hash) ||
        manifest.total_size !== request.expected_size ||
        !Array.isArray(manifest.chunks)
    ) {
        return false;
    }
    let offset = 0;
    for (const chunk of manifest.chunks) {
        if (
            !chunk || !validHash(chunk.hash) ||
            !Number.isSafeInteger(chunk.offset) || chunk.offset !== offset ||
            !Number.isSafeInteger(chunk.size) || chunk.size <= 0
        ) {
            return false;
        }
        offset += chunk.size;
    }
    return offset === manifest.total_size;
}

export function desktopHashWorkerCount(
    platform: NodeJS.Platform,
    architecture: string,
    logicalCores: number,
): number {
    const cores = Number.isFinite(logicalCores) ? Math.max(1, Math.trunc(logicalCores)) : 1;
    if (platform === "win32" && architecture === "arm64") return Math.min(2, cores);
    if (platform === "darwin" && architecture === "arm64") return Math.min(4, Math.max(1, cores - 1));
    return Math.min(4, Math.max(1, cores - 1));
}

/**
 * Bounded worker_threads scheduler. Jobs contain pathname + metadata only;
 * binary payloads in either direction are rejected as a protocol violation.
 */
export class DesktopHashWorkerPool {
    private readonly slots: WorkerSlot[] = [];
    private readonly queue: PendingJob[] = [];
    private nextJobId = 1;
    private closed = false;

    constructor(
        private readonly workerFactory: HashWorkerFactory,
        readonly workerCount: number,
        readonly maxQueuedJobs = Math.max(1, workerCount * 8),
    ) {
        if (!Number.isInteger(workerCount) || workerCount <= 0 || workerCount > 4) {
            throw new RangeError("hash worker count must be between 1 and 4");
        }
        if (!Number.isInteger(maxQueuedJobs) || maxQueuedJobs <= 0) {
            throw new RangeError("hash worker queue bound must be positive");
        }
        try {
            for (let index = 0; index < workerCount; index++) this.spawn(index, 0);
        } catch (error) {
            this.closed = true;
            for (const slot of this.slots) {
                slot.dead = true;
                if (slot.startupTimer) clearTimeout(slot.startupTimer);
                try { void slot.worker.terminate(); } catch { /* construction rollback */ }
            }
            throw error;
        }
    }

    run(input: HashWorkerInput, signal?: AbortSignal): Promise<HashWorkerResult> {
        if (this.closed) {
            return Promise.reject(new HashWorkerPoolError("hash worker pool is closed", "CLOSED"));
        }
        if (signal?.aborted) return Promise.reject(abortError());
        const freeSlot = this.slots.some((slot) => slot.ready && !slot.dead && !slot.current);
        if (!freeSlot && this.queue.length >= this.maxQueuedJobs) {
            return Promise.reject(new HashWorkerPoolError("hash worker queue is full", "QUEUE_FULL"));
        }

        return new Promise<HashWorkerResult>((resolve, reject) => {
            const pending: PendingJob = {
                request: {
                    type: "job",
                    job_id: `hash-${this.nextJobId++}`,
                    absolute_path: input.absolutePath,
                    expected_size: input.expectedSize,
                    expected_mtime: input.expectedMtime,
                    mode: input.mode,
                    feed_bytes: input.feedBytes,
                },
                resolve,
                reject,
                signal,
                abortRequested: false,
            };
            if (signal) {
                pending.abortListener = () => this.abort(pending);
                signal.addEventListener("abort", pending.abortListener, { once: true });
            }
            this.queue.push(pending);
            this.dispatch();
        });
    }

    stats(): HashWorkerPoolStats {
        const live = this.slots.filter((slot) => !slot.dead);
        const ready = live.filter((slot) => slot.ready).length;
        return {
            workers: live.length,
            ready,
            active: live.filter((slot) => slot.current !== null).length,
            queued: this.queue.length,
            wasmMode: ready > 0 ? "simd" : live.length > 0 ? "starting" : "unavailable",
        };
    }

    async close(): Promise<void> {
        if (this.closed) return;
        this.closed = true;
        const error = new HashWorkerPoolError("hash worker pool closed", "CLOSED");
        for (const pending of this.queue.splice(0)) this.settle(pending, error);
        const terminations: Array<Promise<number>> = [];
        for (const slot of this.slots) {
            slot.dead = true;
            if (slot.current) {
                this.settle(slot.current, error);
                slot.current = null;
            }
            if (slot.startupTimer) clearTimeout(slot.startupTimer);
            try {
                terminations.push(Promise.resolve(slot.worker.terminate()));
            } catch {
                // A worker that already exited is equivalent to terminated.
            }
        }
        await Promise.allSettled(terminations);
    }

    private spawn(index: number, restarts: number): void {
        let worker: WorkerLike;
        try {
            worker = this.workerFactory(index);
        } catch (error) {
            if (restarts >= MAX_RESTARTS_PER_SLOT || this.closed) throw error;
            this.spawn(index, restarts + 1);
            return;
        }
        const slot: WorkerSlot = {
            index,
            worker,
            ready: false,
            dead: false,
            restarts,
            current: null,
            startupTimer: null,
        };
        this.slots[index] = slot;
        worker.on("message", (message) => this.onMessage(slot, message));
        worker.on("error", (error) => this.onWorkerFailure(slot, error));
        worker.on("exit", (code) => {
            if (!slot.dead) {
                this.onWorkerFailure(
                    slot,
                    new HashWorkerPoolError(`hash worker exited (${code})`, "WORKER_EXIT"),
                );
            }
        });
        slot.startupTimer = setTimeout(() => {
            this.onWorkerFailure(
                slot,
                new HashWorkerPoolError("hash worker startup timed out", "WORKER_INIT"),
            );
        }, WORKER_START_TIMEOUT_MS);
        (slot.startupTimer as any)?.unref?.();
    }

    private onMessage(slot: WorkerSlot, message: HashWorkerResponse): void {
        if (slot.dead || this.closed || !message || typeof message !== "object") return;
        if (message.type === "ready") {
            if (message.wasm_mode !== "simd") {
                this.onWorkerFailure(
                    slot,
                    new HashWorkerPoolError("desktop worker did not initialize SIMD", "NO_SIMD"),
                );
                return;
            }
            if (slot.startupTimer) clearTimeout(slot.startupTimer);
            slot.startupTimer = null;
            slot.ready = true;
            this.dispatch();
            return;
        }
        if (message.type === "fatal") {
            this.onWorkerFailure(
                slot,
                new HashWorkerPoolError(message.message, "WORKER_INIT"),
            );
            return;
        }

        const pending = slot.current;
        if (!pending || message.job_id !== pending.request.job_id) {
            this.onWorkerFailure(
                slot,
                new HashWorkerPoolError("hash worker response id mismatch", "PROTOCOL"),
            );
            return;
        }
        slot.current = null;
        if (containsBinary(message)) {
            const protocolError = new HashWorkerPoolError(
                "binary payload crossed the hash worker boundary",
                "PROTOCOL",
            );
            this.settle(pending, protocolError);
            this.onWorkerFailure(slot, protocolError);
            return;
        } else if (message.type === "error") {
            this.settle(pending, workerFailure(message.code, message.message));
        } else if (
            message.type !== "result" ||
            message.mode !== pending.request.mode ||
            !validResult(message, pending.request)
        ) {
            const protocolError = new HashWorkerPoolError(
                "invalid hash worker response",
                "PROTOCOL",
            );
            this.settle(pending, protocolError);
            this.onWorkerFailure(slot, protocolError);
            return;
        } else if (pending.abortRequested) {
            this.settle(pending, abortError());
        } else {
            this.settle(pending, null, message);
        }
        this.dispatch();
    }

    private onWorkerFailure(slot: WorkerSlot, error: Error): void {
        if (slot.dead) return;
        slot.dead = true;
        slot.ready = false;
        if (slot.startupTimer) clearTimeout(slot.startupTimer);
        slot.startupTimer = null;
        if (slot.current) {
            this.settle(slot.current, error);
            slot.current = null;
        }
        try {
            void slot.worker.terminate();
        } catch {
            // Already gone.
        }
        if (!this.closed && slot.restarts < MAX_RESTARTS_PER_SLOT) {
            try {
                this.spawn(slot.index, slot.restarts + 1);
            } catch {
                // The all-dead check below fails the remaining queue.
            }
        }
        if (this.slots.every((candidate) => candidate?.dead)) {
            const unavailable = new HashWorkerPoolError("all hash workers unavailable", "UNAVAILABLE");
            for (const pending of this.queue.splice(0)) this.settle(pending, unavailable);
        }
    }

    private abort(pending: PendingJob): void {
        const queuedIndex = this.queue.indexOf(pending);
        if (queuedIndex >= 0) {
            this.queue.splice(queuedIndex, 1);
            this.settle(pending, abortError());
            return;
        }
        const slot = this.slots.find((candidate) => candidate.current === pending);
        if (!slot || slot.dead) return;
        pending.abortRequested = true;
        try {
            slot.worker.postMessage({ type: "cancel", job_id: pending.request.job_id });
        } catch (error) {
            this.onWorkerFailure(slot, error as Error);
        }
    }

    private dispatch(): void {
        if (this.closed) return;
        for (const slot of this.slots) {
            if (this.queue.length === 0) break;
            if (!slot || slot.dead || !slot.ready || slot.current) continue;
            while (this.queue.length > 0 && !slot.current) {
                const pending = this.queue.shift()!;
                if (pending.signal?.aborted) {
                    this.settle(pending, abortError());
                    continue;
                }
                slot.current = pending;
                try {
                    // The request schema has no bytes or transferable buffers.
                    slot.worker.postMessage(pending.request);
                } catch (error) {
                    slot.current = null;
                    this.settle(pending, error as Error);
                    this.onWorkerFailure(slot, error as Error);
                }
            }
        }
    }

    private settle(
        pending: PendingJob,
        error: Error | null,
        result?: HashWorkerResult,
    ): void {
        if (pending.signal && pending.abortListener) {
            pending.signal.removeEventListener("abort", pending.abortListener);
        }
        if (error) pending.reject(error);
        else pending.resolve(result!);
    }
}

/** Create the real Electron/Node pool without importing Node builtins on iOS. */
export function createDesktopHashWorkerPool(workerSource: string): DesktopHashWorkerPool | null {
    try {
        const nodeRequire = (typeof require === "function"
            ? require
            : (globalThis as any).require) as
            | ((id: string) => any)
            | undefined;
        if (!nodeRequire || typeof workerSource !== "string" || workerSource.length === 0) return null;
        const { Worker } = nodeRequire("node:worker_threads") as typeof import("node:worker_threads");
        const os = nodeRequire("node:os") as typeof import("node:os");
        const cores = typeof os.availableParallelism === "function"
            ? os.availableParallelism()
            : os.cpus().length;
        const count = desktopHashWorkerCount(process.platform, process.arch, cores);
        return new DesktopHashWorkerPool(
            (index) => new Worker(workerSource, {
                eval: true,
                name: `obsetync-hash-${index + 1}`,
            }) as unknown as WorkerLike,
            count,
            count * 8,
        );
    } catch {
        return null;
    }
}
