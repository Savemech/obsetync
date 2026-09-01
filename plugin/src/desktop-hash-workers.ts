import type {
    DesktopFileFingerprint,
    HashWorkerErrorCode,
    HashWorkerJob,
    HashWorkerMode,
    HashWorkerRequest,
    HashWorkerResponse,
    HashWorkerResult,
} from "./hash-worker-protocol";
import { MAX_FASTCDC_CHUNK_BYTES } from "./hash-worker-protocol";

export interface HashWorkerInput {
    absolutePath: string;
    expectedSize: number;
    expectedMtime: number;
    mode: HashWorkerMode;
    feedBytes: number;
}

export interface HashWorkerPoolStats {
    workers: number;
    capacity: number;
    limit: number;
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
    retiring: boolean;
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

function validFingerprint(
    value: unknown,
    request: HashWorkerJob,
): value is DesktopFileFingerprint {
    if (!value || typeof value !== "object") return false;
    const fingerprint = value as DesktopFileFingerprint;
    return Number.isSafeInteger(fingerprint.size) &&
        fingerprint.size === request.expected_size &&
        Number.isFinite(fingerprint.mtime) && fingerprint.mtime >= 0 &&
        Math.abs(fingerprint.mtime - request.expected_mtime) <= 1 &&
        Number.isFinite(fingerprint.ctime) && fingerprint.ctime >= 0 &&
        Number.isInteger(fingerprint.device) && fingerprint.device >= 0 &&
        Number.isInteger(fingerprint.inode) && fingerprint.inode >= 0;
}

function validResult(message: HashWorkerResult, request: HashWorkerJob): boolean {
    if (
        message.size !== request.expected_size ||
        !Number.isFinite(message.mtime) ||
        Math.abs(message.mtime - request.expected_mtime) > 1 ||
        !validFingerprint(message.fingerprint, request) ||
        message.size !== message.fingerprint.size ||
        Math.abs(message.mtime - message.fingerprint.mtime) > 1 ||
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
            !Number.isSafeInteger(chunk.size) || chunk.size <= 0 ||
            chunk.size > MAX_FASTCDC_CHUNK_BYTES ||
            chunk.offset > manifest.total_size - chunk.size
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
    private readonly slots: Array<WorkerSlot | undefined> = [];
    private readonly queue: PendingJob[] = [];
    private nextJobId = 1;
    private closed = false;
    private activeWorkerLimit: number;

    constructor(
        private readonly workerFactory: HashWorkerFactory,
        /** Maximum worker capacity; workers above the active limit are lazy. */
        readonly workerCount: number,
        readonly maxQueuedJobs = Math.max(1, workerCount * 8),
        initialWorkerLimit = workerCount,
    ) {
        if (!Number.isInteger(workerCount) || workerCount <= 0 || workerCount > 4) {
            throw new RangeError("hash worker count must be between 1 and 4");
        }
        if (!Number.isInteger(maxQueuedJobs) || maxQueuedJobs <= 0) {
            throw new RangeError("hash worker queue bound must be positive");
        }
        if (
            !Number.isInteger(initialWorkerLimit) || initialWorkerLimit <= 0 ||
            initialWorkerLimit > workerCount
        ) {
            throw new RangeError("initial hash worker limit must fit pool capacity");
        }
        this.activeWorkerLimit = initialWorkerLimit;
        try {
            for (let index = 0; index < initialWorkerLimit; index++) this.spawn(index, 0);
        } catch (error) {
            this.closed = true;
            for (const slot of this.slots) {
                if (!slot) continue;
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
        const freeSlot = this.slots.some((slot, index) =>
            index < this.activeWorkerLimit && !!slot &&
            slot.ready && !slot.dead && !slot.retiring && !slot.current);
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
        const live = this.slots.filter((slot): slot is WorkerSlot => !!slot && !slot.dead);
        const eligible = live.filter((slot) =>
            slot.index < this.activeWorkerLimit && !slot.retiring);
        const ready = eligible.filter((slot) => slot.ready).length;
        return {
            workers: live.length,
            capacity: this.workerCount,
            limit: this.activeWorkerLimit,
            ready,
            active: live.filter((slot) => slot.current !== null).length,
            queued: this.queue.length,
            wasmMode: ready > 0 ? "simd" : eligible.length > 0 ? "starting" : "unavailable",
        };
    }

    /** Additively grow or multiplicatively shrink the live SIMD worker set. */
    setActiveWorkerLimit(limit: number): void {
        if (this.closed) return;
        if (!Number.isInteger(limit) || limit <= 0 || limit > this.workerCount) {
            throw new RangeError("hash worker limit must fit pool capacity");
        }
        if (limit === this.activeWorkerLimit) return;
        const previous = this.activeWorkerLimit;
        if (limit > previous) {
            const spawned: WorkerSlot[] = [];
            const reactivated: WorkerSlot[] = [];
            try {
                for (let index = previous; index < limit; index++) {
                    const slot = this.slots[index];
                    if (slot && !slot.dead) {
                        if (slot.retiring) reactivated.push(slot);
                        slot.retiring = false;
                    } else {
                        this.spawn(index, 0);
                        const created = this.slots[index];
                        if (created) spawned.push(created);
                    }
                }
            } catch (error) {
                for (const slot of spawned) this.retire(slot);
                for (const slot of reactivated) {
                    if (slot.current) slot.retiring = true;
                    else this.retire(slot);
                }
                throw error;
            }
            this.activeWorkerLimit = limit;
            this.dispatch();
            return;
        }
        this.activeWorkerLimit = limit;
        for (let index = limit; index < this.slots.length; index++) {
            const slot = this.slots[index];
            if (!slot || slot.dead) continue;
            if (slot.current) slot.retiring = true;
            else this.retire(slot);
        }
    }

    async close(): Promise<void> {
        if (this.closed) return;
        this.closed = true;
        const error = new HashWorkerPoolError("hash worker pool closed", "CLOSED");
        for (const pending of this.queue.splice(0)) this.settle(pending, error);
        const terminations: Array<Promise<number>> = [];
        for (const slot of this.slots) {
            if (!slot) continue;
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
            retiring: false,
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
        if (slot.retiring || slot.index >= this.activeWorkerLimit) {
            this.retire(slot);
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
        if (
            !this.closed && !slot.retiring && slot.index < this.activeWorkerLimit &&
            slot.restarts < MAX_RESTARTS_PER_SLOT
        ) {
            try {
                this.spawn(slot.index, slot.restarts + 1);
            } catch {
                // The all-dead check below fails the remaining queue.
            }
        }
        const eligible = this.slots.slice(0, this.activeWorkerLimit);
        if (eligible.every((candidate) => !candidate || candidate.dead)) {
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
        const slot = this.slots.find((candidate) => candidate?.current === pending);
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
        for (let index = 0; index < this.activeWorkerLimit; index++) {
            const slot = this.slots[index];
            if (this.queue.length === 0) break;
            if (!slot || slot.dead || slot.retiring || !slot.ready || slot.current) continue;
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

    private retire(slot: WorkerSlot): void {
        if (slot.dead) return;
        slot.dead = true;
        slot.ready = false;
        slot.retiring = false;
        if (slot.startupTimer) clearTimeout(slot.startupTimer);
        slot.startupTimer = null;
        try {
            void slot.worker.terminate();
        } catch {
            // A worker which exited while being retired already released its resources.
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
export interface DesktopHashWorkerPoolOptions {
    initialWorkers?: number;
    maxWorkers?: number;
}

export function createDesktopHashWorkerPool(
    workerSource: string,
    options: DesktopHashWorkerPoolOptions = {},
): DesktopHashWorkerPool | null {
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
        const defaultInitial = desktopHashWorkerCount(process.platform, process.arch, cores);
        const capacity = options.maxWorkers ?? Math.min(4, Math.max(1, cores - 1));
        const initial = options.initialWorkers ?? Math.min(defaultInitial, capacity);
        return new DesktopHashWorkerPool(
            (index) => new Worker(workerSource, {
                eval: true,
                name: `obsetync-hash-${index + 1}`,
            }) as unknown as WorkerLike,
            capacity,
            capacity * 8,
            initial,
        );
    } catch {
        return null;
    }
}
