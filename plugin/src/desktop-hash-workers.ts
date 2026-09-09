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

/** Retain already-admitted job buffers independently of caller-visible errors.
 * This adds ownership lifetime, not a byte allowance or a new reservation. */
export interface HashWorkerLifetimeTracker {
    track<T>(work: () => T | Promise<T>): Promise<T>;
}

export interface HashWorkerPoolStats {
    workers: number;
    capacity: number;
    limit: number;
    ready: number;
    active: number;
    queued: number;
    restarting: number;
    wasmMode: "simd" | "starting" | "unavailable";
}

export type HashWorkerDiagnosticReason =
    | "STARTING" | "READY"
    | "WORKER_SOURCE_MISSING" | "NODE_REQUIRE_UNAVAILABLE"
    | "WORKER_THREADS_UNAVAILABLE" | "OS_INFO_UNAVAILABLE"
    | "INVALID_CONFIGURATION" | "WORKER_CONSTRUCTOR" | "WORKER_LISTENERS"
    | "WORKER_START_TIMEOUT" | "WORKER_INIT" | "NO_SIMD"
    | "WORKER_CRASH" | "WORKER_EXIT" | "PROTOCOL"
    | "RESTART_LIMIT" | "WORKER_TERMINATION_FAILED" | "WORKER_TERMINATION_TIMEOUT";

/** Fixed-enum capability/lifecycle events: never include paths or raw errors. */
export interface HashWorkerDiagnostic {
    kind: "starting" | "ready" | "failure" | "unavailable";
    reason: HashWorkerDiagnosticReason;
    slot?: number;
    attempt?: number;
    retrying?: boolean;
}

export interface HashWorkerPoolDiagnostics {
    closed: boolean;
    startupAttempts: number;
    failureCount: number;
    restartCount: number;
    lastFailure: HashWorkerDiagnostic | null;
    nativeOwnedWorkers: number;
    nativeSpawnsInProgress: number;
    nativeDrainRequested: boolean;
    nativeDrained: boolean;
}

export interface HashWorkerPoolLifecycleOptions {
    onDiagnostic?: (event: HashWorkerDiagnostic) => void;
    /** A throwing constructor cannot return its pool. Hand off its actual
     * native cleanup lifetime, including workers whose termination timed out. */
    onConstructionCleanup?: (actualNativeDrain: Promise<void>) => void;
    /** Test/host injection; production uses the existing bounded startup timeout. */
    timer?: {
        schedule(callback: () => void, delayMs: number): unknown;
        cancel(handle: unknown): void;
    };
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

interface FailedWorkerCleanup {
    completion: Promise<boolean>;
    confirmed: () => boolean;
}

const failedWorkerCleanup = new WeakMap<object, FailedWorkerCleanup>();

/** Before renderer fallback reuses an admitted worker quota, wait for actual
 * cleanup. False means the bounded termination attempt failed/timed out: its
 * lifetime remains charged until a later native exit, so fail closed instead
 * of starting another allocation under the same allowance. Typed original
 * errors/messages remain unchanged; unrelated/never-started errors are safe. */
export function waitForHashWorkerCleanup(error: unknown): Promise<boolean> {
    const cleanup = error && typeof error === "object" ? failedWorkerCleanup.get(error) : undefined;
    return cleanup?.confirmed() ? Promise.resolve(true) : cleanup?.completion ?? Promise.resolve(true);
}

interface PendingJob {
    request: HashWorkerJob;
    resolve: (value: HashWorkerResult) => void;
    reject: (error: Error) => void;
    signal?: AbortSignal;
    abortListener?: () => void;
    abortRequested: boolean;
    lifetime?: HashWorkerLifetimeTracker;
}

interface WorkerSlot {
    index: number;
    worker: WorkerLike;
    ready: boolean;
    dead: boolean;
    retiring: boolean;
    restarts: number;
    current: PendingJob | null;
    startupTimer: unknown | null;
    restartPlanned: boolean;
    termination?: Promise<boolean>;
    nativeTerminated: boolean;
    finishNativeJob?: () => void;
    finishTermination?: () => void;
}

const MAX_RESTARTS_PER_SLOT = 3;
const WORKER_START_TIMEOUT_MS = 15_000;
const WORKER_TERMINATION_TIMEOUT_MS = 5_000;

function reportDiagnostic(
    callback: HashWorkerPoolLifecycleOptions["onDiagnostic"],
    event: HashWorkerDiagnostic,
): void {
    try {
        callback?.({ ...event });
    } catch {
        // Debug/reporting consumers must not interfere with worker ownership.
    }
}

function diagnosticReason(error: Error, wasReady: boolean): HashWorkerDiagnosticReason {
    if (error instanceof HashWorkerPoolError) {
        switch (error.code) {
            case "WORKER_START_TIMEOUT": return "WORKER_START_TIMEOUT";
            case "WORKER_INIT": return "WORKER_INIT";
            case "NO_SIMD": return "NO_SIMD";
            case "WORKER_EXIT": return "WORKER_EXIT";
            case "PROTOCOL": return "PROTOCOL";
        }
    }
    return wasReady ? "WORKER_CRASH" : "WORKER_INIT";
}

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
    private readonly disabledSlots = new Set<number>();
    private readonly restartsBySlot: number[] = [];
    private readonly attemptsBySlot: number[] = [];
    private startupAttempts = 0;
    private failureCount = 0;
    private restartCount = 0;
    private lastFailure: HashWorkerDiagnostic | null = null;
    private readonly terminations = new Set<Promise<boolean>>();
    /** Confirmed exits are removed, including retired/replaced slot objects.
     * Unconfirmed workers never disappear merely because a bounded wait ended. */
    private readonly nativeOwnedSlots = new Set<WorkerSlot>();
    private nativeSpawnsInProgress = 0;
    private nativeDrainPromise: Promise<void> | null = null;
    private resolveNativeDrain: (() => void) | null = null;

    constructor(
        private readonly workerFactory: HashWorkerFactory,
        /** Maximum worker capacity; workers above the active limit are lazy. */
        readonly workerCount: number,
        readonly maxQueuedJobs = Math.max(1, workerCount * 8),
        initialWorkerLimit = workerCount,
        private readonly lifecycle: HashWorkerPoolLifecycleOptions = {},
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
            for (let index = 0; index < initialWorkerLimit; index++) this.spawn(index);
        } catch (error) {
            this.closed = true;
            for (const slot of this.nativeOwnedSlots) {
                slot.dead = true;
                try { this.clearStartupTimer(slot); } catch { /* Preserve the constructor failure. */ }
                void this.terminate(slot);
            }
            try { this.lifecycle.onConstructionCleanup?.(this.closeAndDrainNative()); }
            catch { /* A lifecycle observer must not hide the original failure. */ }
            throw error;
        }
    }

    run(input: HashWorkerInput, signal?: AbortSignal, lifetime?: HashWorkerLifetimeTracker): Promise<HashWorkerResult> {
        if (this.closed) {
            return Promise.reject(new HashWorkerPoolError("hash worker pool is closed", "CLOSED"));
        }
        if (signal?.aborted) return Promise.reject(abortError());
        if (!this.hasEligibleWorkers()) {
            return Promise.reject(new HashWorkerPoolError("all hash workers unavailable", "UNAVAILABLE"));
        }
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
                lifetime,
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
        const restarting = this.slots.filter((slot, index) =>
            index < this.activeWorkerLimit && !!slot?.restartPlanned).length;
        return {
            workers: live.length,
            capacity: this.workerCount,
            limit: this.activeWorkerLimit,
            ready,
            active: live.filter((slot) => slot.current !== null).length,
            queued: this.queue.length,
            restarting,
            wasmMode: ready > 0 ? "simd" : eligible.length > 0 || restarting > 0 ? "starting" : "unavailable",
        };
    }

    diagnostics(): HashWorkerPoolDiagnostics {
        return {
            closed: this.closed,
            startupAttempts: this.startupAttempts,
            failureCount: this.failureCount,
            restartCount: this.restartCount,
            lastFailure: this.lastFailure ? { ...this.lastFailure } : null,
            nativeOwnedWorkers: this.nativeOwnedSlots.size,
            nativeSpawnsInProgress: this.nativeSpawnsInProgress,
            nativeDrainRequested: this.nativeDrainPromise !== null,
            nativeDrained: this.closed && this.nativeOwnedSlots.size === 0 && this.nativeSpawnsInProgress === 0,
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
                        if (slot?.restartPlanned) continue;
                        if (slot?.termination) {
                            if (this.disabledSlots.has(index)) {
                                throw new HashWorkerPoolError("hash worker slot unavailable for this session", "UNAVAILABLE");
                            }
                            slot.restartPlanned = true;
                            reactivated.push(slot);
                            void slot.termination.then((terminated) => {
                                if (!slot.restartPlanned) return;
                                slot.restartPlanned = false;
                                if (terminated && !this.closed && index < this.activeWorkerLimit) {
                                    try { this.spawn(index); } catch { /* already reported */ }
                                }
                                this.failUnavailableQueue();
                            });
                            continue;
                        }
                        this.spawn(index);
                        const created = this.slots[index];
                        if (created) spawned.push(created);
                    }
                }
            } catch (error) {
                for (const slot of spawned) this.retire(slot);
                for (const slot of reactivated) {
                    if (slot.dead) {
                        slot.restartPlanned = false;
                        continue;
                    }
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
            if (!slot) continue;
            if (slot.dead) {
                slot.restartPlanned = false;
                continue;
            }
            if (slot.current) slot.retiring = true;
            else this.retire(slot);
        }
    }

    async close(): Promise<void> {
        if (this.closed) return;
        this.closed = true;
        const error = new HashWorkerPoolError("hash worker pool closed", "CLOSED");
        for (const pending of this.queue.splice(0)) this.settle(pending, error);
        for (const slot of this.slots) {
            if (!slot) continue;
            slot.dead = true;
            slot.restartPlanned = false;
            if (slot.current) {
                const failure = new HashWorkerPoolError(error.message, error.code);
                this.rememberCleanup(failure, slot, this.terminate(slot));
                this.settle(slot.current, failure);
                slot.current = null;
            }
            this.clearStartupTimer(slot);
            void this.terminate(slot);
        }
        await Promise.allSettled([...this.terminations]);
    }

    /** Replacement/unload barrier, unlike close()'s bounded caller wait.
     * A timeout or rejection is NOT native termination. If no authoritative
     * exit/completion arrives this promise intentionally remains pending.
     * Not async: repeated callers receive the same promise after settlement. */
    closeAndDrainNative(): Promise<void> {
        if (this.nativeDrainPromise) return this.nativeDrainPromise;
        this.nativeDrainPromise = new Promise<void>(resolve => { this.resolveNativeDrain = resolve; });
        // A bounded-close error is not evidence of native release either.
        void this.close().catch(() => {});
        this.completeNativeDrainIfReady();
        return this.nativeDrainPromise;
    }

    private completeNativeDrainIfReady(): void {
        if (!this.closed || this.nativeOwnedSlots.size !== 0 || this.nativeSpawnsInProgress !== 0 || !this.resolveNativeDrain) return;
        const resolve = this.resolveNativeDrain;
        this.resolveNativeDrain = null;
        resolve();
    }

    private spawn(index: number): void {
        if (this.closed || this.disabledSlots.has(index)) {
            throw new HashWorkerPoolError("hash worker slot unavailable for this session", "UNAVAILABLE");
        }
        const attempt = (this.attemptsBySlot[index] ?? 0) + 1;
        this.attemptsBySlot[index] = attempt;
        this.startupAttempts++;
        this.report({ kind: "starting", reason: "STARTING", slot: index, attempt });
        // A diagnostic observer can synchronously stop a live pool during
        // growth. Do not construct a new worker after that close boundary.
        if (this.closed || this.disabledSlots.has(index)) throw new HashWorkerPoolError("hash worker pool is closed", "CLOSED");
        let worker: WorkerLike;
        this.nativeSpawnsInProgress++;
        try {
            worker = this.workerFactory(index);
        } catch (error) {
            this.nativeSpawnsInProgress--;
            this.completeNativeDrainIfReady();
            // Synchronous capability/constructor failure cannot be repaired by
            // immediately trying the identical source three more times.
            this.disabledSlots.add(index);
            this.report({
                kind: "failure", reason: "WORKER_CONSTRUCTOR",
                slot: index, attempt, retrying: false,
            });
            throw error;
        }
        const slot: WorkerSlot = {
            index,
            worker,
            ready: false,
            dead: false,
            retiring: false,
            restarts: this.restartsBySlot[index] ?? 0,
            current: null,
            startupTimer: null,
            restartPlanned: false,
            nativeTerminated: false,
        };
        this.nativeOwnedSlots.add(slot);
        this.slots[index] = slot;
        this.nativeSpawnsInProgress--;
        try {
            // Observe authoritative exit even if a later listener fails.
            worker.on("exit", (code) => {
                // An actual exit is authoritative even if terminate() threw,
                // rejected, or exceeded its bounded caller wait earlier.
                this.confirmNativeTermination(slot);
                if (!slot.dead) {
                    this.onWorkerFailure(
                        slot,
                        new HashWorkerPoolError(`hash worker exited (${code})`, "WORKER_EXIT"),
                    );
                }
            });
            // A factory itself may close reentrantly while constructing an
            // admitted worker. Its in-progress owner kept native drain open.
            if (this.closed) {
                slot.dead = true;
                void this.terminate(slot);
                return;
            }
            worker.on("message", (message) => this.onMessage(slot, message));
            worker.on("error", (error) => this.onWorkerFailure(slot, error));
            const onTimeout = () => this.onWorkerFailure(
                slot,
                new HashWorkerPoolError("hash worker startup timed out", "WORKER_START_TIMEOUT"),
            );
            slot.startupTimer = this.lifecycle.timer
                ? this.lifecycle.timer.schedule(onTimeout, WORKER_START_TIMEOUT_MS)
                : setTimeout(onTimeout, WORKER_START_TIMEOUT_MS);
            (slot.startupTimer as { unref?: () => void })?.unref?.();
        } catch (error) {
            slot.dead = true;
            this.disabledSlots.add(index);
            this.clearStartupTimer(slot);
            this.report({
                kind: "failure", reason: "WORKER_LISTENERS",
                slot: index, attempt, retrying: false,
            });
            void this.terminate(slot);
            throw error;
        }
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
            if (slot.ready) return;
            this.clearStartupTimer(slot);
            slot.ready = true;
            this.report({
                kind: "ready", reason: "READY", slot: slot.index,
                attempt: this.attemptsBySlot[slot.index],
            });
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
        if (containsBinary(message)) {
            const protocolError = new HashWorkerPoolError(
                "binary payload crossed the hash worker boundary",
                "PROTOCOL",
            );
            this.onWorkerFailure(slot, protocolError);
            return;
        } else if (message.type === "error") {
            slot.current = null;
            this.finishNativeJob(slot);
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
            this.onWorkerFailure(slot, protocolError);
            return;
        } else if (pending.abortRequested) {
            slot.current = null;
            this.finishNativeJob(slot);
            this.settle(pending, abortError());
        } else {
            slot.current = null;
            this.finishNativeJob(slot);
            this.settle(pending, null, message);
        }
        if (slot.retiring || slot.index >= this.activeWorkerLimit) {
            this.retire(slot);
        }
        this.dispatch();
    }

    private onWorkerFailure(slot: WorkerSlot, error: Error): void {
        if (slot.dead) return;
        const wasReady = slot.ready;
        slot.dead = true;
        slot.ready = false;
        this.clearStartupTimer(slot);
        const pending = slot.current;
        slot.current = null;
        const reason = diagnosticReason(error, wasReady);
        const recoverable = wasReady && (reason === "WORKER_CRASH" || reason === "WORKER_EXIT");
        const retrying = recoverable &&
            !this.closed && !slot.retiring && slot.index < this.activeWorkerLimit &&
            slot.restarts < MAX_RESTARTS_PER_SLOT;
        slot.restartPlanned = retrying;
        if (retrying) this.restartsBySlot[slot.index] = slot.restarts + 1;
        if (!retrying) this.disabledSlots.add(slot.index);
        this.report({
            kind: "failure", reason, slot: slot.index,
            attempt: this.attemptsBySlot[slot.index], retrying,
        });
        if (recoverable && slot.restarts >= MAX_RESTARTS_PER_SLOT) {
            this.report({
                kind: "unavailable", reason: "RESTART_LIMIT", slot: slot.index,
                attempt: this.attemptsBySlot[slot.index], retrying: false,
            });
        }
        // Do not create another WASM heap until the previous thread has really
        // terminated. Catch rejected terminate promises as well as sync throws.
        const termination = this.terminate(slot);
        if (pending) {
            this.rememberCleanup(error, slot, termination);
            this.settle(pending, error);
        }
        void termination.then((terminated) => {
            if (!slot.restartPlanned) return;
            slot.restartPlanned = false;
            if (!terminated || this.closed || slot.index >= this.activeWorkerLimit) {
                this.failUnavailableQueue();
                return;
            }
            this.restartCount++;
            try {
                this.spawn(slot.index);
            } catch {
                // The structured spawn diagnostic already records the cause.
            }
            this.failUnavailableQueue();
        });
        this.failUnavailableQueue();
        this.dispatch();
    }

    private report(event: HashWorkerDiagnostic): void {
        if (event.kind === "failure") {
            this.failureCount++;
            this.lastFailure = { ...event };
        }
        reportDiagnostic(this.lifecycle.onDiagnostic, event);
    }

    private hasEligibleWorkers(): boolean {
        return this.slots.some((slot, index) =>
            index < this.activeWorkerLimit && !!slot && !slot.retiring &&
            (!slot.dead || slot.restartPlanned));
    }

    private failUnavailableQueue(): void {
        if (this.hasEligibleWorkers()) return;
        const unavailable = new HashWorkerPoolError("all hash workers unavailable", "UNAVAILABLE");
        for (const pending of this.queue.splice(0)) this.settle(pending, unavailable);
    }

    private clearStartupTimer(slot: WorkerSlot): void {
        if (slot.startupTimer === null) return;
        if (this.lifecycle.timer) this.lifecycle.timer.cancel(slot.startupTimer);
        else clearTimeout(slot.startupTimer as ReturnType<typeof setTimeout>);
        slot.startupTimer = null;
    }

    private terminate(slot: WorkerSlot): Promise<boolean> {
        if (slot.termination) return slot.termination;
        if (slot.nativeTerminated) {
            slot.termination = Promise.resolve(true);
            return slot.termination;
        }
        let result: Promise<number> | number;
        try {
            result = slot.worker.terminate();
        } catch {
            this.disabledSlots.add(slot.index);
            this.report({
                kind: "failure", reason: "WORKER_TERMINATION_FAILED", slot: slot.index,
                attempt: this.attemptsBySlot[slot.index], retrying: false,
            });
            slot.termination = Promise.resolve(false);
            return slot.termination;
        }
        if (typeof result === "number") {
            this.confirmNativeTermination(slot);
            slot.termination = Promise.resolve(true);
            return slot.termination;
        }
        const termination = new Promise<boolean>((resolve) => {
            let finished = false;
            let timer: unknown | null = null;
            const finish = (success: boolean, reason?: HashWorkerDiagnosticReason) => {
                if (finished) return;
                finished = true;
                slot.finishTermination = undefined;
                if (timer !== null) {
                    try {
                        if (this.lifecycle.timer) this.lifecycle.timer.cancel(timer);
                        else clearTimeout(timer as ReturnType<typeof setTimeout>);
                    } catch {
                        // A broken timer teardown must not retain waiters.
                    }
                }
                if (!success) {
                    this.disabledSlots.add(slot.index);
                    this.report({
                        kind: "failure", reason: reason!, slot: slot.index,
                        attempt: this.attemptsBySlot[slot.index], retrying: false,
                    });
                }
                resolve(success);
            };
            slot.finishTermination = () => finish(true);
            const onTimeout = () => finish(false, "WORKER_TERMINATION_TIMEOUT");
            try {
                timer = this.lifecycle.timer
                    ? this.lifecycle.timer.schedule(onTimeout, WORKER_TERMINATION_TIMEOUT_MS)
                    : setTimeout(onTimeout, WORKER_TERMINATION_TIMEOUT_MS);
                (timer as { unref?: () => void })?.unref?.();
            } catch {
                finish(false, "WORKER_TERMINATION_FAILED");
            }
            Promise.resolve(result).then(
                () => {
                    // Late completion releases native ownership, but does not
                    // undo a prior timeout's disabled-slot/restart decision.
                    this.confirmNativeTermination(slot);
                    finish(true);
                },
                () => finish(false, "WORKER_TERMINATION_FAILED"),
            );
        });
        slot.termination = termination;
        this.terminations.add(termination);
        void termination.then(() => this.terminations.delete(termination));
        return termination;
    }

    private rememberCleanup(error: Error, slot: WorkerSlot, completion: Promise<boolean>): void {
        failedWorkerCleanup.set(error, { completion, confirmed: () => slot.nativeTerminated });
    }

    private finishNativeJob(slot: WorkerSlot): void {
        const finish = slot.finishNativeJob;
        slot.finishNativeJob = undefined;
        finish?.();
    }

    private confirmNativeTermination(slot: WorkerSlot): void {
        slot.nativeTerminated = true;
        this.nativeOwnedSlots.delete(slot);
        this.finishNativeJob(slot);
        slot.finishTermination?.();
        this.completeNativeDrainIfReady();
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
        if (!slot.finishNativeJob) {
            // The lifetime tracker has not admitted/invoked start yet.
            slot.current = null;
            this.settle(pending, abortError());
            this.dispatch();
            return;
        }
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
            while (this.queue.length > 0 && !slot.current && !slot.dead && !slot.retiring) {
                const pending = this.queue.shift()!;
                if (pending.signal?.aborted) {
                    this.settle(pending, abortError());
                    continue;
                }
                slot.current = pending;
                const start = (): Promise<void> => {
                    // An injected tracker may invoke its callback later. Stop
                    // and queue cancellation must not dispatch stale work.
                    if (slot.current !== pending || slot.dead || this.closed) return Promise.resolve();
                    if (pending.signal?.aborted) {
                        slot.current = null;
                        this.settle(pending, abortError());
                        this.dispatch();
                        return Promise.resolve();
                    }
                    const cleanup = new Promise<void>((resolve) => { slot.finishNativeJob = resolve; });
                    try {
                        // The request schema has no bytes or transferables.
                        slot.worker.postMessage(pending.request);
                    } catch (error) {
                        // A failed dispatch is not proof that native work never
                        // started. Hold its quota until termination confirms it.
                        this.onWorkerFailure(slot, error as Error);
                    }
                    return cleanup;
                };
                if (pending.lifetime) {
                    try {
                        void pending.lifetime.track(start).catch((error) => {
                            if (slot.current !== pending) return;
                            // A closed admission can reject without calling
                            // start; no worker buffers were allocated in that case.
                            slot.current = null;
                            this.settle(pending, error as Error);
                            this.dispatch();
                        });
                    } catch (error) {
                        slot.current = null;
                        this.settle(pending, error as Error);
                    }
                } else {
                    void start();
                }
            }
        }
    }

    private retire(slot: WorkerSlot): void {
        if (slot.dead) return;
        slot.dead = true;
        slot.ready = false;
        slot.retiring = false;
        slot.restartPlanned = false;
        this.clearStartupTimer(slot);
        void this.terminate(slot);
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
    onDiagnostic?: (event: HashWorkerDiagnostic) => void;
    onConstructionCleanup?: (actualNativeDrain: Promise<void>) => void;
    /** Optional capability injection; null explicitly models a no-Node host. */
    runtimeRequire?: ((id: string) => any) | null;
}

export function createDesktopHashWorkerPool(
    workerSource: string,
    options: DesktopHashWorkerPoolOptions = {},
): DesktopHashWorkerPool | null {
    const unavailable = (reason: HashWorkerDiagnosticReason): null => {
        reportDiagnostic(options.onDiagnostic, { kind: "unavailable", reason, retrying: false });
        return null;
    };
    if (typeof workerSource !== "string" || workerSource.length === 0) {
        return unavailable("WORKER_SOURCE_MISSING");
    }
    const nodeRequire = options.runtimeRequire !== undefined
        ? options.runtimeRequire
        : (typeof require === "function" ? require : (globalThis as any).require) as
            ((id: string) => any) | undefined;
    if (typeof nodeRequire !== "function") return unavailable("NODE_REQUIRE_UNAVAILABLE");
    let Worker: typeof import("node:worker_threads").Worker;
    try {
        Worker = nodeRequire("node:worker_threads")?.Worker;
        if (typeof Worker !== "function") return unavailable("WORKER_THREADS_UNAVAILABLE");
    } catch {
        return unavailable("WORKER_THREADS_UNAVAILABLE");
    }
    let cores: number;
    let platform: NodeJS.Platform;
    let architecture: string;
    try {
        const os = nodeRequire("node:os") as typeof import("node:os");
        // A renderer may expose lexical require but no process global.
        // Read architecture/platform from the already-authorized Node module.
        platform = os.platform();
        architecture = os.arch();
        cores = typeof os.availableParallelism === "function"
            ? os.availableParallelism()
            : os.cpus().length;
        if (typeof platform !== "string" || typeof architecture !== "string" ||
            !Number.isSafeInteger(cores) || cores <= 0) {
            return unavailable("OS_INFO_UNAVAILABLE");
        }
    } catch {
        return unavailable("OS_INFO_UNAVAILABLE");
    }
    const defaultInitial = desktopHashWorkerCount(platform, architecture, cores);
    const capacity = options.maxWorkers ?? Math.min(4, Math.max(1, cores - 1));
    const initial = options.initialWorkers ?? Math.min(defaultInitial, capacity);
    if (!Number.isInteger(capacity) || capacity < 1 || capacity > 4 ||
        !Number.isInteger(initial) || initial < 1 || initial > capacity) {
        return unavailable("INVALID_CONFIGURATION");
    }
    let latestFailure: HashWorkerDiagnosticReason = "WORKER_CONSTRUCTOR";
    try {
        return new DesktopHashWorkerPool(
            (index) => new Worker(workerSource, {
                eval: true,
                name: `obsetync-hash-${index + 1}`,
            }) as unknown as WorkerLike,
            capacity,
            capacity * 8,
            initial,
            {
                onConstructionCleanup: options.onConstructionCleanup,
                onDiagnostic: (event) => {
                    if (event.kind === "failure") latestFailure = event.reason;
                    reportDiagnostic(options.onDiagnostic, event);
                },
            },
        );
    } catch {
        return unavailable(latestFailure);
    }
}
