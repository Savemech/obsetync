import {
    BROWSER_HASH_WORKER_MAX_FEED_BYTES,
    BROWSER_HASH_WORKER_MAX_HEAP_BYTES,
    BROWSER_HASH_WORKER_MAX_INPUT_BYTES,
    BROWSER_HASH_WORKER_MIN_FEED_BYTES,
    BROWSER_HASH_WORKER_QUALIFICATION_BYTES,
    BROWSER_HASH_WORKER_SCHEMA,
    BROWSER_HASH_WORKER_MAX_SOURCE_CHARS,
    BROWSER_HASH_WORKER_MAX_MODULE_BYTES,
    browserHashQualificationFixture,
    type BrowserHashWorkerRequest,
    type BrowserHashWorkerResponse,
    type BrowserHashWorkerMode,
    validBrowserHash,
    validBrowserHashFeedBytes,
} from "./browser-hash-worker-protocol";

export type BrowserHashWorkerFailure = "UNAVAILABLE" | "SOURCE_INVALID" | "CONSTRUCTOR" |
    "QUALIFICATION_TIMEOUT" | "QUALIFICATION_FAILED" | "TRANSFER_UNSUPPORTED" |
    "PROTOCOL" | "CRASH" | "MESSAGE_ERROR" | "JOB_TIMEOUT" | "CLOSED";

export interface BrowserHashWorkerDiagnostic {
    kind: "starting" | "ready" | "failure" | "unavailable" | "closed";
    reason: BrowserHashWorkerFailure | "QUALIFIED";
    generation: number;
    wasmMode?: BrowserHashWorkerMode;
}

export class BrowserHashWorkerError extends Error {
    constructor(message: string, readonly code: BrowserHashWorkerFailure,
        /** False means a transferred native owner has no release acknowledgement;
         * callers must not start a same-scope whole-file renderer fallback. */
        readonly fallbackSafe = true,
        /** Conservative byte charge whose native release has no observable ACK;
         * this is not a claim that the bytes remain physically resident. */
        readonly unconfirmedReleaseBytes = 0) {
        super(message);
        this.name = "BrowserHashWorkerError";
    }
}

export interface BrowserHashWorkerResult {
    hash: string;
    /** The exact transferred allocation returned by the worker. */
    returnedBytes: Uint8Array;
    hashMs: number;
}

/** Captured startup generation paired with its sole worker owner. */
export interface BrowserHashWorkerRuntime {
    pool: BrowserHashWorkerPool;
    assertCurrent(): void;
}

export interface BrowserHashWorkerStats {
    state: "starting" | "ready" | "unavailable" | "closed";
    wasmMode: BrowserHashWorkerMode | "starting" | "unavailable";
    active: number;
    queued: number;
    /** Logical transferred/queued payload bytes; never presented as process RSS. */
    ownedBytes: number;
    maxOwnedBytes: number;
    sourceChars: number;
    wasmHeapBytes: number | null;
    quarantinedPayloadBytes: number;
    fallbackSafe: boolean;
    generation: number;
}

export interface BrowserHashWorkerDiagnostics {
    qualified: boolean;
    failureCount: number;
    completedJobs: number;
    cancelledJobs: number;
    lastFailure: BrowserHashWorkerFailure | null;
    /** Browser Worker has no exit event; true only means terminate() was invoked. */
    terminationRequested: boolean;
    cleanupEvidence: "not-requested" | "shutdown-unconfirmed";
    unconfirmedReleaseBytes: number;
}

export interface BrowserWorkerLike {
    onmessage: ((event: MessageEvent<BrowserHashWorkerResponse>) => void) | null;
    onerror: ((event: ErrorEvent) => void) | null;
    onmessageerror: ((event: MessageEvent) => void) | null;
    postMessage(message: BrowserHashWorkerRequest, transfer: Transferable[]): void;
    terminate(): void;
}

export interface BrowserHashWorkerHost {
    available(): boolean;
    createUrl(source: string): string;
    revokeUrl(url: string): void;
    createWorker(url: string): BrowserWorkerLike;
}

export interface BrowserHashWorkerPoolOptions {
    host?: BrowserHashWorkerHost;
    /** Owned copies: transferred during qualification and returned before ready(). */
    scalarWasm?: ArrayBuffer;
    simdWasm?: ArrayBuffer;
    maxQueuedJobs?: number;
    maxOwnedBytes?: number;
    qualificationTimeoutMs?: number;
    jobTimeoutMs?: number;
    cancelTimeoutMs?: number;
    cpuSliceMs?: number;
    onDiagnostic?: (event: BrowserHashWorkerDiagnostic) => void;
}

interface PendingJob {
    readonly id: number;
    readonly buffer: ArrayBuffer;
    readonly bytes: number;
    readonly feedBytes: number;
    readonly signal?: AbortSignal;
    resolve(value: BrowserHashWorkerResult): void;
    reject(error: Error): void;
    abortListener?: () => void;
    dispatched: boolean;
    settled: boolean;
}

const defaultHost: BrowserHashWorkerHost = {
    available: () => typeof Worker === "function" && typeof Blob === "function" &&
        typeof URL !== "undefined" && typeof URL.createObjectURL === "function",
    createUrl: source => URL.createObjectURL(new Blob([source], { type: "text/javascript" })),
    revokeUrl: url => URL.revokeObjectURL(url),
    createWorker: url => new Worker(url) as unknown as BrowserWorkerLike,
};

let nextGeneration = 1;
const abortError = () => Object.assign(new Error("browser hash worker job aborted"), { name: "AbortError" });

/**
 * One-worker mobile/browser pool. Construction is not capability evidence:
 * ready() resolves only after real WASM initialization, a known hash, and a
 * transferable-buffer round trip in the production worker itself.
 */
export class BrowserHashWorkerPool {
    readonly generation = nextGeneration++;
    private readonly queue: PendingJob[] = [];
    private active: PendingJob | null = null;
    private worker: BrowserWorkerLike | null = null;
    private url: string | null = null;
    private state: BrowserHashWorkerStats["state"] = "starting";
    private mode: BrowserHashWorkerMode | null = null;
    private wasmHeapBytes: number | null = null;
    private ownedBytes = 0;
    private nextJobId = 1;
    private qualificationTimer: ReturnType<typeof setTimeout> | null = null;
    private jobTimer: ReturnType<typeof setTimeout> | null = null;
    private qualificationResolve!: () => void;
    private qualificationReject!: (error: Error) => void;
    private readonly qualification: Promise<void>;
    private failureCount = 0;
    private completedJobs = 0;
    private cancelledJobs = 0;
    private lastFailure: BrowserHashWorkerFailure | null = null;
    private terminationRequested = false;
    private qualificationBuffer: ArrayBuffer | null = null;
    private moduleByteLengths: readonly [number, number] = [0, 0];
    private qualificationInFlightBytes = 0;
    private quarantinedPayloadBytes = 0;
    private readonly host: BrowserHashWorkerHost;
    private readonly maxQueuedJobs: number;
    private readonly maxOwnedBytes: number;
    private readonly qualificationTimeoutMs: number;
    private readonly jobTimeoutMs: number;
    private readonly cancelTimeoutMs: number;
    private readonly cpuSliceMs: number;

    constructor(readonly source: string, private readonly options: BrowserHashWorkerPoolOptions = {}) {
        this.host = options.host ?? defaultHost;
        this.maxQueuedJobs = options.maxQueuedJobs ?? 4;
        this.maxOwnedBytes = options.maxOwnedBytes ?? 16 * 1024 * 1024;
        this.qualificationTimeoutMs = options.qualificationTimeoutMs ?? 5000;
        this.jobTimeoutMs = options.jobTimeoutMs ?? 30_000;
        this.cancelTimeoutMs = options.cancelTimeoutMs ?? 1000;
        this.cpuSliceMs = options.cpuSliceMs ?? 8;
        if (!Number.isSafeInteger(this.maxQueuedJobs) || this.maxQueuedJobs < 1 || this.maxQueuedJobs > 32 ||
            !Number.isSafeInteger(this.maxOwnedBytes) || this.maxOwnedBytes < BROWSER_HASH_WORKER_MAX_INPUT_BYTES ||
            this.maxOwnedBytes > 64 * 1024 * 1024 ||
            !Number.isFinite(this.qualificationTimeoutMs) || this.qualificationTimeoutMs < 1 ||
            this.qualificationTimeoutMs > 30_000 || !Number.isFinite(this.jobTimeoutMs) ||
            this.jobTimeoutMs < 1 || this.jobTimeoutMs > 120_000 || !Number.isFinite(this.cpuSliceMs) ||
            this.cpuSliceMs < 1 || this.cpuSliceMs > 16 || !Number.isFinite(this.cancelTimeoutMs) ||
            this.cancelTimeoutMs < 1 || this.cancelTimeoutMs > 5000) {
            throw new RangeError("invalid browser hash worker bounds");
        }
        this.qualification = new Promise<void>((resolve, reject) => {
            this.qualificationResolve = resolve;
            this.qualificationReject = reject;
        });
        // A caller may inspect stats and choose renderer fallback without
        // awaiting qualification. Keep that legitimate path rejection-safe.
        void this.qualification.catch(() => {});
        this.start();
    }

    ready(): Promise<void> { return this.qualification; }

    run(bytes: Uint8Array, options: { feedBytes: number; signal?: AbortSignal }): Promise<BrowserHashWorkerResult> {
        if (options.signal?.aborted) return Promise.reject(abortError());
        if (this.state !== "ready") {
            const residual = this.unconfirmedReleaseBytes();
            return Promise.reject(new BrowserHashWorkerError("browser hash worker is unavailable", "UNAVAILABLE",
                !this.terminationRequested, residual));
        }
        if (!(bytes.buffer instanceof ArrayBuffer) || bytes.byteOffset !== 0 ||
            bytes.byteLength !== bytes.buffer.byteLength || bytes.byteLength > BROWSER_HASH_WORKER_MAX_INPUT_BYTES) {
            return Promise.reject(new BrowserHashWorkerError("browser hash input is not an owned bounded buffer", "PROTOCOL"));
        }
        if (!validBrowserHashFeedBytes(options.feedBytes)) {
            return Promise.reject(new RangeError(`feedBytes must be ${BROWSER_HASH_WORKER_MIN_FEED_BYTES}..${BROWSER_HASH_WORKER_MAX_FEED_BYTES}`));
        }
        if (this.queue.length + (this.active ? 1 : 0) >= this.maxQueuedJobs ||
            this.ownedBytes + bytes.byteLength > this.maxOwnedBytes) {
            return Promise.reject(new BrowserHashWorkerError("browser hash worker admission is full", "UNAVAILABLE"));
        }
        const ownedBuffer = bytes.buffer as ArrayBuffer;
        return new Promise<BrowserHashWorkerResult>((resolve, reject) => {
            const pending: PendingJob = {
                id: this.nextJobId++, buffer: ownedBuffer, bytes: bytes.byteLength,
                feedBytes: options.feedBytes, signal: options.signal, resolve, reject,
                dispatched: false, settled: false,
            };
            if (options.signal) {
                pending.abortListener = () => this.abort(pending);
                options.signal.addEventListener("abort", pending.abortListener, { once: true });
            }
            this.ownedBytes += pending.bytes;
            this.queue.push(pending);
            this.dispatch();
        });
    }

    stats(): BrowserHashWorkerStats {
        return { state: this.state, wasmMode: this.mode ?? (this.state === "starting" ? "starting" : "unavailable"),
            active: this.active ? 1 : 0, queued: this.queue.length, ownedBytes: this.ownedBytes,
            maxOwnedBytes: this.maxOwnedBytes, sourceChars: this.source.length,
            wasmHeapBytes: this.wasmHeapBytes, quarantinedPayloadBytes: this.quarantinedPayloadBytes,
            fallbackSafe: !this.terminationRequested, generation: this.generation };
    }

    diagnostics(): BrowserHashWorkerDiagnostics {
        return { qualified: this.mode !== null, failureCount: this.failureCount,
            completedJobs: this.completedJobs, cancelledJobs: this.cancelledJobs,
            lastFailure: this.lastFailure, terminationRequested: this.terminationRequested,
            cleanupEvidence: this.terminationRequested ? "shutdown-unconfirmed" : "not-requested",
            unconfirmedReleaseBytes: this.unconfirmedReleaseBytes() };
    }

    close(): void {
        if (this.state === "closed") return;
        this.stop("CLOSED", "closed");
        this.report({ kind: "closed", reason: "CLOSED", generation: this.generation });
    }

    private start(): void {
        this.report({ kind: "starting", reason: "QUALIFIED", generation: this.generation });
        let scalarWasm: ArrayBuffer | undefined;
        let simdWasm: ArrayBuffer | undefined;
        let fixtureBuffer: ArrayBuffer | undefined;
        try {
            if (!this.host.available()) return this.failQualification("UNAVAILABLE");
            if (typeof this.source !== "string" || this.source.length === 0 ||
                this.source.length > BROWSER_HASH_WORKER_MAX_SOURCE_CHARS) {
                return this.failQualification("SOURCE_INVALID");
            }
            scalarWasm = this.options.scalarWasm;
            simdWasm = this.options.simdWasm;
            if (!(scalarWasm instanceof ArrayBuffer) || !(simdWasm instanceof ArrayBuffer) ||
                scalarWasm.byteLength < 8 || simdWasm.byteLength < 8 ||
                scalarWasm.byteLength > BROWSER_HASH_WORKER_MAX_MODULE_BYTES ||
                simdWasm.byteLength > BROWSER_HASH_WORKER_MAX_MODULE_BYTES) {
                return this.failQualification("SOURCE_INVALID");
            }
            this.moduleByteLengths = [scalarWasm.byteLength, simdWasm.byteLength];
            this.qualificationInFlightBytes = BROWSER_HASH_WORKER_QUALIFICATION_BYTES +
                scalarWasm.byteLength + simdWasm.byteLength;
            this.url = this.host.createUrl(this.source);
            this.worker = this.host.createWorker(this.url);
            this.worker.onmessage = event => this.receive(event.data);
            this.worker.onerror = () => this.fault("CRASH");
            this.worker.onmessageerror = () => this.fault("MESSAGE_ERROR");
            const fixture = browserHashQualificationFixture();
            fixtureBuffer = fixture.buffer as ArrayBuffer;
            this.qualificationBuffer = fixtureBuffer;
            this.qualificationTimer = setTimeout(() => this.failQualification("QUALIFICATION_TIMEOUT"),
                this.qualificationTimeoutMs);
            this.worker.postMessage({ schema: BROWSER_HASH_WORKER_SCHEMA, type: "init",
                generation: this.generation, qualification: fixtureBuffer, scalarWasm, simdWasm },
            [fixtureBuffer, scalarWasm, simdWasm]);
            if (fixture.buffer.byteLength !== 0 || scalarWasm.byteLength !== 0 || simdWasm.byteLength !== 0) {
                this.qualificationInFlightBytes =
                    (fixtureBuffer.byteLength === 0 ? BROWSER_HASH_WORKER_QUALIFICATION_BYTES : 0) +
                    (scalarWasm.byteLength === 0 ? this.moduleByteLengths[0] : 0) +
                    (simdWasm.byteLength === 0 ? this.moduleByteLengths[1] : 0);
                return this.failQualification("TRANSFER_UNSUPPORTED");
            }
        } catch {
            this.qualificationInFlightBytes =
                (fixtureBuffer?.byteLength === 0 ? BROWSER_HASH_WORKER_QUALIFICATION_BYTES : 0) +
                (scalarWasm?.byteLength === 0 ? this.moduleByteLengths[0] : 0) +
                (simdWasm?.byteLength === 0 ? this.moduleByteLengths[1] : 0);
            this.failQualification("CONSTRUCTOR");
        }
    }

    private receive(message: BrowserHashWorkerResponse): void {
        if (this.state === "closed" || this.state === "unavailable") return;
        if (!message || message.schema !== BROWSER_HASH_WORKER_SCHEMA || message.generation !== this.generation) {
            // An event from a retired generation has no authority over this
            // pool. Same-generation malformed messages fail closed below.
            if (message?.generation !== this.generation) return;
            return this.fault("PROTOCOL");
        }
        if (this.state === "starting") {
            if (message.type === "qualified" || message.type === "failed") {
                const returned = message.qualification instanceof ArrayBuffer &&
                    message.qualification.byteLength === BROWSER_HASH_WORKER_QUALIFICATION_BYTES &&
                    message.scalarWasm instanceof ArrayBuffer &&
                    message.scalarWasm.byteLength === this.moduleByteLengths[0] &&
                    message.simdWasm instanceof ArrayBuffer &&
                    message.simdWasm.byteLength === this.moduleByteLengths[1];
                if (returned) this.qualificationInFlightBytes = 0;
            }
            if (message.type !== "qualified" || !(message.qualification instanceof ArrayBuffer) ||
                message.qualification.byteLength !== BROWSER_HASH_WORKER_QUALIFICATION_BYTES ||
                !(message.scalarWasm instanceof ArrayBuffer) || !(message.simdWasm instanceof ArrayBuffer) ||
                message.scalarWasm.byteLength !== this.moduleByteLengths[0] ||
                message.simdWasm.byteLength !== this.moduleByteLengths[1] ||
                (message.wasmMode !== "scalar" && message.wasmMode !== "simd") ||
                !Number.isSafeInteger(message.wasmHeapBytes) || message.wasmHeapBytes <= 0 ||
                message.wasmHeapBytes > BROWSER_HASH_WORKER_MAX_HEAP_BYTES) {
                return this.failQualification("QUALIFICATION_FAILED");
            }
            const check = new Uint8Array(message.qualification);
            if (!check.every((value, index) => value === (index & 255))) {
                return this.failQualification("QUALIFICATION_FAILED");
            }
            this.qualificationBuffer = null;
            this.clearQualificationTimer();
            this.mode = message.wasmMode;
            this.wasmHeapBytes = message.wasmHeapBytes;
            this.state = "ready";
            this.qualificationResolve();
            this.report({ kind: "ready", reason: "QUALIFIED", generation: this.generation,
                wasmMode: this.mode });
            this.dispatch();
            return;
        }
        const active = this.active;
        if (!active || message.type === "qualified" || message.type === "failed" && message.jobId === null ||
            !("jobId" in message) || message.jobId !== active.id) return this.fault("PROTOCOL");
        if (message.type === "failed") {
            if (message.bytes instanceof ArrayBuffer && message.bytes.byteLength === active.bytes) {
                return this.fault(message.reason === "HASH" ? "CRASH" : "PROTOCOL", active, true);
            }
            return this.fault("PROTOCOL");
        }
        if (!(message.bytes instanceof ArrayBuffer) || message.bytes.byteLength !== active.bytes) {
            return this.fault("PROTOCOL");
        }
        this.active = null;
        this.clearJobTimer();
        if (message.type === "cancelled" || active.signal?.aborted) {
            this.cancelledJobs++;
            this.settle(active, abortError());
        } else if (message.type === "result" && validBrowserHash(message.hash) &&
            Number.isFinite(message.hashMs) && message.hashMs >= 0 &&
            Number.isSafeInteger(message.wasmHeapBytes) && message.wasmHeapBytes > 0 &&
            message.wasmHeapBytes <= BROWSER_HASH_WORKER_MAX_HEAP_BYTES) {
            this.completedJobs++;
            this.wasmHeapBytes = message.wasmHeapBytes;
            this.settle(active, null, { hash: message.hash,
                returnedBytes: new Uint8Array(message.bytes), hashMs: message.hashMs });
        } else {
            return this.fault("PROTOCOL", active, true);
        }
        this.dispatch();
    }

    private dispatch(): void {
        if (this.state !== "ready" || this.active || !this.worker) return;
        while (this.queue.length) {
            const pending = this.queue.shift()!;
            if (pending.signal?.aborted) {
                this.settle(pending, abortError());
                continue;
            }
            this.active = pending;
            pending.dispatched = true;
            try {
                this.worker.postMessage({ schema: BROWSER_HASH_WORKER_SCHEMA, type: "hash",
                    generation: this.generation, jobId: pending.id, bytes: pending.buffer,
                    feedBytes: pending.feedBytes, cpuSliceMs: this.cpuSliceMs }, [pending.buffer]);
                if (pending.buffer.byteLength !== 0) return this.fault("TRANSFER_UNSUPPORTED", pending);
                this.jobTimer = setTimeout(() => this.fault("JOB_TIMEOUT", pending), this.jobTimeoutMs);
            } catch {
                this.fault("CRASH", pending);
            }
            return;
        }
    }

    private abort(pending: PendingJob): void {
        if (pending.settled) return;
        if (this.active === pending && this.worker) {
            try {
                this.worker.postMessage({ schema: BROWSER_HASH_WORKER_SCHEMA, type: "cancel",
                    generation: this.generation, jobId: pending.id }, []);
                this.clearJobTimer();
                this.jobTimer = setTimeout(() => this.fault("JOB_TIMEOUT", pending), this.cancelTimeoutMs);
            } catch { this.fault("CRASH", pending); }
            return;
        }
        const index = this.queue.indexOf(pending);
        if (index >= 0) this.queue.splice(index, 1);
        this.cancelledJobs++;
        this.settle(pending, abortError());
    }

    private failQualification(reason: BrowserHashWorkerFailure): void {
        if (this.state !== "starting") return;
        this.failureCount++;
        this.lastFailure = reason;
        this.state = reason === "UNAVAILABLE" || reason === "SOURCE_INVALID" ? "unavailable" : "unavailable";
        this.clearQualificationTimer();
        this.terminate();
        const residual = this.unconfirmedReleaseBytes();
        const error = new BrowserHashWorkerError("browser hash worker qualification failed", reason,
            !this.terminationRequested, residual);
        this.qualificationReject(error);
        this.report({ kind: reason === "UNAVAILABLE" ? "unavailable" : "failure", reason,
            generation: this.generation });
    }

    private fault(reason: BrowserHashWorkerFailure, active = this.active,
        activeBufferReturned = false): void {
        if (this.state === "closed" || this.state === "unavailable") return;
        if (this.state === "starting") return this.failQualification(reason);
        this.failureCount++;
        this.lastFailure = reason;
        this.state = "unavailable";
        this.clearJobTimer();
        this.terminate();
        if (active?.dispatched && !activeBufferReturned) this.quarantinedPayloadBytes += active.bytes;
        const residual = this.unconfirmedReleaseBytes();
        const error = new BrowserHashWorkerError("browser hash worker failed", reason, false, residual);
        if (active && !active.settled) this.settle(active,
            active.signal?.aborted ? abortError() : error);
        this.active = null;
        for (const pending of this.queue.splice(0)) this.settle(pending, error);
        this.report({ kind: "failure", reason, generation: this.generation });
    }

    private stop(reason: BrowserHashWorkerFailure, state: "closed" | "unavailable"): void {
        this.state = state;
        this.clearQualificationTimer();
        this.clearJobTimer();
        if (this.active?.dispatched) this.quarantinedPayloadBytes += this.active.bytes;
        this.terminate();
        const residual = this.unconfirmedReleaseBytes();
        const error = new BrowserHashWorkerError("browser hash worker closed", reason,
            !this.terminationRequested, residual);
        if (this.active) this.settle(this.active, error);
        this.active = null;
        for (const pending of this.queue.splice(0)) this.settle(pending, error);
        this.qualificationReject(error);
    }

    private terminate(): void {
        if (this.worker) {
            this.terminationRequested = true;
            try { this.worker.terminate(); } catch { /* already failed */ }
            this.worker.onmessage = null;
            this.worker.onerror = null;
            this.worker.onmessageerror = null;
            this.worker = null;
        }
        if (this.url) {
            try { this.host.revokeUrl(this.url); } catch { /* best effort */ }
            this.url = null;
        }
        this.qualificationBuffer = null;
    }

    private unconfirmedReleaseBytes(): number {
        return this.quarantinedPayloadBytes + this.qualificationInFlightBytes +
            (this.terminationRequested ? this.wasmHeapBytes ?? 0 : 0);
    }

    private settle(pending: PendingJob, error: Error | null, result?: BrowserHashWorkerResult): void {
        if (pending.settled) return;
        pending.settled = true;
        pending.signal?.removeEventListener("abort", pending.abortListener!);
        this.ownedBytes = Math.max(0, this.ownedBytes - pending.bytes);
        if (error) pending.reject(error);
        else pending.resolve(result!);
    }

    private clearQualificationTimer(): void {
        if (this.qualificationTimer !== null) clearTimeout(this.qualificationTimer);
        this.qualificationTimer = null;
    }

    private clearJobTimer(): void {
        if (this.jobTimer !== null) clearTimeout(this.jobTimer);
        this.jobTimer = null;
    }

    private report(event: BrowserHashWorkerDiagnostic): void {
        try { this.options.onDiagnostic?.(event); } catch { /* diagnostics cannot own lifecycle */ }
    }
}

export function createBrowserHashWorkerPool(
    source: string,
    options: BrowserHashWorkerPoolOptions = {},
): BrowserHashWorkerPool | null {
    const host = options.host ?? defaultHost;
    try {
        if (!host.available()) {
            options.onDiagnostic?.({ kind: "unavailable", reason: "UNAVAILABLE",
                generation: 0 });
            return null;
        }
        return new BrowserHashWorkerPool(source, { ...options, host });
    } catch {
        options.onDiagnostic?.({ kind: "unavailable", reason: "CONSTRUCTOR", generation: 0 });
        return null;
    }
}
