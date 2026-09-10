import type { HashRuntime } from "./hash-runtime";
import {
    ResourceBudget,
    ResourceBudgetClosedError,
    type ResourceBudgetSnapshot,
    type ResourceReservation,
    type ShrinkableResourceReservation,
} from "./resource-budget";
import { throwIfWorkAborted } from "./work-scheduler";
import type { SyncMemoryArbiter, SyncMemoryLease } from "./sync-memory-arbiter";

const MIB = 1024 * 1024;
const MOBILE_TRANSIENT_CEILING = 32 * MIB;
const DESKTOP_TRANSIENT_CEILING = 128 * MIB;
const WORKSET_OVERHEAD_BYTES = 64 * 1024;
const TRANSPORT_HEADER_ALLOWANCE_BYTES = 64 * 1024;
export const TRANSPORT_ERROR_PAYLOAD_ALLOWANCE_BYTES = 1024;

export interface TransientMemoryTuning {
    runtime: HashRuntime;
    transientBudgetBytes: number;
}

/** A scope lends already-admitted bytes through this same small interface. */
export interface TransientReservationBudget {
    reserve(bytes: number, options?: { signal?: AbortSignal }): Promise<ResourceReservation>;
}

function byteCount(value: number, name: string, allowZero = false): number {
    if (!Number.isSafeInteger(value) || value < (allowZero ? 0 : 1)) {
        throw new RangeError(`${name} must be a ${allowZero ? "non-negative" : "positive"} safe integer`);
    }
    return value;
}

/** One policy/accounting pool for admitted source and transfer worksets.
 * Explicit instances support isolated consumers/tests; the exported functions
 * below all use ONE module-wide instance, including hash-source compatibility
 * exports. These estimates do not account for all metadata, worker fixed heaps,
 * retained WASM memory, host allocations or process RSS. */
export class TransientMemoryBudget {
    private budget = new ResourceBudget({ capacityBytes: MOBILE_TRANSIENT_CEILING });
    private arbiter?: SyncMemoryArbiter;

    configure(tuning: TransientMemoryTuning, arbiter?: SyncMemoryArbiter): void {
        byteCount(tuning.transientBudgetBytes, "transientBudgetBytes");
        if (arbiter && this.arbiter && arbiter !== this.arbiter) {
            const current = this.budget.snapshot();
            if (current.activeReservations !== 0 || current.queuedRequests !== 0) {
                throw new Error("cannot replace sync memory arbiter while transient owners are live");
            }
        }
        if (arbiter) this.arbiter = arbiter;
        const ceiling = tuning.runtime === "desktop"
            ? DESKTOP_TRANSIENT_CEILING : MOBILE_TRANSIENT_CEILING;
        const capacityBytes = Math.min(tuning.transientBudgetBytes, ceiling);
        const previous = this.budget.snapshot();
        if (previous.closed) {
            if (previous.activeReservations !== 0) throw new ResourceBudgetClosedError();
            this.budget = new ResourceBudget({ capacityBytes });
        } else {
            this.budget.setCapacity(capacityBytes);
        }
    }

    async reserve(bytes: number, options: { signal?: AbortSignal } = {}): Promise<ShrinkableResourceReservation> {
        const local = await this.budget.reserve(bytes, options);
        let shared: SyncMemoryLease | undefined;
        try {
            shared = this.arbiter
                ? await this.arbiter.reserve("transfer", bytes, options)
                : undefined;
        } catch (error) {
            local.release();
            throw error;
        }
        let released = false;
        return {
            get bytes() { return local.bytes; },
            shrinkTo(nextBytes: number) { local.shrinkTo(nextBytes); },
            release() {
                if (released) return;
                released = true;
                // The shared owner deliberately remains charged at the original
                // complete-workset bound until every child/native tail settles.
                // Local shrink only improves the dynamic per-profile sublimit.
                shared?.release();
                local.release();
            },
        };
    }

    snapshot(): ResourceBudgetSnapshot { return this.budget.snapshot(); }
    close(): void { this.budget.close(); this.arbiter = undefined; }
}

const transientMemory = new TransientMemoryBudget();

export function configureTransientMemory(tuning: TransientMemoryTuning, arbiter?: SyncMemoryArbiter): void {
    transientMemory.configure(tuning, arbiter);
}

export function transientMemorySnapshot(): ResourceBudgetSnapshot {
    return transientMemory.snapshot();
}

export function closeTransientMemory(): void {
    transientMemory.close();
}

export function reserveTransientWorkset(
    bytes: number,
    options: { signal?: AbortSignal } = {},
): Promise<ShrinkableResourceReservation> {
    return transientMemory.reserve(bytes, options);
}

export interface TransientWorksetEstimate {
    /** Caller-owned source/hash buffers retained until scope.close(). */
    ownerBytes: number;
    /** Reusable quota for independently settling transport/crypto work. */
    workBytes: number;
    totalBytes: number;
}

/** One negotiated plaintext pack plus header allowance: pack encoding, auth
 * plaintext, ciphertext, assembled envelope, crypto/native bridge allowances.
 * This does not reserve two simultaneous fallback attempts or retained old
 * recursive retry packs. Such callers must settle/release the first attempt or
 * explicitly include its retained workset before allocating the next one. */
export function estimateTransportWorkset(transportPayloadBytes: number): number {
    byteCount(transportPayloadBytes, "transportPayloadBytes", true);
    return byteCount(
        6 * (transportPayloadBytes + TRANSPORT_HEADER_ALLOWANCE_BYTES) + WORKSET_OVERHEAD_BYTES,
        "transport workset",
    );
}

/** Complete root-outcome HTTP admission. The canonical request encoder can
 * temporarily retain a UTF-16 JSON string plus its UTF-8 body (3B). Response
 * decoding retains the exact plaintext copy, UTF-16 parser text, and the
 * terminal receipt's second bounded JSON validation (6R). Parsed JS objects,
 * fixed crypto/WASM heaps, the host's initial native receive allocation and
 * process RSS remain outside this byte-workspace estimate. */
export function estimateRootOutcomeWorkset(
    requestBytes: number,
    responseBytes: number,
): TransientWorksetEstimate {
    byteCount(requestBytes, "root outcome requestBytes");
    byteCount(responseBytes, "root outcome responseBytes");
    const ownerBytes = byteCount(
        3 * requestBytes + 6 * responseBytes,
        "root outcome owner workset",
    );
    const workBytes = estimateTransportWorkset(Math.max(requestBytes, responseBytes));
    return {
        ownerBytes,
        workBytes,
        totalBytes: byteCount(ownerBytes + workBytes, "complete root outcome workset"),
    };
}

/** Conservative complete upload-batch admission, not observed RSS. sourceBytes
 * includes all simultaneously retained source/ranged data. hashBatchBytes is
 * the largest concatenated hash batch (zero for non-batched streaming hashing).
 * transportPayloadBytes must bound ONE actual negotiated pack for this batch;
 * an existing immutable chunk may not be silently split/redefined to fit it. */
export function estimateUploadBatchWorkset(options: {
    sourceBytes: number;
    hashBatchBytes: number;
    feedBytes: number;
    transportPayloadBytes: number;
    /** Native ranged reads fill these caller-provided arrays directly. Their
     * bounded retained queue needs one allowance, not three whole-file copies. */
    retainedRangeBytes?: number;
}): TransientWorksetEstimate {
    byteCount(options.sourceBytes, "sourceBytes", true);
    byteCount(options.hashBatchBytes, "hashBatchBytes", true);
    byteCount(options.feedBytes, "feedBytes");
    const retainedRangeBytes = byteCount(options.retainedRangeBytes ?? 0, "retainedRangeBytes", true);
    const ownerBytes = byteCount(
        3 * options.sourceBytes + 2 * options.hashBatchBytes +
        2 * options.feedBytes + retainedRangeBytes + WORKSET_OVERHEAD_BYTES,
        "upload owner workset",
    );
    const workBytes = estimateTransportWorkset(options.transportPayloadBytes);
    return {
        ownerBytes, workBytes,
        totalBytes: byteCount(ownerBytes + workBytes, "complete upload workset"),
    };
}

export interface TransientScopeSnapshot {
    totalBytes: number;
    ownerBytes: number;
    ownerClosed: boolean;
    parentReleased: boolean;
    trackedWork: number;
    /** This is a subdivision of the parent, not additional globally-used bytes. */
    work: ResourceBudgetSnapshot;
}

export interface TransientWorkScope extends TransientReservationBudget {
    /** Use only for allocations counted in ownerBytes. This adds lifetime
     * retention, NOT byte allowance. Pass the real native/crypto completion
     * promise, never a timeout/abort Promise.race that may finish before it. */
    track<T>(work: () => T | Promise<T>): Promise<T>;
    /** Reserve from the child quota before creating its buffers. The callback
     * must settle only after those buffers/native consumers are no longer live. */
    run<T>(bytes: number, work: (context: TransientWorkContext) => T | Promise<T>,
        options?: { signal?: AbortSignal }): Promise<T>;
    /** Owner buffers are now released; ungranted child work is rejected. The
     * parent remains charged until all granted children/tracked tasks settle. */
    close(): void;
    snapshot(): TransientScopeSnapshot;
}

export interface TransientWorkContext {
    readonly bytes: number;
    /** Retain THIS child quota, not merely its global parent, while native work
     * outlives a caller-visible timeout. Pass the actual completion promise. */
    track<T>(work: () => T | Promise<T>): Promise<T>;
    /** Before reusing one callback's workspace for a fallback/retry, wait for
     * tracked old native work and fence that old attempt against new tasks. */
    waitForIdle(): Promise<void>;
}

class RetainedWorkContext implements TransientWorkContext {
    readonly bytes: number;
    private ownerClosed = false;
    private released = false;
    private activeTasks = 0;
    private idlePromise?: Promise<void>;
    private resolveIdle?: () => void;

    constructor(private readonly lease: ResourceReservation) { this.bytes = lease.bytes; }

    async track<T>(work: () => T | Promise<T>): Promise<T> {
        if (this.ownerClosed) throw new ResourceBudgetClosedError();
        this.activeTasks++;
        try { return await work(); }
        finally {
            this.activeTasks--;
            if (this.activeTasks === 0) {
                this.resolveIdle?.();
                this.idlePromise = undefined;
                this.resolveIdle = undefined;
            }
            this.releaseIfDrained();
        }
    }

    waitForIdle(): Promise<void> {
        if (this.activeTasks === 0) return Promise.resolve();
        if (!this.idlePromise) {
            this.idlePromise = new Promise<void>((resolve) => { this.resolveIdle = resolve; });
        }
        return this.idlePromise;
    }

    close(): void {
        this.ownerClosed = true;
        this.releaseIfDrained();
    }

    private releaseIfDrained(): void {
        if (this.ownerClosed && !this.released && this.activeTasks === 0) {
            this.released = true;
            this.lease.release();
        }
    }
}

class OwnedTransientWorkScope implements TransientWorkScope {
    private readonly workBudget: ResourceBudget;
    private ownerClosed = false;
    private parentReleased = false;
    private trackedWork = 0;

    constructor(private readonly parent: ResourceReservation, private readonly ownerBytes: number,
        workBytes: number) {
        this.workBudget = new ResourceBudget({ capacityBytes: workBytes });
    }

    async reserve(bytes: number, options: { signal?: AbortSignal } = {}): Promise<ResourceReservation> {
        if (this.ownerClosed) throw new ResourceBudgetClosedError();
        const lease = await this.workBudget.reserve(bytes, options);
        let released = false;
        return {
            bytes: lease.bytes,
            release: () => {
                if (released) return;
                released = true;
                lease.release();
                this.releaseParentIfDrained();
            },
        };
    }

    async track<T>(work: () => T | Promise<T>): Promise<T> {
        if (this.ownerClosed) throw new ResourceBudgetClosedError();
        this.trackedWork++;
        try { return await work(); }
        finally {
            this.trackedWork--;
            this.releaseParentIfDrained();
        }
    }

    async run<T>(bytes: number, work: (context: TransientWorkContext) => T | Promise<T>,
        options: { signal?: AbortSignal } = {}): Promise<T> {
        const lease = await this.reserve(bytes, options);
        const context = new RetainedWorkContext(lease);
        try {
            throwIfWorkAborted(options.signal);
            const result = await work(context);
            throwIfWorkAborted(options.signal);
            return result;
        } finally {
            context.close();
        }
    }

    close(): void {
        if (this.ownerClosed) return;
        this.ownerClosed = true;
        this.workBudget.close();
        this.releaseParentIfDrained();
    }

    snapshot(): TransientScopeSnapshot {
        return {
            totalBytes: this.parent.bytes,
            ownerBytes: this.ownerBytes,
            ownerClosed: this.ownerClosed,
            parentReleased: this.parentReleased,
            trackedWork: this.trackedWork,
            work: this.workBudget.snapshot(),
        };
    }

    private releaseParentIfDrained(): void {
        if (this.ownerClosed && !this.parentReleased && this.trackedWork === 0 &&
            this.workBudget.snapshot().activeReservations === 0) {
            this.parentReleased = true;
            this.parent.release();
        }
    }
}

/** Atomically reserve the COMPLETE simultaneous workset before reading a
 * batch. Hand this scope explicitly to nested transport calls; do not hold the
 * parent then try another global reservation for already-covered work. Owner
 * allocations use track(); independently reusable transport bytes use run(). */
export async function reserveTransientScope(
    workset: Pick<TransientWorksetEstimate, "ownerBytes" | "workBytes">,
    options: { signal?: AbortSignal; budget?: TransientReservationBudget } = {},
): Promise<TransientWorkScope> {
    byteCount(workset.ownerBytes, "ownerBytes", true);
    byteCount(workset.workBytes, "workBytes");
    const totalBytes = byteCount(workset.ownerBytes + workset.workBytes, "scope totalBytes");
    const parent = await (options.budget ?? transientMemory).reserve(totalBytes, options);
    try {
        throwIfWorkAborted(options.signal);
        return new OwnedTransientWorkScope(parent, workset.ownerBytes, workset.workBytes);
    } catch (error) {
        parent.release();
        throw error;
    }
}
