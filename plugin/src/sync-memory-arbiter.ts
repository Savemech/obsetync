import {
    ResourceBudgetClosedError, ResourceBudgetOversizedError, ResourceBudgetQueueFullError,
    type ResourceReservation,
} from "./resource-budget";

/** Managed owner accounting only. This is not a process-RSS or JS-heap meter. */
export type SyncMemoryOwnerClass = "interactive" | "root-review" | "root-plan" |
    "prepared-manifest" | "browser-worker" | "transfer";

export interface SyncMemoryArbiterOptions {
    capacityBytes: number;
    /** Capacity kept available to interactive owners while background work is live. */
    interactiveReserveBytes?: number;
    maxQueuedRequests?: number;
    maxQueuedBytes?: number;
}

export interface SyncMemoryLease extends ResourceReservation {
    readonly owner: SyncMemoryOwnerClass;
}

export class SyncMemoryUnavailableError extends Error {
    constructor(readonly owner: SyncMemoryOwnerClass, readonly requestedBytes: number) {
        super(`sync memory is currently unavailable for ${owner} (${requestedBytes} bytes)`);
        this.name = "SyncMemoryUnavailableError";
    }
}

export interface SyncMemoryArbiterSnapshot {
    capacityBytes: number;
    interactiveReserveBytes: number;
    usedBytes: number;
    peakUsedBytes: number;
    activeLeases: number;
    queuedBytes: number;
    queuedRequests: number;
    closed: boolean;
    owners: Record<SyncMemoryOwnerClass, { usedBytes: number; activeLeases: number; queuedBytes: number; queuedRequests: number }>;
}

interface Waiter {
    owner: SyncMemoryOwnerClass;
    bytes: number;
    resolve(value: SyncMemoryLease): void;
    reject(error: unknown): void;
    signal?: AbortSignal;
    onAbort?: () => void;
}

const OWNER_CLASSES: readonly SyncMemoryOwnerClass[] = ["interactive", "root-review", "root-plan",
    "prepared-manifest", "browser-worker", "transfer"];
const MAX_QUEUED_REQUESTS = 1024;

function safeNonnegative(value: number, name: string): number {
    if (!Number.isSafeInteger(value) || value < 0) throw new RangeError(`${name} must be a nonnegative safe integer`);
    return value;
}
function safePositive(value: number, name: string): number {
    if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError(`${name} must be a positive safe integer`);
    return value;
}
function aborted(signal: AbortSignal): unknown {
    if (signal.reason !== undefined) return signal.reason;
    const error = new Error("sync memory admission aborted"); error.name = "AbortError"; return error;
}

/**
 * One injected admission authority for resident and transient sync owners.
 * Interactive waiters have their own FIFO lane and may use reserved capacity;
 * background owners cannot consume that reserve. Both queue count and queued
 * demand are bounded, and accounting is acquired before caller allocation.
 */
export class SyncMemoryArbiter {
    private readonly capacityBytes: number;
    private readonly interactiveReserveBytes: number;
    private readonly maxQueuedRequests: number;
    private readonly maxQueuedBytes: number;
    private readonly interactiveQueue: Waiter[] = [];
    private readonly backgroundQueue: Waiter[] = [];
    private usedBytes = 0;
    private peakUsedBytes = 0;
    private activeLeases = 0;
    private queuedBytes = 0;
    private closed = false;
    private readonly ownerUsed = new Map<SyncMemoryOwnerClass, number>();
    private readonly ownerActive = new Map<SyncMemoryOwnerClass, number>();

    constructor(options: SyncMemoryArbiterOptions) {
        this.capacityBytes = safePositive(options.capacityBytes, "capacityBytes");
        this.interactiveReserveBytes = safeNonnegative(options.interactiveReserveBytes ?? 0, "interactiveReserveBytes");
        if (this.interactiveReserveBytes >= this.capacityBytes) {
            throw new RangeError("interactiveReserveBytes must be less than capacityBytes");
        }
        this.maxQueuedRequests = safePositive(options.maxQueuedRequests ?? 64, "maxQueuedRequests");
        if (this.maxQueuedRequests > MAX_QUEUED_REQUESTS) {
            throw new RangeError(`maxQueuedRequests must not exceed ${MAX_QUEUED_REQUESTS}`);
        }
        this.maxQueuedBytes = safePositive(options.maxQueuedBytes ?? Math.min(Number.MAX_SAFE_INTEGER, this.capacityBytes * 4), "maxQueuedBytes");
    }

    reserve(owner: SyncMemoryOwnerClass, bytes: number, options: { signal?: AbortSignal; wait?: boolean } = {}): Promise<SyncMemoryLease> {
        try { this.assertOwner(owner); safePositive(bytes, "reservation bytes"); }
        catch (error) { return Promise.reject(error); }
        if (options.signal?.aborted) return Promise.reject(aborted(options.signal));
        if (this.closed) return Promise.reject(new ResourceBudgetClosedError());
        const ownerCapacity = owner === "interactive" ? this.capacityBytes : this.capacityBytes - this.interactiveReserveBytes;
        if (bytes > ownerCapacity) return Promise.reject(new ResourceBudgetOversizedError(bytes, ownerCapacity));
        const lane = owner === "interactive" ? this.interactiveQueue : this.backgroundQueue;
        const mayEnter = owner === "interactive" ? lane.length === 0 : lane.length === 0 && this.interactiveQueue.length === 0;
        if (mayEnter && this.fits(owner, bytes)) return Promise.resolve(this.grant(owner, bytes));
        if (options.wait === false) return Promise.reject(new SyncMemoryUnavailableError(owner, bytes));
        if (this.interactiveQueue.length + this.backgroundQueue.length >= this.maxQueuedRequests ||
            bytes > this.maxQueuedBytes - this.queuedBytes) return Promise.reject(new ResourceBudgetQueueFullError());
        return new Promise<SyncMemoryLease>((resolve, reject) => {
            const waiter: Waiter = { owner, bytes, resolve, reject, signal: options.signal };
            if (waiter.signal) {
                waiter.onAbort = () => {
                    const queue = waiter.owner === "interactive" ? this.interactiveQueue : this.backgroundQueue;
                    const index = queue.indexOf(waiter);
                    if (index < 0) return;
                    queue.splice(index, 1); this.detach(waiter); reject(aborted(waiter.signal!)); this.drain();
                };
                waiter.signal.addEventListener("abort", waiter.onAbort, { once: true });
            }
            lane.push(waiter); this.queuedBytes += bytes;
        });
    }

    /** Synchronous fail-fast admission for owners which must detach caller
     * input before their first await. Null never queues or overcommits. */
    tryReserve(owner: SyncMemoryOwnerClass, bytes: number): SyncMemoryLease | null {
        this.assertOwner(owner); safePositive(bytes, "reservation bytes");
        if (this.closed) return null;
        const ownerCapacity = owner === "interactive" ? this.capacityBytes : this.capacityBytes - this.interactiveReserveBytes;
        if (bytes > ownerCapacity) throw new ResourceBudgetOversizedError(bytes, ownerCapacity);
        const lane = owner === "interactive" ? this.interactiveQueue : this.backgroundQueue;
        const mayEnter = owner === "interactive" ? lane.length === 0 : lane.length === 0 && this.interactiveQueue.length === 0;
        return mayEnter && this.fits(owner, bytes) ? this.grant(owner, bytes) : null;
    }

    /** Adapter for transient owners that use the ResourceBudget reserve shape. */
    budgetFor(owner: SyncMemoryOwnerClass): { reserve(bytes: number, options?: { signal?: AbortSignal }): Promise<ResourceReservation> } {
        this.assertOwner(owner);
        return { reserve: (bytes, options) => this.reserve(owner, bytes, options) };
    }

    close(): void {
        if (this.closed) return;
        this.closed = true;
        const error = new ResourceBudgetClosedError();
        for (const queue of [this.interactiveQueue, this.backgroundQueue]) {
            for (const waiter of queue.splice(0)) { this.detach(waiter); waiter.reject(error); }
        }
    }

    snapshot(): SyncMemoryArbiterSnapshot {
        const owners = {} as SyncMemoryArbiterSnapshot["owners"];
        for (const owner of OWNER_CLASSES) {
            const waiters = (owner === "interactive" ? this.interactiveQueue : this.backgroundQueue).filter(row => row.owner === owner);
            owners[owner] = { usedBytes: this.ownerUsed.get(owner) ?? 0, activeLeases: this.ownerActive.get(owner) ?? 0,
                queuedBytes: waiters.reduce((sum, row) => sum + row.bytes, 0), queuedRequests: waiters.length };
        }
        return { capacityBytes: this.capacityBytes, interactiveReserveBytes: this.interactiveReserveBytes,
            usedBytes: this.usedBytes, peakUsedBytes: this.peakUsedBytes, activeLeases: this.activeLeases,
            queuedBytes: this.queuedBytes, queuedRequests: this.interactiveQueue.length + this.backgroundQueue.length,
            closed: this.closed, owners };
    }

    private assertOwner(owner: SyncMemoryOwnerClass): void {
        if (!OWNER_CLASSES.includes(owner)) throw new TypeError("invalid sync memory owner class");
    }
    private fits(owner: SyncMemoryOwnerClass, bytes: number): boolean {
        const limit = owner === "interactive" ? this.capacityBytes : this.capacityBytes - this.interactiveReserveBytes;
        return bytes <= limit - this.usedBytes;
    }
    private grant(owner: SyncMemoryOwnerClass, bytes: number): SyncMemoryLease {
        this.usedBytes += bytes; this.activeLeases++; this.peakUsedBytes = Math.max(this.peakUsedBytes, this.usedBytes);
        this.ownerUsed.set(owner, (this.ownerUsed.get(owner) ?? 0) + bytes);
        this.ownerActive.set(owner, (this.ownerActive.get(owner) ?? 0) + 1);
        let released = false;
        return { owner, bytes, release: () => {
            if (released) return; released = true;
            this.usedBytes -= bytes; this.activeLeases--;
            this.ownerUsed.set(owner, (this.ownerUsed.get(owner) ?? 0) - bytes);
            this.ownerActive.set(owner, (this.ownerActive.get(owner) ?? 0) - 1);
            this.drain();
        } };
    }
    private detach(waiter: Waiter): void {
        this.queuedBytes -= waiter.bytes;
        if (waiter.signal && waiter.onAbort) waiter.signal.removeEventListener("abort", waiter.onAbort);
    }
    private drain(): void {
        if (this.closed) return;
        for (;;) {
            const queue = this.interactiveQueue.length ? this.interactiveQueue : this.backgroundQueue;
            const waiter = queue[0];
            if (!waiter || !this.fits(waiter.owner, waiter.bytes)) return;
            queue.shift(); this.detach(waiter); waiter.resolve(this.grant(waiter.owner, waiter.bytes));
        }
    }
}
