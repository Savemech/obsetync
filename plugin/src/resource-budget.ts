/**
 * Admission accounting for caller-owned temporary byte buffers. Reserve before
 * allocating, then retain the lease until all associated buffers are released.
 * This does not measure host/native allocations, GC retention, or process RSS,
 * and therefore is not by itself an OOM guarantee.
 */

export interface ResourceBudgetOptions {
    capacityBytes: number;
    /** Queue metadata remains bounded even when each waiter requests one byte. */
    maxQueuedRequests?: number;
    /** Demand limit, not allocated memory; defaults to four times capacity. */
    maxQueuedBytes?: number;
}

export interface ResourceReservation {
    readonly bytes: number;
    /** Idempotent. Call only once the associated allocations are no longer live. */
    release(): void;
}

/** A reservation whose live owner can become smaller without an
 * release/reacquire gap. Shrinking is monotonic and immediately wakes FIFO
 * waiters against the returned capacity. */
export interface ShrinkableResourceReservation extends ResourceReservation {
    shrinkTo(bytes: number): void;
}

export interface ResourceBudgetSnapshot {
    capacityBytes: number;
    usedBytes: number;
    peakUsedBytes: number;
    availableBytes: number;
    activeReservations: number;
    queuedBytes: number;
    queuedRequests: number;
    maxQueuedBytes: number;
    maxQueuedRequests: number;
    closed: boolean;
}

export class ResourceBudgetOversizedError extends RangeError {
    constructor(readonly requestedBytes: number, readonly capacityBytes: number) {
        super(`resource reservation exceeds capacity (${requestedBytes} > ${capacityBytes} bytes)`);
        this.name = "ResourceBudgetOversizedError";
    }
}

export class ResourceBudgetQueueFullError extends Error {
    constructor() {
        super("resource reservation queue is full");
        this.name = "ResourceBudgetQueueFullError";
    }
}

export class ResourceBudgetClosedError extends Error {
    constructor() {
        super("resource budget is closed");
        this.name = "ResourceBudgetClosedError";
    }
}

interface ReservationWaiter {
    bytes: number;
    resolve(reservation: ShrinkableResourceReservation): void;
    reject(error: unknown): void;
    signal?: AbortSignal;
    onAbort?: () => void;
}

const DEFAULT_MAX_QUEUED_REQUESTS = 64;
const MAX_QUEUED_REQUESTS = 1024;

function positiveBytes(bytes: number, name: string): number {
    if (!Number.isSafeInteger(bytes) || bytes <= 0) {
        throw new RangeError(`${name} must be a positive safe-integer byte count`);
    }
    return bytes;
}

function abortReason(signal: AbortSignal): unknown {
    if (signal.reason !== undefined) return signal.reason;
    const error = new Error("resource reservation was aborted");
    error.name = "AbortError";
    return error;
}

/**
 * Strict FIFO reservations with synchronous accounting at grant time. A small
 * request cannot overtake an older waiter, even when it would fit immediately.
 * Queue byte/count ceilings bound unadmitted demand; callers must not allocate
 * the requested buffers while waiting.
 *
 * Cancellation applies only BEFORE grant. Afterwards, including an abort in
 * the microtask before `await reserve()` resumes, the owner must release the
 * lease in `finally`. Automatically releasing a granted lease on abort/close
 * would undercount buffers still retained by unfinished native/crypto work.
 */
export class ResourceBudget {
    private capacityBytes: number;
    private readonly maxQueuedRequests: number;
    private readonly maxQueuedBytes: number;
    private readonly queue: ReservationWaiter[] = [];
    private usedBytes = 0;
    private peakUsedBytes = 0;
    private activeReservations = 0;
    private queuedBytes = 0;
    private closed = false;

    constructor(options: ResourceBudgetOptions) {
        this.capacityBytes = positiveBytes(options.capacityBytes, "capacityBytes");
        this.maxQueuedRequests = options.maxQueuedRequests ?? DEFAULT_MAX_QUEUED_REQUESTS;
        if (!Number.isSafeInteger(this.maxQueuedRequests) ||
            this.maxQueuedRequests < 1 || this.maxQueuedRequests > MAX_QUEUED_REQUESTS) {
            throw new RangeError(`maxQueuedRequests must be between 1 and ${MAX_QUEUED_REQUESTS}`);
        }
        const defaultQueuedBytes = this.capacityBytes > Number.MAX_SAFE_INTEGER / 4
            ? Number.MAX_SAFE_INTEGER
            : this.capacityBytes * 4;
        this.maxQueuedBytes = positiveBytes(
            options.maxQueuedBytes ?? defaultQueuedBytes, "maxQueuedBytes",
        );
    }

    reserve(bytes: number, options: { signal?: AbortSignal } = {}): Promise<ShrinkableResourceReservation> {
        try { positiveBytes(bytes, "reservation bytes"); }
        catch (error) { return Promise.reject(error); }
        if (options.signal?.aborted) return Promise.reject(abortReason(options.signal));
        if (this.closed) return Promise.reject(new ResourceBudgetClosedError());
        if (bytes > this.capacityBytes) {
            return Promise.reject(new ResourceBudgetOversizedError(bytes, this.capacityBytes));
        }
        if (this.queue.length === 0 && bytes <= this.capacityBytes - this.usedBytes) {
            // Reserve before returning: concurrent calls cannot all observe
            // the same free bytes while their await continuations are queued.
            return Promise.resolve(this.grant(bytes));
        }
        if (this.queue.length >= this.maxQueuedRequests ||
            bytes > this.maxQueuedBytes - this.queuedBytes) {
            return Promise.reject(new ResourceBudgetQueueFullError());
        }
        return new Promise<ShrinkableResourceReservation>((resolve, reject) => {
            const waiter: ReservationWaiter = { bytes, resolve, reject, signal: options.signal };
            if (waiter.signal) {
                waiter.onAbort = () => {
                    const index = this.queue.indexOf(waiter);
                    if (index < 0) return;
                    this.queue.splice(index, 1);
                    this.detach(waiter);
                    waiter.reject(abortReason(waiter.signal!));
                    // Removing a blocked head may make the next FIFO request
                    // fit without waiting for another holder to release.
                    this.drain();
                };
                waiter.signal.addEventListener("abort", waiter.onAbort, { once: true });
            }
            this.queue.push(waiter);
            this.queuedBytes += bytes;
        });
    }

    /** Existing grants remain valid above the new ceiling. Oversized queued
     * work rejects explicitly instead of blocking every later waiter forever.
     * Queue demand limits are intentionally unchanged by a capacity update. */
    setCapacity(capacityBytes: number): void {
        positiveBytes(capacityBytes, "capacityBytes");
        if (this.closed) throw new ResourceBudgetClosedError();
        this.capacityBytes = capacityBytes;
        for (let index = 0; index < this.queue.length;) {
            const waiter = this.queue[index];
            if (waiter.bytes <= capacityBytes) {
                index++;
                continue;
            }
            this.queue.splice(index, 1);
            this.detach(waiter);
            waiter.reject(new ResourceBudgetOversizedError(waiter.bytes, capacityBytes));
        }
        this.drain();
    }

    /** Stop admission and reject waiters, but retain all live-grant accounting. */
    close(): void {
        if (this.closed) return;
        this.closed = true;
        const error = new ResourceBudgetClosedError();
        for (const waiter of this.queue.splice(0)) {
            this.detach(waiter);
            waiter.reject(error);
        }
    }

    snapshot(): ResourceBudgetSnapshot {
        return {
            capacityBytes: this.capacityBytes,
            usedBytes: this.usedBytes,
            peakUsedBytes: this.peakUsedBytes,
            availableBytes: Math.max(0, this.capacityBytes - this.usedBytes),
            activeReservations: this.activeReservations,
            queuedBytes: this.queuedBytes,
            queuedRequests: this.queue.length,
            maxQueuedBytes: this.maxQueuedBytes,
            maxQueuedRequests: this.maxQueuedRequests,
            closed: this.closed,
        };
    }

    private grant(bytes: number): ShrinkableResourceReservation {
        this.usedBytes += bytes;
        this.activeReservations++;
        this.peakUsedBytes = Math.max(this.peakUsedBytes, this.usedBytes);
        let released = false;
        let retainedBytes = bytes;
        return {
            get bytes() { return retainedBytes; },
            shrinkTo: (nextBytes: number) => {
                positiveBytes(nextBytes, "reservation bytes");
                if (released) throw new Error("resource reservation was already released");
                if (nextBytes > retainedBytes) {
                    throw new RangeError("resource reservation cannot grow while shrinking");
                }
                if (nextBytes === retainedBytes) return;
                this.usedBytes -= retainedBytes - nextBytes;
                retainedBytes = nextBytes;
                this.drain();
            },
            release: () => {
                if (released) return;
                released = true;
                this.usedBytes -= retainedBytes;
                retainedBytes = 0;
                this.activeReservations--;
                this.drain();
            },
        };
    }

    /** Remove queued demand and listeners exactly once before settlement. */
    private detach(waiter: ReservationWaiter): void {
        this.queuedBytes -= waiter.bytes;
        if (waiter.signal && waiter.onAbort) {
            waiter.signal.removeEventListener("abort", waiter.onAbort);
        }
    }

    private drain(): void {
        if (this.closed) return;
        while (this.queue.length > 0) {
            const waiter = this.queue[0];
            if (waiter.bytes > this.capacityBytes - this.usedBytes) return;
            this.queue.shift();
            this.detach(waiter);
            waiter.resolve(this.grant(waiter.bytes));
        }
    }
}
