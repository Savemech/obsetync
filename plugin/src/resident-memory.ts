/**
 * Synchronous accounting for caller-supplied long-lived native-memory estimates.
 *
 * This ledger does not inspect allocations and is not a WASM heap, RSS, GC, or
 * device-memory measurement. "Exact" below means only that its safe-integer
 * arithmetic exactly follows the byte charges supplied by its caller. Admission
 * is deliberately fail-fast: a resident owner must never sit in a FIFO queue
 * waiting for bytes that can be released only after that same owner is replaced.
 */

export interface NativeResidentLedgerOptions {
    capacityBytes: number;
}

export interface NativeResidentLedgerSnapshot {
    capacityBytes: number;
    usedBytes: number;
    peakUsedBytes: number;
    availableBytes: number;
    overcommittedBytes: number;
    activeLeases: number;
    closed: boolean;
    refusedReservations: number;
    refusedGrowths: number;
}

const leaseBrand: unique symbol = Symbol("NativeResidentLease");

/**
 * An owner-scoped accounting capability. Tokens are created only by their exact
 * ledger. `release()` is idempotent, but resizing, splitting, or transferring a
 * released token is an ownership error. No GC/finalizer releases a charge.
 */
export interface NativeResidentLease {
    readonly [leaseBrand]: true;
    readonly bytes: number;
    release(): void;
}

export class NativeResidentLeaseOwnershipError extends TypeError {
    constructor() {
        super("native resident lease does not belong to this ledger");
        this.name = "NativeResidentLeaseOwnershipError";
    }
}

export class NativeResidentLeaseReleasedError extends Error {
    constructor() {
        super("native resident lease is no longer active");
        this.name = "NativeResidentLeaseReleasedError";
    }
}

interface LeaseState {
    bytes: number;
    active: boolean;
}

const MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER;

function positiveSafeInteger(value: number, name: string): number {
    if (!Number.isSafeInteger(value) || value <= 0) {
        throw new RangeError(`${name} must be a positive safe-integer byte count`);
    }
    return value;
}

function incrementCounter(value: number): number {
    // Refusal telemetry must never make otherwise-safe accounting inexact.
    return value === MAX_SAFE_INTEGER ? value : value + 1;
}

/**
 * Independent, non-queuing admission for long-lived native-memory estimates.
 * All public operations are synchronous and invoke no caller callback.
 */
export class NativeResidentLedger {
    private capacityBytes: number;
    private usedBytes = 0;
    private peakUsedBytes = 0;
    private activeLeases = 0;
    private refusedReservations = 0;
    private refusedGrowths = 0;
    private closed = false;
    private readonly leases = new WeakMap<object, LeaseState>();

    constructor(options: NativeResidentLedgerOptions) {
        this.capacityBytes = positiveSafeInteger(options.capacityBytes, "capacityBytes");
    }

    /**
     * Reserve before creating the charged owner. `null` means immediate policy
     * refusal (closed or insufficient capacity); this method never queues.
     */
    tryReserve(bytes: number): NativeResidentLease | null {
        positiveSafeInteger(bytes, "reservation bytes");
        if (this.closed || bytes > this.available()) {
            this.refusedReservations = incrementCounter(this.refusedReservations);
            return null;
        }
        const lease = this.createLease(bytes);
        this.usedBytes += bytes;
        this.activeLeases++;
        this.peakUsedBytes = Math.max(this.peakUsedBytes, this.usedBytes);
        return lease;
    }

    /** Add an exact positive charge. Failure leaves both lease and ledger intact. */
    tryGrow(lease: NativeResidentLease, additionalBytes: number): boolean {
        const additional = positiveSafeInteger(additionalBytes, "additionalBytes");
        const state = this.activeState(lease);
        if (additional > MAX_SAFE_INTEGER - state.bytes) {
            throw new RangeError("native resident lease byte count exceeds the safe-integer range");
        }
        return this.tryResizeState(state, state.bytes + additional);
    }

    /**
     * Change the exact charge. Growth is fail-fast and denied after close;
     * shrinking is always permitted and must be called only after the owner has
     * actually released those native bytes.
     */
    tryResize(lease: NativeResidentLease, bytes: number): boolean {
        const target = positiveSafeInteger(bytes, "lease bytes");
        return this.tryResizeState(this.activeState(lease), target);
    }

    /**
     * Split `bytes` into a second owner on this same ledger. Both resulting
     * leases remain positive; total charge and peak do not change.
     */
    split(lease: NativeResidentLease, bytes: number): NativeResidentLease {
        const amount = positiveSafeInteger(bytes, "split bytes");
        const state = this.activeState(lease);
        if (amount >= state.bytes) {
            throw new RangeError("split bytes must be smaller than the source lease");
        }
        if (this.activeLeases === MAX_SAFE_INTEGER) {
            throw new RangeError("native resident active lease count exceeds the safe-integer range");
        }
        // Allocate the new capability before changing any accounting. No user
        // callback or coercion can re-enter between the following mutations.
        const result = this.createLease(amount);
        state.bytes -= amount;
        this.activeLeases++;
        return result;
    }

    /**
     * Move the whole charge to a new owner capability. The source becomes inert;
     * total bytes, peak, and active lease count are unchanged.
     */
    transfer(lease: NativeResidentLease): NativeResidentLease {
        const state = this.activeState(lease);
        const result = this.createLease(state.bytes);
        state.bytes = 0;
        state.active = false;
        return result;
    }

    /** Merge two disjoint charges into `target` without changing total bytes
     * or allocating another capability. `source` is consumed. This is the O(1)
     * ownership primitive used when one native store absorbs another admitted
     * output cohort; it is not proof that any native bytes were freed. */
    absorb(target: NativeResidentLease, source: NativeResidentLease): void {
        if (target === source) throw new NativeResidentLeaseOwnershipError();
        const targetState = this.activeState(target);
        const sourceState = this.activeState(source);
        const combined = targetState.bytes + sourceState.bytes;
        if (!Number.isSafeInteger(combined) || combined > this.usedBytes) {
            throw new RangeError("combined native resident lease exceeds the safe-integer range");
        }
        targetState.bytes = combined;
        sourceState.bytes = 0;
        sourceState.active = false;
        this.activeLeases--;
    }

    /** Idempotent for a known lease; foreign/forged capabilities are rejected. */
    release(lease: NativeResidentLease): void {
        const state = this.ownedState(lease);
        if (!state.active) return;
        this.usedBytes -= state.bytes;
        this.activeLeases--;
        state.bytes = 0;
        state.active = false;
    }

    /** Existing live charges survive a shrink, even when now overcommitted. */
    setCapacity(capacityBytes: number): void {
        this.capacityBytes = positiveSafeInteger(capacityBytes, "capacityBytes");
    }

    /** Reject future reservations/growth, retaining every live charge. */
    close(): void {
        this.closed = true;
    }

    snapshot(): NativeResidentLedgerSnapshot {
        return {
            capacityBytes: this.capacityBytes,
            usedBytes: this.usedBytes,
            peakUsedBytes: this.peakUsedBytes,
            availableBytes: this.available(),
            overcommittedBytes: Math.max(0, this.usedBytes - this.capacityBytes),
            activeLeases: this.activeLeases,
            closed: this.closed,
            refusedReservations: this.refusedReservations,
            refusedGrowths: this.refusedGrowths,
        };
    }

    private available(): number {
        return Math.max(0, this.capacityBytes - this.usedBytes);
    }

    private tryResizeState(state: LeaseState, target: number): boolean {
        if (target <= state.bytes) {
            this.usedBytes -= state.bytes - target;
            state.bytes = target;
            return true;
        }
        const additional = target - state.bytes;
        if (this.closed || additional > this.available()) {
            this.refusedGrowths = incrementCounter(this.refusedGrowths);
            return false;
        }
        state.bytes = target;
        this.usedBytes += additional;
        this.peakUsedBytes = Math.max(this.peakUsedBytes, this.usedBytes);
        return true;
    }

    private ownedState(lease: NativeResidentLease): LeaseState {
        if ((typeof lease !== "object" && typeof lease !== "function") || lease === null) {
            throw new NativeResidentLeaseOwnershipError();
        }
        const state = this.leases.get(lease);
        if (!state) throw new NativeResidentLeaseOwnershipError();
        return state;
    }

    private activeState(lease: NativeResidentLease): LeaseState {
        const state = this.ownedState(lease);
        if (!state.active) throw new NativeResidentLeaseReleasedError();
        return state;
    }

    private createLease(bytes: number): NativeResidentLease {
        const state: LeaseState = { bytes, active: true };
        let lease!: NativeResidentLease;
        lease = Object.freeze(Object.defineProperties({}, {
            [leaseBrand]: { value: true },
            bytes: { enumerable: true, get: () => state.active ? state.bytes : 0 },
            release: { enumerable: true, value: () => this.release(lease) },
        })) as NativeResidentLease;
        this.leases.set(lease, state);
        return lease;
    }
}
