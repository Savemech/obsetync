const DEFAULT_MAX_ACTIVE = 64;
const MAX_ACTIVE_LIMIT = 256;

export type CurrentHostWorkErrorCode =
    | "CURRENT_HOST_WORK_CLOSED"
    | "CURRENT_HOST_WORK_SATURATED"
    | "CURRENT_HOST_WORK_STALE";

export class CurrentHostWorkError extends Error {
    readonly name = "CurrentHostWorkError";

    constructor(readonly code: CurrentHostWorkErrorCode) {
        super(code === "CURRENT_HOST_WORK_CLOSED"
            ? "Current host work admission is closed"
            : code === "CURRENT_HOST_WORK_SATURATED"
                ? "Current host work admission is saturated"
                : "Current host work lease is stale");
    }
}

/** A path-free retirement failure. Individual task promises retain their
 * original causes; the shared retirement diagnostic deliberately does not. */
export class CurrentHostWorkAggregateError extends Error {
    readonly name = "CurrentHostWorkAggregateError";
    readonly code = "CURRENT_HOST_WORK_FAILED";

    constructor(readonly totalFailures: number) {
        super("Current host work failed during retirement");
    }
}

export interface CurrentHostWorkLease {
    readonly generation: number;
    readonly signal: AbortSignal;
    isCurrent(): boolean;
    assertCurrent(): void;
}

export interface CurrentHostWorkSnapshot {
    closed: boolean;
    generation: number;
    active: number;
    drained: boolean;
    failureCount: number;
}

export interface CurrentHostWorkOptions {
    maxActive?: number;
}

function boundedInteger(value: number | undefined, fallback: number, maximum: number): number {
    const selected = value ?? fallback;
    if (!Number.isSafeInteger(selected) || selected < 1 || selected > maximum) {
        throw new RangeError("Current host work bound is invalid");
    }
    return selected;
}

/** Owns asynchronous operations for exactly one current-host generation.
 *
 * `run` admits synchronously before invoking user code. `closeAndDrain`
 * synchronously closes admission and revokes every lease, then waits for the
 * actual returned tasks (including their awaited cleanup) to settle. A closed
 * generation never reopens; replacement hosts must construct a new owner. */
export class CurrentHostWorkOwner {
    private readonly maxActive: number;
    private readonly abortController = new AbortController();
    private generation = 1;
    private closed = false;
    private active = 0;
    private failureCount = 0;
    private drainPromise: Promise<void> | null = null;
    private resolveDrain: (() => void) | null = null;
    private rejectDrain: ((reason: unknown) => void) | null = null;

    constructor(options: CurrentHostWorkOptions = {}) {
        this.maxActive = boundedInteger(options.maxActive, DEFAULT_MAX_ACTIVE, MAX_ACTIVE_LIMIT);
    }

    /** Closed/saturated admission throws before operation code can run. */
    run<T>(operation: (lease: CurrentHostWorkLease) => T | PromiseLike<T>): Promise<T> {
        if (this.closed) throw new CurrentHostWorkError("CURRENT_HOST_WORK_CLOSED");
        if (this.active >= this.maxActive) throw new CurrentHostWorkError("CURRENT_HOST_WORK_SATURATED");

        const admittedGeneration = this.generation;
        const lease: CurrentHostWorkLease = Object.freeze({
            generation: admittedGeneration,
            signal: this.abortController.signal,
            isCurrent: () => !this.closed && this.generation === admittedGeneration,
            assertCurrent: () => {
                if (this.closed || this.generation !== admittedGeneration) {
                    throw new CurrentHostWorkError("CURRENT_HOST_WORK_STALE");
                }
            },
        });
        this.active++;

        let started: Promise<T>;
        try { started = Promise.resolve(operation(lease)); }
        catch (error) { started = Promise.reject(error); }

        const owned = started.then(value => {
            this.settle();
            return value;
        }, error => {
            this.recordFailure(error);
            this.settle();
            throw error;
        });
        // The owner observes every rejection even when its caller stops
        // observing. The returned promise still carries the original outcome.
        void owned.catch(() => {});
        return owned;
    }

    /** Deliberately not async: every call shares one retirement tail. */
    closeAndDrain(): Promise<void> {
        if (this.drainPromise) return this.drainPromise;
        this.closed = true;
        this.generation++;
        this.drainPromise = new Promise<void>((resolve, reject) => {
            this.resolveDrain = resolve;
            this.rejectDrain = reject;
        });
        this.abortController.abort();
        this.completeIfDrained();
        return this.drainPromise;
    }

    snapshot(): CurrentHostWorkSnapshot {
        return {
            closed: this.closed,
            generation: this.generation,
            active: this.active,
            drained: this.closed && this.active === 0,
            failureCount: this.failureCount,
        };
    }

    private settle(): void {
        this.active--;
        this.completeIfDrained();
    }

    private recordFailure(_error: unknown): void {
        // Admission closes synchronously, so an owner settling after this cut
        // is necessarily one of the bounded active owners captured by it.
        // Earlier settled failures belong only to their individual promises.
        if (this.closed) this.failureCount++;
    }

    private completeIfDrained(): void {
        if (!this.closed || this.active !== 0 || !this.resolveDrain || !this.rejectDrain) return;
        const resolve = this.resolveDrain;
        const reject = this.rejectDrain;
        this.resolveDrain = null;
        this.rejectDrain = null;
        if (this.failureCount > 0) reject(new CurrentHostWorkAggregateError(this.failureCount));
        else resolve();
    }
}
