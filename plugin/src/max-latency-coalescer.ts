export interface CoalescerTimers {
    now(): number;
    setTimeout(callback: () => void, delayMs: number): unknown;
    clearTimeout(handle: unknown): void;
}

export interface MaxLatencyCoalescerOptions {
    quietMs: number;
    maxLatencyMs: number;
    run: () => void | Promise<void>;
    onError?: (error: unknown) => void;
    timers?: CoalescerTimers;
}

export interface MaxLatencyCoalescerSnapshot {
    pending: boolean;
    running: boolean;
    closed: boolean;
    pendingForMs: number;
}

const DEFAULT_TIMERS: CoalescerTimers = {
    now: () => globalThis.performance?.now?.() ?? Date.now(),
    setTimeout: (callback, delayMs) => globalThis.setTimeout(callback, delayMs),
    clearTimeout: handle => globalThis.clearTimeout(handle as ReturnType<typeof globalThis.setTimeout>),
};

function positive(value: number, label: string): number {
    if (!Number.isSafeInteger(value) || value < 1) {
        throw new RangeError(`${label} must be a positive safe integer`);
    }
    return value;
}

/**
 * Coalesces a burst after a short quiet period, but repeated events cannot
 * postpone the callback past maxLatencyMs. Exactly one callback is active;
 * events arriving during it become one bounded follow-up burst.
 */
export class MaxLatencyCoalescer {
    private readonly quietMs: number;
    private readonly maxLatencyMs: number;
    private readonly runCallback: () => void | Promise<void>;
    private readonly onError: (error: unknown) => void;
    private readonly timers: CoalescerTimers;
    private timer: unknown | null = null;
    private pending = false;
    private running = false;
    private closed = false;
    private burstStartedAt: number | null = null;

    constructor(options: MaxLatencyCoalescerOptions) {
        this.quietMs = positive(options.quietMs, "coalescer quiet delay");
        this.maxLatencyMs = positive(options.maxLatencyMs, "coalescer maximum latency");
        if (this.maxLatencyMs < this.quietMs) {
            throw new RangeError("coalescer maximum latency is below its quiet delay");
        }
        this.runCallback = options.run;
        this.onError = options.onError ?? (() => {});
        this.timers = options.timers ?? DEFAULT_TIMERS;
    }

    trigger(): void {
        if (this.closed) return;
        const now = this.timers.now();
        this.pending = true;
        this.burstStartedAt ??= now;
        if (!this.running) this.schedule(now);
    }

    close(): void {
        if (this.closed) return;
        this.closed = true;
        this.pending = false;
        this.burstStartedAt = null;
        this.clearTimer();
    }

    snapshot(): MaxLatencyCoalescerSnapshot {
        return {
            pending: this.pending,
            running: this.running,
            closed: this.closed,
            pendingForMs: this.burstStartedAt === null
                ? 0
                : Math.max(0, this.timers.now() - this.burstStartedAt),
        };
    }

    private schedule(now: number): void {
        if (this.closed || this.running || this.burstStartedAt === null) return;
        this.clearTimer();
        const remaining = Math.max(0, this.burstStartedAt + this.maxLatencyMs - now);
        const delay = Math.min(this.quietMs, remaining);
        this.timer = this.timers.setTimeout(() => this.fire(), delay);
    }

    private fire(): void {
        this.timer = null;
        if (this.closed || this.running || !this.pending) return;
        this.pending = false;
        this.burstStartedAt = null;
        this.running = true;
        let result: void | Promise<void>;
        try { result = this.runCallback(); }
        catch (error) { result = Promise.reject(error); }
        void Promise.resolve(result).catch(error => this.onError(error)).finally(() => {
            this.running = false;
            if (this.pending && !this.closed) this.schedule(this.timers.now());
        });
    }

    private clearTimer(): void {
        if (this.timer === null) return;
        this.timers.clearTimeout(this.timer);
        this.timer = null;
    }
}
