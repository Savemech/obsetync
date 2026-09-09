import type { PerfOperation } from "./perf-trace";

/** A task boundary, not a microtask or an animation-frame dependency. */
export type WorkSchedulerBackend = "node-immediate" | "message-channel" | "timer";
export type WorkLane = "interactive" | "urgent-file" | "bulk" | "maintenance";

interface WorkMessagePort {
    onmessage: ((event: any) => void) | null;
    postMessage(message: unknown): void;
    close(): void;
    ref?(): void;
    unref?(): void;
}

interface WorkMessageChannel {
    port1: WorkMessagePort;
    port2: WorkMessagePort;
}

/** Injectable host capabilities keep scheduling tests independent of wall time. */
export interface WorkSchedulerRuntime {
    immediate?: {
        schedule(callback: () => void): unknown;
        cancel(handle: unknown): void;
    };
    createMessageChannel?: () => WorkMessageChannel;
    scheduleTimer(callback: () => void): unknown;
    cancelTimer(handle: unknown): void;
    /** Monotonic milliseconds. Wall-clock jumps must not reorder queued work. */
    now?: () => number;
}

export interface WorkYieldOptions {
    signal?: AbortSignal;
    perf?: PerfOperation;
    lane?: WorkLane;
    /** Maximum queue wait from admission, in milliseconds. Once expired the
     * oldest deadline wins regardless of lane. This is not a task timeout. */
    deadlineMs?: number;
}

export interface WorkSchedulerSnapshot {
    backend: WorkSchedulerBackend | "uninitialized" | "closed";
    pendingJobs: number;
    maxPendingJobs: number;
    fallbackCount: number;
    pendingByLane: Record<WorkLane, number>;
    oldestWaitMs: number;
    expiredDeadlines: number;
    interactiveReserve: number;
    urgentFileReserve: number;
}

interface TaskDriver {
    backend: WorkSchedulerBackend;
    schedule(callback: () => void): void;
    dispose(): void;
}

interface PendingYield {
    resolve(): void;
    reject(error: unknown): void;
    signal?: AbortSignal;
    onAbort?: () => void;
    endWait?: () => void;
    lane: WorkLane;
    enqueuedAt: number;
    deadlineAt: number | null;
    sequence: number;
}

const DEFAULT_MAX_PENDING = 64;
const MAX_DEADLINE_MS = 60 * 60 * 1_000;
const BULK_AGING_MS = 500;
const MAINTENANCE_AGING_MS = 2_000;
const LANES: readonly WorkLane[] = ["interactive", "urgent-file", "bulk", "maintenance"];
/** Weighted fairness: latency-sensitive work owns most turns, but a busy
 * editor cannot starve bulk or maintenance forever. */
const SERVICE_PATTERN: readonly WorkLane[] = [
    "interactive", "interactive", "interactive", "interactive",
    "urgent-file", "interactive", "urgent-file", "bulk",
    "interactive", "urgent-file", "bulk", "maintenance",
];

function abortError(message: string): Error {
    const error = new Error(message);
    error.name = "AbortError";
    return error;
}

function signalError(signal: AbortSignal): unknown {
    return signal.reason ?? abortError("Scheduled work was aborted");
}

/** Compatible with hosts whose AbortSignal predates throwIfAborted(). */
export function throwIfWorkAborted(signal?: AbortSignal): void {
    if (signal?.aborted) throw signalError(signal);
}

function defaultRuntime(): WorkSchedulerRuntime {
    let immediate: WorkSchedulerRuntime["immediate"];
    try {
        // Obsidian desktop can expose lexical require without globalThis.require.
        // Keep this dynamic: mobile bundles must not import a Node builtin.
        const runtimeRequire = typeof require === "function"
            ? require
            : (globalThis as { require?: (id: string) => any }).require;
        const timers = runtimeRequire?.("node:timers");
        if (typeof timers?.setImmediate === "function" &&
            typeof timers?.clearImmediate === "function") {
            immediate = {
                schedule: (callback) => timers.setImmediate(callback),
                cancel: (handle) => timers.clearImmediate(handle),
            };
        }
    } catch {
        // Sandboxed renderers and mobile have no usable Node timer capability.
    }
    return {
        immediate,
        createMessageChannel: typeof globalThis.MessageChannel === "function"
            ? () => new globalThis.MessageChannel()
            : undefined,
        scheduleTimer: (callback) => globalThis.setTimeout(callback, 0),
        cancelTimer: (handle) => globalThis.clearTimeout(handle as ReturnType<typeof setTimeout>),
        now: () => {
            const value = globalThis.performance?.now?.();
            return typeof value === "number" && Number.isFinite(value) && value >= 0
                ? value
                : Date.now();
        },
    };
}

function callbackDriver(
    backend: "node-immediate" | "timer",
    schedule: (callback: () => void) => unknown,
    cancel: (handle: unknown) => void,
): TaskDriver {
    let handle: unknown;
    let scheduled = false;
    return {
        backend,
        schedule(callback) {
            handle = schedule(() => {
                scheduled = false;
                callback();
            });
            scheduled = true;
        },
        dispose() {
            if (scheduled) cancel(handle);
            scheduled = false;
        },
    };
}

function messageDriver(createChannel: () => WorkMessageChannel): TaskDriver {
    const channel = createChannel();
    let pending: (() => void) | undefined;
    const closePorts = () => {
        try {
            channel.port1.close();
        } finally {
            channel.port2.close();
        }
    };
    try {
        channel.port1.onmessage = () => {
            const callback = pending;
            pending = undefined;
            channel.port1.unref?.();
            callback?.();
        };
        // Idle channels must not keep Node alive, but outstanding work must.
        channel.port1.unref?.();
        channel.port2.unref?.();
    } catch (error) {
        closePorts();
        throw error;
    }
    return {
        backend: "message-channel",
        schedule(callback) {
            pending = callback;
            channel.port1.ref?.();
            channel.port2.postMessage(0);
        },
        dispose() {
            pending = undefined;
            channel.port1.onmessage = null;
            closePorts();
        },
    };
}

/**
 * Bounded lane-aware cooperation with the host event loop. Only one native
 * callback is outstanding, and each callback releases at most one caller.
 * Deadlines and aging cap latency; a fixed weighted service pattern reserves
 * most turns for interactive/urgent work without starving bulk/maintenance.
 *
 * The selected capability is tried at runtime, with fallback on setup/send
 * failure. Its presence is NOT a guarantee of hidden-window throughput, nor
 * does any backend allow execution while the OS has suspended the app.
 * Mobile visibility gating remains the sync engine's responsibility.
 */
export class WorkScheduler {
    private readonly queues: Record<WorkLane, PendingYield[]> = {
        interactive: [],
        "urgent-file": [],
        bulk: [],
        maintenance: [],
    };
    private readonly factories: Array<() => TaskDriver>;
    private readonly maxPending: number;
    private readonly now: () => number;
    private readonly interactiveReserve: number;
    private readonly urgentFileReserve: number;
    private nextFactory = 0;
    private driver?: TaskDriver;
    private scheduled = false;
    private closed = false;
    private fallbackCount = 0;
    private pendingCount = 0;
    private sequence = 0;
    private serviceCursor = 0;
    private lastNow = 0;
    private expiredDeadlines = 0;
    private agingOverrideAvailable = true;

    constructor(options: { runtime?: WorkSchedulerRuntime; maxPendingJobs?: number } = {}) {
        this.maxPending = options.maxPendingJobs ?? DEFAULT_MAX_PENDING;
        if (!Number.isSafeInteger(this.maxPending) || this.maxPending < 1 || this.maxPending > 1024) {
            throw new RangeError("Scheduler queue limit must be an integer between 1 and 1024");
        }
        this.interactiveReserve = Math.floor(this.maxPending / 8);
        this.urgentFileReserve = Math.floor(this.maxPending / 8);
        const runtime = options.runtime ?? defaultRuntime();
        this.now = runtime.now ?? (() => {
            const value = globalThis.performance?.now?.();
            return typeof value === "number" && Number.isFinite(value) && value >= 0
                ? value
                : Date.now();
        });
        this.factories = [];
        if (runtime.immediate) {
            const immediate = runtime.immediate;
            this.factories.push(() => callbackDriver(
                "node-immediate",
                (callback) => immediate.schedule(callback),
                (handle) => immediate.cancel(handle),
            ));
        }
        if (runtime.createMessageChannel) {
            const createChannel = runtime.createMessageChannel;
            this.factories.push(() => messageDriver(createChannel));
        }
        this.factories.push(() => callbackDriver(
            "timer",
            (callback) => runtime.scheduleTimer(callback),
            (handle) => runtime.cancelTimer(handle),
        ));
    }

    yield(options: WorkYieldOptions = {}): Promise<void> {
        if (options.signal?.aborted) return Promise.reject(signalError(options.signal));
        if (this.closed) return Promise.reject(abortError("Work scheduler is closed"));
        const lane = options.lane ?? "bulk";
        if (!LANES.includes(lane)) return Promise.reject(new RangeError("Unknown work scheduler lane"));
        if (!this.hasAdmission(lane)) {
            return Promise.reject(new Error("Work scheduler queue is full or reserved for urgent work"));
        }
        const deadlineMs = options.deadlineMs;
        if (deadlineMs !== undefined &&
            (!Number.isSafeInteger(deadlineMs) || deadlineMs < 0 || deadlineMs > MAX_DEADLINE_MS)) {
            return Promise.reject(new RangeError("Scheduler deadline must be a bounded non-negative integer"));
        }
        const enqueuedAt = this.readNow();
        return new Promise<void>((resolve, reject) => {
            const entry: PendingYield = {
                resolve,
                reject,
                signal: options.signal,
                endWait: options.perf?.phase("scheduler_wait"),
                lane,
                enqueuedAt,
                deadlineAt: deadlineMs === undefined
                    ? null
                    : Math.min(Number.MAX_SAFE_INTEGER, enqueuedAt + deadlineMs),
                sequence: this.nextSequence(),
            };
            if (entry.signal) {
                entry.onAbort = () => {
                    const queue = this.queues[entry.lane];
                    const index = queue.indexOf(entry);
                    if (index < 0) return;
                    queue.splice(index, 1);
                    this.pendingCount--;
                    this.finish(entry, signalError(entry.signal!));
                    // Keep the single outstanding task. Cancelling/reposting
                    // MessageChannel messages for each abort would be unbounded.
                };
                entry.signal.addEventListener("abort", entry.onAbort, { once: true });
            }
            this.queues[lane].push(entry);
            this.pendingCount++;
            this.scheduleNext();
        });
    }

    snapshot(): WorkSchedulerSnapshot {
        return {
            backend: this.closed ? "closed" : this.driver?.backend ?? "uninitialized",
            pendingJobs: this.pendingCount,
            maxPendingJobs: this.maxPending,
            fallbackCount: this.fallbackCount,
            pendingByLane: {
                interactive: this.queues.interactive.length,
                "urgent-file": this.queues["urgent-file"].length,
                bulk: this.queues.bulk.length,
                maintenance: this.queues.maintenance.length,
            },
            oldestWaitMs: this.oldestWait(this.readNow()),
            expiredDeadlines: this.expiredDeadlines,
            interactiveReserve: this.interactiveReserve,
            urgentFileReserve: this.urgentFileReserve,
        };
    }

    dispose(): void {
        if (this.closed) return;
        this.closed = true;
        this.scheduled = false;
        this.closeDriver();
        const error = abortError("Work scheduler is closed");
        for (const lane of LANES) {
            for (const entry of this.queues[lane].splice(0)) this.finish(entry, error);
        }
        this.pendingCount = 0;
    }

    private closeDriver(): void {
        try {
            this.driver?.dispose();
        } catch {
            // Teardown failure must not retain caller promises or abort listeners.
        }
        this.driver = undefined;
    }

    private finish(entry: PendingYield, error?: unknown): void {
        if (entry.onAbort) entry.signal?.removeEventListener("abort", entry.onAbort);
        entry.endWait?.();
        if (error !== undefined) entry.reject(error);
        else entry.resolve();
    }

    private readNow(): number {
        let observed: number;
        try { observed = this.now(); } catch { return this.lastNow; }
        if (!Number.isFinite(observed) || observed > Number.MAX_SAFE_INTEGER ||
            observed < this.lastNow) return this.lastNow;
        this.lastNow = observed;
        return observed;
    }

    private nextSequence(): number {
        if (this.sequence >= Number.MAX_SAFE_INTEGER) {
            // At most maxPending entries exist. Rebase their relative order
            // before the next enqueue instead of allowing precision loss.
            const ordered = LANES.flatMap(lane => this.queues[lane])
                .sort((a, b) => a.sequence - b.sequence);
            ordered.forEach((entry, index) => { entry.sequence = index; });
            this.sequence = ordered.length;
        }
        return this.sequence++;
    }

    private oldestWait(now: number): number {
        let oldest = now;
        let found = false;
        for (const lane of LANES) {
            const entry = this.queues[lane][0];
            if (!entry) continue;
            found = true;
            oldest = Math.min(oldest, entry.enqueuedAt);
        }
        return found ? Math.max(0, now - oldest) : 0;
    }

    private hasAdmission(lane: WorkLane): boolean {
        if (this.pendingCount >= this.maxPending) return false;
        if (lane === "interactive") return true;
        const missingInteractive = Math.max(
            0,
            this.interactiveReserve - this.queues.interactive.length,
        );
        if (this.pendingCount >= this.maxPending - missingInteractive) return false;
        if (lane === "urgent-file") return true;
        const missingUrgent = Math.max(
            0,
            this.urgentFileReserve - this.queues["urgent-file"].length,
        );
        return this.pendingCount < this.maxPending - missingInteractive - missingUrgent;
    }

    private takeNext(): PendingYield | undefined {
        if (this.pendingCount === 0) return undefined;
        const now = this.readNow();
        let deadline: PendingYield | undefined;
        for (const lane of LANES) {
            const entry = this.queues[lane][0];
            if (!entry || entry.deadlineAt === null || entry.deadlineAt > now) continue;
            if (!deadline || entry.deadlineAt < deadline.deadlineAt! ||
                (entry.deadlineAt === deadline.deadlineAt && entry.sequence < deadline.sequence)) {
                deadline = entry;
            }
        }
        if (deadline) {
            this.expiredDeadlines = Math.min(Number.MAX_SAFE_INTEGER, this.expiredDeadlines + 1);
            return this.shift(deadline.lane);
        }

        // Age only the heads: FIFO within a lane is an invariant. Compare the
        // time each background lane became eligible, not raw wait duration.
        const bulk = this.queues.bulk[0];
        const maintenance = this.queues.maintenance[0];
        const bulkDue = bulk ? bulk.enqueuedAt + BULK_AGING_MS : Number.POSITIVE_INFINITY;
        const maintenanceDue = maintenance
            ? maintenance.enqueuedAt + MAINTENANCE_AGING_MS
            : Number.POSITIVE_INFINITY;
        if (this.agingOverrideAvailable && (bulkDue <= now || maintenanceDue <= now)) {
            this.agingOverrideAvailable = false;
            return this.shift(maintenanceDue < bulkDue ? "maintenance" : "bulk");
        }

        for (let offset = 0; offset < SERVICE_PATTERN.length; offset++) {
            const index = (this.serviceCursor + offset) % SERVICE_PATTERN.length;
            const lane = SERVICE_PATTERN[index];
            if (this.queues[lane].length === 0) continue;
            this.serviceCursor = (index + 1) % SERVICE_PATTERN.length;
            this.agingOverrideAvailable = true;
            return this.shift(lane);
        }
        return undefined;
    }

    private shift(lane: WorkLane): PendingYield | undefined {
        const entry = this.queues[lane].shift();
        if (entry) this.pendingCount--;
        return entry;
    }

    private scheduleNext(): void {
        if (this.scheduled || this.closed || this.pendingCount === 0) return;
        let failure: unknown;
        while (this.driver || this.nextFactory < this.factories.length) {
            try {
                this.driver ??= this.factories[this.nextFactory++]();
                this.scheduled = true;
                this.driver.schedule(() => {
                    this.scheduled = false;
                    if (this.closed) return;
                    const entry = this.takeNext();
                    if (entry) this.finish(entry);
                    this.scheduleNext();
                });
                return;
            } catch (error) {
                failure = error;
                this.scheduled = false;
                this.fallbackCount++;
                this.closeDriver();
            }
        }
        const error = failure ?? new Error("No task scheduling capability is available");
        for (const lane of LANES) {
            for (const entry of this.queues[lane].splice(0)) this.finish(entry, error);
        }
        this.pendingCount = 0;
    }
}

let sharedScheduler: WorkScheduler | undefined;

/** Shared by scan, push and chunking; disposed by the plugin unload hook. */
export function yieldWork(options: WorkYieldOptions = {}): Promise<void> {
    // Do not recreate resources after unload for already-cancelled operations.
    if (options.signal?.aborted) return Promise.reject(signalError(options.signal));
    sharedScheduler ??= new WorkScheduler();
    return sharedScheduler.yield(options);
}

export function workSchedulerSnapshot(): WorkSchedulerSnapshot {
    return sharedScheduler?.snapshot() ?? {
        backend: "uninitialized",
        pendingJobs: 0,
        maxPendingJobs: DEFAULT_MAX_PENDING,
        fallbackCount: 0,
        pendingByLane: { interactive: 0, "urgent-file": 0, bulk: 0, maintenance: 0 },
        oldestWaitMs: 0,
        expiredDeadlines: 0,
        interactiveReserve: Math.floor(DEFAULT_MAX_PENDING / 8),
        urgentFileReserve: Math.floor(DEFAULT_MAX_PENDING / 8),
    };
}

export function disposeWorkScheduler(): void {
    sharedScheduler?.dispose();
    sharedScheduler = undefined;
}
