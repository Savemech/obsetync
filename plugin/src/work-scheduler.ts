import type { PerfOperation } from "./perf-trace";

/** A task boundary, not a microtask or an animation-frame dependency. */
export type WorkSchedulerBackend = "node-immediate" | "message-channel" | "timer";

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
}

export interface WorkYieldOptions {
    signal?: AbortSignal;
    perf?: PerfOperation;
}

export interface WorkSchedulerSnapshot {
    backend: WorkSchedulerBackend | "uninitialized" | "closed";
    pendingJobs: number;
    maxPendingJobs: number;
    fallbackCount: number;
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
}

const DEFAULT_MAX_PENDING = 64;

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
 * Bounded FIFO cooperation with the host event loop. Only one native callback
 * is outstanding, and each callback releases at most one caller. Even a long
 * chain of awaits therefore passes through real task boundaries.
 *
 * The selected capability is tried at runtime, with fallback on setup/send
 * failure. Its presence is NOT a guarantee of hidden-window throughput, nor
 * does any backend allow execution while the OS has suspended the app.
 * Mobile visibility gating remains the sync engine's responsibility.
 */
export class WorkScheduler {
    private readonly queue: PendingYield[] = [];
    private readonly factories: Array<() => TaskDriver>;
    private readonly maxPending: number;
    private nextFactory = 0;
    private driver?: TaskDriver;
    private scheduled = false;
    private closed = false;
    private fallbackCount = 0;

    constructor(options: { runtime?: WorkSchedulerRuntime; maxPendingJobs?: number } = {}) {
        this.maxPending = options.maxPendingJobs ?? DEFAULT_MAX_PENDING;
        if (!Number.isSafeInteger(this.maxPending) || this.maxPending < 1 || this.maxPending > 1024) {
            throw new RangeError("Scheduler queue limit must be an integer between 1 and 1024");
        }
        const runtime = options.runtime ?? defaultRuntime();
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
        if (this.queue.length >= this.maxPending) {
            return Promise.reject(new Error("Work scheduler queue is full"));
        }
        return new Promise<void>((resolve, reject) => {
            const entry: PendingYield = {
                resolve,
                reject,
                signal: options.signal,
                endWait: options.perf?.phase("scheduler_wait"),
            };
            if (entry.signal) {
                entry.onAbort = () => {
                    const index = this.queue.indexOf(entry);
                    if (index < 0) return;
                    this.queue.splice(index, 1);
                    this.finish(entry, signalError(entry.signal!));
                    // Keep the single outstanding task. Cancelling/reposting
                    // MessageChannel messages for each abort would be unbounded.
                };
                entry.signal.addEventListener("abort", entry.onAbort, { once: true });
            }
            this.queue.push(entry);
            this.scheduleNext();
        });
    }

    snapshot(): WorkSchedulerSnapshot {
        return {
            backend: this.closed ? "closed" : this.driver?.backend ?? "uninitialized",
            pendingJobs: this.queue.length,
            maxPendingJobs: this.maxPending,
            fallbackCount: this.fallbackCount,
        };
    }

    dispose(): void {
        if (this.closed) return;
        this.closed = true;
        this.scheduled = false;
        this.closeDriver();
        const error = abortError("Work scheduler is closed");
        for (const entry of this.queue.splice(0)) this.finish(entry, error);
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

    private scheduleNext(): void {
        if (this.scheduled || this.closed || this.queue.length === 0) return;
        let failure: unknown;
        while (this.driver || this.nextFactory < this.factories.length) {
            try {
                this.driver ??= this.factories[this.nextFactory++]();
                this.scheduled = true;
                this.driver.schedule(() => {
                    this.scheduled = false;
                    if (this.closed) return;
                    const entry = this.queue.shift();
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
        for (const entry of this.queue.splice(0)) this.finish(entry, error);
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
    };
}

export function disposeWorkScheduler(): void {
    sharedScheduler?.dispose();
    sharedScheduler = undefined;
}
