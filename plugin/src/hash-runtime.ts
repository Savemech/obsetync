export type HashRuntime = "desktop" | "mobile" | "unknown";

export interface HashTuning {
    runtime: HashRuntime;
    feedBytes: number;
    minFeedBytes: number;
    maxFeedBytes: number;
    maxBatchFiles: number;
    maxBatchBytes: number;
    maxSingleBatchFileBytes: number;
    maxBatchHoldMs: number;
    readConcurrency: number;
}

export interface HashFeedback {
    bytes: number;
    durationMs: number;
    eventLoopLagMs?: number;
    memoryPressure?: boolean;
}

export interface BatchLimits {
    maxFiles: number;
    maxBytes: number;
    maxSingleBytes: number;
    maxHoldMs?: number;
}

const KIB = 1024;
const MIB = 1024 * KIB;
const MIN_FEED = 64 * KIB;
const HEALTHY_SAMPLES_TO_GROW = 4;
const LAG_DECREASE_THRESHOLD_MS = 32;

export function hashTuningForRuntime(runtime: HashRuntime): HashTuning {
    if (runtime === "desktop") {
        return {
            runtime,
            feedBytes: 512 * KIB,
            minFeedBytes: MIN_FEED,
            maxFeedBytes: MIB,
            maxBatchFiles: 256,
            maxBatchBytes: 8 * MIB,
            maxSingleBatchFileBytes: MIB,
            maxBatchHoldMs: 8,
            readConcurrency: 4,
        };
    }
    if (runtime === "mobile") {
        return {
            runtime,
            feedBytes: 256 * KIB,
            minFeedBytes: MIN_FEED,
            maxFeedBytes: 256 * KIB,
            maxBatchFiles: 64,
            maxBatchBytes: 2 * MIB,
            maxSingleBatchFileBytes: MIB,
            maxBatchHoldMs: 4,
            readConcurrency: 2,
        };
    }
    return {
        runtime,
        feedBytes: 128 * KIB,
        minFeedBytes: MIN_FEED,
        maxFeedBytes: 512 * KIB,
        maxBatchFiles: 64,
        maxBatchBytes: 2 * MIB,
        maxSingleBatchFileBytes: MIB,
        maxBatchHoldMs: 4,
        readConcurrency: 2,
    };
}

export class AdaptiveHashTuner {
    private tuning: HashTuning;
    private healthySamples = 0;

    constructor(
        runtime: HashRuntime,
        private readonly onChange?: (tuning: HashTuning) => void,
    ) {
        this.tuning = hashTuningForRuntime(runtime);
    }

    current(): HashTuning {
        return { ...this.tuning };
    }

    observe(feedback: HashFeedback): void {
        if (
            !Number.isFinite(feedback.bytes) || feedback.bytes < 0 ||
            !Number.isFinite(feedback.durationMs) || feedback.durationMs < 0
        ) {
            return;
        }
        const overloaded = feedback.memoryPressure === true ||
            (feedback.eventLoopLagMs !== undefined &&
                feedback.eventLoopLagMs >= LAG_DECREASE_THRESHOLD_MS);
        if (overloaded) {
            this.healthySamples = 0;
            this.setFeed(Math.max(this.tuning.minFeedBytes, this.tuning.feedBytes / 2));
            return;
        }

        const healthy = feedback.bytes >= this.tuning.feedBytes &&
            feedback.durationMs > 0 &&
            (feedback.eventLoopLagMs === undefined || feedback.eventLoopLagMs <= 8);
        if (!healthy) {
            this.healthySamples = 0;
            return;
        }
        this.healthySamples++;
        if (this.healthySamples >= HEALTHY_SAMPLES_TO_GROW) {
            this.healthySamples = 0;
            this.setFeed(Math.min(this.tuning.maxFeedBytes, this.tuning.feedBytes * 2));
        }
    }

    private setFeed(bytes: number): void {
        const feedBytes = Math.trunc(bytes);
        if (feedBytes === this.tuning.feedBytes) return;
        this.tuning = { ...this.tuning, feedBytes };
        this.onChange?.(this.current());
    }
}

export function planByteBoundedBatches<T>(
    items: readonly T[],
    sizeOf: (item: T) => number,
    limits: BatchLimits,
    now: () => number = () => globalThis.performance?.now?.() ?? Date.now(),
): T[][] {
    if (
        !Number.isInteger(limits.maxFiles) || limits.maxFiles <= 0 ||
        !Number.isFinite(limits.maxBytes) || limits.maxBytes <= 0 ||
        !Number.isFinite(limits.maxSingleBytes) || limits.maxSingleBytes <= 0 ||
        (limits.maxHoldMs !== undefined &&
            (!Number.isFinite(limits.maxHoldMs) || limits.maxHoldMs <= 0))
    ) {
        throw new RangeError("hash batch limits must be positive");
    }

    const batches: T[][] = [];
    let current: T[] = [];
    let currentBytes = 0;
    let currentStartedAt = 0;
    const flush = () => {
        if (current.length === 0) return;
        batches.push(current);
        current = [];
        currentBytes = 0;
        currentStartedAt = 0;
    };

    for (const item of items) {
        const observedAt = now();
        const size = sizeOf(item);
        if (!Number.isFinite(size) || size < 0) {
            throw new RangeError("hash batch item size must be finite and non-negative");
        }
        const singleton = size > limits.maxSingleBytes || size > limits.maxBytes;
        if (singleton) {
            flush();
            batches.push([item]);
            continue;
        }
        if (
            current.length > 0 &&
            limits.maxHoldMs !== undefined &&
            observedAt - currentStartedAt >= limits.maxHoldMs
        ) {
            flush();
        }
        if (
            current.length >= limits.maxFiles ||
            (current.length > 0 && currentBytes + size > limits.maxBytes)
        ) {
            flush();
        }
        if (current.length === 0) currentStartedAt = observedAt;
        current.push(item);
        currentBytes += size;
    }
    flush();
    return batches;
}

let activeTuner = new AdaptiveHashTuner("unknown");

export function configureHashRuntime(
    runtime: HashRuntime,
    onChange?: (tuning: HashTuning) => void,
): HashTuning {
    activeTuner = new AdaptiveHashTuner(runtime, onChange);
    return activeTuner.current();
}

export function getHashTuning(): HashTuning {
    return activeTuner.current();
}

export function observeHashFeedback(feedback: HashFeedback): void {
    activeTuner.observe(feedback);
}
