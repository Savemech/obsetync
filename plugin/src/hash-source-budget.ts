import {
    TransientMemoryBudget,
    reserveTransientWorkset,
    type TransientMemoryTuning,
    type TransientReservationBudget,
} from "./transient-memory";
export {
    configureTransientMemory as configureHashSourceBudget,
    transientMemorySnapshot as hashSourceBudgetSnapshot,
    closeTransientMemory as closeHashSourceBudget,
} from "./transient-memory";
import { throwIfWorkAborted } from "./work-scheduler";

const SOURCE_OVERHEAD_BYTES = 64 * 1024;

export class HashSourceGrowthError extends Error {
    constructor(readonly expectedBytes: number, readonly observedBytes: number) {
        super(`hash source grew beyond its admitted size (${observedBytes} > ${expectedBytes} bytes)`);
        this.name = "HashSourceGrowthError";
    }
}

function validBytes(value: number, name: string, allowZero = false): number {
    if (!Number.isSafeInteger(value) || value < (allowZero ? 0 : 1)) {
        throw new RangeError(`${name} must be a ${allowZero ? "non-negative" : "positive"} safe integer`);
    }
    return value;
}

/** Whole-file native source + renderer storage + bridge copy allowance, plus
 * two WASM feed buffers and fixed overhead. This is an admission estimate, not
 * a claim that host/native allocation or retained WASM heaps are measurable. */
export function estimateHashSourceWorkset(sourceBytes: number, feedBytes: number): number {
    validBytes(sourceBytes, "sourceBytes", true);
    validBytes(feedBytes, "feedBytes");
    return validBytes(3 * sourceBytes + 2 * feedBytes + SOURCE_OVERHEAD_BYTES, "hash source workset");
}

/** Allow bounded native read-ahead, iterator-owned chunks and WASM feed copies.
 * The consumer must keep its actual stream highWaterMark/feed within this cap. */
export function estimateStreamingHashWorkset(feedBytes: number): number {
    validBytes(feedBytes, "feedBytes");
    return validBytes(6 * feedBytes + SOURCE_OVERHEAD_BYTES, "streaming hash workset");
}

export type HashSourceBudgetTuning = TransientMemoryTuning;
/** Compatibility constructor for explicitly isolated consumers/tests. Normal
 * source and transport helpers below share the single transient-memory pool. */
export class HashSourceBudget extends TransientMemoryBudget {}

const sourceBudget = { reserve: reserveTransientWorkset };

export interface HashSourceOptions {
    /** Fresh regular-file stat taken before calling this helper. */
    sourceBytes: number;
    /** Maximum feed allowed throughout this source's consumer lifetime. */
    feedBytes: number;
    read(): Promise<Uint8Array>;
    /** Must return only the digest; do not retain/return source byte views. */
    consume(data: Uint8Array): string | Promise<string>;
    signal?: AbortSignal;
    /** Explicit pool for isolated consumers/tests; otherwise use the shared pool. */
    budget?: TransientReservationBudget;
}

/** Reserve before entering native read, and do not race an uncancellable read
 * against AbortSignal. Its lease stays held until read/consumer really settle.
 * An adapter can still allocate a file that grew after stat; reject that result
 * before hashing, but do not describe this post-read check as a native RAM cap. */
export async function withHashSource(options: HashSourceOptions): Promise<string> {
    throwIfWorkAborted(options.signal);
    const budget = options.budget ?? sourceBudget;
    const lease = await budget.reserve(
        estimateHashSourceWorkset(options.sourceBytes, options.feedBytes),
        { signal: options.signal },
    );
    let data: Uint8Array | undefined;
    try {
        throwIfWorkAborted(options.signal);
        data = await options.read();
        throwIfWorkAborted(options.signal);
        // A view can retain more than its visible bytes. Do not let a reader
        // smuggle an unexpectedly large backing allocation past admission.
        const observedBytes = Math.max(data.byteLength, data.buffer.byteLength);
        if (observedBytes > options.sourceBytes) {
            throw new HashSourceGrowthError(options.sourceBytes, observedBytes);
        }
        const digest = await options.consume(data);
        throwIfWorkAborted(options.signal);
        return digest;
    } finally {
        data = undefined;
        lease.release();
    }
}

export async function withStreamingHashSource(options: {
    feedBytes: number;
    /** Open the bounded stream only inside this callback; settle after close. */
    consume(): Promise<string>;
    signal?: AbortSignal;
    budget?: TransientReservationBudget;
}): Promise<string> {
    throwIfWorkAborted(options.signal);
    const budget = options.budget ?? sourceBudget;
    const lease = await budget.reserve(estimateStreamingHashWorkset(options.feedBytes), {
        signal: options.signal,
    });
    try {
        throwIfWorkAborted(options.signal);
        const digest = await options.consume();
        throwIfWorkAborted(options.signal);
        return digest;
    } finally {
        lease.release();
    }
}
