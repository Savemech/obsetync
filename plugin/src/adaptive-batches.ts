import type { BatchLimits } from "./hash-runtime";

function snapshotLimits(limits: BatchLimits): Readonly<BatchLimits> {
    const snapshot = {
        maxFiles: limits.maxFiles,
        maxBytes: limits.maxBytes,
        maxSingleBytes: limits.maxSingleBytes,
        maxHoldMs: limits.maxHoldMs,
    };
    if (
        !Number.isInteger(snapshot.maxFiles) || snapshot.maxFiles <= 0 ||
        !Number.isFinite(snapshot.maxBytes) || snapshot.maxBytes <= 0 ||
        !Number.isFinite(snapshot.maxSingleBytes) || snapshot.maxSingleBytes <= 0 ||
        (snapshot.maxHoldMs !== undefined &&
            (!Number.isFinite(snapshot.maxHoldMs) || snapshot.maxHoldMs <= 0))
    ) {
        throw new RangeError("adaptive batch limits must be positive");
    }
    return Object.freeze(snapshot);
}

/**
 * Lazily plan one batch at a time using the consumer's current limits. No
 * batches or slices of the remaining tail are retained. The input collection
 * must keep a stable order while the iterator is in use.
 *
 * sizeOf must be pure: a byte/hold/singleton boundary can inspect the next
 * item without consuming it, then reevaluate it with the NEXT batch's limits.
 * Unknown sizes can map to limits.maxSingleBytes + 1, as in the eager planner.
 * An oversize singleton is only a grouping rule, never memory admission.
 *
 * Hold time applies while collecting a batch, not while its consumer awaits
 * read/upload. A boundary yields even if the next item has already been
 * inspected; that item's hold clock starts fresh on the next iteration.
 */
export function* adaptiveBatches<T>(
    items: readonly T[],
    sizeOf: (item: T, limits: Readonly<BatchLimits>) => number,
    getLimits: () => BatchLimits,
    now: () => number = () => globalThis.performance?.now?.() ?? Date.now(),
): Generator<T[], void, unknown> {
    let cursor = 0;
    while (cursor < items.length) {
        const limits = snapshotLimits(getLimits());
        const batch: T[] = [];
        let bytes = 0;
        let startedAt = 0;
        while (cursor < items.length && batch.length < limits.maxFiles) {
            const item = items[cursor];
            const observedAt = now();
            const size = sizeOf(item, limits);
            if (!Number.isFinite(size) || size < 0) {
                throw new RangeError("adaptive batch item size must be finite and non-negative");
            }
            const singleton = size > limits.maxSingleBytes || size > limits.maxBytes;
            if (batch.length > 0 && (
                singleton || bytes + size > limits.maxBytes ||
                (limits.maxHoldMs !== undefined && observedAt - startedAt >= limits.maxHoldMs)
            )) {
                break;
            }
            if (batch.length === 0) startedAt = observedAt;
            batch.push(item);
            bytes += size;
            cursor++;
            if (singleton) break;
        }
        yield batch;
    }
}
