import type { FileChange } from "./push";
import type { SyncPriority } from "./settings";
import { throwIfWorkAborted } from "./work-scheduler";

export const PRIORITY_SORT_WORK_UNITS = 256;
export const PRIORITY_SORT_MAX_CHANGES = 65_536;

export interface PrioritySortOptions {
    /** MUST yield to the actual host; Promise.resolve alone is not UI fairness. */
    cooperate(): Promise<void>;
    signal?: AbortSignal;
    /** May lower, never raise, the reviewed queue's row ceiling. */
    maxChanges?: number;
}

export class PrioritySortError extends Error {
    constructor(readonly code: "PRIORITY_SORT_CHANGED" | "PRIORITY_SORT_ADMISSION" | "PRIORITY_SORT_RANDOM") {
        super(code === "PRIORITY_SORT_CHANGED" ? "priority sort input changed" :
            code === "PRIORITY_SORT_ADMISSION" ? "priority sort admission rejected" :
                "priority sort random source returned an invalid sample");
        this.name = "PrioritySortError";
    }
}

type Comparator = (left: FileChange, right: FileChange) => number;

function deterministicComparator(priority: SyncPriority): Comparator | null {
    switch (priority) {
        case "oldest": return (a, b) => (a.mtime ?? 0) - (b.mtime ?? 0);
        case "newest": return (a, b) => (b.mtime ?? 0) - (a.mtime ?? 0);
        case "smallest": return (a, b) => (a.size ?? 0) - (b.size ?? 0);
        case "biggest": return (a, b) => (b.size ?? 0) - (a.size ?? 0);
        case "alphabetic": return (a, b) => a.path.localeCompare(b.path);
        default: return null;
    }
}

/** Fisher-Yates consumes exactly one random draw and one work unit per
 * remaining position. The input copy is planner-owned, so no caller row or
 * array is mutated. */
function* shuffleSteps(
    changes: readonly FileChange[],
    length: number,
    random: () => number,
): Generator<void, FileChange[], void> {
    const copied = new Array<FileChange>(length);
    for (let index = 0; index < length; index++) { copied[index] = changes[index]; yield; }
    for (let index = length - 1; index > 0; index--) {
        const sample = random();
        if (!Number.isFinite(sample) || sample < 0 || sample >= 1) {
            throw new PrioritySortError("PRIORITY_SORT_RANDOM");
        }
        const selected = Math.floor(sample * (index + 1));
        const previous = copied[index]; copied[index] = copied[selected]; copied[selected] = previous;
        yield;
    }
    return copied;
}

function* stableSortSteps(
    changes: readonly FileChange[],
    length: number,
    compare: Comparator,
): Generator<void, FileChange[], void> {
    let source = new Array<FileChange>(length), target = new Array<FileChange>(length);
    for (let index = 0; index < length; index++) {
        source[index] = changes[index];
        yield;
    }
    for (let width = 1; width < length; width *= 2) {
        for (let start = 0; start < length; start += 2 * width) {
            const middle = Math.min(start + width, length), end = Math.min(start + 2 * width, length);
            let left = start, right = middle, output = start;
            while (left < middle && right < end) {
                // Native stable Array.sort treats zero/NaN as a tie. Choosing
                // the left row unless the comparator is positive preserves it.
                target[output++] = compare(source[left], source[right]) > 0
                    ? source[right++] : source[left++];
                yield;
            }
            while (left < middle) { target[output++] = source[left++]; yield; }
            while (right < end) { target[output++] = source[right++]; yield; }
        }
        [source, target] = [target, source];
    }
    return source;
}

function admittedLimit(value: number | undefined): number {
    if (value === undefined) return PRIORITY_SORT_MAX_CHANGES;
    if (!Number.isSafeInteger(value) || value < 1 || value > PRIORITY_SORT_MAX_CHANGES) {
        throw new PrioritySortError("PRIORITY_SORT_ADMISSION");
    }
    return value;
}

/** Exact synchronous compatibility behavior, including the historical random
 * comparator and sequential no-copy result. */
export function sortByPriority(changes: FileChange[], priority: SyncPriority): FileChange[] {
    switch (priority) {
        case "oldest": return [...changes].sort((a, b) => (a.mtime ?? 0) - (b.mtime ?? 0));
        case "newest": return [...changes].sort((a, b) => (b.mtime ?? 0) - (a.mtime ?? 0));
        case "smallest": return [...changes].sort((a, b) => (a.size ?? 0) - (b.size ?? 0));
        case "biggest": return [...changes].sort((a, b) => (b.size ?? 0) - (a.size ?? 0));
        case "alphabetic": return [...changes].sort((a, b) => a.path.localeCompare(b.path));
        case "random": return [...changes].sort(() => Math.random() - 0.5);
        default: return changes;
    }
}

/** Stable cooperative preparation sort for deterministic priorities. Random
 * uses an unbiased Fisher-Yates permutation: indexed copy and each swap are
 * separate bounded work units. This intentionally differs from the historical
 * biased native random comparator retained by the synchronous compatibility
 * API. Caller owns the dense valid array and scalar rows and
 * MUST keep both immutable until return; the length fence is not deep mutation
 * detection. A deterministic sort admits before allocating two arrays of at
 * most 65,536 row references (random allocates one). One result array remains
 * planner-owned; this count bound is not shared memory admission, measured
 * heap/RSS, or a guarantee that allocation/localeCompare/Math.random/GC yields. */
export async function sortByPriorityCooperatively(
    changes: FileChange[],
    priority: SyncPriority,
    options: PrioritySortOptions,
): Promise<FileChange[]> {
    if (typeof options?.cooperate !== "function") throw new TypeError("priority sort requires host cooperation");
    const cooperate = options.cooperate, signal = options.signal;
    const length = changes.length, limit = admittedLimit(options.maxChanges);
    if (length > limit) throw new PrioritySortError("PRIORITY_SORT_ADMISSION");
    const assertCurrent = () => {
        throwIfWorkAborted(signal);
        if (changes.length !== length) throw new PrioritySortError("PRIORITY_SORT_CHANGED");
    };
    assertCurrent();
    const compare = deterministicComparator(priority);
    if (!compare && priority !== "random") {
        const result = changes;
        assertCurrent();
        return result;
    }
    // Capture once before the first possible host turn. A different plugin or
    // test replacing the ambient function cannot splice RNG implementations
    // into one reviewed permutation.
    const steps = compare ? stableSortSteps(changes, length, compare) :
        shuffleSteps(changes, length, Math.random);
    let work = 0;
    for (;;) {
        assertCurrent();
        const step = steps.next();
        if (step.done) {
            assertCurrent();
            return step.value;
        }
        if (++work < PRIORITY_SORT_WORK_UNITS) continue;
        assertCurrent();
        await cooperate();
        assertCurrent();
        work = 0;
    }
}
