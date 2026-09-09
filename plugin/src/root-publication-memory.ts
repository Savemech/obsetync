import { estimateStreamingHashWorkset } from "./hash-source-budget";
import { TREE_ROOT_EXPORT_PAGE_BYTES } from "./tree-root-export-job";

export const ROOT_INTENT_HASH_FEED_BYTES = 64 * 1024;
const ROOT_INTENT_FIXED_PREPARATION_BYTES = 256 * 1024 + 512;

export interface RootIntentPreparationWorkset {
    /** Caller-owned base64, decoder, preimage and bounded encoding scratch. */
    ownerBytes: number;
    /** Reusable streaming-hasher quota. */
    workBytes: number;
    totalBytes: number;
}

function positiveSafeInteger(value: number, name: string): number {
    if (!Number.isSafeInteger(value) || value < 1) throw new RangeError(`${name} must be a positive safe integer`);
    return value;
}

/** Conservative controlled-copy allowance for materializing one immutable
 * root request and hashing its canonical preimage. It is not an allocator,
 * GC, native btoa/atob, metadata/WAL, or process-RSS measurement. */
export function estimateRootIntentPreparationWorkset(rootBytes: number): RootIntentPreparationWorkset {
    positiveSafeInteger(rootBytes, "rootBytes");
    const base64Chars = 4 * Math.ceil(rootBytes / 3);
    const ownerBytes = positiveSafeInteger(
        6 * rootBytes + 2 * base64Chars + ROOT_INTENT_FIXED_PREPARATION_BYTES,
        "root intent preparation ownerBytes",
    );
    const workBytes = estimateStreamingHashWorkset(ROOT_INTENT_HASH_FEED_BYTES);
    return { ownerBytes, workBytes,
        totalBytes: positiveSafeInteger(ownerBytes + workBytes, "root intent preparation totalBytes") };
}

/** Largest v1 conservative arena whose export owner and fixed preparation
 * quota can fit the pool. offsetBytes <= (arena - 512) / 32. Actual admission
 * still arbitrates concurrent owners and may wait or reject independently. */
export function rootPublicationArenaLimit(capacityBytes: number, preparationBytes: number): number {
    positiveSafeInteger(capacityBytes, "capacityBytes");
    positiveSafeInteger(preparationBytes, "preparationBytes");
    const remaining = capacityBytes - 2 * TREE_ROOT_EXPORT_PAGE_BYTES - preparationBytes;
    const limit = Math.floor((32 * remaining + 512) / 65);
    return Math.max(512, Math.min(0xffff_ffff, limit));
}
