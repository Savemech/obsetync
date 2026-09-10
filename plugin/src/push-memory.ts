import type { HashTuning } from "./hash-runtime";
import { BULK_MOBILE_MAX_BYTES, BULK_SERVER_MAX_BYTES } from "./bulk-codec";
import { MAX_FASTCDC_CHUNK_BYTES } from "./hash-worker-protocol";
import { estimateUploadBatchWorkset, TRANSPORT_ERROR_PAYLOAD_ALLOWANCE_BYTES,
    type TransientWorksetEstimate } from "./transient-memory";

/** StreamingChunker owns one fixed FastCDC maximum-size window. */
export const PUSH_CHUNKER_WORK_BYTES = MAX_FASTCDC_CHUNK_BYTES;

export interface PushMemoryFile {
    size: number;
    backingBytes?: number;
    chunked: boolean;
    /** Only a path/stat worker job followed by the native ranged reader. */
    ranged: boolean;
    /** Path-only hashing may allocate one native feed buffer per active job. */
    worker?: boolean;
}

export interface PushMemoryPlan extends TransientWorksetEstimate {
    rangeQueueBytes: number;
    manifestBytes: number;
    transportPayloadBytes: number;
}

/** Current FastCDC minimum is 256 KiB (last chunk may be smaller). This is
 * a conservative serialization allowance, not a change to chunk boundaries. */
export function estimateManifestBytes(size: number): number {
    return 256 + Math.ceil(size / (256 * 1024)) * 192;
}

/** Plan the COMPLETE simultaneous source/hash/transport lifetime before reads.
 * Existing immutable chunks can exceed the negotiated bulk object cap: their
 * single-object HTTP workspace remains included, without redefining chunks. */
export function planPushMemory(
    files: readonly PushMemoryFile[],
    tuning: HashTuning,
    rangeQueueBytes = Math.max(MAX_FASTCDC_CHUNK_BYTES, Math.min(8 * 1024 * 1024, tuning.maxBatchBytes)),
): PushMemoryPlan {
    let sourceBytes = 0;
    let hashBatchBytes = 0;
    let retainedRangeBytes = 0;
    let contentBytes = 0;
    let manifestBytes = 0;
    let largestSingle = 0;
    for (const file of files) {
        if (!Number.isSafeInteger(file.size) || file.size < 0 ||
            (file.backingBytes !== undefined && (!Number.isSafeInteger(file.backingBytes) || file.backingBytes < file.size))) {
            throw new RangeError("invalid push source size");
        }
        if (file.ranged) {
            retainedRangeBytes += Math.min(file.size, rangeQueueBytes);
            contentBytes += Math.min(file.size, rangeQueueBytes);
        } else {
            const retained = file.backingBytes ?? file.size;
            sourceBytes += retained;
            contentBytes += file.size;
            if (!file.chunked) hashBatchBytes += file.size;
        }
        if (file.chunked) {
            const manifest = estimateManifestBytes(file.size);
            manifestBytes += manifest;
            largestSingle = Math.max(largestSingle, Math.min(file.size, MAX_FASTCDC_CHUNK_BYTES), manifest);
        } else {
            largestSingle = Math.max(largestSingle, file.size);
        }
    }
    const bulkCap = tuning.runtime === "desktop" ? BULK_SERVER_MAX_BYTES : BULK_MOBILE_MAX_BYTES;
    const transportPayloadBytes = Math.max(TRANSPORT_ERROR_PAYLOAD_ALLOWANCE_BYTES, largestSingle,
        Math.min(bulkCap, contentBytes + manifestBytes + files.length * 42 + 10));
    const estimate = estimateUploadBatchWorkset({
        // JSON string, encoded manifest and serializer allowance have the same
        // lifetime as the batch; source factor also covers their copies.
        sourceBytes: sourceBytes + manifestBytes,
        retainedRangeBytes,
        hashBatchBytes,
        feedBytes: tuning.maxFeedBytes,
        transportPayloadBytes,
    });
    const workerFeeds = Math.min(tuning.readConcurrency,
        files.filter(file => file.worker || file.ranged).length);
    // Worker fixed heaps remain outside this ledger, but each running job's
    // native feed + WASM bridge buffer belongs to this admitted workset.
    const extraFeeds = 2 * tuning.maxFeedBytes * Math.max(0, workerFeeds - 1);
    estimate.ownerBytes += extraFeeds;
    // Hash/chunker and upload are separate phases. Lend the reusable transport
    // quota to chunkers instead of pretending both peaks happen simultaneously.
    const concurrentChunkers = Math.max(1, Math.min(tuning.readConcurrency,
        files.filter(file => file.chunked && (file.worker || file.ranged)).length));
    if (files.some(file => file.chunked)) {
        estimate.workBytes = Math.max(estimate.workBytes, concurrentChunkers * PUSH_CHUNKER_WORK_BYTES);
    }
    estimate.totalBytes = estimate.ownerBytes + estimate.workBytes;
    return { ...estimate, rangeQueueBytes, manifestBytes, transportPayloadBytes };
}

/** Conservative byte grouping for ordinary small sources. Exact workset
 * admission still runs afterwards; a singleton is never automatic approval. */
export function pushGroupingByteLimit(tuning: HashTuning, capacityBytes: number): number {
    const fixed = 2 * tuning.maxFeedBytes + 1024 * 1024;
    return Math.max(1, Math.min(tuning.maxBatchBytes, Math.floor((capacityBytes - fixed) / 11)));
}
