import { BulkObjectKind, type BulkUploadRecord } from "./bulk-codec";
import { HashWorkerFileDriftError, type DesktopHashWorkerPool } from "./desktop-hash-workers";
import {
    uploadDesktopMissingRanges,
    openDesktopRangeReader,
    type DesktopRangeReaderOpener,
} from "./desktop-ranged-upload";
import { selectReconcileMissingRanges } from "./reconcile-content";
import { PUSH_CHUNKER_WORK_BYTES } from "./push-memory";
import type { TransientWorkScope } from "./transient-memory";
import { throwIfWorkAborted } from "./work-scheduler";
import { validateManifest } from "./pull";

export interface DesktopReconcileUploadInput {
    absolutePath: string;
    expectedHash: string;
    expectedSize: number;
    expectedMtime: number;
    feedBytes: number;
}

export type DesktopReconcileUploadResult =
    | {
        status: "drifted";
        workerReadMs: number;
        workerHashMs: number;
    }
    | {
        status: "uploaded";
        neededBytes: number;
        uploadedBytes: number;
        peakBufferedBytes: number;
        workerReadMs: number;
        workerHashMs: number;
        rangeReadMs: number;
    };

/** Rebuild one missing large-file manifest without materializing the file in
 * the renderer. Pass 1 runs in the SIMD worker; pass 2 reads only the server's
 * missing ranges and publishes the manifest after their ACKs. */
export async function reconcileDesktopLargeFile(
    worker: Pick<DesktopHashWorkerPool, "run">,
    input: DesktopReconcileUploadInput,
    checkContentChunks: (hashes: readonly string[]) => Promise<string[]>,
    putRecords: (records: readonly BulkUploadRecord[]) => Promise<void>,
    signal?: AbortSignal,
    openReader?: DesktopRangeReaderOpener,
    options: {
        memory?: TransientWorkScope;
        maxBufferedBytes?: number;
        beforeRead?: () => Promise<void>;
        onNeededBytes?: (bytes: number) => void;
    } = {},
): Promise<DesktopReconcileUploadResult> {
    throwIfWorkAborted(signal);
    const job = {
        absolutePath: input.absolutePath,
        expectedSize: input.expectedSize,
        expectedMtime: input.expectedMtime,
        mode: "manifest" as const,
        feedBytes: input.feedBytes,
    };
    const result = options.memory
        ? await options.memory.run(PUSH_CHUNKER_WORK_BYTES,
            context => context.track(() => worker.run(job, signal, context)), { signal })
        : await worker.run(job, signal);
    throwIfWorkAborted(signal);
    if (result.mode !== "manifest") {
        throw new Error("reconcile hash worker mode mismatch");
    }
    if (result.manifest.file_hash !== input.expectedHash) {
        return {
            status: "drifted",
            workerReadMs: result.read_ms,
            workerHashMs: result.hash_ms,
        };
    }
    validateManifest(result.manifest, input.expectedHash, input.expectedSize);

    const missingHashes = result.manifest.chunks.length > 0
        ? await checkContentChunks(result.manifest.chunks.map((chunk) => chunk.hash))
        : [];
    throwIfWorkAborted(signal);
    const missingRanges = selectReconcileMissingRanges(
        result.manifest.chunks,
        missingHashes,
    );
    const neededBytes = missingRanges.reduce((sum, range) => sum + range.size, 0);
    options.onNeededBytes?.(neededBytes);
    let manifestBytes = 0;
    const upload = () => uploadDesktopMissingRanges(
        {
            absolutePath: input.absolutePath,
            fingerprint: result.fingerprint,
        },
        missingRanges,
        putRecords,
        async () => {
            throwIfWorkAborted(signal);
            const manifestRecord: BulkUploadRecord = {
                kind: BulkObjectKind.Manifest,
                hash: input.expectedHash,
                data: new TextEncoder().encode(JSON.stringify({
                    file_hash: input.expectedHash,
                    total_size: result.manifest.total_size,
                    chunks: result.manifest.chunks,
                })),
            };
            manifestBytes = manifestRecord.data.byteLength;
            await putRecords([manifestRecord]);
        },
        {
            maxBufferedBytes: options.maxBufferedBytes,
            openReader: async (source) => {
                throwIfWorkAborted(signal);
                const reader = await (openReader ?? openDesktopRangeReader)(source);
                // uploadDesktopMissingRanges always closes this reader, even
                // if cancellation arrived while native open was outstanding.
                return {
                    verify: async () => {
                        throwIfWorkAborted(signal);
                        await reader.verify();
                        throwIfWorkAborted(signal);
                    },
                    read: async (offset, size) => {
                        await options.beforeRead?.();
                        throwIfWorkAborted(signal);
                        const data = await reader.read(offset, size);
                        throwIfWorkAborted(signal);
                        if (data.byteLength !== size || data.buffer.byteLength > size) {
                            throw new HashWorkerFileDriftError("range exceeded admitted source bytes");
                        }
                        return data;
                    },
                    close: () => reader.close(),
                };
            },
        },
    );
    // Includes native reader close, not merely the caller-visible upload ACK.
    const ranged = options.memory ? await options.memory.track(upload) : await upload();
    throwIfWorkAborted(signal);
    return {
        status: "uploaded",
        neededBytes,
        uploadedBytes: ranged.uploadedBytes,
        peakBufferedBytes: Math.max(ranged.peakBufferedBytes, manifestBytes),
        workerReadMs: result.read_ms,
        workerHashMs: result.hash_ms,
        rangeReadMs: ranged.readMs,
    };
}
