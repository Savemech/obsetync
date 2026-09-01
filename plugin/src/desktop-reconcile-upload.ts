import { BulkObjectKind, type BulkUploadRecord } from "./bulk-codec";
import type { DesktopHashWorkerPool } from "./desktop-hash-workers";
import {
    uploadDesktopMissingRanges,
    type DesktopRangeReaderOpener,
} from "./desktop-ranged-upload";
import { selectReconcileMissingRanges } from "./reconcile-content";

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
): Promise<DesktopReconcileUploadResult> {
    const result = await worker.run({
        absolutePath: input.absolutePath,
        expectedSize: input.expectedSize,
        expectedMtime: input.expectedMtime,
        mode: "manifest",
        feedBytes: input.feedBytes,
    }, signal);
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

    const missingHashes = result.manifest.chunks.length > 0
        ? await checkContentChunks(result.manifest.chunks.map((chunk) => chunk.hash))
        : [];
    const missingRanges = selectReconcileMissingRanges(
        result.manifest.chunks,
        missingHashes,
    );
    const neededBytes = missingRanges.reduce((sum, range) => sum + range.size, 0);
    let manifestBytes = 0;
    const ranged = await uploadDesktopMissingRanges(
        {
            absolutePath: input.absolutePath,
            fingerprint: result.fingerprint,
        },
        missingRanges,
        putRecords,
        async () => {
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
        openReader ? { openReader } : undefined,
    );
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
