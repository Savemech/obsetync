import { BulkObjectKind, type BulkUploadRecord } from "./bulk-codec";
import { HashWorkerFileDriftError } from "./desktop-hash-workers";
import {
    MAX_FASTCDC_CHUNK_BYTES,
    type DesktopFileFingerprint,
} from "./hash-worker-protocol";

/** The range queue owns at most this much chunk data between transport ACKs.
 * The transport may briefly allocate its own equally bounded encoded pack. */
export const DESKTOP_RANGE_QUEUE_BYTES = 8 * 1024 * 1024;
export const DESKTOP_RANGE_QUEUE_RECORDS = 256;

export interface DesktopRangeSource {
    absolutePath: string;
    fingerprint: DesktopFileFingerprint;
}

export interface DesktopMissingRange {
    hash: string;
    offset: number;
    size: number;
}

export interface DesktopRangeReader {
    read(offset: number, size: number): Promise<Uint8Array>;
    verify(): Promise<void>;
    close(): Promise<void>;
}

export type DesktopRangeReaderOpener = (
    source: DesktopRangeSource,
) => Promise<DesktopRangeReader>;

export interface DesktopRangedUploadOptions {
    maxBufferedBytes?: number;
    maxBufferedRecords?: number;
    openReader?: DesktopRangeReaderOpener;
}

export interface DesktopRangedUploadResult {
    uploadedBytes: number;
    uploadedRanges: number;
    peakBufferedBytes: number;
    readMs: number;
}

const now = (): number => globalThis.performance?.now?.() ?? Date.now();
const validHash = (value: string): boolean => /^[0-9a-f]{64}$/.test(value);
const timestampMatches = (left: number, right: number): boolean =>
    Math.abs(left - right) <= 1;

export function validDesktopFileFingerprint(
    value: unknown,
): value is DesktopFileFingerprint {
    if (!value || typeof value !== "object") return false;
    const fingerprint = value as DesktopFileFingerprint;
    return Number.isSafeInteger(fingerprint.size) && fingerprint.size >= 0 &&
        Number.isFinite(fingerprint.mtime) && fingerprint.mtime >= 0 &&
        Number.isFinite(fingerprint.ctime) && fingerprint.ctime >= 0 &&
        Number.isInteger(fingerprint.device) && fingerprint.device >= 0 &&
        Number.isInteger(fingerprint.inode) && fingerprint.inode >= 0;
}

export function desktopFileFingerprintMatches(
    expected: DesktopFileFingerprint,
    actual: DesktopFileFingerprint,
): boolean {
    return expected.size === actual.size &&
        timestampMatches(expected.mtime, actual.mtime) &&
        timestampMatches(expected.ctime, actual.ctime) &&
        expected.device === actual.device &&
        expected.inode === actual.inode;
}

function fingerprintFromStats(stats: import("node:fs").Stats): DesktopFileFingerprint {
    return {
        size: Number(stats.size),
        mtime: Number(stats.mtimeMs),
        ctime: Number(stats.ctimeMs),
        device: Number(stats.dev),
        inode: Number(stats.ino),
    };
}

function nodeModules(): {
    fs: typeof import("node:fs/promises");
    path: typeof import("node:path");
} {
    const requireNode = (typeof require === "function"
        ? require
        : (globalThis as typeof globalThis & { require?: NodeRequire }).require) as
        | NodeRequire
        | undefined;
    if (!requireNode) {
        throw new Error("desktop ranged upload requires the Electron Node.js runtime");
    }
    return {
        fs: requireNode("node:fs/promises") as typeof import("node:fs/promises"),
        path: requireNode("node:path") as typeof import("node:path"),
    };
}

function driftError(message: string): HashWorkerFileDriftError {
    return new HashWorkerFileDriftError(message);
}

function isVanished(error: unknown): boolean {
    const code = (error as NodeJS.ErrnoException | undefined)?.code;
    return code === "ENOENT" || code === "ESTALE";
}

/** Open one immutable-by-fingerprint view of a desktop pathname. Both the
 * open handle and pathname are checked, so an atomic replacement cannot hide
 * behind an otherwise stable handle. */
export const openDesktopRangeReader: DesktopRangeReaderOpener = async (source) => {
    if (
        typeof source.absolutePath !== "string" ||
        source.absolutePath.includes("\0") ||
        !validDesktopFileFingerprint(source.fingerprint)
    ) {
        throw new RangeError("invalid desktop range source");
    }
    const { fs, path } = nodeModules();
    if (!path.isAbsolute(source.absolutePath)) {
        throw new RangeError("desktop range source must be absolute");
    }

    let handle: import("node:fs/promises").FileHandle;
    try {
        handle = await fs.open(source.absolutePath, "r");
    } catch (error) {
        if (isVanished(error)) throw driftError("file disappeared before ranged upload");
        throw error;
    }
    let closed = false;

    const verify = async (): Promise<void> => {
        if (closed) throw new Error("desktop range reader is closed");
        try {
            const [handleStats, pathStats] = await Promise.all([
                handle.stat(),
                fs.stat(source.absolutePath),
            ]);
            const handleFingerprint = fingerprintFromStats(handleStats);
            const pathFingerprint = fingerprintFromStats(pathStats);
            if (
                !desktopFileFingerprintMatches(source.fingerprint, handleFingerprint) ||
                !desktopFileFingerprintMatches(source.fingerprint, pathFingerprint) ||
                !desktopFileFingerprintMatches(handleFingerprint, pathFingerprint)
            ) {
                throw driftError("file changed between manifest planning and ranged upload");
            }
        } catch (error) {
            if (error instanceof HashWorkerFileDriftError) throw error;
            if (isVanished(error)) throw driftError("file disappeared during ranged upload");
            throw error;
        }
    };

    return {
        async read(offset, size): Promise<Uint8Array> {
            if (
                !Number.isSafeInteger(offset) || offset < 0 ||
                !Number.isSafeInteger(size) || size <= 0 ||
                size > MAX_FASTCDC_CHUNK_BYTES ||
                offset > source.fingerprint.size - size
            ) {
                throw new RangeError("invalid desktop file range");
            }
            const output = new Uint8Array(size);
            let written = 0;
            while (written < size) {
                const { bytesRead } = await handle.read(
                    output,
                    written,
                    size - written,
                    offset + written,
                );
                if (bytesRead === 0) {
                    throw driftError("file ended during ranged upload");
                }
                written += bytesRead;
            }
            return output;
        },
        verify,
        async close(): Promise<void> {
            if (closed) return;
            closed = true;
            await handle.close();
        },
    };
};

function validateRanges(
    source: DesktopRangeSource,
    ranges: readonly DesktopMissingRange[],
): void {
    if (!validDesktopFileFingerprint(source.fingerprint)) {
        throw new RangeError("invalid desktop range source fingerprint");
    }
    let previousEnd = 0;
    const hashes = new Set<string>();
    for (const range of ranges) {
        if (
            !range || !validHash(range.hash) || hashes.has(range.hash) ||
            !Number.isSafeInteger(range.offset) || range.offset < previousEnd ||
            !Number.isSafeInteger(range.size) || range.size <= 0 ||
            range.size > MAX_FASTCDC_CHUNK_BYTES ||
            range.offset > source.fingerprint.size - range.size
        ) {
            throw new RangeError("invalid or overlapping desktop upload ranges");
        }
        hashes.add(range.hash);
        previousEnd = range.offset + range.size;
    }
}

/** Execute pass 2 of desktop large-file upload. Only ranges selected by the
 * server's missing bitmap are read. ACKed packs are discarded before the next
 * pack is filled; a retry therefore starts from a fresh missing bitmap and
 * naturally resumes at the first object the server still lacks. */
export async function uploadDesktopMissingRanges(
    source: DesktopRangeSource,
    ranges: readonly DesktopMissingRange[],
    putRecords: (records: readonly BulkUploadRecord[]) => Promise<void>,
    finalize: () => Promise<void>,
    options: DesktopRangedUploadOptions = {},
): Promise<DesktopRangedUploadResult> {
    validateRanges(source, ranges);
    const maxBufferedBytes = options.maxBufferedBytes ?? DESKTOP_RANGE_QUEUE_BYTES;
    const maxBufferedRecords = options.maxBufferedRecords ?? DESKTOP_RANGE_QUEUE_RECORDS;
    if (
        !Number.isSafeInteger(maxBufferedBytes) || maxBufferedBytes <= 0 ||
        maxBufferedBytes > DESKTOP_RANGE_QUEUE_BYTES ||
        !Number.isSafeInteger(maxBufferedRecords) || maxBufferedRecords <= 0 ||
        maxBufferedRecords > DESKTOP_RANGE_QUEUE_RECORDS
    ) {
        throw new RangeError("invalid desktop range queue bounds");
    }

    const reader = await (options.openReader ?? openDesktopRangeReader)(source);
    let primaryError: unknown;
    let pending: BulkUploadRecord[] = [];
    let pendingBytes = 0;
    let peakBufferedBytes = 0;
    let uploadedBytes = 0;
    let uploadedRanges = 0;
    let readMs = 0;

    const flush = async (): Promise<void> => {
        if (pending.length === 0) return;
        await putRecords(pending);
        uploadedBytes += pendingBytes;
        uploadedRanges += pending.length;
        pending = [];
        pendingBytes = 0;
    };

    try {
        if (ranges.length === 0) await reader.verify();
        for (const range of ranges) {
            if (
                pending.length > 0 &&
                (pending.length >= maxBufferedRecords ||
                    pendingBytes + range.size > maxBufferedBytes)
            ) {
                await flush();
            }

            const readStarted = now();
            await reader.verify();
            const data = await reader.read(range.offset, range.size);
            readMs += Math.max(0, now() - readStarted);
            if (data.byteLength !== range.size) {
                throw driftError("ranged upload returned a truncated chunk");
            }
            pending.push({
                kind: BulkObjectKind.ContentChunk,
                hash: range.hash,
                data,
            });
            pendingBytes += data.byteLength;
            peakBufferedBytes = Math.max(peakBufferedBytes, pendingBytes);

            if (
                pending.length >= maxBufferedRecords ||
                pendingBytes >= maxBufferedBytes
            ) {
                await flush();
            }
        }
        await flush();

        // Manifests are dependency records: upload only after every missing
        // content chunk was ACKed. Verify once more after that ACK so no stale
        // file metadata can reach the candidate/root commit.
        await finalize();
        await reader.verify();
        return {
            uploadedBytes,
            uploadedRanges,
            peakBufferedBytes,
            readMs,
        };
    } catch (error) {
        primaryError = error;
        throw error;
    } finally {
        try {
            await reader.close();
        } catch (closeError) {
            if (primaryError === undefined) throw closeError;
        }
    }
}
