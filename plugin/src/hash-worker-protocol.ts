export type HashWorkerMode = "hash" | "manifest";

/** Identity of the exact desktop file scanned by a hash worker. Size/mtime
 * alone do not detect an atomic same-metadata pathname replacement. */
export interface DesktopFileFingerprint {
    size: number;
    mtime: number;
    ctime: number;
    device: number;
    inode: number;
}

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

/** Both observations are native timestamps. Keep their fractional milliseconds. */
export function desktopFileFingerprintMatches(
    expected: DesktopFileFingerprint,
    actual: DesktopFileFingerprint,
): boolean {
    return expected.size === actual.size &&
        expected.mtime === actual.mtime &&
        expected.ctime === actual.ctime &&
        expected.device === actual.device &&
        expected.inode === actual.inode;
}

export const MAX_FASTCDC_CHUNK_BYTES = 4 * 1024 * 1024;

export interface HashWorkerJob {
    type: "job";
    job_id: string;
    absolute_path: string;
    expected_size: number;
    expected_mtime: number;
    mode: HashWorkerMode;
    feed_bytes: number;
}

export interface HashWorkerCancel {
    type: "cancel";
    job_id: string;
}

export type HashWorkerRequest = HashWorkerJob | HashWorkerCancel;

export interface HashWorkerReady {
    type: "ready";
    wasm_mode: "scalar" | "simd";
}

export interface HashWorkerHashResult {
    type: "result";
    job_id: string;
    mode: "hash";
    hash: string;
    size: number;
    mtime: number;
    fingerprint: DesktopFileFingerprint;
    read_ms: number;
    hash_ms: number;
}

export interface HashWorkerManifestResult {
    type: "result";
    job_id: string;
    mode: "manifest";
    manifest: {
        file_hash: string;
        total_size: number;
        chunks: Array<{ hash: string; offset: number; size: number }>;
    };
    size: number;
    mtime: number;
    fingerprint: DesktopFileFingerprint;
    read_ms: number;
    hash_ms: number;
}

export type HashWorkerResult = HashWorkerHashResult | HashWorkerManifestResult;

export type HashWorkerErrorCode =
    | "CANCELLED"
    | "FILE_DRIFT"
    | "INVALID_JOB"
    | "IO"
    | "INTERNAL";

export interface HashWorkerFailure {
    type: "error";
    job_id: string;
    code: HashWorkerErrorCode;
    message: string;
}

export interface HashWorkerFatal {
    type: "fatal";
    message: string;
}

export type HashWorkerResponse =
    | HashWorkerReady
    | HashWorkerResult
    | HashWorkerFailure
    | HashWorkerFatal;
