export type HashWorkerMode = "hash" | "manifest";

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
