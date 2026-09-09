/** Browser hash workers receive owned buffers only; vault paths never cross the boundary. */
export const BROWSER_HASH_WORKER_SCHEMA = 1 as const;
export const BROWSER_HASH_WORKER_MAX_INPUT_BYTES = 8 * 1024 * 1024;
export const BROWSER_HASH_WORKER_MAX_HEAP_BYTES = 16 * 1024 * 1024;
export const BROWSER_HASH_WORKER_MAX_SOURCE_CHARS = 512 * 1024;
export const BROWSER_HASH_WORKER_MAX_MODULE_BYTES = 1024 * 1024;
export const BROWSER_HASH_WORKER_MAX_FEED_BYTES = 1024 * 1024;
export const BROWSER_HASH_WORKER_MIN_FEED_BYTES = 64 * 1024;
export const BROWSER_HASH_WORKER_QUALIFICATION_BYTES = 64 * 1024;
export const BROWSER_HASH_WORKER_QUALIFICATION_HASH =
    "f16c9f867cc5384c7329aa4481eba6518c50ab21bf8a615e26bad55e57569b2c";

export type BrowserHashWorkerMode = "scalar" | "simd";

export interface BrowserHashWorkerInit {
    schema: typeof BROWSER_HASH_WORKER_SCHEMA;
    type: "init";
    generation: number;
    qualification: ArrayBuffer;
    scalarWasm: ArrayBuffer;
    simdWasm: ArrayBuffer;
}

export interface BrowserHashWorkerJob {
    schema: typeof BROWSER_HASH_WORKER_SCHEMA;
    type: "hash";
    generation: number;
    jobId: number;
    bytes: ArrayBuffer;
    feedBytes: number;
    cpuSliceMs: number;
}

export interface BrowserHashWorkerCancel {
    schema: typeof BROWSER_HASH_WORKER_SCHEMA;
    type: "cancel";
    generation: number;
    jobId: number;
}

export type BrowserHashWorkerRequest = BrowserHashWorkerInit | BrowserHashWorkerJob |
    BrowserHashWorkerCancel;

export interface BrowserHashWorkerQualified {
    schema: typeof BROWSER_HASH_WORKER_SCHEMA;
    type: "qualified";
    generation: number;
    qualification: ArrayBuffer;
    scalarWasm: ArrayBuffer;
    simdWasm: ArrayBuffer;
    wasmMode: BrowserHashWorkerMode;
    wasmHeapBytes: number;
}

export interface BrowserHashWorkerResult {
    schema: typeof BROWSER_HASH_WORKER_SCHEMA;
    type: "result";
    generation: number;
    jobId: number;
    bytes: ArrayBuffer;
    hash: string;
    hashMs: number;
    wasmHeapBytes: number;
}

export interface BrowserHashWorkerCancelled {
    schema: typeof BROWSER_HASH_WORKER_SCHEMA;
    type: "cancelled";
    generation: number;
    jobId: number;
    bytes: ArrayBuffer;
}

export interface BrowserHashWorkerFailed {
    schema: typeof BROWSER_HASH_WORKER_SCHEMA;
    type: "failed";
    generation: number;
    jobId: number | null;
    reason: "INIT" | "PROTOCOL" | "HASH";
    bytes?: ArrayBuffer;
    qualification?: ArrayBuffer;
    scalarWasm?: ArrayBuffer;
    simdWasm?: ArrayBuffer;
}

export type BrowserHashWorkerResponse = BrowserHashWorkerQualified |
    BrowserHashWorkerResult | BrowserHashWorkerCancelled | BrowserHashWorkerFailed;

export function browserHashQualificationFixture(): Uint8Array {
    return Uint8Array.from({ length: BROWSER_HASH_WORKER_QUALIFICATION_BYTES }, (_, i) => i & 255);
}

export function validBrowserHashFeedBytes(value: unknown): value is number {
    return Number.isSafeInteger(value) && Number(value) >= BROWSER_HASH_WORKER_MIN_FEED_BYTES &&
        Number(value) <= BROWSER_HASH_WORKER_MAX_FEED_BYTES;
}

export function validBrowserHash(value: unknown): value is string {
    return typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
}
