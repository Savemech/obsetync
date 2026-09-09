// @ts-ignore generated together with both production WASM variants
import initScalar, * as scalar from "../wasm/sync_core";
// @ts-ignore generated together with both production WASM variants
import initSimd, * as simd from "../wasm/sync_core_simd";
import {
    BROWSER_HASH_WORKER_MAX_INPUT_BYTES,
    BROWSER_HASH_WORKER_MAX_HEAP_BYTES,
    BROWSER_HASH_WORKER_MAX_MODULE_BYTES,
    BROWSER_HASH_WORKER_QUALIFICATION_HASH,
    BROWSER_HASH_WORKER_SCHEMA,
    type BrowserHashWorkerMode,
    type BrowserHashWorkerRequest,
    validBrowserHashFeedBytes,
} from "./browser-hash-worker-protocol";

interface WorkerHasher { update(bytes: Uint8Array): void; finalize(): string; free(): void }
interface WorkerExports { Hasher: new () => WorkerHasher }
const host = globalThis as unknown as {
    onmessage: ((event: MessageEvent<BrowserHashWorkerRequest>) => void) | null;
    postMessage(message: unknown, transfer?: Transferable[]): void;
};

let generation: number | null = null;
let wasm: WorkerExports | null = null;
let wasmMode: BrowserHashWorkerMode | null = null;
let wasmMemory: WebAssembly.Memory | null = null;
let activeJob: number | null = null;
const cancelled = new Set<number>();

const reply = (message: unknown, transfer: Transferable[] = []) => host.postMessage(message, transfer);
const envelope = (message: object) => ({ schema: BROWSER_HASH_WORKER_SCHEMA, generation, ...message });
const yieldWorker = () => new Promise<void>(resolve => setTimeout(resolve, 0));

async function initialize(scalarBuffer: ArrayBuffer, simdBuffer: ArrayBuffer): Promise<void> {
    const simdBytes = new Uint8Array(simdBuffer);
    try {
        if (WebAssembly.validate(simdBytes as BufferSource)) {
            const initialized = await initSimd({ module_or_path: simdBytes });
            wasm = simd as unknown as WorkerExports;
            wasmMemory = initialized.memory;
            wasmMode = "simd";
            return;
        }
    } catch { /* scalar is the independently qualified fallback */ }
    const scalarBytes = new Uint8Array(scalarBuffer);
    if (!WebAssembly.validate(scalarBytes as BufferSource)) throw new Error("scalar validation failed");
    const initialized = await initScalar({ module_or_path: scalarBytes });
    wasm = scalar as unknown as WorkerExports;
    wasmMemory = initialized.memory;
    wasmMode = "scalar";
}

async function hash(bytes: Uint8Array, feedBytes: number, cpuSliceMs: number, jobId: number): Promise<string | null> {
    const hasher = new wasm!.Hasher();
    try {
        let sliceStarted = performance.now();
        for (let offset = 0; offset < bytes.byteLength;) {
            if (cancelled.has(jobId)) return null;
            const end = Math.min(bytes.byteLength, offset + feedBytes);
            hasher.update(bytes.subarray(offset, end));
            offset = end;
            if (performance.now() - sliceStarted >= cpuSliceMs) {
                await yieldWorker();
                sliceStarted = performance.now();
            }
        }
        if (cancelled.has(jobId)) return null;
        return hasher.finalize();
    } finally {
        hasher.free();
    }
}

host.onmessage = async (event) => {
    const input = event.data;
    if (!input || input.schema !== BROWSER_HASH_WORKER_SCHEMA) return;
    if (input.type === "cancel") {
        if (input.generation === generation && input.jobId === activeJob) cancelled.add(input.jobId);
        return;
    }
    if (input.type === "init") {
        if (generation !== null || !Number.isSafeInteger(input.generation) || input.generation <= 0 ||
            !(input.qualification instanceof ArrayBuffer) || !(input.scalarWasm instanceof ArrayBuffer) ||
            !(input.simdWasm instanceof ArrayBuffer) || input.scalarWasm.byteLength < 8 ||
            input.simdWasm.byteLength < 8 || input.scalarWasm.byteLength > BROWSER_HASH_WORKER_MAX_MODULE_BYTES ||
            input.simdWasm.byteLength > BROWSER_HASH_WORKER_MAX_MODULE_BYTES) {
            reply(envelope({ type: "failed", jobId: null, reason: "PROTOCOL" }));
            return;
        }
        generation = input.generation;
        try {
            await initialize(input.scalarWasm, input.simdWasm);
            const digest = await hash(new Uint8Array(input.qualification), 64 * 1024, 8, 0);
            const wasmHeapBytes = wasmMemory?.buffer.byteLength ?? Number.POSITIVE_INFINITY;
            if (digest !== BROWSER_HASH_WORKER_QUALIFICATION_HASH || !wasmMode ||
                wasmHeapBytes > BROWSER_HASH_WORKER_MAX_HEAP_BYTES) throw new Error("self-test failed");
            reply(envelope({ type: "qualified", qualification: input.qualification,
                scalarWasm: input.scalarWasm, simdWasm: input.simdWasm, wasmMode, wasmHeapBytes }),
            [input.qualification, input.scalarWasm, input.simdWasm]);
        } catch {
            reply(envelope({ type: "failed", jobId: null, reason: "INIT",
                qualification: input.qualification, scalarWasm: input.scalarWasm,
                simdWasm: input.simdWasm }), [input.qualification, input.scalarWasm, input.simdWasm]);
        }
        return;
    }
    if (input.type !== "hash" || input.generation !== generation || !wasm || activeJob !== null ||
        !Number.isSafeInteger(input.jobId) || input.jobId <= 0 || !(input.bytes instanceof ArrayBuffer) ||
        input.bytes.byteLength > BROWSER_HASH_WORKER_MAX_INPUT_BYTES ||
        !validBrowserHashFeedBytes(input.feedBytes) || !Number.isFinite(input.cpuSliceMs) ||
        input.cpuSliceMs < 1 || input.cpuSliceMs > 16) {
        const transfer = input.type === "hash" && input.bytes instanceof ArrayBuffer ? [input.bytes] : [];
        reply(envelope({ type: "failed", jobId: input.type === "hash" ? input.jobId : null,
            reason: "PROTOCOL", ...(transfer.length ? { bytes: input.bytes } : {}) }), transfer);
        return;
    }
    activeJob = input.jobId;
    const started = performance.now();
    try {
        const digest = await hash(new Uint8Array(input.bytes), input.feedBytes, input.cpuSliceMs, input.jobId);
        const wasmHeapBytes = wasmMemory?.buffer.byteLength ?? Number.POSITIVE_INFINITY;
        if (wasmHeapBytes > BROWSER_HASH_WORKER_MAX_HEAP_BYTES) throw new Error("worker heap exceeded bound");
        if (digest === null) {
            reply(envelope({ type: "cancelled", jobId: input.jobId, bytes: input.bytes }), [input.bytes]);
        } else {
            reply(envelope({ type: "result", jobId: input.jobId, bytes: input.bytes,
                hash: digest, hashMs: Math.max(0, performance.now() - started), wasmHeapBytes }),
            [input.bytes]);
        }
    } catch {
        reply(envelope({ type: "failed", jobId: input.jobId, reason: "HASH", bytes: input.bytes }),
            [input.bytes]);
    } finally {
        cancelled.delete(input.jobId);
        activeJob = null;
    }
};
