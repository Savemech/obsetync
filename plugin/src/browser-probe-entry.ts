// Static glue imports only: WASM bytes arrive from the already embedded plugin
// through a bounded synthetic diagnostic request, never fetch/importScripts.
// @ts-ignore generated together with both production WASM variants
import initScalar, * as scalar from "../wasm/sync_core";
// @ts-ignore generated together with both production WASM variants
import initSimd, * as simd from "../wasm/sync_core_simd";
import { PROBE_FIXTURE_BYTES, PROBE_MAX_MODULE_BYTES, PROBE_MAX_HEAP_BYTES,
    type BrowserProbeRequest, type BrowserProbeResult } from "./browser-probe-protocol";

const host = globalThis as unknown as {
    onmessage: ((event: MessageEvent) => void) | null;
    postMessage(message: unknown, transfer?: Transferable[]): void;
};
let started = false;
host.onmessage = async (event: MessageEvent<BrowserProbeRequest>) => {
    if (started) return;
    started = true;
    const input = event.data;
    if (input?.type !== "run" || !(input.fixture instanceof ArrayBuffer) ||
        input.fixture.byteLength !== PROBE_FIXTURE_BYTES ||
        ![input.scalar, input.simd].every(bytes => bytes instanceof ArrayBuffer &&
            bytes.byteLength > 0 && bytes.byteLength <= PROBE_MAX_MODULE_BYTES)) {
        host.postMessage({ type: "failed" });
        return;
    }
    const result: BrowserProbeResult = {
        type: "result", fixture: input.fixture, scalar: input.scalar, simd: input.simd,
        scalarHash: null, scalarHeapBytes: null,
        simdValidated: false, simdHash: null, simdHeapBytes: null,
    };
    try {
        const exports = await initScalar({ module_or_path: new Uint8Array(input.scalar) });
        const heap = exports.memory.buffer.byteLength;
        if (heap <= PROBE_MAX_HEAP_BYTES) {
            result.scalarHash = scalar.wasm_hash(new Uint8Array(input.fixture));
            result.scalarHeapBytes = exports.memory.buffer.byteLength;
        }
    } catch { /* fixed result, no host exception text in diagnostics */ }
    try {
        result.simdValidated = WebAssembly.validate(input.simd);
        if (result.simdValidated) {
            const exports = await initSimd({ module_or_path: new Uint8Array(input.simd) });
            const heap = exports.memory.buffer.byteLength;
            if (heap <= PROBE_MAX_HEAP_BYTES) {
                result.simdHash = simd.wasm_hash(new Uint8Array(input.fixture));
                result.simdHeapBytes = exports.memory.buffer.byteLength;
            }
        }
    } catch { /* scalar success remains independently visible */ }
    host.postMessage(result, [input.fixture, input.scalar, input.simd]);
    host.postMessage({ type: "released", detached: [input.fixture, input.scalar, input.simd]
        .every(buffer => buffer.byteLength === 0) });
};
host.postMessage({ type: "ready" });
