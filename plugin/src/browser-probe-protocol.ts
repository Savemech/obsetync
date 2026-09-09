/** Diagnostic-only messages. No paths, credentials or vault contents. */
export const PROBE_FIXTURE_BYTES = 64 * 1024;
/** BLAKE3 of the synthetic byte pattern, verified by scalar/SIMD packaging tests. */
export const PROBE_FIXTURE_HASH = "f16c9f867cc5384c7329aa4481eba6518c50ab21bf8a615e26bad55e57569b2c";
export const PROBE_MAX_MODULE_BYTES = 1024 * 1024;
export const PROBE_MAX_HEAP_BYTES = 8 * 1024 * 1024;

export function probeFixture(): Uint8Array {
    return Uint8Array.from({ length: PROBE_FIXTURE_BYTES }, (_, i) => i & 255);
}

export interface BrowserProbeRequest {
    type: "run";
    fixture: ArrayBuffer;
    scalar: ArrayBuffer;
    simd: ArrayBuffer;
}

export interface BrowserProbeResult {
    type: "result";
    fixture: ArrayBuffer;
    scalar: ArrayBuffer;
    simd: ArrayBuffer;
    scalarHash: string | null;
    scalarHeapBytes: number | null;
    simdValidated: boolean;
    simdHash: string | null;
    simdHeapBytes: number | null;
}
