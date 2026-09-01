export type WasmMode = "scalar" | "simd";
export type SimdFallbackReason = "validation" | "initialization";

export interface WasmCandidate<T> {
    mode: WasmMode;
    bytes: Uint8Array;
    exports: T;
    initialize(moduleBytes: Uint8Array): Promise<unknown>;
}

export interface WasmSelection<T> {
    mode: WasmMode;
    bytes: number;
    exports: T;
}

export interface WasmLoaderOptions<T> {
    scalar: WasmCandidate<T>;
    simd: WasmCandidate<T>;
    validate?: (moduleBytes: Uint8Array) => boolean;
    onSimdFallback?: (reason: SimdFallbackReason) => void;
}

function runtimeValidate(moduleBytes: Uint8Array): boolean {
    return WebAssembly.validate(moduleBytes as BufferSource);
}

async function initializeBest<T>(options: WasmLoaderOptions<T>): Promise<WasmSelection<T>> {
    const validate = options.validate ?? runtimeValidate;
    let simdValid = false;
    try {
        simdValid = validate(options.simd.bytes);
    } catch {
        simdValid = false;
    }

    if (simdValid) {
        try {
            await options.simd.initialize(options.simd.bytes);
            return {
                mode: "simd",
                bytes: options.simd.bytes.byteLength,
                exports: options.simd.exports,
            };
        } catch {
            options.onSimdFallback?.("initialization");
        }
    } else {
        options.onSimdFallback?.("validation");
    }

    let scalarValid = false;
    try {
        scalarValid = validate(options.scalar.bytes);
    } catch {
        scalarValid = false;
    }
    if (!scalarValid) {
        throw new Error("Obsetync scalar WASM failed runtime validation");
    }
    await options.scalar.initialize(options.scalar.bytes);
    return {
        mode: "scalar",
        bytes: options.scalar.bytes.byteLength,
        exports: options.scalar.exports,
    };
}

/** Create a session-scoped loader. The first selection promise is retained so
 * concurrent callers cannot initialize two wasm-bindgen module instances. */
export function createWasmLoader<T>(
    options: WasmLoaderOptions<T>,
): () => Promise<WasmSelection<T>> {
    let cached: Promise<WasmSelection<T>> | null = null;
    return () => {
        cached ??= initializeBest(options);
        return cached;
    };
}
