import { createWasmLoader, type WasmCandidate } from "./wasm-runtime";

const check = (condition: unknown, message: string) => {
    if (!condition) throw new Error(message);
};

const bytes = (marker: number) => new Uint8Array([marker]);

function candidate(
    mode: "scalar" | "simd",
    marker: number,
    initialize: () => Promise<void>,
): WasmCandidate<{ marker: number }> {
    return {
        mode,
        bytes: bytes(marker),
        exports: { marker },
        initialize: async () => initialize(),
    };
}

async function supportedSimdIsSelectedAndCached(): Promise<void> {
    let scalarInits = 0;
    let simdInits = 0;
    const loader = createWasmLoader({
        scalar: candidate("scalar", 1, async () => { scalarInits++; }),
        simd: candidate("simd", 2, async () => { simdInits++; }),
        validate: (moduleBytes) => moduleBytes[0] === 2,
    });

    const first = await loader();
    const second = await loader();
    check(first === second, "session loader did not cache its selection");
    check(first.mode === "simd" && first.exports.marker === 2, "SIMD was not selected");
    check(simdInits === 1, "SIMD initialized more than once");
    check(scalarInits === 0, "scalar initialized despite SIMD success");
}

async function unsupportedSimdFallsBackToScalar(): Promise<void> {
    let simdInits = 0;
    const fallbacks: string[] = [];
    const loader = createWasmLoader({
        scalar: candidate("scalar", 1, async () => {}),
        simd: candidate("simd", 2, async () => { simdInits++; }),
        validate: (moduleBytes) => moduleBytes[0] === 1,
        onSimdFallback: (reason) => fallbacks.push(reason),
    });

    const selected = await loader();
    check(selected.mode === "scalar", "unsupported SIMD did not fall back");
    check(simdInits === 0, "unsupported SIMD was instantiated");
    check(fallbacks.join("") === "validation", "validation fallback was not reported");
}

async function simdInitFailureFallsBackToScalar(): Promise<void> {
    let scalarInits = 0;
    const fallbacks: string[] = [];
    const loader = createWasmLoader({
        scalar: candidate("scalar", 1, async () => { scalarInits++; }),
        simd: candidate("simd", 2, async () => { throw new Error("compile failed"); }),
        validate: () => true,
        onSimdFallback: (reason) => fallbacks.push(reason),
    });

    const selected = await loader();
    check(selected.mode === "scalar", "SIMD init failure did not fall back");
    check(scalarInits === 1, "scalar fallback was not initialized");
    check(fallbacks.join("") === "initialization", "init fallback was not reported");
}

async function invalidScalarFailsClosed(): Promise<void> {
    const loader = createWasmLoader({
        scalar: candidate("scalar", 1, async () => {}),
        simd: candidate("simd", 2, async () => {}),
        validate: () => false,
    });
    let caught: unknown;
    try {
        await loader();
    } catch (error) {
        caught = error;
    }
    check(caught instanceof Error, "invalid scalar module did not fail closed");
    const message = caught instanceof Error ? caught.message : "";
    check(message.includes("scalar"), "scalar failure lost its diagnostic");
}

void supportedSimdIsSelectedAndCached()
    .then(unsupportedSimdFallsBackToScalar)
    .then(simdInitFailureFallsBackToScalar)
    .then(invalidScalarFailsClosed)
    .then(() => console.log("wasm-runtime.test: 13 assertions passed"))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
