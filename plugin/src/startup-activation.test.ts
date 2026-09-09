import { publishStartupResult } from "./startup-activation";
import { createWasmLoader, type WasmSelection } from "./wasm-runtime";

let assertions = 0;
const check = (condition: unknown, message: string): void => {
    assertions++;
    if (!condition) throw new Error(message);
};
function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<T>((done, fail) => { resolve = done; reject = fail; });
    return { promise, resolve, reject };
}
const selection = (name: string, mode: "scalar" | "simd"): WasmSelection<{ name: string }> => ({
    exports: { name }, mode, bytes: 1,
});

/** Production publication seam, synthetic compilation results only. No real
 * Obsidian plugin/native lifecycle or wasm-bindgen module is emulated here. */
async function lateCompilationCannotReplaceCurrentModuleOrProfile(): Promise<void> {
    let generation = 1;
    let unloaded = false;
    const publications: string[] = [];
    let installed: WasmSelection<{ name: string }> | null = null;
    const install = (value: WasmSelection<{ name: string }>) => {
        installed = value;
        publications.push(`${value.exports.name}:${value.mode}`);
    };
    const old = deferred<WasmSelection<{ name: string }>>();
    const oldPublish = publishStartupResult(old.promise, () => generation === 1 && !unloaded, install);
    generation = 2;
    const current = selection("current", "scalar");
    check((await publishStartupResult(Promise.resolve(current), () => generation === 2 && !unloaded, install))?.value === current,
        "current compilation result was not available to resource construction");
    old.resolve(selection("obsolete", "simd"));
    check(await oldPublish === null && installed === current && publications.join() === "current:scalar",
        "late old compilation replaced current exports or published a stale runtime profile");

    const unloading = deferred<WasmSelection<{ name: string }>>();
    const afterUnload = publishStartupResult(unloading.promise, () => !unloaded, install);
    unloaded = true;
    unloading.resolve(selection("unloaded", "simd"));
    check(await afterUnload === null && publications.length === 1,
        "native compilation finishing after unload mutated module/governor/profile state");
}

async function staleFailuresAreIgnoredButCurrentFailuresPropagate(): Promise<void> {
    let current = true;
    let publications = 0;
    const old = deferred<string>();
    const stale = publishStartupResult(old.promise, () => current, () => { publications++; });
    current = false;
    old.reject(new Error("obsolete compilation failure"));
    check(await stale === null && publications === 0,
        "obsolete compilation rejection escaped to replacement startup/status handling");
    const expected = new Error("current compilation failed");
    let error: unknown;
    try { await publishStartupResult(Promise.reject(expected), () => true, () => { publications++; }); }
    catch (caught) { error = caught; }
    check(error === expected && publications === 0, "current compilation failure was hidden or published exports");
}

async function sharedLoaderPublishesOnlyTheCurrentGeneration(): Promise<void> {
    const compile = deferred<void>();
    let generation = 1;
    let compilations = 0;
    let publications = 0;
    const exports = { name: "session-module" };
    const loader = createWasmLoader({
        simd: { mode: "simd", bytes: new Uint8Array(1), exports,
            initialize: async () => { compilations++; await compile.promise; } },
        scalar: { mode: "scalar", bytes: new Uint8Array(1), exports,
            initialize: async () => { throw new Error("unexpected scalar fallback"); } },
        validate: () => true,
    });
    const old = publishStartupResult(loader(), () => generation === 1, () => { publications++; });
    generation = 2;
    const current = publishStartupResult(loader(), () => generation === 2, () => { publications++; });
    compile.resolve();
    const [oldResult, currentResult] = await Promise.all([old, current]);
    check(oldResult === null && currentResult?.value.exports === exports,
        "session-shared compilation selected the wrong initialization owner");
    check(compilations === 1 && publications === 1,
        "concurrent initializations duplicated compilation or governor/profile publication");
}

const watchdog = setTimeout(() => { throw new Error("startup publication test did not settle"); }, 10_000);
void lateCompilationCannotReplaceCurrentModuleOrProfile()
    .then(staleFailuresAreIgnoredButCurrentFailuresPropagate)
    .then(sharedLoaderPublishesOnlyTheCurrentGeneration)
    .then(() => console.log(`startup-activation.test: ${assertions} assertions passed`))
    .catch(error => { console.error(error); process.exitCode = 1; })
    .finally(() => clearTimeout(watchdog));
