import esbuild from "esbuild";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { stat } from "node:fs/promises";
import { resolveBuildIdentity } from "./scripts/build-identity-config.mjs";

const production = process.argv[2] === "production";
const pluginDirectory = fileURLToPath(new URL("./", import.meta.url));
const buildIdentity = await resolveBuildIdentity({ pluginDirectory, production });
const sourceDir = fileURLToPath(new URL("./src", import.meta.url));
const wasmDir = fileURLToPath(new URL("./wasm", import.meta.url));
const BROWSER_HASH_WORKER_MAX_SOURCE_CHARS = 512 * 1024;
const BROWSER_HASH_WORKER_MAX_MODULE_BYTES = 1024 * 1024;
for (const name of ["sync_core_bg.wasm", "sync_core_simd_bg.wasm"]) {
    const bytes = (await stat(fileURLToPath(new URL(`./wasm/${name}`, import.meta.url)))).size;
    if (bytes < 8 || bytes > BROWSER_HASH_WORKER_MAX_MODULE_BYTES) {
        throw new Error(`${name} violates browser hash worker module bound: ${bytes} bytes`);
    }
}

const workerEntries = new Map([
    ["obsetync-hash-worker-source", "src/hash-worker-entry.ts"],
    ["obsetync-browser-probe-source", "src/browser-probe-entry.ts"],
    ["obsetync-browser-hash-worker-source", "src/browser-hash-worker-entry.ts"],
]);

const hashWorkerSourcePlugin = {
    name: "obsetync-worker-sources",
    setup(build) {
        build.onResolve({ filter: /^obsetync-.*-source$/ }, ({ path }) => {
            if (workerEntries.has(path)) return { path, namespace: "embedded-worker" };
        });
        build.onLoad({ filter: /.*/, namespace: "embedded-worker" }, async ({ path }) => {
            const desktop = path === "obsetync-hash-worker-source";
            const result = await esbuild.build({
                absWorkingDir: pluginDirectory,
                entryPoints: [workerEntries.get(path)],
                bundle: true,
                platform: desktop ? "node" : "browser",
                format: desktop ? "cjs" : "iife",
                target: desktop ? "node18" : "es2020",
                loader: desktop ? { ".wasm": "binary" } : {},
                sourcemap: desktop && !production ? "inline" : false,
                minify: production,
                keepNames: true,
                write: false,
                metafile: !production,
                logLevel: production ? "warning" : "silent",
            });
            const contents = result.outputFiles[0]?.text;
            if (!contents) throw new Error(`${path} produced no JavaScript output`);
            if (path === "obsetync-browser-hash-worker-source" && contents.length > BROWSER_HASH_WORKER_MAX_SOURCE_CHARS) {
                throw new Error(`browser hash worker source violates bound: ${contents.length} chars`);
            }
            return {
                contents, loader: "text",
                watchFiles: Object.keys(result.metafile?.inputs ?? {}).map(input => resolve(pluginDirectory, input)),
                watchDirs: [sourceDir, wasmDir],
            };
        });
    },
};

const context = await esbuild.context({
    entryPoints: ["src/main.ts"],
    bundle: true,
    plugins: [hashWorkerSourcePlugin],
    external: [
        "obsidian",
        "electron",
        "@codemirror/autocomplete",
        "@codemirror/collab",
        "@codemirror/commands",
        "@codemirror/language",
        "@codemirror/lint",
        "@codemirror/search",
        "@codemirror/state",
        "@codemirror/view",
        "@lezer/common",
        "@lezer/highlight",
        "@lezer/lr",
    ],
    // The "binary" loader reads both scalar/SIMD .wasm files at build time.
    // The desktop worker is separately bundled with its own SIMD instance and
    // embedded as source for worker_threads({eval:true}); no vault bytes cross
    // that boundary. Everything stays self-contained in main.js, with no WASM
    // or worker download. This is what unblocks iOS, where BRAT +
    // Obsidian's mobile plugin loader inconsistently honor the manifest's
    // `pluginFiles` field, sometimes leaving the WASM binary missing and
    // sending the plugin into silent-stub mode.
    loader: {
        ".wasm": "binary",
    },
    format: "cjs",
    target: "es2020",
    logLevel: "info",
    sourcemap: production ? false : "inline",
    treeShaking: true,
    outfile: "main.js",
    minify: production,
    // Identifier minification would mangle class names (ObsetyncSyncEngine →
    // `ht`), erasing the heap-snapshot / Performance-trace attribution the
    // Obsetync* prefixes exist for. keepNames pins class + function .name in
    // release builds at a few KB of bundle overhead.
    keepNames: true,
    define: {
        __OBSETYNC_BUILD_SEMVER__: JSON.stringify(buildIdentity.semver),
        __OBSETYNC_BUILD_GIT_COMMIT__: JSON.stringify(buildIdentity.gitCommit),
        __OBSETYNC_BUILD_SOURCE_STATE__: JSON.stringify(buildIdentity.sourceState),
        __OBSETYNC_BUILD_MODE__: JSON.stringify(buildIdentity.buildMode),
    },
});

if (production) {
    await context.rebuild();
    process.exit(0);
} else {
    await context.watch();
}
