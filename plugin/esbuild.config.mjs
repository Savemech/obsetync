import esbuild from "esbuild";
import { fileURLToPath } from "node:url";

const production = process.argv[2] === "production";
const sourceDir = fileURLToPath(new URL("./src", import.meta.url));
const wasmDir = fileURLToPath(new URL("./wasm", import.meta.url));

async function bundleHashWorker() {
    const result = await esbuild.build({
        entryPoints: ["src/hash-worker-entry.ts"],
        bundle: true,
        platform: "node",
        format: "cjs",
        target: "node18",
        loader: { ".wasm": "binary" },
        write: false,
        minify: production,
        keepNames: true,
        sourcemap: production ? false : "inline",
        logLevel: production ? "warning" : "silent",
    });
    const output = result.outputFiles.find((file) => file.path.endsWith(".js")) ??
        result.outputFiles[0];
    if (!output) throw new Error("hash worker bundle produced no JavaScript output");
    return output.text;
}

let hashWorkerSource = await bundleHashWorker();
const hashWorkerSourcePlugin = {
    name: "obsetync-hash-worker-source",
    setup(build) {
        build.onResolve(
            { filter: /^obsetync-hash-worker-source$/ },
            () => ({ path: "obsetync-hash-worker-source", namespace: "obsetync-worker" }),
        );
        build.onLoad(
            { filter: /.*/, namespace: "obsetync-worker" },
            async () => {
                if (!production) hashWorkerSource = await bundleHashWorker();
                return {
                    contents: hashWorkerSource,
                    loader: "text",
                    watchDirs: [sourceDir, wasmDir],
                };
            },
        );
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
});

if (production) {
    await context.rebuild();
    process.exit(0);
} else {
    await context.watch();
}
