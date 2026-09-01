import esbuild from "esbuild";

const production = process.argv[2] === "production";

const context = await esbuild.context({
    entryPoints: ["src/main.ts"],
    bundle: true,
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
    // The "binary" loader reads both scalar/SIMD .wasm files at build time
    // and emits them as base64-encoded Uint8Array constants inside main.js.
    // That makes runtime selection self-contained, with no separate WASM
    // download. This is what unblocks iOS, where BRAT +
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
