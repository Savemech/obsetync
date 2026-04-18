import esbuild from "esbuild";
import { copyFileSync, mkdirSync, existsSync } from "fs";

const production = process.argv[2] === "production";

// Copy WASM file to output if it exists.
const wasmSrc = "wasm/sync_core_bg.wasm";
if (existsSync(wasmSrc)) {
    mkdirSync("dist", { recursive: true });
    copyFileSync(wasmSrc, "dist/sync_core_bg.wasm");
}

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
    format: "cjs",
    target: "es2020",
    logLevel: "info",
    sourcemap: production ? false : "inline",
    treeShaking: true,
    outfile: "main.js",
    minify: production,
});

if (production) {
    await context.rebuild();
    process.exit(0);
} else {
    await context.watch();
}
