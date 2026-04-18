import esbuild from "esbuild";
import { copyFileSync, existsSync } from "fs";

const production = process.argv[2] === "production";

// Copy WASM output (from wasm-pack at ./wasm/) up to the plugin root so that
// main.js can load sync_core.js via `.obsidian/plugins/obsetync/sync_core.js`
// on both desktop and iOS, and BRAT / manual installs see a flat file tree.
for (const f of ["sync_core.js", "sync_core_bg.wasm"]) {
    const src = `wasm/${f}`;
    if (existsSync(src)) copyFileSync(src, f);
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
