import { alignTreeFormatFromRoot } from "./tree-format";

let assertions = 0;
function ok(condition: unknown, message: string): void {
    assertions++;
    if (!condition) throw new Error(message);
}

const rows = new Map([
    ["a.md", { hash: "a".repeat(64), mtime: 10, size: 1 }],
    ["notes/b.md", { hash: "b".repeat(64), mtime: 20, size: 2 }],
]);
const syncBase = {
    allPaths: () => [...rows.keys()],
    getEntry: (path: string) => rows.get(path) ?? null,
    getTreeMtime: (path: string) => path === "a.md" ? 11 : null,
};

let version = 1;
let rebuilds = 0;
let rebuiltRows: any[] = [];
const tree = {
    tree_version: () => version,
    rebuild_from_entries_in_version: (next: number, json: string) => {
        rebuilds++;
        version = next;
        rebuiltRows = JSON.parse(json);
    },
};
const wasm = {
    wasm_root_version_from_bytes: (bytes: Uint8Array) => bytes[0],
};

const changed = alignTreeFormatFromRoot(wasm, tree, syncBase, new Uint8Array([2]));
ok(changed.changed && changed.version === 2, "v1→v2 transition was not detected");
ok(rebuilds === 1 && version === 2, "Tree v2 graph was not rebuilt exactly once");
ok(rebuiltRows.length === 2, "Tree format rebuild lost sync-base entries");
ok(rebuiltRows[0].mtime_ms === 11, "verified tree mtime was not preserved");
ok(rebuiltRows[1].mtime_ms === 20, "entry mtime fallback was not preserved");

const unchanged = alignTreeFormatFromRoot(wasm, tree, syncBase, new Uint8Array([2]));
ok(!unchanged.changed && rebuilds === 1, "same-version root rebuilt the graph");

let rejected = false;
try {
    alignTreeFormatFromRoot(wasm, tree, syncBase, new Uint8Array([3]));
} catch {
    rejected = true;
}
ok(rejected, "unknown authenticated root version was accepted");

console.log(`tree-format.test: ${assertions} assertions passed`);
