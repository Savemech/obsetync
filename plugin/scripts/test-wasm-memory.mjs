import assert from "node:assert/strict";
import Module from "node:module";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";

// Exercise the actual formatter in memory. These fixtures test host validation,
// not a native allocator, heap/RSS coverage, or a real-device memory limit.
const directory = fileURLToPath(new URL("../", import.meta.url));
const built = await build({
    absWorkingDir: directory, entryPoints: ["src/wasm-memory.ts"], bundle: true,
    platform: "node", format: "cjs", target: "node20", write: false,
    metafile: true, logLevel: "silent",
});
assert.deepEqual(Object.keys(built.metafile.inputs), ["src/wasm-memory.ts"]);
const filename = fileURLToPath(new URL("./wasm-memory-in-memory.cjs", import.meta.url));
const bundled = new Module(filename);
bundled.filename = filename;
bundled.paths = Module._nodeModulePaths(directory);
bundled._compile(built.outputFiles[0].text, filename);
const {
    formatWasmMemoryDebug,
    formatTreeChunkMemoryDebug,
    formatTreeMetadataMemoryDebug,
    formatReplacementInputMemoryDebug,
    formatReplacementV1EntriesMemoryDebug,
    formatReplacementV1GraphMemoryDebug,
    formatReplacementSortMemoryDebug,
    formatReplacementV2PlanningMemoryDebug,
    formatReplacementV2PostSortMemoryDebug,
    formatReplacementV2RangesMemoryDebug,
} = bundled.exports;
assert.equal(typeof formatWasmMemoryDebug, "function");
assert.equal(typeof formatTreeChunkMemoryDebug, "function");
assert.equal(typeof formatTreeMetadataMemoryDebug, "function");
assert.equal(typeof formatReplacementInputMemoryDebug, "function");
assert.equal(typeof formatReplacementV1EntriesMemoryDebug, "function");
assert.equal(typeof formatReplacementV1GraphMemoryDebug, "function");
assert.equal(typeof formatReplacementSortMemoryDebug, "function");
assert.equal(typeof formatReplacementV2PlanningMemoryDebug, "function");
assert.equal(typeof formatReplacementV2PostSortMemoryDebug, "function");
assert.equal(typeof formatReplacementV2RangesMemoryDebug, "function");

const COUNTERS = [
    "live_requested_bytes", "peak_requested_bytes", "live_allocations", "successful_allocations",
    "successful_reallocations", "allocation_failures", "reallocation_failures",
];
function snapshot(overrides = {}) {
    return {
        schema: 1, scope: "wasm-instance", enabled: true, counters_valid: true, precision_lost: false,
        live_requested_bytes: 1024, peak_requested_bytes: 1024 * 1024, live_allocations: 3,
        successful_allocations: 9, successful_reallocations: 4, allocation_failures: 1,
        reallocation_failures: 2, linear_memory_bytes: 4 * 1024 * 1024, ...overrides,
    };
}
function format(value) { return formatWasmMemoryDebug({ wasm_memory_snapshot() { return value; } }); }
function invalid(lines) {
    assert.match(lines[0], /^WASM memory: +invalid \(/);
    assert(!lines.some(line => line.startsWith("WASM allocations:")));
    assert(!lines.some(line => line.startsWith("WASM failures:")));
    assert(!lines.some(line => line.includes("requested live")));
}
function limitations(lines) {
    assert(lines.some(line => /this instance only.*not RSS.*not a quota/.test(line)));
    assert(lines.some(line => /excludes stack\/static, JS, other workers\/instances, allocator overhead and realloc overlap/.test(line)));
}

const TREE_SUMMARY_FIELDS = ["chunks", "map_capacity", "payload_bytes", "buffer_capacity_bytes", "counters_valid"];
const TREE_FIELDS = ["schema", "scope", "resident", "replacement", "retiring", "other_private_jobs_unmeasured"];
function chunkSummary(overrides = {}) {
    return { chunks: 3, map_capacity: 7, payload_bytes: 1024,
        buffer_capacity_bytes: 2048, counters_valid: true, ...overrides };
}
function treeSnapshot(overrides = {}) {
    return { schema: 1, scope: "tree-chunk-stores", resident: chunkSummary(),
        replacement: chunkSummary({ chunks: 2, map_capacity: 4, payload_bytes: 512,
            buffer_capacity_bytes: 768 }),
        retiring: chunkSummary({ chunks: 1, map_capacity: 1, payload_bytes: 64,
            buffer_capacity_bytes: 64 }), other_private_jobs_unmeasured: true, ...overrides };
}
function formatTree(value) {
    return formatTreeChunkMemoryDebug({ chunk_memory_snapshot() { return value; } });
}
function invalidTree(lines) {
    assert.match(lines[0], /^Tree chunks: +invalid \(/);
    assert(!lines.some(line => /^Tree chunks (resident|private replacement|retiring):/.test(line)));
    assert(!lines.join("\n").includes("private/vault/🧪"));
}
function treeLimitations(lines) {
    assert(lines.some(line => /root\/candidate shared store counted once.*node buffers summed across physically owned chunks/.test(line)));
    assert(lines.some(line => /map cap is logical capacity, not bytes\/allocated buckets.*excludes metadata\/sort\/root strings.*not total heap\/RSS\/admission/.test(line)));
    assert(lines.some(line => /already included in WASM instance memory; do not add/.test(line)));
}

test("actual formatter calls the snapshot once with its receiver and emits exact path-free counters", () => {
    let calls = 0, gets = 0;
    const owner = {
        get wasm_memory_snapshot() {
            gets++;
            return function () {
                assert.equal(this, owner); assert.equal(arguments.length, 0); calls++;
                return snapshot();
            };
        },
    };
    const lines = formatWasmMemoryDebug(owner);
    assert.equal(gets, 1); assert.equal(calls, 1);
    assert.deepEqual(lines.slice(0, 4), [
        "WASM memory:       requested live 1.00 KiB (1024 B) · peak 1.00 MiB (1048576 B)",
        "WASM allocations:  3 live · 9 alloc/zeroed successes · 4 realloc successes",
        "WASM failures:     1 alloc/zeroed · 2 realloc",
        "WASM linear:       4.00 MiB (4194304 B) · not RSS",
    ]);
    limitations(lines);
});

test("absent module/export stays unsupported without fabricated zero values", () => {
    for (const owner of [undefined, null, {}, { wasm_memory_snapshot: undefined }]) {
        assert.deepEqual(formatWasmMemoryDebug(owner), ["WASM memory:       unsupported (snapshot export unavailable)"]);
    }
    for (const method of [null, false, 0, "snapshot", {}]) invalid(formatWasmMemoryDebug({ wasm_memory_snapshot: method }));
});

test("native disabled zeros are reported as disabled, not a measured empty heap", () => {
    const raw = snapshot({ enabled: false, linear_memory_bytes: null });
    for (const key of COUNTERS) raw[key] = 0;
    const lines = format(raw);
    assert.equal(lines[0], "WASM memory:       disabled (allocator telemetry unavailable)");
    assert(!lines.some(line => /0 B|requested live|WASM allocations:/.test(line)));
    limitations(lines);
    for (const overrides of [{ counters_valid: false }, { precision_lost: true }, { live_allocations: 1 }, { linear_memory_bytes: 0 }]) {
        invalid(format({ ...raw, ...overrides }));
    }
});

test("native-invalid counters are suppressed while valid linear size survives", () => {
    const lines = format(snapshot({ counters_valid: false }));
    invalid(lines); assert.match(lines[0], /native counters marked invalid/);
    assert(lines.some(line => line.includes("4194304 B")));
    limitations(lines);
});

test("overflow nulls and precision loss never masquerade as zero or exact counts", () => {
    const lines = format(snapshot({ counters_valid: false, precision_lost: true, live_requested_bytes: null }));
    invalid(lines); assert.match(lines[0], /precision lost/);
    assert(lines.some(line => line.includes("4194304 B")));
    invalid(format(snapshot({ precision_lost: true })));
    for (const key of COUNTERS) invalid(format(snapshot({ [key]: null })));
    invalid(format(snapshot({ counters_valid: false, precision_lost: true, linear_memory_bytes: null })));
});

test("every counter rejects unsafe integers and wrong types without formatting arbitrary values", () => {
    const badValues = [-1, 0.5, NaN, Infinity, -Infinity, Number.MAX_SAFE_INTEGER + 1,
        "private-value", true, 1n, undefined, {}, []];
    for (const key of COUNTERS) {
        for (const value of badValues) {
            const lines = format(snapshot({ [key]: value }));
            invalid(lines);
            assert(lines.some(line => line.includes("4194304 B")), `safe linear size lost for ${key}`);
            assert.doesNotMatch(lines.join("\n"), /private-value|NaN|Infinity|undefined|\[object/);
        }
    }
    for (const value of badValues) {
        const lines = format(snapshot({ linear_memory_bytes: value }));
        invalid(lines); assert(lines.some(line => line.startsWith("WASM linear:") && line.includes("unavailable")));
    }
});

test("schema, scope, required fields and flags are validated, never inferred", () => {
    for (const raw of [undefined, null, 1, false, "private-value", [], Promise.resolve(snapshot()),
        snapshot({ schema: 2 }), snapshot({ schema: "1" }), snapshot({ scope: "private-value" })]) {
        invalid(format(raw));
    }
    for (const key of ["schema", "scope", "enabled", "counters_valid", "precision_lost", ...COUNTERS, "linear_memory_bytes"]) {
        const raw = snapshot(); delete raw[key]; invalid(format(raw));
    }
    for (const key of ["enabled", "counters_valid", "precision_lost"]) {
        for (const value of [0, 1, "true", null, undefined]) invalid(format(snapshot({ [key]: value })));
    }
    invalid(format(Object.create(snapshot())));
});

test("export/access exceptions are isolated without revealing error payloads", () => {
    const secret = "private/vault/credential";
    for (const owner of [
        { wasm_memory_snapshot() { throw new Error(secret); } },
        { get wasm_memory_snapshot() { throw secret; } },
        new Proxy({}, { get() { throw new Error(secret); } }),
    ]) {
        const lines = formatWasmMemoryDebug(owner);
        invalid(lines); assert.match(lines[0], /snapshot export failed/);
        assert(!lines.join("\n").includes(secret));
    }
});

test("snapshot accessors are not executed and reflection failures retain a previously checked linear size", () => {
    let accesses = 0;
    const raw = snapshot();
    Object.defineProperty(raw, "live_allocations", { get() { accesses++; throw new Error("private getter"); } });
    const lines = format(raw);
    invalid(lines); assert.equal(accesses, 0); assert(lines.some(line => line.includes("4194304 B")));
    const proxy = new Proxy(snapshot(), { getOwnPropertyDescriptor(target, key) {
        if (key === "live_allocations") throw new Error("private reflection");
        return Object.getOwnPropertyDescriptor(target, key);
    } });
    const failed = format(proxy);
    invalid(failed); assert(failed.some(line => line.includes("4194304 B")));
    assert.doesNotMatch([...lines, ...failed].join("\n"), /private getter|private reflection/);
});

test("safe integer endpoints and real zero samples remain exact; a contradictory peak does not", () => {
    const zero = snapshot({ linear_memory_bytes: 0 });
    for (const key of COUNTERS) zero[key] = 0;
    assert.equal(format(zero)[0], "WASM memory:       requested live 0 B · peak 0 B");
    const large = snapshot({ linear_memory_bytes: Number.MAX_SAFE_INTEGER });
    for (const key of COUNTERS) large[key] = Number.MAX_SAFE_INTEGER;
    const lines = format(large);
    assert.match(lines[0], /9007199254740991 B/);
    assert.match(lines[1], /9007199254740991 live/);
    assert.match(lines[3], /9007199254740991 B/);
    invalid(format(snapshot({ peak_requested_bytes: 1023 })));
});

test("missing linear observation is explicit and never derived from requested bytes", () => {
    const lines = format(snapshot({ linear_memory_bytes: null }));
    assert.match(lines[0], /requested live/);
    assert.equal(lines[3], "WASM linear:       unavailable · not RSS");
});

test("formatter does not mutate or retain observations and ignores unknown payload fields", () => {
    let extraReads = 0, calls = 0;
    const first = snapshot();
    Object.defineProperty(first, "private", { get() { extraReads++; throw new Error("secret"); } });
    Object.freeze(first);
    const owner = { wasm_memory_snapshot() { return calls++ === 0 ? first : snapshot({ live_requested_bytes: 2 }); } };
    const before = formatWasmMemoryDebug(owner);
    before[0] = "caller-owned result";
    const after = formatWasmMemoryDebug(owner);
    assert.match(after[0], /requested live 2 B/); assert.equal(first.live_requested_bytes, 1024);
    assert.equal(extraReads, 0); assert.equal(calls, 2);
    assert(format(snapshot({ unexpected: "private-value".repeat(100_000) })).join("\n").length < 1024);
});

test("tree chunk formatter reports exact resident, replacement and retiring ownership without summing heaps", () => {
    let calls = 0, gets = 0;
    const owner = {
        get chunk_memory_snapshot() {
            gets++;
            return function () {
                assert.equal(this, owner); assert.equal(arguments.length, 0); calls++;
                return treeSnapshot();
            };
        },
    };
    const lines = formatTreeChunkMemoryDebug(owner);
    assert.equal(gets, 1); assert.equal(calls, 1);
    assert.deepEqual(lines.slice(0, 3), [
        "Tree chunks resident: 3 chunks · map cap 7 slots · payload 1.00 KiB (1024 B) · buffer cap 2.00 KiB (2048 B)",
        "Tree chunks private replacement: 2 chunks · map cap 4 slots · payload 512 B · buffer cap 768 B",
        "Tree chunks retiring: 1 chunk · map cap 1 slot · payload 64 B · buffer cap 64 B",
    ]);
    treeLimitations(lines);
    assert(lines.some(line => /other private jobs unmeasured: yes/.test(line)));
    assert(!lines.some(line => /total.*(3648|3\.56 KiB)/.test(line)), "formatter summed nested counters");
});

test("tree chunk formatter distinguishes missing, malformed and throwing exports with fixed path-free errors", () => {
    for (const owner of [undefined, null, {}, { chunk_memory_snapshot: undefined }]) {
        assert.deepEqual(formatTreeChunkMemoryDebug(owner), ["Tree chunks:        unsupported (snapshot export unavailable)"]);
    }
    for (const method of [null, false, 0, "private/vault/🧪", {}]) {
        invalidTree(formatTreeChunkMemoryDebug({ chunk_memory_snapshot: method }));
    }
    const secret = "private/vault/🧪/0xdeadbeef";
    for (const owner of [
        { chunk_memory_snapshot() { throw new Error(secret); } },
        { get chunk_memory_snapshot() { throw secret; } },
        new Proxy({}, { get() { throw new Error(secret); } }),
    ]) invalidTree(formatTreeChunkMemoryDebug(owner));
});

test("tree chunk schema, scope, own fields and private-job flag are strict", () => {
    for (const raw of [undefined, null, false, 1, "private/vault/🧪", [], Promise.resolve(treeSnapshot()),
        treeSnapshot({ schema: 2 }), treeSnapshot({ schema: "1" }),
        treeSnapshot({ scope: "tree-chunk-stores@0xdeadbeef/private/vault/🧪" }),
        treeSnapshot({ pointer: "0xdeadbeef" })]) invalidTree(formatTree(raw));
    for (const key of TREE_FIELDS) {
        const raw = treeSnapshot(); delete raw[key]; invalidTree(formatTree(raw));
    }
    for (const value of [0, 1, "true", null, undefined, {}, []]) {
        invalidTree(formatTree(treeSnapshot({ other_private_jobs_unmeasured: value })));
    }
    invalidTree(formatTree(Object.create(treeSnapshot())));
    const accessor = treeSnapshot(); let reads = 0;
    Object.defineProperty(accessor, "scope", { get() { reads++; return "tree-chunk-stores"; } });
    invalidTree(formatTree(accessor)); assert.equal(reads, 0, "tree report getter was executed");
    const reflected = new Proxy(treeSnapshot(), { ownKeys() { throw new Error("private/vault/🧪"); } });
    invalidTree(formatTree(reflected));
});

test("every tree chunk counter is an exact safe nonnegative own data value", () => {
    const badValues = [-1, 0.5, NaN, Infinity, -Infinity, Number.MAX_SAFE_INTEGER + 1,
        "private/vault/🧪", true, 1n, null, undefined, {}, []];
    for (const section of ["resident", "replacement", "retiring"]) {
        for (const key of TREE_SUMMARY_FIELDS.slice(0, 4)) {
            for (const value of badValues) {
                invalidTree(formatTree(treeSnapshot({ [section]: chunkSummary({ [key]: value }) })));
            }
        }
        for (const value of [0, 1, "true", null, undefined]) {
            invalidTree(formatTree(treeSnapshot({ [section]: chunkSummary({ counters_valid: value }) })));
        }
        invalidTree(formatTree(treeSnapshot({ [section]: chunkSummary({ pointer: "private/vault/🧪" }) })));
        const accessor = chunkSummary(); let reads = 0;
        Object.defineProperty(accessor, "chunks", { get() { reads++; return 3; } });
        invalidTree(formatTree(treeSnapshot({ [section]: accessor })));
        assert.equal(reads, 0, "tree summary getter was executed");
    }
});

test("tree chunk invariants and invalid native counters fail closed without losing scope limits", () => {
    for (const section of ["resident", "replacement", "retiring"]) {
        for (const summary of [
            chunkSummary({ chunks: 8, map_capacity: 7 }),
            chunkSummary({ payload_bytes: 2049, buffer_capacity_bytes: 2048 }),
        ]) invalidTree(formatTree(treeSnapshot({ [section]: summary })));
        const lines = formatTree(treeSnapshot({ [section]: chunkSummary({ counters_valid: false }) }));
        invalidTree(lines); assert.match(lines[0], /native counters marked invalid/);
        assert(lines.some(line => /not total heap\/RSS\/admission/.test(line)));
    }
});

test("tree chunk zero and maximum safe counters remain exact and private-job uncertainty is explicit", () => {
    const zero = chunkSummary({ chunks: 0, map_capacity: 0, payload_bytes: 0, buffer_capacity_bytes: 0 });
    const zeroLines = formatTree(treeSnapshot({ resident: zero, replacement: zero, retiring: zero,
        other_private_jobs_unmeasured: false }));
    assert.match(zeroLines[0], /0 chunks.*map cap 0 slots.*payload 0 B.*buffer cap 0 B/);
    assert(zeroLines.some(line => /other private jobs unmeasured: no/.test(line)));
    treeLimitations(zeroLines);
    const maximum = chunkSummary({ chunks: Number.MAX_SAFE_INTEGER, map_capacity: Number.MAX_SAFE_INTEGER,
        payload_bytes: Number.MAX_SAFE_INTEGER, buffer_capacity_bytes: Number.MAX_SAFE_INTEGER });
    const maxLines = formatTree(treeSnapshot({ resident: maximum }));
    assert(maxLines[0].includes("9007199254740991 chunks"));
    assert(maxLines[0].includes("9007199254740991 B"));
});

function stringMetadata(overrides = {}) {
    return { strings: 2, length_bytes: 12, capacity_bytes: 32, counters_valid: true, ...overrides };
}
function rootMetadata(overrides = {}) {
    return { roots: 1, strings: stringMetadata({ strings: 3, length_bytes: 20, capacity_bytes: 64 }),
        v1_children_length: 1, v1_children_capacity: 4, v1_children_backing_capacity_bytes: 176,
        counters_valid: true, ...overrides };
}
function metadataOwner(overrides = {}) {
    return { committed: rootMetadata(), candidate: rootMetadata(), tree_ids: stringMetadata(),
        candidate_baseline: { present: true, hashes: 3, capacity_slots: 7, logical_hash_bytes: 96, counters_valid: true },
        counters_valid: true, ...overrides };
}
function metadataSnapshot(overrides = {}) {
    return { schema: 1, scope: "tree-metadata", wrapper_ids: stringMetadata(), resident: metadataOwner(),
        replacement: null, retiring: null, other_private_jobs_unmeasured: true, ...overrides };
}
function formatMetadata(value) {
    return formatTreeMetadataMemoryDebug({ metadata_memory_snapshot() { return value; } });
}
function invalidMetadata(lines) {
    assert.match(lines[0], /^Tree metadata: +invalid \(/);
    assert(!lines.some(line => /^Tree metadata (wrapper IDs:|resident[ :]|private replacement[ :]|retiring[ :])/.test(line)));
    assert(!lines.join("\n").includes("private/vault/🧪"));
    assert(lines.some(line => /not total heap\/RSS\/admission/.test(line)));
}
function metadataAt(value, path) { return path.reduce((current, key) => current[key], value); }
function metadataObjects(value, path = []) {
    const result = [path];
    for (const [key, entry] of Object.entries(value)) {
        if (entry && typeof entry === "object") result.push(...metadataObjects(entry, [...path, key]));
    }
    return result;
}

test("tree metadata calls the actual export once with its receiver and keeps unmeasured owners explicit", () => {
    let calls = 0, reads = 0;
    const owner = { get metadata_memory_snapshot() {
        reads++;
        return function () { assert.equal(this, owner); assert.equal(arguments.length, 0); calls++; return metadataSnapshot(); };
    } };
    const lines = formatTreeMetadataMemoryDebug(owner);
    assert.equal(calls, 1); assert.equal(reads, 1);
    assert.equal(lines[0], "Tree metadata wrapper IDs: 2 strings · length 12 B · capacity 32 B");
    assert.equal(lines[1], "Tree metadata resident committed: 1 roots · 3 strings · length 20 B · capacity 64 B · v1 children 1/4 · vector capacity 176 B");
    assert(lines.includes("Tree metadata private replacement: unmeasured"));
    assert(lines.includes("Tree metadata retiring: unmeasured"));
    assert(lines.includes("Tree metadata resident baseline: present · 3 hashes · 7 capacity slots · logical keys 96 B"));
    assert(lines.some(line => /already included in WASM instance memory; do not add/.test(line)));
    assert(lines.some(line => /logical keys; set capacity is slots, not bucket bytes/.test(line)));
    assert(lines.some(line => /excludes node buffers, input\/sort\/codec\/other private scratch/.test(line)));
    assert(lines.includes("Tree metadata other private jobs unmeasured: yes"));
});

test("tree metadata unsupported, malformed and throwing exports remain path-free", () => {
    for (const owner of [undefined, null, {}, { metadata_memory_snapshot: undefined }]) {
        assert.deepEqual(formatTreeMetadataMemoryDebug(owner), ["Tree metadata:     unsupported (snapshot export unavailable)"]);
    }
    for (const method of [null, false, 1, "private/vault/🧪", {}]) {
        invalidMetadata(formatTreeMetadataMemoryDebug({ metadata_memory_snapshot: method }));
    }
    for (const owner of [
        { metadata_memory_snapshot() { throw new Error("private/vault/🧪"); } },
        { get metadata_memory_snapshot() { throw new Error("private/vault/🧪"); } },
        new Proxy({}, { get() { throw new Error("private/vault/🧪"); } }),
    ]) invalidMetadata(formatTreeMetadataMemoryDebug(owner));
    for (const raw of [undefined, null, [], true, 1, "private/vault/🧪", Promise.resolve(metadataSnapshot()),
        metadataSnapshot({ schema: 2 }), metadataSnapshot({ scope: "tree-metadata/private/vault/🧪" }),
        metadataSnapshot({ wrapper_ids: null }), metadataSnapshot({ resident: null }),
        metadataSnapshot({ other_private_jobs_unmeasured: false }),
        metadataSnapshot({ replacement: metadataOwner(), other_private_jobs_unmeasured: false }),
        metadataSnapshot({ retiring: metadataOwner(), other_private_jobs_unmeasured: false })]) {
        invalidMetadata(formatMetadata(raw));
    }
});

test("every tree metadata level rejects extra, missing, inherited and accessor fields without invoking them", () => {
    const complete = () => metadataSnapshot({ replacement: metadataOwner(), retiring: metadataOwner() });
    const paths = metadataObjects(complete());
    let accessorCalls = 0;
    for (const path of paths) {
        for (const field of Object.keys(metadataAt(complete(), path))) {
            const missing = complete(); delete metadataAt(missing, path)[field]; invalidMetadata(formatMetadata(missing));
            const accessor = complete(); Object.defineProperty(metadataAt(accessor, path), field, {
                enumerable: true, get() { accessorCalls++; throw new Error("private/vault/🧪"); },
            });
            invalidMetadata(formatMetadata(accessor));
        }
        for (const key of ["extra", Symbol("private/vault/🧪")]) {
            const extra = complete(); Object.defineProperty(metadataAt(extra, path), key, { value: "private/vault/🧪" });
            invalidMetadata(formatMetadata(extra));
        }
        const inherited = complete();
        if (!path.length) invalidMetadata(formatMetadata(Object.create(inherited)));
        else {
            const parent = metadataAt(inherited, path.slice(0, -1)), key = path.at(-1);
            parent[key] = Object.create(parent[key]); invalidMetadata(formatMetadata(inherited));
        }
    }
    assert.equal(accessorCalls, 0);
    invalidMetadata(formatMetadata(new Proxy(complete(), { ownKeys() { throw new Error("private/vault/🧪"); } })));
});

test("every tree metadata numeric counter and boolean is strictly validated at every owner", () => {
    const complete = () => metadataSnapshot({ replacement: metadataOwner(), retiring: metadataOwner() });
    const invalidNumbers = [-1, 0.25, NaN, Infinity, -Infinity, Number.MAX_SAFE_INTEGER + 1, 1n,
        "private/vault/🧪", true, null, undefined, {}, []];
    for (const path of metadataObjects(complete())) {
        for (const [key, current] of Object.entries(metadataAt(complete(), path))) {
            if (typeof current !== "number" && typeof current !== "boolean") continue;
            for (const value of typeof current === "number" ? invalidNumbers : [0, 1, "true", null, undefined, {}, []]) {
                const raw = complete(); metadataAt(raw, path)[key] = value; invalidMetadata(formatMetadata(raw));
            }
            if (key === "counters_valid") {
                const raw = complete(); metadataAt(raw, path)[key] = false;
                const lines = formatMetadata(raw); invalidMetadata(lines);
                assert.match(lines[0], /native counters marked invalid/);
            }
        }
    }
});

test("tree metadata consistency checks do not invent native layout or set bucket bytes", () => {
    const failures = [
        metadataSnapshot({ wrapper_ids: stringMetadata({ capacity_bytes: 11 }) }),
        metadataSnapshot({ wrapper_ids: stringMetadata({ strings: 0 }) }),
    ];
    for (const section of ["resident", "replacement", "retiring"]) {
        for (const root of [rootMetadata({ roots: 0 }), rootMetadata({ v1_children_length: 5 }),
            rootMetadata({ v1_children_capacity: 0, v1_children_length: 0 }),
            rootMetadata({ v1_children_backing_capacity_bytes: 3 })]) {
            failures.push(metadataSnapshot({ [section]: metadataOwner({ committed: root }) }));
        }
        for (const baseline of [
            { present: false, hashes: 3, capacity_slots: 7, logical_hash_bytes: 96, counters_valid: true },
            { present: true, hashes: 3, capacity_slots: 2, logical_hash_bytes: 96, counters_valid: true },
            { present: true, hashes: 3, capacity_slots: 7, logical_hash_bytes: 95, counters_valid: true },
            { present: true, hashes: Number.MAX_SAFE_INTEGER, capacity_slots: Number.MAX_SAFE_INTEGER,
                logical_hash_bytes: Number.MAX_SAFE_INTEGER, counters_valid: true },
        ]) failures.push(metadataSnapshot({ [section]: metadataOwner({ candidate_baseline: baseline }) }));
    }
    for (const value of failures) invalidMetadata(formatMetadata(value));
});

test("tree metadata distinguishes absent roots from retained empty backing and accepts safe exact maxima", () => {
    const emptyStrings = stringMetadata({ strings: 0, length_bytes: 0, capacity_bytes: 0 });
    const absentRoot = rootMetadata({ roots: 0, strings: emptyStrings, v1_children_length: 0,
        v1_children_capacity: 0, v1_children_backing_capacity_bytes: 0 });
    const emptyOwner = metadataOwner({ committed: absentRoot, candidate: absentRoot, tree_ids: emptyStrings,
        candidate_baseline: { present: false, hashes: 0, capacity_slots: 0, logical_hash_bytes: 0, counters_valid: true } });
    const zero = formatMetadata(metadataSnapshot({ wrapper_ids: emptyStrings, resident: emptyOwner,
        replacement: emptyOwner, retiring: emptyOwner, other_private_jobs_unmeasured: false }));
    assert(!zero[0].includes("invalid")); assert(!zero.some(line => /: unmeasured$/.test(line)));
    assert(zero.includes("Tree metadata retiring baseline: absent · 0 hashes · 0 capacity slots · logical keys 0 B"));
    const retained = formatMetadata(metadataSnapshot({ resident: metadataOwner({
        committed: rootMetadata({ strings: stringMetadata({ length_bytes: 0 }), v1_children_length: 0 }),
        candidate_baseline: { present: true, hashes: 0, capacity_slots: 7, logical_hash_bytes: 0, counters_valid: true },
    }) }));
    assert(retained[1].includes("length 0 B · capacity 32 B · v1 children 0/4 · vector capacity 176 B"));
    assert(retained.some(line => /baseline: present · 0 hashes · 7 capacity slots/.test(line)));
    const max = Number.MAX_SAFE_INTEGER;
    const maximum = formatMetadata(metadataSnapshot({ wrapper_ids: stringMetadata({ strings: max, length_bytes: max, capacity_bytes: max }) }));
    assert(maximum[0].includes(`${max} strings`)); assert(maximum[0].includes(`${max} B`));
    // V2 endpoints are strings in an inline RangeRef, not a v1 children Vec.
    const v2 = formatMetadata(metadataSnapshot({ resident: metadataOwner({ committed: rootMetadata({
        strings: stringMetadata({ strings: 4 }), v1_children_length: 0, v1_children_capacity: 0,
        v1_children_backing_capacity_bytes: 0,
    }) }) }));
    assert(v2[1].includes("4 strings")); assert(v2[1].includes("v1 children 0/0 · vector capacity 0 B"));
});

function vecMemory(overrides = {}) {
    return { owners: 1, length_slots: 3, capacity_slots: 8, slot_size_bytes: 48,
        backing_capacity_bytes: 384, counters_valid: true, ...overrides };
}
function emptyVecMemory() {
    return vecMemory({ owners: 0, length_slots: 0, capacity_slots: 0, slot_size_bytes: 0,
        backing_capacity_bytes: 0 });
}
function inputMemory(overrides = {}) {
    return { entries: vecMemory(), paths: stringMetadata({ strings: 3, length_bytes: 12, capacity_bytes: 32 }),
        counters_valid: true, ...overrides };
}
function emptyInputMemory() {
    return inputMemory({ entries: emptyVecMemory(),
        paths: stringMetadata({ strings: 0, length_bytes: 0, capacity_bytes: 0 }) });
}
function inputSnapshot(overrides = {}) {
    return { schema: 1, scope: "replacement-input", replacement: inputMemory(), retiring: emptyInputMemory(),
        other_input_owners_unmeasured: false, ...overrides };
}
function formatInput(value) {
    return formatReplacementInputMemoryDebug({ replacement_input_memory_snapshot() { return value; } });
}
function invalidInput(lines) {
    assert.match(lines[0], /^Replacement input: +invalid \(/);
    assert(!lines.join("\n").includes("private/vault/🧪"));
    assert(lines.some(line => /excludes page\/serde bridge/.test(line)));
}

test("replacement input reports exact Vec and path components without summing allocator memory", () => {
    let calls = 0;
    const owner = { replacement_input_memory_snapshot() { assert.equal(this, owner); calls++; return inputSnapshot(); } };
    const lines = formatReplacementInputMemoryDebug(owner);
    assert.equal(calls, 1);
    assert.equal(lines[0], "Replacement input private: 3/8 entries · slot 48 B · Vec backing 384 B · paths 3 strings · length 12 B · capacity 32 B");
    assert.equal(lines[1], "Replacement input retiring: 0/0 entries · slot 0 B · Vec backing 0 B · paths 0 strings · length 0 B · capacity 0 B");
    assert(lines.some(line => /already included in WASM instance memory; do not add/.test(line)));
    assert(lines.includes("Replacement input other owners unmeasured: no"));
});

test("replacement input nullable coverage and malformed exports fail closed path-free", () => {
    for (const owner of [undefined, null, {}, { replacement_input_memory_snapshot: undefined }]) {
        assert.deepEqual(formatReplacementInputMemoryDebug(owner),
            ["Replacement input:  unsupported (snapshot export unavailable)"]);
    }
    for (const raw of [undefined, null, [], true, 1, "private/vault/🧪",
        inputSnapshot({ schema: 2 }), inputSnapshot({ scope: "private/vault/🧪" }),
        inputSnapshot({ replacement: null }), inputSnapshot({ retiring: null }),
        inputSnapshot({ other_input_owners_unmeasured: "true" })]) invalidInput(formatInput(raw));
    const unknown = formatInput(inputSnapshot({ replacement: null, retiring: null,
        other_input_owners_unmeasured: true }));
    assert(unknown.includes("Replacement input private: unmeasured"));
    assert(unknown.includes("Replacement input retiring: unmeasured"));
    for (const method of [null, false, 1, "private/vault/🧪", {}]) {
        invalidInput(formatReplacementInputMemoryDebug({ replacement_input_memory_snapshot: method }));
    }
    invalidInput(formatReplacementInputMemoryDebug({ replacement_input_memory_snapshot() {
        throw new Error("private/vault/🧪");
    } }));
});

test("replacement input exact fields, numeric relations and native validity are strict", () => {
    const failures = [
        inputSnapshot({ replacement: inputMemory({ entries: vecMemory({ length_slots: 9 }) }) }),
        inputSnapshot({ replacement: inputMemory({ entries: vecMemory({ backing_capacity_bytes: 383 }) }) }),
        inputSnapshot({ replacement: inputMemory({ entries: vecMemory({ owners: 0 }) }) }),
        inputSnapshot({ replacement: inputMemory({ entries: vecMemory({ slot_size_bytes: 0 }) }) }),
        inputSnapshot({ replacement: inputMemory({ paths: stringMetadata({ strings: 2 }) }) }),
    ];
    for (const raw of failures) invalidInput(formatInput(raw));
    for (const path of metadataObjects(inputSnapshot({ replacement: inputMemory(), retiring: inputMemory() }))) {
        const current = metadataAt(inputSnapshot({ replacement: inputMemory(), retiring: inputMemory() }), path);
        for (const field of Object.keys(current)) {
            const missing = inputSnapshot({ replacement: inputMemory(), retiring: inputMemory() });
            delete metadataAt(missing, path)[field]; invalidInput(formatInput(missing));
            const extra = inputSnapshot({ replacement: inputMemory(), retiring: inputMemory() });
            metadataAt(extra, path).extra = "private/vault/🧪"; invalidInput(formatInput(extra));
        }
    }
    const complete = () => inputSnapshot({ replacement: inputMemory(), retiring: inputMemory() });
    const invalidNumbers = [-1, 0.25, NaN, Infinity, -Infinity, Number.MAX_SAFE_INTEGER + 1, 1n,
        "private/vault/🧪", true, null, undefined, {}, []];
    for (const path of metadataObjects(complete())) {
        for (const [key, current] of Object.entries(metadataAt(complete(), path))) {
            if (typeof current !== "number" && typeof current !== "boolean") continue;
            const badValues = typeof current === "number"
                ? invalidNumbers : [0, 1, "true", null, undefined, {}, []];
            for (const value of badValues) {
                const raw = complete(); metadataAt(raw, path)[key] = value; invalidInput(formatInput(raw));
            }
            if (key === "counters_valid") {
                const raw = complete(); metadataAt(raw, path)[key] = false;
                const lines = formatInput(raw); invalidInput(lines);
                assert.match(lines[0], /native counters marked invalid/);
            }
        }
    }
});

function v1EntriesMemory(overrides = {}) {
    return {
        input: vecMemory({ length_slots: 2, capacity_slots: 8 }),
        groups: vecMemory({ owners: 3, length_slots: 4, capacity_slots: 12,
            backing_capacity_bytes: 576 }),
        sorting_rows: 5,
        rows: vecMemory({ length_slots: 1, capacity_slots: 4, backing_capacity_bytes: 192 }),
        leaf: vecMemory({ length_slots: 2, capacity_slots: 4, backing_capacity_bytes: 192 }),
        retiring_rows: vecMemory({ length_slots: 1, capacity_slots: 2, backing_capacity_bytes: 96 }),
        paths: stringMetadata({ strings: 15, length_bytes: 120, capacity_bytes: 240 }),
        counters_valid: true,
        ...overrides,
    };
}
function emptyV1EntriesMemory() {
    return v1EntriesMemory({ input: emptyVecMemory(), groups: emptyVecMemory(), sorting_rows: 0,
        rows: emptyVecMemory(), leaf: emptyVecMemory(), retiring_rows: emptyVecMemory(),
        paths: stringMetadata({ strings: 0, length_bytes: 0, capacity_bytes: 0 }) });
}
function v1EntriesSnapshot(overrides = {}) {
    return { schema: 1, scope: "replacement-v1-entries", replacement: v1EntriesMemory(),
        retiring: emptyV1EntriesMemory(), other_entry_owners_unmeasured: false, ...overrides };
}
function formatV1Entries(value) {
    return formatReplacementV1EntriesMemoryDebug({
        replacement_v1_entries_memory_snapshot() { return value; },
    });
}
function invalidV1Entries(lines) {
    assert.match(lines[0], /^Replacement V1 entries: invalid \(/);
    assert(!lines.join("\n").includes("private/vault/🧪"));
    assert(lines.some(line => /excludes group keys\/map nodes/.test(line)));
}

test("replacement V1 entries reports disjoint redistribution owners and aggregate group backing", () => {
    let calls = 0;
    const owner = { replacement_v1_entries_memory_snapshot() {
        assert.equal(this, owner); calls++; return v1EntriesSnapshot();
    } };
    const lines = formatReplacementV1EntriesMemoryDebug(owner);
    assert.equal(calls, 1);
    assert.equal(lines[0], "Replacement V1 entries private: input 2/8 (384 B)"
        + " · groups 3 owners 4/12 (576 B) · sorting 5 · rows 1/4 (192 B)"
        + " · leaf 2/4 (192 B) · encoded 1/2 (96 B)"
        + " · paths 15 strings · length 120 B · capacity 240 B");
    assert(lines[1].includes("input 0/0 (0 B) · groups 0 owners 0/0 (0 B)"));
    assert(lines.some(line => /sorter backing stays in replacement-sort/.test(line)));
    assert(lines.includes("Replacement V1 entries other owners unmeasured: no"));
});

test("replacement V1 entries schema, conservation, aggregate and native validity fail closed", () => {
    for (const owner of [undefined, null, {},
        { replacement_v1_entries_memory_snapshot: undefined }]) {
        assert.deepEqual(formatReplacementV1EntriesMemoryDebug(owner),
            ["Replacement V1 entries: unsupported (snapshot export unavailable)"]);
    }
    for (const raw of [undefined, null, [], true, "private/vault/🧪",
        v1EntriesSnapshot({ schema: 2 }), v1EntriesSnapshot({ scope: "private/vault/🧪" }),
        v1EntriesSnapshot({ replacement: null }),
        v1EntriesSnapshot({ other_entry_owners_unmeasured: "false" }),
        v1EntriesSnapshot({ replacement: v1EntriesMemory({ sorting_rows: 6 }) }),
        v1EntriesSnapshot({ replacement: v1EntriesMemory({ input: vecMemory({ owners: 2 }) }) }),
        v1EntriesSnapshot({ replacement: v1EntriesMemory({ groups: vecMemory({ owners: 3,
            length_slots: 4, capacity_slots: 12, backing_capacity_bytes: 575 }) }) }),
        v1EntriesSnapshot({ replacement: v1EntriesMemory({ groups: vecMemory({ owners: 0,
            length_slots: 0, capacity_slots: 12, backing_capacity_bytes: 576 }) }) }),
        v1EntriesSnapshot({ replacement: v1EntriesMemory({ groups: vecMemory({ owners: 13,
            length_slots: 4, capacity_slots: 12, backing_capacity_bytes: 576 }) }) }),
        v1EntriesSnapshot({ replacement: v1EntriesMemory({ rows: vecMemory({ length_slots: 1,
            capacity_slots: 4, slot_size_bytes: 40, backing_capacity_bytes: 160 }) }) }),
        v1EntriesSnapshot({ replacement: v1EntriesMemory({ counters_valid: false }) }),
    ]) invalidV1Entries(formatV1Entries(raw));
    const nullable = formatV1Entries(v1EntriesSnapshot({ replacement: null, retiring: null,
        other_entry_owners_unmeasured: true }));
    assert(nullable.includes("Replacement V1 entries private: unmeasured"));
    for (const method of [null, false, 1, "private/vault/🧪", {}]) {
        invalidV1Entries(formatReplacementV1EntriesMemoryDebug({
            replacement_v1_entries_memory_snapshot: method,
        }));
    }
    invalidV1Entries(formatReplacementV1EntriesMemoryDebug({
        replacement_v1_entries_memory_snapshot() { throw new Error("private/vault/🧪"); },
    }));
});

test("replacement V1 entries requires exact own fields and safe counters", () => {
    const complete = () => v1EntriesSnapshot({ retiring: v1EntriesMemory() });
    for (const path of metadataObjects(complete())) {
        const current = metadataAt(complete(), path);
        for (const field of Object.keys(current)) {
            const missing = complete(); delete metadataAt(missing, path)[field];
            invalidV1Entries(formatV1Entries(missing));
            const extra = complete(); metadataAt(extra, path).extra = "private/vault/🧪";
            invalidV1Entries(formatV1Entries(extra));
        }
    }
    const invalidNumbers = [-1, 0.25, NaN, Infinity, -Infinity,
        Number.MAX_SAFE_INTEGER + 1, 1n, "private/vault/🧪", true, null, undefined, {}, []];
    for (const field of ["sorting_rows"]) {
        for (const value of invalidNumbers) {
            const raw = complete(); raw.replacement[field] = value;
            invalidV1Entries(formatV1Entries(raw));
        }
    }
});

function v1GraphMemory(overrides = {}) {
    return {
        group_keys: stringMetadata({ strings: 3, length_bytes: 9, capacity_bytes: 15 }),
        prefix: stringMetadata({ strings: 1, length_bytes: 2, capacity_bytes: 4 }),
        leaf_hashes: vecMemory({ length_slots: 2, capacity_slots: 4, slot_size_bytes: 32,
            backing_capacity_bytes: 128 }),
        hashes: emptyVecMemory(),
        children: vecMemory({ length_slots: 4, capacity_slots: 8, slot_size_bytes: 40,
            backing_capacity_bytes: 320 }),
        sorting_children: 0,
        retiring_children: emptyVecMemory(),
        child_labels: stringMetadata({ strings: 4, length_bytes: 20, capacity_bytes: 40 }),
        root_children: vecMemory({ length_slots: 2, capacity_slots: 4, slot_size_bytes: 40,
            backing_capacity_bytes: 160 }),
        root_child_labels: stringMetadata({ strings: 2, length_bytes: 5, capacity_bytes: 11 }),
        identities: stringMetadata({ strings: 2, length_bytes: 11, capacity_bytes: 16 }),
        counters_valid: true,
        ...overrides,
    };
}
function emptyV1GraphMemory() {
    const strings = () => stringMetadata({ strings: 0, length_bytes: 0, capacity_bytes: 0 });
    return v1GraphMemory({ group_keys: strings(), prefix: strings(), leaf_hashes: emptyVecMemory(),
        hashes: emptyVecMemory(), children: emptyVecMemory(), sorting_children: 0,
        retiring_children: emptyVecMemory(), child_labels: strings(), root_children: emptyVecMemory(),
        root_child_labels: strings(), identities: strings() });
}
function v1GraphSnapshot(overrides = {}) {
    return { schema: 1, scope: "replacement-v1-graph", replacement: v1GraphMemory(),
        retiring: emptyV1GraphMemory(), other_graph_owners_unmeasured: false, ...overrides };
}
function formatV1Graph(value) {
    return formatReplacementV1GraphMemoryDebug({
        replacement_v1_graph_memory_snapshot() { return value; },
    });
}
function invalidV1Graph(lines) {
    assert.match(lines[0], /^Replacement V1 graph: invalid \(/);
    assert(!lines.join("\n").includes("private/vault/🧪"));
    assert(lines.some(line => /excludes BTree\/HashSet buckets/.test(line)));
}

test("replacement V1 graph reports exact disjoint assembly Vec and String owners", () => {
    let calls = 0;
    const owner = { replacement_v1_graph_memory_snapshot() {
        assert.equal(this, owner); calls++; return v1GraphSnapshot();
    } };
    const lines = formatReplacementV1GraphMemoryDebug(owner);
    assert.equal(calls, 1);
    assert.equal(lines[0], "Replacement V1 graph private: groups 3 · hashes 2 · children 4"
        + " · root children 2 · Vec backing 608 B · String capacity 86 B");
    assert(lines[1].includes("groups 0 · hashes 0 · children 0 · root children 0"));
    assert(lines.some(line => /provisional root enters metadata/.test(line)));
    assert(lines.some(line => /sorter backing is separate/.test(line)));
    assert(lines.includes("Replacement V1 graph other owners unmeasured: no"));
});

test("replacement V1 graph schema and conservation relations fail closed path-free", () => {
    for (const owner of [undefined, null, {},
        { replacement_v1_graph_memory_snapshot: undefined }]) {
        assert.deepEqual(formatReplacementV1GraphMemoryDebug(owner),
            ["Replacement V1 graph: unsupported (snapshot export unavailable)"]);
    }
    const failures = [undefined, null, [], true, "private/vault/🧪",
        v1GraphSnapshot({ schema: 2 }), v1GraphSnapshot({ scope: "private/vault/🧪" }),
        v1GraphSnapshot({ replacement: null }),
        v1GraphSnapshot({ other_graph_owners_unmeasured: "false" }),
        v1GraphSnapshot({ replacement: v1GraphMemory({ prefix: stringMetadata({ strings: 2 }) }) }),
        v1GraphSnapshot({ replacement: v1GraphMemory({ identities: stringMetadata({ strings: 3 }) }) }),
        v1GraphSnapshot({ replacement: v1GraphMemory({ sorting_children: 1 }) }),
        v1GraphSnapshot({ replacement: v1GraphMemory({ root_child_labels:
            stringMetadata({ strings: 1 }) }) }),
        v1GraphSnapshot({ replacement: v1GraphMemory({ hashes: vecMemory({ length_slots: 1,
            capacity_slots: 4, slot_size_bytes: 16, backing_capacity_bytes: 64 }) }) }),
        v1GraphSnapshot({ replacement: v1GraphMemory({ root_children: vecMemory({
            length_slots: 2, capacity_slots: 4, slot_size_bytes: 48,
            backing_capacity_bytes: 192 }) }) }),
        v1GraphSnapshot({ replacement: v1GraphMemory({ counters_valid: false }) }),
    ];
    for (const raw of failures) invalidV1Graph(formatV1Graph(raw));
    const nullable = formatV1Graph(v1GraphSnapshot({ replacement: null, retiring: null,
        other_graph_owners_unmeasured: true }));
    assert(nullable.includes("Replacement V1 graph private: unmeasured"));
    for (const method of [null, false, 1, "private/vault/🧪", {}]) {
        invalidV1Graph(formatReplacementV1GraphMemoryDebug({
            replacement_v1_graph_memory_snapshot: method,
        }));
    }
    invalidV1Graph(formatReplacementV1GraphMemoryDebug({
        replacement_v1_graph_memory_snapshot() { throw new Error("private/vault/🧪"); },
    }));
});

test("replacement V1 graph requires exact own fields and safe counters", () => {
    const complete = () => v1GraphSnapshot({ retiring: v1GraphMemory() });
    for (const path of metadataObjects(complete())) {
        const current = metadataAt(complete(), path);
        for (const field of Object.keys(current)) {
            const missing = complete(); delete metadataAt(missing, path)[field];
            invalidV1Graph(formatV1Graph(missing));
            const extra = complete(); metadataAt(extra, path).extra = "private/vault/🧪";
            invalidV1Graph(formatV1Graph(extra));
        }
    }
    const invalidNumbers = [-1, 0.25, NaN, Infinity, -Infinity,
        Number.MAX_SAFE_INTEGER + 1, 1n, "private/vault/🧪", true, null, undefined, {}, []];
    for (const value of invalidNumbers) {
        const raw = complete(); raw.replacement.sorting_children = value;
        invalidV1Graph(formatV1Graph(raw));
    }
});

function postSortSnapshot(overrides = {}) {
    return { schema: 1, scope: "replacement-v2-post-sort", replacement: inputMemory(),
        retiring: emptyInputMemory(), other_post_sort_owners_unmeasured: false, ...overrides };
}
function formatPostSort(value) {
    return formatReplacementV2PostSortMemoryDebug({
        replacement_v2_post_sort_memory_snapshot() { return value; },
    });
}
function invalidPostSort(lines) {
    assert.match(lines[0], /^Replacement V2 post-sort: invalid \(/);
    assert(!lines.join("\n").includes("private/vault/🧪"));
    assert(lines.some(line => /nullable during sorter ownership/.test(line)));
}

test("replacement V2 post-sort reports exact entry ownership without double counting", () => {
    let calls = 0;
    const owner = { replacement_v2_post_sort_memory_snapshot() {
        assert.equal(this, owner); calls++; return postSortSnapshot();
    } };
    const lines = formatReplacementV2PostSortMemoryDebug(owner);
    assert.equal(calls, 1);
    assert.equal(lines[0], "Replacement V2 post-sort private: 3/8 entries · slot 48 B"
        + " · Vec backing 384 B · paths 3 strings · length 12 B · capacity 32 B");
    assert.equal(lines[1], "Replacement V2 post-sort retiring: 0/0 entries · slot 0 B"
        + " · Vec backing 0 B · paths 0 strings · length 0 B · capacity 0 B");
    assert(lines.some(line => /already included in WASM instance memory; do not add/.test(line)));
    assert(lines.includes("Replacement V2 post-sort other owners unmeasured: no"));
});

test("replacement V2 post-sort nullable sorting coverage and failures are path-free", () => {
    for (const owner of [undefined, null, {},
        { replacement_v2_post_sort_memory_snapshot: undefined }]) {
        assert.deepEqual(formatReplacementV2PostSortMemoryDebug(owner),
            ["Replacement V2 post-sort: unsupported (snapshot export unavailable)"]);
    }
    const sorting = formatPostSort(postSortSnapshot({ replacement: null,
        other_post_sort_owners_unmeasured: true }));
    assert(sorting.includes("Replacement V2 post-sort private: unmeasured"));
    for (const raw of [undefined, null, [], true, "private/vault/🧪",
        postSortSnapshot({ schema: 2 }), postSortSnapshot({ scope: "private/vault/🧪" }),
        postSortSnapshot({ replacement: null }), postSortSnapshot({ retiring: null }),
        postSortSnapshot({ other_post_sort_owners_unmeasured: "true" })]) {
        invalidPostSort(formatPostSort(raw));
    }
    for (const method of [null, false, 1, "private/vault/🧪", {}]) {
        invalidPostSort(formatReplacementV2PostSortMemoryDebug({
            replacement_v2_post_sort_memory_snapshot: method,
        }));
    }
    invalidPostSort(formatReplacementV2PostSortMemoryDebug({
        replacement_v2_post_sort_memory_snapshot() { throw new Error("private/vault/🧪"); },
    }));
});

test("replacement V2 post-sort schema, relations and native validity are strict", () => {
    const failures = [
        postSortSnapshot({ replacement: inputMemory({ entries: vecMemory({ length_slots: 9 }) }) }),
        postSortSnapshot({ replacement: inputMemory({ entries: vecMemory({ backing_capacity_bytes: 383 }) }) }),
        postSortSnapshot({ replacement: inputMemory({ paths: stringMetadata({ strings: 2 }) }) }),
    ];
    for (const raw of failures) invalidPostSort(formatPostSort(raw));
    const complete = () => postSortSnapshot({ replacement: inputMemory(), retiring: inputMemory() });
    for (const path of metadataObjects(complete())) {
        const current = metadataAt(complete(), path);
        for (const field of Object.keys(current)) {
            const missing = complete(); delete metadataAt(missing, path)[field];
            invalidPostSort(formatPostSort(missing));
            const extra = complete(); metadataAt(extra, path).extra = "private/vault/🧪";
            invalidPostSort(formatPostSort(extra));
        }
    }
    for (const path of metadataObjects(complete())) {
        for (const [key, current] of Object.entries(metadataAt(complete(), path))) {
            if (typeof current !== "boolean" || key !== "counters_valid") continue;
            const raw = complete(); metadataAt(raw, path)[key] = false;
            const lines = formatPostSort(raw); invalidPostSort(lines);
            assert.match(lines[0], /native counters marked invalid/);
        }
    }
});

function sortMemory(overrides = {}) {
    return { sorters: 1, values: vecMemory(),
        source_indices: vecMemory({ length_slots: 3, capacity_slots: 8, slot_size_bytes: 4,
            backing_capacity_bytes: 32 }),
        target_indices: vecMemory({ length_slots: 3, capacity_slots: 8, slot_size_bytes: 4,
            backing_capacity_bytes: 32 }),
        counters_valid: true, ...overrides };
}
function emptySortMemory() {
    return sortMemory({ sorters: 0, values: emptyVecMemory(), source_indices: emptyVecMemory(),
        target_indices: emptyVecMemory() });
}
function sortOwner(overrides = {}) {
    return { entries: sortMemory(), children: emptySortMemory(), counters_valid: true, ...overrides };
}
function emptySortOwner() {
    return sortOwner({ entries: emptySortMemory(), children: emptySortMemory() });
}
function sortSnapshot(overrides = {}) {
    return { schema: 1, scope: "replacement-sort", replacement: sortOwner(),
        retiring: emptySortOwner(), other_sort_owners_unmeasured: false, ...overrides };
}
function formatSort(value) {
    return formatReplacementSortMemoryDebug({ replacement_sort_memory_snapshot() { return value; } });
}
function invalidSort(lines) {
    assert.match(lines[0], /^Replacement sort: +invalid \(/);
    assert(!lines.join("\n").includes("private/vault/🧪"));
    assert(lines.some(line => /excludes nested Strings/.test(line)));
}

test("replacement sort reports exact active entry Vec backing without calling it total memory", () => {
    let calls = 0;
    const owner = { replacement_sort_memory_snapshot() { assert.equal(this, owner); calls++; return sortSnapshot(); } };
    const lines = formatReplacementSortMemoryDebug(owner);
    assert.equal(calls, 1);
    assert.equal(lines[0], "Replacement sort private entries: values 3/8 × 48 B · backing 384 B · source 3/8 × 4 B · backing 32 B · target 3/8 × 4 B · backing 32 B");
    assert.equal(lines[1], "Replacement sort private children: none");
    assert.equal(lines[2], "Replacement sort retiring entries: none");
    assert(lines.some(line => /already included in WASM instance memory; do not add/.test(line)));
    assert(lines.includes("Replacement sort other owners unmeasured: no"));
});

test("replacement sort nullable ownership and malformed exports fail closed path-free", () => {
    for (const owner of [undefined, null, {}, { replacement_sort_memory_snapshot: undefined }]) {
        assert.deepEqual(formatReplacementSortMemoryDebug(owner),
            ["Replacement sort:   unsupported (snapshot export unavailable)"]);
    }
    for (const raw of [undefined, null, [], true, 1, "private/vault/🧪",
        sortSnapshot({ schema: 2 }), sortSnapshot({ scope: "private/vault/🧪" }),
        sortSnapshot({ replacement: null }), sortSnapshot({ retiring: null }),
        sortSnapshot({ other_sort_owners_unmeasured: "true" })]) invalidSort(formatSort(raw));
    const unknown = formatSort(sortSnapshot({ replacement: null, retiring: null,
        other_sort_owners_unmeasured: true }));
    assert(unknown.includes("Replacement sort private: unmeasured"));
    assert(unknown.includes("Replacement sort retiring: unmeasured"));
    for (const method of [null, false, 1, "private/vault/🧪", {}]) {
        invalidSort(formatReplacementSortMemoryDebug({ replacement_sort_memory_snapshot: method }));
    }
    invalidSort(formatReplacementSortMemoryDebug({ replacement_sort_memory_snapshot() {
        throw new Error("private/vault/🧪");
    } }));
});

test("replacement sort fields, active-owner relations and native validity are strict", () => {
    const failures = [
        sortSnapshot({ replacement: sortOwner({ entries: sortMemory({ sorters: 0 }) }) }),
        sortSnapshot({ replacement: sortOwner({ entries: sortMemory({ values: emptyVecMemory() }) }) }),
        sortSnapshot({ replacement: sortOwner({ children: sortMemory() }) }),
        sortSnapshot({ replacement: sortOwner({ entries: sortMemory({
            target_indices: vecMemory({ length_slots: 3, capacity_slots: 8, slot_size_bytes: 8,
                backing_capacity_bytes: 64 }),
        }) }) }),
    ];
    for (const raw of failures) invalidSort(formatSort(raw));
    const complete = () => sortSnapshot({ replacement: sortOwner(), retiring: sortOwner() });
    for (const path of metadataObjects(complete())) {
        const current = metadataAt(complete(), path);
        for (const field of Object.keys(current)) {
            const missing = complete(); delete metadataAt(missing, path)[field]; invalidSort(formatSort(missing));
            const extra = complete(); metadataAt(extra, path).extra = "private/vault/🧪";
            invalidSort(formatSort(extra));
        }
    }
    const invalidNumbers = [-1, 0.25, NaN, Infinity, -Infinity, Number.MAX_SAFE_INTEGER + 1, 1n,
        "private/vault/🧪", true, null, undefined, {}, []];
    for (const path of metadataObjects(complete())) {
        for (const [key, current] of Object.entries(metadataAt(complete(), path))) {
            if (typeof current !== "number" && typeof current !== "boolean") continue;
            const badValues = typeof current === "number"
                ? invalidNumbers : [0, 1, "true", null, undefined, {}, []];
            for (const value of badValues) {
                const raw = complete(); metadataAt(raw, path)[key] = value; invalidSort(formatSort(raw));
            }
            if (key === "counters_valid") {
                const raw = complete(); metadataAt(raw, path)[key] = false;
                const lines = formatSort(raw); invalidSort(lines);
                assert.match(lines[0], /native counters marked invalid/);
            }
        }
    }
});

const PLANNING_VECTORS = ["spans", "leaf_spans", "planned_ranges", "next_planned_ranges",
    "planned_spans", "planned_internal_spans"];
const PLANNING_LABELS = ["spans", "leaf spans", "planned ranges", "next planned ranges",
    "planned spans", "planned internal spans"];
function planningOwner(overrides = {}) {
    return { ...Object.fromEntries(PLANNING_VECTORS.map((name, index) => [name, vecMemory({
        length_slots: index + 1, capacity_slots: index + 4, slot_size_bytes: 12 + index * 4,
        backing_capacity_bytes: (index + 4) * (12 + index * 4),
    })])), counters_valid: true, ...overrides };
}
function emptyPlanningOwner() {
    return planningOwner(Object.fromEntries(PLANNING_VECTORS.map(name => [name, emptyVecMemory()])));
}
function planningSnapshot(overrides = {}) {
    return { schema: 1, scope: "replacement-v2-planning", replacement: planningOwner(),
        retiring: emptyPlanningOwner(), other_planning_owners_unmeasured: false, ...overrides };
}
function formatPlanning(value) {
    return formatReplacementV2PlanningMemoryDebug({ replacement_v2_planning_memory_snapshot() { return value; } });
}
function planningLimitations(lines) {
    assert(lines.some(line => /six POD descriptor Vec backings only.*already included in WASM instance memory; do not add/.test(line)));
    assert(lines.some(line => /excludes entries\/path Strings, RangeRef endpoints, closure\/maps, codec scratch, allocator overhead, total heap\/RSS and admission/.test(line)));
}
function invalidPlanning(lines) {
    assert.match(lines[0], /^Replacement V2 planning: invalid \(/);
    assert(!lines.some(line => /^Replacement V2 planning (private|retiring)( |:)/.test(line)),
        "invalid planning snapshot exposed owner counters");
    assert(!lines.join("\n").includes("private/vault/🧪"));
    planningLimitations(lines);
}

test("replacement V2 planning calls the actual receiver once and maps all six retained vectors", () => {
    const raw = planningSnapshot({ retiring: planningOwner(), other_planning_owners_unmeasured: true });
    const expected = structuredClone(raw);
    for (const path of metadataObjects(raw)) Object.freeze(metadataAt(raw, path));
    let calls = 0, gets = 0;
    const owner = { get replacement_v2_planning_memory_snapshot() {
        gets++;
        return function () {
            assert.equal(this, owner); assert.equal(arguments.length, 0); calls++; return raw;
        };
    } };
    const lines = formatReplacementV2PlanningMemoryDebug(owner);
    assert.equal(gets, 1); assert.equal(calls, 1); assert.deepEqual(raw, expected);
    for (const [ownerName, values] of [["private", raw.replacement], ["retiring", raw.retiring]]) {
        for (const [index, name] of PLANNING_VECTORS.entries()) {
            const value = values[name];
            assert(lines.includes(`Replacement V2 planning ${ownerName} ${PLANNING_LABELS[index]}: ${value.length_slots}/${value.capacity_slots} × ${value.slot_size_bytes} B · backing ${value.backing_capacity_bytes} B`));
        }
    }
    assert.equal(lines.length, 15);
    assert(lines.includes("Replacement V2 planning other owners unmeasured: yes"));
    planningLimitations(lines);
});

test("replacement V2 planning distinguishes null, absent vectors and empty retained backing", () => {
    for (const missing of [["replacement"], ["retiring"], ["replacement", "retiring"]]) {
        const raw = planningSnapshot({ other_planning_owners_unmeasured: true });
        for (const name of missing) raw[name] = null;
        const lines = formatPlanning(raw);
        for (const name of missing) {
            const label = name === "replacement" ? "private" : "retiring";
            assert(lines.includes(`Replacement V2 planning ${label}: unmeasured`));
            assert(!lines.some(line => line.startsWith(`Replacement V2 planning ${label} `)));
        }
        planningLimitations(lines);
        raw.other_planning_owners_unmeasured = false;
        invalidPlanning(formatPlanning(raw));
    }
    const absent = formatPlanning(planningSnapshot({ replacement: emptyPlanningOwner() }));
    assert.equal(absent.filter(line => line.endsWith(": none")).length, 12);
    assert(absent.includes("Replacement V2 planning other owners unmeasured: no"));
    const retained = planningOwner(Object.fromEntries(PLANNING_VECTORS.map(name => [name,
        vecMemory({ length_slots: 0, capacity_slots: 7, slot_size_bytes: 12, backing_capacity_bytes: 84 })])));
    const lines = formatPlanning(planningSnapshot({ replacement: retained }));
    for (const label of PLANNING_LABELS) {
        assert(lines.includes(`Replacement V2 planning private ${label}: 0/7 × 12 B · backing 84 B`));
    }
    const allocatedEmpty = planningSnapshot({ replacement: planningOwner({ spans: vecMemory({
        length_slots: 0, capacity_slots: 0, slot_size_bytes: 12, backing_capacity_bytes: 0,
    }) }) });
    assert(formatPlanning(allocatedEmpty).includes("Replacement V2 planning private spans: 0/0 × 12 B · backing 0 B"));
    const max = Number.MAX_SAFE_INTEGER;
    const maximum = planningSnapshot({ replacement: planningOwner({ spans: vecMemory({
        length_slots: max, capacity_slots: max, slot_size_bytes: 1, backing_capacity_bytes: max,
    }) }) });
    const maximumLine = formatPlanning(maximum).find(line => line.startsWith("Replacement V2 planning private spans:"));
    assert(maximumLine?.startsWith(`Replacement V2 planning private spans: ${max}/${max} × 1 B · backing `));
    assert(maximumLine.endsWith(`(${max} B)`), "human-readable units lost the exact safe-integer byte value");
});

test("replacement V2 planning missing, malformed and throwing exports stay path-free", () => {
    for (const owner of [undefined, null, {}, { replacement_v2_planning_memory_snapshot: undefined }]) {
        assert.deepEqual(formatReplacementV2PlanningMemoryDebug(owner),
            ["Replacement V2 planning: unsupported (snapshot export unavailable)"]);
    }
    for (const method of [null, false, 1, "private/vault/🧪", {}, []]) {
        invalidPlanning(formatReplacementV2PlanningMemoryDebug({ replacement_v2_planning_memory_snapshot: method }));
    }
    let coercions = 0;
    const secretError = { toString() { coercions++; return "private/vault/🧪"; } };
    for (const owner of [
        { replacement_v2_planning_memory_snapshot() { throw new Error("private/vault/🧪"); } },
        { get replacement_v2_planning_memory_snapshot() { throw secretError; } },
        { replacement_v2_planning_memory_snapshot() { throw secretError; } },
    ]) invalidPlanning(formatReplacementV2PlanningMemoryDebug(owner));
    for (const raw of [undefined, null, [], true, 1, "private/vault/🧪",
        planningSnapshot({ schema: 0 }), planningSnapshot({ schema: 2 }),
        planningSnapshot({ scope: "private/vault/🧪" }),
        planningSnapshot({ other_planning_owners_unmeasured: "true" }),
        planningSnapshot({ replacement: [] }), planningSnapshot({ retiring: "private/vault/🧪" }),
    ]) invalidPlanning(formatPlanning(raw));
    invalidPlanning(formatPlanning(new Proxy(planningSnapshot(), {
        ownKeys() { throw secretError; },
    })));
    assert.equal(coercions, 0, "diagnostics stringified a private thrown payload");
});

test("replacement V2 planning rejects missing, inherited, accessor and extra fields at every level", () => {
    const complete = () => planningSnapshot({ retiring: planningOwner() });
    let accessorCalls = 0;
    for (const path of metadataObjects(complete())) {
        for (const field of Object.keys(metadataAt(complete(), path))) {
            const missing = complete(); delete metadataAt(missing, path)[field];
            invalidPlanning(formatPlanning(missing));
            const accessor = complete(); Object.defineProperty(metadataAt(accessor, path), field, {
                enumerable: true, get() { accessorCalls++; throw new Error("private/vault/🧪"); },
            });
            invalidPlanning(formatPlanning(accessor));
        }
        for (const key of ["extra", Symbol("private/vault/🧪")]) {
            const extra = complete(); Object.defineProperty(metadataAt(extra, path), key, { value: "private/vault/🧪" });
            invalidPlanning(formatPlanning(extra));
        }
        const inherited = complete();
        if (!path.length) invalidPlanning(formatPlanning(Object.create(inherited)));
        else {
            const parent = metadataAt(inherited, path.slice(0, -1)), key = path.at(-1);
            parent[key] = Object.create(parent[key]); invalidPlanning(formatPlanning(inherited));
        }
        const hostile = complete();
        const proxy = new Proxy(metadataAt(hostile, path), {
            getOwnPropertyDescriptor() { throw new Error("private/vault/🧪"); },
        });
        if (!path.length) invalidPlanning(formatPlanning(proxy));
        else {
            metadataAt(hostile, path.slice(0, -1))[path.at(-1)] = proxy;
            invalidPlanning(formatPlanning(hostile));
        }
    }
    assert.equal(accessorCalls, 0);
});

test("replacement V2 planning safe integers and native validity are checked across all owners", () => {
    const complete = () => planningSnapshot({ retiring: planningOwner() });
    const invalidNumbers = [-1, 0.25, NaN, Infinity, -Infinity, Number.MAX_SAFE_INTEGER + 1, 1n,
        "private/vault/🧪", true, null, undefined, {}, []];
    for (const path of metadataObjects(complete())) {
        for (const [key, current] of Object.entries(metadataAt(complete(), path))) {
            if (typeof current !== "number" && typeof current !== "boolean") continue;
            const badValues = typeof current === "number" ? invalidNumbers : [0, 1, "true", null, undefined, {}, []];
            for (const value of badValues) {
                const raw = complete(); metadataAt(raw, path)[key] = value; invalidPlanning(formatPlanning(raw));
            }
            if (key === "counters_valid") {
                const raw = complete(); metadataAt(raw, path)[key] = false;
                const lines = formatPlanning(raw); invalidPlanning(lines);
                assert.match(lines[0], /native counters marked invalid/);
            }
        }
    }
});

test("replacement V2 planning enforces exact Vec backing without inferring a native layout", () => {
    const invalidVectors = [
        vecMemory({ owners: 2 }), vecMemory({ owners: 0 }),
        vecMemory({ length_slots: 9 }), vecMemory({ slot_size_bytes: 0 }),
        vecMemory({ backing_capacity_bytes: 383 }),
        vecMemory({ length_slots: 0, capacity_slots: Number.MAX_SAFE_INTEGER,
            slot_size_bytes: 2, backing_capacity_bytes: Number.MAX_SAFE_INTEGER }),
    ];
    // Each absent-vector field must stay zero; owners=0 is not permission to
    // hide a retained capacity/slot size/length/backing behind a "none" line.
    for (const key of ["length_slots", "capacity_slots", "slot_size_bytes", "backing_capacity_bytes"]) {
        invalidVectors.push({ ...emptyVecMemory(), [key]: 1 });
    }
    for (const name of ["replacement", "retiring"]) {
        for (const vector of PLANNING_VECTORS) {
            for (const invalid of invalidVectors) {
                const raw = planningSnapshot({ retiring: planningOwner() });
                raw[name][vector] = invalid; invalidPlanning(formatPlanning(raw));
            }
        }
    }
    // The ABI supplies the real native slot size; synthetic host validation
    // accepts an internally consistent different layout rather than guessing it.
    const raw = planningSnapshot({ replacement: planningOwner({ planned_ranges: vecMemory({
        length_slots: 4, capacity_slots: 9, slot_size_bytes: 40, backing_capacity_bytes: 360,
    }) }) });
    assert(formatPlanning(raw).includes("Replacement V2 planning private planned ranges: 4/9 × 40 B · backing 360 B"));
});

const RANGE_VECTORS = ["ranges", "next_ranges", "closure_pending", "closure_expanding"];
const RANGE_LABELS = ["ranges", "next ranges", "closure pending", "closure expanding"];
function rangesOwner(overrides = {}) {
    return { ...Object.fromEntries(RANGE_VECTORS.map((name, index) => [name, vecMemory({
        length_slots: index + 1, capacity_slots: index + 4, slot_size_bytes: 64,
        backing_capacity_bytes: (index + 4) * 64,
    })])), vector_paths: stringMetadata({ strings: 20, length_bytes: 80, capacity_bytes: 120 }),
    descriptor_ranges: 3, descriptor_paths: stringMetadata({ strings: 6, length_bytes: 24, capacity_bytes: 40 }),
    counters_valid: true, ...overrides };
}
function emptyRangesOwner() {
    return rangesOwner({ ...Object.fromEntries(RANGE_VECTORS.map(name => [name, emptyVecMemory()])),
        vector_paths: stringMetadata({ strings: 0, length_bytes: 0, capacity_bytes: 0 }),
        descriptor_ranges: 0, descriptor_paths: stringMetadata({ strings: 0, length_bytes: 0, capacity_bytes: 0 }) });
}
function rangesSnapshot(overrides = {}) {
    return { schema: 1, scope: "replacement-v2-ranges", replacement: rangesOwner(),
        retiring: emptyRangesOwner(), other_range_owners_unmeasured: false, ...overrides };
}
function formatRanges(value) {
    return formatReplacementV2RangesMemoryDebug({ replacement_v2_ranges_memory_snapshot() { return value; } });
}
function rangesLimitations(lines) {
    assert(lines.some(line => /retained RangeRef Vec backing \+ vector\/map endpoint Strings.*already included in WASM instance memory; do not add/.test(line)));
    assert(lines.some(line => /descriptor count is logical, not map bytes.*root endpoints stay in metadata.*excludes HashMap\/HashSet buckets, entries\/sort\/planning, codec scratch, allocator overhead, total heap\/RSS and admission/.test(line)));
}
function invalidRanges(lines) {
    assert.match(lines[0], /^Replacement V2 ranges: invalid \(/);
    assert(!lines.some(line => /^Replacement V2 ranges (private|retiring)( |:)/.test(line)));
    assert.doesNotMatch(lines.join("\n"), /private\/vault\/🧪|NaN|Infinity|undefined|\[object/);
    rangesLimitations(lines);
}

test("replacement V2 ranges calls the actual receiver once and reports disjoint retained owners", () => {
    const raw = rangesSnapshot({ retiring: rangesOwner(), other_range_owners_unmeasured: true });
    const expected = structuredClone(raw);
    for (const path of metadataObjects(raw)) Object.freeze(metadataAt(raw, path));
    let gets = 0, calls = 0;
    const owner = { get replacement_v2_ranges_memory_snapshot() {
        gets++;
        return function () {
            assert.equal(this, owner); assert.equal(arguments.length, 0); calls++; return raw;
        };
    } };
    const lines = formatReplacementV2RangesMemoryDebug(owner);
    assert.equal(gets, 1); assert.equal(calls, 1); assert.deepEqual(raw, expected);
    for (const name of ["private", "retiring"]) {
        for (const [index, label] of RANGE_LABELS.entries()) {
            assert(lines.includes(`Replacement V2 ranges ${name} ${label}: ${index + 1}/${index + 4} × 64 B · backing ${(index + 4) * 64} B`));
        }
        assert(lines.includes(`Replacement V2 ranges ${name} vector paths: 20 strings · length 80 B · capacity 120 B`));
        assert(lines.includes(`Replacement V2 ranges ${name} descriptors: 3 logical ranges · 6 strings · length 24 B · capacity 40 B`));
    }
    assert.equal(lines.length, 15);
    assert(lines.includes("Replacement V2 ranges other owners unmeasured: yes"));
    rangesLimitations(lines);
});

test("replacement V2 ranges distinguishes null coverage from absent and retained-empty buffers", () => {
    for (const missing of [["replacement"], ["retiring"], ["replacement", "retiring"]]) {
        const raw = rangesSnapshot({ other_range_owners_unmeasured: true });
        for (const name of missing) raw[name] = null;
        const lines = formatRanges(raw);
        for (const name of missing) {
            const label = name === "replacement" ? "private" : "retiring";
            assert(lines.includes(`Replacement V2 ranges ${label}: unmeasured`));
            assert(!lines.some(line => line.startsWith(`Replacement V2 ranges ${label} `)));
        }
        raw.other_range_owners_unmeasured = false; invalidRanges(formatRanges(raw));
    }
    const absent = formatRanges(rangesSnapshot({ replacement: emptyRangesOwner() }));
    assert.equal(absent.filter(line => line.endsWith(": none")).length, 8);
    const retained = emptyRangesOwner();
    for (const name of RANGE_VECTORS) retained[name] = vecMemory({ length_slots: 0,
        capacity_slots: 7, slot_size_bytes: 64, backing_capacity_bytes: 448 });
    const lines = formatRanges(rangesSnapshot({ replacement: retained }));
    for (const label of RANGE_LABELS) {
        assert(lines.includes(`Replacement V2 ranges private ${label}: 0/7 × 64 B · backing 448 B`));
    }
    // String owners can be empty values; their descriptor count is still exact.
    const emptyPaths = rangesOwner({ vector_paths: stringMetadata({ strings: 20, length_bytes: 0, capacity_bytes: 9 }),
        descriptor_paths: stringMetadata({ strings: 6, length_bytes: 0, capacity_bytes: 0 }) });
    assert(formatRanges(rangesSnapshot({ replacement: emptyPaths })).includes(
        "Replacement V2 ranges private vector paths: 20 strings · length 0 B · capacity 9 B"));
});

test("replacement V2 ranges missing, malformed and throwing exports are path-free", () => {
    for (const owner of [undefined, null, {}, { replacement_v2_ranges_memory_snapshot: undefined }]) {
        assert.deepEqual(formatReplacementV2RangesMemoryDebug(owner),
            ["Replacement V2 ranges: unsupported (snapshot export unavailable)"]);
    }
    for (const method of [null, false, 1, "private/vault/🧪", {}, []]) {
        invalidRanges(formatReplacementV2RangesMemoryDebug({ replacement_v2_ranges_memory_snapshot: method }));
    }
    let coercions = 0;
    const secret = { toString() { coercions++; return "private/vault/🧪"; } };
    for (const owner of [
        { get replacement_v2_ranges_memory_snapshot() { throw secret; } },
        { replacement_v2_ranges_memory_snapshot() { throw secret; } },
        { replacement_v2_ranges_memory_snapshot() { throw new Error("private/vault/🧪"); } },
    ]) invalidRanges(formatReplacementV2RangesMemoryDebug(owner));
    for (const raw of [undefined, null, [], false, 1, "private/vault/🧪", Promise.resolve(rangesSnapshot()),
        rangesSnapshot({ schema: 2 }), rangesSnapshot({ scope: "private/vault/🧪" }),
        rangesSnapshot({ other_range_owners_unmeasured: "true" }), rangesSnapshot({ replacement: [] }),
        rangesSnapshot({ retiring: "private/vault/🧪" }),
    ]) invalidRanges(formatRanges(raw));
    invalidRanges(formatRanges(new Proxy(rangesSnapshot(), { ownKeys() { throw secret; } })));
    assert.equal(coercions, 0);
});

test("replacement V2 ranges rejects missing inherited accessor and extra fields at every level", () => {
    const complete = () => rangesSnapshot({ retiring: rangesOwner() });
    let reads = 0;
    for (const path of metadataObjects(complete())) {
        for (const field of Object.keys(metadataAt(complete(), path))) {
            const missing = complete(); delete metadataAt(missing, path)[field]; invalidRanges(formatRanges(missing));
            const accessor = complete(); Object.defineProperty(metadataAt(accessor, path), field, {
                enumerable: true, get() { reads++; throw new Error("private/vault/🧪"); },
            });
            invalidRanges(formatRanges(accessor));
        }
        for (const key of ["extra", Symbol("private/vault/🧪")]) {
            const extra = complete(); Object.defineProperty(metadataAt(extra, path), key, { value: "private/vault/🧪" });
            invalidRanges(formatRanges(extra));
        }
        for (const alter of [value => Object.create(value), value => new Proxy(value, {
            getOwnPropertyDescriptor() { throw new Error("private/vault/🧪"); },
        })]) {
            const raw = complete(), changed = alter(metadataAt(raw, path));
            if (path.length === 0) invalidRanges(formatRanges(changed));
            else {
                metadataAt(raw, path.slice(0, -1))[path.at(-1)] = changed; invalidRanges(formatRanges(raw));
            }
        }
    }
    assert.equal(reads, 0, "snapshot accessors executed");
});

test("replacement V2 ranges validates all counters and native validity across both owners", () => {
    const complete = () => rangesSnapshot({ retiring: rangesOwner() });
    const badNumbers = [-1, 0.25, NaN, Infinity, -Infinity, Number.MAX_SAFE_INTEGER + 1, 1n,
        "private/vault/🧪", true, null, undefined, {}, []];
    for (const path of metadataObjects(complete())) {
        for (const [key, current] of Object.entries(metadataAt(complete(), path))) {
            if (typeof current !== "number" && typeof current !== "boolean") continue;
            for (const value of typeof current === "number" ? badNumbers : [0, "true", null, undefined, {}, []]) {
                const raw = complete(); metadataAt(raw, path)[key] = value; invalidRanges(formatRanges(raw));
            }
            if (key === "counters_valid") {
                const raw = complete(); metadataAt(raw, path)[key] = false;
                const lines = formatRanges(raw); invalidRanges(lines); assert.match(lines[0], /native counters marked invalid/);
            }
        }
    }
});

test("replacement V2 ranges enforces exact backing and two endpoints per physical descriptor", () => {
    const badVectors = [vecMemory({ owners: 2 }), vecMemory({ owners: 0 }),
        vecMemory({ length_slots: 9 }), vecMemory({ slot_size_bytes: 0 }),
        vecMemory({ backing_capacity_bytes: 383 }),
        vecMemory({ length_slots: 0, capacity_slots: 0, slot_size_bytes: 64, backing_capacity_bytes: 0 }),
        vecMemory({ length_slots: 0, capacity_slots: Number.MAX_SAFE_INTEGER, slot_size_bytes: 2,
            backing_capacity_bytes: Number.MAX_SAFE_INTEGER })];
    for (const key of ["length_slots", "capacity_slots", "slot_size_bytes", "backing_capacity_bytes"])
        badVectors.push({ ...emptyVecMemory(), [key]: 1 });
    for (const owner of ["replacement", "retiring"]) {
        for (const name of RANGE_VECTORS) for (const vector of badVectors) {
            const raw = rangesSnapshot({ retiring: rangesOwner() }); raw[owner][name] = vector;
            invalidRanges(formatRanges(raw));
        }
        for (const edit of [
            value => { value.vector_paths.strings--; },
            value => { value.descriptor_paths.strings++; },
            value => { value.descriptor_ranges = 0; },
            value => { value.vector_paths.capacity_bytes = 79; },
            value => { value.descriptor_paths.length_bytes = 41; },
            value => { value.ranges.slot_size_bytes = 32; value.ranges.backing_capacity_bytes = 128; },
        ]) {
            const raw = rangesSnapshot({ retiring: rangesOwner() }); edit(raw[owner]); invalidRanges(formatRanges(raw));
        }
        for (const key of ["length_bytes", "capacity_bytes"]) for (const paths of ["vector_paths", "descriptor_paths"]) {
            const raw = rangesSnapshot({ replacement: emptyRangesOwner(), retiring: emptyRangesOwner() });
            raw[owner][paths][key] = 1; invalidRanges(formatRanges(raw));
        }
    }
    // The host does not hardcode wasm32 layout: all four owners may agree on a
    // different layout. Native exact-layout testing belongs to native/parity gates.
    const alternate = rangesOwner();
    for (const name of RANGE_VECTORS) {
        alternate[name].slot_size_bytes = 80;
        alternate[name].backing_capacity_bytes = alternate[name].capacity_slots * 80;
    }
    assert(formatRanges(rangesSnapshot({ replacement: alternate })).includes(
        "Replacement V2 ranges private ranges: 1/4 × 80 B · backing 320 B"));
});

test("replacement V2 ranges checks sum and product precision before accepting counts", () => {
    const max = Number.MAX_SAFE_INTEGER, half = Math.floor(max / 2);
    const hugeVector = vecMemory({ length_slots: half + 1, capacity_slots: half + 1,
        slot_size_bytes: 1, backing_capacity_bytes: half + 1 });
    const product = emptyRangesOwner(); product.ranges = hugeVector;
    product.vector_paths = stringMetadata({ strings: max, length_bytes: 0, capacity_bytes: 0 });
    invalidRanges(formatRanges(rangesSnapshot({ replacement: product })));
    const sum = emptyRangesOwner(); sum.ranges = hugeVector; sum.next_ranges = { ...hugeVector };
    invalidRanges(formatRanges(rangesSnapshot({ replacement: sum })));
    const descriptors = emptyRangesOwner(); descriptors.descriptor_ranges = half + 1;
    descriptors.descriptor_paths = stringMetadata({ strings: max, length_bytes: 0, capacity_bytes: 0 });
    invalidRanges(formatRanges(rangesSnapshot({ replacement: descriptors })));
    const capacities = emptyRangesOwner();
    for (const name of ["ranges", "next_ranges"]) capacities[name] = { ...hugeVector, length_slots: 0 };
    invalidRanges(formatRanges(rangesSnapshot({ replacement: capacities })));
    const pathsOverflow = rangesOwner(); pathsOverflow.descriptor_paths.capacity_bytes = max;
    invalidRanges(formatRanges(rangesSnapshot({ replacement: pathsOverflow })));
    // A boundary-safe logical count remains printable without making a map-size claim.
    const safe = emptyRangesOwner(); safe.descriptor_ranges = half;
    safe.descriptor_paths = stringMetadata({ strings: half * 2, length_bytes: 0, capacity_bytes: 0 });
    assert(formatRanges(rangesSnapshot({ replacement: safe })).some(line => line.includes(
        `descriptors: ${half} logical ranges · ${half * 2} strings`)));
});
