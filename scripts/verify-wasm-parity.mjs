#!/usr/bin/env node

import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const outputDir = resolve(process.argv[2] ?? "plugin/wasm");
const scalarModuleUrl = pathToFileURL(resolve(outputDir, "sync_core.js")).href;
const simdModuleUrl = pathToFileURL(resolve(outputDir, "sync_core_simd.js")).href;
const scalar = await import(scalarModuleUrl);
const simd = await import(simdModuleUrl);
const scalarBytes = await readFile(resolve(outputDir, "sync_core_bg.wasm"));
const simdBytes = await readFile(resolve(outputDir, "sync_core_simd_bg.wasm"));

if (!WebAssembly.validate(scalarBytes)) {
    throw new Error("scalar module did not validate");
}
if (!WebAssembly.validate(simdBytes)) {
    throw new Error("SIMD module did not validate on this test host");
}
await scalar.default({ module_or_path: scalarBytes });
await simd.default({ module_or_path: simdBytes });

function deterministicPayload(length) {
    const bytes = new Uint8Array(length);
    let state = 0x6d2b79f5;
    for (let index = 0; index < length; index++) {
        state = Math.imul(state ^ (state >>> 15), 1 | state);
        state ^= state + Math.imul(state ^ (state >>> 7), 61 | state);
        bytes[index] = (state ^ (state >>> 14)) & 0xff;
    }
    return bytes;
}

function streamingHash(module, bytes, feedBytes) {
    const hasher = new module.Hasher();
    try {
        for (let offset = 0; offset < bytes.length; offset += feedBytes) {
            hasher.update(bytes.subarray(offset, offset + feedBytes));
        }
        return hasher.finalize();
    } finally {
        hasher.free();
    }
}

function chunkManifest(module, bytes, feedBytes) {
    const chunker = new module.WasmChunker();
    try {
        for (let offset = 0; offset < bytes.length; offset += feedBytes) {
            chunker.update(bytes.subarray(offset, offset + feedBytes));
        }
        return chunker.finish();
    } finally {
        chunker.free();
    }
}

const feedSizes = [64, 128, 256, 512, 1024].map((kib) => kib * 1024);
for (const length of [0, 1, 63, 64, 65, 1024, 65_535, 65_536, 65_537, 1_048_593]) {
    const bytes = deterministicPayload(length);
    const expected = scalar.wasm_hash(bytes);
    if (simd.wasm_hash(bytes) !== expected) {
        throw new Error(`scalar/SIMD hash mismatch at ${length} bytes`);
    }
    for (const feedBytes of feedSizes) {
        if (streamingHash(scalar, bytes, feedBytes) !== expected) {
            throw new Error(`scalar streaming mismatch at ${length}/${feedBytes}`);
        }
        if (streamingHash(simd, bytes, feedBytes) !== expected) {
            throw new Error(`SIMD streaming mismatch at ${length}/${feedBytes}`);
        }
    }
}

const large = deterministicPayload(10 * 1024 * 1024 + 123);
const baselineManifest = JSON.stringify(chunkManifest(scalar, large, feedSizes[0]));
for (const feedBytes of feedSizes.slice(1)) {
    if (JSON.stringify(chunkManifest(scalar, large, feedBytes)) !== baselineManifest) {
        throw new Error(`scalar FastCDC boundary mismatch at feed ${feedBytes}`);
    }
    if (JSON.stringify(chunkManifest(simd, large, feedBytes)) !== baselineManifest) {
        throw new Error(`SIMD FastCDC boundary mismatch at feed ${feedBytes}`);
    }
}

const batchData = deterministicPayload(3000);
const offsets = new Uint32Array([0, 1000, 2000]);
const sizes = new Uint32Array([1000, 1000, 1000]);
if (
    JSON.stringify(scalar.wasm_hash_batch(batchData, offsets, sizes)) !==
    JSON.stringify(simd.wasm_hash_batch(batchData, offsets, sizes))
) {
    throw new Error("scalar/SIMD batch hash mismatch");
}

function deterministicTreeEntries(count) {
    const rows = [];
    for (let index = 0; index < count; index++) {
        const path = `notes/${String(index % 17).padStart(2, "0")}/${String(index).padStart(6, "0")}.md`;
        rows.push({
            path,
            hash: scalar.wasm_hash(deterministicPayload(128 + (index % 257))),
            mtime_ms: 1_700_000_000_000 + index,
            size: 128 + (index % 257),
        });
    }
    return rows;
}

function treeV2State(module, entries) {
    const tree = new module.WasmTree("parity-vault", "parity-device");
    tree.set_tree_version(2);
    if (tree.tree_version() !== 2) {
        tree.free();
        throw new Error("WasmTree did not select Tree v2");
    }
    tree.build_from_entries(JSON.stringify(entries));
    const rootBytes = tree.root_bytes();
    if (!rootBytes || module.wasm_root_version_from_bytes(rootBytes) !== 2) {
        tree.free();
        throw new Error("WasmTree emitted a non-v2 persisted root");
    }
    return tree;
}

function assertTreeParity(left, right, phase) {
    if (left.root_hash_hex() !== right.root_hash_hex()) {
        throw new Error(`scalar/SIMD Tree v2 root mismatch after ${phase}`);
    }
    if (left.total_files() !== right.total_files()) {
        throw new Error(`scalar/SIMD Tree v2 count mismatch after ${phase}`);
    }
    const leftChunks = JSON.stringify(scalar.wasm_tree_committed_chunk_hashes(left));
    const rightChunks = JSON.stringify(simd.wasm_tree_committed_chunk_hashes(right));
    if (leftChunks !== rightChunks) {
        throw new Error(`scalar/SIMD Tree v2 graph mismatch after ${phase}`);
    }
}

const treeEntries = deterministicTreeEntries(2_048);
const scalarTree = treeV2State(scalar, treeEntries);
const simdTree = treeV2State(simd, treeEntries);
try {
    assertTreeParity(scalarTree, simdTree, "rebuild");

    const replacement = [{
        ...treeEntries[1_024],
        hash: scalar.wasm_hash(deterministicPayload(8_193)),
        mtime_ms: 1_800_000_000_000,
        size: 8_193,
    }];
    for (const tree of [scalarTree, simdTree]) {
        tree.begin_candidate();
        tree.candidate_update_batch(JSON.stringify(replacement));
        tree.candidate_delete_batch(JSON.stringify([treeEntries[7].path, treeEntries[1_777].path]));
    }
    if (scalarTree.candidate_root_hash_hex() !== simdTree.candidate_root_hash_hex()) {
        throw new Error("scalar/SIMD Tree v2 candidate mismatch");
    }
    scalarTree.commit_candidate();
    simdTree.commit_candidate();
    assertTreeParity(scalarTree, simdTree, "candidate commit");

    const committed = scalarTree.root_hash_hex();
    for (const tree of [scalarTree, simdTree]) {
        tree.begin_candidate();
        tree.candidate_delete_batch(JSON.stringify([treeEntries[42].path]));
        tree.abort_candidate();
    }
    assertTreeParity(scalarTree, simdTree, "candidate abort");
    if (scalarTree.root_hash_hex() !== committed) {
        throw new Error("Tree v2 abort changed the committed root");
    }

    const transitionedEntries = treeEntries
        .filter((entry) => entry.path !== treeEntries[7].path && entry.path !== treeEntries[1_777].path)
        .map((entry) => entry.path === replacement[0].path ? replacement[0] : entry);
    for (const tree of [scalarTree, simdTree]) {
        tree.rebuild_from_entries_in_version(1, JSON.stringify(transitionedEntries));
        if (tree.tree_version() !== 1) throw new Error("live Tree v2→v1 rebuild failed");
    }
    assertTreeParity(scalarTree, simdTree, "live v2→v1 rebuild");
    for (const tree of [scalarTree, simdTree]) {
        tree.rebuild_from_entries_in_version(2, JSON.stringify(transitionedEntries));
        if (tree.tree_version() !== 2) throw new Error("live Tree v1→v2 rebuild failed");
    }
    assertTreeParity(scalarTree, simdTree, "live v1→v2 rebuild");
    if (scalarTree.root_hash_hex() !== committed) {
        throw new Error("live Tree format roundtrip changed semantic v2 root");
    }
} finally {
    scalarTree.free();
    simdTree.free();
}

console.log(JSON.stringify({
    scalar_bytes: scalarBytes.length,
    simd_bytes: simdBytes.length,
    payload_bytes: large.length,
    feed_sizes_tested: feedSizes.length,
    tree_v2_entries: treeEntries.length,
    parity: true,
}));
