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

console.log(JSON.stringify({
    scalar_bytes: scalarBytes.length,
    simd_bytes: simdBytes.length,
    payload_bytes: large.length,
    feed_sizes_tested: feedSizes.length,
    parity: true,
}));
