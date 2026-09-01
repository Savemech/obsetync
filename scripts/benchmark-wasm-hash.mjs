#!/usr/bin/env node

import { readFile } from "node:fs/promises";
import { cpus } from "node:os";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const outputDir = resolve(process.argv[2] ?? "plugin/wasm");
const payloadMiB = Number.parseInt(process.argv[3] ?? "32", 10);
const measuredIterations = Number.parseInt(process.argv[4] ?? "7", 10);
if (!Number.isInteger(payloadMiB) || payloadMiB <= 0) {
    throw new RangeError("payload MiB must be a positive integer");
}
if (!Number.isInteger(measuredIterations) || measuredIterations < 3) {
    throw new RangeError("measured iterations must be an integer >= 3");
}

const scalar = await import(pathToFileURL(resolve(outputDir, "sync_core.js")).href);
const simd = await import(pathToFileURL(resolve(outputDir, "sync_core_simd.js")).href);
const scalarBytes = await readFile(resolve(outputDir, "sync_core_bg.wasm"));
const simdBytes = await readFile(resolve(outputDir, "sync_core_simd_bg.wasm"));
await scalar.default({ module_or_path: scalarBytes });
await simd.default({ module_or_path: simdBytes });

const payload = new Uint8Array(payloadMiB * 1024 * 1024);
let state = 0x6d2b79f5;
for (let index = 0; index < payload.length; index++) {
    state = Math.imul(state ^ (state >>> 15), 1 | state);
    state ^= state + Math.imul(state ^ (state >>> 7), 61 | state);
    payload[index] = (state ^ (state >>> 14)) & 0xff;
}

function hash(module, feedBytes) {
    const hasher = new module.Hasher();
    try {
        for (let offset = 0; offset < payload.length; offset += feedBytes) {
            hasher.update(payload.subarray(offset, offset + feedBytes));
        }
        return hasher.finalize();
    } finally {
        hasher.free();
    }
}

function timeHash(module, feedBytes) {
    const started = process.hrtime.bigint();
    const digest = hash(module, feedBytes);
    return {
        digest,
        elapsedNs: Number(process.hrtime.bigint() - started),
    };
}

function median(values) {
    const ordered = [...values].sort((a, b) => a - b);
    return ordered[Math.floor(ordered.length / 2)];
}

const expected = scalar.wasm_hash(payload);
const feedSizes = [64, 256, 512, 1024].map((kib) => kib * 1024);
const samples = new Map();
for (const mode of ["scalar", "simd"]) {
    for (const feedBytes of feedSizes) samples.set(`${mode}/${feedBytes}`, []);
}

// Warm every code path before measuring JIT/compiler startup effects.
for (const feedBytes of feedSizes) {
    if (hash(scalar, feedBytes) !== expected || hash(simd, feedBytes) !== expected) {
        throw new Error(`warmup hash mismatch at feed ${feedBytes}`);
    }
}

for (let iteration = 0; iteration < measuredIterations; iteration++) {
    const modes = iteration % 2 === 0
        ? [["scalar", scalar], ["simd", simd]]
        : [["simd", simd], ["scalar", scalar]];
    const rotatedFeeds = feedSizes.map(
        (_, index) => feedSizes[(index + iteration) % feedSizes.length],
    );
    for (const [mode, module] of modes) {
        for (const feedBytes of rotatedFeeds) {
            const sample = timeHash(module, feedBytes);
            if (sample.digest !== expected) {
                throw new Error(`${mode} hash mismatch at feed ${feedBytes}`);
            }
            samples.get(`${mode}/${feedBytes}`).push(sample.elapsedNs);
        }
    }
}

const results = [];
for (const mode of ["scalar", "simd"]) {
    for (const feedBytes of feedSizes) {
        const elapsedNs = samples.get(`${mode}/${feedBytes}`);
        const medianNs = median(elapsedNs);
        results.push({
            mode,
            feed_bytes: feedBytes,
            elapsed_ns: elapsedNs,
            median_ns: medianNs,
            median_mib_per_second: payloadMiB / (medianNs / 1e9),
            hashes_match: true,
        });
    }
}

const best = (mode) => results
    .filter((row) => row.mode === mode)
    .sort((a, b) => a.median_ns - b.median_ns)[0];
const bestScalar = best("scalar");
const bestSimd = best("simd");

console.log(JSON.stringify({
    schema_version: 1,
    runtime: {
        platform: process.platform,
        architecture: process.arch,
        node: process.version,
        cpu: cpus()[0]?.model ?? "unknown",
    },
    workload: {
        deterministic_seed: "0x6d2b79f5",
        payload_bytes: payload.length,
        measured_iterations: measuredIterations,
        warmup_iterations_per_mode_and_feed: 1,
    },
    artifacts: {
        scalar_bytes: scalarBytes.length,
        simd_bytes: simdBytes.length,
    },
    results,
    summary: {
        best_scalar_feed_bytes: bestScalar.feed_bytes,
        best_simd_feed_bytes: bestSimd.feed_bytes,
        best_simd_speedup: bestScalar.median_ns / bestSimd.median_ns,
        all_hashes_match: true,
    },
}, null, 2));
