#!/usr/bin/env node

import { mkdtemp, open, readFile, rm, stat } from "node:fs/promises";
import { cpus, platform, arch, tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { Worker } from "node:worker_threads";

const workerBundle = resolve(process.argv[2] ?? "/tmp/obsetync-hash-worker.cjs");
const fileCount = Number.parseInt(process.argv[3] ?? "8", 10);
const fileMiB = Number.parseInt(process.argv[4] ?? "16", 10);
const workerCount = Number.parseInt(process.argv[5] ?? "4", 10);
if (
    !Number.isInteger(fileCount) || fileCount < 2 ||
    !Number.isInteger(fileMiB) || fileMiB < 1 ||
    !Number.isInteger(workerCount) || workerCount < 1 || workerCount > 4
) {
    throw new RangeError("usage: benchmark-hash-workers.mjs bundle [files>=2] [MiB>=1] [workers=1..4]");
}

function hasBinary(value, seen = new Set()) {
    if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) return true;
    if (value === null || typeof value !== "object") return false;
    if (seen.has(value)) return false;
    seen.add(value);
    return Object.values(value).some((nested) => hasBinary(nested, seen));
}

class WorkerClient {
    constructor(source, index) {
        this.worker = new Worker(source, { eval: true, name: `obsetync-bench-${index}` });
        this.pending = new Map();
        this.nextId = 1;
        this.metadataOnly = true;
        this.ready = new Promise((resolveReady, rejectReady) => {
            this.resolveReady = resolveReady;
            this.rejectReady = rejectReady;
        });
        this.worker.on("message", (message) => {
            this.metadataOnly &&= !hasBinary(message);
            if (message?.type === "ready") {
                if (message.wasm_mode !== "simd") {
                    this.rejectReady(new Error("worker did not select SIMD"));
                } else {
                    this.resolveReady();
                }
                return;
            }
            if (message?.type === "fatal") {
                this.rejectReady(new Error(message.message));
                return;
            }
            const waiter = this.pending.get(message?.job_id);
            if (!waiter) return;
            this.pending.delete(message.job_id);
            if (message.type === "result") waiter.resolve(message);
            else {
                const error = new Error(message.message);
                error.code = message.code;
                waiter.reject(error);
            }
        });
        this.worker.on("error", (error) => {
            this.rejectReady(error);
            for (const waiter of this.pending.values()) waiter.reject(error);
            this.pending.clear();
        });
    }

    start(input) {
        const jobId = `bench-${this.nextId++}`;
        const request = {
            type: "job",
            job_id: jobId,
            absolute_path: input.path,
            expected_size: input.size,
            expected_mtime: input.mtime,
            mode: input.mode,
            feed_bytes: input.feedBytes,
        };
        this.metadataOnly &&= !hasBinary(request);
        const promise = new Promise((resolveResult, rejectResult) => {
            this.pending.set(jobId, { resolve: resolveResult, reject: rejectResult });
        });
        this.worker.postMessage(request);
        return { jobId, promise };
    }

    async run(input) {
        return await this.start(input).promise;
    }

    cancel(jobId) {
        this.worker.postMessage({ type: "cancel", job_id: jobId });
    }

    async close() {
        await this.worker.terminate();
    }
}

function percentile(samples, quantile) {
    if (samples.length === 0) return 0;
    const ordered = [...samples].sort((left, right) => left - right);
    return ordered[Math.min(ordered.length - 1, Math.ceil(ordered.length * quantile) - 1)];
}

const source = await readFile(workerBundle, "utf8");
const directory = await mkdtemp(join(tmpdir(), "obsetync-workers-"));
const clients = [];
try {
    const block = Buffer.allocUnsafe(1024 * 1024);
    let state = 0x6d2b79f5;
    for (let index = 0; index < block.length; index++) {
        state = Math.imul(state ^ (state >>> 15), 1 | state);
        state ^= state + Math.imul(state ^ (state >>> 7), 61 | state);
        block[index] = (state ^ (state >>> 14)) & 0xff;
    }

    const files = [];
    for (let fileIndex = 0; fileIndex < fileCount; fileIndex++) {
        const path = join(directory, `payload-${fileIndex}.bin`);
        const handle = await open(path, "w");
        try {
            block[0] = fileIndex;
            for (let mib = 0; mib < fileMiB; mib++) await handle.write(block);
        } finally {
            await handle.close();
        }
        const info = await stat(path);
        files.push({ path, size: info.size, mtime: info.mtimeMs });
    }

    for (let index = 0; index < workerCount; index++) {
        clients.push(new WorkerClient(source, index));
    }
    await Promise.all(clients.map((client) => client.ready));

    const lagSamples = [];
    const intervalMs = 5;
    let expectedTick = performance.now() + intervalMs;
    const timer = setInterval(() => {
        const observed = performance.now();
        lagSamples.push(Math.max(0, observed - expectedTick));
        expectedTick = observed + intervalMs;
    }, intervalMs);

    let nextFile = 0;
    const results = [];
    const started = performance.now();
    await Promise.all(clients.map(async (client) => {
        while (nextFile < files.length) {
            const file = files[nextFile++];
            results.push(await client.run({
                ...file,
                mode: "hash",
                feedBytes: 512 * 1024,
            }));
        }
    }));
    const elapsedMs = performance.now() - started;
    clearInterval(timer);

    const fileZeroHash = await clients[0].run({
        ...files[0],
        mode: "hash",
        feedBytes: 512 * 1024,
    });
    const manifest = await clients[0].run({
        ...files[0],
        mode: "manifest",
        feedBytes: 512 * 1024,
    });
    if (manifest.manifest.file_hash !== fileZeroHash.hash) {
        throw new Error("worker hash/manifest file hash mismatch");
    }
    const fileZeroInfo = await stat(files[0].path);
    const fingerprintStable = [fileZeroHash, manifest].every((result) =>
        result.fingerprint?.size === fileZeroInfo.size &&
        Math.abs(result.fingerprint?.mtime - fileZeroInfo.mtimeMs) <= 1 &&
        Math.abs(result.fingerprint?.ctime - fileZeroInfo.ctimeMs) <= 1 &&
        result.fingerprint?.device === fileZeroInfo.dev &&
        result.fingerprint?.inode === fileZeroInfo.ino);
    if (!fingerprintStable) {
        throw new Error("worker result was not bound to the scanned pathname fingerprint");
    }

    let driftDetected = false;
    try {
        await clients[0].run({
            ...files[0],
            mtime: files[0].mtime + 100,
            mode: "hash",
            feedBytes: 512 * 1024,
        });
    } catch (error) {
        driftDetected = error.code === "FILE_DRIFT";
    }
    if (!driftDetected) throw new Error("worker accepted a stale file fingerprint");

    const cancelled = clients[0].start({
        ...files[0],
        mode: "manifest",
        feedBytes: 64 * 1024,
    });
    clients[0].cancel(cancelled.jobId);
    let cancellationObserved = false;
    try {
        await cancelled.promise;
    } catch (error) {
        cancellationObserved = error.code === "CANCELLED";
    }
    if (!cancellationObserved) throw new Error("running worker cancellation was not observed");

    const bytes = files.reduce((sum, file) => sum + file.size, 0);
    const p95LagMs = percentile(lagSamples, 0.95);
    console.log(JSON.stringify({
        schema_version: 1,
        runtime: {
            platform: platform(),
            architecture: arch(),
            node: process.version,
            cpu: cpus()[0]?.model ?? "unknown",
        },
        workload: {
            files: files.length,
            bytes,
            workers: workerCount,
            feed_bytes: 512 * 1024,
        },
        elapsed_ms: elapsedMs,
        throughput_mib_per_second: (bytes / 1024 / 1024) / (elapsedMs / 1000),
        event_loop_lag: {
            samples: lagSamples.length,
            p50_ms: percentile(lagSamples, 0.50),
            p95_ms: p95LagMs,
            p99_ms: percentile(lagSamples, 0.99),
            target_p95_ms: 16,
            target_met: p95LagMs < 16,
        },
        invariants: {
            scalar_simd_hash_parity_inherited_from_slice3: true,
            hash_manifest_parity: true,
            pathname_fingerprint_stable: fingerprintStable,
            protocol_metadata_only: clients.every((client) => client.metadataOnly),
            drift_detected: driftDetected,
            cancellation_observed: cancellationObserved,
        },
    }, null, 2));
} finally {
    await Promise.allSettled(clients.map((client) => client.close()));
    await rm(directory, { recursive: true, force: true });
}
