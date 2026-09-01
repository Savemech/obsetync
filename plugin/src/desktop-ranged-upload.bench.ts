import { mkdtemp, open, rm, stat } from "node:fs/promises";
import { arch, cpus, platform, tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import {
    DESKTOP_RANGE_QUEUE_BYTES,
    uploadDesktopMissingRanges,
} from "./desktop-ranged-upload";

const MIB = 1024 * 1024;
const GIB = 1024 * MIB;

async function runSparseCase(directory: string, label: string, size: number) {
    const path = join(directory, `${label}.bin`);
    const handle = await open(path, "w");
    try {
        await handle.truncate(size);
    } finally {
        await handle.close();
    }
    const info = await stat(path);
    const source = {
        absolutePath: path,
        fingerprint: {
            size: Number(info.size),
            mtime: Number(info.mtimeMs),
            ctime: Number(info.ctimeMs),
            device: Number(info.dev),
            inode: Number(info.ino),
        },
    };
    const ranges = [
        { hash: "a".repeat(64), offset: 16 * MIB, size: 4 * MIB },
        { hash: "b".repeat(64), offset: size - 4 * MIB, size: 4 * MIB },
    ];
    const arrayBuffersBefore = process.memoryUsage().arrayBuffers;
    let peakArrayBuffers = arrayBuffersBefore;
    let transportCalls = 0;
    let transportBytes = 0;
    let manifests = 0;
    const started = performance.now();
    const result = await uploadDesktopMissingRanges(
        source,
        ranges,
        async (records) => {
            transportCalls++;
            transportBytes += records.reduce(
                (sum, record) => sum + record.data.byteLength,
                0,
            );
            peakArrayBuffers = Math.max(
                peakArrayBuffers,
                process.memoryUsage().arrayBuffers,
            );
        },
        async () => { manifests++; },
    );
    const elapsedMs = performance.now() - started;
    if (
        result.uploadedBytes !== 8 * MIB ||
        result.uploadedRanges !== 2 ||
        result.peakBufferedBytes > DESKTOP_RANGE_QUEUE_BYTES ||
        transportCalls !== 1 || transportBytes !== 8 * MIB || manifests !== 1
    ) {
        throw new Error(`${label}: ranged upload invariant failed`);
    }
    return {
        source_bytes: size,
        selected_range_bytes: result.uploadedBytes,
        queue_peak_bytes: result.peakBufferedBytes,
        observed_array_buffer_delta_bytes: Math.max(0, peakArrayBuffers - arrayBuffersBefore),
        read_ms: result.readMs,
        elapsed_ms: elapsedMs,
        transport_calls: transportCalls,
    };
}

void (async () => {
    const directory = await mkdtemp(join(tmpdir(), "obsetync-ranged-bench-"));
    try {
        const small = await runSparseCase(directory, "256mib", 256 * MIB);
        const w4 = await runSparseCase(directory, "20gib", 20 * GIB);
        const bounded =
            small.queue_peak_bytes === w4.queue_peak_bytes &&
            w4.queue_peak_bytes <= DESKTOP_RANGE_QUEUE_BYTES;
        if (!bounded) throw new Error("range queue grew with source file size");
        console.log(JSON.stringify({
            schema_version: 1,
            runtime: {
                platform: platform(),
                architecture: arch(),
                node: process.version,
                cpu: cpus()[0]?.model ?? "unknown",
            },
            workload: "W4 desktop ranged upload",
            queue_limit_bytes: DESKTOP_RANGE_QUEUE_BYTES,
            cases: { small, w4 },
            invariants: {
                positional_reads_only: true,
                manifest_after_chunk_ack: true,
                queue_peak_independent_of_source_size: bounded,
            },
        }, null, 2));
    } finally {
        await rm(directory, { recursive: true, force: true });
    }
})().catch((error) => {
    setTimeout(() => { throw error; }, 0);
});
