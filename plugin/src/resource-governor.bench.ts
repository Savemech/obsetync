import { mkdtemp, open, rm, stat, writeFile } from "node:fs/promises";
import { arch, availableParallelism, cpus, platform, tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import hashWorkerSource from "obsetync-hash-worker-source";
import { createDesktopHashWorkerPool } from "./desktop-hash-workers";
import { resourceProfilesFor, type ResourceProfile } from "./resource-governor";

const MIB = 1024 * 1024;
const FILES = 32;
const FILE_BYTES = 8 * MIB;
const TOTAL_BYTES = FILES * FILE_BYTES;
const RUNS_PER_PROFILE = 3;
const UI_TICK_MS = 5;

interface SourceFile {
    path: string;
    size: number;
    mtime: number;
}

interface RunResult {
    profile: string;
    workers: number;
    feed_bytes: number;
    elapsed_ms: number;
    throughput_mib_per_second: number;
    event_loop_lag_p95_ms: number;
    event_loop_lag_samples: number;
    rss_start_bytes: number;
    peak_rss_bytes: number;
    peak_rss_delta_bytes: number;
}

function percentile(values: number[], quantile: number): number {
    if (values.length === 0) return 0;
    const sorted = [...values].sort((left, right) => left - right);
    return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * quantile))];
}

function median(values: number[]): number {
    return percentile(values, 0.5);
}

async function createSources(directory: string): Promise<SourceFile[]> {
    const sources: SourceFile[] = [];
    for (let index = 0; index < FILES; index++) {
        const path = join(directory, `governor-${index}.bin`);
        const handle = await open(path, "w");
        try {
            await handle.truncate(FILE_BYTES);
        } finally {
            await handle.close();
        }
        const metadata = await stat(path);
        sources.push({ path, size: Number(metadata.size), mtime: Number(metadata.mtimeMs) });
    }
    return sources;
}

async function waitForReady(
    pool: NonNullable<ReturnType<typeof createDesktopHashWorkerPool>>,
    expected: number,
): Promise<void> {
    const deadline = Date.now() + 30_000;
    while (pool.stats().ready !== expected) {
        if (Date.now() >= deadline) {
            throw new Error(`worker readiness timed out (${pool.stats().ready}/${expected})`);
        }
        await new Promise<void>((resolve) => setTimeout(resolve, 5));
    }
}

async function runProfile(
    pool: NonNullable<ReturnType<typeof createDesktopHashWorkerPool>>,
    profile: ResourceProfile,
    sources: SourceFile[],
): Promise<RunResult> {
    pool.setActiveWorkerLimit(profile.tuning.hashConcurrency);
    await waitForReady(pool, profile.tuning.hashConcurrency);

    const lagSamples: number[] = [];
    let expected = performance.now() + UI_TICK_MS;
    const rssStart = process.memoryUsage().rss;
    let peakRss = rssStart;
    const timer = setInterval(() => {
        const now = performance.now();
        lagSamples.push(Math.max(0, now - expected));
        expected = now + UI_TICK_MS;
        peakRss = Math.max(peakRss, process.memoryUsage().rss);
    }, UI_TICK_MS);
    const started = performance.now();
    try {
        const results = await Promise.all(sources.map((source) => pool.run({
            absolutePath: source.path,
            expectedSize: source.size,
            expectedMtime: source.mtime,
            mode: "hash",
            feedBytes: profile.tuning.feedBytes,
        })));
        const hashes = new Set(results.map((result) =>
            result.mode === "hash" ? result.hash : "invalid"));
        if (hashes.size !== 1 || hashes.has("invalid")) {
            throw new Error("production workers disagreed on identical file content");
        }
    } finally {
        clearInterval(timer);
    }
    const elapsedMs = performance.now() - started;
    return {
        profile: profile.name,
        workers: profile.tuning.hashConcurrency,
        feed_bytes: profile.tuning.feedBytes,
        elapsed_ms: elapsedMs,
        throughput_mib_per_second: TOTAL_BYTES / MIB / (elapsedMs / 1000),
        event_loop_lag_p95_ms: percentile(lagSamples, 0.95),
        event_loop_lag_samples: lagSamples.length,
        rss_start_bytes: rssStart,
        peak_rss_bytes: peakRss,
        peak_rss_delta_bytes: Math.max(0, peakRss - rssStart),
    };
}

void (async () => {
    const logicalCores = availableParallelism();
    const runtimeArchitecture = arch() === "arm64"
        ? "arm64"
        : arch() === "x64"
            ? "x64"
            : "unknown";
    const runtimeOs = platform() === "win32"
        ? "win32"
        : platform() === "darwin"
            ? "darwin"
            : platform() === "linux"
                ? "linux"
                : "unknown";
    const selected = resourceProfilesFor({
        runtime: "desktop",
        architecture: runtimeArchitecture,
        os: runtimeOs,
        hardwareConcurrency: logicalCores,
        simdAvailable: true,
    });
    const conservative = selected.profiles.find((item) => item.name === "conservative");
    const adaptive = selected.profiles.find((item) => item.name === "balanced");
    if (!conservative || !adaptive || adaptive.family === "generic") {
        throw new Error("governor release benchmark requires x86_64, M1, or Windows ARM64");
    }
    const pool = createDesktopHashWorkerPool(hashWorkerSource, {
        initialWorkers: adaptive.tuning.hashConcurrency,
        maxWorkers: adaptive.tuning.maxHashConcurrency,
    });
    if (!pool) throw new Error("production SIMD worker pool is unavailable");
    const directory = await mkdtemp(join(tmpdir(), "obsetync-governor-bench-"));
    try {
        const sources = await createSources(directory);
        await waitForReady(pool, adaptive.tuning.hashConcurrency);
        // Warm the filesystem cache and WASM instances before alternating runs.
        await runProfile(pool, adaptive, sources);
        const runs: RunResult[] = [];
        for (let iteration = 0; iteration < RUNS_PER_PROFILE; iteration++) {
            const order = iteration % 2 === 0
                ? [conservative, adaptive]
                : [adaptive, conservative];
            for (const selectedProfile of order) {
                runs.push(await runProfile(pool, selectedProfile, sources));
            }
        }
        const conservativeRuns = runs.filter((run) => run.profile === conservative.name);
        const adaptiveRuns = runs.filter((run) => run.profile === adaptive.name);
        const conservativeMedian = median(
            conservativeRuns.map((run) => run.throughput_mib_per_second),
        );
        const adaptiveMedian = median(
            adaptiveRuns.map((run) => run.throughput_mib_per_second),
        );
        const worstLag = Math.max(...runs.map((run) => run.event_loop_lag_p95_ms));
        const throughputGate = adaptiveMedian >= conservativeMedian * 0.95;
        const uiGate = worstLag <= 16;
        const report = {
            schema_version: 1,
            evidence: "actual production SIMD worker/controller run",
            profile_family: adaptive.family,
            runtime: {
                platform: platform(),
                architecture: arch(),
                node: process.version,
                cpu: cpus()[0]?.model ?? "unknown",
                logical_cores: logicalCores,
            },
            workload: {
                files: FILES,
                bytes_per_file: FILE_BYTES,
                total_bytes: TOTAL_BYTES,
                warm_cache: true,
                runs_per_profile: RUNS_PER_PROFILE,
                alternating_order: true,
            },
            profiles: {
                conservative: conservative.tuning,
                adaptive_selected: adaptive.tuning,
            },
            runs,
            medians: {
                conservative_mib_per_second: conservativeMedian,
                adaptive_mib_per_second: adaptiveMedian,
                adaptive_ratio: adaptiveMedian / conservativeMedian,
                worst_event_loop_lag_p95_ms: worstLag,
            },
            gates: {
                adaptive_not_worse_than_conservative_by_more_than_5_percent: throughputGate,
                event_loop_lag_p95_at_most_16_ms: uiGate,
                passed: throughputGate && uiGate,
            },
        };
        const encoded = JSON.stringify(report, null, 2);
        const outputFlag = process.argv.indexOf("--output");
        if (outputFlag >= 0) {
            const outputPath = process.argv[outputFlag + 1];
            if (!outputPath) throw new Error("--output requires a result path");
            await writeFile(outputPath, encoded);
        }
        console.log(encoded);
        if (!report.gates.passed) process.exitCode = 1;
    } finally {
        await pool.close();
        await rm(directory, { recursive: true, force: true });
    }
})().catch((error) => {
    console.error(error);
    process.exitCode = 1;
});
