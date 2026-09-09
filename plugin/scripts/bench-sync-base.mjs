import { build } from "esbuild";
import { spawn } from "node:child_process";
import { lstat, mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const RUNS = 3;
const ENTRIES = 25_000;
const SEED_BATCH = 256;
const DELAY_RESOLUTION_MS = 10;
const pluginDirectory = fileURLToPath(new URL("../", import.meta.url));
let benchmarkStage = "setup";

/** Serialized into the generated fixture worker below. Its imported bindings
 * are real Node filesystem APIs and the actual production sync-base classes. */
async function fixtureWorker() {
    let workerPhase = "setup";
    let completed = false;
    process.once("beforeExit", () => {
        if (!completed && !process.exitCode) {
            process.stderr.write(JSON.stringify({ error: "fixture-unfinished", code: "UNFINISHED_" + workerPhase.toUpperCase() }));
            process.exitCode = 1;
        }
    });
    const mode = process.argv[2];
    const fixtureDirectory = await fs.realpath(process.argv[3]);
    if ((mode !== "seed" && mode !== "cold-load") || !path.isAbsolute(fixtureDirectory)) {
        throw new Error("invalid benchmark invocation");
    }
    const metrics = () => ({ readCalls: 0, writeCalls: 0, readUtf8Bytes: 0, writeUtf8Bytes: 0,
        maxReadUtf8Bytes: 0, maxWriteUtf8Bytes: 0 });
    let activeMetrics = null;
    const target = relative => {
        const resolved = path.resolve(fixtureDirectory, relative);
        if (path.isAbsolute(relative) || !resolved.startsWith(fixtureDirectory + path.sep)) {
            throw new Error("benchmark adapter target escaped its fixture");
        }
        return resolved;
    };
    const adapter = {
        exists: async relative => {
            try { await fs.stat(target(relative)); return true; }
            catch (error) { if (error.code === "ENOENT") return false; throw error; }
        },
        stat: async relative => {
            try {
                const result = await fs.stat(target(relative));
                return { type: result.isFile() ? "file" : "folder", size: result.size };
            } catch (error) { if (error.code === "ENOENT") return null; throw error; }
        },
        read: async relative => {
            const raw = await fs.readFile(target(relative), "utf8");
            if (activeMetrics) {
                const bytes = Buffer.byteLength(raw, "utf8");
                activeMetrics.readCalls++;
                activeMetrics.readUtf8Bytes += bytes;
                activeMetrics.maxReadUtf8Bytes = Math.max(activeMetrics.maxReadUtf8Bytes, bytes);
            }
            return raw;
        },
        write: async (relative, raw) => {
            if (activeMetrics) {
                const bytes = Buffer.byteLength(raw, "utf8");
                activeMetrics.writeCalls++;
                activeMetrics.writeUtf8Bytes += bytes;
                activeMetrics.maxWriteUtf8Bytes = Math.max(activeMetrics.maxWriteUtf8Bytes, bytes);
            }
            await fs.writeFile(target(relative), raw, "utf8");
        },
        mkdir: async relative => { await fs.mkdir(target(relative), { recursive: true }); },
        rename: async (from, to) => { await fs.rename(target(from), target(to)); },
        remove: async relative => { await fs.unlink(target(relative)); },
    };
    const round = value => Math.round(value * 1000) / 1000;
    const phases = {};
    async function measure(name, work) {
        workerPhase = name;
        const histogram = monitorEventLoopDelay({ resolution: DELAY_RESOLUTION_MS });
        histogram.enable();
        // Let the sampler arm before timing work; this delay is not part of
        // the reported phase duration and is not a UI/responsiveness assertion.
        await sleep(DELAY_RESOLUTION_MS * 2);
        histogram.reset();
        const io = metrics();
        activeMetrics = io;
        const started = performance.now();
        try {
            await work();
            const durationMs = performance.now() - started;
            await immediate();
            const samples = Number(histogram.count);
            phases[name] = {
                durationMs: round(durationMs),
                eventLoopSamples: samples,
                eventLoopDelayP95Ms: samples ? round(histogram.percentile(95) / 1e6) : null,
                eventLoopDelayMaxMs: samples ? round(histogram.max / 1e6) : null,
                ...io,
            };
        } finally {
            activeMetrics = null;
            histogram.disable();
        }
    }
    const syntheticPath = index => `synthetic/group-${String(Math.floor(index / SEED_BATCH)).padStart(3, "0")}/note-${String(index).padStart(5, "0")}.md`;
    const hash = index => (index + 1).toString(16).padStart(64, "0");
    const changedIndex = 17;
    const changedHash = "e".repeat(64);
    const marker = nextCursorHex => ({
        version: 1, vaultId: "synthetic-benchmark", fromRoot: "1".repeat(64), toRoot: "2".repeat(64),
        nextCursorHex, complete: false, recordsSeen: ENTRIES, filesApplied: ENTRIES,
        bytesTotal: ENTRIES, downloaded: 0, bytesDownloaded: 0, deltasHadMtime: true,
    });
    const base = new ObsetyncSyncBase({ vault: { adapter } });
    if (mode === "seed") {
        await measure("initialLoad", () => base.load());
        await measure("seed", async () => {
            for (let start = 0; start < ENTRIES; start += SEED_BATCH) {
                const end = Math.min(ENTRIES, start + SEED_BATCH);
                for (let index = start; index < end; index++) {
                    base.setEntry(syntheticPath(index), hash(index), 1_800_000_000_000 + index, index % 4096,
                        1_799_999_999_000 + index);
                }
                await immediate();
            }
            base.setTreeBaseRoot("1".repeat(64));
            base.setLastSyncTimestamp(1_800_000_000_000);
            base.setDiffPageCheckpoint(marker("abcd"));
        });
        await measure("checkpoint", () => base.checkpoint());
        await measure("save", () => base.save());
        // Exercise a later committed generation after the snapshot cut. Cold
        // recovery must combine snapshot pages with this exact WAL suffix.
        base.setEntry(syntheticPath(changedIndex), changedHash, 1_800_000_100_000, 777, 1_800_000_099_000);
        base.setTreeBaseRoot("2".repeat(64));
        base.setDiffPageCheckpoint(marker("bcde"));
        await measure("postSnapshotCheckpoint", () => base.checkpoint());
    } else {
        await measure("coldLoad", () => base.load());
    }

    // Verification is intentionally outside the phase timings and sampler.
    workerPhase = "verification";
    assert.equal(base.entryCount(), ENTRIES);
    for (let index = 0; index < ENTRIES; index++) {
        assert.equal(base.getHash(syntheticPath(index)), index === changedIndex ? changedHash : hash(index));
    }
    assert.equal(base.treeBaseRoot, "2".repeat(64));
    assert.equal(base.lastSyncTimestamp, 1_800_000_000_000);
    assert.equal(base.diffPageCheckpoint?.nextCursorHex, "bcde");
    assert.equal(base.diffPageCheckpoint?.filesApplied, ENTRIES);
    assert.equal(base.getTreeMtime(syntheticPath(changedIndex)), 1_800_000_099_000);
    const head = JSON.parse(JSON.parse(await fs.readFile(target(`${SYNC_BASE_STORE_PATH}/head.json`), "utf8")).payload);
    assert(head.generation > head.snapshot.id);
    assert(head.sequence > head.snapshot.cut);
    assert(head.walStart <= head.walEnd);
    for (const phase of Object.values(phases)) {
        assert(phase.maxReadUtf8Bytes <= STORE_LIMITS.frameBytes);
        assert(phase.maxWriteUtf8Bytes <= STORE_LIMITS.frameBytes);
    }
    let diskLogicalBytes = 0;
    let diskAllocatedBytes = 0;
    let fileCount = 0;
    const directories = [fixtureDirectory];
    while (directories.length) {
        const directory = directories.pop();
        for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
            // The fixture is private and contains no symlinks; avoid following
            // one if that ownership invariant is ever broken.
            assert(!entry.isSymbolicLink());
            const full = path.join(directory, entry.name);
            if (entry.isDirectory()) directories.push(full);
            else {
                const stat = await fs.stat(full);
                diskLogicalBytes += stat.size;
                diskAllocatedBytes += stat.blocks * 512;
                fileCount++;
            }
        }
    }
    process.stdout.write(JSON.stringify({ phases, diskLogicalBytes, diskAllocatedBytes, fileCount,
        verification: { entries: ENTRIES, allHashes: true, cursor: true, treeRoot: true,
            separateTreeMtime: true, newerGeneration: true, snapshotPages: head.snapshot.pages,
            postSnapshotSegments: head.walEnd - head.walStart + 1, sequence: head.sequence,
            snapshotCut: head.snapshot.cut } }));
    completed = true;
}

function runWorker(bundlePath, mode, directory) {
    return new Promise((resolve, reject) => {
        const child = spawn(process.execPath, [bundlePath, mode, directory], { stdio: ["ignore", "pipe", "pipe"] });
        let stdout = "";
        let stderr = "";
        let exceeded = false;
        const timeout = setTimeout(() => { child.kill("SIGKILL"); }, 180_000);
        child.stdout.on("data", chunk => {
            stdout += chunk;
            if (stdout.length > 1024 * 1024) { exceeded = true; child.kill("SIGKILL"); }
        });
        child.stderr.on("data", chunk => {
            if (stderr.length < 4096) stderr += chunk;
        });
        child.once("error", error => { clearTimeout(timeout); reject(error); });
        child.once("close", code => {
            clearTimeout(timeout);
            if (code !== 0 || exceeded) {
                // Worker diagnostics are fixed-shape/path-free, never native
                // exception text or a stack containing filesystem locations.
                let diagnostic;
                try { diagnostic = JSON.parse(stderr); } catch { diagnostic = { code: "NO_WORKER_REPORT" }; }
                reject(Object.assign(new Error("sync-base benchmark worker failed"), { code: diagnostic.code }));
                return;
            }
            try { resolve(JSON.parse(stdout)); }
            catch { reject(Object.assign(new Error("invalid benchmark worker report"), {
                code: stdout.trim() ? "INVALID_WORKER_REPORT" : "EMPTY_WORKER_REPORT",
            })); }
        });
    });
}

const median = values => [...values].sort((left, right) => left - right)[Math.floor(values.length / 2)];

async function main() {
    const temporaryParent = await realpath("/tmp");
    const created = await mkdtemp(join(temporaryParent, "obsetync-sync-base-bench-"));
    const fixtureRoot = await realpath(created);
    let report;
    try {
        benchmarkStage = "bundle";
        const bundled = await build({
            absWorkingDir: pluginDirectory,
            stdin: {
                contents: `
                    import * as fs from "node:fs/promises";
                    import * as path from "node:path";
                    import { strict as assert } from "node:assert";
                    import { monitorEventLoopDelay, performance } from "node:perf_hooks";
                    import { setImmediate as immediate, setTimeout as sleep } from "node:timers/promises";
                    import { ObsetyncSyncBase, SYNC_BASE_STORE_PATH } from "./src/sync-base";
                    import { STORE_LIMITS } from "./src/segmented-store";
                    const ENTRIES = ${ENTRIES};
                    const SEED_BATCH = ${SEED_BATCH};
                    const DELAY_RESOLUTION_MS = ${DELAY_RESOLUTION_MS};
                    (${fixtureWorker.toString()})().catch(error => {
                        const code = typeof error?.code === "string" && /^[A-Z0-9_]{1,64}$/.test(error.code)
                            ? error.code : "WORKER_FAILURE";
                        process.stderr.write(JSON.stringify({ error: "fixture-failed", code }));
                        process.exitCode = 1;
                    });
                `,
                sourcefile: "sync-base-native-fixture.ts", resolveDir: pluginDirectory, loader: "ts",
            },
            bundle: true, platform: "node", format: "cjs", target: "node20",
            write: false, logLevel: "silent",
        });
        const bundlePath = join(fixtureRoot, "benchmark.cjs");
        await writeFile(bundlePath, bundled.outputFiles[0].contents);
        const runs = [];
        for (let run = 1; run <= RUNS; run++) {
            benchmarkStage = "seed";
            const directory = join(fixtureRoot, `run-${run}`);
            await mkdir(directory);
            const seed = await runWorker(bundlePath, "seed", directory);
            // Separate process: no live index, module cache or JS heap from
            // seeding is reused. Filesystem caches are deliberately NOT evicted.
            benchmarkStage = "cold-load";
            const cold = await runWorker(bundlePath, "cold-load", directory);
            runs.push({ run, phases: { ...seed.phases, ...cold.phases },
                diskLogicalBytes: cold.diskLogicalBytes, diskAllocatedBytes: cold.diskAllocatedBytes,
                fileCount: cold.fileCount, verification: cold.verification });
        }
        benchmarkStage = "report";
        const phaseNames = Object.keys(runs[0].phases);
        const medians = Object.fromEntries(phaseNames.map(name => {
            const phases = runs.map(run => run.phases[name]);
            const sampled = phases.map(phase => phase.eventLoopDelayP95Ms).filter(value => value !== null);
            return [name, {
                durationMs: median(phases.map(phase => phase.durationMs)),
                eventLoopDelayP95Ms: sampled.length === RUNS ? median(sampled) : null,
                sampledRuns: sampled.length,
                maxReadUtf8Bytes: Math.max(...phases.map(phase => phase.maxReadUtf8Bytes)),
                maxWriteUtf8Bytes: Math.max(...phases.map(phase => phase.maxWriteUtf8Bytes)),
            }];
        }));
        report = {
            benchmark: "sync-base-native-filesystem-sanity", schema: 1,
            platform: process.platform, arch: process.arch, node: process.version,
            corpus: { entries: ENTRIES, seedBatch: SEED_BATCH, syntheticMetadataOnly: true,
                vaultContentBytes: 0, hashes: "deterministic synthetic identifiers, no content hashing" },
            measurement: { runs: RUNS, eventLoopResolutionMs: DELAY_RESOLUTION_MS,
                eventLoopDelay: "raw Node sampler delay, includes sampling interval; not renderer UI lag",
                coldLoad: "fresh Node process, filesystem cache not evicted; module startup excluded",
                writes: "native filesystem completion and production readback, no fsync guarantee",
                diskBytes: "fixture data files only; generated benchmark bundle excluded",
                verificationIncludedInTimings: false, performanceGate: null,
                scope: "bounded native temporary fixture; not Obsidian/WebView/iPhone or production-vault proof" },
            medians, diskLogicalBytesMedian: median(runs.map(run => run.diskLogicalBytes)),
            diskAllocatedBytesMedian: median(runs.map(run => run.diskAllocatedBytes)), runs,
            cleanup: "owned temporary fixture removed",
        };
    } finally {
        const stat = await lstat(fixtureRoot);
        if (!stat.isDirectory() || stat.isSymbolicLink() || dirname(fixtureRoot) !== temporaryParent ||
            !/^obsetync-sync-base-bench-[A-Za-z0-9]+$/.test(basename(fixtureRoot)) ||
            await realpath(fixtureRoot) !== fixtureRoot) {
            throw new Error("temporary benchmark fixture ownership could not be validated");
        }
        await rm(fixtureRoot, { recursive: true, force: true });
    }
    process.stdout.write(JSON.stringify(report, null, 2) + "\n");
}

main().catch(error => {
    const code = typeof error?.code === "string" && /^[A-Z0-9_]{1,64}$/.test(error.code)
        ? error.code : "BENCHMARK_FAILURE";
    process.stderr.write(JSON.stringify({ error: "sync-base-sanity-failed", stage: benchmarkStage, code }) + "\n");
    process.exitCode = 1;
});
