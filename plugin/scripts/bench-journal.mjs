import { build } from "esbuild";
import { spawn } from "node:child_process";
import { lstat, mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const RUNS = 3;
const DISTINCT_ENTRIES = 25_000;
const SAME_PATH_EDITS = 10_000;
const SUBMISSION_BURST = 256;
const DELAY_RESOLUTION_MS = 10;
const pluginDirectory = fileURLToPath(new URL("../", import.meta.url));
let benchmarkStage = "setup";

/** Serialized into an isolated worker with the actual production journal and
 * native filesystem adapter. No vault, Obsidian runtime, or mock journal. */
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
    assert(mode === "seed" || mode === "cold-load");
    assert(path.isAbsolute(fixtureDirectory));
    const target = relative => {
        const resolved = path.resolve(fixtureDirectory, relative);
        assert(!path.isAbsolute(relative) && resolved.startsWith(fixtureDirectory + path.sep));
        return resolved;
    };
    const metrics = () => ({ readCalls: 0, writeCalls: 0, renameCalls: 0, removeCalls: 0,
        readUtf8Bytes: 0, writeUtf8Bytes: 0, maxReadUtf8Bytes: 0, maxWriteUtf8Bytes: 0,
        walSegmentWrites: 0, snapshotPageWrites: 0, headStageWrites: 0,
        walHeadWrites: 0, snapshotHeadWrites: 0, headPromotions: 0 });
    const totalAdapterIO = metrics();
    let activeMetrics = null;
    const record = update => {
        update(totalAdapterIO);
        if (activeMetrics) update(activeMetrics);
    };
    const adapter = {
        exists: async relative => {
            try { await fs.stat(target(relative)); return true; }
            catch (error) { if (error.code === "ENOENT") return false; throw error; }
        },
        stat: async relative => {
            try {
                const stat = await fs.stat(target(relative));
                return { type: stat.isFile() ? "file" : "folder", size: stat.size };
            } catch (error) { if (error.code === "ENOENT") return null; throw error; }
        },
        read: async relative => {
            const raw = await fs.readFile(target(relative), "utf8");
            const bytes = Buffer.byteLength(raw, "utf8");
            record(io => {
                io.readCalls++;
                io.readUtf8Bytes += bytes;
                io.maxReadUtf8Bytes = Math.max(io.maxReadUtf8Bytes, bytes);
            });
            return raw;
        },
        write: async (relative, raw) => {
            const bytes = Buffer.byteLength(raw, "utf8");
            // This bounded head decode is instrumentation, included in phase
            // timings. Count actual successful native calls, not planned work.
            const head = relative === `${JOURNAL_STORE_PATH}/head.json.next`
                ? JSON.parse(JSON.parse(raw).payload) : null;
            await fs.writeFile(target(relative), raw, "utf8");
            record(io => {
                io.writeCalls++;
                io.writeUtf8Bytes += bytes;
                io.maxWriteUtf8Bytes = Math.max(io.maxWriteUtf8Bytes, bytes);
                if (relative.startsWith(`${JOURNAL_STORE_PATH}/wal-`)) io.walSegmentWrites++;
                if (relative.startsWith(`${JOURNAL_STORE_PATH}/page-`)) io.snapshotPageWrites++;
                if (head) {
                    io.headStageWrites++;
                    if (head.generation === head.snapshot.id) io.snapshotHeadWrites++;
                    else io.walHeadWrites++;
                }
            });
        },
        mkdir: async relative => { await fs.mkdir(target(relative), { recursive: true }); },
        rename: async (from, to) => {
            await fs.rename(target(from), target(to));
            record(io => {
                io.renameCalls++;
                if (from === `${JOURNAL_STORE_PATH}/head.json.next` && to === `${JOURNAL_STORE_PATH}/head.json`) {
                    io.headPromotions++;
                }
            });
        },
        remove: async relative => { await fs.unlink(target(relative)); record(io => { io.removeCalls++; }); },
    };
    const round = value => Math.round(value * 1000) / 1000;
    const phases = {};
    async function measure(name, submissions, bursts, work) {
        workerPhase = name;
        const histogram = monitorEventLoopDelay({ resolution: DELAY_RESOLUTION_MS });
        histogram.enable();
        // Arm outside the timed work. This is a raw Node sampler, not a
        // renderer UI-lag measurement or a responsiveness assertion.
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
                durationMs: round(durationMs), submissions, submissionBursts: bursts,
                submissionsPerSecond: submissions ? round(submissions * 1000 / durationMs) : null,
                walHeadsPerSecond: io.walHeadWrites ? round(io.walHeadWrites * 1000 / durationMs) : null,
                submissionsPerWalHead: submissions && io.walHeadWrites ? round(submissions / io.walHeadWrites) : null,
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
    const baseTs = 1_800_000_000_000;
    const totalAppends = DISTINCT_ENTRIES + SAME_PATH_EDITS;
    const hotIndex = 17;
    const syntheticPath = index => `synthetic/group-${String(Math.floor(index / SUBMISSION_BURST)).padStart(3, "0")}/note-${String(index).padStart(5, "0")}.md`;
    const renameOld = "synthetic/rename-old.md";
    const renameNew = "synthetic/rename-new.md";
    const renameId = totalAppends + 1;
    const postCompactId = totalAppends + 3;
    const postCompactTs = baseTs + totalAppends + 4;
    const createJournal = () => new ObsetyncJournal({ vault: { adapter } });
    const journal = createJournal();
    if (mode === "seed") {
        await measure("initialLoad", 0, 0, () => journal.load());
        const distinctIds = new Uint32Array(DISTINCT_ENTRIES);
        const editedIds = new Uint32Array(SAME_PATH_EDITS);
        async function appendBursts(count, entry, returnedIds) {
            for (let start = 0; start < count; start += SUBMISSION_BURST) {
                const pending = [];
                const end = Math.min(count, start + SUBMISSION_BURST);
                // Submit a whole burst before awaiting: exercise the real
                // wrapper's coalesced durable batch, not 35k serial commits.
                for (let index = start; index < end; index++) pending.push(journal.append(entry(index)));
                returnedIds.set(await Promise.all(pending), start);
            }
        }
        await measure("appendDistinct", DISTINCT_ENTRIES, Math.ceil(DISTINCT_ENTRIES / SUBMISSION_BURST), () =>
            appendBursts(DISTINCT_ENTRIES, index => ({ action: "created", path: syntheticPath(index),
                ts: baseTs + index, synced: false }), distinctIds));
        await measure("appendSamePath", SAME_PATH_EDITS, Math.ceil(SAME_PATH_EDITS / SUBMISSION_BURST), () =>
            appendBursts(SAME_PATH_EDITS, index => ({ action: "modified", path: syntheticPath(hotIndex),
                ts: baseTs + DISTINCT_ENTRIES + index, synced: false }), editedIds));

        workerPhase = "verification";
        for (let index = 0; index < DISTINCT_ENTRIES; index++) assert.equal(distinctIds[index], index + 1);
        for (let index = 0; index < SAME_PATH_EDITS; index++) assert.equal(editedIds[index], DISTINCT_ENTRIES + index + 1);
        assert.equal(journal.unsyncedCount(), DISTINCT_ENTRIES);
        // The newer destination hint must not erase the still-pending old
        // half. Preparation/ACK checks are intentionally outside timings.
        assert.deepEqual(await journal.appendGroup([
            { action: "deleted", path: renameOld, ts: baseTs + totalAppends + 1, synced: false },
            { action: "created", path: renameNew, ts: baseTs + totalAppends + 2, synced: false },
        ]), [renameId, renameId]);
        assert.equal(await journal.append({ action: "modified", path: renameNew,
            ts: baseTs + totalAppends + 3, synced: false }), totalAppends + 2);
        await journal.acknowledge([{ path: renameNew, throughId: totalAppends + 2 }]);
        // compact() is the authoritative journal snapshot/checkpoint API;
        // append batches already checkpoint their WAL suffix through a head.
        await measure("compact", 0, 0, () => journal.compact());
        let returnedId;
        await measure("postCompactAppend", 1, 1, async () => {
            returnedId = await journal.append({ action: "modified", path: syntheticPath(hotIndex),
                ts: postCompactTs, synced: false });
        });
        assert.equal(returnedId, postCompactId);
    } else {
        await measure("coldLoad", 0, 0, () => journal.load());
    }

    workerPhase = "verification";
    function verifyPending(loaded, includeRenameHalf) {
        const seen = new Uint8Array(DISTINCT_ENTRIES);
        let count = 0;
        let previousId = 0;
        let oldHalfCount = 0;
        for (const entry of loaded.iterateUnsynced()) {
            assert(entry.id > previousId);
            previousId = entry.id;
            assert.equal(entry.synced, false);
            assert.equal(entry.oldPath, undefined);
            if (entry.path === renameOld) {
                assert(includeRenameHalf);
                assert.equal(entry.id, renameId);
                assert.equal(entry.action, "deleted");
                assert.equal(entry.ts, baseTs + totalAppends + 2);
                oldHalfCount++;
            } else {
                const match = /\/note-([0-9]{5})\.md$/.exec(entry.path);
                assert(match);
                const index = Number(match[1]);
                assert(index < DISTINCT_ENTRIES && !seen[index]);
                seen[index] = 1;
                assert.equal(entry.path, syntheticPath(index));
                assert.equal(entry.id, index === hotIndex ? postCompactId : index + 1);
                assert.equal(entry.action, index === hotIndex ? "modified" : "created");
                assert.equal(entry.ts, index === hotIndex ? postCompactTs : baseTs + index);
            }
            count++;
        }
        for (const present of seen) assert.equal(present, 1);
        assert.equal(oldHalfCount, includeRenameHalf ? 1 : 0);
        assert.equal(count, DISTINCT_ENTRIES + (includeRenameHalf ? 1 : 0));
        assert.equal(loaded.unsyncedCount(), count);
    }
    verifyPending(journal, true);
    const head = JSON.parse(JSON.parse(await fs.readFile(target(`${JOURNAL_STORE_PATH}/head.json`), "utf8")).payload);
    assert(head.generation > head.snapshot.id);
    assert(head.sequence > head.snapshot.cut);
    assert(head.walStart <= head.walEnd);
    assert.equal(head.snapshot.metadata.nextId, postCompactId);
    for (const phase of Object.values(phases)) {
        assert(phase.maxReadUtf8Bytes <= STORE_LIMITS.frameBytes);
        assert(phase.maxWriteUtf8Bytes <= STORE_LIMITS.frameBytes);
        assert.equal(phase.headStageWrites, phase.headPromotions);
    }
    const measuredAdapterIO = { ...totalAdapterIO };
    let diskLogicalBytes = 0;
    let diskAllocatedBytes = 0;
    let fileCount = 0;
    const directories = [fixtureDirectory];
    while (directories.length) {
        const directory = directories.pop();
        for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
            assert(!entry.isSymbolicLink());
            const full = path.join(directory, entry.name);
            if (entry.isDirectory()) directories.push(full);
            else {
                const stat = await fs.stat(full);
                assert(stat.isFile());
                diskLogicalBytes += stat.size;
                diskAllocatedBytes += stat.blocks * 512;
                fileCount++;
            }
        }
    }
    let restartHighwaterVerified = false;
    let completedRenameAckVerified = false;
    if (mode === "cold-load") {
        // Untimed native writes: prove nextId survived the fresh-process load,
        // then retire the remaining rename half and reload a second snapshot.
        const probe = "synthetic/highwater-probe.md";
        assert.equal(await journal.append({ action: "created", path: probe,
            ts: postCompactTs + 1, synced: false }), totalAppends + 4);
        await journal.acknowledge([{ path: renameOld, throughId: renameId },
            { path: probe, throughId: totalAppends + 4 }]);
        await journal.compact();
        const reloaded = createJournal();
        await reloaded.load();
        verifyPending(reloaded, false);
        completedRenameAckVerified = true;
        assert.equal(await reloaded.append({ action: "created", path: probe,
            ts: postCompactTs + 2, synced: false }), totalAppends + 5);
        await reloaded.acknowledge([{ path: probe, throughId: totalAppends + 5 }]);
        restartHighwaterVerified = true;
    }
    process.stdout.write(JSON.stringify({ phases, adapterIOBeforeVerificationProbes: measuredAdapterIO,
        diskLogicalBytes, diskAllocatedBytes, fileCount,
        verification: { pendingEntries: DISTINCT_ENTRIES + 1, allPendingIdentities: true,
            allReturnedAppendIds: mode === "seed", coalescedSamePath: true, renameHalfSurvivedCompact: true,
            renameHalfSurvivedFreshProcess: mode === "cold-load", restartHighwaterVerified,
            completedRenameAckVerified, snapshotPages: head.snapshot.pages,
            postSnapshotSegments: head.walEnd - head.walStart + 1,
            sequence: head.sequence, snapshotCut: head.snapshot.cut, nextAppendId: totalAppends + 4 } }));
    completed = true;
}

async function buildWorkerBundle() {
    return build({
        absWorkingDir: pluginDirectory,
        stdin: {
            contents: `
                import * as fs from "node:fs/promises";
                import * as path from "node:path";
                import { strict as assert } from "node:assert";
                import { monitorEventLoopDelay, performance } from "node:perf_hooks";
                import { setImmediate as immediate, setTimeout as sleep } from "node:timers/promises";
                import { ObsetyncJournal, JOURNAL_STORE_PATH } from "./src/journal";
                import { STORE_LIMITS } from "./src/segmented-store";
                const DISTINCT_ENTRIES = ${DISTINCT_ENTRIES};
                const SAME_PATH_EDITS = ${SAME_PATH_EDITS};
                const SUBMISSION_BURST = ${SUBMISSION_BURST};
                const DELAY_RESOLUTION_MS = ${DELAY_RESOLUTION_MS};
                (${fixtureWorker.toString()})().catch(error => {
                    const code = typeof error?.code === "string" && /^[A-Z0-9_]{1,64}$/.test(error.code)
                        ? error.code : "WORKER_FAILURE";
                    process.stderr.write(JSON.stringify({ error: "fixture-failed", code }));
                    process.exitCode = 1;
                });
            `,
            sourcefile: "journal-native-fixture.ts", resolveDir: pluginDirectory, loader: "ts",
        },
        bundle: true, platform: "node", format: "cjs", target: "node20", write: false, logLevel: "silent",
    });
}

function runWorker(bundlePath, mode, directory) {
    return new Promise((resolve, reject) => {
        const child = spawn(process.execPath, [bundlePath, mode, directory], { stdio: ["ignore", "pipe", "pipe"] });
        let stdout = "";
        let stderr = "";
        let stoppedCode = null;
        const timeout = setTimeout(() => { stoppedCode = "WORKER_TIMEOUT"; child.kill("SIGKILL"); }, 180_000);
        child.stdout.on("data", chunk => {
            if (stoppedCode) return;
            stdout += chunk;
            if (stdout.length > 1024 * 1024) { stoppedCode = "WORKER_OUTPUT_LIMIT"; child.kill("SIGKILL"); }
        });
        child.stderr.on("data", chunk => { stderr = (stderr + chunk).slice(0, 4096); });
        child.once("error", error => { clearTimeout(timeout); reject(error); });
        child.once("close", code => {
            clearTimeout(timeout);
            if (code !== 0 || stoppedCode) {
                let diagnostic;
                try { diagnostic = JSON.parse(stderr); } catch { diagnostic = { code: "NO_WORKER_REPORT" }; }
                reject(Object.assign(new Error("journal benchmark worker failed"), { code: stoppedCode ?? diagnostic.code }));
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
    const args = process.argv.slice(2);
    if (args.length > 1 || (args.length === 1 && args[0] !== "--check")) {
        throw Object.assign(new Error("invalid benchmark arguments"), { code: "INVALID_ARGUMENTS" });
    }
    benchmarkStage = "bundle";
    const bundled = await buildWorkerBundle();
    if (args[0] === "--check") {
        // Compile only: do not create a fixture, start workers, seed rows, or
        // run any sampler/timing when another validation workload is active.
        process.stdout.write(JSON.stringify({ benchmark: "journal-native-filesystem-sanity",
            check: "bundle-only", timingsRun: false, bundleBytes: bundled.outputFiles[0].contents.length }) + "\n");
        return;
    }
    benchmarkStage = "setup";
    const temporaryParent = await realpath("/tmp");
    const created = await mkdtemp(join(temporaryParent, "obsetync-journal-bench-"));
    const fixtureRoot = await realpath(created);
    async function validateOwnedRoot() {
        const stat = await lstat(fixtureRoot);
        if (!stat.isDirectory() || stat.isSymbolicLink() || dirname(fixtureRoot) !== temporaryParent ||
            !/^obsetync-journal-bench-[A-Za-z0-9]+$/.test(basename(fixtureRoot)) ||
            await realpath(fixtureRoot) !== fixtureRoot) {
            throw Object.assign(new Error("invalid temporary fixture ownership"), { code: "FIXTURE_OWNERSHIP" });
        }
    }
    let report;
    try {
        await validateOwnedRoot();
        const bundlePath = join(fixtureRoot, "benchmark.cjs");
        await writeFile(bundlePath, bundled.outputFiles[0].contents);
        const runs = [];
        for (let run = 1; run <= RUNS; run++) {
            benchmarkStage = "seed";
            const directory = join(fixtureRoot, `run-${run}`);
            await mkdir(directory);
            const seed = await runWorker(bundlePath, "seed", directory);
            benchmarkStage = "cold-load";
            // Fresh process and heap, deliberately not an evicted OS cache.
            const cold = await runWorker(bundlePath, "cold-load", directory);
            runs.push({ run, phases: { ...seed.phases, ...cold.phases },
                adapterIOBeforeVerificationProbes: { seed: seed.adapterIOBeforeVerificationProbes,
                    coldLoad: cold.adapterIOBeforeVerificationProbes },
                diskLogicalBytes: cold.diskLogicalBytes, diskAllocatedBytes: cold.diskAllocatedBytes,
                fileCount: cold.fileCount,
                verification: { ...cold.verification, allReturnedAppendIds: seed.verification.allReturnedAppendIds } });
        }
        benchmarkStage = "report";
        const medians = Object.fromEntries(Object.keys(runs[0].phases).map(name => {
            const phases = runs.map(run => run.phases[name]);
            const sampled = phases.filter(phase => phase.eventLoopDelayP95Ms !== null);
            const medianIfMeasured = field => phases.every(phase => phase[field] !== null)
                ? median(phases.map(phase => phase[field])) : null;
            return [name, { durationMs: median(phases.map(phase => phase.durationMs)),
                submissionsPerSecond: medianIfMeasured("submissionsPerSecond"),
                walHeadsPerSecond: medianIfMeasured("walHeadsPerSecond"),
                submissionsPerWalHead: medianIfMeasured("submissionsPerWalHead"),
                walHeadWrites: median(phases.map(phase => phase.walHeadWrites)),
                snapshotHeadWrites: median(phases.map(phase => phase.snapshotHeadWrites)),
                headPromotions: median(phases.map(phase => phase.headPromotions)),
                eventLoopDelayP95Ms: sampled.length === RUNS ? median(sampled.map(phase => phase.eventLoopDelayP95Ms)) : null,
                sampledRuns: sampled.length,
                maxReadUtf8Bytes: Math.max(...phases.map(phase => phase.maxReadUtf8Bytes)),
                maxWriteUtf8Bytes: Math.max(...phases.map(phase => phase.maxWriteUtf8Bytes)) }];
        }));
        report = {
            benchmark: "journal-native-filesystem-sanity", schema: 1,
            platform: process.platform, arch: process.arch, node: process.version,
            corpus: { distinctEntries: DISTINCT_ENTRIES, samePathEdits: SAME_PATH_EDITS,
                submissionBurst: SUBMISSION_BURST, syntheticMetadataOnly: true, vaultContentBytes: 0,
                rename: "one linked rename, newer destination edit and destination ACK before compaction" },
            measurement: { runs: RUNS, eventLoopResolutionMs: DELAY_RESOLUTION_MS,
                eventLoopDelay: "raw Node sampler delay, includes sampling interval; not renderer UI lag",
                coldLoad: "fresh Node process, filesystem cache not evicted; module startup excluded",
                writes: "native filesystem completion and production readback, no fsync guarantee",
                batches: "adjacent actual wrapper submissions; count WAL heads and snapshot heads separately",
                checkpoint: "append batches publish their head; compact is the explicit journal snapshot/checkpoint",
                instrumentation: "native IO counters and bounded head classification included in timings",
                diskBytes: "fixture data before untimed verification probes; generated benchmark bundle excluded",
                verification: "fresh-process identity/highwater check; then untimed rename retirement, compact and same-process new-instance reload",
                verificationIncludedInTimings: false, performanceGate: null,
                scope: "native temporary fixture; not Obsidian/WebView/iPhone or production-vault proof" },
            medians, diskLogicalBytesMedian: median(runs.map(run => run.diskLogicalBytes)),
            diskAllocatedBytesMedian: median(runs.map(run => run.diskAllocatedBytes)), runs,
            cleanup: "owned temporary fixture removed",
        };
    } finally {
        await validateOwnedRoot();
        await rm(fixtureRoot, { recursive: true, force: true });
    }
    process.stdout.write(JSON.stringify(report, null, 2) + "\n");
}

main().catch(error => {
    const code = typeof error?.code === "string" && /^[A-Z0-9_]{1,64}$/.test(error.code)
        ? error.code : "BENCHMARK_FAILURE";
    process.stderr.write(JSON.stringify({ error: "journal-sanity-failed", stage: benchmarkStage, code }) + "\n");
    process.exitCode = 1;
});
