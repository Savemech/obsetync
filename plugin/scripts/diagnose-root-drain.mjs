import { strict as assert } from "node:assert";
import { build } from "esbuild";
import { spawn } from "node:child_process";
import { lstat, mkdtemp, readFile, realpath, rm } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const pluginDirectory = fileURLToPath(new URL("../", import.meta.url));
const counts = [600, 2500];
const arguments_ = process.argv.slice(2);
if (arguments_.length === 1 && arguments_[0] === "--help") {
    process.stdout.write(`Usage: node scripts/diagnose-root-drain.mjs [--help]

Runs the actual engine drain for 600 and 2500 independent synthetic dirty paths.
Reports observed stat calls, full audit plus selected revalidation input lengths,
cooperative graph creation/selection counters, per-path visit multiplicities,
read-only readiness visits, bounded dirty claims/take/restore work, completed
cooperative yields and root-continuation turns.
Assertions check one full review plus one selected re-stat (2N), one admitted
graph, bounded exact claims, exact roots/base/journal and absence of abandoned
cooperative work. This is not a throughput acceptance gate.

Timings are one instrumented run per size, not medians or an old/new comparison.
The reused fixture has a synthetic JSON tree/server, one-byte sources, all
content objects present and MemorySegmentedIO. Its server performs an extra
cloned RootIntentStore readback per root; measured whole-drain time includes it.
The 16 ms heartbeat measures Node callback gaps, not active-note commit latency,
Obsidian responsiveness, native storage performance, mobile suspension or RSS.

Only a validated private /tmp/obsetync-root-drain-counts-* bundle is written;
it is removed after the strict child exits. No vault/network/source edits occur.
`);
    process.exit(0);
}
assert.equal(arguments_.length, 0, "unsupported diagnostic arguments; use --help");
const directory = await mkdtemp("/tmp/obsetync-root-drain-counts-");
function replaceOnce(source, before, after) {
    assert.equal(source.split(before).length, 2, "diagnostic source seam changed");
    return source.replace(before, after);
}

/** A work-count diagnostic, not a throughput gate. The same actual engine,
 * journal/base/runtime and test server ports as engine-root.test are reused
 * through their shared side-effect-free fixture module. Production/test files
 * on disk are never rewritten. Transparent wrappers
 * count actual calls, not a simulated queue algorithm or an old baseline. */
async function childMain() {
    let completed = false, phase = "setup";
    process.once("beforeExit", () => {
        if (!completed && !process.exitCode) {
            process.stderr.write(JSON.stringify({ error: "diagnostic-unfinished", phase }) + "\n");
            process.exitCode = 1;
        }
    });
    assert(process.execArgv.includes("--unhandled-rejections=strict"));
    globalThis.__obsetyncTestNotice = () => ({ setMessage() {}, hide() {} });
    globalThis.__obsetyncTestDebounce = () => () => {};
    // Expected engine logs contain synthetic paths but would swamp counters.
    // Assertion/child failures still reach stderr through the explicit catch.
    console.log = console.warn = () => {};
    const results = [];
    const round = value => Math.round(value * 1000) / 1000;
    const percentile = (values, fraction) => values.length
        ? round([...values].sort((left, right) => left - right)[Math.ceil(values.length * fraction) - 1]) : null;
    for (const count of COUNTS) {
        phase = "seed";
        const f = await fixture(count), a = await f.create("work-counts");
        let heartbeat;
        try {
            const paths = Array.from({ length: count }, (_, index) => pathAt(index));
            await f.queue(a.engine, paths, 2);
            const work = {
                statCalls: 0, statMultiplicity: new Map(),
                materialize: [], materializeMs: 0,
                planner: [], plannerMs: 0, plans: [],
                hostYieldsRequested: 0, hostYieldsCompleted: 0, rootContinuations: 0,
                readinessCalls: 0, readinessHintVisits: 0, readinessTakeCalls: 0, readinessRestoreCalls: 0,
                dirtyTakeLengths: [], dirtyRestoreLengths: [], dirtyClaimLengths: [], dirtyClaimedPaths: 0,
            };
            let readinessDepth = 0;
            const readiness = a.engine.hasRunnablePendingChanges.bind(a.engine);
            a.engine.hasRunnablePendingChanges = (...args) => {
                work.readinessCalls++; readinessDepth++;
                try { return readiness(...args); } finally { readinessDepth--; }
            };
            const iterate = a.engine.pendingChanges.iterate.bind(a.engine.pendingChanges);
            a.engine.pendingChanges.iterate = (...args) => {
                const view = iterate(...args), duringReadiness = readinessDepth > 0;
                return (function* () {
                    for (const hint of view) { if (duringReadiness) work.readinessHintVisits++; yield hint; }
                })();
            };
            const take = a.engine.pendingChanges.take.bind(a.engine.pendingChanges);
            a.engine.pendingChanges.take = (...args) => {
                if (readinessDepth > 0) work.readinessTakeCalls++;
                const result = take(...args); work.dirtyTakeLengths.push(result.length); return result;
            };
            const claim = a.engine.pendingChanges.claimHints.bind(a.engine.pendingChanges);
            a.engine.pendingChanges.claimHints = (...args) => {
                work.dirtyClaimLengths.push(args[0].length);
                const result = claim(...args);
                if (result) work.dirtyClaimedPaths += result.length;
                return result;
            };
            const restore = a.engine.pendingChanges.restore.bind(a.engine.pendingChanges);
            a.engine.pendingChanges.restore = (...args) => {
                if (readinessDepth > 0) work.readinessRestoreCalls++;
                work.dirtyRestoreLengths.push(args[0].length); return restore(...args);
            };
            const stat = a.engine.io.stat.bind(a.engine.io);
            a.engine.io.stat = async (...args) => {
                work.statCalls++;
                work.statMultiplicity.set(args[0], (work.statMultiplicity.get(args[0]) ?? 0) + 1);
                return stat(...args);
            };
            const materialize = a.engine.materializeDirtyChanges.bind(a.engine);
            a.engine.materializeDirtyChanges = async (...args) => {
                work.materialize.push(args[0].length);
                const started = performance.now();
                try { return await materialize(...args); }
                finally { work.materializeMs += performance.now() - started; }
            };
            const heavy = a.engine.waitForHeavyWork.bind(a.engine);
            a.engine.waitForHeavyWork = (...args) => {
                if (args[0] === "root continuation") work.rootContinuations++;
                return heavy(...args);
            };
            const heartbeatGaps = [];
            let previousHeartbeat = performance.now();
            heartbeat = setInterval(() => {
                const now = performance.now(); heartbeatGaps.push(now - previousHeartbeat); previousHeartbeat = now;
            }, 16);
            globalThis.__rootDrainWork = work;
            phase = "drain";
            const started = performance.now();
            await a.engine.pushPending();
            const durationMs = performance.now() - started;
            globalThis.__rootDrainWork = undefined;
            clearInterval(heartbeat); heartbeat = undefined;
            phase = "verify";
            const expectedBatches = [];
            for (let left = count; left > 0; left -= 256) expectedBatches.push(Math.min(left, 256));
            const expectedLengths = [count, ...expectedBatches], expectedVisits = 2 * count;
            assert.deepEqual(work.materialize, expectedLengths);
            assert.equal(work.plans.length, 1, "quiet drain rebuilt its reviewed dependency graph");
            const plan = work.plans[0];
            assert.equal(plan.queued, count); assert.equal(plan.ready, count); assert.equal(plan.dependencies, 0);
            assert.equal(plan.initial.preflightRows, 2 * count); assert.equal(plan.initial.graphSteps, 5 * count);
            assert.equal(plan.latest.selectedPaths, count); assert.equal(plan.latest.remainingEligiblePaths, 0);
            assert(plan.latest.selectionComponentVisits <= count + expectedBatches.length);
            assert.equal(plan.latest.disposed, true, "drain retained its reviewed plan after completion");
            assert.deepEqual(plan.batches.filter(size => size > 0), expectedBatches);
            assert.deepEqual(work.planner, [], "quiet drain unexpectedly fell back to synchronous full selection");
            assert.equal(work.statCalls, expectedVisits);
            assert.equal(work.statMultiplicity.size, count);
            assert([...work.statMultiplicity.values()].every(visits => visits === 2));
            assert.deepEqual(work.dirtyTakeLengths, []);
            assert.equal(work.dirtyClaimedPaths, count);
            assert.equal(work.dirtyClaimLengths.reduce((sum, size) => sum + size, 0), count);
            assert(work.dirtyClaimLengths.every(size => size > 0 && size <= 256));
            assert(work.dirtyRestoreLengths.every(size => size <= 256), "restored whole unselected remainder");
            assert(work.readinessCalls > 0 && work.readinessHintVisits > 0);
            assert(work.readinessHintVisits <= work.readinessCalls, "independent readiness did not stop at its first runnable hint");
            assert.equal(work.readinessTakeCalls, 0); assert.equal(work.readinessRestoreCalls, 0);
            assert.equal(work.rootContinuations, expectedBatches.length - 1);
            assert.equal(work.hostYieldsRequested, work.hostYieldsCompleted);
            assert.equal(f.server.requests.length, expectedBatches.length);
            assert.equal(f.server.legacy, 0); assert.equal(f.journal.unsyncedCount(), 0);
            assert.equal(a.engine.pendingChanges.size, 0);
            for (const path of paths) assert.equal(f.base.getHash(path), hashFor(2));
            const multiplicities = {};
            for (const visits of work.statMultiplicity.values()) multiplicities[visits] = (multiplicities[visits] ?? 0) + 1;
            results.push({ paths: count, roots: f.server.requests.length, publicationSizes: f.server.publicationSizes,
                materializationInputLengths: work.materialize, materializationPathVisits: expectedVisits,
                sourceStatCalls: work.statCalls, sourceStatVisitsPerPathHistogram: multiplicities,
                cooperativePlans: work.plans.map(plan => ({ queued: plan.queued, ready: plan.ready, dependencies: plan.dependencies,
                    creationDurationMs: round(plan.creationMs), selectionDurationMs: round(plan.selectionMs),
                    preflightRows: plan.initial.preflightRows, graphSteps: plan.initial.graphSteps,
                    graphNodes: plan.initial.graphNodes, graphComponents: plan.initial.graphComponents,
                    cooperationCalls: plan.initial.cooperationCalls, selectionComponentVisits: plan.latest.selectionComponentVisits,
                    selectedPaths: plan.latest.selectedPaths, batches: plan.batches,
                    estimatedBuildMetadataBytes: plan.initial.buildPeakMetadataBytes,
                    finalEstimatedRetainedMetadataBytes: plan.latest.retainedMetadataBytes, disposed: plan.latest.disposed })),
                synchronousPlannerCalls: work.planner,
                materializationDurationMs: round(work.materializeMs), wholeDrainDurationMs: round(durationMs),
                readiness: { calls: work.readinessCalls, yieldedHints: work.readinessHintVisits,
                    takeCalls: work.readinessTakeCalls, restoreCalls: work.readinessRestoreCalls },
                dirtyQueue: { claimCalls: work.dirtyClaimLengths.length,
                    claimInputVisits: work.dirtyClaimLengths.reduce((sum, size) => sum + size, 0),
                    claimedPaths: work.dirtyClaimedPaths, maxClaimPaths: Math.max(0, ...work.dirtyClaimLengths),
                    takeCalls: work.dirtyTakeLengths.length,
                    takeInputVisits: work.dirtyTakeLengths.reduce((sum, value) => sum + value, 0),
                    restoreCalls: work.dirtyRestoreLengths.length,
                    restoreInputVisits: work.dirtyRestoreLengths.reduce((sum, value) => sum + value, 0),
                    takeLengths: work.dirtyTakeLengths, restoreLengths: work.dirtyRestoreLengths },
                hostYieldsRequested: work.hostYieldsRequested, hostYieldsCompleted: work.hostYieldsCompleted,
                rootContinuationHostBoundaries: work.rootContinuations, scheduler: workSchedulerSnapshot().backend,
                syntheticHeartbeat: { requestedIntervalMs: 16, callbacks: heartbeatGaps.length,
                    callbackGapP50Ms: percentile(heartbeatGaps, 0.5), callbackGapP95Ms: percentile(heartbeatGaps, 0.95),
                    callbackGapMaxMs: heartbeatGaps.length ? round(Math.max(...heartbeatGaps)) : null },
            });
        } finally {
            globalThis.__rootDrainWork = undefined;
            if (heartbeat !== undefined) clearInterval(heartbeat);
            await f.close();
        }
    }
    disposeWorkScheduler();
    process.stdout.write(JSON.stringify({ diagnostic: "actual-engine-root-drain-work-counts", node: process.version,
        platform: process.platform, architecture: process.arch,
        corpus: "independent one-byte notes; all object hashes already present; no concurrent edits",
        limitations: ["synthetic JSON tree and server; real production local classes over MemorySegmentedIO",
            "server fixture clones/validates local intents per request; durations are not a transport baseline",
            "instrumented bundle; no Obsidian/native storage/mobile/RSS or active-note service guarantee"],
        results }, null, 2) + "\n");
    completed = true;
}

try {
    const output = join(directory, "diagnostic.cjs");
    await build({
        absWorkingDir: pluginDirectory, bundle: true, platform: "node", format: "cjs", target: "node20", outfile: output,
        stdin: { resolveDir: pluginDirectory, loader: "js", contents: `
            import { strict as assert } from "node:assert";
            import { fixture, pathAt, hashFor } from "./src/engine-root-test-fixture";
            import { workSchedulerSnapshot, disposeWorkScheduler } from "./src/work-scheduler";
            const COUNTS = ${JSON.stringify(counts)};
            (${childMain.toString()})().catch(error => { console.error(error); process.exitCode = 1; });
        ` },
        plugins: [{ name: "root-drain-diagnostic-only", setup(builder) {
            builder.onLoad({ filter: /[\\/]root-batch-plan\.ts$/ }, async ({ path }) => ({
                contents: replaceOnce(await readFile(path, "utf8"), "export async function createRootBatchPlan(", "async function measuredCreateRootBatchPlan(") + `
                    export async function createRootBatchPlan(...args: Parameters<typeof measuredCreateRootBatchPlan>): Promise<RootBatchPlan> {
                        const work = (globalThis as any).__rootDrainWork, started = performance.now();
                        const plan = await measuredCreateRootBatchPlan(...args);
                        if (!work) return plan;
                        const sample = {ready: args[0].length, queued: args[1].length, dependencies: args[2].length,
                            creationMs: performance.now() - started, selectionMs: 0, initial: plan.snapshot(), latest: plan.snapshot(), batches: [] as number[]};
                        work.plans.push(sample);
                        const next = plan.next.bind(plan), dispose = plan.dispose.bind(plan);
                        plan.next = () => {
                            const started = performance.now(), result = next();
                            sample.selectionMs += performance.now() - started;
                            sample.latest = plan.snapshot(); sample.batches.push(result?.ready.length ?? 0);
                            return result;
                        };
                        plan.dispose = () => { dispose(); sample.latest = plan.snapshot(); };
                        return plan;
                    }`, loader: "ts",
            }));
            builder.onLoad({ filter: /[\\/]root-batch\.ts$/ }, async ({ path }) => ({
                contents: replaceOnce(await readFile(path, "utf8"), "export function selectRootBatch(", "function measuredRootBatch(") + `
                    export function selectRootBatch(...args: Parameters<typeof measuredRootBatch>): ReturnType<typeof measuredRootBatch> {
                        const work = (globalThis as any).__rootDrainWork, started = performance.now();
                        const result = measuredRootBatch(...args);
                        if (work) { work.plannerMs += performance.now() - started;
                            work.planner.push({ready: args[0].length, queued: args[1].length, dependencies: args[2]?.length ?? 0,
                                selected: result.ready.length}); }
                        return result;
                    }`, loader: "ts",
            }));
            builder.onLoad({ filter: /[\\/]work-scheduler\.ts$/ }, async ({ path }) => ({
                contents: replaceOnce(await readFile(path, "utf8"), "export function yieldWork(", "function measuredYieldWork(") + `
                    export function yieldWork(...args: Parameters<typeof measuredYieldWork>): Promise<void> {
                        const work = (globalThis as any).__rootDrainWork;
                        if (work) work.hostYieldsRequested++;
                        return measuredYieldWork(...args).then(() => { if (work) work.hostYieldsCompleted++; });
                    }`, loader: "ts",
            }));
            builder.onResolve({ filter: /^obsidian$/ }, () => ({ path: "obsidian", namespace: "shell" }));
            builder.onLoad({ filter: /.*/, namespace: "shell" }, () => ({ loader: "js", contents: `
                export class TAbstractFile {} export class TFile extends TAbstractFile {}
                export class Notice { constructor(...args) { return globalThis.__obsetyncTestNotice(...args); } }
                export function debounce(...args) { return globalThis.__obsetyncTestDebounce(...args); }
                export function requestUrl() { throw new Error("diagnostic attempted native network"); }
                export const Platform = new Proxy({}, {get() {throw new Error("diagnostic attempted native platform detection");}});
            ` }));
        } }],
    });
    const status = await new Promise((resolve, reject) => {
        const child = spawn(process.execPath, ["--unhandled-rejections=strict", output], { stdio: "inherit" });
        const timeout = setTimeout(() => {
            process.stderr.write(JSON.stringify({ error: "diagnostic-timeout", timeoutMs: 60000 }) + "\n");
            child.kill("SIGKILL");
        }, 60000);
        child.once("error", error => { clearTimeout(timeout); reject(error); });
        child.once("close", code => { clearTimeout(timeout); resolve(code ?? 1); });
    });
    if (status !== 0) process.exitCode = status;
} finally {
    const resolved = await realpath(directory), metadata = await lstat(directory);
    assert.equal(resolved, directory); assert.equal(dirname(resolved), "/tmp");
    assert(basename(resolved).startsWith("obsetync-root-drain-counts-") && metadata.isDirectory() && !metadata.isSymbolicLink());
    await rm(resolved, { recursive: true, force: true });
}
