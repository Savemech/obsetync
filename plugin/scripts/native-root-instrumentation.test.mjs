import { strict as assert } from "node:assert";
import test from "node:test";
import Module from "node:module";
import { mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { nativeRootInstrumentationPlugin, NATIVE_ROOT_INSTRUMENTATION_LIMITS } from "./lib/native-root-instrumentation.mjs";

const pluginDirectory = fileURLToPath(new URL("../", import.meta.url));
const sources = ["src/root-batch-plan.ts", "src/work-scheduler.ts", "src/root-reviewed-queue.ts"];
function collector() {
    return { plans: [], hostYieldsRequested: 0, hostYieldsCompleted: 0,
        candidateMutationActiveLoops: 0 };
}
function hints(count) {
    return Array.from({ length: count }, (_, index) => ({ path: `synthetic/${index}.md`, action: "modified", mtime: 1, size: 2 }));
}
async function bundled() {
    const result = await build({ absWorkingDir: pluginDirectory, bundle: true, platform: "node", format: "cjs", target: "node20",
        write: false, logLevel: "silent", plugins: [nativeRootInstrumentationPlugin()], stdin: {
            resolveDir: pluginDirectory, sourcefile: "native-root-instrumentation-test.ts", loader: "ts", contents: `
                export { createRootBatchPlan } from "./src/root-batch-plan";
                export { yieldWork, disposeWorkScheduler, workSchedulerSnapshot } from "./src/work-scheduler";
                export { sampleNativeRootPublication } from "./scripts/lib/native-root-publication";
            `,
        } });
    const filename = join(pluginDirectory, "native-root-instrumentation-in-memory.cjs");
    const module = new Module(filename); module.filename = filename;
    module.paths = Module._nodeModulePaths(pluginDirectory);
    module._compile(result.outputFiles[0].text, filename);
    return module.exports;
}

test("real planner/scheduler bundle records one graph and actual bounded selections without source edits", async () => {
    const before = await Promise.all(sources.map(path => readFile(join(pluginDirectory, path), "utf8")));
    const api = await bundled(), work = collector(), queued = hints(300);
    globalThis.__nativeRootWork = work;
    try {
        const plan = await api.createRootBatchPlan(queued, queued, [], { cooperate: api.yieldWork });
        assert.equal(work.plans.length, 1); assert.equal(work.planCreatesRequested, 1); assert.equal(work.planCreatesCompleted, 1);
        const first = plan.next(), second = plan.next();
        assert.equal(first.ready.length, 256); assert.equal(second.ready.length, 44);
        assert.equal(first.queued[0], queued[0], "instrumentation changed original hint ownership");
        assert.equal(second.queued[0], queued[256]); assert.equal(plan.next(), null);
        plan.dispose();
        const sample = work.plans[0];
        assert.equal(sample.ready, 300); assert.equal(sample.queued, 300); assert.equal(sample.dependencies, 0);
        assert.equal(sample.initial.graphNodes, 300); assert.equal(sample.initial.selectedPaths, 0);
        assert.equal(sample.latest.selectedPaths, 300); assert.equal(sample.latest.disposed, true);
        assert.equal(sample.latest.retainedMetadataBytes, 0); assert.deepEqual(sample.batches, [256, 44, 0]);
        assert.equal(sample.selectionCalls, 3); assert.equal(sample.disposalCalls, 1);
        assert(sample.creationMs >= 0 && sample.selectionMs >= 0);
        assert(work.hostYieldsRequested > 0); assert.equal(work.hostYieldsCompleted, work.hostYieldsRequested);
        assert.equal(work.instrumentationErrors ?? 0, 0);
        assert(!JSON.stringify(work).includes("synthetic/"), "collector retained caller paths");
        assert.deepEqual(await Promise.all(sources.map(path => readFile(join(pluginDirectory, path), "utf8"))), before);
    } finally { globalThis.__nativeRootWork = undefined; api.disposeWorkScheduler(); }
});

test("no collector leaves actual plan methods unwrapped", async () => {
    const api = await bundled(); globalThis.__nativeRootWork = undefined;
    try {
        const queued = hints(1), plan = await api.createRootBatchPlan(queued, queued, [], { cooperate: api.yieldWork });
        assert.equal(Object.hasOwn(plan, "next"), false); assert.equal(Object.hasOwn(plan, "dispose"), false);
        assert.equal(plan.next().queued[0], queued[0]); await api.yieldWork(); plan.dispose();
    } finally { api.disposeWorkScheduler(); }
});

test("native fixture samples the current owned root publication ABI without consuming its owner", async () => {
    const api = await bundled();
    let releases = 0;
    const rootExport = { bytes: new Uint8Array(17), release: () => { releases++; } };
    const sample = api.sampleNativeRootPublication({ entries: [{}, {}], rootExport, journalCuts: [{}] });
    assert.deepEqual(sample, { entries: 2, rootBytes: 17, cuts: 1 });
    assert.equal(releases, 0);
});

test("failed real creation, selection and scheduler preserve native error identity and completion accounting", async () => {
    const api = await bundled(), work = collector(), abort = new AbortController(), failure = new Error("exact owner cancellation");
    abort.abort(failure); globalThis.__nativeRootWork = work;
    try {
        await assert.rejects(api.createRootBatchPlan([], [], [], { cooperate: api.yieldWork, signal: abort.signal }), error => error === failure);
        assert.equal(work.planCreatesRequested, 1); assert.equal(work.planCreatesFailed, 1); assert.equal(work.plans.length, 0);
        await assert.rejects(api.yieldWork({ signal: abort.signal }), error => error === failure);
        assert.equal(work.hostYieldsRequested, 1); assert.equal(work.hostYieldsFailed, 1); assert.equal(work.hostYieldsCompleted, 0);
        const later = new AbortController(), queued = hints(1);
        const plan = await api.createRootBatchPlan(queued, queued, [], { cooperate: api.yieldWork, signal: later.signal });
        later.abort(failure);
        assert.throws(() => plan.next(), error => error === failure);
        assert.equal(work.plans[0].selectionFailures, 1); assert.equal(work.plans[0].latest.selectedPaths, 0);
        assert.deepEqual(work.plans[0].batches, [], "failed selection fabricated an empty successful batch");
        plan.dispose(); assert.equal(work.plans[0].latest.disposed, true);
        assert.equal(work.instrumentationErrors ?? 0, 0);
    } finally { globalThis.__nativeRootWork = undefined; api.disposeWorkScheduler(); }
});

test("actual completion stays with the invocation owner when the measured global changes", async () => {
    const api = await bundled(), original = collector(), next = collector();
    globalThis.__nativeRootWork = original;
    try {
        const pendingYield = api.yieldWork();
        assert.equal(original.hostYieldsCompleted, 0);
        globalThis.__nativeRootWork = next;
        await pendingYield;
        assert.equal(original.hostYieldsCompleted, 1); assert.equal(next.hostYieldsCompleted, 0);
        globalThis.__nativeRootWork = original;
        const pendingPlan = api.createRootBatchPlan([], [], [], { cooperate: api.yieldWork });
        globalThis.__nativeRootWork = undefined;
        const plan = await pendingPlan;
        assert.equal(original.plans.length, 1); assert.equal(next.plans.length, 0);
        plan.next(); plan.dispose();
        assert.equal(original.plans[0].latest.disposed, true);
        assert.equal(original.plans[0].selectionCalls, 1);
    } finally { globalThis.__nativeRootWork = undefined; api.disposeWorkScheduler(); }
});

test("scheduler attributes only yields requested by an active candidate mutation loop", async () => {
    const api = await bundled(), work = collector();
    globalThis.__nativeRootWork = work;
    try {
        await api.yieldWork();
        assert.equal(work.candidateMutationLoopYieldsRequested ?? 0, 0,
            "pre-begin/global cooperation was attributed to candidate mutation");
        work.candidateMutationActiveLoops = 1;
        const pending = api.yieldWork();
        assert.equal(work.candidateMutationLoopYieldsRequested, 1);
        work.candidateMutationActiveLoops = 0;
        await pending;
        assert.equal(work.candidateMutationLoopYieldsCompleted, 1,
            "completion lost the loop attribution captured at request time");
        assert.equal(work.hostYieldsRequested, 2); assert.equal(work.hostYieldsCompleted, 2);
    } finally { globalThis.__nativeRootWork = undefined; api.disposeWorkScheduler(); }
});

test("reviewed-queue observer counts only successful retained source-drift decisions", async () => {
    const root = await mkdtemp("/tmp/obsetync-native-instrumentation-test-");
    try {
        const target = join(root, "root-reviewed-queue.ts");
        await writeFile(target, `
            type DirtyFileChange = { retained: boolean };
            export class RootReviewedQueue {
                retainFailedIndependentSelection(hints: readonly DirtyFileChange[]): boolean {
                    (globalThis as any).__nativeRootQueueCalls++;
                    if (hints.length === 2) throw (globalThis as any).__nativeRootQueueFailure;
                    return hints.length === 1 && hints[0].retained;
                }
            }
        `, "utf8");
        const result = await build({ entryPoints: [target], bundle: true, write: false, logLevel: "silent",
            platform: "node", format: "cjs", target: "node20", plugins: [nativeRootInstrumentationPlugin()] });
        const filename = join(root, "queue.cjs"), module = new Module(filename); module.filename = filename;
        module.paths = Module._nodeModulePaths(root); module._compile(result.outputFiles[0].text, filename);
        const owner = new module.exports.RootReviewedQueue(), work = collector(), failure = new Error("exact queue failure");
        globalThis.__nativeRootWork = work; globalThis.__nativeRootQueueFailure = failure; globalThis.__nativeRootQueueCalls = 0;
        assert.equal(owner.retainFailedIndependentSelection([{ retained: false }]), false);
        assert.equal(work.sourceDriftRetentionsAccepted ?? 0, 0); assert.equal(globalThis.__nativeRootQueueCalls, 1);
        assert.equal(owner.retainFailedIndependentSelection([{ retained: true }]), true);
        assert.equal(work.sourceDriftRetentionsAccepted, 1); assert.equal(globalThis.__nativeRootQueueCalls, 2);
        assert.throws(() => owner.retainFailedIndependentSelection([{ retained: true }, { retained: true }]), error => error === failure);
        assert.equal(work.sourceDriftRetentionsAccepted, 1); assert.equal(globalThis.__nativeRootQueueCalls, 3);
        Object.defineProperty(work, "sourceDriftRetentionsAccepted", { configurable: true,
            get: () => 1, set: () => { throw new Error("poisoned observer counter"); } });
        assert.equal(owner.retainFailedIndependentSelection([{ retained: true }]), true);
        assert.equal(globalThis.__nativeRootQueueCalls, 4); assert.equal(work.sourceDriftRetentionsAccepted, 1);
        assert.equal(work.instrumentationErrors, 1);
        globalThis.__nativeRootWork = undefined;
        assert.equal(owner.retainFailedIndependentSelection([{ retained: true }]), true);
        assert.equal(globalThis.__nativeRootQueueCalls, 5); assert.equal(work.sourceDriftRetentionsAccepted, 1);
        assert.equal(work.instrumentationErrors, 1);
    } finally {
        globalThis.__nativeRootWork = undefined; delete globalThis.__nativeRootQueueFailure; delete globalThis.__nativeRootQueueCalls;
        assert.equal(await realpath(root), root);
        assert.equal(dirname(root), "/tmp"); assert(basename(root).startsWith("obsetync-native-instrumentation-test-"));
        await rm(root, { recursive: true });
    }
});

test("fixed plan/batch caps report dropped observations without changing scheduling", async () => {
    const api = await bundled(), work = collector(); globalThis.__nativeRootWork = work;
    try {
        for (let index = 0; index < NATIVE_ROOT_INSTRUMENTATION_LIMITS.plans + 1; index++) {
            const plan = await api.createRootBatchPlan([], [], [], { cooperate: api.yieldWork });
            if (index === 0) for (let batch = 0; batch < NATIVE_ROOT_INSTRUMENTATION_LIMITS.batchesPerPlan + 1; batch++) {
                assert.equal(plan.next(), null);
            }
            else assert.equal(plan.next(), null);
            plan.dispose();
        }
        assert.equal(work.plans.length, NATIVE_ROOT_INSTRUMENTATION_LIMITS.plans);
        assert.equal(work.planCreatesCompleted, NATIVE_ROOT_INSTRUMENTATION_LIMITS.plans + 1);
        assert.equal(work.plansDropped, 1);
        assert.equal(work.plans[0].batches.length, NATIVE_ROOT_INSTRUMENTATION_LIMITS.batchesPerPlan);
        assert.equal(work.plans[0].selectionCalls, NATIVE_ROOT_INSTRUMENTATION_LIMITS.batchesPerPlan + 1);
        assert.equal(work.plans[0].batchSamplesDropped, 1); assert.equal(work.batchSamplesDropped, 1);
        assert.equal(work.instrumentationErrors ?? 0, 0);
    } finally { globalThis.__nativeRootWork = undefined; api.disposeWorkScheduler(); }
});

test("missing or duplicate exact source seams fail the build", async () => {
    const root = await mkdtemp("/tmp/obsetync-native-instrumentation-test-");
    try {
        for (const [name, signature] of [["root-batch-plan.ts", "export async function createRootBatchPlan("],
            ["work-scheduler.ts", "export function yieldWork("],
            ["root-reviewed-queue.ts", "    retainFailedIndependentSelection(hints: readonly DirtyFileChange[]): boolean {"]]) {
            const target = join(root, name);
            for (const body of ["export const changed = true;", `${signature}) {}\n// ${signature}\n`]) {
                await writeFile(target, body, "utf8");
                await assert.rejects(build({ entryPoints: [target], bundle: true, write: false, logLevel: "silent",
                    platform: "node", plugins: [nativeRootInstrumentationPlugin()] }), /source seam changed/);
            }
        }
    } finally {
        assert.equal(await realpath(root), root);
        assert.equal(dirname(root), "/tmp"); assert(basename(root).startsWith("obsetync-native-instrumentation-test-"));
        await rm(root, { recursive: true });
    }
});
