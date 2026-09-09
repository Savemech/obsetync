import { strict as assert } from "node:assert";
import type { CapturedBaseTreeEntries, CapturedBaseTreeEntry } from "./sync-base";
import { ResourceBudget, ResourceBudgetOversizedError } from "./resource-budget";
import {
    ROOT_TREE_REPAIR_LIMITS, RootTreeRepairError, estimatePagedRootTreeRepairWorkset,
    rebuildRootTreeFromBase, RootTreeRetirementError, RootTreeOutputAdmissionDeniedError,
    drainRootTreeRetirement, hasPendingRootTreeRetirement,
} from "./root-tree-repair";
import { RootTreeResidentAdmission } from "./root-tree-resident-admission";
import { yieldWork } from "./work-scheduler";

// Actual host driver/accounting, deliberately synthetic native graph port.
// These tests prove input paging and owner lifetimes, not native rebuild
// responsiveness, allocator use, fsync, packaged codec parity or mobile RSS.
const OLD_ROOT = "a".repeat(64), BASE_ROOT = "b".repeat(64), NEW_ROOT = "c".repeat(64);
const PAGE_BYTES = 256 * 1024, PAGE_ENTRIES = 256;
type Row = CapturedBaseTreeEntry;
type NativePhase = "begin" | "append" | "start" | "step" | "plan" | "resume" |
    "sort-plan" | "sort-start" |
    "output-plan" | "output-resume" | "finish" | "cancel" | "deferred-finish" |
    "deferred-cancel" | "retire";
interface BuildProgress { done: boolean; units: number; completed: number; phase: string }
interface RetirementProgress { done: boolean; units: number; completed: number }
function gate() {
    let resolve!: () => void;
    return { promise: new Promise<void>(done => { resolve = done; }), resolve: () => resolve() };
}
const outcome = <T>(work: Promise<T>): Promise<{ value: T } | { error: unknown }> =>
    work.then(value => ({ value }), error => ({ error }));
function failure(result: Awaited<ReturnType<typeof outcome>>): unknown {
    assert("error" in result, "rebuild unexpectedly succeeded"); return result.error;
}
function rows(count: number, longUnicode = false): Row[] {
    const result: Row[] = [];
    for (let index = 0; index < count; index++) {
        const head = `dir/${index.toString().padStart(5, "0")}-`;
        const tail = "\u{10000}";
        // Keep successful synthetic v2 rows within the actual native UTF-8
        // path ceiling while still forcing the page's byte (not count) limit.
        const path = longUnicode
            ? head + "é".repeat((4096 - Buffer.byteLength(head + tail)) / 2) + tail
            : `${head}note.md`;
        result.push({ path, hash: "d".repeat(64), mtime_ms: index + 10, size: index });
    }
    return result;
}
function capturedBase(input: readonly Row[]) {
    const owned = input.map(row => Object.freeze({ ...row }));
    let generation = 0, iterations = 0, visited = 0;
    return {
        rows: owned,
        get iterations() { return iterations; }, get visited() { return visited; },
        mutate() { generation++; },
        captureTreeEntries(): CapturedBaseTreeEntries {
            const captured = generation;
            return Object.freeze({ entryCount: owned.length, treeBaseRoot: BASE_ROOT,
                *entries() { iterations++; for (const row of owned) { visited++; yield { ...row }; } },
                isCurrent: () => generation === captured });
        },
    };
}

function fixture(input = rows(513)) {
    const base = capturedBase(input), serializedBytes = Buffer.byteLength(JSON.stringify(input));
    let workBytes = estimatePagedRootTreeRepairWorkset(serializedBytes, input.length);
    const budget = new ResourceBudget({ capacityBytes: Math.max(workBytes, ROOT_TREE_REPAIR_LIMITS.scratchBytes) });
    const allocations: number[] = [], releases: number[] = [], phases: NativePhase[] = [];
    const pages: { offset: number; bytes: number; rows: Row[]; json: string }[] = [];
    const state = { version: 1, committedRevision: 4, candidateRevision: 7, root: OLD_ROOT,
        files: 1, begin: 0, finish: 0, cancel: 0, legacy: 0,
        active: false, expectedCount: 0, expectedBytes: 0, targetVersion: 0, received: 0,
        buildEnabled: false, buildStarted: false, buildDone: false, starts: 0, steps: 0,
        sortPlanReads: 0, sortStarts: 0,
        completed: 0, totalWork: 0, atomicFinishes: 0,
        planEnabled: false, planReady: false, planReads: 0, planResumes: 0,
        outputPlanReads: 0, outputPlanResumes: 0,
        retiring: false, retirementSteps: 0, retirementCompleted: 0, retirementWork: 0,
        deferredFinishes: 0, deferredCancels: 0 };
    const buildProgress: BuildProgress[] = [], stepLimits: number[] = [];
    const hooks: { native?: (phase: NativePhase) => void; returnedCount?: (count: number) => unknown;
        progress?: (progress: BuildProgress, call: number) => unknown;
        retirementProgress?: (progress: RetirementProgress, call: number) => unknown } = {};
    const port = { reserve: async (bytes: number, options?: { signal?: AbortSignal }) => {
        allocations.push(bytes);
        const lease = await budget.reserve(bytes, options);
        let released = false;
        return { bytes: lease.bytes, release() {
            if (released) return;
            released = true; releases.push(bytes); lease.release();
        } };
    } };
    function native(phase: NativePhase) {
        phases.push(phase);
        assert.equal(budget.snapshot().usedBytes, workBytes, `${phase}: native owner lacks complete input admission`);
        assert.equal(budget.snapshot().activeReservations, 1, `${phase}: scratch/global reservations overlap`);
        assert.deepEqual(releases, [ROOT_TREE_REPAIR_LIMITS.scratchBytes], `${phase}: complete input released before native settlement`);
        hooks.native?.(phase);
    }
    const tree = {
        start_replacement_rebuild_job: undefined as ((token: number) => void) | undefined,
        replacement_rebuild_sort_memory_plan_v1_job: undefined as ((token: number) => unknown) | undefined,
        start_replacement_rebuild_sort_memory_v1_job: undefined as ((token: number, source: number, target: number) => void) | undefined,
        step_replacement_rebuild_job: undefined as ((token: number, maxUnits: number) => unknown) | undefined,
        step_replacement_rebuild_output_memory_v1_job: undefined as
            ((token: number, maxUnits: number) => unknown) | undefined,
        replacement_rebuild_plan_job: undefined as ((token: number) => unknown) | undefined,
        resume_replacement_rebuild_job: undefined as ((token: number, expectedBytes: number) => void) | undefined,
        replacement_rebuild_output_memory_plan_v1_job: undefined as ((token: number) => unknown) | undefined,
        resume_replacement_rebuild_output_memory_v1_job: undefined as ((token: number, nodeBytes: number,
            endpointPeakBytes: number, endpointResidentBytes: number) => void) | undefined,
        cancel_replacement_rebuild_job_deferred: undefined as ((token: number) => void) | undefined,
        finish_replacement_rebuild_job_deferred: undefined as ((token: number) => void) | undefined,
        step_tree_retirement: undefined as ((token: number, maxUnits: number) => unknown) | undefined,
        committed_revision: (() => state.committedRevision) as (() => number) | undefined,
        candidate_revision: (() => state.candidateRevision) as (() => number) | undefined,
        tree_version: () => state.version,
        root_hash_hex: () => state.root,
        total_files: () => state.files,
        rebuild_from_entries_in_version() { state.legacy++; throw new Error("complete job API fell back to whole JSON rebuild"); },
        begin_replacement_rebuild_job(version: number, count: number, bytes: number) {
            native("begin"); assert.equal(state.active, false); state.begin++;
            assert.equal(count, input.length); assert.equal(bytes, serializedBytes);
            state.targetVersion = version; state.expectedCount = count; state.expectedBytes = bytes;
            state.active = true; return 23;
        },
        append_replacement_rebuild_job(token: number, offset: number, json: string): number {
            assert.equal(token, 23); assert(state.active); assert.equal(offset, state.received);
            native("append");
            const decoded: Row[] = JSON.parse(json), bytes = Buffer.byteLength(json);
            assert(decoded.length > 0 && decoded.length <= PAGE_ENTRIES);
            assert(bytes <= PAGE_BYTES, "native feed exceeds the byte ceiling");
            pages.push({ offset, bytes, rows: decoded, json }); state.received += decoded.length;
            return (hooks.returnedCount?.(state.received) ?? state.received) as number;
        },
        finish_replacement_rebuild_job(token: number) {
            assert.equal(token, 23); assert(state.active); assert.equal(state.received, state.expectedCount);
            if (state.buildStarted) assert(state.buildDone, "finish published an unfinished private builder");
            else state.atomicFinishes++; // Existing paged-input ABI compatibility.
            assert.equal(2 + pages.reduce((sum, page) => sum + page.bytes - 2, 0) + Math.max(0, pages.length - 1),
                state.expectedBytes, "feed pages do not reconstruct the exact logical JSON byte total");
            // Ready finish consumes native ownership even when the subsequent
            // build/revision hook fails. The committed graph stays unchanged.
            state.active = false; state.finish++; native("finish");
            state.version = state.targetVersion; state.files = state.expectedCount;
            state.root = NEW_ROOT; state.committedRevision++;
        },
        cancel_tree_job(token: number) {
            assert.equal(token, 23); assert(state.active && !state.retiring); state.cancel++;
            native("cancel"); state.active = false;
        },
    };
    function enableBuild(totalWork = 513) {
        state.buildEnabled = true; state.totalWork = totalWork;
        tree.start_replacement_rebuild_job = token => {
            assert.equal(token, 23); assert(state.active && !state.buildStarted);
            assert.equal(state.received, state.expectedCount, "build began before all input pages");
            state.starts++; native("start"); state.buildStarted = true;
        };
        tree.step_replacement_rebuild_job = (token, maxUnits) => {
            assert.equal(token, 23); assert(state.active && state.buildStarted && !state.buildDone);
            assert(!state.planReady || state.planResumes === 1, "build crossed an unacknowledged node plan");
            assert(Number.isSafeInteger(maxUnits) && maxUnits >= 1 && maxUnits <= 256);
            stepLimits.push(maxUnits); state.steps++; native("step");
            if (state.planEnabled && state.planReads === 0 && state.outputPlanReads === 0) {
                state.completed++; state.planReady = true;
                const progress = { done: false, units: 1, completed: state.completed, phase: "plan ready" };
                buildProgress.push(progress);
                return hooks.progress ? hooks.progress({ ...progress }, state.steps) : progress;
            }
            const units = Math.min(maxUnits, state.totalWork - state.completed);
            state.completed += units; state.buildDone = state.completed === state.totalWork;
            const progress = { done: state.buildDone, units, completed: state.completed,
                phase: state.buildDone ? "done" : state.steps === 1 ? "sort" : "build" };
            buildProgress.push(progress);
            return hooks.progress ? hooks.progress({ ...progress }, state.steps) : progress;
        };
    }
    function enablePlan(plan = { nodeCount: 3, leafCount: 2, internalCount: 1,
        nodePayloadBytes: 123_456, maxNodeBytes: 65_536, storedRootBytes: 256 }) {
        if (!state.buildEnabled) enableBuild(2);
        state.planEnabled = true;
        tree.replacement_rebuild_plan_job = token => {
            assert.equal(token, 23); assert(state.active && state.planReady && state.planReads === 0);
            state.planReads++; native("plan"); return { ...plan };
        };
        tree.resume_replacement_rebuild_job = (token, expectedBytes) => {
            assert.equal(token, 23); assert(state.active && state.planReady && state.planReads === 1);
            assert.equal(expectedBytes, plan.nodePayloadBytes);
            state.planResumes++; state.planReady = false; native("resume");
        };
    }
    function enableSortPlan(admitWorkspace = true) {
        if (!state.buildEnabled) enableBuild(2);
        workBytes = estimatePagedRootTreeRepairWorkset(serializedBytes, input.length) +
            (admitWorkspace ? 8 * input.length : 0);
        budget.setCapacity(Math.max(workBytes, ROOT_TREE_REPAIR_LIMITS.scratchBytes));
        const plan = { schema: 1, scope: "v2-replacement-sort-indices", entryCount: input.length,
            indexSizeBytes: 4, sourceIndexRequestedBytes: 4 * input.length,
            targetIndexRequestedBytes: 4 * input.length, peakAdmissionBytes: 8 * input.length };
        tree.replacement_rebuild_sort_memory_plan_v1_job = token => {
            assert.equal(token, 23); assert(state.active && !state.buildStarted);
            assert.equal(state.received, state.expectedCount, "sort plan read before complete input");
            state.sortPlanReads++; native("sort-plan"); return { ...plan };
        };
        tree.start_replacement_rebuild_sort_memory_v1_job = (token, source, target) => {
            assert.equal(token, 23); assert(state.active && !state.buildStarted);
            assert.equal(state.received, state.expectedCount);
            assert.equal(state.sortPlanReads, 1);
            assert.equal(source, plan.sourceIndexRequestedBytes); assert.equal(target, plan.targetIndexRequestedBytes);
            state.sortStarts++; native("sort-start"); state.buildStarted = true;
        };
        return plan;
    }
    function enableOutputPlan(plan: { schema: 1; scope: "v1-replacement-output" | "v2-replacement-output";
        nodePayloadBytes: number; rangeEndpointPeakRequestedBytes: number;
        rangeEndpointResidentRequestedBytes: number; peakAdmissionBytes: number;
        residentAdmissionBytes: number } = { schema: 1 as const, scope: "v2-replacement-output" as const,
        nodePayloadBytes: 123_456, rangeEndpointPeakRequestedBytes: 1_000,
        rangeEndpointResidentRequestedBytes: 200, peakAdmissionBytes: 124_456,
        residentAdmissionBytes: 123_656 }) {
        if (plan.scope === "v2-replacement-output") {
            if (!state.planEnabled) enablePlan();
        } else {
            if (!state.buildEnabled) enableBuild(2);
            state.planEnabled = true;
        }
        const expectedNodePlanReads = plan.scope === "v2-replacement-output" ? 1 : 0;
        const legacyStep = tree.step_replacement_rebuild_job!;
        let v1BarrierExposed = false;
        tree.step_replacement_rebuild_output_memory_v1_job = (token, maxUnits) => {
            if (plan.scope === "v1-replacement-output" && !v1BarrierExposed) {
                assert.equal(token, 23); assert(state.active && state.buildStarted && !state.buildDone);
                assert(Number.isSafeInteger(maxUnits) && maxUnits >= 1 && maxUnits <= 256);
                v1BarrierExposed = true; state.planReady = true; state.steps++;
                stepLimits.push(maxUnits); native("step");
                const progress = { done: false, units: 0, completed: state.completed, phase: "plan ready" };
                buildProgress.push(progress);
                return hooks.progress ? hooks.progress({ ...progress }, state.steps) : progress;
            }
            return legacyStep.call(tree, token, maxUnits);
        };
        tree.replacement_rebuild_output_memory_plan_v1_job = token => {
            assert.equal(token, 23); assert(state.active && state.planReady && state.planReads === expectedNodePlanReads);
            state.outputPlanReads++; native("output-plan"); return { ...plan };
        };
        tree.resume_replacement_rebuild_output_memory_v1_job = (token, node, endpointPeak, endpointResident) => {
            assert.equal(token, 23); assert(state.active && state.planReady && state.planReads === expectedNodePlanReads);
            assert.equal(node, plan.nodePayloadBytes);
            assert.equal(endpointPeak, plan.rangeEndpointPeakRequestedBytes);
            assert.equal(endpointResident, plan.rangeEndpointResidentRequestedBytes);
            state.outputPlanResumes++; state.planResumes++; state.planReady = false; native("output-resume");
        };
        return plan;
    }
    function enableRetirement(totalWork = 513) {
        if (!state.buildEnabled) enableBuild();
        state.retirementWork = totalWork;
        tree.cancel_replacement_rebuild_job_deferred = token => {
            assert.equal(token, 23); assert(state.active && !state.retiring);
            native("deferred-cancel"); state.deferredCancels++; state.retiring = true;
        };
        tree.finish_replacement_rebuild_job_deferred = token => {
            assert.equal(token, 23); assert(state.active && state.buildStarted && state.buildDone && !state.retiring);
            native("deferred-finish"); state.deferredFinishes++; state.retiring = true;
            state.version = state.targetVersion; state.files = state.expectedCount;
            state.root = NEW_ROOT; state.committedRevision++;
        };
        tree.step_tree_retirement = (token, maxUnits) => {
            assert.equal(token, 23); assert(state.active && state.retiring);
            assert(Number.isSafeInteger(maxUnits) && maxUnits >= 1 && maxUnits <= 256);
            native("retire"); state.retirementSteps++;
            const units = Math.min(maxUnits, state.retirementWork - state.retirementCompleted);
            state.retirementCompleted += units;
            const done = state.retirementCompleted === state.retirementWork;
            if (done) { state.active = false; state.retiring = false; }
            const progress = { done, units, completed: state.retirementCompleted };
            return hooks.retirementProgress?.(progress, state.retirementSteps) ?? progress;
        };
    }
    function assertReleased() {
        assert.equal(budget.snapshot().usedBytes, 0);
        assert.equal(budget.snapshot().activeReservations, 0);
        assert.equal(budget.snapshot().queuedRequests, 0);
        assert.equal(state.active, false);
        assert.equal(state.legacy, 0);
        assert.deepEqual(allocations, [ROOT_TREE_REPAIR_LIMITS.scratchBytes, workBytes]);
        assert.deepEqual(releases, allocations);
    }
    return { base, tree, state, hooks, pages, phases, budget, port, allocations, releases,
        serializedBytes, get workBytes() { return workBytes; }, assertReleased, enableBuild, enablePlan, enableOutputPlan, enableSortPlan,
        enableRetirement, buildProgress, stepLimits };
}

async function countAndBytePagedSuccess(): Promise<void> {
    for (const input of [rows(0), rows(1), rows(256), rows(257), rows(513), rows(100, true)]) {
        const f = fixture(input); let cooperations = 0, sawEmptyPreFinish = false;
        try {
            const result = await rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
                { budget: f.port, cooperate: async () => {
                    cooperations++;
                    if (!input.length && f.state.active) sawEmptyPreFinish = true;
                } });
            assert.deepEqual(result, { rebuiltRoot: NEW_ROOT, capturedBaseRoot: BASE_ROOT,
                entryCount: input.length, serializedBytes: f.serializedBytes });
            assert.deepEqual(f.pages.flatMap(page => page.rows), input);
            assert.equal(f.state.begin, 1); assert.equal(f.state.finish, 1); assert.equal(f.state.cancel, 0);
            assert.equal(f.base.iterations, 2); assert.equal(f.base.visited, 2 * input.length);
            assert.equal(f.state.committedRevision, 5);
            assert(cooperations >= Math.floor(input.length / 256) + 1 + Math.max(1, f.pages.length),
                "counting, before-begin, or after-feed cooperation was omitted");
            if (!input.length) assert(sawEmptyPreFinish, "empty input finished without a separate host boundary");
            if (input.length === 513) assert.deepEqual(f.pages.map(page => page.rows.length), [256, 256, 1]);
            if (input.length === 100) {
                assert(f.pages.length > 1);
                assert(input.every(row => Buffer.byteLength(row.path) === 4096));
                for (let page = 0; page < f.pages.length - 1; page++) {
                    assert(f.pages[page].rows.length < PAGE_ENTRIES, "unicode test did not hit the byte-trigger boundary");
                    const nextRowBytes = Buffer.byteLength(JSON.stringify(f.pages[page + 1].rows[0]));
                    assert(f.pages[page].bytes + 1 + nextRowBytes > PAGE_BYTES);
                }
            }
            f.assertReleased();
        } finally { f.budget.close(); }
    }
}

async function actualHostTaskBetweenPages(): Promise<void> {
    const f = fixture(rows(257)); let hostRan = false, observed = false;
    let scheduled: ReturnType<typeof setImmediate> | undefined;
    f.hooks.native = phase => {
        if (phase !== "append") return;
        if (f.pages.length === 0) scheduled = setImmediate(() => { hostRan = true; });
        else { observed = true; assert(hostRan, "next native page ran before the scheduled host task"); }
    };
    try {
        await rebuildRootTreeFromBase(f.base, f.tree, 1, undefined,
            { budget: f.port, cooperate: signal => yieldWork({ signal }) });
        assert(hostRan && observed); f.assertReleased();
    } finally { if (scheduled) clearImmediate(scheduled); f.budget.close(); }
}

async function invalidatedAfterFeedCancels(): Promise<void> {
    for (const mode of ["base", "abort", "scope", "committed", "candidate", "version", "fallback-hash", "fallback-count", "cooperate"] as const) {
        const f = fixture(rows(257)), controller = new AbortController(), reason = new Error(`after-feed ${mode}`);
        let invalidated = false, scopeCurrent = true;
        if (mode.startsWith("fallback")) { f.tree.committed_revision = undefined; f.tree.candidate_revision = undefined; }
        const result = await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, controller.signal, {
            budget: f.port, assertCurrent() { if (!scopeCurrent) throw reason; },
            cooperate: async () => {
                if (f.pages.length !== 1 || invalidated) return;
                invalidated = true;
                if (mode === "base") f.base.mutate();
                if (mode === "abort") controller.abort(reason);
                if (mode === "scope") scopeCurrent = false;
                if (mode === "committed") f.state.committedRevision++;
                if (mode === "candidate") f.state.candidateRevision++;
                if (mode === "version") f.state.version = 2;
                if (mode === "fallback-hash") f.state.root = "f".repeat(64);
                if (mode === "fallback-count") f.state.files++;
                if (mode === "cooperate") throw reason;
            },
        }));
        const error = failure(result); assert(invalidated);
        if (["abort", "scope", "cooperate"].includes(mode)) assert.equal(error, reason);
        else assert(error instanceof RootTreeRepairError);
        assert.equal(f.pages.length, 1); assert.equal(f.state.finish, 0); assert.equal(f.state.cancel, 1);
        assert.equal(f.state.committedRevision, mode === "committed" ? 5 : 4);
        f.assertReleased(); f.budget.close();
    }
}

async function externalCheckCannotInvalidateThenPublish(): Promise<void> {
    for (const mode of ["base", "abort"] as const) {
        const f = fixture(rows(1)), controller = new AbortController();
        const reason = new Error(`reentrant final ${mode}`); let ready = false, injected = false;
        const result = await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, controller.signal, {
            budget: f.port, cooperate: async () => { if (f.pages.length === 1) ready = true; },
            assertCurrent() {
                if (!ready || injected) return;
                injected = true;
                if (mode === "base") f.base.mutate(); else controller.abort(reason);
            },
        }));
        const error = failure(result); assert(injected);
        if (mode === "abort") assert.equal(error, reason);
        else assert(error instanceof RootTreeRepairError && error.code === "BASE_CHANGED");
        assert.equal(f.state.finish, 0); assert.equal(f.state.cancel, 1); assert.equal(f.state.root, OLD_ROOT);
        f.assertReleased(); f.budget.close();
    }
    // Replacing a caller-owned options property cannot remove the original
    // engine/wrapper scope witness after the operation has yielded.
    const f = fixture(rows(257)), reason = new Error("original scope invalidated");
    let scopeCurrent = true;
    const options = {
        budget: f.port,
        cooperate: async () => {
            if (f.pages.length !== 1) return;
            scopeCurrent = false; options.assertCurrent = () => {};
        },
        assertCurrent() { if (!scopeCurrent) throw reason; },
    };
    try {
        const result = await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined, options));
        assert.equal(failure(result), reason);
        assert.equal(f.state.finish, 0); assert.equal(f.state.cancel, 1);
        f.assertReleased();
    } finally { f.budget.close(); }
}

async function finalFeedAwaitAndEmptyFinishAreFenced(): Promise<void> {
    for (const mode of ["late-abort", "late-base", "empty-abort"] as const) {
        const f = fixture(rows(mode === "empty-abort" ? 0 : 1)), controller = new AbortController();
        const reason = new Error(`before-finish ${mode}`); let afterFeedYield = false, scheduled = false, ran = false;
        const result = await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, controller.signal, {
            budget: f.port,
            cooperate: async () => {
                if (f.pages.length === 1) afterFeedYield = true;
                if (mode === "empty-abort" && f.state.active) { ran = true; controller.abort(reason); }
            },
            assertCurrent() {
                if (!afterFeedYield || scheduled) return;
                scheduled = true;
                // Queued from flush's final check: it runs after that check,
                // but before the caller resumes its `await flush()`.
                queueMicrotask(() => {
                    ran = true;
                    if (mode === "late-base") f.base.mutate(); else controller.abort(reason);
                });
            },
        }));
        const error = failure(result); assert(ran);
        if (mode === "late-base") assert(error instanceof RootTreeRepairError && error.code === "BASE_CHANGED");
        else assert.equal(error, reason);
        assert.equal(f.state.finish, 0, "an invalidated final page continuation published the replacement");
        assert.equal(f.state.cancel, 1); assert.equal(f.state.root, OLD_ROOT);
        assert.equal(f.pages.length, mode === "empty-abort" ? 0 : 1);
        f.assertReleased(); f.budget.close();
    }
}

async function queuedAdmissionHasNoSecondPassOrNativeWork(): Promise<void> {
    for (const mode of ["success", "abort", "base", "owner", "shrink"] as const) {
        const f = fixture(rows(257)), controller = new AbortController(), entered = gate();
        let calls = 0, blocker: Awaited<ReturnType<ResourceBudget["reserve"]>> | undefined;
        const reason = new Error("queued repair stopped");
        const pending = outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, controller.signal, {
            budget: { reserve: async (bytes, options) => {
                if (++calls === 2) {
                    assert.equal(f.budget.snapshot().usedBytes, 0, "scratch lease survived into full admission");
                    blocker = await f.budget.reserve(f.budget.snapshot().capacityBytes);
                    const work = f.port.reserve(bytes, options); entered.resolve(); return work;
                }
                return f.port.reserve(bytes, options);
            } }, cooperate: async () => {},
        }));
        try {
            await entered.promise;
            assert.equal(f.base.iterations, 1); assert.equal(f.base.visited, 257);
            assert.equal(f.state.begin, 0); assert.equal(f.pages.length, 0);
            assert.equal(f.budget.snapshot().queuedRequests, 1);
            if (mode === "abort") controller.abort(reason);
            if (mode === "base") f.base.mutate();
            if (mode === "owner") f.state.committedRevision++;
            if (mode === "shrink") f.budget.setCapacity(ROOT_TREE_REPAIR_LIMITS.scratchBytes);
            blocker!.release(); blocker = undefined;
            const result = await pending;
            if (mode === "success") { assert("value" in result); f.assertReleased(); }
            else {
                const error = failure(result);
                if (mode === "abort") assert.equal(error, reason);
                if (mode === "shrink") assert(error instanceof ResourceBudgetOversizedError);
                assert.equal(f.state.begin, 0); assert.equal(f.base.iterations, 1);
                assert.equal(f.budget.snapshot().usedBytes, 0); assert.equal(f.budget.snapshot().queuedRequests, 0);
            }
        } finally { blocker?.release(); await pending; f.budget.close(); }
    }
}

async function nativeErrorsAndProgressReleaseOwners(): Promise<void> {
    for (const mode of ["begin", "append", "progress", "finish"] as const) {
        const f = fixture(rows(257)), reason = new Error(`native ${mode} failed`);
        f.hooks.native = phase => { if (phase === mode) throw reason; };
        if (mode === "progress") f.hooks.returnedCount = value => value + 1;
        const result = await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
            { budget: f.port, cooperate: async () => {} }));
        const error = failure(result);
        if (mode === "progress") assert(error instanceof RootTreeRepairError && error.code === "INVALID_TREE");
        else assert.equal(error, reason);
        assert.equal(f.state.cancel, mode === "append" || mode === "progress" ? 1 : 0);
        assert.equal(f.state.finish, mode === "finish" ? 1 : 0);
        assert.equal(f.state.root, OLD_ROOT); assert.equal(f.state.committedRevision, 4);
        f.assertReleased(); f.budget.close();
    }
}

async function incompleteApiAndInvalidWitnessFailClosed(): Promise<void> {
    const methods = ["begin_replacement_rebuild_job", "append_replacement_rebuild_job", "finish_replacement_rebuild_job", "cancel_tree_job"] as const;
    for (const method of methods) for (const value of [undefined, null, 0, "function"]) {
        const f = fixture(rows(1)); (f.tree as any)[method] = value;
        const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
            { budget: f.port, cooperate: async () => {} })));
        assert(error instanceof RootTreeRepairError && error.code === "INVALID_TREE");
        assert.equal(f.allocations.length, 0); assert.equal(f.state.legacy, 0); assert.equal(f.state.begin, 0);
        f.budget.close();
    }
    for (const witness of ["committed_revision", "candidate_revision"] as const) {
        for (const invalid of [NaN, -1, Infinity, 0.5, Number.MAX_SAFE_INTEGER + 1]) {
            const f = fixture(rows(1)); f.tree[witness] = () => invalid;
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
                { budget: f.port, cooperate: async () => {} })));
            assert(error instanceof RootTreeRepairError && error.code === "INVALID_TREE");
            assert.equal(f.allocations.length, 0); assert.equal(f.state.begin, 0); f.budget.close();
        }
    }
}

function inputEstimatorBoundaries(): void {
    for (const bytes of [2, PAGE_BYTES - 1, PAGE_BYTES, PAGE_BYTES + 1, ROOT_TREE_REPAIR_LIMITS.serializedBytes]) {
        for (const count of [0, 1, 256, ROOT_TREE_REPAIR_LIMITS.entries]) {
            assert.equal(estimatePagedRootTreeRepairWorkset(bytes, count),
                2 * bytes + 128 * count + 6 * Math.min(bytes, PAGE_BYTES) + 128 * 1024);
        }
    }
    for (const invalid of [0, 1, -1, NaN, Infinity, 0.5, ROOT_TREE_REPAIR_LIMITS.serializedBytes + 1, Number.MAX_SAFE_INTEGER]) {
        assert.throws(() => estimatePagedRootTreeRepairWorkset(invalid, 0), RootTreeRepairError);
    }
    for (const invalid of [-1, NaN, Infinity, 0.5, ROOT_TREE_REPAIR_LIMITS.entries + 1, Number.MAX_SAFE_INTEGER]) {
        assert.throws(() => estimatePagedRootTreeRepairWorkset(2, invalid), RootTreeRepairError);
    }
}

async function resumableBuildStepsAndActualYields(): Promise<void> {
    for (const item of [{ count: 0, work: 0 }, { count: 1, work: 513 }, { count: 257, work: 770 }]) {
        const f = fixture(rows(item.count)); f.enableBuild(item.work);
        let yieldedStep = 0, taskStep = 0;
        const afterSteps: number[] = [], scheduled: ReturnType<typeof setImmediate>[] = [];
        f.hooks.native = phase => {
            if (phase === "start") assert.equal(f.state.received, item.count);
            if (phase === "step") {
                if (f.state.steps > 1) {
                    assert.equal(yieldedStep, f.state.steps - 1, "next step preceded prior step cooperation");
                    assert.equal(taskStep, f.state.steps - 1, "next step preceded a real host task");
                }
                assert.equal(f.state.root, OLD_ROOT); assert.equal(f.state.committedRevision, 4);
                const step = f.state.steps;
                scheduled.push(setImmediate(() => { taskStep = step; }));
            }
            if (phase === "finish") {
                assert.equal(yieldedStep, f.state.steps, "finish skipped the final done checkpoint");
                assert.equal(taskStep, f.state.steps, "finish preceded the final real host task");
                assert.equal(f.state.root, OLD_ROOT);
            }
        };
        try {
            const result = await rebuildRootTreeFromBase(f.base, f.tree, 2, undefined, {
                budget: f.port, cooperate: async signal => {
                    if (f.state.steps > yieldedStep) { yieldedStep = f.state.steps; afterSteps.push(yieldedStep); }
                    await yieldWork({ signal });
                },
            });
            const expectedSteps = Math.max(1, Math.ceil(item.work / 256));
            assert.equal(f.state.starts, 1); assert.equal(f.state.steps, expectedSteps);
            assert.equal(f.state.finish, 1); assert.equal(f.state.cancel, 0); assert.equal(f.state.atomicFinishes, 0);
            assert.deepEqual(f.stepLimits, Array(expectedSteps).fill(256));
            assert.deepEqual(afterSteps, Array.from({ length: expectedSteps }, (_, index) => index + 1));
            assert.equal(f.buildProgress.at(-1)?.completed, item.work);
            assert.equal(f.buildProgress.at(-1)?.done, true);
            assert.equal(result.rebuiltRoot, NEW_ROOT); assert.equal(result.capturedBaseRoot, BASE_ROOT);
            assert.equal(f.state.committedRevision, 5);
            f.assertReleased();
        } finally { for (const task of scheduled) clearImmediate(task); f.budget.close(); }
    }
}

async function buildCheckpointInvalidationCancels(): Promise<void> {
    for (const mode of ["abort", "base", "scope", "committed", "candidate", "version", "cooperate"] as const) {
        const f = fixture(rows(1)), controller = new AbortController(), reason = new Error(`build checkpoint ${mode}`);
        const entered = gate(), release = gate(); let currentScope = true, settled = false;
        f.enableBuild();
        const pending = outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, controller.signal, {
            budget: f.port, assertCurrent() { if (!currentScope) throw reason; },
            cooperate: async () => {
                if (f.state.steps !== 1) return;
                entered.resolve(); await release.promise;
                if (mode === "cooperate") throw reason;
            },
        })).finally(() => { settled = true; });
        try {
            await entered.promise;
            assert.equal(settled, false); assert.equal(f.state.root, OLD_ROOT);
            assert(f.state.active && f.state.buildStarted && !f.state.buildDone);
            assert.equal(f.budget.snapshot().usedBytes, f.workBytes);
            assert.deepEqual(f.releases, [ROOT_TREE_REPAIR_LIMITS.scratchBytes]);
            if (mode === "abort") controller.abort(reason);
            if (mode === "base") f.base.mutate();
            if (mode === "scope") currentScope = false;
            if (mode === "committed") f.state.committedRevision++;
            if (mode === "candidate") f.state.candidateRevision++;
            if (mode === "version") f.state.version = 2;
            release.resolve(); const error = failure(await pending);
            if (["abort", "scope", "cooperate"].includes(mode)) assert.equal(error, reason);
            else assert(error instanceof RootTreeRepairError);
            assert.equal(f.state.steps, 1); assert.equal(f.state.finish, 0); assert.equal(f.state.cancel, 1);
            assert.equal(f.state.root, OLD_ROOT); f.assertReleased();
        } finally { release.resolve(); await pending; f.budget.close(); }
    }
}

async function finalBuildDoneAwaitStillOwnsCancellation(): Promise<void> {
    for (const mode of ["abort", "base", "scope", "committed", "candidate"] as const) {
        const f = fixture(rows(1)), controller = new AbortController(), reason = new Error(`final build ${mode}`);
        const entered = gate(), release = gate(); let currentScope = true, held = false;
        f.enableBuild(1);
        const pending = outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, controller.signal, {
            budget: f.port, assertCurrent() { if (!currentScope) throw reason; },
            cooperate: async () => {
                if (!f.state.buildDone || held) return;
                held = true; entered.resolve(); await release.promise;
            },
        }));
        try {
            await entered.promise;
            assert(f.state.active && f.state.buildDone); assert.equal(f.state.finish, 0);
            assert.equal(f.state.root, OLD_ROOT); assert.equal(f.budget.snapshot().usedBytes, f.workBytes);
            if (mode === "abort") controller.abort(reason);
            if (mode === "base") f.base.mutate();
            if (mode === "scope") currentScope = false;
            if (mode === "committed") f.state.committedRevision++;
            if (mode === "candidate") f.state.candidateRevision++;
            release.resolve(); const error = failure(await pending);
            if (mode === "abort" || mode === "scope") assert.equal(error, reason);
            else assert(error instanceof RootTreeRepairError);
            assert.equal(f.state.finish, 0, "done progress was treated as publication authority");
            assert.equal(f.state.cancel, 1); assert.equal(f.state.root, OLD_ROOT);
            f.assertReleased();
        } finally { release.resolve(); await pending; f.budget.close(); }
    }
}

async function malformedBuildProgressCannotFinish(): Promise<void> {
    let accessorReads = 0;
    const patches: { label: string; at?: number; apply: (row: BuildProgress) => unknown }[] = [
        { label: "null", apply: () => null },
        { label: "array", apply: row => Object.assign([], row) },
        { label: "missing-done", apply: ({ done: _, ...row }) => row },
        { label: "missing-units", apply: ({ units: _, ...row }) => row },
        { label: "missing-completed", apply: ({ completed: _, ...row }) => row },
        { label: "missing-phase", apply: ({ phase: _, ...row }) => row },
        { label: "extra", apply: row => ({ ...row, extra: true }) },
        { label: "hidden-extra", apply: row => Object.defineProperty({ ...row }, "hidden", { value: true }) },
        { label: "symbol-extra", apply: row => ({ ...row, [Symbol("unexpected")]: true }) },
        { label: "changing-done-accessor", apply: row => Object.defineProperty({ ...row }, "done", {
            enumerable: true,
            // Repeated ordinary property reads would validate false then
            // return true, despite an unfinished native builder. Reject the
            // descriptor without executing this getter at all.
            get() { accessorReads++; return accessorReads >= 3; },
        }) },
        { label: "nonboolean-done", apply: row => ({ ...row, done: 1 }) },
        { label: "units-cap", apply: row => ({ ...row, units: 257, completed: 257 }) },
        { label: "unsafe-units", apply: row => ({ ...row, units: Number.MAX_SAFE_INTEGER + 1 }) },
        { label: "negative-units", apply: row => ({ ...row, units: -1 }) },
        { label: "fractional-units", apply: row => ({ ...row, units: 0.5 }) },
        { label: "nan-units", apply: row => ({ ...row, units: NaN }) },
        { label: "infinite-units", apply: row => ({ ...row, units: Infinity }) },
        { label: "not-done-no-progress", apply: row => ({ ...row, units: 0, completed: 0 }) },
        { label: "unsafe-completed", apply: row => ({ ...row, completed: Number.MAX_SAFE_INTEGER + 1 }) },
        { label: "negative-completed", apply: row => ({ ...row, completed: -1 }) },
        { label: "fractional-completed", apply: row => ({ ...row, completed: 0.5 }) },
        { label: "nan-completed", apply: row => ({ ...row, completed: NaN }) },
        { label: "infinite-completed", apply: row => ({ ...row, completed: Infinity }) },
        { label: "jump-completed", apply: row => ({ ...row, completed: row.completed + 1 }) },
        { label: "backwards-completed", at: 2, apply: row => ({ ...row, completed: 0 }) },
        { label: "reset-at-new-phase", at: 2, apply: row => ({ ...row, completed: row.units, phase: "next" }) },
        { label: "empty-phase", apply: row => ({ ...row, phase: "" }) },
        { label: "long-phase", apply: row => ({ ...row, phase: "x".repeat(65) }) },
        { label: "nonstring-phase", apply: row => ({ ...row, phase: null }) },
    ];
    for (const patch of patches) {
        accessorReads = 0;
        const f = fixture(rows(1)); f.enableBuild(513);
        f.hooks.progress = (row, call) => call === (patch.at ?? 1) ? patch.apply(row) : row;
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
                { budget: f.port, cooperate: async () => {} })));
            assert(error instanceof RootTreeRepairError && error.code === "INVALID_TREE", patch.label);
            assert.equal(accessorReads, 0, "native progress validation invoked an unowned accessor");
            assert.equal(f.state.steps, patch.at ?? 1, patch.label);
            assert.equal(f.state.finish, 0); assert.equal(f.state.cancel, 1); assert.equal(f.state.root, OLD_ROOT);
            f.assertReleased();
        } finally { f.budget.close(); }
    }
    // Phase labels are diagnostics, not an enum gate that blocks a compatible
    // future native phase. The exact 64-character boundary is valid.
    const f = fixture(rows(1)); f.enableBuild(1);
    f.hooks.progress = row => ({ ...row, phase: "future-phase-" + "x".repeat(51) });
    try {
        await rebuildRootTreeFromBase(f.base, f.tree, 2, undefined, { budget: f.port, cooperate: async () => {} });
        assert.equal(f.state.finish, 1); f.assertReleased();
    } finally { f.budget.close(); }
}

async function buildErrorsRetainCancelableOwnership(): Promise<void> {
    for (const phase of ["start", "step", "finish"] as const) {
        const f = fixture(rows(1)), reason = new Error(`native build ${phase}`); f.enableBuild(1);
        f.hooks.native = event => { if (event === phase) throw reason; };
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
                { budget: f.port, cooperate: async () => {} })));
            assert.equal(error, reason);
            assert.equal(f.state.cancel, phase === "finish" ? 0 : 1);
            assert.equal(f.state.finish, phase === "finish" ? 1 : 0);
            assert.equal(f.state.root, OLD_ROOT); assert.equal(f.state.committedRevision, 4);
            f.assertReleased();
        } finally { f.budget.close(); }
    }
}

async function optionalBuildApiIsAtomicAndCaptured(): Promise<void> {
    for (const method of ["start_replacement_rebuild_job", "step_replacement_rebuild_job"] as const) {
        for (const invalid of [undefined, null, 0, "function"]) {
            const f = fixture(rows(1)); f.enableBuild(); (f.tree as any)[method] = invalid;
            try {
                const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
                    { budget: f.port, cooperate: async () => {} })));
                assert(error instanceof RootTreeRepairError && error.code === "INVALID_TREE");
                assert.equal(f.allocations.length, 0); assert.equal(f.state.begin, 0); assert.equal(f.state.legacy, 0);
            } finally { f.budget.close(); }
        }
    }
    // A build extension cannot substitute for missing input ownership.
    const absentInput = fixture(rows(1)); absentInput.enableBuild();
    for (const method of ["begin_replacement_rebuild_job", "append_replacement_rebuild_job", "finish_replacement_rebuild_job"])
        (absentInput.tree as any)[method] = undefined;
    try {
        const error = failure(await outcome(rebuildRootTreeFromBase(absentInput.base, absentInput.tree, 2, undefined,
            { budget: absentInput.port, cooperate: async () => {} })));
        assert(error instanceof RootTreeRepairError && error.code === "INVALID_TREE");
        assert.equal(absentInput.allocations.length, 0); assert.equal(absentInput.state.legacy, 0);
    } finally { absentInput.budget.close(); }
    const f = fixture(rows(1)); f.enableBuild(); let replaced = false;
    try {
        await rebuildRootTreeFromBase(f.base, f.tree, 2, undefined, {
            budget: f.port, cooperate: async () => {
                if (f.state.steps !== 1 || replaced) return;
                replaced = true;
                f.tree.step_replacement_rebuild_job = () => { throw new Error("late step method was reread"); };
                f.tree.start_replacement_rebuild_job = () => { throw new Error("late start method was reread"); };
            },
        });
        assert(replaced); assert.equal(f.state.steps, 3); assert.equal(f.state.finish, 1); f.assertReleased();
    } finally { f.budget.close(); }
}

async function deferredFinishRetainsLeaseAndYields(): Promise<void> {
    for (const totalWork of [0, 513]) {
        const f = fixture(rows(1)); f.enableBuild(1); f.enableRetirement(totalWork);
        const abort = new AbortController(), reached = gate(), resume = gate();
        let scopeValid = true, scopeCalls = 0, scopeAtPublish = 0, marker = false, settled = false;
        let held = false;
        f.hooks.native = phase => {
            if (phase === "deferred-finish") scopeAtPublish = scopeCalls;
            if (phase === "retire") {
                if (f.state.retirementSteps) assert(marker, "next retirement step starved an actual host task");
                marker = false; setImmediate(() => { marker = true; });
                assert.equal(f.state.root, NEW_ROOT, "retirement changed the committed publication");
            }
        };
        try {
            const operation = outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, abort.signal, {
                budget: f.port,
                assertCurrent: () => { scopeCalls++; assert(scopeValid, "retirement rechecked the old scope"); },
                cooperate: async signal => {
                    if (!f.state.retirementSteps) return;
                    assert.equal(signal, undefined, "retirement inherited the operation abort signal");
                    if (!held) { held = true; reached.resolve(); await resume.promise; }
                    await yieldWork();
                },
            })).then(result => { settled = true; return result; });
            await reached.promise;
            assert.equal(settled, false); assert.equal(f.budget.snapshot().usedBytes, f.workBytes);
            assert(hasPendingRootTreeRetirement(f.tree));
            resume.resolve();
            const result = await operation;
            assert("value" in result); assert.equal(result.value.rebuiltRoot, NEW_ROOT);
            assert(marker, "final retirement step did not yield to an actual task");
            assert(scopeCalls >= scopeAtPublish);
            assert.equal(f.state.retirementSteps, Math.max(1, Math.ceil(totalWork / 256)));
            assert.equal(f.state.deferredFinishes, 1); assert.equal(f.state.deferredCancels, 0);
            assert.equal(f.state.finish, 0); assert.equal(f.state.cancel, 0);
            assert(!hasPendingRootTreeRetirement(f.tree)); f.assertReleased();
        } finally { resume.resolve(); f.budget.close(); }
    }
}

async function publicationResultRechecksAfterRetirement(): Promise<void> {
    for (const cut of ["partial", "done"] as const) {
        for (const mode of ["abort", "base", "scope", "committed", "candidate", "version", "hash", "count"] as const) {
            const f = fixture(rows(1)); f.enableBuild(1); f.enableRetirement(cut === "done" ? 1 : 513);
            if (mode === "hash" || mode === "count") f.tree.committed_revision = undefined;
            const abort = new AbortController(), reached = gate(), resume = gate();
            const scopeError = new Error("publication scope changed during retirement");
            let held = false, scopeValid = true, settled = false;
            try {
                const operation = outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, abort.signal, {
                    budget: f.port,
                    assertCurrent: () => { if (!scopeValid) throw scopeError; },
                    cooperate: async signal => {
                        if (!f.state.retirementSteps) return;
                        assert.equal(signal, undefined);
                        if (!held) { held = true; reached.resolve(); await resume.promise; }
                        // Every cleanup step must continue even after these
                        // originally authorizing guards have become invalid.
                        await yieldWork();
                    },
                })).then(result => { settled = true; return result; });
                await reached.promise;
                assert.equal(f.state.root, NEW_ROOT); assert.equal(settled, false);
                assert(hasPendingRootTreeRetirement(f.tree)); assert.equal(f.budget.snapshot().usedBytes, f.workBytes);
                switch (mode) {
                    case "abort": abort.abort(); break;
                    case "base": f.base.mutate(); break;
                    case "scope": scopeValid = false; break;
                    case "committed": f.state.committedRevision++; break;
                    case "candidate": f.state.candidateRevision++; break;
                    case "version": f.state.version = 1; break;
                    case "hash": f.state.root = "e".repeat(64); break;
                    case "count": f.state.files++; break;
                }
                const visibleRoot = f.state.root, committedRevision = f.state.committedRevision;
                resume.resolve();
                const error = failure(await operation);
                if (mode === "abort") assert(error instanceof Error && error.name === "AbortError");
                else if (mode === "scope") assert.equal(error, scopeError);
                else assert(error instanceof RootTreeRepairError && error.code === (mode === "base" ? "BASE_CHANGED" : "INVALID_TREE"));
                assert.equal(f.state.retirementSteps, cut === "done" ? 1 : 3, "stale result interrupted mandatory cleanup");
                assert.equal(f.state.deferredFinishes, 1); assert.equal(f.state.deferredCancels, 0);
                assert.equal(f.state.root, visibleRoot, "late stale result rolled back committed state");
                assert.equal(f.state.committedRevision, committedRevision);
                assert(!hasPendingRootTreeRetirement(f.tree)); f.assertReleased();
            } finally { resume.resolve(); f.budget.close(); }
        }
    }
}

async function deferredCancellationIgnoresAbortAndGuards(): Promise<void> {
    for (const cut of ["input", "build", "done"] as const) {
        const f = fixture(rows(1)); f.enableBuild(cut === "done" ? 1 : 513); f.enableRetirement();
        const abort = new AbortController(), reached = gate(), resume = gate();
        let aborted = false, held = false, marker = false;
        f.hooks.native = phase => {
            if (phase === "retire") {
                if (f.state.retirementSteps) assert(marker);
                marker = false; setImmediate(() => { marker = true; });
                assert.equal(f.state.root, OLD_ROOT);
            }
        };
        try {
            const result = outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, abort.signal, {
                budget: f.port,
                assertCurrent: () => { if (aborted) throw new Error("cleanup checked stale scope"); },
                cooperate: async signal => {
                    if (f.state.retiring || f.state.retirementSteps) {
                        assert.equal(signal, undefined);
                        if (!held) { held = true; reached.resolve(); await resume.promise; }
                        await yieldWork(); return;
                    }
                    if (!aborted && (cut === "input" ? f.state.received > 0 : f.state.steps > 0)) {
                        aborted = true; abort.abort();
                    }
                },
            }));
            await reached.promise;
            assert(hasPendingRootTreeRetirement(f.tree)); assert.equal(f.budget.snapshot().usedBytes, f.workBytes);
            assert.equal(f.state.active, true); assert.equal(f.state.root, OLD_ROOT);
            resume.resolve();
            const error = failure(await result);
            assert(error instanceof Error && error.name === "AbortError");
            assert.equal(f.state.deferredCancels, 1); assert.equal(f.state.deferredFinishes, 0);
            assert.equal(f.state.retirementSteps, 3); assert.equal(f.state.cancel, 0); assert(marker);
            assert.equal(f.state.root, OLD_ROOT); assert.equal(f.state.committedRevision, 4);
            f.assertReleased();
        } finally { resume.resolve(); f.budget.close(); }
    }
}

async function deferredErrorsRetainThenDrainExactOwner(): Promise<void> {
    for (const cut of ["append", "start", "step", "deferred-finish"] as const) {
        const f = fixture(rows(1)); f.enableBuild(1); f.enableRetirement(257);
        const expected = new Error(`native ${cut} failed`);
        f.hooks.native = phase => { if (phase === cut) throw expected; };
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
                { budget: f.port, cooperate: async () => {} })));
            assert.equal(error, expected); assert.equal(f.state.deferredCancels, 1);
            assert.equal(f.state.deferredFinishes, 0); assert.equal(f.state.retirementSteps, 2);
            assert.equal(f.state.root, OLD_ROOT); assert.equal(f.state.cancel, 0); f.assertReleased();
        } finally { f.budget.close(); }
    }
    for (const cut of ["deferred-cancel", "retire"] as const) {
        const f = fixture(rows(1)); f.enableBuild(1); f.enableRetirement(257);
        const original = new Error("source build failed"), cleanup = new Error(`native ${cut} failed`);
        let fault = true;
        f.hooks.native = phase => {
            if (phase === "start") throw original;
            if (phase === cut && fault) throw cleanup;
        };
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
                { budget: f.port, cooperate: async () => {} })));
            assert(error instanceof RootTreeRetirementError);
            assert.equal(error.cleanupCause, cleanup); assert.equal(error.originalCause, original);
            assert(hasPendingRootTreeRetirement(f.tree)); assert.equal(f.state.active, true);
            assert.equal(f.budget.snapshot().usedBytes, f.workBytes);
            assert.deepEqual(f.releases, [ROOT_TREE_REPAIR_LIMITS.scratchBytes]);
            fault = false;
            const first = drainRootTreeRetirement(f.tree), second = drainRootTreeRetirement(f.tree);
            assert.equal(first, second, "concurrent cleanup callers did not join one flight");
            await first;
            assert.equal(f.state.deferredCancels, 1); assert.equal(f.state.retirementSteps, 2);
            assert.equal(f.state.root, OLD_ROOT); assert(!hasPendingRootTreeRetirement(f.tree));
            f.assertReleased();
        } finally { f.budget.close(); }
    }
}

async function deferredRetirementBlocksNewAdmissionAndPinsMethods(): Promise<void> {
    const f = fixture(rows(1)); f.enableBuild(1); f.enableRetirement(257);
    const reached = gate(), resume = gate(), abortNext = new AbortController(); let held = false;
    try {
        const first = outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined, {
            budget: f.port, cooperate: async () => {
                if (f.state.retirementSteps !== 1 || held) return;
                held = true; reached.resolve(); await resume.promise;
            },
        }));
        await reached.promise;
        const before = f.allocations.slice();
        // Neither registry retry nor a new repair may reread method properties.
        f.tree.cancel_replacement_rebuild_job_deferred = () => { throw new Error("reread deferred cancel"); };
        f.tree.finish_replacement_rebuild_job_deferred = () => { throw new Error("reread deferred finish"); };
        f.tree.step_tree_retirement = () => { throw new Error("reread retirement step"); };
        let hookReads = 0;
        const originalHook = () => {}, replacementHook = () => {};
        let currentHook = originalHook;
        const nextOptions = {
            budget: f.port,
            cooperate: async () => {},
            get onNodePayloadPlan() { hookReads++; return currentHook; },
        };
        const next = outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, abortNext.signal, nextOptions));
        assert.equal(hookReads, 1, "policy hook was not captured before waiting for prior retirement");
        currentHook = replacementHook;
        await Promise.resolve(); await Promise.resolve();
        assert.deepEqual(f.allocations, before); assert.equal(f.state.begin, 1);
        abortNext.abort(); resume.resolve();
        assert("value" in await first);
        const error = failure(await next); assert(error instanceof Error && error.name === "AbortError");
        assert.equal(f.state.begin, 1); assert.equal(f.state.retirementSteps, 2); f.assertReleased();
    } finally { resume.resolve(); f.budget.close(); }

    for (const cancel of [false, true]) {
        const pinned = fixture(rows(1)); pinned.enableBuild(1); pinned.enableRetirement(1);
        const abort = new AbortController(); let replaced = false;
        try {
            const result = await outcome(rebuildRootTreeFromBase(pinned.base, pinned.tree, 2, abort.signal, {
                budget: pinned.port, cooperate: async () => {
                    if (replaced || !pinned.state.buildDone) return;
                    replaced = true;
                    pinned.tree.cancel_replacement_rebuild_job_deferred = () => { throw new Error("late cancel property read"); };
                    pinned.tree.finish_replacement_rebuild_job_deferred = () => { throw new Error("late finish property read"); };
                    pinned.tree.step_tree_retirement = () => { throw new Error("late retirement property read"); };
                    if (cancel) abort.abort();
                },
            }));
            assert(replaced);
            if (cancel) {
                const error = failure(result); assert(error instanceof Error && error.name === "AbortError");
                assert.equal(pinned.state.deferredCancels, 1); assert.equal(pinned.state.root, OLD_ROOT);
            } else { assert("value" in result); assert.equal(pinned.state.deferredFinishes, 1); }
            pinned.assertReleased();
        } finally { pinned.budget.close(); }
    }
}

async function concurrentPreBeginRepairsCannotReplaceOwnership(): Promise<void> {
    const f = fixture(rows(1)); f.enableBuild(1); f.enableRetirement(1);
    f.budget.setCapacity(f.workBytes);
    const secondBudget = new ResourceBudget({ capacityBytes: f.workBytes });
    const secondPort = { reserve: async (bytes: number, options?: { signal?: AbortSignal }) => {
        const lease = await secondBudget.reserve(bytes, options);
        return { bytes: lease.bytes, release: () => lease.release() };
    } };
    const entered = gate(), resume = gate(); let held = 0;
    const cooperate = async () => {
        if (held >= 2 || f.state.active) return;
        held++; if (held === 2) entered.resolve(); await resume.promise;
    };
    try {
        const first = outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
            { budget: f.port, cooperate }));
        const second = outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
            { budget: secondPort, cooperate }));
        await entered.promise; assert.equal(f.state.begin, 0);
        resume.resolve();
        const [firstResult, secondResult] = await Promise.all([first, second]);
        assert("value" in firstResult, "first pre-begin owner did not finish");
        const error = failure(secondResult);
        assert(error instanceof RootTreeRepairError && error.code === "INVALID_TREE");
        assert.match(error.message, /active repair owner/);
        assert.equal(f.state.begin, 1); assert.equal(f.state.deferredFinishes, 1);
        assert.equal(f.state.deferredCancels, 0); assert.equal(f.state.retirementSteps, 1);
        assert(!hasPendingRootTreeRetirement(f.tree));
        assert.equal(secondBudget.snapshot().usedBytes, 0);
        assert.equal(secondBudget.snapshot().activeReservations, 0);
        f.assertReleased();
    } finally { resume.resolve(); secondBudget.close(); f.budget.close(); }
}

async function retirementFailuresAreNotFalseCompletion(): Promise<void> {
    for (const final of [false, true]) {
        const f = fixture(rows(1)); f.enableBuild(1); f.enableRetirement(final ? 1 : 257);
        const expected = new Error("cleanup scheduler failed"); let fault = true;
        try {
            const result = outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined, {
                budget: f.port, cooperate: async () => {
                    if (f.state.retirementSteps && fault) throw expected;
                },
            }));
            const error = failure(await result);
            if (final) {
                assert.equal(error, expected); assert(!hasPendingRootTreeRetirement(f.tree)); f.assertReleased();
            } else {
                assert(error instanceof RootTreeRetirementError && error.cleanupCause === expected);
                assert(hasPendingRootTreeRetirement(f.tree)); assert.equal(f.budget.snapshot().usedBytes, f.workBytes);
                fault = false; await drainRootTreeRetirement(f.tree); f.assertReleased();
            }
            assert.equal(f.state.root, NEW_ROOT); assert.equal(f.state.deferredFinishes, 1);
        } finally { f.budget.close(); }
    }
    const invalid = [
        null, {}, { done: false, units: 0, completed: 0 }, { done: false, units: 257, completed: 257 },
        { done: false, units: 1, completed: 2 }, { done: false, units: 1.5, completed: 1.5 },
        { done: true, units: 0, completed: Number.MAX_SAFE_INTEGER + 1 },
        { done: false, units: 1, completed: 1, extra: true },
        Object.defineProperty({ done: false, units: 1, completed: 1 }, "hidden", { value: 1 }),
        { done: false, units: 1, completed: 1, [Symbol("unexpected")]: 1 },
    ];
    let accessorCalls = 0;
    invalid.push(Object.defineProperty({ units: 1, completed: 1 }, "done", {
        enumerable: true, get() { accessorCalls++; return true; },
    }) as any);
    for (const value of invalid) {
        const f = fixture(rows(1)); f.enableBuild(1); f.enableRetirement(1);
        const realStep = f.tree.step_tree_retirement!; let malformed = true;
        // The synthetic corrupt port reports invalid data without consuming
        // its owner. Retry models a repaired port, not inferred native progress.
        f.tree.step_tree_retirement = (token, units) => malformed ? value : realStep(token, units);
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
                { budget: f.port, cooperate: async () => {} })));
            assert(error instanceof RootTreeRetirementError);
            assert(hasPendingRootTreeRetirement(f.tree)); assert.equal(f.budget.snapshot().usedBytes, f.workBytes);
            assert.equal(f.state.active, true); assert.equal(f.state.retirementSteps, 0);
            assert.equal(accessorCalls, 0);
            malformed = false; await drainRootTreeRetirement(f.tree); f.assertReleased();
        } finally { f.budget.close(); }
    }
}

async function partialRetirementApiFailsBeforeAllocation(): Promise<void> {
    const methods = ["cancel_replacement_rebuild_job_deferred", "finish_replacement_rebuild_job_deferred", "step_tree_retirement"] as const;
    for (const method of methods) for (const bad of [undefined, null, 0, "function"]) {
        const f = fixture(rows(1)); f.enableRetirement(); (f.tree as any)[method] = bad;
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
                { budget: f.port, cooperate: async () => {} })));
            assert(error instanceof RootTreeRepairError && error.code === "INVALID_TREE");
            assert.equal(f.allocations.length, 0); assert.equal(f.state.begin, 0);
        } finally { f.budget.close(); }
    }
    const f = fixture(rows(1)); f.enableRetirement();
    f.tree.start_replacement_rebuild_job = undefined; f.tree.step_replacement_rebuild_job = undefined;
    try {
        const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
            { budget: f.port, cooperate: async () => {} })));
        assert(error instanceof RootTreeRepairError && error.code === "INVALID_TREE");
        assert.equal(f.allocations.length, 0); assert.equal(f.state.begin, 0);
    } finally { f.budget.close(); }
}

async function v2NodePlanPausesBeforeResume(): Promise<void> {
    const f = fixture(rows(3)); f.enableBuild(2); f.enablePlan(); f.enableRetirement(1);
    const reached = gate(), resume = gate(); let observed: unknown;
    try {
        const operation = outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined, {
            budget: f.port,
            cooperate: async () => {},
            onNodePayloadPlan: async plan => {
                observed = plan;
                assert(Object.isFrozen(plan));
                assert.deepEqual(plan, { nodeCount: 3, leafCount: 2, internalCount: 1,
                    nodePayloadBytes: 123_456, maxNodeBytes: 65_536, storedRootBytes: 256 });
                reached.resolve(); await resume.promise;
            },
        }));
        const boundary = await Promise.race([
            reached.promise.then(() => ({ reached: true } as const)),
            operation.then(settled => ({ reached: false, settled } as const)),
        ]);
        assert(boundary.reached, "node plan operation settled before reaching its policy hook");
        assert.equal(f.state.planReads, 1); assert.equal(f.state.planResumes, 0);
        assert.equal(f.state.steps, 1); assert.equal(f.state.deferredFinishes, 0);
        assert.equal(f.state.root, OLD_ROOT);
        assert.equal(f.budget.snapshot().usedBytes, f.workBytes, "node plan policy lost its admitted input owner");
        assert.deepEqual(f.phases.slice(-2), ["step", "plan"]);
        resume.resolve();
        const result = await operation;
        assert("value" in result); assert.equal(result.value.rebuiltRoot, NEW_ROOT); assert.equal(observed !== undefined, true);
        assert.equal(f.state.planResumes, 1); assert.equal(f.state.steps, 2);
        assert(f.phases.indexOf("resume") < f.phases.lastIndexOf("step"));
        f.assertReleased();
    } finally { resume.resolve(); f.budget.close(); }

    const rejected = fixture(rows(1)); rejected.enableBuild(2); rejected.enablePlan(); rejected.enableRetirement(1);
    const expected = new Error("node payload policy refused");
    try {
        const error = failure(await outcome(rebuildRootTreeFromBase(rejected.base, rejected.tree, 2, undefined, {
            budget: rejected.port, cooperate: async () => {}, onNodePayloadPlan: () => { throw expected; },
        })));
        assert.equal(error, expected); assert.equal(rejected.state.planReads, 1);
        assert.equal(rejected.state.planResumes, 0); assert.equal(rejected.state.steps, 1);
        assert.equal(rejected.state.deferredCancels, 1); assert.equal(rejected.state.root, OLD_ROOT);
        rejected.assertReleased();
    } finally { rejected.budget.close(); }

    const pinned = fixture(rows(1)); pinned.enableBuild(2); pinned.enablePlan(); pinned.enableRetirement(1);
    let originalCalls = 0, replacementCalls = 0, replaced = false;
    const mutableOptions = {
        budget: pinned.port,
        cooperate: async () => {},
        onNodePayloadPlan: () => { originalCalls++; },
    };
    mutableOptions.cooperate = async () => {
        if (!replaced) {
            replaced = true;
            mutableOptions.onNodePayloadPlan = () => { replacementCalls++; };
        }
    };
    try {
        const result = await rebuildRootTreeFromBase(pinned.base, pinned.tree, 2, undefined, mutableOptions);
        assert.equal(result.rebuiltRoot, NEW_ROOT);
        assert.equal(originalCalls, 1); assert.equal(replacementCalls, 0);
        assert.equal(pinned.state.planResumes, 1); pinned.assertReleased();
    } finally { pinned.budget.close(); }

    const invalidated = fixture(rows(1)); invalidated.enableBuild(2); invalidated.enablePlan(); invalidated.enableRetirement(1);
    const realReadPlan = invalidated.tree.replacement_rebuild_plan_job!; let policyCalls = 0;
    invalidated.tree.replacement_rebuild_plan_job = token => {
        const plan = realReadPlan(token); invalidated.base.mutate(); return plan;
    };
    try {
        const error = failure(await outcome(rebuildRootTreeFromBase(invalidated.base, invalidated.tree, 2, undefined, {
            budget: invalidated.port, cooperate: async () => {}, onNodePayloadPlan: () => { policyCalls++; },
        })));
        assert(error instanceof RootTreeRepairError && error.code === "BASE_CHANGED");
        assert.equal(policyCalls, 0); assert.equal(invalidated.state.planResumes, 0);
        assert.equal(invalidated.state.deferredCancels, 1); assert.equal(invalidated.state.root, OLD_ROOT);
        invalidated.assertReleased();
    } finally { invalidated.budget.close(); }

    const drifted = fixture(rows(1)); drifted.enableBuild(2); drifted.enablePlan(); drifted.enableRetirement(1);
    const policyReached = gate(), policyRelease = gate();
    try {
        const operation = outcome(rebuildRootTreeFromBase(drifted.base, drifted.tree, 2, undefined, {
            budget: drifted.port,
            cooperate: async () => {},
            onNodePayloadPlan: async () => { policyReached.resolve(); await policyRelease.promise; },
        }));
        await policyReached.promise;
        assert.equal(drifted.state.planResumes, 0); assert.equal(drifted.state.deferredFinishes, 0);
        assert.equal(drifted.budget.snapshot().usedBytes, drifted.workBytes);
        drifted.base.mutate(); policyRelease.resolve();
        const error = failure(await operation);
        assert(error instanceof RootTreeRepairError && error.code === "BASE_CHANGED");
        assert.equal(drifted.state.planResumes, 0); assert.equal(drifted.state.deferredCancels, 1);
        assert.equal(drifted.state.root, OLD_ROOT); drifted.assertReleased();
    } finally { policyRelease.resolve(); drifted.budget.close(); }
}

async function malformedOrPartialNodePlanFailsClosed(): Promise<void> {
    const absent = fixture(rows(1)); let absentPolicyCalls = 0;
    try {
        const error = failure(await outcome(rebuildRootTreeFromBase(absent.base, absent.tree, 2, undefined, {
            budget: absent.port, cooperate: async () => {}, onNodePayloadPlan: () => { absentPolicyCalls++; },
        })));
        assert(error instanceof RootTreeRepairError && error.code === "INVALID_TREE");
        assert.equal(absentPolicyCalls, 0); assert.equal(absent.allocations.length, 0); assert.equal(absent.state.begin, 0);
    } finally { absent.budget.close(); }

    const omitted = fixture(rows(1)); omitted.enableBuild(1); omitted.enableRetirement(1);
    let omittedPlanReads = 0, omittedResumes = 0, omittedPolicyCalls = 0;
    omitted.tree.replacement_rebuild_plan_job = () => { omittedPlanReads++; return {}; };
    omitted.tree.resume_replacement_rebuild_job = () => { omittedResumes++; };
    try {
        const error = failure(await outcome(rebuildRootTreeFromBase(omitted.base, omitted.tree, 2, undefined, {
            budget: omitted.port, cooperate: async () => {}, onNodePayloadPlan: () => { omittedPolicyCalls++; },
        })));
        assert(error instanceof RootTreeRepairError && error.code === "INVALID_TREE");
        assert.equal(omittedPlanReads, 0); assert.equal(omittedResumes, 0); assert.equal(omittedPolicyCalls, 0);
        assert.equal(omitted.state.finish, 0); assert.equal(omitted.state.deferredCancels, 1);
        assert.equal(omitted.state.root, OLD_ROOT); omitted.assertReleased();
    } finally { omitted.budget.close(); }

    for (const method of ["replacement_rebuild_plan_job", "resume_replacement_rebuild_job"] as const) {
        const f = fixture(rows(1)); f.enableBuild(2); f.enablePlan(); (f.tree as any)[method] = undefined;
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
                { budget: f.port, cooperate: async () => {} })));
            assert(error instanceof RootTreeRepairError && error.code === "INVALID_TREE");
            assert.equal(f.allocations.length, 0); assert.equal(f.state.begin, 0);
        } finally { f.budget.close(); }
    }

    const invalid: unknown[] = [
        null, {},
        { nodeCount: 3, leafCount: 2, internalCount: 1, nodePayloadBytes: 2, maxNodeBytes: 3, storedRootBytes: 64 },
        { nodeCount: 2, leafCount: 2, internalCount: 1, nodePayloadBytes: 10, maxNodeBytes: 8, storedRootBytes: 64 },
        { nodeCount: 0, leafCount: 0, internalCount: 0, nodePayloadBytes: 1, maxNodeBytes: 0, storedRootBytes: 64 },
        { nodeCount: 1, leafCount: 1, internalCount: 0, nodePayloadBytes: 8, maxNodeBytes: 8, storedRootBytes: 16_385 },
        { nodeCount: 1, leafCount: 1, internalCount: 0, nodePayloadBytes: Number.MAX_SAFE_INTEGER + 1, maxNodeBytes: 8, storedRootBytes: 64 },
        { nodeCount: 1, leafCount: 1, internalCount: 0, nodePayloadBytes: 8, maxNodeBytes: 8, storedRootBytes: 64, extra: true },
    ];
    let accessorCalls = 0;
    invalid.push(Object.defineProperty({ leafCount: 1, internalCount: 0, nodePayloadBytes: 8,
        maxNodeBytes: 8, storedRootBytes: 64 }, "nodeCount", {
        enumerable: true, get() { accessorCalls++; return 1; },
    }));
    for (const value of invalid) {
        const f = fixture(rows(1)); f.enableBuild(2); f.enablePlan(); f.enableRetirement(1);
        f.tree.replacement_rebuild_plan_job = () => value;
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
                { budget: f.port, cooperate: async () => {} })));
            assert(error instanceof RootTreeRepairError && error.code === "INVALID_TREE");
            assert.equal(f.state.planResumes, 0); assert.equal(f.state.steps, 1);
            assert.equal(f.state.deferredCancels, 1); assert.equal(f.state.root, OLD_ROOT);
            f.assertReleased();
        } finally { f.budget.close(); }
    }
    assert.equal(accessorCalls, 0);
}

async function v2RequestedOutputAdmissionLifecycle(): Promise<void> {
    {
        const f = fixture(rows(3)); f.enableBuild(2); const plan = f.enableOutputPlan(); f.enableRetirement(1);
        const admission = new RootTreeResidentAdmission({ capacityBytes: plan.peakAdmissionBytes });
        let policySnapshot: ReturnType<RootTreeResidentAdmission["snapshot"]> | undefined;
        let registeredBeforeBegin = false;
        let registeredBeforeFinish = false;
        f.hooks.native = phase => {
            if (phase === "begin") registeredBeforeBegin = hasPendingRootTreeRetirement(f.tree);
            if (phase === "deferred-finish") registeredBeforeFinish = hasPendingRootTreeRetirement(f.tree);
        };
        try {
            const result = await rebuildRootTreeFromBase(f.base, f.tree, 2, undefined, {
                budget: f.port, cooperate: async () => {}, residentAdmission: admission,
                onNodePayloadPlan: () => { policySnapshot = admission.snapshot(); },
            });
            assert.equal(result.rebuiltRoot, NEW_ROOT);
            assert(registeredBeforeBegin, "retirement slot was allocated after native begin");
            assert(registeredBeforeFinish, "retirement owner was allocated after native publication");
            assert.equal(f.state.outputPlanReads, 1); assert.equal(f.state.outputPlanResumes, 1);
            assert.equal(policySnapshot?.privateBytes, plan.peakAdmissionBytes,
                "policy hook ran before fail-fast output admission");
            assert.deepEqual(admission.snapshot(), {
                scope: "v2-tree-output-requested-buffers",
                ledger: { capacityBytes: plan.peakAdmissionBytes, usedBytes: plan.residentAdmissionBytes,
                    peakUsedBytes: plan.peakAdmissionBytes,
                    availableBytes: plan.peakAdmissionBytes - plan.residentAdmissionBytes,
                    overcommittedBytes: 0, activeLeases: 1, closed: false,
                    refusedReservations: 0, refusedGrowths: 0 },
                residentTrees: 1, privateAttempts: 0, retiringOwners: 0,
                privateBytes: 0, residentBytes: plan.residentAdmissionBytes, retiringBytes: 0,
            });
            // Models the caller's successful native free boundary.
            admission.releaseResidentAfterFree(f.tree);
            assert.equal(admission.snapshot().ledger.usedBytes, 0);
        } finally { admission.close(); f.budget.close(); }
    }

    {
        const f = fixture(rows(1)); f.enableBuild(2); const plan = f.enableOutputPlan(); f.enableRetirement(1);
        const admission = new RootTreeResidentAdmission({ capacityBytes: plan.peakAdmissionBytes - 1 });
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
                { budget: f.port, cooperate: async () => {}, residentAdmission: admission })));
            assert(error instanceof RootTreeOutputAdmissionDeniedError);
            assert.equal(error.requestedBytes, plan.peakAdmissionBytes);
            assert.equal(f.state.outputPlanReads, 1); assert.equal(f.state.outputPlanResumes, 0);
            assert.equal(f.state.deferredCancels, 1); assert.equal(f.state.root, OLD_ROOT);
            assert.equal(admission.snapshot().ledger.usedBytes, 0);
            assert.equal(admission.snapshot().ledger.refusedReservations, 1);
            f.assertReleased();
        } finally { admission.close(); f.budget.close(); }
    }

    {
        const f = fixture(rows(1)); f.enableBuild(2); const plan = f.enableOutputPlan(); f.enableRetirement(1);
        const admission = new RootTreeResidentAdmission({ capacityBytes: plan.peakAdmissionBytes });
        const mark = admission.markV2GraphComplete.bind(admission);
        let marks = 0;
        admission.markV2GraphComplete = (tree, committed, candidate) => {
            mark(tree, committed, candidate); marks++; f.base.mutate();
        };
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined, {
                budget: f.port, cooperate: async () => {}, residentAdmission: admission,
            })));
            assert(error instanceof RootTreeRepairError && error.code === "BASE_CHANGED",
                "reentrant provenance marker bypassed the final base fence");
            assert.equal(marks, 1); assert.equal(f.state.root, NEW_ROOT);
            assert.equal(admission.snapshot().residentBytes, plan.residentAdmissionBytes,
                "late stale-result rejection released the installed graph");
            admission.releaseResidentAfterFree(f.tree);
        } finally { admission.close(); f.budget.close(); }
    }

    {
        const f = fixture(rows(1)); f.enableBuild(2); const plan = f.enableOutputPlan(); f.enableRetirement(1);
        const admission = new RootTreeResidentAdmission({ capacityBytes: plan.peakAdmissionBytes });
        const entered = gate(), resume = gate();
        try {
            const operation = outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined, {
                budget: f.port, residentAdmission: admission, cooperate: async () => {},
                onNodePayloadPlan: async () => { entered.resolve(); await resume.promise; },
            }));
            await entered.promise;
            assert(hasPendingRootTreeRetirement(f.tree), "active rebuild lost its pre-registered slot");
            const drainError = failure(await outcome(drainRootTreeRetirement(f.tree)));
            assert(drainError instanceof RootTreeRetirementError);
            assert.equal(f.state.deferredCancels, 0, "external drain cancelled an active rebuild");
            assert.equal(f.state.active, true); assert.equal(f.state.root, OLD_ROOT);
            assert.equal(f.budget.snapshot().usedBytes, f.workBytes);
            assert.equal(admission.snapshot().privateBytes, plan.peakAdmissionBytes);
            resume.resolve();
            assert("value" in await operation);
            assert.equal(f.state.deferredFinishes, 1); f.assertReleased();
            admission.releaseResidentAfterFree(f.tree);
        } finally { resume.resolve(); admission.close(); f.budget.close(); }
    }

    {
        const f = fixture(rows(1)); f.enableBuild(2); const plan = f.enableOutputPlan(); f.enableRetirement(1);
        const admission = new RootTreeResidentAdmission({ capacityBytes: plan.peakAdmissionBytes });
        const expected = new Error("policy rejected admitted output");
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined, {
                budget: f.port, cooperate: async () => {}, residentAdmission: admission,
                onNodePayloadPlan: () => { throw expected; },
            })));
            assert.equal(error, expected); assert.equal(f.state.outputPlanResumes, 0);
            assert.equal(f.state.deferredCancels, 1);
            assert.equal(admission.snapshot().ledger.usedBytes, 0,
                "pre-resume policy rejection retained an impossible output owner");
            f.assertReleased();
        } finally { admission.close(); f.budget.close(); }
    }

    {
        const f = fixture(rows(1)); f.enableBuild(3); const plan = f.enableOutputPlan(); f.enableRetirement(257);
        const admission = new RootTreeResidentAdmission({ capacityBytes: plan.peakAdmissionBytes });
        const buildFailure = new Error("injected post-resume progress failure");
        const cleanupFailure = new Error("injected cleanup yield failure");
        let failCleanup = true;
        f.hooks.progress = (progress, call) => { if (call === 2) throw buildFailure; return progress; };
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined, {
                budget: f.port, residentAdmission: admission, cooperate: async () => {
                    if (f.state.retirementSteps && failCleanup) throw cleanupFailure;
                },
            })));
            assert(error instanceof RootTreeRetirementError);
            assert.equal(error.cleanupCause, cleanupFailure); assert.equal(error.originalCause, buildFailure);
            assert(hasPendingRootTreeRetirement(f.tree));
            assert.equal(admission.snapshot().retiringBytes, plan.peakAdmissionBytes);
            assert.equal(admission.snapshot().residentBytes, 0);
            failCleanup = false; await drainRootTreeRetirement(f.tree);
            assert.equal(admission.snapshot().ledger.usedBytes, 0); f.assertReleased();
        } finally { admission.close(); f.budget.close(); }
    }

    {
        const f = fixture(rows(1)); f.enableBuild(2); const plan = f.enableOutputPlan(); f.enableRetirement(1);
        const admission = new RootTreeResidentAdmission({ capacityBytes: plan.peakAdmissionBytes });
        let invalidated = false;
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined, {
                budget: f.port, residentAdmission: admission, cooperate: async () => {
                    if (f.state.retirementSteps === 1 && !invalidated) { invalidated = true; f.base.mutate(); }
                },
            })));
            assert(error instanceof RootTreeRepairError && error.code === "BASE_CHANGED");
            assert.equal(f.state.root, NEW_ROOT);
            assert.equal(admission.snapshot().residentBytes, plan.residentAdmissionBytes,
                "post-publication rejection released the installed resident output");
            admission.releaseResidentAfterFree(f.tree);
        } finally { admission.close(); f.budget.close(); }
    }

    {
        // V1 now crosses the same fail-fast output boundary. A retained
        // deferred-finish failure must retire that exact private allowance and
        // leave the previously resident cohort installed.
        const f = fixture(rows(1)); f.enableBuild(1);
        const plan = f.enableOutputPlan({ schema: 1, scope: "v1-replacement-output",
            nodePayloadBytes: 200, rangeEndpointPeakRequestedBytes: 100,
            rangeEndpointResidentRequestedBytes: 100, peakAdmissionBytes: 300,
            residentAdmissionBytes: 300 });
        f.enableRetirement(1);
        const admission = new RootTreeResidentAdmission({ capacityBytes: 1_000 });
        const old = admission.reserve(f.tree, { peakBytes: 400, residentBytes: 400 })!;
        admission.ready(old);
        admission.releaseRetired(admission.publish(old));
        const expected = new Error("V1 deferred finish failed");
        f.hooks.native = phase => { if (phase === "deferred-finish") throw expected; };
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 1, undefined,
                { budget: f.port, cooperate: async () => {}, residentAdmission: admission })));
            assert.equal(error, expected);
            assert.equal(f.state.deferredCancels, 1); assert.equal(f.state.root, OLD_ROOT);
            assert(!hasPendingRootTreeRetirement(f.tree));
            assert.equal(admission.snapshot().residentBytes, 400,
                "failed V1 replacement released the prior V2 resident cohort");
            assert.equal(admission.snapshot().privateAttempts, 0);
            assert.equal(admission.snapshot().retiringOwners, 0);
            assert.equal(f.state.outputPlanReads, 1); assert.equal(f.state.outputPlanResumes, 1);
            assert.equal(admission.snapshot().ledger.peakUsedBytes, 400 + plan.peakAdmissionBytes);
            f.assertReleased();
            admission.releaseResidentAfterFree(f.tree);
        } finally { admission.close(); f.budget.close(); }
    }
}

async function outputAdmissionRequiresCompleteAbiBeforeAllocation(): Promise<void> {
    for (const partial of ["none", "step", "reader", "resume"] as const) {
        const f = fixture(rows(1)); f.enableBuild(2); f.enablePlan(); f.enableRetirement(1);
        if (partial === "step") f.tree.step_replacement_rebuild_output_memory_v1_job = () => ({});
        if (partial === "reader") f.tree.replacement_rebuild_output_memory_plan_v1_job = () => ({});
        if (partial === "resume") f.tree.resume_replacement_rebuild_output_memory_v1_job = () => {};
        const admission = new RootTreeResidentAdmission({ capacityBytes: 1_000_000 });
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
                { budget: f.port, cooperate: async () => {}, residentAdmission: admission })));
            assert(error instanceof RootTreeRepairError && error.code === "INVALID_TREE");
            assert.equal(f.allocations.length, 0); assert.equal(f.state.begin, 0);
            assert.equal(admission.snapshot().ledger.usedBytes, 0);
        } finally { admission.close(); f.budget.close(); }
    }

    const valid = { schema: 1, scope: "v2-replacement-output", nodePayloadBytes: 123_456,
        rangeEndpointPeakRequestedBytes: 1_000, rangeEndpointResidentRequestedBytes: 200,
        peakAdmissionBytes: 124_456, residentAdmissionBytes: 123_656 };
    const invalid: unknown[] = [
        null, {}, { ...valid, schema: 2 }, { ...valid, scope: "tree-memory" },
        { ...valid, nodePayloadBytes: 123_455 },
        { ...valid, rangeEndpointResidentRequestedBytes: 1_001 },
        { ...valid, peakAdmissionBytes: valid.peakAdmissionBytes + 1 },
        { ...valid, residentAdmissionBytes: valid.residentAdmissionBytes - 1 },
        { ...valid, rangeEndpointPeakRequestedBytes: Number.MAX_SAFE_INTEGER },
        { ...valid, extra: true },
    ];
    let accessorCalls = 0;
    invalid.push(Object.defineProperty({ ...valid }, "scope", {
        enumerable: true, get() { accessorCalls++; return "v2-replacement-output"; },
    }));
    for (const value of invalid) {
        const f = fixture(rows(1)); f.enableBuild(2); f.enableOutputPlan(); f.enableRetirement(1);
        f.tree.replacement_rebuild_output_memory_plan_v1_job = () => value;
        const admission = new RootTreeResidentAdmission({ capacityBytes: 1_000_000 });
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
                { budget: f.port, cooperate: async () => {}, residentAdmission: admission })));
            assert(error instanceof RootTreeRepairError && error.code === "INVALID_TREE");
            assert.equal(f.state.outputPlanResumes, 0); assert.equal(f.state.deferredCancels, 1);
            assert.equal(admission.snapshot().ledger.usedBytes, 0); f.assertReleased();
        } finally { admission.close(); f.budget.close(); }
    }
    assert.equal(accessorCalls, 0);
}

async function v2SortWorkspaceAdmissionAndCompatibility(): Promise<void> {
    for (const input of [rows(0), rows(1), rows(513), rows(100, true)]) {
        const f = fixture(input); f.enableSortPlan(); f.enableRetirement(257);
        try {
            const result = await rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
                { budget: f.port, cooperate: signal => yieldWork({ signal }) });
            assert.equal(result.rebuiltRoot, NEW_ROOT);
            assert.equal(f.workBytes, estimatePagedRootTreeRepairWorkset(f.serializedBytes, input.length) + 8 * input.length);
            assert.equal(f.state.sortPlanReads, 1); assert.equal(f.state.sortStarts, 1);
            assert.equal(f.state.starts, 0, "admitted V2 fell back to the unmeasured start");
            assert(f.phases.indexOf("sort-plan") > f.phases.lastIndexOf("append"));
            assert(f.phases.indexOf("sort-start") > f.phases.indexOf("sort-plan"));
            assert.equal(f.state.deferredFinishes, 1); f.assertReleased();
        } finally { f.budget.close(); }
    }
    const v1 = fixture(rows(3)); v1.enableSortPlan(false);
    try {
        await rebuildRootTreeFromBase(v1.base, v1.tree, 1, undefined,
            { budget: v1.port, cooperate: async () => {} });
        assert.equal(v1.state.starts, 1); assert.equal(v1.state.sortPlanReads, 0);
        assert.equal(v1.state.sortStarts, 0); v1.assertReleased();
    } finally { v1.budget.close(); }
}

async function sortWorkspaceRefusalAndPreBeginAbort(): Promise<void> {
    const refused = fixture(rows(3)); refused.enableSortPlan();
    refused.budget.setCapacity(refused.workBytes - 1);
    try {
        const error = failure(await outcome(rebuildRootTreeFromBase(refused.base, refused.tree, 2, undefined,
            { budget: refused.port, cooperate: async () => {} })));
        assert(error instanceof ResourceBudgetOversizedError);
        assert.equal(refused.state.begin, 0); assert.equal(refused.base.iterations, 1);
        assert.equal(refused.state.sortPlanReads, 0);
        assert.deepEqual(refused.allocations, [ROOT_TREE_REPAIR_LIMITS.scratchBytes, refused.workBytes]);
        assert.deepEqual(refused.releases, [ROOT_TREE_REPAIR_LIMITS.scratchBytes]);
        assert.equal(refused.budget.snapshot().usedBytes, 0);
    } finally { refused.budget.close(); }

    const queued = fixture(rows(3)); queued.enableSortPlan();
    const held = await queued.budget.reserve(1), controller = new AbortController();
    try {
        const operation = outcome(rebuildRootTreeFromBase(queued.base, queued.tree, 2, controller.signal,
            { budget: queued.port, cooperate: async () => {} }));
        await new Promise<void>(resolve => setImmediate(resolve));
        assert.equal(queued.budget.snapshot().queuedRequests, 1);
        assert.equal(queued.state.begin, 0); assert.equal(queued.base.iterations, 1);
        controller.abort();
        assert.equal((failure(await operation) as Error).name, "AbortError");
        assert.equal(queued.state.begin, 0); assert.equal(queued.budget.snapshot().usedBytes, 1);
    } finally { held.release(); queued.budget.close(); }

    const granted = fixture(rows(3)); granted.enableSortPlan();
    const afterGrant = new AbortController();
    try {
        const error = failure(await outcome(rebuildRootTreeFromBase(granted.base, granted.tree, 2, afterGrant.signal, {
            budget: { reserve: async (bytes, options) => {
                const lease = await granted.port.reserve(bytes, options);
                if (bytes === granted.workBytes) afterGrant.abort();
                return lease;
            } }, cooperate: async () => {},
        })));
        assert.equal((error as Error).name, "AbortError");
        assert.equal(granted.state.begin, 0); granted.assertReleased();
    } finally { granted.budget.close(); }
}

async function sortWorkspacePartialAbiFailsBeforeAdmission(): Promise<void> {
    const methods = ["replacement_rebuild_sort_memory_plan_v1_job", "start_replacement_rebuild_sort_memory_v1_job"] as const;
    for (const method of methods) for (const invalid of [undefined, null, 3]) {
        const f = fixture(rows(1)); f.enableSortPlan();
        (f.tree as any)[method] = invalid;
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
                { budget: f.port, cooperate: async () => {} })));
            assert(error instanceof RootTreeRepairError && error.code === "INVALID_TREE");
            assert.equal(f.allocations.length, 0); assert.equal(f.state.begin, 0);
        } finally { f.budget.close(); }
    }
    const f = fixture(rows(1)); f.enableSortPlan();
    f.tree.start_replacement_rebuild_job = undefined; f.tree.step_replacement_rebuild_job = undefined;
    try {
        const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
            { budget: f.port, cooperate: async () => {} })));
        assert(error instanceof RootTreeRepairError && error.code === "INVALID_TREE");
        assert.equal(f.allocations.length, 0); assert.equal(f.state.begin, 0);
    } finally { f.budget.close(); }
}

async function malformedSortWorkspacePlansRetainCancelableInput(): Promise<void> {
    const valid = { schema: 1, scope: "v2-replacement-sort-indices", entryCount: 1,
        indexSizeBytes: 4, sourceIndexRequestedBytes: 4, targetIndexRequestedBytes: 4, peakAdmissionBytes: 8 };
    let accessorCalls = 0;
    const invalid: unknown[] = [null, [], {}, { ...valid, schema: 2 }, { ...valid, scope: "replacement-sort" },
        { ...valid, entryCount: 2 }, { ...valid, indexSizeBytes: 8 },
        { ...valid, sourceIndexRequestedBytes: 3 }, { ...valid, targetIndexRequestedBytes: 3 },
        { ...valid, peakAdmissionBytes: 7 }, { ...valid, extra: 1 }, { ...valid, [Symbol("extra")]: 1 },
        Object.defineProperty({ ...valid }, "hidden", { value: 1 })];
    for (const field of ["entryCount", "indexSizeBytes", "sourceIndexRequestedBytes",
        "targetIndexRequestedBytes", "peakAdmissionBytes"] as const) {
        for (const value of [-1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, "4", 4n]) {
            invalid.push({ ...valid, [field]: value });
        }
    }
    for (const field of Object.keys(valid)) invalid.push(Object.defineProperty({ ...valid }, field,
        { get() { accessorCalls++; return 4; } }));
    for (const value of invalid) {
        const f = fixture(rows(1)); f.enableSortPlan(); f.enableRetirement(1);
        f.tree.replacement_rebuild_sort_memory_plan_v1_job = () => value;
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
                { budget: f.port, cooperate: async () => {} })));
            assert(error instanceof RootTreeRepairError && error.code === "INVALID_TREE");
            assert.equal(f.state.sortStarts, 0); assert.equal(f.state.starts, 0);
            assert.equal(f.state.deferredCancels, 1); assert.equal(f.state.root, OLD_ROOT);
            f.assertReleased();
        } finally { f.budget.close(); }
    }
    assert.equal(accessorCalls, 0, "plan validation invoked an accessor");
}

async function sortWorkspaceMethodsAndFinalWitnessArePinned(): Promise<void> {
    const pinned = fixture(rows(257)); pinned.enableSortPlan();
    try {
        await rebuildRootTreeFromBase(pinned.base, pinned.tree, 2, undefined, {
            budget: pinned.port, cooperate: async () => {
                pinned.tree.replacement_rebuild_sort_memory_plan_v1_job = () => { throw new Error("plan reread"); };
                pinned.tree.start_replacement_rebuild_sort_memory_v1_job = () => { throw new Error("start reread"); };
            },
        });
        assert.equal(pinned.state.sortStarts, 1); assert.equal(pinned.state.starts, 0); pinned.assertReleased();
    } finally { pinned.budget.close(); }

    for (const mode of ["base", "scope", "abort", "committed", "candidate", "descriptor", "start"] as const) {
        const f = fixture(rows(1)); f.enableSortPlan(); f.enableRetirement(1);
        const original = f.tree.replacement_rebuild_sort_memory_plan_v1_job!;
        const abort = new AbortController(), scopeError = new Error("sort plan scope changed");
        let scopeValid = true;
        f.tree.replacement_rebuild_sort_memory_plan_v1_job = token => {
            const plan = original(token) as object;
            if (mode === "base") f.base.mutate();
            if (mode === "scope") scopeValid = false;
            if (mode === "abort") abort.abort();
            if (mode === "committed") f.state.committedRevision++;
            if (mode === "candidate") f.state.candidateRevision++;
            if (mode === "descriptor") return new Proxy(plan, {
                getOwnPropertyDescriptor(target, property) {
                    f.base.mutate(); return Reflect.getOwnPropertyDescriptor(target, property);
                },
            });
            return plan;
        };
        if (mode === "start") f.hooks.native = phase => { if (phase === "sort-start") f.base.mutate(); };
        try {
            const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, abort.signal,
                { budget: f.port, cooperate: async () => {}, assertCurrent: () => { if (!scopeValid) throw scopeError; } })));
            if (mode === "scope") assert.equal(error, scopeError);
            else if (mode === "abort") assert.equal((error as Error).name, "AbortError");
            else assert(error instanceof RootTreeRepairError);
            assert.equal(f.state.sortStarts, mode === "start" ? 1 : 0);
            assert.equal(f.state.steps, 0); assert.equal(f.state.deferredCancels, 1);
            assert.equal(f.state.root, OLD_ROOT); f.assertReleased();
        } finally { f.budget.close(); }
    }
}

async function sortWorkspaceStartFailureAndRetirementRetry(): Promise<void> {
    const f = fixture(rows(1)); f.enableSortPlan(); f.enableRetirement(1);
    const startError = new Error("second sorter index reserve refused");
    const cleanupError = new Error("retirement temporarily failed");
    let failCleanup = true;
    f.hooks.native = phase => {
        if (phase === "sort-start") throw startError;
        if (phase === "retire" && failCleanup) throw cleanupError;
    };
    try {
        const error = failure(await outcome(rebuildRootTreeFromBase(f.base, f.tree, 2, undefined,
            { budget: f.port, cooperate: signal => yieldWork({ signal }) })));
        assert(error instanceof RootTreeRetirementError);
        assert.equal(error.originalCause, startError); assert.equal(error.cleanupCause, cleanupError);
        assert(hasPendingRootTreeRetirement(f.tree)); assert.equal(f.state.buildStarted, false);
        assert.equal(f.state.root, OLD_ROOT); assert.equal(f.state.committedRevision, 4);
        assert.equal(f.budget.snapshot().usedBytes, f.workBytes);
        assert.deepEqual(f.releases, [ROOT_TREE_REPAIR_LIMITS.scratchBytes]);
        assert.equal(f.state.deferredCancels, 1);
        failCleanup = false;
        await drainRootTreeRetirement(f.tree);
        assert.equal(f.state.deferredCancels, 1, "cleanup retry repeated the input-to-retirement transition");
        assert(!hasPendingRootTreeRetirement(f.tree)); f.assertReleased();
    } finally { f.budget.close(); }
}

async function run() {
    inputEstimatorBoundaries();
    await countAndBytePagedSuccess();
    await actualHostTaskBetweenPages();
    await invalidatedAfterFeedCancels();
    await externalCheckCannotInvalidateThenPublish();
    await finalFeedAwaitAndEmptyFinishAreFenced();
    await queuedAdmissionHasNoSecondPassOrNativeWork();
    await nativeErrorsAndProgressReleaseOwners();
    await incompleteApiAndInvalidWitnessFailClosed();
    await resumableBuildStepsAndActualYields();
    await buildCheckpointInvalidationCancels();
    await finalBuildDoneAwaitStillOwnsCancellation();
    await malformedBuildProgressCannotFinish();
    await buildErrorsRetainCancelableOwnership();
    await optionalBuildApiIsAtomicAndCaptured();
    await deferredFinishRetainsLeaseAndYields();
    await publicationResultRechecksAfterRetirement();
    await deferredCancellationIgnoresAbortAndGuards();
    await deferredErrorsRetainThenDrainExactOwner();
    await deferredRetirementBlocksNewAdmissionAndPinsMethods();
    await concurrentPreBeginRepairsCannotReplaceOwnership();
    await retirementFailuresAreNotFalseCompletion();
    await partialRetirementApiFailsBeforeAllocation();
    await v2NodePlanPausesBeforeResume();
    await malformedOrPartialNodePlanFailsClosed();
    await v2RequestedOutputAdmissionLifecycle();
    await outputAdmissionRequiresCompleteAbiBeforeAllocation();
    await v2SortWorkspaceAdmissionAndCompatibility();
    await sortWorkspaceRefusalAndPreBeginAbort();
    await sortWorkspacePartialAbiFailsBeforeAdmission();
    await malformedSortWorkspacePlansRetainCancelableInput();
    await sortWorkspaceMethodsAndFinalWitnessArePinned();
    await sortWorkspaceStartFailureAndRetirementRetry();
    console.log("root-tree-repair-job.test: 33 suites passed (paged/build/plan/sort-output admission/retirement ownership, pre-encode V2 boundary, post-retirement publication fences, exact progress, actual yields, quarantined cleanup, compatibility)");
}
let completed = false;
process.once("beforeExit", () => { if (!completed) throw new Error("root rebuild job tests left an unsettled owner"); });
void run().then(() => { completed = true; }, error => { completed = true; setTimeout(() => { throw error; }, 0); });
