import { strict as assert } from "node:assert";
import type { TreeJobProgress } from "./tree-candidate-job";
import { applyTreeCandidateMutation, type ApplyTreeCandidateMutationOptions, type CandidateMutationJobTree,
    type TreeCandidateMutationKind, type CandidateMutationOutputOwner, type CandidateMutationOutputMemoryReady,
    hasPendingTreeCandidateMutationRetirement, drainTreeCandidateMutationRetirement,
    abortTreeCandidateAfterMutationRetirement,
    parseCandidateMutationOutputMemoryPlan, parseCandidateMutationOutputMemoryReady,
    TREE_CANDIDATE_MUTATION_CPU_SLICE_MS, TREE_CANDIDATE_MUTATION_MAX_RELIABLE_STEPS_PER_TURN,
    TREE_CANDIDATE_MUTATION_STEP_UNITS,
    TreeCandidateMutationRetirementError } from "./tree-candidate-mutation-job";
import { RootTreeResidentAdmission } from "./root-tree-resident-admission";
import { admitCandidateMutationOutput } from "./candidate-mutation-output-admission";

function gate() {
    let release!: () => void;
    const promise = new Promise<void>(resolve => { release = resolve; });
    return { promise, release };
}

const payload = "opaque serialized bytes: helper must not parse JSON or inspect paths";
const pending = (completed: number, reachable = 0): TreeJobProgress =>
    ({ done: false, units: 1, completed, remaining: 1, reachable });
const done = (completed: number, reachable = 0): TreeJobProgress =>
    ({ done: true, units: 1, completed, remaining: 0, reachable });
function chunkedProgress(total: number): TreeJobProgress[] {
    const rows: TreeJobProgress[] = [];
    for (let completed = 0; completed < total;) {
        const units = Math.min(TREE_CANDIDATE_MUTATION_STEP_UNITS, total - completed);
        completed += units;
        rows.push({ done: completed === total, units, completed,
            remaining: completed === total ? 0 : 1, reachable: 0 });
    }
    return rows;
}

class MutationFixture implements CandidateMutationJobTree {
    candidate = true;
    candidateRevision = 1;
    readonly committedRevision = 1;
    job = false;
    token = 37;
    steps = 0;
    finishes = 0;
    cancels = 0;
    legacyCalls = 0;
    events: string[] = [];
    begun: Array<{ kind: TreeCandidateMutationKind; payload: string }> = [];
    progress: unknown[] = [pending(1), pending(2, 1), done(3, 2)];
    afterStep?: () => void;
    afterFinish?: () => void;

    has_candidate(): boolean { return this.candidate; }
    begin_candidate_update_job(json: string): number { return this.begin("update", json); }
    begin_candidate_delete_job(json: string): number { return this.begin("delete", json); }
    private begin(kind: TreeCandidateMutationKind, json: string): number {
        assert.equal(this.job, false); assert.equal(this.candidate, true);
        this.job = true; this.events.push(`begin:${kind}`); this.begun.push({ kind, payload: json });
        return this.token;
    }
    step_tree_job(token: number, maxUnits: number): unknown {
        assert.equal(token, this.token); assert.equal(maxUnits, TREE_CANDIDATE_MUTATION_STEP_UNITS);
        assert.equal(this.job, true); assert.equal(this.candidate, true);
        this.steps++; this.events.push("step"); assert(this.progress.length > 0);
        const next = this.progress.shift();
        if (next instanceof Error) { this.job = false; throw next; }
        this.afterStep?.(); return next;
    }
    finish_candidate_mutation_job(token: number): void {
        assert.equal(token, this.token); assert.equal(this.job, true);
        this.finishes++; this.events.push("finish"); this.job = false; this.candidateRevision++;
        this.afterFinish?.();
    }
    cancel_tree_job(token: number): void {
        assert.equal(token, this.token); assert.equal(this.job, true);
        this.cancels++; this.events.push("cancel"); this.job = false;
    }
    options(extra: Partial<ApplyTreeCandidateMutationOptions> = {}): ApplyTreeCandidateMutationOptions {
        return {
            cooperate: async () => { this.events.push("yield"); },
            legacy: () => { this.legacyCalls++; throw new Error("unexpected legacy fallback"); },
            // Preserve the original one-step-per-turn fixture unless a test
            // explicitly exercises time-budgeted coalescing.
            now: () => (this.steps + ((this as MutationFixture & { retiredSteps?: number }).retiredSteps ?? 0)) *
                TREE_CANDIDATE_MUTATION_CPU_SLICE_MS,
            ...extra,
        };
    }
}

async function successUsesExactKindPayloadAndBoundedSteps(): Promise<void> {
    for (const kind of ["update", "delete"] as const) {
        const tree = new MutationFixture();
        await applyTreeCandidateMutation(tree, kind, payload, tree.options());
        assert.deepEqual(tree.begun, [{ kind, payload }]);
        assert.deepEqual(tree.events, ["yield", `begin:${kind}`, "step", "yield", "step", "yield", "step", "yield", "finish"]);
        assert.equal(tree.steps, 3); assert.equal(tree.finishes, 1); assert.equal(tree.cancels, 0);
        assert.equal(tree.legacyCalls, 0); assert.equal(tree.job, false); assert.equal(tree.candidate, true);
        assert.equal(tree.candidateRevision, 2); assert.equal(tree.committedRevision, 1);
    }
    const empty = new MutationFixture();
    empty.progress = [{ done: true, units: 0, completed: 0, remaining: 0, reachable: 0 }];
    await applyTreeCandidateMutation(empty, "update", "[]", empty.options());
    assert.deepEqual(empty.events, ["yield", "begin:update", "step", "yield", "finish"]);
}

async function heldCancellationRetainsActualWaitAndOuterCandidate(): Promise<void> {
    for (const kind of ["update", "delete"] as const) {
        const tree = new MutationFixture(), controller = new AbortController(), entered = gate(), held = gate();
        const failure = new Error("mutation stopped while host is held"); let yields = 0, settled = false;
        const work = applyTreeCandidateMutation(tree, kind, payload, tree.options({ signal: controller.signal,
            cooperate: async () => { if (++yields === 2) { entered.release(); await held.promise; } },
        }));
        const outcome = work.then(() => undefined, error => error).finally(() => { settled = true; });
        try {
            await entered.promise; controller.abort(failure);
            await Promise.resolve(); await Promise.resolve();
            assert.equal(settled, false); assert.equal(tree.job, true); assert.equal(tree.cancels, 0);
            assert.equal(tree.steps, 1); assert.equal(tree.candidateRevision, 1);
        } finally { held.release(); }
        assert.equal(await outcome, failure); assert.equal(tree.job, false); assert.equal(tree.cancels, 1);
        assert.equal(tree.finishes, 0); assert.equal(tree.candidate, true); assert.equal(tree.candidateRevision, 1);
        assert.equal(tree.committedRevision, 1); assert.equal(tree.legacyCalls, 0);
    }
}

async function invalidAndInconsistentProgressCannotFinish(): Promise<void> {
    const cases: unknown[][] = [
        [null], [undefined], ["progress"],
        [{ ...pending(1), units: 2 }], [{ ...pending(1), units: 0 }],
        [{ done: false, units: TREE_CANDIDATE_MUTATION_STEP_UNITS + 1,
            completed: TREE_CANDIDATE_MUTATION_STEP_UNITS + 1, remaining: 1, reachable: 0 }],
        [{ ...pending(1), completed: Number.NaN }], [{ ...pending(1), remaining: -1 }],
        [{ ...pending(1), reachable: .5 }], [{ ...done(1), remaining: 1 }],
        [{ ...pending(1), done: "no" }], [pending(0)], [pending(2)], [pending(1, 2)],
        [pending(1, 1), pending(2, 0)], [pending(1), done(1)],
    ];
    for (const values of cases) {
        const tree = new MutationFixture(); tree.progress = values;
        await assert.rejects(applyTreeCandidateMutation(tree, "update", payload, tree.options()), /progress/);
        assert.equal(tree.cancels, 1); assert.equal(tree.finishes, 0); assert.equal(tree.job, false);
        assert.equal(tree.candidate, true); assert.equal(tree.candidateRevision, 1); assert.equal(tree.legacyCalls, 0);
    }
}

async function scopeGuardsFenceBeginStepAndFinish(): Promise<void> {
    for (const phase of ["before", "first-yield", "step", "done", "finish"] as const) {
        const tree = new MutationFixture(), failure = new Error(`stale mutation ${phase}`);
        let current = phase !== "before";
        tree.afterStep = () => { if (phase === "step" || phase === "done" && tree.progress.length === 0) current = false; };
        tree.afterFinish = () => { if (phase === "finish") current = false; };
        await assert.rejects(applyTreeCandidateMutation(tree, "delete", payload, tree.options({
            assertCurrent: () => { if (!current) throw failure; },
            cooperate: async () => { if (phase === "first-yield") current = false; },
        })), error => error === failure);
        assert.equal(tree.begun.length, Number(phase !== "before" && phase !== "first-yield"));
        assert.equal(tree.finishes, Number(phase === "finish"));
        assert.equal(tree.cancels, Number(phase === "step" || phase === "done"));
        assert.equal(tree.candidate, true); assert.equal(tree.job, false); assert.equal(tree.committedRevision, 1);
    }
    const tree = new MutationFixture(), controller = new AbortController(), stopped = new Error("ownership hook stopped operation");
    await assert.rejects(applyTreeCandidateMutation(tree, "update", payload, tree.options({
        signal: controller.signal, assertCurrent: () => controller.abort(stopped),
    })), error => error === stopped);
    assert.equal(tree.begun.length, 0); assert.deepEqual(tree.events, []);
}

async function nativeFailuresKeepTheirDifferentCleanupContracts(): Promise<void> {
    const step = new MutationFixture(), stepError = new Error("native step dropped job"); step.progress = [stepError];
    await assert.rejects(applyTreeCandidateMutation(step, "update", payload, step.options()), error => error === stepError);
    assert.equal(step.job, false); assert.equal(step.cancels, 0); assert.equal(step.finishes, 0);
    assert.equal(step.candidate, true); assert.equal(step.legacyCalls, 0);

    const finish = new MutationFixture(), finishError = new Error("native finish retained job");
    finish.finish_candidate_mutation_job = () => { assert.equal(finish.job, true); throw finishError; };
    await assert.rejects(applyTreeCandidateMutation(finish, "delete", payload, finish.options()), error => error === finishError);
    assert.equal(finish.job, false); assert.equal(finish.cancels, 1); assert.equal(finish.candidate, true);
    assert.equal(finish.candidateRevision, 1); assert.equal(finish.legacyCalls, 0);

    const yielded = new MutationFixture(), yieldError = new Error("host yield failed"); let yields = 0;
    await assert.rejects(applyTreeCandidateMutation(yielded, "update", payload, yielded.options({
        cooperate: async () => { if (++yields === 2) throw yieldError; },
    })), error => error === yieldError);
    assert.equal(yielded.steps, 1); assert.equal(yielded.cancels, 1); assert.equal(yielded.candidate, true);

    const missing = new MutationFixture(); missing.afterFinish = () => { missing.candidate = false; };
    await assert.rejects(applyTreeCandidateMutation(missing, "update", payload, missing.options()), /candidate disappeared/);
    assert.equal(missing.job, false); assert.equal(missing.cancels, 0); assert.equal(missing.finishes, 1);
}

async function partialCapabilitiesNeverFallBack(): Promise<void> {
    const names = ["begin_candidate_update_job", "begin_candidate_delete_job", "finish_candidate_mutation_job",
        "step_tree_job", "cancel_tree_job"] as const;
    for (const name of names) for (const value of [undefined, null, 42]) {
        const tree = new MutationFixture(); Object.defineProperty(tree, name, { value });
        await assert.rejects(applyTreeCandidateMutation(tree, "update", payload, tree.options()), /incomplete candidate mutation/);
        assert.equal(tree.begun.length, 0); assert.equal(tree.legacyCalls, 0); assert.equal(tree.candidate, true);
    }
    const noCandidate = new MutationFixture(); noCandidate.candidate = false;
    await assert.rejects(applyTreeCandidateMutation(noCandidate, "delete", payload, noCandidate.options()), /candidate disappeared/);
    assert.deepEqual(noCandidate.events, []);

    const tree = new MutationFixture(); tree.token = 0;
    await assert.rejects(applyTreeCandidateMutation(tree, "update", payload, tree.options()), /invalid token/);
    assert.equal(tree.cancels, 1); assert.equal(tree.job, false); assert.equal(tree.steps, 0); assert.equal(tree.candidate, true);
}

async function legacyExecutesOnceWithPreAndPostGuards(): Promise<void> {
    for (const phase of ["success", "before", "yield", "native", "scope", "candidate"] as const) {
        const controller = new AbortController(), failure = new Error(`legacy mutation ${phase}`);
        let calls = 0, yields = 0, current = true, candidate = true;
        if (phase === "before") controller.abort(failure);
        // Older trees already expose these shared methods; neither claims
        // mutation support without any mutation-specific entry point.
        const tree: CandidateMutationJobTree = { has_candidate: () => candidate,
            step_tree_job: () => { throw new Error("unexpected legacy step"); },
            cancel_tree_job: () => { throw new Error("unexpected legacy cancel"); },
        };
        const work = applyTreeCandidateMutation(tree, "delete", payload, { signal: controller.signal,
            assertCurrent: () => { if (!current) throw failure; },
            cooperate: async () => { yields++; if (phase === "yield") controller.abort(failure); },
            legacy: () => {
                calls++; assert.equal(yields, 1);
                if (phase === "native") controller.abort(failure);
                if (phase === "scope") current = false;
                if (phase === "candidate") candidate = false;
            },
        });
        if (phase === "success") await work;
        else await assert.rejects(work, phase === "candidate" ? /candidate disappeared/ : error => error === failure);
        assert.equal(calls, Number(phase !== "before" && phase !== "yield"));
        assert.equal(yields, Number(phase !== "before"));
    }
}

async function cleanupFailureNeverMasksPrimary(): Promise<void> {
    const originalConsoleError = console.error;
    try {
        for (const frozen of [false, true]) for (const dropped of [false, true]) {
            const tree = new MutationFixture(), primary = new Error("primary mutation finish failure");
            if (frozen) Object.freeze(primary);
            const cleanup = new Error("cancel cleanup failure"), diagnostics: unknown[][] = [];
            tree.finish_candidate_mutation_job = () => { throw primary; };
            const cancel = tree.cancel_tree_job.bind(tree);
            tree.cancel_tree_job = token => { if (dropped) cancel(token); throw cleanup; };
            console.error = (...values: unknown[]) => { diagnostics.push(values); if (frozen) throw new Error("logger failed"); };
            await assert.rejects(applyTreeCandidateMutation(tree, "update", payload, tree.options()), error => error === primary);
            if (!frozen) assert.deepEqual((primary as Error & { treeCandidateCleanupErrors?: unknown[] }).treeCandidateCleanupErrors, [cleanup]);
            assert.equal(diagnostics.length, 1); assert.equal(tree.cancels, Number(dropped));
            assert.equal(tree.job, !dropped, "failed cancellation falsely claimed native ownership was released");
            assert.equal(tree.candidate, true); assert.equal(tree.committedRevision, 1);
        }
    } finally { console.error = originalConsoleError; }
}

async function optionsAreCapturedBeforeHostWait(): Promise<void> {
    const tree = new MutationFixture(), original = new AbortController(); let yields = 0;
    const replacement = new AbortController(); replacement.abort(new Error("replacement signal must not be read"));
    const options = tree.options({ signal: original.signal, cooperate: async () => {
        yields++;
        options.cooperate = async () => { throw new Error("replacement scheduler must not run"); };
        options.assertCurrent = () => { throw new Error("replacement guard must not run"); };
        options.now = () => { throw new Error("replacement clock must not run"); };
        options.signal = replacement.signal;
    } });
    await applyTreeCandidateMutation(tree, "update", payload, options);
    assert.equal(yields, 4); assert.equal(tree.finishes, 1); assert.equal(tree.cancels, 0);
}

async function mutationOwnershipHandoffPrecedesFalliblePostGuards(): Promise<void> {
    const native = new MutationFixture(), failure = new Error("post-finish ownership changed");
    let current = true, witnessed = 0;
    native.afterFinish = () => { current = false; };
    await assert.rejects(applyTreeCandidateMutation(native, "update", payload, native.options({
        assertCurrent: () => { if (!current) throw failure; },
        onCandidateMutated: () => { witnessed = native.candidateRevision; },
    })), error => error === failure);
    assert.equal(native.finishes, 1); assert.equal(native.cancels, 0);
    assert.equal(witnessed, 2, "native mutation was not handed off before its rejecting final guard");

    let candidate = true, legacyRevision = 4, legacyWitness = 0, legacyCurrent = true;
    const legacy: CandidateMutationJobTree = { has_candidate: () => candidate };
    await assert.rejects(applyTreeCandidateMutation(legacy, "delete", payload, {
        cooperate: async () => {},
        assertCurrent: () => { if (!legacyCurrent) throw failure; },
        legacy: () => { legacyRevision++; legacyCurrent = false; },
        onCandidateMutated: () => { legacyWitness = legacyRevision; },
    }), error => error === failure);
    assert.equal(legacyWitness, 5, "legacy mutation handoff ran after the rejecting final guard");
    candidate = false;

    const captured = new MutationFixture(); let originalCalls = 0;
    const options = captured.options({
        cooperate: async () => {
            options.onCandidateMutated = () => { throw new Error("replacement mutation handoff was used"); };
        },
        onCandidateMutated: () => { originalCalls++; },
    });
    await applyTreeCandidateMutation(captured, "update", payload, options);
    assert.equal(originalCalls, 1, "mutation handoff was not captured before the first host wait");
}

const outputPlan = { schema: 1, scope: "v2-candidate-mutation-output", nodePayloadBytes: 600,
    rangeEndpointPeakRequestedBytes: 400, rangeEndpointResidentRequestedBytes: 100,
    peakAdmissionBytes: 1_000, residentAdmissionBytes: 700 } as const;
const outputReady = { schema: 1, scope: "v2-candidate-mutation-output", stagedNodePayloadBytes: 400,
    rangeEndpointResidentRequestedBytes: 100, residentAdmissionBytes: 500 } as const;
const newProgress = () => [
    { ...pending(1), phase: "plan ready" },
    { ...pending(2, 1), phase: "emit" },
    { ...done(3, 2), phase: "ready" },
];

class OutputMutationFixture extends MutationFixture {
    version = 2;
    retiring = false;
    plan: unknown = outputPlan;
    ready: unknown = outputReady;
    outputProgress: unknown[] = newProgress();
    resumes = 0; planReads = 0; readyReads = 0; retiredSteps = 0;
    retirementCompleted = 0; retirementTotal = 2;
    afterRetirement?: () => void;
    aborts = 0;
    tree_version(): number { return this.version; }
    candidate_revision(): number { return this.candidateRevision; }
    abort_candidate(): void {
        assert.equal(this.job, false, "candidate abort crossed native mutation ownership");
        assert.equal(this.candidate, true); this.candidate = false; this.candidateRevision++; this.aborts++;
    }
    step_candidate_mutation_output_memory_v1_job(token: number, units: number): unknown {
        assert.equal(token, this.token); assert.equal(units, TREE_CANDIDATE_MUTATION_STEP_UNITS);
        assert.equal(this.job, true); assert.equal(this.retiring, false);
        this.events.push("output-step"); this.steps++;
        assert(this.outputProgress.length > 0);
        const next = this.outputProgress.shift();
        if (next instanceof Error) { this.retiring = true; throw next; }
        this.afterStep?.(); return next;
    }
    candidate_mutation_output_memory_plan_v1_job(token: number): unknown {
        assert.equal(token, this.token); assert.equal(this.job, true); this.planReads++;
        this.events.push("plan"); return this.plan;
    }
    resume_candidate_mutation_output_memory_v1_job(token: number, node: number, peak: number, resident: number): void {
        assert.equal(token, this.token); assert.deepEqual([node, peak, resident], [600, 400, 100]);
        this.events.push("resume"); this.resumes++;
    }
    candidate_mutation_output_memory_ready_v1_job(token: number): unknown {
        assert.equal(token, this.token); this.readyReads++; this.events.push("ready"); return this.ready;
    }
    finish_candidate_mutation_job_deferred(token: number): void {
        assert.equal(token, this.token); assert.equal(this.job, true); assert.equal(this.retiring, false);
        this.finishes++; this.candidateRevision++; this.retiring = true; this.events.push("output-finish");
        this.afterFinish?.();
    }
    cancel_candidate_mutation_job_deferred(token: number): void {
        assert.equal(token, this.token); assert.equal(this.job, true);
        this.cancels++; this.retiring = true; this.events.push("output-cancel");
    }
    step_tree_retirement(token: number, units: number): unknown {
        assert.equal(token, this.token); assert.equal(units, 256);
        assert.equal(this.retiring, true); assert.equal(this.job, true);
        this.retiredSteps++; this.retirementCompleted++; this.events.push("retire-step");
        const done = this.retirementCompleted === this.retirementTotal;
        if (done) { this.job = false; this.retiring = false; }
        this.afterRetirement?.();
        return { done, units: 1, completed: this.retirementCompleted };
    }
    override options(extra: Partial<ApplyTreeCandidateMutationOptions> = {}): ApplyTreeCandidateMutationOptions {
        return super.options({ cooperateRetirement: async () => { this.events.push("retire-yield"); }, ...extra });
    }
}

async function mutationQuantumAmortizesHostCrossingsAndKeepsChunkYields(): Promise<void> {
    const total = 1_025;
    const dataSteps = Math.ceil(total / TREE_CANDIDATE_MUTATION_STEP_UNITS);
    assert.equal(TREE_CANDIDATE_MUTATION_STEP_UNITS, 64);
    assert.equal(TREE_CANDIDATE_MUTATION_CPU_SLICE_MS, 4);
    assert.equal(TREE_CANDIDATE_MUTATION_MAX_RELIABLE_STEPS_PER_TURN, 32);

    const compatibility = new MutationFixture();
    compatibility.progress = chunkedProgress(total);
    let compatibilityYields = 0;
    await applyTreeCandidateMutation(compatibility, "update", payload, compatibility.options({
        cooperate: async () => { compatibilityYields++; },
    }));
    assert.equal(compatibility.steps, dataSteps);
    // One pre-begin turn plus one turn after every expired bounded chunk,
    // including the final Done-to-finish boundary.
    assert.equal(compatibilityYields, dataSteps + 1);
    assert.equal(compatibility.finishes, 1);

    const output = new OutputMutationFixture();
    output.outputProgress = [
        { done: false, units: 0, completed: 0, remaining: 1, reachable: 0, phase: "plan ready" },
        ...chunkedProgress(total).map(row => ({ ...row, phase: row.done ? "ready" : "build" })),
    ];
    let outputYields = 0;
    await applyTreeCandidateMutation(output, "update", payload, output.options({
        cooperate: async () => { outputYields++; },
    }));
    // Output admission adds one zero-work plan-barrier call/turn; the final
    // Done-to-Ready boundary also remains responsive.
    assert.equal(output.steps, dataSteps + 1);
    assert.equal(outputYields, dataSteps + 2);
    assert.equal(output.resumes, 1); assert.equal(output.finishes, 1);
    assert.equal(output.retiredSteps, 2);
}

async function timeBudgetCoalescesFastStepsAndBoundsCancellation(): Promise<void> {
    const tree = new MutationFixture();
    tree.progress = chunkedProgress(TREE_CANDIDATE_MUTATION_STEP_UNITS * 10);
    let clock = 100, yields = 0;
    const stepsAtYield: number[] = [];
    tree.afterStep = () => { clock += 1; };
    await applyTreeCandidateMutation(tree, "update", payload, tree.options({
        now: () => clock,
        cooperate: async () => {
            yields++;
            if (tree.job) stepsAtYield.push(tree.steps);
        },
    }));
    assert.equal(tree.steps, 10);
    assert.equal(yields, 3, "fast native steps still paid one host turn apiece");
    assert.deepEqual(stepsAtYield, [4, 8], "CPU slices did not stop at the deterministic 4ms budget");

    const frozen = new MutationFixture();
    frozen.progress = chunkedProgress(TREE_CANDIDATE_MUTATION_STEP_UNITS * 65);
    let frozenYields = 0;
    await applyTreeCandidateMutation(frozen, "update", payload, frozen.options({
        now: () => 0,
        cooperate: async () => { frozenYields++; },
    }));
    assert.equal(frozen.steps, 65); assert.equal(frozenYields, 66,
        "coarse/frozen clock did not fail safe to one primitive per host turn");

    const stopped = new MutationFixture(), controller = new AbortController();
    stopped.progress = chunkedProgress(TREE_CANDIDATE_MUTATION_STEP_UNITS * 10);
    const failure = new Error("cancelled at bounded host turn");
    clock = 0; yields = 0;
    stopped.afterStep = () => { clock += 3; };
    await assert.rejects(applyTreeCandidateMutation(stopped, "delete", payload, stopped.options({
        signal: controller.signal,
        now: () => clock,
        cooperate: async () => {
            if (++yields === 2) controller.abort(failure);
        },
    })), error => error === failure);
    assert.equal(stopped.steps, 2, "cancellation crossed the first expired CPU slice");
    assert.equal(stopped.cancels, 1); assert.equal(stopped.finishes, 0);

    const fenced = new MutationFixture(), synchronousStop = new AbortController();
    fenced.progress = chunkedProgress(TREE_CANDIDATE_MUTATION_STEP_UNITS * 3);
    fenced.afterStep = () => synchronousStop.abort(failure);
    await assert.rejects(applyTreeCandidateMutation(fenced, "update", payload, fenced.options({
        signal: synchronousStop.signal,
        now: () => 0,
    })), error => error === failure);
    assert.equal(fenced.steps, 1, "coalescing skipped the per-step cancellation fence");
    assert.equal(fenced.cancels, 1);

}

async function unreliableClockFailsSafeForOneStepAndRecovers(): Promise<void> {
    for (const fault of ["throw", "equal", "nan", "backward"] as const) {
        const tree = new MutationFixture();
        tree.progress = chunkedProgress(TREE_CANDIDATE_MUTATION_STEP_UNITS * 4);
        let sample = 0, yields = 0;
        const stepsAtYield: number[] = [];
        const values = [10, fault === "equal" ? 10 : fault === "nan" ? Number.NaN : 9, 20, 21, 22, 24];
        await applyTreeCandidateMutation(tree, "update", payload, tree.options({
            now: () => {
                sample++;
                if (fault === "throw" && sample === 2) throw new Error("clock sample failed");
                return values[sample - 1];
            },
            cooperate: async () => {
                yields++;
                if (tree.job) stepsAtYield.push(tree.steps);
            },
        }));
        assert.equal(tree.steps, 4, `${fault} clock changed native progress`);
        assert.equal(yields, 3, `${fault} clock did not recover batching on the next valid turn`);
        assert.deepEqual(stepsAtYield, [1, 4],
            `${fault} clock crossed one primitive before fail-safe yield or failed to recover`);
        assert.equal(tree.finishes, 1); assert.equal(tree.cancels, 0);
    }
}

async function outputAdmissionBarrierPrecedesCoalescedBuild(): Promise<void> {
    const tree = new OutputMutationFixture(), owner = new OutputOwnerFixture(tree.events);
    let clock = 0, admitted = false, mutationYields = 0, retirementYields = 0;
    tree.outputProgress = [
        { done: false, units: 0, completed: 0, remaining: 1, reachable: 0, phase: "plan ready" },
        { ...pending(1, 1), phase: "build" },
        { ...done(2, 2), phase: "ready" },
    ];
    tree.afterStep = () => {
        clock += 1;
        if (tree.steps > 1) assert.equal(admitted, true,
            "native output build crossed the synchronous admission barrier");
    };
    tree.afterRetirement = () => { clock += TREE_CANDIDATE_MUTATION_CPU_SLICE_MS; };
    await applyTreeCandidateMutation(tree, "update", payload, tree.options({
        now: () => clock,
        cooperate: async () => { mutationYields++; },
        cooperateRetirement: async () => { retirementYields++; },
        onOutputMemoryPlan: () => { admitted = true; return owner; },
    }));
    assert.equal(mutationYields, 1, "sub-budget output steps were not coalesced");
    assert.equal(tree.steps, 3); assert.equal(tree.resumes, 1); assert.equal(tree.finishes, 1);
    assert.equal(retirementYields, 2, "mutation batching weakened cooperative retirement");
    assert.equal(owner.state, "published"); assert.equal(owner.released, true);

    const stopped = new OutputMutationFixture(), stoppedOwner = new OutputOwnerFixture(stopped.events);
    const controller = new AbortController(), failure = new Error("stopped at Done-to-Ready host turn");
    let stoppedClock = 0, stoppedYields = 0;
    stopped.afterStep = () => { stoppedClock += stopped.steps === 3 ? 4 : 1; };
    await assert.rejects(applyTreeCandidateMutation(stopped, "update", payload, stopped.options({
        signal: controller.signal,
        now: () => stoppedClock,
        cooperate: async () => {
            if (++stoppedYields === 2) controller.abort(failure);
        },
        onOutputMemoryPlan: () => stoppedOwner,
    })), error => error === failure);
    assert.equal(stopped.readyReads, 0, "expired Done step reached Ready before its host/cancel fence");
    assert.equal(stopped.finishes, 0); assert.equal(stopped.cancels, 1);
    assert.equal(stoppedOwner.state, "detached"); assert.equal(stoppedOwner.live, 0);
}

async function retirementSharesBudgetButKeepsFinalHostTurn(): Promise<void> {
    const tree = new OutputMutationFixture(), owner = new OutputOwnerFixture(tree.events);
    let clock = 0, retirementYields = 0;
    tree.retirementTotal = 10;
    tree.afterRetirement = () => { clock += 2; };
    await applyTreeCandidateMutation(tree, "update", payload, tree.options({
        now: () => clock,
        cooperate: async () => {},
        cooperateRetirement: async () => { retirementYields++; },
        onOutputMemoryPlan: () => owner,
    }));
    assert.equal(tree.retiredSteps, 10);
    assert.equal(retirementYields, 5,
        "retirement neither shared its CPU budget nor retained the final host turn");
    assert.equal(hasPendingTreeCandidateMutationRetirement(tree), false);
    assert.equal(owner.released, true);
}

/** Synthetic scoped-output ledger, not a native allocation measurement. */
class OutputOwnerFixture implements CandidateMutationOutputOwner {
    live = outputPlan.peakAdmissionBytes as number;
    released = false;
    state = "reserved";
    constructor(readonly events: string[],
        readonly expectedReady: CandidateMutationOutputMemoryReady = outputReady) {}
    releaseBeforeOutput(): void { this.events.push("owner-release"); this.live = 0; this.released = true; this.state = "released"; }
    ready(plan: CandidateMutationOutputMemoryReady): void {
        assert.equal(this.state, "reserved"); assert.deepEqual(plan, this.expectedReady);
        this.events.push("owner-ready"); this.state = "ready"; this.live = plan.residentAdmissionBytes;
    }
    published(): void { this.events.push("owner-published"); this.state = "published"; }
    detached(): void { this.events.push("owner-detached"); this.state = "detached"; }
    retired(): void {
        this.events.push("owner-retired"); this.released = true;
        if (this.state === "detached") this.live = 0;
        else assert.equal(this.state, "published", "private retirement confused with committed/candidate ownership");
    }
}

async function outputSuccessTransfersBeforeGuardsAndJoinsRetirement(): Promise<void> {
    for (const kind of ["update", "delete"] as const) {
        const tree = new OutputMutationFixture(), owner = new OutputOwnerFixture(tree.events);
        const held = gate(), entered = gate(); let retiredYields = 0, settled = false;
        const work = applyTreeCandidateMutation(tree, kind, payload, tree.options({
            onOutputMemoryPlan: plan => { assert.deepEqual(plan, outputPlan); tree.events.push("policy"); return owner; },
            onCandidateMutated: () => { tree.events.push("mutated"); assert.equal(owner.state, "published"); },
            cooperateRetirement: async () => {
                tree.events.push("retire-yield");
                if (++retiredYields === 1) { entered.release(); await held.promise; }
            },
        })).finally(() => { settled = true; });
        await entered.promise;
        assert.equal(settled, false); assert.equal(tree.job, true); assert.equal(owner.released, false);
        assert.equal(owner.live, outputReady.residentAdmissionBytes);
        assert.equal(tree.candidateRevision, 2); assert.equal(tree.committedRevision, 1);
        assert.equal(hasPendingTreeCandidateMutationRetirement(tree), true);
        const joined = drainTreeCandidateMutationRetirement(tree); let joinedDone = false;
        void joined.then(() => { joinedDone = true; });
        await Promise.resolve(); assert.equal(joinedDone, false);
        held.release(); await work; await joined;
        assert.deepEqual(tree.events, ["yield", `begin:${kind}`, "output-step", "plan", "policy", "resume",
            "yield", "output-step", "yield", "output-step", "yield", "ready", "owner-ready", "output-finish",
            "owner-published", "mutated", "retire-step", "retire-yield", "retire-step", "retire-yield", "owner-retired"]);
        assert.equal(tree.resumes, 1); assert.equal(tree.finishes, 1); assert.equal(tree.cancels, 0);
        assert.equal(owner.live, 500, "finishing cleanup released candidate-resident output");
        assert.equal(hasPendingTreeCandidateMutationRetirement(tree), false);
        await drainTreeCandidateMutationRetirement(tree); assert.equal(tree.retiredSteps, 2);
    }
}

async function outputPolicyRefusalAndAbortBeforeResumeAllocateNothing(): Promise<void> {
    for (const phase of ["refuse", "stop"] as const) {
        const tree = new OutputMutationFixture(), failure = new Error(`output policy ${phase}`);
        const controller = new AbortController(), owner = new OutputOwnerFixture(tree.events);
        await assert.rejects(applyTreeCandidateMutation(tree, "update", payload, tree.options({ signal: controller.signal,
            onOutputMemoryPlan: () => {
                if (phase === "refuse") throw failure;
                controller.abort(failure); return owner;
            },
        })), error => error === failure);
        assert.equal(tree.resumes, 0); assert.equal(tree.finishes, 0); assert.equal(tree.cancels, 1);
        assert.equal(tree.retiredSteps, 2); assert.equal(tree.candidateRevision, 1);
        assert.equal(hasPendingTreeCandidateMutationRetirement(tree), false);
        if (phase === "stop") {
            assert.equal(owner.live, 0); assert.equal(owner.state, "released");
            assert(!tree.events.includes("owner-retired"));
        }
    }
}

async function outputOwnerCaptureFailureReleasesActualAdmission(): Promise<void> {
    const tree = new OutputMutationFixture();
    const admission = new RootTreeResidentAdmission({ capacityBytes: 2_000 });
    const captureFailure = new Error("output owner capture getter failed");
    await assert.rejects(applyTreeCandidateMutation(tree, "update", payload, tree.options({
        onOutputMemoryPlan: plan => {
            const actual = admitCandidateMutationOutput(admission, tree, plan);
            return new Proxy(actual, {
                get(target, property, receiver) {
                    if (property === "ready") throw captureFailure;
                    return Reflect.get(target, property, receiver);
                },
            });
        },
    })), error => error === captureFailure);
    assert.equal(tree.resumes, 0); assert.equal(tree.finishes, 0);
    assert.equal(tree.cancels, 1); assert.equal(tree.retiredSteps, 2);
    assert.equal(hasPendingTreeCandidateMutationRetirement(tree), false);
    const snapshot = admission.snapshot();
    assert.deepEqual([snapshot.ledger.usedBytes, snapshot.ledger.activeLeases,
        snapshot.privateAttempts, snapshot.retiringOwners, snapshot.residentTrees], [0, 0, 0, 0, 0]);
}

async function outputPlanAndReadyAreStrictDetachedOwnData(): Promise<void> {
    let accessorCalls = 0;
    const accessor = { ...outputPlan };
    Object.defineProperty(accessor, "nodePayloadBytes", { get() { accessorCalls++; return 600; }, enumerable: true });
    const planCases: unknown[] = [null, [], Object.create(outputPlan), { ...outputPlan, extra: 1 },
        { ...outputPlan, schema: 2 }, { ...outputPlan, scope: "path/secret.md" }, accessor,
        { ...outputPlan, nodePayloadBytes: NaN }, { ...outputPlan, rangeEndpointPeakRequestedBytes: Infinity },
        { ...outputPlan, nodePayloadBytes: -1 }, { ...outputPlan, nodePayloadBytes: .5 },
        { ...outputPlan, peakAdmissionBytes: 1_100 }, { ...outputPlan, residentAdmissionBytes: 701 },
        { ...outputPlan, rangeEndpointResidentRequestedBytes: 401, residentAdmissionBytes: 1_001 },
        { ...outputPlan, nodePayloadBytes: Number.MAX_SAFE_INTEGER, peakAdmissionBytes: Number.MAX_SAFE_INTEGER },
        { ...outputPlan, [Symbol("hidden")]: 1 }];
    for (const value of planCases) {
        const tree = new OutputMutationFixture(); tree.plan = value;
        await assert.rejects(applyTreeCandidateMutation(tree, "update", payload, tree.options()), /candidate mutation/);
        assert.equal(tree.resumes, 0); assert.equal(tree.finishes, 0); assert.equal(tree.retiredSteps, 2);
    }
    assert.equal(accessorCalls, 0);
    const parsed = parseCandidateMutationOutputMemoryPlan(outputPlan);
    assert.notEqual(parsed, outputPlan); assert(Object.isFrozen(parsed));
    const readyAccessor = { ...outputReady };
    Object.defineProperty(readyAccessor, "stagedNodePayloadBytes", { get() { accessorCalls++; return 400; } });
    for (const value of [null, { ...outputReady, extra: 1 }, { ...outputReady, schema: 2 }, readyAccessor,
        { ...outputReady, stagedNodePayloadBytes: 601, residentAdmissionBytes: 701 },
        { ...outputReady, rangeEndpointResidentRequestedBytes: 99, residentAdmissionBytes: 499 },
        { ...outputReady, stagedNodePayloadBytes: NaN }, { ...outputReady, residentAdmissionBytes: 501 }]) {
        const tree = new OutputMutationFixture(), owner = new OutputOwnerFixture(tree.events); tree.ready = value;
        await assert.rejects(applyTreeCandidateMutation(tree, "delete", payload,
            tree.options({ onOutputMemoryPlan: () => owner })), /candidate mutation/);
        assert.equal(tree.finishes, 0); assert.equal(tree.resumes, 1); assert.equal(owner.live, 0);
        assert.equal(owner.state, "detached"); assert.equal(tree.retiredSteps, 2);
    }
    assert.equal(accessorCalls, 0);
    assert(Object.isFrozen(parseCandidateMutationOutputMemoryReady(outputReady, parsed)));
}

async function outputProgressBarrierAndNoopAreExact(): Promise<void> {
    for (const progress of [
        [{ ...done(1), phase: "ready" }], [{ ...pending(1, 1), phase: "emit" }],
        [{ ...pending(1), phase: "plan ready" }, { ...pending(2), phase: "plan ready" }],
        [{ ...pending(1), phase: "plan ready", hidden: true }],
        [{ ...pending(1), phase: "plan ready", completed: 2 }],
        [{ ...pending(1), phase: "plan ready", units: 2 }],
        [{ ...pending(1), phase: "plan ready", remaining: 2 }],
        [{ ...pending(1), phase: 2 }],
        [{ ...pending(1), phase: "plan ready" }, { ...pending(2, 1), phase: "emit" }, { ...done(3, 0), phase: "ready" }],
    ]) {
        const tree = new OutputMutationFixture(); tree.outputProgress = progress;
        await assert.rejects(applyTreeCandidateMutation(tree, "update", payload, tree.options()), /candidate mutation/);
        assert.equal(tree.finishes, 0); assert.equal(tree.cancels, 1); assert.equal(tree.retiredSteps, 2);
        assert.equal(hasPendingTreeCandidateMutationRetirement(tree), false);
    }
    const noop = new OutputMutationFixture(); noop.outputProgress = [{ done: true, units: 0,
        completed: 0, remaining: 0, reachable: 0, phase: "ready" }];
    await applyTreeCandidateMutation(noop, "update", "[]", noop.options({
        onOutputMemoryPlan: () => { throw new Error("noop must not acquire an output owner"); },
    }));
    assert.equal(noop.planReads, 0); assert.equal(noop.readyReads, 0); assert.equal(noop.resumes, 0);
    assert.equal(noop.finishes, 1); assert.equal(noop.retiredSteps, 2);
}

async function outputErrorsAndStopDrainBeforeRejecting(): Promise<void> {
    for (const phase of ["step", "resume", "finish", "ready-owner", "post-finish", "held-yield"] as const) {
        const tree = new OutputMutationFixture(), failure = new Error(`new native ${phase}`);
        const owner = new OutputOwnerFixture(tree.events), controller = new AbortController();
        let yields = 0, handedOff = 0;
        const held = gate(), entered = gate(); let settled = false;
        if (phase === "step") tree.outputProgress = [newProgress()[0], failure];
        if (phase === "resume") tree.resume_candidate_mutation_output_memory_v1_job = () => { tree.retiring = true; throw failure; };
        if (phase === "finish") tree.finish_candidate_mutation_job_deferred = () => { throw failure; };
        if (phase === "ready-owner") owner.ready = () => { throw failure; };
        if (phase === "post-finish") tree.afterFinish = () => controller.abort(failure);
        const work = applyTreeCandidateMutation(tree, "update", payload, tree.options({ signal: controller.signal,
            onOutputMemoryPlan: () => owner,
            onCandidateMutated: () => { handedOff++; assert.equal(owner.state, "published"); },
            cooperate: async () => {
                if (++yields === 2 && phase === "held-yield") { entered.release(); await held.promise; }
            },
            cooperateRetirement: async () => { assert.equal(tree.retiring || !tree.job, true); },
        }));
        const outcome = work.then(() => undefined, error => error).finally(() => { settled = true; });
        if (phase === "held-yield") {
            await entered.promise; controller.abort(failure); await Promise.resolve();
            assert.equal(settled, false); assert.equal(tree.cancels, 0); assert.equal(owner.released, false);
            held.release();
        }
        assert.equal(await outcome, failure); assert.equal(tree.retiredSteps, 2);
        assert.equal(handedOff, Number(phase === "post-finish"));
        assert.equal(tree.candidateRevision, phase === "post-finish" ? 2 : 1);
        assert.equal(owner.live, phase === "post-finish" ? 500 : 0);
        assert.equal(hasPendingTreeCandidateMutationRetirement(tree), false);
    }
}

async function failedOutputRetirementRetainsPinnedRetryAndPrimary(): Promise<void> {
    const originalLog = console.error; console.error = () => {};
    try {
        for (const phase of ["cancel", "step", "yield", "published", "retired"] as const) {
            const tree = new OutputMutationFixture(), owner = new OutputOwnerFixture(tree.events);
            const primary = new Error("primary retained mutation"), cleanup = new Error(`retirement ${phase}`);
            let fail = true, yields = 0;
            if (phase === "cancel") {
                const cancel = tree.cancel_candidate_mutation_job_deferred.bind(tree);
                tree.cancel_candidate_mutation_job_deferred = token => { if (fail) throw cleanup; cancel(token); };
            }
            if (phase === "step") {
                const step = tree.step_tree_retirement.bind(tree);
                tree.step_tree_retirement = (token, units) => { if (fail) throw cleanup; return step(token, units); };
            }
            if (phase === "published") {
                const publish = owner.published.bind(owner);
                owner.published = () => { if (fail) throw cleanup; publish(); };
            }
            if (phase === "retired") {
                const retire = owner.retired.bind(owner);
                owner.retired = () => { if (fail) throw cleanup; retire(); };
            }
            if (phase !== "published") tree.outputProgress = [newProgress()[0], primary];
            const outcome = await applyTreeCandidateMutation(tree, "update", payload, tree.options({
                onOutputMemoryPlan: () => owner,
                cooperateRetirement: async () => { yields++; if (phase === "yield" && fail) throw cleanup; },
            })).then(() => undefined, error => error);
            assert.equal(outcome, phase === "published" ? cleanup : primary);
            assert.equal(hasPendingTreeCandidateMutationRetirement(tree), true);
            assert.equal(owner.released, false);
            // Retry must use captured methods and owner transitions, not a
            // new session's mutable wrapper properties.
            tree.step_tree_retirement = () => { throw new Error("replacement retire used"); };
            tree.cancel_candidate_mutation_job_deferred = () => { throw new Error("replacement cancel used"); };
            fail = false; await drainTreeCandidateMutationRetirement(tree);
            assert.equal(hasPendingTreeCandidateMutationRetirement(tree), false); assert.equal(owner.released, true);
            assert.equal(tree.retiredSteps, 2); assert.equal(owner.live, phase === "published" ? 500 : 0);
            if (phase === "yield") assert.equal(yields, 2);
        }
    } finally { console.error = originalLog; }
}

async function outputCapabilitiesAndCapturedCallbacksAreAtomic(): Promise<void> {
    const methods = ["step_candidate_mutation_output_memory_v1_job", "candidate_mutation_output_memory_plan_v1_job",
        "resume_candidate_mutation_output_memory_v1_job", "candidate_mutation_output_memory_ready_v1_job",
        "finish_candidate_mutation_job_deferred", "cancel_candidate_mutation_job_deferred",
        "step_tree_retirement", "tree_version", "candidate_revision", "abort_candidate"] as const;
    for (const name of methods) for (const value of [undefined, null, 42]) {
        const tree = new OutputMutationFixture(); Object.defineProperty(tree, name, { value });
        await assert.rejects(applyTreeCandidateMutation(tree, "update", payload, tree.options()), /incomplete/);
        assert.equal(tree.begun.length, 0); assert.equal(tree.legacyCalls, 0);
    }
    const stopped = new OutputMutationFixture(), controller = new AbortController();
    const stop = new Error("already stopped"); controller.abort(stop);
    stopped.tree_version = () => { throw new Error("freed native wrapper was read"); };
    await assert.rejects(applyTreeCandidateMutation(stopped, "update", payload,
        stopped.options({ signal: controller.signal })), error => error === stop);
    assert.equal(stopped.begun.length, 0);
    const old = new MutationFixture();
    await assert.rejects(applyTreeCandidateMutation(old, "update", payload,
        old.options({ onOutputMemoryPlan: () => undefined })), /requires the complete output API/);
    assert.equal(old.begun.length, 0);
    const v1 = new OutputMutationFixture(); v1.version = 1;
    v1.plan = { ...outputPlan, scope: "v1-candidate-mutation-output" };
    v1.ready = { ...outputReady, scope: "v1-candidate-mutation-output" };
    let v1Plans = 0;
    await applyTreeCandidateMutation(v1, "delete", payload, v1.options({
        onOutputMemoryPlan: plan => {
            v1Plans++;
            assert.equal(plan.scope, "v1-candidate-mutation-output");
            return new OutputOwnerFixture(v1.events, v1.ready as CandidateMutationOutputMemoryReady);
        },
    }));
    assert.equal(v1Plans, 1); assert.equal(v1.planReads, 1);
    assert.equal(v1.finishes, 1); assert.equal(v1.retiredSteps, 2);

    const pinned = new OutputMutationFixture(), owner = new OutputOwnerFixture(pinned.events);
    let policies = 0;
    const options = pinned.options({ onOutputMemoryPlan: () => { policies++; return owner; },
        cooperate: async () => {
            options.onOutputMemoryPlan = () => { throw new Error("replaced policy"); };
            options.cooperateRetirement = async () => { throw new Error("replaced cleanup scheduler"); };
            pinned.resume_candidate_mutation_output_memory_v1_job = () => { throw new Error("replaced resume"); };
        },
    });
    await applyTreeCandidateMutation(pinned, "update", payload, options);
    assert.equal(policies, 1); assert.equal(pinned.resumes, 1); assert.equal(pinned.retiredSteps, 2);
}

async function outputActiveOwnerRejectsReentryAndFinalYieldIsOwned(): Promise<void> {
    const tree = new OutputMutationFixture(), owner = new OutputOwnerFixture(tree.events);
    let activeRejected: Promise<void> | undefined, cleanupRejected: Promise<void> | undefined;
    let turns = 0;
    await applyTreeCandidateMutation(tree, "update", payload, tree.options({
        onOutputMemoryPlan: () => {
            activeRejected = assert.rejects(drainTreeCandidateMutationRetirement(tree), TreeCandidateMutationRetirementError);
            return owner;
        },
        cooperateRetirement: async () => {
            if (++turns === 1) {
                cleanupRejected = assert.rejects(drainTreeCandidateMutationRetirement(tree), TreeCandidateMutationRetirementError);
            }
        },
    }));
    await activeRejected; await cleanupRejected;
    const final = new OutputMutationFixture(), finalOwner = new OutputOwnerFixture(final.events);
    const failure = new Error("last host turn rejected"); let finalTurns = 0;
    await assert.rejects(applyTreeCandidateMutation(final, "delete", payload, final.options({
        onOutputMemoryPlan: () => finalOwner,
        cooperateRetirement: async () => { if (++finalTurns === 2) throw failure; },
    })), TreeCandidateMutationRetirementError);
    assert.equal(finalOwner.released, true); assert.equal(finalOwner.live, 500);
    assert.equal(hasPendingTreeCandidateMutationRetirement(final), false);
}

async function outputRetirementCarriesExactCandidateAbortDebt(): Promise<void> {
    const originalLog = console.error; console.error = () => {};
    try {
        for (const newer of [false, true]) {
            const tree = new OutputMutationFixture(), failure = new Error("first retirement refused");
            const nativeStep = tree.step_tree_retirement.bind(tree);
            let failRetirement = true, failAbort = true;
            tree.step_tree_retirement = (token, units) => {
                if (failRetirement) throw failure;
                return nativeStep(token, units);
            };
            const nativeAbort = tree.abort_candidate.bind(tree);
            tree.abort_candidate = () => {
                assert.equal(tree.job, false);
                if (failAbort) throw failure;
                nativeAbort();
            };
            tree.outputProgress = [newProgress()[0], failure];
            await assert.rejects(applyTreeCandidateMutation(tree, "update", payload, tree.options()), error => error === failure);
            assert.equal(tree.candidateRevision, 1); assert.equal(tree.aborts, 0);
            await assert.rejects(abortTreeCandidateAfterMutationRetirement(tree, 1), TreeCandidateMutationRetirementError);
            assert.equal(tree.aborts, 0); assert.equal(tree.job, true);
            await assert.rejects(abortTreeCandidateAfterMutationRetirement(tree, 2), /conflicting/);
            failRetirement = false;
            if (newer) tree.candidateRevision = 2;
            if (!newer) {
                await assert.rejects(drainTreeCandidateMutationRetirement(tree), TreeCandidateMutationRetirementError);
                assert.equal(tree.job, false); assert.equal(tree.aborts, 0);
                assert.equal(hasPendingTreeCandidateMutationRetirement(tree), true,
                    "native drain erased a failed outer candidate-abort obligation");
            }
            failAbort = false;
            // Captured revision/abort methods survive replacement of the JS
            // wrapper methods while exact cleanup is pending.
            tree.candidate_revision = () => { throw new Error("replacement revision used"); };
            tree.abort_candidate = () => { throw new Error("replacement abort used"); };
            assert.equal(await abortTreeCandidateAfterMutationRetirement(tree, 1), !newer);
            assert.equal(tree.aborts, newer ? 0 : 1); assert.equal(tree.candidate, newer);
            assert.equal(hasPendingTreeCandidateMutationRetirement(tree), false);
            assert.equal(tree.retiredSteps, 2);
        }
        const direct = new OutputMutationFixture();
        assert.equal(await abortTreeCandidateAfterMutationRetirement(direct, 0), false);
        assert.equal(await abortTreeCandidateAfterMutationRetirement(direct, 1), true);
        assert.equal(direct.aborts, 1);
        assert.equal(await abortTreeCandidateAfterMutationRetirement(direct, 1), true);
        await assert.rejects(abortTreeCandidateAfterMutationRetirement(direct, NaN), /revision/);
    } finally { console.error = originalLog; }
}

async function outputMalformedRetirementAndAsyncPolicyDoNotPass(): Promise<void> {
    const originalLog = console.error; console.error = () => {};
    try {
        let getters = 0;
        const accessor = { done: false, units: 1, completed: 1 };
        Object.defineProperty(accessor, "units", { get() { getters++; return 1; } });
        for (const malformed of [null, accessor, { done: false, units: 0, completed: 0 },
            { done: false, units: 257, completed: 257 }, { done: false, units: 1, completed: 2 },
            { done: false, units: NaN, completed: NaN }, { done: true, units: 1, completed: 1, extra: true }]) {
            const tree = new OutputMutationFixture(); let valid = false;
            const retire = tree.step_tree_retirement.bind(tree);
            tree.step_tree_retirement = (token, budget) => valid ? retire(token, budget) : malformed;
            await assert.rejects(applyTreeCandidateMutation(tree, "update", payload, tree.options()), TreeCandidateMutationRetirementError);
            assert.equal(hasPendingTreeCandidateMutationRetirement(tree), true);
            assert.equal(tree.retiredSteps, 0);
            valid = true; await drainTreeCandidateMutationRetirement(tree);
            assert.equal(hasPendingTreeCandidateMutationRetirement(tree), false);
        }
        assert.equal(getters, 0);
        const asyncPolicy = new OutputMutationFixture();
        await assert.rejects(applyTreeCandidateMutation(asyncPolicy, "update", payload, asyncPolicy.options({
            onOutputMemoryPlan: (() => Promise.reject(new Error("invalid async policy"))) as unknown as
                NonNullable<ApplyTreeCandidateMutationOptions["onOutputMemoryPlan"]>,
        })), /must be synchronous/);
        assert.equal(asyncPolicy.resumes, 0); assert.equal(asyncPolicy.retiredSteps, 2);

        const asyncHook = new OutputMutationFixture(), owner = new OutputOwnerFixture(asyncHook.events);
        owner.ready = async () => { throw new Error("invalid async ready"); };
        await assert.rejects(applyTreeCandidateMutation(asyncHook, "update", payload,
            asyncHook.options({ onOutputMemoryPlan: () => owner })), /must be synchronous/);
        assert.equal(asyncHook.finishes, 0); assert.equal(owner.live, 0); assert.equal(asyncHook.retiredSteps, 2);
        await Promise.resolve();
    } finally { console.error = originalLog; }
}

let completed = false;
process.once("beforeExit", () => {
    if (!completed && !process.exitCode) { console.error("tree-candidate-mutation-job suite did not finish"); process.exitCode = 1; }
});
async function run(): Promise<void> {
    const suites = [successUsesExactKindPayloadAndBoundedSteps, heldCancellationRetainsActualWaitAndOuterCandidate,
        invalidAndInconsistentProgressCannotFinish, scopeGuardsFenceBeginStepAndFinish,
        nativeFailuresKeepTheirDifferentCleanupContracts, partialCapabilitiesNeverFallBack,
        legacyExecutesOnceWithPreAndPostGuards, cleanupFailureNeverMasksPrimary, optionsAreCapturedBeforeHostWait,
        mutationOwnershipHandoffPrecedesFalliblePostGuards, outputSuccessTransfersBeforeGuardsAndJoinsRetirement,
        mutationQuantumAmortizesHostCrossingsAndKeepsChunkYields,
        timeBudgetCoalescesFastStepsAndBoundsCancellation, unreliableClockFailsSafeForOneStepAndRecovers,
        outputAdmissionBarrierPrecedesCoalescedBuild,
        retirementSharesBudgetButKeepsFinalHostTurn,
        outputPolicyRefusalAndAbortBeforeResumeAllocateNothing, outputOwnerCaptureFailureReleasesActualAdmission,
        outputPlanAndReadyAreStrictDetachedOwnData,
        outputProgressBarrierAndNoopAreExact, outputErrorsAndStopDrainBeforeRejecting,
        failedOutputRetirementRetainsPinnedRetryAndPrimary, outputCapabilitiesAndCapturedCallbacksAreAtomic,
        outputActiveOwnerRejectsReentryAndFinalYieldIsOwned, outputRetirementCarriesExactCandidateAbortDebt,
        outputMalformedRetirementAndAsyncPolicyDoNotPass];
    for (const suite of suites) await suite();
    completed = true;
    console.log(`tree-candidate-mutation-job: ${suites.length} host-driver suites passed (synthetic native ports)`);
}
void run().catch(error => { console.error(error); process.exitCode = 1; });
