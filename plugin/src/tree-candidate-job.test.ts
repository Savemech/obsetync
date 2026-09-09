import { strict as assert } from "node:assert";
import { abortTreeCandidateAfterReachabilityRetirement, beginTreeCandidate,
    collectTreeCandidateChunkPages, collectTreeCandidateChunks, TREE_CANDIDATE_JOB_STEP_UNITS,
    TREE_CANDIDATE_CHUNK_SORT_STEP_UNITS,
    TREE_REACHABILITY_RETIREMENT_STEP_UNITS, drainTreeReachabilityRetirement,
    hasPendingTreeReachabilityRetirement, parseCandidateOpenMemoryPlan,
    type CandidateJobTree, type CandidateOpenMemoryOwner, type CandidateOpenMemoryPlan,
    type TreeJobProgress } from "./tree-candidate-job";

const h = (value: number): string => value.toString(16).padStart(64, "0");

class TreeFixture implements CandidateJobTree {
    candidate = false;
    job = false;
    legacyBegins = 0;
    cancellations = 0;
    aborts = 0;
    finishReachable = 1;
    readonly token = 7;
    steps: Array<TreeJobProgress | Error> = [
        { done: false, units: 1, completed: 1, remaining: 1, reachable: 0 },
        { done: true, units: 1, completed: 2, remaining: 0, reachable: 1 },
    ];

    begin_candidate(): void { this.legacyBegins++; this.candidate = true; }
    has_candidate(): boolean { return this.candidate; }
    abort_candidate(): void { assert.equal(this.candidate, true); this.aborts++; this.candidate = false; }
    begin_candidate_job(): number { assert.equal(this.job, false); this.job = true; return this.token; }
    step_tree_job(token: number, maxUnits: number): TreeJobProgress {
        assert.equal(token, this.token); assert.equal(maxUnits, TREE_CANDIDATE_JOB_STEP_UNITS);
        assert.equal(this.job, true); const next = this.steps.shift(); assert(next);
        if (next instanceof Error) { this.job = false; throw next; }
        return next;
    }
    finish_candidate_job(token: number): number {
        assert.equal(token, this.token); assert.equal(this.job, true); this.job = false; this.candidate = true;
        return this.finishReachable;
    }
    cancel_tree_job(token: number): void {
        assert.equal(token, this.token); assert.equal(this.job, true); this.cancellations++; this.job = false;
    }
}

async function successAndOwnershipHandoff(): Promise<void> {
    const tree = new TreeFixture(); let yields = 0, opened: number | null | undefined;
    const result = await beginTreeCandidate(tree, {
        cooperate: async () => { yields++; },
        onCandidateOpened: reachable => { assert.equal(tree.candidate, true); opened = reachable; },
    });
    assert.deepEqual(result, { cooperative: true, reachable: 1 }); assert.equal(opened, 1);
    assert.equal(yields, 2); assert.equal(tree.job, false); assert.equal(tree.candidate, true);
    assert.equal(tree.legacyBegins, 0); assert.equal(tree.cancellations, 0); assert.equal(tree.aborts, 0);
}

async function cancellationAndFailuresLeaveNoProvisionalOwner(): Promise<void> {
    const controller = new AbortController(), cancelled = new TreeFixture(); let yields = 0;
    await assert.rejects(beginTreeCandidate(cancelled, {
        signal: controller.signal,
        cooperate: async () => { if (++yields === 2) controller.abort(new Error("held begin cancelled")); },
        onCandidateOpened: () => { throw new Error("candidate opened after cancellation"); },
    }), /held begin cancelled/);
    assert.equal(cancelled.job, false); assert.equal(cancelled.candidate, false);
    assert.equal(cancelled.cancellations, 1); assert.equal(cancelled.aborts, 0);

    const nativeFailure = new TreeFixture(), failure = new Error("native step failed");
    nativeFailure.steps = [failure];
    await assert.rejects(beginTreeCandidate(nativeFailure, {
        cooperate: async () => {}, onCandidateOpened: () => {},
    }), error => error === failure);
    assert.equal(nativeFailure.job, false); assert.equal(nativeFailure.cancellations, 0);
    assert.equal(nativeFailure.candidate, false);

    const malformed = new TreeFixture();
    malformed.steps = [{ done: false, units: 2, completed: 2, remaining: 1, reachable: 1 }];
    await assert.rejects(beginTreeCandidate(malformed, {
        cooperate: async () => {}, onCandidateOpened: () => {},
    }), /invalid progress/);
    assert.equal(malformed.job, false); assert.equal(malformed.cancellations, 1);
    assert.equal(malformed.candidate, false);

    const stalled = new TreeFixture();
    stalled.steps = [{ done: false, units: 1, completed: 0, remaining: 1, reachable: 0 }];
    await assert.rejects(beginTreeCandidate(stalled, {
        cooperate: async () => {}, onCandidateOpened: () => {},
    }), /progress/);
    assert.equal(stalled.job, false); assert.equal(stalled.cancellations, 1);
}

async function finishToCallerGapAlwaysHasACleanupOwner(): Promise<void> {
    const tree = new TreeFixture(), handoffFailure = new Error("handoff failed");
    await assert.rejects(beginTreeCandidate(tree, {
        cooperate: async () => {}, onCandidateOpened: () => { throw handoffFailure; },
    }), error => error === handoffFailure);
    assert.equal(tree.job, false); assert.equal(tree.candidate, false); assert.equal(tree.aborts, 1);

    const wrongCount = new TreeFixture(); wrongCount.finishReachable = 2;
    await assert.rejects(beginTreeCandidate(wrongCount, {
        cooperate: async () => {}, onCandidateOpened: () => {},
    }), /invalid reachable count/);
    assert.equal(wrongCount.candidate, false); assert.equal(wrongCount.aborts, 1);

    const finishFailure = new TreeFixture(), failed = new Error("finish failed with job retained");
    finishFailure.finish_candidate_job = () => { throw failed; };
    await assert.rejects(beginTreeCandidate(finishFailure, {
        cooperate: async () => {}, onCandidateOpened: () => {},
    }), error => error === failed);
    assert.equal(finishFailure.job, false); assert.equal(finishFailure.cancellations, 1);

    const missingCandidate = new TreeFixture();
    missingCandidate.finish_candidate_job = () => { missingCandidate.job = false; return 1; };
    await assert.rejects(beginTreeCandidate(missingCandidate, {
        cooperate: async () => {}, onCandidateOpened: () => {},
    }), /without a candidate/);
    assert.equal(missingCandidate.candidate, false); assert.equal(missingCandidate.aborts, 0);
}

async function cleanupFailureDoesNotReplacePrimaryFailure(): Promise<void> {
    const tree = new TreeFixture(), controller = new AbortController();
    const primary = new Error("source ownership changed"), cleanup = new Error("native cancel failed");
    const cancel = tree.cancel_tree_job.bind(tree);
    tree.cancel_tree_job = token => { cancel(token); throw cleanup; };
    const originalConsoleError = console.error; const diagnostics: unknown[][] = [];
    console.error = (...values: unknown[]) => { diagnostics.push(values); };
    let yields = 0;
    try {
        await assert.rejects(beginTreeCandidate(tree, {
            signal: controller.signal,
            cooperate: async () => { if (++yields === 2) controller.abort(primary); },
            onCandidateOpened: () => {},
        }), error => {
            assert.equal(error, primary);
            assert.deepEqual((error as Error & { treeCandidateCleanupErrors?: unknown[] }).treeCandidateCleanupErrors,
                [cleanup]);
            return true;
        });
    } finally { console.error = originalConsoleError; }
    assert.equal(tree.job, false); assert.equal(diagnostics.length, 1);
}

async function legacyAndPartialCapabilitiesFailSafely(): Promise<void> {
    let candidate = false, aborts = 0, yields = 0;
    const legacy: CandidateJobTree = {
        begin_candidate: () => { candidate = true; }, has_candidate: () => candidate,
        abort_candidate: () => { aborts++; candidate = false; },
    };
    const result = await beginTreeCandidate(legacy, {
        cooperate: async () => { yields++; },
        onCandidateOpened: reachable => { assert.equal(reachable, null); },
    });
    assert.deepEqual(result, { cooperative: false, reachable: null }); assert.equal(candidate, true); assert.equal(yields, 1);
    candidate = false;
    await assert.rejects(beginTreeCandidate(legacy, {
        cooperate: async () => {}, onCandidateOpened: () => { throw new Error("legacy handoff failed"); },
    }), /legacy handoff failed/);
    assert.equal(candidate, false); assert.equal(aborts, 1);

    const partial = { ...legacy, begin_candidate_job: () => 1 };
    await assert.rejects(beginTreeCandidate(partial, {
        cooperate: async () => {}, onCandidateOpened: () => {},
    }), /incomplete candidate job API/);
    assert.equal(candidate, false);

    const nonCallable = { ...legacy, begin_candidate_job: 42 } as unknown as CandidateJobTree;
    await assert.rejects(beginTreeCandidate(nonCallable, {
        cooperate: async () => {}, onCandidateOpened: () => {},
    }), /incomplete candidate job API/);
    assert.equal(candidate, false);

    // Replacement retirement and revision existed before the additive
    // reachability family; their presence alone must keep the legacy path.
    const oldPackaged = new TreeFixture() as CandidateJobTree;
    oldPackaged.candidate_revision = () => 0;
    oldPackaged.step_tree_retirement = () => { throw new Error("unexpected retirement"); };
    assert.deepEqual(await beginTreeCandidate(oldPackaged, {
        cooperate: async () => {}, onCandidateOpened: () => {},
    }), { cooperative: true, reachable: 1 });
}

class ChunkTreeFixture implements CandidateJobTree {
    candidate = true;
    job = false;
    steps = 0;
    cancellations = 0;
    finishes = 0;
    readonly token = 11;

    begin_candidate(): void { throw new Error("unexpected begin"); }
    has_candidate(): boolean { return this.candidate; }
    abort_candidate(): void { this.candidate = false; }
    begin_candidate_chunks_job(): number { assert.equal(this.job, false); this.job = true; return this.token; }
    step_tree_job(token: number, maxUnits: number): TreeJobProgress {
        assert.equal(token, this.token); assert.equal(maxUnits, 1); assert.equal(this.job, true); this.steps++;
        return this.steps < 2
            ? { done: false, units: 1, completed: 1, remaining: 1, reachable: 1 }
            : { done: true, units: 1, completed: 2, remaining: 0, reachable: 2 };
    }
    finish_candidate_chunks_job(token: number): unknown {
        assert.equal(token, this.token); assert.equal(this.job, true); this.finishes++; this.job = false;
        return { all: [h(1), h(2)], fresh: [h(2)] };
    }
    cancel_tree_job(token: number): void {
        assert.equal(token, this.token); assert.equal(this.job, true); this.cancellations++; this.job = false;
    }
}

async function candidateChunkCollectionIsSingleTraversalAndCancellable(): Promise<void> {
    const tree = new ChunkTreeFixture(); let yields = 0;
    assert.deepEqual(await collectTreeCandidateChunks(tree, {
        cooperate: async () => { yields++; }, legacy: () => { throw new Error("unexpected fallback"); },
    }), { all: [h(1), h(2)], fresh: [h(2)] });
    assert.equal(yields, 2); assert.equal(tree.steps, 2); assert.equal(tree.finishes, 1);
    assert.equal(tree.cancellations, 0); assert.equal(tree.candidate, true);

    const cancelled = new ChunkTreeFixture(), controller = new AbortController(); yields = 0;
    await assert.rejects(collectTreeCandidateChunks(cancelled, {
        signal: controller.signal,
        cooperate: async () => { if (++yields === 2) controller.abort(new Error("chunk collection stopped")); },
        legacy: () => { throw new Error("unexpected fallback"); },
    }), /chunk collection stopped/);
    assert.equal(cancelled.job, false); assert.equal(cancelled.cancellations, 1);
    assert.equal(cancelled.finishes, 0); assert.equal(cancelled.candidate, true);

    const stalled = new ChunkTreeFixture();
    stalled.step_tree_job = () => ({ done: false, units: 1, completed: 0, remaining: 1, reachable: 0 });
    await assert.rejects(collectTreeCandidateChunks(stalled, {
        cooperate: async () => {}, legacy: () => ({}),
    }), /progress/);
    assert.equal(stalled.cancellations, 1); assert.equal(stalled.candidate, true);
}

async function candidateChunkPlansAndCapabilitiesFailClosed(): Promise<void> {
    const malformed = new ChunkTreeFixture();
    malformed.finish_candidate_chunks_job = () => {
        malformed.job = false; return { all: [h(1)], fresh: [h(2)] };
    };
    await assert.rejects(collectTreeCandidateChunks(malformed, {
        cooperate: async () => {}, legacy: () => ({}),
    }), /invalid plan/);
    assert.equal(malformed.candidate, true);

    const finishFailure = new ChunkTreeFixture(), failure = new Error("chunk finish failed");
    finishFailure.finish_candidate_chunks_job = () => { throw failure; };
    await assert.rejects(collectTreeCandidateChunks(finishFailure, {
        cooperate: async () => {}, legacy: () => ({}),
    }), error => error === failure);
    assert.equal(finishFailure.cancellations, 1); assert.equal(finishFailure.candidate, true);

    const legacy: CandidateJobTree = {
        begin_candidate: () => {}, has_candidate: () => true, abort_candidate: () => {},
    };
    let yields = 0;
    assert.deepEqual(await collectTreeCandidateChunks(legacy, {
        cooperate: async () => { yields++; }, legacy: () => ({ all: [h(1), h(2)], fresh: [h(1)] }),
    }), { all: [h(1), h(2)], fresh: [h(1)] });
    assert.equal(yields, 1);

    const stopped = new AbortController();
    await assert.rejects(collectTreeCandidateChunks(legacy, {
        signal: stopped.signal, cooperate: async () => {}, legacy: () => {
            stopped.abort(new Error("legacy chunk collection stopped"));
            return { all: [], fresh: [] };
        },
    }), /legacy chunk collection stopped/);

    const exotic = [h(3)];
    Object.defineProperty(exotic, "slice", { value: () => [] });
    assert.deepEqual(await collectTreeCandidateChunks(legacy, {
        cooperate: async () => {}, legacy: () => ({ all: exotic, fresh: exotic }),
    }), { all: [h(3)], fresh: [h(3)] });

    const partial = { ...legacy, begin_candidate_chunks_job: () => 1 };
    await assert.rejects(collectTreeCandidateChunks(partial, {
        cooperate: async () => {}, legacy: () => ({ all: [], fresh: [] }),
    }), /incomplete candidate chunk job API/);
}

class DeferredTreeFixture extends TreeFixture {
    retirement = false;
    retirementRemaining = 257;
    retirementSteps = 0;
    retirementFinishes = 0;
    deferredCancels = 0;
    revision = 0;
    chunkPlan = { all: [h(1)], fresh: [h(1)] };
    lastRetirement?: { start: number; progress: { done: boolean; units: number; completed: number } };

    candidate_revision(): number { return this.revision; }
    override begin_candidate_job(): number {
        assert.equal(this.candidate, false); assert.equal(this.retirement, false);
        return super.begin_candidate_job();
    }
    step_reachability_job_deferred(token: number, maxUnits: number): TreeJobProgress {
        assert.equal(token, this.token); assert.equal(maxUnits, TREE_CANDIDATE_JOB_STEP_UNITS);
        assert.equal(this.job, true); const next = this.steps.shift(); assert(next);
        if (next instanceof Error) { this.job = false; this.retirement = true; throw next; }
        return next;
    }
    finish_candidate_job_deferred(token: number): number {
        assert.equal(token, this.token); assert.equal(this.job, true);
        this.job = false; this.retirement = true; this.candidate = true; this.revision++;
        return this.finishReachable;
    }
    begin_candidate_chunks_job(): number {
        assert.equal(this.candidate, true); assert.equal(this.job, false); assert.equal(this.retirement, false);
        this.job = true; return this.token;
    }
    finish_candidate_chunks_job_deferred(token: number): unknown {
        assert.equal(token, this.token); assert.equal(this.job, true);
        this.job = false; this.retirement = true; return this.chunkPlan;
    }
    finish_candidate_chunks_job(): unknown { throw new Error("legacy chunk finish was selected"); }
    cancel_reachability_job_deferred(token: number): void {
        assert.equal(token, this.token);
        if (this.retirement) return;
        assert.equal(this.job, true); this.job = false; this.retirement = true; this.deferredCancels++;
    }
    step_reachability_retirement(token: number, expectedCompleted: number, maxUnits: number): unknown {
        assert.equal(token, this.token); assert.equal(this.retirement, true);
        if (this.lastRetirement?.start === expectedCompleted) return this.lastRetirement.progress;
        if (this.lastRetirement) {
            assert.equal(expectedCompleted, this.lastRetirement.progress.completed);
            assert.equal(this.lastRetirement.progress.done, false);
            this.lastRetirement = undefined;
        }
        assert.equal(expectedCompleted, 257 - this.retirementRemaining);
        assert.equal(maxUnits, TREE_REACHABILITY_RETIREMENT_STEP_UNITS);
        this.retirementSteps++;
        const units = Math.min(maxUnits, this.retirementRemaining);
        this.retirementRemaining -= units;
        const done = this.retirementRemaining === 0;
        const progress = { done, units, completed: 257 - this.retirementRemaining };
        this.lastRetirement = { start: expectedCompleted, progress };
        return progress;
    }
    finish_reachability_retirement(token: number, expectedCompleted: number): void {
        assert.equal(token, this.token); assert.equal(expectedCompleted, 257);
        if (!this.retirement && this.retirementFinishes > 0) return;
        assert.equal(this.retirementRemaining, 0); this.retirement = false; this.retirementFinishes++;
    }
    override abort_candidate(): void {
        assert.equal(this.retirement, false, "candidate abort crossed live reachability owner");
        super.abort_candidate(); this.revision++;
    }
}

const candidateOpenPlan = Object.freeze({
    schema: 1,
    scope: "v2-candidate-open-root",
    residentChunkCount: 25_000,
    rootStringCount: 4,
    rootIdentityRequestedBytes: 96,
    rootEndpointRequestedBytes: 160,
    rootStringRequestedBytes: 256,
    baselineKeySnapshotRequestedBytes: 0,
    peakAdmissionBytes: 256,
    baselineStrategy: "insertion-generation-v1",
} satisfies CandidateOpenMemoryPlan);

const candidateOpenPlanV1 = Object.freeze({
    ...candidateOpenPlan,
    scope: "v1-candidate-open-root",
    rootStringCount: 7,
    rootIdentityRequestedBytes: 96,
    rootEndpointRequestedBytes: 416,
    rootStringRequestedBytes: 512,
    peakAdmissionBytes: 512,
} satisfies CandidateOpenMemoryPlan);

class MemoryDeferredTreeFixture extends DeferredTreeFixture {
    memoryPlan: unknown;
    planCalls = 0;
    resumeCalls = 0;
    prepared = false;
    resumeFailure?: Error;
    events: string[] = [];

    constructor(readonly version = 2, plan: CandidateOpenMemoryPlan = candidateOpenPlan) {
        super(); this.memoryPlan = plan;
    }

    tree_version(): number { return this.version; }

    candidate_open_memory_plan_v1_job(token: number): unknown {
        assert.equal(token, this.token); assert.equal(this.job, true); this.planCalls++;
        this.events.push("plan"); return this.memoryPlan;
    }
    resume_candidate_open_memory_v1_job(token: number, identityBytes: number,
        endpointBytes: number, rootStringBytes: number): void {
        assert.equal(token, this.token); assert.equal(this.job, true);
        const plan = this.memoryPlan as CandidateOpenMemoryPlan;
        assert.deepEqual([identityBytes, endpointBytes, rootStringBytes],
            [plan.rootIdentityRequestedBytes, plan.rootEndpointRequestedBytes,
                plan.rootStringRequestedBytes]);
        this.resumeCalls++; this.events.push("resume");
        if (this.resumeFailure) throw this.resumeFailure;
        this.prepared = true;
    }
    override finish_candidate_job_deferred(token: number): number {
        assert.equal(this.prepared, true, "candidate finish bypassed prepared root");
        this.events.push("finish"); this.prepared = false;
        return super.finish_candidate_job_deferred(token);
    }
    override cancel_reachability_job_deferred(token: number): void {
        this.prepared = false;
        super.cancel_reachability_job_deferred(token);
    }
    override finish_reachability_retirement(token: number, expectedCompleted: number): void {
        super.finish_reachability_retirement(token, expectedCompleted);
        this.events.push("retirement-finish");
    }
}

function memoryOwner(events: string[]): CandidateOpenMemoryOwner {
    return {
        releaseBeforeOutput: () => { events.push("release"); },
        ready: () => { events.push("ready"); },
        published: () => { events.push("published"); },
        detached: () => { events.push("detached"); },
        retired: () => { events.push("retired"); },
    };
}

async function candidateOpenMemoryPlanIsStrictAndPinned(): Promise<void> {
    const parsed = parseCandidateOpenMemoryPlan({ ...candidateOpenPlan });
    assert.deepEqual(parsed, candidateOpenPlan); assert.equal(Object.isFrozen(parsed), true);
    for (const malformed of [
        { ...candidateOpenPlan, extra: 1 },
        { ...candidateOpenPlan, rootStringCount: 3 },
        { ...candidateOpenPlan, rootStringRequestedBytes: 255 },
        { ...candidateOpenPlan, baselineKeySnapshotRequestedBytes: 1 },
        { ...candidateOpenPlan, peakAdmissionBytes: Number.MAX_SAFE_INTEGER },
        Object.assign(Object.create({}), candidateOpenPlan),
    ]) assert.throws(() => parseCandidateOpenMemoryPlan(malformed), /candidate open/);
    const accessor = { ...candidateOpenPlan };
    Object.defineProperty(accessor, "rootStringRequestedBytes", { get: () => 256 });
    assert.throws(() => parseCandidateOpenMemoryPlan(accessor), /candidate open/);

    const partial = new DeferredTreeFixture();
    Object.defineProperty(partial, "candidate_open_memory_plan_v1_job", {
        value: () => candidateOpenPlan,
    });
    await assert.rejects(beginTreeCandidate(partial, {
        cooperate: async () => {}, cooperateRetirement: async () => {}, onCandidateOpened: () => {},
    }), /incomplete candidate open memory API/);
    assert.equal(partial.job, false);

    const withoutDeferred = new TreeFixture();
    Object.defineProperties(withoutDeferred, {
        tree_version: { value: () => 2 },
        candidate_open_memory_plan_v1_job: { value: () => candidateOpenPlan },
        resume_candidate_open_memory_v1_job: { value: () => {} },
    });
    await assert.rejects(beginTreeCandidate(withoutDeferred, {
        cooperate: async () => {}, onCandidateOpened: () => {},
    }), /requires deferred reachability ownership/);
    assert.equal(withoutDeferred.job, false);

    const v1 = new MemoryDeferredTreeFixture(1, candidateOpenPlanV1);
    assert.deepEqual(await beginTreeCandidate(v1, {
        cooperate: async () => {}, cooperateRetirement: async () => {}, onCandidateOpened: () => {},
    }), { cooperative: true, reachable: 1 });
    assert.equal(v1.planCalls, 1); assert.equal(v1.resumeCalls, 1);
    assert.deepEqual(v1.events, ["plan", "resume", "finish", "retirement-finish"]);
    assert.equal(v1.candidate, true); assert.equal(v1.revision, 1);
    assert.equal(v1.retirement, false); assert.equal(hasPendingTreeReachabilityRetirement(v1), false);

    const mismatchedV1 = new MemoryDeferredTreeFixture(1, candidateOpenPlan);
    await assert.rejects(beginTreeCandidate(mismatchedV1, {
        cooperate: async () => {}, cooperateRetirement: async () => {}, onCandidateOpened: () => {},
    }), /tree version mismatch/);
    assert.equal(mismatchedV1.resumeCalls, 0); assert.equal(mismatchedV1.deferredCancels, 1);
}

async function candidateOpenMemorySuccessTransfersOwnershipInOrder(): Promise<void> {
    const tree = new MemoryDeferredTreeFixture();
    const methodReads = new Map<string, number>();
    for (const name of ["candidate_open_memory_plan_v1_job", "resume_candidate_open_memory_v1_job"] as const) {
        const method = MemoryDeferredTreeFixture.prototype[name];
        Object.defineProperty(tree, name, { configurable: true, get() {
            const count = (methodReads.get(name) ?? 0) + 1; methodReads.set(name, count);
            if (count > 1) throw new Error(`${name} was reread`);
            return method;
        } });
    }
    const owner = memoryOwner(tree.events);
    const ownerReads = new Map<string, number>();
    for (const name of ["releaseBeforeOutput", "ready", "published", "detached", "retired"] as const) {
        const method = owner[name];
        Object.defineProperty(owner, name, { configurable: true, get() {
            const count = (ownerReads.get(name) ?? 0) + 1; ownerReads.set(name, count);
            if (count > 1) throw new Error(`${name} owner callback was reread`);
            return method;
        } });
    }
    assert.deepEqual(await beginTreeCandidate(tree, {
        cooperate: async () => {}, cooperateRetirement: async () => {},
        onOpenMemoryPlan: plan => {
            tree.events.push("admit"); assert.deepEqual(plan, candidateOpenPlan); return owner;
        },
        onCandidateOpened: () => { tree.events.push("handoff"); },
    }), { cooperative: true, reachable: 1 });
    assert.deepEqual(tree.events, ["plan", "admit", "resume", "ready", "finish", "published",
        "retirement-finish", "retired", "handoff"]);
    assert.deepEqual([...methodReads.values()], [1, 1]);
    assert.deepEqual([...ownerReads.values()], [1, 1, 1, 1, 1]);
    assert.equal(tree.planCalls, 1); assert.equal(tree.resumeCalls, 1);
    assert.equal(tree.candidate, true); assert.equal(hasPendingTreeReachabilityRetirement(tree), false);

    const v1 = new MemoryDeferredTreeFixture(1, candidateOpenPlanV1);
    assert.deepEqual(await beginTreeCandidate(v1, {
        cooperate: async () => {}, cooperateRetirement: async () => {},
        onOpenMemoryPlan: plan => {
            v1.events.push("admit"); assert.deepEqual(plan, candidateOpenPlanV1);
            return memoryOwner(v1.events);
        },
        onCandidateOpened: () => { v1.events.push("handoff"); },
    }), { cooperative: true, reachable: 1 });
    assert.deepEqual(v1.events, ["plan", "admit", "resume", "ready", "finish", "published",
        "retirement-finish", "retired", "handoff"]);
    assert.equal(v1.planCalls, 1); assert.equal(v1.resumeCalls, 1);
    assert.equal(v1.candidate, true); assert.equal(hasPendingTreeReachabilityRetirement(v1), false);
}

async function candidateOpenMemoryFailuresRetireTheExactOwner(): Promise<void> {
    const beforeResume = new MemoryDeferredTreeFixture(), stopped = new AbortController();
    await assert.rejects(beginTreeCandidate(beforeResume, {
        signal: stopped.signal, cooperate: async () => {}, cooperateRetirement: async () => {},
        onOpenMemoryPlan: () => { beforeResume.events.push("admit"); stopped.abort(new Error("policy changed"));
            return memoryOwner(beforeResume.events); },
        onCandidateOpened: () => {},
    }), /policy changed/);
    assert.deepEqual(beforeResume.events, ["plan", "admit", "release", "retirement-finish"]);
    assert.equal(beforeResume.resumeCalls, 0); assert.equal(beforeResume.candidate, false);
    assert.equal(hasPendingTreeReachabilityRetirement(beforeResume), false);

    const resumeFailed = new MemoryDeferredTreeFixture();
    resumeFailed.resumeFailure = new Error("prepared root allocation failed");
    await assert.rejects(beginTreeCandidate(resumeFailed, {
        cooperate: async () => {}, cooperateRetirement: async () => {},
        onOpenMemoryPlan: () => { resumeFailed.events.push("admit"); return memoryOwner(resumeFailed.events); },
        onCandidateOpened: () => {},
    }), /prepared root allocation failed/);
    assert.deepEqual(resumeFailed.events,
        ["plan", "admit", "resume", "detached", "retirement-finish", "retired"]);
    assert.equal(resumeFailed.candidate, false); assert.equal(resumeFailed.deferredCancels, 1);
    assert.equal(hasPendingTreeReachabilityRetirement(resumeFailed), false);

    const noOwner = new MemoryDeferredTreeFixture();
    await assert.rejects(beginTreeCandidate(noOwner, {
        cooperate: async () => {}, cooperateRetirement: async () => {},
        onOpenMemoryPlan: () => undefined as unknown as CandidateOpenMemoryOwner,
        onCandidateOpened: () => {},
    }), /returned no ownership/);
    assert.equal(noOwner.resumeCalls, 0); assert.equal(noOwner.deferredCancels, 1);
    assert.equal(hasPendingTreeReachabilityRetirement(noOwner), false);

    const malformed = new MemoryDeferredTreeFixture(); malformed.memoryPlan = { ...candidateOpenPlan, extra: 1 };
    await assert.rejects(beginTreeCandidate(malformed, {
        cooperate: async () => {}, cooperateRetirement: async () => {},
        onOpenMemoryPlan: () => { throw new Error("malformed plan reached policy"); },
        onCandidateOpened: () => {},
    }), /candidate open memory plan/);
    assert.equal(malformed.resumeCalls, 0); assert.equal(malformed.deferredCancels, 1);
    assert.equal(hasPendingTreeReachabilityRetirement(malformed), false);
}

async function candidateOpenMemoryPostPublicationUsesExactAbort(): Promise<void> {
    const tree = new MemoryDeferredTreeFixture(); let exactAborts = 0;
    await assert.rejects(beginTreeCandidate(tree, {
        cooperate: async () => {}, cooperateRetirement: async () => {},
        onOpenMemoryPlan: () => memoryOwner(tree.events),
        abortCandidateOpened: expectedRevision => {
            assert.equal(expectedRevision, 1); assert.equal(tree.revision, 1); assert.equal(tree.candidate, true);
            exactAborts++; tree.abort_candidate();
        },
        onCandidateOpened: () => { throw new Error("handoff failed after publication"); },
    }), /handoff failed after publication/);
    assert.equal(exactAborts, 1); assert.equal(tree.candidate, false); assert.equal(tree.revision, 2);
    assert.equal(hasPendingTreeReachabilityRetirement(tree), false);
}

async function candidateOpenMemoryFailedExactAbortRetainsRetryDebt(): Promise<void> {
    const tree = new MemoryDeferredTreeFixture(); let exactAborts = 0;
    const originalConsoleError = console.error; console.error = () => {};
    try {
        await assert.rejects(beginTreeCandidate(tree, {
            cooperate: async () => {}, cooperateRetirement: async () => {},
            onOpenMemoryPlan: () => memoryOwner(tree.events),
            abortCandidateOpened: expectedRevision => {
                assert.equal(expectedRevision, 1); exactAborts++;
                if (exactAborts === 1) throw new Error("terminal settlement temporarily failed");
                tree.abort_candidate();
            },
            onCandidateOpened: () => { throw new Error("handoff failed before ownership transfer"); },
        }), error => error instanceof Error && error.message.includes("handoff failed") &&
            (error as Error & { treeCandidateCleanupErrors?: unknown[] }).treeCandidateCleanupErrors?.length === 1);
    } finally { console.error = originalConsoleError; }
    assert.equal(exactAborts, 1); assert.equal(tree.candidate, true);
    assert.equal(hasPendingTreeReachabilityRetirement(tree), true);
    await drainTreeReachabilityRetirement(tree);
    assert.equal(exactAborts, 2); assert.equal(tree.candidate, false);
    assert.equal(hasPendingTreeReachabilityRetirement(tree), false);
}

async function deferredFinishDrainsBeforeHandoffAndRejectsStaleOwnership(): Promise<void> {
    const tree = new DeferredTreeFixture();
    let reachedResolve!: () => void, release!: () => void;
    const reached = new Promise<void>(resolve => { reachedResolve = resolve; });
    const held = new Promise<void>(resolve => { release = resolve; });
    let cleanupYields = 0, handedOff = false, settled = false;
    const operation = beginTreeCandidate(tree, {
        cooperate: async () => {},
        cooperateRetirement: async () => {
            cleanupYields++;
            if (cleanupYields === 1) { reachedResolve(); await held; }
        },
        onCandidateOpened: () => { handedOff = true; },
    }).then(result => { settled = true; return result; });
    await reached;
    assert.equal(handedOff, false); assert.equal(settled, false);
    assert.equal(hasPendingTreeReachabilityRetirement(tree), true);
    release();
    assert.deepEqual(await operation, { cooperative: true, reachable: 1 });
    assert.equal(cleanupYields, 2); assert.equal(handedOff, true);
    assert.equal(hasPendingTreeReachabilityRetirement(tree), false);

    const drifted = new DeferredTreeFixture(); cleanupYields = 0;
    await assert.rejects(beginTreeCandidate(drifted, {
        cooperate: async () => {},
        cooperateRetirement: async () => {
            if (++cleanupYields === 2) drifted.revision++;
        },
        onCandidateOpened: () => { throw new Error("stale candidate was handed off"); },
    }), /candidate changed during reachability retirement/);
    assert.equal(drifted.candidate, true, "newer candidate ownership was rolled back");
    assert.equal(drifted.aborts, 0);
}

async function deferredCancellationAndStepFailureAlwaysDrain(): Promise<void> {
    const controller = new AbortController(), cancelled = new DeferredTreeFixture();
    let normalYields = 0, cleanupYields = 0;
    await assert.rejects(beginTreeCandidate(cancelled, {
        signal: controller.signal,
        cooperate: async () => {
            if (++normalYields === 2) controller.abort(new Error("operation stopped"));
        },
        cooperateRetirement: async () => {
            assert.equal(controller.signal.aborted, true); cleanupYields++;
        },
        onCandidateOpened: () => {},
    }), /operation stopped/);
    assert.equal(cancelled.deferredCancels, 1); assert.equal(cleanupYields, 2);
    assert.equal(cancelled.retirement, false); assert.equal(cancelled.candidate, false);
    assert.equal(hasPendingTreeReachabilityRetirement(cancelled), false);

    const failed = new DeferredTreeFixture(), failure = new Error("native traversal failed");
    failed.steps = [failure];
    await assert.rejects(beginTreeCandidate(failed, {
        cooperate: async () => {}, cooperateRetirement: async () => {}, onCandidateOpened: () => {},
    }), error => error === failure);
    assert.equal(failed.deferredCancels, 0, "same-token error retirement was cancelled twice");
    assert.equal(failed.retirementSteps, 2); assert.equal(hasPendingTreeReachabilityRetirement(failed), false);
}

async function deferredCleanupFailureKeepsStrongRetryOwnership(): Promise<void> {
    const tree = new DeferredTreeFixture(), primary = new Error("source became stale");
    let normalYields = 0, failCleanup = true;
    const step = tree.step_reachability_retirement.bind(tree);
    tree.step_reachability_retirement = (token, expectedCompleted, maxUnits) => {
        if (failCleanup) throw new Error("allocator cleanup failed");
        return step(token, expectedCompleted, maxUnits);
    };
    const originalConsoleError = console.error; console.error = () => {};
    try {
        await assert.rejects(beginTreeCandidate(tree, {
            cooperate: async () => { if (++normalYields === 2) throw primary; },
            cooperateRetirement: async () => {}, onCandidateOpened: () => {},
        }), error => error === primary);
    } finally { console.error = originalConsoleError; }
    assert.equal(hasPendingTreeReachabilityRetirement(tree), true);
    assert.equal(tree.retirement, true);
    failCleanup = false;
    await drainTreeReachabilityRetirement(tree);
    assert.equal(hasPendingTreeReachabilityRetirement(tree), false);
    assert.equal(tree.retirement, false);
}

async function deferredCleanupReplaysLostProgressAndFinishAcknowledgement(): Promise<void> {
    const malformed = new DeferredTreeFixture();
    const step = malformed.step_reachability_retirement.bind(malformed);
    let corruptProgress = true;
    malformed.step_reachability_retirement = (token, expectedCompleted, maxUnits) => {
        const value = step(token, expectedCompleted, maxUnits);
        return corruptProgress ? { done: false, units: 1, completed: 99 } : value;
    };
    const originalConsoleError = console.error; console.error = () => {};
    try {
        await assert.rejects(beginTreeCandidate(malformed, {
            cooperate: async () => {}, cooperateRetirement: async () => {}, onCandidateOpened: () => {},
        }), /retirement failed/);
    } finally { console.error = originalConsoleError; }
    assert.equal(malformed.retirementSteps, 1, "malformed delivery did not advance native work twice");
    assert.equal(malformed.retirement, true); assert.equal(malformed.candidate, true);
    assert.equal(hasPendingTreeReachabilityRetirement(malformed), true);
    corruptProgress = false;
    await drainTreeReachabilityRetirement(malformed);
    assert.equal(malformed.retirementSteps, 2, "retry replayed the cached first step before advancing");
    assert.equal(malformed.retirement, false); assert.equal(malformed.candidate, false);
    assert.equal(malformed.aborts, 1); assert.equal(hasPendingTreeReachabilityRetirement(malformed), false);

    const lostFinish = new DeferredTreeFixture();
    const finish = lostFinish.finish_reachability_retirement.bind(lostFinish);
    let loseAcknowledgement = true;
    lostFinish.finish_reachability_retirement = (token, expectedCompleted) => {
        finish(token, expectedCompleted);
        if (loseAcknowledgement) throw new Error("lost finish acknowledgement");
    };
    console.error = () => {};
    try {
        await assert.rejects(beginTreeCandidate(lostFinish, {
            cooperate: async () => {}, cooperateRetirement: async () => {}, onCandidateOpened: () => {},
        }), /retirement failed/);
    } finally { console.error = originalConsoleError; }
    assert.equal(lostFinish.retirement, false); assert.equal(lostFinish.retirementFinishes, 1);
    assert.equal(lostFinish.candidate, true); assert.equal(hasPendingTreeReachabilityRetirement(lostFinish), true);
    loseAcknowledgement = false;
    await drainTreeReachabilityRetirement(lostFinish);
    assert.equal(lostFinish.retirementFinishes, 1, "finish retry was acknowledged from the native tombstone");
    assert.equal(lostFinish.candidate, false); assert.equal(lostFinish.aborts, 1);
    assert.equal(hasPendingTreeReachabilityRetirement(lostFinish), false);
}

async function deferredPostFinishGetterFailureRetainsExactAbortDebt(): Promise<void> {
    const tree = new DeferredTreeFixture();
    const hasCandidate = tree.has_candidate.bind(tree);
    let getterFails = true;
    tree.has_candidate = () => {
        if (getterFails) throw new Error("candidate getter unavailable");
        return hasCandidate();
    };
    const originalConsoleError = console.error; console.error = () => {};
    try {
        await assert.rejects(beginTreeCandidate(tree, {
            cooperate: async () => {}, cooperateRetirement: async () => {}, onCandidateOpened: () => {},
        }), /candidate getter unavailable/);
    } finally { console.error = originalConsoleError; }
    assert.equal(tree.retirement, false, "native cursor was fully retired before the host getter retry");
    assert.equal(tree.candidate, true); assert.equal(tree.aborts, 0);
    assert.equal(hasPendingTreeReachabilityRetirement(tree), true, "exact abort debt pins the wrapper");
    getterFails = false;
    await drainTreeReachabilityRetirement(tree);
    assert.equal(tree.candidate, false); assert.equal(tree.aborts, 1);
    assert.equal(hasPendingTreeReachabilityRetirement(tree), false);
}

async function deferredCleanupPinsCallbacksAndJoinsConcurrentDrains(): Promise<void> {
    const pinned = new DeferredTreeFixture();
    let cleanupYields = 0;
    const options = {
        cooperate: async () => {
            options.cooperateRetirement = async () => { throw new Error("mutated cleanup callback was used"); };
        },
        cooperateRetirement: async () => { cleanupYields++; },
        onCandidateOpened: () => {},
    };
    await beginTreeCandidate(pinned, options);
    assert.equal(cleanupYields, 2, "cleanup callback was captured before the first await");

    const joined = new DeferredTreeFixture();
    const step = joined.step_reachability_retirement.bind(joined);
    let failCleanup = true;
    joined.step_reachability_retirement = (token, expectedCompleted, maxUnits) => {
        if (failCleanup) throw new Error("cleanup temporarily unavailable");
        return step(token, expectedCompleted, maxUnits);
    };
    let releaseCleanup!: () => void, cleanupEntered!: () => void;
    const heldCleanup = new Promise<void>(resolve => { releaseCleanup = resolve; });
    const cleanupStarted = new Promise<void>(resolve => { cleanupEntered = resolve; });
    const controller = new AbortController(); let normalYields = 0;
    const originalConsoleError = console.error; console.error = () => {};
    try {
        await assert.rejects(beginTreeCandidate(joined, {
            signal: controller.signal,
            cooperate: async () => {
                if (++normalYields === 2) controller.abort(new Error("retain cleanup owner"));
            },
            cooperateRetirement: async () => {
                cleanupEntered();
                await heldCleanup;
            },
            onCandidateOpened: () => {},
        }), /retain cleanup owner/);
    } finally { console.error = originalConsoleError; }
    failCleanup = false;
    const first = drainTreeReachabilityRetirement(joined);
    await cleanupStarted;
    const second = drainTreeReachabilityRetirement(joined);
    assert.equal(first, second, "caller during a held cleanup boundary did not join the same flight");
    releaseCleanup();
    await first;
    assert.equal(joined.retirementSteps, 2); assert.equal(hasPendingTreeReachabilityRetirement(joined), false);
}

async function deferredCleanupRejectsReentryAndPinsCandidateOperations(): Promise<void> {
    const reentrant = new DeferredTreeFixture(); let cleanupYields = 0;
    await beginTreeCandidate(reentrant, {
        cooperate: async () => {},
        cooperateRetirement: async () => {
            cleanupYields++;
            await assert.rejects(drainTreeReachabilityRetirement(reentrant), error =>
                error instanceof Error && "cleanupCause" in error &&
                String((error as { cleanupCause: unknown }).cleanupCause).includes("cannot re-enter"));
        },
        onCandidateOpened: () => {},
    });
    assert.equal(cleanupYields, 2); assert.equal(reentrant.retirement, false);
    assert.equal(hasPendingTreeReachabilityRetirement(reentrant), false);

    const pinned = new DeferredTreeFixture(), failure = new Error("final cleanup turn failed");
    let turns = 0;
    await assert.rejects(beginTreeCandidate(pinned, {
        cooperate: async () => {},
        cooperateRetirement: async () => {
            if (++turns === 2) {
                pinned.has_candidate = () => { throw new Error("replacement has_candidate was called"); };
                pinned.abort_candidate = () => { throw new Error("replacement abort_candidate was called"); };
                pinned.candidate_revision = () => { throw new Error("replacement revision was called"); };
                throw failure;
            }
        },
        onCandidateOpened: () => {},
    }), error => error instanceof Error && "cleanupCause" in error &&
        (error as { cleanupCause: unknown }).cleanupCause === failure);
    assert.equal(pinned.retirement, false); assert.equal(pinned.candidate, false);
    assert.equal(pinned.aborts, 1); assert.equal(hasPendingTreeReachabilityRetirement(pinned), false);
}

async function deferredAbortDebtRejectsConflictingRevision(): Promise<void> {
    const tree = new DeferredTreeFixture();
    const step = tree.step_reachability_retirement.bind(tree);
    let failCleanup = true;
    tree.step_reachability_retirement = (token, expectedCompleted, maxUnits) => {
        if (failCleanup) throw new Error("retain candidate abort debt");
        return step(token, expectedCompleted, maxUnits);
    };
    const originalConsoleError = console.error; console.error = () => {};
    try {
        await assert.rejects(beginTreeCandidate(tree, {
            cooperate: async () => {}, cooperateRetirement: async () => {}, onCandidateOpened: () => {},
        }), /retirement failed/);
    } finally { console.error = originalConsoleError; }
    assert.equal(tree.revision, 1); assert.equal(tree.candidate, true);
    await assert.rejects(abortTreeCandidateAfterReachabilityRetirement(tree, 2), /conflicting candidate revision/);
    assert.equal(tree.candidate, true); assert.equal(hasPendingTreeReachabilityRetirement(tree), true);
    failCleanup = false;
    assert.equal(await abortTreeCandidateAfterReachabilityRetirement(tree, 1), true);
    assert.equal(tree.candidate, false); assert.equal(tree.aborts, 1);
    assert.equal(hasPendingTreeReachabilityRetirement(tree), false);
}

async function deferredChunkPlanReturnsOnlyAfterCleanup(): Promise<void> {
    const tree = new DeferredTreeFixture(); tree.candidate = true;
    tree.steps = [
        { done: false, units: 1, completed: 1, remaining: 1, reachable: 0 },
        { done: true, units: 1, completed: 2, remaining: 0, reachable: 1 },
    ];
    let cleanupYields = 0;
    assert.deepEqual(await collectTreeCandidateChunks(tree, {
        cooperate: async () => {}, cooperateRetirement: async () => { cleanupYields++; },
        legacy: () => { throw new Error("unexpected legacy plan"); },
    }), tree.chunkPlan);
    assert.equal(cleanupYields, 2); assert.equal(tree.retirement, false);
    assert.equal(hasPendingTreeReachabilityRetirement(tree), false);

    const partial = new TreeFixture() as CandidateJobTree;
    partial.step_reachability_job_deferred = () => ({});
    await assert.rejects(beginTreeCandidate(partial, {
        cooperate: async () => {}, cooperateRetirement: async () => {}, onCandidateOpened: () => {},
    }), /incomplete deferred reachability API/);

    const pinned = new DeferredTreeFixture();
    const keys = ["step_reachability_job_deferred", "finish_candidate_job_deferred",
        "finish_candidate_chunks_job_deferred", "cancel_reachability_job_deferred",
        "step_reachability_retirement", "finish_reachability_retirement", "candidate_revision"] as const;
    const reads = new Map<string, number>();
    for (const key of keys) {
        const method = DeferredTreeFixture.prototype[key];
        Object.defineProperty(pinned, key, {
            configurable: true,
            get() {
                const count = (reads.get(key) ?? 0) + 1; reads.set(key, count);
                if (count > 1) throw new Error(`deferred getter ${key} was reread`);
                return method;
            },
        });
    }
    await beginTreeCandidate(pinned, {
        cooperate: async () => {}, cooperateRetirement: async () => {}, onCandidateOpened: () => {},
    });
    assert.deepEqual([...reads.values()], keys.map(() => 1));
}

class PagedChunkTreeFixture extends DeferredTreeFixture {
    readonly all = Array.from({ length: 300 }, (_, index) => h(index + 1));
    readonly fresh = this.all.filter((_, index) => index % 3 === 0);
    traversalReady = false;
    sortStarted = false;
    sortCompleted = 0;
    pageOffset = 0;
    pageFinishes = 0;

    constructor() {
        super();
        this.candidate = true;
        this.steps = Array.from({ length: this.all.length * 2 }, (_, index) => ({
            done: index + 1 === this.all.length * 2,
            units: 1,
            completed: index + 1,
            remaining: this.all.length * 2 - index - 1,
            reachable: Math.floor((index + 1) / 2),
        }));
    }

    override step_reachability_job_deferred(token: number, maxUnits: number): TreeJobProgress {
        const result = super.step_reachability_job_deferred(token, maxUnits);
        this.traversalReady = result.done;
        return result;
    }

    candidate_chunks_sort_memory_plan_v1_job(token: number): unknown {
        assert.equal(token, this.token); assert.equal(this.job, true); assert.equal(this.traversalReady, true);
        assert.equal(this.sortStarted, false);
        const bytes = this.all.length * 32;
        return { schema: 1, scope: "candidate-chunk-plan-sort-workspace",
            hashCount: this.all.length, hashSizeBytes: 32,
            sourceHashesRequestedBytes: bytes, scratchHashesRequestedBytes: bytes,
            peakAdmissionBytes: bytes * 2, reachableSetUnmeasured: true,
            pageOutputUnmeasured: true, sortStrategy: "stable-lsd-radix-v1", pageMaxHashes: 256 };
    }

    resume_candidate_chunks_sort_memory_v1_job(token: number, source: number, scratch: number): void {
        assert.equal(token, this.token); assert.equal(source, this.all.length * 32);
        assert.equal(scratch, source); assert.equal(this.traversalReady, true); this.sortStarted = true;
    }

    step_candidate_chunks_sort_v1_job(token: number, maxUnits: number): unknown {
        assert.equal(token, this.token); assert.equal(this.sortStarted, true);
        assert(maxUnits > 0 && maxUnits <= TREE_CANDIDATE_CHUNK_SORT_STEP_UNITS);
        const total = this.all.length * 66;
        assert(this.sortCompleted < total, "READY sort was stepped");
        const units = Math.min(maxUnits, total - this.sortCompleted);
        this.sortCompleted += units;
        const done = this.sortCompleted === total;
        return { schema: 1, scope: "candidate-chunk-plan-sort", done, units,
            completed: this.sortCompleted, remaining: total - this.sortCompleted,
            allCount: this.all.length, phase: done ? "ready" : "count-byte" };
    }

    candidate_chunks_plan_info_v1_job(token: number): unknown {
        assert.equal(token, this.token); assert.equal(this.sortCompleted, this.all.length * 66);
        return { schema: 1, scope: "candidate-chunk-plan-pages",
            allCount: this.all.length, pageMaxHashes: 256 };
    }

    read_candidate_chunks_page_v1_job(token: number, offset: number, maxHashes: number): unknown {
        assert.equal(token, this.token); assert.equal(offset, this.pageOffset); assert.equal(maxHashes, 256);
        const nextOffset = Math.min(this.all.length, offset + maxHashes);
        const all = this.all.slice(offset, nextOffset);
        const selected = new Set(all);
        const fresh = this.fresh.filter(hash => selected.has(hash));
        this.pageOffset = nextOffset;
        return { schema: 1, scope: "candidate-chunk-plan-page", offset, nextOffset,
            done: nextOffset === this.all.length, all, fresh };
    }

    finish_candidate_chunks_plan_v1_job(token: number): void {
        assert.equal(token, this.token); assert.equal(this.pageOffset, this.all.length);
        assert.equal(this.job, true); this.pageFinishes++; this.job = false; this.retirement = true;
    }
}

async function pagedCandidateChunksBoundCallbacksAndCleanup(): Promise<void> {
    const tree = new PagedChunkTreeFixture();
    const pages: Array<{ all: string[]; fresh: string[] }> = [];
    let releases = 0, admissions = 0;
    const summary = await collectTreeCandidateChunkPages(tree, {
        cooperate: async () => {}, cooperateRetirement: async () => {}, legacy: () => {
            throw new Error("paged fixture used legacy chunk plan");
        },
        onSortMemoryPlan: async plan => {
            admissions++;
            assert.equal(plan.hashCount, tree.all.length);
            assert.equal(plan.peakAdmissionBytes, tree.all.length * 64);
            return { release: () => { releases++; } };
        },
        onPage: async page => {
            assert(page.all.length <= 256); assert(page.fresh.length <= page.all.length);
            pages.push({ all: page.all, fresh: page.fresh });
        },
    });
    assert.deepEqual(summary, { allCount: tree.all.length, freshCount: tree.fresh.length });
    assert.deepEqual(pages.flatMap(page => page.all), tree.all);
    assert.deepEqual(pages.flatMap(page => page.fresh), tree.fresh);
    assert.equal(pages.length, 2); assert.equal(admissions, 1); assert.equal(releases, 1);
    assert.equal(tree.pageFinishes, 1); assert.equal(tree.retirement, false);

    const stopped = new PagedChunkTreeFixture(), controller = new AbortController();
    let stoppedPages = 0, stoppedReleases = 0;
    await assert.rejects(collectTreeCandidateChunkPages(stopped, {
        signal: controller.signal, cooperate: async () => {}, cooperateRetirement: async () => {},
        legacy: () => ({}),
        onSortMemoryPlan: () => ({ release: () => { stoppedReleases++; } }),
        onPage: async () => {
            stoppedPages++; controller.abort(new Error("paged chunk sink stopped"));
        },
    }), /paged chunk sink stopped/);
    assert.equal(stoppedPages, 1); assert.equal(stopped.pageFinishes, 0);
    assert.equal(stoppedReleases, 1); assert.equal(stopped.retirement, false);
    assert.equal(stopped.candidate, true);

    const failed = new PagedChunkTreeFixture(), sinkFailure = new Error("paged sink failed");
    let failedReleases = 0;
    await assert.rejects(collectTreeCandidateChunkPages(failed, {
        cooperate: async () => {}, cooperateRetirement: async () => {}, legacy: () => ({}),
        onSortMemoryPlan: () => ({ release: () => { failedReleases++; } }),
        onPage: async () => { throw sinkFailure; },
    }), error => error === sinkFailure);
    assert.equal(failedReleases, 1); assert.equal(failed.retirement, false);
    assert.equal(failed.pageFinishes, 0); assert.equal(failed.candidate, true);
}

async function pagedCandidateChunksCapabilitiesAndLegacyFailClosed(): Promise<void> {
    const partial = new PagedChunkTreeFixture() as CandidateJobTree;
    partial.read_candidate_chunks_page_v1_job = undefined;
    await assert.rejects(collectTreeCandidateChunkPages(partial, {
        cooperate: async () => {}, cooperateRetirement: async () => {}, legacy: () => ({}),
        onSortMemoryPlan: () => ({ release: () => {} }), onPage: async () => {},
    }), /incomplete candidate chunk page API/);
    assert.equal((partial as PagedChunkTreeFixture).job, false);

    const legacy: CandidateJobTree = {
        begin_candidate: () => {}, has_candidate: () => true, abort_candidate: () => {},
    };
    const all = Array.from({ length: 600 }, (_, index) => h(index + 1));
    const fresh = all.filter((_, index) => index % 7 === 0);
    const pages: string[][] = []; let admissions = 0;
    const summary = await collectTreeCandidateChunkPages(legacy, {
        cooperate: async () => {}, cooperateRetirement: async () => {},
        legacy: () => ({ all, fresh }),
        onSortMemoryPlan: () => { admissions++; return { release: () => {} }; },
        onPage: async page => { assert(page.all.length <= 256); pages.push(page.all); },
    });
    assert.deepEqual(summary, { allCount: 600, freshCount: fresh.length });
    assert.deepEqual(pages.flat(), all); assert.deepEqual(pages.map(page => page.length), [256, 256, 88]);
    assert.equal(admissions, 0);
}

let completed = false;
process.once("beforeExit", () => {
    if (!completed && !process.exitCode) { console.error("tree-candidate-job suite did not finish"); process.exitCode = 1; }
});
async function run(): Promise<void> {
    await successAndOwnershipHandoff();
    await cancellationAndFailuresLeaveNoProvisionalOwner();
    await finishToCallerGapAlwaysHasACleanupOwner();
    await cleanupFailureDoesNotReplacePrimaryFailure();
    await legacyAndPartialCapabilitiesFailSafely();
    await candidateChunkCollectionIsSingleTraversalAndCancellable();
    await candidateChunkPlansAndCapabilitiesFailClosed();
    await deferredFinishDrainsBeforeHandoffAndRejectsStaleOwnership();
    await deferredCancellationAndStepFailureAlwaysDrain();
    await deferredCleanupFailureKeepsStrongRetryOwnership();
    await deferredCleanupReplaysLostProgressAndFinishAcknowledgement();
    await deferredPostFinishGetterFailureRetainsExactAbortDebt();
    await deferredCleanupPinsCallbacksAndJoinsConcurrentDrains();
    await deferredCleanupRejectsReentryAndPinsCandidateOperations();
    await deferredAbortDebtRejectsConflictingRevision();
    await deferredChunkPlanReturnsOnlyAfterCleanup();
    await pagedCandidateChunksBoundCallbacksAndCleanup();
    await pagedCandidateChunksCapabilitiesAndLegacyFailClosed();
    await candidateOpenMemoryPlanIsStrictAndPinned();
    await candidateOpenMemorySuccessTransfersOwnershipInOrder();
    await candidateOpenMemoryFailuresRetireTheExactOwner();
    await candidateOpenMemoryPostPublicationUsesExactAbort();
    await candidateOpenMemoryFailedExactAbortRetainsRetryDebt();
    completed = true;
    console.log("tree-candidate-job.test: bounded steps, cancellation and ownership handoff passed");
}
void run().catch(error => { console.error(error); process.exitCode = 1; });
