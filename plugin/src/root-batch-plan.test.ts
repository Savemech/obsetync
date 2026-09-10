import { strict as assert } from "node:assert";
import { setImmediate } from "node:timers/promises";
import { DirtyPathSet, type DirtyFileChange } from "./dirty-set";
import { DeferredChangeTracker } from "./deferred-changes";
import { ROOT_BATCH_LIMITS, selectRootBatch, rootBatchPathBytesSteps, type RootBatchDependency } from "./root-batch";
import { createRootBatchPlan, ROOT_BATCH_PLAN_ADMISSION, RootBatchPlanError, type RootPlannedBatch } from "./root-batch-plan";
import { SyncMemoryArbiter } from "./sync-memory-arbiter";

let assertions = 0;
const check = (value: unknown, message: string) => { assertions++; assert.ok(value, message); };
const same = (actual: unknown, expected: unknown, message: string) => { assertions++; assert.deepEqual(actual, expected, message); };
function deferred<T>() {
    let resolve!: (value: T) => void, reject!: (error: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
const microtasks = async () => { await Promise.resolve(); await Promise.resolve(); };
const hint = (path: string, journalId?: number): DirtyFileChange => ({ action: "modified", path,
    ...(journalId === undefined ? {} : { journalId }), mtime: 1, size: 2 });
const edge = (left: string, right: string, generation = 1): RootBatchDependency =>
    ({ left, right, generation, pending: false, uncertain: false });
const paths = (rows: readonly { path: string }[]) => rows.map(row => row.path);
const cooperate = async () => { await setImmediate(); };
async function rejects(work: Promise<unknown>, code: RootBatchPlanError["code"]): Promise<void> {
    assertions++; await assert.rejects(work, error => error instanceof RootBatchPlanError && error.code === code);
}
const comparable = (batch: RootPlannedBatch | null) => batch && ({ ready: batch.ready, queued: batch.queued,
    selectedGroups: batch.selectedGroups, cutCount: batch.cutCount, estimatedMetadataBytes: batch.estimatedMetadataBytes,
    deferred: batch.deferred });

function exactPathEncoding(): void {
    const values = ["", 'quote"name', "back\\slash", "\b\t\n\f\r\u0000\u001f", "é/ユニコード/😀", "\ud800", "\udc00",
        `${"a".repeat(4095)}😀z`, "\ud800\ud800\udc00\udc00"];
    for (let code = 0; code <= 0xffff; code++) values.push(String.fromCharCode(code));
    for (const value of values) {
        const scan = rootBatchPathBytesSteps(value); let step = scan.next(); while (!step.done) step = scan.next();
        assertions++; assert.equal(step.value, new TextEncoder().encode(JSON.stringify(value)).byteLength);
    }
}

async function sharedSafetyAndParity(): Promise<void> {
    const queued = [hint("a.md", 2), hint("b.md", 3), hint("note.md", 5)];
    const cases = [
        { ready: queued, queued, deps: [edge("a.md", "b.md", 2)] },
        { ready: [queued[0], queued[2]], queued, deps: [edge("a.md", "b.md", 2)] },
        { ready: [queued[2]], queued, deps: [edge("a.md", "b.md", 2)] },
        { ready: queued, queued, deps: [edge("a.md", "outside", 2), edge("outside", "b.md", 2)] },
        { ready: queued, queued, deps: [{ ...edge("a.md", "b.md", 2), pending: true }] },
        { ready: queued, queued, deps: [{ ...edge("a.md", "b.md", 2), uncertain: true }] },
        { ready: queued, queued, deps: [edge("a.md", "b.md", 3)] },
        { ready: queued, queued, deps: [edge("a.md", "b.md", 0)] },
        { ready: queued, queued, deps: [edge("a.md", "b.md", 2), edge("b.md", "a.md", 2)] },
    ];
    for (const value of cases) {
        const old = selectRootBatch(value.ready, value.queued, value.deps);
        const plan = await createRootBatchPlan(value.ready, value.queued, value.deps, { cooperate });
        const batch = plan.next();
        same(comparable(batch), comparable(old), "shared group/hold/priority semantics diverged without a packing hole");
        same(plan.next(), null, "intrinsic holds were revisited instead of being classified once");
        check(plan.snapshot().selectedPaths + plan.snapshot().heldPaths === queued.length, "held paths disappeared from aggregate accounting");
    }
    const shared = [hint("old.md", 2), hint("new.md", 2), hint("other.md", 7)];
    const plan = await createRootBatchPlan(shared, shared, [], { cooperate, limits: { maxPaths: 1 } });
    same(paths(plan.next()!.ready), ["other.md"], "same-ID oversized component split or blocked unrelated work");
    same(plan.snapshot().deferred["component-limit"], { components: 1, paths: 2 }, "same-ID component was not explicitly held");
    const tracker = new DeferredChangeTracker(), registration = tracker.registerLegacyRename("old.md", "new.md");
    const pending = await createRootBatchPlan(shared, shared, tracker.captureDependencies(), { cooperate });
    registration.confirm(2);
    same(paths(pending.next()!.ready), ["other.md"], "later confirmation mutated an already captured pending epoch");
    const fresh = await createRootBatchPlan(shared, shared, tracker.captureDependencies(), { cooperate });
    same(paths(fresh.next()!.ready), paths(shared), "explicit rebuild did not observe the newly confirmed group");
}

async function prefixHolesAndLateGroups(): Promise<void> {
    const queued = [hint("first.md", 1), hint("a.md", 2), hint("b.md", 3), hint("later.md", 4)];
    const deps = [edge("a.md", "b.md", 2)];
    same(paths(selectRootBatch(queued, queued, deps, { maxPaths: 2 }).ready), ["first.md", "later.md"],
        "legacy greedy selector no longer fills holes");
    const plan = await createRootBatchPlan(queued, queued, deps, { cooperate, limits: { maxPaths: 2 } });
    same(paths(plan.next()!.ready), ["first.md"], "new prefix planner scanned past a temporarily nonfitting group");
    const second = plan.next()!;
    same(paths(second.ready), ["a.md", "b.md"], "boundary component was split, consumed early or starved");
    same(second.selectedGroups, [["a.md", "b.md"]], "whole group membership was not exposed for runtime deferral");
    same(paths(plan.next()!.ready), ["later.md"], "independent later work was lost behind a prefix hole");
    same(plan.next(), null, "completed prefix iterator restarted");
    check(plan.snapshot().selectionComponentVisits <= 3 + 3, "prefix next revisited whole remaining queue");

    const late = Array.from({ length: 600 }, (_, index) => hint(`n${index}.md`, index + 1));
    const linked = await createRootBatchPlan(late, late, [edge(late[0].path, late[599].path)], { cooperate });
    const first = linked.next()!;
    check(first.ready.length === 256 && first.ready.some(row => row.path === late[599].path),
        "a peer beyond the first page was omitted from its leading component");
    same(first.selectedGroups, [[late[0].path, late[599].path]], "late dependency did not belong to the first complete root");
    const oversized = late.slice(0, 300).map((row, index) => index ? edge(late[index - 1].path, row.path) : null)
        .filter((row): row is RootBatchDependency => row !== null);
    const held = await createRootBatchPlan(late, late, oversized, { cooperate });
    same(paths(held.next()!.ready), paths(late.slice(300, 556)), "permanently oversized prefix blocked independent work");
    same(held.snapshot().deferred["component-limit"], { components: 1, paths: 300 }, "oversized full component was not held once");
}

async function twentyFiveThousandRowsAreVisitedOnce(): Promise<void> {
    const queued = Array.from({ length: 25_000 }, (_, index) => hint(`notes/${String(index).padStart(5, "0")}.md`, index + 1));
    const ready = [...queued].reverse(); let hostTurns = 0;
    const plan = await createRootBatchPlan(ready, queued, [], { cooperate: async () => { hostTurns++; await setImmediate(); } });
    const built = plan.snapshot();
    same([built.preflightRows, built.queuedRows, built.readyRows, built.dependencyRows, built.graphNodes, built.graphComponents],
        [50_000, 25_000, 25_000, 0, 25_000, 25_000], "full reviewed corpus was not built exactly once");
    same(built.graphSteps, 125_000, "graph traversal is not linear in the independent corpus");
    check(hostTurns >= 195 && hostTurns === built.cooperationCalls, "preflight/build omitted actual host cooperation");
    check(built.admittedMetadataBytes <= ROOT_BATCH_PLAN_ADMISSION.maxMetadataBytes, "25k metadata model exceeded its actual admission");
    const all: string[] = []; let batches = 0, lastVisits = 0;
    for (;;) {
        const batch = plan.next(); if (!batch) break;
        batches++;
        check(batch.ready.length > 0 && batch.ready.length <= 256 && batch.queued.length === batch.ready.length,
            "reusable planner returned an unbounded/empty transaction");
        check(batch.estimatedMetadataBytes <= ROOT_BATCH_LIMITS.metadataBytes && batch.cutCount <= 256,
            "reusable selection exceeded publication limits");
        check(!("retained" in batch) && !("unselected" in batch), "next allocated another full tail disposition");
        for (const row of batch.queued) {
            const index = Number(row.path.slice(6, 11)); check(row === queued[index], "selected hint lost original owner proof");
        }
        all.push(...paths(batch.ready));
        const current = plan.snapshot();
        check(current.selectionComponentVisits - lastVisits <= batch.ready.length + 1, "next revisited the remaining component backlog");
        lastVisits = current.selectionComponentVisits;
        same(current.graphSteps, built.graphSteps, "next rebuilt dependency graph");
    }
    same(all, paths(ready), "repeated plan changed priority or lost/duplicated work");
    same(batches, 98, "25k independent entries did not drain in bounded roots");
    same(plan.snapshot().selectionComponentVisits, 25_097, "component selection was not one linear pass plus page boundaries");
    same(hostTurns, built.cooperationCalls, "synchronous bounded next unexpectedly scheduled work");
    same(plan.snapshot().remainingEligiblePaths, 0, "consumed scheduling entries remained selectable");
    same([built.sharedReadyRows, built.overrideReadyRows], [25_000, 0], "identical scheduling metadata retained duplicate ready rows");
    check(plan.snapshot().retainedMetadataBytes === ROOT_BATCH_PLAN_ADMISSION.workspaceBytes,
        "drained compact plan retained consumed metadata charges");
    console.log(`root batch plan fixture: 25000 no-hash rows admitted=${built.admittedMetadataBytes} bytes`);
}

async function realisticHashfulCorpusSharesActualRecords(): Promise<void> {
    const queued = Array.from({ length: 25_000 }, (_, index) => ({
        ...hint(`notes/projects/archive/p-${String(index % 500).padStart(3, "0")}/workstream/meeting-and-research-note-${String(index).padStart(5, "0")}.md`, index + 1),
        hash: index.toString(16).padStart(64, "0"),
    }));
    const ready = [...queued].reverse().map(row => ({ ...row }));
    const plan = await createRootBatchPlan(ready, queued, [], { cooperate });
    const built = plan.snapshot();
    same([built.sharedReadyRows, built.overrideReadyRows], [25_000, 0], "fullscan matching hashes/stats were not actually interned");
    same(built.preflightMetadataBytes, built.buildPeakMetadataBytes, "shared matching rows allocated/charged overrides");
    check(built.buildPeakMetadataBytes > 16 * 1024 * 1024 &&
        built.buildPeakMetadataBytes <= ROOT_BATCH_PLAN_ADMISSION.maxMetadataBytes &&
        built.retainedMetadataBytes <= built.buildPeakMetadataBytes,
        "realistic long-path graph did not exercise and stay within the expanded admission cap");
    let count = 0;
    for (;;) {
        const batch = plan.next(); if (!batch) break;
        for (const row of batch.ready) {
            const expected = ready[count++];
            same(row, { action: expected.action, path: expected.path, hash: expected.hash, mtime: expected.mtime, size: expected.size },
                "compacted output silently dropped/changed hash/stat or leaked journalId");
        }
        for (const row of batch.queued) check(row === queued[row.journalId! - 1], "hashful compaction lost original hint proof");
    }
    same(count, 25_000, "hashful compact plan did not drain the complete reviewed corpus");
    await rejects(createRootBatchPlan(ready, queued, [], {
        cooperate, admission: { maxMetadataBytes: 16 * 1024 * 1024 },
    }), "ROOT_BATCH_PLAN_ADMISSION");
    // Different actual ready metadata requires real override records. A cap
    // equal to the shared-row peak must reject rather than pretend the rows
    // were shared.
    const different = ready.map(row => ({ ...row, mtime: row.mtime! + 1 }));
    await rejects(createRootBatchPlan(different, queued, [], {
        cooperate, admission: { maxMetadataBytes: built.buildPeakMetadataBytes },
    }), "ROOT_BATCH_PLAN_ADMISSION");
    same(ROOT_BATCH_PLAN_ADMISSION.maxMetadataBytes, 32 * 1024 * 1024,
        "long-path admission ceiling changed unexpectedly");
    same([ROOT_BATCH_PLAN_ADMISSION.queuedRowBytes, ROOT_BATCH_PLAN_ADMISSION.readyRowBytes, ROOT_BATCH_PLAN_ADMISSION.dependencyBytes],
        [352, 128, 256], "compaction reduced experimental weights instead of removing records");
    console.log(`root batch plan hashful long-path fixture: shared=25000 peak=${built.buildPeakMetadataBytes} retained=${built.retainedMetadataBytes} bytes; differing-stat corpus rejected at shared peak`);
}

async function overridesAreChargedAndProjectedExactly(): Promise<void> {
    const queued = [hint("a.md", 1), { ...hint("b.md", 2), mtime: 0 }, hint("c.md", 3), { ...hint("d.md", 4), hash: "d".repeat(64) }];
    const ready = [
        { ...queued[0] }, { ...queued[1], mtime: -0 }, { ...queued[2], hash: "c".repeat(64) },
        { ...queued[3], action: "created" as const },
    ];
    const baseline = await createRootBatchPlan(queued, queued, [], { cooperate });
    const plan = await createRootBatchPlan(ready, queued, [], { cooperate });
    const status = plan.snapshot();
    same([status.sharedReadyRows, status.overrideReadyRows], [1, 3], "different actual ready fields were incorrectly interned");
    same(status.buildPeakMetadataBytes - baseline.snapshot().buildPeakMetadataBytes,
        3 * ROOT_BATCH_PLAN_ADMISSION.readyRowBytes + 128, "actual ready override or distinct retained hash charge was omitted");
    check(status.preflightMetadataBytes < status.buildPeakMetadataBytes, "preflight pretended to know shared-ready overrides");
    const batch = plan.next()!;
    same(comparable(batch), comparable(selectRootBatch(ready, queued)), "interned/overridden full output changed existing selector semantics");
    check(Object.is(batch.ready[1].mtime, -0), "negative-zero ready metadata was normalized during interning");
    check(batch.ready.every(row => !("journalId" in row)), "bounded output projection leaked witness journal watermarks");
    const exact = await createRootBatchPlan(ready, queued, [], { cooperate, admission: { maxMetadataBytes: status.buildPeakMetadataBytes } });
    check(exact.next()!.ready.length === 4, "exact override peak cap rejected the actual projection");
    await rejects(createRootBatchPlan(ready, queued, [], { cooperate, admission: { maxMetadataBytes: status.buildPeakMetadataBytes - 1 } }),
        "ROOT_BATCH_PLAN_ADMISSION");
}

async function compactPlanDropsBuildGraphAndKeepsHashWitness(): Promise<void> {
    const queued = [hint("a.md", 1), hint("b.md", 2), hint("held.md", 3), hint("note.md", 4)]
        .map(row => ({ ...row, hash: "a".repeat(64) }));
    const plan = await createRootBatchPlan(queued, queued, [edge("a.md", "b.md"), edge("held.md", "outside.md")], { cooperate });
    const status = plan.snapshot();
    check(status.retainedMetadataBytes < status.buildPeakMetadataBytes,
        "compact retained model kept held rows and discarded graph edge storage");
    // This is a structural ownership assertion, NOT a heap/GC measurement.
    // The returned object must not retain union nodes or both membership lists.
    const stored = (plan as unknown as { components: Array<Record<string, any>> }).components;
    for (const component of stored) {
        check(!("ready" in component) && !("queued" in component), "compact plan retained parallel graph membership arrays");
        for (const member of component.members) {
            check(!("parent" in member) && !("weight" in member) && !("component" in member),
                "compact member retained its union graph/cycle");
            check(member.ready === member.witness, "matching full scalar record was copied despite sharing counter");
        }
    }
    queued[1].hash = "b".repeat(64);
    assertions++; assert.throws(() => plan.next(), error => error instanceof RootBatchPlanError && error.code === "ROOT_BATCH_PLAN_CHANGED");
    same(plan.snapshot().selectedPaths, 0, "changed queued hash partly consumed an interned component");
    queued[1].hash = "a".repeat(64);
    const output = plan.next()!;
    same(paths(output.ready), ["a.md", "b.md", "note.md"], "compaction leaked a held component or lost independent work");
    check(output.ready.every(row => row.hash === "a".repeat(64) && !("journalId" in row)),
        "shared witness projection dropped hashes or exposed journal generation");
    same(plan.snapshot().retainedMetadataBytes, ROOT_BATCH_PLAN_ADMISSION.workspaceBytes,
        "consumed compact entries stayed in retained charge model");
}

async function originalProofAndAtomicWitnessFailure(): Promise<void> {
    const dirty = new DirtyPathSet(); dirty.add(hint("a.md"), 1); dirty.add(hint("b.md"), 2);
    const queued = dirty.take(), ready = queued.map(row => ({ ...row }));
    const plan = await createRootBatchPlan(ready, queued, [], { cooperate });
    ready[0].size = 99;
    queued[1].size = 50;
    assertions++; assert.throws(() => plan.next(), error => error instanceof RootBatchPlanError && error.code === "ROOT_BATCH_PLAN_CHANGED");
    same(plan.snapshot().selectedPaths, 0, "mutated second hint partly consumed a first hint");
    queued[1].size = 2;
    const selected = plan.next()!;
    same(selected.ready[0].size, 2, "caller-owned materialized object altered captured prepared metadata");
    check(selected.queued[0] === queued[0], "selection cloned away actual detached hint provenance");
    dirty.restore(selected.queued);
    same(dirty.commitRetirement(dirty.captureRetirement([{ path: "a.md", throughId: 1 }, { path: "b.md", throughId: 2 }])), 2,
        "original trusted hint proof did not survive scheduling");

    dirty.add(hint("a.md"), 3); const old = dirty.take();
    dirty.add({ ...hint("a.md"), mtime: 99 }); dirty.restore(old);
    const inherited = dirty.take(), noAuthority = await createRootBatchPlan(inherited, inherited, [], { cooperate });
    dirty.restore(noAuthority.next()!.queued);
    same(dirty.commitRetirement(dirty.captureRetirement([{ path: "a.md", throughId: 3 }])), 0,
        "planner converted a carried journal watermark into own durable provenance");

    const data = [hint("data.md", 1)], hidden = await createRootBatchPlan(data, data, [], { cooperate });
    Object.defineProperty(data[0], "data", { value: new Uint8Array(1) });
    assertions++; assert.throws(() => hidden.next(), error => error instanceof RootBatchPlanError && error.code === "ROOT_BATCH_PLAN_CHANGED");
}

async function admissionAndLimitsBeforeCopies(): Promise<void> {
    let scalarCopies = 0;
    const raw = { ...hint("a.md", 1), get action() { scalarCopies++; return "modified" as const; } };
    await rejects(createRootBatchPlan([raw, raw], [raw, raw], [], { cooperate, admission: { maxPaths: 1 } }), "ROOT_BATCH_PLAN_ADMISSION");
    same(scalarCopies, 0, "cardinality rejection copied/validated graph scalar state");
    await rejects(createRootBatchPlan([raw], [raw], [], { cooperate,
        admission: { maxMetadataBytes: ROOT_BATCH_PLAN_ADMISSION.workspaceBytes + ROOT_BATCH_PLAN_ADMISSION.queuedRowBytes - 1 } }), "ROOT_BATCH_PLAN_ADMISSION");
    same(scalarCopies, 0, "byte rejection copied graph rows before preflight admission");
    await rejects(createRootBatchPlan([], [], new Array(ROOT_BATCH_PLAN_ADMISSION.maxDependencies + 1), { cooperate }), "ROOT_BATCH_PLAN_ADMISSION");
    const queued = [hint("a.md", 1), hint("b.md", 2)];
    await rejects(createRootBatchPlan(queued, queued, [edge("a.md", "x.md"), edge("b.md", "y.md")],
        { cooperate, admission: { maxPaths: 3 } }), "ROOT_BATCH_PLAN_ADMISSION");
    const base = await createRootBatchPlan(queued, queued, [], { cooperate }), exact = base.snapshot().admittedMetadataBytes;
    const admitted = await createRootBatchPlan(queued, queued, [], { cooperate, admission: { maxMetadataBytes: exact } });
    check(admitted.next()!.ready.length === 2, "exact metadata ceiling was rejected");
    await rejects(createRootBatchPlan(queued, queued, [], { cooperate, admission: { maxMetadataBytes: exact - 1 } }), "ROOT_BATCH_PLAN_ADMISSION");
    for (const field of ["maxPaths", "maxDependencies", "maxMetadataBytes"] as const) {
        for (const value of [0, -1, NaN, Infinity, 1.5, ROOT_BATCH_PLAN_ADMISSION[field] + 1]) {
            await rejects(createRootBatchPlan([], [], [], { cooperate, admission: { [field]: value } }), "ROOT_BATCH_PLAN_ADMISSION");
        }
    }
    const huge = { ...hint("x".repeat(1_000_000), 1), get action(): never { throw new Error("oversized path reached graph copying"); } };
    assertions++; await assert.rejects(createRootBatchPlan([huge], [huge], [], { cooperate }), /invalid root batch path/);
    const exactRow = selectRootBatch([queued[0]], [queued[0]]).estimatedMetadataBytes;
    const held = await createRootBatchPlan(queued, queued, [], { cooperate, limits: { maxMetadataBytes: exactRow - 1 } });
    same(held.next(), null, "publication-byte oversized component escaped cache admission into a root");
    same(held.snapshot().heldPaths, 2, "publication cap failure was confused with consumed work");
    // Array cardinality is the advertised bound: a custom iterator must not
    // smuggle additional rows/edges through length-based admission.
    const boundedQueued = queued.slice(), boundedReady = queued.slice(), boundedEdges = [edge("a.md", "b.md")];
    let iteratorCalls = 0;
    for (const array of [boundedQueued, boundedReady, boundedEdges]) {
        Object.defineProperty(array, Symbol.iterator, { value: function* () { iteratorCalls++; throw new Error("unadmitted custom iterator"); } });
    }
    const bounded = await createRootBatchPlan(boundedReady, boundedQueued, boundedEdges,
        { cooperate, admission: { maxPaths: 2, maxDependencies: 1 } });
    same([bounded.snapshot().queuedRows, bounded.snapshot().readyRows, bounded.snapshot().dependencyRows, iteratorCalls],
        [2, 2, 1, 0], "custom iterators bypassed admitted input cardinality");
    same(paths(bounded.next()!.ready), ["a.md", "b.md"], "indexed source capture changed valid rows");
}

async function cancellationAndCooperationOwnTheActualWait(): Promise<void> {
    const queued = Array.from({ length: 300 }, (_, index) => hint(`n${index}.md`, index + 1));
    const aborted = new AbortController(), reason = new Error("cancelled before planning"); aborted.abort(reason);
    assertions++; await assert.rejects(createRootBatchPlan(queued, queued, [], { cooperate: async () => { throw new Error("unexpected host call"); },
        signal: aborted.signal }), error => error === reason);
    let graphCopies = 0;
    const watched = queued.map(row => ({ ...row, get action() { graphCopies++; return "modified" as const; } }));
    const native = deferred<void>(), entered = deferred<void>(), signal = new AbortController();
    let done = false;
    const work = createRootBatchPlan(watched, watched, [], { signal: signal.signal,
        cooperate: () => { entered.resolve(); return native.promise; } });
    void work.then(() => { done = true; }, () => { done = true; });
    await entered.promise;
    signal.abort(reason); await microtasks();
    check(!done && graphCopies === 0, "cancelled preflight virtually joined host wait or copied graph before admission");
    native.resolve(); assertions++; await assert.rejects(work, error => error === reason);
    same(graphCopies, 0, "abort inside cooperate admitted another graph row after return");

    const mutated = queued.map(row => ({ ...row }));
    await rejects(createRootBatchPlan(mutated, mutated, [], { cooperate: async () => { mutated.push(hint("late.md", 301)); } }),
        "ROOT_BATCH_PLAN_CHANGED");
    const failure = new Error("host cooperation rejected");
    assertions++; await assert.rejects(createRootBatchPlan(queued, queued, [], { cooperate: async () => { throw failure; } }), error => error === failure);
    const original = await createRootBatchPlan(queued, queued, [], { cooperate });
    const changedRows = queued.map(row => ({ ...row })); let turns = 0;
    await rejects(createRootBatchPlan(changedRows, changedRows, [], {
        admission: { maxMetadataBytes: original.snapshot().admittedMetadataBytes },
        cooperate: async () => {
            if (++turns === 2) changedRows[0].hash = "a".repeat(64); // This row was already charged in both preflight loops.
            await setImmediate();
        },
    }), "ROOT_BATCH_PLAN_ADMISSION");
    const live = new AbortController(), plan = await createRootBatchPlan(queued, queued, [], { cooperate, signal: live.signal });
    live.abort(reason);
    assertions++; assert.throws(() => plan.next(), error => error === reason);
    same(plan.snapshot().selectedPaths, 0, "abort consumed scheduling positions");
}

async function disposeAndDetachedAggregates(): Promise<void> {
    const queued = [hint("a.md", 1), hint("b.md", 2)], plan = await createRootBatchPlan(queued, queued, [], { cooperate, limits: { maxPaths: 1 } });
    const first = plan.next()!, status = plan.snapshot();
    status.deferred["transaction-limit"].paths = 999; status.selectedPaths = 999;
    first.deferred["transaction-limit"].paths = 500;
    same(plan.snapshot().remainingEligiblePaths, 1, "caller mutated live aggregate state");
    plan.dispose(); plan.dispose();
    check(plan.snapshot().disposed, "disposed plan still held scheduling admission open");
    assertions++; assert.throws(() => plan.next(), error => error instanceof RootBatchPlanError && error.code === "ROOT_BATCH_PLAN_DISPOSED");
    same(paths(first.queued), ["a.md"], "dispose changed an already returned caller-owned selection");
    same(paths(queued), ["a.md", "b.md"], "dispose removed pending dirty input");
}

async function sharedAdmissionPrecedesGraphAllocationAndFollowsLifetime(): Promise<void> {
    const denied = new SyncMemoryArbiter({ capacityBytes: ROOT_BATCH_PLAN_ADMISSION.maxMetadataBytes - 1 });
    let rowReads = 0;
    const rows = new Proxy([hint("guarded.md", 1)], {
        get(target, property, receiver) {
            if (property === "0") rowReads++;
            return Reflect.get(target, property, receiver);
        },
    });
    assertions++;
    await assert.rejects(createRootBatchPlan(rows, rows, [], { cooperate, memoryArbiter: denied }), /exceeds capacity/);
    same(rowReads, 0, "plan inspected a row before shared workset admission");

    const arbiter = new SyncMemoryArbiter({ capacityBytes: ROOT_BATCH_PLAN_ADMISSION.maxMetadataBytes });
    const plan = await createRootBatchPlan([hint("owned.md", 1)], [hint("owned.md", 1)], [], { cooperate, memoryArbiter: arbiter });
    same(arbiter.snapshot().owners["root-plan"].usedBytes, ROOT_BATCH_PLAN_ADMISSION.maxMetadataBytes,
        "plan did not retain its full configured model lease");
    plan.next();
    same(arbiter.snapshot().usedBytes, ROOT_BATCH_PLAN_ADMISSION.maxMetadataBytes,
        "draining a plan released admission while retained owner state remained live");
    plan.dispose(); plan.dispose();
    same(arbiter.snapshot().usedBytes, 0, "plan dispose did not idempotently release admission");
}

let completed = false;
process.once("beforeExit", () => {
    if (!completed) { console.error("root batch plan tests did not settle their actual cooperation waits"); process.exitCode = 1; }
});
void (async () => {
    exactPathEncoding(); await sharedSafetyAndParity(); await prefixHolesAndLateGroups();
    await twentyFiveThousandRowsAreVisitedOnce(); await realisticHashfulCorpusSharesActualRecords();
    await overridesAreChargedAndProjectedExactly(); await compactPlanDropsBuildGraphAndKeepsHashWitness();
    await originalProofAndAtomicWitnessFailure();
    await admissionAndLimitsBeforeCopies(); await cancellationAndCooperationOwnTheActualWait();
    await disposeAndDetachedAggregates();
    await sharedAdmissionPrecedesGraphAllocationAndFollowsLifetime();
    completed = true; console.log(`root batch plan: ${assertions} assertions passed`);
})().catch(error => { console.error(error); process.exitCode = 1; });
