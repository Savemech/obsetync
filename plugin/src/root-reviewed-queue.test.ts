import { strict as assert } from "node:assert";
import { DirtyPathSet, type DirtyFileChange } from "./dirty-set";
import { DeferredChangeTracker, DeferredDependencyCaptureError } from "./deferred-changes";
import { materializeFileChanges, PathIsDirectoryError, type FileStat } from "./file-safety";
import { RootReviewedQueue, RootReviewInvalidated, RootReviewPaused, RootReviewPreempted, ROOT_REVIEW_LIMITS,
    type RootReviewedQueuePorts } from "./root-reviewed-queue";
import type { RootLocalOmission } from "./root-local-omissions";
import type { FileChange } from "./push";
import { PrioritySortError, sortByPriorityCooperatively } from "./priority-sort";
import { SyncMemoryArbiter } from "./sync-memory-arbiter";
import { ROOT_BATCH_PLAN_ADMISSION } from "./root-batch-plan";

let assertions = 0;
const check = (value: unknown, message: string) => { assertions++; assert.ok(value, message); };
const same = (value: unknown, expected: unknown, message: string) => { assertions++; assert.deepEqual(value, expected, message); };
async function rejects(work: Promise<unknown>, pattern: RegExp | ((error: unknown) => boolean), message: string) {
    assertions++; await assert.rejects(work, pattern, message);
}
const paths = (hints: readonly DirtyFileChange[]) => hints.map(hint => hint.path);
const hint = (path: string, mtime = 1, size = 1): DirtyFileChange => ({ path, action: "modified", mtime, size });
type State = FileStat | "excluded" | "directory" | null | Error;
function fixture(names = ["a.md", "b.md"]) {
    const dirty = new DirtyPathSet(), deferred = new DeferredChangeTracker(), states = new Map<string, State>();
    const tracked = new Set(names), reviews: Array<[number, number]> = [], controller = new AbortController();
    for (let index = 0; index < names.length; index++) {
        dirty.add(hint(names[index]), index + 1); states.set(names[index], { mtime: 1, size: 1 });
    }
    const f = {
        dirty, deferred, states, tracked, reviews, controller, calls: 0, yields: 0,
        active: null as string | null, current: true, approved: true,
        inFlight: new Set<string>(),
        override: undefined as RootReviewedQueuePorts["materialize"] | undefined,
        cooperateHook: undefined as (() => void | Promise<void>) | undefined,
        normal: (hints: DirtyFileChange[], omissions: RootLocalOmission[]) => materializeFileChanges(hints,
            async path => {
                const state = states.get(path);
                if (state instanceof Error) throw state;
                if (state === "directory") throw new PathIsDirectoryError(path);
                if (state === "excluded") throw new Error("excluded source must not be statted");
                return state ?? null;
            }, path => tracked.has(path), path => states.get(path) === "excluded",
            (path, reason) => omissions.push({ path, reason })),
        ports: undefined as unknown as RootReviewedQueuePorts,
    };
    f.ports = { dirty, deferred, materialize: async (hints, omissions) => {
        f.calls++; return f.override ? f.override(hints, omissions) : f.normal(hints, omissions);
    }, review: async (total, deletes) => { reviews.push([total, deletes]); return f.approved; },
    tracked: path => tracked.has(path), sort: values => values,
    cooperate: async () => { f.yields++; await f.cooperateHook?.(); },
    assertCurrent: () => { if (!f.current) throw new RootReviewInvalidated("fixture scope changed"); },
    activePath: () => f.active, upsertInFlight: path => f.inFlight.has(path),
    retryKey: "fixture", retryDeferred: false, signal: controller.signal };
    return f;
}

async function planningRevisionFencesPartitionAndPreparationTail() {
    for (const phase of ["during", "after"] as const) {
        const names = Array.from({ length: 300 }, (_, index) => `planning-${index}.md`);
        const f = fixture(names), row = [...f.dirty.iterate()][0];
        const original = f.deferred.partitionCooperatively.bind(f.deferred);
        let changed = false;
        const changePlanningState = () => {
            if (changed) return;
            changed = true;
            f.deferred.settle([row], [row], [{
                path: row.path, reason: "source-too-large", requiredBytes: 2, capacityBytes: 1,
            }], "fixture", Date.now());
        };
        f.deferred.partitionCooperatively = async (queued, materialized, retryKey, now, force, options) => {
            const result = await original(queued, materialized, retryKey, now, force, {
                ...options,
                cooperate: async () => {
                    if (phase === "during") changePlanningState();
                    await options.cooperate();
                },
            });
            if (phase === "after") changePlanningState();
            return result;
        };
        await rejects(RootReviewedQueue.create(f.ports), error => error instanceof RootReviewInvalidated &&
            error.reason === "deferred planning state changed",
        `${phase}-partition cooldown mutation escaped the preparation fence`);
        check(changed, `${phase}-partition fixture never changed planning state`);
        check(f.dirty.size === names.length && f.reviews.length === 1,
            `${phase}-partition invalidation claimed work or repeated full review`);
    }
}

async function dependencyCaptureStaysInsidePreparationFence() {
    for (const mutation of ["dependency", "cooldown"] as const) {
        for (const phase of ["during", "after"] as const) {
            const f = fixture(["note.md"]), row = [...f.dirty.iterate()][0];
            for (let index = 0; index < 300; index++) {
                f.deferred.registerLegacyRename(`missing-a/${index}.md`, `missing-b/${index}.md`, index + 1);
            }
            const original = f.deferred.captureDependenciesCooperatively.bind(f.deferred);
            let changed = false;
            const changePreparation = () => {
                if (changed) return;
                changed = true;
                if (mutation === "dependency") {
                    f.deferred.registerLegacyRename("late-a.md", "late-b.md", 999);
                } else {
                    f.deferred.settle([row], [row], [{
                        path: row.path, reason: "source-too-large", requiredBytes: 2, capacityBytes: 1,
                    }], "fixture", Date.now());
                }
            };
            f.deferred.captureDependenciesCooperatively = async options => {
                const result = await original({
                    ...options,
                    cooperate: async () => {
                        if (phase === "during") changePreparation();
                        await options.cooperate();
                    },
                });
                if (phase === "after") changePreparation();
                return result;
            };
            const reason = mutation === "dependency" ? "rename dependency changed" : "deferred planning state changed";
            await rejects(RootReviewedQueue.create(f.ports), error => error instanceof RootReviewInvalidated &&
                error.reason === reason, `${mutation}/${phase} mutation escaped dependency-capture preparation fence`);
            check(changed, `${mutation}/${phase} fixture never changed preparation state`);
            check(f.dirty.size === 1 && f.reviews.length === 1,
                `${mutation}/${phase} dependency capture claimed work or repeated review`);
        }
    }
}

async function dependencyCaptureAdmissionFailsClosed() {
    const f = fixture(["note.md"]);
    f.deferred.captureDependenciesCooperatively = async () => {
        throw new DeferredDependencyCaptureError("DEFERRED_DEPENDENCIES_ADMISSION");
    };
    await rejects(RootReviewedQueue.create(f.ports), error => error instanceof RootReviewInvalidated &&
        error.reason === "deferred dependency admission", "dependency admission did not invalidate reviewed preparation");
    check(f.dirty.size === 1 && f.reviews.length === 1,
        "dependency admission failure claimed work or repeated full review");
}

async function prioritySortStaysInsidePreparationFence() {
    for (const mutation of ["dependency", "cooldown"] as const) {
        for (const phase of ["during", "after"] as const) {
            const names = Array.from({ length: 300 }, (_, index) => `sort-${index}.md`);
            const f = fixture(names), row = [...f.dirty.iterate()][0]; let changed = false;
            const changePreparation = () => {
                if (changed) return;
                changed = true;
                if (mutation === "dependency") f.deferred.registerLegacyRename("sort-a.md", "sort-b.md", 999);
                else f.deferred.settle([row], [row], [{
                    path: row.path, reason: "source-too-large", requiredBytes: 2, capacityBytes: 1,
                }], "fixture", Date.now());
            };
            f.ports.sort = async (values, options) => {
                const result = await sortByPriorityCooperatively(values, "oldest", {
                    ...options,
                    cooperate: async () => {
                        if (phase === "during") changePreparation();
                        await options.cooperate();
                    },
                });
                if (phase === "after") changePreparation();
                return result;
            };
            const reason = mutation === "dependency" ? "rename dependency changed" : "deferred planning state changed";
            await rejects(RootReviewedQueue.create(f.ports), error => error instanceof RootReviewInvalidated &&
                error.reason === reason, `${mutation}/${phase} mutation escaped priority-sort preparation fence`);
            check(changed, `${mutation}/${phase} priority-sort fixture never changed preparation state`);
            check(f.dirty.size === names.length && f.reviews.length === 1,
                `${mutation}/${phase} priority sort claimed work or repeated full review`);
        }
    }

    const admitted = fixture(["note.md"]);
    admitted.ports.sort = async () => { throw new PrioritySortError("PRIORITY_SORT_ADMISSION"); };
    await rejects(RootReviewedQueue.create(admitted.ports), error => error instanceof RootReviewInvalidated &&
        error.reason === "priority sort admission", "priority sort admission did not invalidate reviewed preparation");
    check(admitted.dirty.size === 1 && admitted.reviews.length === 1,
        "priority sort admission failure claimed work or repeated review");

    const random = fixture(["note.md"]);
    random.ports.sort = async () => { throw new PrioritySortError("PRIORITY_SORT_RANDOM"); };
    await rejects(RootReviewedQueue.create(random.ports), error => error instanceof RootReviewInvalidated &&
        error.reason === "priority sort random source", "invalid random source did not invalidate reviewed preparation");
    check(random.dirty.size === 1 && random.reviews.length === 1,
        "invalid random source claimed work or repeated review");
}

async function lateEditsAreNotFalseExhaustion() {
    const f = fixture(["note.md"]), owner = await RootReviewedQueue.create(f.ports);
    try {
        const pending = owner.next();
        f.states.set("note.md", { mtime: 2, size: 2 }); f.dirty.add(hint("note.md", 2, 2), 2);
        const selected = await pending;
        check(selected !== null, "late edit made a nonempty queue look exhausted");
        same(selected!.queued.map(value => value.journalId), [2], "late edit selected the superseded journal generation");
        check(selected!.ready[0].mtime === 2 && f.calls <= 4, "late overlay was not freshly materialized with bounded work");
        f.dirty.restore(selected!.queued);
        check(f.dirty.commitRetirement(f.dirty.captureRetirement([{ path: "note.md", throughId: 1 }])) === 0,
            "selection licensed an older ACK over the newer dirty generation");
    } finally { owner.dispose(); }

    const deletion = fixture(["note.md"]), deleted = await RootReviewedQueue.create(deletion.ports);
    try {
        const pending = deleted.next();
        deletion.states.set("note.md", null); deletion.dirty.add({ path: "note.md", action: "deleted" }, 2);
        await rejects(pending, /new tracked deletion/, "late tracked deletion bypassed the full-review rebuild");
        check(deletion.dirty.size === 1 && deletion.dirty.capture().get("note.md")!.journalId === 2,
            "invalidated deletion review dropped its authoritative hint");
    } finally { deleted.dispose(); }

    const churn = fixture(["note.md"]), changing = await RootReviewedQueue.create(churn.ports);
    try {
        let id = 2;
        churn.override = async (hints, omissions) => {
            const result = await churn.normal(hints, omissions);
            churn.dirty.add(hint("note.md"), ++id); return result;
        };
        churn.dirty.add(hint("note.md"), id);
        await rejects(changing.next(), /continuous dirty overlay changes/, "continuous edits spun an unbounded overlay loop");
        check(churn.calls <= 2 + ROOT_REVIEW_LIMITS.overlayRefreshes && churn.dirty.size === 1,
            "bounded churn handling lost work or exceeded its stat refresh limit");
    } finally { changing.dispose(); }
}

async function lateEditsAfterSkippedAndExhaustedPlan() {
    for (const empty of [false, true]) {
        const f = fixture(empty ? [] : ["note.md"]), owner = await RootReviewedQueue.create(f.ports);
        let inserted = false, yielded = false;
        f.ports.activePath = () => {
            // Explicit synchronous host callback after the loop's initial
            // overlay check: exercise stale claim and exhausted-plan branches.
            if (!inserted) {
                inserted = true; f.tracked.add("note.md"); f.states.set("note.md", { mtime: 2, size: 2 });
                f.dirty.add(hint("note.md", 2, 2), 2);
            }
            return "note.md";
        };
        f.cooperateHook = () => {
            if (!yielded && !empty) {
                yielded = true; f.states.set("note.md", { mtime: 3, size: 3 }); f.dirty.add(hint("note.md", 3, 3), 3);
            }
        };
        try {
            const selected = await owner.next();
            check(selected?.queued.length === 1 && selected.queued[0].journalId === (empty ? 2 : 3),
                `${empty ? "exhausted" : "yielded stale"} plan failed to re-drain its late overlay`);
            if (!empty) check(yielded, "stale-claim test did not cross the intended cooperation boundary");
            f.dirty.restore(selected!.queued);
        } finally { owner.dispose(); }
    }
}

type Malformation = "missing" | "duplicate" | "contradictory" | "unknown" | "bad-reason" | "bad-action" | "bytes";
function malformed(f: ReturnType<typeof fixture>, mode: Malformation): RootReviewedQueuePorts["materialize"] {
    return async (hints, omissions) => {
        const result = await f.normal(hints, omissions);
        if (mode === "missing") return result.slice(1);
        if (mode === "duplicate") return [result[0], result[0]];
        if (mode === "contradictory") { omissions.push({ path: result[0].path, reason: "excluded" }); return [result[0]]; }
        if (mode === "unknown") return [{ ...result[0], path: "not-queued.md" }, result[1]];
        if (mode === "bad-reason") { omissions.push({ path: result[0].path, reason: "unknown" as any }); return [result[1]]; }
        if (mode === "bad-action") return [{ ...result[0], action: "renamed" as any }, result[1]];
        Object.defineProperty(result[0], "data", { get: () => { throw new Error("file bytes must never be read"); } });
        return result;
    };
}
async function everyPathRequiresExactlyOneClassification() {
    for (const mode of ["missing", "duplicate", "contradictory", "unknown", "bad-reason", "bad-action", "bytes"] as const) {
        const initial = fixture(); initial.override = malformed(initial, mode);
        await rejects(RootReviewedQueue.create(initial.ports), error => error instanceof RootReviewInvalidated,
            `initial ${mode} classification was accepted`);
        check(initial.reviews.length === 0 && initial.dirty.size === 2, `initial ${mode} undercounted review or consumed dirty work`);

        const selected = fixture(), owner = await RootReviewedQueue.create(selected.ports);
        const retirement = selected.dirty.captureRetirement([{ path: "a.md", throughId: 1 }, { path: "b.md", throughId: 2 }]);
        selected.override = malformed(selected, mode);
        try {
            await rejects(owner.next(), error => error instanceof RootReviewInvalidated, `selected ${mode} classification was accepted`);
            check(selected.dirty.size === 2 && selected.dirty.commitRetirement(retirement) === 2,
                `selected ${mode} did not restore both exact original hint identities`);
        } finally { owner.dispose(); }

        const overlay = fixture(), overlayOwner = await RootReviewedQueue.create(overlay.ports);
        overlay.dirty.add(hint("a.md"), 3); overlay.dirty.add(hint("b.md"), 4);
        overlay.override = malformed(overlay, mode);
        try {
            await rejects(overlayOwner.next(), error => error instanceof RootReviewInvalidated,
                `overlay ${mode} classification was accepted`);
            check(overlay.dirty.capture().get("a.md")!.journalId === 3 && overlay.dirty.capture().get("b.md")!.journalId === 4,
                `overlay ${mode} changed newer dirty generations`);
        } finally { overlayOwner.dispose(); }
    }
}

async function classificationDriftAndIOAreNotDeletesOrOmissions() {
    const f = fixture(["note.md"]), owner = await RootReviewedQueue.create(f.ports);
    try {
        f.states.set("note.md", null);
        await rejects(owner.next(), /selected source classification changed/, "unobserved upsert-to-delete drift reused review authority");
        check(f.dirty.size === 1 && f.dirty.capture().get("note.md")!.journalId === 1, "classification drift lost the original queue cut");
    } finally { owner.dispose(); }
    const io = fixture(["note.md"]), ioOwner = await RootReviewedQueue.create(io.ports);
    try {
        const failure = Object.assign(new Error("synthetic IO failure"), { code: "EIO" }); io.states.set("note.md", failure);
        await rejects(ioOwner.next(), error => error === failure, "IO failure was reclassified as deletion/omission");
        check(io.dirty.size === 1, "IO failure dropped claimed work");
    } finally { ioOwner.dispose(); }
    const paused = fixture(); paused.approved = false;
    await rejects(RootReviewedQueue.create(paused.ports), error => error instanceof RootReviewPaused, "refused review admitted a plan");
    check(paused.dirty.size === 2, "refused review consumed dirty work");
}

async function positiveLocalOmissionsAndGroups() {
    const f = fixture(); f.states.set("a.md", "excluded"); f.states.set("b.md", "directory"); f.tracked.delete("b.md");
    const owner = await RootReviewedQueue.create(f.ports);
    try {
        same(f.reviews, [[0, 0]], "positive local omissions counted as remote deletions");
        const selected = await owner.next();
        check(selected?.kind === "local" && selected.ready.length === 0 && selected.queued.length === 2,
            "explicit independent omissions did not produce a bounded local cut");
        f.dirty.restore(selected!.queued);
    } finally { owner.dispose(); }
    for (const explicitLink of [false, true]) {
        const linked = fixture(); linked.states.set("a.md", "excluded");
        if (explicitLink) linked.deferred.registerLegacyRename("a.md", "b.md", 1);
        else { linked.dirty.take(); linked.dirty.add(hint("a.md"), 1); linked.dirty.add(hint("b.md"), 1); }
        const held = await RootReviewedQueue.create(linked.ports);
        try {
            check(await held.next() === null && linked.dirty.size === 2,
                "local omission split a linked/shared-ID group with a materialized peer");
        } finally { held.dispose(); }
    }
    const noID = fixture(["note.md"]); noID.dirty.add(hint("note.md", 2, 2)); noID.states.set("note.md", "excluded");
    const unproven = await RootReviewedQueue.create(noID.ports);
    try {
        const selected = await unproven.next(); check(selected?.kind === "local", "inherited watermark could not select a local observation");
        noID.dirty.restore(selected!.queued);
        check(noID.dirty.commitRetirement(noID.dirty.captureRetirement([{ path: "note.md", throughId: 1 }])) === 0,
            "local omission selection bestowed old ACK authority on a newer no-ID hint");
    } finally { unproven.dispose(); }
}

async function successfulCoolingSourceRebuildsHeldDeletes() {
    const f = fixture(["large.md", "delete.md"]); f.states.set("delete.md", null);
    const queued = [...f.dirty.iterate()], materialized = await f.normal(queued, []);
    // Actual deferred metadata tracker, but no journal/base/network mutation.
    f.deferred.settle(queued, materialized, [
        { path: "large.md", reason: "source-too-large", requiredBytes: 100, capacityBytes: 10 },
        { path: "delete.md", reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0 },
    ], "fixture", Date.now());
    const owner = await RootReviewedQueue.create(f.ports);
    try {
        check(await owner.next() === null && f.dirty.size === 2, "cooling attachment failed to hold the delete");
        f.states.set("large.md", { mtime: 2, size: 2 }); f.dirty.add(hint("large.md", 2, 2), 3);
        const selected = await owner.next();
        same(paths(selected!.queued), ["large.md"], "new small source generation did not get an independent retry");
        // Simulate ONLY the caller's already-successful settlement boundary.
        owner.settled(new Set(["large.md"]), [], true);
        await rejects(owner.next(), /cooling completed.*rebuild/, "last cooling source succeeded but stale graph falsely exhausted held deletes");
        check(f.dirty.has("delete.md") && !f.dirty.has("large.md"), "cooling transition changed authoritative pending work");
    } finally { owner.dispose(); }
    const rebuilt = await RootReviewedQueue.create(f.ports);
    try {
        const deletion = await rebuilt.next();
        check(deletion?.ready.length === 1 && deletion.ready[0].path === "delete.md" && deletion.ready[0].action === "deleted",
            "fresh full audit did not recover the previously held delete");
        same(f.reviews.at(-1), [1, 1], "rebuild skipped complete remaining deletion review");
        f.dirty.restore(deletion!.queued);
    } finally { rebuilt.dispose(); }
}

async function partialCoolingAndDetachedSnapshot() {
    const f = fixture(["one.md", "two.md", "delete.md"]); f.states.set("delete.md", null);
    const queued = [...f.dirty.iterate()], materialized = await f.normal(queued, []);
    f.deferred.settle(queued, materialized, [
        { path: "one.md", reason: "source-too-large", requiredBytes: 100, capacityBytes: 10 },
        { path: "two.md", reason: "source-too-large", requiredBytes: 100, capacityBytes: 10 },
        { path: "delete.md", reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0 },
    ], "fixture", Date.now());
    const owner = await RootReviewedQueue.create(f.ports);
    try {
        const before = owner.snapshot();
        same(Object.keys(before).sort(), ["cooling", "deferred", "disposed", "reviewMetadataBytes", "rows", "urgent"],
            "queue snapshot exposed variable path/identity fields");
        check(before.rows === 3 && before.cooling === 2 && before.urgent === 0 && before.reviewMetadataBytes > 0,
            "queue snapshot did not report current ledger/cooling charge");
        const held = owner.snapshot().deferred["missing-peer"].paths;
        before.rows = 0; before.cooling = 0; before.reviewMetadataBytes = 0; before.deferred["missing-peer"].paths = 999;
        check(owner.snapshot().rows === 3 && owner.snapshot().cooling === 2 && owner.snapshot().reviewMetadataBytes > 0 &&
            owner.snapshot().deferred["missing-peer"].paths === held, "caller mutated live queue/plan diagnostics");

        f.states.set("one.md", { mtime: 2, size: 2 }); f.dirty.add(hint("one.md", 2, 2), 4);
        const first = await owner.next(); same(paths(first!.queued), ["one.md"], "first cooling source retry was not independent");
        owner.settled(new Set(["one.md"]), [], true);
        check(owner.snapshot().cooling === 1 && await owner.next() === null,
            "partial cooling completion forced a rebuild or exposed the held delete");
        f.states.set("two.md", { mtime: 2, size: 2 }); f.dirty.add(hint("two.md", 2, 2), 5);
        const last = await owner.next(); same(paths(last!.queued), ["two.md"], "last cooling source retry was not selected");
        owner.settled(new Set(["two.md"]), [], true);
        check(owner.snapshot().cooling === 0 && owner.snapshot().rows === 1, "accepted settlements did not update live ledger diagnostics");
        await rejects(owner.next(), /cooling completed.*rebuild/, "nonempty-to-empty cooling boundary did not require full graph rebuild");
    } finally { owner.dispose(); }
    const closed = owner.snapshot();
    check(closed.disposed && closed.rows === 0 && closed.urgent === 0 && closed.cooling === 0 && closed.reviewMetadataBytes === 0,
        "disposed snapshot retained live ledger/cooling charge");
    check(Object.values(closed.deferred).every(value => value.components === 0 && value.paths === 0),
        "disposed snapshot reported stale graph holds");
}

async function forcedRetriesSurviveLocalAndPrefixBatches() {
    const prefixes = Array.from({ length: 300 }, (_, index) => `note-${index}.md`);
    const f = fixture(["ignored.md", "delete.md", ...prefixes, "large.md"]);
    f.states.set("ignored.md", "excluded"); f.states.set("delete.md", null); f.ports.retryDeferred = true;
    const queued = [...f.dirty.iterate()], current = await f.normal(queued, []);
    f.deferred.settle(queued, current, [
        { path: "large.md", reason: "source-too-large", requiredBytes: 100, capacityBytes: 10 },
        { path: "delete.md", reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0 },
    ], "fixture", Date.now());
    const owner = await RootReviewedQueue.create(f.ports);
    try {
        check(owner.hasForcedRetries() && owner.snapshot().cooling === 1, "force retry erased known cooling before a source attempt");
        const local = await owner.next();
        check(local?.kind === "local" && local.queued[0].path === "ignored.md", "forced retry fixture did not begin with local ACK");
        owner.settled(new Set(["ignored.md"]), [], true);
        check(owner.hasForcedRetries(), "local ACK consumed unrelated forced source opportunity");
        const first = await owner.next();
        check(first!.queued.length === 255 && first!.queued.every(value => value.path.startsWith("note-")) &&
            f.dirty.has("delete.md") && f.dirty.has("large.md"), "cooled source outside batch failed to hold an earlier independent delete");
        owner.settled(new Set(paths(first!.queued)), [], true);
        check(owner.hasForcedRetries(), "unrelated prefix root consumed later forced source opportunity");
        const second = await owner.next();
        check(second!.queued.some(value => value.path === "large.md") && !second!.queued.some(value => value.path === "delete.md"),
            "late forced source was skipped after local/prefix batches or delete was invented in its cut");
        owner.settled(new Set(paths(second!.queued)), [], true);
        await rejects(owner.next(), /cooling completed.*rebuild/, "successful late source did not rebuild its previously held delete");
        check(f.dirty.size === 1 && f.dirty.has("delete.md"), "prefix/source settlement lost the held delete");
    } finally { owner.dispose(); }
    check(!owner.hasForcedRetries(), "disposed owner kept retry admission alive");
}

async function forcedCoolingDeletesRequireWholeAtomicCandidate() {
    for (const linked of [false, true]) {
        const f = fixture(["large.md", "delete.md", "ignored.md"]);
        f.states.set("delete.md", null); f.states.set("ignored.md", "excluded"); f.ports.retryDeferred = true;
        if (linked) f.deferred.registerLegacyRename("delete.md", "large.md", 1);
        const queued = [...f.dirty.iterate()], current = await f.normal(queued, []);
        f.deferred.settle(queued, current, [
            { path: "large.md", reason: "source-too-large", requiredBytes: 100, capacityBytes: 10 },
            { path: "delete.md", reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0 },
        ], "fixture", Date.now());
        const owner = await RootReviewedQueue.create(f.ports);
        const actualClaim = f.dirty.claimHints.bind(f.dirty), claims: string[][] = [];
        f.dirty.claimHints = hints => { claims.push(paths(hints)); return actualClaim(hints); };
        try {
            const local = await owner.next(); check(local?.kind === "local", "atomic force fixture did not exercise prior local ACK");
            owner.settled(new Set(["ignored.md"]), [], true); claims.length = 0;
            const selection = await owner.next();
            same(paths(selection!.queued).sort(), ["delete.md", "large.md"], "complete forced cooling candidate was not tried together");
            check(claims.length === 1 && claims[0].length === 2 && !owner.hasForcedRetries(),
                "cooling source/delete was not one atomic proof claim or retained a spent forced retry");
            if (linked) check(selection!.selectedGroups.length === 1 && selection!.selectedGroups[0].length === 2,
                "forced source/delete lost transitive group membership");
            // Unit proof of retry consumption; actual push source refusal and
            // WAL/base preservation are tested at the engine boundary.
            f.dirty.restore(selection!.queued);
        } finally { owner.dispose(); }
    }

    const stale = fixture(["large.md", "delete.md"]); stale.states.set("delete.md", null); stale.ports.retryDeferred = true;
    stale.deferred.registerLegacyRename("delete.md", "large.md", 1);
    const queued = [...stale.dirty.iterate()];
    stale.deferred.settle(queued, await stale.normal(queued, []), [
        { path: "large.md", reason: "source-too-large", requiredBytes: 100, capacityBytes: 10 },
        { path: "delete.md", reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0 },
    ], "fixture", Date.now());
    const owner = await RootReviewedQueue.create(stale.ports), original = stale.dirty.claimHints.bind(stale.dirty);
    stale.dirty.claimHints = hints => { stale.dirty.add(hint("large.md", 2, 2), 3); return original(hints); };
    try {
        await rejects(owner.next(), /cooling retry candidate changed/, "stale atomic cooling proof partially claimed a delete");
        check(stale.dirty.size === 2 && stale.dirty.has("delete.md") && stale.dirty.capture().get("large.md")!.journalId === 3,
            "atomic cooling failure lost a delete or newer source hint");
    } finally { owner.dispose(); }
}

async function freshStatEligibilitySurvivesOldHintCooldown() {
    for (const manual of [false, true]) for (const freshSize of [0, 2]) {
        const prefixes = Array.from({ length: 300 }, (_, index) => `prefix-${index}.md`);
        const f = fixture(["ignored.md", "delete.md", ...prefixes, "large.md"]);
        f.states.set("ignored.md", "excluded"); f.states.set("delete.md", null); f.ports.retryDeferred = manual;
        const queued = [...f.dirty.iterate()], old = queued.at(-1)!;
        f.deferred.settle(queued, await f.normal(queued, []), [
            { path: "large.md", reason: "source-too-large", requiredBytes: 100, capacityBytes: 10 },
            { path: "delete.md", reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0 },
        ], "fixture", Date.now());
        // No live event: the old dirty generation is unchanged, while a new
        // stat makes this source eligible during the full metadata review.
        f.states.set("large.md", { mtime: 2, size: freshSize });
        check(!f.deferred.hasRunnable([old], "fixture", Date.now(), false), "fresh-stat repro lost its original cooled hint");
        const normal = f.deferred.partition(queued, await f.normal(queued, []), "fixture", Date.now(), false);
        check(normal.ready.some(change => change.path === "large.md") && normal.deferred.length === 0,
            "fresh-stat repro was not eligible under actual normal partition");
        const owner = await RootReviewedQueue.create(f.ports);
        try {
            check(owner.snapshot().cooling === 1, `${manual ? "manual" : "normal"} fresh-stat review forgot known cooling`);
            check(owner.hasRunnableReviewed() && owner.hasForcedRetries() === manual,
                "fresh normal eligibility was lost or mislabeled as explicit force");
            const local = await owner.next(); check(local?.kind === "local", "fresh-stat retry did not cross its local ACK prefix");
            owner.settled(new Set(paths(local!.queued)), [], true);
            check(owner.hasRunnableReviewed(), "local ACK consumed another source's fresh eligibility");
            const prefix = await owner.next();
            check(prefix!.queued.length === 255 && prefix!.queued.every(value => value.path.startsWith("prefix-")),
                "fresh-stat source outside candidate failed to hold an independent delete");
            owner.settled(new Set(paths(prefix!.queued)), [], true);
            check(owner.hasRunnableReviewed(), "prefix root consumed another source's fresh eligibility");
            const source = await owner.next(), selected = source?.queued.find(value => value.path === "large.md");
            check(selected !== undefined && selected.journalId === old.journalId && selected.mtime === old.mtime && selected.size === old.size,
                "fresh reviewed source was skipped or its original dirty proof was replaced");
            check(source!.ready.some(value => value.path === "large.md" && value.mtime === 2 && value.size === freshSize) &&
                !source!.queued.some(value => value.path === "delete.md"), "fresh-stat retry lost materialized state or invented a held delete");
            check(!owner.hasRunnableReviewed(), "actual source claim failed to consume finite reviewed/forced slots");
            f.dirty.restore(source!.queued);
            check(await owner.next() === null && !owner.hasRunnableReviewed(), "restored old proof fabricated an endless retry opportunity");
        } finally { owner.dispose(); }
        check(!owner.hasRunnableReviewed(), "disposed owner retained reviewed eligibility");
    }
}

async function cooledPositiveOmissionsNeedNoSourceRetry() {
    for (const state of ["excluded", "directory"] as const) for (const manual of [false, true]) {
        const f = fixture(["former-large.md"]); f.ports.retryDeferred = manual;
        const queued = [...f.dirty.iterate()];
        f.deferred.settle(queued, await f.normal(queued, []), [
            { path: "former-large.md", reason: "source-too-large", requiredBytes: 100, capacityBytes: 10 },
        ], "fixture", Date.now());
        f.states.set("former-large.md", state); if (state === "directory") f.tracked.clear();
        const owner = await RootReviewedQueue.create(f.ports);
        try {
            check(!owner.hasRunnableReviewed() && !owner.hasForcedRetries() && owner.snapshot().cooling === 0,
                "positive omission allocated an unnecessary source retry slot");
            const local = await owner.next();
            check(local?.kind === "local" && local.ready.length === 0 && local.queued.length === 1,
                `old source cooldown blocked a positively proven ${state} local cut`);
            same(f.reviews, [[0, 0]], "cooled omission fabricated a reviewed remote delete");
            f.dirty.restore(local!.queued);
            const cut = f.dirty.captureRetirement([{ path: "former-large.md", throughId: 1 }]);
            check(f.dirty.commitRetirement(cut) === 1, "cooled omission lost its exact original retirement proof");
            owner.settled(new Set(["former-large.md"]), [], true);
            check(await owner.next() === null && f.dirty.size === 0, "settled omission invented root work or retained a retry loop");
        } finally { owner.dispose(); }
    }
}

async function freshOverlayEligibilityNeverGrantsNewJournalAuthority() {
    const f = fixture(["large.md", "delete.md"]); f.states.set("delete.md", null);
    const queued = [...f.dirty.iterate()];
    f.deferred.settle(queued, await f.normal(queued, []), [
        { path: "large.md", reason: "source-too-large", requiredBytes: 100, capacityBytes: 10 },
        { path: "delete.md", reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0 },
    ], "fixture", Date.now());
    const owner = await RootReviewedQueue.create(f.ports);
    try {
        check(!owner.hasRunnableReviewed() && await owner.next() === null, "unchanged normal cooled source gained an unsolicited retry");
        // A scan hint may carry an older watermark without owning that ID.
        f.dirty.add(hint("large.md")); f.states.set("large.md", { mtime: 2, size: 2 });
        const selected = await owner.next();
        same(paths(selected!.queued), ["large.md"], "fresh bounded overlay eligibility was skipped or selected a held delete");
        check(selected!.ready[0].mtime === 2 && !owner.hasRunnableReviewed(), "overlay did not consume one fresh source opportunity");
        f.dirty.restore(selected!.queued);
        check(f.dirty.commitRetirement(f.dirty.captureRetirement([{ path: "large.md", throughId: 1 }])) === 0,
            "fresh overlay eligibility granted old journal ACK authority to a newer no-ID hint");
        check(f.dirty.size === 2 && await owner.next() === null, "overlay retry lost held work or fabricated another finite opportunity");
    } finally { owner.dispose(); }

    const replaced = fixture(["large.md"]), initial = [...replaced.dirty.iterate()];
    replaced.deferred.settle(initial, await replaced.normal(initial, []), [
        { path: "large.md", reason: "source-too-large", requiredBytes: 100, capacityBytes: 10 },
    ], "fixture", Date.now());
    replaced.states.set("large.md", { mtime: 2, size: 2 });
    const replaceOwner = await RootReviewedQueue.create(replaced.ports);
    try {
        check(replaceOwner.hasRunnableReviewed(), "replacement repro lacked the old reviewed slot");
        replaced.dirty.add(hint("large.md")); replaced.states.set("large.md", { mtime: 1, size: 1 });
        check(await replaceOwner.next() === null && !replaceOwner.hasRunnableReviewed() && replaced.dirty.size === 1,
            "new row inherited a stale fresh-stat eligibility slot");
    } finally { replaceOwner.dispose(); }
}

async function slicedSuffixRestoreIsInvisibleButRealSameScalarEventIsUrgent() {
    const names = Array.from({ length: 300 }, (_, index) => `note-${index.toString().padStart(3, "0")}.md`);
    const f = fixture(names), owner = await RootReviewedQueue.create(f.ports);
    try {
        const selected = await owner.next();
        same(selected!.queued.length, 256, "fixture did not create a bounded first plan cut");
        const accepted = selected!.queued.slice(0, 32), suffix = selected!.queued.slice(32);
        // Restoring the queue's exact owned claim intentionally does not emit
        // an overlay mutation. The remaining static plan can therefore run
        // before this consumed suffix is recovered by one final rebuild.
        f.dirty.restore(suffix);
        owner.settled(new Set(accepted.map(row => row.path)), [], true);

        const remainingPlan = await owner.next();
        same(paths(remainingPlan!.queued), names.slice(256),
            "restored slice suffix preempted the unconsumed reviewed plan");
        check(owner.snapshot().urgent === 0, "exact sliced suffix entered the interactive overlay");
        owner.settled(new Set(remainingPlan!.queued.map(row => row.path)), [], true);
        check(await owner.next() === null && f.dirty.size === suffix.length,
            "exhausted reviewed plan consumed or lost its sliced suffix before rebuild");

        // A real callback is observable even when journal persistence fails
        // and coarse metadata exactly matches the restored row. It inherits
        // only the scalar watermark, never the old owner's ACK authority.
        const sameScalarPath = suffix[0].path;
        f.dirty.add(hint(sameScalarPath));
        const urgent = await owner.next();
        same(paths(urgent!.queued), [sameScalarPath],
            "same-scalar callback was confused with the invisible exact restore");
        f.dirty.restore(urgent!.queued);
        check(f.dirty.commitRetirement(f.dirty.captureRetirement([
            { path: sameScalarPath, throughId: suffix[0].journalId! },
        ])) === 0, "same-scalar callback inherited the restored row's journal authority");
    } finally { owner.dispose(); }
}

async function bulkPriorityAndUrgentActiveFirstStayDistinct() {
    const f = fixture(["a.md", "b.md", "c.md"]); f.ports.sort = values => [...values].reverse();
    const owner = await RootReviewedQueue.create(f.ports);
    try {
        const bulk = await owner.next();
        same(paths(bulk!.queued), ["c.md", "b.md", "a.md"], "bulk selected hints lost reviewed source priority");
        same(paths(bulk!.ready), ["c.md", "b.md", "a.md"], "bulk rematerialization returned dirty insertion order");
        f.dirty.restore(bulk!.queued);
        check(f.dirty.commitRetirement(f.dirty.captureRetirement([
            { path: "a.md", throughId: 1 }, { path: "b.md", throughId: 2 }, { path: "c.md", throughId: 3 },
        ])) === 3, "bounded priority reorder discarded original claim proofs");
    } finally { owner.dispose(); }
    const urgent = fixture(["a.md", "b.md", "c.md"]); urgent.ports.sort = values => [...values].reverse();
    const active = await RootReviewedQueue.create(urgent.ports); urgent.active = "b.md";
    for (const [index, path] of ["a.md", "b.md", "c.md"].entries()) {
        urgent.states.set(path, { mtime: 2, size: 2 }); urgent.dirty.add(hint(path, 2, 2), index + 4);
    }
    try {
        const selected = await active.next();
        same(paths(selected!.ready), ["b.md", "a.md", "c.md"], "bulk sort overrode urgent active-note-first order");
        urgent.dirty.restore(selected!.queued);
    } finally { active.dispose(); }
}

async function rebuiltQueuePromotesExactRecentUpsertsBeforeStaticPriority() {
    const f = fixture(["old-a.md", "old-b.md", "live-a.md", "live-b.md"]);
    f.ports.sort = values => [...values].reverse();
    f.ports.recentUpsert = row => row.path.startsWith("live-") &&
        row.journalId === (row.path === "live-a.md" ? 3 : 4);
    const owner = await RootReviewedQueue.create(f.ports);
    try {
        const selected = await owner.next();
        same(paths(selected!.queued), ["live-a.md", "live-b.md"],
            "review rebuild demoted exact recent upserts into the static priority tail");
        check(selected!.ready.every(change => change.action === "modified"),
            "recent scheduling provenance bypassed ordinary source classification");
        f.dirty.restore(selected!.queued);
    } finally { owner.dispose(); }
}

async function initialReviewPreemptionBuildsOnlyAReviewedRecentPrefix() {
    const names = Array.from({ length: 300 }, (_, index) => `bulk-${index}.md`);
    const f = fixture(names), recent = "live.md";
    let injected = false, preempt = false;
    const materializedSizes: number[] = [];
    f.ports.recentUpsert = row => row.path === recent && row.journalId === 301;
    f.ports.recentUpsertPaths = () => [recent];
    f.ports.preemptInitialReview = () => preempt;
    f.ports.authorizeRecentPrefix = () => true;
    f.override = async (hints, omissions, afterCooperate) => {
        materializedSizes.push(hints.length);
        if (hints.length > 256) {
            await f.normal(hints.slice(0, 256), omissions);
            check(!injected, "initial materialization crossed its first cooperative boundary twice");
            injected = true;
            f.tracked.add(recent); f.states.set(recent, { mtime: 2, size: 2 });
            f.dirty.add(hint(recent, 2, 2), 301); preempt = true;
            afterCooperate?.();
        }
        return f.normal(hints, omissions);
    };
    let authority: RootReviewPreempted | undefined;
    try { await RootReviewedQueue.create(f.ports); }
    catch (error) { if (error instanceof RootReviewPreempted) authority = error; else throw error; }
    check(authority !== undefined, "completed recent upsert did not interrupt the authority-free whole review");
    check(f.reviews.length === 0 && f.dirty.size === 301 && materializedSizes[0] === 300,
        "preempted whole review classified, reviewed or claimed partial work");

    const prefix = await RootReviewedQueue.createRecentPrefix(f.ports, authority!);
    try {
        same(materializedSizes, [300, 1], "recent prefix rematerialized the whole dirty backlog");
        same(f.reviews, [[1, 0]], "recent prefix inherited whole-review or deletion authority");
        const selected = await prefix.next();
        same(paths(selected!.queued), [recent], "recent prefix selected unrelated bulk work");
        check(selected!.ready[0].action === "modified" && selected!.queued[0].journalId === 301,
            "recent prefix bypassed exact source classification or durable generation ownership");
        prefix.settled(new Set([recent]), [], true);
        await rejects(prefix.next(), error => error instanceof RootReviewInvalidated &&
            error.reason === "recent upsert prefix exhausted; full review required",
        "one-shot recent prefix falsely exhausted the unreviewed bulk backlog");
        check(f.dirty.size === 300 && [...f.dirty.iterate()].every(row => row.path.startsWith("bulk-")),
            "recent prefix consumed or changed unreviewed bulk work");
    } finally { prefix.dispose(); }
}

async function recentPrefixAuthorityIsOneShotAndPolicyBound() {
    const names = Array.from({ length: 300 }, (_, index) => `policy-${index}.md`);
    const f = fixture(names);
    f.ports.preemptInitialReview = () => true;
    let allowed = false;
    f.ports.authorizeRecentPrefix = () => allowed;
    let authority: RootReviewPreempted | undefined;
    try { await RootReviewedQueue.create(f.ports); }
    catch (error) { if (error instanceof RootReviewPreempted) authority = error; else throw error; }
    check(authority !== undefined, "fixture did not issue recent-prefix authority");
    await rejects(RootReviewedQueue.createRecentPrefix(f.ports, authority!), error =>
        error instanceof RootReviewInvalidated && error.reason === "recent upsert prefix policy changed",
    "recent prefix did not re-check policy against its own full dirty capture");
    allowed = true;
    await rejects(RootReviewedQueue.createRecentPrefix(f.ports, authority!), error =>
        error instanceof RootReviewInvalidated && error.reason === "recent upsert prefix lacks one-shot authority",
    "consumed preemption authority was reusable after a failed policy check");
    check(f.reviews.length === 0 && f.dirty.size === names.length,
        "failed recent-prefix authority changed review or dirty state");
}

async function recentPrefixWithholdsDeleteLinkSharedIdAndCoolingRows() {
    const special = ["safe.md", "missing.md", "linked-a.md", "linked-b.md",
        "shared-a.md", "shared-b.md", "cool.md"];
    const names = [...special, ...Array.from({ length: 293 }, (_, index) => `bulk-${index}.md`)];
    const f = fixture(names);
    f.states.set("missing.md", null);
    f.deferred.registerLegacyRename("linked-a.md", "linked-b.md", 1);
    f.dirty.add(hint("shared-a.md"), 500); f.dirty.add(hint("shared-b.md"), 500);
    const cooled = f.dirty.capture().get("cool.md")!;
    f.deferred.settle([cooled], await f.normal([cooled], []), [{ path: "cool.md", reason: "source-too-large",
        requiredBytes: 2, capacityBytes: 1 }], "fixture", Date.now());
    f.ports.recentUpsert = row => special.includes(row.path);
    f.ports.recentUpsertPaths = () => special;
    f.ports.preemptInitialReview = () => true;
    f.ports.authorizeRecentPrefix = () => true;
    let authority: RootReviewPreempted | undefined;
    try { await RootReviewedQueue.create(f.ports); }
    catch (error) { if (error instanceof RootReviewPreempted) authority = error; else throw error; }
    check(authority !== undefined, "fixture did not issue structural prefix authority");
    const prefix = await RootReviewedQueue.createRecentPrefix(f.ports, authority!);
    try {
        same(f.reviews, [[2, 0]], "recent prefix counted or authorized structural/deletion rows");
        const selected = await prefix.next();
        same(paths(selected!.queued), ["safe.md"],
            "recent prefix admitted a delete, rename link, shared journal ID or cooling source");
        prefix.settled(new Set(["safe.md"]), [], true);
        await rejects(prefix.next(), /full review required/,
            "structural rows escaped the mandatory whole rebuild after the safe prefix");
        check(f.dirty.size === names.length - 1 && special.slice(1).every(path => f.dirty.has(path)),
            "recent prefix dropped a withheld structural or cooling row");
    } finally { prefix.dispose(); }
}

async function recentPrefixNeverAdmitsPostCaptureOverlayRows() {
    const names = ["safe.md", ...Array.from({ length: 299 }, (_, index) => `bulk-${index}.md`)];
    const f = fixture(names);
    f.ports.recentUpsert = row => row.path === "safe.md";
    f.ports.recentUpsertPaths = () => ["safe.md"];
    f.ports.preemptInitialReview = () => true;
    f.ports.authorizeRecentPrefix = () => true;
    let authority: RootReviewPreempted | undefined;
    try { await RootReviewedQueue.create(f.ports); }
    catch (error) { if (error instanceof RootReviewPreempted) authority = error; else throw error; }
    const prefix = await RootReviewedQueue.createRecentPrefix(f.ports, authority!);
    try {
        f.tracked.add("late.md"); f.states.set("late.md", { mtime: 2, size: 2 });
        f.dirty.add(hint("late.md", 2, 2), 301);
        await rejects(prefix.next(), error => error instanceof RootReviewInvalidated &&
            error.reason === "recent upsert prefix observed a newer dirty generation",
        "post-capture overlay row inherited recent-prefix authority");
        check(f.dirty.size === names.length + 1 && f.dirty.has("safe.md") && f.dirty.has("late.md"),
            "post-capture prefix invalidation claimed or dropped dirty work");
    } finally { prefix.dispose(); }
}

async function inFlightActiveUrgentWaitsForItsDurableGeneration() {
    const f = fixture(["active.md", "peer.md"]);
    f.active = "active.md";
    f.inFlight.add("active.md");
    f.ports.recentUpsert = row => row.path === "peer.md";
    const owner = await RootReviewedQueue.create(f.ports);
    try {
        const peer = await owner.next();
        same(paths(peer!.queued), ["peer.md"],
            "in-flight active callback displaced a completed urgent peer");
        check(owner.snapshot().urgent === 0 && f.dirty.has("active.md"),
            "peer selection claimed the held static generation");
        owner.settled(new Set(["peer.md"]), [], true);

        const calls = f.calls;
        check(await owner.next() === null && f.calls === calls,
            "static plan claimed or rematerialized an in-flight reviewed upsert");
        check(owner.snapshot().urgent === 1 && f.dirty.has("active.md"),
            "static plan failed to retain bounded urgency for the held upsert");

        f.inFlight.clear();
        const active = await owner.next();
        same(paths(active!.queued), ["active.md"],
            "completed active callback did not regain its reviewed scheduling turn");
        check(active!.queued[0].journalId === 1,
            "callback fence changed the urgent row's journal generation");
    } finally { owner.dispose(); }
}

async function inFlightStructuralAndCoolingComponentsFailClosed() {
    const deletion = fixture(["delete.md"]);
    deletion.states.set("delete.md", null);
    deletion.dirty.add({ path: "delete.md", action: "deleted" }, 2);
    deletion.inFlight.add("delete.md");
    const deletionOwner = await RootReviewedQueue.create(deletion.ports);
    try {
        await rejects(deletionOwner.next(), error => error instanceof RootReviewInvalidated &&
            error.reason === "in-flight reviewed component changed",
        "in-flight replacement allowed a reviewed deletion component to proceed");
        check(deletion.dirty.has("delete.md"), "failed-closed deletion lost its dirty owner");
    } finally { deletionOwner.dispose(); }

    const linked = fixture(["old.md", "new.md"]);
    linked.deferred.registerLegacyRename("old.md", "new.md", 1);
    linked.inFlight.add("old.md");
    const linkedOwner = await RootReviewedQueue.create(linked.ports);
    try {
        await rejects(linkedOwner.next(), error => error instanceof RootReviewInvalidated &&
            error.reason === "in-flight reviewed component changed",
        "in-flight replacement split or selected a reviewed rename component");
        check(linked.dirty.size === 2, "failed-closed rename lost a dirty endpoint");
    } finally { linkedOwner.dispose(); }

    const cooling = fixture(["large.md"]), queued = [...cooling.dirty.iterate()];
    cooling.ports.retryDeferred = true;
    cooling.deferred.settle(queued, await cooling.normal(queued, []), [
        { path: "large.md", reason: "source-too-large", requiredBytes: 100, capacityBytes: 10 },
    ], "fixture", Date.now());
    cooling.inFlight.add("large.md");
    const coolingOwner = await RootReviewedQueue.create(cooling.ports);
    try {
        await rejects(coolingOwner.next(), error => error instanceof RootReviewInvalidated &&
            error.reason === "in-flight reviewed component changed",
        "in-flight replacement consumed a cooled source retry");
        check(cooling.dirty.has("large.md"), "failed-closed cooling lost its dirty source");
    } finally { coolingOwner.dispose(); }
}

async function failedIndependentSelectionsRetainOnlyFreshBoundedGenerations() {
    const f = fixture(["a.md", "b.md", "c.md"]), owner = await RootReviewedQueue.create(f.ports);
    f.active = "b.md";
    try {
        const first = await owner.next(); f.dirty.restore(first!.queued);
        check(owner.retainFailedIndependentSelection(first!.queued), "first source drift did not retain its reviewed selection");
        check(!owner.retainFailedIndependentSelection(first!.queued), "same reviewed generation received two drift retries");

        // The event precedes next(), exactly like a callback captured during
        // the failed source read. Overlay refresh replaces only that row.
        f.states.set("b.md", { mtime: 2, size: 2 }); f.dirty.add(hint("b.md", 2, 2), 4);
        const second = await owner.next();
        same(paths(second!.queued), ["b.md", "a.md", "c.md"], "retained selection lost active-path priority");
        check(second!.queued[0].journalId === 4, "retained retry selected the superseded active generation");
        f.dirty.restore(second!.queued);
        check(owner.retainFailedIndependentSelection(second!.queued), "new reviewed generation did not receive its finite retry");
        check(!owner.retainFailedIndependentSelection(second!.queued), "new reviewed generation received a duplicate retry");
    } finally { owner.dispose(); }

    const structural = fixture(["old.md", "new.md"]), structuralOwner = await RootReviewedQueue.create(structural.ports);
    try {
        const selected = await structuralOwner.next(); structural.dirty.restore(selected!.queued);
        const before = structuralOwner.snapshot(); structural.deferred.registerLegacyRename("old.md", "new.md", 1);
        let failure: unknown;
        try { structuralOwner.retainFailedIndependentSelection(selected!.queued); } catch (error) { failure = error; }
        check(failure instanceof RootReviewInvalidated, "new rename dependency retained an old independent selection");
        check(structuralOwner.snapshot().urgent === before.urgent && structural.dirty.size === 2,
            "failed structural retention mutated urgent state or pending proofs");
    } finally { structuralOwner.dispose(); }

    const cooled = fixture(["large.md"]); cooled.ports.retryDeferred = true;
    const queued = [...cooled.dirty.iterate()];
    cooled.deferred.settle(queued, await cooled.normal(queued, []), [
        { path: "large.md", reason: "source-too-large", requiredBytes: 100, capacityBytes: 10 },
    ], "fixture", Date.now());
    const cooledOwner = await RootReviewedQueue.create(cooled.ports);
    try {
        const selected = await cooledOwner.next(); check(selected?.queued.length === 1, "forced cooled source was not selected");
        cooled.dirty.restore(selected!.queued);
        check(!cooledOwner.retainFailedIndependentSelection(selected!.queued),
            "source drift recreated a consumed explicit cooldown retry");
    } finally { cooledOwner.dispose(); }
}

async function laterComponentFailureRestoresEarlierClaims() {
    for (const newerNoID of [false, true]) {
        const f = fixture(["a.md", "b.md", "c.md"]), owner = await RootReviewedQueue.create(f.ports);
        const failure = new Error("synthetic second claim failure"), original = f.dirty.claimHints.bind(f.dirty);
        let calls = 0;
        f.dirty.claimHints = hints => {
            if (++calls === 2) {
                if (newerNoID) f.dirty.add(hint("a.md", 2, 2));
                throw failure;
            }
            return original(hints);
        };
        try {
            await rejects(owner.next(), error => error === failure, "later component failure was swallowed");
            check(calls === 2 && f.dirty.size === 3, "later claim failure lost an already-claimed dirty hint");
            check(f.dirty.capture().get("a.md")!.mtime === (newerNoID ? 2 : 1),
                "restoring an earlier claim overwrote a newer live observation");
            const retired = f.dirty.commitRetirement(f.dirty.captureRetirement([
                { path: "a.md", throughId: 1 }, { path: "b.md", throughId: 2 }, { path: "c.md", throughId: 3 },
            ]));
            check(retired === (newerNoID ? 2 : 3), "exception restoration lost original proofs or granted an old ID to new no-ID work");
        } finally { owner.dispose(); }
    }
}

async function closeAndScopeFailureRestoreActualClaimedCut() {
    const f = fixture(), owner = await RootReviewedQueue.create(f.ports);
    let enter!: () => void, finish!: () => void;
    const entered = new Promise<void>(resolve => { enter = resolve; }), pending = new Promise<void>(resolve => { finish = resolve; });
    f.override = async (hints, omissions) => { enter(); await pending; return f.normal(hints, omissions); };
    const selection = owner.next(); await entered;
    check(f.dirty.size === 0, "test did not reach an actual claimed materialization");
    owner.dispose(); f.dirty.add(hint("a.md", 3, 3), 3); finish();
    await rejects(selection, /disposed owner/, "disposed queue returned claimed work after materialization");
    check(f.dirty.size === 2 && f.dirty.capture().get("a.md")!.journalId === 3 && f.dirty.capture().get("b.md")!.journalId === 2,
        "dispose tail overwrote newer live work or lost an older claimed peer");
    const scope = fixture(), scopeOwner = await RootReviewedQueue.create(scope.ports);
    scope.override = async (hints, omissions) => { const result = await scope.normal(hints, omissions); scope.current = false; return result; };
    try {
        await rejects(scopeOwner.next(), /fixture scope changed/, "stale scope returned a valid selection");
        check(scope.dirty.size === 2, "scope failure failed to restore claimed paths");
    } finally { scopeOwner.dispose(); }
}

async function sharedReviewAndPlanAdmissionOwnFullLifetime() {
    const denied = fixture(["guarded.md"]);
    denied.ports.memoryArbiter = new SyncMemoryArbiter({ capacityBytes: ROOT_REVIEW_LIMITS.metadataBytes - 1 });
    let captures = 0;
    const capture = denied.dirty.captureTracking.bind(denied.dirty);
    denied.dirty.captureTracking = limit => { captures++; return capture(limit); };
    await rejects(RootReviewedQueue.create(denied.ports), /exceeds capacity/,
        "review admission denial was not propagated for deterministic retry");
    same(captures, 0, "review tracking/maps were allocated before shared admission");

    const nested = fixture(["nested.md"]);
    const nestedArbiter = new SyncMemoryArbiter({ capacityBytes: ROOT_BATCH_PLAN_ADMISSION.maxMetadataBytes });
    nested.ports.memoryArbiter = nestedArbiter;
    const marker = Symbol("timeout"); let timer!: ReturnType<typeof setTimeout>;
    const creation = RootReviewedQueue.create(nested.ports);
    const outcome = await Promise.race([creation.then(() => null, error => error),
        new Promise<typeof marker>(resolve => { timer = setTimeout(() => resolve(marker), 250); })]);
    clearTimeout(timer);
    if (outcome === marker) {
        nested.controller.abort(); await assert.rejects(creation);
        assert.fail("nested plan waited forever on its own review owner's lease");
    }
    check(outcome instanceof Error && /currently unavailable/.test(outcome.message),
        "nested plan did not fail fast with bounded retry semantics");
    same(nestedArbiter.snapshot().usedBytes, 0, "failed nested plan retained its review lease");

    const live = fixture(["owned.md"]);
    const capacity = ROOT_REVIEW_LIMITS.metadataBytes + ROOT_BATCH_PLAN_ADMISSION.maxMetadataBytes;
    const arbiter = new SyncMemoryArbiter({ capacityBytes: capacity });
    live.ports.memoryArbiter = arbiter;
    const owner = await RootReviewedQueue.create(live.ports);
    same(arbiter.snapshot().owners["root-review"].usedBytes, ROOT_REVIEW_LIMITS.metadataBytes,
        "review owner did not retain its full model lease");
    same(arbiter.snapshot().owners["root-plan"].usedBytes, ROOT_BATCH_PLAN_ADMISSION.maxMetadataBytes,
        "nested plan did not share the same aggregate ceiling");
    same(arbiter.snapshot().usedBytes, capacity, "review and plan leases were not co-accounted");
    owner.dispose(); owner.dispose();
    same(arbiter.snapshot().usedBytes, 0, "review disposal did not release nested owners exactly once");
}

let completed = false;
process.once("beforeExit", () => { if (!completed) { console.error("root-reviewed-queue tests did not finish"); process.exitCode = 1; } });
void (async () => {
    await planningRevisionFencesPartitionAndPreparationTail();
    await dependencyCaptureStaysInsidePreparationFence();
    await dependencyCaptureAdmissionFailsClosed();
    await prioritySortStaysInsidePreparationFence();
    await lateEditsAreNotFalseExhaustion(); await lateEditsAfterSkippedAndExhaustedPlan(); await everyPathRequiresExactlyOneClassification();
    await classificationDriftAndIOAreNotDeletesOrOmissions(); await positiveLocalOmissionsAndGroups();
    await successfulCoolingSourceRebuildsHeldDeletes(); await partialCoolingAndDetachedSnapshot(); await closeAndScopeFailureRestoreActualClaimedCut();
    await forcedRetriesSurviveLocalAndPrefixBatches(); await forcedCoolingDeletesRequireWholeAtomicCandidate();
    await freshStatEligibilitySurvivesOldHintCooldown();
    await cooledPositiveOmissionsNeedNoSourceRetry(); await freshOverlayEligibilityNeverGrantsNewJournalAuthority();
    await slicedSuffixRestoreIsInvisibleButRealSameScalarEventIsUrgent();
    await bulkPriorityAndUrgentActiveFirstStayDistinct();
    await rebuiltQueuePromotesExactRecentUpsertsBeforeStaticPriority();
    await initialReviewPreemptionBuildsOnlyAReviewedRecentPrefix();
    await recentPrefixAuthorityIsOneShotAndPolicyBound();
    await recentPrefixWithholdsDeleteLinkSharedIdAndCoolingRows();
    await recentPrefixNeverAdmitsPostCaptureOverlayRows();
    await inFlightActiveUrgentWaitsForItsDurableGeneration();
    await inFlightStructuralAndCoolingComponentsFailClosed();
    await failedIndependentSelectionsRetainOnlyFreshBoundedGenerations();
    await laterComponentFailureRestoresEarlierClaims();
    await sharedReviewAndPlanAdmissionOwnFullLifetime();
    completed = true; console.log(`root-reviewed-queue: ${assertions} assertions passed`);
})().catch(error => { console.error(error); process.exitCode = 1; });
