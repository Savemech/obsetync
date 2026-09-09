import { strict as assert } from "node:assert";
import { setImmediate, clearImmediate } from "node:timers";
import { fixture, hashFor, pathAt, gate, observe, turns, type Fixture } from "./engine-root-test-fixture";
import { transientMemorySnapshot } from "./transient-memory";

type EnginePort = Awaited<ReturnType<Fixture["create"]>>;

async function prepared(count = 1) {
    const f = await fixture(count), a = await f.create("cache");
    // Resolve initial recovery before placing fault gates so each test
    // isolates the cache tail of its accepted root.
    await a.engine.recoverRootBeforeWork();
    assert.equal(a.treeState.activeJob, null);
    assert.equal(transientMemorySnapshot().usedBytes, 0);
    return { f, a };
}

async function accepted(f: Fixture, a: EnginePort) {
    assert.equal(f.server.requests.length, 1);
    assert.equal(f.server.legacy, 0);
    assert.equal(a.treeState.commits, 1);
    assert.equal(a.treeState.aborts, 0);
    assert.equal(a.treeHashAt(pathAt(0)), hashFor(2));
    assert.equal(f.base.getHash(pathAt(0)), hashFor(2));
    assert.equal(f.journal.unsyncedCount(), 0);
    assert.equal(a.engine.pendingChanges.size, 0);
    const cold = await f.cold();
    assert.equal(cold.sequence, 1); assert.equal(cold.pending, null);
    assert.equal(cold.base.treeBaseRoot, f.server.root);
    assert.equal(cold.base.getHash(pathAt(0)), hashFor(2));
    assert.equal(cold.journal.unsyncedCount(), 0);
    assert.equal(a.treeState.activeJob, null);
    assert.equal(transientMemorySnapshot().usedBytes, 0);
}

async function exactPagedExportFinishesBeforeCacheWrite() {
    const { f, a } = await prepared(600);
    const rootBytes = a.tree.root_bytes;
    const eventsCut = f.events.length;
    let written: Uint8Array | undefined, expected: Uint8Array | undefined;
    let jobAtWrite: unknown = "not called", finishesAtWrite = 0, chargedAtWrite = 0;
    let committedReadsAtBegin = -1, committedFinishesAtBegin = -1;
    const admissions: number[] = [], planCharges: number[] = [];
    try {
        a.tree.root_bytes = () => { throw new Error("committed cache used the whole-root compatibility getter"); };
        f.control.rootExport = event => {
            if (event.scope !== "committed") return;
            if (event.phase === "begin") {
                committedReadsAtBegin = a.treeState.rootReads;
                committedFinishesAtBegin = a.treeState.rootFinishes;
            }
            if (event.phase === "plan") planCharges.push(transientMemorySnapshot().usedBytes);
            if (event.phase === "admit") admissions.push(transientMemorySnapshot().usedBytes);
        };
        f.control.cache = bytes => {
            written = new Uint8Array(bytes).slice(); expected = rootBytes();
            jobAtWrite = a.treeState.activeJob; finishesAtWrite = a.treeState.rootFinishes;
            chargedAtWrite = transientMemorySnapshot().usedBytes;
        };
        await f.queue(a.engine, [pathAt(0)], 2); await a.engine.pushPending();
        assert(written && expected); assert(written.length > 65536, "fixture did not reach a second export page");
        assert.deepEqual(written, expected); assert.deepEqual(f.cached(), expected);
        assert(committedReadsAtBegin >= 0 && committedFinishesAtBegin >= 0);
        assert.equal(a.treeState.rootReads - committedReadsAtBegin, Math.ceil(expected.length / 65536));
        assert.equal(finishesAtWrite, committedFinishesAtBegin + 1); assert.equal(jobAtWrite, null);
        assert(chargedAtWrite > written.length); assert.equal(planCharges.length, 1);
        assert.equal(admissions.length, 1);
        assert(admissions[0] > planCharges[0], "build started outside shared admission");
        const events = f.events.slice(eventsCut);
        for (const prior of ["terminal-write", "base-publication", "journal-ack", "cache:candidate-commit"]) {
            assert(events.indexOf(prior) >= 0 && events.indexOf(prior) < events.indexOf("cache:committed-root-export-begin"),
                `${prior} must precede committed cache export`);
        }
        assert(events.indexOf("cache:committed-root-export-finished") < events.indexOf("cache-write"));
        await accepted(f, a);
    } finally { f.control.cache = undefined; f.control.rootExport = undefined; await f.close(); }
}

async function actualHostTaskRunsBetweenBuildSteps() {
    const { f, a } = await prepared();
    let taskRan = false, sawNextStep = false, taskSeenByNextStep = false;
    let task: ReturnType<typeof setImmediate> | undefined;
    try {
        f.control.rootExport = event => {
            if (event.scope !== "committed") return;
            // The actual Node scheduler uses setImmediate. Queue on that same
            // FIFO host source, without assuming timer-vs-check phase ordering.
            if (event.phase === "build" && event.completed === 0) task = setImmediate(() => { taskRan = true; });
            if (event.phase === "build" && event.completed === 1) { sawNextStep = true; taskSeenByNextStep = taskRan; }
        };
        await f.queue(a.engine, [pathAt(0)], 2); await a.engine.pushPending();
        assert(sawNextStep); assert(taskSeenByNextStep, "committed root steps yielded only microtasks, not a real host task");
        await accepted(f, a);
    } finally { if (task !== undefined) clearImmediate(task); f.control.rootExport = undefined; await f.close(); }
}

async function revisionWitnessAvoidsRepeatedRootHashesAndRejectsAnotherTree() {
    const { f, a } = await prepared(600);
    const before = { hashes: a.treeState.rootHashReads, exports: a.treeState.rootExports,
        builds: a.treeState.rootBuilds, cancels: a.treeState.rootCancels };
    const original = a.engine.tree, revision = a.tree.committed_revision;
    try {
        await a.engine.saveCachedRoot();
        assert.equal(a.treeState.rootExports, before.exports + 1);
        assert.equal(a.treeState.rootBuilds, before.builds + 1);
        assert.equal(a.treeState.rootHashReads, before.hashes + 1,
            "cache owner checks repeated the O(n) semantic root hash around awaits");

        let writes = 0, replaced = false;
        f.control.cache = () => { writes++; };
        f.control.rootExport = event => {
            if (event.scope !== "committed") return;
            if (!replaced && event.phase === "build" && event.completed === 0) {
                replaced = true;
                // Same methods/revision/hash on a different wrapper are not an
                // ownership witness for the in-flight native job.
                a.engine.tree = Object.create(original);
            }
        };
        await a.engine.saveCachedRoot();
        assert(replaced); assert.equal(writes, 0);
        assert.equal(a.treeState.rootCancels, before.cancels + 1);
        assert.equal(a.treeState.activeJob, null);
        a.engine.tree = original;

        const stableRevision = revision(); let invalidated = false;
        f.control.rootExport = event => {
            if (event.scope !== "committed") return;
            if (!invalidated && event.phase === "build" && event.completed === 0) {
                invalidated = true; a.tree.set_committed_revision_for_test(Number.NaN);
            }
        };
        await a.engine.saveCachedRoot();
        assert(invalidated); assert.equal(writes, 0);
        assert.equal(a.treeState.rootCancels, before.cancels + 2);
        assert.equal(a.treeState.activeJob, null);
        a.tree.set_committed_revision_for_test(stableRevision);

        const exports = a.treeState.rootExports;
        a.tree.committed_revision = () => Number.NaN;
        await a.engine.saveCachedRoot();
        assert.equal(a.treeState.rootExports, exports, "invalid revision getter did not fail before native begin");
        assert.equal(writes, 0);
        (a.tree as any).committed_revision = 7;
        await a.engine.saveCachedRoot();
        assert.equal(a.treeState.rootExports, exports, "non-callable revision getter did not fail before native begin");
        assert.equal(writes, 0);
    } finally {
        a.engine.tree = original; a.tree.committed_revision = revision;
        f.control.rootExport = undefined; f.control.cache = undefined; await f.close();
    }
}

async function writeLifetimeSurvivesStopUntilActualSettlement() {
    for (const failed of [false, true]) {
        const { f, a } = await prepared(), held = gate(), initialCache = f.cached();
        const expectedRootBytes = a.tree.root_bytes;
        let borrowed: ArrayBuffer | undefined, expected: Uint8Array | undefined;
        let finishedBeforeWrite = false;
        let push: ReturnType<typeof observe> | undefined, stopped: ReturnType<typeof observe> | undefined;
        try {
            f.control.cache = async bytes => {
                borrowed = bytes; expected = expectedRootBytes();
                finishedBeforeWrite = a.treeState.activeJob === null;
                await held.wait();
            };
            await f.queue(a.engine, [pathAt(0)], 2);
            push = observe(a.engine.pushPending()); await held.entered;
            assert(borrowed && expected); assert(finishedBeforeWrite);
            assert.deepEqual(new Uint8Array(borrowed), expected);
            assert.equal(borrowed.byteLength, expected.length, "cache write borrowed a larger unrelated backing buffer");
            assert.equal(f.base.getHash(pathAt(0)), hashFor(2)); assert.equal(f.journal.unsyncedCount(), 0);
            const charged = transientMemorySnapshot().usedBytes; assert(charged > expected.length);
            stopped = observe(a.engine.stopAndDrain()); await turns();
            assert.equal(push.state.settled, false); assert.equal(stopped.state.settled, false);
            assert.equal(transientMemorySnapshot().usedBytes, charged, "stop released a live native write's lease");
            assert.deepEqual(new Uint8Array(borrowed), expected, "stop detached or changed the borrowed bytes");
            if (failed) held.reject(new Error("held native cache write failed")); else held.resolve();
            await push.done; await stopped.done;
            assert.equal(push.state.error, undefined); assert.equal(stopped.state.error, undefined);
            assert.deepEqual(f.cached(), failed ? initialCache : expected);
            borrowed = undefined;
            await accepted(f, a);
        } finally {
            held.resolve(); await push?.done; await stopped?.done;
            borrowed = undefined; f.control.cache = undefined; await f.close();
        }
    }
}

async function exportErrorsCannotUndoAcceptedTail() {
    for (const failure of ["begin", "plan", "admit", "build", "read", "finish", "parse", "write"] as const) {
        const { f, a } = await prepared(), initialCache = f.cached();
        let committedBefore = { exports: -1, finishes: -1, cancels: -1 };
        let committedFinished = false, reached = 0, writes = 0;
        const parse = a.engine.wasm.wasm_root_hash_from_bytes;
        try {
            f.control.rootExport = event => {
                if (event.scope !== "committed") return;
                if (event.phase === "begin") committedBefore = {
                    exports: a.treeState.rootExports,
                    finishes: a.treeState.rootFinishes,
                    cancels: a.treeState.rootCancels,
                };
                if (event.phase === "finish") committedFinished = true;
                if (event.phase === failure) { reached++; throw new Error(`synthetic root export ${failure} failure`); }
            };
            f.control.cache = () => { writes++; if (failure === "write") { reached++; throw new Error("cache write failure"); } };
            if (failure === "parse") a.engine.wasm.wasm_root_hash_from_bytes = (bytes: Uint8Array) => {
                if (committedFinished) { reached++; throw new Error("cache parser failure"); }
                return parse(bytes);
            };
            await f.queue(a.engine, [pathAt(0)], 2); await a.engine.pushPending();
            assert.equal(reached, 1, `failure fixture never reached ${failure}`);
            assert.equal(writes, failure === "write" ? 1 : 0);
            assert.deepEqual(f.cached(), initialCache);
            assert(committedBefore.exports >= 0 && committedBefore.finishes >= 0 && committedBefore.cancels >= 0);
            assert.equal(a.treeState.rootExports - committedBefore.exports, failure === "begin" ? 0 : 1);
            assert.equal(a.treeState.rootFinishes - committedBefore.finishes, failure === "write" || failure === "parse" ? 1 : 0);
            assert.equal(a.treeState.rootCancels - committedBefore.cancels, ["admit", "read", "finish"].includes(failure) ? 1 : 0,
                "native step errors drop their job; other unfinished jobs require matching cancellation");
            assert.equal(a.engine.rootRepairRequired, false, "best-effort cache error recreated a settled root intent");
            await accepted(f, a);
        } finally { a.engine.wasm.wasm_root_hash_from_bytes = parse; f.control.rootExport = undefined; f.control.cache = undefined; await f.close(); }
    }
}

async function driftAndCancellationKeepAcceptedGenerations() {
    for (const failure of ["revision-drift", "legacy-hash-drift", "cancel"] as const) {
        const { f, a } = await prepared(), initialCache = f.cached();
        const rootHash = a.tree.root_hash_hex, revision = a.tree.committed_revision, cancels = a.treeState.rootCancels;
        let reached = false, writes = 0, newest: number | undefined, abortedAtCancellation: boolean | undefined;
        try {
            // A newer actual callback must survive the accepted old root even
            // when its cache export is stopped midway through admitted work.
            f.control.beforeRootAnswer = async () => {
                newest = await f.event(a.engine, pathAt(0), 3);
                if (failure === "legacy-hash-drift") (a.tree as any).committed_revision = undefined;
            };
            f.control.rootExport = event => {
                if (event.scope !== "committed") return;
                if (event.phase === "cancel") {
                    abortedAtCancellation = a.engine.hashWorkerAbort.signal.aborted;
                    // Drift must be detected by the owner fence, not masked
                    // by a simultaneous stop. Quiesce only once cleanup starts.
                    void a.engine.quiesceAndDrain(); return;
                }
                if (event.phase !== "build" || event.completed !== 0) return;
                reached = true;
                if (failure === "revision-drift") a.tree.bump_committed_revision_for_test();
                else if (failure === "legacy-hash-drift") a.tree.root_hash_hex = () => "f".repeat(64);
                else a.engine.hashWorkerAbort.abort(new Error("stop during cache export"));
            };
            f.control.cache = () => { writes++; };
            await f.queue(a.engine, [pathAt(0)], 2); await a.engine.pushPending();
            assert(reached); assert(newest !== undefined); assert.equal(writes, 0);
            assert.equal(abortedAtCancellation, failure === "cancel");
            assert.equal(a.treeState.rootCancels, cancels + 1); assert.equal(a.treeState.activeJob, null);
            assert.deepEqual(f.cached(), initialCache); assert.equal(f.server.requests.length, 1);
            assert.equal(f.base.getHash(pathAt(0)), hashFor(2)); assert.equal(a.treeHashAt(pathAt(0)), hashFor(2));
            assert.equal(a.engine.pendingChanges.size, 1); assert.equal(f.journal.unsynced()[0]?.id, newest);
            assert.equal(transientMemorySnapshot().usedBytes, 0);
            const cold = await f.cold(); assert.equal(cold.pending, null); assert.equal(cold.sequence, 1);
            assert.equal(cold.base.getHash(pathAt(0)), hashFor(2)); assert.equal(cold.journal.unsynced()[0]?.id, newest);
        } finally { a.tree.root_hash_hex = rootHash; a.tree.committed_revision = revision;
            f.control.rootExport = undefined; f.control.cache = undefined;
            f.control.beforeRootAnswer = undefined; await f.close(); }
    }
}

async function activeCandidateCannotBeWrittenAsCommittedCache() {
    const { f, a } = await prepared();
    try {
        await f.queue(a.engine, [pathAt(0)], 2); await a.engine.pushPending();
        const cached = f.cached(), exports = a.treeState.rootExports, cut = f.storage.events.length;
        let writes = 0; f.control.cache = () => { writes++; };
        a.tree.begin_candidate();
        await a.engine.saveCachedRoot();
        assert(a.tree.has_candidate()); assert.equal(a.treeState.rootExports, exports); assert.equal(writes, 0);
        assert.deepEqual(f.cached(), cached); assert.equal(f.storage.events.length, cut);
        assert.equal(f.server.requests.length, 1); assert.equal(f.journal.unsyncedCount(), 0);
        assert.equal(f.base.getHash(pathAt(0)), hashFor(2)); assert.equal(transientMemorySnapshot().usedBytes, 0);
        a.tree.abort_candidate();
    } finally { f.control.cache = undefined; await f.close(); }
}

async function recoveredAcceptedOutcomeUsesCommittedExporter() {
    for (const failedWrite of [false, true]) {
        const { f, a } = await prepared();
        const initialCache = f.cached();
        try {
            f.control.beforeRootAnswer = async () => { throw new Error("accepted response lost"); };
            await f.queue(a.engine, [pathAt(0)], 2); await a.engine.pushPending();
            assert.equal(a.engine.rootRepairRequired, true); assert.equal(f.journal.unsyncedCount(), 1);
            f.control.beforeRootAnswer = undefined;
            const finishes = a.treeState.rootFinishes, repairs = a.treeState.repairs;
            let writes = 0, finishedAtWrite = false;
            const expectedBytes = a.tree.root_bytes;
            a.tree.root_bytes = () => { throw new Error("recovery cache used legacy getter"); };
            f.control.cache = () => {
                writes++; finishedAtWrite = a.treeState.activeJob === null && a.treeState.rootFinishes === finishes + 1;
                if (failedWrite) throw new Error("recovered cache write failure");
            };
            await a.engine.recoverRootBeforeWork();
            assert.equal(writes, 1); assert(finishedAtWrite); assert.equal(f.server.queries, 1);
            assert.equal(f.server.requests.length, 1); assert.equal(a.engine.rootRepairRequired, false);
            assert.equal(a.treeState.repairs, repairs + 1); assert.equal(a.treeHashAt(pathAt(0)), hashFor(2));
            assert.equal(f.base.getHash(pathAt(0)), hashFor(2)); assert.equal(f.journal.unsyncedCount(), 0);
            assert.equal(a.engine.pendingChanges.size, 0);
            assert.deepEqual(f.cached(), failedWrite ? initialCache : expectedBytes());
            const cold = await f.cold(); assert.equal(cold.pending, null); assert.equal(cold.sequence, 1);
            assert.equal(cold.journal.unsyncedCount(), 0); assert.equal(cold.base.treeBaseRoot, f.server.root);
            assert.equal(a.treeState.activeJob, null); assert.equal(transientMemorySnapshot().usedBytes, 0);
        } finally { f.control.beforeRootAnswer = undefined; f.control.cache = undefined; await f.close(); }
    }
}

async function run() {
    const globals = globalThis as any;
    const oldNotice = globals.__obsetyncTestNotice, oldDebounce = globals.__obsetyncTestDebounce;
    const notice = () => ({ setMessage() {}, hide() {} }), debounce = () => () => {};
    globals.__obsetyncTestNotice = notice; globals.__obsetyncTestDebounce = debounce;
    try {
        await exactPagedExportFinishesBeforeCacheWrite();
        await actualHostTaskRunsBetweenBuildSteps();
        await revisionWitnessAvoidsRepeatedRootHashesAndRejectsAnotherTree();
        await writeLifetimeSurvivesStopUntilActualSettlement();
        await exportErrorsCannotUndoAcceptedTail();
        await driftAndCancellationKeepAcceptedGenerations();
        await activeCandidateCannotBeWrittenAsCommittedCache();
        await recoveredAcceptedOutcomeUsesCommittedExporter();
        console.log("cached-root-export.test: 8 actual-engine cache/export lifetime suites passed (synthetic tree/adapter/transport)");
    } finally {
        if (globals.__obsetyncTestNotice === notice) {
            if (oldNotice === undefined) delete globals.__obsetyncTestNotice; else globals.__obsetyncTestNotice = oldNotice;
        }
        if (globals.__obsetyncTestDebounce === debounce) {
            if (oldDebounce === undefined) delete globals.__obsetyncTestDebounce; else globals.__obsetyncTestDebounce = oldDebounce;
        }
    }
}

let completed = false;
process.once("beforeExit", () => { if (!completed) throw new Error("cached root integration test exited with an unsettled owner"); });
void run().then(() => { completed = true; }, error => { completed = true; setTimeout(() => { throw error; }, 0); });
