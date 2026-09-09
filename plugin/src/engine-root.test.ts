import { strict as assert } from "node:assert";
import { SYNC_BASE_STORE_PATH } from "./sync-base";
import { JOURNAL_STORE_PATH } from "./journal";
import { ROOT_INTENT_PATH } from "./root-intent";
import { PathIsDirectoryError } from "./file-safety";
import { RootTreeResidentAdmission } from "./root-tree-resident-admission";
import { TREE_CANDIDATE_MUTATION_STEP_UNITS } from "./tree-candidate-mutation-job";
import { fixture, hashFor, pathAt, gate, rejection, turns, observe, rows, type Fixture } from "./engine-root-test-fixture";

async function boundedPublicationsPreserveUnselectedAndNewEvents() {
    const f = await fixture(600), a = await f.create("batch");
    const roots = [gate(), gate(), gate(), gate(), gate()];
    try {
        await f.queue(a.engine, Array.from({ length: 600 }, (_, index) => pathAt(index)), 2);
        f.control.beforeRootAnswer = () => roots[f.server.requests.length - 1].wait();
        const cut = f.storage.events.length;
        const push = observe(a.engine.pushPending()); await roots[0].entered;
        assert.equal(f.server.publicationSizes[0], 256);
        assert.equal(a.engine.pendingChanges.size, 344, "unselected rows were detached past bounded publication");
        assert.equal(f.journal.unsyncedCount(), 600, "unselected or not-yet-ACKed journal rows disappeared");
        const newest = await f.event(a.engine, pathAt(0), 3);
        let hostTaskRan = false;
        setTimeout(() => { hostTaskRan = true; }, 0);
        roots[0].resolve(); await roots[1].entered;
        assert.equal(hostTaskRan, true, "manual root drain did not yield a host task between bounded publications");
        assert.equal(push.state.settled, false, "one manual push abandoned the remaining roots");
        assert.deepEqual(f.server.publicationSizes, [256, 32],
            "fresh work did not stop the next bulk root at its ACK-safe boundary");
        // The selected 256-path owner retains its 224-path continuation until
        // the 32-path root settles; WAL still proves all 345 unacknowledged
        // generations while only the unclaimed 89 remain in pendingChanges.
        assert.equal(f.journal.unsyncedCount(), 345); assert.equal(a.engine.pendingChanges.size, 89);
        assert(!f.server.cuts[1].some(cut => cut.path === pathAt(0)),
            "preempted bulk root consumed the pending urgent generation");
        assert.equal(f.journal.unsynced().find(row => row.path === pathAt(0))?.id, newest);
        roots[1].resolve(); await roots[2].entered;
        assert.equal(push.state.settled, false);
        assert.equal(f.journal.unsyncedCount(), 313); assert.equal(a.engine.pendingChanges.size, 312);
        assert.deepEqual(f.server.cuts[2], [{ path: pathAt(0), throughId: newest }],
            "urgent root did not publish exactly the newest callback generation");
        assert.equal(f.journal.unsynced().find(row => row.path === pathAt(0))?.id, newest);
        roots[2].resolve(); await roots[3].entered;
        assert.equal(push.state.settled, false);
        assert.equal(f.journal.unsyncedCount(), 312); assert.equal(a.engine.pendingChanges.size, 224);
        assert(!f.journal.unsynced().some(row => row.path === pathAt(0)), "urgent accepted generation was not ACKed");
        assert.equal(f.base.getHash(pathAt(0)), hashFor(3));
        roots[3].resolve(); await roots[4].entered;
        assert.equal(push.state.settled, false);
        assert.equal(f.journal.unsyncedCount(), 224); assert.equal(a.engine.pendingChanges.size, 0);
        roots[4].resolve(); await push.done; f.control.beforeRootAnswer = undefined;
        assert.equal(push.state.error, undefined);
        assert.deepEqual(f.server.publicationSizes, [256, 32, 1, 88, 224]);
        assert(f.server.cuts.every(cuts => cuts.length <= 256));
        assert.equal(f.server.legacy, 0); assert.equal(f.journal.unsyncedCount(), 0); assert.equal(a.engine.pendingChanges.size, 0);
        assert.equal(f.base.getHash(pathAt(0)), hashFor(3)); assert.equal(f.server.entries.get(pathAt(0))?.hash, hashFor(3));
        assert.equal(f.base.entryCount(), 600);
        const baseWrites = f.storage.events.slice(cut);
        const headPublishes = baseWrites.filter(event => event.method === "rename" && event.phase === "after" &&
            event.path === `${SYNC_BASE_STORE_PATH}/head.json.next` && event.to === `${SYNC_BASE_STORE_PATH}/head.json`);
        assert.equal(headPublishes.length, 5, "durable root publication used split base/root/timestamp saves");
        const operations = baseWrites.flatMap(event => rows(event, SYNC_BASE_STORE_PATH));
        assert.equal(operations.filter(row => row.op === "root-publication").length, 5);
        assert(!operations.some(row => row.op === "tree-root" || row.op === "timestamp"), "legacy base bookkeeping ran alongside root settlement");
        const cold = await f.cold(); assert.equal(cold.sequence, 5); assert.equal(cold.pending, null);
        assert.equal(cold.journal.unsyncedCount(), 0); assert.equal(cold.base.treeBaseRoot, f.server.root);
    } finally { for (const root of roots) root.resolve(); await f.close(); }
}

async function loseAccepted(f: Fixture, engine: any) {
    await f.queue(engine, [pathAt(0)], 2);
    const lost = new Error("accepted root response lost");
    f.control.beforeRootAnswer = async () => { throw lost; };
    await engine.pushPending(); f.control.beforeRootAnswer = undefined;
    assert.equal(engine.rootRepairRequired, true); assert.equal(engine.rootRuntime.pending()?.terminal, null);
    assert.equal(f.base.getHash(pathAt(0)), hashFor(1)); assert.equal(f.journal.unsyncedCount(), 1);
}

async function everyOrdinaryEntryWaitsForOutcomeRecovery() {
    for (const kind of ["pull", "reconcile", "full-scan", "metadata"] as const) {
        const f = await fixture(), a = await f.create(kind), query = gate();
        try {
            await loseAccepted(f, a.engine); const cut = f.events.length;
            f.control.beforeQueryAnswer = query.wait;
            const body = new Error("ordinary body reached after verified recovery");
            if (kind === "metadata") f.control.statBulk = () => { throw body; };
            else f.control.checkpoint = phase => { if (phase === kind) throw body; };
            const pending = observe(kind === "pull" ? a.engine.pullRemote() : kind === "reconcile"
                ? a.engine.reconcileContent() : kind === "full-scan" ? a.engine.fullScan() : a.engine.partialMtimeScan());
            await query.entered; await turns();
            assert.equal(pending.state.settled, false);
            assert(!f.events.slice(cut).some(event => event.includes(":body:") || event.endsWith(":stat-bulk") || event.endsWith(":ordinary-diff")),
                `${kind} crossed unresolved accepted outcome`);
            assert.equal(f.journal.unsyncedCount(), 1); assert.equal(a.treeState.repairs, 0);
            query.resolve(); await pending.done;
            assert.equal(f.server.requests.length, 1, `${kind} blindly resent the accepted root`);
            assert.equal(f.server.queries, 1); assert.equal(a.treeState.repairs, 1);
            assert.equal(a.treeHashAt(pathAt(0)), hashFor(2)); assert.equal(a.engine.pendingChanges.size, 0);
            assert.equal(f.journal.unsyncedCount(), 0); assert.equal(a.engine.rootRuntime.pending(), null);
            const events = f.events.slice(cut), repaired = events.indexOf(`${kind}:repair`);
            const bodyIndex = kind === "metadata" ? events.indexOf(`${kind}:stat-bulk`) : events.indexOf(`${kind}:body:${kind}`);
            assert(repaired >= 0 && bodyIndex > repaired, `${kind} body ran before actual base-derived tree repair`);
            assert.equal(a.engine.rootRepairRequired, false);
        } finally { query.resolve(); f.control.checkpoint = undefined; f.control.statBulk = undefined; await f.close(); }
    }
}

async function unsupportedPendingNeverFallsBackOrScans() {
    const f = await fixture(), a = await f.create("unsupported");
    try {
        await loseAccepted(f, a.engine); f.control.caps = null;
        const cut = f.events.length;
        for (const operation of [() => a.engine.pushPending(), () => a.engine.pullRemote(),
            () => a.engine.reconcileContent(), () => a.engine.fullScan(), () => a.engine.partialMtimeScan()]) {
            assert.match(String(await rejection(operation())), /outcome remains pending/);
        }
        assert.equal(f.server.legacy, 0); assert.equal(f.server.requests.length, 1); assert.equal(f.server.queries, 0);
        assert.equal(f.journal.unsyncedCount(), 1); assert.equal(a.engine.pendingChanges.size, 1);
        assert.equal(a.treeState.repairs, 0);
        assert(!f.events.slice(cut).some(event => event.includes(":body:") || event.endsWith(":stat-bulk")));
        assert(a.engine.rootRuntime.pending());
    } finally { await f.close(); }
}

async function acceptedCutFailuresPreserveNewerWorkAndCurrentBase() {
    for (const failure of ["candidate", "cache"] as const) {
        const f = await fixture(), a = await f.create(failure), root = gate();
        try {
            await f.queue(a.engine, [pathAt(0)], 2);
            const oldCache = f.cached();
            f.control.beforeRootAnswer = root.wait;
            if (failure === "candidate") f.control.commitFailure = true;
            else f.control.cache = () => { throw new Error("native root cache write failed"); };
            const push = a.engine.pushPending(); await root.entered;
            const newest = await f.event(a.engine, pathAt(0), 3);
            // Cache writes are optional and do not stop a healthy root drain.
            // Quiesce while the accepted response is held to isolate its tail
            // and the stale-cache replacement, without publishing N+1 here.
            const quiesced = failure === "cache" ? a.engine.quiesceAndDrain() : undefined;
            root.resolve(); await push;
            await quiesced;
            f.control.beforeRootAnswer = undefined; f.control.cache = undefined;
            assert.equal(f.server.requests.length, 1); assert.equal(f.base.getHash(pathAt(0)), hashFor(2));
            assert.equal(f.journal.unsynced()[0]?.id, newest); assert.equal(a.engine.pendingChanges.size, 1);
            assert.equal(failure === "cache" ? (await f.cold()).pending : a.engine.rootRuntime.pending(), null,
                "already settled root was reinvented as unresolved");
            // A newer durable local base overlay models state already applied
            // before replacement/recovery, not permission to replay the old
            // intent's entries. Its honest parent remains the accepted root.
            const honestParent = f.base.treeBaseRoot;
            f.base.setEntry(pathAt(0), hashFor(4), 4, 1); await f.base.save();
            let active = a;
            if (failure === "cache") {
                assert.deepEqual(f.cached(), oldCache);
                await a.engine.stopAndDrain();
                active = await f.create("cache-restart", { staleCache: true });
                assert.equal(active.treeHashAt(pathAt(0)), hashFor(1));
            } else assert.equal(a.engine.rootRepairRequired, true);
            await active.engine.recoverRootBeforeWork();
            assert.equal(active.treeHashAt(pathAt(0)), hashFor(4), "repair replayed historical candidate over newer current base");
            assert.equal(active.engine.treeBaseRoot, honestParent, "partial current base invented a new honest server parent");
            assert.equal(f.base.getHash(pathAt(0)), hashFor(4)); assert.equal(f.journal.unsynced()[0]?.id, newest);
            assert.equal(active.engine.pendingChanges.size, 1); assert.equal(f.server.requests.length, 1);
            const cold = await f.cold(); assert.equal(cold.base.getHash(pathAt(0)), hashFor(4));
            assert.equal(cold.journal.unsynced()[0]?.id, newest);
        } finally { root.resolve(); f.control.cache = undefined; await f.close(); }
    }
}

async function startupCapturesWhileRecoveryQueryWaits() {
    const f = await fixture(), old = await f.create("old"), query = gate();
    try {
        await loseAccepted(f, old.engine); await old.engine.stopAndDrain();
        const next = await f.create("startup", { active: false, staleCache: true });
        f.control.beforeQueryAnswer = query.wait;
        let pullSawNewer = false;
        // Explicitly isolated startup-order ports: ordinary pull/metadata work
        // is not under test here. Actual local preparation, root recovery,
        // journal callbacks and root publication remain untouched.
        next.engine.pullRemote = async () => {
            f.events.push("startup:ordinary-pull-stub");
            pullSawNewer = f.journal.unsynced().some(row => row.path === pathAt(0)) && next.engine.pendingChanges.has(pathAt(0));
            assert.equal(f.base.getHash(pathAt(0)), hashFor(2));
        };
        next.engine.partialMtimeScan = async () => { f.events.push("startup:metadata-stub"); };
        const start = observe(next.engine.start()); await query.entered;
        assert.equal(start.state.settled, false);
        assert(!f.events.includes("startup:ordinary-pull-stub"));
        assert(next.engine.eventRefs.some((ref: any) => ref.event === "modify"));
        const latest = await f.event(next.engine, pathAt(0), 3);
        assert.equal(f.journal.unsynced()[0]?.id, latest);
        query.resolve(); await start.done;
        assert.equal(start.state.error, undefined); assert.equal(pullSawNewer, true);
        assert(f.events.indexOf("startup:repair") < f.events.indexOf("startup:ordinary-pull-stub"));
        assert.equal(f.server.requests.length, 2); assert.equal(f.server.entries.get(pathAt(0))?.hash, hashFor(3));
        assert.equal(f.journal.unsyncedCount(), 0);
    } finally { query.resolve(); await f.close(); }
}

async function stopAndReplacementJoinRootNativeAndAcceptedTail() {
    const f = await fixture(), a = await f.create("drain-A"), root = gate(), base = gate(), ack = gate();
    try {
        await f.queue(a.engine, [pathAt(0)], 2); f.control.beforeRootAnswer = root.wait;
        let baseHeld = false, ackHeld = false;
        f.control.boundary = async event => {
            if (!baseHeld && rows(event, SYNC_BASE_STORE_PATH).some(row => row.op === "root-publication")) {
                baseHeld = true; await base.wait();
            }
            if (!ackHeld && rows(event, JOURNAL_STORE_PATH).some(row => row.op === "ack")) { ackHeld = true; await ack.wait(); }
        };
        const push = observe(a.engine.pushPending()); await root.entered;
        const stop = observe(a.engine.stopAndDrain()); await turns();
        assert.equal(stop.state.settled, false); assert.equal(a.engine.isStopped(), true);
        root.resolve(); await base.entered; await turns(); assert.equal(stop.state.settled, false);
        base.resolve(); await ack.entered; await turns(); assert.equal(stop.state.settled, false);
        assert.equal(f.journal.unsyncedCount(), 1);
        ack.resolve(); await push.done; await stop.done;
        assert.equal(push.state.error, undefined); assert.equal(stop.state.error, undefined);
        assert.equal(f.base.getHash(pathAt(0)), hashFor(2)); assert.equal(f.journal.unsyncedCount(), 0);
        f.control.boundary = undefined; f.control.beforeRootAnswer = undefined;
        const b = await f.create("drain-B"); await f.queue(b.engine, [pathAt(0)], 3); await b.engine.pushPending();
        assert.equal(f.server.floor, 2); assert.equal(f.server.entries.get(pathAt(0))?.hash, hashFor(3));
        const cold = await f.cold(); assert.equal(cold.sequence, 2); assert.equal(cold.base.getHash(pathAt(0)), hashFor(3));
        assert.equal(cold.journal.unsyncedCount(), 0); assert.equal(f.server.legacy, 0);
    } finally { root.resolve(); base.resolve(); ack.resolve(); await f.close(); }
}

async function ambiguousRetirementPreservesOnlyUnacknowledgedGenerations() {
    // These are adapter-completion faults, not filesystem crash/fsync claims.
    // A retire WAL alone is an orphan; a validated staged/promoted head can
    // already make retirement authoritative despite its caller seeing EIO.
    for (const boundary of ["wal", "stage", "promote"] as const) {
        for (const change of ["none", "journal-newer", "volatile-newer"] as const) {
            for (const handoff of [false, true]) {
                const f = await fixture(), a = await f.create("retire-A"), fault = gate();
                const context = `${boundary}/${change}/${handoff ? "handoff" : "same-renderer"}`;
                try {
                    const [originalId] = await f.queue(a.engine, [pathAt(0)], 2);
                    let retireWal: string | undefined, failed = false;
                    f.control.boundary = async event => {
                        if (rows(event, ROOT_INTENT_PATH).some(row => row.op === "retire")) retireWal = event.path;
                        if (!retireWal || failed || event.phase !== "after") return;
                        const selected = boundary === "wal" ? event.method === "write" && event.path === retireWal
                            : boundary === "stage" ? event.method === "write" && event.path === `${ROOT_INTENT_PATH}/head.json.next`
                            : event.method === "rename" && event.path === `${ROOT_INTENT_PATH}/head.json.next` &&
                                event.to === `${ROOT_INTENT_PATH}/head.json`;
                        if (selected) {
                            failed = true; await fault.wait();
                            throw Object.assign(new Error(`retirement ${boundary} completion lost`), { code: "EIO" });
                        }
                    };
                    const push = observe(a.engine.pushPending()); await fault.entered;
                    assert.equal(push.state.settled, false, context);
                    assert.equal(f.base.getHash(pathAt(0)), hashFor(2), context);
                    assert.equal(f.journal.unsyncedCount(), 0, "retirement fault must follow actual journal ACK");
                    let newest: number | undefined;
                    if (change === "journal-newer") {
                        newest = await f.event(a.engine, pathAt(0), 3);
                        assert(newest > originalId);
                    } else if (change === "volatile-newer") {
                        // A real dirty-set scan/failed-append hint, deliberately
                        // not claimed durable and with no own journal ID.
                        f.source.set(pathAt(0), 3);
                        a.engine.pendingChanges.add({ action: "modified", path: pathAt(0), hash: hashFor(3), mtime: 3, size: 1 });
                    }
                    fault.resolve(); await push.done; f.control.boundary = undefined;
                    assert.equal(push.state.error, undefined, "push reports its failure without escaping the operation owner");
                    assert(failed, context); assert.equal(a.engine.rootRepairRequired, true, context);
                    assert.equal(a.engine.rootRuntime.snapshot().loaded, false, "ambiguous intent IO must require validated reload");
                    assert.equal(a.engine.pendingChanges.size, 1, "old detached snapshot or live newer hint must remain until recovery");
                    const before = await f.cold();
                    assert.equal(before.sequence, 1, context);
                    if (boundary === "wal") assert.equal(before.pending?.terminal?.status, "accepted", "unreferenced retirement WAL became committed");
                    else assert.equal(before.pending, null, "validated retirement head was ignored");
                    assert.equal(before.journal.unsyncedCount(), change === "journal-newer" ? 1 : 0, context);
                    assert.equal(before.base.getHash(pathAt(0)), hashFor(2), context);

                    let active = a;
                    if (handoff) {
                        await a.engine.quiesceAndDrain();
                        active = await f.create("retire-B", { active: false });
                        await a.engine.handoffCaptureTo(active.engine);
                        await active.engine.prepareLocal();
                        Object.assign(active.engine, { startupReplayInProgress: false, startupCompleted: true, state: "idle" });
                    }
                    await active.engine.recoverRootBeforeWork();
                    assert.equal(active.engine.rootRepairRequired, false, context);
                    assert.equal(active.engine.rootRuntime.pending(), null, context);
                    assert.equal(active.engine.pendingChanges.size, change === "none" ? 0 : 1, context);
                    assert.equal(active.treeHashAt(pathAt(0)), hashFor(2), context);
                    assert.equal(f.journal.unsyncedCount(), change === "journal-newer" ? 1 : 0, context);
                    if (change !== "none") {
                        const retained = active.engine.pendingChanges.take();
                        assert.equal(retained.length, 1, context); assert.equal(retained[0].path, pathAt(0), context);
                        assert.equal(retained[0].mtime, 3, "recovery restored the ACKed old generation over a live newer hint");
                        if (change === "journal-newer") {
                            assert.equal(retained[0].journalId, newest, context);
                            assert.equal(f.journal.unsynced()[0]?.id, newest, context);
                        }
                        active.engine.pendingChanges.restore(retained);
                    }
                    assert.equal(f.server.requests.length, 1, "local terminal retirement failure caused a blind root resend");
                    assert.equal(f.server.queries, 0, "a persisted terminal receipt does not require another server lookup");
                    assert.equal(f.server.legacy, 0, context);
                } finally { fault.resolve(); f.control.boundary = undefined; await f.close(); }
            }
        }
    }
}

async function changedApiScopeNeverDispatchesAnUnownedRoot() {
    for (const boundary of ["selection", "prepared"] as const) {
        const f = await fixture(), a = await f.create(`scope-${boundary}`);
        try {
            await f.queue(a.engine, [pathAt(0)], 2);
            const stale = new Error("captured API scope is stale");
            let changed = boundary === "selection", scopeChecks = 0;
            f.control.assertApiScope = () => { scopeChecks++; if (changed) throw stale; };
            f.control.boundary = event => {
                if (boundary === "prepared" && rows(event, ROOT_INTENT_PATH).some(row => row.op === "intent")) changed = true;
            };
            const push = observe(a.engine.pushPending()); await push.done;
            assert(changed); assert(scopeChecks > 0);
            if (boundary === "selection") assert.equal(push.state.error, stale, "entry recovery fence did not reject a stale scope");
            else {
                assert.equal(push.state.error, undefined);
                assert.equal(a.engine.lastError?.origin, "push");
                assert.equal(a.engine.lastError?.message, stale.message);
            }
            assert.equal(f.server.requests.length, 0, "stale captured credentials dispatched a root request");
            assert.equal(f.server.queries, 0); assert.equal(f.server.legacy, 0);
            assert.equal(f.base.getHash(pathAt(0)), hashFor(1)); assert.equal(f.journal.unsyncedCount(), 1);
            assert.equal(a.engine.pendingChanges.size, 1);
            const disk = await f.cold();
            assert.equal(disk.sequence, boundary === "selection" ? 0 : 1);
            if (boundary === "selection") assert.equal(disk.pending, null);
            else {
                assert.equal(disk.pending?.terminal, null);
                assert.equal(disk.pending?.intent.request.sequence, 1);
                assert.equal(a.engine.rootRepairRequired, true, "prepared-but-unsent intent must go through outcome recovery");
            }
            // Deliberately do not invent a cancellation response: this fixture
            // proves dispatch fencing and durable ambiguity, not its recovery.
        } finally { f.control.boundary = undefined; f.control.assertApiScope = undefined; await f.close(); }
    }
}

async function changedApiScopeFencesOrdinaryWorkButJoinsAcceptedTail() {
    const f = await fixture(), a = await f.create("ordinary-scope");
    try {
        const stale = new Error("ordinary work lost its captured API owner");
        let checks = 0;
        f.control.assertApiScope = () => { checks++; throw stale; };
        const cut = f.events.length;
        for (const [kind, operation] of [
            ["pull", () => a.engine.pullRemote()], ["reconcile", () => a.engine.reconcileContent()],
            ["full-scan", () => a.engine.fullScan()], ["metadata", () => a.engine.partialMtimeScan()],
        ] as const) {
            const previousChecks = checks;
            const pending = observe(operation()); await pending.done;
            assert(checks > previousChecks, `${kind} bypassed the current captured-scope guard`);
            assert.equal(f.server.diffs, 0, `${kind} dispatched ordinary pull with stale captured scope`);
            assert.equal(f.server.requests.length, 0); assert.equal(f.server.queries, 0); assert.equal(f.server.legacy, 0);
            assert(!f.events.slice(cut).some(event => event.includes(":body:") || event.endsWith(":stat-bulk") || event.endsWith(":source-read")),
                `${kind} entered ordinary work before resolving scope ownership`);
            assert.equal(f.base.getHash(pathAt(0)), hashFor(1)); assert.equal(f.journal.unsyncedCount(), 0);
        }
    } finally { f.control.assertApiScope = undefined; await f.close(); }

    const accepted = await fixture(), owner = await accepted.create("accepted-scope"), root = gate();
    try {
        await accepted.queue(owner.engine, [pathAt(0)], 2);
        accepted.control.beforeRootAnswer = root.wait;
        const push = observe(owner.engine.pushPending()); await root.entered;
        const latest = await accepted.event(owner.engine, pathAt(0), 3);
        accepted.control.assertApiScope = () => { throw new Error("API owner changed after native acceptance"); };
        root.resolve(); await push.done;
        assert.equal(accepted.base.getHash(pathAt(0)), hashFor(2), "scope change discarded the already accepted local tail");
        assert.equal(accepted.journal.unsynced()[0]?.id, latest, "accepted tail ACKed a later callback");
        assert.equal(owner.engine.pendingChanges.size, 1);
        assert.equal(owner.engine.rootRuntime.pending(), null, "accepted tail failed to retire its durable receipt");
        assert.equal(accepted.server.requests.length, 1, "auto-drain sent a new root after scope changed");
        const disk = await accepted.cold(); assert.equal(disk.sequence, 1); assert.equal(disk.pending, null);
        assert.equal(disk.base.getHash(pathAt(0)), hashFor(2)); assert.equal(disk.journal.unsynced()[0]?.id, latest);
    } finally { root.resolve(); accepted.control.assertApiScope = undefined; await accepted.close(); }
}

function assertNoRootOrBasePublication(f: Fixture, storageCut: number, originalRoot: string | null) {
    assert.equal(f.server.requests.length, 0); assert.equal(f.server.legacy, 0);
    assert.equal(f.base.treeBaseRoot, originalRoot);
    assert(!f.storage.events.slice(storageCut).some(event => event.method === "write" &&
        event.path.startsWith(`${SYNC_BASE_STORE_PATH}/`)), "local omission mutated the durable sync base");
}

async function standaloneLocalOmissionsRetireWithoutRootOrBase() {
    const f = await fixture(), a = await f.create("local-omission", { ignorePatterns: ["ignored/"] });
    try {
        const ignored = "ignored/old-note.md", folder = "old-folder";
        f.control.statErrors.set(folder, new PathIsDirectoryError(folder));
        const ids = await f.queue(a.engine, [ignored, folder], 2);
        const originalRoot = f.base.treeBaseRoot, cut = f.storage.events.length;
        await a.engine.pushPending();
        assert.equal(f.journal.unsyncedCount(), 0); assert.equal(a.engine.pendingChanges.size, 0);
        assertNoRootOrBasePublication(f, cut, originalRoot);
        const acknowledgements = f.storage.events.slice(cut).flatMap(event => rows(event, JOURNAL_STORE_PATH)).filter(row => row.op === "ack");
        assert.deepEqual(acknowledgements.map(row => ({ path: row.path, throughId: row.throughId })),
            [{ path: ignored, throughId: ids[0] }, { path: folder, throughId: ids[1] }].sort((left, right) => left.path < right.path ? -1 : 1));
        const disk = await f.cold(); assert.equal(disk.sequence, 0); assert.equal(disk.pending, null);
        assert.equal(disk.journal.unsyncedCount(), 0);
        // A positive type observation alone cannot license discarding an
        // unrelated no-ID hint merely because nothing was publishable.
        a.engine.pendingChanges.add({ path: folder, action: "modified", mtime: 3, size: 1 });
        const noIdCut = f.storage.events.length;
        await a.engine.pushPending();
        assert.equal(a.engine.pendingChanges.size, 1);
        assert(!f.storage.events.slice(noIdCut).flatMap(event => rows(event, JOURNAL_STORE_PATH)).some(row => row.op === "ack"));
        assertNoRootOrBasePublication(f, cut, originalRoot);
    } finally { await f.close(); }
}

async function localOmissionAckKeepsNewCallbackAndVolatileHint() {
    for (const change of ["journal-newer", "volatile-newer"] as const) {
        const f = await fixture(), a = await f.create("omission-race"), ack = gate();
        try {
            const folder = "old-folder";
            f.control.statErrors.set(folder, new PathIsDirectoryError(folder));
            const [originalId] = await f.queue(a.engine, [folder], 2);
            const originalRoot = f.base.treeBaseRoot, cut = f.storage.events.length;
            let held = false;
            f.control.boundary = async event => {
                if (!held && rows(event, JOURNAL_STORE_PATH).some(row => row.op === "ack")) { held = true; await ack.wait(); }
            };
            const push = observe(a.engine.pushPending()); await ack.entered;
            f.control.statErrors.delete(folder);
            let callback: ReturnType<typeof observe<number>> | undefined;
            if (change === "journal-newer") callback = observe(f.event(a.engine, folder, 3));
            else {
                f.source.set(folder, 3);
                a.engine.pendingChanges.add({ action: "modified", path: folder, hash: hashFor(3), mtime: 3, size: 1 });
            }
            // Native journal append is serialized behind the held ACK. Close
            // only new operation admission; the already-started callback and
            // the ACK keep their actual promises and shared live hint owner.
            const drain = observe(a.engine.quiesceAndDrain()); await turns();
            assert.equal(drain.state.settled, false); assert.equal(push.state.settled, false);
            ack.resolve(); await push.done; await callback?.done; await drain.done;
            assert.equal(push.state.error, undefined); assert.equal(drain.state.error, undefined);
            assert.equal(callback?.state.error, undefined);
            assert.equal(a.engine.pendingChanges.size, 1, `${change} disappeared during local-only ACK`);
            const hints = a.engine.pendingChanges.take();
            assert.equal(hints[0]?.path, folder); assert.equal(hints[0]?.mtime, 3);
            if (change === "journal-newer") {
                assert(callback!.state.value! > originalId); assert.equal(hints[0].journalId, callback!.state.value);
                assert.equal(f.journal.unsynced()[0]?.id, callback!.state.value);
            } else assert.equal(f.journal.unsyncedCount(), 0, "volatile no-ID hint must not be presented as durable");
            a.engine.pendingChanges.restore(hints);
            assertNoRootOrBasePublication(f, cut, originalRoot);
            const disk = await f.cold(); assert.equal(disk.sequence, 0);
            assert.equal(disk.journal.unsyncedCount(), change === "journal-newer" ? 1 : 0);
        } finally { ack.resolve(); f.control.boundary = undefined; await f.close(); }
    }
}

async function ambiguousLocalAckReloadsItsActualCut() {
    for (const boundary of ["wal", "stage", "promote"] as const) {
        for (const newer of [false, true]) {
            const f = await fixture(), a = await f.create("local-ack-recovery"), fault = gate();
            try {
                const folder = "old-folder";
                f.control.statErrors.set(folder, new PathIsDirectoryError(folder));
                await f.queue(a.engine, [folder], 2);
                const originalRoot = f.base.treeBaseRoot, cut = f.storage.events.length;
                let ackWal: string | undefined, failed = false;
                f.control.boundary = async event => {
                    if (rows(event, JOURNAL_STORE_PATH).some(row => row.op === "ack")) ackWal = event.path;
                    if (!ackWal || failed || event.phase !== "after") return;
                    const selected = boundary === "wal" ? event.method === "write" && event.path === ackWal
                        : boundary === "stage" ? event.method === "write" && event.path === `${JOURNAL_STORE_PATH}/head.json.next`
                        : event.method === "rename" && event.path === `${JOURNAL_STORE_PATH}/head.json.next` &&
                            event.to === `${JOURNAL_STORE_PATH}/head.json`;
                    if (selected) { failed = true; await fault.wait(); throw Object.assign(new Error("local ACK completion lost"), { code: "EIO" }); }
                };
                const push = observe(a.engine.pushPending()); await fault.entered;
                if (newer) {
                    f.control.statErrors.delete(folder); f.source.set(folder, 3);
                    a.engine.pendingChanges.add({ action: "modified", path: folder, hash: hashFor(3), mtime: 3, size: 1 });
                }
                fault.resolve(); await push.done; f.control.boundary = undefined;
                assert.equal(push.state.error, undefined); assert.equal(a.engine.rootRepairRequired, true);
                const disk = await f.cold(); assert.equal(disk.sequence, 0); assert.equal(disk.pending, null);
                assert.equal(disk.journal.unsyncedCount(), boundary === "wal" ? 1 : 0,
                    "recovery confused an orphan ACK row with the authoritative head");
                await a.engine.recoverRootBeforeWork();
                assert.equal(a.engine.rootRepairRequired, false);
                assert.equal(f.journal.unsyncedCount(), boundary === "wal" ? 1 : 0);
                assert.equal(a.engine.pendingChanges.size, newer || boundary === "wal" ? 1 : 0,
                    "local ACK retirement removed unacknowledged or live no-ID work");
                if (newer) {
                    const hints = a.engine.pendingChanges.take(); assert.equal(hints[0]?.mtime, 3);
                    a.engine.pendingChanges.restore(hints);
                } else if (boundary === "wal") {
                    await a.engine.pushPending();
                    assert.equal(f.journal.unsyncedCount(), 0); assert.equal(a.engine.pendingChanges.size, 0);
                }
                assertNoRootOrBasePublication(f, cut, originalRoot);
            } finally { fault.resolve(); f.control.boundary = undefined; await f.close(); }
        }
    }
}

async function omittedRenamePeerHoldsItsGroupWhileIndependentWorkPublishes() {
    const f = await fixture(2);
    try {
        const oldPath = pathAt(1), newPath = "ignored/renamed.md";
        f.source.delete(oldPath); f.source.set(newPath, 2);
        const [generation] = await f.journal.appendGroup([
            { action: "deleted", path: oldPath, ts: 2, synced: false },
            { action: "created", path: newPath, ts: 2, synced: false },
        ]);
        // This is the actual replay of a persisted rename whose destination
        // later became excluded, not a fabricated dependency-only planner.
        const a = await f.create("omitted-rename", { ignorePatterns: ["ignored/"] });
        await f.queue(a.engine, [pathAt(0)], 3);
        await a.engine.pushPending();
        assert.deepEqual(f.server.publicationSizes, [1]);
        assert.equal(f.server.entries.get(pathAt(0))?.hash, hashFor(3));
        assert.equal(f.server.entries.get(oldPath)?.hash, hashFor(1), "linked delete escaped an omitted destination");
        assert.equal(f.server.entries.has(newPath), false);
        assert.equal(a.engine.pendingChanges.size, 2);
        assert.deepEqual(f.journal.unsynced().map(row => ({ id: row.id, action: row.action, path: row.path, oldPath: row.oldPath })),
            [{ id: generation, action: "renamed", path: newPath, oldPath }]);
        assert.equal(a.engine.deferredChanges.captureDependencies().length, 1);
        const disk = await f.cold(); assert.equal(disk.journal.unsynced()[0]?.id, generation);
        assert.equal(disk.base.getHash(oldPath), hashFor(1)); assert.equal(disk.sequence, 1);
    } finally { await f.close(); }
}

async function materializationFailureCannotBecomeLocalOmissionAck() {
    for (const error of [Object.assign(new Error("temporarily unavailable"), { code: "EIO" }),
        Object.assign(new Error("not a typed folder observation"), { code: "EISDIR" })]) {
        const f = await fixture(), a = await f.create("omission-io", { ignorePatterns: ["ignored/"] });
        try {
            const ignored = "ignored/first.md";
            await f.queue(a.engine, [ignored, pathAt(0)], 2);
            f.control.statErrors.set(pathAt(0), error);
            const originalRoot = f.base.treeBaseRoot, cut = f.storage.events.length;
            await a.engine.pushPending();
            assert.equal(a.engine.lastError?.message, error.message);
            assert.equal(f.journal.unsyncedCount(), 2); assert.equal(a.engine.pendingChanges.size, 2);
            assert(!f.storage.events.slice(cut).flatMap(event => rows(event, JOURNAL_STORE_PATH)).some(row => row.op === "ack"),
                "an early exclusion was ACKed before the later materialization error");
            assertNoRootOrBasePublication(f, cut, originalRoot);
        } finally { await f.close(); }
    }
}

async function localOmissionCutsAreBoundedAndWaitForFullBulkReview() {
    const f = await fixture(), a = await f.create("omission-bounds", { ignorePatterns: ["ignored/"] });
    try {
        const paths = Array.from({ length: 257 }, (_, index) => `ignored/${pathAt(index)}`);
        await f.queue(a.engine, paths, 2);
        const cut = f.storage.events.length, originalRoot = f.base.treeBaseRoot;
        await a.engine.pushPending();
        const batches = f.storage.events.slice(cut).map(event => rows(event, JOURNAL_STORE_PATH).filter(row => row.op === "ack")).filter(batch => batch.length);
        assert.deepEqual(batches.map(batch => batch.length), [256, 1]);
        assert.equal(f.journal.unsyncedCount(), 0); assert.equal(a.engine.pendingChanges.size, 0);
        assertNoRootOrBasePublication(f, cut, originalRoot);
    } finally { await f.close(); }

    const bulk = await fixture(600), owner = await bulk.create("omission-review", { ignorePatterns: ["ignored/"] });
    try {
        const ignored = "ignored/first.md", deleted = Array.from({ length: 600 }, (_, index) => pathAt(index));
        await bulk.queue(owner.engine, [ignored, ...deleted], 2);
        for (const path of deleted) bulk.source.delete(path);
        const cut = bulk.storage.events.length, originalRoot = bulk.base.treeBaseRoot;
        await owner.engine.pushPending();
        assert.equal(owner.engine.bulkChangeReviewRequired, true, "bounded root/local-ACK slicing hid the full deletion review");
        assert.equal(bulk.journal.unsyncedCount(), 601); assert.equal(owner.engine.pendingChanges.size, 601);
        assert(!bulk.storage.events.slice(cut).flatMap(event => rows(event, JOURNAL_STORE_PATH)).some(row => row.op === "ack"),
            "local omission was retired before the full batch passed review");
        assert.equal(bulk.base.entryCount(), 600); assertNoRootOrBasePublication(bulk, cut, originalRoot);
    } finally { await bulk.close(); }
}

async function metadataMutexProtectsNativeAuditAndStillDrainsLiveEdits() {
    for (const phase of ["read", "config-list"] as const) {
        const f = await fixture(2), a = await f.create(`metadata-${phase}`, { syncConfig: phase === "config-list" }), held = gate();
        try {
            if (phase === "read") {
                f.source.set(pathAt(0), 2);
                f.control.beforeRead = path => path === pathAt(0) ? held.wait() : undefined;
            } else f.control.listConfig = async () => { await held.wait(); return new Map(); };
            const scan = observe(a.engine.partialMtimeScan()); await held.entered;
            assert.equal(scan.state.settled, false); assert.equal(a.engine.syncing, true); assert.equal(a.engine.state, "scanning");
            const latest = await f.event(a.engine, pathAt(1), 3);
            assert.equal(f.journal.unsynced()[0]?.id, latest, "metadata ownership blocked the actual live journal callback");
            assert.equal(a.engine.pendingChanges.size, 1);
            const eventCut = f.events.length, storageCut = f.storage.events.length;
            // The direct push models an already-fired debounce/manual request
            // while the scan owns base. No methods under test are replaced.
            for (const operation of [() => a.engine.pushPending(), () => a.engine.pullRemote(),
                () => a.engine.fullScan(), () => a.engine.reconcileContent(), () => a.engine.partialMtimeScan()]) {
                const blocked = observe(operation()); await blocked.done; assert.equal(blocked.state.error, undefined);
            }
            assert.equal(f.server.preflights, 0); assert.equal(f.server.requests.length, 0); assert.equal(f.server.diffs, 0);
            assert(!f.events.slice(eventCut).some(event => event.includes(":body:") || event.endsWith(":stat-bulk") ||
                event.endsWith(":source-read") || event.endsWith(":config-list")), "another operation entered a body during metadata's native await");
            assertNoRootOrBasePublication(f, storageCut, f.base.treeBaseRoot);
            assert.equal(f.base.getHash(pathAt(0)), hashFor(1)); assert.equal(f.base.getHash(pathAt(1)), hashFor(1));
            assert.equal(a.engine.syncing, true); assert.equal(scan.state.settled, false);
            const roots = Array.from({ length: phase === "read" ? 2 : 1 }, () => gate());
            f.control.beforeRootAnswer = () => roots[f.server.requests.length - 1].wait();
            held.resolve(); await roots[0].entered;
            assert.equal(scan.state.settled, false);
            assert.deepEqual(f.server.publicationSizes, [1]);
            assert.deepEqual(f.server.cuts[0], [{ path: pathAt(1), throughId: latest }],
                "metadata completion did not publish the live callback as the first urgent root");
            assert.equal(f.journal.unsyncedCount(), phase === "read" ? 2 : 1);
            assert.equal(a.engine.pendingChanges.size, phase === "read" ? 1 : 0);
            roots[0].resolve();
            if (phase === "read") {
                await roots[1].entered;
                assert.equal(scan.state.settled, false);
                assert.deepEqual(f.server.publicationSizes, [1, 1]);
                assert.deepEqual(f.server.cuts[1].map(cut => cut.path), [pathAt(0)],
                    "scan-discovered bulk work did not follow the urgent callback root");
                assert.equal(f.journal.unsyncedCount(), 1); assert.equal(a.engine.pendingChanges.size, 0);
                roots[1].resolve();
            }
            await scan.done; f.control.beforeRootAnswer = undefined;
            assert.equal(scan.state.error, undefined); assert.equal(a.engine.syncing, false);
            assert.equal(f.server.requests.length, phase === "read" ? 2 : 1,
                "metadata completion stranded an edit whose push fired during the scan");
            assert.deepEqual(f.server.publicationSizes, phase === "read" ? [1, 1] : [1]);
            assert.equal(f.server.entries.get(pathAt(1))?.hash, hashFor(3));
            assert.equal(f.server.entries.get(pathAt(0))?.hash, hashFor(phase === "read" ? 2 : 1));
            assert.equal(f.journal.unsyncedCount(), 0); assert.equal(a.engine.pendingChanges.size, 0);
            const disk = await f.cold(); assert.equal(disk.sequence, phase === "read" ? 2 : 1); assert.equal(disk.journal.unsyncedCount(), 0);
        } finally { held.resolve(); f.control.beforeRootAnswer = undefined; f.control.beforeRead = undefined; f.control.listConfig = undefined; await f.close(); }
    }
}

async function metadataErrorAndAbortReleaseOnlyAfterActualRead() {
    for (const failure of ["io", "abort"] as const) {
        const f = await fixture(2), a = await f.create(`metadata-${failure}`), read = gate();
        try {
            const error = Object.assign(new Error("metadata source read unavailable"), { code: "EIO" });
            f.source.set(pathAt(0), 2);
            f.control.beforeRead = async path => { if (path === pathAt(0)) { await read.wait(); if (failure === "io") throw error; } };
            const scan = observe(a.engine.partialMtimeScan()); await read.entered;
            const latest = await f.event(a.engine, pathAt(1), 3);
            const storageCut = f.storage.events.length, originalRoot = f.base.treeBaseRoot;
            const drain = failure === "abort" ? observe(a.engine.quiesceAndDrain()) : undefined;
            await turns(); assert.equal(scan.state.settled, false); assert.equal(a.engine.syncing, true);
            if (drain) assert.equal(drain.state.settled, false, "abort pretended the native read had already completed");
            read.resolve(); await scan.done; await drain?.done;
            assert.equal(a.engine.syncing, false); assert.equal(drain?.state.error, undefined);
            if (failure === "io") assert.equal(scan.state.error, error);
            else assert.equal((scan.state.error as Error)?.name, "AbortError");
            assert.equal(f.journal.unsynced()[0]?.id, latest); assert.equal(a.engine.pendingChanges.size, 1);
            assertNoRootOrBasePublication(f, storageCut, originalRoot);
            f.control.beforeRead = undefined;
            if (failure === "io") {
                await a.engine.partialMtimeScan();
                assert.equal(a.engine.syncing, false); assert.equal(f.server.requests.length, 1);
                assert.equal(f.server.entries.get(pathAt(0))?.hash, hashFor(2));
                assert.equal(f.server.entries.get(pathAt(1))?.hash, hashFor(3));
                assert.equal(f.journal.unsyncedCount(), 0); assert.equal(a.engine.pendingChanges.size, 0);
            } else {
                const disk = await f.cold(); assert.equal(disk.sequence, 0); assert.equal(disk.journal.unsynced()[0]?.id, latest);
            }
        } finally { read.resolve(); f.control.beforeRead = undefined; await f.close(); }
    }
}

async function automaticDebounceOwnsEarlyScopeRejection() {
    // The normal runner launches this actual bundle in a strict child. Do not
    // install unhandledRejection/uncaughtException listeners to hide a leak.
    assert(process.execArgv.includes("--unhandled-rejections=strict"));
    const f = await fixture();
    let callback: (() => void) | undefined;
    try {
        const a = await f.create("automatic-scope", { autoSync: true, captureDebounce(value) { callback = value; } });
        assert(callback, "actual attachVaultListeners did not register its debounce callback");
        const [latest] = await f.queue(a.engine, [pathAt(0)], 2);
        const cut = f.storage.events.length;
        const stale = new Error("automatic callback lost its captured API owner");
        f.control.assertApiScope = () => { throw stale; };
        assert.equal(callback(), undefined, "fixture must invoke the actual fire-and-forget callback");
        // Give Node a host turn to surface an unhandled rejection, rather than
        // ending the suite immediately after the callback's synchronous return.
        await new Promise<void>(resolve => setTimeout(resolve, 0));
        assert.equal(a.engine.lastError?.origin, "push"); assert.equal(a.engine.lastError?.message, stale.message);
        assert.equal(f.server.preflights, 0); assert.equal(f.server.requests.length, 0); assert.equal(f.server.diffs, 0);
        assert.equal(f.server.queries, 0); assert.equal(f.server.legacy, 0);
        assert(!f.storage.events.slice(cut).some(event => event.method === "write" || event.method === "rename" || event.method === "remove"),
            "automatic rejection changed persistent state");
        assert.equal(f.base.getHash(pathAt(0)), hashFor(1)); assert.equal(f.journal.unsynced()[0]?.id, latest);
        assert.equal(a.engine.pendingChanges.size, 1);
        await a.engine.stopAndDrain();
    } finally { f.control.assertApiScope = undefined; await f.close(); }
}

async function continuationReadinessDoesNotDetachTheBacklog() {
    const f = await fixture();
    try {
        const a = await f.create("readiness-cut");
        await f.queue(a.engine, [pathAt(0)], 2);
        for (let index = 1; index < 25_000; index++) {
            a.engine.pendingChanges.add({ action: "modified", path: pathAt(index), size: 1, mtime: 2 });
        }
        const dirty = a.engine.pendingChanges;
        const originalTake = dirty.take, originalRestore = dirty.restore, originalIterate = dirty.iterate;
        const storageCut = f.storage.events.length;
        let visited = 0, closed = 0;
        dirty.take = dirty.restore = () => { throw new Error("readiness detached or restored the full backlog"); };
        dirty.iterate = function* () {
            try {
                for (const hint of originalIterate.call(dirty)) { visited++; yield hint; }
            } finally { closed++; }
        };
        try {
            for (let check = 0; check < 4; check++) assert.equal(a.engine.hasRunnablePendingChanges(), true);
            assert.equal(visited, 4, "each early runnable witness should cost one lazy hint visit");
            assert.equal(closed, 4, "eligibility retained a captured iterator after short circuit");
            assert.equal(dirty.size, 25_000);
            assert.equal(f.journal.unsyncedCount(), 1);
            assert.equal(f.server.requests.length, 0);
            assert.equal(f.storage.events.length, storageCut, "readiness must not perform persistence IO");
        } finally { dirty.take = originalTake; dirty.restore = originalRestore; dirty.iterate = originalIterate; }
        assert.deepEqual([...dirty.paths()], Array.from({ length: 25_000 }, (_, index) => pathAt(index)),
            "read-only readiness changed sequential priority");
    } finally { await f.close(); }
}

async function outputAdmissionRefusalRetriesOnlyWhenFactsChange() {
    const f = await fixture(), a = await f.create("output-refusal");
    const plan = { nodePayloadBytes: 600, rangeEndpointPeakRequestedBytes: 400,
        rangeEndpointResidentRequestedBytes: 100, peakAdmissionBytes: 1_000,
        residentAdmissionBytes: 700 };
    const admission = new RootTreeResidentAdmission({ capacityBytes: 999 });
    let phase: "idle" | "input" | "build" | "plan" | "ready" | "retiring" = "idle";
    let residentVersion = 1;
    let received = 0, completed = 0, planReads = 0, resumes = 0, growDuringRetirement = false;
    const residentRootHash = a.tree.root_hash_hex.bind(a.tree);
    let residentGraphPresent = false;
    const stepReplacement = (token: number) => {
        assert.equal(token, 71);
        if (phase === "build") {
            phase = "plan"; completed++; return { done: false, units: 1, completed, phase: "plan ready" };
        }
        assert.equal(phase, "ready"); completed++;
        return { done: true, units: 1, completed, phase: "ready" };
    };
    Object.assign(a.tree, {
        tree_version: () => residentVersion,
        candidate_revision: () => 0,
        root_hash_hex: () => residentGraphPresent ? residentRootHash() : null,
        begin_replacement_rebuild_job(version: number) {
            assert.equal(version, 2); assert.equal(phase, "idle");
            phase = "input"; received = 0; completed = 0; return 71;
        },
        append_replacement_rebuild_job(token: number, offset: number, json: string) {
            assert.equal(token, 71); assert.equal(phase, "input"); assert.equal(offset, received);
            received += JSON.parse(json).length; return received;
        },
        finish_replacement_rebuild_job() { throw new Error("deferred fixture used consuming finish"); },
        start_replacement_rebuild_job(token: number) {
            assert.equal(token, 71); assert.equal(phase, "input"); phase = "build";
        },
        step_replacement_rebuild_job: stepReplacement,
        step_replacement_rebuild_output_memory_v1_job: stepReplacement,
        replacement_rebuild_plan_job(token: number) {
            assert.equal(token, 71); assert.equal(phase, "plan"); planReads++;
            return { nodeCount: 1, leafCount: 1, internalCount: 0,
                nodePayloadBytes: plan.nodePayloadBytes, maxNodeBytes: plan.nodePayloadBytes,
                storedRootBytes: 128 };
        },
        resume_replacement_rebuild_job() { throw new Error("output admission used legacy resume"); },
        replacement_rebuild_output_memory_plan_v1_job(token: number) {
            assert.equal(token, 71); assert.equal(phase, "plan");
            return { schema: 1, scope: "v2-replacement-output", ...plan };
        },
        resume_replacement_rebuild_output_memory_v1_job(token: number, node: number,
            endpointPeak: number, endpointResident: number) {
            assert.equal(token, 71); assert.equal(phase, "plan");
            assert.deepEqual([node, endpointPeak, endpointResident], [plan.nodePayloadBytes,
                plan.rangeEndpointPeakRequestedBytes, plan.rangeEndpointResidentRequestedBytes]);
            resumes++; phase = "ready";
        },
        cancel_replacement_rebuild_job_deferred(token: number) {
            assert.equal(token, 71); assert.notEqual(phase, "idle"); phase = "retiring";
        },
        finish_replacement_rebuild_job_deferred(token: number) {
            assert.equal(token, 71); assert.equal(phase, "ready");
            a.tree.bump_committed_revision_for_test(); residentGraphPresent = true;
            residentVersion = 2; phase = "retiring";
        },
        step_tree_retirement(token: number) {
            assert.equal(token, 71); assert.equal(phase, "retiring");
            if (growDuringRetirement) {
                growDuringRetirement = false; admission.setCapacity(plan.peakAdmissionBytes);
            }
            phase = "idle"; return { done: true, units: 1, completed: 1 };
        },
        cancel_tree_job() { throw new Error("deferred fixture used consuming cancel"); },
    });
    a.engine.rootTreeResidentAdmission = admission;
    // Cached-root export is orthogonal here; the synthetic tree keeps its V1
    // JSON codec while this test drives the V2 replacement-only ABI.
    a.engine.saveCachedRoot = async () => {};
    assert.equal(a.engine.rootRuntime.lastSequence, 0);
    assert.equal(a.engine.rootRepairRequired, false);
    a.engine.rootRepairVersion = 2;
    a.engine.preparedScopeHash = "scope-v1";
    a.engine.preparedTransfers = {
        plan: {},
        scopeForTree: async (version: number) => `scope-v${version}`,
    };
    const originalLoad = f.base.load.bind(f.base); let loads = 0;
    f.base.load = async () => { loads++; await originalLoad(); };
    const originalRecord = a.engine.recordSyncFailure.bind(a.engine); let failures = 0;
    a.engine.recordSyncFailure = (...args: unknown[]) => { failures++; return originalRecord(...args); };
    try {
        const first = await rejection(a.engine.recoverRootBeforeWork());
        assert.equal((first as any).code, "ROOT_TREE_OUTPUT_ADMISSION_DENIED");
        assert.equal(a.engine.rootRepairVersion, 2,
            "refused cross-format rebuild forgot its authenticated target");
        assert.equal(a.engine.preparedScopeHash, "scope-v1",
            "refused rebuild activated a prepared scope for unpublished output");
        assert.deepEqual([loads, planReads, resumes, failures], [1, 1, 0, 1]);
        assert.deepEqual([f.server.requests.length, f.server.diffs], [0, 0],
            "sequence-zero cold-base admission reached ordinary network work");
        assert.equal(await rejection(a.engine.recoverRootBeforeWork()), first,
            "unchanged facts did not reuse the exact refusal");
        assert.deepEqual([loads, planReads, resumes, failures], [1, 1, 0, 1]);

        await rejection(a.engine.forceSync());
        assert.deepEqual([loads, planReads, resumes, failures], [2, 2, 0, 2],
            "Sync Now did not explicitly retry admission");
        assert.equal(await rejection(a.engine.recoverRootBeforeWork()) !== undefined, true);
        assert.deepEqual([loads, planReads, resumes, failures], [2, 2, 0, 2]);

        growDuringRetirement = true;
        await rejection(a.engine.fullScan());
        assert.deepEqual([loads, planReads, resumes, failures], [3, 3, 0, 3],
            "Full Rescan did not explicitly retry admission");
        await a.engine.recoverRootBeforeWork();
        assert.deepEqual([loads, planReads, resumes, failures], [4, 4, 1, 3],
            "refusal cached facts changed during cleanup and suppressed a now-admissible retry");
        assert.equal(a.engine.rootRepairRequired, false);
        assert.equal(a.engine.rootRepairVersion, null);
        assert.equal(a.engine.preparedScopeHash, "scope-v2",
            "successful format transition retained the old prepared scope");
        assert.equal(admission.snapshot().residentBytes, plan.residentAdmissionBytes);
        admission.releaseResidentAfterFree(a.tree);
    } finally {
        admission.close(); await f.close();
    }
}

async function initialPushAdmitsTheEmptyCommittedGraphBeforeFileWork() {
    const f = await fixture(0), a = await f.create("initial-push-admission");
    const plan = { nodePayloadBytes: 600, rangeEndpointPeakRequestedBytes: 400,
        rangeEndpointResidentRequestedBytes: 100, peakAdmissionBytes: 1_000,
        residentAdmissionBytes: 700 };
    const admission = new RootTreeResidentAdmission({ capacityBytes: 999 });
    const residentRootHash = a.tree.root_hash_hex.bind(a.tree);
    const residentTotalFiles = a.tree.total_files.bind(a.tree);
    const cancelTreeJob = a.tree.cancel_tree_job.bind(a.tree);
    const rootExportInfo = a.tree.root_export_info.bind(a.tree);
    let phase: "idle" | "input" | "build" | "plan" | "ready" | "retiring" = "idle";
    let residentGraphPresent = false, received = 0, completed = 0;
    let begins = 0, planReads = 0, resumes = 0, finishes = 0, retirements = 0;
    let activeToken = 0, failPublishedRetirement = false;
    let injectMutationRetirementCallback: (() => void) | null = null;
    const retiredTokens: number[] = [];
    let cacheSaves = 0;
    // This fixture opts into the real V2 ledger, so it must expose the same
    // complete output family as packaged WASM. Disabling the policy here would
    // hide an unadmitted mutation immediately after the admitted bootstrap.
    const mutationPlan = { schema: 1, scope: "v2-candidate-mutation-output",
        nodePayloadBytes: 200, rangeEndpointPeakRequestedBytes: 40,
        rangeEndpointResidentRequestedBytes: 20, peakAdmissionBytes: 240, residentAdmissionBytes: 220 };
    const mutationActual = 170;
    let mutation: { token: number; kind: "update" | "delete"; payload: string;
        phase: "plan" | "planned" | "build" | "ready" | "retiring" } | null = null;
    let mutationBegins = 0, mutationResumes = 0, mutationFinishes = 0, mutationRetirements = 0;
    let mutationResidentBytes = 0, candidateRevision = 0;
    const finishCandidate = a.tree.finish_candidate_job.bind(a.tree);
    const commitCandidate = a.tree.commit_candidate.bind(a.tree), abortCandidate = a.tree.abort_candidate.bind(a.tree);
    const beginMutation = (kind: "update" | "delete", payload: string) => {
        assert.equal(phase, "idle"); assert.equal(mutation, null); assert(a.tree.has_candidate());
        mutation = { token: 700 + ++mutationBegins, kind, payload, phase: "plan" }; return mutation.token;
    };
    const mutationOwner = (token: number) => {
        assert(mutation); assert.equal(mutation.token, token); assert(a.tree.has_candidate()); return mutation;
    };
    const stepReplacement = (token: number) => {
        assert.equal(token, activeToken);
        if (phase === "build") {
            phase = "plan"; completed++;
            return { done: false, units: 1, completed, phase: "plan ready" };
        }
        assert.equal(phase, "ready"); completed++;
        return { done: true, units: 1, completed, phase: "ready" };
    };
    f.base.setTreeBaseRoot(null); await f.base.save();
    Object.assign(a.engine, {
        treeBaseRoot: null,
        localRootHash: null,
        rootTreeResidentAdmission: admission,
        saveCachedRoot: async () => { cacheSaves++; },
    });
    Object.assign(a.engine.wasm, { wasm_root_version_from_bytes: () => 2 });
    Object.assign(a.tree, {
        tree_version: () => 2,
        candidate_revision: () => candidateRevision,
        finish_candidate_job(token: number) { const result = finishCandidate(token); candidateRevision++; return result; },
        commit_candidate() { assert.equal(mutation, null); const result = commitCandidate(); candidateRevision++; return result; },
        abort_candidate() { assert.equal(mutation, null); const result = abortCandidate(); candidateRevision++; return result; },
        begin_candidate_update_job: (payload: string) => beginMutation("update", payload),
        begin_candidate_delete_job: (payload: string) => beginMutation("delete", payload),
        finish_candidate_mutation_job() { throw new Error("admitted V2 mutation used legacy finish"); },
        step_candidate_mutation_output_memory_v1_job(token: number, budget: number) {
            assert.equal(budget, TREE_CANDIDATE_MUTATION_STEP_UNITS);
            const owner = mutationOwner(token);
            if (owner.phase === "plan") {
                owner.phase = "planned";
                return { done: false, units: 1, completed: 1, remaining: 1, reachable: 0, phase: "plan ready" };
            }
            assert.equal(owner.phase, "build"); owner.phase = "ready";
            return { done: true, units: 1, completed: 2, remaining: 0, reachable: 1, phase: "ready" };
        },
        candidate_mutation_output_memory_plan_v1_job(token: number) {
            assert.equal(mutationOwner(token).phase, "planned"); return { ...mutationPlan };
        },
        resume_candidate_mutation_output_memory_v1_job(token: number, node: number, peak: number, resident: number) {
            const owner = mutationOwner(token); assert.equal(owner.phase, "planned");
            assert.deepEqual([node, peak, resident], [200, 40, 20]);
            assert.equal(admission.snapshot().privateBytes, 240, "mutation resumed before its separate peak reservation");
            assert.equal(admission.snapshot().residentBytes, 700 + mutationResidentBytes,
                "mutation replaced an existing resident output allowance");
            mutationResumes++; owner.phase = "build";
        },
        candidate_mutation_output_memory_ready_v1_job(token: number) {
            assert.equal(mutationOwner(token).phase, "ready");
            return { schema: 1, scope: "v2-candidate-mutation-output", stagedNodePayloadBytes: 150,
                rangeEndpointResidentRequestedBytes: 20, residentAdmissionBytes: mutationActual };
        },
        finish_candidate_mutation_job_deferred(token: number) {
            const owner = mutationOwner(token); assert.equal(owner.phase, "ready");
            assert.equal(admission.snapshot().privateBytes, mutationActual, "Ready did not shrink to actual mutation output");
            if (owner.kind === "update") a.tree.candidate_update_batch(owner.payload);
            else a.tree.candidate_delete_batch(owner.payload);
            candidateRevision++; mutationFinishes++; mutationResidentBytes += mutationActual; owner.phase = "retiring";
        },
        cancel_candidate_mutation_job_deferred(token: number) { mutationOwner(token).phase = "retiring"; },
        root_hash_hex: () => residentGraphPresent ? residentRootHash() : null,
        total_files: () => residentGraphPresent ? residentTotalFiles() : 0,
        build_from_entries: () => {
            throw new Error("initial push used synchronous tree bootstrap");
        },
        rebuild_from_entries_in_version: () => {
            throw new Error("initial push used consuming replacement");
        },
        begin_replacement_rebuild_job(version: number) {
            assert.equal(version, 2); assert.equal(phase, "idle");
            begins++; activeToken = 300 + begins;
            received = 0; completed = 0; phase = "input"; return activeToken;
        },
        append_replacement_rebuild_job() {
            throw new Error("empty push bootstrap unexpectedly fed a nonempty page");
        },
        finish_replacement_rebuild_job() { throw new Error("initial push used consuming finish"); },
        start_replacement_rebuild_job(token: number) {
            assert.equal(token, activeToken); assert.equal(phase, "input"); assert.equal(received, 0); phase = "build";
        },
        step_replacement_rebuild_job: stepReplacement,
        step_replacement_rebuild_output_memory_v1_job: stepReplacement,
        replacement_rebuild_plan_job(token: number) {
            assert.equal(token, activeToken); assert.equal(phase, "plan"); planReads++;
            return { nodeCount: 1, leafCount: 1, internalCount: 0,
                nodePayloadBytes: plan.nodePayloadBytes, maxNodeBytes: plan.nodePayloadBytes,
                storedRootBytes: 128 };
        },
        replacement_rebuild_output_memory_plan_v1_job(token: number) {
            assert.equal(token, activeToken); assert.equal(phase, "plan");
            return { schema: 1, scope: "v2-replacement-output", ...plan };
        },
        resume_replacement_rebuild_job() { throw new Error("initial push used legacy resume"); },
        resume_replacement_rebuild_output_memory_v1_job(token: number, node: number,
            endpointPeak: number, endpointResident: number) {
            assert.equal(token, activeToken); assert.equal(phase, "plan");
            assert.deepEqual([node, endpointPeak, endpointResident], [plan.nodePayloadBytes,
                plan.rangeEndpointPeakRequestedBytes, plan.rangeEndpointResidentRequestedBytes]);
            resumes++; phase = "ready";
        },
        cancel_replacement_rebuild_job_deferred(token: number) {
            assert.equal(token, activeToken); assert.notEqual(phase, "idle"); phase = "retiring";
        },
        finish_replacement_rebuild_job_deferred(token: number) {
            assert.equal(token, activeToken); assert.equal(phase, "ready");
            a.tree.bump_committed_revision_for_test(); residentGraphPresent = true;
            finishes++; phase = "retiring";
        },
        step_tree_retirement(token: number) {
            if (mutation !== null) {
                assert.equal(mutationOwner(token).phase, "retiring");
                if (mutationFinishes > 0) assert.equal(admission.snapshot().residentBytes, 700 + mutationResidentBytes,
                    "mutation retirement did not retain the attached output cohort");
                const callback = injectMutationRetirementCallback;
                injectMutationRetirementCallback = null;
                callback?.();
                mutation = null; mutationRetirements++; return { done: true, units: 1, completed: 1 };
            }
            assert.equal(token, activeToken); assert.equal(phase, "retiring");
            retirements++; retiredTokens.push(token);
            if (failPublishedRetirement && token === 302) {
                failPublishedRetirement = false;
                throw new Error("injected initial-push retirement failure");
            }
            phase = "idle"; return { done: true, units: 1, completed: 1 };
        },
        cancel_tree_job: cancelTreeJob,
        root_export_info(token: number) {
            return { ...rootExportInfo(token), version: 2 };
        },
    });
    try {
        await f.queue(a.engine, [pathAt(0)], 1);
        a.engine.bulkChangeReviewRequired = true;
        await a.engine.pushPending();
        assert.deepEqual([begins, f.server.preflights, cacheSaves], [0, 0, 0],
            "blocked push prepared a committed bootstrap owner");
        a.engine.bulkChangeReviewRequired = false;

        await a.engine.pushPending();
        const first = a.engine.pushBootstrapAdmissionRefusal?.error;
        assert.equal(first?.code, "ROOT_TREE_OUTPUT_ADMISSION_DENIED");
        assert.deepEqual([begins, planReads, resumes, finishes, retirements], [1, 1, 0, 0, 1]);
        assert.equal(a.engine.rootRepairRequired, false,
            "local push refusal became a pre-authenticated general recovery request");
        assert.equal(a.engine.rootRepairVersion, null);
        assert.equal(a.engine.pendingChanges.size, 1);
        assert.equal(f.journal.unsyncedCount(), 1);
        assert.deepEqual([f.server.preflights, f.server.checks, f.server.requests.length, cacheSaves], [2, 0, 0, 0],
            "refused initial graph reached content/root/cache publication work");
        assert(!f.events.includes("initial-push-admission:source-read"));

        await a.engine.pushPending();
        assert.equal(a.engine.pushBootstrapAdmissionRefusal?.error, first,
            "unchanged initial-push facts did not retain the exact refusal");
        assert.deepEqual([begins, planReads, resumes, finishes, retirements], [1, 1, 0, 0, 1]);
        assert.deepEqual([f.server.preflights, f.server.checks, f.server.requests.length, cacheSaves], [4, 0, 0, 0]);

        admission.setCapacity(plan.peakAdmissionBytes);
        failPublishedRetirement = true;
        await a.engine.pushPending();
        assert.deepEqual([begins, planReads, resumes, finishes, retirements], [2, 2, 1, 1, 2]);
        assert.deepEqual(retiredTokens, [301, 302]);
        assert.equal(a.engine.pendingChanges.size, 1);
        assert.equal(f.journal.unsyncedCount(), 1);
        assert.deepEqual([f.server.preflights, f.server.checks, f.server.requests.length, cacheSaves], [6, 0, 0, 0],
            "retirement failure published local authority or consumed queued work");

        // General recovery owns the retry before push planning. It must drain
        // token 302 before opening 303 and must not cache the still-unaccepted
        // parentless graph. The second replacement temporarily coexists with
        // the installed 700-byte resident owner.
        admission.setCapacity(plan.peakAdmissionBytes + plan.residentAdmissionBytes);
        const preacceptFailure = new Error("injected post-recovery source failure");
        f.control.statErrors.set(pathAt(0), preacceptFailure);
        await a.engine.pushPending();
        f.control.statErrors.delete(pathAt(0));
        assert.deepEqual([begins, planReads, resumes, finishes, retirements], [3, 3, 2, 2, 4]);
        assert.deepEqual(retiredTokens, [301, 302, 302, 303],
            "retirement retry opened a new replacement before draining the exact old token");
        assert.equal(a.engine.pendingChanges.size, 1);
        assert.equal(f.journal.unsyncedCount(), 1);
        assert.equal(f.server.requests.length, 0);
        assert.equal(cacheSaves, 0,
            "parentless recovery cached its rebuilt graph before server acceptance");
        assert.equal(a.engine.treeBaseRoot, null);
        assert.equal(a.engine.localRootHash, null);
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [0, 0, 0, 0],
            "bootstrap refusal/recovery/source failure reached candidate output allocation");
        const cold = await f.cold();
        assert.equal(cold.base.treeBaseRoot, null);
        assert.equal(cold.journal.unsyncedCount(), 1);
        assert.equal(cold.pending, null);

        await a.engine.pushPending();
        assert.deepEqual([begins, planReads, resumes, finishes, retirements], [3, 3, 2, 2, 4],
            "healthy recovered graph was rebuilt again before publication");
        assert.equal(a.engine.rootRepairRequired, false);
        assert.equal(a.engine.rootRepairVersion, null);
        assert.equal(a.engine.pendingChanges.size, 0);
        assert.equal(f.journal.unsyncedCount(), 0);
        assert.equal(f.server.requests.length, 1);
        assert.equal(cacheSaves, 1, "local bootstrap cached a root before server acceptance");
        assert.equal(f.server.requests[0].parent_root, "",
            "initial publication invented a verified parent");
        assert.equal(f.server.entries.get(pathAt(0))?.hash, hashFor(1));
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [1, 1, 1, 1],
            "successful bootstrap push did not use the complete admitted mutation lifecycle");
        const snapshot = admission.snapshot();
        assert.deepEqual([snapshot.privateAttempts, snapshot.retiringOwners,
            snapshot.residentTrees, snapshot.residentBytes], [0, 0, 1, 700 + mutationActual]);

        // An automatic retry of the exact same whole-cut V2 mutation refusal
        // must remain pending without repeating source/hash/Plan work.
        const sourceReads = () => f.events.filter(event => event === "initial-push-admission:source-read").length;
        admission.setCapacity(snapshot.residentBytes + mutationPlan.peakAdmissionBytes - 1);
        await f.event(a.engine, pathAt(0), 2);
        await a.engine.pushPending();
        assert.equal(a.engine.candidateMutationAdmissionRefusal?.error.code,
            "CANDIDATE_MUTATION_OUTPUT_ADMISSION_DENIED");
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [2, 1, 1, 2]);
        const refusedReads = sourceReads(), refusedPreflights = f.server.preflights;
        await a.engine.pushPending();
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [2, 1, 1, 2],
            "unchanged automatic refusal repeated native mutation planning");
        assert.deepEqual([sourceReads(), f.server.preflights], [refusedReads, refusedPreflights],
            "unchanged automatic refusal repeated source or transport preparation");
        assert.equal(a.engine.pendingChanges.size, 1);
        assert.equal(f.journal.unsyncedCount(), 1);

        // A same-path N+1 which arrives during exact retirement must prevent
        // the old attempt from installing a stale marker. It remains the
        // authoritative queued generation after the older claim is restored.
        const newerId = await f.event(a.engine, pathAt(0), 3);
        const newerHint = [...a.engine.pendingChanges.iterate()][0];
        assert.equal(newerHint.journalId, newerId);
        injectMutationRetirementCallback = () => {
            a.engine.pendingChanges.add(newerHint, newerHint.journalId);
        };
        await a.engine.pushPending();
        assert.equal(a.engine.candidateMutationAdmissionRefusal, null,
            "late equal-looking callback installed a stale refusal marker");
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [3, 1, 1, 3]);
        await a.engine.pushPending();
        assert.equal(a.engine.candidateMutationAdmissionRefusal?.error.code,
            "CANDIDATE_MUTATION_OUTPUT_ADMISSION_DENIED");
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [4, 1, 1, 4],
            "newer generation did not receive one real admission attempt");
        await a.engine.pushPending();
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [4, 1, 1, 4],
            "newer unchanged refusal was not suppressed");

        admission.setCapacity(snapshot.residentBytes + mutationPlan.peakAdmissionBytes);
        await a.engine.pushPending();
        assert.equal(a.engine.pendingChanges.size, 0);
        assert.equal(f.journal.unsyncedCount(), 0);
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [5, 2, 2, 5],
            "capacity growth did not invalidate and complete the refused mutation");

        // Sync Now is an explicit retry epoch. It must make one real attempt
        // even when every dirty/base/tree/ledger fact otherwise still matches.
        const residentAfterSecondRoot = admission.snapshot().residentBytes;
        admission.setCapacity(residentAfterSecondRoot + mutationPlan.peakAdmissionBytes - 1);
        await f.event(a.engine, pathAt(0), 4);
        await a.engine.pushPending();
        await a.engine.pushPending();
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [6, 2, 2, 6]);
        const originalPull = a.engine.pullRemote, originalReconcile = a.engine.reconcileContent;
        a.engine.pullRemote = async () => {};
        a.engine.reconcileContent = async () => ({ smallUploaded: 0, largeUploaded: 0,
            treeChunksUploaded: 0, bytes: 0, deferred: 0, drifted: 0, readErrors: 0, uploadErrors: 0 });
        try { await a.engine.forceSync(); }
        finally { a.engine.pullRemote = originalPull; a.engine.reconcileContent = originalReconcile; }
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [7, 2, 2, 7],
            "Sync Now did not invalidate an unchanged automatic refusal");
        assert.equal(a.engine.pendingChanges.size, 1);
        admission.setCapacity(residentAfterSecondRoot + mutationPlan.peakAdmissionBytes);
        await a.engine.pushPending();
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [8, 3, 3, 8]);
        assert.equal(a.engine.pendingChanges.size, 0);
        assert.equal(f.journal.unsyncedCount(), 0);

        // A refusal belongs to the admission generation which actually denied
        // it. Capacity becoming sufficient during native retirement must not
        // let the old error suppress the now-admissible automatic retry.
        const residentAfterThirdRoot = admission.snapshot().residentBytes;
        admission.setCapacity(residentAfterThirdRoot + mutationPlan.peakAdmissionBytes - 1);
        await f.event(a.engine, pathAt(0), 5);
        injectMutationRetirementCallback = () => {
            admission.setCapacity(residentAfterThirdRoot + mutationPlan.peakAdmissionBytes);
        };
        await a.engine.pushPending();
        assert.equal(a.engine.candidateMutationAdmissionRefusal, null,
            "pre-growth refusal was rebound to sufficient post-cleanup capacity");
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [9, 3, 3, 9]);
        await a.engine.pushPending();
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [10, 4, 4, 10],
            "sufficient capacity installed during cleanup did not trigger a real retry");
        assert.equal(a.engine.pendingChanges.size, 0);
        assert.equal(f.journal.unsyncedCount(), 0);

        // The exact mutation-cut attestation is produced only after late
        // source admission/dependency closure. One oversized peer plus one
        // refused independent mutation must not cache a marker for both paths.
        const residentAfterFourthRoot = admission.snapshot().residentBytes;
        admission.setCapacity(residentAfterFourthRoot + mutationPlan.peakAdmissionBytes - 1);
        await f.queue(a.engine, [pathAt(0), pathAt(1)], 6);
        const originalStat = a.engine.io.stat;
        a.engine.io.stat = async (path: string) => path === pathAt(1)
            ? { mtime: 6, size: 1024 * 1024 * 1024 }
            : originalStat.call(a.engine.io, path);
        try {
            await a.engine.pushPending();
            assert.equal(a.engine.candidateMutationAdmissionRefusal, null,
                "late source deferral installed a marker for a mixed cut");
            await a.engine.pushPending();
            assert.equal(a.engine.candidateMutationAdmissionRefusal, null,
                "repeated mixed cut became globally suppressed");
        } finally {
            a.engine.io.stat = originalStat;
        }
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [12, 4, 4, 12],
            "mixed late-deferral cut did not make two real mutation attempts");
        assert.equal(a.engine.pendingChanges.size, 2);
        admission.setCapacity(residentAfterFourthRoot + mutationPlan.peakAdmissionBytes);
        await a.engine.pushPending();
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [13, 5, 5, 13]);
        assert.equal(a.engine.pendingChanges.size, 0);
        assert.equal(f.journal.unsyncedCount(), 0);

        // Exactly 256 paths are still one complete bounded cut and may be
        // suppressed. A policy change is an explicit witness invalidation and
        // must grant a fresh real attempt for the same durable generations.
        const residentAfterFifthRoot = admission.snapshot().residentBytes;
        admission.setCapacity(residentAfterFifthRoot + mutationPlan.peakAdmissionBytes - 1);
        await f.queue(a.engine, Array.from({ length: 256 }, (_, index) => pathAt(index)), 7);
        await a.engine.pushPending();
        assert.equal(a.engine.candidateMutationAdmissionRefusal?.inputCount, 256);
        await a.engine.pushPending();
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [14, 5, 5, 14],
            "complete 256-path refusal was not suppressed");
        a.engine.syncPriority = "newest";
        await a.engine.pushPending();
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [15, 5, 5, 15],
            "policy change did not invalidate the complete-cut refusal");
        await a.engine.pushPending();
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [15, 5, 5, 15],
            "unchanged refusal under the new policy was not suppressed");

        a.engine.deferredChanges.registerLegacyRename("unrelated-old.md", "unrelated-new.md");
        await a.engine.pushPending();
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [16, 5, 5, 16],
            "dependency-only change did not invalidate the complete-cut refusal");
        await a.engine.pushPending();
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [16, 5, 5, 16],
            "unchanged refusal under the new dependency graph was not suppressed");

        const sameRoot = f.base.treeBaseRoot;
        await f.base.load();
        assert.equal(f.base.treeBaseRoot, sameRoot);
        await a.engine.pushPending();
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [17, 5, 5, 17],
            "same-root base reload did not invalidate the complete-cut refusal");
        assert.equal(a.engine.pendingChanges.size, 256);
        assert.equal(f.journal.unsyncedCount(), 256);
        await a.engine.pushPending();
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [17, 5, 5, 17],
            "unchanged refusal after base reload was not suppressed");
        admission.setCapacity(residentAfterFifthRoot + mutationPlan.peakAdmissionBytes);
        await a.engine.pushPending();
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [18, 6, 6, 18]);
        assert.equal(a.engine.pendingChanges.size, 0);
        assert.equal(f.journal.unsyncedCount(), 0);

        // A bounded selection from a larger pending set is not a whole-cut
        // witness. Repeated automatic calls must keep making progress attempts
        // instead of globally suppressing the other 257-path queue.
        const residentAfterSixthRoot = admission.snapshot().residentBytes;
        admission.setCapacity(residentAfterSixthRoot + mutationPlan.peakAdmissionBytes - 1);
        await f.queue(a.engine, Array.from({ length: 257 }, (_, index) => pathAt(index)), 8);
        await a.engine.pushPending();
        assert.equal(a.engine.candidateMutationAdmissionRefusal, null,
            "partial bounded cut installed a global mutation refusal marker");
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [19, 6, 6, 19]);
        await a.engine.pushPending();
        assert.equal(a.engine.candidateMutationAdmissionRefusal, null,
            "repeated partial cut installed a global mutation refusal marker");
        assert.deepEqual([mutationBegins, mutationResumes, mutationFinishes, mutationRetirements], [20, 6, 6, 20],
            "larger pending set was globally suppressed after one bounded refusal");
        assert.equal(a.engine.pendingChanges.size, 257);
        assert.equal(f.journal.unsyncedCount(), 257);
        admission.releaseResidentAfterFree(a.tree);
    } finally {
        admission.close(); await f.close();
    }
}

async function stoppingDuringCommittedBootstrapCannotOpenCandidate() {
    const f = await fixture(0);
    f.base.setTreeBaseRoot(null); await f.base.save();
    const a = await f.create("initial-push-stop");
    const release = gate();
    a.engine.prepareCommittedTreeForPush = async () => {
        await release.wait();
    };
    try {
        await f.queue(a.engine, [pathAt(0)], 1);
        const pushing = observe(a.engine.pushPending());
        await release.entered;
        const stopping = a.engine.stopAndDrain();
        release.resolve();
        await Promise.all([pushing.done, stopping]);
        assert.equal(pushing.state.error, undefined);
        assert.equal(a.treeState.commits, 0);
        assert.equal(a.treeState.aborts, 0);
        assert.equal(f.server.checks, 0);
        assert.equal(f.server.requests.length, 0);
        assert.equal(a.engine.pendingChanges.size, 1);
        assert.equal(f.journal.unsyncedCount(), 1);
        const cold = await f.cold();
        assert.equal(cold.base.treeBaseRoot, null);
        assert.equal(cold.journal.unsyncedCount(), 1);
        assert.equal(cold.pending, null);
    } finally { release.resolve(); await f.close(); }
}

async function stoppingDuringPullMutationRetiresBeforeOperationRelease() {
    const f = await fixture(), a = await f.create("pull-mutation-stop");
    const admission = new RootTreeResidentAdmission({ capacityBytes: 90 });
    const path = pathAt(0), oldRoot = a.engine.treeBaseRoot, oldCache = f.cached();
    let revision = 0, mutationPhase: "idle" | "plan" | "planned" | "build" | "ready" | "retiring" = "idle";
    let mutationPayload = "", retirementSteps = 0, rootFetches = 0;
    let retirementEnteredResolve!: () => void;
    const retirementEntered = new Promise<void>(resolve => { retirementEnteredResolve = resolve; });
    const finishCandidate = a.tree.finish_candidate_job.bind(a.tree);
    const candidateDelete = a.tree.candidate_delete_batch.bind(a.tree);
    const commitCandidate = a.tree.commit_candidate.bind(a.tree);
    const abortCandidate = a.tree.abort_candidate.bind(a.tree);
    Object.assign(a.engine, { rootTreeResidentAdmission: admission });
    Object.assign(a.engine.api, {
        getDiff: async () => [{ action: "deleted", path }],
        getRoot: async () => { rootFetches++; return oldCache; },
    });
    Object.assign(a.engine.io, {
        deleteFile: async (deleted: string) => { assert.equal(deleted, path); f.source.delete(deleted); },
    });
    Object.assign(a.tree, {
        tree_version: () => 2,
        candidate_revision: () => revision,
        finish_candidate_job(token: number) {
            const result = finishCandidate(token); revision++; return result;
        },
        commit_candidate() { const result = commitCandidate(); revision++; return result; },
        abort_candidate() { const result = abortCandidate(); revision++; return result; },
        begin_candidate_delete_job(payload: string) {
            assert.equal(mutationPhase, "idle"); assert.equal(a.tree.has_candidate(), true);
            mutationPayload = payload; mutationPhase = "plan"; return 401;
        },
        begin_candidate_update_job() { throw new Error("unexpected pull update mutation"); },
        finish_candidate_mutation_job() { throw new Error("stopped V2 pull used consuming mutation finish"); },
        step_candidate_mutation_output_memory_v1_job(token: number, units: number) {
            assert.equal(token, 401); assert.equal(units, TREE_CANDIDATE_MUTATION_STEP_UNITS);
            if (mutationPhase === "plan") {
                mutationPhase = "planned";
                return { done: false, units: 1, completed: 1, remaining: 1, reachable: 0, phase: "plan ready" };
            }
            assert.equal(mutationPhase, "build"); mutationPhase = "ready";
            return { done: true, units: 1, completed: 2, remaining: 0, reachable: 1, phase: "ready" };
        },
        candidate_mutation_output_memory_plan_v1_job(token: number) {
            assert.equal(token, 401); assert.equal(mutationPhase, "planned");
            return { schema: 1, scope: "v2-candidate-mutation-output", nodePayloadBytes: 70,
                rangeEndpointPeakRequestedBytes: 20, rangeEndpointResidentRequestedBytes: 10,
                peakAdmissionBytes: 90, residentAdmissionBytes: 80 };
        },
        resume_candidate_mutation_output_memory_v1_job(token: number, node: number, peak: number, resident: number) {
            assert.equal(token, 401); assert.equal(mutationPhase, "planned");
            assert.deepEqual([node, peak, resident], [70, 20, 10]); mutationPhase = "build";
        },
        candidate_mutation_output_memory_ready_v1_job(token: number) {
            assert.equal(token, 401); assert.equal(mutationPhase, "ready");
            return { schema: 1, scope: "v2-candidate-mutation-output", stagedNodePayloadBytes: 40,
                rangeEndpointResidentRequestedBytes: 10, residentAdmissionBytes: 50 };
        },
        finish_candidate_mutation_job_deferred(token: number) {
            assert.equal(token, 401); assert.equal(mutationPhase, "ready");
            candidateDelete(mutationPayload); revision++; mutationPhase = "retiring";
        },
        cancel_candidate_mutation_job_deferred() { throw new Error("published stopped mutation was cancelled"); },
        step_tree_retirement(token: number, units: number) {
            assert.equal(token, 401); assert.equal(units, 256); assert.equal(mutationPhase, "retiring");
            retirementSteps++;
            if (retirementSteps === 1) {
                retirementEnteredResolve();
                return { done: false, units: 1, completed: 1 };
            }
            mutationPhase = "idle";
            return { done: true, units: 1, completed: 2 };
        },
    });
    try {
        const pulling = observe(a.engine.pullRemote());
        await retirementEntered;
        const stopping = observe(a.engine.stopAndDrain());
        await turns();
        assert.equal(pulling.state.settled, false,
            "pull operation released while native mutation retirement was pending");
        assert.equal(stopping.state.settled, false,
            "stopAndDrain skipped the pull-owned native retirement");
        assert.equal(rootFetches, 0);
        assert.equal(a.treeState.commits, 0);
        assert.equal(f.base.getEntry(path), null,
            "stop fixture did not durably apply the remote base deletion");
        assert.equal(a.treeHashAt(path), hashFor(1),
            "stopped pull changed the committed graph before exact cleanup");
        assert.deepEqual(f.cached(), oldCache);
        await Promise.all([pulling.done, stopping.done]);
        assert.equal(pulling.state.error, undefined);
        assert.equal(stopping.state.error, undefined);
        assert.equal(retirementSteps, 2);
        assert.equal(a.treeState.commits, 0);
        assert.equal(a.treeState.aborts, 1,
            "stopped pull did not abort its exact candidate after retirement");
        assert.equal(rootFetches, 0);
        assert.equal(a.engine.treeBaseRoot, oldRoot);
        assert.equal(a.engine.localRootHash, oldRoot);
        assert.equal(a.engine.rootRepairRequired, true);
        assert.equal(admission.snapshot().residentBytes, 50,
            "candidate abort released output conservatively owned by the tree store");
        admission.releaseResidentAfterFree(a.tree);
    } finally {
        admission.close(); await f.close();
    }
}

async function failedPullMutationCleanupPrecedesFullScanInspection() {
    const f = await fixture(2), a = await f.create("pull-cleanup-full-scan");
    const admission = new RootTreeResidentAdmission({ capacityBytes: 90 });
    const path = pathAt(0), oldRoot = a.engine.treeBaseRoot;
    const cleanupFailure = new Error("injected pull mutation retirement failure");
    let revision = 0, payload = "", retirementAttempts = 0, inspections = 0;
    let failRetirement = true;
    let phase: "idle" | "plan" | "planned" | "build" | "ready" | "retiring" = "idle";
    const finishCandidate = a.tree.finish_candidate_job.bind(a.tree);
    const deleteCandidate = a.tree.candidate_delete_batch.bind(a.tree);
    const commitCandidate = a.tree.commit_candidate.bind(a.tree);
    const abortCandidate = a.tree.abort_candidate.bind(a.tree);
    Object.assign(a.engine, { rootTreeResidentAdmission: admission });
    Object.assign(a.engine.api, {
        getDiff: async () => [{ action: "deleted", path }],
        getRoot: async () => { throw new Error("failed pull fetched a root after retained cleanup"); },
    });
    Object.assign(a.engine.io, {
        deleteFile: async (deleted: string) => { assert.equal(deleted, path); f.source.delete(deleted); },
    });
    Object.assign(a.tree, {
        tree_version: () => 2,
        candidate_revision: () => revision,
        finish_candidate_job(token: number) {
            const result = finishCandidate(token); revision++; return result;
        },
        commit_candidate() { const result = commitCandidate(); revision++; return result; },
        abort_candidate() { const result = abortCandidate(); revision++; return result; },
        begin_candidate_delete_job(value: string) {
            assert.equal(phase, "idle"); payload = value; phase = "plan"; return 402;
        },
        begin_candidate_update_job() { throw new Error("unexpected pull update mutation"); },
        finish_candidate_mutation_job() { throw new Error("V2 pull used consuming mutation finish"); },
        step_candidate_mutation_output_memory_v1_job(token: number, units: number) {
            assert.equal(token, 402); assert.equal(units, TREE_CANDIDATE_MUTATION_STEP_UNITS);
            if (phase === "plan") {
                phase = "planned";
                return { done: false, units: 1, completed: 1, remaining: 1, reachable: 0, phase: "plan ready" };
            }
            assert.equal(phase, "build"); phase = "ready";
            return { done: true, units: 1, completed: 2, remaining: 0, reachable: 1, phase: "ready" };
        },
        candidate_mutation_output_memory_plan_v1_job(token: number) {
            assert.equal(token, 402); assert.equal(phase, "planned");
            return { schema: 1, scope: "v2-candidate-mutation-output", nodePayloadBytes: 70,
                rangeEndpointPeakRequestedBytes: 20, rangeEndpointResidentRequestedBytes: 10,
                peakAdmissionBytes: 90, residentAdmissionBytes: 80 };
        },
        resume_candidate_mutation_output_memory_v1_job(token: number, node: number,
            peak: number, resident: number) {
            assert.equal(token, 402); assert.equal(phase, "planned");
            assert.deepEqual([node, peak, resident], [70, 20, 10]); phase = "build";
        },
        candidate_mutation_output_memory_ready_v1_job(token: number) {
            assert.equal(token, 402); assert.equal(phase, "ready");
            return { schema: 1, scope: "v2-candidate-mutation-output", stagedNodePayloadBytes: 40,
                rangeEndpointResidentRequestedBytes: 10, residentAdmissionBytes: 50 };
        },
        finish_candidate_mutation_job_deferred(token: number) {
            assert.equal(token, 402); assert.equal(phase, "ready");
            deleteCandidate(payload); revision++; phase = "retiring";
        },
        cancel_candidate_mutation_job_deferred() { throw new Error("published mutation was cancelled"); },
        step_tree_retirement(token: number, units: number) {
            assert.equal(token, 402); assert.equal(units, 256); assert.equal(phase, "retiring");
            retirementAttempts++;
            if (failRetirement) throw cleanupFailure;
            phase = "idle";
            // Isolate the cleanup-only continuation. The test already proved
            // the failed pull raised the repair fence; clearing it at the
            // exact native retirement boundary prevents Full Rescan's later,
            // unrelated replacement from obscuring the pre-body ordering.
            a.engine.rootRepairRequired = false;
            a.engine.rootRepairVersion = null;
            return { done: true, units: 1, completed: 1 };
        },
    });
    try {
        const pulling = observe(a.engine.pullRemote());
        await pulling.done;
        assert.equal(pulling.state.error, undefined,
            "public pull leaked its retained-cleanup failure");
        assert.equal(a.engine.rootRepairRequired, true);
        assert.equal(f.base.getEntry(path), null,
            "failed pull did not preserve its durably applied base deletion");
        assert.equal(a.treeHashAt(path), hashFor(1));
        assert.equal(a.treeState.commits, 0); assert.equal(a.treeState.aborts, 0);
        assert(retirementAttempts >= 2,
            "failed pull did not retain and retry its exact mutation owner");

        // Full Rescan observes the real failed-pull repair gate at entry. Its
        // shared recovery must drain the retained slot before vault inspection.
        failRetirement = false;
        f.control.statBulk = () => {
            inspections++;
            assert.equal(phase, "idle", "Full Rescan inspected the vault before native cleanup");
            assert.equal(a.treeState.aborts, 1,
                "Full Rescan inspected the vault before the exact stale candidate abort");
        };
        await a.engine.fullScan();
        assert.equal(inspections, 1);
        assert.equal(a.engine.rootRepairRequired, false);
        assert.equal(a.engine.treeBaseRoot, oldRoot);
        assert.equal(a.engine.localRootHash, oldRoot);
        assert.equal(a.treeState.commits, 0); assert.equal(a.treeState.aborts, 1);
        assert.equal(admission.snapshot().residentBytes, 50,
            "cleanup-only recovery lost conservatively retained resident output");
        admission.releaseResidentAfterFree(a.tree);
    } finally {
        f.control.statBulk = undefined;
        admission.close(); await f.close();
    }
}

async function interruptedFullScanPublishesItsDurablePrefixAfterRestart() {
    const f = await fixture(600), first = await f.create("scan-prefix-A");
    const durableCut = gate();
    let sawAppendWal = false;
    let held = false;
    try {
        for (let index = 0; index < 600; index++) f.source.set(pathAt(index), 2);
        f.control.boundary = async event => {
            if (rows(event, JOURNAL_STORE_PATH).some(row => row.op === "append")) {
                sawAppendWal = true;
            }
            if (!held && sawAppendWal && event.method === "rename" && event.phase === "after" &&
                event.path === `${JOURNAL_STORE_PATH}/head.json.next` &&
                event.to === `${JOURNAL_STORE_PATH}/head.json`) {
                held = true;
                await durableCut.wait();
            }
        };
        const scan = observe(first.engine.fullScan());
        await durableCut.entered;
        const stop = observe(first.engine.stopAndDrain());
        await turns();
        assert.equal(stop.state.settled, false,
            "renderer stop returned while a durable scan cut was publishing");
        durableCut.resolve();
        await scan.done; await stop.done;
        f.control.boundary = undefined;
        assert(scan.state.error, "stopped Full Rescan unexpectedly consumed the whole vault");
        assert.equal(stop.state.error, undefined);
        const interrupted = await f.cold();
        assert.equal(interrupted.journal.unsyncedCount(), 256,
            "interrupted Full Rescan lost or over-published its bounded durable prefix");

        const resumed = await f.create("scan-prefix-B");
        assert.equal(resumed.engine.pendingChanges.size, 256,
            "restart did not recover the completed Full Rescan prefix before network work");
        const recoveredPushCut = f.events.length;
        await resumed.engine.pushPending(true);
        assert.equal(f.events.slice(recoveredPushCut)
            .filter(event => event === "scan-prefix-B:source-read").length, 0,
        "restart re-read a scan prefix whose hash still matched its exact source stat");
        assert.deepEqual(f.server.publicationSizes, [256],
            "restart did not publish the recovered prefix as its first bounded root");
        assert.equal(f.journal.unsyncedCount(), 0);

        const readCut = f.events.length;
        await resumed.engine.fullScan();
        const remainingReads = f.events.slice(readCut)
            .filter(event => event === "scan-prefix-B:source-read").length;
        assert.equal(remainingReads, 344,
            "resumed Full Rescan rehashed its already published durable prefix");
        assert.deepEqual(f.server.publicationSizes, [256, 256, 88]);
        assert.equal(f.journal.unsyncedCount(), 0);
        const completed = await f.cold();
        assert.equal(completed.base.entryCount(), 600);
        assert.equal(completed.base.allPaths().filter(path => completed.base.getHash(path) === hashFor(2)).length, 600);
    } finally {
        durableCut.resolve();
        f.control.boundary = undefined;
        await f.close();
    }
}

async function fullScanRebuildWaitsForV2OutputAdmission() {
    const f = await fixture(), a = await f.create("full-scan-v2-admission");
    const plan = { nodePayloadBytes: 600, rangeEndpointPeakRequestedBytes: 400,
        rangeEndpointResidentRequestedBytes: 100, peakAdmissionBytes: 1_000,
        residentAdmissionBytes: 700 };
    const admission = new RootTreeResidentAdmission({
        capacityBytes: plan.peakAdmissionBytes,
    });
    let phase: "idle" | "input" | "build" | "plan" | "ready" | "retiring" = "idle";
    let received = 0, completed = 0, planReads = 0, resumes = 0;
    let finishes = 0, retirements = 0, inspections = 0;
    const residentRootHash = a.tree.root_hash_hex.bind(a.tree);
    let residentGraphPresent = false;
    const stepReplacement = (token: number) => {
        assert.equal(token, 91);
        if (phase === "build") {
            phase = "plan"; completed++;
            return { done: false, units: 1, completed, phase: "plan ready" };
        }
        assert.equal(phase, "ready"); completed++;
        return { done: true, units: 1, completed, phase: "ready" };
    };
    Object.assign(a.tree, {
        tree_version: () => 2,
        candidate_revision: () => 0,
        root_hash_hex: () => residentGraphPresent ? residentRootHash() : null,
        total_files: () => residentGraphPresent ? 1 : 0,
        rebuild_from_entries_in_version: () => {
            throw new Error("Full Rescan used consuming replacement");
        },
        build_from_entries: () => {
            throw new Error("Full Rescan used synchronous tree bootstrap");
        },
        begin_replacement_rebuild_job(version: number) {
            assert.equal(version, 2); assert.equal(phase, "idle");
            phase = "input"; received = 0; completed = 0; return 91;
        },
        append_replacement_rebuild_job(token: number, offset: number, json: string) {
            assert.equal(token, 91); assert.equal(phase, "input"); assert.equal(offset, received);
            received += JSON.parse(json).length; return received;
        },
        finish_replacement_rebuild_job() { throw new Error("Full Rescan used consuming finish"); },
        start_replacement_rebuild_job(token: number) {
            assert.equal(token, 91); assert.equal(phase, "input"); assert.equal(received, 1); phase = "build";
        },
        step_replacement_rebuild_job: stepReplacement,
        step_replacement_rebuild_output_memory_v1_job: stepReplacement,
        replacement_rebuild_plan_job(token: number) {
            assert.equal(token, 91); assert.equal(phase, "plan"); planReads++;
            return { nodeCount: 1, leafCount: 1, internalCount: 0,
                nodePayloadBytes: plan.nodePayloadBytes, maxNodeBytes: plan.nodePayloadBytes,
                storedRootBytes: 128 };
        },
        replacement_rebuild_output_memory_plan_v1_job(token: number) {
            assert.equal(token, 91); assert.equal(phase, "plan");
            return { schema: 1, scope: "v2-replacement-output", ...plan };
        },
        resume_replacement_rebuild_job() { throw new Error("Full Rescan used legacy resume"); },
        resume_replacement_rebuild_output_memory_v1_job(token: number, node: number,
            endpointPeak: number, endpointResident: number) {
            assert.equal(token, 91); assert.equal(phase, "plan");
            assert.deepEqual([node, endpointPeak, endpointResident], [plan.nodePayloadBytes,
                plan.rangeEndpointPeakRequestedBytes, plan.rangeEndpointResidentRequestedBytes]);
            resumes++; phase = "ready";
        },
        cancel_replacement_rebuild_job_deferred(token: number) {
            assert.equal(token, 91); assert.notEqual(phase, "idle"); phase = "retiring";
        },
        finish_replacement_rebuild_job_deferred(token: number) {
            assert.equal(token, 91); assert.equal(phase, "ready");
            a.tree.bump_committed_revision_for_test(); residentGraphPresent = true;
            finishes++; phase = "retiring";
        },
        step_tree_retirement(token: number) {
            assert.equal(token, 91); assert.equal(phase, "retiring");
            retirements++; phase = "idle"; return { done: true, units: 1, completed: 1 };
        },
        cancel_tree_job() { throw new Error("Full Rescan used consuming cancel"); },
    });
    a.engine.rootTreeResidentAdmission = admission;
    a.engine.saveCachedRoot = async () => {};
    a.engine.pushBlocked = true;
    f.control.statBulk = () => { inspections++; };
    let stopGate: ReturnType<typeof gate> | null = null;
    try {
        await a.engine.fullScan();
        assert.equal(a.engine.rootRepairRequired, false);
        assert.equal(a.engine.pushBlocked, false);
        assert.deepEqual([planReads, resumes, finishes, retirements, inspections], [1, 1, 1, 1, 1],
            "cold Full Rescan repeated its entry recovery rebuild");
        assert.equal(admission.snapshot().residentBytes, plan.residentAdmissionBytes);

        admission.setCapacity(plan.peakAdmissionBytes + plan.residentAdmissionBytes - 1);
        a.engine.pushBlocked = true;
        f.control.statBulk = () => {
            throw new Error("Full Rescan inspected vault before replacement admission");
        };
        const refused = await rejection(a.engine.fullScan());
        assert.equal((refused as any).code, "ROOT_TREE_OUTPUT_ADMISSION_DENIED");
        assert.equal(a.engine.rootRepairRequired, true);
        assert.equal(a.engine.rootRepairVersion, 2);
        assert.equal(a.engine.pushBlocked, true,
            "refused Full Rescan cleared the existing publish block");
        assert.deepEqual([planReads, resumes, finishes, retirements, inspections], [2, 1, 1, 2, 1],
            "refused Full Rescan crossed output admission or inspected vault state");
        assert.equal(admission.snapshot().residentBytes, plan.residentAdmissionBytes,
            "refused Full Rescan lost the old resident graph");
        assert.equal(f.server.requests.length, 0);

        assert.equal(await rejection(a.engine.recoverRootBeforeWork()), refused,
            "automatic retry did not reuse the exact Full Rescan refusal");
        assert.equal(planReads, 2, "unchanged refusal rebuilt the private graph again");

        admission.setCapacity(plan.peakAdmissionBytes + plan.residentAdmissionBytes);
        f.control.statBulk = () => { inspections++; };
        await a.engine.fullScan();
        assert.equal(a.engine.rootRepairRequired, false);
        assert.equal(a.engine.rootRepairVersion, null);
        assert.equal(a.engine.getState(), "idle");
        assert.equal(a.engine.pushBlocked, false);
        assert.deepEqual([planReads, resumes, finishes, retirements, inspections], [3, 2, 2, 3, 2]);
        assert.equal(admission.snapshot().residentBytes, plan.residentAdmissionBytes);
        assert.equal(a.engine.treeBaseRoot, f.base.treeBaseRoot);
        assert.equal(a.engine.pendingChanges.size, 0);

        const statFailure = new Error("injected Full Rescan stat failure");
        a.engine.pushBlocked = true;
        a.engine.bulkChangeReviewRequired = true;
        a.engine.rootRepairRequired = true;
        a.engine.rootRepairVersion = 2;
        f.control.statBulk = () => { throw statFailure; };
        assert.equal(await rejection(a.engine.fullScan()), statFailure);
        assert.equal(a.engine.rootRepairRequired, false,
            "post-rebuild scan failure fabricated unresolved native repair");
        assert.equal(a.engine.pushBlocked, true,
            "post-rebuild scan failure cleared the prior publish block");
        assert.equal(a.engine.bulkChangeReviewRequired, true,
            "post-rebuild scan failure consumed operator review");
        assert.deepEqual([planReads, resumes, finishes, retirements], [4, 3, 3, 4]);
        assert.equal(f.server.requests.length, 0);

        const build = stopGate = gate();
        const originalWaitForHeavyWork = a.engine.waitForHeavyWork.bind(a.engine);
        let held = false;
        a.engine.waitForHeavyWork = async (workPhase: string, operationId?: string) => {
            if (!held && workPhase === "root recovery" && phase === "ready") {
                held = true;
                await build.wait();
            }
            await originalWaitForHeavyWork(workPhase, operationId);
        };
        f.control.statBulk = () => {
            throw new Error("stopped Full Rescan reached vault inspection");
        };
        const scan = observe(a.engine.fullScan());
        await build.entered;
        assert.equal(admission.snapshot().ledger.usedBytes,
            plan.residentAdmissionBytes + plan.peakAdmissionBytes,
            "held Full Rescan did not retain old resident plus admitted private peak");
        const latest = await f.event(a.engine, pathAt(0), 2);
        const stop = observe(a.engine.stopAndDrain());
        await turns();
        assert.equal(stop.state.settled, false,
            "stop returned while Full Rescan still owned admitted native output");
        assert.equal(f.journal.unsynced().find(row => row.path === pathAt(0))?.id, latest);
        build.resolve();
        await scan.done; await stop.done;
        assert(scan.state.error, "aborted Full Rescan unexpectedly completed");
        assert.equal(stop.state.error, undefined);
        assert.deepEqual([planReads, resumes, finishes, retirements, inspections], [5, 4, 3, 5, 2]);
        assert.equal(admission.snapshot().residentBytes, plan.residentAdmissionBytes);
        assert.equal(admission.snapshot().privateBytes, 0);
        assert.equal(admission.snapshot().retiringBytes, 0);
        assert.equal(f.server.requests.length, 0);
        assert.equal(f.journal.unsyncedCount(), 1);
        assert.equal(a.engine.pendingChanges.has(pathAt(0)), true);
        admission.releaseResidentAfterFree(a.tree);
    } finally {
        stopGate?.resolve();
        f.control.statBulk = undefined;
        admission.close(); await f.close();
    }
}

async function compatibilityFullScanRetryClearsItsRepairGate() {
    const f = await fixture(), a = await f.create("full-scan-compatibility");
    const rebuild = a.tree.rebuild_from_entries_in_version.bind(a.tree);
    const failure = new Error("injected compatibility rebuild failure");
    let attempts = 0;
    Object.assign(a.engine, { rootRuntime: undefined, saveCachedRoot: async () => {} });
    Object.assign(a.tree, {
        build_from_entries: () => { throw new Error("compatibility Full Rescan used direct build"); },
        rebuild_from_entries_in_version(version: number, json: string) {
            attempts++;
            if (attempts === 1) throw failure;
            rebuild(version, json);
        },
    });
    a.engine.pushBlocked = true;
    try {
        assert.equal(await rejection(a.engine.fullScan()), failure);
        assert.equal(a.engine.rootRepairRequired, true);
        assert.equal(a.engine.rootRepairVersion, 1);
        assert.equal(a.engine.pushBlocked, true);
        assert.equal(attempts, 1);

        await a.engine.fullScan();
        assert.equal(attempts, 2);
        assert.equal(a.engine.rootRepairRequired, false,
            "successful compatibility retry retained a stale repair gate");
        assert.equal(a.engine.rootRepairVersion, null);
        assert.equal(a.engine.pushBlocked, false);
        assert.equal(a.engine.treeBaseRoot, f.base.treeBaseRoot);
        assert.equal(a.engine.getState(), "idle");
        assert.equal(f.server.requests.length, 0);
    } finally { await f.close(); }
}

async function compatibilityReconcileUsesBoundedReplacement() {
    const f = await fixture(), a = await f.create("reconcile-compatibility");
    const residentRootHash = a.tree.root_hash_hex.bind(a.tree);
    const rebuild = a.tree.rebuild_from_entries_in_version.bind(a.tree);
    const failure = new Error("injected reconcile rebuild failure");
    let present = false, attempts = 0;
    Object.assign(a.engine, { rootRuntime: undefined, saveCachedRoot: async () => {} });
    Object.assign(a.tree, {
        root_hash_hex: () => present ? residentRootHash() : null,
        total_files: () => present ? 1 : 0,
        build_from_entries: () => { throw new Error("reconcile used direct tree bootstrap"); },
        rebuild_from_entries_in_version(version: number, json: string) {
            attempts++;
            if (attempts === 1) throw failure;
            rebuild(version, json); present = true;
        },
    });
    Object.assign(a.engine.api, {
        checkChunks: async () => [],
        checkContent: async () => [],
        checkManifests: async () => [],
    });
    a.engine.pushBlocked = true;
    try {
        assert.equal(await rejection(a.engine.reconcileContent()), failure);
        assert.equal(a.engine.rootRepairRequired, true);
        assert.equal(a.engine.rootRepairVersion, 1);
        assert.equal(a.engine.pushBlocked, true);
        assert.equal(attempts, 1);

        const result = await a.engine.reconcileContent();
        assert.deepEqual(result, { smallUploaded: 0, largeUploaded: 0,
            treeChunksUploaded: 0, bytes: 0, deferred: 0, drifted: 0,
            readErrors: 0, uploadErrors: 0 });
        assert.equal(attempts, 2);
        assert.equal(a.engine.rootRepairRequired, false);
        assert.equal(a.engine.rootRepairVersion, null);
        assert.equal(a.engine.pushBlocked, true,
            "content reconcile unexpectedly unblocked root publication");
        assert.equal(a.engine.treeBaseRoot, f.base.treeBaseRoot);
        assert.equal(f.server.requests.length, 0);
    } finally { await f.close(); }
}

async function reconcileRetriesPublishedRetirementBeforeAnotherReplacement() {
    const f = await fixture(), a = await f.create("reconcile-retirement-retry");
    const plan = { nodePayloadBytes: 600, rangeEndpointPeakRequestedBytes: 400,
        rangeEndpointResidentRequestedBytes: 100, peakAdmissionBytes: 1_000,
        residentAdmissionBytes: 700 };
    const admission = new RootTreeResidentAdmission({
        capacityBytes: plan.peakAdmissionBytes + plan.residentAdmissionBytes,
    });
    const cleanupFailure = new Error("injected reconcile retirement failure");
    const residentRootHash = a.tree.root_hash_hex.bind(a.tree);
    let phase: "idle" | "input" | "build" | "plan" | "ready" | "retiring" = "idle";
    let residentGraphPresent = false, activeToken = 0, received = 0, completed = 0;
    let begins = 0, finishes = 0, retirementAttempts = 0;
    const retiredTokens: number[] = [];
    const stepReplacement = (token: number) => {
        assert.equal(token, activeToken);
        if (phase === "build") {
            phase = "plan"; completed++;
            return { done: false, units: 1, completed, phase: "plan ready" };
        }
        assert.equal(phase, "ready"); completed++;
        return { done: true, units: 1, completed, phase: "ready" };
    };
    Object.assign(a.engine, {
        rootRuntime: undefined,
        rootTreeResidentAdmission: admission,
        saveCachedRoot: async () => {},
    });
    Object.assign(a.tree, {
        tree_version: () => 2,
        candidate_revision: () => 0,
        root_hash_hex: () => residentGraphPresent ? residentRootHash() : null,
        total_files: () => residentGraphPresent ? 1 : 0,
        rebuild_from_entries_in_version: () => {
            throw new Error("reconcile used consuming replacement");
        },
        build_from_entries: () => {
            throw new Error("reconcile used synchronous tree bootstrap");
        },
        begin_replacement_rebuild_job(version: number) {
            assert.equal(version, 2); assert.equal(phase, "idle");
            activeToken = 200 + ++begins; phase = "input"; received = 0; completed = 0;
            return activeToken;
        },
        append_replacement_rebuild_job(token: number, offset: number, json: string) {
            assert.equal(token, activeToken); assert.equal(phase, "input"); assert.equal(offset, received);
            received += JSON.parse(json).length; return received;
        },
        finish_replacement_rebuild_job() { throw new Error("reconcile used consuming finish"); },
        start_replacement_rebuild_job(token: number) {
            assert.equal(token, activeToken); assert.equal(phase, "input"); assert.equal(received, 1);
            phase = "build";
        },
        step_replacement_rebuild_job: stepReplacement,
        step_replacement_rebuild_output_memory_v1_job: stepReplacement,
        replacement_rebuild_plan_job(token: number) {
            assert.equal(token, activeToken); assert.equal(phase, "plan");
            return { nodeCount: 1, leafCount: 1, internalCount: 0,
                nodePayloadBytes: plan.nodePayloadBytes, maxNodeBytes: plan.nodePayloadBytes,
                storedRootBytes: 128 };
        },
        replacement_rebuild_output_memory_plan_v1_job(token: number) {
            assert.equal(token, activeToken); assert.equal(phase, "plan");
            return { schema: 1, scope: "v2-replacement-output", ...plan };
        },
        resume_replacement_rebuild_job() { throw new Error("reconcile used legacy resume"); },
        resume_replacement_rebuild_output_memory_v1_job(token: number, node: number,
            endpointPeak: number, endpointResident: number) {
            assert.equal(token, activeToken); assert.equal(phase, "plan");
            assert.deepEqual([node, endpointPeak, endpointResident], [plan.nodePayloadBytes,
                plan.rangeEndpointPeakRequestedBytes, plan.rangeEndpointResidentRequestedBytes]);
            phase = "ready";
        },
        cancel_replacement_rebuild_job_deferred(token: number) {
            assert.equal(token, activeToken); assert.notEqual(phase, "idle"); phase = "retiring";
        },
        finish_replacement_rebuild_job_deferred(token: number) {
            assert.equal(token, activeToken); assert.equal(phase, "ready");
            a.tree.bump_committed_revision_for_test(); residentGraphPresent = true;
            finishes++; phase = "retiring";
        },
        step_tree_retirement(token: number) {
            assert.equal(token, activeToken); assert.equal(phase, "retiring");
            retiredTokens.push(token); retirementAttempts++;
            if (retirementAttempts === 1) throw cleanupFailure;
            phase = "idle"; return { done: true, units: 1, completed: 1 };
        },
        cancel_tree_job() { throw new Error("reconcile used consuming cancel"); },
    });
    Object.assign(a.engine.api, {
        checkChunks: async () => [],
        checkContent: async () => [],
        checkManifests: async () => [],
    });
    a.engine.pushBlocked = true;
    try {
        const failed = await rejection(a.engine.reconcileContent());
        assert.equal((failed as any).code, "ROOT_TREE_RETIREMENT_FAILED");
        assert.equal((failed as any).cleanupCause, cleanupFailure);
        assert.equal(a.engine.rootRepairRequired, true);
        assert.equal(a.engine.rootRepairVersion, 2);
        assert.equal(residentGraphPresent, true,
            "fixture did not fail after publishing the replacement graph");
        assert.deepEqual([begins, finishes, ...retiredTokens], [1, 1, 201]);
        const quarantined = admission.snapshot();
        assert.deepEqual([
            quarantined.ledger.capacityBytes, quarantined.ledger.usedBytes,
            quarantined.residentTrees, quarantined.privateAttempts,
            quarantined.retiringOwners, quarantined.privateBytes,
            quarantined.residentBytes, quarantined.retiringBytes,
        ], [1_700, 700, 1, 0, 1, 0, 700, 0]);

        const result = await a.engine.reconcileContent();
        assert.deepEqual(result, { smallUploaded: 0, largeUploaded: 0,
            treeChunksUploaded: 0, bytes: 0, deferred: 0, drifted: 0,
            readErrors: 0, uploadErrors: 0 });
        assert.deepEqual([begins, finishes, ...retiredTokens], [2, 2, 201, 201, 202],
            "retry started another replacement before draining the published owner");
        assert.equal(a.engine.rootRepairRequired, false);
        assert.equal(a.engine.rootRepairVersion, null);
        assert.equal(a.engine.pushBlocked, true,
            "content reconcile unexpectedly unblocked root publication");
        const recovered = admission.snapshot();
        assert.deepEqual([
            recovered.ledger.capacityBytes, recovered.ledger.usedBytes,
            recovered.residentTrees, recovered.privateAttempts,
            recovered.retiringOwners, recovered.privateBytes,
            recovered.residentBytes, recovered.retiringBytes,
        ], [1_700, 700, 1, 0, 0, 0, 700, 0]);
        assert.equal(f.server.requests.length, 0);
        admission.releaseResidentAfterFree(a.tree);
    } finally {
        admission.close(); await f.close();
    }
}

async function firstPullUsesReplacementForAnEmptyVolatileTree() {
    // Empty sync-base keeps recoverRootBeforeWork on its fast path, so this
    // reaches the distinct first-pull bootstrap gate rather than the cold-base
    // gate covered above.
    const f = await fixture(0);
    f.base.setTreeBaseRoot(null); await f.base.save();
    const a = await f.create("first-pull-replacement");
    const residentRootHash = a.tree.root_hash_hex.bind(a.tree);
    const replacement = a.tree.rebuild_from_entries_in_version.bind(a.tree);
    let residentGraphPresent = false;
    Object.assign(a.tree, {
        root_hash_hex: () => residentGraphPresent ? residentRootHash() : null,
        build_from_entries: () => {
            throw new Error("first pull used synchronous tree bootstrap");
        },
        rebuild_from_entries_in_version: (version: number, json: string) => {
            replacement(version, json);
            residentGraphPresent = true;
        },
    });
    try {
        assert.equal(a.engine.rootRuntime.lastSequence, 0);
        assert.equal(a.engine.rootRepairRequired, false);
        await a.engine.pullRemote();
        assert.equal(a.treeState.repairs, 1);
        assert.equal(a.engine.rootRepairRequired, false);
        assert.equal(a.engine.treeBaseRoot, f.base.treeBaseRoot);
        assert.equal(a.tree.root_hash_hex(), f.base.treeBaseRoot);
        assert.equal(f.base.verifiedBaseRequired, false);
        assert.deepEqual([f.server.diffs, f.server.requests.length], [1, 0]);
    } finally { await f.close(); }
}

async function nonemptyV2FirstPullWaitsForOutputAdmissionAndRecovers() {
    const f = await fixture(0);
    f.base.setTreeBaseRoot(null); await f.base.save();
    const a = await f.create("first-pull-v2-admission");
    const path = pathAt(0), target = "d".repeat(64), rootBytes = new Uint8Array([2]);
    f.source.set(path, 1);
    let diffs = 0;
    Object.assign(a.engine.api, {
        getDiff: async () => {
            diffs++;
            return [{ action: "added", path, hash: hashFor(1), size: 1, mtime_ms: 1 }];
        },
        getRoot: async () => rootBytes,
    });
    Object.assign(a.engine.wasm, {
        wasm_root_hash_from_bytes: () => target,
        wasm_root_version_from_bytes: () => 2,
    });

    const plan = { nodePayloadBytes: 600, rangeEndpointPeakRequestedBytes: 400,
        rangeEndpointResidentRequestedBytes: 100, peakAdmissionBytes: 1_000,
        residentAdmissionBytes: 700 };
    const admission = new RootTreeResidentAdmission({ capacityBytes: 999 });
    let phase: "idle" | "input" | "build" | "plan" | "ready" | "retiring" |
        "mutation" | "mutation-retiring" = "idle";
    let version = 1, present = false, received = 0, planReads = 0, resumes = 0;
    let finishes = 0, retirements = 0, mutationRetirements = 0, candidateRevision = 0;
    let mutationPayload = "";
    const finishCandidate = a.tree.finish_candidate_job.bind(a.tree);
    const candidateUpdate = a.tree.candidate_update_batch.bind(a.tree);
    const commitCandidate = a.tree.commit_candidate.bind(a.tree);
    const abortCandidate = a.tree.abort_candidate.bind(a.tree);
    const stepReplacement = (token: number) => {
        assert.equal(token, 81);
        if (phase === "build") {
            phase = "plan"; return { done: false, units: 1, completed: 1, phase: "plan ready" };
        }
        assert.equal(phase, "ready");
        return { done: true, units: 1, completed: 2, phase: "ready" };
    };
    Object.assign(a.tree, {
        tree_version: () => version,
        candidate_revision: () => candidateRevision,
        finish_candidate_job(token: number) {
            const result = finishCandidate(token); candidateRevision++; return result;
        },
        commit_candidate() { const result = commitCandidate(); candidateRevision++; return result; },
        abort_candidate() { const result = abortCandidate(); candidateRevision++; return result; },
        root_hash_hex: () => present ? target : null,
        total_files: () => present ? 1 : 0,
        build_from_entries: () => { throw new Error("V2 first pull used synchronous tree bootstrap"); },
        rebuild_from_entries_in_version: () => { throw new Error("V2 first pull used consuming replacement"); },
        begin_replacement_rebuild_job(requested: number) {
            assert.equal(requested, 2); assert.equal(phase, "idle");
            phase = "input"; received = 0; return 81;
        },
        append_replacement_rebuild_job(token: number, offset: number, json: string) {
            assert.equal(token, 81); assert.equal(phase, "input"); assert.equal(offset, received);
            received += JSON.parse(json).length; return received;
        },
        finish_replacement_rebuild_job() { throw new Error("V2 first pull used consuming finish"); },
        start_replacement_rebuild_job(token: number) {
            assert.equal(token, 81); assert.equal(phase, "input"); assert.equal(received, 1); phase = "build";
        },
        step_replacement_rebuild_job: stepReplacement,
        step_replacement_rebuild_output_memory_v1_job: stepReplacement,
        replacement_rebuild_plan_job(token: number) {
            assert.equal(token, 81); assert.equal(phase, "plan"); planReads++;
            return { nodeCount: 1, leafCount: 1, internalCount: 0,
                nodePayloadBytes: plan.nodePayloadBytes, maxNodeBytes: plan.nodePayloadBytes,
                storedRootBytes: 128 };
        },
        replacement_rebuild_output_memory_plan_v1_job(token: number) {
            assert.equal(token, 81); assert.equal(phase, "plan");
            return { schema: 1, scope: "v2-replacement-output", ...plan };
        },
        resume_replacement_rebuild_job() { throw new Error("V2 first pull used legacy resume"); },
        resume_replacement_rebuild_output_memory_v1_job(token: number, node: number,
            endpointPeak: number, endpointResident: number) {
            assert.equal(token, 81); assert.equal(phase, "plan");
            assert.deepEqual([node, endpointPeak, endpointResident], [plan.nodePayloadBytes,
                plan.rangeEndpointPeakRequestedBytes, plan.rangeEndpointResidentRequestedBytes]);
            resumes++; phase = "ready";
        },
        cancel_replacement_rebuild_job_deferred(token: number) {
            assert.equal(token, 81); assert.notEqual(phase, "idle"); phase = "retiring";
        },
        finish_replacement_rebuild_job_deferred(token: number) {
            assert.equal(token, 81); assert.equal(phase, "ready");
            present = true; version = 2; finishes++; phase = "retiring";
        },
        step_tree_retirement(token: number) {
            if (token === 82) {
                assert.equal(phase, "mutation-retiring"); mutationRetirements++; phase = "idle";
                return { done: true, units: 1, completed: 1 };
            }
            assert.equal(token, 81); assert.equal(phase, "retiring");
            retirements++; phase = "idle"; return { done: true, units: 1, completed: 1 };
        },
        cancel_tree_job() { throw new Error("V2 first pull used consuming cancel"); },
        begin_candidate_update_job(payload: string) {
            assert.equal(phase, "idle"); assert.equal(a.tree.has_candidate(), true);
            mutationPayload = payload; phase = "mutation"; return 82;
        },
        begin_candidate_delete_job() { throw new Error("unexpected delete mutation"); },
        finish_candidate_mutation_job() { throw new Error("V2 pull used consuming mutation finish"); },
        step_candidate_mutation_output_memory_v1_job(token: number, units: number) {
            assert.equal(token, 82); assert.equal(units, TREE_CANDIDATE_MUTATION_STEP_UNITS);
            assert.equal(phase, "mutation");
            // The recovery replacement already contains this identical row.
            // Native V2 reports a valid all-zero no-op without PlanReady.
            return { done: true, units: 0, completed: 0, remaining: 0, reachable: 0, phase: "ready" };
        },
        candidate_mutation_output_memory_plan_v1_job() { throw new Error("no-op mutation exposed a plan"); },
        resume_candidate_mutation_output_memory_v1_job() { throw new Error("no-op mutation resumed output"); },
        candidate_mutation_output_memory_ready_v1_job() { throw new Error("no-op mutation exposed Ready memory"); },
        finish_candidate_mutation_job_deferred(token: number) {
            assert.equal(token, 82); assert.equal(phase, "mutation");
            candidateUpdate(mutationPayload); candidateRevision++; phase = "mutation-retiring";
        },
        cancel_candidate_mutation_job_deferred(token: number) {
            assert.equal(token, 82); assert.equal(phase, "mutation"); phase = "mutation-retiring";
        },
    });
    a.engine.rootTreeResidentAdmission = admission;
    a.engine.saveCachedRoot = async () => {};
    let scopeAttempts = 0;
    a.engine.preparedScopeHash = "scope-v1";
    a.engine.preparedTransfers = {
        plan: {},
        scopeForTree: async (requested: number) => {
            assert.equal(requested, 2); scopeAttempts++;
            if (scopeAttempts === 1) throw new Error("injected V2 prepared-scope failure");
            return "scope-v2";
        },
    };
    try {
        await a.engine.pullRemote();
        assert.equal(a.engine.getState(), "error");
        assert.equal(a.engine.rootRepairRequired, true);
        assert.equal(a.engine.rootRepairVersion, 2);
        assert.equal(a.engine.treeBaseRoot, null);
        assert.equal(f.base.verifiedBaseRequired, true);
        assert.equal(f.base.getHash(path), hashFor(1));
        assert.deepEqual([diffs, planReads, resumes, finishes, retirements], [1, 1, 0, 0, 1]);
        assert.equal(admission.snapshot().residentBytes, 0);
        assert.equal(f.server.requests.length, 0,
            "refused first-pull output reached root publication");

        admission.setCapacity(plan.peakAdmissionBytes);
        assert.match(String(await rejection(a.engine.pullRemote())), /prepared-scope failure/);
        assert.equal(a.engine.rootRepairRequired, true);
        assert.equal(a.engine.rootRepairVersion, 2);
        assert.equal(a.engine.preparedScopeHash, "scope-v1");
        assert.equal(a.engine.treeBaseRoot, null);
        assert.equal(f.base.verifiedBaseRequired, true);
        assert.deepEqual([diffs, planReads, resumes, finishes, retirements, scopeAttempts], [1, 2, 1, 1, 2, 1]);
        assert.equal(admission.snapshot().residentBytes, plan.residentAdmissionBytes);
        assert.equal(f.server.requests.length, 0,
            "failed prepared-scope tail reached ordinary pull or publication");

        // A replacement needs its complete new peak alongside the published
        // resident until the old graph's retirement proves completion.
        admission.setCapacity(plan.peakAdmissionBytes + plan.residentAdmissionBytes);
        await a.engine.pullRemote();
        assert.equal(a.engine.getState(), "idle");
        assert.equal(a.engine.rootRepairRequired, false);
        assert.equal(a.engine.rootRepairVersion, null);
        assert.equal(a.tree.tree_version(), 2);
        assert.equal(a.tree.root_hash_hex(), target);
        assert.equal(a.engine.treeBaseRoot, target);
        assert.equal(a.engine.localRootHash, target);
        assert.equal(f.base.treeBaseRoot, target);
        assert.equal(f.base.verifiedBaseRequired, false);
        assert.equal(a.engine.preparedScopeHash, "scope-v2");
        assert.deepEqual([diffs, planReads, resumes, finishes, retirements, scopeAttempts,
            mutationRetirements], [2, 3, 2, 2, 3, 2, 1]);
        assert.equal(admission.snapshot().residentBytes, plan.residentAdmissionBytes);
        assert.equal(f.server.requests.length, 0);
        admission.releaseResidentAfterFree(a.tree);
    } finally {
        admission.close(); await f.close();
    }
}

async function firstPullRootProbeFailureReleasesItsOperation() {
    const f = await fixture(0), a = await f.create("first-pull-probe-failure");
    const failure = new Error("native root probe failed");
    Object.assign(a.tree, { root_hash_hex: () => { throw failure; } });
    try {
        await a.engine.pullRemote();
        assert.equal(a.engine.syncing, false);
        assert.equal(a.engine.getState(), "error");
        assert.equal(a.engine.getLastError()?.message.includes(failure.message), true);
        assert.deepEqual([f.server.diffs, f.server.requests.length, a.treeState.repairs], [0, 0, 0]);
    } finally { await f.close(); }
}

async function failedPullRebaseCannotAdvanceBaseOrCachedRoot() {
    const f = await fixture(), a = await f.create("pull-rebase-failure");
    const path = pathAt(0), oldBase = a.engine.treeBaseRoot, oldLocal = a.engine.localRootHash;
    const oldCache = f.cached();
    let rootFetches = 0;
    Object.assign(a.engine.api, {
        getDiff: async () => [{ action: "deleted", path }],
        getRoot: async () => { rootFetches++; return oldCache; },
    });
    Object.assign(a.engine.io, {
        deleteFile: async (deleted: string) => { assert.equal(deleted, path); f.source.delete(deleted); },
    });
    f.control.commitFailure = true;
    try {
        await a.engine.pullRemote();
        assert.equal(a.engine.rootRepairRequired, true,
            "failed pull rebase did not fence later ordinary work behind root repair");
        assert.equal(a.engine.rootRepairVersion, 1);
        assert.equal(a.engine.treeBaseRoot, oldBase);
        assert.equal(a.engine.localRootHash, oldLocal);
        assert.equal(f.base.treeBaseRoot, oldBase,
            "failed pull rebase advanced the durable merge base");
        assert.equal(f.base.getEntry(path), null,
            "test did not reach the applied sync-base state before rebase failed");
        assert.equal(a.treeHashAt(path), hashFor(1),
            "failed candidate transaction changed the committed tree");
        assert.deepEqual(f.cached(), oldCache,
            "failed pull rebase replaced the last accepted cached root");
        assert.equal(rootFetches, 0,
            "failed pull rebase fetched a server root that could be adopted");
        assert.deepEqual([a.treeState.commits, a.treeState.aborts], [0, 1]);
    } finally { await f.close(); }
}

async function completedPagedCheckpointRebuildsNonemptyStaleTree() {
    const f = await fixture(), a = await f.create("completed-paged-rebuild");
    const path = pathAt(0), target = f.base.treeBaseRoot!;
    let unexpectedPageRequest = false;
    f.base.setDiffPageCheckpoint({
        version: 1,
        vaultId: "vault",
        fromRoot: target,
        toRoot: target,
        nextCursorHex: null,
        complete: true,
        recordsSeen: 1,
        filesApplied: 1,
        bytesTotal: 1,
        downloaded: 1,
        bytesDownloaded: 1,
        deltasHadMtime: true,
    });
    await f.base.checkpoint();
    a.tree.build_from_entries(JSON.stringify([{
        path: "stale.md", hash: hashFor(9), mtime_ms: 9, size: 1,
    }]));
    Object.assign(a.engine.api, {
        supportsPagedDiff: async () => true,
        getDiffPage: async () => {
            unexpectedPageRequest = true;
            throw new Error("completed checkpoint requested another page");
        },
        getRootAt: async (_vault: string, root: string) => {
            assert.equal(root, target);
            return f.cached();
        },
    });
    try {
        assert.equal(a.tree.root_hash_hex() === target, false,
            "test did not install a nonempty stale volatile tree");
        await a.engine.pullRemote();
        assert.equal(unexpectedPageRequest, false);
        assert.equal(a.treeState.repairs, 1,
            "completed checkpoint bypassed the base-derived replacement path");
        assert.equal(a.treeHashAt(path), hashFor(1));
        assert.equal(a.treeHashAt("stale.md"), undefined);
        assert.equal(a.tree.root_hash_hex(), target);
        assert.equal(a.engine.treeBaseRoot, target);
        assert.equal(a.engine.localRootHash, target);
        assert.equal(f.base.diffPageCheckpoint, null,
            "verified completed checkpoint was not retired with base adoption");
        assert.equal(a.engine.rootRepairRequired, false);
    } finally { await f.close(); }
}

async function deferredFreshPullKeepsItsVerifiedDiffFallback() {
    const f = await fixture(0), a = await f.create("deferred-first-pull");
    const target = "f".repeat(64);
    a.engine.treeBaseRoot = null;
    a.engine.localRootHash = null;
    try {
        await a.engine.settlePullResult({
            newRootHash: target,
            newRootBytes: null,
            applied: 1,
            treeParity: false,
            deltasHadMtime: true,
            deferredCount: 1,
            localDeferredCount: 0,
            remoteOmissionCount: 0,
            downloaded: 1,
        });
        assert.equal(a.engine.treeBaseRoot, null);
        assert.equal(a.engine.localRootHash, null,
            "deferred first pull advanced the next diff past missing files");
        assert.equal(a.engine.lastPullServerRoot, target,
            "deferred server observation disappeared from diagnostics/push guard");
        await f.queue(a.engine, [pathAt(0)], 2);
        await a.engine.pushPending();
        assert.equal(f.server.requests.length, 0,
            "all-deferred first pull allowed an empty-parent publication");
        assert.equal(a.engine.pendingChanges.size, 1);
        assert.equal(f.journal.unsyncedCount(), 1);
    } finally { await f.close(); }
}

async function deferredFreshPullWithoutAHeadKeepsItsDurableFence() {
    const f = await fixture(0), a = await f.create("deferred-first-pull-no-head");
    a.engine.treeBaseRoot = null;
    a.engine.localRootHash = null;
    a.engine.lastPullServerRoot = null;
    try {
        // pull.ts persists this before applying a first non-empty remote diff.
        f.base.setVerifiedBaseRequired(true);
        await f.base.checkpoint();
        await a.engine.settlePullResult({
            newRootHash: null,
            newRootBytes: null,
            applied: 0,
            treeParity: null,
            deltasHadMtime: true,
            deferredCount: 1,
            localDeferredCount: 0,
            remoteOmissionCount: 0,
            downloaded: 0,
        });
        assert.equal(a.engine.lastPullServerRoot, null);
        const coldBeforePush = await f.cold();
        assert.equal(coldBeforePush.base.verifiedBaseRequired, true,
            "unverified remote-work fence did not survive reload");
        await f.queue(a.engine, [pathAt(0)], 2);
        await a.engine.pushPending();
        assert.equal(f.server.requests.length, 0,
            "head-fetch failure allowed an empty-parent publication");
        assert.equal(a.engine.pendingChanges.size, 1);
        assert.equal(f.journal.unsyncedCount(), 1);
    } finally { await f.close(); }
}

async function ignoredFreshPullCannotAdoptOrPublishMissingLeaves() {
    const f = await fixture(0), a = await f.create("ignored-first-pull");
    const target = "e".repeat(64);
    a.engine.treeBaseRoot = null;
    a.engine.localRootHash = null;
    f.base.setVerifiedBaseRequired(true);
    await f.base.checkpoint();
    try {
        await a.engine.settlePullResult({
            newRootHash: target,
            newRootBytes: null,
            applied: 0,
            treeParity: true,
            deltasHadMtime: true,
            deferredCount: 0,
            localDeferredCount: 0,
            remoteOmissionCount: 1,
            downloaded: 0,
        });
        assert.equal(a.engine.treeBaseRoot, null,
            "all-ignored fresh pull adopted a root with missing local leaves");
        assert.equal(f.base.verifiedBaseRequired, true);
        await f.queue(a.engine, [pathAt(0)], 2);
        await a.engine.pushPending();
        assert.equal(f.server.requests.length, 0,
            "ignored remote leaves were published as deletions from an empty parent");
        assert.equal(a.engine.pendingChanges.size, 1);
    } finally { await f.close(); }
}

async function run() {
    // Stub only Obsidian UI shell. Debounce intentionally never schedules a
    // background push: publication calls and root ownership remain actual.
    const globals = globalThis as any;
    const oldNotice = globals.__obsetyncTestNotice, oldDebounce = globals.__obsetyncTestDebounce;
    const notice = () => ({ setMessage() {}, hide() {} }), debounce = () => () => {};
    globals.__obsetyncTestNotice = notice; globals.__obsetyncTestDebounce = debounce;
    try {
        await boundedPublicationsPreserveUnselectedAndNewEvents();
        await everyOrdinaryEntryWaitsForOutcomeRecovery();
        await unsupportedPendingNeverFallsBackOrScans();
        await acceptedCutFailuresPreserveNewerWorkAndCurrentBase();
        await startupCapturesWhileRecoveryQueryWaits();
        await stopAndReplacementJoinRootNativeAndAcceptedTail();
        await ambiguousRetirementPreservesOnlyUnacknowledgedGenerations();
        await changedApiScopeNeverDispatchesAnUnownedRoot();
        await changedApiScopeFencesOrdinaryWorkButJoinsAcceptedTail();
        await standaloneLocalOmissionsRetireWithoutRootOrBase();
        await localOmissionAckKeepsNewCallbackAndVolatileHint();
        await ambiguousLocalAckReloadsItsActualCut();
        await omittedRenamePeerHoldsItsGroupWhileIndependentWorkPublishes();
        await materializationFailureCannotBecomeLocalOmissionAck();
        await localOmissionCutsAreBoundedAndWaitForFullBulkReview();
        await metadataMutexProtectsNativeAuditAndStillDrainsLiveEdits();
        await metadataErrorAndAbortReleaseOnlyAfterActualRead();
        await automaticDebounceOwnsEarlyScopeRejection();
        await continuationReadinessDoesNotDetachTheBacklog();
        await outputAdmissionRefusalRetriesOnlyWhenFactsChange();
        await initialPushAdmitsTheEmptyCommittedGraphBeforeFileWork();
        await stoppingDuringCommittedBootstrapCannotOpenCandidate();
        await stoppingDuringPullMutationRetiresBeforeOperationRelease();
        await failedPullMutationCleanupPrecedesFullScanInspection();
        await interruptedFullScanPublishesItsDurablePrefixAfterRestart();
        await fullScanRebuildWaitsForV2OutputAdmission();
        await compatibilityFullScanRetryClearsItsRepairGate();
        await compatibilityReconcileUsesBoundedReplacement();
        await reconcileRetriesPublishedRetirementBeforeAnotherReplacement();
        await firstPullUsesReplacementForAnEmptyVolatileTree();
        await nonemptyV2FirstPullWaitsForOutputAdmissionAndRecovers();
        await firstPullRootProbeFailureReleasesItsOperation();
        await failedPullRebaseCannotAdvanceBaseOrCachedRoot();
        await completedPagedCheckpointRebuildsNonemptyStaleTree();
        await deferredFreshPullKeepsItsVerifiedDiffFallback();
        await deferredFreshPullWithoutAHeadKeepsItsDurableFence();
        await ignoredFreshPullCannotAdoptOrPublishMissingLeaves();
        console.log("engine-root.test: 37 actual engine/root/journal/base integration suites passed (synthetic tree/transport)");
    } finally {
        if (globals.__obsetyncTestNotice === notice) {
            if (oldNotice === undefined) delete globals.__obsetyncTestNotice; else globals.__obsetyncTestNotice = oldNotice;
        }
        if (globals.__obsetyncTestDebounce === debounce) {
            if (oldDebounce === undefined) delete globals.__obsetyncTestDebounce; else globals.__obsetyncTestDebounce = oldDebounce;
        }
    }
}
void run().catch(error => { setTimeout(() => { throw error; }, 0); });
