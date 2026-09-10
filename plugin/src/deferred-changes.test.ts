import { strict as assert } from "node:assert";
import { DeferredChangeTracker, DeferredDependencyCaptureError, DeferredPartitionError,
    DEFERRED_CHANGE_RETRY_MS, DEFERRED_DEPENDENCY_CAPTURE_WORK_UNITS,
    DEFERRED_PARTITION_MAX_DEPENDENCIES, DEFERRED_PARTITION_WORK_UNITS } from "./deferred-changes";
import { DirtyPathSet, type DirtyFileChange } from "./dirty-set";
import { ObsetyncJournal } from "./journal";
import { MemorySegmentedIO } from "./segmented-store-test-io";
import type { DeferredPushChange, FileChange } from "./push";

const large: DirtyFileChange = {
    action: "created", path: "private/large.bin", size: 200, mtime: 1, journalId: 10,
};
const oldPath: DirtyFileChange = { action: "deleted", path: "private/old.bin", journalId: 12 };
const small: DirtyFileChange = { action: "created", path: "note.md", size: 2, mtime: 2, journalId: 11 };
const deferredLarge: DeferredPushChange = {
    path: large.path, reason: "source-too-large", requiredBytes: 640, capacityBytes: 320,
};
const deferredDelete: DeferredPushChange = {
    path: oldPath.path, reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0,
};

function settlementRestoresOnlyDeferredAndPreservesNewerEdits(): void {
    const tracker = new DeferredChangeTracker();
    const dirty = new DirtyPathSet();
    for (const change of [large, small, oldPath]) dirty.add(change, change.journalId);
    const queued = dirty.take();
    // This event arrives during upload. Restoring the old oversized snapshot
    // must not replace the newer state or acknowledge its newer watermark.
    dirty.add({ ...large, action: "modified", size: 3, mtime: 9, hash: "new-hash" }, 13);
    const outcome = tracker.settle(queued, queued, [deferredLarge, deferredDelete], "mobile:320", 1_000);
    assert.deepEqual(outcome.acknowledged, [{ path: small.path, throughId: 11 }]);
    dirty.restore(outcome.retained);
    assert.equal(dirty.size, 2);
    const next = dirty.take();
    const latest = next.find((change) => change.path === large.path)!;
    assert.equal(latest.journalId, 13);
    assert.equal(latest.size, 3);
    assert.equal(latest.hash, "new-hash");
    assert.equal(tracker.hasRunnable(next, "mobile:320", 1_001), true);
    const planned = tracker.partition(next, next, "mobile:320", 1_001);
    assert.equal(planned.ready.length, 2);
    assert.deepEqual(planned.deferred, []);
    const accepted = tracker.settle(next, next, [], "mobile:320", 1_002);
    assert.deepEqual(accepted.retained, []);
    assert.deepEqual(accepted.acknowledged.map((item) => item.throughId).sort(), [12, 13]);
    assert.equal(tracker.summary().count, 0);
}

function cooldownDoesNotSpinAndCanBeInvalidated(): void {
    const tracker = new DeferredChangeTracker();
    const initial = tracker.settle([large, oldPath], [large, oldPath], [deferredLarge, deferredDelete], "mobile:320", 0);
    for (let now = 3_000; now < DEFERRED_CHANGE_RETRY_MS; now += 3_000) {
        assert.equal(tracker.hasRunnable(initial.retained, "mobile:320", now), false);
    }
    assert.equal(tracker.hasRunnable(initial.retained, "mobile:320", DEFERRED_CHANGE_RETRY_MS), true);
    assert.equal(tracker.hasRunnable(initial.retained, "desktop:1024", 3_000), true);
    assert.equal(tracker.hasRunnable(initial.retained, "mobile:320", 3_000, true), true);
    assert.equal(tracker.hasRunnable([{ ...large, journalId: 20 }, oldPath], "mobile:320", 3_000), true);
    assert.equal(tracker.hasRunnable([{ ...large, mtime: 30 }, oldPath], "mobile:320", 3_000), true);
    const manual = tracker.partition(initial.retained, initial.retained, "mobile:320", 3_000, true);
    assert.equal(manual.ready.length, 2);
    assert.deepEqual(manual.deferred, []);
    // Only the journal is durable. A new process rechecks admission once.
    assert.equal(new DeferredChangeTracker().hasRunnable(initial.retained, "mobile:320", 3_000), true);
}

function unavailableRangeWaitsForCapabilityOrSourceChange(): void {
    const tracker = new DeferredChangeTracker();
    const unavailable: DeferredPushChange = {
        path: large.path,
        reason: "range-unavailable",
        requiredBytes: 640,
        capacityBytes: 320,
    };
    const initial = tracker.settle([large], [large], [unavailable], "mobile:rejected", 0);
    assert.equal(tracker.hasRunnable(initial.retained, "mobile:rejected", Number.MAX_SAFE_INTEGER), false,
        "elapsed time retried a stable missing mobile capability");
    assert.deepEqual(tracker.partition(initial.retained, initial.retained,
        "mobile:rejected", Number.MAX_SAFE_INTEGER).deferred, [unavailable]);
    assert.equal(tracker.hasRunnable(initial.retained, "mobile:qualified", 1), true,
        "a changed mobile capability did not invalidate suppression");
    assert.equal(tracker.hasRunnable([{ ...large, mtime: 2 }], "mobile:rejected", 1), true,
        "a changed source generation inherited capability suppression");
    assert.equal(tracker.hasRunnable(initial.retained, "mobile:rejected", 1, true), true,
        "an explicit retry could not override capability suppression");
    const summary = tracker.summary();
    assert.equal(summary.rangeUnavailable, 1);
    assert.equal(summary.sourceTooLarge, 0);
    assert.equal(summary.nextRetryAt, null);
}

function lazyReadinessStopsWithoutConsumingTheBacklog(): void {
    const tracker = new DeferredChangeTracker();
    const initial = tracker.settle([large, oldPath], [large, oldPath],
        [deferredLarge, deferredDelete], "mobile:320", 0);
    let visits = 0, closed = 0;
    function* withRunnableTail(): IterableIterator<DirtyFileChange> {
        try {
            visits++; yield initial.retained[0];
            visits++; yield small;
            throw new Error("readiness eagerly consumed the remaining backlog");
        } finally { closed++; }
    }
    assert.equal(tracker.hasRunnable(withRunnableTail(), "mobile:320", 1), true);
    assert.equal(visits, 2);
    assert.equal(closed, 1, "short circuit retained its captured iterator");
    visits = closed = 0;
    assert.equal(tracker.hasRunnable(withRunnableTail(), "mobile:320", 1, true), true);
    assert.equal(visits, 1, "force readiness consumed more than the nonempty witness");
    assert.equal(closed, 1);
    function* empty(): IterableIterator<DirtyFileChange> { /* An empty cut is never runnable. */ }
    assert.equal(tracker.hasRunnable(empty(), "mobile:320", 1), false);
    assert.equal(tracker.hasRunnable(empty(), "mobile:320", 1, true), false);
    assert.equal(tracker.hasRunnable(initial.retained.values(), "mobile:320", 1), false,
        "iterable conversion lost the global cooling-source gate");
    assert.equal(tracker.hasRunnable([initial.retained[1]].values(), "mobile:320", 1), true,
        "dependency-only state has nothing left to wait for");
    assert.equal(tracker.hasRunnable(initial.retained.values(), "mobile:320", DEFERRED_CHANGE_RETRY_MS), true);
    assert.equal(tracker.hasRunnable(initial.retained.values(), "desktop:1024", 1), true);
}

function unrelatedNotesProgressWhileAllDeletesStayHeld(): void {
    const tracker = new DeferredChangeTracker();
    const initial = tracker.settle([large, oldPath], [large, oldPath], [deferredLarge, deferredDelete], "mobile:320", 0);
    const unrelatedDelete: DirtyFileChange = { action: "deleted", path: "another-old.md", journalId: 14 };
    const queued = [...initial.retained, small, unrelatedDelete];
    assert.equal(tracker.hasRunnable(queued, "mobile:320", 20_000), true);
    const planned = tracker.partition(queued, queued, "mobile:320", 20_000);
    assert.deepEqual(planned.ready.map((change) => change.path), [small.path]);
    assert.deepEqual(planned.deferred.map((change) => change.path).sort(), [large.path, oldPath.path, unrelatedDelete.path].sort());
    const settled = tracker.settle(queued, queued, planned.deferred, "mobile:320", 20_000);
    assert.deepEqual(settled.acknowledged, [{ path: small.path, throughId: small.journalId }]);
    assert.equal(settled.retained.length, 3);
    assert.equal(tracker.summary().nextRetryAt, DEFERRED_CHANGE_RETRY_MS,
        "unrelated commits must not postpone an attachment forever");
    assert.equal(tracker.hasRunnable(settled.retained, "mobile:320", 23_000), false);
    const retried = tracker.settle(settled.retained, settled.retained, planned.deferred, "mobile:320", 60_001);
    assert.equal(tracker.summary().nextRetryAt, 60_001 + DEFERRED_CHANGE_RETRY_MS);
    assert.equal(triedCount(retried.retained), 3);

    // A stat change detected while another note triggers materialization may
    // invalidate admission even if no new event/journal ID was observed.
    const smaller = { ...large, size: 3, mtime: 5 };
    const changed = tracker.partition(settled.retained, [smaller, oldPath, unrelatedDelete], "mobile:320", 60_002);
    assert.equal(changed.ready.length, 3);
    assert.deepEqual(changed.deferred, []);
}

function triedCount(changes: readonly DirtyFileChange[]): number {
    assert.ok(changes.every((change) => !("data" in change)), "deferred queue retained file bytes");
    return changes.length;
}

function ignoredOrDeletedSourceReleasesDependencies(): void {
    const tracker = new DeferredChangeTracker();
    const initial = tracker.settle([large, oldPath], [large, oldPath], [deferredLarge, deferredDelete], "mobile:320", 0);
    const ignored = tracker.partition(initial.retained, [oldPath], "mobile:320", 1);
    assert.deepEqual(ignored.ready.map((change) => change.path), [oldPath.path]);
    const accepted = tracker.settle(initial.retained, [oldPath], [], "mobile:320", 1);
    assert.equal(accepted.retained.length, 0);
    assert.equal(accepted.acknowledged.length, 2);
    assert.equal(tracker.summary().count, 0);

    const next = tracker.settle([large, oldPath], [large, oldPath], [deferredLarge, deferredDelete], "mobile:320", 2);
    const deleted = tracker.partition(next.retained, [
        { path: large.path, action: "deleted" }, oldPath,
    ], "mobile:320", 3);
    assert.equal(deleted.ready.length, 2);
    assert.deepEqual(deleted.deferred, []);
}

async function cooperativePartitionMatchesSynchronousPolicy(): Promise<void> {
    const tracker = new DeferredChangeTracker();
    tracker.registerLegacyRename(oldPath.path, "missing/bridge.bin", 12);
    tracker.registerLegacyRename("missing/bridge.bin", "private/peer.bin", 13);
    const peer: DirtyFileChange = {
        action: "modified", path: "private/peer.bin", size: 3, mtime: 3, journalId: 13,
    };
    const initial = tracker.settle([large, oldPath], [large, oldPath],
        [deferredLarge, deferredDelete], "mobile:320", 0);
    const unrelatedDelete: DirtyFileChange = { action: "deleted", path: "unrelated-old.md", journalId: 14 };
    const queued = [...initial.retained, peer, small, unrelatedDelete];
    const materialized: FileChange[] = [...queued];
    const expected = tracker.partition(queued, materialized, "mobile:320", 1, false);
    let yields = 0;
    const actual = await tracker.partitionCooperatively(queued, materialized, "mobile:320", 1, false, {
        cooperate: async () => { yields++; },
    });
    assert.deepEqual(actual, expected, "cooperative partition drifted from the synchronous compatibility policy");
    assert.ok(actual.ready.every((change) => materialized.includes(change)), "cooperative ready rows lost caller identity");
    assert.deepEqual(actual.ready.map(change => change.path), [small.path]);
    assert.deepEqual(actual.deferred.map(change => change.path), [large.path, oldPath.path, unrelatedDelete.path, peer.path]);
    assert.equal(yields, 0, "small partition paid an unnecessary host-wait round trip");

    const forced = await tracker.partitionCooperatively(queued, materialized, "mobile:320", 1, true, {
        cooperate: async () => { yields++; },
    });
    assert.deepEqual(forced, tracker.partition(queued, materialized, "mobile:320", 1, true));
    assert.equal(forced.ready.length, materialized.length);
    assert.deepEqual(forced.deferred, []);

    const bulk = Array.from({ length: DEFERRED_PARTITION_WORK_UNITS + 1 }, (_, index): DirtyFileChange => ({
        action: "modified", path: `bulk/${index}.md`, mtime: 1, size: 1, journalId: 100 + index,
    }));
    let bulkYields = 0;
    const bulkResult = await tracker.partitionCooperatively(bulk, bulk, "mobile:320", 1, false, {
        cooperate: async () => { bulkYields++; },
    });
    assert.deepEqual(bulkResult, tracker.partition(bulk, bulk, "mobile:320", 1, false),
        "policy parity was lost after an actual cooperative boundary");
    assert.ok(bulkYields > 0);
}

async function cooperativePartitionBoundsHighDegreeTraversal(): Promise<void> {
    const tracker = new DeferredChangeTracker();
    const hub: DirtyFileChange = { ...large, path: "hub.bin" };
    tracker.settle([hub], [hub], [{ ...deferredLarge, path: hub.path }], "mobile:320", 0);
    const degree = DEFERRED_PARTITION_WORK_UNITS * 4;
    for (let index = 0; index < degree; index++) {
        tracker.registerLegacyRename(hub.path, `missing/${index}.md`, index + 1);
    }
    const independent: DirtyFileChange = { ...small, path: "independent.md" };
    let yields = 0;
    const result = await tracker.partitionCooperatively([hub, independent], [hub, independent], "mobile:320", 1, false, {
        cooperate: async () => { yields++; },
    });
    assert.deepEqual(result.ready.map(change => change.path), [independent.path]);
    assert.deepEqual(result.deferred.map(change => change.path), [hub.path]);
    // Both the high-degree hub and its reverse leaf edges are individual work
    // units; this would be much smaller if one adjacency were a hidden spread.
    assert.ok(yields >= 8, `high-degree closure crossed too few bounded waits: ${yields}`);
}

async function cooperativePartitionRejectsExcessiveReachableEdges(): Promise<void> {
    const tracker = new DeferredChangeTracker(), hub: DirtyFileChange = { ...large, path: "dense/0.md" };
    tracker.settle([hub], [hub], [{ ...deferredLarge, path: hub.path }], "mobile:320", 0);
    const endpoints = 363; // 65,703 undirected pairs: just beyond graph admission.
    let links = 0;
    outer: for (let left = 0; left < endpoints; left++) {
        for (let right = left + 1; right < endpoints; right++) {
            tracker.registerLegacyRename(`dense/${left}.md`, `dense/${right}.md`, ++links);
            if (links > DEFERRED_PARTITION_MAX_DEPENDENCIES) break outer;
        }
    }
    assert.equal(links, DEFERRED_PARTITION_MAX_DEPENDENCIES + 1);
    const independent: DirtyFileChange = { ...small, path: "dense-independent.md" };
    let yields = 0;
    await assert.rejects(tracker.partitionCooperatively(
        [hub, independent], [hub, independent], "mobile:320", 1, false,
        { cooperate: async () => { yields++; } },
    ), (error: unknown) => error instanceof DeferredPartitionError && error.code === "DEFERRED_PARTITION_ADMISSION");
    assert.ok(yields > 0, "oversized graph was rejected without bounded traversal checkpoints");
    assert.equal(tracker.summary().count, 1, "read-only graph rejection changed cooldown ownership");
    assert.equal(tracker.captureDependencies().length, DEFERRED_PARTITION_MAX_DEPENDENCIES + 1,
        "synchronous compatibility capture unexpectedly gained a new ceiling");
    await assert.rejects(tracker.captureDependenciesCooperatively({ cooperate: async () => {} }),
        (error: unknown) => error instanceof DeferredDependencyCaptureError &&
            error.code === "DEFERRED_DEPENDENCIES_ADMISSION");
}

async function cooperativePartitionRejectsMutationAbortAndYieldFailure(): Promise<void> {
    const rows = Array.from({ length: DEFERRED_PARTITION_WORK_UNITS + 1 }, (_, index): DirtyFileChange => ({
        action: "modified", path: `rows/${index}.md`, mtime: 1, size: 1, journalId: index + 1,
    }));
    const tracker = new DeferredChangeTracker();
    let mutated = false;
    await assert.rejects(tracker.partitionCooperatively(rows, rows, "test", 0, false, {
        cooperate: async () => {
            if (mutated) return;
            mutated = true;
            tracker.settle([rows[0]], [rows[0]], [{
                path: rows[0].path, reason: "source-too-large", requiredBytes: 2, capacityBytes: 1,
            }], "test", 0);
        },
    }), (error: unknown) => error instanceof DeferredPartitionError && error.code === "DEFERRED_PARTITION_CHANGED");
    assert.equal(mutated, true, "mutation test never entered a partition-owned host wait");
    assert.equal(tracker.summary().count, 1, "failed read-only partition changed the concurrent tracker mutation");

    const linked = new DeferredChangeTracker();
    await assert.rejects(linked.partitionCooperatively(rows, rows, "test", 0, false, {
        cooperate: async () => { linked.registerLegacyRename("rows/0.md", "rows/1.md", 1); },
    }), (error: unknown) => error instanceof DeferredPartitionError && error.code === "DEFERRED_PARTITION_CHANGED");

    const before = new AbortController(); before.abort(new Error("before partition"));
    await assert.rejects(new DeferredChangeTracker().partitionCooperatively(rows, rows, "test", 0, false, {
        cooperate: async () => {}, signal: before.signal,
    }), /before partition/);
    const during = new AbortController();
    await assert.rejects(new DeferredChangeTracker().partitionCooperatively(rows, rows, "test", 0, false, {
        cooperate: async () => { during.abort(new Error("during partition")); }, signal: during.signal,
    }), /during partition/);
    const failure = new Error("host yield failed");
    await assert.rejects(new DeferredChangeTracker().partitionCooperatively(rows, rows, "test", 0, false, {
        cooperate: async () => { throw failure; },
    }), error => error === failure);
    const resizedQueued = rows.slice(), resizedMaterialized: FileChange[] = [...rows];
    await assert.rejects(new DeferredChangeTracker().partitionCooperatively(
        resizedQueued, resizedMaterialized, "test", 0, false, {
            cooperate: async () => { resizedQueued.pop(); },
        },
    ), (error: unknown) => error instanceof DeferredPartitionError && error.code === "DEFERRED_PARTITION_CHANGED");
}

function planningRevisionAndRetainedHintsAreDetached(): void {
    const tracker = new DeferredChangeTracker(), empty = tracker.planningRevision;
    const settled = tracker.settle([large], [large], [deferredLarge], "mobile:320", 0);
    const stored = { ...settled.retained[0] };
    assert.notEqual(tracker.planningRevision, empty, "new cooldown record retained an old planning witness");
    const stable = tracker.planningRevision;
    tracker.settle([stored], [stored], [deferredLarge], "mobile:320", 1);
    assert.equal(tracker.planningRevision, stable, "semantically unchanged cooldown record rotated its witness");
    settled.retained[0].mtime = 999;
    const planned = tracker.partition([stored], [stored], "mobile:320", 2, false);
    assert.deepEqual(planned.ready, [], "caller mutation of retained output changed private cooldown state");
    assert.deepEqual(planned.deferred, [deferredLarge]);
    tracker.settle([stored], [stored], [], "mobile:320", 3);
    assert.notEqual(tracker.planningRevision, stable, "cooldown deletion retained an old planning witness");
    const cleared = tracker.planningRevision;
    tracker.settle([stored], [stored], [], "mobile:320", 4);
    assert.equal(tracker.planningRevision, cleared, "no-op cooldown deletion rotated its witness");
    const dependency = tracker.dependencyRevision;
    tracker.registerLegacyRename("a.md", "b.md", 1);
    assert.notEqual(tracker.dependencyRevision, dependency);
    assert.notEqual(tracker.planningRevision, cleared, "dependency mutation did not invalidate planning state");
}

function summaryIsAggregateAndDetached(): void {
    const tracker = new DeferredChangeTracker();
    const sameGeneration = { ...small, journalId: oldPath.journalId };
    const outcome = tracker.settle([large, oldPath, sameGeneration], [large, oldPath, sameGeneration],
        [deferredLarge, deferredDelete], "mobile:320", 5);
    assert.equal(outcome.retained.length, 3, "legacy shared-ID destination was acknowledged alone");
    assert.deepEqual(outcome.acknowledged, []);
    const summary = tracker.summary();
    assert.equal(summary.count, 3);
    assert.equal(summary.sourceTooLarge, 1);
    assert.equal(summary.dependentDeletes, 1);
    assert.equal(summary.dependentChanges, 1);
    assert.equal(summary.maxRequiredBytes, 640);
    assert.equal(summary.minCapacityBytes, 320);
    assert.equal(summary.nextRetryAt, 5 + DEFERRED_CHANGE_RETRY_MS);
    assert.ok(!JSON.stringify(summary).includes("private"));
    assert.ok(!("path" in summary) && !("hash" in summary) && !("journalId" in summary));
    summary.count = 0;
    assert.equal(tracker.summary().count, 3);
    assert.throws(() => tracker.settle([large], [large], [{ ...deferredLarge, requiredBytes: NaN }], "x", 0));
    assert.throws(() => tracker.settle([large], [large], [{ ...deferredLarge, path: "not-in-snapshot" }], "x", 0));
    assert.equal(tracker.summary().count, 3, "invalid outcome partially changed retry state");
}

class MemoryAdapter extends MemorySegmentedIO {
    failAppend = false;
    override async write(path: string, value: string): Promise<void> {
        if (this.failAppend && /\/wal-[^/]+\.json$/.test(path)) {
            await super.write(path, value.slice(0, 15));
            throw new Error("injected WAL append failure");
        }
        await super.write(path, value);
    }
}

async function legacyRenameAndNewerDestinationRemainDurableAcrossRestart(): Promise<void> {
    const adapter = new MemoryAdapter();
    const app = { vault: { adapter } } as any;
    const journal = new ObsetyncJournal(app);
    await journal.load();
    const largeId = await journal.append({ action: "created", path: large.path, ts: 1, synced: false });
    const renameId = await journal.append({ action: "renamed", path: "new.md", oldPath: "old.md", ts: 2, synced: false });
    const smallId = await journal.append({ action: "created", path: small.path, ts: 3, synced: false });
    const newerId = await journal.append({ action: "modified", path: "new.md", ts: 4, synced: false });
    const tracker = new DeferredChangeTracker();
    tracker.registerLegacyRename("old.md", "new.md", renameId);
    const queued: DirtyFileChange[] = [
        { ...large, journalId: largeId },
        { action: "deleted", path: "old.md", journalId: renameId },
        { action: "modified", path: "new.md", size: 3, mtime: 4, journalId: newerId },
        { ...small, journalId: smallId },
    ];
    const outcome = tracker.settle(queued, queued, [deferredLarge, {
        path: "old.md", reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0,
    }], "mobile:320", 0);
    assert.deepEqual(outcome.acknowledged, [{ path: small.path, throughId: smallId }]);
    await journal.acknowledge(outcome.acknowledged);
    tracker.commitSettlement(outcome);
    const restarted = new ObsetyncJournal(app);
    await restarted.load();
    assert.deepEqual(restarted.unsynced().map((entry) => entry.id), [largeId, renameId, newerId]);
    assert.ok(restarted.unsynced().some((entry) => entry.oldPath === "old.md"), "withheld old-path deletion vanished from WAL");
    assert.equal(outcome.retained.find((hint) => hint.path === "new.md")?.journalId, newerId);

    const accepted = tracker.settle(outcome.retained, outcome.retained, [], "desktop:1024", 1);
    await restarted.acknowledge(accepted.acknowledged);
    tracker.commitSettlement(accepted);
    const afterCommit = new ObsetyncJournal(app);
    await afterCommit.load();
    assert.deepEqual(afterCommit.unsynced(), []);
    assert.equal(tracker.summary().count, 0);
}

async function ackFailureLeavesAllGenerationsAvailableForRetry(): Promise<void> {
    const adapter = new MemoryAdapter();
    const journal = new ObsetyncJournal({ vault: { adapter } } as any);
    await journal.load();
    const largeId = await journal.append({ action: "created", path: large.path, ts: 1, synced: false });
    const smallId = await journal.append({ action: "created", path: small.path, ts: 2, synced: false });
    const queued = [{ ...large, journalId: largeId }, { ...small, journalId: smallId }];
    const tracker = new DeferredChangeTracker();
    const settled = tracker.settle(queued, queued, [deferredLarge], "mobile:320", 0);
    const dirty = new DirtyPathSet();
    dirty.restore(settled.retained);
    adapter.failAppend = true;
    await assert.rejects(journal.acknowledge(settled.acknowledged), /WAL append failure/);
    dirty.restore(queued); // Same catch-path restoration used by pushPending.
    assert.equal(dirty.size, 2);
    assert.equal(journal.unsynced().length, 2);
    assert.equal(tracker.hasRunnable(dirty.take(), "mobile:320", 1), true);
}

function chainedLegacyRenamesHoldTransitively(): void {
    const tracker = new DeferredChangeTracker();
    tracker.registerLegacyRename("a.md", "b.md", 1);
    tracker.registerLegacyRename("b.md", "c.md", 2);
    const queued: DirtyFileChange[] = [
        large,
        { path: "a.md", action: "deleted", journalId: 1 },
        { path: "b.md", action: "deleted", journalId: 2 },
        { path: "c.md", action: "created", journalId: 3, size: 1, mtime: 1 },
    ];
    const settled = tracker.settle(queued, queued, [deferredLarge, {
        path: "a.md", reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0,
    }], "mobile:320", 0);
    assert.equal(settled.retained.length, 4);
    assert.deepEqual(settled.acknowledged, []);
}

const linkedPair = (leftId: number, rightId: number): DirtyFileChange[] => [
    { path: "b.md", action: "deleted", journalId: leftId },
    { path: "c.md", action: "modified", journalId: rightId },
];
const heldLeft: DeferredPushChange = {
    path: "b.md", reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0,
};

function assertPairLinked(tracker: DeferredChangeTracker, leftId: number, rightId: number): void {
    const pair = linkedPair(leftId, rightId);
    const outcome = tracker.settle(pair, pair, [heldLeft], "test", 0);
    assert.deepEqual(outcome.acknowledged, [], "linked destination could erase the held source's sole WAL row");
    assert.deepEqual(outcome.retained.map((hint) => hint.path), ["b.md", "c.md"]);
}

function renameRetirementRequiresBothDurablyAcknowledgedGenerations(): void {
    const tracker = new DeferredChangeTracker();
    tracker.registerLegacyRename("b.md", "c.md", 2);
    const oldPair = linkedPair(1, 1);
    const old = tracker.settle(oldPair, oldPair, [], "test", 0);
    // Settling is preparation, not durability; even mutating the public output
    // cannot grant this older token authority to retire a newer dependency.
    for (const watermark of old.acknowledged) watermark.throughId = 999;
    tracker.commitSettlement(old);
    assertPairLinked(tracker, 2, 3);

    const pair = linkedPair(2, 3);
    const prepared = tracker.settle(pair, pair, [], "test", 0);
    assertPairLinked(tracker, 2, 4); // save/conflict-copy/ACK has not succeeded.
    tracker.commitSettlement(prepared);
    tracker.commitSettlement(prepared); // idempotent completion.
    const reusedPaths = linkedPair(10, 11);
    const later = tracker.settle(reusedPaths, reusedPaths, [heldLeft], "test", 1);
    assert.deepEqual(later.acknowledged, [{ path: "c.md", throughId: 11 }],
        "an acknowledged rename leaked a dependency onto later independent path reuse");

    tracker.registerLegacyRename("b.md", "c.md", 12);
    const partial = [linkedPair(12, 13)[1]];
    tracker.commitSettlement(tracker.settle(partial, partial, [], "test", 2));
    assertPairLinked(tracker, 12, 14);
}

function newerAndPendingRegistrationsSurviveOlderSettlementCompletion(): void {
    const tracker = new DeferredChangeTracker();
    tracker.registerLegacyRename("b.md", "c.md", 1);
    const firstPair = linkedPair(1, 1);
    const first = tracker.settle(firstPair, firstPair, [], "test", 0);
    // The durable generation is unknown until appendGroup's await returns.
    const firstRegistration = tracker.registerLegacyRename("b.md", "c.md");
    const secondRegistration = tracker.registerLegacyRename("c.md", "b.md");
    firstRegistration.confirm(2);
    firstRegistration.confirm(); // repeated confirmation must not poison it.
    tracker.commitSettlement(first);
    assertPairLinked(tracker, 2, 3);

    const pendingPair = linkedPair(2, 3);
    tracker.commitSettlement(tracker.settle(pendingPair, pendingPair, [], "test", 0));
    secondRegistration.confirm(4);
    assertPairLinked(tracker, 4, 5);

    const settledPair = linkedPair(4, 5);
    const settled = tracker.settle(settledPair, settledPair, [], "test", 0);
    tracker.registerLegacyRename("b.md", "c.md", 6);
    tracker.commitSettlement(settled);
    assertPairLinked(tracker, 6, 7);
}

function ambiguousRegistrationCannotBeRetiredByGuessedWatermarks(): void {
    const tracker = new DeferredChangeTracker();
    const registration = tracker.registerLegacyRename("b.md", "c.md");
    assert.throws(() => registration.confirm(NaN), /generation/);
    registration.confirm(); // a failed append may still have reached storage.
    tracker.registerLegacyRename("b.md", "c.md", 2);
    const pair = linkedPair(100, 101);
    tracker.commitSettlement(tracker.settle(pair, pair, [], "test", 0));
    assertPairLinked(tracker, 102, 103);
    assert.throws(() => tracker.registerLegacyRename("x.md", "y.md", 0), /generation/);
    assert.throws(() => tracker.registerLegacyRename("x.md", "y.md", Number.MAX_SAFE_INTEGER + 1), /generation/);
}

function dependencySnapshotsAreDetachedCurrentPathPairs(): void {
    const tracker = new DeferredChangeTracker();
    tracker.registerLegacyRename("a.md", "b.md", 1);
    tracker.registerLegacyRename("b.md", "a.md", 2);
    const registration = tracker.registerLegacyRename("b.md", "c.md");
    const first = tracker.captureDependencies();
    assert.equal(first.length, 2, "opposite adjacency or repeated rename inflated dependency snapshot");
    assert.deepEqual(first, [
        { left: "a.md", right: "b.md", generation: 2, pending: false, uncertain: false },
        { left: "b.md", right: "c.md", generation: 0, pending: true, uncertain: false },
    ]);
    registration.confirm(3);
    assert.equal(first[1].pending, true, "late confirmation mutated captured dependency state");
    assert.throws(() => (first[0] as any).generation = 100, TypeError);
    assert.throws(() => (first as any).push({}), TypeError);
    tracker.commitAcknowledged([{ path: "a.md", throughId: 2 }, { path: "b.md", throughId: 2 }]);
    assert.equal(first.length, 2, "retirement changed a stable caller snapshot");
    assert.deepEqual(tracker.captureDependencies(), [
        { left: "b.md", right: "c.md", generation: 3, pending: false, uncertain: false },
    ]);
    const ambiguous = tracker.registerLegacyRename("b.md", "c.md"); ambiguous.confirm();
    assert.equal(tracker.captureDependencies()[0].uncertain, true, "ambiguous append became a known dependency");
}

async function cooperativeDependencyCapturePreservesOrderAndBoundsWork(): Promise<void> {
    const tracker = new DeferredChangeTracker();
    tracker.registerLegacyRename("a.md", "b.md", 1);
    tracker.registerLegacyRename("b.md", "a.md", 2);
    const pending = tracker.registerLegacyRename("b.md", "c.md");
    const expected = tracker.captureDependencies();
    let smallYields = 0;
    const captured = await tracker.captureDependenciesCooperatively({ cooperate: async () => { smallYields++; } });
    assert.deepEqual(captured, expected);
    assert.equal(smallYields, 0, "small dependency snapshot paid an unnecessary host wait");
    assert.equal(Object.isFrozen(captured), true);
    assert.ok(captured.every(Object.isFrozen));
    assert.throws(() => (captured as any).push({}), TypeError);
    pending.confirm(3);
    assert.equal(captured[1].pending, true, "late confirmation mutated a detached cooperative snapshot");

    const highDegree = new DeferredChangeTracker(), degree = DEFERRED_DEPENDENCY_CAPTURE_WORK_UNITS * 2;
    for (let index = 0; index < degree; index++) {
        highDegree.registerLegacyRename("hub.md", `leaf/${index}.md`, index + 1);
    }
    let yields = 0;
    const bulk = await highDegree.captureDependenciesCooperatively({ cooperate: async () => { yields++; } });
    assert.deepEqual(bulk, highDegree.captureDependencies(), "cooperative dependency order drifted from compatibility capture");
    assert.equal(bulk.length, degree);
    // Outer hub/leaves and both adjacency directions are all work units.
    assert.ok(yields >= 6, `high-degree/reverse adjacency crossed too few checkpoints: ${yields}`);
}

async function cooperativeDependencyCaptureRejectsMutationAbortAndYieldFailure(): Promise<void> {
    const make = () => {
        const tracker = new DeferredChangeTracker();
        for (let index = 0; index < DEFERRED_DEPENDENCY_CAPTURE_WORK_UNITS; index++) {
            tracker.registerLegacyRename("hub.md", `leaf/${index}.md`, index + 1);
        }
        return tracker;
    };
    const changed = make(); let mutation = false;
    await assert.rejects(changed.captureDependenciesCooperatively({ cooperate: async () => {
        if (mutation) return;
        mutation = true; changed.registerLegacyRename("new-a.md", "new-b.md", 999);
    } }), (error: unknown) => error instanceof DeferredDependencyCaptureError &&
        error.code === "DEFERRED_DEPENDENCIES_CHANGED");
    assert.equal(mutation, true);

    const cooldown = make(), expected = cooldown.captureDependencies(); let settled = false;
    const stable = await cooldown.captureDependenciesCooperatively({ cooperate: async () => {
        if (settled) return;
        settled = true; cooldown.settle([large], [large], [deferredLarge], "test", 0);
    } });
    assert.deepEqual(stable, expected, "cooldown-only mutation invalidated a structural snapshot");
    assert.equal(cooldown.summary().count, 1);

    const before = new AbortController(); before.abort(new Error("before dependency capture"));
    await assert.rejects(make().captureDependenciesCooperatively({ cooperate: async () => {}, signal: before.signal }),
        /before dependency capture/);
    const during = new AbortController();
    await assert.rejects(make().captureDependenciesCooperatively({
        cooperate: async () => { during.abort(new Error("during dependency capture")); }, signal: during.signal,
    }), /during dependency capture/);
    const failure = new Error("dependency host yield failed");
    await assert.rejects(make().captureDependenciesCooperatively({ cooperate: async () => { throw failure; } }),
        error => error === failure);

    const admitted = new DeferredChangeTracker();
    admitted.registerLegacyRename("a.md", "b.md", 1);
    admitted.registerLegacyRename("c.md", "d.md", 2);
    assert.equal((await admitted.captureDependenciesCooperatively({
        cooperate: async () => {}, maxDependencies: 2,
    })).length, 2);
    await assert.rejects(admitted.captureDependenciesCooperatively({
        cooperate: async () => {}, maxDependencies: 1,
    }), (error: unknown) => error instanceof DeferredDependencyCaptureError &&
        error.code === "DEFERRED_DEPENDENCIES_ADMISSION");

    const reversePrefix = new DeferredChangeTracker();
    for (let index = 0; index < 2_048; index++) {
        reversePrefix.registerLegacyRename(`reverse/${index}.md`, "early-hub.md", index + 1);
    }
    let reverseYields = 0;
    await assert.rejects(reversePrefix.captureDependenciesCooperatively({
        cooperate: async () => { reverseYields++; }, maxDependencies: 16,
    }), (error: unknown) => error instanceof DeferredDependencyCaptureError &&
        error.code === "DEFERRED_DEPENDENCIES_ADMISSION");
    assert.equal(reverseYields, 0, "reverse-only adjacency tail crossed work checkpoints before admission refusal");
    for (const maxDependencies of [0, -1, 1.5, DEFERRED_PARTITION_MAX_DEPENDENCIES + 1]) {
        await assert.rejects(admitted.captureDependenciesCooperatively({ cooperate: async () => {}, maxDependencies }),
            (error: unknown) => error instanceof DeferredDependencyCaptureError &&
                error.code === "DEFERRED_DEPENDENCIES_ADMISSION");
    }
}

function dependencyRevisionTracksOnlyActualDurableLinkChanges(): void {
    const tracker = new DeferredChangeTracker();
    const empty = tracker.dependencyRevision;
    assert.equal(tracker.dependencyRevision, empty, "reading revision created a new witness");
    assert.equal(Object.isFrozen(empty), true);
    assert.notEqual(new DeferredChangeTracker().dependencyRevision, empty, "revision leaked across owners");
    assert.equal(tracker.hasLinkedPath("b.md"), false);
    tracker.registerLegacyRename("b.md", "b.md", 1).confirm(2);
    assert.equal(tracker.dependencyRevision, empty, "same-path no-op invalidated dependencies");
    assert.equal(tracker.hasLinkedPath("b.md"), false);
    assert.throws(() => tracker.registerLegacyRename("b.md", "c.md", 0), /generation/);
    assert.equal(tracker.dependencyRevision, empty, "invalid registration partially created an edge");

    const durable = tracker.registerLegacyRename("b.md", "c.md", 2);
    const created = tracker.dependencyRevision;
    assert.notEqual(created, empty);
    assert.equal(Object.isFrozen(created), true);
    assert.equal(tracker.hasLinkedPath("b.md"), true);
    assert.equal(tracker.hasLinkedPath("c.md"), true);
    assert.equal(tracker.hasLinkedPath("other.md"), false);
    tracker.registerLegacyRename("b.md", "c.md", 2);
    tracker.registerLegacyRename("c.md", "b.md", 2);
    tracker.registerLegacyRename("c.md", "b.md", 1);
    durable.confirm(99); // Already-durable registrations have no pending owner.
    assert.equal(tracker.dependencyRevision, created, "unchanged durable edge invalidated the reviewed graph");

    tracker.captureDependencies();
    tracker.summary();
    tracker.hasRunnable([small], "test", 0);
    tracker.partition([small], [small], "test", 0);
    const cooled = tracker.settle([large], [large], [deferredLarge], "test", 0);
    tracker.commitSettlement(cooled);
    assert.equal(tracker.dependencyRevision, created, "retry state or a read changed structural revision");

    tracker.registerLegacyRename("c.md", "b.md", 3);
    assert.notEqual(tracker.dependencyRevision, created, "new durable generation reused an old graph witness");
    assert.deepEqual(tracker.captureDependencies(), [
        { left: "b.md", right: "c.md", generation: 3, pending: false, uncertain: false },
    ]);
}

function dependencyRevisionTracksPendingAndAmbiguousRegistrations(): void {
    const tracker = new DeferredChangeTracker();
    const empty = tracker.dependencyRevision;
    const first = tracker.registerLegacyRename("b.md", "c.md");
    const onePending = tracker.dependencyRevision;
    assert.notEqual(onePending, empty);
    assert.equal(tracker.hasLinkedPath("b.md"), true);
    assert.equal(tracker.hasLinkedPath("c.md"), true);
    const second = tracker.registerLegacyRename("c.md", "b.md");
    const twoPending = tracker.dependencyRevision;
    assert.notEqual(twoPending, onePending, "second pending registration reused the first owner's witness");
    assert.throws(() => first.confirm(NaN), /generation/);
    assert.equal(tracker.dependencyRevision, twoPending, "invalid confirmation consumed its pending owner");
    tracker.commitAcknowledged([{ path: "b.md", throughId: 99 }, { path: "c.md", throughId: 99 }]);
    assert.equal(tracker.dependencyRevision, twoPending, "guessed ACK retired a pending edge");

    first.confirm(2);
    const firstConfirmed = tracker.dependencyRevision;
    assert.notEqual(firstConfirmed, twoPending);
    assert.equal(tracker.captureDependencies()[0].pending, true, "one confirmation consumed both pending owners");
    first.confirm();
    assert.equal(tracker.dependencyRevision, firstConfirmed, "duplicate confirmation changed dependencies");
    second.confirm(1); // Older completion clears pending, but never lowers the edge's generation.
    const bothConfirmed = tracker.dependencyRevision;
    assert.notEqual(bothConfirmed, firstConfirmed);
    assert.deepEqual(tracker.captureDependencies(), [
        { left: "b.md", right: "c.md", generation: 2, pending: false, uncertain: false },
    ]);

    const ambiguous = tracker.registerLegacyRename("b.md", "c.md");
    const pendingAgain = tracker.dependencyRevision;
    assert.notEqual(pendingAgain, bothConfirmed);
    ambiguous.confirm();
    const uncertain = tracker.dependencyRevision;
    assert.notEqual(uncertain, pendingAgain);
    assert.equal(tracker.captureDependencies()[0].uncertain, true);
    assert.equal(tracker.captureDependencies()[0].pending, false);
    ambiguous.confirm(100);
    tracker.registerLegacyRename("b.md", "c.md", 2);
    tracker.commitAcknowledged([{ path: "b.md", throughId: 100 }, { path: "c.md", throughId: 100 }]);
    assert.equal(tracker.dependencyRevision, uncertain, "repeated confirmation/guessed ACK cleared an uncertain edge");
    assert.equal(tracker.hasLinkedPath("b.md"), true);
    assert.equal(tracker.hasLinkedPath("c.md"), true);
}

function dependencyRevisionRetiresOnlyCurrentAcknowledgedEdges(): void {
    const tracker = new DeferredChangeTracker();
    tracker.registerLegacyRename("b.md", "c.md", 2);
    const created = tracker.dependencyRevision;
    const oldPair = linkedPair(1, 1);
    const old = tracker.settle(oldPair, oldPair, [], "test", 0);
    const pair = linkedPair(2, 2);
    const prepared = tracker.settle(pair, pair, [], "test", 0);
    assert.equal(tracker.dependencyRevision, created, "settle retired an edge before its ACK");
    tracker.commitSettlement(old);
    tracker.commitSettlement({ ...prepared }); // A copied public result has no private retirement authority.
    tracker.commitAcknowledged([{ path: "c.md", throughId: 2 }]);
    tracker.commitAcknowledged([{ path: "b.md", throughId: 1 }, { path: "c.md", throughId: 2 }]);
    assert.equal(tracker.dependencyRevision, created, "old/foreign/one-sided completion invalidated dependencies");

    tracker.registerLegacyRename("b.md", "c.md", 3);
    const advanced = tracker.dependencyRevision;
    tracker.commitSettlement(prepared);
    assert.equal(tracker.dependencyRevision, advanced, "in-flight older settlement retired a newer edge");
    tracker.registerLegacyRename("c.md", "d.md", 4);
    const chain = tracker.dependencyRevision;
    assert.throws(() => tracker.commitAcknowledged([
        { path: "b.md", throughId: 3 }, { path: "c.md", throughId: 3 }, { path: "d.md", throughId: NaN },
    ]), /generation/);
    assert.equal(tracker.dependencyRevision, chain, "malformed ACK partially retired a valid prefix");
    tracker.commitAcknowledged([{ path: "b.md", throughId: 3 }, { path: "c.md", throughId: 3 }]);
    const firstRetired = tracker.dependencyRevision;
    assert.notEqual(firstRetired, chain);
    assert.equal(tracker.hasLinkedPath("b.md"), false);
    assert.equal(tracker.hasLinkedPath("c.md"), true, "removing one edge forgot its surviving adjacent edge");
    assert.equal(tracker.hasLinkedPath("d.md"), true);
    tracker.commitAcknowledged([{ path: "b.md", throughId: 3 }, { path: "c.md", throughId: 3 }]);
    assert.equal(tracker.dependencyRevision, firstRetired, "repeated ACK invalidated an unchanged graph");
    tracker.commitAcknowledged([{ path: "c.md", throughId: 4 }, { path: "d.md", throughId: 4 }]);
    const allRetired = tracker.dependencyRevision;
    assert.notEqual(allRetired, firstRetired);
    assert.notEqual(allRetired, created, "empty graph reused an old witness after structural changes");
    assert.equal(tracker.hasLinkedPath("c.md"), false);
    assert.equal(tracker.hasLinkedPath("d.md"), false);
    assert.deepEqual(tracker.captureDependencies(), []);
    tracker.commitSettlement(prepared);
    assert.equal(tracker.dependencyRevision, allRetired);
}

async function dependencyRevisionWaitsForActualJournalAck(fail: boolean): Promise<void> {
    const adapter = new MemoryAdapter();
    const app = { vault: { adapter } } as any;
    let journal = new ObsetyncJournal(app);
    await journal.load();
    const generation = await journal.append({ action: "renamed", oldPath: "b.md", path: "c.md", ts: 1, synced: false });
    const tracker = new DeferredChangeTracker();
    tracker.registerLegacyRename("b.md", "c.md", generation);
    const revision = tracker.dependencyRevision;
    const pair = linkedPair(generation, generation);
    const settlement = tracker.settle(pair, pair, [], "test", 0);
    let entered!: () => void, release!: () => void;
    const started = new Promise<void>((resolve) => { entered = resolve; });
    const gate = new Promise<void>((resolve) => { release = resolve; });
    adapter.onBoundary = async (event) => {
        if (event.method !== "write" || event.phase !== "before" || !/\/wal-[^/]+\.json$/.test(event.path)) return;
        adapter.onBoundary = undefined;
        entered();
        await gate;
    };
    adapter.failAppend = fail;
    const ack = journal.acknowledge(settlement.acknowledged);
    // Attach rejection ownership before opening the fault-injected write.
    const completion = fail ? assert.rejects(ack, /WAL append failure/) : ack;
    await started;
    assert.equal(tracker.dependencyRevision, revision, "pending native ACK changed dependencies");
    assert.equal(tracker.hasLinkedPath("b.md"), true);
    assert.equal(tracker.hasLinkedPath("c.md"), true);
    release();
    await completion;
    assert.equal(tracker.dependencyRevision, revision, "journal IO itself prematurely retired a tracker edge");
    if (fail) {
        assert.equal(journal.unsynced().length, 1, "failed ACK removed pending journal ownership");
        assert.equal(tracker.hasLinkedPath("b.md"), true);
        adapter.failAppend = false;
        journal = new ObsetyncJournal(app);
        await journal.load();
        assert.deepEqual(journal.unsynced().map((entry) => entry.id), [generation], "restart lost the failed-ACK rename");
        assert.equal(tracker.dependencyRevision, revision, "recovery alone changed the session edge");
        await journal.acknowledge(settlement.acknowledged);
        assert.equal(tracker.dependencyRevision, revision, "successful retry skipped explicit settlement completion");
    }
    tracker.commitSettlement(settlement);
    const retired = tracker.dependencyRevision;
    assert.notEqual(retired, revision);
    assert.equal(tracker.hasLinkedPath("b.md"), false);
    assert.equal(tracker.hasLinkedPath("c.md"), false);
    tracker.commitSettlement(settlement);
    assert.equal(tracker.dependencyRevision, retired, "duplicate durable completion invalidated dependencies");
    const restarted = new ObsetyncJournal(app);
    await restarted.load();
    assert.deepEqual(restarted.unsynced(), [], "retired link did not correspond to a durable journal ACK");
}

async function run(): Promise<void> {
    settlementRestoresOnlyDeferredAndPreservesNewerEdits();
    cooldownDoesNotSpinAndCanBeInvalidated();
    unavailableRangeWaitsForCapabilityOrSourceChange();
    lazyReadinessStopsWithoutConsumingTheBacklog();
    unrelatedNotesProgressWhileAllDeletesStayHeld();
    ignoredOrDeletedSourceReleasesDependencies();
    await cooperativePartitionMatchesSynchronousPolicy();
    await cooperativePartitionBoundsHighDegreeTraversal();
    await cooperativePartitionRejectsExcessiveReachableEdges();
    await cooperativePartitionRejectsMutationAbortAndYieldFailure();
    planningRevisionAndRetainedHintsAreDetached();
    summaryIsAggregateAndDetached();
    chainedLegacyRenamesHoldTransitively();
    renameRetirementRequiresBothDurablyAcknowledgedGenerations();
    newerAndPendingRegistrationsSurviveOlderSettlementCompletion();
    ambiguousRegistrationCannotBeRetiredByGuessedWatermarks();
    dependencySnapshotsAreDetachedCurrentPathPairs();
    await cooperativeDependencyCapturePreservesOrderAndBoundsWork();
    await cooperativeDependencyCaptureRejectsMutationAbortAndYieldFailure();
    dependencyRevisionTracksOnlyActualDurableLinkChanges();
    dependencyRevisionTracksPendingAndAmbiguousRegistrations();
    dependencyRevisionRetiresOnlyCurrentAcknowledgedEdges();
    await legacyRenameAndNewerDestinationRemainDurableAcrossRestart();
    await ackFailureLeavesAllGenerationsAvailableForRetry();
    await dependencyRevisionWaitsForActualJournalAck(false);
    await dependencyRevisionWaitsForActualJournalAck(true);
    console.log("deferred-changes.test: retained generations, bounded retries, durable rename dependencies and cooperative preparation gates passed");
}

let completed = false;
process.once("beforeExit", () => {
    if (!completed) {
        console.error("deferred-changes.test: pending test work did not complete");
        process.exitCode = 1;
    }
});
void run().then(() => { completed = true; }).catch((error) => {
    completed = true;
    console.error(error);
    process.exitCode = 1;
});
