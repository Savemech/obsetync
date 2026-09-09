import { ObsetyncSyncBase, SYNC_BASE_PENDING_LIMITS, SYNC_BASE_STORE_PATH } from "./sync-base";
import { MemorySegmentedIO, readStoreTestFrame, storeTestIOError } from "./segmented-store-test-io";
import { StoreRecoveryError, STORE_LIMITS } from "./segmented-store";
import { strict as assert } from "node:assert";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

class MemoryAdapter extends MemorySegmentedIO {
    appendCalls = 0;
    duringNextAppend: (() => void) | null = null;
    override async write(path: string, value: string): Promise<void> {
        if (path.includes("/wal-")) {
            this.appendCalls++;
            const hook = this.duringNextAppend;
            this.duringNextAppend = null;
            hook?.();
        }
        await super.write(path, value);
    }
}

async function run(): Promise<void> {
    const adapter = new MemoryAdapter();
    const app = { vault: { adapter } } as any;
    const first = new ObsetyncSyncBase(app);
    await first.load();
    first.setEntry("done.md", "a".repeat(64), 10, 4, 9);
    await first.checkpoint();
    check(adapter.appendCalls === 1, "batch checkpoint rewrote the full snapshot");

    // Cold restart before snapshot compaction replays the published cut.
    const recovered = new ObsetyncSyncBase(app);
    await recovered.load();
    check(recovered.getHash("done.md") === "a".repeat(64), "WAL checkpoint was not recovered");
    check(recovered.getTreeMtime("done.md") === 9, "server tree mtime was lost in WAL");
    check(
        recovered.refreshLocalMetadata("done.md", 50, 4),
        "verified local metadata refresh was ignored",
    );
    check(recovered.getEntry("done.md")?.mtime === 50, "local mtime was not refreshed");
    check(
        recovered.getTreeMtime("done.md") === 9,
        "local metadata refresh changed the committed tree mtime",
    );
    check(
        !recovered.refreshLocalMetadata("done.md", 50, 4),
        "identical local metadata created a redundant mutation",
    );
    const approval = recovered.approveBulkChange("vault", 10_000, 58, 1234);
    check(approval.changeLimit === 10_100, "bulk approval growth allowance is not bounded");
    check(
        recovered.bulkChangeApprovalCovers("vault", 10_100, 58),
        "persisted approval did not cover its bounded restart workload",
    );
    check(
        !recovered.bulkChangeApprovalCovers("vault", 10_101, 58),
        "persisted approval covered an oversized workload",
    );
    check(
        !recovered.bulkChangeApprovalCovers("vault", 10_000, 59),
        "persisted approval covered additional tracked deletions",
    );
    check(
        !recovered.bulkChangeApprovalCovers("another-vault", 10_000, 58),
        "persisted approval escaped its vault",
    );
    recovered.setDiffPageCheckpoint({
        version: 1,
        vaultId: "vault",
        fromRoot: "1".repeat(64),
        toRoot: "2".repeat(64),
        nextCursorHex: "abcd",
        complete: false,
        recordsSeen: 10,
        filesApplied: 9,
        bytesTotal: 123,
        downloaded: 4,
        bytesDownloaded: 88,
        deltasHadMtime: true,
    });
    await recovered.checkpoint();

    const cursorRecovered = new ObsetyncSyncBase(app);
    await cursorRecovered.load();
    check(cursorRecovered.diffPageCheckpoint?.nextCursorHex === "abcd",
        "diff page cursor was not recovered from WAL");
    check(cursorRecovered.diffPageCheckpoint?.filesApplied === 9,
        "diff page aggregate progress was lost");
    check(cursorRecovered.getEntry("done.md")?.mtime === 50,
        "refreshed local metadata was not recovered from WAL");
    check(cursorRecovered.getTreeMtime("done.md") === 9,
        "recovered metadata refresh lost the committed tree mtime");
    check(cursorRecovered.bulkChangeApproval?.approvedAt === 1234,
        "bulk approval was not recovered from WAL");
    check(cursorRecovered.clearDiffPageCheckpoint(), "diff page cursor did not clear");
    check(cursorRecovered.clearBulkChangeApproval(), "bulk approval did not clear");
    await cursorRecovered.checkpoint();
    const cursorCleared = new ObsetyncSyncBase(app);
    await cursorCleared.load();
    check(cursorCleared.diffPageCheckpoint === null, "cleared diff cursor resurrected after restart");
    check(cursorCleared.bulkChangeApproval === null, "cleared bulk approval resurrected after restart");

    cursorCleared.setEntry("later.md", "b".repeat(64), 20, 5);
    await cursorCleared.save();
    check(cursorCleared.entryCount() === 2, "snapshot compaction lost an entry");
    check(
        readStoreTestFrame(adapter, `${SYNC_BASE_STORE_PATH}/head.json`).walEnd + 1 ===
            readStoreTestFrame(adapter, `${SYNC_BASE_STORE_PATH}/head.json`).walStart,
        "snapshot compaction did not advance its WAL cut",
    );

    const compacted = new ObsetyncSyncBase(app);
    await compacted.load();
    check(compacted.getHash("done.md") === "a".repeat(64), "compacted snapshot lost old state");
    check(compacted.getHash("later.md") === "b".repeat(64), "compacted snapshot lost new state");

    const raceAdapter = new MemoryAdapter();
    const racing = new ObsetyncSyncBase({ vault: { adapter: raceAdapter } } as any);
    await racing.load();
    racing.setEntry("first.md", "c".repeat(64), 30, 6);
    raceAdapter.duringNextAppend = () => {
        racing.setEntry("during-append.md", "d".repeat(64), 31, 7);
    };
    await racing.checkpoint();
    const raceRecovered = new ObsetyncSyncBase({ vault: { adapter: raceAdapter } } as any);
    await raceRecovered.load();
    check(
        raceRecovered.getHash("during-append.md") === "d".repeat(64),
        "mutation arriving during final WAL append remained memory-only",
    );
}

const LEGACY = ".obsidian/plugins/obsetync/sync-base.json";
const LEGACY_WAL = ".obsidian/plugins/obsetync/sync-base.wal.ndjson";
const makeBase = (io: MemorySegmentedIO) => new ObsetyncSyncBase({ vault: { adapter: io } } as any);
const marker = () => ({ version: 1 as const, vaultId: "vault", fromRoot: "1".repeat(64), toRoot: "2".repeat(64),
    nextCursorHex: "abcd", complete: false, recordsSeen: 1, filesApplied: 1, bytesTotal: 3,
    downloaded: 1, bytesDownloaded: 3, deltasHadMtime: true });

async function legacyMigrationIsStrictAndPreservesOriginals(): Promise<void> {
    const io = new MemorySegmentedIO();
    const original = JSON.stringify({ lastSyncTimestamp: 10, treeBaseRoot: "a".repeat(64), entries: {
        "old.md": { hash: "b".repeat(64), mtime: 10.5, size: 3, treeMtime: 9 },
    } });
    io.files.set(LEGACY, original);
    const wal = `${JSON.stringify({ op: "set", path: "new.md", entry: { hash: "c".repeat(64), mtime: 11, size: 4 } })}\n`;
    io.files.set(LEGACY_WAL, wal);
    const base = makeBase(io);
    await base.load();
    assert.equal(base.entryCount(), 2);
    assert.equal(base.getTreeMtime("old.md"), 9);
    assert.equal(io.files.get(LEGACY), original);
    assert.equal(io.files.get(LEGACY_WAL), wal);
    base.removeEntry("old.md"); await base.checkpoint();
    const fresh = makeBase(io); await fresh.load();
    assert.equal(fresh.getEntry("old.md"), null, "legacy originals were replayed over new authoritative store");

    for (const bad of [
        '{"op":"future","value":1}\n',
        `${wal}{bad middle}\n${JSON.stringify({ op: "diff-page", value: marker() })}\n`,
        `${wal}{"op":"set"`,
    ]) {
        const invalid = new MemorySegmentedIO(); invalid.files.set(LEGACY, original); invalid.files.set(LEGACY_WAL, bad);
        await assert.rejects(makeBase(invalid).load(), StoreRecoveryError);
        assert.equal(invalid.files.size, 2, "failed migration wrote a guessed authoritative state");
    }
    const torn = new MemorySegmentedIO(); torn.files.set(LEGACY_WAL, `${wal}{"op":"diff-page"`);
    const prefix = makeBase(torn); await prefix.load();
    assert.equal(prefix.getHash("new.md"), "c".repeat(64)); assert.equal(prefix.diffPageCheckpoint, null);
    assert.equal(torn.files.get(LEGACY_WAL), `${wal}{"op":"diff-page"`, "migration deleted recovery source");
    const conflicting = new MemorySegmentedIO();
    conflicting.files.set(LEGACY, original); conflicting.files.set(`${LEGACY}.next`, original.replace('"lastSyncTimestamp":10', '"lastSyncTimestamp":11'));
    await assert.rejects(makeBase(conflicting).load(), /snapshots differ/);
}

async function readFailureAndRejectedWriteCannotPublishGuessedState(): Promise<void> {
    const seed = new MemorySegmentedIO(); const base = makeBase(seed); await base.load();
    base.setEntry("keep.md", "a".repeat(64), 1, 3); await base.checkpoint();
    for (const path of [...seed.files.keys()].filter(path => !path.endsWith(".bak"))) {
        const io = seed.clone();
        io.onBoundary = event => { if (event.path === path && event.method === "read") throw storeTestIOError("EIO"); };
        const failed = makeBase(io);
        await assert.rejects(failed.load(), StoreRecoveryError);
        assert.throws(() => failed.setEntry("wrong.md", "b".repeat(64), 2, 3), /validated recovery/);
        await assert.rejects(failed.save(), /validated recovery/);
    }
    const working = makeBase(seed); await working.load();
    working.setEntry("new.md", "b".repeat(64), 2, 3); working.setDiffPageCheckpoint(marker());
    let torn = false;
    seed.onBoundary = event => {
        if (!torn && event.method === "write" && event.phase === "before" && event.path.includes("/wal-")) {
            torn = true; seed.files.set(event.path, event.data!.slice(0, 20)); throw storeTestIOError();
        }
    };
    await assert.rejects(working.checkpoint());
    assert.equal(working.pendingAdmissionSnapshot().operations, 2,
        "failed WAL write released pending operation owners before recovery");
    assert.ok(working.pendingAdmissionSnapshot().estimatedBytes > 0,
        "failed WAL write erased retained pending metadata accounting");
    await assert.rejects(working.checkpoint(), /validated recovery/);
    seed.onBoundary = undefined;
    const recovered = makeBase(seed); await recovered.load();
    assert.deepEqual(recovered.pendingAdmissionSnapshot(), {
        operations: 0, estimatedBytes: 0,
        maxOperations: SYNC_BASE_PENDING_LIMITS.operations,
        maxEstimatedBytes: SYNC_BASE_PENDING_LIMITS.estimatedBytes,
    }, "validated reload did not start with empty pending admission");
    assert.equal(recovered.getHash("keep.md"), "a".repeat(64));
    assert.equal(recovered.getHash("new.md"), null); assert.equal(recovered.diffPageCheckpoint, null,
        "cursor escaped an unreferenced failed transaction");
    recovered.setEntry("new.md", "b".repeat(64), 2, 3); recovered.setDiffPageCheckpoint(marker());
    await recovered.checkpoint(); const complete = makeBase(seed); await complete.load();
    assert.equal(complete.getHash("new.md"), "b".repeat(64)); assert.equal(complete.diffPageCheckpoint?.nextCursorHex, "abcd");
}

async function capturedSnapshotDoesNotMixPostCutChanges(): Promise<void> {
    const io = new MemorySegmentedIO(); const base = makeBase(io); await base.load();
    for (let index = 0; index < 700; index++) base.setEntry(`note-${String(index).padStart(4, "0")}.md`, "a".repeat(64), 1, 3);
    await base.checkpoint();
    let changed = false;
    let publishedSnapshot: ReturnType<MemorySegmentedIO["snapshot"]> | undefined;
    io.onBoundary = event => {
        if (!changed && event.method === "write" && event.phase === "after" && event.path.includes("/page-")) {
            changed = true;
            base.removeEntry("note-0500.md");
            base.setEntry("later.md", "b".repeat(64), 2, 3);
            base.setDiffPageCheckpoint(marker());
        }
        if (changed && !publishedSnapshot && event.method === "rename" && event.phase === "after" && event.to?.endsWith("/head.json")) {
            publishedSnapshot = io.snapshot();
        }
    };
    await base.save(); io.onBoundary = undefined;
    assert.ok(publishedSnapshot);
    const atCut = makeBase(new MemorySegmentedIO(publishedSnapshot)); await atCut.load();
    assert.equal(atCut.entryCount(), 700);
    assert.equal(atCut.getHash("note-0500.md"), "a".repeat(64), "later delete changed an earlier snapshot page");
    assert.equal(atCut.getHash("later.md"), null); assert.equal(atCut.diffPageCheckpoint, null);
    const after = makeBase(io); await after.load();
    assert.equal(after.getHash("note-0500.md"), null); assert.equal(after.getHash("later.md"), "b".repeat(64));
    assert.equal(after.diffPageCheckpoint?.nextCursorHex, "abcd");
    const exposed = after.getEntry("later.md")!; exposed.hash = "c".repeat(64);
    assert.equal(after.getHash("later.md"), "b".repeat(64), "caller mutated a captured index value");
    for (const event of io.events) if (event.method === "write" && event.data) {
        assert.ok(new TextEncoder().encode(event.data).byteLength <= STORE_LIMITS.frameBytes, "snapshot serialized whole index");
    }
}

async function pendingAdmissionBoundsQueuedAndInflightOwners(): Promise<void> {
    const countIo = new MemoryAdapter();
    const countBounded = new ObsetyncSyncBase({ vault: { adapter: countIo } } as any, {
        pendingAdmission: { maxOperations: 2, maxEstimatedBytes: 64 * 1024 },
    });
    await countBounded.load();
    countBounded.setEntry("first.md", "a".repeat(64), 1, 1);
    let injected = false;
    countIo.duringNextAppend = () => {
        injected = true;
        countBounded.setEntry("during.md", "b".repeat(64), 2, 2);
        assert.throws(
            () => countBounded.setEntry("refused.md", "c".repeat(64), 3, 3),
            error => error instanceof StoreRecoveryError && error.code === "LIMIT",
            "in-flight WAL owner was omitted from pending count admission",
        );
        assert.equal(countBounded.getHash("refused.md"), null,
            "count refusal partially changed the visible authoritative index");
        const live = countBounded.pendingAdmissionSnapshot();
        assert.equal(live.operations, 2, "in-flight and newly queued owners were not both charged");
        assert.ok(live.estimatedBytes > 0 && live.estimatedBytes <= live.maxEstimatedBytes);
    };
    await countBounded.checkpoint();
    assert.ok(injected, "count admission fixture missed the live WAL boundary");
    assert.deepEqual(countBounded.pendingAdmissionSnapshot(), {
        operations: 0, estimatedBytes: 0, maxOperations: 2, maxEstimatedBytes: 64 * 1024,
    }, "successful drain did not release pending admission exactly");
    const countRecovered = makeBase(countIo); await countRecovered.load();
    assert.equal(countRecovered.getHash("first.md"), "a".repeat(64));
    assert.equal(countRecovered.getHash("during.md"), "b".repeat(64));
    assert.equal(countRecovered.getHash("refused.md"), null,
        "count-refused mutation reached durable recovery");

    const byteIo = new MemoryAdapter();
    const byteBounded = new ObsetyncSyncBase({ vault: { adapter: byteIo } } as any, {
        pendingAdmission: { maxOperations: 8, maxEstimatedBytes: 2048 },
    });
    await byteBounded.load();
    const firstLongPath = `${"a".repeat(300)}.md`;
    const refusedLongPath = `${"b".repeat(300)}.md`;
    byteBounded.setEntry(firstLongPath, "d".repeat(64), 4, 4);
    const beforeRefusal = byteBounded.pendingAdmissionSnapshot();
    const cutBeforeRefusal = byteBounded.captureTreeEntries();
    assert.equal(beforeRefusal.operations, 1);
    assert.ok(beforeRefusal.estimatedBytes > 0 && beforeRefusal.estimatedBytes <= 2048);
    assert.throws(
        () => byteBounded.setEntry(refusedLongPath, "e".repeat(64), 5, 5),
        error => error instanceof StoreRecoveryError && error.code === "LIMIT",
        "pending byte admission retained an oversized second owner",
    );
    assert.deepEqual(byteBounded.pendingAdmissionSnapshot(), beforeRefusal,
        "byte refusal changed pending owner accounting");
    assert.ok(cutBeforeRefusal.isCurrent(),
        "byte refusal invalidated an otherwise unchanged authoritative cut");
    assert.equal(byteBounded.getHash(refusedLongPath), null,
        "byte refusal partially changed the visible authoritative index");
    await byteBounded.checkpoint();
    byteBounded.setEntry(refusedLongPath, "e".repeat(64), 5, 5);
    await byteBounded.checkpoint();
    const byteRecovered = makeBase(byteIo); await byteRecovered.load();
    assert.equal(byteRecovered.getHash(firstLongPath), "d".repeat(64));
    assert.equal(byteRecovered.getHash(refusedLongPath), "e".repeat(64),
        "released byte admission did not allow a later durable retry");
    assert.throws(() => byteRecovered.approveBulkChange("v".repeat(4097), 1, 0),
        /invalid bulk change approval/, "unbounded approval id reached pending allocation");

    assert.throws(() => new ObsetyncSyncBase({ vault: { adapter: new MemoryAdapter() } } as any, {
        pendingAdmission: { maxOperations: SYNC_BASE_PENDING_LIMITS.operations + 1 },
    }), RangeError, "embedding options widened the production operation ceiling");
    assert.throws(() => new ObsetyncSyncBase({ vault: { adapter: new MemoryAdapter() } } as any, {
        pendingAdmission: { maxEstimatedBytes: SYNC_BASE_PENDING_LIMITS.estimatedBytes + 1 },
    }), RangeError, "embedding options widened the production byte ceiling");
}

async function initializedMetadataCannotOutrunFirstTransaction(): Promise<void> {
    for (const legacy of [false, true]) {
        const io = new MemorySegmentedIO();
        if (legacy) io.files.set(LEGACY, JSON.stringify({ lastSyncTimestamp: 0, entries: {} }));
        const base = makeBase(io); await base.load();
        base.setEntry("older.md", "a".repeat(64), 1, 3);
        let injected = false;
        let firstCut: ReturnType<MemorySegmentedIO["snapshot"]> | undefined;
        io.onBoundary = event => {
            if (!injected && event.method === "write" && event.phase === "after" && event.path.includes("/wal-")) {
                injected = true;
                base.setEntry("future.md", "b".repeat(64), 2, 3);
                base.setDiffPageCheckpoint(marker());
            }
            if (injected && !firstCut && event.method === "rename" && event.phase === "after" && event.to?.endsWith("/head.json")) {
                firstCut = io.snapshot();
            }
        };
        await base.checkpoint(); io.onBoundary = undefined;
        assert.ok(firstCut);
        const first = makeBase(new MemorySegmentedIO(firstCut)); await first.load();
        assert.equal(first.getHash("older.md"), "a".repeat(64));
        assert.equal(first.getHash("future.md"), null);
        assert.equal(first.diffPageCheckpoint, null, "shared initialization metadata published a cursor before its entry rows");
        const final = makeBase(io); await final.load();
        assert.equal(final.getHash("future.md"), "b".repeat(64));
        assert.equal(final.diffPageCheckpoint?.nextCursorHex, "abcd");
    }
}

void run()
    .then(legacyMigrationIsStrictAndPreservesOriginals)
    .then(readFailureAndRejectedWriteCannotPublishGuessedState)
    .then(capturedSnapshotDoesNotMixPostCutChanges)
    .then(pendingAdmissionBoundsQueuedAndInflightOwners)
    .then(initializedMetadataCannotOutrunFirstTransaction)
    .then(() => console.log(`sync-base.test: ${assertions} compatibility assertions + strict migration, failed IO, atomic cut and captured-snapshot regressions passed`))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
