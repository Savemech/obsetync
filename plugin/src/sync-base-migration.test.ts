import { strict as assert } from "node:assert";
import { ObsetyncSyncBase, SYNC_BASE_STORE_PATH } from "./sync-base";
import { StoreRecoveryError, segmentedMigrationPaths } from "./segmented-store";
import { MemorySegmentedIO, storeTestIOError } from "./segmented-store-test-io";

const LEGACY = ".obsidian/plugins/obsetync/sync-base.json";
const HEAD = SYNC_BASE_STORE_PATH + "/head.json";
const hash = "1".repeat(64);
const root = "2".repeat(64);
const baseFor = (io: MemorySegmentedIO) => new ObsetyncSyncBase({ vault: { adapter: io } } as any);
function fixture(): MemorySegmentedIO {
    const io = new MemorySegmentedIO();
    // A real existing legacy file necessarily has an existing parent.
    io.directories.add(".obsidian/plugins/obsetync");
    io.files.set(LEGACY, JSON.stringify({ lastSyncTimestamp: 17, treeBaseRoot: root,
        entries: Object.fromEntries(Array.from({ length: 601 }, (_, index) => ["note-" + index + ".md",
            { hash, mtime: 1, size: index, treeMtime: 2 }])) }));
    return io;
}

async function interrupted(): Promise<MemorySegmentedIO> {
    const io = fixture();
    let stopped = false;
    io.onBoundary = (event, adapter) => {
        if (event.method === "write" && event.phase === "before" && event.path.includes("/page-") && event.path.endsWith("-1.json")) {
            stopped = true;
            adapter.files.set(event.path, event.data!.slice(0, 19));
            throw storeTestIOError("ENOSPC");
        }
    };
    await assert.rejects(baseFor(io).load());
    io.onBoundary = undefined;
    assert(stopped && !io.files.has(HEAD));
    assert(io.files.has(segmentedMigrationPaths(SYNC_BASE_STORE_PATH).intent));
    return io;
}

async function run(): Promise<void> {
    const io = await interrupted();
    const originals = io.files.get(LEGACY);
    io.files.set(SYNC_BASE_STORE_PATH + "/unrelated-evidence.txt", "do not discard");
    io.events.length = 0;
    const base = baseFor(io);
    await base.load();
    assert.equal(base.entryCount(), 601);
    assert.equal(base.treeBaseRoot, root);
    assert.equal(base.lastSyncTimestamp, 17);
    assert.equal(base.getTreeMtime("note-17.md"), 2);
    assert.equal(io.files.get(LEGACY), originals);
    assert.equal(io.files.get(SYNC_BASE_STORE_PATH + "/unrelated-evidence.txt"), "do not discard");
    assert(!io.events.some(event => event.method === "write" && event.path.includes("/page-") && event.path.endsWith("-0.json")),
        "exact first migration page was needlessly rewritten");

    // Advancing the base forbids stale legacy reimport even when every head is
    // subsequently lost. This models explicit corruption, not a fsync claim.
    base.setEntry("later.md", "3".repeat(64), 8, 9);
    await base.checkpoint();
    assert(io.files.has(segmentedMigrationPaths(SYNC_BASE_STORE_PATH).advanced));
    for (const suffix of ["", ".next", ".bak"]) io.files.delete(HEAD + suffix);
    const evidence = io.snapshot();
    const lost = baseFor(io);
    await assert.rejects(lost.load(), (error: unknown) => error instanceof StoreRecoveryError && error.code === "RECOVERY_REQUIRED");
    assert.deepEqual(io.snapshot(), evidence);
    assert.throws(() => lost.setEntry("must-not-write.md", hash, 1, 1), StoreRecoveryError);

    const changed = await interrupted();
    const source = JSON.parse(changed.files.get(LEGACY)!);
    source.entries["note-17.md"].hash = "4".repeat(64);
    changed.files.set(LEGACY, JSON.stringify(source));
    const changedEvidence = changed.snapshot();
    await assert.rejects(baseFor(changed).load(), (error: unknown) => error instanceof StoreRecoveryError && error.code === "CONTRADICTING_COPIES");
    assert.deepEqual(changed.snapshot(), changedEvidence);

    const unmarked = fixture();
    unmarked.directories.add(SYNC_BASE_STORE_PATH);
    const unmarkedEvidence = unmarked.snapshot();
    await assert.rejects(baseFor(unmarked).load(), StoreRecoveryError);
    assert.deepEqual(unmarked.snapshot(), unmarkedEvidence);
    console.log("sync-base-migration.test: exact-source retry, preserved originals, advancement and changed-source recovery gates passed");
}

void run().catch(error => { console.error(error); process.exitCode = 1; });
