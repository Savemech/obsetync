import { strict as assert } from "node:assert";
import { SegmentedStore, STORE_LIMITS, StoreRecoveryError } from "./segmented-store";
import { checksumJournalUtf8 } from "./journal-format";
import {
    MemorySegmentedIO, readStoreTestFrame, sealStoreTestFrame, storeTestIOError,
    type StoreTestBoundary, type StoreTestDisk,
} from "./segmented-store-test-io";

const DIRECTORY = "test-store";
const HEAD = `${DIRECTORY}/head.json`;
const noPause = async () => {};
type Row = { kind: "entry"; path: string; value: string } |
    { kind: "delete"; path: string } | { kind: "cursor"; cursor: number };
const entry = (path: string, value: string): Row => ({ kind: "entry", path, value });
const cursor = (value: number): Row => ({ kind: "cursor", cursor: value });

interface RecoveredState {
    present: boolean;
    entries: Array<[string, string]>;
    cursor: number;
    sequence: number;
}

/** The actual class explicitly requires provisional callbacks. This test
 * consumer publishes its map/cursor only after load validated the whole cut. */
async function recover(io: MemorySegmentedIO): Promise<RecoveredState> {
    const entries = new Map<string, string>();
    let committedCursor = 0;
    const apply = (unknown: unknown) => {
        const row = unknown as Row;
        if (row.kind === "entry") entries.set(row.path, row.value);
        else if (row.kind === "delete") entries.delete(row.path);
        else if (row.kind === "cursor") committedCursor = row.cursor;
        else throw new Error("invalid test application row");
    };
    const store = new SegmentedStore(io, DIRECTORY, { yield: noPause, automaticMaintenance: false });
    const present = await store.load((metadata) => {
        committedCursor = (metadata as { cursor: number }).cursor;
    }, apply, apply);
    return { present, entries: [...entries].sort(([left], [right]) => left.localeCompare(right)),
        cursor: committedCursor, sequence: store.sequence };
}

async function open(io: MemorySegmentedIO): Promise<SegmentedStore> {
    const store = new SegmentedStore(io, DIRECTORY, { yield: noPause, automaticMaintenance: false });
    await store.load(() => {}, () => {}, () => {});
    return store;
}

async function fixture(): Promise<MemorySegmentedIO> {
    const io = new MemorySegmentedIO();
    const store = await open(io);
    await store.initialize([entry("a.md", "old-a"), entry("b.md", "old-b")], { cursor: 1 });
    // A preceding commit creates a backup so the next publication exercises
    // remove(.bak), both renames, and all their before/after boundaries.
    await store.commit([entry("seed.md", "old-seed"), cursor(2)]);
    return io;
}

function rewrite(io: MemorySegmentedIO, path: string, mutate: (value: any) => void): void {
    const value = readStoreTestFrame(io, path);
    mutate(value);
    io.files.set(path, sealStoreTestFrame(value));
}

function selectedPaths(io: MemorySegmentedIO): { snapshot: string; wal: string } {
    const head = readStoreTestFrame(io, HEAD);
    return {
        snapshot: `${DIRECTORY}/page-${head.epoch}-${head.snapshot.id}-0.json`,
        wal: `${DIRECTORY}/wal-${head.epoch}-${head.walStart}.json`,
    };
}

async function rejectsRecovery(io: MemorySegmentedIO, expected: StoreRecoveryError["code"]): Promise<void> {
    await assert.rejects(recover(io), (error: unknown) =>
        error instanceof StoreRecoveryError && error.code === expected);
}

async function boundedPagesSnapshotAppendAndStableIterator(): Promise<void> {
    const io = new MemorySegmentedIO();
    let yields = 0;
    const store = new SegmentedStore(io, DIRECTORY, {
        yield: async () => { yields++; }, automaticMaintenance: false,
    });
    assert.equal(await store.load(() => {}, () => {}, () => {}), false);
    const stableRows = Object.freeze(Array.from({ length: 601 }, (_, index) =>
        Object.freeze(entry(`note-${index}.md`, `snapshot-${index}`))));
    let iteratorCount = 0;
    let traversed = 0;
    const source: Iterable<Row> = {
        [Symbol.iterator]: function* () {
            assert.equal(++iteratorCount, 1, "storage traversed its snapshot source more than once");
            for (const row of stableRows) { traversed++; yield row; }
        },
    };
    let firstWriteTraversed = 0;
    io.onBoundary = (event) => {
        if (event.method === "write" && event.phase === "before" && event.path.includes("/page-") && !firstWriteTraversed) {
            firstWriteTraversed = traversed;
        }
    };
    await store.initialize(source, { cursor: 1 });
    io.onBoundary = undefined;
    assert.equal(traversed, stableRows.length);
    assert.ok(firstWriteTraversed <= STORE_LIMITS.rowsPerPage + 1, "snapshot was eagerly materialized before the first page write");
    assert.equal(readStoreTestFrame(io, HEAD).snapshot.pages, 3);
    assert.equal(store.sequence, 0);
    assert.equal(store.walSegments, 0);
    assert.equal(yields, 3);

    const changes: Row[] = [entry("note-0.md", "new-0"), { kind: "delete", path: "note-1.md" }, cursor(2)];
    await store.commit(changes);
    assert.equal(store.sequence, changes.length);
    assert.equal(store.walSegments, 1);
    const afterAppend = await recover(io.clone());
    assert.equal(afterAppend.entries.length, 600);
    assert.equal(new Map(afterAppend.entries).get("note-0.md"), "new-0");
    assert.equal(new Map(afterAppend.entries).has("note-1.md"), false);
    assert.equal(afterAppend.cursor, 2);

    await store.snapshot(afterAppend.entries.map(([path, value]) => entry(path, value)), { cursor: 2 });
    const snapshotHead = readStoreTestFrame(io, HEAD);
    assert.equal(snapshotHead.snapshot.cut, changes.length);
    assert.equal(snapshotHead.sequence, changes.length);
    assert.equal(snapshotHead.snapshot.id, snapshotHead.generation);
    assert.equal(store.walSegments, 0);
    await store.commit([entry("after-snapshot.md", "new"), cursor(3)]);
    const final = await recover(io.clone());
    assert.equal(final.sequence, 5);
    assert.equal(final.cursor, 3);
    assert.equal(new Map(final.entries).get("after-snapshot.md"), "new");

    // UTF-8 payload sizing must split large rows well before the row-count cap.
    await store.commit(Array.from({ length: 70 }, (_, index) => entry(`large-${index}`, "🙂".repeat(2000))));
    let sawByteBoundedPage = false;
    for (const [path, raw] of io.files) {
        if (!path.includes("/page-") && !path.includes("/wal-")) continue;
        const frame = JSON.parse(raw);
        const page = JSON.parse(frame.payload);
        assert.ok(page.rows.length <= STORE_LIMITS.rowsPerPage);
        assert.ok(new TextEncoder().encode(frame.payload).byteLength <= STORE_LIMITS.payloadBytes);
        assert.ok(new TextEncoder().encode(raw).byteLength <= STORE_LIMITS.frameBytes);
        if (page.rows[0]?.path.startsWith("large-") && page.rows.length < 70) sawByteBoundedPage = true;
    }
    assert.ok(sawByteBoundedPage, "UTF-8 row sizes did not constrain page grouping");
    const writesBeforeNoop = io.events.filter((event) => event.method === "write").length;
    await store.commit([]);
    assert.equal(io.events.filter((event) => event.method === "write").length, writesBeforeNoop);
}

let checkedBoundaries = 0;
let checkedRejections = 0;
let checkedTornWrites = 0;
let checkedMaintenanceFailures = 0;

/** At every real adapter boundary a fresh loader observes one entire old/new
 * state or explicitly refuses recovery. Cursor and entries are compared as a
 * single value; partial callback progress is never treated as a published cut. */
async function checkPublicationBoundaries(
    name: string,
    initial: StoreTestDisk,
    operation: (store: SegmentedStore) => Promise<void>,
): Promise<void> {
    const baseline = new MemorySegmentedIO(initial);
    const store = await open(baseline);
    const oldState = JSON.stringify(await recover(new MemorySegmentedIO(initial)));
    const gcCandidates = new Set<string>();
    if (baseline.files.has(`${HEAD}.bak`)) {
        const previousBackup = readStoreTestFrame(baseline, `${HEAD}.bak`);
        for (let part = 0; part < previousBackup.snapshot.pages; part++) {
            gcCandidates.add(`${DIRECTORY}/page-${previousBackup.epoch}-${previousBackup.snapshot.id}-${part}.json`);
        }
        for (let id = previousBackup.walStart; id <= previousBackup.walEnd; id++) {
            gcCandidates.add(`${DIRECTORY}/wal-${previousBackup.epoch}-${id}.json`);
        }
    }
    baseline.events.length = 0;
    const checkpoints: Array<{ event: StoreTestBoundary; disk: StoreTestDisk }> = [];
    baseline.onBoundary = (event, io) => { checkpoints.push({ event, disk: io.snapshot() }); };
    await operation(store);
    baseline.onBoundary = undefined;
    const newState = JSON.stringify(await recover(baseline.clone()));
    assert.notEqual(newState, oldState, `${name} did not change its visible cut`);
    const acceptable = new Set([oldState, newState]);

    // Prove the publication boundary from actual adapter history, not merely
    // the presence of head.json or an operation phase label. The stage was
    // promoted, then its exact new bytes were read back. Injecting a failure
    // inside that read (including its after hook) is STILL authoritative: the
    // verified bytes have not returned to promote() yet. Only later calls may
    // be classified as non-fatal maintenance.
    const publishedBytes = baseline.files.get(HEAD)!;
    const promoted = checkpoints.find(({ event, disk }) => event.method === "rename" && event.phase === "after" &&
        event.path === `${HEAD}.next` && event.to === HEAD && disk.files.get(HEAD) === publishedBytes);
    assert.ok(promoted, `${name} has no recorded successor promotion`);
    const verified = checkpoints.find(({ event, disk }) => event.index > promoted.event.index &&
        event.method === "read" && event.phase === "after" && event.path === HEAD && disk.files.get(HEAD) === publishedBytes);
    assert.ok(verified, `${name} has no exact successor verification readback`);
    const beforeRead = checkpoints[verified.event.index - 1];
    assert.equal(beforeRead.event.method, "read");
    assert.equal(beforeRead.event.phase, "before");
    assert.equal(beforeRead.event.path, HEAD);
    assert.equal(beforeRead.disk.files.get(HEAD), publishedBytes);
    const maintenance = (event: StoreTestBoundary): boolean => event.index > verified.event.index;
    for (const { event } of checkpoints) {
        if (!maintenance(event)) continue;
        const backupCheck = event.path === `${HEAD}.bak` && ["exists", "stat", "read"].includes(event.method);
        const cleanup = gcCandidates.has(event.path) && ["exists", "remove"].includes(event.method);
        assert.ok(backupCheck || cleanup, `${name} has an unclassified operation after publication: ${event.method} ${event.path}`);
    }

    const check = async (io: MemorySegmentedIO, boundary: string) => {
        try {
            const state = JSON.stringify(await recover(io));
            assert.ok(acceptable.has(state), `${name} exposed mixed cursor/entries at ${boundary}: ${state}`);
        } catch (error) {
            if (!(error instanceof StoreRecoveryError)) throw error;
        }
    };

    for (const { event, disk } of checkpoints) {
        if (maintenance(event)) {
            assert.equal(JSON.stringify(await recover(new MemorySegmentedIO(disk))), newState,
                `${name} post-publication maintenance damaged the confirmed cut at ${event.method}/${event.phase}`);
        } else {
            await check(new MemorySegmentedIO(disk), `${event.method}/${event.phase}/${event.index}`);
        }
        checkedBoundaries++;
    }

    // Rejections before and after completion model both clear failure and
    // ambiguous native completion. A poisoned instance must never keep writing.
    for (const { event } of checkpoints) {
        const io = new MemorySegmentedIO(initial);
        const failedStore = await open(io);
        io.events.length = 0;
        let injected = false;
        io.onBoundary = (candidate) => {
            if (candidate.index === event.index) { injected = true; throw storeTestIOError(); }
        };
        if (maintenance(event)) {
            await operation(failedStore); // A rejection is a test failure here.
        } else {
            await assert.rejects(operation(failedStore), `${name} swallowed authoritative ${event.method}/${event.phase} IO failure`);
        }
        assert.ok(injected, "fault boundary was not reached");
        io.onBoundary = undefined;
        if (maintenance(event)) {
            const recovered = await recover(io.clone());
            assert.equal(JSON.stringify(recovered), newState,
                `${name} maintenance failure rolled back or mixed the already confirmed cut`);
            assert.equal(failedStore.deferredCleanupCount, 1, "maintenance failure was not recorded exactly once");
            assert.equal(failedStore.sequence, recovered.sequence);
            // Verify actual further publication, not just a resolved no-op.
            await failedStore.commit([cursor(recovered.cursor + 1)]);
            const afterRetry = await recover(io.clone());
            assert.deepEqual(afterRetry.entries, recovered.entries);
            assert.equal(afterRetry.cursor, recovered.cursor + 1);
            assert.equal(afterRetry.sequence, recovered.sequence + 1);
            assert.equal(failedStore.deferredCleanupCount, 1);
            checkedMaintenanceFailures++;
        } else {
            await assert.rejects(failedStore.commit([]), (error: unknown) =>
                error instanceof StoreRecoveryError && error.code === "RECOVERY_REQUIRED");
            await check(io.clone(), `rejected ${event.method}/${event.phase}/${event.index}`);
            checkedRejections++;
        }
    }

    for (const { event } of checkpoints) {
        if (event.method !== "write" || event.phase !== "before") continue;
        assert.equal(maintenance(event), false, "unexpected maintenance write requires an explicit fault contract");
        const io = new MemorySegmentedIO(initial);
        const failedStore = await open(io);
        io.events.length = 0;
        io.onBoundary = (candidate, adapter) => {
            if (candidate.index !== event.index) return;
            adapter.files.set(candidate.path, candidate.data!.slice(0, Math.floor(candidate.data!.length / 2)));
            throw storeTestIOError("ENOSPC");
        };
        await assert.rejects(operation(failedStore));
        io.onBoundary = undefined;
        await check(io.clone(), `torn ${event.path}`);
        checkedTornWrites++;
    }
}

async function restartAndFailureAtEveryPublicationBoundary(): Promise<void> {
    const absent = new MemorySegmentedIO().snapshot();
    await checkPublicationBoundaries("initialize", absent, (store) =>
        store.initialize(Array.from({ length: 270 }, (_, index) => entry(`initial-${index}`, "initial")), { cursor: 1 }));

    const initial = (await fixture()).snapshot();
    const mutations: Row[] = Array.from({ length: 270 }, (_, index) => entry(`batch-${index}`, `new-${index}`));
    mutations.push({ kind: "delete", path: "b.md" }, cursor(3));
    await checkPublicationBoundaries("commit entries+cursor", initial, (store) => store.commit(mutations));
    await checkPublicationBoundaries("snapshot entries+cursor", initial, (store) =>
        store.snapshot(Array.from({ length: 270 }, (_, index) => entry(`snapshot-${index}`, "replacement")), { cursor: 9 }));

    // Unlike the first snapshot, this following publication has a genuinely
    // superseded closure to reclaim. Classify and fault actual cleanup calls,
    // not only the newly added maintenance backup verification reads.
    const gcInitial = new MemorySegmentedIO(initial);
    const gcStore = await open(gcInitial);
    await gcStore.snapshot(Array.from({ length: 270 }, (_, index) => entry(`gc-base-${index}`, "base")), { cursor: 4 });
    await checkPublicationBoundaries("commit entries+cursor with GC", gcInitial.snapshot(), (store) =>
        store.commit([entry("after-gc.md", "new"), cursor(5)]));
}

async function referencedDamageAndUnknownSchemaFailClosed(): Promise<void> {
    const baseline = await fixture();
    const paths = selectedPaths(baseline);
    for (const path of [paths.snapshot, paths.wal]) {
        const missing = baseline.clone();
        missing.files.delete(path);
        await rejectsRecovery(missing, "CORRUPT");
        const corrupted = baseline.clone();
        corrupted.files.set(path, corrupted.files.get(path)!.slice(0, -8));
        await rejectsRecovery(corrupted, "CORRUPT");
        for (const code of ["EIO", "ENOENT"]) {
            const unavailable = baseline.clone();
            unavailable.onBoundary = (event) => {
                if (event.path === path && event.method === "read" && event.phase === "before") throw storeTestIOError(code);
            };
            await rejectsRecovery(unavailable, "READ_FAILED");
        }
        const badChecksum = baseline.clone();
        const frame = JSON.parse(badChecksum.files.get(path)!);
        frame.crc32 = "00000000";
        badChecksum.files.set(path, JSON.stringify(frame));
        await rejectsRecovery(badChecksum, "CORRUPT");
        const futurePage = baseline.clone();
        rewrite(futurePage, path, (page) => { page.schema = 2; });
        await rejectsRecovery(futurePage, "UNKNOWN_SCHEMA");
    }

    for (const suffix of ["", ".next", ".bak"]) {
        for (const location of ["frame", "record"]) {
            const future = baseline.clone();
            const path = `${HEAD}${suffix}`;
            future.files.set(path, future.files.get(HEAD)!);
            if (location === "record") rewrite(future, path, (head) => { head.schema = 2; });
            else {
                const value = JSON.parse(future.files.get(path)!);
                value.schema = 2;
                future.files.set(path, JSON.stringify(value));
            }
            await rejectsRecovery(future, "UNKNOWN_SCHEMA");
        }
    }

    const headIOFailure = baseline.clone();
    headIOFailure.onBoundary = (event) => {
        if (event.path === HEAD && event.method === "read" && event.phase === "before") throw storeTestIOError();
    };
    await rejectsRecovery(headIOFailure, "READ_FAILED");
    const brokenBackup = baseline.clone();
    brokenBackup.files.set(`${HEAD}.bak`, "{corrupt durable backup");
    await rejectsRecovery(brokenBackup, "CORRUPT");
    const directoryWithoutHead = new MemorySegmentedIO();
    directoryWithoutHead.directories.add(DIRECTORY);
    await rejectsRecovery(directoryWithoutHead, "RECOVERY_REQUIRED");
}

async function headIdentityAndSequenceBoundsAreValidated(): Promise<void> {
    const baseline = await fixture();
    const headMutations: Array<(head: any) => void> = [
        (head) => { head.generation = 0; },
        (head) => { head.generation = Number.MAX_SAFE_INTEGER; },
        (head) => { head.epoch = "not-an-epoch"; },
        (head) => { head.snapshot.id = head.generation + 1; },
        (head) => { head.snapshot.cut = head.sequence + 1; },
        (head) => { head.sequence++; },
        (head) => { head.nextSegment++; },
        (head) => { head.walStart++; },
        (head) => { head.snapshot.pages = STORE_LIMITS.snapshotPages + 1; },
        (head) => { head.walEnd = head.walStart + STORE_LIMITS.replaySegments; head.nextSegment = head.walEnd + 1; },
    ];
    for (const mutate of headMutations) {
        const io = baseline.clone();
        rewrite(io, HEAD, mutate);
        await rejectsRecovery(io, "CORRUPT");
    }
    const differentEpoch = baseline.clone();
    rewrite(differentEpoch, `${HEAD}.bak`, (head) => { head.epoch = "a".repeat(32); });
    await rejectsRecovery(differentEpoch, "CONTRADICTING_COPIES");
    const contradiction = baseline.clone();
    contradiction.files.set(`${HEAD}.next`, contradiction.files.get(HEAD)!);
    rewrite(contradiction, `${HEAD}.next`, (head) => { head.snapshot.metadata.cursor = 999; });
    await rejectsRecovery(contradiction, "CONTRADICTING_COPIES");

    const paths = selectedPaths(baseline);
    const pageMutations: Array<[string, (page: any) => void]> = [
        [paths.snapshot, (page) => { page.part++; }],
        [paths.snapshot, (page) => { page.from++; }],
        [paths.snapshot, (page) => { page.through++; }],
        [paths.wal, (page) => { page.epoch = "b".repeat(32); }],
        [paths.wal, (page) => { page.id++; }],
        [paths.wal, (page) => { page.from++; page.through++; }],
        [paths.wal, (page) => { page.through++; }],
        [paths.wal, (page) => { page.rows = []; }],
        [paths.wal, (page) => { page.rows = Array.from({ length: STORE_LIMITS.rowsPerPage + 1 }, () => cursor(1)); }],
    ];
    for (const [path, mutate] of pageMutations) {
        const io = baseline.clone();
        rewrite(io, path, mutate);
        await rejectsRecovery(io, "CORRUPT");
    }
    const oversizedPayload = baseline.clone();
    rewrite(oversizedPayload, paths.wal, (page) => {
        page.rows = [entry("too-large", "x".repeat(STORE_LIMITS.payloadBytes))];
    });
    await rejectsRecovery(oversizedPayload, "LIMIT");
}

async function sealedPayloadLimitIncludesWhitespace(): Promise<void> {
    const baseline = await fixture();
    const paths = selectedPaths(baseline);
    for (const path of [paths.snapshot, paths.wal]) {
        const io = baseline.clone();
        const page = readStoreTestFrame(io, path);
        const compact = JSON.stringify(page);
        assert.ok(checksumJournalUtf8(compact).bytes < STORE_LIMITS.payloadBytes);
        // Valid JSON and CRC, with a small normalized page. Measuring only
        // JSON.stringify(parsedPage) loses this padding and bypasses the inner
        // payload cap even though the outer 512 KiB frame gate still passes.
        const payload = " ".repeat(STORE_LIMITS.payloadBytes + 1) + compact;
        const raw = JSON.stringify({ schema: 1, payload, ...checksumJournalUtf8(payload) });
        assert.deepEqual(JSON.parse(payload), page);
        assert.ok(checksumJournalUtf8(payload).bytes > STORE_LIMITS.payloadBytes);
        assert.ok(checksumJournalUtf8(raw).bytes < STORE_LIMITS.frameBytes);
        io.files.set(path, raw);
        const originalHead = io.files.get(HEAD);
        const store = new SegmentedStore(io, DIRECTORY, { yield: noPause, automaticMaintenance: false });
        let snapshots = 0;
        let mutations = 0;
        await assert.rejects(store.load(() => {}, () => { snapshots++; }, () => { mutations++; }),
            (error: unknown) => error instanceof StoreRecoveryError && error.code === "LIMIT");
        if (path === paths.snapshot) assert.equal(snapshots, 0, "oversized snapshot reached application callbacks");
        assert.equal(mutations, 0, "oversized sealed payload advanced application mutations/cursor");
        await assert.rejects(store.commit([]),
            (error: unknown) => error instanceof StoreRecoveryError && error.code === "RECOVERY_REQUIRED");
        assert.equal(io.files.get(HEAD), originalHead);
        assert.equal(io.files.get(path), raw, "failed payload admission overwrote its recovery evidence");
    }
}

async function unreferencedStagedFramesAreNeverReplayed(): Promise<void> {
    const baseline = await fixture();
    const expected = await recover(baseline.clone());
    const head = readStoreTestFrame(baseline, HEAD);
    const ignoredPaths = [
        `${DIRECTORY}/wal-${head.epoch}-${head.nextSegment}.json`,
        `${DIRECTORY}/page-${head.epoch}-${head.generation + 1}-0.json`,
        `${DIRECTORY}/wal-${"f".repeat(32)}-1.json`,
    ];
    for (const path of ignoredPaths) baseline.files.set(path, "not even a valid frame");
    baseline.files.set(`${HEAD}.next`, "{torn unpublished head");
    baseline.events.length = 0;
    assert.deepEqual(await recover(baseline), expected);
    assert.ok(!baseline.events.some((event) => ignoredPaths.includes(event.path)), "loader visited an unreferenced staged page");
}

async function selectedStageIsNeverRewrittenDuringRecovery(): Promise<void> {
    const baseline = await fixture();
    const writer = await open(baseline);
    let selectedStage: StoreTestDisk | undefined;
    baseline.onBoundary = (event, io) => {
        if (event.method === "write" && event.phase === "after" && event.path === `${HEAD}.next`) {
            selectedStage = io.snapshot();
        }
    };
    await writer.commit([entry("new.md", "new cut"), cursor(3)]);
    baseline.onBoundary = undefined;
    assert.ok(selectedStage);
    const expected = await recover(baseline.clone());
    const recovering = new MemorySegmentedIO(selectedStage);
    const stagedBytes = recovering.files.get(`${HEAD}.next`)!;
    let attemptedRewrite = false;
    recovering.onBoundary = (event, io) => {
        if (event.method === "write" && event.phase === "before" && event.path === `${HEAD}.next`) {
            attemptedRewrite = true;
            io.files.set(event.path, "{torn newest publication");
            throw storeTestIOError("ENOSPC");
        }
    };
    assert.deepEqual(await recover(recovering), expected);
    assert.equal(attemptedRewrite, false, "recovery rewrote the only newest selected publication in place");
    assert.equal(recovering.files.get(HEAD), stagedBytes);
}

async function recoveryPromotionFailuresCannotRollBackTheNewestValidatedCut(): Promise<void> {
    const published = await fixture();
    const writer = await open(published);
    await writer.commit([entry("newest.md", "must not roll back"), cursor(3)]);
    const expected = await recover(published.clone());
    const newest = published.files.get(HEAD)!;
    const older = published.files.get(`${HEAD}.bak`)!;

    for (const selected of [".next", ".bak"]) {
        const damaged = published.clone();
        damaged.files.set(HEAD, older);
        damaged.files.set(`${HEAD}${selected}`, newest);
        // This is a valid interruption state with one unambiguous newest
        // publication. Recovery must never downgrade it to the older cursor,
        // even if another recovery attempt is itself interrupted.
        const baseline = damaged.clone();
        const checkpoints: Array<{ event: StoreTestBoundary; disk: StoreTestDisk }> = [];
        baseline.onBoundary = (event, io) => { checkpoints.push({ event, disk: io.snapshot() }); };
        assert.deepEqual(await recover(baseline), expected);

        const check = async (io: MemorySegmentedIO, label: string) => {
            try { assert.deepEqual(await recover(io), expected, label); }
            catch (error) { if (!(error instanceof StoreRecoveryError)) throw error; }
        };
        for (const { event, disk } of checkpoints) {
            await check(new MemorySegmentedIO(disk), `recovery ${selected} ${event.method}/${event.phase}`);
            checkedBoundaries++;
            const rejected = damaged.clone();
            let injected = false;
            rejected.onBoundary = (candidate) => {
                if (candidate.index === event.index) { injected = true; throw storeTestIOError(); }
            };
            await assert.rejects(recover(rejected));
            assert.ok(injected);
            rejected.onBoundary = undefined;
            await check(rejected.clone(), `recovery rejection ${selected} ${event.method}/${event.phase}`);
            checkedRejections++;
        }
        for (const { event } of checkpoints) {
            if (event.method !== "write" || event.phase !== "before") continue;
            const rejected = damaged.clone();
            rejected.onBoundary = (candidate, io) => {
                if (candidate.index !== event.index) return;
                io.files.set(candidate.path, "{torn replacement during recovery");
                throw storeTestIOError("ENOSPC");
            };
            await assert.rejects(recover(rejected));
            rejected.onBoundary = undefined;
            await check(rejected.clone(), `torn ${selected} repair`);
            checkedTornWrites++;
        }
    }
}

async function limitsRejectBeforeUnboundedReadAndPoisonFailedWriter(): Promise<void> {
    const baseline = await fixture();
    const paths = selectedPaths(baseline);
    for (const [path, limit] of [[HEAD, STORE_LIMITS.headBytes], [paths.wal, STORE_LIMITS.frameBytes]] as const) {
        const oversized = baseline.clone();
        oversized.files.set(path, "x".repeat(limit + 1));
        await rejectsRecovery(oversized, "LIMIT");
        assert.ok(!oversized.events.some((event) => event.path === path && event.method === "read"),
            "oversized metadata did not reject before the adapter allocated a whole read");
    }

    for (const rows of [[entry("oversized", "🙂".repeat(STORE_LIMITS.payloadBytes))], [undefined]]) {
        const io = baseline.clone();
        const store = await open(io);
        const before = await recover(io.clone());
        await assert.rejects(store.commit(rows), (error: unknown) => error instanceof StoreRecoveryError);
        await assert.rejects(store.commit([cursor(999)]), (error: unknown) =>
            error instanceof StoreRecoveryError && error.code === "RECOVERY_REQUIRED");
        assert.deepEqual(await recover(io.clone()), before);
    }

    // Semantic application failure during replay cannot authorize writes from
    // a partially built caller index either.
    const io = baseline.clone();
    const store = new SegmentedStore(io, DIRECTORY, { yield: noPause, automaticMaintenance: false });
    await assert.rejects(store.load(() => {}, () => { throw new Error("invalid application row"); }, () => {}), /application row/);
    await assert.rejects(store.commit([]), (error: unknown) => error instanceof StoreRecoveryError && error.code === "RECOVERY_REQUIRED");
}

async function run(): Promise<void> {
    await boundedPagesSnapshotAppendAndStableIterator();
    await restartAndFailureAtEveryPublicationBoundary();
    await referencedDamageAndUnknownSchemaFailClosed();
    await headIdentityAndSequenceBoundsAreValidated();
    await sealedPayloadLimitIncludesWhitespace();
    await unreferencedStagedFramesAreNeverReplayed();
    await selectedStageIsNeverRewrittenDuringRecovery();
    await recoveryPromotionFailuresCannotRollBackTheNewestValidatedCut();
    await limitsRejectBeforeUnboundedReadAndPoisonFailedWriter();
    console.log(`segmented-store.test: ${checkedBoundaries} restart boundaries, ${checkedRejections} authoritative IO rejections, ${checkedMaintenanceFailures} accepted maintenance failures, ${checkedTornWrites} torn writes and bounds/identity regressions passed`);
}

void run().catch((error) => { console.error(error); process.exitCode = 1; });
