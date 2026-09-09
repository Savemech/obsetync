import { strict as assert } from "node:assert";
import { SegmentedStore, STORE_LIMITS, type SegmentedStoreOptions } from "./segmented-store";
import {
    MemorySegmentedIO, readStoreTestFrame, storeTestIOError,
    type StoreTestDisk,
} from "./segmented-store-test-io";

const DIRECTORY = "gc-test-store";
const HEAD = `${DIRECTORY}/head.json`;
const noPause = async () => {};
type Row = { kind: "entry"; path: string; value: string } | { kind: "cursor"; cursor: number };
const entry = (path: string, value: string): Row => ({ kind: "entry", path, value });
const cursorRow = (cursor: number): Row => ({ kind: "cursor", cursor });
const rows = (prefix: string, count = 270): Row[] => Array.from({ length: count }, (_, index) =>
    entry(`note-${index}.md`, `${prefix}-${index}`));
const isData = (path: string) => path.includes("/page-") || path.includes("/wal-");

function closure(head: any): Set<string> {
    const paths = new Set<string>();
    for (let part = 0; part < head.snapshot.pages; part++) {
        paths.add(`${DIRECTORY}/page-${head.epoch}-${head.snapshot.id}-${part}.json`);
    }
    for (let id = head.walStart; id <= head.walEnd; id++) paths.add(`${DIRECTORY}/wal-${head.epoch}-${id}.json`);
    return paths;
}

function retainedClosure(io: MemorySegmentedIO): Set<string> {
    const paths = new Set<string>();
    for (const path of [HEAD, `${HEAD}.bak`]) {
        if (!io.files.has(path)) continue;
        for (const dataPath of closure(readStoreTestFrame(io, path))) paths.add(dataPath);
    }
    return paths;
}

interface State { entries: Array<[string, string]>; cursor: number; sequence: number }

/** Select each retained head independently and replay it with the actual
 * loader. A present .bak JSON file is not sufficient evidence of recoverability
 * if one of the pages reachable from that backup has already been deleted. */
async function replay(io: MemorySegmentedIO, suffix = ""): Promise<State> {
    const copy = io.clone();
    const raw = copy.files.get(`${HEAD}${suffix}`);
    assert.ok(raw, `missing retained head ${suffix}`);
    copy.files.set(HEAD, raw);
    copy.files.delete(`${HEAD}.next`);
    copy.files.delete(`${HEAD}.bak`);
    const entries = new Map<string, string>();
    let cursor = 0;
    const apply = (value: unknown) => {
        const row = value as Row;
        if (row.kind === "entry") entries.set(row.path, row.value);
        else cursor = row.cursor;
    };
    const store = new SegmentedStore(copy, DIRECTORY, {
        yield: noPause, maintenanceYield: noPause, automaticMaintenance: false,
    });
    assert.equal(await store.load((metadata) => { cursor = (metadata as { cursor: number }).cursor; }, apply, apply), true);
    return { entries: [...entries].sort(([left], [right]) => left.localeCompare(right)), cursor, sequence: store.sequence };
}

async function open(io: MemorySegmentedIO, options: SegmentedStoreOptions = {}): Promise<SegmentedStore> {
    const store = new SegmentedStore(io, DIRECTORY, {
        yield: noPause, maintenanceYield: noPause, automaticMaintenance: false, ...options,
    });
    await store.load(() => {}, () => {}, () => {});
    return store;
}

let checkedPublications = 0;
let checkedGCInterruptions = 0;
let checkedBackupGuardSkips = 0;

async function manyCyclesRetainOnlyTwoCompleteReachableClosures(): Promise<void> {
    const io = new MemorySegmentedIO();
    const store = await open(io);
    const live = new Map<string, string>();
    for (let index = 0; index < 601; index++) live.set(`note-${index}.md`, `initial-${index}`);
    let cursor = 0;
    await store.initialize([...live].map(([path, value]) => entry(path, value)), { cursor });
    const historical = new Map<number, State>();
    let candidates = new Set<string>();
    let removed = 0;
    io.onBoundary = (event, adapter) => {
        if (event.method !== "remove" || event.phase !== "before" || !isData(event.path)) return;
        assert.ok(candidates.has(event.path), "GC targeted a page outside the known superseded backup closure");
        assert.equal(retainedClosure(adapter).has(event.path), false, "GC removed a reachable current/backup page");
        assert.equal(adapter.files.has(`${HEAD}.next`), false, "GC began before verified head promotion completed");
        removed++;
    };
    const verify = async () => {
        await store.runMaintenance();
        const current = readStoreTestFrame(io, HEAD);
        const expected: State = { entries: [...live].sort(([left], [right]) => left.localeCompare(right)),
            cursor, sequence: store.sequence };
        historical.set(current.generation, expected);
        assert.deepEqual(await replay(io), expected);
        if (io.files.has(`${HEAD}.bak`)) {
            const backup = readStoreTestFrame(io, `${HEAD}.bak`);
            assert.deepEqual(await replay(io, ".bak"), historical.get(backup.generation));
        }
        const reachable = retainedClosure(io);
        for (const path of reachable) assert.ok(io.files.has(path), "retained closure contains a missing page");
        assert.deepEqual(new Set([...io.files.keys()].filter(isData)), reachable,
            "normal completed publication leaked data outside its two retained heads");
        const heads = io.files.has(`${HEAD}.bak`) ? 2 : 1;
        assert.equal(io.files.size, reachable.size + heads);
        assert.ok(io.files.size <= 2 + 2 * Math.ceil(live.size / STORE_LIMITS.rowsPerPage) + 2,
            "steady-state files grew with historical snapshot/commit count");
        assert.equal(store.deferredCleanupCount, 0);
        checkedPublications++;
    };
    const prepare = () => {
        candidates = io.files.has(`${HEAD}.bak`) ? closure(readStoreTestFrame(io, `${HEAD}.bak`)) : new Set();
    };
    await verify();
    for (let cycle = 0; cycle < 40; cycle++) {
        prepare();
        const firstPath = `note-${cycle}.md`;
        live.set(firstPath, `before-snapshot-${cycle}`);
        await store.commit([entry(firstPath, live.get(firstPath)!), cursorRow(++cursor)]);
        await verify();
        prepare();
        await store.snapshot([...live].map(([path, value]) => entry(path, value)), { cursor });
        await verify();
        prepare();
        const secondPath = `note-${cycle + 100}.md`;
        live.set(secondPath, `after-snapshot-${cycle}`);
        await store.commit([entry(secondPath, live.get(secondPath)!), cursorRow(++cursor)]);
        await verify();
    }
    assert.ok(removed >= 40 * 3, "cycles did not exercise actual superseded-page reclamation");
}

async function readyForGC(options: SegmentedStoreOptions = {}): Promise<{ io: MemorySegmentedIO; store: SegmentedStore; candidate: Set<string> }> {
    const io = new MemorySegmentedIO();
    const store = await open(io, options);
    await store.initialize(rows("A"), { cursor: 1 });
    await store.commit([entry("mutated.md", "A mutation"), cursorRow(2)]);
    await store.snapshot(rows("B"), { cursor: 3 });
    await store.runMaintenance();
    // Main is B's new snapshot, backup is A+WAL. The next snapshot must keep B
    // and C while deleting both A's old snapshot pages and its retired WAL.
    return { io, store, candidate: closure(readStoreTestFrame(io, `${HEAD}.bak`)) };
}

async function failedCleanupNeverRejectsTheAlreadyCommittedHead(): Promise<void> {
    for (const method of ["exists", "remove"] as const) {
        for (const phase of ["before", "after"] as const) {
            const { io, store, candidate } = await readyForGC();
            const expectedBackup = await replay(io);
            let rejected = 0;
            io.onBoundary = (event) => {
                if (event.method === method && event.phase === phase && candidate.has(event.path)) {
                    rejected++;
                    throw storeTestIOError();
                }
            };
            await store.snapshot(rows("C"), { cursor: 4 });
            await store.runMaintenance();
            io.onBoundary = undefined;
            assert.equal(rejected, candidate.size);
            assert.equal(store.deferredCleanupCount, candidate.size);
            assert.equal((await replay(io)).cursor, 4, "cleanup error rolled back the verified new head");
            assert.deepEqual(await replay(io, ".bak"), expectedBackup);
            for (const path of candidate) {
                assert.equal(io.files.has(path), !(method === "remove" && phase === "after"),
                    "rejected cleanup did not preserve its actual before/after native outcome");
            }
            // Cleanup failure is not a publication failure and must not poison
            // the writer. A later publication reactivates the retained exact
            // closure without constructing an unbounded per-path retry list.
            await store.commit([entry("still-writable.md", "yes"), cursorRow(5)]);
            await store.runMaintenance();
            assert.equal((await replay(io)).cursor, 5);
            assert.equal(store.deferredCleanupCount, candidate.size);
            assert.equal(store.pendingCleanupJobs, 0);
            for (const path of candidate) assert.equal(io.files.has(path), false);
        }
    }
}

async function maintenanceYieldFailureDoesNotUndoPublication(): Promise<void> {
    let rejectMaintenance = true;
    const { io, store, candidate } = await readyForGC({ maintenanceYield: async () => {
        if (rejectMaintenance) throw new Error("scheduler disposed during cleanup");
    } });
    await store.snapshot(rows("C"), { cursor: 4 });
    await store.runMaintenance();
    assert.equal((await replay(io)).cursor, 4);
    assert.equal(store.deferredCleanupCount, 1);
    assert.equal(store.pendingCleanupJobs, 1);
    for (const path of candidate) assert.equal(io.files.has(path), true);
    rejectMaintenance = false;
    await store.runMaintenance();
    assert.equal(store.pendingCleanupJobs, 0);
    for (const path of candidate) assert.equal(io.files.has(path), false);
    await store.commit([cursorRow(5)]);
    assert.equal((await replay(io)).cursor, 5);
}

async function automaticMaintenanceWaitDoesNotOwnTheWriterLane(): Promise<void> {
    let releaseLane: (() => void) | undefined;
    let blockLane = false;
    let laneCalls = 0;
    const { io, store, candidate } = await readyForGC({
        automaticMaintenance: true,
        maintenanceYield: async () => {
            laneCalls++;
            if (blockLane) await new Promise<void>(resolve => { releaseLane = resolve; });
        },
    });
    blockLane = true;
    await store.snapshot(rows("C"), { cursor: 4 });
    await Promise.resolve();
    await Promise.resolve();
    assert.ok(laneCalls > 0, "cleanup did not request the maintenance lane");
    assert.ok(releaseLane, "maintenance lane was not held by the fixture");
    for (const path of candidate) assert.ok(io.files.has(path));

    // The scheduler wait is deliberately outside SegmentedStore's serialized
    // writer. A foreground publication must finish before maintenance is let in.
    await store.commit([entry("foreground.md", "accepted"), cursorRow(5)]);
    assert.equal((await replay(io)).cursor, 5);
    blockLane = false;
    releaseLane!();
    await store.runMaintenance();
    assert.equal(store.pendingCleanupJobs, 0);
    for (const path of candidate) assert.equal(io.files.has(path), false);
}

async function nonClosingDrainJoinsAutomaticNativeIO(): Promise<void> {
    let enter!: () => void, release!: () => void;
    const entered = new Promise<void>(resolve => { enter = resolve; });
    const released = new Promise<void>(resolve => { release = resolve; });
    let laneTurns = 0;
    const { io, store, candidate } = await readyForGC({
        automaticMaintenance: true,
        maintenanceYield: async () => {
            if (++laneTurns === 1) { enter(); await released; }
        }
    });
    await store.snapshot(rows("C"), { cursor: 4 });
    await entered;
    let drained = false;
    const drain = store.drainMaintenance().then(() => { drained = true; });
    while (store.pendingCleanupJobs > 0) await Promise.resolve();
    assert.ok(laneTurns > 1, "explicit maintenance did not pass the held automatic lane wait");
    assert.equal(drained, false, "non-closing drain returned before the automatic flight completed");
    release(); await drain;
    assert.equal(store.pendingCleanupJobs, 0);
    for (const path of candidate) assert.equal(io.files.has(path), false);

    await store.commit([entry("after-drain.md", "accepted"), cursorRow(5)]);
    assert.equal((await replay(io)).cursor, 5, "non-closing drain closed future store admission");
}

async function injectedYieldAlsoOwnsMaintenanceUnlessOverridden(): Promise<void> {
    let fixtureIO: MemorySegmentedIO | undefined;
    let candidate = new Set<string>();
    let enterLane!: () => void;
    let releaseLane!: () => void;
    const entered = new Promise<void>(resolve => { enterLane = resolve; });
    const released = new Promise<void>(resolve => { releaseLane = resolve; });
    let maintenanceTurns = 0;
    const { io, store, candidate: retired } = await readyForGC({
        automaticMaintenance: true,
        // Deliberately omit maintenanceYield: deterministic/migration callers
        // that inject their own yield must not reach the global scheduler.
        maintenanceYield: undefined,
        yield: async () => {
            if (!fixtureIO) return;
            let cursor = -1;
            try { cursor = (readStoreTestFrame(fixtureIO, HEAD).snapshot.metadata as { cursor: number }).cursor; }
            catch { /* no published head yet */ }
            if (cursor === 4 && [...candidate].some(path => fixtureIO!.files.has(path)) && ++maintenanceTurns === 1) {
                enterLane();
                await released;
            }
        },
    });
    fixtureIO = io; candidate = retired;
    await store.snapshot(rows("C"), { cursor: 4 });
    await entered;
    assert.equal(maintenanceTurns, 1, "automatic cleanup bypassed the explicitly injected yield");
    for (const path of candidate) assert.ok(io.files.has(path));
    releaseLane();
    await store.runMaintenance();
    for (const path of candidate) assert.equal(io.files.has(path), false);
}

async function explicitMaintenanceWaitDoesNotOwnTheWriterLane(): Promise<void> {
    let enterLane!: () => void;
    let releaseLane!: () => void;
    const entered = new Promise<void>(resolve => { enterLane = resolve; });
    const released = new Promise<void>(resolve => { releaseLane = resolve; });
    let blocked = false;
    let laneTurns = 0;
    const { io, store, candidate } = await readyForGC({ maintenanceYield: async () => {
        if (!blocked) return;
        if (++laneTurns === 2) {
            enterLane();
            await released;
        }
    } });
    await store.snapshot(rows("C"), { cursor: 4 });
    blocked = true;
    const maintenance = store.runMaintenance();
    await entered;

    // runMaintenance() waits outside the serialized writer and enqueues each
    // exact path separately. One slice has completed and the next is waiting,
    // so foreground work must pass between them.
    assert.equal([...candidate].filter(path => io.files.has(path)).length, candidate.size - 1);
    await store.commit([entry("explicit-foreground.md", "accepted"), cursorRow(5)]);
    assert.equal((await replay(io)).cursor, 5);
    assert.equal([...candidate].filter(path => io.files.has(path)).length, candidate.size - 1,
        "foreground publication unexpectedly admitted another maintenance slice");
    blocked = false;
    releaseLane();
    await maintenance;
    for (const path of candidate) assert.equal(io.files.has(path), false);
    await store.runMaintenance();
    assert.equal(store.pendingCleanupJobs, 0);
}

async function closeCancelsWaitingAdmissionAndJoinsAdmittedIO(): Promise<void> {
    {
        let enterLane!: () => void;
        const entered = new Promise<void>(resolve => { enterLane = resolve; });
        const { io, store, candidate } = await readyForGC({
            automaticMaintenance: true,
            maintenanceYield: signal => new Promise<void>((_resolve, reject) => {
                enterLane();
                const aborted = () => reject(signal.reason ?? new Error("maintenance aborted"));
                if (signal.aborted) aborted();
                else signal.addEventListener("abort", aborted, { once: true });
            }),
        });
        await store.snapshot(rows("C"), { cursor: 4 });
        await entered;
        await store.closeAndDrainMaintenance();
        for (const path of candidate) assert.ok(io.files.has(path), "cancelled lane still started cleanup IO");
    }

    {
        let enterLane!: () => void;
        let releaseLane!: () => void;
        const entered = new Promise<void>(resolve => { enterLane = resolve; });
        const released = new Promise<void>(resolve => { releaseLane = resolve; });
        const { io, store, candidate } = await readyForGC({
            automaticMaintenance: true,
            maintenanceYield: async () => { enterLane(); await released; },
        });
        await store.snapshot(rows("C"), { cursor: 4 });
        await entered;
        let retired = false;
        const retirement = store.closeAndDrainMaintenance().then(() => { retired = true; });
        await Promise.resolve();
        assert.equal(retired, false, "close did not own the pending scheduler admission");
        releaseLane();
        await retirement;
        for (const path of candidate) assert.ok(io.files.has(path), "closed wait still started cleanup IO");
        await store.runMaintenance();
        for (const path of candidate) assert.ok(io.files.has(path), "closed store rescheduled cleanup");
    }

    {
        const { io, store, candidate } = await readyForGC({ automaticMaintenance: true });
        const expectedBackup = await replay(io);
        let enterRemove!: () => void;
        let releaseRemove!: () => void;
        const removeEntered = new Promise<void>(resolve => { enterRemove = resolve; });
        const removeReleased = new Promise<void>(resolve => { releaseRemove = resolve; });
        let held = false;
        io.onBoundary = async event => {
            if (!held && event.method === "remove" && event.phase === "before" && candidate.has(event.path)) {
                held = true;
                enterRemove();
                await removeReleased;
            }
        };
        await store.snapshot(rows("C"), { cursor: 4 });
        await removeEntered;
        let retired = false;
        const retirement = store.closeAndDrainMaintenance().then(() => { retired = true; });
        await Promise.resolve();
        assert.equal(retired, false, "close returned before admitted native IO completed");
        releaseRemove();
        await retirement;
        io.onBoundary = undefined;
        const removes = io.events.filter(event => event.method === "remove" && event.phase === "before" &&
            candidate.has(event.path)).length;
        await Promise.resolve();
        assert.equal(io.events.filter(event => event.method === "remove" && event.phase === "before" &&
            candidate.has(event.path)).length, removes, "retired store scheduled a later delete");
        assert.equal((await replay(io)).cursor, 4);
        assert.deepEqual(await replay(io, ".bak"), expectedBackup);
    }
}

async function explicitPassAdmitsEveryExactPathSeparately(): Promise<void> {
    let laneTurns = 0;
    const { io, store, candidate } = await readyForGC({
        maintenanceYield: async () => { laneTurns++; },
    });
    const before = laneTurns;
    await store.snapshot(rows("C"), { cursor: 4 });
    await store.runMaintenance();
    assert.equal(laneTurns - before, candidate.size,
        "one maintenance admission concealed more than one exact-path IO slice");
    assert.equal(store.pendingCleanupJobs, 0);
    for (const path of candidate) assert.equal(io.files.has(path), false);
}

async function persistentFailuresCannotGrowCleanupOwnershipWithoutBound(): Promise<void> {
    const { io, store, candidate } = await readyForGC({ maxPendingCleanupJobs: 1 });
    io.onBoundary = (event) => {
        if (event.method === "exists" && event.phase === "before" && candidate.has(event.path)) {
            throw storeTestIOError();
        }
    };
    await store.snapshot(rows("C"), { cursor: 4 });
    await store.runMaintenance();
    assert.equal(store.pendingCleanupJobs, 1);
    const failures = store.deferredCleanupCount;
    io.onBoundary = undefined;

    // A second retired closure cannot enlarge the one-job memory budget. Its
    // files leak conservatively; current and backup remain replayable/writable.
    const overflowCandidate = closure(readStoreTestFrame(io, `${HEAD}.bak`));
    await store.snapshot(rows("D"), { cursor: 5 });
    assert.equal(store.pendingCleanupJobs, 1);
    assert.equal(store.deferredCleanupCount, failures + 1);
    await store.runMaintenance();
    assert.equal(store.pendingCleanupJobs, 0);
    assert.equal((await replay(io)).cursor, 5);
    assert.equal((await replay(io, ".bak")).cursor, 4);
    assert.ok([...overflowCandidate].some(path => io.files.has(path)),
        "queue overflow unexpectedly authorized deletion without retained evidence");
}

async function everyGCInterruptionPreservesBothRetainedHeads(): Promise<void> {
    const { io, store, candidate } = await readyForGC();
    const expectedBackup = await replay(io);
    const checkpoints: Array<{ label: string; disk: StoreTestDisk }> = [];
    io.onBoundary = (event, adapter) => {
        if (!candidate.has(event.path) || (event.method !== "exists" && event.method !== "remove")) return;
        checkpoints.push({ label: `${event.method}/${event.phase}`, disk: adapter.snapshot() });
    };
    await store.snapshot(rows("C"), { cursor: 4 });
    await store.runMaintenance();
    io.onBoundary = undefined;
    const expectedCurrent = await replay(io);
    assert.equal(checkpoints.length, candidate.size * 4);
    for (const { label, disk } of checkpoints) {
        const interrupted = new MemorySegmentedIO(disk);
        assert.deepEqual(await replay(interrupted), expectedCurrent, `GC ${label} damaged current head`);
        assert.deepEqual(await replay(interrupted, ".bak"), expectedBackup, `GC ${label} damaged backup head`);
        for (const path of retainedClosure(interrupted)) assert.ok(interrupted.files.has(path));
        checkedGCInterruptions++;
    }
}

async function cleanupDoesNotDiscoverOrDeleteUnpublishedOrphans(): Promise<void> {
    const { io, store } = await readyForGC();
    const head = readStoreTestFrame(io, HEAD);
    const orphanPaths = [
        `${DIRECTORY}/page-${head.epoch}-${head.generation + 100}-0.json`,
        `${DIRECTORY}/wal-${head.epoch}-${head.nextSegment + 100}.json`,
        `${DIRECTORY}/page-${"f".repeat(32)}-1-0.json`,
    ];
    for (const path of orphanPaths) io.files.set(path, "unpublished orphan preserved for separate maintenance");
    await store.snapshot(rows("C"), { cursor: 4 });
    await store.runMaintenance();
    for (const path of orphanPaths) {
        assert.equal(io.files.get(path), "unpublished orphan preserved for separate maintenance");
        assert.equal(io.events.some((event) => event.method === "remove" && event.path === path), false);
    }
}

async function sharedSnapshotAndOverlappingWalRangesStayProtectedUntilBothHeadsAdvance(): Promise<void> {
    const io = new MemorySegmentedIO();
    const store = await open(io);
    await store.initialize(rows("A", 1), { cursor: 0 });
    for (let generation = 1; generation <= 32; generation++) {
        await store.commit([entry("note-0.md", `mutation-${generation}`), cursorRow(generation)]);
    }
    await store.runMaintenance();
    assert.equal(store.walSegments, 32);
    assert.equal(io.events.some((event) => event.method === "remove" && isData(event.path)), false,
        "GC pruned a WAL prefix still reachable from the shared retained snapshot");
    const oldPaths = retainedClosure(io);
    const oldState = await replay(io);
    await store.snapshot([entry("note-0.md", "mutation-32")], { cursor: 32 });
    await store.runMaintenance();
    for (const path of oldPaths) assert.ok(io.files.has(path), "first snapshot pruned its still-retained predecessor");
    assert.deepEqual(await replay(io, ".bak"), oldState);
    await store.snapshot([], { cursor: 33 });
    await store.runMaintenance();
    for (const path of oldPaths) assert.equal(io.files.has(path), false, "second snapshot leaked a retired WAL range");
    assert.deepEqual(await replay(io, ".bak"), oldState);
    assert.deepEqual((await replay(io)).entries, []);
    await store.snapshot([], { cursor: 34 });
    await store.runMaintenance();
    assert.equal(io.files.size, 2, "empty current/backup snapshots leaked retired data pages");
    assert.equal((await replay(io)).cursor, 34);
    assert.equal((await replay(io, ".bak")).cursor, 33);
}

async function unverifiedHeadReadbackMustNotStartGC(): Promise<void> {
    const { io, store, candidate } = await readyForGC();
    const expectedBackup = await replay(io);
    const before = readStoreTestFrame(io, HEAD).generation;
    let injected = false;
    io.events.length = 0;
    io.onBoundary = (event, adapter) => {
        if (event.method === "read" && event.phase === "after" && event.path === HEAD &&
            readStoreTestFrame(adapter, HEAD).generation > before) {
            injected = true;
            throw storeTestIOError();
        }
    };
    await assert.rejects(store.snapshot(rows("C"), { cursor: 4 }));
    io.onBoundary = undefined;
    assert.ok(injected, "fixture did not reject the promoted head's verification readback");
    assert.equal(io.events.some((event) => event.method === "remove" && isData(event.path)), false,
        "GC began before the successor publication was verified");
    assert.equal(store.deferredCleanupCount, 0);
    for (const path of candidate) assert.ok(io.files.has(path));
    // Adapter completion was ambiguous, but this test's actual bytes contain
    // a complete new head. Fresh recovery may adopt it, with backup intact.
    assert.equal((await replay(io)).cursor, 4);
    assert.deepEqual(await replay(io, ".bak"), expectedBackup);
}

async function unexpectedActualBackupSkipsCleanupWithoutRejectingPublication(): Promise<void> {
    for (const mode of ["mismatch", "read-failure", "missing", "corrupt"] as const) {
        const { io, store, candidate } = await readyForGC();
        const cachedPredecessorState = await replay(io);
        const supersededBackupState = await replay(io, ".bak");
        const supersededBackupRaw = io.files.get(`${HEAD}.bak`)!;
        const before = readStoreTestFrame(io, HEAD).generation;
        let successorReadback = false;
        let rejectedReads = 0;
        io.events.length = 0;
        io.onBoundary = (event, adapter) => {
            if (!successorReadback && event.method === "read" && event.phase === "after" && event.path === HEAD &&
                readStoreTestFrame(adapter, HEAD).generation > before) {
                successorReadback = true;
                // Deliberately violate the single-writer assumption at the
                // adapter boundary. A stale cached predecessor must not grant
                // GC authority over the different, actually retained backup.
                if (mode === "mismatch") adapter.files.set(`${HEAD}.bak`, supersededBackupRaw);
                else if (mode === "missing") adapter.files.delete(`${HEAD}.bak`);
                else if (mode === "corrupt") adapter.files.set(`${HEAD}.bak`, "{unexpected corrupt backup");
            }
            if (mode === "read-failure" && successorReadback && event.path === `${HEAD}.bak` &&
                event.method === "read" && event.phase === "before") {
                rejectedReads++;
                throw storeTestIOError();
            }
        };
        await store.snapshot(rows("C"), { cursor: 4 });
        io.onBoundary = undefined;
        assert.ok(successorReadback, `${mode} did not reach the intended post-publication guard`);
        assert.equal(rejectedReads, mode === "read-failure" ? 1 : 0);
        assert.equal(store.deferredCleanupCount, 1, `${mode} did not record one deferred cleanup guard failure`);
        assert.equal(io.events.some((event) => event.method === "remove" && isData(event.path)), false,
            `${mode} still pruned against an unverified cached predecessor`);
        for (const path of candidate) assert.ok(io.files.has(path), `${mode} deleted a possible retained backup page`);
        assert.equal((await replay(io)).cursor, 4, `${mode} rejected or rolled back the verified successor`);
        await store.commit([]); // Maintenance failure did not poison the writer.

        if (mode === "mismatch" || mode === "read-failure") {
            assert.deepEqual(await replay(io, ".bak"),
                mode === "mismatch" ? supersededBackupState : cachedPredecessorState,
                `${mode} left a valid backup head with an unreplayable closure`);
            // A later ordinary publication may proceed once the adapter is
            // stable; this guard is not a cross-process locking protocol.
            await store.commit([entry("after-guard.md", "accepted"), cursorRow(5)]);
            assert.equal((await replay(io)).cursor, 5);
            assert.equal((await replay(io, ".bak")).cursor, 4);
            assert.equal(store.deferredCleanupCount, 1);
        }
        checkedBackupGuardSkips++;
    }
}

async function run(): Promise<void> {
    await manyCyclesRetainOnlyTwoCompleteReachableClosures();
    await failedCleanupNeverRejectsTheAlreadyCommittedHead();
    await maintenanceYieldFailureDoesNotUndoPublication();
    await automaticMaintenanceWaitDoesNotOwnTheWriterLane();
    await nonClosingDrainJoinsAutomaticNativeIO();
    await injectedYieldAlsoOwnsMaintenanceUnlessOverridden();
    await explicitMaintenanceWaitDoesNotOwnTheWriterLane();
    await closeCancelsWaitingAdmissionAndJoinsAdmittedIO();
    await explicitPassAdmitsEveryExactPathSeparately();
    await persistentFailuresCannotGrowCleanupOwnershipWithoutBound();
    await everyGCInterruptionPreservesBothRetainedHeads();
    await cleanupDoesNotDiscoverOrDeleteUnpublishedOrphans();
    await sharedSnapshotAndOverlappingWalRangesStayProtectedUntilBothHeadsAdvance();
    await unverifiedHeadReadbackMustNotStartGC();
    await unexpectedActualBackupSkipsCleanupWithoutRejectingPublication();
    console.log(`segmented-store-gc.test: ${checkedPublications} bounded publications, ${checkedGCInterruptions} GC restart boundaries, ${checkedBackupGuardSkips} actual-backup guard skips and failure/closure regressions passed`);
}

void run().catch((error) => { console.error(error); process.exitCode = 1; });
