import { strict as assert } from "node:assert";
import { createHash } from "node:crypto";
import {
    SegmentedStore, StoreRecoveryError, createMigrationProof, segmentedMigrationPaths,
    type MigrationProof,
} from "./segmented-store";
import {
    MemorySegmentedIO, readStoreTestFrame, sealStoreTestFrame, storeTestIOError,
    type StoreTestDisk, type StoreTestBoundary,
} from "./segmented-store-test-io";

const DIRECTORY = "migration-store";
const HEAD = `${DIRECTORY}/head.json`;
const PATHS = segmentedMigrationPaths(DIRECTORY);
const noPause = async () => {};
const metadata = { nextId: 901, cursor: "legacy-cut", version: 1 };
const rows = Object.freeze(Array.from({ length: 270 }, (_, index) => Object.freeze({ id: index + 1, path: `note-${index}.md` })));
const originals = new Map([["legacy/main", "exact legacy main bytes\n"], ["legacy/backup", "exact backup bytes\n"]]);
const sourceSha256 = createHash("sha256").update(JSON.stringify([...originals])).digest("hex");
const fresh = () => new MemorySegmentedIO({ files: originals, directories: new Set(["legacy"]) });
const storeFor = (io: MemorySegmentedIO, directory = DIRECTORY) => new SegmentedStore(io, directory, { yield: noPause });
const codeIs = (code: StoreRecoveryError["code"]) => (error: unknown) => error instanceof StoreRecoveryError && error.code === code;

async function load(io: MemorySegmentedIO) {
    const store = storeFor(io);
    const snapshot: unknown[] = [];
    const mutations: unknown[] = [];
    let loadedMetadata: unknown;
    const present = await store.load((value) => { loadedMetadata = value; }, (row) => { snapshot.push(row); }, (row) => { mutations.push(row); });
    return { store, present, metadata: loadedMetadata, snapshot, mutations };
}

function originalsIntact(io: MemorySegmentedIO): void {
    for (const [path, bytes] of originals) assert.equal(io.files.get(path), bytes, "migration modified original source evidence");
}

async function assertInitial(io: MemorySegmentedIO): Promise<void> {
    const restored = await load(io);
    assert.equal(restored.present, true);
    assert.deepEqual(restored.metadata, metadata);
    assert.deepEqual(restored.snapshot, rows);
    assert.deepEqual(restored.mutations, []);
    assert.equal(restored.store.sequence, 0);
    originalsIntact(io);
}

/** Exactly the caller contract: only RECOVERY_REQUIRED permits preparing the
 * same proven source and attempting explicit retry. Other errors stay errors. */
async function recoverOrRetry(io: MemorySegmentedIO, proof: MigrationProof): Promise<void> {
    let recovered: Awaited<ReturnType<typeof load>>;
    try { recovered = await load(io); }
    catch (error) {
        if (!codeIs("RECOVERY_REQUIRED")(error)) throw error;
        await storeFor(io).retryInitialization(rows, metadata, proof);
        await assertInitial(io);
        return;
    }
    if (!recovered.present) await recovered.store.initializeMigration(rows, metadata, proof);
    await assertInitial(io);
}

let restartBoundaries = 0;
let rejectedBoundaries = 0;
let partialWrites = 0;
let fenceFailures = 0;

async function proofAndExplicitRecoveryContract(proof: MigrationProof): Promise<void> {
    assert.equal(proof.metadataSha256, createHash("sha256").update(JSON.stringify(metadata)).digest("hex"));
    await assert.rejects(createMigrationProof("bad kind", sourceSha256, metadata), codeIs("CORRUPT"));
    await assert.rejects(createMigrationProof("legacy-v1", "mtime-size", metadata), codeIs("CORRUPT"));
    await assert.rejects(createMigrationProof("legacy-v1", sourceSha256, undefined), codeIs("CORRUPT"));
    await assert.rejects(createMigrationProof("legacy-v1", sourceSha256, "x".repeat(128 * 1024)), codeIs("LIMIT"));

    const io = fresh();
    const store = (await load(io)).store;
    io.events.length = 0;
    await store.initializeMigration(rows, metadata, proof);
    const intentWrite = io.events.findIndex(event => event.method === "write" && event.phase === "after" && event.path === PATHS.intent);
    const mkdir = io.events.findIndex(event => event.method === "mkdir" && event.phase === "before" && event.path === DIRECTORY);
    assert.ok(intentWrite >= 0 && intentWrite < mkdir, "intent was not durable before first target-directory creation");
    await assertInitial(io.clone());
    assert.equal(io.files.has(PATHS.advanced), false);
    await assert.rejects(store.retryInitialization(rows, metadata, proof), codeIs("RECOVERY_REQUIRED"));
    await assert.rejects(store.initializeMigration(rows, metadata, proof), codeIs("RECOVERY_REQUIRED"));
    originalsIntact(io);

    const mutable = fresh();
    const mutableStore = (await load(mutable)).store;
    const callerMetadata = { ...metadata };
    const callerProof = { ...proof };
    const initializing = mutableStore.initializeMigration(rows, callerMetadata, callerProof);
    callerMetadata.nextId++;
    callerProof.sourceSha256 = "e".repeat(64);
    await initializing;
    await assertInitial(mutable);
}

async function prepared(proof: MigrationProof): Promise<MemorySegmentedIO> {
    const io = fresh();
    const store = (await load(io)).store;
    let injected = false;
    io.onBoundary = (event, adapter) => {
        if (event.method === "write" && event.phase === "before" && event.path.includes("/page-")) {
            injected = true;
            adapter.files.set(event.path, event.data!.slice(0, 123));
            throw storeTestIOError("ENOSPC");
        }
    };
    await assert.rejects(store.initializeMigration(rows, metadata, proof));
    io.onBoundary = undefined;
    assert.ok(injected);
    assert.ok(io.files.has(PATHS.intent));
    assert.equal(io.files.has(HEAD), false);
    await assert.rejects(load(io.clone()), codeIs("RECOVERY_REQUIRED"));
    return io;
}

async function initializationAndRetryAtEveryBoundary(proof: MigrationProof): Promise<void> {
    for (const retry of [false, true]) {
        const initial = retry ? (await prepared(proof)).snapshot() : fresh().snapshot();
        const baseline = new MemorySegmentedIO(initial);
        const writer = retry ? storeFor(baseline) : (await load(baseline)).store;
        baseline.events.length = 0;
        const checkpoints: Array<{ event: StoreTestBoundary; disk: StoreTestDisk }> = [];
        baseline.onBoundary = (event, io) => { checkpoints.push({ event, disk: io.snapshot() }); };
        const operation = (store: SegmentedStore) => retry ? store.retryInitialization(rows, metadata, proof) :
            store.initializeMigration(rows, metadata, proof);
        await operation(writer);
        baseline.onBoundary = undefined;
        await assertInitial(baseline);

        for (const { disk } of checkpoints) {
            await recoverOrRetry(new MemorySegmentedIO(disk), proof);
            restartBoundaries++;
        }
        for (const { event } of checkpoints) {
            const io = new MemorySegmentedIO(initial);
            const failed = retry ? storeFor(io) : (await load(io)).store;
            io.events.length = 0;
            let injected = false;
            io.onBoundary = (candidate) => {
                if (candidate.index === event.index) { injected = true; throw storeTestIOError(); }
            };
            await assert.rejects(operation(failed));
            assert.ok(injected);
            io.onBoundary = undefined;
            await assert.rejects(failed.commit([]), codeIs("RECOVERY_REQUIRED"));
            await recoverOrRetry(io, proof);
            rejectedBoundaries++;
        }
        for (const { event } of checkpoints) {
            if (event.method !== "write" || event.phase !== "before") continue;
            const io = new MemorySegmentedIO(initial);
            const failed = retry ? storeFor(io) : (await load(io)).store;
            io.events.length = 0;
            io.onBoundary = (candidate, adapter) => {
                if (candidate.index !== event.index) return;
                adapter.files.set(candidate.path, candidate.data!.slice(0, Math.floor(candidate.data!.length / 2)));
                throw storeTestIOError("ENOSPC");
            };
            await assert.rejects(operation(failed));
            io.onBoundary = undefined;
            if (event.path === PATHS.intent) {
                // No valid source identity survived. Even with an absent target
                // directory, a caller-supplied proof cannot overwrite unknown
                // on-disk evidence and pretend it was the same attempted import.
                const evidence = io.snapshot();
                assert.equal(io.directories.has(DIRECTORY), false);
                await assert.rejects(load(io), codeIs("CORRUPT"));
                await assert.rejects(storeFor(io).retryInitialization(rows, metadata, proof), codeIs("CORRUPT"));
                assert.deepEqual(io.snapshot(), evidence);
            } else await recoverOrRetry(io, proof);
            originalsIntact(io);
            partialWrites++;
        }
    }
}

async function proofMismatchAndUnmarkedEvidenceNeverGetRewritten(proof: MigrationProof): Promise<void> {
    const initial = await prepared(proof);
    for (const changed of [
        { ...proof, sourceKind: "other-parser-v1" },
        { ...proof, sourceSha256: "f".repeat(64) },
        { ...proof, metadataSha256: "e".repeat(64) },
    ]) {
        const io = initial.clone();
        const before = io.snapshot();
        await assert.rejects(storeFor(io).retryInitialization(rows, metadata, changed), codeIs("CONTRADICTING_COPIES"));
        assert.deepEqual(io.snapshot(), before);
    }
    const changedMetadata = initial.clone();
    const before = changedMetadata.snapshot();
    await assert.rejects(storeFor(changedMetadata).retryInitialization(rows, { ...metadata, nextId: 902 }, proof), codeIs("CONTRADICTING_COPIES"));
    assert.deepEqual(changedMetadata.snapshot(), before);

    const unmarked = fresh();
    unmarked.directories.add(DIRECTORY);
    unmarked.files.set(`${DIRECTORY}/unknown-original`, "preserve this evidence");
    const unmarkedBefore = unmarked.snapshot();
    await assert.rejects(load(unmarked), codeIs("RECOVERY_REQUIRED"));
    await assert.rejects(storeFor(unmarked).retryInitialization(rows, metadata, proof), codeIs("RECOVERY_REQUIRED"));
    await assert.rejects(storeFor(unmarked).initializeMigration(rows, metadata, proof), codeIs("RECOVERY_REQUIRED"));
    assert.deepEqual(unmarked.snapshot(), unmarkedBefore);

    const cachedAbsent = fresh();
    const cachedStore = (await load(cachedAbsent)).store;
    cachedAbsent.files.set(PATHS.intent, initial.files.get(PATHS.intent)!);
    const cachedEvidence = cachedAbsent.snapshot();
    await assert.rejects(cachedStore.initialize(rows, metadata), codeIs("RECOVERY_REQUIRED"));
    assert.deepEqual(cachedAbsent.snapshot(), cachedEvidence, "plain initialize bypassed a newly present migration intent");

    for (const path of [PATHS.intent, PATHS.advanced]) {
        for (const mode of ["unknown", "corrupt", "unavailable"] as const) {
            const io = initial.clone();
            const marker = readStoreTestFrame(io, PATHS.intent);
            if (path === PATHS.advanced) marker.kind = "migration-advanced";
            if (mode === "unknown") marker.schema = 2;
            io.files.set(path, mode === "corrupt" ? "{partial marker" : sealStoreTestFrame(marker));
            if (mode === "unavailable") io.onBoundary = event => {
                if (event.path === path && event.method === "read" && event.phase === "before") throw storeTestIOError();
            };
            const expected = mode === "unknown" ? "UNKNOWN_SCHEMA" : mode === "corrupt" ? "CORRUPT" : "READ_FAILED";
            const evidence = io.snapshot();
            await assert.rejects(load(io), codeIs(expected));
            await assert.rejects(storeFor(io).retryInitialization(rows, metadata, proof), codeIs(expected));
            assert.deepEqual(io.snapshot(), evidence);
        }
    }

    const completed = initial.clone();
    await recoverOrRetry(completed, proof);
    const page = [...initial.files.keys()].find(path => path.includes("/page-"))!;
    for (const mode of ["valid-different", "unknown-schema", "not-a-prefix"] as const) {
        const io = initial.clone();
        const value = readStoreTestFrame(completed, page);
        if (mode === "valid-different") value.rows[0].path = "unrelated.md";
        if (mode === "unknown-schema") value.schema = 2;
        io.files.set(page, mode === "not-a-prefix" ? "{other incomplete content" : sealStoreTestFrame(value));
        const evidence = io.snapshot();
        await assert.rejects(storeFor(io).retryInitialization(rows, metadata, proof),
            codeIs(mode === "valid-different" ? "CONTRADICTING_COPIES" : mode === "unknown-schema" ? "UNKNOWN_SCHEMA" : "CORRUPT"));
        assert.deepEqual(io.snapshot(), evidence);
    }
}

async function advanceFencePrecedesAnyNoninitialDataAndBlocksStaleRetry(proof: MigrationProof): Promise<void> {
    for (const snapshot of [false, true]) {
        const baseline = fresh();
        const initialStore = (await load(baseline)).store;
        await initialStore.initializeMigration(rows, metadata, proof);
        const unadvanced = baseline.snapshot();
        const writer = (await load(baseline)).store; // Exercise generic restart, not only the initializing instance.
        await writer.commit([]);
        assert.equal(baseline.files.has(PATHS.advanced), false, "a no-op incorrectly advanced initialization");
        baseline.events.length = 0;
        const operation = (store: SegmentedStore) => snapshot ? store.snapshot(rows, { ...metadata, nextId: 999 }) :
            store.commit([{ id: 901, path: "newer.md" }]);
        await operation(writer);
        const firstData = baseline.events.find(event => event.method === "write" && event.phase === "before" &&
            (event.path.includes("/page-") || event.path.includes("/wal-")))!;
        const fenceReadback = baseline.events.find(event => event.method === "read" && event.phase === "after" && event.path === PATHS.advanced)!;
        assert.ok(fenceReadback && firstData && fenceReadback.index < firstData.index,
            "post-init data writes outran the durable advance fence");

        for (const event of baseline.events.filter(event => event.index <= fenceReadback.index)) {
            const io = new MemorySegmentedIO(unadvanced);
            const failed = (await load(io)).store;
            io.events.length = 0;
            io.onBoundary = candidate => { if (candidate.index === event.index) throw storeTestIOError(); };
            await assert.rejects(operation(failed));
            io.onBoundary = undefined;
            assert.equal(io.events.some(candidate => candidate.method === "write" &&
                (candidate.path.includes("/page-") || candidate.path.includes("/wal-"))), false,
                "failed fence attempt still wrote newer data");
            await assertInitial(io.clone());
            originalsIntact(io);
            fenceFailures++;
        }

        const partial = new MemorySegmentedIO(unadvanced);
        const partialWriter = (await load(partial)).store;
        partial.onBoundary = (event, io) => {
            if (event.method === "write" && event.phase === "before" && event.path === PATHS.advanced) {
                io.files.set(event.path, event.data!.slice(0, 123));
                throw storeTestIOError("ENOSPC");
            }
        };
        await assert.rejects(operation(partialWriter));
        partial.onBoundary = undefined;
        await assert.rejects(load(partial), codeIs("CORRUPT"));
        assert.equal(partial.events.some(event => event.method === "write" &&
            (event.path.includes("/page-") || event.path.includes("/wal-"))), false);

        const lostHeads = baseline.clone();
        lostHeads.files.delete(HEAD); lostHeads.files.delete(`${HEAD}.bak`); lostHeads.files.delete(`${HEAD}.next`);
        const evidence = lostHeads.snapshot();
        await assert.rejects(load(lostHeads), codeIs("RECOVERY_REQUIRED"));
        await assert.rejects(storeFor(lostHeads).retryInitialization(rows, metadata, proof), codeIs("RECOVERY_REQUIRED"));
        assert.deepEqual(lostHeads.snapshot(), evidence, "same stale legacy source overwrote an already advanced store");

        const missingIntent = baseline.clone();
        missingIntent.files.delete(PATHS.intent);
        const orphanFence = missingIntent.snapshot();
        await assert.rejects(load(missingIntent), codeIs("CONTRADICTING_COPIES"));
        await assert.rejects(storeFor(missingIntent).retryInitialization(rows, metadata, proof), codeIs("CONTRADICTING_COPIES"));
        assert.deepEqual(missingIntent.snapshot(), orphanFence, "orphaned advance fence was silently ignored");

        const missingFence = baseline.clone();
        missingFence.files.delete(PATHS.advanced);
        await assert.rejects(load(missingFence), codeIs("RECOVERY_REQUIRED"));
        await assert.rejects(storeFor(missingFence).retryInitialization(rows, metadata, proof), codeIs("RECOVERY_REQUIRED"));
    }
}

async function asyncReplayCallbacksAreAwaitedAndRemainProvisional(proof: MigrationProof): Promise<void> {
    const io = fresh();
    const first = (await load(io)).store;
    await first.initializeMigration(rows, metadata, proof);
    await first.commit([{ id: 901, path: "newer.md" }]);
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    let entered!: () => void;
    const firstCallback = new Promise<void>(resolve => { entered = resolve; });
    let snapshotRows = 0;
    let mutationRows = 0;
    let metadataDone = false;
    const store = storeFor(io);
    const loading = store.load(async () => { await Promise.resolve(); metadataDone = true; }, async () => {
        assert.ok(metadataDone);
        if (snapshotRows === 0) { entered(); await gate; }
        snapshotRows++;
    }, async () => { assert.equal(snapshotRows, rows.length); await Promise.resolve(); mutationRows++; });
    await firstCallback;
    assert.equal(snapshotRows, 0);
    assert.equal(mutationRows, 0);
    let published = false;
    const queued = store.commit([{ id: 902, path: "after-replay.md" }]).then(() => { published = true; });
    for (let index = 0; index < 20; index++) await Promise.resolve();
    assert.equal(published, false, "writer ran before provisional async replay callbacks completed");
    release();
    assert.equal(await loading, true);
    await queued;
    assert.equal(snapshotRows, rows.length);
    assert.equal(mutationRows, 1);
    const failed = storeFor(io);
    await assert.rejects(failed.load(() => {}, async () => { await Promise.resolve(); throw new Error("async row rejected"); }, () => {}), /async row rejected/);
    await assert.rejects(failed.commit([]), codeIs("RECOVERY_REQUIRED"));
}

async function run(): Promise<void> {
    const proof = await createMigrationProof("journal-legacy-v1", sourceSha256, metadata);
    await proofAndExplicitRecoveryContract(proof);
    await initializationAndRetryAtEveryBoundary(proof);
    await proofMismatchAndUnmarkedEvidenceNeverGetRewritten(proof);
    await advanceFencePrecedesAnyNoninitialDataAndBlocksStaleRetry(proof);
    await asyncReplayCallbacksAreAwaitedAndRemainProvisional(proof);
    console.log(`segmented-store-migration.test: ${restartBoundaries} initialization/retry restart boundaries, ${rejectedBoundaries} IO failures, ${partialWrites} partial writes, ${fenceFailures} advance-fence failures and proof/async replay regressions passed`);
}

void run().catch(error => { console.error(error); process.exitCode = 1; });
