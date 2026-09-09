import { strict as assert } from "node:assert";
import { ObsetyncSyncBase, SYNC_BASE_STORE_PATH, ROOT_BASE_PUBLICATION_LIMITS,
    detachRootBasePublication, type RootBasePublication } from "./sync-base";
import { SegmentedStore, StoreRecoveryError } from "./segmented-store";
import { MemorySegmentedIO, readStoreTestFrame, sealStoreTestFrame, storeTestIOError,
    type StoreTestDisk } from "./segmented-store-test-io";

const hash = (digit: string) => digit.repeat(64);
const baseFor = (io: MemorySegmentedIO) => new ObsetyncSyncBase({ vault: { adapter: io } } as any);
function publication(sequence = 1): RootBasePublication {
    return { identity: { scopeHash: hash("a"), sequence, mutationId: sequence.toString(16).padStart(32, "0"),
        requestHash: hash("b") }, candidateRoot: hash("c"), committedAt: 100,
        entries: [ { action: "upsert", path: "a.md", hash: hash("d"), mtime: 10, size: 8, treeMtime: 9 },
            { action: "delete", path: "z.md" } ] };
}
async function fixture(): Promise<{ io: MemorySegmentedIO; base: ObsetyncSyncBase }> {
    const io = new MemorySegmentedIO();
    const base = baseFor(io); await base.load();
    base.setEntry("a.md", hash("1"), 1, 1);
    base.setEntry("z.md", hash("2"), 2, 2);
    base.setTreeBaseRoot(hash("3")); base.setLastSyncTimestamp(3);
    await base.checkpoint(); io.events.length = 0;
    return { io, base };
}
async function restored(disk: StoreTestDisk): Promise<ObsetyncSyncBase> {
    const base = baseFor(new MemorySegmentedIO(disk)); await base.load(); return base;
}
function assertPublished(base: ObsetyncSyncBase): void {
    assert.equal(base.getHash("a.md"), hash("d"));
    assert.equal(base.getTreeMtime("a.md"), 9);
    assert.equal(base.getHash("z.md"), null);
    assert.equal(base.treeBaseRoot, hash("c"));
    assert.equal(base.lastSyncTimestamp, 100);
    assert.deepEqual(base.lastAppliedPublication?.identity, publication().identity);
    assert.match(base.lastAppliedPublication!.payloadHash, /^[0-9a-f]{64}$/);
}
function assertOld(base: ObsetyncSyncBase): void {
    assert.equal(base.getHash("a.md"), hash("1")); assert.equal(base.getHash("z.md"), hash("2"));
    assert.equal(base.treeBaseRoot, hash("3")); assert.equal(base.lastSyncTimestamp, 3);
    assert.equal(base.lastAppliedPublication, null);
}
function gate(): { promise: Promise<void>; resolve(): void } {
    let resolve!: () => void;
    const promise = new Promise<void>(done => { resolve = done; });
    return { promise, resolve };
}

async function oneAtomicPublicationAndDetachedSubmission(): Promise<void> {
    const { io, base } = await fixture();
    const submitted = publication();
    const pending = base.commitRootPublication(submitted);
    submitted.identity.requestHash = hash("e");
    submitted.candidateRoot = hash("f");
    (submitted.entries[0] as any).hash = hash("0");
    assertOld(base);
    assert.equal(await pending, "applied");
    assertPublished(base); assertPublished(await restored(io.snapshot()));
    assert.equal(io.events.filter(event => event.method === "rename" && event.phase === "after" &&
        event.to === `${SYNC_BASE_STORE_PATH}/head.json`).length, 1, "publication used split base/root heads");
    const rows = io.events.filter(event => event.method === "write" && event.phase === "before" &&
        event.path.includes("/wal-")).flatMap(event => JSON.parse(JSON.parse(event.data!).payload).rows);
    assert.equal(rows.at(-1).schema, 2);
    assert.equal(rows.at(-1).op, "root-publication");
    assert.equal(rows.length, 3);
    const marker = base.lastAppliedPublication!;
    marker.identity.requestHash = hash("f"); marker.candidateRoot = hash("e");
    assertPublished(base);
}

async function duplicateDoesNotReplayOverNewerMutations(): Promise<void> {
    const { io, base } = await fixture();
    assert.deepEqual(await Promise.all([base.commitRootPublication(publication()),
        base.commitRootPublication(publication())]), ["applied", "already-applied"]);
    base.setEntry("a.md", hash("4"), 40, 4);
    base.setEntry("z.md", hash("5"), 50, 5);
    base.setTreeBaseRoot(hash("6")); base.setLastSyncTimestamp(60);
    await base.checkpoint(); io.events.length = 0;
    assert.equal(await base.commitRootPublication(publication()), "already-applied");
    assert.equal(io.events.length, 0, "exact duplicate emitted storage IO");
    assert.equal(base.getHash("a.md"), hash("4")); assert.equal(base.getHash("z.md"), hash("5"));
    assert.equal(base.treeBaseRoot, hash("6")); assert.equal(base.lastSyncTimestamp, 60);
    const reload = await restored(io.snapshot());
    assert.equal(await reload.commitRootPublication(publication()), "already-applied");
    assert.equal(reload.getHash("a.md"), hash("4"));

    const changes: Array<(value: RootBasePublication) => void> = [
        value => { value.candidateRoot = hash("e"); },
        value => { value.committedAt++; },
        value => { (value.entries[0] as any).size++; },
        value => { value.identity.mutationId = "f".repeat(32); },
        value => { value.identity.requestHash = hash("e"); },
        value => { value.identity.scopeHash = hash("e"); },
    ];
    for (const change of changes) {
        const wrong = publication(); change(wrong);
        await assert.rejects(base.commitRootPublication(wrong), StoreRecoveryError);
        assert.equal(io.events.length, 0, "identity/payload contradiction changed storage");
    }
    assert.equal(await base.commitRootPublication(publication(2)), "applied");
    await assert.rejects(base.commitRootPublication(publication()), /older than/);
    await base.save(); io.events.length = 0;
    await base.save();
    assert.equal(io.events.length, 0, "rejected/duplicate submission versions kept the full snapshot dirty");
}

async function newerSettersStayBehindNativePublication(): Promise<void> {
    const { io, base } = await fixture();
    const entered = gate(); const release = gate(); let gated = false;
    const cuts: StoreTestDisk[] = [];
    io.onBoundary = async event => {
        if (!gated && event.method === "write" && event.phase === "before" && event.path.includes("/wal-")) {
            gated = true; entered.resolve(); await release.promise;
        }
        if (event.method === "rename" && event.phase === "after" && event.to?.endsWith("/head.json")) cuts.push(io.snapshot());
    };
    const committing = base.commitRootPublication(publication());
    await entered.promise; assertOld(base);
    base.setEntry("a.md", hash("7"), 70, 7);
    base.setEntry("z.md", hash("8"), 80, 8);
    base.setTreeBaseRoot(hash("9")); base.setLastSyncTimestamp(90);
    const checkpoint = base.checkpoint();
    assert.equal(base.getHash("a.md"), hash("7"));
    assert.equal(base.lastAppliedPublication, null, "marker exposed before native verification");
    release.resolve();
    assert.equal(await committing, "applied"); await checkpoint; io.onBoundary = undefined;
    assert.equal(cuts.length, 2);
    assertPublished(await restored(cuts[0]));
    const final = await restored(cuts[1]);
    assert.equal(final.getHash("a.md"), hash("7")); assert.equal(final.getHash("z.md"), hash("8"));
    assert.equal(final.treeBaseRoot, hash("9")); assert.equal(final.lastSyncTimestamp, 90);
    assert.deepEqual(final.lastAppliedPublication?.identity, publication().identity);
}

async function earlierSaveCannotPublishLaterSettersAheadOfBarrier(): Promise<void> {
    const { io, base } = await fixture();
    base.setEntry("a.md", hash("4"), 4, 4);
    const entered = gate(); const release = gate(); let gated = false;
    const cuts: StoreTestDisk[] = [];
    io.onBoundary = async event => {
        if (!gated && event.method === "write" && event.phase === "before" && event.path.includes("/wal-")) {
            gated = true; entered.resolve(); await release.promise;
        }
        if (event.method === "rename" && event.phase === "after" && event.to?.endsWith("/head.json")) cuts.push(io.snapshot());
    };
    const oldSave = base.save(); await entered.promise;
    const committing = base.commitRootPublication(publication());
    base.setEntry("a.md", hash("7"), 7, 7);
    release.resolve(); await oldSave;
    assert.equal(await committing, "applied"); io.onBoundary = undefined;
    let seenPublication = false;
    for (const cut of cuts) {
        const state = await restored(cut);
        if (state.lastAppliedPublication) seenPublication = true;
        if (!seenPublication) assert.equal(state.getHash("a.md"), hash("4"), "old save flushed a post-publication setter early");
    }
    assert.equal(cuts.length, 4, "expected pre-cut checkpoint, snapshot, publication and later setter cut");
    assertPublished(await restored(cuts[2]));
    assert.equal((await restored(cuts[3])).getHash("a.md"), hash("7"));
}

async function acceptedCutSurvivesLaterSetterFailureAndQueuedPublications(): Promise<void> {
    const { io, base } = await fixture();
    let walWrites = 0;
    io.onBoundary = event => {
        if (event.method !== "write" || event.phase !== "before" || !event.path.includes("/wal-")) return;
        walWrites++;
        if (walWrites === 1) base.setEntry("a.md", hash("7"), 70, 7);
        else throw storeTestIOError("ENOSPC");
    };
    await assert.rejects(base.commitRootPublication(publication()), /ENOSPC/);
    assert.equal(walWrites, 2);
    assert.throws(() => base.lastAppliedPublication, /validated recovery/, "unavailable base marker licensed settlement before reload");
    assert.equal(base.getHash("a.md"), hash("7"), "later visible setter was dropped");
    assert.throws(() => base.setEntry("unsafe.md", hash("f"), 1, 1), /validated recovery/);
    io.onBoundary = undefined;
    const resumedIO = new MemorySegmentedIO(io.snapshot());
    const reload = baseFor(resumedIO); await reload.load();
    assertPublished(reload);
    assert.equal(await reload.commitRootPublication(publication()), "already-applied");

    // Multiple queued callers retain submission order even when a later
    // ordinary setter is immediately visible before either native commit.
    const next = publication(2);
    (next.entries[0] as any).hash = hash("e"); next.candidateRoot = hash("f"); next.committedAt = 200;
    const first = reload.commitRootPublication(publication());
    reload.setEntry("a.md", hash("8"), 80, 8);
    const second = reload.commitRootPublication(next);
    reload.setEntry("a.md", hash("9"), 90, 9);
    assert.deepEqual(await Promise.all([first, second]), ["already-applied", "applied"]);
    assert.equal(reload.getHash("a.md"), hash("9"));
    assert.equal(reload.treeBaseRoot, hash("f"));
    assert.equal(reload.lastAppliedPublication?.identity.sequence, 2);
    const latest = await restored(resumedIO.snapshot());
    assert.equal(latest.getHash("a.md"), hash("9"));
    assert.equal(latest.treeBaseRoot, hash("f"));
    assert.equal(latest.lastAppliedPublication?.identity.sequence, 2);
}

async function sameVisibleRootSetterAndBoundedPublicationQueue(): Promise<void> {
    const { io, base } = await fixture();
    const entered = gate(); const release = gate(); let gated = false;
    io.onBoundary = async event => {
        if (!gated && event.method === "write" && event.phase === "before" && event.path.includes("/wal-")) {
            gated = true; entered.resolve(); await release.promise;
        }
    };
    const active = base.commitRootPublication(publication()); await entered.promise;
    base.setTreeBaseRoot(hash("3")); // Same visible value, but later than the admitted candidate.
    const pending = Array.from({ length: ROOT_BASE_PUBLICATION_LIMITS.queuedRequests - 1 }, () =>
        base.commitRootPublication(publication()));
    let inspected = false;
    const excessive = { ...publication() };
    Object.defineProperty(excessive, "entries", { get() { inspected = true; return publication().entries; } });
    await assert.rejects(base.commitRootPublication(excessive), /queue is full/);
    assert.equal(inspected, false, "full queue copied new caller metadata before refusing it");
    release.resolve();
    assert.deepEqual(await Promise.all([active, ...pending]), ["applied", "already-applied", "already-applied", "already-applied"]);
    io.onBoundary = undefined;
    assert.equal(base.treeBaseRoot, hash("3"), "same-value post-submission root setter was skipped");
    assert.equal((await restored(io.snapshot())).treeBaseRoot, hash("3"));
    assert.equal(await base.commitRootPublication(publication(2)), "applied", "completed queue owners were not released");
}

async function strictInputBoundsNeverWrite(): Promise<void> {
    const { io, base } = await fixture();
    const invalid: unknown[] = [
        { ...publication(), extra: true },
        { ...publication(), candidateRoot: "C".repeat(64) },
        { ...publication(), committedAt: NaN },
        { ...publication(), identity: { ...publication().identity, sequence: 0 } },
        { ...publication(), identity: { ...publication().identity, sequence: Number.MAX_SAFE_INTEGER + 1 } },
        { ...publication(), entries: [publication().entries[0], publication().entries[0]] },
        { ...publication(), entries: [...publication().entries].reverse() },
        { ...publication(), entries: [{ action: "delete", path: "../bad" }] },
        { ...publication(), entries: [{ action: "delete", path: "a.md", hash: hash("d") }] },
        { ...publication(), entries: [{ action: "upsert", path: "a.md", hash: hash("d"), size: -1, mtime: 1 }] },
        { ...publication(), entries: Array.from({ length: ROOT_BASE_PUBLICATION_LIMITS.entries + 1 }, (_, i) =>
            ({ action: "delete", path: `${String(i).padStart(4, "0")}.md` })) },
        { ...publication(), entries: Array.from({ length: 100 }, (_, i) =>
            ({ action: "delete", path: `${String(i).padStart(4, "0")}${"x".repeat(3000)}.md` })) },
    ];
    for (const value of invalid) {
        assert.throws(() => detachRootBasePublication(value), StoreRecoveryError);
        await assert.rejects(base.commitRootPublication(value as any), StoreRecoveryError);
        assert.equal(io.events.length, 0);
        assertOld(base);
    }
    const maximum = publication(Number.MAX_SAFE_INTEGER);
    maximum.entries = Array.from({ length: 256 }, (_, i) => ({ action: "delete", path: `${String(i).padStart(4, "0")}.md` }));
    const detached = detachRootBasePublication(maximum);
    (maximum.entries[0] as any).path = "changed";
    assert.equal(detached.entries[0].path, "0000.md");
    assert.equal(detached.identity.sequence, Number.MAX_SAFE_INTEGER);
    assert.equal(await base.commitRootPublication(detached), "applied");
}

async function publicationFailuresNeverExposeMixedState(): Promise<void> {
    const { io: seed } = await fixture();
    const successIO = seed.clone(); const success = baseFor(successIO); await success.load(); successIO.events.length = 0;
    await success.commitRootPublication(publication());
    const count = successIO.events.length;
    for (let boundary = 0; boundary < count; boundary++) {
        const io = seed.clone(); const base = baseFor(io); await base.load(); io.events.length = 0;
        let injected = false;
        io.onBoundary = event => {
            if (event.index === boundary) { injected = true; throw storeTestIOError("EIO"); }
        };
        let accepted = false;
        try { accepted = await base.commitRootPublication(publication()) === "applied"; }
        catch {
            assert.equal(base.getHash("a.md"), hash("1")); assert.equal(base.getHash("z.md"), hash("2"));
            assert.equal(base.treeBaseRoot, hash("3")); assert.equal(base.lastSyncTimestamp, 3);
            assert.throws(() => base.lastAppliedPublication, /validated recovery/);
            assert.throws(() => base.setEntry("unsafe.md", hash("f"), 1, 1), /validated recovery/);
        }
        assert.ok(injected, `boundary ${boundary} not reached`);
        io.onBoundary = undefined;
        let reload: ObsetyncSyncBase;
        try { reload = await restored(io.snapshot()); }
        catch (error) {
            assert.ok(error instanceof StoreRecoveryError); assert.equal(accepted, false); continue;
        }
        if (accepted || reload.lastAppliedPublication) {
            assertPublished(reload);
            assert.equal(await reload.commitRootPublication(publication()), "already-applied");
        } else {
            assertOld(reload);
            assert.equal(await reload.commitRootPublication(publication()), "applied");
            assertPublished(reload);
        }
    }
    console.log(`sync-base-publication: ${count} actual adapter failure boundaries checked`);
}

async function markerSurvivesCompactionAndFencesOlderReaders(): Promise<void> {
    const { io, base } = await fixture();
    await base.commitRootPublication(publication());
    const beforeSnapshot = io.snapshot();
    await base.save();
    const afterSnapshot = io.snapshot();
    const head = readStoreTestFrame(io, `${SYNC_BASE_STORE_PATH}/head.json`);
    assert.equal(head.snapshot.metadata.schema, 2);
    assert.deepEqual(head.snapshot.metadata.lastAppliedPublication, base.lastAppliedPublication);
    for (const disk of [beforeSnapshot, afterSnapshot]) {
        const restoredBase = await restored(disk);
        assert.equal(await restoredBase.commitRootPublication(publication()), "already-applied");
        const oldIO = new MemorySegmentedIO(disk);
        const oldReader = new SegmentedStore(oldIO, SYNC_BASE_STORE_PATH);
        // Historical v1 base validators reject unknown metadata/operation
        // schemas; real store callbacks remain provisional after this failure.
        await assert.rejects(oldReader.load(metadata => {
            const value = metadata as any;
            if (value.schema !== undefined && value.schema !== 1) throw new Error("old metadata reader fenced");
        }, () => {}, row => {
            const value = row as any;
            if (value.schema !== undefined && value.schema !== 1) throw new Error("old WAL reader fenced");
        }), /old .* reader fenced/);
        const events = oldIO.events.length;
        await assert.rejects(oldReader.commit([{ op: "timestamp", value: 999 }]), StoreRecoveryError);
        assert.equal(oldIO.events.length, events, "failed old reader could overwrite authority");
    }
    // Current readers must not silently ignore the marker if a future or
    // missing schema is presented with an otherwise valid storage checksum.
    for (const alter of [
        (metadata: any) => { delete metadata.schema; },
        (metadata: any) => { metadata.schema = 3; },
        (metadata: any) => { delete metadata.lastAppliedPublication; },
        (metadata: any) => { metadata.futureAuthority = true; },
    ]) {
        const corrupted = new MemorySegmentedIO(afterSnapshot);
        // Keep candidates mutually consistent so the semantic parser, not a
        // filename contradiction, is the reason the store becomes unwritable.
        for (const suffix of ["", ".bak"]) {
            const path = `${SYNC_BASE_STORE_PATH}/head.json${suffix}`;
            if (!corrupted.files.has(path)) continue;
            const value = readStoreTestFrame(corrupted, path);
            if (value.snapshot.metadata.schema !== 2) continue;
            alter(value.snapshot.metadata);
            corrupted.files.set(path, sealStoreTestFrame(value));
        }
        await assert.rejects(baseFor(corrupted).load(), StoreRecoveryError);
    }
}

const tests = [oneAtomicPublicationAndDetachedSubmission, duplicateDoesNotReplayOverNewerMutations,
    newerSettersStayBehindNativePublication, earlierSaveCannotPublishLaterSettersAheadOfBarrier,
    acceptedCutSurvivesLaterSetterFailureAndQueuedPublications, sameVisibleRootSetterAndBoundedPublicationQueue,
    strictInputBoundsNeverWrite, publicationFailuresNeverExposeMixedState,
    markerSurvivesCompactionAndFencesOlderReaders];
let completed = false;
const premature = () => { if (!completed) { console.error("sync-base-publication tests exited with unresolved work"); process.exitCode = 1; } };
process.on("beforeExit", premature);
void (async () => {
    for (const test of tests) await test();
    completed = true; process.removeListener("beforeExit", premature);
    console.log(`sync-base-publication.test: ${tests.length} regression groups passed`);
})().catch(error => { completed = true; process.removeListener("beforeExit", premature); console.error(error); process.exitCode = 1; });
