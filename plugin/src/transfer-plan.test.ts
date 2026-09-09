import { strict as assert } from "node:assert";
import { PreparedTransferPlan, PREPARED_STORE_OPERATION_BYTES, PREPARED_TRANSFER_PATH, TransferPlanError,
    type PreparedManifestInput } from "./transfer-plan";
import { SyncMemoryArbiter } from "./sync-memory-arbiter";
import { STORE_LIMITS, segmentedMigrationPaths } from "./segmented-store";
import { MemorySegmentedIO, readStoreTestFrame, sealStoreTestFrame, storeTestIOError,
    type StoreTestBoundary } from "./segmented-store-test-io";
import { checksumJournalUtf8 } from "./journal-format";

let assertions = 0;
function check(value: unknown, message: string): asserts value { assertions++; assert.ok(value, message); }
const same = (actual: unknown, expected: unknown, message: string) => { assertions++; assert.deepEqual(actual, expected, message); };
const digest = (value: number) => value.toString(16).padStart(64, "0");
const SCOPE = digest(1);
const HEAD = `${PREPARED_TRANSFER_PATH}/head.json`;
const isWal = (path: string) => path.startsWith(`${PREPARED_TRANSFER_PATH}/wal-`);
const isMutation = (event: StoreTestBoundary) => ["write", "rename", "remove", "mkdir"].includes(event.method);
function deferred() {
    let resolve!: () => void;
    const promise = new Promise<void>(done => { resolve = done; });
    return { promise, resolve };
}
function input(options: { path?: string; scope?: string; chunks?: number; generation?: number } = {}): PreparedManifestInput {
    const count = options.chunks ?? 2;
    const size = count * 3;
    return { scopeHash: options.scope ?? SCOPE, path: options.path ?? "prepared.md",
        journalThroughId: options.generation ?? 1, baseRoot: digest(99),
        source: { size, mtime: 10, fingerprint: { kind: "desktop-v1", size, mtime: 10, ctime: 9, device: 1, inode: 2 } },
        manifest: { file_hash: digest(500 + count), total_size: size,
            chunks: Array.from({ length: count }, (_, index) => ({ hash: digest(index + 10), offset: index * 3, size: 3 })) } };
}
async function fixture(limits?: ConstructorParameters<typeof PreparedTransferPlan>[1]) {
    const io = new MemorySegmentedIO();
    const plan = new PreparedTransferPlan(io, limits);
    await plan.load();
    return { io, plan };
}
async function reopened(io: MemorySegmentedIO): Promise<PreparedTransferPlan> {
    const plan = new PreparedTransferPlan(io); await plan.load(); return plan;
}
async function rejects(work: Promise<unknown> | (() => unknown), code?: TransferPlanError["code"]): Promise<void> {
    let error: unknown;
    try { if (typeof work === "function") work(); else await work; } catch (caught) { error = caught; }
    check(error instanceof Error, "operation unexpectedly succeeded");
    if (code) check(error instanceof TransferPlanError && error.code === code,
        `expected ${code}, received ${(error as any)?.code}`);
}
function assertBoundedFrames(events: readonly StoreTestBoundary[]): void {
    for (const event of events) {
        if (event.method !== "write" || event.phase !== "after" || !/\/(wal|page)-/.test(event.path)) continue;
        const frame = JSON.parse(event.data!);
        const page = JSON.parse(frame.payload);
        check(frame.bytes <= STORE_LIMITS.payloadBytes && checksumJournalUtf8(event.data!).bytes <= STORE_LIMITS.frameBytes &&
            page.rows.length <= STORE_LIMITS.rowsPerPage, "prepared manifest exceeded a portable page/frame bound");
        check(page.rows.every((row: any) => !row.header?.manifest?.chunks && !row.header?.chunks),
            "prepared header embedded a whole chunk array");
    }
}

async function scopeCasOwnershipAndRestart(): Promise<void> {
    const { io, plan } = await fixture();
    const firstInput = input({ chunks: 300, generation: 5 });
    const original = structuredClone(firstInput);
    const before = io.events.length;
    const first = plan.retain(firstInput);
    firstInput.path = "caller-mutated.md";
    if (firstInput.source.fingerprint.kind !== "desktop-v1") throw new Error("desktop fixture changed source kind");
    firstInput.source.fingerprint.inode = 200;
    firstInput.manifest.chunks[0].hash = digest(777);
    const retained = await first;
    check(retained.retained && retained.mutationId === 1, "first prepared mutation was not issued after retention");
    same(plan.lookup(SCOPE, "prepared.md"), { ...original, mutationId: 1 }, "caller mutation changed accepted manifest ownership");
    check(io.events.slice(before).filter(event => event.method === "write" && event.phase === "after" && event.path === `${HEAD}.next`).length === 1,
        "one multi-page manifest published more than one logical head");
    check(io.events.slice(before).filter(event => event.method === "write" && event.phase === "after" && isWal(event.path)).length >= 2,
        "fixture did not span bounded header/chunk pages");
    assertBoundedFrames(io.events);
    const exported = plan.lookup(SCOPE, "prepared.md")!;
    exported.manifest.chunks[0].size = 200;
    if (exported.source.fingerprint.kind !== "desktop-v1") throw new Error("desktop fixture restored wrong source kind");
    exported.source.fingerprint.ctime = 99;
    const retainedSource = plan.lookup(SCOPE, "prepared.md")!;
    check(retainedSource.source.fingerprint.kind === "desktop-v1" && retainedSource.manifest.chunks[0].size === 3 &&
        retainedSource.source.fingerprint.ctime === 9, "lookup exposed mutable retained values");
    check(plan.lookup(digest(2), "prepared.md") === null, "another server/policy scope reused an old hint");
    const replacement = await plan.retain({ ...input({ generation: 6 }), expectedMutationId: 1 });
    check(replacement.retained && replacement.mutationId === 2, "CAS replacement did not advance prepared mutation ID");
    check(!await plan.discard(SCOPE, "prepared.md", 1), "stale completion discarded a newer prepared record");
    same(await plan.retain({ ...input({ generation: 7 }), expectedMutationId: 1 }), { retained: false, reason: "stale" }, "stale retain CAS replaced a newer record");
    same(await plan.retain(input({ generation: 5 })), { retained: false, reason: "stale" }, "older journal generation replaced a newer hint");
    const noGeneration = input(); delete noGeneration.journalThroughId;
    same(await plan.retain(noGeneration), { retained: false, reason: "stale" }, "unknown generation replaced a known newer journal hint");
    const separate = await plan.retain({ ...input({ scope: digest(2) }), expectedMutationId: null });
    check(separate.retained && separate.mutationId === 3, "new scope invalidated the store or reused a mutation ID");
    await plan.compact();
    const restored = await reopened(io);
    check(restored.lookup(SCOPE, "prepared.md")?.mutationId === 2 && restored.lookup(digest(2), "prepared.md")?.mutationId === 3,
        "cold recovery mixed identities or lost replacements");
    check(await restored.discard(SCOPE, "prepared.md", 2), "matching discard did not remove the exact prepared generation");
    await restored.compact();
    const emptyScope = await reopened(io);
    check(emptyScope.lookup(SCOPE, "prepared.md") === null && emptyScope.lookup(digest(2), "prepared.md") !== null,
        "discard touched a separate identity scope or was not durable");
    const fourth = await emptyScope.retain(input({ generation: 9 }));
    check(fourth.retained && fourth.mutationId === 4, "discard/compaction reused an issued mutation ID");
    const lookupEvents = io.events.length;
    emptyScope.lookup(SCOPE, "prepared.md");
    check(io.events.length === lookupEvents, "hint lookup read source bytes or performed storage/network IO");
}

async function capsRejectBeforeCopyAndKeepOldHints(): Promise<void> {
    const { io, plan } = await fixture({ records: 2, chunks: 3, retainedBytes: 4096, queuedRequests: 2 });
    check((await plan.retain(input())).retained, "bounded first record was not retained");
    const oversized = input();
    let copied = false;
    oversized.manifest.chunks = new Proxy([], { get: (target, property, receiver) => {
        if (property === "length") return 4;
        if (property === Symbol.iterator) { copied = true; throw new Error("oversize input was iterated"); }
        return Reflect.get(target, property, receiver);
    } });
    const before = io.events.length;
    same(await plan.retain(oversized), { retained: false, reason: "limit" }, "oversize prepared-only item was not refused");
    check(!copied && io.events.length === before && plan.lookup(SCOPE, "prepared.md")?.mutationId === 1,
        "oversize admission copied data, wrote storage, or evicted current prepared hint");
    same(await plan.retain(input({ path: "second.md" })), { retained: false, reason: "limit" }, "global chunk cap was exceeded");
    const smaller = await plan.retain(input({ chunks: 1, generation: 2 }));
    check(smaller.retained && smaller.mutationId === 2, "replacing within a cap consumed IDs for rejected items");
    check((await plan.retain(input({ path: "second.md" }))).retained, "replacement did not release owned chunk capacity");
    same(await plan.retain(input({ path: "third.md", chunks: 0 })), { retained: false, reason: "limit" }, "record-count cap was exceeded");
    const snapshot = plan.snapshot();
    check(snapshot.records === 2 && snapshot.chunks === 3 && snapshot.estimatedRetainedBytes <= snapshot.limits.retainedBytes,
        "prepared aggregate accounting exceeded a configured cap");
    const invalid = input(); invalid.source.fingerprint.kind = "future" as any;
    await rejects(plan.retain(invalid), "UNKNOWN_SCHEMA");
    check(plan.snapshot().ready, "invalid caller input poisoned otherwise valid persisted state");

    const queue = await fixture({ queuedRequests: 2 });
    const entered = deferred(), release = deferred();
    let held = false;
    queue.io.onBoundary = async event => {
        if (!held && event.method === "write" && event.phase === "after" && isWal(event.path)) {
            held = true; entered.resolve(); await release.promise;
        }
    };
    const one = queue.plan.retain(input());
    await entered.promise;
    const two = queue.plan.retain(input({ path: "queued.md" }));
    same(await queue.plan.retain(input({ path: "overflow.md" })), { retained: false, reason: "limit" }, "queued caller retention was unbounded");
    check(queue.plan.snapshot().queuedRequests === 2, "queued ownership was released while native write was pending");
    release.resolve();
    await Promise.all([one, two]);
    check(queue.plan.snapshot().queuedRequests === 0 && queue.plan.snapshot().estimatedQueuedBytes === 0,
        "settled prepared queue leaked accounting");
}

async function publicationVisibilityAndClose(): Promise<void> {
    const { io, plan } = await fixture();
    await plan.retain(input());
    const old = plan.lookup(SCOPE, "prepared.md");
    const entered = deferred(), release = deferred();
    let moved = false, held = false;
    io.onBoundary = async event => {
        if (event.method === "rename" && event.phase === "after" && event.to === HEAD) moved = true;
        if (!held && moved && event.method === "read" && event.phase === "before" && event.path === HEAD) {
            held = true; entered.resolve(); await release.promise;
        }
    };
    let accepted = false;
    const current = plan.retain(input({ chunks: 300, generation: 2 })).then(result => { accepted = true; return result; });
    await entered.promise;
    same(plan.lookup(SCOPE, "prepared.md"), old, "unverified new head exposed a partial/replacement manifest");
    check(!accepted, "mutation ID resolved before final head verification");
    const queued = plan.retain(input({ path: "must-not-run.md" }));
    const queuedResult = Promise.allSettled([queued]);
    plan.close();
    await rejects(() => plan.lookup(SCOPE, "prepared.md"), "CLOSED");
    await rejects(plan.retain(input()), "CLOSED");
    check(plan.snapshot().estimatedQueuedBytes > 0, "close released native in-flight ownership early");
    release.resolve();
    const completed = await current;
    check(completed.retained && completed.mutationId === 2, "already-dispatched complete native publication lost its accepted cut on close");
    check((await queuedResult)[0].status === "rejected", "queued mutation started after close");
    await plan.closeAndDrain();
    io.onBoundary = undefined;
    const restored = await reopened(io);
    check(restored.lookup(SCOPE, "prepared.md")?.manifest.chunks.length === 300 && restored.lookup(SCOPE, "must-not-run.md") === null,
        "close lost committed manifest or revived queued work");
}

async function faultedPublicationRetainsOnlyWholeCuts(): Promise<void> {
    const seed = await fixture(); await seed.plan.retain(input());
    const disk = seed.io.snapshot();
    const referenceIO = new MemorySegmentedIO(disk);
    const reference = await reopened(referenceIO);
    const start = referenceIO.events.length;
    await reference.retain(input({ chunks: 300, generation: 2 }));
    const boundaries = referenceIO.events.slice(start).length;
    let rejected = 0, acceptedMaintenance = 0;
    for (let target = 0; target < boundaries; target++) {
        const io = new MemorySegmentedIO(disk);
        const plan = await reopened(io);
        let index = 0, hit = false;
        io.onBoundary = () => { if (index++ === target) { hit = true; throw storeTestIOError(); } };
        let failed = false;
        try { await plan.retain(input({ chunks: 300, generation: 2 })); } catch { failed = true; }
        check(hit, "prepared publication fault missed its intended boundary");
        io.onBoundary = undefined;
        if (failed) {
            rejected++;
            check(!plan.snapshot().ready, "failed prepared publication did not poison the writer");
            const unchanged = io.snapshot();
            await rejects(() => plan.lookup(SCOPE, "prepared.md"), "RECOVERY_REQUIRED");
            await rejects(plan.retain(input({ path: "poisoned.md" })), "RECOVERY_REQUIRED");
            await rejects(plan.discard(SCOPE, "prepared.md", 1), "RECOVERY_REQUIRED");
            same(io.snapshot(), unchanged, "poisoned prepared writer mutated recovery evidence");
        } else acceptedMaintenance++;
        const restored = await reopened(io);
        const actual = restored.lookup(SCOPE, "prepared.md")!;
        check(actual.mutationId === 1 || actual.mutationId === 2, "recovery invented a prepared mutation ID");
        check(actual.manifest.chunks.length === (actual.mutationId === 1 ? 2 : 300) &&
            actual.manifest.chunks.every((chunk, i) => chunk.hash === digest(i + 10) && chunk.offset === i * 3 && chunk.size === 3),
            "fault recovery exposed a partial or mixed manifest cut");
        if (!failed) check(actual.mutationId === 2, "successful retain did not recover its accepted manifest");
    }
    check(rejected > 0, "publication failure matrix never exercised an authoritative rejection");
    console.log(`transfer-plan fault matrix: ${rejected} rejected boundaries, ${acceptedMaintenance} accepted maintenance faults`);
}

async function malformedClosureCannotPromoteAStage(): Promise<void> {
    const { io, plan } = await fixture(); await plan.retain(input({ chunks: 300 }));
    const head = readStoreTestFrame(io, HEAD);
    const lastWal = `${PREPARED_TRANSFER_PATH}/wal-${head.epoch}-${head.walEnd}.json`;
    const page = readStoreTestFrame(io, lastWal);
    check(page.rows.at(-1).op === "seal", "incomplete-closure fixture lacks a manifest seal");
    page.rows.pop(); page.through--; head.sequence--;
    io.files.set(lastWal, sealStoreTestFrame(page));
    io.files.set(`${HEAD}.next`, sealStoreTestFrame(head));
    io.files.set(HEAD, io.files.get(`${HEAD}.bak`)!);
    const before = io.snapshot(); const events = io.events.length;
    const restored = new PreparedTransferPlan(io);
    await rejects(restored.load(), "CORRUPT");
    same(io.snapshot(), before, "incomplete logical manifest was promoted before final closure validation");
    check(!io.events.slice(events).some(isMutation), "rejected semantic closure modified stage/head evidence");
    check(!restored.snapshot().ready, "incomplete recovered record became visible");
}

async function unknownCorruptAndAmbiguousStoresStayClosed(): Promise<void> {
    const seed = await fixture(); await seed.plan.retain(input());
    const cases: Array<[string, TransferPlanError["code"], (io: MemorySegmentedIO) => void]> = [
        ["future metadata", "UNKNOWN_SCHEMA", io => { const h = readStoreTestFrame(io, HEAD); h.snapshot.metadata.hashFormat = "future"; io.files.set(HEAD, sealStoreTestFrame(h)); }],
        ["unknown chunk owner", "CORRUPT", io => { const h = readStoreTestFrame(io, HEAD); const p = `${PREPARED_TRANSFER_PATH}/wal-${h.epoch}-${h.walStart}.json`; const page = readStoreTestFrame(io, p); page.rows[1].mutationId++; io.files.set(p, sealStoreTestFrame(page)); }],
        ["future fingerprint", "UNKNOWN_SCHEMA", io => { const h = readStoreTestFrame(io, HEAD); const p = `${PREPARED_TRANSFER_PATH}/wal-${h.epoch}-${h.walStart}.json`; const page = readStoreTestFrame(io, p); page.rows[0].header.source.fingerprint.kind = "future"; io.files.set(p, sealStoreTestFrame(page)); }],
        ["same generation fork", "CONTRADICTING_COPIES", io => { const h = readStoreTestFrame(io, HEAD); h.snapshot.metadata.records++; io.files.set(`${HEAD}.next`, sealStoreTestFrame(h)); }],
    ];
    for (const [name, code, mutate] of cases) {
        const io = seed.io.clone(); mutate(io); const before = io.snapshot();
        const plan = new PreparedTransferPlan(io);
        await rejects(plan.load(), code);
        check(!plan.snapshot().ready && plan.snapshot().records === 0, `${name} exposed provisional prepared state`);
        same(io.snapshot(), before, `${name} caused a destructive reset`);
        await rejects(plan.retain(input()), "RECOVERY_REQUIRED");
    }
    const unavailable = seed.io.clone();
    unavailable.onBoundary = event => { if (event.method === "read" && event.phase === "before" && event.path === HEAD) throw storeTestIOError("EACCES"); };
    await rejects(new PreparedTransferPlan(unavailable).load(), "READ_FAILED");
}

async function monotonicReplayAndExhaustedWatermark(): Promise<void> {
    const seed = await fixture();
    await seed.plan.retain(input({ generation: 5 }));
    await seed.plan.retain(input({ generation: 6 }));
    for (const mode of ["older", "unknown"] as const) {
        const io = seed.io.clone();
        const head = readStoreTestFrame(io, HEAD);
        const path = `${PREPARED_TRANSFER_PATH}/wal-${head.epoch}-${head.walEnd}.json`;
        const page = readStoreTestFrame(io, path);
        const header = page.rows.find((row: any) => row.op === "prepared").header;
        if (mode === "older") header.journalThroughId = 4;
        else delete header.journalThroughId;
        io.files.set(path, sealStoreTestFrame(page));
        const before = io.snapshot(), events = io.events.length;
        const plan = new PreparedTransferPlan(io);
        await rejects(plan.load(), "CORRUPT");
        check(!plan.snapshot().ready && !io.events.slice(events).some(isMutation),
            `${mode} journal generation was accepted or changed recovery evidence`);
        same(io.snapshot(), before, `${mode} journal replacement reset the store`);
    }

    const exhausted = await fixture();
    // Advance the source-initialization fence, then synthesize a valid empty
    // snapshot at the last issuable ID without iterating quadrillions of rows.
    await exhausted.plan.compact();
    const head = readStoreTestFrame(exhausted.io, HEAD);
    head.snapshot.metadata.nextMutationId = Number.MAX_SAFE_INTEGER - 1;
    exhausted.io.files.set(HEAD, sealStoreTestFrame(head));
    const last = await reopened(exhausted.io);
    const accepted = await last.retain(input());
    check(accepted.retained && accepted.mutationId === Number.MAX_SAFE_INTEGER - 1,
        "final safe mutation generation was not issued consistently");
    const fromWal = await reopened(exhausted.io);
    check(fromWal.lookup(SCOPE, "prepared.md")?.mutationId === Number.MAX_SAFE_INTEGER - 1,
        "exhausted high-water mark could not recover from WAL");
    await fromWal.compact();
    const fromSnapshot = await reopened(exhausted.io);
    check(fromSnapshot.lookup(SCOPE, "prepared.md")?.mutationId === Number.MAX_SAFE_INTEGER - 1,
        "exhausted high-water mark could not recover from snapshot metadata");
    const before = exhausted.io.events.length;
    await rejects(fromSnapshot.retain(input({ path: "cannot-issue.md" })), "CORRUPT");
    check(exhausted.io.events.length === before && fromSnapshot.snapshot().ready,
        "exhaustion wrote an unsafe ID or poisoned a valid retained cut");
    check(await fromSnapshot.discard(SCOPE, "prepared.md", Number.MAX_SAFE_INTEGER - 1),
        "ID exhaustion prevented exact existing-generation cleanup");
    await fromSnapshot.compact();
    const cleaned = await reopened(exhausted.io);
    check(cleaned.snapshot().records === 0, "exhausted cleanup was not durable");
    await rejects(cleaned.retain(input()), "CORRUPT");
}

async function boundedAtomicCleanupKeepsNewerHints(): Promise<void> {
    const { io, plan } = await fixture();
    await plan.retain(input({ path: "a.md" }));
    await plan.retain(input({ path: "b.md" }));
    await plan.retain(input({ path: "hot.md" }));
    const entered = deferred(), release = deferred();
    let held = false;
    io.onBoundary = async event => {
        if (!held && event.method === "write" && event.phase === "after" && isWal(event.path)) {
            held = true; entered.resolve(); await release.promise;
        }
    };
    const events = io.events.length;
    const newer = plan.retain(input({ path: "hot.md", generation: 2 }));
    await entered.promise;
    const tokens = [
        { scopeHash: SCOPE, path: "a.md", expectedMutationId: 1 },
        { scopeHash: SCOPE, path: "b.md", expectedMutationId: 2 },
        { scopeHash: SCOPE, path: "hot.md", expectedMutationId: 3 },
        { scopeHash: SCOPE, path: "a.md", expectedMutationId: 1 },
    ];
    const cleanup = plan.discardMany(tokens);
    tokens[0].path = "hot.md"; tokens[0].expectedMutationId = 4;
    tokens[1].scopeHash = digest(88);
    tokens.length = 0;
    check(plan.snapshot().records === 3, "queued cleanup changed the public cut before verification");
    release.resolve();
    const prepared = await newer;
    check(prepared.retained && prepared.mutationId === 4, "newer source preparation did not finish");
    same(await cleanup, 2, "batch cleanup lost detached tokens or counted stale/duplicate generations");
    io.onBoundary = undefined;
    const headWrites = io.events.slice(events).filter(event => event.method === "write" &&
        event.phase === "after" && event.path === `${HEAD}.next`);
    same(headWrites.length, 2, "one replacement plus multi-record cleanup did not publish exactly two heads");
    check(plan.lookup(SCOPE, "a.md") === null && plan.lookup(SCOPE, "b.md") === null &&
        plan.lookup(SCOPE, "hot.md")?.mutationId === 4, "cleanup discarded newer prepared source or retained matching old hints");
    await plan.compact();
    const restored = await reopened(io);
    check(restored.snapshot().records === 1 && restored.lookup(SCOPE, "hot.md")?.mutationId === 4,
        "atomic cleanup did not survive compact/reload");
    const noops = io.events.length;
    same(await restored.discardMany([{ scopeHash: SCOPE, path: "hot.md", expectedMutationId: 3 }]), 0,
        "stale cleanup unexpectedly matched");
    same(await restored.discardMany([]), 0, "empty cleanup unexpectedly matched");
    check(io.events.length === noops, "empty/stale cleanup performed a storage publication");
    await rejects(restored.discardMany(Array.from({ length: 129 }, () => ({
        scopeHash: SCOPE, path: "hot.md", expectedMutationId: 4,
    }))), "LIMIT");
    check(restored.snapshot().ready && restored.snapshot().queuedRequests === 0,
        "oversize cleanup poisoned persisted state or leaked queue ownership");

    // Fail the actual discard WAL write. The accepted hint remains recoverable;
    // the rejected caller cannot continue mutating until an explicit load.
    let injected = false;
    io.onBoundary = event => {
        if (event.method === "write" && event.phase === "before" && isWal(event.path) &&
            JSON.parse(JSON.parse(event.data!).payload).rows.some((row: any) => row.op === "discard")) {
            injected = true; throw storeTestIOError();
        }
    };
    await rejects(restored.discardMany([{ scopeHash: SCOPE, path: "hot.md", expectedMutationId: 4 }]));
    check(injected && !restored.snapshot().ready, "failed cleanup did not poison the stage");
    await rejects(restored.discardMany([]), "RECOVERY_REQUIRED");
    io.onBoundary = undefined;
    await restored.load();
    check(restored.lookup(SCOPE, "hot.md")?.mutationId === 4,
        "failed cleanup lost the prior accepted manifest on explicit recovery");
}

async function emptyInitializationUsesProofNotOrphanGuessing(): Promise<void> {
    const marker = segmentedMigrationPaths(PREPARED_TRANSFER_PATH);
    const baseline = await fixture();
    const initialWrites = baseline.io.events.filter(event => event.method === "write" && event.phase === "after");
    check(initialWrites[0].path === marker.intent, "empty source proof did not precede target publication");
    const second = await fixture();
    same(readStoreTestFrame(baseline.io, marker.intent).proof, readStoreTestFrame(second.io, marker.intent).proof,
        "same empty source produced a random/unrepeatable migration proof");
    const io = new MemorySegmentedIO(); let interrupted = false;
    io.onBoundary = event => {
        if (!interrupted && event.method === "mkdir" && event.phase === "after" && event.path === PREPARED_TRANSFER_PATH) {
            interrupted = true; throw storeTestIOError();
        }
    };
    await rejects(new PreparedTransferPlan(io).load()); io.onBoundary = undefined;
    check(interrupted && io.files.has(marker.intent), "initialization failure did not retain its source proof");
    check((await reopened(io)).snapshot().ready, "exact interrupted empty initialization could not retry from proof");

    const orphan = new MemorySegmentedIO();
    orphan.directories.add(PREPARED_TRANSFER_PATH.slice(0, PREPARED_TRANSFER_PATH.lastIndexOf("/")));
    orphan.directories.add(PREPARED_TRANSFER_PATH);
    const before = orphan.snapshot();
    await rejects(new PreparedTransferPlan(orphan).load(), "RECOVERY_REQUIRED");
    same(orphan.snapshot(), before, "unproven orphan directory was reset to empty");
    const advanced = baseline.io.clone(); const loaded = await reopened(advanced); await loaded.retain(input());
    for (const suffix of ["", ".next", ".bak"]) advanced.files.delete(`${HEAD}${suffix}`);
    const advancedBefore = advanced.snapshot();
    await rejects(new PreparedTransferPlan(advanced).load(), "RECOVERY_REQUIRED");
    same(advanced.snapshot(), advancedBefore, "missing advanced heads licensed an empty prepared-state reset");
}

async function storeScratchAdmissionPrecedesAllocationAndWrites(): Promise<void> {
    const retainedBytes = 4096, slack = 2048;
    const memory = new SyncMemoryArbiter({ capacityBytes: retainedBytes + PREPARED_STORE_OPERATION_BYTES + slack });
    const io = new MemorySegmentedIO(), plan = new PreparedTransferPlan(io, { retainedBytes }, memory);
    await plan.load();
    same(plan.snapshot().managedResidentBytes, retainedBytes,
        "idle store claimed transient operation scratch as resident memory");
    const blocker = memory.tryReserve("transfer", slack + 1); check(blocker, "scratch denial fixture did not consume its slack");
    const before = io.events.length;
    await rejects(plan.compact(), "LIMIT");
    check(!io.events.slice(before).some(isMutation), "compact wrote store bytes before scratch admission");

    const candidate = input({ chunks: 1 }); let iterated = false;
    candidate.manifest.chunks = new Proxy(candidate.manifest.chunks, { get(target, property, receiver) {
        if (property === Symbol.iterator) iterated = true;
        return Reflect.get(target, property, receiver);
    } });
    same(await plan.retain(candidate), { retained: false, reason: "limit" },
        "retain scratch refusal did not remain a cache-only limit");
    check(!iterated && !io.events.slice(before).some(isMutation),
        "retain copied rows or wrote store bytes before operation admission");
    blocker.release(); await plan.compact(); await plan.closeAndDrain();
    same(memory.snapshot().usedBytes, 0, "store close leaked resident or operation admission");
}

const watchdog = setTimeout(() => { throw new Error("prepared transfer stage tests did not settle"); }, 30_000);
void scopeCasOwnershipAndRestart()
    .then(capsRejectBeforeCopyAndKeepOldHints)
    .then(publicationVisibilityAndClose)
    .then(faultedPublicationRetainsOnlyWholeCuts)
    .then(malformedClosureCannotPromoteAStage)
    .then(unknownCorruptAndAmbiguousStoresStayClosed)
    .then(monotonicReplayAndExhaustedWatermark)
    .then(boundedAtomicCleanupKeepsNewerHints)
    .then(emptyInitializationUsesProofNotOrphanGuessing)
    .then(storeScratchAdmissionPrecedesAllocationAndWrites)
    .then(() => console.log(`transfer-plan.test: ${assertions} assertions passed`))
    .catch(error => { console.error(error); process.exitCode = 1; })
    .finally(() => clearTimeout(watchdog));
