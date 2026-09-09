import { strict as assert } from "node:assert";
import { blake3 } from "@noble/hashes/blake3";
import type { ObsetyncApi, OwnedObjects } from "./api";
import { BulkObjectKind } from "./bulk-codec";
import type { PlatformIO } from "./platform";
import type { WasmModule } from "./push";
import { ObsetyncJournal, JOURNAL_STORE_PATH } from "./journal";
import { ObsetyncSyncBase, SYNC_BASE_STORE_PATH } from "./sync-base";
import { RootIntentStore, ROOT_INTENT_PATH, ROOT_INTENT_LIMITS } from "./root-intent";
import { RootSyncRuntime, type RootCandidatePublication, type RootSyncIdentity } from "./root-sync-runtime";
import { ROOT_MAX_BYTES, computeRootRequestHash, type RootCancelRequest, type RootCommitIntent,
    type RootOutcome, type RootOutcomeCapabilities, type RootOutcomeIdentity, type RootTerminalOutcome } from "./root-outcome";
import { MemorySegmentedIO, type StoreTestBoundary } from "./segmented-store-test-io";
import { ResourceBudget } from "./resource-budget";
import { estimateTransportWorkset, reserveTransientScope } from "./transient-memory";
import { estimateTreeRootExportWorkset, type OwnedTreeRootExport } from "./tree-root-export-job";
import { estimateRootIntentPreparationWorkset } from "./root-publication-memory";

// Actual runtime/coordinators/stores. Only transport, platform files and a
// deliberately synthetic tree decoder are ports; no real server/Merkle/native
// claim. The streaming Hasher test double uses actual portable BLAKE3.
const h = (value: number) => value.toString(16).padStart(64, "0");
const digest = (data: Uint8Array) => Buffer.from(blake3(data)).toString("hex");
const clone = <T>(value: T): T => structuredClone(value);
const SCOPE = h(1), INCARNATION = h(2), CANDIDATE = h(3), OBSERVED = h(9);
const CAPABILITIES: RootOutcomeCapabilities = { serverIncarnation: INCARNATION,
    maxSequence: Number.MAX_SAFE_INTEGER, commitBytes: 704 * 1024, rootBytes: ROOT_MAX_BYTES,
    queryBytes: 4096, cancelBytes: 4096, receiptBytes: 64 * 1024, streamsPerVault: 128 };
const rootBytes = (marker = "captured") => new TextEncoder().encode(JSON.stringify({ version: 1, root: CANDIDATE, marker }));
type FixtureRootExport = OwnedTreeRootExport & { readonly test: {
    budget: ResourceBudget; releaseCalls: number; releases: number; totalBytes: number;
} };
async function ownedRoot(bytes: Uint8Array): Promise<FixtureRootExport> {
    // Real admission and child leases, synthetic already-produced root bytes.
    // This is an ownership fixture, not a WASM exporter/native allocation test.
    const preparation = estimateRootIntentPreparationWorkset(bytes.length);
    const workset = estimateTreeRootExportWorkset(bytes.length, 0, preparation.totalBytes);
    const totalBytes = workset.ownerBytes + workset.workBytes;
    const budget = new ResourceBudget({ capacityBytes: totalBytes });
    const memory = await reserveTransientScope(workset, { budget });
    let retained: Uint8Array | undefined = bytes.slice();
    const test = { budget, releaseCalls: 0, releases: 0, totalBytes };
    return { memory, test,
        get bytes() { if (!retained) throw new Error("test root export was released"); return retained; },
        release() {
            test.releaseCalls++;
            if (!retained) return;
            retained = undefined; test.releases++; memory.close();
        } };
}
function assertExportReleased(owner: FixtureRootExport): void {
    assert.equal(owner.test.releases, 1);
    assert.equal(owner.memory.snapshot().parentReleased, true);
    assert.equal(owner.test.budget.snapshot().usedBytes, 0);
    assert.throws(() => owner.bytes, /released/);
    owner.release(); owner.release();
    assert.equal(owner.test.releases, 1, "repeated cleanup released the parent twice");
}
function gate<T = void>() {
    let resolve!: (value: T) => void, reject!: (error: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
async function turns() { for (let index = 0; index < 20; index++) await Promise.resolve(); }
const rejection = (promise: Promise<unknown>) => promise.then(
    () => { throw new Error("expected runtime rejection"); }, (error: unknown) => error,
);
const errorOf = (work: () => unknown) => { try { work(); } catch (error) { return error; } throw new Error("expected synchronous runtime rejection"); };
function receipt(request: RootOutcomeIdentity, status: "accepted" | "cancelled" = "accepted",
    conflicts: Array<{ path: string; base_hash: string; side_a_hash: string; side_b_hash: string }> = []): RootTerminalOutcome {
    const envelope = { protocol_version: 1 as const, server_incarnation: INCARNATION,
        sequence: request.sequence, mutation_id: request.mutation_id, request_hash: request.request_hash };
    return status === "cancelled" ? { ...envelope, status, result: { cancelled: true } } :
        { ...envelope, status, result: { merged: true, root_hash: OBSERVED, conflicts, auto_resolved: 0, text_merged: 0 } };
}

class Network {
    readonly calls: { method: string; vault?: string; value?: unknown }[] = [];
    caps: RootOutcomeCapabilities | null = { ...CAPABILITIES };
    floor = 0;
    currentRoot: string | null = null;
    terminal: RootTerminalOutcome | null = null;
    onNegotiate?: () => Promise<RootOutcomeCapabilities | null>;
    onQuery?: (identity?: RootOutcomeIdentity) => Promise<RootOutcome>;
    onCommit?: (request: RootCommitIntent) => Promise<RootTerminalOutcome>;
    onCancel?: (request: RootCancelRequest) => Promise<RootTerminalOutcome>;
    onDownload?: (kind: BulkObjectKind, hashes: string[]) => Promise<OwnedObjects>;
    async negotiateRootOutcomes(force?: boolean): Promise<RootOutcomeCapabilities | null> {
        assert.equal(force, true); this.calls.push({ method: "negotiate" });
        return this.onNegotiate ? this.onNegotiate() : this.caps === null ? null : clone(this.caps);
    }
    async queryRootOutcome(vault: string, identity?: RootOutcomeIdentity): Promise<RootOutcome> {
        this.calls.push({ method: "query", vault, value: clone(identity) });
        if (this.onQuery) return this.onQuery(identity);
        if (identity && this.terminal) return clone(this.terminal);
        return { protocol_version: 1, server_incarnation: INCARNATION, status: identity ? "unknown" : "stream",
            last_sequence: this.floor, current_root_hash: this.currentRoot };
    }
    async commitRootOutcome(vault: string, request: RootCommitIntent): Promise<RootTerminalOutcome> {
        this.calls.push({ method: "commit", vault, value: clone(request) });
        if (this.onCommit) return this.onCommit(request);
        return this.accept(request);
    }
    async cancelRootOutcome(vault: string, request: RootCancelRequest): Promise<RootTerminalOutcome> {
        this.calls.push({ method: "cancel", vault, value: clone(request) });
        if (this.onCancel) return this.onCancel(request);
        this.floor = request.sequence; this.terminal = receipt(request, "cancelled");
        return clone(this.terminal);
    }
    async getObjectsOwned(kind: BulkObjectKind, hashes: string[]): Promise<OwnedObjects> {
        this.calls.push({ method: "download", value: { kind, hashes: [...hashes] } });
        if (!this.onDownload) throw new Error("unexpected conflict download");
        return this.onDownload(kind, hashes);
    }
    accept(request: RootOutcomeIdentity): RootTerminalOutcome {
        this.floor = request.sequence; this.currentRoot = OBSERVED;
        this.terminal = receipt(request); return clone(this.terminal);
    }
    count(method: string): number { return this.calls.filter(call => call.method === method).length; }
}

function filePort() {
    const files = new Map<string, Uint8Array>(), directories = new Set<string>(), calls: string[] = [];
    const io: PlatformIO = {
        async stat(path) { calls.push(`stat:${path}`); const data = files.get(path); return data ? { size: data.length, mtime: 1 } : null; },
        getAbsolutePath() { return null; },
        async readFile(path) { calls.push(`read:${path}`); if (path === "note.md") throw new Error("must not read current local conflict source");
            const data = files.get(path); if (!data) throw new Error("missing synthetic file"); return data.slice(); },
        async writeFile(path, data) { calls.push(`write:${path}`); files.set(path, data.slice()); },
        async appendFile() { throw new Error("unexpected append for small fixture"); },
        async replaceFile() { throw new Error("conflict publication must not replace files"); },
        async renameFile() { throw new Error("conflict publication must not rename files"); },
        async copyFileExclusive(from, to) { calls.push(`copy:${to}`); if (files.has(to)) throw Object.assign(new Error("exists"), { code: "EEXIST" });
            files.set(to, files.get(from)!.slice()); },
        supportsNativeAppend() { return false; },
        async listDirectory(path) { calls.push(`list:${path}`); return { files: [...files.keys()].filter(key => key.startsWith(path + "/")), folders: [] }; },
        async deleteFile(path) { calls.push(`delete:${path}`); files.delete(path); },
        async mkdir(path) { calls.push(`mkdir:${path}`); directories.add(path); },
        async exists(path) { calls.push(`exists:${path}`); return files.has(path) || directories.has(path); },
        statBulk() { return new Map(); }, listFiles() { return []; }, async listObsidianConfig() { return new Map(); },
    };
    return { io, files, calls };
}

async function fixture(options: { storage?: MemorySegmentedIO; network?: Network; load?: boolean;
    beforeHeavyBatch?: () => Promise<void> } = {}) {
    const storage = options.storage ?? new MemorySegmentedIO(), network = options.network ?? new Network();
    const app = { vault: { adapter: storage } } as any;
    const base = new ObsetyncSyncBase(app), journal = new ObsetyncJournal(app);
    await base.load(); await journal.load();
    const platform = filePort(), hasherStates: { updates: number[]; freed: number }[] = [];
    const exports: FixtureRootExport[] = [];
    const hooks: { hash?: (index: number, phase: "construct" | "update" | "finalize" | "free") => void;
        parse?: () => void } = {};
    class Hasher {
        private readonly inner = blake3.create({});
        readonly state = { updates: [] as number[], freed: 0 };
        readonly index = hasherStates.length;
        constructor() { hasherStates.push(this.state); hooks.hash?.(this.index, "construct"); }
        update(bytes: Uint8Array) { assert.equal(this.state.freed, 0); this.state.updates.push(bytes.length);
            hooks.hash?.(this.index, "update"); this.inner.update(bytes); }
        finalize() { assert.equal(this.state.freed, 0); hooks.hash?.(this.index, "finalize"); return Buffer.from(this.inner.digest()).toString("hex"); }
        free() { assert.equal(++this.state.freed, 1); this.inner.destroy(); hooks.hash?.(this.index, "free"); }
    }
    const decode = (bytes: Uint8Array) => { hooks.parse?.(); return JSON.parse(new TextDecoder().decode(bytes)); };
    const wasm = { Hasher, wasm_root_hash_from_bytes: (bytes: Uint8Array) => decode(bytes).root,
        wasm_root_version_from_bytes: (bytes: Uint8Array) => decode(bytes).version } as unknown as WasmModule;
    let scopeChecks = 0, allowed = true, onScope: (() => void) | undefined;
    const identity: RootSyncIdentity = { vaultId: "vault", deviceId: "device", scopeHash: Promise.resolve(SCOPE),
        assertApiScope() { scopeChecks++; onScope?.(); if (!allowed) throw new Error("captured credential owner changed"); } };
    const runtime = new RootSyncRuntime(identity, storage, network as unknown as ObsetyncApi, platform.io, wasm, base, journal, options.beforeHeavyBatch);
    if (options.load !== false) await runtime.load();
    return { storage, network, base, journal, runtime, platform, hasherStates, hooks,
        setAllowed(value: boolean) { allowed = value; }, setScopeHook(value: typeof onScope) { onScope = value; },
        scopeChecks: () => scopeChecks,
        async candidate(bytes = rootBytes()): Promise<RootCandidatePublication & { rootExport: FixtureRootExport }> {
            const data = new Uint8Array([11, 22, 33]);
            const throughId = await journal.append({ action: "modified", path: "note.md", ts: 1, synced: false });
            const rootExport = await ownedRoot(bytes); exports.push(rootExport);
            return { rootExport,
                candidateRoot: CANDIDATE, treeVersion: 1, parentRoot: "", journalEpoch: journal.validatedEpoch!,
                journalCuts: [{ path: "note.md", throughId }], entries: [{ action: "upsert", path: "note.md", hash: digest(data), size: data.length, mtime: 1 }] };
        },
        async inspectDisk() {
            const store = new RootIntentStore(storage.clone(), SCOPE, digest); await store.load();
            const result = { pending: store.pending(), lastSequence: store.lastSequence }; await store.closeAndDrain(); return result;
        },
        assertHashersFreed() {
            for (const state of hasherStates) { assert.equal(state.freed, 1); assert(state.updates.every(size => size <= 65536)); }
            for (const owner of exports) assertExportReleased(owner);
        },
    };
}
function walHas(event: StoreTestBoundary, operation: string): boolean {
    return event.method === "write" && event.phase === "before" && event.path.startsWith(`${ROOT_INTENT_PATH}/wal-`) &&
        JSON.parse(JSON.parse(event.data!).payload).rows.some((row: any) => row.op === operation);
}

async function loadIsStrictlyLocal() {
    const f = await fixture({ load: false });
    f.base.setEntry("unflushed.md", h(70), 1, 2);
    const through = await f.journal.append({ action: "modified", path: "pending.md", ts: 1, synced: false });
    f.storage.onBoundary = event => {
        if (event.path.startsWith(SYNC_BASE_STORE_PATH) || event.path.startsWith(JOURNAL_STORE_PATH)) {
            throw new Error("root runtime touched shared base/journal during local load");
        }
    };
    const first = f.runtime.load(); assert.equal(first, f.runtime.load()); await first;
    assert.deepEqual(f.network.calls, []); assert.equal(f.scopeChecks(), 0);
    assert.equal(f.base.getHash("unflushed.md"), h(70)); assert.equal(f.journal.unsynced()[0].id, through);
    assert.deepEqual(f.runtime.snapshot(), { loaded: true, pending: false, closed: false });
    f.storage.onBoundary = undefined;
    await f.runtime.dispose(); assert.equal(f.runtime.snapshot().loaded, false);
    // The runtime does not own the engine's shared persistence handles.
    await f.journal.append({ action: "modified", path: "after.md", ts: 2, synced: false });
    f.base.setEntry("after.md", h(71), 2, 3); await f.base.save();
}

async function selectionRequiresCompleteFreshPreflight() {
    const f = await fixture(); assert.equal(await f.runtime.selectMode(), "durable");
    assert.deepEqual(f.network.calls.map(call => call.method), ["negotiate", "query"]);
    assert.equal(f.network.calls[1].value, undefined); assert.equal(f.runtime.lastSequence, 0);
    f.network.floor = 4;
    await assert.rejects(f.runtime.selectMode(), /high-water/);
    const candidate = await f.candidate();
    await assert.rejects(f.runtime.publish(candidate), /negotiated stream/);
    assert.equal(f.runtime.lastSequence, 0); assert.equal(f.runtime.pending(), null); assert.equal(f.network.count("commit"), 0);
    f.network.floor = 0; f.network.onQuery = async () => ({ protocol_version: 1, status: "stream",
        server_incarnation: h(90), last_sequence: 0, current_root_hash: null });
    await assert.rejects(f.runtime.selectMode(), /restarted/);
    await assert.rejects(f.runtime.publish(candidate), /negotiated stream/);
    f.network.onQuery = undefined;
    f.network.caps = null; assert.equal(await f.runtime.selectMode(), "legacy");
    await assert.rejects(f.runtime.publish(candidate), /negotiated stream/);
    for (const invalid of [undefined, false]) {
        f.network.onNegotiate = async () => invalid as unknown as RootOutcomeCapabilities;
        await assert.rejects(f.runtime.selectMode(), /invalid root capability/);
        await assert.rejects(f.runtime.publish(candidate), /negotiated stream/);
    }
    const unavailable = new Error("capability network failed");
    f.network.onNegotiate = async () => { throw unavailable; };
    assert.equal(await rejection(f.runtime.selectMode()), unavailable);
    await assert.rejects(f.runtime.publish(candidate), /negotiated stream/);
    await f.runtime.dispose();
}

async function localInitializationCanRetryActualReadFailure() {
    const f = await fixture({ load: false });
    const failure = Object.assign(new Error("transient root storage read failed"), { code: "EIO" });
    let injected = false;
    f.storage.onBoundary = event => {
        if (!injected && event.phase === "before" && event.path.startsWith(ROOT_INTENT_PATH)) {
            injected = true;
            throw failure;
        }
    };
    await assert.rejects(f.runtime.load(), (error: any) => error.code === "READ_FAILED");
    assert(injected); assert.equal(f.runtime.snapshot().loaded, false);
    f.storage.onBoundary = undefined;
    const retry = f.runtime.load(); assert.equal(retry, f.runtime.load());
    await retry;
    assert.deepEqual(f.runtime.snapshot(), { loaded: true, pending: false, closed: false });
    assert.equal(f.runtime.lastSequence, 0); assert.deepEqual(f.network.calls, []);
    await f.runtime.dispose(); f.assertHashersFreed();
}

async function exactCandidateCapturedAndDurableBeforeSend() {
    const f = await fixture(), candidate = await f.candidate();
    const { rootExport, ...metadata } = candidate;
    const original = { ...clone(metadata), rootBytes: rootExport.bytes.slice() };
    await f.runtime.selectMode();
    let sent: RootCommitIntent | undefined;
    f.network.onCommit = async request => {
        sent = clone(request);
        const disk = await f.inspectDisk();
        assert.deepEqual(disk.pending?.intent.request, request, "network dispatched before exact intent publication");
        assert.deepEqual(disk.pending?.intent.publication.entries, original.entries);
        assert.deepEqual(disk.pending?.intent.journalCuts, original.journalCuts);
        assert.equal(disk.pending?.intent.journalEpoch, original.journalEpoch);
        assert.equal(disk.pending?.intent.publication.candidateRoot, original.candidateRoot);
        const { request_hash, ...wire } = request;
        assert.equal(request_hash, await computeRootRequestHash("vault", "device", wire, digest));
        return f.network.accept(request);
    };
    const publication = f.runtime.publish(candidate);
    // Raw bytes have transferred to publish and must not be mutated by their
    // former owner. Changing the input property cannot redirect its capture.
    (candidate as any).rootExport = { get bytes() { throw new Error("late root owner read"); },
        release() { throw new Error("late root owner released"); } };
    (candidate as any).candidateRoot = h(30); (candidate as any).parentRoot = h(31);
    (candidate as any).journalEpoch = "f".repeat(32);
    (candidate.entries[0] as any).hash = h(32); (candidate.entries[0] as any).path = "later.md";
    (candidate.journalCuts[0] as any).throughId = 999;
    assert.deepEqual(await publication, { status: "accepted", candidateRoot: CANDIDATE, observedRoot: OBSERVED });
    assert.deepEqual(new Uint8Array(Buffer.from(sent!.root, "base64")), original.rootBytes);
    assert.equal(sent!.parent_root, original.parentRoot);
    assert.equal(f.base.treeBaseRoot, CANDIDATE); assert.equal(f.base.getHash("note.md"), (original.entries[0] as any).hash);
    assert.equal(f.journal.unsyncedCount(), 0); assert.equal(f.runtime.pending(), null); assert.equal(f.runtime.lastSequence, 1);
    assert.deepEqual(await f.inspectDisk(), { pending: null, lastSequence: 1 });
    assertExportReleased(rootExport);
    await assert.rejects(f.runtime.publish({ ...metadata, rootExport }), /negotiated stream/, "one selection authorized repeated publication");
    await f.runtime.dispose(); f.assertHashersFreed();
}

async function candidateBoundsCannotPrepareOrDispatch() {
    for (const invalid of ["hash", "version", "raw-max", "offer-max", "entries", "cuts", "sequence"] as const) {
        const f = await fixture(), candidate = await f.candidate(invalid === "raw-max" ? new Uint8Array(ROOT_MAX_BYTES + 1) : rootBytes());
        if (invalid === "offer-max") f.network.caps = { ...CAPABILITIES, rootBytes: 1 };
        if (invalid === "sequence") f.network.caps = { ...CAPABILITIES, maxSequence: 0 };
        await f.runtime.selectMode();
        if (invalid === "hash") (candidate as any).candidateRoot = h(55);
        if (invalid === "version") (candidate as any).treeVersion = 2;
        if (invalid === "entries") (candidate as any).entries = Array(ROOT_INTENT_LIMITS.entries + 1).fill(candidate.entries[0]);
        if (invalid === "cuts") (candidate as any).journalCuts = Array(ROOT_INTENT_LIMITS.journalCuts + 1).fill(candidate.journalCuts[0]);
        await assert.rejects(f.runtime.publish(candidate), /publication limits|sequence is exhausted/);
        assertExportReleased(candidate.rootExport);
        assert.equal(f.network.count("commit"), 0); assert.equal(f.runtime.pending(), null); assert.equal(f.runtime.lastSequence, 0);
        assert.equal(f.base.lastAppliedPublication, null); assert.equal(f.journal.unsyncedCount(), 1);
        await f.runtime.dispose(); f.assertHashersFreed();
    }
}

async function rootExportIsOwnedThroughHashAndReleasedBeforeStoreAndNetwork() {
    const f = await fixture(), candidate = await f.candidate(rootBytes("x".repeat(180 * 1024)));
    const owner = candidate.rootExport;
    await f.runtime.selectMode();
    const firstHasher = f.hasherStates.length, firstUpdate = gate(), walEntered = gate(), releaseWal = gate();
    let firstFinalized = false, firstFreed = false, verifiedByStore = false, walHeld = false, settled = false;
    f.hooks.hash = (index, phase) => {
        if (index === firstHasher) {
            assert.equal(owner.test.releases, 0, "root owner released while its first digest was live");
            assert.equal(owner.memory.snapshot().parentReleased, false);
            assert.equal(owner.test.budget.snapshot().usedBytes, owner.test.totalBytes);
            assert(owner.memory.snapshot().work.usedBytes > 0, "first digest did not borrow admitted child quota");
            if (phase === "update") firstUpdate.resolve();
            if (phase === "finalize") firstFinalized = true;
            if (phase === "free") firstFreed = true;
        } else {
            assert(firstFinalized && firstFreed);
            assertExportReleased(owner);
            verifiedByStore = true;
        }
    };
    f.storage.onBoundary = async event => {
        if (!walHeld && walHas(event, "intent")) {
            walHeld = true;
            assert(verifiedByStore, "intent WAL preceded the real store's digest verification");
            assertExportReleased(owner); walEntered.resolve(); await releaseWal.promise;
        }
    };
    f.network.onNegotiate = async () => { assertExportReleased(owner); return { ...CAPABILITIES }; };
    f.network.onCommit = async request => {
        assert(verifiedByStore && walHeld); assertExportReleased(owner); return f.network.accept(request);
    };
    const work = f.runtime.publish(candidate).finally(() => { settled = true; });
    try {
        await firstUpdate.promise;
        // The real host yield in streaming hash has not settled yet: no
        // fake timeout/race substitutes for joining this Hasher's completion.
        assert.equal(f.hasherStates[firstHasher].freed, 0);
        assert.equal(owner.test.releases, 0); assert.equal(settled, false);
        await walEntered.promise;
        assertExportReleased(owner); await turns(); assert.equal(settled, false);
        assert.equal(f.network.count("commit"), 0);
        releaseWal.resolve();
        assert.deepEqual(await work, { status: "accepted", candidateRoot: CANDIDATE, observedRoot: OBSERVED });
        assert(firstFinalized && firstFreed && verifiedByStore);
        assert.equal(f.journal.unsyncedCount(), 0); assertExportReleased(owner);
    } finally {
        releaseWal.resolve(); await work.catch(() => undefined);
        f.hooks.hash = undefined; f.storage.onBoundary = undefined;
        await f.runtime.dispose(); f.assertHashersFreed();
    }
}

async function rootExportFailurePathsReleaseExactlyOneOwner() {
    for (const phase of ["parse", "first-update", "first-finalize", "verify-update", "prepare-validation", "prepare-wal", "closed", "close-during-hash"] as const) {
        const f = await fixture(), candidate = await f.candidate(rootBytes("x".repeat(80 * 1024)));
        const owner = candidate.rootExport, firstHasher = f.hasherStates.length;
        const failure = new Error(`owned publication ${phase} failed`);
        await f.runtime.selectMode();
        let injected = false, closing: Promise<void> | undefined;
        if (phase === "parse") f.hooks.parse = () => { injected = true; throw failure; };
        if (phase === "prepare-validation") (candidate as any).journalEpoch = "invalid";
        if (phase === "prepare-wal") f.storage.onBoundary = event => {
            if (!injected && walHas(event, "intent")) {
                assertExportReleased(owner); injected = true; throw failure;
            }
        };
        f.hooks.hash = (index, event) => {
            if (index === firstHasher) {
                assert.equal(owner.test.releases, 0);
                if ((phase === "first-update" && event === "update") ||
                    (phase === "first-finalize" && event === "finalize")) {
                    injected = true; throw failure;
                }
                if (phase === "close-during-hash" && event === "update" && !injected) {
                    injected = true; closing = f.runtime.quiesce();
                    assert.equal(owner.test.releases, 0, "quiesce released an active preparation owner");
                }
            } else if (phase === "verify-update" && event === "update") {
                assertExportReleased(owner); injected = true; throw failure;
            }
        };
        if (phase === "closed") await f.runtime.quiesce();
        try {
            const error = await rejection(f.runtime.publish(candidate));
            if (["parse", "first-update", "first-finalize", "verify-update", "prepare-wal"].includes(phase)) {
                assert(injected, `failure never reached ${phase}`); assert.equal(error, failure);
            } else if (phase === "prepare-validation") assert.match(String(error), /journal epoch/i);
            else assert.match(String(error), /closing/);
            assertExportReleased(owner);
            assert.equal(f.network.count("commit"), 0);
            assert.equal(f.base.lastAppliedPublication, null); assert.equal(f.journal.unsyncedCount(), 1);
            if (phase !== "prepare-wal") { assert.equal(f.runtime.pending(), null); assert.equal(f.runtime.lastSequence, 0); }
            else assert.equal(f.runtime.snapshot().loaded, false, "failed prepare WAL must poison its real store");
            await closing;
        } finally {
            f.hooks.hash = undefined; f.hooks.parse = undefined; f.storage.onBoundary = undefined;
            await f.runtime.dispose(); f.assertHashersFreed();
        }
    }
}

async function lostResponsesRecoverWithoutResendOrSequenceAdoption() {
    for (const accepted of [true, false]) {
        const f = await fixture(), candidate = await f.candidate(), lost = new Error("lost commit response");
        await f.runtime.selectMode();
        f.network.onCommit = async request => { if (accepted) f.network.accept(request); throw lost; };
        assert.equal(await rejection(f.runtime.publish(candidate)), lost);
        const pending = f.runtime.pending()!;
        assert.equal(pending.terminal, null); assert.equal(f.runtime.lastSequence, 1);
        assert.equal(f.base.lastAppliedPublication, null); assert.equal(f.journal.unsyncedCount(), 1);
        const before = f.network.calls.length; f.network.caps = null;
        await assert.rejects(f.runtime.selectMode(), /root recovery/);
        assert.equal(f.network.calls.length, before, "pending intent negotiated legacy fallback");
        f.network.caps = { ...CAPABILITIES }; await f.runtime.dispose();
        f.network.onCommit = undefined;
        const restarted = await fixture({ storage: f.storage.clone(), network: f.network });
        assert.equal(f.network.calls.length, before, "load dispatched pending recovery automatically");
        assert.deepEqual(restarted.runtime.pending()?.intent, pending.intent);
        assert.deepEqual(await restarted.runtime.resolvePending(), accepted
            ? { status: "accepted", candidateRoot: CANDIDATE, observedRoot: OBSERVED } : { status: "cancelled" });
        assert.equal(f.network.count("commit"), 1, "cold recovery resent the original root mutation");
        assert.equal(f.network.count("cancel"), accepted ? 0 : 1);
        assert.equal(restarted.runtime.pending(), null); assert.equal(restarted.runtime.lastSequence, 1);
        assert.equal(restarted.journal.unsyncedCount(), accepted ? 0 : 1);
        assert.equal(restarted.base.lastAppliedPublication === null, !accepted);
        assert.equal(await restarted.runtime.selectMode(), "durable");
        const second = await restarted.candidate(); await restarted.runtime.publish(second);
        assert.equal(restarted.runtime.lastSequence, 2); assert.equal(f.network.floor, 2);
        await restarted.runtime.dispose(); restarted.assertHashersFreed(); f.assertHashersFreed();
    }
}

async function receivedTailAndDisposalJoinActualOwners() {
    const f = await fixture(), candidate = await f.candidate(); await f.runtime.selectMode();
    const entered = gate(), native = gate<RootTerminalOutcome>(), terminalStarted = gate(), terminalWrite = gate();
    let request!: RootCommitIntent;
    f.network.onCommit = async value => { request = clone(value); entered.resolve(); return native.promise; };
    let held = false;
    f.storage.onBoundary = async event => {
        if (!held && walHas(event, "terminal")) { held = true; terminalStarted.resolve(); await terminalWrite.promise; }
    };
    const operation = f.runtime.publish(candidate); await entered.promise;
    const closing = f.runtime.quiesce(); assert.equal(closing, f.runtime.quiesce());
    let drained = false, disposed = false;
    void closing.then(() => { drained = true; });
    const disposal = f.runtime.dispose().then(() => { disposed = true; });
    await turns(); assert.equal(drained, false); assert.equal(disposed, false);
    await assert.rejects(f.runtime.publish(candidate), /closing/);
    await assert.rejects(f.runtime.selectMode(), /root recovery|closing/);
    assert.match(String(errorOf(() => f.runtime.resolvePending())), /closing/);
    const calls = f.network.calls.length;
    native.resolve(f.network.accept(request)); await terminalStarted.promise;
    assert.equal(f.base.lastAppliedPublication, null); assert.equal(f.journal.unsyncedCount(), 1);
    await turns(); assert.equal(drained, false); assert.equal(disposed, false);
    // A later journal generation arrives while the accepted receipt is still
    // being persisted. The captured ACK must not erase this new work.
    const late = await f.journal.append({ action: "modified", path: "note.md", ts: 2, synced: false });
    terminalWrite.resolve();
    assert.deepEqual(await operation, { status: "accepted", candidateRoot: CANDIDATE, observedRoot: OBSERVED });
    await closing; await disposal;
    assert.equal(f.network.calls.length, calls, "closed received tail dispatched another request");
    assert.equal(f.journal.unsynced()[0]?.id, late); assert.equal(f.base.treeBaseRoot, CANDIDATE);
    assert.deepEqual(await f.inspectDisk(), { pending: null, lastSequence: 1 });
    f.assertHashersFreed();
}

async function closeStopsPendingSelectionAndReentrantDispatch() {
    const f = await fixture(), entered = gate(), answer = gate<RootOutcomeCapabilities | null>();
    f.network.onNegotiate = async () => { entered.resolve(); return answer.promise; };
    const selection = rejection(f.runtime.selectMode()); await entered.promise;
    await f.runtime.quiesce(); answer.resolve({ ...CAPABILITIES });
    assert.match(String(await selection), /closing/); assert.equal(f.network.count("query"), 0);
    await f.runtime.dispose();
    const g = await fixture(); let closing: Promise<void> | undefined;
    g.setScopeHook(() => { closing = g.runtime.quiesce(); });
    await assert.rejects(g.runtime.selectMode(), /closing/);
    assert.equal(g.network.calls.length, 0, "synchronously closed scope hook still admitted native preflight");
    await closing; await g.runtime.dispose();
}

async function closedPreparationMustJoinBeforeDisposal() {
    const f = await fixture(), candidate = await f.candidate(rootBytes("x".repeat(180 * 1024)));
    await f.runtime.selectMode();
    const preparation = rejection(f.runtime.publish(candidate));
    // The engine owns the pre-coordinator hash operation. Quiesce closes
    // admission, but is not a substitute for joining that engine promise.
    await f.runtime.quiesce();
    assert.match(String(await preparation), /closing/);
    assert.equal(f.runtime.pending(), null); assert.equal(f.runtime.lastSequence, 0);
    assert.equal(f.network.count("commit"), 0); assert.equal(f.journal.unsyncedCount(), 1);
    assert.equal(f.base.lastAppliedPublication, null);
    f.assertHashersFreed();
    await f.runtime.dispose();
    assert.deepEqual(await f.inspectDisk(), { pending: null, lastSequence: 0 });
}

async function conflictDownloadsRecheckCapturedOwnerAfterWait() {
    for (const closeInsideHook of [false, true]) {
        const entered = gate(), release = gate();
        let owner!: FixtureRootExport;
        const f = await fixture({ beforeHeavyBatch: async () => { assertExportReleased(owner); entered.resolve(); await release.promise; } });
        const candidate = await f.candidate(), row = candidate.entries[0]; assert.equal(row.action, "upsert");
        owner = candidate.rootExport;
        const fileHash = (row as any).hash;
        f.network.onCommit = async request => receipt(request, "accepted", [{ path: "note.md", base_hash: h(11), side_a_hash: h(12), side_b_hash: fileHash }]);
        await f.runtime.selectMode();
        const operation = rejection(f.runtime.publish(candidate)); await entered.promise;
        assert.equal(f.runtime.pending()?.terminal?.status, "accepted");
        assert.equal(f.base.treeBaseRoot, CANDIDATE); assert.equal(f.journal.unsyncedCount(), 1);
        if (closeInsideHook) f.setScopeHook(() => { void f.runtime.quiesce(); });
        else f.setAllowed(false);
        release.resolve();
        assert.match(String(await operation), closeInsideHook ? /closing/ : /credential owner changed/);
        assert.equal(f.network.count("download"), 0, "credential/suspension fence allowed conflict download");
        assert.equal(f.runtime.pending()?.terminal?.status, "accepted"); assert.equal(f.journal.unsyncedCount(), 1);
        assert.equal(f.platform.calls.some(call => call.startsWith("copy:")), false);
        await f.runtime.dispose(); f.assertHashersFreed();
    }
    // Successful guarded download still uses the real retention/verification
    // and conflict-copy settlement path, without reading current note.md.
    const f = await fixture(), candidate = await f.candidate(), data = new Uint8Array([11, 22, 33]);
    const budget = new ResourceBudget({ capacityBytes: 2 * 1024 * 1024 });
    let releases = 0;
    f.network.onDownload = async (kind, hashes) => {
        assertExportReleased(candidate.rootExport);
        assert.equal(kind, BulkObjectKind.Content); assert.deepEqual(hashes, [digest(data)]);
        const memory = await reserveTransientScope({ ownerBytes: data.length, workBytes: estimateTransportWorkset(data.length) }, { budget });
        const objects = new Map([[digest(data), data]]);
        return { objects, memory, release() { releases++; objects.clear(); memory.close(); } };
    };
    f.network.onCommit = async request => receipt(request, "accepted", [{ path: "note.md", base_hash: h(11), side_a_hash: h(12), side_b_hash: digest(data) }]);
    await f.runtime.selectMode();
    assert.deepEqual(await f.runtime.publish(candidate), { status: "accepted", candidateRoot: CANDIDATE, observedRoot: OBSERVED });
    assert.equal(f.network.count("download"), 1); assert.equal(releases, 1); assert.equal(budget.snapshot().usedBytes, 0);
    assert.equal(f.journal.unsyncedCount(), 0); assert.equal(f.runtime.pending(), null);
    const copies = [...f.platform.files].filter(([path]) => path.includes(" (conflict sync "));
    assert.equal(copies.length, 1); assert.deepEqual(copies[0][1], data);
    assert(!f.platform.calls.includes("read:note.md")); await f.runtime.dispose(); f.assertHashersFreed();
}

async function reloadAfterPoisonDoesNotInventAnotherIntent() {
    const f = await fixture(), candidate = await f.candidate(); await f.runtime.selectMode();
    let failed = false; const failure = new Error("terminal write lost native acknowledgement");
    f.storage.onBoundary = event => {
        if (!failed && walHas(event, "terminal")) { failed = true; throw failure; }
    };
    assert.equal(await rejection(f.runtime.publish(candidate)), failure);
    assert.equal(f.runtime.snapshot().loaded, false); assert.equal(f.journal.unsyncedCount(), 1);
    f.storage.onBoundary = undefined;
    const count = f.network.calls.length; await f.runtime.reload();
    assert.equal(f.network.calls.length, count, "storage reload did recovery network work");
    assert.equal(f.runtime.pending()?.terminal, null); assert.equal(f.runtime.lastSequence, 1);
    await f.runtime.resolvePending();
    assert.equal(f.network.count("commit"), 1); assert.equal(f.runtime.pending(), null);
    assert.equal(f.journal.unsyncedCount(), 0); await f.runtime.dispose(); f.assertHashersFreed();
}

async function finalFreshGuardIsInvocationLocalAndSurvivesPreparationWaits() {
    for (const phase of ["prepare", "negotiate"] as const) {
        const f = await fixture(), candidate = await f.candidate(), entered = gate(), release = gate();
        await f.runtime.selectMode();
        const failure = new Error(`review invalidated during runtime ${phase}`);
        let applicable = true, guardCalls = 0;
        if (phase === "prepare") {
            let held = false;
            f.storage.onBoundary = async event => {
                if (!held && walHas(event, "intent")) { held = true; entered.resolve(); await release.promise; }
            };
        } else f.network.onNegotiate = async () => { entered.resolve(); await release.promise; return { ...CAPABILITIES }; };
        const operation = rejection(f.runtime.publish(candidate, () => {
            guardCalls++; if (!applicable) throw failure;
        }));
        await entered.promise;
        assert.equal(guardCalls, 0); assert.equal(f.network.count("commit"), 0);
        applicable = false; release.resolve();
        assert.equal(await operation, failure, `${phase}: runtime dropped the final applicability guard`);
        assert.equal(guardCalls, 1); assert.equal(f.network.count("commit"), 0);
        assert.equal(f.base.lastAppliedPublication, null); assert.equal(f.journal.unsyncedCount(), 1);
        const saved = await f.inspectDisk();
        assert.equal(saved.pending?.terminal, null); assert.equal(saved.lastSequence, 1);
        assert.deepEqual(saved.pending?.intent.publication.entries, candidate.entries);
        assert.deepEqual(saved.pending?.intent.journalCuts, candidate.journalCuts);
        await f.runtime.dispose(); f.assertHashersFreed();

        const restarted = await fixture({ storage: f.storage.clone(), network: f.network });
        assert.deepEqual(await restarted.runtime.resolvePending(), { status: "cancelled" },
            `${phase}: rejected fresh guard prevented cold resolution`);
        assert.equal(f.network.count("commit"), 0); assert.equal(f.network.count("cancel"), 1);
        assert.equal(restarted.journal.unsyncedCount(), 1); assert.equal(restarted.base.lastAppliedPublication, null);
        assert.equal(guardCalls, 1, "old invocation guard was retained by recovery");
        await restarted.runtime.dispose(); restarted.assertHashersFreed();
    }

    const f = await fixture(), candidate = await f.candidate(), entered = gate(), release = gate();
    await f.runtime.selectMode();
    let applicable = true, guardCalls = 0;
    f.network.onCommit = async request => { entered.resolve(); await release.promise; return f.network.accept(request); };
    const operation = f.runtime.publish(candidate, () => {
        guardCalls++; if (!applicable) throw new Error("old source review is no longer current");
    });
    await entered.promise;
    applicable = false; f.setAllowed(false);
    const later = await f.journal.append({ action: "modified", path: "note.md", ts: 2, synced: false });
    release.resolve();
    assert.deepEqual(await operation, { status: "accepted", candidateRoot: CANDIDATE, observedRoot: OBSERVED });
    assert.equal(guardCalls, 1, "received accepted tail re-ran the old review or API scope");
    assert.deepEqual(f.journal.unsynced().map(row => row.id), [later]);
    assert.equal(f.runtime.pending(), null); assert.equal(f.base.treeBaseRoot, CANDIDATE);
    await f.runtime.dispose(); f.assertHashersFreed();

    const g = await fixture(), next = await g.candidate(); await g.runtime.selectMode();
    let drain: Promise<void> | undefined;
    assert.deepEqual(await g.runtime.publish(next, () => { drain = g.runtime.quiesce(); }),
        { status: "deferred", reason: "closing" }, "reentrant fresh guard close admitted native root");
    assert(drain); await drain;
    assert.equal(g.network.count("commit"), 0); assert.equal(g.runtime.pending()?.terminal, null);
    assert.equal(g.journal.unsyncedCount(), 1); assert.equal(g.base.lastAppliedPublication, null);
    await g.runtime.dispose(); g.assertHashersFreed();
}

async function downgradeCaptureRequiresQuiescenceAndDisposeIsOneOwner() {
    const f = await fixture();
    assert.throws(() => f.runtime.captureDowngradeAuthority(), /quiesced/);
    const draining = f.runtime.quiesce();
    assert.throws(() => f.runtime.captureDowngradeAuthority(), /fully quiesced/,
        "starting quiescence exposed authority before admitted tails settled");
    await draining;
    assert.deepEqual(f.runtime.snapshot(), { loaded: true, pending: false, closed: true });
    assert.deepEqual(f.runtime.captureDowngradeAuthority(), { pending: null, lastSequence: 0 });
    const first = f.runtime.dispose(), repeated = f.runtime.dispose();
    assert.equal(first, repeated, "root runtime returned two disposal owners");
    await first;
    assert.deepEqual(f.runtime.snapshot(), { loaded: false, pending: false, closed: true });
    assert.throws(() => f.runtime.captureDowngradeAuthority(), /validated load/);
    f.assertHashersFreed();
}

async function run() {
    const tests = [loadIsStrictlyLocal, localInitializationCanRetryActualReadFailure, selectionRequiresCompleteFreshPreflight,
        exactCandidateCapturedAndDurableBeforeSend, candidateBoundsCannotPrepareOrDispatch,
        rootExportIsOwnedThroughHashAndReleasedBeforeStoreAndNetwork, rootExportFailurePathsReleaseExactlyOneOwner,
        lostResponsesRecoverWithoutResendOrSequenceAdoption, receivedTailAndDisposalJoinActualOwners,
        closeStopsPendingSelectionAndReentrantDispatch, closedPreparationMustJoinBeforeDisposal,
        conflictDownloadsRecheckCapturedOwnerAfterWait,
        reloadAfterPoisonDoesNotInventAnotherIntent, finalFreshGuardIsInvocationLocalAndSurvivesPreparationWaits,
        downgradeCaptureRequiresQuiescenceAndDisposeIsOneOwner];
    for (const test of tests) await test();
    console.log(`root-sync-runtime.test: ${tests.length} real-store integration suites passed (synthetic tree/transport, portable BLAKE3)`);
}
let completed = false;
process.once("beforeExit", () => {
    if (!completed) { console.error("root-sync-runtime suite did not finish"); process.exitCode = 1; }
});
void run().then(() => { completed = true; }).catch(error => { completed = true; console.error(error); process.exitCode = 1; });
