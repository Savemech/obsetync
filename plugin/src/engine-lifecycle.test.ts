import { createHash } from "node:crypto";
import { TFile } from "obsidian";
import { ObsetyncSyncEngine } from "./sync";
import { ObsetyncSyncBase, SYNC_BASE_STORE_PATH } from "./sync-base";
import { ObsetyncJournal, JOURNAL_STORE_PATH } from "./journal";
import { MemorySegmentedIO, type StoreTestBoundary } from "./segmented-store-test-io";
import { installIncrementalTreeTestAbi } from "./incremental-tree-test-abi";

(globalThis as any).window ??= globalThis;
let assertions = 0;
const check = (value: unknown, message: string): void => {
    assertions++;
    if (!value) throw new Error(message);
};
const PATH = "note.md";
const bytes = (version: number) => new Uint8Array([version]);
const hash = (value: Uint8Array) => createHash("sha256").update(value).digest("hex");
const hashFor = (version: number) => hash(bytes(version));
type Entry = { hash: string; mtime: number; size: number };
const rootFor = (entries: Map<string, Entry>) => hash(new TextEncoder().encode(JSON.stringify([...entries])));

function gate() {
    let entered!: () => void;
    let resolve!: () => void;
    let reject!: (error: unknown) => void;
    const started = new Promise<void>(yes => { entered = yes; });
    const completion = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
    return { started, resolve, reject, wait: async () => { entered(); await completion; } };
}
function observe<T>(work: Promise<T>) {
    const state: { settled: boolean; error?: unknown; value?: T } = { settled: false };
    const done = work.then(value => { state.settled = true; state.value = value; },
        error => { state.settled = true; state.error = error; });
    return { state, done };
}
const turn = () => new Promise<void>(resolve => setImmediate(resolve));

/** Actual engine constructor + real segmented base/journal. Only filesystem
 * source bytes, server transport and WASM tree are deterministic ports. Native
 * completion is modeled by held promises, not by status flags or elapsed time. */
async function fixture() {
    const adapter = new MemorySegmentedIO();
    const refs = new Set<{ event: string; callback: (...args: any[]) => Promise<void> }>();
    const registration = { before: undefined as undefined | ((event: string) => void) };
    const events: string[] = [];
    const cachedRoots: Uint8Array[] = [];
    const app = {
        vault: {
            adapter: Object.assign(adapter, { writeBinary: async (_path: string, data: ArrayBuffer) => {
                cachedRoots.push(new Uint8Array(data).slice()); events.push("root-cache");
            } }),
            on: (event: string, callback: (...args: any[]) => Promise<void>) => {
                registration.before?.(event);
                const ref = { event, callback }; refs.add(ref); return ref;
            },
            offref: (ref: any) => { refs.delete(ref); },
        },
        workspace: { offref: () => {} },
    };
    const base = new ObsetyncSyncBase(app as any);
    const journal = new ObsetyncJournal(app as any);
    await base.load(); await journal.load();
    base.setEntry(PATH, hashFor(1), 1, 1);
    base.setTreeBaseRoot(rootFor(new Map([[PATH, base.getEntry(PATH)!]])));
    await base.save();
    const source = { version: 2 };
    const server = { hash: hashFor(1), root: base.treeBaseRoot! };
    const engines: any[] = [];
    const create = (name: string, active = true, policy: { vaultId?: string; ignores?: string[] } = {}) => {
        let committed = new Map([[PATH, base.getEntry(PATH)!]]);
        let candidate: Map<string, Entry> | null = null;
        const state = {
            roots: 0, preflights: 0, reads: 0, commits: 0, aborts: 0, laneCloses: 0,
            beforeRootAnswer: undefined as undefined | (() => Promise<void>),
            beforeRead: undefined as undefined | (() => Promise<void>),
        };
        const tree = {
            tree_version: () => 1,
            root_hash_hex: () => rootFor(committed), total_files: () => committed.size,
            root_bytes: () => new TextEncoder().encode(JSON.stringify([...committed])),
            begin_candidate: () => { if (candidate) throw new Error("nested candidate"); candidate = new Map(committed); },
            has_candidate: () => candidate !== null,
            candidate_root_hash_hex: () => candidate ? rootFor(candidate) : undefined,
            candidate_root_bytes: () => candidate ? new TextEncoder().encode(JSON.stringify([...candidate])) : undefined,
            candidate_total_files: () => candidate?.size ?? 0,
            candidate_update_batch: (raw: string) => {
                if (!candidate) throw new Error("missing candidate");
                for (const row of JSON.parse(raw)) candidate.set(row.path, { hash: row.hash, mtime: row.mtime_ms, size: row.size });
            },
            candidate_delete_batch: (raw: string) => {
                if (!candidate) throw new Error("missing candidate");
                for (const path of JSON.parse(raw)) candidate.delete(path);
            },
            commit_candidate: () => {
                if (!candidate) throw new Error("missing candidate");
                state.commits++; events.push(`${name}:candidate-commit`);
                committed = candidate; candidate = null;
                return { before: 1, reachable: 1, removed: 0, after: 1 };
            },
            abort_candidate: () => {
                state.aborts++; candidate = null;
                return { before: 1, reachable: 1, removed: 0, after: 1 };
            },
        };
        installIncrementalTreeTestAbi(tree, hash);
        const api = {
            ensureTransportReady: async () => { state.preflights++; },
            checkContent: async () => [],
            closeDataLane: () => { state.laneCloses++; },
            putRoot: async (_vault: string, data: Uint8Array) => {
                state.roots++; events.push(`${name}:root-sent`);
                const accepted = new Map<string, Entry>(JSON.parse(new TextDecoder().decode(data)));
                const acceptedRoot = rootFor(accepted);
                server.hash = accepted.get(PATH)!.hash; server.root = acceptedRoot;
                await state.beforeRootAnswer?.();
                events.push(`${name}:root-answer`);
                return { root_hash: acceptedRoot, conflicts: [] };
            },
        };
        const io = {
            getAbsolutePath: () => null,
            stat: async () => ({ mtime: source.version, size: 1 }),
            readFile: async () => {
                state.reads++; const data = bytes(source.version); await state.beforeRead?.(); return data;
            },
        };
        const wasm = {
            wasm_should_chunk: () => false,
            wasm_root_hash_from_bytes: (data: Uint8Array) => hash(data),
            wasm_root_version_from_bytes: () => 1,
            wasm_hash_batch: (data: Uint8Array, offsets: Uint32Array, sizes: Uint32Array) =>
                [...offsets].map((offset, index) => hash(data.subarray(offset, offset + sizes[index]))),
            wasm_tree_committed_chunk_hashes: () => [], wasm_tree_candidate_chunk_hashes: () => [],
            wasm_tree_new_candidate_chunk_hashes: () => [],
        };
        const engine = new ObsetyncSyncEngine(app as any, api as any, io as any, base, journal,
            wasm as any, tree as any, policy.vaultId ?? "vault", 30_000, "oldest", () => {}, base.treeBaseRoot,
            false, name, false, false, policy.ignores ?? [], undefined, false) as any;
        // This fixture starts at an already-activated session. Do not replace
        // pushPending/stop/drain or their new lifecycle registration machinery.
        if (active) Object.assign(engine, { startupReplayInProgress: false, localPrepared: true, startupCompleted: true,
            treeBaseRoot: base.treeBaseRoot, state: "idle" });
        engines.push(engine);
        return { engine, state };
    };
    return {
        adapter, app, base, journal, source, server, events, refs, registration, cachedRoots, create,
        queue: async (engine: any, version: number) => {
            source.version = version;
            const id = await journal.append({ action: "modified", path: PATH, ts: version, synced: false });
            engine.pendingChanges.add({ action: "modified", path: PATH, hash: hashFor(version), mtime: version, size: 1 }, id);
            return id;
        },
        cold: async () => {
            const coldApp = { vault: { adapter: new MemorySegmentedIO(adapter.snapshot()) } };
            const coldBase = new ObsetyncSyncBase(coldApp as any);
            const coldJournal = new ObsetyncJournal(coldApp as any);
            await coldBase.load(); await coldJournal.load();
            return { base: coldBase, journal: coldJournal };
        },
        close: () => { for (const engine of engines) engine.stop(); },
    };
}
type Fixture = Awaited<ReturnType<typeof fixture>>;
function walHas(event: StoreTestBoundary, path: string, operation: string): boolean {
    return event.method === "write" && event.phase === "before" && event.path.startsWith(`${path}/wal-`) &&
        JSON.parse(JSON.parse(event.data!).payload).rows.some((row: any) => row.op === operation);
}
function stopAndDrain(engine: any): Promise<void> {
    check(typeof engine.stopAndDrain === "function", "engine.stopAndDrain() lifecycle barrier is not implemented");
    return engine.stopAndDrain();
}
function attachModify(f: Fixture, engine: any) {
    const globals = globalThis as any;
    const previous = globals.__obsetyncTestDebounce;
    const before = new Set(f.refs);
    try {
        globals.__obsetyncTestDebounce = () => () => {};
        engine.attachVaultListeners();
    } finally {
        if (previous === undefined) delete globals.__obsetyncTestDebounce;
        else globals.__obsetyncTestDebounce = previous;
    }
    return [...f.refs].find(ref => !before.has(ref) && ref.event === "modify")!.callback;
}
function withUi<T>(work: () => T): T {
    const globals = globalThis as any;
    const debounce = globals.__obsetyncTestDebounce, notice = globals.__obsetyncTestNotice;
    try {
        globals.__obsetyncTestDebounce = () => () => {};
        globals.__obsetyncTestNotice = () => ({ setMessage: () => {}, hide: () => {} });
        return work();
    } finally {
        if (debounce === undefined) delete globals.__obsetyncTestDebounce;
        else globals.__obsetyncTestDebounce = debounce;
        if (notice === undefined) delete globals.__obsetyncTestNotice;
        else globals.__obsetyncTestNotice = notice;
    }
}
const eventOf = (engine: any, event: string): ((...args: any[]) => Promise<void>) =>
    engine.eventRefs.find((ref: any) => ref.event === event).callback;
const fileAt = (path = PATH, version = 2) => Object.assign(new TFile(), { path, stat: { size: 1, mtime: version } });
const handoff = (old: any, next: any): Promise<void> => withUi(() => old.handoffCaptureTo(next));

/** Documents why the synchronous stop API alone is not a replacement-owner
 * barrier. This remains an intentionally unsafe control case after drain is
 * added: main must await the stronger ownership handoff before starting B. */
async function legacyStopLateAckOverwritesNewerDurableBase(): Promise<void> {
    const f = await fixture(); const delayed = gate();
    try {
        const a = f.create("A"); await f.queue(a.engine, 2);
        a.state.beforeRootAnswer = delayed.wait;
        const pushingA = a.engine.pushPending();
        await delayed.started;
        a.engine.stop();
        const b = f.create("B"); await f.queue(b.engine, 3);
        await b.engine.pushPending();
        check(f.base.getHash(PATH) === hashFor(3), "baseline B did not commit newer shared metadata");
        delayed.resolve(); await pushingA;
        const cold = await f.cold();
        check(f.server.hash === hashFor(3) && cold.base.getHash(PATH) === hashFor(2),
            "unsafe-control fixture did not reproduce A late-ACK overwriting B's durable base");
        check(cold.journal.unsyncedCount() === 0,
            "baseline needs both generations acknowledged to expose silent stale local state");
        console.log("engine-lifecycle baseline: server=B, cold sync-base=A, journal=empty after stop without drain");
    } finally { delayed.resolve(); f.close(); }
}

async function drainWaitsForRootBaseAndJournalNativeSettlement(): Promise<void> {
    const f = await fixture(); const root = gate(), base = gate(), ack = gate();
    try {
        const a = f.create("A"); await f.queue(a.engine, 2);
        a.state.beforeRootAnswer = root.wait;
        let baseHeld = false, ackHeld = false;
        f.adapter.onBoundary = async event => {
            if (!baseHeld && walHas(event, SYNC_BASE_STORE_PATH, "set")) { baseHeld = true; await base.wait(); }
            if (!ackHeld && walHas(event, JOURNAL_STORE_PATH, "ack")) { ackHeld = true; await ack.wait(); }
        };
        const pushing = observe(a.engine.pushPending());
        await root.started;
        const first = observe(stopAndDrain(a.engine)), repeated = observe(stopAndDrain(a.engine));
        await turn();
        check(a.engine.isStopped() && !first.state.settled && !repeated.state.settled,
            "stop/drain did not synchronously stop admission or wait for the old root ACK");
        root.resolve(); await base.started; await turn();
        check(!first.state.settled && !repeated.state.settled && !pushing.state.settled,
            "root ACK was mistaken for completed native sync-base publication");
        base.resolve(); await ack.started; await turn();
        check(!first.state.settled && f.journal.unsyncedCount() === 1,
            "base save was mistaken for completed durable journal ACK");
        ack.resolve(); await Promise.all([first.done, repeated.done, pushing.done]);
        check(first.state.error === undefined && repeated.state.error === undefined && pushing.state.error === undefined,
            "successful drain/accepted transaction unexpectedly rejected");
        check(f.base.getHash(PATH) === hashFor(2) && f.journal.unsyncedCount() === 0,
            "drain lost the server-accepted old transaction's local settlement");
        const b = f.create("B"); await f.queue(b.engine, 3); await b.engine.pushPending();
        const cold = await f.cold();
        check(f.server.hash === hashFor(3) && cold.base.getHash(PATH) === hashFor(3) && cold.journal.unsyncedCount() === 0,
            "joined replacement still allowed old metadata to overwrite newer B");
        await stopAndDrain(a.engine);
    } finally { root.resolve(); base.resolve(); ack.resolve(); f.close(); }
}

async function stoppedAdmissionCannotStartAnotherOperation(): Promise<void> {
    const f = await fixture();
    try {
        const a = f.create("A"); await f.queue(a.engine, 2);
        await stopAndDrain(a.engine);
        const before = a.state.preflights + a.state.roots + a.state.reads;
        await a.engine.pushPending(); await a.engine.pullRemote();
        await a.engine.reconcileContent(); await a.engine.fullScan(); await a.engine.partialMtimeScan();
        check(a.state.preflights + a.state.roots + a.state.reads === before,
            "stopped engine admitted new filesystem/network work");
        check(a.engine.pendingChanges.size === 1 && f.journal.unsyncedCount() === 1,
            "stopped admission erased unpublished work");
    } finally { f.close(); }
}

async function startedCaptureIsDrainedThroughItsNativeAppend(): Promise<void> {
    for (const failAppend of [false, true]) {
        const f = await fixture(); const append = gate();
        try {
            const a = f.create("A"); const modify = attachModify(f, a.engine);
            let held = false;
            f.adapter.onBoundary = async event => {
                if (!held && walHas(event, JOURNAL_STORE_PATH, "append")) {
                    held = true; await append.wait();
                    if (failAppend) throw new Error("injected native journal append failure");
                }
            };
            const file = Object.assign(new TFile(), { path: PATH, stat: { size: 1, mtime: 2 } });
            const callback = observe(modify(file));
            await append.started;
            const drained = observe(stopAndDrain(a.engine));
            await turn();
            check(!drained.state.settled && !callback.state.settled,
                "drain completed while a registered local callback still owned a native append");
            append.resolve(); await Promise.all([callback.done, drained.done]);
            check(a.engine.pendingChanges.size === 1 && a.engine.localEventsInFlight.size === 0,
                "started callback lost its dirty hint or leaked its local guard after stop");
            const pending = a.engine.pendingChanges.take();
            if (failAppend) {
                check(pending[0].journalId === undefined && f.journal.unsyncedCount() === 0,
                    "failed native append invented a durable generation or lost its volatile hint");
            } else {
                const cold = await f.cold();
                check(cold.journal.unsyncedCount() === 1 && pending[0].journalId === cold.journal.unsynced()[0].id,
                    "drained started callback did not preserve its durable replay evidence");
            }
        } finally { append.resolve(); f.close(); }
    }
}

async function rejectedRootDoesNotPretendToBeNativeCompletion(): Promise<void> {
    const f = await fixture(); const native = gate();
    try {
        const a = f.create("A"); const id = await f.queue(a.engine, 2);
        a.state.beforeRootAnswer = native.wait;
        const pushing = observe(a.engine.pushPending());
        await native.started;
        a.engine.stop();
        const drained = observe(stopAndDrain(a.engine)); await turn();
        check(!drained.state.settled, "abort flag/socket close pretended an issued root request had settled");
        native.reject(new Error("lost root response after transport failure"));
        await Promise.all([drained.done, pushing.done]);
        check(a.state.commits === 0 && a.engine.pendingChanges.size === 1 && f.base.getHash(PATH) === hashFor(1),
            "failed root was treated as accepted or erased its retry hint");
        const cold = await f.cold();
        check(cold.journal.unsynced()[0]?.id === id, "unknown root outcome was durably acknowledged");
    } finally { native.resolve(); f.close(); }
}

async function checkpointBeginAndCompleteBelongToDrain(): Promise<void> {
    for (const phase of ["begin", "complete"] as const) {
        const f = await fixture(); const native = gate();
        try {
            const a = f.create("A"); await f.queue(a.engine, 2);
            a.engine.operationCheckpoint = {
                begin: async () => { if (phase === "begin") await native.wait(); return "operation"; },
                progress: () => {},
                complete: async () => { if (phase === "complete") await native.wait(); },
            };
            const pushing = observe(a.engine.pushPending()); await native.started;
            const preflights = a.state.preflights;
            const drained = observe(stopAndDrain(a.engine)); await turn();
            check(!drained.state.settled, `checkpoint ${phase} native work was omitted from drain`);
            native.resolve(); await Promise.all([drained.done, pushing.done]);
            if (phase === "begin") {
                check(a.state.roots === 0 && a.state.preflights === preflights && a.engine.pendingChanges.size === 1,
                    "stopped checkpoint continuation admitted fresh network work or lost pending work");
            } else check(f.journal.unsyncedCount() === 0 && a.state.commits === 1,
                "post-ACK checkpoint drain undid an accepted transaction");
        } finally { native.resolve(); f.close(); }
    }
}

async function quiescenceCapturesEditsWhileAcceptedRootFinishes(): Promise<void> {
    const f = await fixture(); const root = gate();
    try {
        const a = f.create("A"); const modify = attachModify(f, a.engine);
        await f.queue(a.engine, 2); a.state.beforeRootAnswer = root.wait;
        const pushing = observe(a.engine.pushPending()); await root.started;
        const drained = observe(a.engine.quiesceAndDrain());
        f.source.version = 3;
        await modify(fileAt(PATH, 3));
        check(!drained.state.settled && a.engine.pendingChanges.has(PATH) && f.journal.unsynced()[0]?.id === 2,
            "capture during quiescence was lost or runtime drained before root completion");
        root.resolve(); await Promise.all([pushing.done, drained.done]);
        check(f.journal.unsyncedCount() === 1 && f.journal.unsynced()[0].id === 2,
            "old accepted root ACK erased the edit captured during quiescence");
        const b = f.create("B", false, { vaultId: "changed-remote-vault", ignores: ["ignored/"] });
        await handoff(a.engine, b.engine);
        check(b.engine.pendingChanges === a.engine.pendingChanges && b.engine.deferredChanges === a.engine.deferredChanges &&
            b.engine.localEventsInFlight === a.engine.localEventsInFlight &&
            b.engine.recentLocalUpserts === a.engine.recentLocalUpserts,
        "same-physical-vault policy change cloned or discarded capture ownership");
        const pending = b.engine.pendingChanges.take(); b.engine.pendingChanges.restore(pending);
        check(pending[0]?.mtime === 3 && pending[0]?.journalId === 2,
            "replacement did not receive the latest captured edit");
    } finally { root.resolve(); f.close(); }
}

async function overlappingEchoAndThirdReplacementKeepLatestCapture(): Promise<void> {
    const f = await fixture(); const nativeRead = gate();
    try {
        const a = f.create("A"); const oldModify = attachModify(f, a.engine);
        a.engine.pullEchoes.register([{ path: PATH, action: "upsert", hash: hashFor(2) }]);
        a.state.beforeRead = nativeRead.wait;
        const oldCallback = observe(oldModify(fileAt(PATH, 2))); await nativeRead.started;
        await a.engine.quiesceAndDrain();
        const b = f.create("B", false);
        const firstHandoff = observe(handoff(a.engine, b.engine));
        check(b.engine.localEventsInFlight === a.engine.localEventsInFlight && b.engine.localEventsInFlight.has(PATH),
            "replacement did not immediately inherit the active old callback's guard");
        f.source.version = 3;
        const bModify = eventOf(b.engine, "modify");
        await bModify(fileAt(PATH, 3));
        const bDrained = observe(b.engine.quiesceAndDrain()); await turn();
        check(!firstHandoff.state.settled && !bDrained.state.settled,
            "third replacement could bypass the inherited old native callback");
        nativeRead.resolve(); await Promise.all([oldCallback.done, firstHandoff.done, bDrained.done]);
        check(b.engine.localEventsInFlight.size === 0, "settled echo callback leaked a shared path guard");
        const current = b.engine.pendingChanges.capture().get(PATH);
        check(current && b.engine.recentLocalUpserts.matches(current),
            "late A echo callback replaced B's newer urgent scheduling provenance");
        const c = f.create("C", false); await handoff(b.engine, c.engine);
        const pending = c.engine.pendingChanges.take(); c.engine.pendingChanges.restore(pending);
        check(pending.length === 1 && pending[0].mtime === 3 && pending[0].journalId === 2 &&
            f.journal.unsynced()[0]?.id === 2,
        "late A echo classification overwrote or acknowledged B's newer callback generation");
        const before = f.journal.unsynced()[0]?.id;
        await oldModify(fileAt(PATH, 4)); await bModify(fileAt(PATH, 5));
        const unchanged = c.engine.pendingChanges.take(); c.engine.pendingChanges.restore(unchanged);
        check(f.journal.unsynced()[0]?.id === before && unchanged[0].mtime === 3,
            "detached stale callbacks entered capture after ownership transfer");
        await c.engine.stopAndDrain();
    } finally { nativeRead.resolve(); f.close(); }
}

async function failedRenameAppendKeepsUncertainLinksAcrossHandoff(): Promise<void> {
    const f = await fixture(); const nativeAppend = gate();
    try {
        const a = f.create("A"); attachModify(f, a.engine); await a.engine.quiesceAndDrain();
        let held = false;
        f.adapter.onBoundary = async event => {
            if (!held && walHas(event, JOURNAL_STORE_PATH, "append")) {
                held = true; await nativeAppend.wait(); throw new Error("injected uncertain rename append failure");
            }
        };
        const callback = observe(eventOf(a.engine, "rename")(fileAt("renamed.md", 3), PATH));
        await nativeAppend.started;
        const b = f.create("B", false); const transferred = observe(handoff(a.engine, b.engine));
        await turn(); check(!transferred.state.settled, "handoff omitted uncertain native rename append");
        nativeAppend.resolve(); await Promise.all([callback.done, transferred.done]);
        const queued = b.engine.pendingChanges.take(); b.engine.pendingChanges.restore(queued);
        check(queued.length === 2 && queued.every((hint: any) => hint.journalId === undefined),
            "failed rename handoff lost an endpoint or invented durable IDs");
        check(b.engine.deferredChanges === a.engine.deferredChanges, "uncertain rename graph was not transferred by ownership");
        const heldResult = b.engine.deferredChanges.settle(queued, queued, [{ path: PATH,
            reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0 }], "lifecycle", 0);
        check(heldResult.retained.length === 2 && heldResult.acknowledged.length === 0,
            "uncertain rename dependency was flattened during capture handoff");
        f.adapter.onBoundary = undefined;
        await b.engine.reloadPersistence();
        check(b.engine.pendingChanges.size === 2 && f.journal.unsyncedCount() === 0,
            "validated persistence reload discarded non-durable current-session rename hints");
    } finally { nativeAppend.resolve(); f.close(); }
}

async function failedNextRegistrationBridgesItsStartedCallbackToRetry(): Promise<void> {
    const f = await fixture(); const nativeAppend = gate();
    try {
        const a = f.create("A"); attachModify(f, a.engine); await a.engine.quiesceAndDrain();
        const b = f.create("B", false), c = f.create("C", false);
        let failedCallback: ReturnType<typeof observe> | undefined;
        let held = false;
        f.adapter.onBoundary = async event => {
            if (!held && walHas(event, JOURNAL_STORE_PATH, "append")) { held = true; await nativeAppend.wait(); }
        };
        const registrationFailure = new Error("injected second listener registration failure");
        f.registration.before = event => {
            if (event !== "create") return;
            f.registration.before = undefined;
            failedCallback = observe(eventOf(b.engine, "modify")(fileAt(PATH, 2)));
            throw registrationFailure;
        };
        let caught: unknown;
        try { await handoff(a.engine, b.engine); } catch (error) { caught = error; }
        check(caught === registrationFailure && failedCallback !== undefined,
            "fixture did not enter failed replacement's reentrant callback before registration threw");
        await nativeAppend.started;
        const retry = observe(handoff(a.engine, c.engine));
        const cDrained = observe(c.engine.quiesceAndDrain()); await turn();
        check(!retry.state.settled && !cDrained.state.settled,
            "retry C bypassed callback ownership admitted by failed replacement B");
        nativeAppend.resolve(); await Promise.all([failedCallback!.done, retry.done, cDrained.done]);
        check(c.engine.pendingChanges.has(PATH) && f.journal.unsyncedCount() === 1 && c.engine.localEventsInFlight.size === 0,
            "failed-B callback result was lost or leaked after retry handoff C");
    } finally { nativeAppend.resolve(); f.registration.before = undefined; f.close(); }
}

async function thirdStopWaitsForActualPersistenceReload(): Promise<void> {
    for (const storePath of [SYNC_BASE_STORE_PATH, JOURNAL_STORE_PATH]) {
        const f = await fixture(); const nativeRead = gate();
        try {
            const b = f.create("B", false);
            let held = false;
            f.adapter.onBoundary = async event => {
                if (!held && event.method === "read" && event.phase === "before" && event.path.startsWith(storePath + "/")) {
                    held = true; await nativeRead.wait();
                }
            };
            const loading = observe(b.engine.reloadPersistence()); await nativeRead.started;
            const drained = observe(b.engine.stopAndDrain()); await turn();
            check(!drained.state.settled && !loading.state.settled,
                "replacement/unload barrier bypassed real native persistence load");
            nativeRead.resolve(); await Promise.all([loading.done, drained.done]);
            check(b.engine.isStopped() && b.state.preflights === 0 && b.state.roots === 0 && !b.engine.localPrepared,
                "stopped persistence reload activated a runtime or local preparation");
        } finally { nativeRead.resolve(); f.close(); }
    }
}

async function stopInsideRealJournalReplayKeepsItsDurableRemainder(): Promise<void> {
    const f = await fixture();
    try {
        await Promise.all(Array.from({ length: 257 }, (_, index) => f.journal.append({
            action: "modified", path: `replay-${index}.md`, ts: 1, synced: false,
        })));
        const b = f.create("B", false);
        let drained: ReturnType<typeof observe> | undefined, enteredAtYield = false;
        b.engine.onStatusUpdate = (status: string) => {
            if (status !== "⟳ journal 256/257") return;
            enteredAtYield = true;
            drained = observe(b.engine.stopAndDrain());
            check(!drained.state.settled, "reentrant stop did not own the currently executing replay");
        };
        const preparing = observe(withUi(() => b.engine.prepareLocal()));
        await preparing.done; if (drained) await drained.done;
        check(enteredAtYield && (preparing.state.error as Error)?.name === "AbortError" && !b.engine.localPrepared,
            "real replay did not abort at its bounded yield after stop");
        const cold = await f.cold();
        check(cold.journal.unsyncedCount() === 257 && b.state.preflights === 0,
            "interrupted replay ACKed its unprocessed durable remainder or activated sync");
    } finally { f.close(); }
}

async function retiredDebugGettersNeverCallReleasedTree(): Promise<void> {
    const f = await fixture();
    const root = gate();
    try {
        const a = f.create("A");
        await f.queue(a.engine, 2);
        a.state.beforeRootAnswer = root.wait;
        const pushing = a.engine.pushPending();
        await root.started;
        const draining = a.engine.quiesceAndDrain();
        check(a.engine.getTreeRootHash() !== null && a.engine.getTreeFileCount() === 1,
            "stopped-but-undrained accepted transaction lost access to its tree");
        root.resolve();
        await Promise.all([pushing, draining]);
        let calls = 0;
        const released = () => { calls++; throw new Error("freed WASM wrapper was invoked"); };
        a.engine.tree.root_hash_hex = released;
        a.engine.tree.total_files = released;
        a.engine.localRootHash = null;
        check(a.engine.getTreeRootHash() === null && a.engine.getTreeFileCount() === -1 &&
            a.engine.getLocalRootHash() === null && calls === 0,
            "post-drain debug relied on trapping inside a freed WASM wrapper");
    } finally { root.resolve(); f.close(); }
}

const deadline = setTimeout(() => {
    console.error("engine-lifecycle.test: native gate suite did not finish"); process.exitCode = 1;
}, 30_000);
void legacyStopLateAckOverwritesNewerDurableBase()
    .then(drainWaitsForRootBaseAndJournalNativeSettlement)
    .then(stoppedAdmissionCannotStartAnotherOperation)
    .then(startedCaptureIsDrainedThroughItsNativeAppend)
    .then(rejectedRootDoesNotPretendToBeNativeCompletion)
    .then(checkpointBeginAndCompleteBelongToDrain)
    .then(quiescenceCapturesEditsWhileAcceptedRootFinishes)
    .then(overlappingEchoAndThirdReplacementKeepLatestCapture)
    .then(failedRenameAppendKeepsUncertainLinksAcrossHandoff)
    .then(failedNextRegistrationBridgesItsStartedCallbackToRetry)
    .then(thirdStopWaitsForActualPersistenceReload)
    .then(stopInsideRealJournalReplayKeepsItsDurableRemainder)
    .then(retiredDebugGettersNeverCallReleasedTree)
    .then(() => { clearTimeout(deadline); console.log(`engine-lifecycle.test: ${assertions} assertions passed`); })
    .catch(error => { clearTimeout(deadline); console.error(error); process.exitCode = 1; });
