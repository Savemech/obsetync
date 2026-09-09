import { TFile } from "obsidian";
import { ObsetyncSyncEngine } from "./sync";
import { DirtyPathSet } from "./dirty-set";
import { DeferredChangeTracker } from "./deferred-changes";
import { LocalEventGuards } from "./local-event-guards";
import { activatePreparedSync } from "./startup-activation";

let assertions = 0;
const check = (condition: unknown, message: string): void => {
    assertions++;
    if (!condition) throw new Error(message);
};
function deferred() {
    let resolve!: () => void;
    const promise = new Promise<void>(done => { resolve = done; });
    return { promise, resolve };
}

function fixture(rows: any[]) {
    const callbacks = new Map<string, (...args: any[]) => Promise<void>>();
    const engine = new ObsetyncSyncEngine({} as any, {} as any, {} as any, {} as any,
        {} as any, {} as any, {} as any, "test") as any;
    let nextId = 1001;
    let nativeReads = 0;
    let network = 0;
    let registrations = 0;
    let replays = 0;
    let durablePaths = new Set<string>(rows.flatMap(row => row.oldPath ? [row.path, row.oldPath] : [row.path]));
    Object.assign(engine, {
        app: { vault: {
            on: (event: string, callback: any) => {
                registrations++;
                callbacks.set(event, callback);
                return { event, callback };
            },
            offref: (ref: any) => { if (callbacks.get(ref.event) === ref.callback) callbacks.delete(ref.event); },
        }, workspace: { offref: () => {} } },
        journal: {
            unsynced: () => { throw new Error("startup must not materialize the journal array"); },
            unsyncedCount: () => { replays++; return rows.length; },
            iterateUnsynced: () => {
                const end = rows.length;
                return (function* () { for (let i = 0; i < end; i++) yield { ...rows[i] }; })();
            },
            capturePendingPaths: () => {
                const captured = durablePaths;
                return { has: (path: string) => captured.has(path) };
            },
            append: async (entry: any) => {
                const id = nextId++;
                rows.push({ ...entry, id });
                durablePaths = new Set(durablePaths).add(entry.path);
                if (entry.oldPath) durablePaths.add(entry.oldPath);
                return id;
            },
            acknowledge: async () => { throw new Error("local replay must not ACK journal rows"); },
        },
        pendingChanges: new DirtyPathSet(), deferredChanges: new DeferredChangeTracker(),
        localEventsInFlight: new LocalEventGuards(), hashWorkerAbort: new AbortController(),
        eventRefs: [],
        syncBase: { treeBaseRoot: "verified-base" }, isExcluded: () => false,
        onStatusUpdate: () => {}, progressHeartbeat: () => {},
        autoSync: true, reenrollmentRequired: false, realtimeWs: false, syncing: false, state: "idle",
        api: { ping: async () => { network++; throw new Error("offline"); },
            ensureTransportReady: async () => { network++; }, closeDataLane: () => {} },
        io: { readFile: async () => { nativeReads++; throw new Error("replay must not read content"); },
            stat: async () => { nativeReads++; throw new Error("replay must not stat content"); } },
    });
    return { engine, callbacks, nativeReads: () => nativeReads, network: () => network,
        registrations: () => registrations, replays: () => replays };
}

function withUiStubs<T>(operation: () => T): T {
    const globals = globalThis as any;
    const previousDebounce = globals.__obsetyncTestDebounce;
    const previousNotice = globals.__obsetyncTestNotice;
    try {
        globals.__obsetyncTestDebounce = (callback: () => void) => callback;
        globals.__obsetyncTestNotice = () => ({ setMessage: () => {}, hide: () => {} });
        return operation();
    } finally {
        if (previousDebounce === undefined) delete globals.__obsetyncTestDebounce;
        else globals.__obsetyncTestDebounce = previousDebounce;
        if (previousNotice === undefined) delete globals.__obsetyncTestNotice;
        else globals.__obsetyncTestNotice = previousNotice;
    }
}

const startWithUiStubs = (engine: any): Promise<void> => withUiStubs(() => engine.start());

async function assertSyncGated(f: ReturnType<typeof fixture>): Promise<void> {
    const beforeNetwork = f.network();
    const beforeReads = f.nativeReads();
    f.engine.bulkChangeReviewRequired = true;
    await f.engine.forceSync();
    // Exercise the real gate even when an activation scenario installs a
    // terminal pull observer to stop startup after the first network phase.
    await (ObsetyncSyncEngine.prototype as any).pullRemote.call(f.engine);
    await f.engine.pushPending();
    await f.engine.reconcileContent();
    await f.engine.fullScan();
    await f.engine.partialMtimeScan();
    check(f.network() === beforeNetwork && f.nativeReads() === beforeReads &&
        f.engine.bulkChangeReviewRequired && f.engine.syncTimer === null,
        "prepared but unactivated engine started network/apply/scan/runtime work");
    f.engine.bulkChangeReviewRequired = false;
}

async function pluginWaitKeepsLocalCaptureButNotSync(): Promise<void> {
    const rows = [{ id: 1, action: "modified", path: "offline.md", ts: 1, synced: false }];
    const f = fixture(rows);
    check(f.engine.isBusy(), "constructed engine left negotiation gate open");
    await assertSyncGated(f);
    const negotiation = deferred();
    const firstPull = deferred();
    const finishPull = deferred();
    let selected = false;
    let syncNetwork = 0;
    const activation = withUiStubs(() => activatePreparedSync({
        engine: f.engine,
        negotiation: negotiation.promise,
        isCurrent: () => true,
        selectTree: () => { selected = true; },
    }));
    // Await the same local preparation, not negotiation: plugin helper must
    // already have started it while the capability response is unresolved.
    await f.engine.prepareLocal();
    check(f.engine.localPrepared && f.engine.pendingChanges.has("offline.md") && !selected,
        "plugin awaited negotiation before completing local replay");
    check(f.registrations() === 4 && f.replays() === 1 && f.engine.isBusy(),
        "local preparation was duplicated or opened activation gate");
    const file = Object.assign(new TFile(), { path: "during-negotiation.md", stat: { size: 3, mtime: 4 } });
    await f.callbacks.get("modify")!(file);
    check(rows.length === 2 && f.engine.pendingChanges.has(file.path),
        "edit during unresolved plugin negotiation did not become durable/protected");
    await assertSyncGated(f);
    f.engine.api.ping = async () => {
        syncNetwork++;
        check(selected && f.engine.pendingChanges.has(file.path), "engine ping preceded tree selection/local capture");
        return { ok: true };
    };
    f.engine.pullRemote = async () => {
        syncNetwork++;
        check(selected, "remote apply began before negotiated tree selection");
        firstPull.resolve();
        await finishPull.promise;
    };
    negotiation.resolve();
    await firstPull.promise;
    check(syncNetwork === 2, "plugin did not activate sync after negotiation selection");
    f.engine.stop();
    finishPull.resolve();
    check(!await activation && f.engine.isStopped() && f.callbacks.size === 0,
        "cancelled plugin activation revived an engine or kept listeners");
    await f.engine.start();
    await f.engine.prepareLocal();
    check(syncNetwork === 2 && f.registrations() === 4 && f.engine.syncTimer === null,
        "start/prepare after stop revived startup resources");
}

async function stalePluginGenerationCannotActivate(): Promise<void> {
    const f = fixture([]);
    const negotiation = deferred();
    let current = true;
    let selections = 0;
    const activation = withUiStubs(() => activatePreparedSync({
        engine: f.engine, negotiation: negotiation.promise, isCurrent: () => current,
        selectTree: () => { selections++; },
    }));
    await f.engine.prepareLocal();
    current = false;
    // Exercise the generation fence independently of main's quiescence.
    negotiation.resolve();
    check(!await activation && selections === 0 && f.network() === 0 && f.callbacks.size === 4 && f.engine.isStopped(),
        "late plugin negotiation selected a format or started a replaced/unloaded engine");
    await f.engine.stopAndDrain();
}

async function staleActivationKeepsCaptureForReplacement(): Promise<void> {
    for (const phase of ["prepare", "negotiation"] as const) {
        const rows: any[] = [];
        const f = fixture(rows);
        const preparing = deferred();
        const entered = deferred();
        const negotiation = deferred();
        let current = true;
        f.engine.preparedTransfers = { plan: { load: async () => {
            entered.resolve();
            if (phase === "prepare") await preparing.promise;
        } }, scopeForTree: async () => "not-selected" };
        const activation = withUiStubs(() => activatePreparedSync({ engine: f.engine,
            negotiation: negotiation.promise, isCurrent: () => current,
            selectTree: () => { throw new Error("stale activation selected a tree"); } }));
        await entered.promise;
        if (phase === "negotiation") await f.engine.prepareLocal();
        current = false;
        const draining = f.engine.quiesceAndDrain();
        const edited = Object.assign(new TFile(), { path: `during-${phase}.md`, stat: { mtime: 5, size: 7 } });
        await f.callbacks.get("modify")!(edited);
        preparing.resolve();
        negotiation.resolve();
        check(!await activation, `${phase}: stale activation reported success`);
        await draining;
        check(f.callbacks.size === 4 && f.engine.pendingChanges.has(edited.path),
            `${phase}: stale continuation terminal-stopped replacement capture`);
        const next = fixture(rows);
        Object.assign(next.engine, { app: f.engine.app, journal: f.engine.journal, syncBase: f.engine.syncBase });
        await withUiStubs(() => f.engine.handoffCaptureTo(next.engine));
        await withUiStubs(() => next.engine.prepareLocal());
        check(next.engine.pendingChanges === f.engine.pendingChanges && next.engine.pendingChanges.has(edited.path),
            `${phase}: captured edit was lost across stale-activation handoff`);
        await next.engine.stopAndDrain();
        check(f.callbacks.size === 0, `${phase}: replacement listeners did not stop`);
    }
}

async function stopDuringPreparationCannotActivate(): Promise<void> {
    const rows = Array.from({ length: 300 }, (_, index) => ({
        id: index + 1, action: "modified", path: `captured-${index}.md`, ts: 1, synced: false,
    }));
    const f = fixture(rows);
    let selections = 0;
    f.engine.onStatusUpdate = () => f.engine.stop();
    const activated = await withUiStubs(() => activatePreparedSync({
        engine: f.engine, negotiation: Promise.resolve(), isCurrent: () => true,
        selectTree: () => { selections++; },
    }));
    check(!activated && !f.engine.localPrepared && f.engine.isStopped() && selections === 0,
        "cancelled preparation activated negotiation or declared replay complete");
    await f.engine.start();
    await f.engine.prepareLocal();
    check(f.callbacks.size === 0 && f.registrations() === 4 && f.network() === 0 && f.engine.syncTimer === null,
        "preparation completion after unload revived listeners/network/timers");
}

async function partialListenerFailureRetriesCleanly(): Promise<void> {
    const f = fixture([]);
    const on = f.engine.app.vault.on;
    f.engine.app.vault.on = (event: string, callback: any) => {
        if (event === "create") throw new Error("injected listener registration failure");
        return on(event, callback);
    };
    let failed = false;
    try { await withUiStubs(() => f.engine.prepareLocal()); } catch { failed = true; }
    check(failed && f.callbacks.size === 0 && f.replays() === 0 && f.engine.isBusy(),
        "partial listener failure kept duplicate registrations or started replay");
    f.engine.app.vault.on = on;
    await withUiStubs(() => f.engine.prepareLocal());
    await f.engine.prepareLocal();
    check(f.callbacks.size === 4 && f.registrations() === 5 && f.replays() === 1,
        "listener registration retry did not retain exactly one callback per event");
    f.engine.stop();
}

async function failedPreparationCanRetryWithoutDuplicateListeners(): Promise<void> {
    const f = fixture([]);
    const replay = f.engine.journal.unsyncedCount;
    let fail = true;
    f.engine.journal.unsyncedCount = () => { if (fail) throw new Error("retryable local metadata failure"); return replay(); };
    let selections = 0;
    const activate = () => withUiStubs(() => activatePreparedSync({
        engine: f.engine, negotiation: Promise.resolve(), isCurrent: () => true,
        selectTree: () => { selections++; },
    }));
    let failed = false;
    try { await activate(); } catch { failed = true; }
    check(failed && selections === 0 && f.network() === 0 && f.registrations() === 4,
        "failed preparation activated a tree or discarded local capture");
    fail = false;
    await f.engine.prepareLocal();
    await f.engine.prepareLocal();
    check(f.registrations() === 4 && f.engine.localPrepared && f.engine.getLastError() === null,
        "successful preparation retry duplicated listeners or retained replay error");
    await assertSyncGated(f);
    f.engine.stop();
}

async function repeatedStartAndLateHealthProbeDoNotDuplicateRuntime(): Promise<void> {
    const globals = globalThis as any;
    const oldWindow = globals.window;
    globals.window ??= globalThis;
    const f = fixture([]);
    let pulls = 0;
    let scans = 0;
    const health = deferred();
    f.engine.api.ping = async () => { await health.promise; return { ok: true }; };
    f.engine.pullRemote = async () => { pulls++; };
    f.engine.partialMtimeScan = async () => { scans++; };
    try {
        const first = startWithUiStubs(f.engine);
        const concurrent = f.engine.start();
        check(first === concurrent, "concurrent start did not share one startup operation");
        health.resolve();
        await first;
        const timer = f.engine.syncTimer;
        check(timer !== null && pulls === 1 && scans === 1, "activation failed to create one runtime");
        await f.engine.start();
        check(f.engine.syncTimer === timer && pulls === 1 && scans === 1 && f.registrations() === 4,
            "repeated start installed duplicate listeners/timers or replayed startup");
        f.engine.stop();

        const stopped = fixture([]);
        const lateHealth = deferred();
        const scanEntered = deferred();
        stopped.engine.api.ping = async () => { await lateHealth.promise; return { ok: true }; };
        stopped.engine.pullRemote = async () => {};
        stopped.engine.partialMtimeScan = async () => { scanEntered.resolve(); };
        const starting = startWithUiStubs(stopped.engine);
        await scanEntered.promise;
        stopped.engine.stop();
        lateHealth.resolve();
        await starting;
        check(stopped.engine.syncTimer === null && stopped.callbacks.size === 0,
            "late health response installed runtime after stop");
    } finally {
        f.engine.stop();
        if (oldWindow === undefined) delete globals.window;
        else globals.window = oldWindow;
    }
}

async function failedActivationRetriesWithoutReplayingCapture(): Promise<void> {
    const globals = globalThis as any;
    const oldWindow = globals.window;
    globals.window ??= globalThis;
    const f = fixture([]);
    let pulls = 0;
    let scans = 0;
    f.engine.api.ping = async () => ({ ok: true });
    f.engine.pullRemote = async () => { if (++pulls === 1) throw new Error("retryable startup pull failure"); };
    f.engine.partialMtimeScan = async () => { scans++; };
    try {
        let failed = false;
        try { await startWithUiStubs(f.engine); } catch { failed = true; }
        check(failed && f.engine.startupReplayInProgress && f.engine.syncTimer === null && f.callbacks.size === 4,
            "failed activation left runtime open or detached local capture");
        await f.engine.start();
        check(pulls === 2 && scans === 1 && f.replays() === 1 && f.registrations() === 4 && f.engine.syncTimer !== null,
            "startup retry repeated replay/listeners or did not activate one runtime");
        const timer = f.engine.syncTimer;
        await f.engine.start();
        check(f.engine.syncTimer === timer && pulls === 2, "successful activation retry was not idempotent");
    } finally {
        f.engine.stop();
        if (oldWindow === undefined) delete globals.window;
        else globals.window = oldWindow;
    }
}

async function replayPrecedesNetworkAndPreservesLiveEdits(): Promise<void> {
    const rows = Array.from({ length: 600 }, (_, index) => ({
        id: index + 1, action: "modified", path: index === 0 ? "same.md" : `old-${index}.md`, ts: 1, synced: false,
    })) as any[];
    rows.push({ id: 601, action: "deleted", path: "same.md", ts: 1, synced: false });
    rows.push({ id: 602, action: "renamed", oldPath: "from.md", path: "to.md", ts: 1, synced: false });
    const expectedPaths = new Set(rows.flatMap(row => row.oldPath ? [row.path, row.oldPath] : [row.path]));
    const f = fixture(rows);
    const pullEntered = deferred();
    const finishPull = deferred();
    const live: Promise<void>[] = [];
    let firstNetworkHints: any[] = [];
    let timerRan = false;
    let progressBatches = 0;
    let edited = false;
    f.engine.api.ping = async () => {
        firstNetworkHints = f.engine.pendingChanges.take();
        f.engine.pendingChanges.restore(firstNetworkHints);
        throw new Error("offline ping");
    };
    f.engine.pullRemote = async () => { pullEntered.resolve(); await finishPull.promise; };
    f.engine.onStatusUpdate = (status: string) => {
        if (!status.startsWith("⟳ journal")) return;
        progressBatches++;
        check(firstNetworkHints.length === 0, "network began before bounded replay finished");
        if (edited) return;
        edited = true;
        check(f.engine.isBusy(), "in-progress replay appeared idle to runtime probes");
        live.push(f.engine.fullScan(), f.engine.partialMtimeScan());
        const file = Object.assign(new TFile(), { path: "same.md", stat: { mtime: 7, size: 9 } });
        live.push(f.callbacks.get("modify")!(file));
    };
    const timer = setTimeout(() => { timerRan = true; }, 0);
    const starting = startWithUiStubs(f.engine);
    await pullEntered.promise;
    await Promise.all(live);
    clearTimeout(timer);
    check(f.callbacks.size === 4, "startup replay ran before vault listeners were attached");
    check(timerRan && progressBatches === 3, "large journal replay did not yield bounded UI tasks/progress");
    check(firstNetworkHints.length === expectedPaths.size &&
        firstNetworkHints.every(hint => expectedPaths.has(hint.path)), "first network await saw incomplete local replay");
    const same = firstNetworkHints.find(hint => hint.path === "same.md");
    check(same?.journalId === 1001 && same.action === "modified" && same.mtime === 7 && same.size === 9,
        "older replay overwrote a live event completed during its yields");
    check(firstNetworkHints.find(hint => hint.path === "from.md")?.action === "deleted" &&
        firstNetworkHints.find(hint => hint.path === "to.md")?.journalId === 602,
        "metadata-only replay lost linked rename halves/generation");
    check(f.nativeReads() === 0 && f.network() === 0, "replay or live debounce started native read/network publication");
    check(rows.length === 603, "local replay acknowledged or rewrote durable rows");
    check(!f.engine.startupReplayInProgress, "completed local replay left network gate stuck");
    // A genuinely slow/offline first pull cannot prevent prior local recovery.
    f.engine.stopped = true;
    finishPull.resolve();
    await starting;
    check(f.engine.pendingChanges.has("same.md"), "offline startup completion discarded recovered edit");
}

async function replayFailureKeepsListenersButGatesNetwork(): Promise<void> {
    const f = fixture([]);
    f.engine.journal.unsyncedCount = () => { throw new Error("unreadable journal metadata"); };
    let failed = false;
    try { await startWithUiStubs(f.engine); } catch { failed = true; }
    check(failed && f.callbacks.size === 4, "failed local replay detached/lost event capture");
    check(f.engine.startupReplayInProgress && f.engine.getLastError()?.origin === "journal-replay",
        "failed local replay cleared its safety gate without diagnostic");
    check(f.engine.isBusy(), "failed replay appeared idle to runtime probes");
    f.engine.bulkChangeReviewRequired = true;
    await f.engine.forceSync();
    await f.engine.pullRemote();
    await f.engine.pushPending();
    await f.engine.reconcileContent();
    await f.engine.fullScan();
    await f.engine.partialMtimeScan();
    check(f.network() === 0, "manual/debounced cycle bypassed failed local replay");
    check(f.engine.bulkChangeReviewRequired && f.engine.treeBaseRoot === "verified-base" && f.nativeReads() === 0,
        "rescan bypassed failed replay and changed approval/tree or read the vault");
    const file = Object.assign(new TFile(), { path: "live.md", stat: { size: 1, mtime: 1 } });
    await f.callbacks.get("create")!(file);
    check(f.engine.pendingChanges.has("live.md") && f.network() === 0,
        "gated startup stopped preserving new local edits or published them prematurely");
}

async function preparedStorageWaitCapturesEditsAndScopesNegotiatedTree(): Promise<void> {
    const rows = [{ id: 1, action: "modified", path: "offline.md", ts: 1, synced: false }];
    const f = fixture(rows);
    const loading = deferred();
    const finishLoad = deferred();
    const scoping = deferred();
    const finishScope = deferred();
    const negotiation = deferred();
    let version = 1;
    let loads = 0;
    let scopes = 0;
    let pulls = 0;
    f.engine.tree = { tree_version: () => version };
    f.engine.preparedTransfers = {
        plan: { load: async () => {
            loads++;
            check(f.callbacks.size === 4 && f.engine.pendingChanges.has("offline.md"),
                "plan replay started before local listeners and journal protection");
            loading.resolve();
            await finishLoad.promise;
        } },
        scopeForTree: async (actual: number) => {
            scopes++;
            check(actual === 2, "prepared scope used the pre-negotiation tree format");
            scoping.resolve();
            await finishScope.promise;
            return "a".repeat(64);
        },
    };
    f.engine.pullRemote = async () => {
        pulls++;
        check(f.engine.preparedScopeHash === "a".repeat(64), "network started before plan scope was ready");
        f.engine.reenrollmentRequired = true;
    };
    const activating = withUiStubs(() => activatePreparedSync({ engine: f.engine,
        negotiation: negotiation.promise, isCurrent: () => true, selectTree: () => { version = 2; } }));
    await loading.promise;
    await assertSyncGated(f);
    await f.callbacks.get("modify")!(Object.assign(new TFile(), {
        path: "during-plan-load.md", stat: { size: 3, mtime: 4 },
    }));
    check(rows.length === 2 && f.engine.pendingChanges.has("during-plan-load.md"),
        "slow plan storage stopped journaling local edits");
    finishLoad.resolve();
    await f.engine.prepareLocal();
    check(scopes === 0 && pulls === 0, "scope or network bypassed tree negotiation");
    negotiation.resolve();
    await scoping.promise;
    await assertSyncGated(f);
    finishScope.resolve();
    await activating;
    check(loads === 1 && scopes === 1 && pulls === 1, "plan preparation/activation duplicated work");
    f.engine.stop();
}

async function failedPlanLoadAndLateScopeStayGated(): Promise<void> {
    const failed = fixture([]);
    let unavailable = true;
    failed.engine.preparedTransfers = { plan: { load: async () => {
        if (unavailable) throw new Error("prepared storage unavailable");
    } }, scopeForTree: async () => "b".repeat(64) };
    let rejected = false;
    try { await withUiStubs(() => failed.engine.prepareLocal()); } catch { rejected = true; }
    check(rejected && failed.engine.getLastError()?.origin === "prepared-plan" && failed.callbacks.size === 4,
        "failed plan load lost capture or fabricated a journal error");
    await assertSyncGated(failed);
    unavailable = false;
    await failed.engine.prepareLocal();
    check(failed.engine.getLastError() === null && failed.registrations() === 4,
        "validated plan retry duplicated listeners or retained its old error");
    failed.engine.stop();

    const stopped = fixture([]);
    const scoping = deferred();
    const finishScope = deferred();
    stopped.engine.tree = { tree_version: () => 1 };
    stopped.engine.preparedTransfers = { plan: { load: async () => {} }, scopeForTree: async () => {
        scoping.resolve(); await finishScope.promise; return "c".repeat(64);
    } };
    const starting = startWithUiStubs(stopped.engine).then(() => false, () => true);
    await scoping.promise;
    stopped.engine.stop();
    finishScope.resolve();
    await starting;
    check(stopped.network() === 0 && stopped.engine.preparedScopeHash === null && stopped.engine.isStopped(),
        "late preparation scope activated a stopped engine");
}

const watchdog = setTimeout(() => { throw new Error("startup replay test did not settle"); }, 10_000);
void replayPrecedesNetworkAndPreservesLiveEdits()
    .then(replayFailureKeepsListenersButGatesNetwork)
    .then(pluginWaitKeepsLocalCaptureButNotSync)
    .then(stalePluginGenerationCannotActivate)
    .then(staleActivationKeepsCaptureForReplacement)
    .then(stopDuringPreparationCannotActivate)
    .then(partialListenerFailureRetriesCleanly)
    .then(failedPreparationCanRetryWithoutDuplicateListeners)
    .then(repeatedStartAndLateHealthProbeDoNotDuplicateRuntime)
    .then(failedActivationRetriesWithoutReplayingCapture)
    .then(preparedStorageWaitCapturesEditsAndScopesNegotiatedTree)
    .then(failedPlanLoadAndLateScopeStayGated)
    .then(() => console.log(`startup-replay.test: ${assertions} assertions passed`))
    .catch(error => { console.error(error); process.exitCode = 1; })
    .finally(() => clearTimeout(watchdog));
