import assert from "node:assert/strict";
import { setImmediate as nextTurn } from "node:timers/promises";
import { createContext, Script } from "node:vm";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";

// Exercise the actual plugin orchestration, not a copied implementation of it.
// Native engine/storage/worker/UI ports are explicit deterministic fixtures;
// engine settlement itself is covered by src/engine-lifecycle.test.ts. This is
// neither an Obsidian host simulation nor a device/native performance test.
// No generated WASM, network, vault reads or temporary files are required.
const root = fileURLToPath(new URL("../", import.meta.url));
const actualInputs = new Set([
    "src/main.ts", "src/startup-activation.ts", "src/runtime-retirement.ts",
    "src/settings-persistence.ts", "src/wasm-memory.ts", "src/resident-memory.ts",
    "src/root-tree-resident-admission.ts", "src/tree-output-settlement.ts",
    "src/tree-candidate-mutation-job.ts", "src/build-identity.ts",
    "src/build-identity-contract.ts",
    "src/legacy-downgrade-host.ts", "src/legacy-downgrade-modal.ts", "src/current-host-work.ts",
]);
const fixtures = new Map([
    ["obsidian", ["App", "Modal", "Plugin", "Notice", "Platform", "Setting", "apiVersion"]],
    ["./platform", ["createPlatformIO"]],
    ["./api", ["ObsetyncApi"]],
    ["./sync-base", ["ObsetyncSyncBase"]],
    ["./journal", ["ObsetyncJournal"]],
    ["./transfer-plan", ["PreparedTransferPlan", "MobilePreparedTransferPlan"]],
    ["./sync-memory-arbiter", ["SyncMemoryArbiter"]],
    ["./object-confirmation-store", ["ObjectConfirmationStore"]],
    ["./transfer-plan-scope", ["preparedScopeForTree"]],
    ["./root-stream-scope", ["captureRootStreamScope"]],
    ["./sync", ["ObsetyncSyncEngine"]],
    ["./root-intent", ["RootIntentStore"]],
    ["./legacy-downgrade", [
        "LegacyDowngradeError",
        "activateLegacyDowngrade",
        "authorizeLegacyDowngradeCurrentImport",
        "completeLegacyDowngradeCurrentImport",
        "inspectLegacyDowngradeState",
        "resumeLegacyDowngrade",
    ]],
    ["./settings", ["DEFAULT_SETTINGS", "ObsetyncSettingTab"]],
    ["./conflict-ui", ["ObsetyncConflictModal", "findConflicts"]],
    ["./debug-log", ["debugLog", "crashLog", "perfSpan"]],
    ["./operation-checkpoint", ["OperationCheckpoint"]],
    ["./ignore", ["migrateLegacyDefaultIgnorePatterns"]],
    ["./perf-trace", ["normalizePerfArchitecture", "perfTrace"]],
    ["./hash-runtime", ["configureHashTuning", "getHashTuning"]],
    ["./work-scheduler", ["disposeWorkScheduler", "workSchedulerSnapshot", "throwIfWorkAborted", "yieldWork"]],
    ["./transient-memory", ["configureTransientMemory", "transientMemorySnapshot", "closeTransientMemory"]],
    ["./wasm-runtime", ["createWasmLoader"]],
    ["./root-tree-repair", ["drainRootTreeRetirement"]],
    ["./tree-candidate-job", ["drainTreeReachabilityRetirement", "TREE_CANDIDATE_JOB_STEP_UNITS"]],
    ["./desktop-hash-workers", ["createDesktopHashWorkerPool"]],
    ["./browser-hash-workers", ["createBrowserHashWorkerPool"]],
    ["./browser-capability-probe", ["runBrowserCapabilityProbe"]],
    ["./browser-probe-protocol", ["PROBE_FIXTURE_HASH"]],
    ["./host-runtime", ["runtimeForHost"]],
    ["./debug-modal", ["ObsetyncDebugModal"]],
    ["./resource-governor", [
        "AdaptiveResourceGovernor",
        "ResourceVisibilityGate",
        "coveredResourceAxesForWindow",
    ]],
    ["./sync-status", ["SyncStatusPresenter"]],
]);
const wasmExports = ["WasmTree", "Hasher", "wasm_root_hash_from_bytes", "wasm_root_version_from_bytes"];
const bundled = await build({
    absWorkingDir: root, entryPoints: ["src/main.ts"], bundle: true,
    platform: "node", format: "cjs", target: "node20", write: false,
    metafile: true, logLevel: "warning",
    define: {
        __OBSETYNC_BUILD_SEMVER__: '"1.11.4"',
        __OBSETYNC_BUILD_GIT_COMMIT__: `"${"a".repeat(40)}"`,
        __OBSETYNC_BUILD_SOURCE_STATE__: '"clean"',
        __OBSETYNC_BUILD_MODE__: '"development"',
    },
    plugins: [{
        name: "explicit-main-lifecycle-ports",
        setup(builder) {
            builder.onResolve({ filter: /.*/ }, ({ path }) => {
                if (fixtures.has(path)) return { path, namespace: "main-fixture" };
                if (/^obsetync-(hash-worker|browser-probe|browser-hash-worker)-source$/.test(path)) {
                    return { path: "source", namespace: "main-fixture" };
                }
                if (/^\.\.\/wasm\/sync_core(?:_simd)?_bg\.wasm$/.test(path)) {
                    return { path: "binary", namespace: "main-fixture" };
                }
                if (/^\.\.\/wasm\/sync_core(?:_simd)?$/.test(path)) {
                    return { path: "wasm", namespace: "main-fixture" };
                }
                return undefined;
            });
            builder.onLoad({ filter: /.*/, namespace: "main-fixture" }, ({ path }) => {
                const fromPort = name => `export const ${name} = globalThis.__mainLifecyclePorts.${name};`;
                if (path === "source") return { contents: 'export default "fixture-only";', loader: "js" };
                if (path === "binary") return { contents: "export default new Uint8Array([0,97,115,109,1,0,0,0]);", loader: "js" };
                if (path === "wasm") return {
                    contents: `export default async function init() {}\n${wasmExports.map(fromPort).join("\n")}`,
                    loader: "js",
                };
                const names = fixtures.get(path);
                if (!names) throw new Error(`Unconfigured fixture module: ${path}`);
                return { contents: names.map(fromPort).join("\n"), loader: "js" };
            });
        },
    }],
});
for (const input of Object.keys(bundled.metafile.inputs)) {
    if (!input.startsWith("main-fixture:")) {
        assert(actualInputs.has(input), `Unexpected non-fixture dependency: ${input}`);
    }
}
for (const input of actualInputs) assert(input in bundled.metafile.inputs, `Actual source missing: ${input}`);
const bundle = bundled.outputFiles[0].text;
if (process.argv.includes("--check")) {
    console.log(JSON.stringify({ suite: "main-lifecycle", mode: "bundle-only", testsExecuted: 0,
        actualSources: [...actualInputs], fixtureModules: fixtures.size }));
    process.exit(0);
}
assert.equal(process.argv.length, 2, "Only --check is supported");

let assertions = 0;
function check(value, message) { assertions++; assert(value, message); }
function equal(actual, expected, message) { assertions++; assert.deepEqual(actual, expected, message); }
function deferred() {
    let resolve, reject;
    const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
function observe(promise) {
    const state = { done: false, error: null, promise };
    void promise.then(() => { state.done = true; }, error => { state.done = true; state.error = error; });
    return state;
}
function installRequestedOutputOwner(plugin, tree, bytes = 4096) {
    const admission = plugin.rootTreeResidentAdmission;
    const attempt = admission.reserve(tree, { peakBytes: bytes * 2, residentBytes: bytes });
    assert(attempt, "Fixture output allowance must fit");
    admission.ready(attempt);
    const prior = admission.publish(attempt);
    admission.releaseRetired(prior);
    assert.equal(admission.snapshot().residentBytes, bytes);
    return admission;
}
async function settleAll(values) {
    const results = await Promise.allSettled(values);
    for (const result of results) if (result.status === "rejected") throw result.reason;
}

// Explicit scheduler port for the actual mutation-retirement driver. Preserve
// abort reason identity and a real host turn; do not replace cleanup yields
// with resolved promises or inherit a stopped operation's signal implicitly.
function fixtureThrowIfWorkAborted(signal) {
    if (signal?.aborted) throw signal.reason ?? Object.assign(new Error("Scheduled work was aborted"), { name: "AbortError" });
}
async function fixtureYieldWork({ signal } = {}) {
    fixtureThrowIfWorkAborted(signal);
    try { await nextTurn(undefined, { signal }); }
    catch (error) { fixtureThrowIfWorkAborted(signal); throw error; }
    fixtureThrowIfWorkAborted(signal);
}

function fixture({ workers = false, mobile = false, legacyState = "none", enrolled = false } = {}) {
    const events = [], gates = new Map(), waiters = [], engines = [], pools = [], browserPools = [], trees = [], saves = [];
    const conflictModals = [];
    const intervals = new Set(); let intervalSequence = 0;
    const counts = new Map();
    const state = { wasmMode: workers ? "simd" : "scalar", failedConstruction: null,
        freeFailure: null, legacyState, freezeFailure: null, activationFailure: null,
        activationFailureState: "interrupted", rootPending: null, rootSequence: 7 };
    function record(name) {
        events.push(name);
        counts.set(name, (counts.get(name) ?? 0) + 1);
        for (let i = waiters.length - 1; i >= 0; i--) {
            if (waiters[i].name === name && counts.get(name) >= waiters[i].count) {
                waiters.splice(i, 1)[0].resolve();
            }
        }
    }
    function block(name) {
        assert(!gates.has(name), `Duplicate test gate: ${name}`);
        const gate = deferred();
        // A regression can skip a gate altogether; the test timeout still
        // fails, while a later explicit rejection is never unhandled.
        void gate.promise.catch(() => {});
        gates.set(name, gate);
        return gate;
    }
    function waitFor(name, count = 1) {
        if ((counts.get(name) ?? 0) >= count) return Promise.resolve();
        const waiter = deferred();
        waiters.push({ name, count, resolve: waiter.resolve });
        return waiter.promise;
    }
    async function step(name) {
        record(name);
        const gate = gates.get(name);
        if (gate) {
            gates.delete(name);
            await gate.promise;
        }
        record(`${name}:done`);
    }
    const count = name => counts.get(name) ?? 0;
    const absent = (name, message) => equal(count(name), 0, message ?? `${name} must not have started`);
    function before(first, second, message) {
        check(events.indexOf(first) >= 0 && events.indexOf(second) > events.indexOf(first),
            message ?? `${first} must precede ${second}`);
    }
    const settings = {
        enrolled, serverUrl: "http://synthetic.invalid", vaultId: "synthetic",
        deviceName: "fixture", deviceId: "fixture-device", serverBoxPub: "fixture-key",
        bearerToken: "fixture-token", wireVersion: "0x02", esPub: "fixture-ephemeral",
        esPubValidUntil: 0, lastOutgoingSeq: 0, ignorePatterns: [], resourceRecoveryHint: 0,
        syncIntervalMs: 30000, syncPriority: "oldest", syncObsidianConfig: false,
        autoSync: false, realtimeWs: false, sharePresence: false,
    };
    let pluginSequence = 0, cachedReads = 0;
    const vault = { adapter: {
        async exists() { return false; },
        async read() { throw new Error("unexpected adapter read"); },
        async write() { throw new Error("unexpected adapter write"); },
        async rename() { throw new Error("unexpected adapter rename"); },
        async remove() { throw new Error("unexpected adapter remove"); },
        async mkdir() {},
        async readBinary() { await step(`cached:${++cachedReads}`); return new ArrayBuffer(0); },
    } };
    const app = { vault, workspace: { onLayoutReady() { throw new Error("Unexpected automatic fixture initialization"); } } };
    class Plugin {
        constructor(appArgument) {
            this.app = appArgument;
            this.manifest = { id: "obsetync", version: "fixture" };
            this.fixtureId = ++pluginSequence;
        }
        async loadData() {
            await step(`plugin${this.fixtureId}:loadData`);
            return structuredClone(settings);
        }
        async saveData(value) {
            const id = saves.length + 1;
            // Keep the actual object handed to native IO: later mutations
            // expose an absent/late snapshot rather than being hidden here.
            saves.push(value);
            await step(`save:${id}`);
        }
        addSettingTab() {}
        addCommand() {}
        addStatusBarItem() { return { setText() {} }; }
        registerDomEvent() {}
    }
    class Modal {
        constructor(owner) {
            this.app = owner;
            this.titleEl = { setText() {} };
            this.contentEl = { createEl() {}, empty() {} };
        }
        open() { this.onOpen?.(); }
        close() { this.onClose?.(); }
    }
    class Setting {
        constructor() { this.buttonEl = { disabled: false }; }
        setName() { return this; }
        addText(callback) { callback({ onChange() {} }); return this; }
        addToggle(callback) { callback({ onChange() {} }); return this; }
        addButton(callback) {
            callback({ buttonEl: this.buttonEl, setButtonText() { return this; },
                setCta() { return this; }, onClick() { return this; } });
            return this;
        }
    }
    class Base {
        constructor(owner) { this.owner = owner; this.loads = 0; record("base:new"); }
        async load() { await step(`base:load:${++this.loads}`); }
        async closeAndDrain() { await step("base:maintenance-drained"); }
    }
    class Journal {
        constructor() { this.loads = 0; record("journal:new"); }
        async load() { await step(`journal:load:${++this.loads}`); }
        async closeAndDrain() { await step("journal:maintenance-drained"); }
    }
    class Plan {
        constructor() { record("plan:new"); }
        close() { record("plan:close"); }
        async closeAndDrain() { this.close(); }
    }
    class MobilePlan extends Plan {
        constructor(...args) { super(...args); record("mobile-plan:new"); }
    }
    class MemoryArbiter {
        constructor(options) { this.options = options; this.closed = false; record("arbiter:new"); }
        snapshot() { return { capacityBytes: this.options.capacityBytes, usedBytes: 0,
            peakUsedBytes: 0, queuedRequests: 0 }; }
        close() { if (!this.closed) { this.closed = true; record("arbiter:close"); } }
    }
    class Confirmations {
        constructor() { record("confirmations:new"); }
        close() { record("confirmations:close"); }
        async closeAndDrain() { this.close(); await step("confirmations:maintenance-drained"); }
    }
    class StatusPresenter {
        observeWindow() {}
        render(base) { return base; }
        diagnostics() { return { trackedRates: 0, activeRateSamples: 0 }; }
    }
    // Public lifetime contract only. Gates stand for actual owners; tests
    // never substitute a main method or inspect/replicate its generation logic.
    class Engine {
        constructor(...args) {
            this.id = `engine${engines.length + 1}`;
            this.base = args[3]; this.journal = args[4]; this.tree = args[6];
            this.rootIdentity = args[22];
            this.browserHash = args[24];
            this.stopped = false; this.owned = new Set(); this.drained = false;
            engines.push(this); record(`${this.id}:new`);
        }
        own(pending) {
            this.owned.add(pending);
            void pending.then(() => this.owned.delete(pending), () => this.owned.delete(pending));
            return pending;
        }
        quiesceAndDrain() {
            record(`${this.id}:quiesce`);
            this.stopped = true;
            if (!this.quiesce) {
                this.quiesce = settleAll([...this.owned, step(`${this.id}:native`)]).then(() => {
                    this.drained = true; record(`${this.id}:drained`);
                });
            }
            return this.quiesce;
        }
        stopAndDrain() { record(`${this.id}:stop`); return this.quiesceAndDrain(); }
        handoffCaptureTo(next) {
            assert(this.drained, "Fixture contract: handoff requires old operations drained");
            const pending = step(`${this.id}:handoff:${next.id}`);
            next.own(pending); // Public contract: the receiver owns the old callback cut.
            return pending;
        }
        reloadPersistence() {
            return this.own((async () => {
                await step(`${this.id}:reload`);
                await this.base.load();
                if (this.stopped) return;
                await this.journal.load();
            })());
        }
        prepareLocal() { return this.own(step(`${this.id}:prepare`)); }
        start() {
            assert(!this.stopped, "A stopped fixture engine must not activate");
            return this.own(step(`${this.id}:start`));
        }
        isStopped() { return this.stopped; }
        freezeForLegacyDowngrade() {
            record(`${this.id}:freeze`);
            if (state.freezeFailure) return Promise.reject(state.freezeFailure);
            this.stopped = true;
            let retired = false;
            return Promise.resolve({
                treeVersion: 1,
                rootIntents: { pending: () => null, lastSequence: state.rootSequence },
                assertHeld: () => { if (retired) throw new Error("fixture downgrade lease retired"); },
                revoke: () => { retired = true; record(`${this.id}:freeze-revoke`); },
                closeAndDrain: async () => {
                    if (retired) return;
                    retired = true;
                    await step(`${this.id}:freeze-close`);
                },
            });
        }
        isReenrollmentRequired() { return false; }
        isBulkChangeReviewRequired() { return false; }
        getActivePeerCount() { return 0; }
    }
    class Tree {
        constructor() { this.id = `tree${trees.length + 1}`; this.freed = false; trees.push(this); record(`${this.id}:new`); }
        set_tree_version() {
            assert(!this.freed, "Tree accessed after free"); record(`${this.id}:select`);
        }
        free() {
            assert(!this.freed, "Tree freed twice"); this.freed = true; record(`${this.id}:free`);
            if (state.freeFailure) throw state.freeFailure;
        }
    }
    class Hasher {
        update() {}
        finalize() { return "f".repeat(64); }
        free() { record("hasher:free"); }
    }
    class Pool {
        constructor() { this.id = `pool${pools.length + 1}`; this.workerCount = 1; pools.push(this); record(`${this.id}:new`); }
        stats() { return { wasmMode: "simd", limit: 1, capacity: 1 }; }
        setActiveWorkerLimit() {}
        closeAndDrainNative() {
            record(`${this.id}:close`);
            return this.drain ??= step(`${this.id}:native`);
        }
    }
    class BrowserPool {
        constructor() {
            this.id = `browser-pool${browserPools.length + 1}`;
            this.generation = browserPools.length + 1;
            this.closed = false; browserPools.push(this); record(`${this.id}:new`);
        }
        ready() { record(`${this.id}:ready`); return Promise.resolve(); }
        stats() { return { state: this.closed ? "closed" : "ready", wasmMode: "simd",
            active: 0, queued: 0, ownedBytes: 0, quarantinedPayloadBytes: 0,
            fallbackSafe: !this.closed, generation: this.generation }; }
        diagnostics() { return { qualified: true, unconfirmedReleaseBytes: 0,
            cleanupEvidence: "not-requested" }; }
        close() { if (!this.closed) { this.closed = true; record(`${this.id}:close`); } }
    }
    let apiSequence = 0, wasmLoads = 0;
    class Api {
        constructor(_url, _key, _token, transportState) {
            this.id = ++apiSequence; this.transportState = transportState; record(`api${this.id}:new`);
            this.baseUrl = _url;
        }
        async negotiateTreeVersion() {
            await step(`api${this.id}:negotiate`);
            return { currentVersion: 1, activation: "active", readyDevices: 1, enrolledDevices: 1 };
        }
        async getHistory() {
            await step(`api${this.id}:history`);
            return [];
        }
    }
    class ConflictModal {
        constructor() {
            this.id = `conflict${conflictModals.length + 1}`;
            this.active = new Set(); this.revoked = false;
            conflictModals.push(this); record(`${this.id}:new`);
        }
        open() { record(`${this.id}:open`); }
        own(pending) {
            if (this.revoked) throw new Error("fixture conflict modal is revoked");
            this.active.add(pending);
            void pending.finally(() => this.active.delete(pending)).catch(() => {});
            return pending;
        }
        beginIo(name) { return this.own(step(name)); }
        revoke() {
            if (this.revoked) return;
            this.revoked = true; record(`${this.id}:revoke`);
        }
        async closeAndDrain() {
            record(`${this.id}:drain`);
            const results = await Promise.allSettled([...this.active]);
            record(`${this.id}:drained`);
            const failure = results.find(result => result.status === "rejected");
            if (failure) throw failure.reason;
        }
    }
    let tuning = { runtime: mobile ? "mobile" : "desktop", hashConcurrency: 1, maxHashConcurrency: 1, readConcurrency: 1,
        networkConcurrency: 1, applyConcurrency: 1, feedBytes: 65536, maxBatchBytes: 1024 * 1024,
        transientBudgetBytes: 32 * 1024 * 1024, yieldBudgetMs: 8 };
    let perfProfile = { runtime: "desktop", architecture: "x64", wasmMode: "unavailable" };
    class Governor {
        constructor(environment) { this.environment = environment; }
        current() { return { family: "fixture", name: "fixture", tuning }; }
        snapshot() { return { inputMode: "active-windows" }; }
        recordSimdAvailability() {}
    }
    class Visibility { dispose() { record("visibility:dispose"); } }
    const noop = () => {};
    const ports = {
        App: class {}, Modal, Setting, Plugin, Notice: class { hide() {} },
        Platform: { isDesktopApp: !mobile, isMobileApp: mobile,
            isMobile: mobile, isIosApp: mobile, isAndroidApp: false }, apiVersion: "fixture",
        createPlatformIO: () => ({}), ObsetyncApi: Api, ObsetyncSyncBase: Base,
        ObsetyncJournal: Journal, PreparedTransferPlan: Plan, MobilePreparedTransferPlan: MobilePlan,
        SyncMemoryArbiter: MemoryArbiter,
        ObjectConfirmationStore: Confirmations,
        preparedScopeForTree: () => () => "fixture-scope",
        // Encoding/credential validation has its own actual-source suite. This
        // explicit port checks main's synchronous capture and owner wiring,
        // without importing the real crypto/runtime into an orchestration VM.
        captureRootStreamScope: (value, owner, endpoint) => {
            const captured = { vaultId: value.vaultId, deviceId: value.deviceId,
                bearerToken: value.bearerToken, serverUrl: value.serverUrl };
            record(`scope:${owner.id}:capture`);
            return { vaultId: captured.vaultId, deviceId: captured.deviceId,
                scopeHash: Promise.resolve(`fixture-root-scope-${owner.id}`),
                assertCurrent(current, currentOwner, currentEndpoint) {
                    record(`scope:${owner.id}:assert`);
                    assert.equal(currentOwner, owner, "Root scope keeps its captured API owner");
                    assert.equal(currentEndpoint, endpoint, "Root scope keeps its captured endpoint");
                    for (const key of Object.keys(captured)) assert.equal(current[key], captured[key]);
                } };
        },
        ObsetyncSyncEngine: Engine, DEFAULT_SETTINGS: settings, ObsetyncSettingTab: class {},
        RootIntentStore: class {
            constructor() { record("legacy-root:new"); }
            async load() { await step("legacy-root:load"); }
            pending() { return state.rootPending; }
            get lastSequence() { return state.rootSequence; }
            snapshot() { return { ready: true, closed: false, pending: state.rootPending !== null }; }
            async closeAndDrain() { record("legacy-root:close"); }
        },
        LegacyDowngradeError: class extends Error {},
        inspectLegacyDowngradeState: async () => { record("legacy:inspect"); return state.legacyState; },
        activateLegacyDowngrade: async () => {
            await step("legacy:activate");
            if (state.activationFailure) {
                state.legacyState = state.activationFailureState;
                throw state.activationFailure;
            }
            state.legacyState = "legacy-active";
        },
        resumeLegacyDowngrade: async () => { await step("legacy:resume"); state.legacyState = "legacy-active"; },
        authorizeLegacyDowngradeCurrentImport: async () => {
            await step("legacy:authorize-import"); state.legacyState = "current-import-authorized";
        },
        completeLegacyDowngradeCurrentImport: async () => {
            await step("legacy:complete-import"); state.legacyState = "current-import-complete";
        },
        ObsetyncConflictModal: ConflictModal, findConflicts: () => state.conflicts ?? [],
        debugLog: { install: () => record("debug:install"), uninstall: () => record("debug:uninstall") },
        crashLog: { install: () => record("crash:install"), uninstall: () => record("crash:uninstall") },
        perfSpan: () => noop,
        OperationCheckpoint: class { async initialize() { await step("checkpoint:initialize"); return null; } },
        migrateLegacyDefaultIgnorePatterns: value => value,
        normalizePerfArchitecture: () => "x64",
        perfTrace: { setVisible: noop, subscribeWindows: () => () => record("perf:unsubscribe"),
            activeSnapshots: () => [],
            getProfile: () => perfProfile, setProfile: value => { perfProfile = value; } },
        configureHashTuning: (value, callback) => { tuning = value; callback(value); return value; },
        getHashTuning: () => tuning,
        disposeWorkScheduler: () => record("scheduler:dispose"), workSchedulerSnapshot: () => ({}),
        throwIfWorkAborted: fixtureThrowIfWorkAborted, yieldWork: fixtureYieldWork,
        TREE_CANDIDATE_JOB_STEP_UNITS: 1,
        configureTransientMemory: noop, transientMemorySnapshot: () => ({}),
        closeTransientMemory: () => record("memory:close"),
        createWasmLoader: options => async () => {
            await step(`wasm:load:${++wasmLoads}`);
            return { mode: state.wasmMode, bytes: 8, exports: options[state.wasmMode].exports };
        },
        drainRootTreeRetirement: tree => step(`${tree.id}:retire`),
        drainTreeReachabilityRetirement: async tree => {
            assert(!tree.freed, "Reachability cleanup must receive its live wrapper");
            await step(`${tree.id}:reachability-retire`);
            assert(!tree.freed, "Tree was freed before reachability cleanup settled");
        },
        createDesktopHashWorkerPool: (_source, options) => {
            if (state.failedConstruction) {
                const cleanup = state.failedConstruction;
                state.failedConstruction = null;
                record("construction:failed"); options.onConstructionCleanup(cleanup);
                return null;
            }
            return new Pool();
        },
        createBrowserHashWorkerPool: () => {
            if (!mobile) throw new Error("Unexpected browser worker on desktop fixture");
            return new BrowserPool();
        },
        runBrowserCapabilityProbe: () => { throw new Error("Unexpected automatic browser probe"); },
        PROBE_FIXTURE_HASH: "fixture", runtimeForHost: () => mobile ? "mobile" : "desktop", ObsetyncDebugModal: class {},
        AdaptiveResourceGovernor: Governor, ResourceVisibilityGate: Visibility,
        coveredResourceAxesForWindow: () => [],
        SyncStatusPresenter: StatusPresenter,
        WasmTree: Tree, Hasher, wasm_root_hash_from_bytes: () => null, wasm_root_version_from_bytes: () => 1,
    };
    const context = createContext({ __mainLifecyclePorts: ports,
        console: { log: noop, warn: noop, error: (...values) => record(`console:error:${String(values[0])}`) },
        performance, process: { arch: "x64", platform: "linux" }, WebAssembly,
        TextEncoder, TextDecoder, AbortController, Uint8Array, ArrayBuffer,
        setInterval: () => { const handle = ++intervalSequence; intervals.add(handle); return handle; },
        clearInterval: handle => intervals.delete(handle),
    });
    function newPlugin() {
        // Re-evaluate the actual bundle: module-local registries would be lost.
        // Symbol.for + globalThis must coordinate these independent instances.
        const PluginClass = new Script(`(() => { const module = { exports: {} }; const exports = module.exports;\n${bundle}\nreturn module.exports.default; })()`,
            { filename: "main-lifecycle.bundle.cjs" }).runInContext(context);
        return new PluginClass(app);
    }
    async function loaded() { const plugin = newPlugin(); await plugin.onload(); return plugin; }
    async function started() { const plugin = await loaded(); await plugin.initSync(); return plugin; }
    async function unload(plugin) { plugin.onunload(); await plugin.retirement; }
    return { app, events, engines, pools, browserPools, conflictModals, trees, saves, intervals, state, block, waitFor, record, count,
        absent, before, newPlugin, loaded, started, unload };
}

const tests = [];
function test(name, run) { tests.push({ name, run }); }

test("scheduler fixture preserves abort identity and real host-task retirement yields", async () => {
    const stopped = new AbortController(), reason = new Error("fixture caller stopped"); stopped.abort(reason);
    assert.throws(() => fixtureThrowIfWorkAborted(stopped.signal), error => error === reason); assertions++;
    await assert.rejects(fixtureYieldWork({ signal: stopped.signal }), error => error === reason); assertions++;
    const active = new AbortController(), during = fixtureYieldWork({ signal: active.signal });
    active.abort(reason);
    await assert.rejects(during, error => error === reason); assertions++;
    const cleanup = observe(fixtureYieldWork());
    await Promise.resolve(); await Promise.resolve();
    equal(cleanup.done, false, "Uncancelled retirement yield is a host task, not a microtask");
    await cleanup.promise;
    equal(cleanup.error, null, "Caller stop does not contaminate signal-free retirement work");
});

test("status refresh owns one bounded interval and releases it on unload", async () => {
    const f = fixture(), plugin = await f.loaded();
    equal(f.intervals.size, 1, "Status rendering created a timer chain or duplicate interval");
    equal(f.count("arbiter:new"), 1, "Plugin did not create exactly one lifetime memory arbiter");
    await f.unload(plugin);
    equal(f.intervals.size, 0, "Plugin unload retained its status refresh interval");
    equal(f.count("arbiter:close"), 1, "Plugin did not close its lifetime memory arbiter exactly once");
    f.before("plan:close", "arbiter:close");
    for (const owner of ["base", "journal", "confirmations"]) {
        f.before(`${owner}:maintenance-drained:done`, "scheduler:dispose");
    }
    f.before("memory:close", "arbiter:close");
});

test("persistence replacement drains old maintenance owners before opening new stores", async () => {
    const f = fixture(), plugin = await f.loaded();
    const base = f.block("base:maintenance-drained");
    const journal = f.block("journal:maintenance-drained");
    const confirmations = f.block("confirmations:maintenance-drained");
    const replacing = observe(plugin.loadCurrentPersistence("desktop", true));
    await Promise.all([
        f.waitFor("base:maintenance-drained"),
        f.waitFor("journal:maintenance-drained"),
        f.waitFor("confirmations:maintenance-drained"),
    ]);
    equal(f.count("base:new"), 1, "Replacement opened a new base before retiring the old owner");
    equal(f.count("journal:new"), 1, "Replacement opened a new journal before retiring the old owner");
    equal(replacing.done, false, "Replacement ignored unsettled maintenance retirement");
    base.resolve(); journal.resolve(); confirmations.resolve();
    await replacing.promise;
    equal(f.count("base:new"), 2, "Replacement did not open one fresh base");
    equal(f.count("journal:new"), 2, "Replacement did not open one fresh journal");
    const replacementOpen = f.events.lastIndexOf("base:new");
    for (const owner of ["base", "journal", "confirmations"]) {
        check(f.events.indexOf(`${owner}:maintenance-drained:done`) < replacementOpen,
            `Replacement opened storage before ${owner} maintenance drained`);
    }
    await f.unload(plugin);
});

test("interrupted and legacy-active startup fence current stores and every network dispatch", async () => {
    for (const legacyState of ["interrupted", "legacy-active"]) {
        const f = fixture({ legacyState, enrolled: true }), plugin = await f.loaded();
        equal(f.count("legacy:inspect"), 1, `${legacyState} startup did not inspect durable authority exactly once`);
        f.absent("base:new", `${legacyState} startup opened the current sync-base`);
        f.absent("journal:new", `${legacyState} startup opened the current journal`);
        f.absent("api1:negotiate", `${legacyState} startup reached tree negotiation`);
        equal(f.engines.length, 0, `${legacyState} startup constructed a network-capable engine`);
        await assert.rejects(plugin.initSync(), /blocked by legacy downgrade state/); assertions++;
        f.absent("api1:negotiate", `${legacyState} accidental init crossed the synchronous host fence`);
        await f.unload(plugin);
    }
});

test("completed import validates root-intent authority before stores and before first network", async () => {
    const f = fixture({ legacyState: "current-import-complete" }), plugin = await f.loaded();
    equal(f.count("legacy-root:load"), 1, "Completed import skipped its durable root-intent validation");
    f.before("legacy-root:load:done", "base:new");
    f.before("legacy-root:close", "base:new", "Validated root owner was not retired before current stores opened");
    f.absent("api1:negotiate", "Local root-floor validation dispatched network work");
    await plugin.initSync();
    equal(f.count("api1:negotiate"), 0, "Validation-only API owner negotiated capabilities");
    equal(f.count("api2:negotiate"), 1, "Fresh current engine did not own the first network negotiation");
    f.before("legacy-root:load:done", "api2:negotiate");
    await f.unload(plugin);
});

test("explicit active import retires stopped and root owners before one fresh current engine", async () => {
    const f = fixture({ legacyState: "legacy-active", enrolled: true }), plugin = await f.loaded();
    plugin.confirmLegacyDowngrade = async action => {
        equal(action, "import", "Active startup requested the wrong explicit confirmation");
        return true;
    };
    await plugin.importLegacyDowngrade();
    equal(f.count("legacy:authorize-import"), 1, "Import authorization marker was not written exactly once");
    equal(f.count("legacy:complete-import"), 1, "Completed-current marker was not written exactly once");
    equal(f.count("legacy-root:close"), 1, "Import retained its root-intent owner past settlement");
    equal(f.engines.length, 1, "Explicit import did not launch exactly one fresh current engine");
    equal(f.count("engine1:start"), 1, "Fresh current engine did not activate exactly once");
    f.before("legacy-root:load:done", "legacy:authorize-import");
    f.before("legacy:authorize-import:done", "base:new");
    f.before("journal:load:1:done", "legacy:complete-import");
    f.before("legacy:complete-import:done", "legacy-root:close");
    f.before("legacy-root:close", "api2:negotiate", "Network started before stopped/root owners retired");
    await f.unload(plugin);
});

test("late activation failure reinspects its interrupted marker and remains fail-closed", async () => {
    const f = fixture(), plugin = await f.started();
    plugin.confirmLegacyDowngrade = async action => {
        equal(action, "prepare", "Prepare path requested the wrong explicit confirmation");
        return true;
    };
    const activation = f.block("legacy:activate");
    f.state.activationFailure = new Error("fixture activation failed after durable prepare");
    f.state.activationFailureState = "interrupted";
    const preparing = observe(plugin.prepareLegacyDowngrade());
    await f.waitFor("legacy:activate");
    equal(f.count("engine1:freeze"), 1, "Activation began without a stopped engine lease");
    check(f.engines[0].isStopped(), "Engine restarted while the durable transition was unresolved");
    activation.resolve();
    await assert.rejects(preparing.promise, /fixture activation failed/); assertions++;
    equal(f.state.legacyState, "interrupted", "Fixture did not expose the durable interrupted cut");
    equal(f.count("legacy:inspect"), 2, "Failure did not reconcile from durable markers");
    equal(f.count("engine1:freeze-close"), 1, "Failed transition leaked its stopped/root authority lease");
    await assert.rejects(plugin.initSync(), /blocked by legacy downgrade state interrupted/); assertions++;
    equal(f.engines.length, 1, "Interrupted recovery accidentally constructed a replacement engine");
    f.absent("api2:negotiate", "Interrupted recovery accidentally dispatched network work");
    await f.unload(plugin);
});

test("prepare closes current-host admission and joins an exact stale history tail before freeze", async () => {
    const f = fixture(), plugin = await f.started();
    plugin.confirmLegacyDowngrade = async action => {
        equal(action, "prepare", "History-tail handoff requested the wrong confirmation");
        return true;
    };
    const historyGate = f.block("api1:history");
    const history = observe(plugin.getRootHistory());
    await f.waitFor("api1:history");

    const preparing = observe(plugin.prepareLegacyDowngrade());
    await nextTurn();
    equal(preparing.done, false, "Prepare did not join the admitted history request");
    check(plugin.currentHostWork.snapshot().closed, "Prepare did not synchronously close current-host admission");
    await assert.rejects(plugin.getRootHistory(), /blocked by legacy downgrade state quiescing/); assertions++;
    equal(f.count("api1:history"), 1, "Closed admission dispatched a second history request");
    f.absent("engine1:freeze", "Engine froze before the admitted history tail settled");
    f.absent("legacy:activate", "Compatibility activation crossed an unsettled current-host tail");

    historyGate.resolve();
    await assert.rejects(history.promise, /Current host work lease is stale/); assertions++;
    await assert.rejects(preparing.promise, /current host work did not retire cleanly/); assertions++;
    f.absent("engine1:freeze", "A rejected stale history owner still allowed engine freeze");
    f.absent("legacy:activate", "A rejected stale history owner still allowed durable activation");
    equal(f.count("legacy:inspect"), 2, "Rejected host-tail retirement skipped durable reinspection");
    equal(f.state.legacyState, "none", "Pre-activation failure changed durable compatibility authority");

    await plugin.getRootHistory();
    equal(f.count("api1:history"), 2, "Durable none recovery did not reopen a fresh current-host owner");
    equal(f.engines.length, 1, "Recoverable pre-freeze failure replaced the running engine");
    await f.unload(plugin);
});

test("prepare revokes an open conflict modal and drains its IO before engine freeze", async () => {
    const f = fixture(), plugin = await f.started();
    f.state.conflicts = [{ path: "fixture-conflict", versions: [] }];
    await plugin.showConflicts();
    equal(f.conflictModals.length, 1, "Conflict command did not open exactly one owned modal");
    const modal = f.conflictModals[0];
    const ioGate = f.block("conflict1:io");
    const io = observe(modal.beginIo("conflict1:io"));
    await f.waitFor("conflict1:io");
    plugin.confirmLegacyDowngrade = async action => {
        equal(action, "prepare", "Conflict-tail handoff requested the wrong confirmation");
        return true;
    };

    const preparing = observe(plugin.prepareLegacyDowngrade());
    await f.waitFor("conflict1:revoke");
    check(modal.revoked, "Compatibility quiesce did not synchronously revoke the open modal");
    equal(preparing.done, false, "Prepare did not wait for conflict modal IO retirement");
    f.absent("engine1:freeze", "Engine froze before the conflict modal IO tail settled");

    ioGate.resolve();
    await io.promise;
    await preparing.promise;
    f.before("conflict1:revoke", "engine1:freeze");
    f.before("conflict1:drained", "engine1:freeze", "Engine froze before the modal reported a drained IO tail");
    equal(f.count("engine1:freeze"), 1, "Successful conflict drain did not freeze exactly once");
    equal(f.count("legacy:activate"), 1, "Successful conflict drain did not activate exactly once");
    await f.unload(plugin);
});

test("root identity is captured before WASM work and dispatched only by its current runtime generation", async () => {
    const f = fixture(), plugin = await f.loaded(), wasm = f.block("wasm:load:1");
    const pending = observe(plugin.initSync());
    await f.waitFor("wasm:load:1");
    f.before("scope:1:capture", "wasm:load:1");
    equal(f.engines.length, 0, "Scope capture precedes asynchronous runtime construction");
    wasm.resolve(); await pending.promise;
    const identity = f.engines[0].rootIdentity;
    equal(identity.vaultId, "synthetic", "Captured vault is passed to the actual engine constructor port");
    equal(identity.deviceId, "fixture-device", "Captured device is passed to the engine");
    equal(await identity.scopeHash, "fixture-root-scope-1", "Exact captured scope promise reaches the engine");
    const checksBefore = f.count("scope:1:assert");
    identity.assertApiScope();
    equal(f.count("scope:1:assert"), checksBefore + 1, "Current dispatch validates the captured credentials and owner");
    const originalToken = plugin.settings.bearerToken;
    plugin.settings.bearerToken = "replacement-token";
    assert.throws(() => identity.assertApiScope()); assertions++;
    plugin.settings.bearerToken = originalToken;
    await plugin.initSync();
    assert.throws(() => identity.assertApiScope(), /generation changed/); assertions++;
    const replacementChecks = f.count("scope:2:assert");
    f.engines[1].rootIdentity.assertApiScope();
    equal(f.count("scope:2:assert"), replacementChecks + 1, "Replacement dispatch uses its own captured owner");
    await f.unload(plugin);
    assert.throws(() => f.engines[1].rootIdentity.assertApiScope(), /generation changed/); assertions++;
});

test("settings scope changes across WASM, cache and negotiation never activate mixed-scope work", async () => {
    for (const boundary of ["wasm:load:1", "cached:1", "api1:negotiate"]) {
        const f = fixture(), plugin = await f.loaded(), held = f.block(boundary);
        const initializing = observe(plugin.initSync());
        await f.waitFor(boundary);
        if (boundary === "api1:negotiate") await f.waitFor("engine1:prepare:done");
        plugin.settings.vaultId = "another-vault";
        held.resolve();
        await Promise.allSettled([initializing.promise]);
        check(initializing.error, `Changed scope at ${boundary} is reported, not silently activated`);
        f.absent("engine1:start", `No ordinary startup pull can follow mixed scope at ${boundary}`);
        if (boundary !== "api1:negotiate") equal(f.engines.length, 0, "No mixed-scope engine is constructed");
        await f.unload(plugin);
        for (const tree of f.trees) check(tree.freed, "Rejected scope still retires its constructed tree");
    }
});

test("reinitialization joins engine and native worker owners before free, handoff, reload and activation", async () => {
    const f = fixture({ workers: true });
    const plugin = await f.started();
    const engine = f.block("engine1:native"), worker = f.block("pool1:native");
    const handoff = f.block("engine1:handoff:engine2"), reload = f.block("engine2:reload");
    const pending = observe(plugin.initSync());
    equal(f.count("engine1:quiesce"), 1, "Old operation admission closes synchronously");
    equal(f.count("pool1:close"), 1, "Independent native worker admission also closes synchronously");
    await nextTurn();
    f.absent("tree1:free"); f.absent("api2:new"); f.absent("base:load:2");
    engine.resolve(); await nextTurn();
    f.absent("tree1:free", "Engine completion alone cannot free the worker-owned tree");
    f.absent("api2:new");
    worker.resolve(); await f.waitFor("engine1:handoff:engine2");
    f.before("engine1:drained", "tree1:free"); f.before("pool1:native:done", "tree1:free");
    f.before("tree1:free", "tree2:new"); f.absent("engine2:reload"); f.absent("base:load:2");
    f.absent("engine2:prepare"); check(!pending.done, "Handoff callback cut remains owned");
    handoff.resolve(); await f.waitFor("engine2:reload");
    f.absent("base:load:2"); f.absent("engine2:start");
    reload.resolve(); await pending.promise;
    f.before("engine1:handoff:engine2:done", "engine2:reload");
    f.before("engine2:reload:done", "base:load:2");
    f.before("journal:load:2:done", "engine2:prepare");
    f.before("engine2:prepare:done", "engine2:start");
    await f.unload(plugin);
    equal(f.count("tree1:free"), 1, "Retired old tree is not freed again on unload");
    equal(f.count("tree2:free"), 1, "Current tree is explicitly freed once");
});

test("mobile browser worker is plugin-lifetime, generation-fenced and closes after final drain", async () => {
    const f = fixture({ mobile: true }), plugin = await f.started();
    equal(f.count("mobile-plan:new"), 1, "Mobile startup did not select exactly one mobile prepared store");
    equal(f.count("plan:new"), 1, "Mobile startup stacked desktop and mobile prepared stores");
    equal(f.browserPools.length, 1, "Mobile startup constructs one qualified browser pool");
    check(f.engines[0].browserHash?.pool === f.browserPools[0],
        "Qualified pool is explicitly passed to the engine");
    const oldFence = f.engines[0].browserHash.assertCurrent;
    const drain = f.block("engine1:native");
    const restarting = observe(plugin.initSync());
    await nextTurn();
    f.absent("browser-pool1:close", "Re-init must not retire an unacknowledged browser heap");
    drain.resolve();
    await restarting.promise;
    equal(f.browserPools.length, 1, "Re-init stacked a second browser/WASM heap");
    equal(f.count("browser-pool1:close"), 0, "Plugin-lifetime pool churned during re-init");
    check(f.engines[1].browserHash?.pool === f.browserPools[0],
        "Replacement engine did not reuse the qualified plugin-lifetime pool");
    assert.throws(oldFence, /generation changed/); assertions++;

    const unloadDrain = f.block("engine2:native");
    plugin.onunload();
    await nextTurn();
    equal(f.count("browser-pool1:close"), 0,
        "Unload did not retire browser pool before accepted engine work");
    unloadDrain.resolve();
    await plugin.retirement;
    f.before("engine2:drained", "browser-pool1:close");
    equal(f.count("browser-pool1:close"), 1, "Unload retires browser pool exactly once");
});

test("third generation joins an inherited handoff cut and never activates the stale continuation", async () => {
    const f = fixture(), plugin = await f.started();
    const handoff = f.block("engine1:handoff:engine2");
    const second = observe(plugin.initSync());
    await f.waitFor("engine1:handoff:engine2");
    const third = observe(plugin.initSync());
    await nextTurn();
    equal(f.count("engine2:quiesce"), 1, "Third init quiesces the receiver of the outstanding handoff");
    f.absent("api3:new"); f.absent("tree2:free");
    check(!third.done, "Third generation cannot bypass inherited native callback ownership");
    handoff.resolve(); await Promise.all([second.promise, third.promise]);
    f.absent("engine2:reload"); f.absent("engine2:prepare"); f.absent("engine2:start");
    equal(f.count("engine3:start"), 1, "Only the latest generation activates");
    f.before("engine1:handoff:engine2:done", "tree2:free");
    f.before("engine2:handoff:engine3:done", "engine3:reload");
    await f.unload(plugin);
});

test("third generation joins a receiver's actual persistence reload before touching shared storage", async () => {
    const f = fixture(), plugin = await f.started();
    const base = f.block("base:load:2");
    const second = observe(plugin.initSync());
    await f.waitFor("base:load:2");
    const third = observe(plugin.initSync());
    await nextTurn();
    f.absent("api3:new"); f.absent("tree2:free"); f.absent("journal:load:2");
    base.resolve(); await Promise.all([second.promise, third.promise]);
    f.absent("engine2:prepare"); f.absent("engine2:start");
    f.before("base:load:2:done", "engine3:new");
    equal(f.count("engine3:start"), 1, "Newest runtime activates only after owned persistence replay");
    await f.unload(plugin);
});

test("unload stops admission immediately and a separately evaluated bundle waits before logs or storage", async () => {
    const f = fixture({ workers: true }), first = await f.started();
    const engine = f.block("engine1:native"), worker = f.block("pool1:native");
    first.onunload();
    equal(f.count("engine1:stop"), 1, "Unload synchronously closes engine admission");
    equal(f.count("pool1:close"), 1, "Unload synchronously starts native worker drain");
    equal(f.count("perf:unsubscribe"), 1, "New controller input is detached synchronously");
    const second = f.newPlugin(), loading = observe(second.onload());
    await nextTurn();
    equal(f.count("debug:install"), 1, "Replacement must not overwrite the old logger");
    f.absent("plugin2:loadData"); f.absent("tree1:free"); f.absent("plan:close");
    f.absent("scheduler:dispose"); f.absent("memory:close");
    engine.resolve(); await nextTurn();
    check(!loading.done, "One completed owner cannot release the same-vault retirement registry");
    f.absent("tree1:free");
    worker.resolve(); await Promise.all([first.retirement, loading.promise]);
    f.before("pool1:native:done", "tree1:free");
    f.before("tree1:free", "plan:close"); f.before("memory:close", "plugin2:loadData");
    const uninstall = f.events.indexOf("debug:uninstall"), installs = f.events.reduce((out, value, i) => {
        if (value === "debug:install") out.push(i); return out;
    }, []);
    check(installs[1] > uninstall, "Cross-bundle logger installation follows prior cleanup");
    first.onunload();
    equal(f.count("tree1:free"), 1, "Repeated unload is idempotent");
    await f.unload(second);
});

test("failed owner still joins its sibling and retains a fail-closed cross-bundle retirement", async () => {
    const f = fixture({ workers: true }), first = await f.started();
    const engine = f.block("engine1:native"), worker = f.block("pool1:native");
    const failure = new Error("fixture native drain failure");
    first.onunload();
    const retirement = observe(first.retirement), second = f.newPlugin(), loading = observe(second.onload());
    engine.reject(failure); await nextTurn();
    check(!retirement.done && !loading.done, "Rejected owner does not short-circuit another native owner");
    f.absent("tree1:free"); f.absent("memory:close"); f.absent("plugin2:loadData");
    worker.resolve(); await Promise.allSettled([retirement.promise, loading.promise]);
    equal(retirement.error, failure, "Retirement communicates the real owner failure");
    equal(loading.error, failure, "Replacement cannot open shared state after uncertain retirement");
    f.absent("tree1:free"); f.absent("plan:close");
    const third = f.newPlugin(), again = observe(third.onload());
    await Promise.allSettled([again.promise]);
    equal(again.error, failure, "Failure remains registered, not consumed by one waiting bundle");
    f.absent("plugin3:loadData");
});

test("unload during initial native storage load joins onload and fences later journal construction", async () => {
    const f = fixture(), base = f.block("base:load:1"), first = f.newPlugin();
    const loading = observe(first.onload());
    await f.waitFor("base:load:1");
    first.onunload();
    const retirement = observe(first.retirement), second = f.newPlugin(), next = observe(second.onload());
    await nextTurn();
    check(!retirement.done && !next.done, "Native onload work belongs to the retiring runtime");
    f.absent("journal:new"); f.absent("debug:uninstall"); f.absent("plugin2:loadData");
    base.resolve(); await Promise.all([loading.promise, retirement.promise, next.promise]);
    f.before("base:load:1:done", "debug:uninstall");
    f.before("debug:uninstall", "plugin2:loadData");
    equal(f.count("journal:new"), 1, "Only the second plugin constructs a journal");
    equal(f.engines.length, 0, "Unloaded onload continuation never initializes sync");
    await f.unload(second);
});

test("unloading a replacement waiting on prior retirement does not create a registry self-cycle", async () => {
    const f = fixture(), first = await f.started();
    const engine = f.block("engine1:native");
    first.onunload();
    const second = f.newPlugin(), loading = observe(second.onload());
    second.onunload();
    const secondRetirement = observe(second.retirement);
    const third = f.newPlugin(), next = observe(third.onload());
    await nextTurn();
    f.absent("plugin2:loadData"); f.absent("plugin3:loadData");
    engine.resolve(); await Promise.all([first.retirement, loading.promise, secondRetirement.promise, next.promise]);
    f.absent("plugin2:loadData", "Already-unloaded waiter never starts storage/logging initialization");
    equal(f.count("plugin3:loadData"), 1, "A third bundle proceeds after both captured retirement cuts");
    await f.unload(third);
});

test("failed pool construction is owned even when no pool object is returned", async () => {
    const f = fixture({ workers: true }), plugin = await f.loaded();
    const construction = deferred(); f.state.failedConstruction = construction.promise;
    const first = observe(plugin.initSync());
    await f.waitFor("construction:failed");
    const second = observe(plugin.initSync());
    await nextTurn();
    f.absent("api2:new"); f.absent("tree1:new");
    check(!first.done && !second.done, "Both generations wait actual failed-constructor cleanup");
    construction.resolve(); await Promise.all([first.promise, second.promise]);
    equal(f.count("api2:new"), 1, "New generation starts after the orphan worker lifetime ends");
    equal(f.engines.length, 1, "Failed/stale constructor continuation never creates an engine");
    equal(f.count("engine1:start"), 1, "Current generation can use a fresh pool");
    await f.unload(plugin);
});

test("unload also waits failed construction cleanup before releasing services or same-vault registry", async () => {
    const f = fixture({ workers: true }), first = await f.loaded();
    const construction = deferred(); f.state.failedConstruction = construction.promise;
    const initializing = observe(first.initSync());
    await f.waitFor("construction:failed");
    first.onunload();
    const retirement = observe(first.retirement), second = f.newPlugin(), loading = observe(second.onload());
    await nextTurn();
    check(!retirement.done && !loading.done, "Constructor cleanup without a pool remains a native owner");
    f.absent("scheduler:dispose"); f.absent("plugin2:loadData");
    construction.resolve(); await Promise.all([initializing.promise, retirement.promise, loading.promise]);
    equal(f.engines.length, 0, "Constructor completion after unload does not install an engine");
    await f.unload(second);
});

test("actual settings writer snapshots and serializes native publications, coalesces pending and drains unload", async () => {
    const f = fixture(), first = await f.loaded();
    const firstWrite = f.block("save:1"), latestWrite = f.block("save:2");
    first.settings.ignorePatterns = ["first"];
    const one = observe(first.saveSettings());
    first.settings.ignorePatterns[0] = "second";
    const two = observe(first.saveSettings());
    first.settings.ignorePatterns[0] = "latest";
    const latest = observe(first.saveSettings());
    first.settings.ignorePatterns[0] = "not-saved";
    equal(f.saves.length, 1, "Only one native settings write is active");
    equal([...f.saves[0].ignorePatterns], ["first"], "Active native snapshot cannot observe caller mutation");
    first.onunload();
    const next = f.newPlugin(), loading = observe(next.onload());
    await first.saveSettings(); // Public API deliberately no-ops after unload.
    await nextTurn();
    check(!one.done && !two.done && !latest.done && !loading.done, "Unload owns both accepted publication slots");
    f.absent("debug:uninstall"); f.absent("plugin2:loadData");
    firstWrite.resolve(); await f.waitFor("save:2");
    equal([...f.saves[1].ignorePatterns], ["latest"], "Queued slot publishes its captured latest value");
    equal(f.saves.length, 2, "Superseded pending snapshot does not become a third native write");
    await nextTurn(); check(!loading.done && !two.done && !latest.done, "Starting the pending write is not drain completion");
    latestWrite.resolve(); await Promise.all([one.promise, two.promise, latest.promise, first.retirement, loading.promise]);
    f.before("save:2:done", "debug:uninstall");
    equal(f.saves.length, 2, "Post-unload save admits no work");
    await f.unload(next);
});

test("settings write rejection is reported but does not cancel the admitted latest publication or fake its drain", async () => {
    const f = fixture(), plugin = await f.loaded();
    const firstWrite = f.block("save:1"), nextWrite = f.block("save:2");
    const failure = new Error("fixture native settings failure");
    const first = observe(plugin.saveSettings());
    plugin.settings.lastOutgoingSeq = 2;
    const second = observe(plugin.saveSettings());
    plugin.onunload(); const retirement = observe(plugin.retirement);
    firstWrite.reject(failure); await f.waitFor("save:2"); await nextTurn();
    equal(first.error, failure, "Failed save caller receives actual native rejection");
    check(!retirement.done && !second.done, "Drain still waits the admitted next native publication");
    f.absent("debug:uninstall");
    nextWrite.resolve(); await Promise.all([second.promise, retirement.promise]);
    equal(f.saves[1].lastOutgoingSeq, 2, "Latest publication survives prior native failure");
});

test("stale cached-root construction never accesses its freed tree or activates after replacement", async () => {
    const f = fixture(), plugin = await f.loaded(), cached = f.block("cached:1");
    const first = observe(plugin.initSync());
    await f.waitFor("cached:1");
    const second = observe(plugin.initSync());
    await second.promise;
    equal(f.count("tree1:free"), 1, "Unpublished tree is retired by its successor");
    equal(f.engines.length, 1, "Only the latest cached-root continuation creates an engine");
    cached.resolve(); await first.promise;
    f.absent("tree1:select", "Stale native read completion must not use the freed tree");
    equal(f.engines.length, 1, "Stale completion does not construct an extra engine");
    await f.unload(plugin);
    equal(f.count("tree1:free"), 1, "Stale tree remains once-only retired");
});

test("reinitialization and unload share actual tree retirement before free and service teardown", async () => {
    const f = fixture(), plugin = await f.started(), cleanup = f.block("tree1:retire");
    const admission = installRequestedOutputOwner(plugin, f.trees[0]);
    const reinit = observe(plugin.initSync());
    await f.waitFor("tree1:retire");
    plugin.onunload();
    const retirement = observe(plugin.retirement);
    await nextTurn();
    equal(f.count("tree1:retire"), 1, "Concurrent callers share the same cleanup owner");
    f.absent("tree1:free"); f.absent("scheduler:dispose"); f.absent("memory:close");
    check(!reinit.done && !retirement.done, "Actual cleanup, not prior engine drain, gates release");
    cleanup.resolve(); await Promise.all([reinit.promise, retirement.promise]);
    f.before("tree1:retire:done", "tree1:free");
    f.before("tree1:free", "scheduler:dispose");
    equal(f.count("tree1:free"), 1, "Shared retirement frees exactly once");
    equal(admission.snapshot().ledger.usedBytes, 0,
        "Successful native free releases its scoped resident output owner");
    equal(f.trees.length, 1, "Reinit fenced during cleanup cannot construct a replacement");
});

test("failed tree cleanup keeps its wrapper and services available for a safe retry", async () => {
    const f = fixture(), plugin = await f.started(), cleanup = f.block("tree1:retire");
    const failure = new Error("fixture retirement failed");
    const reinit = observe(plugin.initSync());
    await f.waitFor("tree1:retire"); cleanup.reject(failure);
    await Promise.allSettled([reinit.promise]);
    equal(reinit.error, failure, "Caller receives the actual native cleanup failure");
    f.absent("tree1:free"); f.absent("memory:close");
    equal(f.trees.length, 1, "Failed cleanup cannot admit a replacement");
    await plugin.initSync();
    equal(f.count("tree1:retire"), 2, "Retry drains the still-owned wrapper");
    equal(f.count("tree1:free"), 1, "Successful retry permits one free");
    equal(f.trees.length, 2, "Only completed retirement admits replacement");
    await f.unload(plugin);
});

test("reachability cleanup gates shared reinit, unload, replacement cleanup and next-bundle teardown", async () => {
    const f = fixture({ workers: true }), first = await f.started();
    const reachability = f.block("tree1:reachability-retire"), replacement = f.block("tree1:retire");
    const reinit = observe(first.initSync());
    await f.waitFor("tree1:reachability-retire");
    first.onunload();
    const retirement = observe(first.retirement), second = f.newPlugin(), loading = observe(second.onload());
    await nextTurn();
    equal(f.count("tree1:reachability-retire"), 1, "Reinit and unload join one reachability cleanup owner");
    check(!reinit.done && !retirement.done && !loading.done, "Reachability drain holds all successor continuations");
    f.absent("tree1:retire", "Replacement cleanup cannot overtake reachability cleanup");
    f.absent("tree1:free"); f.absent("tree2:new"); f.absent("plugin2:loadData");
    for (const name of ["plan:close", "scheduler:dispose", "memory:close", "debug:uninstall", "crash:uninstall"])
        f.absent(name, `Reachability still needs live services: ${name}`);

    reachability.resolve(); await f.waitFor("tree1:retire");
    f.before("tree1:reachability-retire:done", "tree1:retire");
    check(!retirement.done && !loading.done, "Completed reachability does not bypass outstanding replacement cleanup");
    f.absent("tree1:free"); f.absent("scheduler:dispose"); f.absent("memory:close");
    replacement.resolve(); await Promise.all([reinit.promise, retirement.promise, loading.promise]);
    f.before("tree1:retire:done", "tree1:free");
    for (const name of ["plan:close", "scheduler:dispose", "memory:close", "debug:uninstall", "crash:uninstall"])
        f.before("tree1:free", name);
    f.before("memory:close", "plugin2:loadData");
    equal(f.count("tree1:free"), 1, "Joined reachability retirement permits exactly one native free");
    equal(f.trees.length, 1, "Unloaded reinit cannot construct a new native tree");
    await f.unload(second);
});

test("failed reachability cleanup retains its wrapper and services until explicit reinit retry settles", async () => {
    const f = fixture(), plugin = await f.started(), retainedTree = f.trees[0];
    const reachability = f.block("tree1:reachability-retire"), failure = new Error("fixture reachability cleanup failed");
    const reinit = observe(plugin.initSync());
    await f.waitFor("tree1:reachability-retire"); reachability.reject(failure);
    await Promise.allSettled([reinit.promise]);
    equal(reinit.error, failure, "Actual reachability cleanup rejection reaches the reinit caller");
    equal(plugin.tree, retainedTree, "Failed cleanup keeps the exact old wrapper installed");
    equal(retainedTree.freed, false, "Rejected reachability cleanup is not permission to free");
    f.absent("tree1:retire"); f.absent("api2:new");
    for (const name of ["tree1:free", "plan:close", "scheduler:dispose", "memory:close", "debug:uninstall"])
        f.absent(name);

    const retryCleanup = f.block("tree1:reachability-retire"), retry = observe(plugin.initSync());
    await f.waitFor("tree1:reachability-retire", 2);
    check(!retry.done, "Retry awaits actual cleanup instead of treating prior rejection as a drain");
    equal(plugin.tree, retainedTree, "Retry still owns the rejected wrapper");
    f.absent("tree1:retire"); f.absent("tree1:free"); f.absent("api2:new");
    retryCleanup.resolve(); await retry.promise;
    equal(f.count("tree1:reachability-retire"), 2, "Explicit retry re-enters the same cleanup port");
    equal(f.count("tree1:retire"), 1, "Only the successful reachability cut admits replacement cleanup");
    f.before("tree1:reachability-retire:done", "tree1:retire");
    f.before("tree1:retire:done", "tree1:free");
    equal(f.count("tree1:free"), 1, "Successful retry frees the retained wrapper exactly once");
    equal(f.trees.length, 2, "New runtime construction follows proven retirement");
    await f.unload(plugin);
});

test("unload reachability failure preserves same-vault cross-bundle rejection without releasing services", async () => {
    const f = fixture(), first = await f.started(), reachability = f.block("tree1:reachability-retire");
    const failure = new Error("fixture unload reachability cleanup failed");
    first.onunload();
    const retirement = observe(first.retirement), second = f.newPlugin(), loading = observe(second.onload());
    await f.waitFor("tree1:reachability-retire"); reachability.reject(failure);
    await Promise.allSettled([retirement.promise, loading.promise]);
    equal(retirement.error, failure, "Unload reports the actual reachability failure");
    equal(loading.error, failure, "Next bundle cannot start while the old cleanup is uncertain");
    equal(first.tree, f.trees[0], "Rejected unload retains the native wrapper");
    for (const name of ["tree1:retire", "tree1:free", "plan:close", "scheduler:dispose", "memory:close", "debug:uninstall", "plugin2:loadData"])
        f.absent(name);
    const third = f.newPlugin(), again = observe(third.onload());
    await Promise.allSettled([again.promise]);
    equal(again.error, failure, "A later bundle cannot consume or bypass the rejected cleanup cut");
    f.absent("plugin3:loadData");
    equal(f.count("tree1:reachability-retire"), 1, "Waiting bundles never retry cleanup behind the old owner's back");
});

test("throwing native tree free poisons reinitialization and retirement without retrying the destructor", async () => {
    const f = fixture(), plugin = await f.started();
    const admission = installRequestedOutputOwner(plugin, f.trees[0]);
    const failure = new Error("fixture tree free failure"); f.state.freeFailure = failure;
    const retry = observe(plugin.initSync()); await Promise.allSettled([retry.promise]);
    equal(retry.error, failure, "Destructor failure propagates to initialization");
    f.absent("api2:new");
    f.state.freeFailure = null;
    const again = observe(plugin.initSync()); await Promise.allSettled([again.promise]);
    check(again.error?.message.includes("runtime recovery"), "Later init remains fail-closed after unproved free");
    equal(f.count("tree1:free"), 1, "Throwing native destructor is never retried");
    equal(admission.snapshot().residentBytes, 4096,
        "Throwing native free retains the scoped resident output charge");
    plugin.onunload(); const retirement = observe(plugin.retirement);
    const next = f.newPlugin(), loading = observe(next.onload());
    await Promise.allSettled([retirement.promise, loading.promise]);
    check(retirement.error && loading.error, "Unload and later bundle retain the retirement poison");
    equal(admission.snapshot().residentBytes, 4096,
        "Closed failed bundle keeps quarantined resident ownership");
    f.absent("plugin2:loadData"); f.absent("plan:close");
});

const completed = [];
for (const { name, run } of tests) {
    let timer;
    try {
        await Promise.race([run(), new Promise((_, reject) => {
            timer = setTimeout(() => reject(new Error(`Deterministic gate did not settle: ${name}`)), 10000);
        })]);
        completed.push(name);
    } catch (error) {
        console.error(`main-lifecycle FAILED: ${name}`);
        throw error;
    } finally { clearTimeout(timer); }
}
console.log(JSON.stringify({ suite: "main-lifecycle", mode: "actual-bundled-main", testsExecuted: completed.length,
    assertions, actualSources: [...actualInputs], cases: completed,
    limitations: "Synthetic dependency ports; no real Obsidian/native WASM/worker/storage/network/device coverage" }));
