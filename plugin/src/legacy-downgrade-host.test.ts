import { strict as assert } from "node:assert";
import {
    LegacyDowngradeHostCoordinator,
    LegacyDowngradeHostError,
    type LegacyDowngradeActivationPorts,
    type LegacyDowngradeStoppedHost,
} from "./legacy-downgrade-host";

function deferred<T = void>() {
    let resolve!: (value: T | PromiseLike<T>) => void, reject!: (error: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
const turns = async () => { for (let index = 0; index < 8; index++) await Promise.resolve(); };
function host() {
    let stopped = true, retires = 0;
    const owner: LegacyDowngradeStoppedHost = {
        assertStopped() { assert.equal(stopped, true); },
        async retire() { if (!stopped) throw new Error("double retirement"); stopped = false; retires++; },
    };
    return { owner, retires: () => retires };
}

function retryingHost(failures: number) {
    let stopped = true, retires = 0;
    const owner: LegacyDowngradeStoppedHost = {
        assertStopped() { assert.equal(stopped, true); },
        async retire() {
            retires++;
            if (retires <= failures) throw new Error(`retirement failure ${retires}`);
            if (!stopped) throw new Error("double successful retirement");
            stopped = false;
        },
    };
    return { owner, retires: () => retires };
}

async function transitionsAndSynchronousFences(): Promise<void> {
    const coordinator = new LegacyDowngradeHostCoordinator();
    const stale = coordinator.authorize("sync");
    const stopped = host(), quiesce = deferred(), activate = deferred(), handoff = deferred();
    const ports: LegacyDowngradeActivationPorts = {
        async quiesce(context) { await quiesce.promise; context.assertCurrent(); return stopped.owner; },
        async activate(context) { await activate.promise; context.assertCurrent(); },
        async handoff(context) { await handoff.promise; context.assertCurrent(); },
    };
    const preparing = coordinator.prepare(ports);
    assert.deepEqual(coordinator.snapshot(), { state: "quiescing", generation: 2,
        operation: "prepare", stopped: false, unloaded: false });
    assert.throws(() => coordinator.prepare(ports), (error: any) =>
        error instanceof LegacyDowngradeHostError && error.code === "BUSY");
    let staleActionRan = false;
    assert.throws(() => { stale.assertCurrent(); staleActionRan = true; }, (error: any) =>
        error instanceof LegacyDowngradeHostError && error.code === "STALE");
    assert.equal(staleActionRan, false, "stale sync action crossed its synchronous pre-await fence");

    quiesce.resolve(); await turns();
    assert.equal(coordinator.snapshot().state, "activating");
    activate.resolve(); await turns();
    assert.equal(coordinator.snapshot().state, "legacy-handoff");
    handoff.resolve(); await preparing;
    assert.deepEqual(coordinator.snapshot(), { state: "active-awaiting-import", generation: 2,
        operation: null, stopped: true, unloaded: false });
    assert.throws(() => coordinator.authorize("settings"), (error: any) =>
        error instanceof LegacyDowngradeHostError && error.code === "STATE");
    assert.equal(stopped.retires(), 0, "activation success restarted/retired the stopped host");

    let imports = 0;
    await coordinator.importLegacy({
        async quiesce() { throw new Error("already-stopped host was quiesced twice"); },
        async importLegacy(context) {
            assert.equal(coordinator.snapshot().state, "import-authorized");
            context.assertCurrent(); imports++;
        },
    });
    assert.equal(imports, 1); assert.equal(stopped.retires(), 1);
    assert.equal(coordinator.snapshot().state, "current-active");
    coordinator.authorize("sync").assertCurrent();
}

async function failureResumeAndUnloadJoin(): Promise<void> {
    const preflight = new LegacyDowngradeHostCoordinator();
    await assert.rejects(preflight.prepare({
        async quiesce() { throw new Error("freeze refused"); },
        async activate() {}, async handoff() {},
    }), /freeze refused/);
    assert.equal(preflight.snapshot().state, "none",
        "a refusal before stopped-host ownership invented an interrupted durable transition");

    const coordinator = new LegacyDowngradeHostCoordinator();
    const stopped = host();
    let quiesces = 0;
    await assert.rejects(coordinator.prepare({
        async quiesce() { quiesces++; return stopped.owner; },
        async activate() { throw new Error("activation interrupted"); },
        async handoff() { throw new Error("unreachable"); },
    }), /activation interrupted/);
    assert.equal(coordinator.snapshot().state, "interrupted");
    await coordinator.resume({
        async quiesce() { throw new Error("resume discarded the held stopped owner"); },
        async activate() {}, async handoff() {},
    });
    assert.equal(quiesces, 1); assert.equal(coordinator.snapshot().state, "active-awaiting-import");

    const joining = new LegacyDowngradeHostCoordinator("interrupted");
    const second = host(), activation = deferred();
    let handoffs = 0;
    const running = joining.resume({
        async quiesce() { return second.owner; },
        async activate() { await activation.promise; },
        async handoff() { handoffs++; },
    });
    await turns();
    const unloaded = joining.unload();
    assert.equal(unloaded, joining.unload(), "repeated unload did not share its retirement tail");
    let joined = false; void unloaded.then(() => { joined = true; }); await turns();
    assert.equal(joined, false, "unload returned before the admitted activation tail");
    activation.resolve();
    await assert.rejects(running, (error: any) =>
        error instanceof LegacyDowngradeHostError && error.code === "RETIRED");
    await unloaded;
    assert.equal(handoffs, 0); assert.equal(second.retires(), 1);
    assert.deepEqual(joining.snapshot(), { state: "retired", generation: 3,
        operation: null, stopped: false, unloaded: true });

    const immediate = new LegacyDowngradeHostCoordinator();
    let quiesceCalls = 0;
    const abandoned = immediate.prepare({
        async quiesce() { quiesceCalls++; return host().owner; },
        async activate() {}, async handoff() {},
    });
    const immediateUnload = immediate.unload();
    await assert.rejects(abandoned, (error: any) =>
        error instanceof LegacyDowngradeHostError && error.code === "RETIRED");
    await immediateUnload;
    assert.equal(quiesceCalls, 0, "synchronous unload admitted a not-yet-started quiesce callback");

    const importRecovery = new LegacyDowngradeHostCoordinator("import-authorized");
    const importHost = host();
    await assert.rejects(importRecovery.importLegacy({
        async quiesce() { return importHost.owner; },
        async importLegacy() { throw new Error("import interrupted"); },
    }), /import interrupted/);
    assert.equal(importRecovery.snapshot().state, "import-authorized",
        "authorized import recovery was downgraded to an ambiguous phase");
    await importRecovery.importLegacy({
        async quiesce() { throw new Error("retry discarded its stopped owner"); },
        async importLegacy() {},
    });
    assert.equal(importRecovery.snapshot().state, "current-active");
    importRecovery.authorize("init").assertCurrent();

    const completedImport = new LegacyDowngradeHostCoordinator("current-active");
    const currentSync = completedImport.authorize("sync");
    currentSync.assertCurrent();
    assert.throws(() => completedImport.prepare({
        async quiesce() { return host().owner; }, async activate() {}, async handoff() {},
    }), (error: any) => error instanceof LegacyDowngradeHostError && error.code === "STATE",
    "completed marker chain admitted an unsupported repeat downgrade");
}

async function retirementFailureRetainsExactOwner(): Promise<void> {
    const importing = new LegacyDowngradeHostCoordinator("active-awaiting-import");
    const importHost = retryingHost(1);
    let imports = 0;
    await assert.rejects(importing.importLegacy({
        async quiesce() { return importHost.owner; },
        async importLegacy() { imports++; },
    }), /retirement failure 1/);
    assert.deepEqual(importing.snapshot(), { state: "active-awaiting-import", generation: 2,
        operation: null, stopped: true, unloaded: false });
    await importing.importLegacy({
        async quiesce() { throw new Error("retirement retry replaced the exact stopped owner"); },
        async importLegacy() { imports++; },
    });
    assert.equal(imports, 2, "retry did not replay the idempotent import boundary");
    assert.equal(importHost.retires(), 2, "failed retirement was not retried exactly once");
    assert.equal(importing.snapshot().state, "current-active");

    const unloading = new LegacyDowngradeHostCoordinator();
    const unloadHost = retryingHost(1);
    await assert.rejects(unloading.prepare({
        async quiesce() { return unloadHost.owner; },
        async activate() { throw new Error("retain stopped host"); },
        async handoff() {},
    }), /retain stopped host/);
    const first = unloading.unload();
    assert.equal(first, unloading.unload(), "concurrent unload did not share its failing retirement tail");
    await assert.rejects(first, /retirement failure 1/);
    assert.equal(unloading.snapshot().stopped, true, "failed unload retirement dropped stopped ownership");
    const retry = unloading.unload();
    assert.notEqual(retry, first, "settled rejection was cached instead of permitting exact-owner retry");
    await retry;
    assert.equal(retry, unloading.unload(), "successful retry was not shared idempotently");
    assert.equal(unloadHost.retires(), 2, "unload retried more than the one failed retirement");
    assert.equal(unloading.snapshot().stopped, false);

    const staleReturn = new LegacyDowngradeHostCoordinator();
    const returnedHost = retryingHost(1), quiesce = deferred();
    const abandoned = staleReturn.prepare({
        async quiesce() { await quiesce.promise; return returnedHost.owner; },
        async activate() {}, async handoff() {},
    });
    await turns();
    const joined = staleReturn.unload();
    quiesce.resolve();
    await assert.rejects(abandoned, /retirement failure 1/);
    await joined;
    assert.equal(returnedHost.retires(), 2,
        "stale quiesce return was lost after its first retirement rejection");
    assert.equal(staleReturn.snapshot().stopped, false);
}

async function run(): Promise<void> {
    await transitionsAndSynchronousFences();
    await failureResumeAndUnloadJoin();
    await retirementFailureRetainsExactOwner();
    console.log("legacy-downgrade-host.test: state/generation/ownership assertions passed");
}
void run().catch(error => { setTimeout(() => { throw error; }, 0); });
