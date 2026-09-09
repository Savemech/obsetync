import { ObsetyncSyncEngine, type ReconcileResult } from "./sync";

let assertions = 0;
const check = (condition: unknown, message: string): void => {
    assertions++;
    if (!condition) throw new Error(message);
};
const complete: ReconcileResult = { smallUploaded: 0, largeUploaded: 0, treeChunksUploaded: 0,
    bytes: 0, deferred: 0, drifted: 0, readErrors: 0, uploadErrors: 0 };

/** Actual engine methods, fake fields only: no Obsidian constructor, adapter,
 * vault, journal mutation, or native host lifecycle is emulated here. */
function fixture() {
    const statuses: string[] = [];
    const engine = Object.create(ObsetyncSyncEngine.prototype) as any;
    Object.assign(engine, {
        stopped: false, syncing: false, reenrollmentRequired: false, bulkChangeReviewRequired: false,
        state: "idle", localRootHash: "same-observed-root", treeBaseRoot: "base",
        pendingChanges: { size: 0 }, deferredChanges: { summary: () => ({ count: 0 }) },
        journal: {
            unsynced: () => { throw new Error("status must not materialize the journal array"); },
            iterateUnsynced: () => { throw new Error("status must not iterate the journal"); },
            capturePendingPaths: () => { throw new Error("status must not capture pending paths"); },
            unsyncedCount: () => 0,
        }, syncBase: { entryCount: () => 1 },
        hashWorkerAbort: new AbortController(), lastError: null, lastRepairSummary: null,
        onStatusUpdate: (status: string) => statuses.push(status), pullRemote: async () => {},
        _reconcileInner: async () => ({ ...complete }),
    });
    let pushes = 0;
    engine.pushPending = async () => { pushes++; engine.state = "idle"; };
    return { engine, statuses, pushes: () => pushes };
}

async function incompleteRepairNeverClaimsCompletion(): Promise<void> {
    const { engine, statuses, pushes } = fixture();
    engine._reconcileInner = async () => ({ ...complete, deferred: 2, drifted: 1, readErrors: 1 });
    await engine.forceSync();
    check(pushes() === 1, "incomplete repair prevented unrelated pending push");
    check(engine.getLastRepairSummary()?.incomplete === true, "equal observed roots concealed incomplete content repair");
    check(engine.getLastError()?.origin === "reconcile", "incomplete repair omitted durable diagnostic error");
    check(statuses.at(-1) === "sync ⚠ repair 2", "forceSync final status claimed successful repair");
    check(engine.pendingIdleStatus() === "sync ⚠ repair 2", "later idle state concealed pending content repair");
    const copied = engine.getLastRepairSummary();
    copied.result.deferred = 0;
    check(engine.getLastRepairSummary().result.deferred === 2, "summary caller mutated engine status");
    engine._reconcileInner = async () => ({ ...complete });
    await engine.forceSync();
    check(engine.getLastRepairSummary()?.incomplete === false && engine.getLastError() === null,
        "successful follow-up repair did not clear its own incomplete error");
    check(engine.pendingIdleStatus() === "sync ✓", "successful follow-up left false repair warning");
}

async function failedCheckAndCancellationRemainDistinct(): Promise<void> {
    const { engine, pushes } = fixture();
    engine._reconcileInner = async () => { throw new Error("injected repair check failure"); };
    await engine.forceSync();
    const summary = engine.getLastRepairSummary();
    check(summary?.incomplete === true && summary.result === null,
        "failed check invented a successful repair or fabricated missing count");
    check(pushes() === 1 && engine.pendingIdleStatus() === "sync ⚠ repair pending",
        "failed repair either blocked unrelated push or lost final warning");
    engine._reconcileInner = async () => {
        engine.hashWorkerAbort.abort();
        throw engine.hashWorkerAbort.signal.reason;
    };
    let error: any;
    try { await engine.forceSync(); } catch (caught) { error = caught; }
    check(error?.name === "AbortError" && pushes() === 1, "forceSync swallowed cancellation and started another push");
    check(!engine.syncing, "cancelled repair retained engine mutex");
}

async function checkpointBeginFailureReleasesEngine(): Promise<void> {
    const { engine } = fixture();
    let enteredRepair = false;
    engine.operationCheckpoint = { begin: async () => { throw new Error("checkpoint unavailable"); } };
    engine._reconcileInner = async () => { enteredRepair = true; return complete; };
    let failed = false;
    try { await engine.reconcileContent(); } catch { failed = true; }
    check(failed && !enteredRepair && !engine.syncing, "failed checkpoint begin leaked engine mutex or entered repair");
    check(engine.getLastRepairSummary()?.incomplete && engine.getLastRepairSummary()?.result === null,
        "failed checkpoint begin fabricated a completed repair");
}

async function repairMutexPreventsIdleCapabilityProbe(): Promise<void> {
    const { engine } = fixture();
    check(!engine.isBusy(), "idle engine reported busy before repair");
    engine._reconcileInner = async () => {
        check(engine.getState() === "idle" && engine.isBusy(),
            "idle phase label concealed active reconcile mutex from diagnostics");
        return { ...complete };
    };
    await engine.reconcileContent();
    check(!engine.isBusy(), "completed repair retained busy diagnostic gate");
}

function pendingStatusUsesConstantTimeJournalCount(): void {
    const { engine } = fixture();
    engine.journal.unsyncedCount = () => 3;
    check(engine.pendingIdleStatus() === "sync …", "pending durable journal was concealed by idle status");
    engine.journal.unsyncedCount = () => 0;
    check(engine.pendingIdleStatus() === "sync ✓", "empty durable journal did not clear pending status");
}

function rootRecoveryCannotLookComplete(): void {
    const { engine } = fixture();
    let pending = true;
    engine.rootRuntime = { snapshot: () => ({ loaded: true, pending, closed: false }) };
    check(engine.hasPendingRootWork() && engine.pendingIdleStatus() === "sync ⏸ root recovery",
        "prepared root with an empty dirty/journal count looked complete");
    pending = false; engine.rootRepairRequired = true;
    check(engine.hasPendingRootWork() && engine.pendingIdleStatus() === "sync ⏸ root recovery",
        "retired root concealed a still-unrepaired derived tree");
    engine.rootRepairRequired = false;
    engine.rootPendingReason = "root dependencies/limits"; engine.pendingChanges.size = 2;
    check(engine.pendingIdleStatus() === "sync ⏸ root dependencies/limits", "idle status lost the root hold reason");
    engine.rootBatchSummary = { selected: 256, cuts: 250, metadataBytes: 128000,
        held: { "missing-peer": { paths: 2, components: 1 } } };
    const status = engine.getRootSyncStatus();
    check(status.lastBatch.selected === 256 && status.lastBatch.held["missing-peer"].paths === 2,
        "root batch diagnostic omitted the bounded selection/holds");
    status.lastBatch.held["missing-peer"].paths = 0;
    check(engine.getRootSyncStatus().lastBatch.held["missing-peer"].paths === 2, "diagnostic caller mutated held-path counts");
    engine.pendingChanges.size = 0;
    check(engine.pendingIdleStatus() === "sync ✓", "historical hold label concealed a now-empty queue");
}

void incompleteRepairNeverClaimsCompletion()
    .then(failedCheckAndCancellationRemainDistinct)
    .then(checkpointBeginFailureReleasesEngine)
    .then(repairMutexPreventsIdleCapabilityProbe)
    .then(pendingStatusUsesConstantTimeJournalCount)
    .then(rootRecoveryCannotLookComplete)
    .then(() => console.log(`reconcile-status.test: ${assertions} assertions passed`))
    .catch(error => { console.error(error); process.exitCode = 1; });
