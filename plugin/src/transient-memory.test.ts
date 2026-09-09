import {
    closeTransientMemory,
    configureTransientMemory,
    estimateRootOutcomeWorkset,
    estimateTransportWorkset,
    estimateUploadBatchWorkset,
    reserveTransientScope,
    TransientMemoryBudget,
    transientMemorySnapshot,
    type TransientWorkContext,
} from "./transient-memory";
import {
    closeHashSourceBudget,
    configureHashSourceBudget,
    estimateHashSourceWorkset,
    hashSourceBudgetSnapshot,
    withHashSource,
} from "./hash-source-budget";
import { ResourceBudget, ResourceBudgetClosedError, ResourceBudgetOversizedError } from "./resource-budget";
import { SyncMemoryArbiter } from "./sync-memory-arbiter";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};
function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
const rejected = (promise: Promise<unknown>): Promise<unknown> => promise.then(
    () => { throw new Error("expected transient work to reject"); },
    (error: unknown) => error,
);
async function microtasks(): Promise<void> {
    for (let index = 0; index < 8; index++) await Promise.resolve();
}

function estimatesAndAliases(): void {
    check(configureHashSourceBudget === configureTransientMemory,
        "source and transfer configuration control different pools");
    check(hashSourceBudgetSnapshot === transientMemorySnapshot,
        "source and transfer snapshots observe different pools");
    check(closeHashSourceBudget === closeTransientMemory,
        "source shutdown does not stop shared transfer admission");
    const mib = 1024 * 1024;
    const workset = estimateUploadBatchWorkset({
        sourceBytes: 2 * mib, hashBatchBytes: 2 * mib,
        feedBytes: 256 * 1024, transportPayloadBytes: 2 * mib,
    });
    check(workset.ownerBytes === 3 * 2 * mib + 2 * 2 * mib + 2 * 256 * 1024 + 65536,
        "owner estimate omitted source/hash/native copy allowances");
    check(workset.workBytes === estimateTransportWorkset(2 * mib), "transport estimate changed between APIs");
    check(workset.workBytes === 6 * (2 * mib + 65536) + 65536, "crypto/native pack allowances missing");
    check(workset.totalBytes === workset.ownerBytes + workset.workBytes && workset.totalBytes < 32 * mib,
        "default mobile batch exceeds its conservative total cap");
    const ranged = estimateUploadBatchWorkset({
        sourceBytes: 0, hashBatchBytes: 0, feedBytes: mib,
        transportPayloadBytes: 4 * mib, retainedRangeBytes: 4 * mib,
    });
    check(ranged.ownerBytes === 4 * mib + 2 * mib + 65536,
        "native ranged queue was treated as three whole-file sources");
    check(ranged.totalBytes < 32 * mib, "bounded immutable 4MiB range was unnecessarily rejected");
    const empty = estimateUploadBatchWorkset({
        sourceBytes: 0, hashBatchBytes: 0, feedBytes: 1, transportPayloadBytes: 0,
    });
    check(empty.totalBytes > 0 && empty.workBytes > 0, "empty transfer lost protocol overhead allowance");
    const rootOutcome = estimateRootOutcomeWorkset(704 * 1024, 64 * 1024 + 512);
    check(rootOutcome.ownerBytes === 3 * 704 * 1024 + 6 * (64 * 1024 + 512),
        "root outcome estimate omitted canonical JSON/response validation copies");
    check(rootOutcome.workBytes === estimateTransportWorkset(704 * 1024),
        "root outcome transport allowance did not cover the larger direction");
    check(rootOutcome.totalBytes === 7_343_104 && rootOutcome.totalBytes < 32 * mib,
        "maximum root outcome does not fit the documented mobile admission ceiling");
    for (const invalid of [0, -1, NaN, Infinity, 0.5, Number.MAX_SAFE_INTEGER]) {
        let error: unknown;
        try { estimateRootOutcomeWorkset(invalid, 1); } catch (caught) { error = caught; }
        check(error instanceof RangeError, "invalid/overflowing root request estimate accepted");
    }
    for (const invalid of [-1, NaN, Infinity, 0.5, Number.MAX_SAFE_INTEGER]) {
        let error: unknown;
        try {
            estimateUploadBatchWorkset({
                sourceBytes: 0, hashBatchBytes: 0, feedBytes: 1,
                transportPayloadBytes: 0, retainedRangeBytes: invalid,
            });
        } catch (caught) { error = caught; }
        check(error instanceof RangeError, "invalid/overflowing retained range estimate accepted");
    }
}

async function fullCapacitySubscopeDoesNotDeadlock(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 100 });
    const scope = await reserveTransientScope({ ownerBytes: 60, workBytes: 40 }, { budget });
    check(budget.snapshot().usedBytes === 100, "scope did not charge the complete workset upfront");
    const result = await scope.run(40, async (context) => {
        check(context.bytes === 40, "work context has incorrect quota");
        check(budget.snapshot().usedBytes === 100, "nested work double-counted global bytes");
        check(scope.snapshot().work.usedBytes === 40, "nested work bypassed its local quota");
        return "uploaded";
    });
    check(result === "uploaded" && scope.snapshot().work.usedBytes === 0,
        "full-capacity nested work stalled or leaked quota");
    check(budget.snapshot().usedBytes === 100, "child completion released still-owned batch buffers");
    check(await rejected(scope.reserve(41)) instanceof ResourceBudgetOversizedError,
        "child exceeded its reserved transport allowance");
    scope.close();
    scope.close();
    check(budget.snapshot().usedBytes === 0 && scope.snapshot().parentReleased,
        "closing an idle scope did not release its parent");
    check(await rejected(scope.reserve(1)) instanceof ResourceBudgetClosedError,
        "closed scope accepted new child work");
    budget.close();
}

async function scopeCloseWaitsForAllOwners(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 100 });
    const scope = await reserveTransientScope({ ownerBytes: 60, workBytes: 40 }, { budget });
    const native = deferred<void>();
    const tracked = rejected(scope.track(() => native.promise));
    const child = await scope.reserve(40);
    const queued = rejected(scope.reserve(10));
    scope.close();
    check(await queued instanceof ResourceBudgetClosedError, "close did not reject ungranted children");
    check(scope.snapshot().work.queuedBytes === 0 && scope.snapshot().trackedWork === 1,
        "close lost queued cleanup or an active owner-covered native task");
    child.release();
    child.release();
    check(budget.snapshot().usedBytes === 100, "last child released an unfinished owner-covered read");
    const error = new Error("native read failed after owner timeout");
    native.reject(error);
    check(await tracked === error, "tracked failure was swallowed");
    check(budget.snapshot().usedBytes === 0 && scope.snapshot().parentReleased,
        "settled owner-covered work did not release the closed parent");
    check(await rejected(scope.track(() => undefined)) instanceof ResourceBudgetClosedError,
        "closed owner scope accepted new native work");
    budget.close();
}

async function timeoutDoesNotFreeChildCryptoWorkspace(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 100 });
    const scope = await reserveTransientScope({ ownerBytes: 60, workBytes: 40 }, { budget });
    const native = deferred<void>();
    const timeout = new Error("caller-visible timeout");
    let tracked: Promise<void> | undefined;
    let oldContext: TransientWorkContext | undefined;
    const failed = scope.run(40, (context) => {
        oldContext = context;
        tracked = context.track(() => native.promise);
        throw timeout;
    });
    check(await rejected(failed) === timeout, "caller-visible error was delayed or replaced");
    check(scope.snapshot().work.usedBytes === 40, "timeout freed still-active native crypto workspace");
    let retryStarted = false;
    const retry = scope.run(40, () => { retryStarted = true; return "retry"; });
    await microtasks();
    check(!retryStarted && scope.snapshot().work.queuedRequests === 1,
        "retry reused work quota while old native buffers were live");
    check(await rejected(oldContext!.track(() => undefined)) instanceof ResourceBudgetClosedError,
        "timed-out context accepted new late-session native work");
    native.resolve();
    await tracked;
    check(await retry === "retry" && retryStarted, "settled native work failed to resume queued retry");
    scope.close();
    check(budget.snapshot().usedBytes === 0, "crypto-retained child leaked parent accounting");
    budget.close();
}

async function reuseWaitsForNativeSettlement(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 100 });
    const scope = await reserveTransientScope({ ownerBytes: 60, workBytes: 40 }, { budget });
    const native = deferred<void>();
    let fallbackStarted = false;
    const result = scope.run(40, async (context) => {
        const first = context.track(() => native.promise);
        const idle = context.waitForIdle();
        check(idle === context.waitForIdle(), "idle waiting allocated separate waiter queues");
        await idle;
        await first;
        fallbackStarted = true;
        return context.track(async () => "fallback");
    });
    await microtasks();
    check(!fallbackStarted && scope.snapshot().work.usedBytes === 40,
        "same-attempt fallback reused an active native buffer allowance");
    native.resolve();
    check(await result === "fallback", "workspace could not be reused after native settlement");
    scope.close();
    check(budget.snapshot().usedBytes === 0, "same-context fallback leaked the global lease");
    budget.close();
}

async function cancelledNativeWorkRetainsClosedParent(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 100 });
    const abort = new AbortController();
    const creating = reserveTransientScope({ ownerBytes: 60, workBytes: 40 }, { budget, signal: abort.signal });
    abort.abort();
    check((await rejected(creating) as Error).name === "AbortError" && budget.snapshot().usedBytes === 0,
        "same-turn abort leaked an unused granted parent");

    const scope = await reserveTransientScope({ ownerBytes: 60, workBytes: 40 }, { budget });
    const native = deferred<string>();
    const cancel = new AbortController();
    const running = rejected(scope.run(40, () => native.promise, { signal: cancel.signal }));
    await microtasks();
    cancel.abort();
    scope.close();
    check(budget.snapshot().usedBytes === 100 && scope.snapshot().work.usedBytes === 40,
        "abort/close freed an unreturned native operation");
    native.resolve("discarded");
    check((await running as Error).name === "AbortError", "cancelled native result was published");
    check(budget.snapshot().usedBytes === 0, "native completion did not drain closed accounting");
    budget.close();
}

async function hashSourcesAndTransfersShareAdmission(): Promise<void> {
    const bytes = estimateHashSourceWorkset(8, 4);
    const policy = new TransientMemoryBudget();
    policy.configure({ runtime: "mobile", transientBudgetBytes: bytes });
    const scope = await reserveTransientScope({ ownerBytes: bytes - 1, workBytes: 1 }, { budget: policy });
    let reads = 0;
    const source = withHashSource({
        sourceBytes: 8, feedBytes: 4, budget: policy,
        read: async () => { reads++; return new Uint8Array(8); },
        consume: () => "hash",
    });
    await microtasks();
    check(reads === 0 && policy.snapshot().queuedRequests === 1,
        "source admission bypassed a full shared transfer workset");
    await scope.run(1, () => undefined);
    scope.close();
    check(await source === "hash" && reads === 1 && policy.snapshot().usedBytes === 0,
        "released transfer workset did not resume source admission");
    check(policy.snapshot().peakUsedBytes === bytes, "source/transfer accounting overlapped independent ceilings");
    policy.close();
}

async function transientReservationsUseTheInjectedLifetimeArbiter(): Promise<void> {
    const arbiter = new SyncMemoryArbiter({ capacityBytes: 100, interactiveReserveBytes: 10 });
    const policy = new TransientMemoryBudget();
    policy.configure({ runtime: "mobile", transientBudgetBytes: 100 }, arbiter);
    const first = await policy.reserve(60);
    check(arbiter.snapshot().owners.transfer.usedBytes === 60,
        "transient reservation bypassed the injected lifetime arbiter");
    const review = await arbiter.reserve("root-review", 30);
    const second = policy.reserve(1);
    await microtasks();
    check(arbiter.snapshot().queuedRequests === 1 && policy.snapshot().usedBytes === 61,
        "aggregate ceiling did not hold a locally admitted transfer before allocation");
    check((() => {
        try {
            policy.configure({ runtime: "mobile", transientBudgetBytes: 100 },
                new SyncMemoryArbiter({ capacityBytes: 100 }));
            return false;
        } catch { return true; }
    })(), "live transient owners allowed arbiter replacement");
    review.release();
    const granted = await second;
    granted.release(); first.release(); policy.close(); arbiter.close();
    check(arbiter.snapshot().usedBytes === 0 && policy.snapshot().usedBytes === 0,
        "combined local/shared transfer ownership leaked after settlement");
}

estimatesAndAliases();
void fullCapacitySubscopeDoesNotDeadlock()
    .then(scopeCloseWaitsForAllOwners)
    .then(timeoutDoesNotFreeChildCryptoWorkspace)
    .then(reuseWaitsForNativeSettlement)
    .then(cancelledNativeWorkRetainsClosedParent)
    .then(hashSourcesAndTransfersShareAdmission)
    .then(transientReservationsUseTheInjectedLifetimeArbiter)
    .then(() => console.log(`transient-memory.test: ${assertions} assertions passed`))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
