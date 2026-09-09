import { strict as assert } from "node:assert";
import { EngineWorkClosedError, EngineWorkTracker } from "./engine-work";

function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (reason: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
const microtasks = async () => { await Promise.resolve(); await Promise.resolve(); };
function closedAdmission(tracker: EngineWorkTracker): void {
    assert.throws(() => tracker.enter(), error => error instanceof EngineWorkClosedError &&
        error.name === "EngineWorkClosedError" && error.code === "ENGINE_WORK_CLOSED" &&
        error.message === "Engine work admission is closed");
}

async function emptyCloseAndStableIdentity(): Promise<void> {
    const tracker = new EngineWorkTracker();
    assert.deepEqual(tracker.snapshot(), { closed: false, active: 0, drained: false });
    const drain = tracker.closeAndDrain();
    assert.equal(drain, tracker.closeAndDrain());
    assert.deepEqual(tracker.snapshot(), { closed: true, active: 0, drained: true });
    closedAdmission(tracker);
    await drain;
    assert.equal(drain, tracker.closeAndDrain());
    closedAdmission(tracker);
    const detached = tracker.snapshot();
    detached.closed = false;
    detached.active = 100;
    assert.deepEqual(tracker.snapshot(), { closed: true, active: 0, drained: true });
}

async function allOwnersAndIdempotentRelease(): Promise<void> {
    const tracker = new EngineWorkTracker();
    const first = tracker.enter();
    const second = tracker.enter();
    assert.equal(tracker.snapshot().active, 2, "admission must not wait for a microtask");
    const drain = tracker.closeAndDrain();
    let settled = false;
    void drain.then(() => { settled = true; });
    assert.equal(drain, tracker.closeAndDrain());
    second();
    second();
    assert.deepEqual(tracker.snapshot(), { closed: true, active: 1, drained: false });
    closedAdmission(tracker);
    await microtasks();
    assert.equal(settled, false);
    first();
    assert.deepEqual(tracker.snapshot(), { closed: true, active: 0, drained: true });
    await drain;
    assert.equal(settled, true);
    first();
    second();
    assert.equal(tracker.snapshot().active, 0);
    assert.equal(drain, tracker.closeAndDrain());
}

async function idleWhileOpenDoesNotEndLifetime(): Promise<void> {
    const tracker = new EngineWorkTracker();
    const release = tracker.enter();
    release();
    assert.deepEqual(tracker.snapshot(), { closed: false, active: 0, drained: false });
    const later = tracker.enter();
    release();
    assert.equal(tracker.snapshot().active, 1, "old double release consumed a later owner");
    const drain = tracker.closeAndDrain();
    let settled = false;
    void drain.then(() => { settled = true; });
    await microtasks();
    assert.equal(settled, false);
    later();
    await drain;
}

async function failedWorkStillOwnsPendingCleanup(): Promise<void> {
    const tracker = new EngineWorkTracker();
    const actualIO = deferred<void>();
    const cleanupIO = deferred<void>();
    const cleanupEntered = deferred<void>();
    const failure = new Error("started fixture IO failed");
    const release = tracker.enter();
    const started = (async () => {
        try { await actualIO.promise; }
        finally {
            cleanupEntered.resolve();
            try { await cleanupIO.promise; }
            finally { release(); }
        }
    })();
    const rejected = assert.rejects(started, error => error === failure);
    const drain = tracker.closeAndDrain();
    let drained = false;
    void drain.then(() => { drained = true; });
    actualIO.reject(failure);
    await cleanupEntered.promise;
    await microtasks();
    assert.equal(drained, false, "a failed phase released ownership before its actual cleanup");
    assert.equal(tracker.snapshot().active, 1);
    assert.equal(drain, tracker.closeAndDrain());
    cleanupIO.resolve();
    await rejected;
    await drain;
    assert.equal(tracker.snapshot().active, 0);
}

async function callerRaceDoesNotVirtuallyCompleteActualWork(): Promise<void> {
    const tracker = new EngineWorkTracker();
    const realIO = deferred<number>();
    const release = tracker.enter();
    const actual = (async () => {
        try { return await realIO.promise; }
        finally { release(); }
    })();
    const cancellationNotice = new Error("caller stopped waiting");
    const caller = Promise.race([actual, Promise.reject(cancellationNotice)]);
    await assert.rejects(caller, error => error === cancellationNotice);
    assert.deepEqual(tracker.snapshot(), { closed: false, active: 1, drained: false });
    const drain = tracker.closeAndDrain();
    let settled = false;
    void drain.then(() => { settled = true; });
    await microtasks();
    assert.equal(settled, false);
    realIO.resolve(42);
    assert.equal(await actual, 42);
    await drain;
}

async function synchronousRegistrationAndReentrantStop(): Promise<void> {
    const tracker = new EngineWorkTracker();
    const priorOwner = tracker.enter();
    let drain!: Promise<void>;
    const startupFailure = new Error("synchronous startup failed");
    function startThenStopReentrantly(): void {
        const release = tracker.enter();
        try {
            assert.equal(tracker.snapshot().active, 2);
            // A callback may stop the engine before the work reaches its
            // first await. Its already-admitted owner must still be counted.
            drain = tracker.closeAndDrain();
            assert.deepEqual(tracker.snapshot(), { closed: true, active: 2, drained: false });
            closedAdmission(tracker);
            throw startupFailure;
        } finally { release(); }
    }
    assert.throws(startThenStopReentrantly, error => error === startupFailure);
    assert.deepEqual(tracker.snapshot(), { closed: true, active: 1, drained: false });
    let settled = false;
    void drain.then(() => { settled = true; closedAdmission(tracker); });
    await microtasks();
    assert.equal(settled, false);
    priorOwner();
    await drain;
    assert.equal(settled, true);
    assert.equal(drain, tracker.closeAndDrain());
}

let completed = false;
process.once("beforeExit", () => {
    if (!completed) {
        console.error("engine work tests did not reach their final drain");
        process.exitCode = 1;
    }
});
void (async () => {
    await emptyCloseAndStableIdentity();
    await allOwnersAndIdempotentRelease();
    await idleWhileOpenDoesNotEndLifetime();
    await failedWorkStillOwnsPendingCleanup();
    await callerRaceDoesNotVirtuallyCompleteActualWork();
    await synchronousRegistrationAndReentrantStop();
    completed = true;
    console.log("engine work: 6 lifecycle/drain groups passed");
})().catch(error => { console.error(error); process.exitCode = 1; });
