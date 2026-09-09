import { strict as assert } from "node:assert";
import {
    CurrentHostWorkAggregateError,
    CurrentHostWorkError,
    CurrentHostWorkLease,
    CurrentHostWorkOwner,
} from "./current-host-work";

function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (reason: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}

const microtasks = async () => { await Promise.resolve(); await Promise.resolve(); };

async function stableEmptyCloseAndSynchronousAdmissionFence(): Promise<void> {
    const owner = new CurrentHostWorkOwner();
    assert.deepEqual(owner.snapshot(), {
        closed: false, generation: 1, active: 0, drained: false, failureCount: 0,
    });
    const drain = owner.closeAndDrain();
    assert.equal(drain, owner.closeAndDrain());
    assert.deepEqual(owner.snapshot(), {
        closed: true, generation: 2, active: 0, drained: true, failureCount: 0,
    });
    let invoked = false;
    assert.throws(() => owner.run(() => { invoked = true; }), error =>
        error instanceof CurrentHostWorkError && error.code === "CURRENT_HOST_WORK_CLOSED");
    assert.equal(invoked, false);
    await drain;
    assert.equal(drain, owner.closeAndDrain());
}

async function leaseRevocationAndActualSettlementOwnership(): Promise<void> {
    const owner = new CurrentHostWorkOwner();
    const cleanup = deferred<void>();
    let lease!: CurrentHostWorkLease;
    let aborts = 0;
    const work = owner.run(async admitted => {
        lease = admitted;
        admitted.signal.addEventListener("abort", () => { aborts++; }, { once: true });
        await cleanup.promise;
        admitted.assertCurrent();
    });
    assert.equal(owner.snapshot().active, 1, "operation was not owned synchronously");
    assert.equal(lease.generation, 1);
    assert.equal(lease.isCurrent(), true);

    const drain = owner.closeAndDrain();
    assert.equal(aborts, 1, "close did not revoke the admitted lease synchronously");
    assert.equal(lease.isCurrent(), false);
    assert.throws(() => lease.assertCurrent(), error =>
        error instanceof CurrentHostWorkError && error.code === "CURRENT_HOST_WORK_STALE");
    let drained = false;
    void drain.catch(() => {}).then(() => { drained = true; });
    await microtasks();
    assert.equal(drained, false, "retirement completed before admitted cleanup settled");
    cleanup.resolve();
    await assert.rejects(work, error =>
        error instanceof CurrentHostWorkError && error.code === "CURRENT_HOST_WORK_STALE");
    await assert.rejects(drain, error => error instanceof CurrentHostWorkAggregateError &&
        error.totalFailures === 1);
    assert.equal(owner.snapshot().active, 0);
}

async function rejectedSiblingDoesNotShortCircuitDrain(): Promise<void> {
    const owner = new CurrentHostWorkOwner();
    const first = deferred<void>();
    const second = deferred<void>();
    const third = deferred<void>();
    const privateFailure = new Error("private/vault/path.md");
    const otherFailure = new Error("note contents");
    const tasks = [owner.run(() => first.promise), owner.run(() => second.promise), owner.run(() => third.promise)];
    const observed = tasks.map(task => task.catch(() => {}));
    const drain = owner.closeAndDrain();
    let drained = false;
    void drain.catch(() => {}).then(() => { drained = true; });

    first.reject(privateFailure);
    await microtasks();
    assert.equal(drained, false);
    assert.equal(owner.snapshot().active, 2);
    second.resolve();
    await microtasks();
    assert.equal(drained, false);
    third.reject(otherFailure);
    await Promise.all(observed);

    await assert.rejects(drain, error => {
        assert.ok(error instanceof CurrentHostWorkAggregateError);
        assert.equal(error.message, "Current host work failed during retirement");
        assert.equal(error.message.includes("vault"), false, "aggregate diagnostic leaked a path");
        assert.equal(error.message.includes("contents"), false, "aggregate diagnostic leaked data");
        assert.equal(error.totalFailures, 2);
        assert.deepEqual(Object.keys(error).sort(), ["code", "name", "totalFailures"],
            "aggregate exposed raw task causes");
        return true;
    });
    assert.equal(drained, true);
    assert.equal(drain, owner.closeAndDrain(), "repeated close did not share the rejected tail");
}

async function settledFailuresDoNotPoisonLaterCloseAndSaturation(): Promise<void> {
    const owner = new CurrentHostWorkOwner({ maxActive: 1 });
    const pending = deferred<void>();
    const first = owner.run(() => pending.promise);
    let invoked = false;
    assert.throws(() => owner.run(() => { invoked = true; }), error =>
        error instanceof CurrentHostWorkError && error.code === "CURRENT_HOST_WORK_SATURATED");
    assert.equal(invoked, false);
    pending.reject(new Error("first cause"));
    await first.catch(() => {});

    const second = owner.run(() => { throw new Error("second cause"); });
    await second.catch(() => {});
    assert.deepEqual(owner.snapshot(), {
        closed: false, generation: 1, active: 0, drained: false, failureCount: 0,
    }, "fully settled failures leaked into a later retirement cut");
    const drain = owner.closeAndDrain();
    await drain;
    assert.deepEqual(owner.snapshot(), {
        closed: true, generation: 2, active: 0, drained: true, failureCount: 0,
    });
}

let completed = false;
process.once("beforeExit", () => {
    if (!completed) {
        console.error("current host work tests did not reach their final drain");
        process.exitCode = 1;
    }
});
void (async () => {
    await stableEmptyCloseAndSynchronousAdmissionFence();
    await leaseRevocationAndActualSettlementOwnership();
    await rejectedSiblingDoesNotShortCircuitDrain();
    await settledFailuresDoNotPoisonLaterCloseAndSaturation();
    completed = true;
    console.log("current host work: 4 admission/revocation/drain groups passed");
})().catch(error => { console.error(error); process.exitCode = 1; });
