import { strict as assert } from "node:assert";
import { ResourceBudgetQueueFullError } from "./resource-budget";
import { SyncMemoryArbiter } from "./sync-memory-arbiter";

async function sharedCeilingAndInteractiveReserve(): Promise<void> {
    const arbiter = new SyncMemoryArbiter({ capacityBytes: 100, interactiveReserveBytes: 20,
        maxQueuedRequests: 4, maxQueuedBytes: 200 });
    const plan = await arbiter.reserve("root-plan", 80);
    let backgroundSettled = false, interactiveSettled = false;
    const background = arbiter.reserve("transfer", 1).then(value => { backgroundSettled = true; return value; });
    const interactive = arbiter.reserve("interactive", 20).then(value => { interactiveSettled = true; return value; });
    await Promise.resolve();
    assert.equal(backgroundSettled, false);
    assert.equal(interactiveSettled, true, "background wait must not consume or block interactive reserve");
    const interactiveLease = await interactive;
    assert.equal(arbiter.snapshot().usedBytes, 100);
    interactiveLease.release(); plan.release();
    const backgroundLease = await background;
    assert.equal(arbiter.snapshot().owners.transfer.activeLeases, 1);
    backgroundLease.release();
    assert.equal(arbiter.snapshot().usedBytes, 0);
}

async function boundedQueuesAbortAndClose(): Promise<void> {
    const arbiter = new SyncMemoryArbiter({ capacityBytes: 10, maxQueuedRequests: 1, maxQueuedBytes: 10 });
    const held = await arbiter.reserve("root-review", 10);
    const controller = new AbortController();
    const queued = arbiter.reserve("root-plan", 5, { signal: controller.signal });
    await assert.rejects(arbiter.reserve("transfer", 1), error => error instanceof ResourceBudgetQueueFullError);
    const reason = new Error("cancelled admission"); controller.abort(reason);
    await assert.rejects(queued, error => error === reason);
    assert.equal(arbiter.snapshot().queuedRequests, 0);
    arbiter.close(); held.release(); held.release();
    assert.equal(arbiter.snapshot().usedBytes, 0, "close must retain live accounting until owner release");
    await assert.rejects(arbiter.reserve("interactive", 1), /closed/);
}

function synchronousAdmissionNeverQueues(): void {
    const arbiter = new SyncMemoryArbiter({ capacityBytes: 10 });
    const held = arbiter.tryReserve("prepared-manifest", 10); assert(held);
    assert.equal(arbiter.tryReserve("transfer", 1), null);
    assert.equal(arbiter.snapshot().queuedRequests, 0);
    held.release();
    const next = arbiter.tryReserve("transfer", 1); assert(next); next.release();
}

void (async () => {
    await sharedCeilingAndInteractiveReserve();
    await boundedQueuesAbortAndClose();
    synchronousAdmissionNeverQueues();
    console.log("sync memory arbiter: 3 groups passed");
})().catch(error => { console.error(error); process.exitCode = 1; });
