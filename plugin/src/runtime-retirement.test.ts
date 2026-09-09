import { strict as assert } from "node:assert";
import { awaitRuntimeRetirement, registerRuntimeRetirement, settleRuntimeOwners, startRuntimeOwner } from "./runtime-retirement";

function gate() {
    let resolve!: () => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
const tick = async () => { for (let index = 0; index < 8; index++) await Promise.resolve(); };

async function realOwnersAndErrors(): Promise<void> {
    const native = gate();
    const failed = new Error("owner failed");
    let called = false;
    const immediateFailure = startRuntimeOwner(() => { called = true; throw failed; });
    assert.equal(called, true, "admission closure must be synchronous");
    let finished = false;
    const joined = settleRuntimeOwners([immediateFailure, native.promise]);
    const observed = joined.then(() => { throw new Error("lost rejection"); }, error => { finished = true; assert.equal(error, failed); });
    await tick();
    assert.equal(finished, false, "failure must not forget a still-native sibling");
    native.resolve();
    await observed;
    await settleRuntimeOwners([]);
}

async function registryWaitsEveryCutAndSeparatesVaults(): Promise<void> {
    const vault = {};
    const first = gate();
    const second = gate();
    const retiredA = registerRuntimeRetirement(vault, first.promise);
    assert.equal(awaitRuntimeRetirement(vault), retiredA);
    await awaitRuntimeRetirement({});
    const retiredB = registerRuntimeRetirement(vault, second.promise);
    assert.equal(awaitRuntimeRetirement(vault), retiredB);
    let done = false;
    const wait = retiredB.then(() => { done = true; });
    second.resolve();
    await tick();
    assert.equal(done, false);
    first.resolve();
    await wait;
    assert.notEqual(awaitRuntimeRetirement(vault), retiredB, "only completed current cut is cleared");
    await awaitRuntimeRetirement(vault);
}

async function failedRetirementStaysClosedAfterAllSiblings(): Promise<void> {
    const vault = {};
    const slow = gate();
    const failed = new Error("unconfirmed retirement");
    const old = registerRuntimeRetirement(vault, Promise.reject(failed));
    await assert.rejects(old, error => error === failed);
    const next = registerRuntimeRetirement(vault, slow.promise);
    let done = false;
    const observed = next.catch(error => { done = true; assert.equal(error, failed); });
    await tick();
    assert.equal(done, false);
    slow.resolve();
    await observed;
    assert.equal(awaitRuntimeRetirement(vault), next);
    await assert.rejects(awaitRuntimeRetirement(vault), error => error === failed);
}

async function unloadDuringWaitingLoadDoesNotFollowItself(): Promise<void> {
    const vault = {};
    const previous = gate();
    const old = registerRuntimeRetirement(vault, previous.promise);
    const loadCut = awaitRuntimeRetirement(vault);
    assert.equal(loadCut, old);
    let loadReturned = false;
    const loading = loadCut.then(() => { loadReturned = true; });
    const unload = registerRuntimeRetirement(vault, settleRuntimeOwners([loading]));
    assert.equal(awaitRuntimeRetirement(vault), unload);
    previous.resolve();
    await unload;
    assert.equal(loadReturned, true, "waiting load must not acquire a future self-dependent cut");
}

let completed = false;
process.once("beforeExit", () => { if (!completed && !process.exitCode) { console.error("runtime retirement tests did not settle"); process.exitCode = 1; } });
void (async () => {
    await realOwnersAndErrors();
    await registryWaitsEveryCutAndSeparatesVaults();
    await failedRetirementStaysClosedAfterAllSiblings();
    await unloadDuringWaitingLoadDoesNotFollowItself();
    completed = true;
    console.log("runtime-retirement.test: 4 owner/registry/reload groups passed");
})().catch(error => { console.error(error); process.exitCode = 1; });
