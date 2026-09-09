import { strict as assert } from "node:assert";
import {
    RootTreeResidentAdmission,
    RootTreeResidentStateError,
    type RootTreeResidentAttempt,
    type RootTreeResidentPlan,
    type RootTreeResidentRetirement,
} from "./root-tree-resident-admission";
import {
    captureV2CandidateSettlementWitness,
    settleTreeCandidateOutput,
    settleTreeCandidateOutputWithAdmission,
} from "./tree-output-settlement";

function reserve(owner: RootTreeResidentAdmission, tree: object, peakBytes: number,
    residentBytes = peakBytes): RootTreeResidentAttempt {
    const attempt = owner.reserve(tree, { peakBytes, residentBytes });
    assert(attempt, "fixture expected fail-fast admission");
    assert(!(attempt instanceof Promise));
    return attempt;
}

function resident(owner: RootTreeResidentAdmission, tree: object, bytes: number): void {
    const attempt = reserve(owner, tree, bytes);
    owner.ready(attempt);
    owner.releaseRetired(owner.publish(attempt));
}

function charges(owner: RootTreeResidentAdmission, expected: {
    privateBytes: number; residentBytes: number; retiringBytes: number;
    privateAttempts: number; residentTrees: number; retiringOwners: number; activeLeases: number;
}): void {
    const current = owner.snapshot();
    assert.equal(current.scope, "v2-tree-output-requested-buffers");
    for (const key of ["privateBytes", "residentBytes", "retiringBytes", "privateAttempts",
        "residentTrees", "retiringOwners"] as const) assert.equal(current[key], expected[key], key);
    assert.equal(current.ledger.activeLeases, expected.activeLeases, "activeLeases");
    assert.equal(current.ledger.usedBytes,
        expected.privateBytes + expected.residentBytes + expected.retiringBytes, "disjoint charge sum");
    assert(current.ledger.peakUsedBytes >= current.ledger.usedBytes);
}

function empty(owner: RootTreeResidentAdmission): void {
    charges(owner, { privateBytes: 0, residentBytes: 0, retiringBytes: 0,
        privateAttempts: 0, residentTrees: 0, retiringOwners: 0, activeLeases: 0 });
}

function exactPlansAreDetachedBeforeOwnership(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = {};
    for (const invalid of [-1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, "1", 1n]) {
        assert.throws(() => owner.reserve(tree, { peakBytes: invalid, residentBytes: 0 } as RootTreeResidentPlan), TypeError);
        assert.throws(() => owner.reserve(tree, { peakBytes: 20, residentBytes: invalid } as RootTreeResidentPlan), TypeError);
    }
    let getterCalls = 0;
    let coercions = 0;
    for (const invalid of [null, [], {}, { peakBytes: 20 }, { peakBytes: 20, residentBytes: 21 },
        { peakBytes: 20, residentBytes: 10, extra: 1 },
        { peakBytes: 20, residentBytes: 10, [Symbol("hidden")]: true },
        Object.defineProperty({ peakBytes: 20, residentBytes: 10 }, "extra", { value: 1 }),
        { get peakBytes() { getterCalls++; return 20; }, residentBytes: 10 },
        { peakBytes: 20, get residentBytes() { getterCalls++; return 10; } },
        { peakBytes: { [Symbol.toPrimitive]() { coercions++; return 20; } }, residentBytes: 10 }]) {
        assert.throws(() => owner.reserve(tree, invalid as RootTreeResidentPlan), TypeError);
    }
    assert.equal(getterCalls, 0, "plan getters must never run");
    assert.equal(coercions, 0, "plan scalar coercion must never run");
    empty(owner);
    const plan = { peakBytes: 80, residentBytes: 30 };
    const attempt = owner.reserve(tree, plan)!;
    plan.peakBytes = 0;
    plan.residentBytes = 0;
    owner.ready(attempt);
    assert.equal(owner.snapshot().privateBytes, 30, "caller changed the reserved target after admission");
    owner.releaseRetired(owner.detachPrivate(attempt));
    empty(owner);
}

function failFastOldPlusNewAdmissionAndShrink(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = {};
    resident(owner, tree, 60);
    assert.equal(owner.reserve(tree, { peakBytes: 41, residentBytes: 1 }), null);
    charges(owner, { privateBytes: 0, residentBytes: 60, retiringBytes: 0,
        privateAttempts: 0, residentTrees: 1, retiringOwners: 0, activeLeases: 1 });
    assert.equal(owner.snapshot().ledger.refusedReservations, 1);
    const next = reserve(owner, tree, 40, 30);
    charges(owner, { privateBytes: 40, residentBytes: 60, retiringBytes: 0,
        privateAttempts: 1, residentTrees: 1, retiringOwners: 0, activeLeases: 2 });
    assert.throws(() => owner.publish(next), RootTreeResidentStateError, "publish before Ready");
    assert.equal(owner.snapshot().ledger.usedBytes, 100);
    owner.ready(next);
    assert.throws(() => owner.ready(next), RootTreeResidentStateError);
    charges(owner, { privateBytes: 30, residentBytes: 60, retiringBytes: 0,
        privateAttempts: 1, residentTrees: 1, retiringOwners: 0, activeLeases: 2 });
    const old = owner.publish(next);
    charges(owner, { privateBytes: 0, residentBytes: 30, retiringBytes: 60,
        privateAttempts: 0, residentTrees: 1, retiringOwners: 1, activeLeases: 2 });
    assert.equal(owner.snapshot().ledger.peakUsedBytes, 100);
    assert.throws(() => owner.reserve(tree, { peakBytes: 0, residentBytes: 0 }), RootTreeResidentStateError);
    assert.throws(() => owner.releaseResidentAfterFree(tree), RootTreeResidentStateError);
    owner.releaseRetired(old);
    assert.equal(owner.snapshot().ledger.usedBytes, 30);
    owner.releaseResidentAfterFree(tree);
    empty(owner);
}

function zeroOutputStillRequiresRetirementProof(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = {};
    const zero = reserve(owner, tree, 0);
    charges(owner, { privateBytes: 0, residentBytes: 0, retiringBytes: 0,
        privateAttempts: 1, residentTrees: 0, retiringOwners: 0, activeLeases: 0 });
    owner.ready(zero);
    const initialRetirement = owner.publish(zero);
    charges(owner, { privateBytes: 0, residentBytes: 0, retiringBytes: 0,
        privateAttempts: 0, residentTrees: 1, retiringOwners: 1, activeLeases: 0 });
    assert.throws(() => owner.reserve(tree, { peakBytes: 0, residentBytes: 0 }), RootTreeResidentStateError);
    assert.throws(() => owner.releaseResidentAfterFree(tree), RootTreeResidentStateError);
    owner.releaseRetired(initialRetirement);
    resident(owner, tree, 70);
    const replacement = reserve(owner, tree, 0);
    owner.ready(replacement);
    const old = owner.publish(replacement);
    charges(owner, { privateBytes: 0, residentBytes: 0, retiringBytes: 70,
        privateAttempts: 0, residentTrees: 1, retiringOwners: 1, activeLeases: 1 });
    owner.releaseRetired(old);
    charges(owner, { privateBytes: 0, residentBytes: 0, retiringBytes: 0,
        privateAttempts: 0, residentTrees: 1, retiringOwners: 0, activeLeases: 0 });
    owner.releaseResidentAfterFree(tree);
    empty(owner);

    const peakOnly = reserve(owner, {}, 90, 0);
    owner.ready(peakOnly);
    assert.equal(owner.snapshot().ledger.activeLeases, 0, "Ready released proven peak-only buffers");
    const cancelled = owner.detachPrivate(peakOnly);
    assert.equal(owner.snapshot().retiringOwners, 1, "zero private cancellation lost its native barrier");
    owner.releaseRetired(cancelled);
    empty(owner);
}

function privateCancellationRetainsExactCurrentAllowance(): void {
    for (const ready of [false, true]) {
        const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
        const tree = {};
        resident(owner, tree, 50);
        const attempt = reserve(owner, tree, 40, 15);
        if (ready) owner.ready(attempt);
        const retirement = owner.detachPrivate(attempt);
        charges(owner, { privateBytes: 0, residentBytes: 50, retiringBytes: ready ? 15 : 40,
            privateAttempts: 0, residentTrees: 1, retiringOwners: 1, activeLeases: 2 });
        // A rejected cleanup wait supplies no native completion proof.
        assert.throws(() => { throw new Error("synthetic native retirement failure"); }, /retirement failure/);
        assert.equal(owner.snapshot().retiringBytes, ready ? 15 : 40);
        assert.throws(() => owner.reserve(tree, { peakBytes: 1, residentBytes: 1 }), RootTreeResidentStateError);
        owner.releaseRetired(retirement);
        assert.equal(owner.snapshot().ledger.usedBytes, 50, "cancel released the committed owner");
        owner.releaseResidentAfterFree(tree);
        empty(owner);
    }
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = {};
    owner.releaseRetired(owner.detachPrivate(reserve(owner, tree, 60)));
    empty(owner);
    // Cancelled unpublished wrappers are still eligible for another job.
    owner.releaseRetired(owner.detachPrivate(reserve(owner, tree, 10)));
    empty(owner);
}

function capacityShrinkAndCloseNeverForgetOwners(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = {};
    resident(owner, tree, 60);
    const attempt = reserve(owner, tree, 40, 20);
    owner.setCapacity(10);
    assert.equal(owner.snapshot().ledger.overcommittedBytes, 90);
    assert.equal(owner.reserve({}, { peakBytes: 1, residentBytes: 1 }), null);
    owner.close();
    owner.close();
    assert.equal(owner.reserve({}, { peakBytes: 0, residentBytes: 0 }), null, "closed zero-byte bypass");
    assert.equal(owner.reserve({}, { peakBytes: 1, residentBytes: 1 }), null);
    owner.ready(attempt);
    const old = owner.publish(attempt);
    assert.equal(owner.snapshot().ledger.usedBytes, 80);
    assert.equal(owner.snapshot().ledger.overcommittedBytes, 70);
    owner.setCapacity(200);
    assert.equal(owner.snapshot().ledger.closed, true, "capacity update reopened admission");
    assert.equal(owner.reserve({}, { peakBytes: 1, residentBytes: 1 }), null);
    owner.releaseRetired(old);
    owner.releaseResidentAfterFree(tree);
    empty(owner);

    const cancelledOwner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const privateJob = reserve(cancelledOwner, {}, 40);
    cancelledOwner.close();
    cancelledOwner.releaseRetired(cancelledOwner.detachPrivate(privateJob));
    empty(cancelledOwner);
}

function capabilitiesAreOwnerLocalAndSingleUse(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const other = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = {};
    const attempt = reserve(owner, tree, 60, 20);
    const foreign = reserve(other, {}, 10);
    const copied = Object.freeze(Object.defineProperties({}, Object.getOwnPropertyDescriptors(attempt))) as RootTreeResidentAttempt;
    for (const invalid of [foreign, copied, {}, null, 1] as RootTreeResidentAttempt[]) {
        assert.throws(() => owner.ready(invalid), RootTreeResidentStateError);
        assert.throws(() => owner.publish(invalid), RootTreeResidentStateError);
        assert.throws(() => owner.detachPrivate(invalid), RootTreeResidentStateError);
    }
    assert.equal(owner.snapshot().ledger.usedBytes, 60);
    assert.equal(other.snapshot().ledger.usedBytes, 10);
    assert.throws(() => owner.reserve(tree, { peakBytes: 0, residentBytes: 0 }), RootTreeResidentStateError);
    assert.throws(() => owner.releaseResidentAfterFree(tree), RootTreeResidentStateError);
    owner.ready(attempt);
    const retirement = owner.publish(attempt);
    for (const action of [() => owner.ready(attempt), () => owner.publish(attempt), () => owner.detachPrivate(attempt)]) {
        assert.throws(action, RootTreeResidentStateError);
    }
    const copiedRetirement = Object.freeze(Object.defineProperties(
        {}, Object.getOwnPropertyDescriptors(retirement))) as RootTreeResidentRetirement;
    assert.throws(() => owner.releaseRetired(copiedRetirement), RootTreeResidentStateError);
    assert.throws(() => other.releaseRetired(retirement), RootTreeResidentStateError);
    assert.throws(() => owner.releaseRetired(attempt as unknown as RootTreeResidentRetirement), RootTreeResidentStateError);
    owner.releaseRetired(retirement);
    assert.throws(() => owner.releaseRetired(retirement), RootTreeResidentStateError);
    owner.releaseResidentAfterFree(tree);
    assert.throws(() => owner.releaseResidentAfterFree(tree), RootTreeResidentStateError);
    assert.throws(() => owner.reserve(tree, { peakBytes: 0, residentBytes: 0 }), RootTreeResidentStateError);
    const legacyWrapper = {};
    owner.releaseResidentAfterFree(legacyWrapper);
    assert.throws(() => owner.releaseResidentAfterFree(legacyWrapper), RootTreeResidentStateError);
    for (const invalid of [null, 1, "tree"] as unknown as object[]) {
        assert.throws(() => owner.reserve(invalid, { peakBytes: 0, residentBytes: 0 }), RootTreeResidentStateError);
        assert.throws(() => owner.releaseResidentAfterFree(invalid), RootTreeResidentStateError);
    }
    other.releaseRetired(other.detachPrivate(foreign));
    empty(owner);
    empty(other);
}

function postPublishFailureAndFreeRequireExplicitProof(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = {};
    resident(owner, tree, 40);
    const attempt = reserve(owner, tree, 60, 30);
    owner.ready(attempt);
    const old = owner.publish(attempt);
    // Simulate a post-publication scope/summary failure: publication cannot be
    // rolled back, and therefore new resident output cannot be released by catch.
    assert.throws(() => { throw new Error("synthetic stale scope"); }, /stale scope/);
    assert.equal(owner.snapshot().residentBytes, 30);
    assert.equal(owner.snapshot().retiringBytes, 40);
    owner.releaseRetired(old);
    assert.equal(owner.snapshot().ledger.usedBytes, 30);
    assert.throws(() => { throw new Error("synthetic native free failure"); }, /free failure/);
    assert.equal(owner.snapshot().residentBytes, 30, "unproven free lost resident ownership");
    owner.releaseResidentAfterFree(tree);
    empty(owner);
}

function aggregateSnapshotsAndSafeIntegerBoundary(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: Number.MAX_SAFE_INTEGER });
    const tree = { secretPath: "must-not-be-reported" };
    const attempt = reserve(owner, tree, Number.MAX_SAFE_INTEGER);
    assert.equal(owner.reserve({}, { peakBytes: 1, residentBytes: 1 }), null);
    const emptyAttempt = reserve(owner, {}, 0);
    assert.equal(owner.snapshot().ledger.activeLeases, 1);
    owner.releaseRetired(owner.detachPrivate(emptyAttempt));
    owner.ready(attempt);
    owner.releaseRetired(owner.publish(attempt));
    const report = owner.snapshot();
    assert.equal(report.residentBytes, Number.MAX_SAFE_INTEGER);
    assert.equal(JSON.stringify(report).includes("must-not-be-reported"), false);
    assert.deepEqual(Object.keys(report).sort(), ["scope", "ledger", "residentTrees", "privateAttempts",
        "retiringOwners", "privateBytes", "residentBytes", "retiringBytes"].sort());
    report.residentBytes = 0;
    report.ledger.usedBytes = 0;
    assert.equal(owner.snapshot().residentBytes, Number.MAX_SAFE_INTEGER);
    assert.equal(owner.snapshot().ledger.usedBytes, Number.MAX_SAFE_INTEGER);
    owner.releaseResidentAfterFree(tree);
    empty(owner);
}

function independentTreesAndStrongCapabilityOwners(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const a = {}, b = {};
    resident(owner, a, 20);
    resident(owner, b, 30);
    const attempt = reserve(owner, a, 40, 10);
    // This verifies an explicit strong owner edge, not GC behavior or eventual
    // collection. Copying the edge still cannot copy its WeakMap authority.
    assert(Object.getOwnPropertySymbols(attempt).some(key =>
        Object.getOwnPropertyDescriptor(attempt, key)?.value === owner));
    owner.ready(attempt);
    const old = owner.publish(attempt);
    assert(Object.getOwnPropertySymbols(old).some(key =>
        Object.getOwnPropertyDescriptor(old, key)?.value === owner));
    charges(owner, { privateBytes: 0, residentBytes: 40, retiringBytes: 20,
        privateAttempts: 0, residentTrees: 2, retiringOwners: 1, activeLeases: 3 });
    owner.releaseResidentAfterFree(b);
    assert.equal(owner.snapshot().ledger.usedBytes, 30);
    owner.releaseRetired(old);
    assert.equal(owner.snapshot().ledger.usedBytes, 10);
    owner.releaseResidentAfterFree(a);
    empty(owner);
}

function reentrantPlanValidationCannotClobberAConcurrentOwner(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = {};
    let nested: RootTreeResidentAttempt | undefined;
    const plan = new Proxy({ peakBytes: 40, residentBytes: 30 }, {
        ownKeys(target) {
            nested = reserve(owner, tree, 20);
            return Reflect.ownKeys(target);
        },
    });
    assert.throws(() => owner.reserve(tree, plan), RootTreeResidentStateError);
    assert.equal(owner.snapshot().privateBytes, 20);
    assert.equal(owner.snapshot().privateAttempts, 1);
    owner.releaseRetired(owner.detachPrivate(nested!));
    empty(owner);
    const closePlan = new Proxy({ peakBytes: 0, residentBytes: 0 }, {
        ownKeys(target) { owner.close(); return Reflect.ownKeys(target); },
    });
    assert.equal(owner.reserve(tree, closePlan), null, "plan trap reopened closed owner");
    empty(owner);
}

function noOutputProofReleasesOnlyUnstartedPrivateOwnership(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = {};
    for (const peak of [0, 70]) {
        const attempt = reserve(owner, tree, peak);
        owner.releaseBeforeOutput(attempt);
        empty(owner);
        assert.throws(() => owner.releaseBeforeOutput(attempt), RootTreeResidentStateError);
        assert.throws(() => owner.ready(attempt), RootTreeResidentStateError);
        assert.throws(() => owner.publish(attempt), RootTreeResidentStateError);
        assert.throws(() => owner.detachPrivate(attempt), RootTreeResidentStateError);
    }
    resident(owner, tree, 40);
    const attempt = reserve(owner, tree, 60, 30);
    owner.releaseBeforeOutput(attempt);
    charges(owner, { privateBytes: 0, residentBytes: 40, retiringBytes: 0,
        privateAttempts: 0, residentTrees: 1, retiringOwners: 0, activeLeases: 1 });
    const started = reserve(owner, tree, 60, 20);
    owner.ready(started);
    assert.throws(() => owner.releaseBeforeOutput(started), RootTreeResidentStateError,
        "Ready owner cannot claim output allocation never began");
    assert.equal(owner.snapshot().ledger.usedBytes, 60);
    const retirement = owner.detachPrivate(started);
    assert.throws(() => owner.releaseBeforeOutput(started), RootTreeResidentStateError);
    owner.releaseRetired(retirement);
    const published = reserve(owner, tree, 20);
    owner.ready(published);
    const prior = owner.publish(published);
    assert.throws(() => owner.releaseBeforeOutput(published), RootTreeResidentStateError);
    owner.releaseRetired(prior);
    owner.releaseResidentAfterFree(tree);
    empty(owner);

    const other = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const foreign = reserve(other, {}, 30);
    const copied = Object.freeze(Object.defineProperties({}, Object.getOwnPropertyDescriptors(foreign))) as RootTreeResidentAttempt;
    assert.throws(() => owner.releaseBeforeOutput(foreign), RootTreeResidentStateError);
    assert.throws(() => other.releaseBeforeOutput(copied), RootTreeResidentStateError);
    assert.equal(other.snapshot().ledger.usedBytes, 30);
    other.close();
    other.releaseBeforeOutput(foreign);
    empty(other);
}

function mutation(owner: RootTreeResidentAdmission, tree: object, peakBytes: number,
    residentBytes = peakBytes): RootTreeResidentAttempt {
    const attempt = owner.reserveCandidateMutation(tree, { peakBytes, residentBytes });
    assert(attempt, "fixture expected candidate mutation admission");
    assert(!(attempt instanceof Promise));
    return attempt;
}

class SettlementTree {
    committed = 5;
    candidate = 7;
    open = true;
    target = 90;
    malformed = false;
    fail = false;
    duringNative: (() => void) | undefined;

    tree_version(): number { return 2; }
    committed_revision(): number { return this.committed; }
    candidate_revision(): number { return this.candidate; }
    has_candidate(): boolean { return this.open; }
    commit_candidate(): never { throw new Error("legacy commit used"); }
    abort_candidate(): never { throw new Error("legacy abort used"); }
    commit_candidate_output_settlement_v1(): unknown { return this.settle("commit"); }
    abort_candidate_output_settlement_v1(): unknown { return this.settle("abort"); }

    private settle(outcome: "commit" | "abort"): unknown {
        if (this.fail) throw new Error("native settlement refused");
        this.duringNative?.();
        if (outcome === "commit") this.committed++;
        this.candidate++;
        this.open = false;
        if (this.malformed) return null;
        const endpoint = Math.min(10, this.target);
        const payload = this.target - endpoint;
        return { schema: 1, scope: "v2-stable-tree-output", outcome, treeVersion: 2,
            before: this.target === 0 ? 2 : 8, reachable: this.target === 0 ? 0 : 5,
            removed: this.target === 0 ? 2 : 3, after: this.target === 0 ? 0 : 5,
            bytesRemoved: 30, committedRevision: this.committed,
            candidateRevision: this.candidate, countersValid: true,
            nodePayloadBytes: payload, rangeEndpointResidentRequestedBytes: endpoint,
            residentAdmissionBytes: this.target };
    }
}

class V1SettlementTree {
    committed = 5;
    candidate = 7;
    open = true;

    tree_version(): number { return 1; }
    committed_revision(): number { return this.committed; }
    candidate_revision(): number { return this.candidate; }
    has_candidate(): boolean { return this.open; }
    commit_candidate(): unknown { return this.settle("commit"); }
    abort_candidate(): unknown { return this.settle("abort"); }

    private settle(outcome: "commit" | "abort"): unknown {
        if (outcome === "commit") this.committed++;
        this.candidate++;
        this.open = false;
        return { before: 8, reachable: 5, removed: 3, after: 5, bytes_removed: 30 };
    }
}

function additiveOutputsAggregateAndMoveTogetherOnReplacement(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = {};
    resident(owner, tree, 60);
    assert.equal(owner.reserveCandidateMutation(tree, { peakBytes: 41, residentBytes: 1 }), null);
    const first = mutation(owner, tree, 40, 25);
    charges(owner, { privateBytes: 40, residentBytes: 60, retiringBytes: 0,
        privateAttempts: 1, residentTrees: 1, retiringOwners: 0, activeLeases: 2 });
    assert.throws(() => owner.attachCandidateMutation(first), RootTreeResidentStateError);
    owner.readyCandidateMutation(first, 25);
    owner.attachCandidateMutation(first);
    charges(owner, { privateBytes: 0, residentBytes: 85, retiringBytes: 0,
        privateAttempts: 0, residentTrees: 1, retiringOwners: 0, activeLeases: 1 });
    const second = mutation(owner, tree, 15, 10);
    owner.readyCandidateMutation(second, 10); owner.attachCandidateMutation(second);
    charges(owner, { privateBytes: 0, residentBytes: 95, retiringBytes: 0,
        privateAttempts: 0, residentTrees: 1, retiringOwners: 0, activeLeases: 1 });
    assert.equal(owner.reserveCandidateMutation(tree, { peakBytes: 6, residentBytes: 1 }), null);
    assert.equal(owner.reserve(tree, { peakBytes: 6, residentBytes: 1 }), null);

    const replacement = reserve(owner, tree, 5, 4);
    owner.ready(replacement);
    const prior = owner.publish(replacement);
    charges(owner, { privateBytes: 0, residentBytes: 4, retiringBytes: 95,
        privateAttempts: 0, residentTrees: 1, retiringOwners: 1, activeLeases: 2 });
    assert.throws(() => owner.reserveCandidateMutation(tree, { peakBytes: 0, residentBytes: 0 }),
        RootTreeResidentStateError, "new mutation crossed unresolved whole-tree retirement");
    assert.throws(() => { throw new Error("native retirement rejected"); }, /retirement rejected/);
    assert.equal(owner.snapshot().retiringBytes, 95, "rejected native cleanup lost cohorts");
    owner.releaseRetired(prior);
    charges(owner, { privateBytes: 0, residentBytes: 4, retiringBytes: 0,
        privateAttempts: 0, residentTrees: 1, retiringOwners: 0, activeLeases: 1 });
    owner.releaseResidentAfterFree(tree);
    empty(owner);
}

function mutationCancellationRetainsOnlyItsPrivateCohortUntilProof(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = {};
    resident(owner, tree, 20);
    for (const ready of [false, true]) {
        const attempt = mutation(owner, tree, 70, 30);
        if (ready) owner.readyCandidateMutation(attempt, 30);
        const retirement = owner.detachPrivate(attempt);
        charges(owner, { privateBytes: 0, residentBytes: 20, retiringBytes: ready ? 30 : 70,
            privateAttempts: 0, residentTrees: 1, retiringOwners: 1, activeLeases: 2 });
        assert.throws(() => owner.attachCandidateMutation(attempt), RootTreeResidentStateError);
        assert.throws(() => owner.releaseResidentAfterFree(tree), RootTreeResidentStateError);
        owner.releaseRetired(retirement);
        assert.equal(owner.snapshot().residentBytes, 20);
    }
    const unstarted = mutation(owner, tree, 60);
    owner.releaseBeforeOutput(unstarted);
    assert.equal(owner.snapshot().ledger.usedBytes, 20);
    assert.throws(() => owner.releaseBeforeOutput(unstarted), RootTreeResidentStateError);
    assert.throws(() => owner.attachCandidateMutation(unstarted), RootTreeResidentStateError);
    owner.releaseResidentAfterFree(tree); empty(owner);
}

function attachedCohortsSurviveOuterCandidateAndFreeFailures(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = {};
    resident(owner, tree, 20);
    const attached = mutation(owner, tree, 40, 30);
    owner.readyCandidateMutation(attached, 30); owner.attachCandidateMutation(attached);
    for (const reason of ["candidate abort", "candidate commit", "scope failure", "free failure"]) {
        // No such event proves full-tree native output retirement. In
        // particular, neither candidate outcome exposes a release capability.
        assert.throws(() => { throw new Error(reason); }, error => (error as Error).message === reason);
        assert.throws(() => owner.detachPrivate(attached), RootTreeResidentStateError);
        assert.throws(() => owner.releaseBeforeOutput(attached), RootTreeResidentStateError);
        charges(owner, { privateBytes: 0, residentBytes: 50, retiringBytes: 0,
            privateAttempts: 0, residentTrees: 1, retiringOwners: 0, activeLeases: 1 });
    }
    const pending = mutation(owner, tree, 50, 10);
    owner.setCapacity(1); owner.close();
    owner.readyCandidateMutation(pending, 10); owner.attachCandidateMutation(pending);
    assert.equal(owner.snapshot().residentBytes, 60);
    assert.equal(owner.snapshot().ledger.overcommittedBytes, 59);
    assert.equal(owner.reserveCandidateMutation(tree, { peakBytes: 0, residentBytes: 0 }), null);
    assert.equal(owner.reserve(tree, { peakBytes: 0, residentBytes: 0 }), null);
    owner.releaseResidentAfterFree(tree); empty(owner);
}

function candidateCapabilitiesCannotCrossOwnersOrReplacementKinds(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const other = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = {}, otherTree = {};
    const replacement = reserve(owner, tree, 20);
    assert.throws(() => owner.attachCandidateMutation(replacement), RootTreeResidentStateError);
    owner.ready(replacement);
    assert.throws(() => owner.attachCandidateMutation(replacement), RootTreeResidentStateError);
    owner.releaseRetired(owner.publish(replacement));
    const attempt = mutation(owner, tree, 30, 10);
    const copied = Object.freeze(Object.defineProperties({}, Object.getOwnPropertyDescriptors(attempt))) as RootTreeResidentAttempt;
    for (const invalid of [copied, {}, null, 1] as RootTreeResidentAttempt[]) {
        assert.throws(() => owner.attachCandidateMutation(invalid), RootTreeResidentStateError);
    }
    assert.throws(() => other.attachCandidateMutation(attempt), RootTreeResidentStateError);
    assert.throws(() => owner.reserve(tree, { peakBytes: 1, residentBytes: 1 }), RootTreeResidentStateError);
    assert.throws(() => owner.reserveCandidateMutation(tree, { peakBytes: 1, residentBytes: 1 }), RootTreeResidentStateError);
    assert.throws(() => owner.releaseResidentAfterFree(tree), RootTreeResidentStateError);
    owner.readyCandidateMutation(attempt, 10);
    assert.throws(() => owner.publish(attempt), RootTreeResidentStateError,
        "mutation output must not retire the prior committed resident");
    owner.attachCandidateMutation(attempt);
    for (const action of [() => owner.ready(attempt), () => owner.publish(attempt),
        () => owner.attachCandidateMutation(attempt), () => owner.detachPrivate(attempt)]) {
        assert.throws(action, RootTreeResidentStateError);
    }
    const independentlyAttached = mutation(other, otherTree, 15);
    other.readyCandidateMutation(independentlyAttached, 15); other.attachCandidateMutation(independentlyAttached);
    owner.releaseResidentAfterFree(tree);
    assert.equal(other.snapshot().residentBytes, 15, "free crossed ledger ownership");
    assert.throws(() => owner.reserveCandidateMutation(tree, { peakBytes: 0, residentBytes: 0 }), RootTreeResidentStateError);
    other.releaseResidentAfterFree(otherTree); empty(owner); empty(other);
}

function zeroCohortsAndZeroReplacementKeepExactBarriers(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = {};
    const zero = mutation(owner, tree, 0);
    assert.equal(owner.snapshot().privateAttempts, 1);
    owner.readyCandidateMutation(zero, 0); owner.attachCandidateMutation(zero);
    charges(owner, { privateBytes: 0, residentBytes: 0, retiringBytes: 0,
        privateAttempts: 0, residentTrees: 1, retiringOwners: 0, activeLeases: 0 });
    const positive = mutation(owner, tree, 50, 40);
    owner.readyCandidateMutation(positive, 40); owner.attachCandidateMutation(positive);
    const internals = owner as unknown as { trees: Map<object, { resident: unknown }> };
    const head = internals.trees.get(tree)!.resident;
    for (let index = 0; index < 1_000; index++) {
        const attempt = mutation(owner, tree, 0);
        owner.readyCandidateMutation(attempt, 0); owner.attachCandidateMutation(attempt);
    }
    assert.equal(internals.trees.get(tree)!.resident, head,
        "zero-byte output retained an unbounded history of cohort nodes");
    assert.equal(owner.snapshot().ledger.activeLeases, 1);
    const emptyReplacement = reserve(owner, tree, 0);
    owner.ready(emptyReplacement);
    const retirement = owner.publish(emptyReplacement);
    charges(owner, { privateBytes: 0, residentBytes: 0, retiringBytes: 40,
        privateAttempts: 0, residentTrees: 1, retiringOwners: 1, activeLeases: 1 });
    owner.releaseRetired(retirement);
    owner.releaseResidentAfterFree(tree); empty(owner);
}

function candidatePlanDetachmentReentrancyAndSafeIntegerSums(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: Number.MAX_SAFE_INTEGER });
    const tree = {};
    let getterCalls = 0;
    const invalid = { get peakBytes() { getterCalls++; return 3; }, residentBytes: 2 };
    assert.throws(() => owner.reserveCandidateMutation(tree, invalid), TypeError);
    assert.equal(getterCalls, 0);
    assert.throws(() => owner.reserveCandidateMutation(tree,
        { peakBytes: 3, residentBytes: 2, extra: true } as RootTreeResidentPlan), TypeError);
    let nested: RootTreeResidentAttempt | null = null;
    const reentrant = new Proxy({ peakBytes: 4, residentBytes: 2 }, {
        ownKeys(target) {
            nested = owner.reserveCandidateMutation(tree, { peakBytes: 3, residentBytes: 2 });
            return Reflect.ownKeys(target);
        },
    });
    assert.throws(() => owner.reserveCandidateMutation(tree, reentrant), RootTreeResidentStateError);
    owner.releaseBeforeOutput(nested!); empty(owner);
    resident(owner, tree, Number.MAX_SAFE_INTEGER - 8);
    const plan = { peakBytes: 8, residentBytes: 6 };
    const attempt = owner.reserveCandidateMutation(tree, plan)!;
    plan.peakBytes = 0; plan.residentBytes = 0;
    owner.readyCandidateMutation(attempt, 6); owner.attachCandidateMutation(attempt);
    assert.equal(owner.snapshot().residentBytes, Number.MAX_SAFE_INTEGER - 2);
    assert.equal(owner.reserveCandidateMutation(tree, { peakBytes: 3, residentBytes: 0 }), null);
    const replacement = reserve(owner, tree, 2, 1);
    owner.ready(replacement);
    const retirement = owner.publish(replacement);
    assert.equal(owner.snapshot().retiringBytes, Number.MAX_SAFE_INTEGER - 2);
    assert.equal(owner.snapshot().ledger.usedBytes, Number.MAX_SAFE_INTEGER - 1);
    owner.releaseRetired(retirement);
    assert.equal(owner.snapshot().ledger.usedBytes, 1);
    owner.releaseResidentAfterFree(tree); empty(owner);
}

function candidateReadyRequiresExactBoundedNativeActualBytes(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const other = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = {};
    resident(owner, tree, 20);
    const attempt = mutation(owner, tree, 80, 30);
    assert.throws(() => owner.ready(attempt), RootTreeResidentStateError,
        "mutation Ready must require an explicit native actual amount");
    let coercions = 0;
    for (const invalid of [-1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1,
        31, 80, "15", 1n, null, undefined,
        { [Symbol.toPrimitive]() { coercions++; return 15; } }]) {
        assert.throws(() => owner.readyCandidateMutation(attempt, invalid as number), RangeError);
        assert.equal(owner.snapshot().privateBytes, 80, "invalid actual changed the private allowance");
    }
    assert.equal(coercions, 0);
    const copied = Object.freeze(Object.defineProperties({}, Object.getOwnPropertyDescriptors(attempt))) as RootTreeResidentAttempt;
    assert.throws(() => owner.readyCandidateMutation(copied, 15), RootTreeResidentStateError);
    assert.throws(() => other.readyCandidateMutation(attempt, 15), RootTreeResidentStateError);
    owner.readyCandidateMutation(attempt, 15);
    charges(owner, { privateBytes: 15, residentBytes: 20, retiringBytes: 0,
        privateAttempts: 1, residentTrees: 1, retiringOwners: 0, activeLeases: 2 });
    assert.throws(() => owner.readyCandidateMutation(attempt, 0), RootTreeResidentStateError,
        "a Ready cohort cannot be shrunk twice using another alleged native report");
    const cancelled = owner.detachPrivate(attempt);
    assert.equal(owner.snapshot().retiringBytes, 15,
        "cancellation retained the planned amount instead of the actual Ready allowance");
    owner.releaseRetired(cancelled);

    const deduplicated = mutation(owner, tree, 80, 30);
    owner.readyCandidateMutation(deduplicated, 0);
    charges(owner, { privateBytes: 0, residentBytes: 20, retiringBytes: 0,
        privateAttempts: 1, residentTrees: 1, retiringOwners: 0, activeLeases: 1 });
    owner.attachCandidateMutation(deduplicated);
    assert.equal(owner.snapshot().residentBytes, 20);
    assert.throws(() => owner.readyCandidateMutation(deduplicated, 0), RootTreeResidentStateError);

    const attached = mutation(owner, tree, 80, 30);
    owner.readyCandidateMutation(attached, 12); owner.attachCandidateMutation(attached);
    assert.equal(owner.snapshot().residentBytes, 32, "attached cohort did not use actual native bytes");
    const replacement = reserve(owner, tree, 40, 10);
    assert.throws(() => owner.readyCandidateMutation(replacement, 10), RootTreeResidentStateError);
    owner.ready(replacement);
    assert.throws(() => owner.readyCandidateMutation(replacement, 0), RootTreeResidentStateError);
    owner.releaseRetired(owner.publish(replacement));
    assert.equal(owner.snapshot().residentBytes, 10);
    owner.releaseResidentAfterFree(tree); empty(owner); empty(other);
}

function mutationEpochFencesEqualLookingAdmissionAba(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 10 });
    const tree = {};
    assert.equal(owner.mutationEpoch, 0);
    assert.equal(owner.reserve(tree, { peakBytes: 11, residentBytes: 1 }), null);
    assert.equal(owner.mutationEpoch, 1, "refused reserve did not advance the admission witness");
    owner.setCapacity(20);
    assert.equal(owner.mutationEpoch, 2);
    const attempt = reserve(owner, tree, 10, 5);
    const reserved = owner.mutationEpoch;
    owner.ready(attempt);
    owner.releaseRetired(owner.detachPrivate(attempt));
    assert.equal(owner.mutationEpoch, reserved! + 3,
        "Ready/detach/retire ABA did not leave a distinct admission generation");
    assert.deepEqual(owner.snapshot().ledger, {
        capacityBytes: 20, usedBytes: 0, peakUsedBytes: 10, availableBytes: 20,
        overcommittedBytes: 0, activeLeases: 0, closed: false,
        refusedReservations: 1, refusedGrowths: 0,
    });
    owner.close();
    assert.equal(owner.mutationEpoch, reserved! + 4);

    const saturated = new RootTreeResidentAdmission({ capacityBytes: 10 });
    (saturated as any).mutationEpochValue = Number.MAX_SAFE_INTEGER;
    saturated.setCapacity(9);
    assert.equal(saturated.mutationEpoch, null,
        "exhausted scheduling witness wrapped instead of disabling itself");
    assert.equal(saturated.reserve({}, { peakBytes: 10, residentBytes: 1 }), null);
    saturated.close();
    assert.equal(saturated.mutationEpoch, null,
        "disabled scheduling witness interrupted later ownership operations");

    const publishing = new RootTreeResidentAdmission({ capacityBytes: 10 });
    const publishingTree = {};
    const replacement = reserve(publishing, publishingTree, 5);
    publishing.ready(replacement);
    (publishing as any).mutationEpochValue = Number.MAX_SAFE_INTEGER;
    const retirement = publishing.publish(replacement);
    assert.equal(publishing.mutationEpoch, null,
        "replacement publication did not disable an exhausted witness safely");
    publishing.releaseRetired(retirement);
    publishing.releaseResidentAfterFree(publishingTree);
    publishing.close();

    const attaching = new RootTreeResidentAdmission({ capacityBytes: 10 });
    const attachingTree = {};
    const mutationAttempt = attaching.reserveCandidateMutation(attachingTree, { peakBytes: 5, residentBytes: 5 })!;
    attaching.readyCandidateMutation(mutationAttempt, 5);
    (attaching as any).mutationEpochValue = Number.MAX_SAFE_INTEGER;
    attaching.attachCandidateMutation(mutationAttempt);
    assert.equal(attaching.mutationEpoch, null,
        "mutation attach did not disable an exhausted witness safely");
    attaching.releaseResidentAfterFree(attachingTree);
}

function stableV2SettlementIsExactOneUseAndFailClosed(): void {
    const admitted = new RootTreeResidentAdmission({ capacityBytes: 200 });
    const tree = new SettlementTree();
    resident(admitted, tree, 100);
    admitted.markV2GraphComplete(tree, tree.committed, tree.candidate);
    const output = mutation(admitted, tree, 40, 40);
    admitted.readyCandidateMutation(output, 40);
    admitted.attachCandidateMutation(output);
    tree.candidate++;
    assert.equal(admitted.advanceV2CandidateRevision(tree, tree.committed, tree.candidate), true);
    assert.equal(admitted.snapshot().ledger.activeLeases, 1,
        "attached output was not absorbed into one aggregate lease");
    const witness = captureV2CandidateSettlementWitness(tree)!;
    const ticket = admitted.prepareV2Settlement(tree, "commit",
        witness.committedRevision, witness.candidateRevision)!;
    const result = settleTreeCandidateOutput(tree, "commit", witness);
    assert(result.proof);
    assert.equal(admitted.settleAfterV2Sweep(ticket, result.proof), true);
    charges(admitted, { privateBytes: 0, residentBytes: 90, retiringBytes: 0,
        privateAttempts: 0, residentTrees: 1, retiringOwners: 0, activeLeases: 1 });
    assert.equal(admitted.settleAfterV2Sweep(ticket, result.proof), false,
        "settlement ticket/proof replay succeeded");
    admitted.releaseResidentAfterFree(tree);
    empty(admitted);

    const oversized = new RootTreeResidentAdmission({ capacityBytes: 200 });
    const oversizedTree = new SettlementTree();
    oversizedTree.target = 120;
    resident(oversized, oversizedTree, 100);
    oversized.markV2GraphComplete(oversizedTree, 5, 7);
    const oversizedWitness = captureV2CandidateSettlementWitness(oversizedTree)!;
    const oversizedTicket = oversized.prepareV2Settlement(oversizedTree, "abort", 5, 7)!;
    const oversizedResult = settleTreeCandidateOutput(oversizedTree, "abort", oversizedWitness);
    assert(oversizedResult.proof);
    assert.equal(oversized.settleAfterV2Sweep(oversizedTicket, oversizedResult.proof), false,
        "post-sweep target grew an already-resident allowance");
    assert.equal(oversized.snapshot().residentBytes, 100);
    assert.equal(oversized.prepareV2Settlement(oversizedTree, "abort", 5, 8), null,
        "failed settlement retained complete provenance");
    oversized.releaseResidentAfterFree(oversizedTree);

    const malformed = new RootTreeResidentAdmission({ capacityBytes: 200 });
    const malformedTree = new SettlementTree();
    malformedTree.malformed = true;
    resident(malformed, malformedTree, 100);
    malformed.markV2GraphComplete(malformedTree, 5, 7);
    const malformedWitness = captureV2CandidateSettlementWitness(malformedTree)!;
    const malformedTicket = malformed.prepareV2Settlement(malformedTree, "commit", 5, 7)!;
    const malformedResult = settleTreeCandidateOutput(malformedTree, "commit", malformedWitness);
    assert.equal(malformedResult.proof, null);
    malformed.abandonV2SettlementAfterSweep(malformedTicket);
    assert.equal(malformed.snapshot().residentBytes, 100,
        "malformed post-success report released resident bytes");
    malformed.releaseResidentAfterFree(malformedTree);

    const retried = new RootTreeResidentAdmission({ capacityBytes: 200 });
    const retriedTree = new SettlementTree();
    retriedTree.fail = true;
    resident(retried, retriedTree, 100);
    retried.markV2GraphComplete(retriedTree, 5, 7);
    let retryWitness = captureV2CandidateSettlementWitness(retriedTree)!;
    let retryTicket = retried.prepareV2Settlement(retriedTree, "commit", 5, 7)!;
    assert.throws(() => settleTreeCandidateOutput(retriedTree, "commit", retryWitness), /refused/);
    retried.cancelV2SettlementBeforeSweep(retryTicket);
    retriedTree.fail = false;
    retryWitness = captureV2CandidateSettlementWitness(retriedTree)!;
    retryTicket = retried.prepareV2Settlement(retriedTree, "commit", 5, 7)!;
    const retryResult = settleTreeCandidateOutput(retriedTree, "commit", retryWitness);
    assert(retryResult.proof);
    assert.equal(retried.settleAfterV2Sweep(retryTicket, retryResult.proof), true);
    assert.equal(retried.snapshot().residentBytes, 90);
    retried.releaseResidentAfterFree(retriedTree);

    const interfered = new RootTreeResidentAdmission({ capacityBytes: 200 });
    const interferedTree = new SettlementTree();
    resident(interfered, interferedTree, 100);
    interfered.markV2GraphComplete(interferedTree, 5, 7);
    const interferedWitness = captureV2CandidateSettlementWitness(interferedTree)!;
    const interferedTicket = interfered.prepareV2Settlement(interferedTree, "abort", 5, 7)!;
    interferedTree.duringNative = () => interfered.setCapacity(199);
    const interferedResult = settleTreeCandidateOutput(interferedTree, "abort", interferedWitness);
    assert(interferedResult.proof);
    assert.equal(interfered.settleAfterV2Sweep(interferedTicket, interferedResult.proof), false,
        "intervening admission mutation did not fence settlement");
    assert.equal(interfered.snapshot().residentBytes, 100);
    interfered.releaseResidentAfterFree(interferedTree);
}

function rootClone(owner: RootTreeResidentAdmission, tree: object,
    total: number, identity: number): RootTreeResidentAttempt {
    const token = owner.reserveCandidateRoot(tree, total, identity);
    assert(token, "fixture candidate root admission refused");
    owner.readyCandidateRoot(token);
    owner.attachCandidateRoot(token);
    return token;
}

function terminalRoot(owner: RootTreeResidentAdmission, tree: SettlementTree,
    outcome: "commit" | "abort"): boolean {
    const witness = captureV2CandidateSettlementWitness(tree)!;
    const ticket = owner.prepareV2Settlement(tree, outcome,
        witness.committedRevision, witness.candidateRevision);
    assert(ticket, "attached root must receive a terminal ticket even without exact graph provenance");
    const result = settleTreeCandidateOutput(tree, outcome, witness);
    if (result.proof) return owner.settleAfterV2Sweep(ticket, result.proof);
    owner.abandonV2SettlementAfterSweep(ticket);
    return false;
}

function openNextRoot(owner: RootTreeResidentAdmission, tree: SettlementTree,
    total: number, identity: number): RootTreeResidentAttempt {
    assert.equal(tree.open, false);
    const token = rootClone(owner, tree, total, identity);
    tree.open = true; tree.candidate++;
    owner.advanceV2CandidateRevision(tree, tree.committed, tree.candidate);
    return token;
}

function rootPlansHaveSeparateReadyAndExactOwnership(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const other = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = {};
    resident(owner, tree, 70);
    let coercions = 0;
    for (const invalid of [-1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1,
        "1", 1n, null, undefined, { valueOf() { coercions++; return 1; } }]) {
        assert.throws(() => owner.reserveCandidateRoot(tree, invalid as number, 0), RangeError);
        assert.throws(() => owner.reserveCandidateRoot(tree, 20, invalid as number), RangeError);
    }
    assert.equal(coercions, 0);
    assert.throws(() => owner.reserveCandidateRoot(tree, 10, 11), RangeError);
    assert.equal(owner.reserveCandidateRoot(tree, 31, 2), null);
    const token = owner.reserveCandidateRoot(tree, 30, 7)!;
    charges(owner, { privateBytes: 30, residentBytes: 70, retiringBytes: 0,
        privateAttempts: 1, residentTrees: 1, retiringOwners: 0, activeLeases: 2 });
    for (const action of [() => owner.attachCandidateRoot(token), () => owner.ready(token),
        () => owner.readyCandidateMutation(token, 30), () => owner.publish(token),
        () => other.readyCandidateRoot(token)]) assert.throws(action, RootTreeResidentStateError);
    const copied = Object.freeze(Object.defineProperties({}, Object.getOwnPropertyDescriptors(token))) as RootTreeResidentAttempt;
    assert.throws(() => owner.readyCandidateRoot(copied), RootTreeResidentStateError);
    owner.readyCandidateRoot(token);
    assert.equal(owner.snapshot().privateBytes, 30, "root Ready silently shrank exact clone request");
    assert.throws(() => owner.readyCandidateRoot(token), RootTreeResidentStateError);
    assert.throws(() => owner.attachCandidateMutation(token), RootTreeResidentStateError);
    assert.throws(() => owner.publish(token), RootTreeResidentStateError);
    owner.attachCandidateRoot(token);
    assert.equal(owner.snapshot().residentBytes, 100);
    assert.equal(owner.snapshot().ledger.activeLeases, 1);
    assert.throws(() => owner.reserveCandidateRoot(tree, 0, 0), RootTreeResidentStateError);
    for (const action of [() => owner.attachCandidateRoot(token), () => owner.readyCandidateRoot(token),
        () => owner.detachPrivate(token), () => owner.releaseBeforeOutput(token)]) {
        assert.throws(action, RootTreeResidentStateError);
    }
    owner.releaseResidentAfterFree(tree); empty(owner);
    assert.throws(() => owner.reserveCandidateRoot(tree, 0, 0), RootTreeResidentStateError);

    for (const kind of ["replacement", "mutation"] as const) {
        const foreignKind = kind === "replacement" ? reserve(other, {}, 10) : mutation(other, {}, 10);
        assert.throws(() => other.readyCandidateRoot(foreignKind), RootTreeResidentStateError);
        assert.throws(() => other.attachCandidateRoot(foreignKind), RootTreeResidentStateError);
        other.releaseBeforeOutput(foreignKind);
    }
    empty(other);
}

function rootCancellationRetainsUntilExactPrivateRetirement(): void {
    for (const ready of [false, true]) {
        const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
        const tree = {};
        resident(owner, tree, 40);
        const token = owner.reserveCandidateRoot(tree, 60, 12)!;
        if (ready) owner.readyCandidateRoot(token);
        owner.setCapacity(1); owner.close();
        const retirement = owner.detachPrivate(token);
        charges(owner, { privateBytes: 0, residentBytes: 40, retiringBytes: 60,
            privateAttempts: 0, residentTrees: 1, retiringOwners: 1, activeLeases: 2 });
        assert.throws(() => owner.releaseResidentAfterFree(tree), RootTreeResidentStateError);
        assert.throws(() => owner.attachCandidateRoot(token), RootTreeResidentStateError);
        owner.releaseRetired(retirement);
        assert.equal(owner.snapshot().residentBytes, 40);
        assert.equal(owner.reserveCandidateRoot(tree, 0, 0), null);
        owner.releaseResidentAfterFree(tree); empty(owner);
    }
    const unstarted = new RootTreeResidentAdmission({ capacityBytes: 10 });
    const tree = {};
    const token = unstarted.reserveCandidateRoot(tree, 10, 2)!;
    unstarted.releaseBeforeOutput(token); empty(unstarted);
    assert.throws(() => unstarted.releaseBeforeOutput(token), RootTreeResidentStateError);
    const resumed = unstarted.reserveCandidateRoot(tree, 10, 2)!;
    unstarted.readyCandidateRoot(resumed);
    assert.throws(() => unstarted.releaseBeforeOutput(resumed), RootTreeResidentStateError);
    unstarted.releaseRetired(unstarted.detachPrivate(resumed)); empty(unstarted);
}

function rootTerminalFallbackKeepsOnlyTwoSlots(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 300 });
    const tree = new SettlementTree();
    resident(owner, tree, 100);
    rootClone(owner, tree, 30, 7);
    assert.equal(terminalRoot(owner, tree, "commit"), false, "unproven graph granted exact shrink");
    assert.equal(owner.snapshot().residentBytes, 130);
    for (let generation = 0; generation < 1_000; generation++) {
        const total = 20 + generation % 3;
        openNextRoot(owner, tree, total, 5);
        const rootBytesBefore = owner.snapshot().residentBytes;
        tree.malformed = generation % 2 === 0;
        assert.equal(terminalRoot(owner, tree, "abort"), false);
        assert.equal(owner.snapshot().residentBytes, rootBytesBefore - total,
            "abort released prior committed identity or retained candidate C");
        openNextRoot(owner, tree, total, 5);
        assert.equal(terminalRoot(owner, tree, "commit"), false);
        assert.equal(owner.snapshot().residentBytes, 100 + total, "commits accumulated root generations");
        assert.equal(owner.snapshot().ledger.activeLeases, 1);
    }
    const live = openNextRoot(owner, tree, 13, 3);
    // Root attachment is independent of short-lived mutation attempts.
    const output = mutation(owner, tree, 40, 40);
    owner.readyCandidateMutation(output, 40); owner.attachCandidateMutation(output);
    assert.throws(() => owner.attachCandidateRoot(live), RootTreeResidentStateError);
    assert.equal(terminalRoot(owner, tree, "abort"), false);
    assert.equal(owner.snapshot().residentBytes, 160, "root abort incorrectly swept mutation output");
    owner.releaseResidentAfterFree(tree); empty(owner);
}

function exactRootSettlementIncludesSurvivingIdentity(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 400 });
    const tree = new SettlementTree();
    resident(owner, tree, 100);
    owner.markV2GraphComplete(tree, tree.committed, tree.candidate);
    rootClone(owner, tree, 30, 7);
    assert.equal(terminalRoot(owner, tree, "commit"), true,
        "terminal root ownership invalidated its own pre-native epoch proof");
    assert.equal(owner.snapshot().residentBytes, 97);
    openNextRoot(owner, tree, 25, 8);
    const epoch = owner.mutationEpoch;
    assert.equal(terminalRoot(owner, tree, "abort"), true);
    assert.equal(owner.snapshot().residentBytes, 97, "abort must keep the original committed I=7");
    assert(owner.mutationEpoch! > epoch!);
    openNextRoot(owner, tree, 25, 8);
    tree.target = 85;
    assert.equal(terminalRoot(owner, tree, "commit"), true);
    assert.equal(owner.snapshot().residentBytes, 93, "commit did not replace I=7 with I=8");
    openNextRoot(owner, tree, 10, 0);
    tree.target = 0;
    assert.equal(terminalRoot(owner, tree, "commit"), true);
    assert.equal(owner.snapshot().residentBytes, 0);
    assert.equal(owner.snapshot().ledger.activeLeases, 0);
    openNextRoot(owner, tree, 0, 0);
    assert.equal(terminalRoot(owner, tree, "abort"), true);
    assert.equal(owner.snapshot().residentTrees, 1, "zero root lost its tree/free proof barrier");
    owner.releaseResidentAfterFree(tree); empty(owner);
}

function rootSettlementRejectionAndNativeErrorPreserveProperOwners(): void {
    for (const invalid of ["epoch", "malformed", "oversized", "saturated"] as const) {
        const owner = new RootTreeResidentAdmission({ capacityBytes: 300 });
        const tree = new SettlementTree();
        resident(owner, tree, 100);
        owner.markV2GraphComplete(tree, tree.committed, tree.candidate);
        rootClone(owner, tree, 30, 7);
        assert.equal(terminalRoot(owner, tree, "commit"), true);
        openNextRoot(owner, tree, 40, 9);
        if (invalid === "epoch") tree.duringNative = () => owner.setCapacity(1);
        if (invalid === "malformed") tree.malformed = true;
        if (invalid === "oversized") tree.target = 122; // 122+9 exceeds post-terminal 137-7=130.
        if (invalid === "saturated") (owner as any).mutationEpochValue = null;
        assert.equal(terminalRoot(owner, tree, "commit"), false, invalid);
        assert.equal(owner.snapshot().residentBytes, 130, "fallback failed to drop exactly prior I=7");
        owner.releaseResidentAfterFree(tree); empty(owner);
    }
    const owner = new RootTreeResidentAdmission({ capacityBytes: 300 });
    const tree = new SettlementTree();
    resident(owner, tree, 100);
    rootClone(owner, tree, 30, 7);
    const witness = captureV2CandidateSettlementWitness(tree)!;
    const ticket = owner.prepareV2Settlement(tree, "abort", tree.committed, tree.candidate)!;
    assert(ticket);
    tree.fail = true;
    assert.throws(() => settleTreeCandidateOutput(tree, "abort", witness), /refused/);
    owner.cancelV2SettlementBeforeSweep(ticket);
    assert.equal(owner.snapshot().residentBytes, 130, "throwing native outcome released candidate clone");
    assert.throws(() => owner.reserveCandidateRoot(tree, 1, 1), RootTreeResidentStateError);
    tree.fail = false;
    assert.equal(terminalRoot(owner, tree, "abort"), false);
    assert.equal(owner.snapshot().residentBytes, 100);
    assert.throws(() => owner.abandonV2SettlementAfterSweep(ticket), RootTreeResidentStateError);
    owner.releaseResidentAfterFree(tree); empty(owner);
}

function rootReplacementAndZeroSlotsKeepRetirementOwnership(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 300 });
    const tree = new SettlementTree();
    resident(owner, tree, 100);
    rootClone(owner, tree, 30, 7);
    terminalRoot(owner, tree, "commit");
    openNextRoot(owner, tree, 40, 9);
    const replacement = reserve(owner, tree, 0);
    owner.ready(replacement);
    const retired = owner.publish(replacement);
    charges(owner, { privateBytes: 0, residentBytes: 0, retiringBytes: 170,
        privateAttempts: 0, residentTrees: 1, retiringOwners: 1, activeLeases: 1 });
    assert.throws(() => owner.reserveCandidateRoot(tree, 0, 0), RootTreeResidentStateError);
    owner.releaseRetired(retired);
    assert.equal(owner.prepareV2Settlement(tree, "commit", tree.committed, tree.candidate), null,
        "replacement retained old candidate or committed metadata slots");
    const zero = owner.reserveCandidateRoot(tree, 0, 0)!;
    owner.readyCandidateRoot(zero); owner.attachCandidateRoot(zero);
    owner.close();
    tree.target = 0;
    assert.equal(terminalRoot(owner, tree, "commit"), false);
    assert.equal(owner.snapshot().ledger.usedBytes, 0);
    assert.equal(owner.snapshot().residentTrees, 1);
    owner.releaseResidentAfterFree(tree); empty(owner);
}

function rootTerminalAdapterAndOverflowRemainFailClosed(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = new SettlementTree();
    resident(owner, tree, 50);
    rootClone(owner, tree, 10, 3);
    tree.malformed = true;
    const committed = settleTreeCandidateOutputWithAdmission(tree, "commit", owner);
    assert.equal(committed.proof, null);
    assert.equal(owner.snapshot().residentBytes, 60, "existing adapter lost conservative committed slot");
    openNextRoot(owner, tree, 20, 4);
    settleTreeCandidateOutputWithAdmission(tree, "abort", owner);
    assert.equal(owner.snapshot().residentBytes, 60, "existing adapter omitted root fallback on missing proof");
    tree.malformed = false;
    openNextRoot(owner, tree, 15, 4);
    tree.target = 20;
    settleTreeCandidateOutputWithAdmission(tree, "commit", owner);
    assert.equal(owner.snapshot().residentBytes, 65, "unproven graph shrank using an otherwise-valid report");
    owner.releaseResidentAfterFree(tree); empty(owner);

    const maximum = new RootTreeResidentAdmission({ capacityBytes: Number.MAX_SAFE_INTEGER });
    const hugeTree = new SettlementTree();
    resident(maximum, hugeTree, Number.MAX_SAFE_INTEGER - 1);
    maximum.markV2GraphComplete(hugeTree, hugeTree.committed, hugeTree.candidate);
    rootClone(maximum, hugeTree, 1, 1);
    hugeTree.target = Number.MAX_SAFE_INTEGER;
    assert.equal(terminalRoot(maximum, hugeTree, "commit"), false,
        "unsafe sum of stable output plus identity authorized exact settlement");
    assert.equal(maximum.snapshot().residentBytes, Number.MAX_SAFE_INTEGER);
    maximum.releaseResidentAfterFree(hugeTree); empty(maximum);
}

function v1RootTerminalAdapterRetiresOnlyProvenCloneSlots(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = new V1SettlementTree();
    resident(owner, tree, 50);
    rootClone(owner, tree, 10, 3);
    settleTreeCandidateOutputWithAdmission(tree, "commit", owner);
    assert.equal(owner.snapshot().residentBytes, 60,
        "Tree v1 first candidate root was not promoted to the committed slot");

    tree.open = true;
    rootClone(owner, tree, 20, 4);
    settleTreeCandidateOutputWithAdmission(tree, "commit", owner);
    assert.equal(owner.snapshot().residentBytes, 70,
        "Tree v1 commit did not retire the previous proven root clone");

    tree.open = true;
    rootClone(owner, tree, 15, 4);
    settleTreeCandidateOutputWithAdmission(tree, "abort", owner);
    assert.equal(owner.snapshot().residentBytes, 70,
        "Tree v1 abort did not retire only the candidate root clone");
    owner.releaseResidentAfterFree(tree); empty(owner);
}

function v1MutationAtomicallyReplacesCandidateRootCharge(): void {
    const owner = new RootTreeResidentAdmission({ capacityBytes: 200 });
    const tree = new V1SettlementTree();
    resident(owner, tree, 50);
    rootClone(owner, tree, 10, 3);

    const first = mutation(owner, tree, 25, 19);
    owner.readyCandidateMutation(first, 19); // staged 7 + new root 12
    assert.equal(owner.snapshot().residentBytes, 60);
    owner.attachV1CandidateMutation(first, 12);
    assert.equal(owner.snapshot().residentBytes, 69,
        "V1 mutation retained both old and replacement candidate roots");
    settleTreeCandidateOutputWithAdmission(tree, "abort", owner);
    assert.equal(owner.snapshot().residentBytes, 57,
        "V1 abort released additive staged chunks or retained the new root");

    tree.open = true;
    rootClone(owner, tree, 11, 4);
    const second = mutation(owner, tree, 20, 18);
    owner.readyCandidateMutation(second, 18); // staged 5 + new root 13
    owner.attachV1CandidateMutation(second, 13);
    assert.equal(owner.snapshot().residentBytes, 75);
    settleTreeCandidateOutputWithAdmission(tree, "commit", owner);
    assert.equal(owner.snapshot().residentBytes, 75,
        "V1 commit did not promote the exact replacement root slot");

    tree.open = true;
    rootClone(owner, tree, 9, 3);
    const invalid = mutation(owner, tree, 8, 8);
    owner.readyCandidateMutation(invalid, 8);
    assert.throws(() => owner.attachV1CandidateMutation(invalid, 9), RootTreeResidentStateError);
    owner.releaseRetired(owner.detachPrivate(invalid));
    settleTreeCandidateOutputWithAdmission(tree, "abort", owner);
    owner.releaseResidentAfterFree(tree); empty(owner);
}

exactPlansAreDetachedBeforeOwnership();
failFastOldPlusNewAdmissionAndShrink();
zeroOutputStillRequiresRetirementProof();
privateCancellationRetainsExactCurrentAllowance();
capacityShrinkAndCloseNeverForgetOwners();
capabilitiesAreOwnerLocalAndSingleUse();
postPublishFailureAndFreeRequireExplicitProof();
aggregateSnapshotsAndSafeIntegerBoundary();
independentTreesAndStrongCapabilityOwners();
reentrantPlanValidationCannotClobberAConcurrentOwner();
noOutputProofReleasesOnlyUnstartedPrivateOwnership();
additiveOutputsAggregateAndMoveTogetherOnReplacement();
mutationCancellationRetainsOnlyItsPrivateCohortUntilProof();
attachedCohortsSurviveOuterCandidateAndFreeFailures();
candidateCapabilitiesCannotCrossOwnersOrReplacementKinds();
zeroCohortsAndZeroReplacementKeepExactBarriers();
candidatePlanDetachmentReentrancyAndSafeIntegerSums();
candidateReadyRequiresExactBoundedNativeActualBytes();
mutationEpochFencesEqualLookingAdmissionAba();
stableV2SettlementIsExactOneUseAndFailClosed();
rootPlansHaveSeparateReadyAndExactOwnership();
rootCancellationRetainsUntilExactPrivateRetirement();
rootTerminalFallbackKeepsOnlyTwoSlots();
exactRootSettlementIncludesSurvivingIdentity();
rootSettlementRejectionAndNativeErrorPreserveProperOwners();
rootReplacementAndZeroSlotsKeepRetirementOwnership();
rootTerminalAdapterAndOverflowRemainFailClosed();
v1RootTerminalAdapterRetiresOnlyProvenCloneSlots();
v1MutationAtomicallyReplacesCandidateRootCharge();
console.log("root-tree-resident-admission.test: 29 requested-output lifecycle/ownership groups passed");
