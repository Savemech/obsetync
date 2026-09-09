import { strict as assert } from "node:assert";
import {
    admitCandidateOpenRoot,
    CandidateOpenRootAdmissionDeniedError,
} from "./candidate-open-root-admission";
import { RootTreeResidentAdmission, RootTreeResidentStateError } from "./root-tree-resident-admission";
import type { CandidateOpenMemoryPlan } from "./tree-candidate-job";

function plan(total = 60, identity = 12): CandidateOpenMemoryPlan {
    return Object.freeze({
        schema: 1,
        scope: "v2-candidate-open-root",
        residentChunkCount: 25_000,
        rootStringCount: total === identity ? 2 : 4,
        rootIdentityRequestedBytes: identity,
        rootEndpointRequestedBytes: total - identity,
        rootStringRequestedBytes: total,
        baselineKeySnapshotRequestedBytes: 0,
        peakAdmissionBytes: total,
        baselineStrategy: "insertion-generation-v1",
    });
}

function seed(admission: RootTreeResidentAdmission, tree: object, bytes: number): void {
    const token = admission.reserve(tree, { peakBytes: bytes, residentBytes: bytes });
    assert(token);
    admission.ready(token);
    admission.releaseRetired(admission.publish(token));
}

function charges(admission: RootTreeResidentAdmission, privateBytes: number,
    residentBytes: number, retiringBytes: number, activeLeases: number): void {
    const snapshot = admission.snapshot();
    assert.equal(snapshot.privateBytes, privateBytes);
    assert.equal(snapshot.residentBytes, residentBytes);
    assert.equal(snapshot.retiringBytes, retiringBytes);
    assert.equal(snapshot.ledger.usedBytes, privateBytes + residentBytes + retiringBytes);
    assert.equal(snapshot.ledger.activeLeases, activeLeases);
}

function empty(admission: RootTreeResidentAdmission): void {
    charges(admission, 0, 0, 0, 0);
    const snapshot = admission.snapshot();
    assert.equal(snapshot.privateAttempts, 0);
    assert.equal(snapshot.retiringOwners, 0);
    assert.equal(snapshot.residentTrees, 0);
}

function exactReservationAndTypedDenialDoNotChargeBaselineKeys(): void {
    const admission = new RootTreeResidentAdmission({ capacityBytes: 99 });
    const tree = {};
    seed(admission, tree, 40);
    assert.throws(() => admitCandidateOpenRoot(admission, tree, plan()), error => {
        assert(error instanceof CandidateOpenRootAdmissionDeniedError);
        assert.equal(error.name, "CandidateOpenRootAdmissionDeniedError");
        assert.equal(error.code, "CANDIDATE_OPEN_ROOT_ADMISSION_DENIED");
        assert.equal(error.requestedBytes, 60);
        assert.equal(error.capacityBytes, 99);
        assert.equal(error.usedBytes, 40);
        return true;
    });
    charges(admission, 0, 40, 0, 1);
    assert.equal(admission.snapshot().privateAttempts, 0);
    admission.setCapacity(100);
    const owner = admitCandidateOpenRoot(admission, tree, plan());
    assert(!(owner instanceof Promise));
    charges(admission, 60, 40, 0, 2);
    assert.equal(admission.snapshot().ledger.peakUsedBytes, 100,
        "scalar baseline incorrectly received an all-resident-key allocation allowance");
    owner.releaseBeforeOutput();
    admission.close();
    assert.throws(() => admitCandidateOpenRoot(admission, tree, plan(0, 0)), error =>
        error instanceof CandidateOpenRootAdmissionDeniedError && error.requestedBytes === 0 &&
        error.capacityBytes === 100 && error.usedBytes === 40);
    charges(admission, 0, 40, 0, 1);
    admission.releaseResidentAfterFree(tree);
    empty(admission);
}

function readyAndPublishKeepExactCloneUntilWholeTreeRetires(): void {
    const admission = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = {};
    seed(admission, tree, 40);
    const input = { ...plan() };
    const owner = admitCandidateOpenRoot(admission, tree, input);
    input.rootStringRequestedBytes = 0;
    input.rootIdentityRequestedBytes = 0;
    input.peakAdmissionBytes = 0;
    assert.throws(() => owner.published(), RootTreeResidentStateError);
    assert.throws(() => owner.retired(), RootTreeResidentStateError);
    owner.ready(); owner.ready();
    charges(admission, 60, 40, 0, 2);
    assert.throws(() => owner.releaseBeforeOutput(), RootTreeResidentStateError);
    owner.published(); owner.published();
    charges(admission, 0, 100, 0, 1);
    assert.equal(admission.snapshot().privateAttempts, 0);
    owner.retired(); owner.retired();
    charges(admission, 0, 100, 0, 1);
    for (const invalid of [() => owner.ready(), () => owner.detached(), () => owner.releaseBeforeOutput()]) {
        assert.throws(invalid, RootTreeResidentStateError);
    }
    const replacement = admission.reserve(tree, { peakBytes: 0, residentBytes: 0 })!;
    admission.ready(replacement);
    const retirement = admission.publish(replacement);
    charges(admission, 0, 0, 100, 1);
    owner.retired();
    charges(admission, 0, 0, 100, 1);
    admission.releaseRetired(retirement);
    admission.releaseResidentAfterFree(tree);
    empty(admission);
}

function preOutputReleaseIsIdempotentAndCannotReopen(): void {
    const admission = new RootTreeResidentAdmission({ capacityBytes: 60 });
    const tree = {};
    const owner = admitCandidateOpenRoot(admission, tree, plan());
    owner.releaseBeforeOutput(); owner.releaseBeforeOutput();
    owner.retired(); owner.retired();
    empty(admission);
    for (const invalid of [() => owner.ready(), () => owner.published(), () => owner.detached()]) {
        assert.throws(invalid, RootTreeResidentStateError);
        empty(admission);
    }
    // Releasing unstarted output does not tombstone the live tree wrapper.
    const retry = admitCandidateOpenRoot(admission, tree, plan());
    retry.detached(); retry.retired();
    empty(admission);
}

function privateAndReadyDetachRetainLeaseUntilRetirement(): void {
    for (const ready of [false, true]) {
        const admission = new RootTreeResidentAdmission({ capacityBytes: 100 });
        const tree = {};
        seed(admission, tree, 40);
        const owner = admitCandidateOpenRoot(admission, tree, plan());
        if (ready) owner.ready();
        assert.throws(() => owner.retired(), RootTreeResidentStateError);
        admission.setCapacity(1); admission.close();
        owner.detached(); owner.detached();
        charges(admission, 0, 40, 60, 2);
        assert.equal(admission.snapshot().ledger.overcommittedBytes, 99);
        assert.throws(() => admission.releaseResidentAfterFree(tree), RootTreeResidentStateError);
        for (const invalid of [() => owner.ready(), () => owner.published(), () => owner.releaseBeforeOutput()]) {
            assert.throws(invalid, RootTreeResidentStateError);
            charges(admission, 0, 40, 60, 2);
        }
        owner.retired(); owner.retired();
        charges(admission, 0, 40, 0, 1);
        assert.throws(() => owner.detached(), RootTreeResidentStateError);
        assert.throws(() => owner.published(), RootTreeResidentStateError);
        admission.releaseResidentAfterFree(tree);
        empty(admission);
    }
}

function failedLedgerCallbacksRetainRetryableAdapterState(): void {
    for (const boundary of ["releaseBeforeOutput", "ready", "published", "detached", "retired"] as const) {
        const admission = new RootTreeResidentAdmission({ capacityBytes: 60 });
        const tree = {};
        const owner = admitCandidateOpenRoot(admission, tree, plan());
        if (boundary === "published") owner.ready();
        if (boundary === "retired") owner.detached();
        const method = {
            releaseBeforeOutput: "releaseBeforeOutput",
            ready: "readyCandidateRoot",
            published: "attachCandidateRoot",
            detached: "detachPrivate",
            retired: "releaseRetired",
        }[boundary] as "releaseBeforeOutput" | "readyCandidateRoot" | "attachCandidateRoot" |
            "detachPrivate" | "releaseRetired";
        const original = admission[method];
        const failure = new Error(`isolated ${boundary} failure before ledger transition`);
        Object.defineProperty(admission, method, { configurable: true, value: () => { throw failure; } });
        const before = admission.snapshot();
        assert.throws(() => owner[boundary](), error => error === failure);
        assert.deepEqual(admission.snapshot(), before, "callback failure released or reclassified charged ownership");
        Object.defineProperty(admission, method, { configurable: true, value: original });
        owner[boundary](); owner[boundary]();
        if (boundary === "ready") { owner.detached(); owner.retired(); }
        else if (boundary === "published") { owner.retired(); admission.releaseResidentAfterFree(tree); }
        else if (boundary === "detached") owner.retired();
        empty(admission);
    }
}

function zeroCloneStillRequiresAllOwnershipBarriers(): void {
    const admission = new RootTreeResidentAdmission({ capacityBytes: 1 });
    const tree = {};
    const owner = admitCandidateOpenRoot(admission, tree, plan(0, 0));
    charges(admission, 0, 0, 0, 0);
    assert.equal(admission.snapshot().privateAttempts, 1);
    assert.throws(() => owner.published(), RootTreeResidentStateError);
    owner.ready(); owner.published(); owner.retired();
    assert.equal(admission.snapshot().residentTrees, 1);
    assert.throws(() => admitCandidateOpenRoot(admission, tree, plan(0, 0)), RootTreeResidentStateError);
    admission.releaseResidentAfterFree(tree);
    empty(admission);
    const cancelled = admitCandidateOpenRoot(admission, {}, plan(0, 0));
    cancelled.detached();
    assert.equal(admission.snapshot().retiringOwners, 1);
    cancelled.retired();
    empty(admission);
}

exactReservationAndTypedDenialDoNotChargeBaselineKeys();
readyAndPublishKeepExactCloneUntilWholeTreeRetires();
preOutputReleaseIsIdempotentAndCannotReopen();
privateAndReadyDetachRetainLeaseUntilRetirement();
failedLedgerCallbacksRetainRetryableAdapterState();
zeroCloneStillRequiresAllOwnershipBarriers();
console.log("candidate-open-root-admission.test: 6 exact admission/ownership bridge groups passed");
