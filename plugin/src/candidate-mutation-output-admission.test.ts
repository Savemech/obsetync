import { strict as assert } from "node:assert";
import {
    admitCandidateMutationOutput,
    candidateMutationRefusalAdmissionEpoch,
    CandidateMutationOutputAdmissionDeniedError,
} from "./candidate-mutation-output-admission";
import { RootTreeResidentAdmission, RootTreeResidentStateError } from "./root-tree-resident-admission";
import type {
    CandidateMutationOutputMemoryPlan,
    CandidateMutationOutputMemoryReady,
} from "./tree-candidate-mutation-job";

const plan: CandidateMutationOutputMemoryPlan = Object.freeze({
    schema: 1,
    scope: "v2-candidate-mutation-output",
    nodePayloadBytes: 70,
    rangeEndpointPeakRequestedBytes: 20,
    rangeEndpointResidentRequestedBytes: 10,
    peakAdmissionBytes: 90,
    residentAdmissionBytes: 80,
});
const ready = (resident: number): CandidateMutationOutputMemoryReady => Object.freeze({
    schema: 1,
    scope: "v2-candidate-mutation-output",
    stagedNodePayloadBytes: resident - 10,
    rangeEndpointResidentRequestedBytes: 10,
    residentAdmissionBytes: resident,
});

function publishedCohortUsesActualReadyAndSurvivesResidualRetirement(): void {
    const admission = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const tree = {};
    const owner = admitCandidateMutationOutput(admission, tree, plan);
    assert.equal(admission.snapshot().privateBytes, 90);
    owner.ready(ready(35));
    owner.ready(ready(35));
    assert.equal(admission.snapshot().privateBytes, 35);
    owner.published();
    owner.published();
    owner.retired();
    assert.equal(admission.snapshot().residentBytes, 35,
        "cursor retirement released published candidate output");
    admission.releaseResidentAfterFree(tree);
    assert.equal(admission.snapshot().ledger.usedBytes, 0);
}

function refusalAndPreOutputReleaseAreExact(): void {
    const admission = new RootTreeResidentAdmission({ capacityBytes: 89 });
    let refusal: CandidateMutationOutputAdmissionDeniedError | null = null;
    assert.throws(() => admitCandidateMutationOutput(admission, {}, plan), error => {
        if (error instanceof CandidateMutationOutputAdmissionDeniedError) refusal = error;
        return error instanceof CandidateMutationOutputAdmissionDeniedError &&
            error.requestedBytes === 90 && error.capacityBytes === 89 && error.usedBytes === 0 &&
            candidateMutationRefusalAdmissionEpoch(error) === admission.mutationEpoch;
    });
    assert.equal(admission.snapshot().ledger.usedBytes, 0);

    admission.setCapacity(100);
    assert.notEqual(candidateMutationRefusalAdmissionEpoch(refusal!), admission.mutationEpoch,
        "capacity change remained indistinguishable from the refused admission generation");
    const owner = admitCandidateMutationOutput(admission, {}, plan);
    owner.releaseBeforeOutput();
    owner.releaseBeforeOutput();
    assert.equal(admission.snapshot().ledger.usedBytes, 0);
    assert.throws(() => owner.ready(ready(30)), RootTreeResidentStateError);
}

function cancelledOutputReleasesOnlyAfterNativeRetirementProof(): void {
    const admission = new RootTreeResidentAdmission({ capacityBytes: 100 });
    const owner = admitCandidateMutationOutput(admission, {}, plan);
    owner.ready(ready(25));
    owner.detached();
    owner.detached();
    assert.equal(admission.snapshot().retiringBytes, 25);
    owner.retired();
    owner.retired();
    assert.equal(admission.snapshot().ledger.usedBytes, 0);
    assert.throws(() => owner.published(), RootTreeResidentStateError);
}

function v1BridgeReplacesTrackedCandidateRootInsteadOfAccumulatingIt(): void {
    const admission = new RootTreeResidentAdmission({ capacityBytes: 200 });
    const tree = {};
    const resident = admission.reserve(tree, { peakBytes: 50, residentBytes: 50 })!;
    admission.ready(resident);
    admission.releaseRetired(admission.publish(resident));
    const root = admission.reserveCandidateRoot(tree, 10, 3)!;
    admission.readyCandidateRoot(root); admission.attachCandidateRoot(root);
    const v1Plan: CandidateMutationOutputMemoryPlan = Object.freeze({
        schema: 1, scope: "v1-candidate-mutation-output",
        nodePayloadBytes: 20, rangeEndpointPeakRequestedBytes: 20,
        rangeEndpointResidentRequestedBytes: 10, peakAdmissionBytes: 40,
        residentAdmissionBytes: 30,
    });
    const owner = admitCandidateMutationOutput(admission, tree, v1Plan);
    owner.ready(Object.freeze({ schema: 1, scope: "v1-candidate-mutation-output",
        stagedNodePayloadBytes: 5, rangeEndpointResidentRequestedBytes: 8,
        residentAdmissionBytes: 13 }));
    owner.published();
    assert.equal(admission.snapshot().residentBytes, 63,
        "V1 bridge accumulated the replaced ten-byte candidate root");
    admission.releaseResidentAfterFree(tree);
}

publishedCohortUsesActualReadyAndSurvivesResidualRetirement();
refusalAndPreOutputReleaseAreExact();
cancelledOutputReleasesOnlyAfterNativeRetirementProof();
v1BridgeReplacesTrackedCandidateRootInsteadOfAccumulatingIt();
console.log("candidate-mutation-output-admission.test: 4 accounting bridge groups passed");
