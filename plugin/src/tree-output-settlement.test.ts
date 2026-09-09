import { strict as assert } from "node:assert";
import {
    captureV2CandidateSettlementWitness,
    settleTreeCandidateOutput,
    takeV2TreeOutputSettlementProof,
    type V2TreeOutputSettlementOutcome,
} from "./tree-output-settlement";

class SettlementTree {
    committed = 4;
    candidate = 7;
    open = true;
    atomicCalls = 0;
    malformed = false;
    throwNative = false;
    reportMutation: ((report: Record<string, unknown>) => unknown) | undefined;

    tree_version(): number { return 2; }
    committed_revision(): number { return this.committed; }
    candidate_revision(): number { return this.candidate; }
    has_candidate(): boolean { return this.open; }
    commit_candidate(): unknown { throw new Error("legacy commit used"); }
    abort_candidate(): unknown { throw new Error("legacy abort used"); }

    private settle(outcome: V2TreeOutputSettlementOutcome): unknown {
        this.atomicCalls++;
        if (this.throwNative) throw new Error("native refused before sweep");
        if (outcome === "commit") this.committed++;
        this.candidate++;
        this.open = false;
        if (this.malformed) return null;
        const report: Record<string, unknown> = {
            schema: 1,
            scope: "v2-stable-tree-output",
            outcome,
            treeVersion: 2,
            before: 8,
            reachable: 5,
            removed: 3,
            after: 5,
            bytesRemoved: 30,
            committedRevision: this.committed,
            candidateRevision: this.candidate,
            countersValid: true,
            nodePayloadBytes: 40,
            rangeEndpointResidentRequestedBytes: 10,
            residentAdmissionBytes: 50,
        };
        return this.reportMutation ? this.reportMutation(report) : report;
    }

    commit_candidate_output_settlement_v1(): unknown { return this.settle("commit"); }
    abort_candidate_output_settlement_v1(): unknown { return this.settle("abort"); }
}

function exactCommitAndAbortProofsAreOneUseAndOwnerLocal(): void {
    for (const outcome of ["commit", "abort"] as const) {
        const tree = new SettlementTree();
        const witness = captureV2CandidateSettlementWitness(tree);
        assert(witness);
        const result = settleTreeCandidateOutput(tree, outcome, witness);
        assert.equal(result.atomicV2, true);
        assert.deepEqual(result.gcStats, {
            before: 8, reachable: 5, removed: 3, after: 5, bytes_removed: 30,
        });
        assert(result.proof);
        const copied = Object.freeze(Object.defineProperties(
            {}, Object.getOwnPropertyDescriptors(result.proof),
        )) as typeof result.proof;
        assert.equal(takeV2TreeOutputSettlementProof(copied!), null);
        assert.deepEqual(takeV2TreeOutputSettlementProof(result.proof), {
            tree,
            outcome,
            beforeCommittedRevision: 4,
            beforeCandidateRevision: 7,
            committedRevision: outcome === "commit" ? 5 : 4,
            candidateRevision: 8,
            residentAdmissionBytes: 50,
        });
        assert.equal(takeV2TreeOutputSettlementProof(result.proof), null, "proof replay succeeded");
    }
}

function postSuccessParsingFailureNeverReclassifiesNativeOutcome(): void {
    const tree = new SettlementTree();
    tree.malformed = true;
    const result = settleTreeCandidateOutput(tree, "commit",
        captureV2CandidateSettlementWitness(tree));
    assert.equal(result.atomicV2, true);
    assert.equal(result.gcStats, null);
    assert.equal(result.proof, null);
    assert.equal(tree.open, false);
    assert.equal(tree.committed, 5);
    assert.equal(tree.candidate, 8);
}

function malformedAuthorityReportsNeverCreateProofs(): void {
    let accessorReads = 0;
    const cases: Array<{
        name: string;
        mutate(report: Record<string, unknown>): unknown;
        verify?: () => void;
    }> = [
        { name: "extra", mutate: report => ({ ...report, extra: 1 }) },
        { name: "missing", mutate: report => {
            assert.equal(Reflect.deleteProperty(report, "bytesRemoved"), true); return report;
        } },
        { name: "symbol", mutate: report => {
            Object.defineProperty(report, Symbol("unexpected"), { value: 1 }); return report;
        } },
        { name: "accessor", mutate: report => {
            Object.defineProperty(report, "nodePayloadBytes", {
                configurable: true, enumerable: true,
                get() { accessorReads++; return 40; },
            });
            return report;
        }, verify: () => assert.equal(accessorReads, 0, "authority parser invoked an accessor") },
        { name: "counters-false", mutate: report => ({ ...report, countersValid: false }) },
        { name: "counters-false-nullable", mutate: report => ({ ...report, countersValid: false,
            nodePayloadBytes: null, rangeEndpointResidentRequestedBytes: null,
            residentAdmissionBytes: null }) },
        { name: "unsafe-count", mutate: report => ({ ...report,
            before: Number.MAX_SAFE_INTEGER + 1 }) },
        { name: "negative-count", mutate: report => ({ ...report, bytesRemoved: -1 }) },
        { name: "nan-revision", mutate: report => ({ ...report, candidateRevision: Number.NaN }) },
        { name: "wrong-schema", mutate: report => ({ ...report, schema: 2 }) },
        { name: "wrong-scope", mutate: report => ({ ...report, scope: "other" }) },
        { name: "wrong-version", mutate: report => ({ ...report, treeVersion: 1 }) },
        { name: "wrong-outcome", mutate: report => ({ ...report,
            outcome: report.outcome === "commit" ? "abort" : "commit" }) },
        { name: "wrong-committed-revision", mutate: report => ({ ...report,
            committedRevision: (report.committedRevision as number) + 1 }) },
        { name: "wrong-candidate-revision", mutate: report => ({ ...report,
            candidateRevision: (report.candidateRevision as number) + 1 }) },
        { name: "after-reachable", mutate: report => ({ ...report,
            after: (report.after as number) + 1 }) },
        { name: "removed", mutate: report => ({ ...report,
            removed: (report.removed as number) + 1 }) },
        { name: "resident-sum", mutate: report => ({ ...report,
            residentAdmissionBytes: (report.residentAdmissionBytes as number) + 1 }) },
    ];
    for (const outcome of ["commit", "abort"] as const) for (const test of cases) {
        const tree = new SettlementTree();
        tree.reportMutation = test.mutate;
        const result = settleTreeCandidateOutput(tree, outcome,
            captureV2CandidateSettlementWitness(tree));
        assert.equal(result.atomicV2, true, `${outcome}/${test.name}: lost atomic outcome`);
        assert.equal(result.gcStats, null, `${outcome}/${test.name}: malformed GC stats escaped`);
        assert.equal(result.proof, null, `${outcome}/${test.name}: malformed report created authority`);
        assert.equal(tree.atomicCalls, 1, `${outcome}/${test.name}: native transition repeated`);
        assert.equal(tree.open, false, `${outcome}/${test.name}: native outcome was reclassified`);
        assert.equal(tree.committed, outcome === "commit" ? 5 : 4,
            `${outcome}/${test.name}: committed revision changed unexpectedly`);
        assert.equal(tree.candidate, 8, `${outcome}/${test.name}: candidate revision changed unexpectedly`);
        test.verify?.();
    }
}

function nativeFailureAndStaleWitnessRemainPreMutationErrors(): void {
    const failed = new SettlementTree();
    failed.throwNative = true;
    assert.throws(() => settleTreeCandidateOutput(failed, "abort",
        captureV2CandidateSettlementWitness(failed)), /native refused before sweep/);
    assert.equal(failed.open, true);
    assert.deepEqual([failed.committed, failed.candidate], [4, 7]);

    const stale = new SettlementTree();
    const witness = captureV2CandidateSettlementWitness(stale)!;
    stale.candidate++;
    assert.throws(() => settleTreeCandidateOutput(stale, "commit", witness), /stale/);
    assert.equal(stale.atomicCalls, 0);
    assert.equal(stale.open, true);

    const reentrant = new SettlementTree();
    const reentrantWitness = captureV2CandidateSettlementWitness(reentrant)!;
    const ownerChanged = new Error("outer owner changed during final fence");
    let fences = 0;
    assert.throws(() => settleTreeCandidateOutput(reentrant, "abort", reentrantWitness, () => {
        fences++; reentrant.candidate++; throw ownerChanged;
    }), error => error === ownerChanged);
    assert.equal(fences, 1);
    assert.equal(reentrant.atomicCalls, 0,
        "final outer-owner fence ran after the pinned atomic native method");
    assert.equal(reentrant.open, true);
}

function legacyAndPartialFamiliesFailClosed(): void {
    let legacyCalls = 0;
    const legacy = {
        tree_version: () => 2,
        has_candidate: () => true,
        commit_candidate: () => {
            legacyCalls++;
            return { before: 2, reachable: 1, removed: 1, after: 1, bytes_removed: 7 };
        },
        abort_candidate: () => { throw new Error("unused"); },
    };
    const result = settleTreeCandidateOutput(legacy, "commit");
    assert.equal(result.atomicV2, false);
    assert.equal(result.proof, null);
    assert.equal(result.gcStats?.after, 1);
    assert.equal(legacyCalls, 1);

    const partial = { ...legacy, commit_candidate_output_settlement_v1: () => ({}) };
    assert.throws(() => settleTreeCandidateOutput(partial, "commit"), /incomplete/);
    assert.equal(legacyCalls, 1, "partial ABI fell through to a native mutation");

    let ownerRevision = 0, reentrantNativeCalls = 0;
    const reentrantLegacy = {
        tree_version: () => 2,
        has_candidate: () => true,
        get commit_candidate() {
            ownerRevision++;
            return () => { reentrantNativeCalls++; };
        },
        abort_candidate: () => {},
    };
    assert.throws(() => settleTreeCandidateOutput(reentrantLegacy, "commit", null, () => {
        if (ownerRevision !== 0) throw new Error("legacy owner changed");
    }), /legacy owner changed/);
    assert.equal(reentrantNativeCalls, 0,
        "legacy method getter ran after the final owner fence");
}

exactCommitAndAbortProofsAreOneUseAndOwnerLocal();
postSuccessParsingFailureNeverReclassifiesNativeOutcome();
malformedAuthorityReportsNeverCreateProofs();
nativeFailureAndStaleWitnessRemainPreMutationErrors();
legacyAndPartialFamiliesFailClosed();
console.log("tree-output-settlement.test: 5 atomic/provenance/fallback groups passed");
