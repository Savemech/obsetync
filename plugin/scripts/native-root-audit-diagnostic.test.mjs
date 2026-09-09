import test from "node:test";
import { strict as assert } from "node:assert";
import { NATIVE_ROOT_AUDIT_INJECTION_PREFIX, nativeRootAuditDiagnosticStatus,
    nativeRootAuditExecutionSchedule, summarizeNativeRootAuditResults } from "./lib/native-root-audit-diagnostic.mjs";

function percentiles(value, samples) {
    return { samples, p50Ms: value, p95Ms: value * 2, maxMs: value * 3 };
}

function result(value = 1) {
    return { scenario: "audit-edits", count: 512, durationMs: value * 100,
        callback: { probes: 73, exactPublished: 70, supersededByAcceptedSuccessor: 3,
            callbackToExactAccepted: { samples: 70, p95Ms: value * 10 },
            callbackToExactAcceptedRootOpportunity: { samples: 70,
                rootAdmissionOpportunities: { samples: 70, p95: 2 },
            } },
        diagnostics: { explicitDrainCalls: 1, auditInitialReviewCalls: 1,
            auditInjectionCalls: 1, auditInitialReviewPreemptions: 1,
            auditPrefixReviewCalls: 1,
            auditWholeReviewRebuildCalls: 1,
            auditInjectionAfterMaterializationStats: NATIVE_ROOT_AUDIT_INJECTION_PREFIX,
            auditRootPublicationsAtInjection: 0, auditRootAdmissionsAtInjection: 0,
            auditRootAdmissionsAtCallbacksComplete: 0,
            terminalFailures: 0, unexpectedFailures: 0,
            publicationObserverOverlaps: 0, publicationObserverMisses: 0,
            acceptedProbeCaptureMisses: 0 },
        audit: { injectionPhase: "initial-review-materialization",
            injectedAfterMaterializationStats: NATIVE_ROOT_AUDIT_INJECTION_PREFIX,
            initialReviewAttemptMs: value * 20, injectionToInitialReviewReturnMs: value * 15,
            callbackToInitialReviewReturn: percentiles(value, 73),
            initialReviewReturnToExactAccepted: percentiles(value * 2, 70),
            callbackToFirstRootAdmission: percentiles(value * 3, 73),
            callbackToPrefixCompleteAdmission: percentiles(value * 4, 73),
            prefixCompleteToWholeReviewCompleteMs: value * 5,
            wholeReviewMs: value * 6,
            liveCutsByRoot: [64, 6, 0, 0], entriesByRoot: [64, 6, 256, 256] } };
}

test("audit diagnostic is isolated and requires pre-admission first-review proof", () => {
    assert.deepEqual(nativeRootAuditExecutionSchedule(2, 9), [
        { scenario: "audit-edits", run: 1, pair: null, orderInPair: 1, executionOrdinal: 1 },
        { scenario: "audit-edits", run: 2, pair: null, orderInPair: 1, executionOrdinal: 2 },
    ]);
    assert.throws(() => nativeRootAuditExecutionSchedule(10, 9), /invalid/);
    assert.equal(nativeRootAuditDiagnosticStatus().localQualificationEligible, false);
    assert.deepEqual(summarizeNativeRootAuditResults([result(1), result(2)]), [{
        scenario: "audit-edits", runs: 2, diagnosticOnly: true, medianDrainMs: 150,
        medianCallbackToExactAcceptedP95Ms: 15,
        medianRootAdmissionOpportunitiesToExactAcceptedP95: 2,
        drainMs: { p50: 150, p95: 200 },
        perRunCallbackToExactAcceptedP95Ms: { p50: 15, p95: 20 },
        perRunRootAdmissionOpportunitiesToExactAcceptedP95: { p50: 2, p95: 2 },
        perRunInitialReviewAttemptMs: { p50: 30, p95: 40 },
        perRunInjectionToInitialReviewReturnMs: { p50: 22.5, p95: 30 },
        perRunCallbackToInitialReviewReturnP95Ms: { p50: 3, p95: 4 },
        perRunInitialReviewReturnToExactAcceptedP95Ms: { p50: 6, p95: 8 },
        perRunCallbackToFirstRootAdmissionP95Ms: { p50: 9, p95: 12 },
        perRunCallbackToPrefixCompleteAdmissionP95Ms: { p50: 12, p95: 16 },
        perRunPrefixCompleteToWholeReviewCompleteMs: { p50: 7.5, p95: 10 },
        perRunWholeReviewMs: { p50: 9, p95: 12 },
    }]);
});

test("audit diagnostic rejects post-admission injection and wrong prefix placement", () => {
    assert.throws(() => summarizeNativeRootAuditResults([{ ...result(), diagnostics: {
        ...result().diagnostics, auditRootAdmissionsAtCallbacksComplete: 1,
    } }]), /proof is incomplete/);
    assert.throws(() => summarizeNativeRootAuditResults([{ ...result(), diagnostics: {
        ...result().diagnostics, auditInitialReviewPreemptions: 0,
    } }]), /proof is incomplete/);
    assert.throws(() => summarizeNativeRootAuditResults([{ ...result(), callback: {
        ...result().callback, callbackToExactAcceptedRootOpportunity: { samples: 70,
            rootAdmissionOpportunities: { samples: 70, p95: 3 },
        },
    } }]), /proof is incomplete/);
    assert.throws(() => summarizeNativeRootAuditResults([{ ...result(), audit: {
        ...result().audit, liveCutsByRoot: [63, 7, 0, 0],
    } }]), /proof is incomplete/);
    assert.throws(() => summarizeNativeRootAuditResults([{ ...result(), audit: {
        ...result().audit, liveCutsByRoot: [64, 6, 1, 0],
    } }]), /proof is incomplete/);
    assert.throws(() => summarizeNativeRootAuditResults([{ ...result(), audit: {
        ...result().audit, entriesByRoot: [64, 6, 256, 255],
    } }]), /proof is incomplete/);
});
