import test from "node:test";
import { strict as assert } from "node:assert";
import { nativeRootAutoDiagnosticStatus, nativeRootAutoExecutionSchedule,
    summarizeNativeRootAutoResults } from "./lib/native-root-auto-diagnostic.mjs";

function result(value = 1) {
    return { scenario: "auto-edits", durationMs: value * 100,
        callback: { probes: 73, exactPublished: 70, supersededByAcceptedSuccessor: 3,
            callbackToExactAccepted: { samples: 70, p95Ms: value * 10 },
            callbackToExactAcceptedRootOpportunity: { samples: 70,
                rootAdmissionOpportunities: { samples: 70, p95: value },
            } },
        diagnostics: { explicitDrainCalls: 1, automaticPushCalls: 2,
            automaticPushCompleted: 2, automaticPushRejected: 0,
            callbacksWhileAutomaticPush: 72, automaticRetriggerSignals: 72,
            terminalFailures: 0, unexpectedFailures: 0,
            publicationObserverOverlaps: 0, publicationObserverMisses: 0,
            acceptedProbeCaptureMisses: 0, automaticCompletionWaitMs: value * 20,
            automaticCompletionElapsedMs: value * 30,
            automaticCompletionBoundMs: 60_000 } };
}

test("auto diagnostic stays separate and requires trigger/retrigger completion proof", () => {
    assert.deepEqual(nativeRootAutoExecutionSchedule(2, 9), [
        { scenario: "auto-edits", run: 1, pair: null, orderInPair: 1, executionOrdinal: 1 },
        { scenario: "auto-edits", run: 2, pair: null, orderInPair: 1, executionOrdinal: 2 },
    ]);
    assert.throws(() => nativeRootAutoExecutionSchedule(10, 9), /invalid/);
    assert.equal(nativeRootAutoDiagnosticStatus().localQualificationEligible, false);
    assert.deepEqual(summarizeNativeRootAutoResults([result(1), result(2)]), [{
        scenario: "auto-edits", runs: 2, diagnosticOnly: true, medianDrainMs: 150,
        medianCallbackToExactAcceptedP95Ms: 15,
        medianRootAdmissionOpportunitiesToExactAcceptedP95: 1.5,
        drainMs: { p50: 150, p95: 200 },
        perRunCallbackToExactAcceptedP95Ms: { p50: 15, p95: 20 },
        perRunRootAdmissionOpportunitiesToExactAcceptedP95: { p50: 1.5, p95: 2 },
        automaticCompletionElapsedMs: { p50: 45, p95: 60 },
    }]);
    assert.throws(() => summarizeNativeRootAutoResults([{ ...result(), diagnostics: {
        ...result().diagnostics, automaticPushCalls: 1, automaticPushCompleted: 1,
    } }]), /proof is incomplete/);
    assert.throws(() => summarizeNativeRootAutoResults([{ ...result(), diagnostics: {
        ...result().diagnostics, publicationObserverMisses: 1,
    } }]), /proof is incomplete/);
});
