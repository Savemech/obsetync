import { median, nearestRankPercentile } from "./native-root-metrics.mjs";

export const NATIVE_ROOT_AUTO_SCENARIO = "auto-edits";

export function nativeRootAutoExecutionSchedule(runs, maximumRuns) {
    if (!Number.isSafeInteger(maximumRuns) || maximumRuns < 1 ||
        !Number.isSafeInteger(runs) || runs < 1 || runs > maximumRuns) {
        throw new RangeError("invalid native root auto diagnostic schedule");
    }
    return Array.from({ length: runs }, (_, index) => ({
        scenario: NATIVE_ROOT_AUTO_SCENARIO,
        run: index + 1,
        pair: null,
        orderInPair: 1,
        executionOrdinal: index + 1,
    }));
}

export function summarizeNativeRootAutoResults(results) {
    if (!Array.isArray(results) || results.length < 1) {
        throw new RangeError("invalid native root auto diagnostic results");
    }
    const distribution = values => ({ p50: median(values),
        p95: nearestRankPercentile(values, 0.95) });
    for (const result of results) {
        const callback = result?.callback, diagnostics = result?.diagnostics;
        const exact = callback?.callbackToExactAccepted;
        const opportunities = callback?.callbackToExactAcceptedRootOpportunity?.rootAdmissionOpportunities;
        if (result?.scenario !== NATIVE_ROOT_AUTO_SCENARIO ||
            !Number.isFinite(result.durationMs) || result.durationMs <= 0 ||
            callback?.probes !== 73 ||
            !Number.isSafeInteger(callback?.exactPublished) || callback.exactPublished < 1 ||
            !Number.isSafeInteger(callback?.supersededByAcceptedSuccessor) ||
            callback.exactPublished + callback.supersededByAcceptedSuccessor !== callback.probes ||
            exact?.samples !== callback.exactPublished || !Number.isFinite(exact.p95Ms) || exact.p95Ms < 0 ||
            callback.callbackToExactAcceptedRootOpportunity?.samples !== callback.exactPublished ||
            opportunities?.samples !== callback.exactPublished ||
            !Number.isSafeInteger(opportunities.p95) || opportunities.p95 < 1 ||
            diagnostics?.explicitDrainCalls !== 1 ||
            !Number.isSafeInteger(diagnostics.automaticPushCalls) || diagnostics.automaticPushCalls < 2 ||
            diagnostics.automaticPushCompleted !== diagnostics.automaticPushCalls ||
            diagnostics.automaticPushRejected !== 0 ||
            !Number.isSafeInteger(diagnostics.callbacksWhileAutomaticPush) ||
            diagnostics.callbacksWhileAutomaticPush < 1 ||
            !Number.isSafeInteger(diagnostics.automaticRetriggerSignals) ||
            diagnostics.automaticRetriggerSignals < 1 ||
            diagnostics.terminalFailures !== 0 || diagnostics.unexpectedFailures !== 0 ||
            diagnostics.publicationObserverOverlaps !== 0 || diagnostics.publicationObserverMisses !== 0 ||
            diagnostics.acceptedProbeCaptureMisses !== 0 ||
            !Number.isFinite(diagnostics.automaticCompletionWaitMs) ||
            !Number.isFinite(diagnostics.automaticCompletionElapsedMs) ||
            !Number.isSafeInteger(diagnostics.automaticCompletionBoundMs) ||
            diagnostics.automaticCompletionWaitMs < 0 ||
            diagnostics.automaticCompletionElapsedMs < diagnostics.automaticCompletionWaitMs ||
            diagnostics.automaticCompletionElapsedMs > diagnostics.automaticCompletionBoundMs) {
            throw new Error("native root auto-trigger/retrigger proof is incomplete");
        }
    }
    const drain = distribution(results.map(result => result.durationMs));
    const exact = distribution(results.map(result => result.callback.callbackToExactAccepted.p95Ms));
    const opportunities = distribution(results.map(result =>
        result.callback.callbackToExactAcceptedRootOpportunity.rootAdmissionOpportunities.p95));
    const completion = distribution(results.map(result => result.diagnostics.automaticCompletionElapsedMs));
    return [{ scenario: NATIVE_ROOT_AUTO_SCENARIO, runs: results.length,
        diagnosticOnly: true, medianDrainMs: drain.p50,
        medianCallbackToExactAcceptedP95Ms: exact.p50,
        medianRootAdmissionOpportunitiesToExactAcceptedP95: opportunities.p50,
        drainMs: drain, perRunCallbackToExactAcceptedP95Ms: exact,
        perRunRootAdmissionOpportunitiesToExactAcceptedP95: opportunities,
        automaticCompletionElapsedMs: completion }];
}

export function nativeRootAutoDiagnosticStatus() {
    return {
        diagnosticOnly: true,
        localSamplingEligible: false,
        localQualificationEligible: false,
        releaseEligible: false,
        reasons: ["auto-trigger/retrigger is a separate diagnostic and is not part of the required quiet+edits qualification"],
        releaseLimitations: ["Node timers/host shell do not qualify Obsidian or mobile lifecycle behavior"],
    };
}
