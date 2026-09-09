import { median, nearestRankPercentile } from "./native-root-metrics.mjs";

export const NATIVE_ROOT_AUDIT_SCENARIO = "audit-edits";
export const NATIVE_ROOT_AUDIT_INJECTION_PREFIX = 256;

export function nativeRootAuditExecutionSchedule(runs, maximumRuns) {
    if (!Number.isSafeInteger(maximumRuns) || maximumRuns < 1 ||
        !Number.isSafeInteger(runs) || runs < 1 || runs > maximumRuns) {
        throw new RangeError("invalid native root audit diagnostic schedule");
    }
    return Array.from({ length: runs }, (_, index) => ({
        scenario: NATIVE_ROOT_AUDIT_SCENARIO,
        run: index + 1,
        pair: null,
        orderInPair: 1,
        executionOrdinal: index + 1,
    }));
}

function finitePercentiles(value, samples) {
    return value?.samples === samples &&
        Number.isFinite(value.p50Ms) && value.p50Ms >= 0 &&
        Number.isFinite(value.p95Ms) && value.p95Ms >= value.p50Ms &&
        Number.isFinite(value.maxMs) && value.maxMs >= value.p95Ms;
}

export function summarizeNativeRootAuditResults(results) {
    if (!Array.isArray(results) || results.length < 1) {
        throw new RangeError("invalid native root audit diagnostic results");
    }
    const distribution = values => ({ p50: median(values),
        p95: nearestRankPercentile(values, 0.95) });
    for (const result of results) {
        const callback = result?.callback, diagnostics = result?.diagnostics, audit = result?.audit;
        const exact = callback?.callbackToExactAccepted;
        const opportunities = callback?.callbackToExactAcceptedRootOpportunity?.rootAdmissionOpportunities;
        const liveCuts = audit?.liveCutsByRoot, entries = audit?.entriesByRoot;
        const staticEntries = Array.isArray(entries) ? entries.slice(2) : [];
        if (result?.scenario !== NATIVE_ROOT_AUDIT_SCENARIO ||
            !Number.isFinite(result.durationMs) || result.durationMs <= 0 ||
            callback?.probes !== 73 || callback?.exactPublished !== 70 ||
            callback?.supersededByAcceptedSuccessor !== 3 ||
            exact?.samples !== callback.exactPublished || !Number.isFinite(exact.p95Ms) || exact.p95Ms < 0 ||
            callback.callbackToExactAcceptedRootOpportunity?.samples !== callback.exactPublished ||
            opportunities?.samples !== callback.exactPublished ||
            opportunities.p95 !== 2 ||
            diagnostics?.explicitDrainCalls !== 1 || diagnostics.auditInitialReviewCalls !== 1 ||
            diagnostics.auditInjectionCalls !== 1 || diagnostics.auditInitialReviewPreemptions !== 1 ||
            diagnostics.auditPrefixReviewCalls !== 1 ||
            diagnostics.auditWholeReviewRebuildCalls !== 1 ||
            diagnostics.auditInjectionAfterMaterializationStats !== NATIVE_ROOT_AUDIT_INJECTION_PREFIX ||
            diagnostics.auditRootPublicationsAtInjection !== 0 ||
            diagnostics.auditRootAdmissionsAtInjection !== 0 ||
            diagnostics.auditRootAdmissionsAtCallbacksComplete !== 0 ||
            diagnostics.terminalFailures !== 0 || diagnostics.unexpectedFailures !== 0 ||
            diagnostics.publicationObserverOverlaps !== 0 || diagnostics.publicationObserverMisses !== 0 ||
            diagnostics.acceptedProbeCaptureMisses !== 0 ||
            audit?.injectionPhase !== "initial-review-materialization" ||
            audit.injectedAfterMaterializationStats !== NATIVE_ROOT_AUDIT_INJECTION_PREFIX ||
            !Number.isFinite(audit.initialReviewAttemptMs) || audit.initialReviewAttemptMs < 0 ||
            !Number.isFinite(audit.injectionToInitialReviewReturnMs) ||
            audit.injectionToInitialReviewReturnMs < 0 ||
            !finitePercentiles(audit.callbackToInitialReviewReturn, callback.probes) ||
            !finitePercentiles(audit.initialReviewReturnToExactAccepted, callback.exactPublished) ||
            !finitePercentiles(audit.callbackToFirstRootAdmission, callback.probes) ||
            !finitePercentiles(audit.callbackToPrefixCompleteAdmission, callback.probes) ||
            !Number.isFinite(audit.prefixCompleteToWholeReviewCompleteMs) ||
            audit.prefixCompleteToWholeReviewCompleteMs < 0 ||
            !Number.isFinite(audit.wholeReviewMs) || audit.wholeReviewMs < 0 ||
            !Array.isArray(liveCuts) || !Array.isArray(entries) || liveCuts.length !== entries.length ||
            liveCuts.length < 3 || JSON.stringify(liveCuts.slice(0, 2)) !== JSON.stringify([64, 6]) ||
            JSON.stringify(entries.slice(0, 2)) !== JSON.stringify([64, 6]) ||
            liveCuts.slice(2).some(value => value !== 0) ||
            staticEntries.some(value => !Number.isSafeInteger(value) || value < 1 || value > 256) ||
            staticEntries.reduce((total, value) => total + value, 0) !== result.count) {
            throw new Error("native root first-review callback proof is incomplete");
        }
    }
    const drain = distribution(results.map(result => result.durationMs));
    const exact = distribution(results.map(result => result.callback.callbackToExactAccepted.p95Ms));
    const opportunities = distribution(results.map(result =>
        result.callback.callbackToExactAcceptedRootOpportunity.rootAdmissionOpportunities.p95));
    const initialAttempt = distribution(results.map(result => result.audit.initialReviewAttemptMs));
    const injectionReturn = distribution(results.map(result => result.audit.injectionToInitialReviewReturnMs));
    const reviewWait = distribution(results.map(result => result.audit.callbackToInitialReviewReturn.p95Ms));
    const postReview = distribution(results.map(result => result.audit.initialReviewReturnToExactAccepted.p95Ms));
    const firstAdmission = distribution(results.map(result => result.audit.callbackToFirstRootAdmission.p95Ms));
    const prefixAdmission = distribution(results.map(result => result.audit.callbackToPrefixCompleteAdmission.p95Ms));
    const rebuildWait = distribution(results.map(result => result.audit.prefixCompleteToWholeReviewCompleteMs));
    const wholeReview = distribution(results.map(result => result.audit.wholeReviewMs));
    return [{ scenario: NATIVE_ROOT_AUDIT_SCENARIO, runs: results.length,
        diagnosticOnly: true, medianDrainMs: drain.p50,
        medianCallbackToExactAcceptedP95Ms: exact.p50,
        medianRootAdmissionOpportunitiesToExactAcceptedP95: opportunities.p50,
        drainMs: drain, perRunCallbackToExactAcceptedP95Ms: exact,
        perRunRootAdmissionOpportunitiesToExactAcceptedP95: opportunities,
        perRunInitialReviewAttemptMs: initialAttempt,
        perRunInjectionToInitialReviewReturnMs: injectionReturn,
        perRunCallbackToInitialReviewReturnP95Ms: reviewWait,
        perRunInitialReviewReturnToExactAcceptedP95Ms: postReview,
        perRunCallbackToFirstRootAdmissionP95Ms: firstAdmission,
        perRunCallbackToPrefixCompleteAdmissionP95Ms: prefixAdmission,
        perRunPrefixCompleteToWholeReviewCompleteMs: rebuildWait,
        perRunWholeReviewMs: wholeReview }];
}

export function nativeRootAuditDiagnosticStatus() {
    return {
        diagnosticOnly: true,
        localSamplingEligible: false,
        localQualificationEligible: false,
        releaseEligible: false,
        reasons: ["first-review callbacks are a separate diagnostic and are not part of the required quiet+edits qualification"],
        releaseLimitations: ["Node host-shell timing does not qualify Obsidian or mobile lifecycle behavior"],
    };
}
