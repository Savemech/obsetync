import { median, nearestRankPercentile } from "./native-root-metrics.mjs";

export const NATIVE_ROOT_MIN_QUALIFICATION_RUNS = 5;
export const NATIVE_ROOT_MAX_RUNS = 9;
export const NATIVE_ROOT_MAX_EXACT_CALLBACK_P95_MS = 2_000;
export const NATIVE_ROOT_MAX_EXACT_CALLBACK_ROOT_OPPORTUNITIES = 3;
const NATIVE_ROOT_EXPECTED_CALLBACK_PROBES = 73;
const NATIVE_ROOT_MIN_EXACT_CALLBACK_PROBES = 70;

export function nativeRootExecutionSchedule(scenario, runs) {
    if (!["quiet", "edits", "all"].includes(scenario) ||
        !Number.isSafeInteger(runs) || runs < 1 || runs > NATIVE_ROOT_MAX_RUNS) {
        throw new RangeError("invalid native root benchmark schedule");
    }
    const schedule = [];
    for (let run = 1; run <= runs; run++) {
        const order = scenario === "all"
            ? (run % 2 === 1 ? ["quiet", "edits"] : ["edits", "quiet"])
            : [scenario];
        for (let orderInPair = 0; orderInPair < order.length; orderInPair++) {
            schedule.push({ scenario: order[orderInPair], run,
                pair: scenario === "all" ? run : null,
                orderInPair: orderInPair + 1, executionOrdinal: schedule.length + 1 });
        }
    }
    return schedule;
}

function nativeRootResultGate(summary, runs) {
    const thresholds = {
        callbackToExactAcceptedP95Ms: NATIVE_ROOT_MAX_EXACT_CALLBACK_P95_MS,
        rootAdmissionOpportunitiesToExactAcceptedP95:
            NATIVE_ROOT_MAX_EXACT_CALLBACK_ROOT_OPPORTUNITIES,
    };
    const observed = {
        callbackToExactAcceptedP95Ms: null,
        rootAdmissionOpportunitiesToExactAcceptedP95: null,
    };
    const reasons = [];
    if (!Array.isArray(summary)) {
        reasons.push("qualification result summary is required");
        return { evaluated: false, passed: false, thresholds, observed, reasons };
    }
    const quiet = summary.filter(row => row?.scenario === "quiet");
    const edits = summary.filter(row => row?.scenario === "edits");
    if (quiet.length !== 1 || edits.length !== 1 || summary.length !== 2 ||
        quiet[0]?.runs !== runs || edits[0]?.runs !== runs) {
        reasons.push("qualification result summary is incomplete");
        return { evaluated: true, passed: false, thresholds, observed, reasons };
    }
    const exact = edits[0].perRunCallbackToExactAcceptedP95Ms;
    const opportunities = edits[0].perRunRootAdmissionOpportunitiesToExactAcceptedP95;
    const finiteDistribution = value => value !== null && typeof value === "object" &&
        Number.isFinite(value.p50) && value.p50 >= 0 &&
        Number.isFinite(value.p95) && value.p95 >= value.p50;
    if (!finiteDistribution(exact) || !finiteDistribution(opportunities)) {
        reasons.push("qualification exact callback result is incomplete or non-finite");
        return { evaluated: true, passed: false, thresholds, observed, reasons };
    }
    observed.callbackToExactAcceptedP95Ms = exact.p95;
    observed.rootAdmissionOpportunitiesToExactAcceptedP95 = opportunities.p95;
    if (exact.p95 > thresholds.callbackToExactAcceptedP95Ms) {
        reasons.push(`exact callback p95 exceeds ${thresholds.callbackToExactAcceptedP95Ms} ms`);
    }
    if (!Number.isSafeInteger(opportunities.p95) || opportunities.p95 < 1 ||
        opportunities.p95 > thresholds.rootAdmissionOpportunitiesToExactAcceptedP95) {
        reasons.push(`exact callback root opportunities exceed ${thresholds.rootAdmissionOpportunitiesToExactAcceptedP95}`);
    }
    return { evaluated: true, passed: reasons.length === 0, thresholds, observed, reasons };
}

export function nativeRootQualificationStatus(scenario, runs, count, summary) {
    const reasons = [];
    const validScenario = ["quiet", "edits", "all"].includes(scenario);
    const validRuns = Number.isSafeInteger(runs) && runs >= 1 && runs <= NATIVE_ROOT_MAX_RUNS;
    const validCount = Number.isSafeInteger(count) && count >= 32 && count <= 25000;
    if (!validScenario) reasons.push("benchmark case is invalid");
    else if (scenario !== "all") reasons.push("both quiet and edits cases are required");
    if (!validRuns) reasons.push("run count is invalid");
    else if (runs < NATIVE_ROOT_MIN_QUALIFICATION_RUNS) {
        reasons.push(`at least ${NATIVE_ROOT_MIN_QUALIFICATION_RUNS} runs per case are required`);
    }
    if (!validCount) reasons.push("corpus size is invalid");
    else if (count !== 25000) reasons.push("the qualification corpus must contain exactly 25000 files");
    const samplingEligible = reasons.length === 0;
    const resultGate = samplingEligible ? nativeRootResultGate(summary, runs) : {
        evaluated: false, passed: false,
        thresholds: {
            callbackToExactAcceptedP95Ms: NATIVE_ROOT_MAX_EXACT_CALLBACK_P95_MS,
            rootAdmissionOpportunitiesToExactAcceptedP95:
                NATIVE_ROOT_MAX_EXACT_CALLBACK_ROOT_OPPORTUNITIES,
        },
        observed: {
            callbackToExactAcceptedP95Ms: null,
            rootAdmissionOpportunitiesToExactAcceptedP95: null,
        },
        reasons: [],
    };
    reasons.push(...resultGate.reasons);
    const localQualificationEligible = samplingEligible && resultGate.passed;
    return { localSamplingEligible: localQualificationEligible,
        localQualificationEligible,
        releaseEligible: false,
        minimumRunsPerCase: NATIVE_ROOT_MIN_QUALIFICATION_RUNS,
        requiredFiles: 25000,
        orderControl: { strategy: scenario === "all" ? "alternating-pairs" : "single-case",
            quietFirst: !validRuns || !validScenario ? null
                : scenario === "all" ? Math.ceil(runs / 2) : scenario === "quiet" ? runs : 0,
            editsFirst: !validRuns || !validScenario ? null
                : scenario === "all" ? Math.floor(runs / 2) : scenario === "edits" ? runs : 0 },
        resultGate,
        reasons,
        releaseLimitations: ["no same-host same-durability reference comparison",
            "no native Obsidian device responsiveness or memory evidence"] };
}

export function summarizeNativeRootResults(results) {
    if (!Array.isArray(results) || results.length === 0 || results.length > 2 * NATIVE_ROOT_MAX_RUNS) {
        throw new RangeError("invalid native root result collection");
    }
    return [...new Set(results.map(result => result.scenario))].map(scenario => {
        if (!["quiet", "edits"].includes(scenario)) throw new TypeError("invalid native root result case");
        const runs = results.filter(result => result.scenario === scenario);
        const distribution = values => ({ p50: median(values), p95: nearestRankPercentile(values, 0.95) });
        const callbackPresence = runs.map(run => Boolean(run.callback?.probes));
        if (callbackPresence.some(Boolean) && !callbackPresence.every(Boolean)) {
            throw new Error("native root callback samples are incomplete");
        }
        if (callbackPresence[0] && runs.some(run => {
            const callback = run.callback, exact = callback.callbackToExactAccepted;
            const opportunity = callback.callbackToExactAcceptedRootOpportunity;
            return callback.probes !== NATIVE_ROOT_EXPECTED_CALLBACK_PROBES ||
                !Number.isSafeInteger(callback.exactPublished) ||
                callback.exactPublished < NATIVE_ROOT_MIN_EXACT_CALLBACK_PROBES ||
                callback.exactPublished > callback.probes ||
                callback.supersededByAcceptedSuccessor !== callback.probes - callback.exactPublished ||
                callback.callbackToAcceptedCut?.samples !== callback.probes ||
                !Number.isFinite(callback.callbackToAcceptedCut.p95Ms) ||
                exact?.samples !== callback.exactPublished || !Number.isFinite(exact.p95Ms) ||
                opportunity?.samples !== callback.exactPublished ||
                opportunity.rootAdmissionOpportunities?.samples !== callback.exactPublished ||
                !Number.isFinite(opportunity.rootAdmissionOpportunities.p95);
        })) throw new Error("native root exact callback samples are incomplete");
        const drain = distribution(runs.map(run => run.durationMs));
        const throughput = distribution(runs.map(run => run.bulkFilesPerSecond));
        const firstRoot = distribution(runs.map(run => run.firstRootAcceptedMs));
        const callback = callbackPresence[0]
            ? distribution(runs.map(run => run.callback.callbackToAcceptedCut.p95Ms)) : null;
        const callbackExact = callbackPresence[0]
            ? distribution(runs.map(run => run.callback.callbackToExactAccepted.p95Ms)) : null;
        const rootOpportunities = callbackPresence[0] ? distribution(runs.map(run =>
            run.callback.callbackToExactAcceptedRootOpportunity.rootAdmissionOpportunities.p95)) : null;
        return { scenario, runs: runs.length,
            medianDrainMs: drain.p50, medianBulkFilesPerSecond: throughput.p50,
            medianFirstRootMs: firstRoot.p50,
            medianCallbackToAcceptedP95Ms: callback?.p50 ?? null,
            medianCallbackToExactAcceptedP95Ms: callbackExact?.p50 ?? null,
            medianRootAdmissionOpportunitiesToExactAcceptedP95: rootOpportunities?.p50 ?? null,
            drainMs: drain, bulkFilesPerSecond: throughput, firstRootMs: firstRoot,
            perRunCallbackToAcceptedP95Ms: callback,
            perRunCallbackToExactAcceptedP95Ms: callbackExact,
            perRunRootAdmissionOpportunitiesToExactAcceptedP95: rootOpportunities };
    });
}
