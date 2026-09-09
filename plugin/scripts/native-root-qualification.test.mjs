import assert from "node:assert/strict";
import test from "node:test";
import { NATIVE_ROOT_MAX_EXACT_CALLBACK_P95_MS,
    NATIVE_ROOT_MAX_EXACT_CALLBACK_ROOT_OPPORTUNITIES, NATIVE_ROOT_MAX_RUNS, nativeRootExecutionSchedule,
    nativeRootQualificationStatus, summarizeNativeRootResults } from "./lib/native-root-qualification.mjs";

function qualificationSummary({ exact = [2000, 2000, 2000, 2000, 2000],
    opportunities = [3, 3, 3, 3, 3] } = {}) {
    const results = [];
    for (let index = 0; index < 5; index++) {
        results.push({ scenario: "quiet", durationMs: 10, bulkFilesPerSecond: 100,
            firstRootAcceptedMs: 5 });
        results.push({ scenario: "edits", durationMs: 10, bulkFilesPerSecond: 100,
            firstRootAcceptedMs: 5, callback: { probes: 73, exactPublished: 70,
                supersededByAcceptedSuccessor: 3,
                callbackToAcceptedCut: { samples: 73, p95Ms: exact[index] },
                callbackToExactAccepted: { samples: 70, p95Ms: exact[index] },
                callbackToExactAcceptedRootOpportunity: { samples: 70,
                    rootAdmissionOpportunities: { samples: 70, p95: opportunities[index] } } } });
    }
    return summarizeNativeRootResults(results);
}

test("all-case schedule alternates quiet/edits order without changing per-case run identity", () => {
    const schedule = nativeRootExecutionSchedule("all", 5);
    assert.deepEqual(schedule.map(row => `${row.scenario}:${row.run}:${row.orderInPair}`), [
        "quiet:1:1", "edits:1:2", "edits:2:1", "quiet:2:2", "quiet:3:1",
        "edits:3:2", "edits:4:1", "quiet:4:2", "quiet:5:1", "edits:5:2",
    ]);
    assert.deepEqual(schedule.map(row => row.executionOrdinal), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert.equal(schedule.filter(row => row.scenario === "quiet").length, 5);
    assert.equal(schedule.filter(row => row.scenario === "edits").length, 5);
});

test("single-case diagnostics stay sequential and cannot qualify local sampling", () => {
    assert.deepEqual(nativeRootExecutionSchedule("edits", 2), [
        { scenario: "edits", run: 1, pair: null, orderInPair: 1, executionOrdinal: 1 },
        { scenario: "edits", run: 2, pair: null, orderInPair: 1, executionOrdinal: 2 },
    ]);
    const summary = qualificationSummary();
    assert.deepEqual(nativeRootQualificationStatus("all", 5, 25000, summary), {
        localSamplingEligible: true, localQualificationEligible: true, releaseEligible: false,
        minimumRunsPerCase: 5,
        requiredFiles: 25000,
        orderControl: { strategy: "alternating-pairs", quietFirst: 3, editsFirst: 2 },
        resultGate: { evaluated: true, passed: true,
            thresholds: { callbackToExactAcceptedP95Ms: NATIVE_ROOT_MAX_EXACT_CALLBACK_P95_MS,
                rootAdmissionOpportunitiesToExactAcceptedP95:
                    NATIVE_ROOT_MAX_EXACT_CALLBACK_ROOT_OPPORTUNITIES },
            observed: { callbackToExactAcceptedP95Ms: 2000,
                rootAdmissionOpportunitiesToExactAcceptedP95: 3 }, reasons: [] },
        reasons: [],
        releaseLimitations: ["no same-host same-durability reference comparison",
            "no native Obsidian device responsiveness or memory evidence"],
    });
    assert.equal(nativeRootQualificationStatus("quiet", 5, 25000).localSamplingEligible, false);
    assert.equal(nativeRootQualificationStatus("all", 4, 25000).localSamplingEligible, false);
    assert.equal(nativeRootQualificationStatus("all", 5, 32).localSamplingEligible, false);
    for (const runs of [NaN, Infinity, "5", 0, NATIVE_ROOT_MAX_RUNS + 1]) {
        assert.equal(nativeRootQualificationStatus("all", runs, 25000).localSamplingEligible, false);
    }
    for (const count of [NaN, Infinity, "25000", 31, 25001]) {
        assert.equal(nativeRootQualificationStatus("all", 5, count).localSamplingEligible, false);
    }
    assert.equal(nativeRootQualificationStatus("unknown", 5, 25000).localSamplingEligible, false);
});

test("qualification fails closed when exact callback latency misses the two-second gate", () => {
    const status = nativeRootQualificationStatus("all", 5, 25000,
        qualificationSummary({ exact: [1000, 1100, 1200, 1300, 2000.001] }));
    assert.equal(status.localSamplingEligible, false);
    assert.equal(status.localQualificationEligible, false);
    assert.equal(status.resultGate.evaluated, true);
    assert.equal(status.resultGate.observed.callbackToExactAcceptedP95Ms, 2000.001);
    assert.match(status.reasons.join("\n"), /exact callback p95 exceeds 2000 ms/);
});

test("qualification fails closed when callback placement escapes three root opportunities", () => {
    const status = nativeRootQualificationStatus("all", 5, 25000,
        qualificationSummary({ opportunities: [1, 2, 3, 3, 4] }));
    assert.equal(status.localSamplingEligible, false);
    assert.equal(status.localQualificationEligible, false);
    assert.equal(status.resultGate.observed.rootAdmissionOpportunitiesToExactAcceptedP95, 4);
    assert.match(status.reasons.join("\n"), /exact callback root opportunities exceed 3/);
});

test("qualification rejects missing, incomplete and non-finite exact result evidence", () => {
    const missing = nativeRootQualificationStatus("all", 5, 25000);
    assert.equal(missing.localSamplingEligible, false);
    assert.equal(missing.resultGate.evaluated, false);
    assert.match(missing.reasons.join("\n"), /result summary is required/);

    const incompleteSummary = qualificationSummary().filter(row => row.scenario !== "quiet");
    const incomplete = nativeRootQualificationStatus("all", 5, 25000, incompleteSummary);
    assert.equal(incomplete.localSamplingEligible, false);
    assert.match(incomplete.reasons.join("\n"), /result summary is incomplete/);

    const nonFiniteSummary = qualificationSummary();
    nonFiniteSummary.find(row => row.scenario === "edits")
        .perRunCallbackToExactAcceptedP95Ms.p95 = Infinity;
    const nonFinite = nativeRootQualificationStatus("all", 5, 25000, nonFiniteSummary);
    assert.equal(nonFinite.localSamplingEligible, false);
    assert.match(nonFinite.reasons.join("\n"), /incomplete or non-finite/);

    const malformedRun = { scenario: "edits", durationMs: 10, bulkFilesPerSecond: 100,
        firstRootAcceptedMs: 5, callback: { probes: 73, exactPublished: 70,
            supersededByAcceptedSuccessor: 3,
            callbackToAcceptedCut: { samples: 73, p95Ms: 1 },
            callbackToExactAccepted: { samples: 70, p95Ms: Infinity },
            callbackToExactAcceptedRootOpportunity: { samples: 70,
                rootAdmissionOpportunities: { samples: 70, p95: 1 } } } };
    assert.throws(() => summarizeNativeRootResults([malformedRun]), /exact callback samples are incomplete/);
});

test("summary preserves legacy aliases and aggregates exact acceptance plus root opportunities", () => {
    const runs = [1, 2, 3].map(value => ({ scenario: "edits", durationMs: value * 10,
        bulkFilesPerSecond: value, firstRootAcceptedMs: value * 100,
        callback: { probes: 73, exactPublished: 70, supersededByAcceptedSuccessor: 3,
            callbackToAcceptedCut: { samples: 73, p95Ms: value * 4000 },
            callbackToExactAccepted: { samples: 70, p95Ms: value * 4 },
            callbackToExactAcceptedRootOpportunity: { samples: 70,
                rootAdmissionOpportunities: { samples: 70, p95: value * 5 } } } }));
    assert.deepEqual(summarizeNativeRootResults(runs), [{
        scenario: "edits", runs: 3,
        medianDrainMs: 20, medianBulkFilesPerSecond: 2, medianFirstRootMs: 200,
        medianCallbackToAcceptedP95Ms: 8000,
        medianCallbackToExactAcceptedP95Ms: 8,
        medianRootAdmissionOpportunitiesToExactAcceptedP95: 10,
        drainMs: { p50: 20, p95: 30 }, bulkFilesPerSecond: { p50: 2, p95: 3 },
        firstRootMs: { p50: 200, p95: 300 },
        perRunCallbackToAcceptedP95Ms: { p50: 8000, p95: 12000 },
        perRunCallbackToExactAcceptedP95Ms: { p50: 8, p95: 12 },
        perRunRootAdmissionOpportunitiesToExactAcceptedP95: { p50: 10, p95: 15 },
    }]);
    assert.throws(() => summarizeNativeRootResults([...runs,
        { scenario: "edits", durationMs: 40, bulkFilesPerSecond: 4,
            firstRootAcceptedMs: 400, callback: { probes: 0 } }]), /callback samples are incomplete/);
    assert.throws(() => summarizeNativeRootResults([{ ...runs[0], callback: {
        probes: 73, exactPublished: 70, supersededByAcceptedSuccessor: 3,
        callbackToAcceptedCut: { samples: 73, p95Ms: 4 },
    } }]), /exact callback samples are incomplete/);
});

test("schedule rejects invalid cases and unbounded run counts", () => {
    for (const input of [["unknown", 1], ["all", 0], ["all", NATIVE_ROOT_MAX_RUNS + 1], ["all", 1.5]]) {
        assert.throws(() => nativeRootExecutionSchedule(...input), RangeError);
    }
});
