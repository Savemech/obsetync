import { strict as assert } from "node:assert";
import { PerfTrace, type PerfOperationWindow } from "./perf-trace";
import {
    AdaptiveResourceGovernor,
    coveredResourceAxesForWindow,
    measurementFromPerf,
    type ResourceAxis,
    type ResourceEnvironment,
} from "./resource-governor";

const MIB = 1024 * 1024;
const DESKTOP: ResourceEnvironment = {
    runtime: "desktop", architecture: "arm64", os: "win32",
    hardwareConcurrency: 8, simdAvailable: true,
};

function harness(coveredAxes: ResourceAxis[] | "production" = [], environment = DESKTOP) {
    let now = 0;
    let visible = true;
    const windows: PerfOperationWindow[] = [];
    const trace = new PerfTrace({
        monotonicNow: () => now,
        wallNow: () => 1_800_000_000_000 + now,
        monitorEventLoop: false,
        monitorWindows: false,
    });
    const governor = new AdaptiveResourceGovernor({ ...environment });
    const offWindows = trace.subscribeWindows((window) => {
        windows.push(window);
        const measurement = {
            ...window,
            operationKind: window.kind,
            visible,
        };
        governor.observeWindow({
            ...measurement,
            budget: {
                limitBytes: 128 * MIB,
                reservedBytes: 8 * MIB,
                coveredAxes: coveredAxes === "production"
                    ? coveredResourceAxesForWindow(measurement)
                    : coveredAxes,
            },
        });
    });
    // This intentionally retains a legacy consumer to prove that operation
    // totals cannot retrain a controller that already received window deltas.
    const offTotals = trace.subscribe((record) => governor.observe(measurementFromPerf(record, { visible })));
    return {
        trace, governor, windows,
        sample(milliseconds = 1_000) {
            now += milliseconds;
            trace.sampleWindows();
        },
        setVisible(next: boolean) {
            visible = next;
            trace.setVisible(next);
            governor.setVisible(next);
        },
        dispose() { offWindows(); offTotals(); },
    };
}

function unfinishedScanRespondsAndSubsequentWindowsContainOnlyDeltas(): void {
    const h = harness([], { ...DESKTOP, architecture: "x64", os: "linux" });
    const scan = h.trace.begin("scan");
    scan.setWorkload({ filesTotal: 25_000 });
    scan.setDemand({ read: 25_000, hash: 25_000 });
    const endBatch = scan.phase("scan_batch");
    scan.increment({ filesCompleted: 30 });
    scan.observeEventLoopLag(150);
    h.sample();
    assert.equal(h.trace.recent().length, 0, "no completed record was needed for control");
    assert.equal(h.trace.activeSnapshots().length, 1);
    assert.deepEqual(h.governor.snapshot().controls, { read: 2, hash: 2, network: 4, apply: 8 });
    scan.increment({ filesCompleted: 10 });
    scan.observeEventLoopLag(4);
    h.sample();
    assert.deepEqual(h.windows.map((window) => window.filesCompleted), [30, 10]);
    assert.equal(h.governor.snapshot().throughput, 10, "window rate uses 10 new files, not 40 total");
    const beforeFinish = h.governor.snapshot().controls;
    endBatch();
    scan.finish();
    assert.equal(h.trace.recent()[0].filesCompleted, 40);
    assert.deepEqual(h.governor.snapshot().controls, beforeFinish, "aggregate UI lag must not decrease twice");
    h.dispose();
}

function hiddenCrossingAndMinuteDelayDoNotBecomeCpuPressure(): void {
    const h = harness(["read", "hash", "network", "apply"]);
    const scan = h.trace.begin("scan");
    scan.setDemand({ read: 100 });
    const initial = h.governor.snapshot().controls;
    scan.increment({ filesCompleted: 100 });
    scan.observeEventLoopLag(4);
    h.sample();
    h.setVisible(false);
    scan.increment({ filesCompleted: 100 });
    scan.observeEventLoopLag(10_001);
    h.sample();
    h.setVisible(true);
    scan.increment({ filesCompleted: 100 });
    scan.observeEventLoopLag(150);
    h.sample();
    for (const window of h.windows.slice(1, 3)) {
        assert.equal(window.continuousVisible, false);
        assert.equal(window.eventLoopLagP95Ms, null);
    }
    scan.increment({ filesCompleted: 100 });
    scan.observeEventLoopLag(10_001);
    h.sample(60_000);
    assert.equal(h.windows.at(-1)?.activeDurationMs, 0);
    assert.equal(h.windows.at(-1)?.eventLoopLagP95Ms, null);
    assert.deepEqual(h.governor.snapshot().controls, initial);
    assert.equal(h.governor.snapshot().usefulWindows, 1, "hidden time did not advance hysteresis");
    scan.increment({ filesCompleted: 100 });
    scan.observeEventLoopLag(4);
    h.sample();
    assert.deepEqual(h.governor.snapshot().controls, initial, "one resumed window cannot immediately probe");
    scan.finish();
    h.dispose();
}

function concurrentIntervalsDoNotMultiplyPressureOrGrowth(): void {
    const h = harness(["read", "hash", "network", "apply"], {
        ...DESKTOP, architecture: "x64", os: "linux",
    });
    const push = h.trace.begin("push");
    const pull = h.trace.begin("pull");
    push.increment({ backpressureEvents: 1 });
    pull.increment({ backpressureEvents: 1 });
    h.sample();
    assert.equal(h.windows[0].startedAtMs, h.windows[1].startedAtMs);
    assert.equal(h.windows[0].endedAtMs, h.windows[1].endedAtMs);
    assert.ok(h.windows[1].sequence > h.windows[0].sequence);
    assert.equal(h.governor.current().tuning.networkConcurrency, 2, "one interval means one pressure cut");
    push.finish();
    pull.finish();
    h.dispose();

    const growth = harness(["read", "hash", "network", "apply"]);
    const first = growth.trace.begin("push");
    const second = growth.trace.begin("push");
    const initial = growth.governor.snapshot().controls;
    for (const operation of [first, second]) {
        operation.setDemand({ network: 100 });
        operation.increment({ filesCompleted: 100 });
        operation.observeEventLoopLag(4);
    }
    growth.sample();
    assert.equal(growth.governor.snapshot().usefulWindows, 1);
    for (const operation of [first, second]) {
        operation.increment({ filesCompleted: 100 });
        operation.observeEventLoopLag(4);
    }
    growth.sample();
    assert.equal(growth.governor.snapshot().usefulWindows, 2);
    assert.deepEqual(growth.governor.snapshot().controls, initial, "two concurrent operations do not provide four healthy windows");
    first.increment({ filesCompleted: 100 });
    first.observeEventLoopLag(4);
    growth.sample();
    assert.equal(growth.governor.current().tuning.networkConcurrency, initial.network + 1);
    first.finish();
    second.finish();
    growth.dispose();
}

function actualWindowStreamDoesNotTurnPartialAccountingIntoGrowth(): void {
    const h = harness([]);
    const scan = h.trace.begin("scan");
    const initial = h.governor.snapshot().controls;
    scan.setDemand({ read: 100, hash: 100, network: 100 });
    for (let index = 0; index < 10; index++) {
        scan.increment({ filesCompleted: 100 });
        scan.observeEventLoopLag(4);
        h.sample();
    }
    assert.equal(h.governor.snapshot().usefulWindows, 10);
    assert.deepEqual(h.governor.snapshot().controls, initial);
    assert.match(h.governor.snapshot().decision, /coverage incomplete/);
    scan.finish();
    h.dispose();
}

function productionCoverageProbesOnlyAdmittedDemandAndRollsBack(): void {
    const h = harness("production");
    const scan = h.trace.begin("scan");
    const initial = h.governor.snapshot().controls;
    scan.setDemand({ read: 1_000, network: 1_000 });
    for (let index = 0; index < 3; index++) {
        scan.increment({ filesCompleted: 100 });
        scan.observeEventLoopLag(4);
        h.sample();
    }
    assert.equal(h.governor.snapshot().probeAxis, "read",
        "production admitted read demand did not start a bounded probe");
    assert.deepEqual(h.governor.snapshot().controls, { ...initial, read: initial.read + 1 });

    scan.increment({ filesCompleted: 10 });
    scan.observeEventLoopLag(4);
    h.sample();
    assert.equal(h.governor.snapshot().probeAxis, null,
        "regressed production probe remained active");
    assert.deepEqual(h.governor.snapshot().controls, initial,
        "regressed production probe did not roll back only its admitted axis");

    for (let index = 0; index < 5; index++) {
        scan.increment({ filesCompleted: 100 });
        scan.observeEventLoopLag(4);
        h.sample();
        assert.deepEqual(h.governor.snapshot().controls, initial,
            "rollback cooldown allowed immediate control oscillation");
    }
    scan.finish();
    h.dispose();

    const uncovered = harness("production");
    const uncoveredScan = uncovered.trace.begin("scan");
    uncoveredScan.setDemand({ network: 1_000 });
    const uncoveredInitial = uncovered.governor.snapshot().controls;
    for (let index = 0; index < 10; index++) {
        uncoveredScan.increment({ filesCompleted: 100 });
        uncoveredScan.observeEventLoopLag(4);
        uncovered.sample();
    }
    assert.deepEqual(uncovered.governor.snapshot().controls, uncoveredInitial,
        "scan network demand acquired coverage from operation kind alone");
    uncoveredScan.finish();
    uncovered.dispose();

    const pushHarness = harness("production");
    const push = pushHarness.trace.begin("push");
    const pushInitial = pushHarness.governor.snapshot().controls;
    push.setDemand({ network: 1_000 });
    for (let index = 0; index < 3; index++) {
        push.increment({ bytesTransferred: MIB });
        push.observeEventLoopLag(4);
        pushHarness.sample();
    }
    assert.equal(pushHarness.governor.snapshot().probeAxis, "network",
        "production admitted push transport did not recover upward");
    assert.deepEqual(pushHarness.governor.snapshot().controls, {
        ...pushInitial,
        network: pushInitial.network + 1,
    });
    push.finish();
    pushHarness.dispose();
}

unfinishedScanRespondsAndSubsequentWindowsContainOnlyDeltas();
hiddenCrossingAndMinuteDelayDoNotBecomeCpuPressure();
concurrentIntervalsDoNotMultiplyPressureOrGrowth();
actualWindowStreamDoesNotTurnPartialAccountingIntoGrowth();
productionCoverageProbesOnlyAdmittedDemandAndRollsBack();
console.log("perf-governor.test: live windows, production coverage, rollback, overlap and budget scope passed");
