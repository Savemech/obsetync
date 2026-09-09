import { strict as assert } from "node:assert";
import {
    AdaptiveResourceGovernor,
    coveredResourceAxesForWindow,
    MobileLifecycleQuiesceError,
    RESOURCE_GOVERNOR_CONSTANTS,
    ResourceVisibilityGate,
    measurementFromPerf,
    resourceProfilesFor,
    type ResourceEnvironment,
    type ResourceMeasurement,
    type ResourceRecoveryHint,
    type ResourceWindowMeasurement,
} from "./resource-governor";

const MIB = 1024 * 1024;

const IOS: ResourceEnvironment = {
    runtime: "mobile",
    architecture: "arm64",
    os: "ios",
    hardwareConcurrency: 6,
    simdAvailable: true,
};
const M1: ResourceEnvironment = {
    runtime: "desktop",
    architecture: "arm64",
    os: "darwin",
    hardwareConcurrency: 8,
    simdAvailable: true,
};
const SNAPDRAGON: ResourceEnvironment = {
    runtime: "desktop",
    architecture: "arm64",
    os: "win32",
    hardwareConcurrency: 12,
    simdAvailable: true,
};
const X86: ResourceEnvironment = {
    runtime: "desktop",
    architecture: "x64",
    os: "linux",
    hardwareConcurrency: 16,
    simdAvailable: true,
};

function measurement(overrides: Partial<ResourceMeasurement> = {}): ResourceMeasurement {
    return {
        outcome: "success",
        durationMs: 1_000,
        bytesTransferred: 100 * MIB,
        filesCompleted: 100,
        eventLoopLagP95Ms: 4,
        retries: 0,
        backpressureEvents: 0,
        peakBatchBytes: 8 * MIB,
        visible: true,
        ...overrides,
    };
}

function platformLaddersAreBounded(): void {
    const ios = resourceProfilesFor(IOS);
    assert.equal(ios.profiles[ios.initialIndex].tuning.hashConcurrency, 1);
    assert.equal(ios.profiles[ios.initialIndex].tuning.transientBudgetBytes, 48 * MIB);
    assert.ok(ios.profiles.every((item) => item.tuning.hashConcurrency === 1));
    assert.ok(ios.profiles.every((item) => item.tuning.maxBatchBytes <= 2 * MIB));
    const android = resourceProfilesFor({ ...IOS, os: "unknown" });
    assert.equal(android.profiles[android.initialIndex].family, "generic");

    const m1 = resourceProfilesFor(M1);
    assert.equal(m1.profiles[m1.initialIndex].family, "macos-arm64");
    assert.equal(m1.profiles[m1.initialIndex].tuning.hashConcurrency, 4);
    assert.equal(m1.profiles[m1.initialIndex].tuning.feedBytes, 512 * 1024);

    const snapdragon = resourceProfilesFor(SNAPDRAGON);
    assert.equal(snapdragon.profiles[snapdragon.initialIndex].family, "windows-arm64");
    assert.equal(snapdragon.profiles[snapdragon.initialIndex].tuning.hashConcurrency, 2);
    assert.equal(snapdragon.profiles.at(-1)?.tuning.hashConcurrency, 4);

    const x86 = resourceProfilesFor(X86);
    assert.equal(x86.profiles[x86.initialIndex].family, "x86-desktop");
    assert.equal(x86.profiles[x86.initialIndex].tuning.hashConcurrency, 4);
    assert.ok(x86.profiles.every((item) => item.tuning.hashConcurrency <= 4));
    assert.ok(x86.profiles.every((item) => item.tuning.hashConcurrency < X86.hardwareConcurrency));

    const scalarX86 = resourceProfilesFor({ ...X86, simdAvailable: false });
    assert.equal(scalarX86.profiles[scalarX86.initialIndex].name, "conservative");
}

function recoveryHintsLowerStartExpireAndClear(): void {
    let now = 1_800_000_000_000;
    const changes: Array<ResourceRecoveryHint | null> = [];
    const hint: ResourceRecoveryHint = {
        schema: 1,
        penalty: 1,
        updatedAt: now,
        reason: "previous renderer interruption during push",
    };
    const governor = new AdaptiveResourceGovernor(X86, {
        now: () => now,
        recoveryHint: hint,
        onRecoveryHintChange: (value) => changes.push(value),
    });
    assert.equal(governor.current().name, "conservative");
    assert.equal(governor.snapshot().recoveryPenalty, 1);
    governor.observe(measurement());
    governor.observe(measurement());
    assert.equal(governor.snapshot().recoveryPenalty, 1);
    governor.observe(measurement());
    assert.equal(governor.snapshot().recoveryPenalty, 0);
    assert.equal(changes.at(-1), null);

    now += RESOURCE_GOVERNOR_CONSTANTS.hintTtlMs + 1;
    const expired: Array<ResourceRecoveryHint | null> = [];
    const fresh = new AdaptiveResourceGovernor(X86, {
        now: () => now,
        recoveryHint: hint,
        onRecoveryHintChange: (value) => expired.push(value),
    });
    assert.equal(fresh.current().name, "balanced");
    assert.deepEqual(expired, [null]);
}

function interruptionAndAimdAreHysteretic(): void {
    let now = 1_800_000_000_000;
    const selected: string[] = [];
    const hints: Array<ResourceRecoveryHint | null> = [];
    const governor = new AdaptiveResourceGovernor(SNAPDRAGON, {
        now: () => now,
        onProfileChange: (item, reason) => selected.push(`${item.name}:${reason}`),
        onRecoveryHintChange: (hint) => hints.push(hint),
    });
    assert.equal(governor.current().name, "conservative");
    governor.recordInterruption("push");
    assert.equal(governor.current().name, "recovery");
    assert.equal(hints.at(-1)?.penalty, 1);
    assert.match(hints.at(-1)?.reason ?? "", /push/);

    // Recovery -> conservative requires two consecutive >=10% windows.
    governor.observe(measurement({ bytesTransferred: 100 * MIB }));
    governor.observe(measurement({ bytesTransferred: 111 * MIB }));
    assert.equal(governor.current().name, "recovery");
    governor.observe(measurement({ bytesTransferred: 123 * MIB }));
    assert.equal(governor.current().name, "conservative");
    assert.equal(selected.length, 2);

    // One overloaded window makes one multiplicative change, never several.
    governor.observe(measurement({
        eventLoopLagP95Ms: 150,
        backpressureEvents: 3,
    }));
    assert.equal(governor.current().name, "recovery");
    assert.equal(selected.length, 3);
    assert.match(governor.snapshot().decision, /multiplicative decrease/);

    // Bad numeric telemetry is ignored without changing limits.
    governor.observe(measurement({ durationMs: Number.NaN }));
    assert.equal(selected.length, 3);
    now++;
}

function additiveProbeRollsBackRegression(): void {
    const governor = new AdaptiveResourceGovernor(SNAPDRAGON);
    assert.equal(governor.current().name, "conservative");
    governor.observe(measurement({ bytesTransferred: 100 * MIB }));
    governor.observe(measurement({ bytesTransferred: 112 * MIB }));
    governor.observe(measurement({ bytesTransferred: 126 * MIB }));
    assert.equal(governor.current().name, "balanced");
    governor.observe(measurement({ bytesTransferred: 90 * MIB }));
    assert.equal(governor.current().name, "conservative");
    assert.match(governor.snapshot().decision, /probe rollback/);
}

function failedProfileApplicationRollsBackSelection(): void {
    const governor = new AdaptiveResourceGovernor(SNAPDRAGON, {
        onProfileChange: (selected) => {
            if (selected.name === "balanced") throw new Error("injected apply failure");
        },
    });
    governor.observe(measurement({ bytesTransferred: 100 * MIB }));
    governor.observe(measurement({ bytesTransferred: 112 * MIB }));
    assert.throws(
        () => governor.observe(measurement({ bytesTransferred: 126 * MIB })),
        /injected apply failure/,
    );
    assert.equal(governor.current().name, "conservative");
    assert.match(governor.snapshot().decision, /transition failed/);
}

function unrelatedOperationKindsDoNotTrainOneAnother(): void {
    const governor = new AdaptiveResourceGovernor(SNAPDRAGON);
    governor.observe(measurement({ operationKind: "push", bytesTransferred: 100 * MIB }));
    governor.observe(measurement({ operationKind: "pull", bytesTransferred: 112 * MIB }));
    governor.observe(measurement({ operationKind: "push", bytesTransferred: 123 * MIB }));
    assert.equal(governor.current().name, "conservative");
    governor.observe(measurement({ operationKind: "push", bytesTransferred: 136 * MIB }));
    assert.equal(governor.current().name, "balanced");
}

function unmeasuredUiLatencyCannotIncreaseConcurrency(): void {
    const governor = new AdaptiveResourceGovernor(SNAPDRAGON);
    governor.observe(measurement({ eventLoopLagP95Ms: null, bytesTransferred: 100 * MIB }));
    governor.observe(measurement({ eventLoopLagP95Ms: null, bytesTransferred: 112 * MIB }));
    governor.observe(measurement({ eventLoopLagP95Ms: null, bytesTransferred: 126 * MIB }));
    assert.equal(governor.current().name, "conservative");
    assert.equal(governor.snapshot().bottleneck, "UI latency unmeasured");
}

function perfWindowsExposePhaseRatesWithoutPrivateData(): void {
    const converted = measurementFromPerf({
        outcome: "success",
        durationMs: 100,
        bytesTotal: 1_000,
        bytesTransferred: 500,
        filesCompleted: 2,
        eventLoopLagP95Ms: 2,
        retries: 0,
        backpressureEvents: 0,
        peakBatchBytes: 1_000,
        phases: { read: 10, hash: 20, upload: 5 },
    } as any);
    assert.equal(converted.phaseThroughput?.read, 100_000);
    assert.equal(converted.phaseThroughput?.hash, 50_000);
    assert.equal(converted.phaseThroughput?.upload, 100_000);
    const governor = new AdaptiveResourceGovernor(X86);
    governor.observe({ ...converted, averageFileBytes: 500 });
    assert.equal(governor.snapshot().bottleneck, "small-file transaction overhead");
    governor.observe({ ...converted, averageFileBytes: MIB });
    assert.equal(governor.snapshot().bottleneck, "hash throughput");
}

function windowMeasurement(
    sequence: number,
    overrides: Partial<ResourceWindowMeasurement> = {},
): ResourceWindowMeasurement {
    return {
        ...measurement(),
        sequence,
        startedAtMs: (sequence - 1) * 1_000,
        endedAtMs: (sequence - 1) * 1_000 + (overrides.durationMs ?? 1_000),
        operationKind: "push",
        activeDurationMs: 1_000,
        continuousVisible: true,
        budget: { limitBytes: 128 * MIB, reservedBytes: 8 * MIB, coveredAxes: ["read", "hash", "network", "apply"] },
        demand: { read: 8, hash: 8, network: 8, apply: 8 },
        ...overrides,
    };
}

function admittedCoverageIsOperationAndDemandScoped(): void {
    assert.deepEqual(coveredResourceAxesForWindow({
        operationKind: "scan",
        demand: { read: 10, hash: 10, network: 10, apply: 10 },
    }), ["read", "hash"]);
    assert.deepEqual(coveredResourceAxesForWindow({
        operationKind: "push",
        demand: { read: 0, hash: 3, network: 2, apply: 4 },
    }), ["hash", "network"]);
    assert.deepEqual(coveredResourceAxesForWindow({
        operationKind: "pull",
        demand: { read: 1, hash: 1, network: 1, apply: 1 },
    }), []);
    assert.deepEqual(coveredResourceAxesForWindow({
        operationKind: "reconcile",
        demand: { read: Number.NaN, hash: -1, network: 1, apply: 1 },
    }), []);
    assert.deepEqual(coveredResourceAxesForWindow({ demand: { read: 1 } }), []);
    assert.deepEqual(coveredResourceAxesForWindow({ operationKind: "scan" }), []);
}

function windowPressureControlsIndependentAxesEvenWithoutProgress(): void {
    const network = new AdaptiveResourceGovernor({ ...X86 });
    const initial = network.snapshot().controls;
    network.observeWindow(windowMeasurement(1, {
        bytesTransferred: 0, filesCompleted: 0, backpressureEvents: 1,
    }));
    assert.deepEqual(network.snapshot().controls, { ...initial, network: 2 });
    assert.equal(network.snapshot().inputMode, "active-windows");

    const cpu = new AdaptiveResourceGovernor({ ...X86 });
    cpu.observeWindow(windowMeasurement(1, {
        bytesTransferred: 0, filesCompleted: 0, eventLoopLagP95Ms: 150,
    }));
    assert.deepEqual(cpu.snapshot().controls, { read: 2, hash: 2, network: 4, apply: 8 });
    const reduced = cpu.snapshot().controls;
    // A completed aggregate after a live window cannot double-decrease limits.
    cpu.observe(measurement({ eventLoopLagP95Ms: 150 }));
    assert.deepEqual(cpu.snapshot().controls, reduced);
    cpu.observeWindow(windowMeasurement(1, { eventLoopLagP95Ms: 150 }));
    cpu.observeWindow(windowMeasurement(0, { eventLoopLagP95Ms: 150 }));
    assert.deepEqual(cpu.snapshot().controls, reduced);
}

function flatHealthyWindowsProbeOnlyOneDemandedAxisAndRespectCaps(): void {
    const governor = new AdaptiveResourceGovernor({ ...SNAPDRAGON });
    const initial = governor.snapshot().controls;
    for (let sequence = 1; sequence <= 3; sequence++) {
        governor.observeWindow(windowMeasurement(sequence, {
            demand: { network: 8 }, maxConcurrency: { hash: 1 },
        }));
    }
    assert.deepEqual(governor.snapshot().controls, { ...initial, hash: 1, network: 3 });
    assert.equal(governor.snapshot().probeAxis, "network");
    governor.observeWindow(windowMeasurement(4, { demand: { network: 8 } }));
    governor.observeWindow(windowMeasurement(5, { demand: { network: 8 } }));
    assert.equal(governor.snapshot().probeAxis, null);
    assert.equal(governor.current().tuning.networkConcurrency, 3);
    governor.observeWindow(windowMeasurement(6, { demand: { network: 8 } }));
    governor.observeWindow(windowMeasurement(7, { demand: { network: 8 } }));
    assert.equal(governor.current().tuning.networkConcurrency, 3);
    governor.observeWindow(windowMeasurement(8, { demand: { network: 8 } }));
    assert.equal(governor.current().tuning.networkConcurrency, 4);
    assert.equal(governor.current().tuning.transientBudgetBytes, 96 * MIB);
}

function windowProbeRegressionRollsBackOnlyItsAxis(): void {
    const governor = new AdaptiveResourceGovernor({ ...SNAPDRAGON });
    const initial = governor.snapshot().controls;
    for (let sequence = 1; sequence <= 3; sequence++) {
        governor.observeWindow(windowMeasurement(sequence, { demand: { read: 8 } }));
    }
    assert.deepEqual(governor.snapshot().controls, { ...initial, read: 3 });
    governor.observeWindow(windowMeasurement(4, {
        demand: { read: 8 }, bytesTransferred: 50 * MIB,
    }));
    assert.deepEqual(governor.snapshot().controls, initial);
    assert.match(governor.snapshot().decision, /probe rollback/);
    governor.observeWindow(windowMeasurement(5, { demand: { read: 8 } }));
    assert.deepEqual(governor.snapshot().controls, initial);
}

function windowsRequireObservedForegroundUiDemandAndBudget(): void {
    const ineligible: Array<Partial<ResourceWindowMeasurement>> = [
        { eventLoopLagP95Ms: null },
        { continuousVisible: false, eventLoopLagP95Ms: 10_001, retries: 5 },
        { visible: false },
        { durationMs: 60_000, activeDurationMs: 1_000 },
        { durationMs: 10, activeDurationMs: 10 },
        { demand: undefined },
        { demand: { network: 0 } },
        { budget: undefined },
        { budget: { limitBytes: 16 * MIB, reservedBytes: 15 * MIB } },
    ];
    for (const override of ineligible) {
        const governor = new AdaptiveResourceGovernor({ ...SNAPDRAGON });
        const initial = governor.snapshot().controls;
        for (let sequence = 1; sequence <= 10; sequence++) {
            governor.observeWindow(windowMeasurement(sequence, override));
        }
        assert.deepEqual(governor.snapshot().controls, initial, JSON.stringify(override));
    }
}

function lifecycleChangesAndPressureCancelUnvalidatedProbes(): void {
    const governor = new AdaptiveResourceGovernor({ ...SNAPDRAGON });
    for (let sequence = 1; sequence <= 3; sequence++) {
        governor.observeWindow(windowMeasurement(sequence, { demand: { network: 8 } }));
    }
    assert.equal(governor.current().tuning.networkConcurrency, 3);
    governor.setVisible(false);
    assert.equal(governor.current().tuning.networkConcurrency, 2);
    governor.setVisible(true);
    governor.observeWindow(windowMeasurement(4, { demand: { network: 8 } }));
    assert.equal(governor.current().tuning.networkConcurrency, 2);
    governor.observeWindow(windowMeasurement(5, { demand: { network: 8 } }));
    governor.observeWindow(windowMeasurement(6, { demand: { network: 8 } }));
    assert.equal(governor.current().tuning.networkConcurrency, 3);
    governor.observeWindow(windowMeasurement(7, { eventLoopLagP95Ms: 150 }));
    assert.equal(governor.current().tuning.networkConcurrency, 2);
    assert.equal(governor.snapshot().probeAxis, null);
}

function memoryWindowsUseLeaseCeilingsWithoutGrowingThem(): void {
    const governor = new AdaptiveResourceGovernor({ ...X86 });
    const before = governor.current().tuning;
    governor.observeWindow(windowMeasurement(1, {
        visible: false, continuousVisible: false,
        budget: { limitBytes: 32 * MIB, reservedBytes: 40 * MIB },
        filesCompleted: 0, bytesTransferred: 0,
    }));
    const after = governor.current().tuning;
    assert.equal(after.transientBudgetBytes, before.transientBudgetBytes);
    assert.equal(after.maxBatchBytes, before.maxBatchBytes / 2);
    assert.equal(after.feedBytes, before.feedBytes / 2);
    assert.deepEqual(governor.snapshot().controls, { read: 2, hash: 2, network: 2, apply: 8 });
}

function overlappingWindowsNeverDoubleCountControlEvidence(): void {
    const governor = new AdaptiveResourceGovernor({ ...SNAPDRAGON });
    const initial = governor.snapshot().controls;
    governor.observeWindow(windowMeasurement(1));
    governor.observeWindow(windowMeasurement(2, { startedAtMs: 0, endedAtMs: 1_000 }));
    governor.observeWindow(windowMeasurement(3, { startedAtMs: 0, endedAtMs: 1_000 }));
    assert.deepEqual(governor.snapshot().controls, initial);
    assert.equal(governor.snapshot().usefulWindows, 1);
    governor.observeWindow(windowMeasurement(4, {
        startedAtMs: 1_000, endedAtMs: 2_000, backpressureEvents: 1,
    }));
    assert.equal(governor.current().tuning.networkConcurrency, 1);
    const reduced = governor.snapshot().controls;
    governor.observeWindow(windowMeasurement(5, {
        startedAtMs: 1_000, endedAtMs: 2_000, eventLoopLagP95Ms: 150,
    }));
    assert.deepEqual(governor.snapshot().controls, reduced);
}

function partialByteBudgetCannotAuthorizeOtherStages(): void {
    const governor = new AdaptiveResourceGovernor({ ...SNAPDRAGON });
    const initial = governor.snapshot().controls;
    for (let sequence = 1; sequence <= 3; sequence++) {
        governor.observeWindow(windowMeasurement(sequence, {
            budget: { limitBytes: 128 * MIB, reservedBytes: 8 * MIB },
        }));
    }
    assert.deepEqual(governor.snapshot().controls, initial);
    assert.match(governor.snapshot().decision, /coverage incomplete/);
    for (let sequence = 4; sequence <= 6; sequence++) {
        governor.observeWindow(windowMeasurement(sequence, {
            demand: { network: 8 },
            budget: { limitBytes: 128 * MIB, reservedBytes: 8 * MIB, coveredAxes: ["read", "hash"] },
        }));
    }
    assert.deepEqual(governor.snapshot().controls, initial);
}

function capabilityChangesAndUnobservedGapsInvalidateProbes(): void {
    const governor = new AdaptiveResourceGovernor({ ...SNAPDRAGON });
    for (let sequence = 1; sequence <= 3; sequence++) {
        governor.observeWindow(windowMeasurement(sequence, { demand: { network: 8 } }));
    }
    assert.equal(governor.current().tuning.networkConcurrency, 3);
    governor.observeWindow(windowMeasurement(4, {
        demand: { network: 8 }, maxConcurrency: { hash: 1 },
    }));
    assert.equal(governor.current().tuning.hashConcurrency, 1);
    assert.equal(governor.current().tuning.networkConcurrency, 2);
    assert.equal(governor.snapshot().probeAxis, null);
    governor.observeWindow(windowMeasurement(5, { demand: { network: 8 } }));
    governor.observeWindow(windowMeasurement(6, { demand: { network: 8 } }));
    assert.equal(governor.current().tuning.networkConcurrency, 3);
    governor.observeWindow(windowMeasurement(7, {
        startedAtMs: 60_000, endedAtMs: 61_000, demand: { network: 8 },
    }));
    assert.equal(governor.current().tuning.networkConcurrency, 2);
    assert.equal(governor.snapshot().probeAxis, null);
}

function recoveryOneAutomaticallyProbesAfterCleanActiveWindows(): void {
    const hint: ResourceRecoveryHint = {
        schema: 1,
        penalty: 2,
        updatedAt: 1_800_000_000_000,
        reason: "previous renderer interruption during full-scan",
    };
    const governor = new AdaptiveResourceGovernor({ ...X86 }, {
        now: () => hint.updatedAt,
        recoveryHint: hint,
    });
    assert.equal(governor.current().name, "recovery");
    assert.equal(governor.current().tuning.networkConcurrency, 1);
    for (let sequence = 1; sequence <= 3; sequence++) {
        governor.observeWindow(windowMeasurement(sequence, { demand: { network: 8 } }));
    }
    assert.equal(governor.current().tuning.networkConcurrency, 2,
        "recovery=1 did not initiate a bounded upward probe");
    assert.equal(governor.snapshot().probeAxis, "network");
    assert.equal(governor.snapshot().recoveryPenalty, 0,
        "clean active windows did not retire the temporary recovery hint");
}

function pressureCooldownPreventsWindowByWindowOscillation(): void {
    const governor = new AdaptiveResourceGovernor({ ...SNAPDRAGON });
    for (let sequence = 1; sequence <= 3; sequence++) {
        governor.observeWindow(windowMeasurement(sequence, { demand: { network: 8 } }));
    }
    assert.equal(governor.current().tuning.networkConcurrency, 3);
    governor.observeWindow(windowMeasurement(4, {
        demand: { network: 8 },
        backpressureEvents: 1,
    }));
    assert.equal(governor.current().tuning.networkConcurrency, 1);
    assert.equal(governor.snapshot().probeAxis, null);

    for (let sequence = 5; sequence <= 9; sequence++) {
        governor.observeWindow(windowMeasurement(sequence, { demand: { network: 8 } }));
        assert.equal(governor.current().tuning.networkConcurrency, 1,
            `post-pressure control regrew after only ${sequence - 4} clean windows`);
        assert.equal(governor.snapshot().probeAxis, null);
    }
    assert.equal((governor as any).windowHealthy, RESOURCE_GOVERNOR_CONSTANTS.windowHealthyToProbe,
        "healthy hysteresis counter grew without bound during cooldown");
    governor.observeWindow(windowMeasurement(10, { demand: { network: 8 } }));
    assert.equal(governor.current().tuning.networkConcurrency, 2);
    assert.equal(governor.snapshot().probeAxis, "network");
}

function windowKindsNumericValidationAndApplicationFailureAreIsolated(): void {
    const governor = new AdaptiveResourceGovernor({ ...SNAPDRAGON });
    const initial = governor.snapshot().controls;
    governor.observeWindow(windowMeasurement(1));
    governor.observeWindow(windowMeasurement(2, { operationKind: "pull" }));
    governor.observeWindow(windowMeasurement(3));
    assert.deepEqual(governor.snapshot().controls, initial);
    governor.observeWindow(windowMeasurement(4, { demand: { read: Number.NaN } }));
    governor.observeWindow(windowMeasurement(4, { activeDurationMs: 2_000 }));
    governor.observeWindow(windowMeasurement(4, { maxConcurrency: { hash: 0 } }));
    governor.observeWindow(windowMeasurement(4, { budget: { limitBytes: Infinity, reservedBytes: 0 } }));
    assert.deepEqual(governor.snapshot().controls, initial);

    const rejected = new AdaptiveResourceGovernor({ ...SNAPDRAGON }, {
        onProfileChange: (_profile, reason) => {
            if (reason.includes("probe +1")) throw new Error("window transition failure");
        },
    });
    rejected.observeWindow(windowMeasurement(1));
    rejected.observeWindow(windowMeasurement(2));
    assert.throws(() => rejected.observeWindow(windowMeasurement(3)), /window transition failure/);
    assert.deepEqual(rejected.snapshot().controls, initial);
    assert.equal(rejected.snapshot().probeAxis, null);
}

async function visibilityGateStopsOnlyNewMobileWork(): Promise<void> {
    const mobile = new ResourceVisibilityGate("mobile", false);
    let resumed = false;
    const waiting = mobile.waitForHeavyWork().then(() => { resumed = true; });
    await Promise.resolve();
    assert.equal(resumed, false);
    assert.equal(mobile.isPaused(), true);
    mobile.setVisible(true);
    await waiting;
    assert.equal(resumed, true);
    assert.equal(mobile.isPaused(), false);

    const desktop = new ResourceVisibilityGate("desktop", false);
    await desktop.waitForHeavyWork();
    assert.equal(desktop.isPaused(), false);

    const disposed = new ResourceVisibilityGate("mobile", false);
    let released = false;
    const blocked = disposed.waitForHeavyWork().then(() => { released = true; });
    disposed.dispose();
    await blocked;
    assert.equal(released, true);

    const epochGate = new ResourceVisibilityGate("mobile", true);
    const engine = new AbortController();
    const work = epochGate.beginHeavyWork(engine.signal);
    assert.deepEqual(epochGate.snapshot(), { runtime: "mobile", paused: false,
        epoch: 1, activeWork: 1, lastQuiesceReason: null });
    assert.equal(epochGate.setVisible(false, "pagehide"), false);
    assert(work.signal.reason instanceof MobileLifecycleQuiesceError);
    assert.equal(work.signal.reason.lifecycleReason, "pagehide");
    assert.equal(epochGate.snapshot().activeWork, 1,
        "lifecycle abort released an owner before its async tail settled");
    assert.equal(epochGate.setVisible(false, "freeze"), false);
    assert.equal(epochGate.snapshot().epoch, 1, "coalesced hide/freeze created another epoch");
    assert.equal(epochGate.snapshot().lastQuiesceReason, "pagehide",
        "duplicate lifecycle events rewrote the authoritative first abort cause");
    work.release(); work.release();
    assert.equal(epochGate.snapshot().activeWork, 0);
    assert.equal(epochGate.setVisible(true), true);
    assert.equal(epochGate.setVisible(true), false, "duplicate visible/pageshow resumed twice");
    const resumedWork = epochGate.beginHeavyWork(engine.signal);
    assert.equal(resumedWork.epoch, 2); assert.equal(resumedWork.signal.aborted, false);
    resumedWork.release();
    const replacementEngine = new AbortController();
    const replacementWork = epochGate.beginHeavyWork(replacementEngine.signal);
    engine.abort(new Error("old engine retired"));
    assert.equal(replacementWork.signal.aborted, false,
        "plugin-lifetime gate pinned or leaked the replaced engine signal");
    replacementWork.release(); epochGate.dispose();

    const desktopWork = desktop.beginHeavyWork(new AbortController().signal);
    desktop.setVisible(false, "pagehide");
    assert.equal(desktopWork.signal.aborted, false, "desktop hidden work was lifecycle-aborted");
    desktopWork.release();
}

async function run(): Promise<void> {
    platformLaddersAreBounded();
    recoveryHintsLowerStartExpireAndClear();
    interruptionAndAimdAreHysteretic();
    additiveProbeRollsBackRegression();
    failedProfileApplicationRollsBackSelection();
    unrelatedOperationKindsDoNotTrainOneAnother();
    unmeasuredUiLatencyCannotIncreaseConcurrency();
    perfWindowsExposePhaseRatesWithoutPrivateData();
    admittedCoverageIsOperationAndDemandScoped();
    windowPressureControlsIndependentAxesEvenWithoutProgress();
    flatHealthyWindowsProbeOnlyOneDemandedAxisAndRespectCaps();
    windowProbeRegressionRollsBackOnlyItsAxis();
    windowsRequireObservedForegroundUiDemandAndBudget();
    lifecycleChangesAndPressureCancelUnvalidatedProbes();
    memoryWindowsUseLeaseCeilingsWithoutGrowingThem();
    overlappingWindowsNeverDoubleCountControlEvidence();
    partialByteBudgetCannotAuthorizeOtherStages();
    capabilityChangesAndUnobservedGapsInvalidateProbes();
    recoveryOneAutomaticallyProbesAfterCleanActiveWindows();
    pressureCooldownPreventsWindowByWindowOscillation();
    windowKindsNumericValidationAndApplicationFailureAreIsolated();
    await visibilityGateStopsOnlyNewMobileWork();
    await visibilityCancellationIsPerWaiter();
    console.log("resource-governor.test: platform/legacy AIMD/independent windows/recovery/visibility assertions passed");
}

async function visibilityCancellationIsPerWaiter(): Promise<void> {
    const gate = new ResourceVisibilityGate("mobile", false);
    const abort = new AbortController();
    const reason = new Error("engine retired");
    const cancelled = gate.waitForHeavyWork(abort.signal);
    const rejected = assert.rejects(cancelled, error => error === reason);
    let siblingDone = false;
    const sibling = gate.waitForHeavyWork().then(() => { siblingDone = true; });
    abort.abort(reason);
    await rejected;
    assert.equal(siblingDone, false);
    assert.equal(gate.isPaused(), true);
    assert.equal((gate as any).waiters.size, 1);
    await assert.rejects(gate.waitForHeavyWork(abort.signal), error => error === reason);
    assert.equal((gate as any).waiters.size, 1);
    gate.setVisible(true);
    await sibling;
    assert.equal((gate as any).waiters.size, 0);
    const late = new AbortController();
    gate.setVisible(false);
    const released = gate.waitForHeavyWork(late.signal);
    gate.dispose();
    await released;
    late.abort();
    assert.equal((gate as any).waiters.size, 0);
    await assert.rejects(new ResourceVisibilityGate("desktop").waitForHeavyWork(abort.signal), error => error === reason);
}

void run().catch((error) => {
    console.error(error);
    process.exitCode = 1;
});
