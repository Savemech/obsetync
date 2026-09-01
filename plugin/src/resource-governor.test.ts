import { strict as assert } from "node:assert";
import {
    AdaptiveResourceGovernor,
    RESOURCE_GOVERNOR_CONSTANTS,
    ResourceVisibilityGate,
    measurementFromPerf,
    resourceProfilesFor,
    type ResourceEnvironment,
    type ResourceMeasurement,
    type ResourceRecoveryHint,
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
    assert.equal(m1.profiles[m1.initialIndex].family, "m1-macos");
    assert.equal(m1.profiles[m1.initialIndex].tuning.hashConcurrency, 4);
    assert.equal(m1.profiles[m1.initialIndex].tuning.feedBytes, 512 * 1024);

    const snapdragon = resourceProfilesFor(SNAPDRAGON);
    assert.equal(snapdragon.profiles[snapdragon.initialIndex].family, "snapdragon-windows");
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
    await visibilityGateStopsOnlyNewMobileWork();
    console.log("resource-governor.test: platform/AIMD/recovery/visibility assertions passed");
}

void run().catch((error) => {
    console.error(error);
    process.exitCode = 1;
});
