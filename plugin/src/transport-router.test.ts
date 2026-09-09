import { TransportRouter, type TransportAttemptMeasurements } from "./transport-router";

let assertions = 0;
function ok(condition: unknown, message: string): void {
    assertions++;
    if (!condition) throw new Error(message);
}

function deferred<T>(): {
    promise: Promise<T>;
    resolve(value: T): void;
    reject(error: unknown): void;
} {
    let resolve!: (value: T) => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}

const retryable = () => ({ retryable: true });

async function healthyWsOwnsTheAttempt(): Promise<void> {
    let ws = 0, http = 0;
    const router = new TransportRouter();
    const result = await router.execute({
        payloadBytes: 1024,
        replay: "content-addressed",
        ws: async (timeouts) => {
            ws++;
            ok(timeouts.creditTimeoutMs === 5_000, "credit timeout default changed");
            ok(timeouts.sendTimeoutMs === 10_000, "send timeout default changed");
            ok(timeouts.ackTimeoutMs > 15_000, "ACK timeout ignored payload service time");
            return "ws";
        },
        http: async () => { http++; return "http"; },
        classifyWsFailure: retryable,
    });
    ok(result === "ws" && ws === 1 && http === 0, "healthy WS did not own the request");
    ok(router.snapshot().circuit === "closed", "healthy WS did not leave circuit closed");
}

async function failuresOpenAndHalfOpenWithoutAProbeStampede(): Promise<void> {
    let now = 0, ws = 0, http = 0;
    const router = new TransportRouter({
        now: () => now,
        jitter: () => 0,
        failureThreshold: 1,
        openBaseMs: 100,
        openMaxMs: 1_000,
        recoverySuccessThreshold: 1,
    });
    const failed = await router.execute({
        payloadBytes: 1,
        replay: "content-addressed",
        ws: async () => { ws++; throw new Error("lost ACK"); },
        http: async () => { http++; return "fallback"; },
        classifyWsFailure: retryable,
    });
    ok(failed === "fallback" && ws === 1 && http === 1,
        "retryable WS failure did not produce one HTTP fallback");
    ok(router.snapshot().circuit === "open" && router.snapshot().openForMs === 100,
        "failure did not open the circuit for the bounded interval");

    const bypassed = await router.execute({
        payloadBytes: 1,
        replay: "content-addressed",
        ws: async () => { ws++; return "unexpected"; },
        http: async () => { http++; return "open-http"; },
        classifyWsFailure: retryable,
    });
    ok(bypassed === "open-http" && ws === 1, "open circuit still attempted WS");

    now = 100;
    const gate = deferred<string>();
    const probe = router.execute({
        payloadBytes: 1,
        replay: "content-addressed",
        ws: async () => { ws++; return gate.promise; },
        http: async () => { http++; return "probe-fallback"; },
        classifyWsFailure: retryable,
    });
    await Promise.resolve();
    const parallel = await router.execute({
        payloadBytes: 1,
        replay: "content-addressed",
        ws: async () => { ws++; return "stampede"; },
        http: async () => { http++; return "parallel-http"; },
        classifyWsFailure: retryable,
    });
    ok(parallel === "parallel-http" && ws === 2,
        "concurrent caller joined the single half-open probe");
    gate.resolve("probe-ok");
    ok(await probe === "probe-ok", "half-open probe result was lost");
    ok(router.snapshot().circuit === "closed", "successful probe did not close circuit");
}

async function retryHintsAndPermanentErrorsAreFailClosed(): Promise<void> {
    let now = 0, http = 0;
    const router = new TransportRouter({
        now: () => now,
        jitter: () => 0,
        failureThreshold: 1,
        openBaseMs: 10,
        openMaxMs: 1_000,
    });
    await router.execute({
        payloadBytes: 0,
        replay: "content-addressed",
        ws: async () => { throw new Error("busy"); },
        http: async () => { http++; return undefined; },
        classifyWsFailure: () => ({ retryable: true, retryAfterMs: 250 }),
    });
    ok(router.snapshot().openForMs === 250, "authenticated retry-after hint was ignored");
    now = 250;
    let rejected = false;
    try {
        await router.execute({
            payloadBytes: 0,
            replay: "content-addressed",
            ws: async () => { throw new Error("invalid payload"); },
            http: async () => { http++; return undefined; },
            classifyWsFailure: () => ({ retryable: false }),
        });
    } catch {
        rejected = true;
    }
    ok(rejected, "permanent WS error was hidden by HTTP fallback");
    ok(http === 1, "permanent WS error dispatched a second transport effect");

    let unsafeDispatches = 0;
    let unsafeRejected = false;
    try {
        await router.execute({
            payloadBytes: 0,
            replay: "unsafe" as any,
            ws: async () => { unsafeDispatches++; return undefined; },
            http: async () => { unsafeDispatches++; return undefined; },
            classifyWsFailure: retryable,
        });
    } catch {
        unsafeRejected = true;
    }
    ok(unsafeRejected, "fallback router accepted an operation without replay proof");
    ok(unsafeDispatches === 0, "fallback router dispatched an operation without replay proof");
}

async function timeoutPolicyIsBoundedAndSizeAware(): Promise<void> {
    const router = new TransportRouter({
        ackBaseTimeoutMs: 1_000,
        ackMinimumBytesPerSecond: 1_000,
        ackMaxTimeoutMs: 5_000,
    });
    ok(router.timeoutsFor(0).ackTimeoutMs === 1_000, "zero payload ACK base changed");
    ok(router.timeoutsFor(2_000).ackTimeoutMs === 3_000, "ACK timeout is not size-aware");
    ok(router.timeoutsFor(100_000).ackTimeoutMs === 5_000, "ACK timeout cap was bypassed");
    let rejected = false;
    try { router.timeoutsFor(-1); } catch { rejected = true; }
    ok(rejected, "negative payload acquired a routing policy");

    let now = 0;
    const jittered = new TransportRouter({
        now: () => now,
        jitter: () => 0.5,
        failureThreshold: 1,
        openBaseMs: 100,
        openMaxMs: 1_000,
    });
    await jittered.execute({
        payloadBytes: 0,
        replay: "content-addressed",
        ws: async () => { throw new Error("offline"); },
        http: async () => undefined,
        classifyWsFailure: retryable,
    });
    ok(jittered.snapshot().openForMs === 110,
        "bounded injectable jitter did not preserve the minimum hold");
}

async function boundedRecoveryHysteresisKeepsBulkOnHttp(): Promise<void> {
    let now = 0, ws = 0, http = 0;
    const seenProbeTimeouts: Array<[number, number, number]> = [];
    const router = new TransportRouter({
        now: () => now,
        jitter: () => 0,
        failureThreshold: 1,
        openBaseMs: 100,
        openMaxMs: 1_000,
        recoverySuccessThreshold: 2,
        recoveryProbeIntervalMs: 50,
        recoveryProbeMaxPayloadBytes: 100,
        creditTimeoutMs: 1_000,
        sendTimeoutMs: 2_000,
        ackBaseTimeoutMs: 3_000,
        ackMaxTimeoutMs: 4_000,
        probeCreditTimeoutMs: 7,
        probeSendTimeoutMs: 8,
        probeAckTimeoutMs: 9,
    });
    const request = (
        lane: "control" | "urgent" | "bulk",
        wsAttempt: () => Promise<string>,
    ) => router.execute({
        payloadBytes: 10,
        replay: "content-addressed" as const,
        lane,
        ws: async timeouts => {
            ws++;
            if (router.snapshot().circuit === "half-open") {
                seenProbeTimeouts.push([
                    timeouts.creditTimeoutMs,
                    timeouts.sendTimeoutMs,
                    timeouts.ackTimeoutMs,
                ]);
            }
            return wsAttempt();
        },
        http: async () => { http++; return "http"; },
        classifyWsFailure: retryable,
    });

    ok(await request("urgent", async () => { throw new Error("lost ACK"); }) === "http",
        "initial lost ACK did not choose the replay-safe HTTP owner");
    now = 100;
    ok(await request("bulk", async () => "bulk-probe") === "http" && ws === 1,
        "bulk became the half-open probe");
    ok(await request("control", async () => "probe-1") === "probe-1",
        "first control recovery probe failed");
    ok(router.snapshot().circuit === "open" &&
        router.snapshot().recoveryWsSuccesses === 1 && router.snapshot().nextProbeInMs === 50,
    "one lucky probe released bulk before hysteresis was satisfied");
    ok(await request("urgent", async () => "too-soon") === "http" && ws === 2,
        "probe interval created an immediate urgent retry storm");

    now = 150;
    ok(await request("control", async () => { throw new Error("second lost ACK"); }) === "http",
        "failed recovery probe lost its single HTTP fallback");
    ok(router.snapshot().recoveryWsSuccesses === 0 && router.snapshot().openForMs === 200,
        "failed recovery probe did not reset evidence and increase bounded backoff");
    now = 350;
    ok(await request("bulk", async () => "oscillating bulk") === "http" && ws === 3,
        "bulk oscillated onto WS after the failed recovery probe");
    ok(await request("control", async () => "probe-2") === "probe-2",
        "recovery probe after backoff failed");
    now = 400;
    ok(await request("urgent", async () => "probe-3") === "probe-3" &&
        router.snapshot().circuit === "closed",
    "two spaced recovery probes did not close the circuit");
    ok(await request("bulk", async () => "healthy-bulk") === "healthy-bulk",
        "healthy WS did not regain bulk after bounded recovery");
    ok(seenProbeTimeouts.length === 4 && seenProbeTimeouts.every(row => row.join() === "7,8,9"),
        "half-open probe did not use bounded phase-specific timeouts");
    ok(http === 5 && router.snapshot().halfOpenInFlight === false,
        "recovery hysteresis duplicated fallback effects or retained a probe owner");
}

async function measuredBulkCanPreferHttpWhileControlStaysOnWs(): Promise<void> {
    let now = 0;
    const carriers: string[] = [];
    const router = new TransportRouter({
        now: () => now,
        performanceMinSamples: 2,
        performanceProbeEvery: 1,
        performanceHysteresisRatio: 1.25,
        performanceHoldMs: 1_000,
    });
    const bulk = () => router.execute({
        payloadBytes: 1_000,
        expectedUsefulBytes: 1_000,
        replay: "content-addressed" as const,
        lane: "bulk" as const,
        ws: async (_timeouts, measurements) => {
            carriers.push("ws");
            measurements.addQueueWaitMs(600);
            measurements.setUsefulBytes(1_000);
            now += 1_000;
            return "ws";
        },
        http: async (measurements) => {
            carriers.push("http");
            measurements.setUsefulBytes(1_000);
            now += 200;
            return "http";
        },
        classifyWsFailure: retryable,
    });

    for (let index = 0; index < 5; index++) await bulk();
    ok(carriers.join() === "ws,ws,http,http,http",
        "confirmed healthy-but-slow WS evidence did not move bulk onto HTTP");
    const measured = router.snapshot();
    ok(measured.bulkPreferenceEstablished && measured.bulkPreferredCarrier === "http",
        "bulk carrier preference was not established from minimum samples");
    ok(measured.wsBulkSamples === 2 && measured.httpBulkSamples === 3,
        "bulk comparison counted an unexpected or unconfirmed sample");
    ok(measured.wsBulkQueueWaitMs === 600 && measured.httpBulkQueueWaitMs === 0,
        "observable carrier queue wait was not retained separately");
    ok((measured.httpBulkThroughputBytesPerSecond ?? 0) >
        (measured.wsBulkThroughputBytesPerSecond ?? Number.MAX_VALUE),
    "useful throughput EWMA did not distinguish the faster carrier");
    ok(measured.lastSelectedCarrier === "http" &&
        measured.lastSelectionReason === "bulk-http-preferred" &&
        measured.lastSelectionLane === "bulk" && measured.lastSelectionPayloadBytes === 1_000 &&
        measured.lastSelectionExpectedUsefulBytes === 1_000,
    "diagnostics did not expose the measured carrier choice and its bounded context");

    const control = await router.execute({
        payloadBytes: 100,
        replay: "content-addressed",
        lane: "control",
        ws: async () => { carriers.push("control-ws"); now += 20; return "ws-control"; },
        http: async () => { carriers.push("control-http"); now += 5; return "http-control"; },
        classifyWsFailure: retryable,
    });
    ok(control === "ws-control" && carriers.at(-1) === "control-ws",
        "bulk throughput preference moved latency-oriented control onto HTTP");
    ok(router.snapshot().wsControlSamples === 1,
        "control outcome was mixed into the bulk evidence window");
    ok(router.snapshot().lastSelectedCarrier === "ws" &&
        router.snapshot().lastSelectionReason === "interactive-ws",
    "diagnostics did not replace the last bulk choice with the actual interactive route");
}

async function hysteresisAndHoldPreventCarrierPingPong(): Promise<void> {
    let now = 0;
    const routed: string[] = [];
    const router = new TransportRouter({
        now: () => now,
        performanceMinSamples: 1,
        performanceProbeEvery: 2,
        performanceHysteresisRatio: 1.25,
        performanceHoldMs: 1_000,
    });
    let wsDuration = 1_000;
    let httpDuration = 100;
    const request = () => router.execute({
        payloadBytes: 1_000,
        replay: "content-addressed" as const,
        lane: "bulk" as const,
        ws: async () => { routed.push("ws"); now += wsDuration; return "ws"; },
        http: async () => { routed.push("http"); now += httpDuration; return "http"; },
        classifyWsFailure: retryable,
    });
    await request(); // initial WS evidence
    await request(); // bounded HTTP exploration
    await request(); // comparison switches to HTTP
    ok(router.snapshot().bulkPreferredCarrier === "http",
        "large measured advantage did not cross switching hysteresis");

    httpDuration = 10_000;
    await request();
    ok(routed.at(-1) === "http" && router.snapshot().bulkPreferredCarrier === "http",
        "preference switched inside its minimum hold window");
    await request();
    ok(routed.at(-1) === "ws" && router.snapshot().bulkPreferredCarrier === "ws",
        "preference did not recover after hold and a large measured reversal");

    now = 0;
    const near = new TransportRouter({
        now: () => now,
        performanceMinSamples: 1,
        performanceProbeEvery: 2,
        performanceHysteresisRatio: 1.25,
        performanceHoldMs: 1,
    });
    const nearRequest = () => near.execute({
        payloadBytes: 1_000,
        replay: "content-addressed" as const,
        lane: "bulk" as const,
        ws: async () => { now += 100; return "ws"; },
        http: async () => { now += 90; return "http"; },
        classifyWsFailure: retryable,
    });
    await nearRequest();
    await nearRequest();
    await nearRequest();
    ok(near.snapshot().bulkPreferenceEstablished &&
        near.snapshot().bulkPreferredCarrier === "ws",
    "small carrier noise crossed the configured hysteresis threshold");
}

async function invalidAndSuspendedSamplesAreBoundedAndIgnored(): Promise<void> {
    let now = 0;
    let visible = true;
    const router = new TransportRouter({
        now: () => now,
        performanceMinSamples: 1,
        performanceProbeEvery: 1_000,
        performanceMaxSampleMs: 500,
        performanceMaxQueueWaitMs: 400,
        performanceMaxUsefulBytes: 1_000,
        performanceMaxThroughputBytesPerSecond: 1_000_000,
    });
    const request = (
        mutate: (measurements: TransportAttemptMeasurements) => void,
    ) => router.execute({
        payloadBytes: 100,
        replay: "content-addressed" as const,
        lane: "bulk" as const,
        sampleValid: () => visible,
        ws: async (_timeouts, measurements) => {
            mutate(measurements);
            return "ws";
        },
        http: async () => "http",
        classifyWsFailure: retryable,
    });

    await request(() => { now += 100; });
    ok(router.snapshot().wsBulkSamples === 1, "valid confirmed outcome was not sampled");
    await request(() => { now += 100; visible = false; });
    visible = true;
    await request(() => { now += 1_000; });
    await request((measurements) => { measurements.addQueueWaitMs(Number.NaN); now += 100; });
    await request((measurements) => { measurements.setUsefulBytes(1_001); now += 100; });
    const excluded = router.snapshot();
    ok(excluded.wsBulkSamples === 1 && excluded.invalidPerformanceSamples === 4,
        "hidden, clock-gap, NaN, or over-cap evidence entered carrier scoring");

    router.resetMeasurements();
    for (let index = 0; index < 70; index++) {
        await request(() => { now += 100; });
    }
    ok(router.snapshot().wsBulkSamples === 64,
        "performance sample counter exceeded its retained bounded window");

    now = 1;
    await request(() => { now += 100; });
    const afterClockReset = router.snapshot();
    ok(afterClockReset.wsBulkSamples === 1 && !afterClockReset.bulkPreferenceEstablished,
        "backwards monotonic clock did not discard incomparable evidence");

    let rejected = 0;
    for (const options of [
        { performanceMinSamples: 0 },
        { performanceMinSamples: 65 },
        { performanceHysteresisRatio: Number.NaN },
        { performanceHysteresisRatio: 1 },
        { performanceMaxSampleMs: 10, performanceMaxQueueWaitMs: 11 },
    ]) {
        try { new TransportRouter(options); } catch { rejected++; }
    }
    ok(rejected === 5, "invalid performance policy options were silently normalized");
    let dispatched = false;
    try {
        await router.execute({
            payloadBytes: 1,
            expectedUsefulBytes: 1_001,
            replay: "content-addressed",
            lane: "bulk",
            ws: async () => { dispatched = true; },
            http: async () => { dispatched = true; },
            classifyWsFailure: retryable,
        });
    } catch { /* expected */ }
    ok(!dispatched, "over-cap expected work reached a carrier callback");
}

async function resetAndFailedOutcomesPreserveSafetyState(): Promise<void> {
    let now = 0;
    const router = new TransportRouter({
        now: () => now,
        jitter: () => 0,
        failureThreshold: 1,
        openBaseMs: 100,
        openMaxMs: 1_000,
    });
    let rejected = false;
    try {
        await router.execute({
            payloadBytes: 100,
            replay: "content-addressed",
            lane: "control",
            ws: async () => { now += 10; throw new Error("lost ACK"); },
            http: async () => { now += 10; throw new Error("HTTP down"); },
            classifyWsFailure: retryable,
        });
    } catch { rejected = true; }
    ok(rejected, "failed fallback outcome was hidden");
    const failed = router.snapshot();
    ok(failed.wsControlSamples === 0 && failed.httpControlSamples === 0,
        "failed carrier outcome was accepted as speed evidence");
    ok(failed.circuit === "open", "retryable WS failure did not retain safety backoff");
    ok(failed.lastSelectedCarrier === "http" &&
        failed.lastSelectionReason === "ws-retryable-fallback",
    "single-owner fallback did not expose its actual carrier and reason");
    router.resetMeasurements();
    const reset = router.snapshot();
    ok(reset.circuit === "open" && reset.consecutiveWsFailures === 1,
        "performance reset weakened circuit/fresh-session failure ownership");
    ok(reset.wsLatencyMs === null && reset.httpLatencyMs === null,
        "performance reset retained stale carrier evidence");
    ok(reset.lastSelectedCarrier === null && reset.lastSelectionReason === null,
        "performance reset retained a stale pre-resume route decision");

    let freshNow = 0;
    const fresh = new TransportRouter({ now: () => freshNow });
    const gate = deferred<void>();
    const inFlight = fresh.execute({
        payloadBytes: 100,
        replay: "content-addressed",
        lane: "bulk",
        ws: async () => { await gate.promise; freshNow += 100; return "ws"; },
        http: async () => "http",
        classifyWsFailure: retryable,
    });
    await Promise.resolve();
    const active = fresh.snapshot();
    ok(active.lastSelectedCarrier === "ws" &&
        active.lastSelectionReason === "bulk-ws-learning" &&
        active.lastSelectionLane === "bulk" && active.lastSelectionPayloadBytes === 100 &&
        active.lastSelectionExpectedUsefulBytes === 100,
    "route diagnostics were unavailable while the selected carrier was in flight");
    fresh.resetMeasurements();
    gate.resolve();
    await inFlight;
    ok(fresh.snapshot().wsBulkSamples === 0,
        "pre-reset in-flight outcome contaminated fresh-session measurements");
}

async function diagnosticsSeparateRequestAndExpectedResponseBytes(): Promise<void> {
    const router = new TransportRouter();
    const result = await router.execute({
        payloadBytes: 128,
        expectedUsefulBytes: 8 * 1024 * 1024,
        replay: "content-addressed",
        lane: "bulk",
        wsUnavailableReason: "ws-payload-too-large",
        http: async () => {
            const active = router.snapshot();
            ok(active.lastSelectedCarrier === "http" &&
                active.lastSelectionReason === "ws-payload-too-large",
            "oversized WS request was diagnosed as a missing carrier");
            ok(active.lastSelectionPayloadBytes === 128 &&
                active.lastSelectionExpectedUsefulBytes === 8 * 1024 * 1024,
            "route diagnostics confused the request with the response bytes used for scoring");
            return "http";
        },
        classifyWsFailure: retryable,
    });
    ok(result === "http", "diagnostic-only WS reason changed HTTP fallback semantics");

    let now = 0;
    let failWs!: (error: Error) => void;
    const delayedFailure = new Promise<never>((_resolve, reject) => { failWs = reject; });
    const concurrent = new TransportRouter({ now: () => now });
    const olderAttempt = concurrent.execute({
        payloadBytes: 1,
        replay: "content-addressed",
        lane: "control",
        ws: async () => delayedFailure,
        http: async () => "fallback",
        classifyWsFailure: retryable,
    });
    await Promise.resolve();
    now = 10;
    await concurrent.execute({
        payloadBytes: 1,
        replay: "content-addressed",
        lane: "control",
        http: async () => "newer-http",
        classifyWsFailure: retryable,
    });
    ok(concurrent.snapshot().lastSelectionAtMs === 10,
        "concurrent HTTP selection did not publish its current timestamp");
    now = 20;
    failWs(new Error("late retryable WS failure"));
    ok(await olderAttempt === "fallback", "late WS failure lost its single HTTP fallback");
    ok(concurrent.snapshot().lastSelectionAtMs === 20,
        "late HTTP fallback reused its pre-WS timestamp and moved diagnostics backwards");
}

void (async () => {
    await healthyWsOwnsTheAttempt();
    await failuresOpenAndHalfOpenWithoutAProbeStampede();
    await retryHintsAndPermanentErrorsAreFailClosed();
    await timeoutPolicyIsBoundedAndSizeAware();
    await boundedRecoveryHysteresisKeepsBulkOnHttp();
    await measuredBulkCanPreferHttpWhileControlStaysOnWs();
    await hysteresisAndHoldPreventCarrierPingPong();
    await invalidAndSuspendedSamplesAreBoundedAndIgnored();
    await resetAndFailedOutcomesPreserveSafetyState();
    await diagnosticsSeparateRequestAndExpectedResponseBytes();
    console.log(`transport-router.test: ${assertions} assertions passed`);
})().catch((error) => {
    setTimeout(() => { throw error; }, 0);
});
