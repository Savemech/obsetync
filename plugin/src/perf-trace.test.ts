import { strict as assert } from "node:assert";
import {
    DEFAULT_PERF_PROFILE,
    PerfTrace,
    perfSampleWeight,
    normalizePerfArchitecture,
    type PerfPlatformProfile,
} from "./perf-trace";

const profile: PerfPlatformProfile = {
    ...DEFAULT_PERF_PROFILE,
    runtime: "desktop",
    architecture: "arm64",
    wasmMode: "scalar",
    hashConcurrency: 1,
    readConcurrency: 4,
    networkConcurrency: 4,
    feedBytes: 65_536,
    batchBytes: 2_097_152,
    diffPageBytes: 0,
};

function run(): void {
    let monotonic = 10;
    let wall = 1_700_000_000_000;
    const trace = new PerfTrace({
        maxRecords: 2,
        monotonicNow: () => monotonic,
        wallNow: () => wall,
        monitorEventLoop: false,
    });
    trace.setProfile(profile);

    const push = trace.begin("push");
    push.setWorkload({
        filesTotal: 10,
        bytesTotal: 1_000,
        filesNeeded: 4,
        bytesNeeded: 250,
    });
    const endRead = push.phase("read");
    monotonic += 7;
    endRead();
    endRead(); // A duplicated finally path must not count a phase twice.
    push.addPhase("hash", 3);
    push.increment({
        filesCompleted: 10,
        bytesTransferred: 250,
        requestCount: 5,
        retries: 1,
        backpressureEvents: 2,
    });
    push.observePeakBatchBytes(128);
    push.observePeakBatchBytes(64);
    push.setDiffPageBytes(524_288);
    push.setWasmChunks({ before: 20, reachable: 12, after: 12 });
    for (let i = 0; i < 95; i++) push.observeEventLoopLag(2);
    for (let i = 0; i < 5; i++) push.observeEventLoopLag(70);
    monotonic += 43;
    wall += 40;
    push.finish("success");
    push.finish("error"); // Finishing twice must not append a second record.

    const first = trace.recent();
    assert.equal(first.length, 1);
    assert.equal(first[0].kind, "push");
    assert.equal(first[0].outcome, "success");
    assert.equal(first[0].durationMs, 50);
    assert.equal(first[0].phases.read, 7);
    assert.equal(first[0].phases.hash, 3);
    assert.equal(first[0].filesTotal, 10);
    assert.equal(first[0].filesNeeded, 4);
    assert.equal(first[0].bytesTransferred, 250);
    assert.equal(first[0].dedupRatio, 0.75);
    assert.equal(first[0].peakBatchBytes, 128);
    assert.equal(first[0].backpressureEvents, 2);
    assert.equal(first[0].wasmChunksBefore, 20);
    assert.equal(first[0].wasmChunksReachable, 12);
    assert.equal(first[0].wasmChunksAfter, 12);
    assert.equal(first[0].eventLoopLagSamples, 100);
    assert.equal(first[0].eventLoopLagP95Ms, 2);
    assert.deepEqual(first[0].profile, { ...profile, diffPageBytes: 524_288 });
    const invalidDiffPage = trace.begin("pull");
    assert.throws(() => invalidDiffPage.setDiffPageBytes(-1), /diffPageBytes/);
    invalidDiffPage.finish("cancelled");

    // Returned records are detached copies, not mutable access to the ring.
    first[0].phases.read = 999;
    first[0].profile.runtime = "mobile";
    assert.equal(trace.recent()[0].phases.read, 7);
    assert.equal(trace.recent()[0].profile.runtime, "desktop");

    monotonic += 5;
    trace.begin("pull").finish("cancelled");
    monotonic += 5;
    trace.begin("scan").finish("error");
    const bounded = trace.recent();
    assert.equal(bounded.length, 2);
    assert.deepEqual(bounded.map((record) => record.kind), ["pull", "scan"]);
    assert.notEqual(bounded[0].operationId, bounded[1].operationId);

    const debug = trace.formatDebug();
    assert.ok(debug.some((line) => line.includes("desktop/arm64")));
    assert.ok(debug.some((line) => line.includes("scan error")));
    // Slashes in numeric progress or visibility labels are not file paths.
    // The exported telemetry schema must not expose identifying fields.
    for (const record of trace.recent()) {
        for (const key of ["path", "filename", "serverUrl", "token", "payload", "error"]) {
            assert.ok(!(key in record), `private field in performance export: ${key}`);
        }
    }

    trace.clear();
    assert.equal(trace.recent().length, 0);

    const delivered: string[] = [];
    const unsubscribe = trace.subscribe((record) => delivered.push(record.operationId));
    let isolatedWarnings = 0;
    const originalWarn = console.warn;
    console.warn = () => { isolatedWarnings++; };
    const unsubscribeFault = trace.subscribe(() => { throw new Error("listener failure injection"); });
    const subscribed = trace.begin("push");
    try {
        subscribed.finish();
    } finally {
        unsubscribeFault();
        console.warn = originalWarn;
    }
    assert.deepEqual(delivered, [subscribed.operationId]);
    assert.equal(isolatedWarnings, 1);
    unsubscribe();
    trace.begin("push").finish();
    assert.equal(delivered.length, 1);

    let blockedMono = 0;
    const blockedTrace = new PerfTrace({
        monotonicNow: () => blockedMono,
        monitorEventLoop: true,
        eventLoopIntervalMs: 25,
    });
    const blocked = blockedTrace.begin("scan");
    blockedMono = 140;
    blocked.finish();
    assert.equal(blockedTrace.recent()[0].eventLoopLagSamples, 1);
    assert.ok((blockedTrace.recent()[0].eventLoopLagP95Ms ?? 0) >= 100);

    // A span that crosses visibility transitions is not evidence of a busy
    // foreground event loop, even when it finishes after becoming visible.
    const visibilityTrace = new PerfTrace({
        monotonicNow: () => blockedMono,
        eventLoopIntervalMs: 25,
    });
    const crossing = visibilityTrace.begin("scan");
    visibilityTrace.setVisible(false);
    blockedMono += 60_000;
    visibilityTrace.setVisible(true);
    crossing.finish();
    assert.equal(visibilityTrace.recent()[0].eventLoopLagSamples, 0);
    assert.equal(visibilityTrace.recent()[0].eventLoopLagP95Ms, null);
    assert.equal(visibilityTrace.recent()[0].eventLoopLagExcludedSamples, 1);

    // A complete visible interval afterwards is still measured. Repeated
    // notifications of the same state must not hide actual terminal lag.
    const visibleAgain = visibilityTrace.begin("scan");
    visibilityTrace.setVisible(true);
    blockedMono += 140;
    visibleAgain.finish();
    assert.equal(visibilityTrace.recent()[1].eventLoopLagSamples, 1);
    assert.ok((visibilityTrace.recent()[1].eventLoopLagP95Ms ?? 0) >= 100);
    assert.equal(visibilityTrace.recent()[1].eventLoopLagExcludedSamples, 0);

    visibilityTrace.setVisible(false);
    const hidden = visibilityTrace.begin("push");
    blockedMono += 60_000;
    hidden.finish();
    assert.equal(visibilityTrace.recent()[2].eventLoopLagSamples, 0);
    assert.equal(visibilityTrace.recent()[2].eventLoopLagExcludedSamples, 1);

    let liveNow = 0;
    const liveTrace = new PerfTrace({
        monotonicNow: () => liveNow,
        monitorEventLoop: false,
    });
    const live = liveTrace.begin("push");
    live.setWorkload({ filesTotal: 100 });
    const endFirst = live.phase("read");
    liveNow = 10;
    const endSecond = live.phase("read");
    const endAck = live.phase("ws_ack_wait");
    liveNow = 40;
    live.increment({ filesCompleted: 2, bytesTransferred: 12 });
    liveNow = 50;
    const snapshot = liveTrace.activeSnapshots()[0];
    assert.equal(snapshot.durationMs, 50);
    assert.equal(snapshot.sinceProgressMs, 10);
    assert.equal(snapshot.filesCompleted, 2);
    assert.deepEqual(snapshot.activePhases, [
        { name: "read", count: 2, durationMs: 50 },
        { name: "ws_ack_wait", count: 1, durationMs: 40 },
    ]);
    snapshot.activePhases[0].count = 99;
    snapshot.phases.read = 999;
    assert.equal(liveTrace.activeSnapshots()[0].activePhases[0].count, 2);
    assert.equal(liveTrace.activeSnapshots()[0].phases.read, undefined);
    assert.ok(liveTrace.formatDebug().some(line => line.includes("ws_ack_wait 40ms")));
    endFirst();
    endFirst();
    assert.equal(liveTrace.activeSnapshots()[0].activePhases[0].count, 1);
    assert.equal(liveTrace.activeSnapshots()[0].phases.read, 50);
    liveNow = 60;
    endSecond();
    endAck();
    assert.deepEqual(liveTrace.activeSnapshots()[0].activePhases, []);
    assert.equal(liveTrace.activeSnapshots()[0].phases.read, 100);
    assert.deepEqual(liveTrace.activeSnapshots(0), []);
    assert.deepEqual(liveTrace.activeSnapshots(-1), []);
    assert.deepEqual(liveTrace.activeSnapshots(Number.NaN), []);
    assert.deepEqual(liveTrace.activeSnapshots(Number.POSITIVE_INFINITY), []);
    assert.ok(!liveTrace.formatDebug(0).some(line => line.includes("running")));
    const lateEnd = live.phase("scheduler_wait");
    live.finish();
    lateEnd();
    assert.deepEqual(liveTrace.activeSnapshots(), []);
    assert.equal(liveTrace.recent().length, 1);
    assert.ok(!liveTrace.formatDebug(0).some(line => line.includes("push success")));

    assert.throws(
        () => new PerfTrace({ maxRecords: 0, monitorEventLoop: false }),
        /maxRecords/,
    );
    assert.equal(normalizePerfArchitecture("arm64"), "arm64");
    assert.equal(normalizePerfArchitecture("aarch64"), "arm64");
    assert.equal(normalizePerfArchitecture("x86_64"), "x64");
    assert.equal(normalizePerfArchitecture("mystery"), "unknown");

    for (const total of [1, 31, 32, 33, 48, 64, 1_000]) {
        const weights = Array.from({ length: total }, (_, index) =>
            perfSampleWeight(index, total));
        assert.equal(
            weights.reduce((sum, weight) => sum + weight, 0),
            total,
            `sample weights did not cover ${total} calls`,
        );
        assert.ok(
            weights.filter((weight) => weight > 0).length <= Math.min(total, 93),
            `sample count was not bounded for ${total} calls`,
        );
    }

    console.log("perf-trace.test: passed");
}

run();
