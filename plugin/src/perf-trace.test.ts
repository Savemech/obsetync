import { strict as assert } from "node:assert";
import {
    DEFAULT_PERF_PROFILE,
    PerfTrace,
    perfSampleWeight,
    normalizePerfArchitecture,
    type PerfOperationWindow,
    type PerfTimerScheduler,
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
        preparedFiles: 10,
        serverConfirmedFiles: 8,
        rootCommittedPaths: 4,
        rootCommittedCuts: 2,
        trackedDeletesCommitted: 1,
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
    assert.equal(first[0].preparedFiles, 10);
    assert.equal(first[0].serverConfirmedFiles, 8);
    assert.equal(first[0].rootCommittedPaths, 4);
    assert.equal(first[0].rootCommittedCuts, 2);
    assert.equal(first[0].trackedDeletesCommitted, 1);
    assert.equal(first[0].dedupRatio, 0.75);
    assert.equal(first[0].peakBatchBytes, 128);
    assert.equal(first[0].backpressureEvents, 2);
    assert.equal(first[0].wasmChunksBefore, 20);
    assert.equal(first[0].wasmChunksReachable, 12);
    assert.equal(first[0].wasmChunksAfter, 12);
    assert.equal(first[0].eventLoopLagSamples, 100);
    assert.equal(first[0].eventLoopLagP95Ms, 2);
    assert.deepEqual(first[0].profile, { ...profile, diffPageBytes: 524_288 });
    assert.ok(trace.formatDebug().some((line) => line.includes("server-confirmed 8")));
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
    live.increment({ filesCompleted: 2, bytesTransferred: 12, preparedFiles: 2,
        serverConfirmedFiles: 1 });
    liveNow = 50;
    const snapshot = liveTrace.activeSnapshots()[0];
    assert.equal(snapshot.durationMs, 50);
    assert.equal(snapshot.sinceProgressMs, 10);
    assert.equal(snapshot.filesCompleted, 2);
    assert.equal(snapshot.preparedFiles, 2);
    assert.equal(snapshot.serverConfirmedFiles, 1);
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

    const boundedCounters = liveTrace.begin("push");
    boundedCounters.increment({ preparedFiles: Number.MAX_SAFE_INTEGER - 1 });
    boundedCounters.increment({ preparedFiles: 10 });
    assert.equal(liveTrace.activeSnapshots().at(-1)?.preparedFiles, Number.MAX_SAFE_INTEGER,
        "milestone counter did not saturate safely");
    assert.throws(() => boundedCounters.increment({ serverConfirmedFiles: 0.5 }), /safe integer/);
    assert.throws(() => boundedCounters.increment({ rootCommittedCuts: -1 }), /safe integer/);
    boundedCounters.finish();

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

    testWindows();
    testOverdueProbeCannotContaminateTheNextWindow();
    console.log("perf-trace.test: passed");
}

function testOverdueProbeCannotContaminateTheNextWindow(): void {
    let now = 0;
    let nextTimer = 1;
    const pending = new Map<ReturnType<typeof setTimeout>, { callback: () => void; delayMs: number }>();
    const timers: PerfTimerScheduler = {
        setTimeout(callback, delayMs) {
            const handle = nextTimer++ as unknown as ReturnType<typeof setTimeout>;
            pending.set(handle, { callback, delayMs });
            return handle;
        },
        clearTimeout(handle) { pending.delete(handle); },
    };
    const fire = (delayMs: number) => {
        const scheduled = Array.from(pending).find(([, timer]) => timer.delayMs === delayMs);
        assert.ok(scheduled, `expected a pending ${delayMs}ms timer`);
        pending.delete(scheduled[0]);
        scheduled[1].callback();
    };
    const trace = new PerfTrace({
        monotonicNow: () => now,
        eventLoopIntervalMs: 250,
        windowIntervalMs: 1_000,
        timers,
    });
    const windows: PerfOperationWindow[] = [];
    const off = trace.subscribeWindows((window) => windows.push(window));
    const scan = trace.begin("scan");
    scan.increment({ filesCompleted: 10 });

    // A suspended/delayed renderer may run its window callback first on resume.
    // The stale lag callback still refers to the probe scheduled at time zero.
    now = 60_000;
    fire(1_000);
    fire(250);
    assert.equal(pending.size, 2, "resume queued catch-up timers instead of one lag and one window timer");
    assert.equal(windows[0].continuousVisible, false);
    assert.equal(windows[0].eventLoopLagP95Ms, null);
    for (const instant of [60_250, 60_500, 60_750]) {
        now = instant;
        fire(250);
    }
    scan.increment({ filesCompleted: 10 });
    now = 61_000;
    fire(1_000);
    assert.equal(windows[1].continuousVisible, true);
    assert.equal(windows[1].eventLoopLagP95Ms, 1, "old 60s probe cannot become fresh UI overload");

    // The ordinary boundary-crossing callback is excluded too, but a genuine
    // new delayed probe entirely inside the next window must remain observable.
    fire(250);
    now = 61_250;
    fire(250);
    now = 61_650;
    fire(250);
    now = 62_000;
    fire(1_000);
    assert.equal(windows[2].continuousVisible, true);
    assert.equal(windows[2].eventLoopLagP95Ms, 256);
    assert.equal(trace.recent().length, 0);
    scan.finish();
    assert.equal(trace.recent()[0].eventLoopLagP95Ms, 256,
        "suspend gap poisoned operation lag or fresh post-resume lag was hidden");
    assert.equal(trace.recent()[0].eventLoopLagExcludedSamples, 1,
        "the stale suspended probe was not classified as excluded evidence");
    off();
    assert.equal(pending.size, 0, "finish/unsubscribe cancel injected timers");
}

function testWindows(): void {
    let now = 0;
    const trace = new PerfTrace({
        monotonicNow: () => now,
        monitorEventLoop: false,
        monitorWindows: false,
    });
    const windows: PerfOperationWindow[] = [];
    const off = trace.subscribeWindows(window => windows.push(window));
    const operation = trace.begin("scan");
    operation.setDemand({ read: 100, hash: 100 });
    operation.increment({ filesCompleted: 20, bytesTransferred: 100, retries: 1 });
    operation.observePeakBatchBytes(256);
    operation.observeEventLoopLag(4);
    now = 1_000;
    trace.sampleWindows();
    assert.deepEqual(windows[0], {
        sequence: 1, operationId: operation.operationId, kind: "scan", outcome: "success",
        startedAtMs: 0, endedAtMs: 1_000, durationMs: 1_000, activeDurationMs: 1_000,
        continuousVisible: true, filesCompleted: 20, bytesTransferred: 100,
        retries: 1, backpressureEvents: 0, peakBatchBytes: 256,
        eventLoopLagP95Ms: 4, demand: { read: 100, hash: 100 },
    });
    windows[0].demand.read = 999;
    operation.increment({ filesCompleted: 3, backpressureEvents: 2 });
    now = 2_000;
    trace.sampleWindows();
    assert.equal(windows[1].filesCompleted, 3, "windows contain deltas, not replayed totals");
    assert.equal(windows[1].bytesTransferred, 0);
    assert.equal(windows[1].retries, 0);
    assert.equal(windows[1].backpressureEvents, 2);
    assert.equal(windows[1].eventLoopLagP95Ms, null, "lag belongs only to its interval");
    assert.equal(windows[1].peakBatchBytes, 0);
    assert.equal(windows[1].demand.read, 100, "listener cannot change operation demand");

    trace.setVisible(false);
    operation.increment({ filesCompleted: 1 });
    operation.observeEventLoopLag(1000);
    now = 3_000;
    trace.sampleWindows();
    trace.setVisible(true);
    operation.observeEventLoopLag(3);
    now = 4_000;
    trace.sampleWindows();
    for (const window of windows.slice(2, 4)) {
        assert.equal(window.continuousVisible, false);
        assert.equal(window.activeDurationMs, 0);
        assert.equal(window.eventLoopLagP95Ms, null);
    }
    operation.observeEventLoopLag(3);
    now = 5_000;
    trace.sampleWindows();
    assert.equal(windows[4].continuousVisible, true);
    assert.equal(windows[4].eventLoopLagP95Ms, 4);
    now = 65_000;
    trace.sampleWindows();
    assert.equal(windows[5].durationMs, 60_000);
    assert.equal(windows[5].activeDurationMs, 0, "late/suspended windows cannot train growth");
    assert.equal(windows[5].continuousVisible, false);
    operation.setDemand({});
    operation.increment({ filesCompleted: 2 });
    now = 65_500;
    operation.finish("cancelled");
    assert.equal(windows[6].outcome, "cancelled");
    assert.equal(windows[6].filesCompleted, 2);
    assert.deepEqual(windows[6].demand, {});
    trace.sampleWindows();
    operation.finish();
    assert.equal(windows.length, 7, "terminal interval is delivered once");
    assert.equal(trace.recent()[0].filesCompleted, 26, "completed totals are unaffected");
    off();

    const late = trace.begin("push");
    late.increment({ filesCompleted: 50 });
    now += 10_000;
    const lateWindows: PerfOperationWindow[] = [];
    const offLate = trace.subscribeWindows(window => lateWindows.push(window));
    now += 1_000;
    late.increment({ filesCompleted: 1 });
    trace.sampleWindows();
    assert.equal(lateWindows[0].durationMs, 1_000);
    assert.equal(lateWindows[0].filesCompleted, 1, "late subscriber starts at a fresh baseline");
    offLate();
    late.increment({ filesCompleted: 10 });
    now += 1_000;
    const offAgain = trace.subscribeWindows(window => lateWindows.push(window));
    now += 1_000;
    trace.sampleWindows();
    assert.equal(lateWindows[1].filesCompleted, 0, "unobserved interval is not replayed");

    let isolated = 0;
    const originalWarn = console.warn;
    console.warn = () => isolated++;
    const badListener = trace.subscribeWindows(() => { throw new Error("injected"); });
    try {
        late.finish("error");
    } finally {
        badListener();
        offAgain();
        console.warn = originalWarn;
    }
    assert.equal(isolated, 1);
    assert.equal(lateWindows.at(-1)?.outcome, "error");
    assert.throws(() => new PerfTrace({ windowIntervalMs: 249 }), /windowIntervalMs/);
    assert.throws(() => new PerfTrace({ windowIntervalMs: Number.NaN }), /windowIntervalMs/);

    // Concurrent operation intervals deliberately retain their overlap so a
    // controller can discard duplicate wall time rather than over-train.
    const offConcurrent = trace.subscribeWindows(window => windows.push(window));
    const a = trace.begin("push");
    const b = trace.begin("pull");
    now += 1_000;
    trace.sampleWindows();
    const [wa, wb] = windows.slice(-2);
    assert.equal(wa.startedAtMs, wb.startedAtMs);
    assert.equal(wa.endedAtMs, wb.endedAtMs);
    assert.ok(wb.sequence > wa.sequence);
    a.finish();
    b.finish();
    offConcurrent();
}

run();
