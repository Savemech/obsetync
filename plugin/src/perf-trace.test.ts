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
    });
    push.observePeakBatchBytes(128);
    push.observePeakBatchBytes(64);
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
    assert.equal(first[0].wasmChunksBefore, 20);
    assert.equal(first[0].wasmChunksReachable, 12);
    assert.equal(first[0].wasmChunksAfter, 12);
    assert.equal(first[0].eventLoopLagSamples, 100);
    assert.equal(first[0].eventLoopLagP95Ms, 2);
    assert.deepEqual(first[0].profile, profile);

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
    assert.ok(debug.every((line) => !line.includes("/") || line.includes("desktop/arm64")));

    trace.clear();
    assert.equal(trace.recent().length, 0);

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

    console.log("perf-trace.test: 53 assertions passed");
}

run();
