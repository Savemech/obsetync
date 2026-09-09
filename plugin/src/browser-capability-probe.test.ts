import { strict as assert } from "node:assert";
import { runBrowserCapabilityProbe, type ProbeHost, type ProbeWorker } from "./browser-capability-probe";
import { PROBE_FIXTURE_HASH, type BrowserProbeRequest } from "./browser-probe-protocol";
import { reserveTransientWorkset, transientMemorySnapshot } from "./transient-memory";

type Behavior = "success" | "scalar-only" | "bad-hash" | "bad-roundtrip" | "no-transfer" | "timeout" | "malformed";
function fixture(behavior: Behavior = "success", onRun?: () => void) {
    let starts = 0;
    let terminations = 0;
    let revocations = 0;
    let worker: ProbeWorker;
    const host: ProbeHost = {
        available: () => true,
        createUrl: () => "blob:synthetic-test",
        revokeUrl: () => { revocations++; },
        createWorker: () => {
            starts++;
            worker = {
                onmessage: null, onerror: null, onmessageerror: null,
                terminate: () => { terminations++; },
                postMessage: (raw, transfer) => {
                    const input = structuredClone(raw, behavior === "no-transfer" ? undefined : { transfer }) as BrowserProbeRequest;
                    onRun?.();
                    if (behavior === "timeout") return;
                    queueMicrotask(() => {
                        if (behavior === "bad-roundtrip") new Uint8Array(input.fixture)[0] = 99;
                        const message = {
                            type: "result", fixture: input.fixture, scalar: input.scalar, simd: input.simd,
                            scalarHash: behavior === "bad-hash" ? "f".repeat(64) : PROBE_FIXTURE_HASH,
                            scalarHeapBytes: 1024 * 1024,
                            simdValidated: behavior !== "scalar-only",
                            simdHash: behavior === "scalar-only" ? null : PROBE_FIXTURE_HASH,
                            simdHeapBytes: behavior === "scalar-only" ? null : 1024 * 1024,
                        };
                        const response = structuredClone(message, { transfer: [input.fixture, input.scalar, input.simd] });
                        worker.onmessage?.({ data: response } as MessageEvent);
                        worker.onmessage?.({ data: { type: "released", detached: [input.fixture, input.scalar, input.simd]
                            .every(buffer => buffer.byteLength === 0) } } as MessageEvent);
                    });
                },
            };
            queueMicrotask(() => worker.onmessage?.({ data: { type: behavior === "malformed" ? "private-error-text" : "ready" } } as MessageEvent));
            return worker;
        },
    };
    const scalar = new Uint8Array([1, 2]);
    const simd = new Uint8Array([3, 4]);
    const run = (signal?: AbortSignal, timeoutMs = 1000) => runBrowserCapabilityProbe({
        host, source: "test synthetic worker source", scalar, simd,
        expectedHash: PROBE_FIXTURE_HASH, signal, timeoutMs,
    });
    return { host, run, scalar, simd, cleanup: () => ({ starts, terminations, revocations }) };
}

async function tests(): Promise<void> {
    for (const behavior of ["success", "scalar-only"] as const) {
        const f = fixture(behavior);
        const result = await f.run();
        assert.equal(result.code, "PASSED");
        assert.equal(result.cooperativeDone, true);
        assert.equal(result.cleanupEvidence, "cooperative-result");
        assert.equal(result.jobDispatched, true);
        assert.equal(result.transferToWorker, true);
        assert.equal(result.transferFromWorker, true);
        assert.equal(result.scalarHashMatches, true);
        assert.equal(result.simdHashMatches, behavior === "scalar-only" ? null : true);
        assert.equal(f.scalar.byteLength, 2, "probe transferred embedded scalar bytes instead of its own copy");
        assert.equal(f.simd.byteLength, 2, "probe transferred embedded SIMD bytes instead of its own copy");
        assert.deepEqual(f.cleanup(), { starts: 1, terminations: 1, revocations: 1 });
    }
    for (const [behavior, code] of [["bad-hash", "HASH_FAILED"], ["bad-roundtrip", "TRANSFER_FAILED"],
        ["no-transfer", "TRANSFER_FAILED"], ["malformed", "PROTOCOL_FAILED"], ["timeout", "TIMED_OUT"]] as const) {
        const f = fixture(behavior);
        const result = await f.run(undefined, behavior === "timeout" ? 10 : 1000);
        assert.equal(result.code, code);
        assert.deepEqual(f.cleanup(), { starts: 1, terminations: 1, revocations: 1 });
        assert.ok(!JSON.stringify(result).includes("private-error-text"), "raw host text escaped fixed diagnostic schema");
    }
    const missing = fixture();
    missing.host.available = () => false;
    assert.equal((await missing.run()).code, "UNAVAILABLE");
    assert.equal(missing.cleanup().starts, 0);
    const denied = fixture();
    denied.host.createWorker = () => { throw new Error("CSP contains a private URL"); };
    assert.equal((await denied.run()).code, "STARTUP_FAILED");
    assert.equal(denied.cleanup().revocations, 1);
    const copyFailed = fixture();
    copyFailed.scalar.slice = () => { throw new Error("private allocation failure"); };
    const failedCopy = await copyFailed.run();
    assert.equal(failedCopy.code, "POST_FAILED");
    assert.equal(failedCopy.jobDispatched, false);
    assert.deepEqual(copyFailed.cleanup(), { starts: 1, terminations: 1, revocations: 1 });
    assert.ok(!JSON.stringify(failedCopy).includes("private allocation failure"));
    const shutdownFailed = fixture();
    const createWorker = shutdownFailed.host.createWorker;
    shutdownFailed.host.createWorker = url => {
        const worker = createWorker(url);
        worker.terminate = () => { throw new Error("private termination failure"); };
        return worker;
    };
    const uncertain = await shutdownFailed.run();
    assert.equal(uncertain.cooperativeDone, true);
    assert.equal(uncertain.terminationRequested, false);
    assert.equal(uncertain.cleanupEvidence, "shutdown-unconfirmed", "completed hash hid failed termination");
    assert.equal(uncertain.blobRevoked, true, "failed termination prevented independent Blob cleanup");
    assert.ok(!JSON.stringify(uncertain).includes("private termination failure"));
    const controller = new AbortController();
    const hidden = fixture("timeout", () => controller.abort("hidden"));
    const interrupted = await hidden.run(controller.signal);
    assert.equal(interrupted.code, "INTERRUPTED");
    assert.equal(interrupted.cooperativeDone, false);
    assert.equal(interrupted.terminationRequested, true, "interruption did not request shutdown");
    assert.equal(interrupted.cleanupEvidence, "shutdown-request-only", "shutdown request was presented as native cleanup");
    const stopped = fixture();
    controller.abort();
    assert.equal((await stopped.run(controller.signal)).code, "CANCELLED");
    assert.equal(stopped.cleanup().starts, 0);

    const lease = await reserveTransientWorkset(transientMemorySnapshot().capacityBytes);
    try {
        const queued = fixture();
        assert.equal((await queued.run(undefined, 10)).code, "TIMED_OUT", "deadline omitted budget queue time");
        assert.equal(queued.cleanup().starts, 0, "queued probe started before memory admission");
    } finally { lease.release(); }
    console.log("browser-capability-probe.test: synthetic transfer, lifecycle, CSP, interruption and bounded admission passed");
}

void tests().catch(error => { console.error(error); process.exitCode = 1; });
