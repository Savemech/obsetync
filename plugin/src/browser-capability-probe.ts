import { PROBE_FIXTURE_BYTES, PROBE_MAX_HEAP_BYTES, PROBE_MAX_MODULE_BYTES,
    probeFixture, type BrowserProbeResult } from "./browser-probe-protocol";
import { reserveTransientWorkset } from "./transient-memory";

export type BrowserProbeCode = "PASSED" | "UNAVAILABLE" | "STARTUP_FAILED" | "POST_FAILED" |
    "PROTOCOL_FAILED" | "TRANSFER_FAILED" | "HASH_FAILED" | "TIMED_OUT" | "CANCELLED" | "INTERRUPTED" | "ADMISSION_FAILED";

export interface BrowserCapabilityReport {
    code: BrowserProbeCode;
    capturedAt: number;
    durationMs: number;
    workerStarted: boolean;
    jobDispatched: boolean;
    cooperativeDone: boolean;
    transferToWorker: boolean | null;
    transferFromWorker: boolean | null;
    scalarHashMatches: boolean | null;
    simdValidated: boolean | null;
    simdHashMatches: boolean | null;
    scalarHeapBytes: number | null;
    simdHeapBytes: number | null;
    terminationRequested: boolean;
    blobRevoked: boolean;
    cleanupEvidence: "not-started" | "cooperative-result" | "shutdown-request-only" | "shutdown-unconfirmed";
}

export interface ProbeWorker {
    onmessage: ((event: MessageEvent) => void) | null;
    onerror: ((event: ErrorEvent) => void) | null;
    onmessageerror: ((event: MessageEvent) => void) | null;
    postMessage(message: unknown, transfer: Transferable[]): void;
    terminate(): void;
}

export interface ProbeHost {
    available(): boolean;
    createUrl(source: string): string;
    revokeUrl(url: string): void;
    createWorker(url: string): ProbeWorker;
}

const browserHost: ProbeHost = {
    available: () => typeof Worker === "function" && typeof Blob === "function" &&
        typeof URL !== "undefined" && typeof URL.createObjectURL === "function",
    createUrl: source => URL.createObjectURL(new Blob([source], { type: "text/javascript" })),
    revokeUrl: url => URL.revokeObjectURL(url),
    createWorker: url => new Worker(url),
};

/** Explicit opt-in diagnostic, not startup capability detection. Exactly one
 * synthetic job/worker, no retries, file access, fetch or production enablement.
 * Worker.terminate() requests shutdown; it does not prove native heap/RSS GC. */
export async function runBrowserCapabilityProbe(options: {
    source: string;
    scalar: Uint8Array;
    simd: Uint8Array;
    expectedHash: string;
    signal?: AbortSignal;
    host?: ProbeHost;
    timeoutMs?: number;
}): Promise<BrowserCapabilityReport> {
    const host = options.host ?? browserHost;
    const started = performance.now();
    const report: BrowserCapabilityReport = {
        code: "UNAVAILABLE", capturedAt: Date.now(), durationMs: 0, workerStarted: false,
        jobDispatched: false, cooperativeDone: false,
        transferToWorker: null, transferFromWorker: null, scalarHashMatches: null,
        simdValidated: null, simdHashMatches: null, scalarHeapBytes: null, simdHeapBytes: null,
        terminationRequested: false, blobRevoked: false, cleanupEvidence: "not-started",
    };
    if (options.signal?.aborted) return { ...report, code: "CANCELLED" };
    try { if (!host.available()) return report; }
    catch { return report; }
    if (![options.scalar, options.simd].every(bytes => bytes.byteLength > 0 &&
        bytes.byteLength <= PROBE_MAX_MODULE_BYTES) || !/^[a-f0-9]{64}$/.test(options.expectedHash) ||
        options.source.length > 512 * 1024) return { ...report, code: "PROTOCOL_FAILED" };
    // Fixed host/worker heaps remain separately reported, not inferred from
    // these JS/source/Blob/bridge estimates. No files or remote data are read.
    const bytes = 4 * (options.scalar.byteLength + options.simd.byteLength +
        PROBE_FIXTURE_BYTES + 2 * options.source.length) + 64 * 1024;
    const controller = new AbortController();
    const signal = controller.signal;
    const parentAbort = () => controller.abort(options.signal?.reason);
    options.signal?.addEventListener("abort", parentAbort, { once: true });
    if (options.signal?.aborted) parentAbort();
    const timeout = Number.isFinite(options.timeoutMs) ? options.timeoutMs! : 5000;
    const deadline = setTimeout(() => controller.abort("probe-timeout"), Math.max(1, Math.min(timeout, 10000)));
    const abortCode = (): BrowserProbeCode => signal.reason === "probe-timeout" ? "TIMED_OUT"
        : signal.reason === "hidden" || signal.reason === "pagehide" ? "INTERRUPTED" : "CANCELLED";
    let lease;
    try { lease = await reserveTransientWorkset(bytes, { signal }); }
    catch {
        clearTimeout(deadline);
        options.signal?.removeEventListener("abort", parentAbort);
        return { ...report, code: signal.aborted ? abortCode() : "ADMISSION_FAILED",
            durationMs: Math.max(0, performance.now() - started) };
    }
    let worker: ProbeWorker | undefined;
    let url: string | undefined;
    let cancel: (() => void) | undefined;
    try {
        report.code = await new Promise<BrowserProbeCode>(resolve => {
            let settled = false;
            let phase: "ready" | "result" | "released" = "ready";
            const finish = (code: BrowserProbeCode) => {
                if (settled) return;
                settled = true;
                resolve(code);
            };
            cancel = () => finish(abortCode());
            signal.addEventListener("abort", cancel, { once: true });
            try {
                url = host.createUrl(options.source);
                worker = host.createWorker(url);
                worker.onerror = () => finish("STARTUP_FAILED");
                worker.onmessageerror = () => finish("PROTOCOL_FAILED");
                worker.onmessage = (event) => {
                    if (settled) return;
                    const message = event.data;
                    if (phase === "ready" && message?.type === "ready") {
                        report.workerStarted = true;
                        phase = "result";
                        try {
                            const fixture = probeFixture().buffer;
                            const scalar = options.scalar.slice().buffer;
                            const simd = options.simd.slice().buffer;
                            report.jobDispatched = true;
                            worker!.postMessage({ type: "run", fixture, scalar, simd }, [fixture, scalar, simd]);
                            report.transferToWorker = fixture.byteLength === 0 && scalar.byteLength === 0 && simd.byteLength === 0;
                            if (!report.transferToWorker) finish("TRANSFER_FAILED");
                        } catch { finish("POST_FAILED"); }
                        return;
                    }
                    if (phase === "result" && message?.type === "result") {
                        const result = message as BrowserProbeResult;
                        if (!(result.fixture instanceof ArrayBuffer) || result.fixture.byteLength !== PROBE_FIXTURE_BYTES ||
                            !(result.scalar instanceof ArrayBuffer) || result.scalar.byteLength !== options.scalar.byteLength ||
                            !(result.simd instanceof ArrayBuffer) || result.simd.byteLength !== options.simd.byteLength ||
                            typeof result.simdValidated !== "boolean" ||
                            ![result.scalarHeapBytes, result.simdHeapBytes].every(value => value === null ||
                                (Number.isSafeInteger(value) && value > 0 && value <= PROBE_MAX_HEAP_BYTES))) {
                            finish("PROTOCOL_FAILED"); return;
                        }
                        const data = new Uint8Array(result.fixture);
                        if (!data.every((value, index) => value === (index & 255))) { finish("TRANSFER_FAILED"); return; }
                        report.scalarHashMatches = result.scalarHash === options.expectedHash;
                        report.simdValidated = result.simdValidated;
                        report.simdHashMatches = result.simdValidated ? result.simdHash === options.expectedHash : null;
                        report.scalarHeapBytes = result.scalarHeapBytes;
                        report.simdHeapBytes = result.simdHeapBytes;
                        phase = "released";
                        return;
                    }
                    if (phase === "released" && message?.type === "released" && typeof message.detached === "boolean") {
                        report.cooperativeDone = true;
                        report.transferFromWorker = message.detached;
                        finish(!message.detached ? "TRANSFER_FAILED" : !report.scalarHashMatches ||
                            (report.simdValidated && !report.simdHashMatches) ? "HASH_FAILED" : "PASSED");
                        return;
                    }
                    finish("PROTOCOL_FAILED");
                };
                if (signal.aborted) cancel();
            } catch { finish("STARTUP_FAILED"); }
        });
    } finally {
        clearTimeout(deadline);
        options.signal?.removeEventListener("abort", parentAbort);
        if (cancel) signal.removeEventListener("abort", cancel);
        if (worker) {
            worker.onmessage = worker.onerror = worker.onmessageerror = null;
            try { worker.terminate(); report.terminationRequested = true; } catch { /* report no proof of shutdown */ }
            report.cleanupEvidence = report.cooperativeDone && report.terminationRequested ? "cooperative-result"
                : report.terminationRequested ? "shutdown-request-only" : "shutdown-unconfirmed";
        }
        if (url) { try { host.revokeUrl(url); report.blobRevoked = true; } catch { /* report cleanup failure */ } }
        // Browser Worker has no native exit acknowledgement. After shutdown
        // request the diagnostic's scheduling reservation ends, but residual
        // native/module/heap lifetime is UNCONFIRMED on timeout/interruption.
        // Never use this probe as complete-stage budget coverage; the caller
        // fences another attempt after uncertain completion/shutdown failure.
        lease.release();
        report.durationMs = Math.max(0, performance.now() - started);
    }
    return report;
}
