import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";
import { PerfTrace, perfSampleWeight } from "./perf-trace";

const FILES = 8_192;
const BYTES_PER_FILE = 64 * 1024;
const SAMPLES = 7;
const RUNS_PER_SAMPLE = 2;
const payload = Buffer.allocUnsafe(BYTES_PER_FILE);
for (let index = 0; index < payload.length; index++) payload[index] = index & 0xff;

let digestSink = 0;

interface PairedTiming {
    baselineMs: number;
    instrumentedMs: number;
}

/**
 * Interleave the measured arms file-by-file and reverse their order on every
 * other file. Both arms therefore see the same scheduler, cache and thermal
 * window, and both pay the same outer high-resolution timer cost.
 */
function pairedWorkload(sample: number): PairedTiming {
    const trace = new PerfTrace({ maxRecords: 2, monitorEventLoop: false });
    const operation = trace.begin("scan");
    operation.setWorkload({
        filesTotal: FILES,
        bytesTotal: FILES * BYTES_PER_FILE,
        filesNeeded: FILES,
        bytesNeeded: FILES * BYTES_PER_FILE,
    });
    operation.observePeakBatchBytes(BYTES_PER_FILE);

    let baselineNs = 0n;
    let instrumentedNs = 0n;

    const runBaseline = (index: number) => {
        const started = process.hrtime.bigint();
        const bytes = Buffer.from(payload);
        const digest = createHash("sha256").update(bytes).digest();
        baselineNs += process.hrtime.bigint() - started;
        digestSink ^= digest[index & 31];
    };

    const runInstrumented = (index: number) => {
        const started = process.hrtime.bigint();
        const sampleWeight = perfSampleWeight(index, FILES);
        const readStarted = sampleWeight > 0 ? performance.now() : 0;
        const bytes = Buffer.from(payload);
        if (sampleWeight > 0) {
            operation.addPhase(
                "read",
                (performance.now() - readStarted) * sampleWeight,
            );
        }

        const hashStarted = sampleWeight > 0 ? performance.now() : 0;
        const digest = createHash("sha256").update(bytes).digest();
        if (sampleWeight > 0) {
            operation.addPhase("hash", (performance.now() - hashStarted) * sampleWeight);
        }
        instrumentedNs += process.hrtime.bigint() - started;
        digestSink ^= digest[index & 31];
    };

    // Each adjacent pair is measured once in A→B and once in B→A order.
    // This cancels the warm-cache/order bias without relying on an odd number
    // of whole-run A/B samples.
    for (let index = 0; index < FILES; index += 2) {
        if ((index / 2 + sample) % 2 === 0) {
            runBaseline(index);
            runInstrumented(index);
            runInstrumented(index + 1);
            runBaseline(index + 1);
        } else {
            runInstrumented(index);
            runBaseline(index);
            runBaseline(index + 1);
            runInstrumented(index + 1);
        }
    }
    operation.increment({ filesCompleted: FILES });
    operation.finish("success");
    return {
        baselineMs: Number(baselineNs) / 1_000_000,
        instrumentedMs: Number(instrumentedNs) / 1_000_000,
    };
}

function median(values: number[]): number {
    const sorted = [...values].sort((left, right) => left - right);
    return sorted[Math.floor(sorted.length / 2)];
}

// Warm the interleaved loop and OpenSSL before sampling.
pairedWorkload(0);
pairedWorkload(1);

const baseline: number[] = [];
const instrumented: number[] = [];
const pairedOverheadPercent: number[] = [];
const rawRunOverheadPercent: number[] = [];
for (let sample = 0; sample < SAMPLES; sample++) {
    // Combine opposite A/B order patterns into one sample. The strong cache
    // bias visible on tiny-file loops changes sign between these two runs;
    // summing before taking the ratio cancels it instead of cherry-picking an
    // odd median from one ordering.
    const first = pairedWorkload(sample % 2);
    const second = pairedWorkload((sample + 1) % 2);
    const baselineMs = first.baselineMs + second.baselineMs;
    const instrumentedMs = first.instrumentedMs + second.instrumentedMs;
    rawRunOverheadPercent.push(
        ((first.instrumentedMs / first.baselineMs) - 1) * 100,
        ((second.instrumentedMs / second.baselineMs) - 1) * 100,
    );
    baseline.push(baselineMs);
    instrumented.push(instrumentedMs);
    pairedOverheadPercent.push(((instrumentedMs / baselineMs) - 1) * 100);
}

const baselineMedianMs = median(baseline);
const instrumentedMedianMs = median(instrumented);
const instrumentationOverheadMs = median(
    instrumented.map((value, index) => value - baseline[index]),
);
const overheadPercent = median(pairedOverheadPercent);
const report = {
    schemaVersion: 1,
    filesPerRun: FILES,
    runsPerSample: RUNS_PER_SAMPLE,
    filesPerSample: FILES * RUNS_PER_SAMPLE,
    bytesPerFile: BYTES_PER_FILE,
    bytesPerRun: FILES * BYTES_PER_FILE,
    totalBytesPerSample: FILES * BYTES_PER_FILE * RUNS_PER_SAMPLE,
    samples: SAMPLES,
    baselineMedianMs,
    instrumentedMedianMs,
    instrumentationOverheadMs,
    overheadPercent,
    pairedOverheadPercent,
    rawRunOverheadPercent,
    pairedMedianPercent: overheadPercent,
    gatePercent: 2,
    passed: overheadPercent < 2,
    digestSink,
};

console.log(JSON.stringify(report, null, 2));
if (!report.passed) process.exitCode = 1;
