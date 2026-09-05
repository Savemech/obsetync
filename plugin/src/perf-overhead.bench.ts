import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";
import { PerfTrace, perfSampleWeight } from "./perf-trace";

const FILES = 8_192;
const BYTES_PER_FILE = 64 * 1024;
const SAMPLES = 7;
const RUNS_PER_SAMPLE = 2;
const LIVE_BATCH_FILES = 4;
const SNAPSHOT_EVERY_FILES = 1_024;
const DEBUG_EVERY_FILES = 4_096;
const payload = Buffer.allocUnsafe(BYTES_PER_FILE);
for (let index = 0; index < payload.length; index++) payload[index] = index & 0xff;

let digestSink = 0;
let telemetrySink = 0;

type Scenario = "sampled-phases" | "live-batches";

interface PairedTiming {
    baselineMs: number;
    instrumentedMs: number;
}

/**
 * Interleave the measured arms file-by-file and reverse their order on every
 * other file. Both arms therefore see the same scheduler, cache and thermal
 * window, and both pay the same outer high-resolution timer cost.
 */
function pairedWorkload(sample: number, scenario: Scenario): PairedTiming {
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
    let endBatch: (() => void) | undefined;

    const runBaseline = (index: number) => {
        const started = process.hrtime.bigint();
        const bytes = Buffer.from(payload);
        const digest = createHash("sha256").update(bytes).digest();
        baselineNs += process.hrtime.bigint() - started;
        digestSink ^= digest[index & 31];
    };

    const runInstrumented = (index: number) => {
        const started = process.hrtime.bigint();
        if (scenario === "live-batches" && index % LIVE_BATCH_FILES === 0) {
            endBatch = operation.phase("scan_batch");
            operation.observePeakBatchBytes(LIVE_BATCH_FILES * BYTES_PER_FILE);
        }
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
        if (scenario === "live-batches") {
            // Include allocation/formatting while a batch is open. These are
            // deliberately occasional, not a hot-loop debug poll per file.
            if (index % SNAPSHOT_EVERY_FILES === 0) {
                telemetrySink += trace.activeSnapshots()[0]?.activePhases.length ?? 0;
            }
            if (index % DEBUG_EVERY_FILES === 0) {
                telemetrySink += trace.formatDebug().join("\n").length;
            }
            if ((index + 1) % LIVE_BATCH_FILES === 0) {
                endBatch!();
                endBatch = undefined;
                operation.increment({ filesCompleted: LIVE_BATCH_FILES });
            }
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
    if (scenario === "sampled-phases") operation.increment({ filesCompleted: FILES });
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

function measureScenario(scenario: Scenario) {
    // Warm each exact instrumented arm and OpenSSL before sampling.
    pairedWorkload(0, scenario);
    pairedWorkload(1, scenario);

    const baseline: number[] = [];
    const instrumented: number[] = [];
    const pairedOverheadPercent: number[] = [];
    const rawRunOverheadPercent: number[] = [];
    for (let sample = 0; sample < SAMPLES; sample++) {
        // Combine opposite A/B order patterns into one sample. Both arms do
        // identical copying/hashing and pay the same outer timer costs.
        const first = pairedWorkload(sample % 2, scenario);
        const second = pairedWorkload((sample + 1) % 2, scenario);
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
    const overheadPercent = median(pairedOverheadPercent);
    return {
        scenario,
        baselineMedianMs: median(baseline),
        instrumentedMedianMs: median(instrumented),
        instrumentationOverheadMs: median(
            instrumented.map((value, index) => value - baseline[index]),
        ),
        overheadPercent,
        baselineMs: baseline,
        instrumentedMs: instrumented,
        pairedOverheadPercent,
        rawRunOverheadPercent,
        gatePercent: 2,
        passed: overheadPercent < 2,
    };
}

const scenarios = (["sampled-phases", "live-batches"] as const).map(measureScenario);
const report = {
    schemaVersion: 2,
    filesPerRun: FILES,
    runsPerSample: RUNS_PER_SAMPLE,
    filesPerSample: FILES * RUNS_PER_SAMPLE,
    bytesPerFile: BYTES_PER_FILE,
    bytesPerRun: FILES * BYTES_PER_FILE,
    totalBytesPerSample: FILES * BYTES_PER_FILE * RUNS_PER_SAMPLE,
    samples: SAMPLES,
    liveBatchFiles: LIVE_BATCH_FILES,
    snapshotEveryFiles: SNAPSHOT_EVERY_FILES,
    debugEveryFiles: DEBUG_EVERY_FILES,
    // This isolates CPU instrumentation cost, not storage, scheduling, mobile
    // memory pressure or UI responsiveness. Open span durations include the
    // interleaved baseline arm and are not used as throughput evidence.
    eventLoopProbesEnabled: false,
    scenarios,
    gatePercent: 2,
    passed: scenarios.every((scenario) => scenario.passed),
    digestSink,
    telemetrySink,
};

console.log(JSON.stringify(report, null, 2));
if (!report.passed) process.exitCode = 1;
