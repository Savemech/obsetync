import { build } from "esbuild";
import { spawn } from "node:child_process";
import { lstat, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const PAIRS = 3;
const SOURCE_BYTES = 64 * 1024 * 1024;
const FEED_BYTES = 256 * 1024;
const DELAY_RESOLUTION_MS = 10;
const pluginDirectory = fileURLToPath(new URL("../", import.meta.url));
let benchmarkStage = "bundle";

// Only the separately bundled fixture worker imports this shim. It delegates
// every read to the actual FileHandle; it does not replace hashing or chunking.
// The extra result field is collected before helper detachment. Production
// sources, protocol types, and worker bundling are not changed.
const workerReadInstrumentation = `
    import { open as nativeOpen, stat } from "node:fs/promises";
    import { parentPort } from "node:worker_threads";
    export { stat };
    let current = null;
    parentPort.on("message", message => {
        if (message?.type === "job") current = {
            jobId: message.job_id, calls: 0, bytes: 0, maxRequestedBytes: 0, maxReturnedBytes: 0
        };
    });
    const nativePost = parentPort.postMessage.bind(parentPort);
    parentPort.postMessage = (message, ...args) => {
        if (message?.type === "result") {
            if (!current || current.jobId !== message.job_id) throw new Error("benchmark read job mismatch");
            const { jobId, ...sourceIO } = current;
            nativePost({ ...message, __benchmarkSourceIO: sourceIO }, ...args);
            current = null;
        } else {
            nativePost(message, ...args);
            if (message?.type === "error") current = null;
        }
    };
    export async function open(...args) {
        const handle = await nativeOpen(...args);
        const nativeRead = handle.read.bind(handle);
        handle.read = async (...readArgs) => {
            const observed = current;
            const result = await nativeRead(...readArgs);
            if (!observed || observed !== current) throw new Error("benchmark read ownership changed");
            observed.calls++;
            observed.bytes += result.bytesRead;
            observed.maxRequestedBytes = Math.max(observed.maxRequestedBytes, readArgs[2]);
            observed.maxReturnedBytes = Math.max(observed.maxReturnedBytes, result.bytesRead);
            return result;
        };
        return handle;
    }
`;

/** Serialized with actual production imports into the private native fixture. */
async function fixtureWorker() {
    let phase = "setup";
    let completed = false;
    process.once("beforeExit", () => {
        if (!completed && !process.exitCode) {
            process.stderr.write(JSON.stringify({ error: "fixture-unfinished", code: "UNFINISHED_" + phase.toUpperCase() }));
            process.exitCode = 1;
        }
    });
    const root = await fs.realpath(process.argv[2]);
    assert(path.isAbsolute(root));
    const target = relative => {
        const resolved = path.resolve(root, relative);
        assert(!path.isAbsolute(relative) && resolved.startsWith(root + path.sep));
        return resolved;
    };
    const counters = () => ({ readCalls: 0, writeCalls: 0, readUtf8Bytes: 0, writeUtf8Bytes: 0,
        maxReadUtf8Bytes: 0, maxWriteUtf8Bytes: 0, headPromotions: 0 });
    let activePlanIO = null;
    const planIO = {
        exists: async relative => {
            try { await fs.stat(target(relative)); return true; }
            catch (error) { if (error.code === "ENOENT") return false; throw error; }
        },
        stat: async relative => {
            try { const value = await fs.stat(target(relative)); return { type: value.isFile() ? "file" : "folder", size: value.size }; }
            catch (error) { if (error.code === "ENOENT") return null; throw error; }
        },
        read: async relative => {
            const raw = await fs.readFile(target(relative), "utf8");
            if (activePlanIO) {
                const bytes = Buffer.byteLength(raw, "utf8");
                activePlanIO.readCalls++;
                activePlanIO.readUtf8Bytes += bytes;
                activePlanIO.maxReadUtf8Bytes = Math.max(activePlanIO.maxReadUtf8Bytes, bytes);
            }
            return raw;
        },
        write: async (relative, raw) => {
            await fs.writeFile(target(relative), raw, "utf8");
            if (activePlanIO) {
                const bytes = Buffer.byteLength(raw, "utf8");
                activePlanIO.writeCalls++;
                activePlanIO.writeUtf8Bytes += bytes;
                activePlanIO.maxWriteUtf8Bytes = Math.max(activePlanIO.maxWriteUtf8Bytes, bytes);
            }
        },
        mkdir: async relative => { await fs.mkdir(target(relative), { recursive: true }); },
        rename: async (from, to) => {
            await fs.rename(target(from), target(to));
            if (activePlanIO && from === `${PREPARED_TRANSFER_PATH}/head.json.next` && to === `${PREPARED_TRANSFER_PATH}/head.json`) {
                activePlanIO.headPromotions++;
            }
        },
        remove: async relative => { await fs.unlink(target(relative)); },
    };
    const round = value => Math.round(value * 1000) / 1000;
    const phases = {};
    async function measure(name, work) {
        phase = name;
        assert.equal(activePlanIO, null);
        const histogram = monitorEventLoopDelay({ resolution: DELAY_RESOLUTION_MS });
        histogram.enable();
        await sleep(DELAY_RESOLUTION_MS * 2); // Arm sampler outside timed work.
        histogram.reset();
        const io = counters();
        activePlanIO = io;
        const started = performance.now();
        try {
            const value = await work();
            const durationMs = performance.now() - started;
            await immediate();
            const samples = Number(histogram.count);
            phases[name] = { durationMs: round(durationMs), planIO: io, eventLoopSamples: samples,
                eventLoopDelayP95Ms: samples ? round(histogram.percentile(95) / 1e6) : null,
                eventLoopDelayMaxMs: samples ? round(histogram.max / 1e6) : null };
            return value;
        } finally { activePlanIO = null; histogram.disable(); }
    }

    const sourcePath = target("synthetic-source.bin");
    let sourceWriteCalls = 0;
    let sourceWrittenBytes = 0;
    const sourceSha256 = await measure("sourcePreparation", async () => {
        const file = await fs.open(sourcePath, "wx");
        const checksum = createHash("sha256");
        const buffer = Buffer.allocUnsafe(1024 * 1024);
        const text = Buffer.from("Synthetic note: revision metadata, links, attachments, and repeated headings.\n", "utf8");
        let state = 0x12345678;
        try {
            for (let offset = 0; offset < SOURCE_BYTES; offset += buffer.length) {
                const length = Math.min(buffer.length, SOURCE_BYTES - offset);
                for (let index = 0; index < length; index++) {
                    state ^= state << 13; state ^= state >>> 17; state ^= state << 5;
                    const position = offset + index;
                    const kind = Math.floor(position / 4096) % 4;
                    buffer[index] = kind === 0 ? text[position % text.length]
                        : kind === 1 ? state & 255
                        : kind === 2 ? (position + Math.floor(position / 1024)) & 31
                        : (state ^ (position >>> 8)) & 255;
                }
                checksum.update(buffer.subarray(0, length));
                let written = 0;
                while (written < length) {
                    const result = await file.write(buffer, written, length - written, offset + written);
                    assert(result.bytesWritten > 0);
                    written += result.bytesWritten;
                    sourceWriteCalls++;
                    sourceWrittenBytes += result.bytesWritten;
                }
            }
        } finally { await file.close(); }
        return checksum.digest("hex");
    });
    const source = await fs.stat(sourcePath);
    assert.equal(source.size, SOURCE_BYTES);
    assert.equal(sourceWrittenBytes, SOURCE_BYTES);
    const scopeHash = await preparedScopeForTree({ vaultId: "synthetic-native-fixture",
        serverUrl: "https://synthetic.invalid", serverBoxPub: "synthetic-server-key",
        deviceId: "synthetic-device", syncObsidianConfig: false, ignorePatterns: [] })(1);
    const manifestPath = "synthetic/native-source.bin";
    let pool;
    let plan;
    let factoryCalls = 0;
    const jobRecords = [];
    const diagnostics = [];
    const pairs = [];
    let baselineManifest;
    let retainedMutationId;
    try {
        await measure("workerStartup", () => new Promise((resolve, reject) => {
            pool = new DesktopHashWorkerPool(() => {
                factoryCalls++;
                return new Worker(workerSource, { eval: true, name: "prepared-manifest-benchmark" });
            }, 1, 1, 1, { onDiagnostic: event => {
                diagnostics.push(event.reason);
                if (event.kind === "ready") resolve();
                else if (event.kind === "failure" || event.kind === "unavailable") {
                    reject(Object.assign(new Error("native worker unavailable"), { code: event.reason }));
                }
            } });
        }));
        async function run(mode) {
            const started = performance.now();
            const result = await pool.run({ absolutePath: sourcePath, expectedSize: SOURCE_BYTES,
                expectedMtime: source.mtimeMs, mode, feedBytes: FEED_BYTES });
            // Capture the bench-only telemetry before the actual helper
            // strips unknown fields while validating/detaching the result.
            const io = result.__benchmarkSourceIO;
            assert(io && Number.isSafeInteger(io.bytes) && io.bytes === SOURCE_BYTES);
            assert(Number.isSafeInteger(io.calls) && io.calls > 0);
            assert(io.maxRequestedBytes > 0 && io.maxRequestedBytes <= FEED_BYTES);
            assert(io.maxReturnedBytes > 0 && io.maxReturnedBytes <= io.maxRequestedBytes);
            jobRecords.push({ mode, wallMs: round(performance.now() - started),
                readMs: round(result.read_ms), hashMs: round(result.hash_ms), sourceIO: { ...io } });
            return result;
        }
        plan = new PreparedTransferPlan(planIO);
        await measure("initialPlanLoad", () => plan.load());
        let retainMs = 0;
        const bootstrap = await measure("bootstrapFreshPreparation", () => resolvePreparedDesktopManifest({
            expectedSize: SOURCE_BYTES, expectedMtime: source.mtimeMs,
            loadHint: async () => plan.lookup(scopeHash, manifestPath),
            discardHint: id => plan.discard(scopeHash, manifestPath, id),
            retain: async result => {
                const started = performance.now();
                const retained = await plan.retain({ scopeHash, path: manifestPath,
                    journalThroughId: 1, expectedMutationId: null,
                    source: { size: result.size, mtime: result.mtime,
                        fingerprint: { kind: "desktop-v1", ...result.fingerprint } }, manifest: result.manifest });
                retainMs = performance.now() - started;
                assert.equal(retained.retained, true);
                retainedMutationId = retained.mutationId;
            },
            run, assertApplicable: () => {},
        }));
        assert.equal(bootstrap.reusedPrepared, false);
        baselineManifest = bootstrap.result.manifest;
        phases.bootstrapFreshPreparation.retainPublicationMs = round(retainMs);
        assert.deepEqual(plan.lookup(scopeHash, manifestPath).manifest, baselineManifest);
        plan.close();
        plan = null;
        assert.deepEqual(jobRecords.map(job => job.mode), ["manifest"]);

        for (let pair = 0; pair < PAIRS; pair++) {
            const order = pair % 2 === 0 ? ["fresh", "reuse"] : ["reuse", "fresh"];
            const sample = { pair: pair + 1, order };
            const pairManifests = {};
            for (const branch of order) {
                const prefix = `pair${pair + 1}${branch}`;
                const jobStart = jobRecords.length;
                if (branch === "fresh") {
                    const result = await measure(prefix, () => run("manifest"));
                    assert.equal(result.mode, "manifest");
                    pairManifests.fresh = result.manifest;
                    sample.fresh = { ...phases[prefix], job: jobRecords[jobStart] };
                } else {
                    // New wrapper/index for every lookup, but explicitly NOT
                    // a fresh process or evicted filesystem cache.
                    plan = new PreparedTransferPlan(planIO);
                    await measure(prefix + "PlanLoad", () => plan.load());
                    let lookupMs = 0;
                    const result = await measure(prefix, () => resolvePreparedDesktopManifest({
                        expectedSize: SOURCE_BYTES, expectedMtime: source.mtimeMs,
                        loadHint: async () => {
                            const started = performance.now();
                            const hint = plan.lookup(scopeHash, manifestPath);
                            lookupMs += performance.now() - started;
                            assert.equal(hint?.mutationId, retainedMutationId);
                            return hint;
                        },
                        discardHint: async () => { throw new Error("unchanged source unexpectedly discarded its hint"); },
                        retain: async () => { throw new Error("reused source unexpectedly ran fresh preparation"); },
                        run, assertApplicable: () => {},
                    }));
                    assert.equal(result.reusedPrepared, true);
                    assert.equal(result.result.read_ms, 0);
                    assert.equal(result.result.hash_ms, 0);
                    assert.equal(round(result.validationReadMs), jobRecords[jobStart].readMs);
                    assert.equal(round(result.validationHashMs), jobRecords[jobStart].hashMs);
                    pairManifests.reuse = result.result.manifest;
                    sample.reuse = { ...phases[prefix], planLoad: phases[prefix + "PlanLoad"],
                        lookupMs: round(lookupMs), validationReadMs: round(result.validationReadMs),
                        validationHashMs: round(result.validationHashMs), job: jobRecords[jobStart] };
                    plan.close();
                    plan = null;
                }
                assert.equal(jobRecords.length, jobStart + 1);
                assert.equal(jobRecords[jobStart].mode, branch === "fresh" ? "manifest" : "hash");
                // Full exact comparison outside the branch timing/sampler.
                assert.deepEqual(pairManifests[branch], baselineManifest);
            }
            assert.deepEqual(pairManifests.fresh, pairManifests.reuse);
            pairs.push(sample);
        }
        const after = await fs.stat(sourcePath);
        assert.equal(after.size, source.size);
        assert.equal(after.mtimeMs, source.mtimeMs);
        assert.equal(after.ctimeMs, source.ctimeMs);
        assert.equal(after.dev, source.dev);
        assert.equal(after.ino, source.ino);
        assert.equal(factoryCalls, 1);
        assert.equal(pool.diagnostics().failureCount, 0);
        assert.equal(pool.diagnostics().restartCount, 0);
        assert.equal(pool.stats().wasmMode, "simd");
        assert.equal(jobRecords.filter(job => job.mode === "manifest").length, PAIRS + 1);
        assert.equal(jobRecords.filter(job => job.mode === "hash").length, PAIRS);
    } finally {
        plan?.close();
        await pool?.close();
    }
    const median = values => [...values].sort((left, right) => left - right)[Math.floor(values.length / 2)];
    const medians = {};
    for (const branch of ["fresh", "reuse"]) {
        const samples = pairs.map(pair => pair[branch]);
        const sampled = samples.filter(value => value.eventLoopDelayP95Ms !== null);
        medians[branch] = { durationMs: median(samples.map(value => value.durationMs)),
            workerWallMs: median(samples.map(value => value.job.wallMs)),
            workerReadMs: median(samples.map(value => value.job.readMs)),
            workerHashMs: median(samples.map(value => value.job.hashMs)),
            eventLoopDelayP95Ms: sampled.length === PAIRS ? median(sampled.map(value => value.eventLoopDelayP95Ms)) : null,
            sampledRuns: sampled.length,
            sourceReadBytes: median(samples.map(value => value.job.sourceIO.bytes)),
            sourceReadCalls: median(samples.map(value => value.job.sourceIO.calls)),
            maxSourceReadBytes: Math.max(...samples.map(value => value.job.sourceIO.maxReturnedBytes)) };
        if (branch === "reuse") {
            medians[branch].newInstancePlanLoadMs = median(samples.map(value => value.planLoad.durationMs));
            medians[branch].lookupMs = median(samples.map(value => value.lookupMs));
        }
    }
    let retainedPlanLogicalBytes = 0;
    let retainedPlanAllocatedBytes = 0;
    let retainedPlanFiles = 0;
    const directories = [root];
    while (directories.length) {
        const directory = directories.pop();
        for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
            assert(!entry.isSymbolicLink());
            const full = path.join(directory, entry.name);
            if (entry.isDirectory()) directories.push(full);
            else if (full !== sourcePath && entry.name !== "benchmark.cjs") {
                const stat = await fs.stat(full);
                assert(stat.isFile());
                retainedPlanLogicalBytes += stat.size;
                retainedPlanAllocatedBytes += stat.blocks * 512;
                retainedPlanFiles++;
            }
        }
    }
    const setupPhases = Object.fromEntries(Object.entries(phases).filter(([key]) => !key.startsWith("pair")));
    process.stdout.write(JSON.stringify({ benchmark: "prepared-manifest-native-worker-sanity", schema: 1,
        platform: process.platform, arch: process.arch, node: process.version,
        corpus: { sourceBytes: SOURCE_BYTES, sourceSha256, sourceWriteCalls, sourceWrittenBytes,
            generator: "xorshift32 seed 0x12345678; alternating 4KiB ASCII/random/counter/random-xor regions",
            fileHash: baselineManifest.file_hash, chunkCount: baselineManifest.chunks.length,
            manifestSha256: createHash("sha256").update(JSON.stringify(baselineManifest)).digest("hex") },
        measurement: { pairs: PAIRS, order: "fresh/reuse, reuse/fresh, fresh/reuse",
            worker: "one actual DesktopHashWorkerPool worker_threads worker with packaged SIMD WASM; no fallback",
            feedBytes: FEED_BYTES, eventLoopResolutionMs: DELAY_RESOLUTION_MS,
            eventLoopDelay: "raw fixture Node sampler including interval; not renderer UI lag",
            fresh: "actual worker manifest job; no plan IO or retain publication in timed comparison branch",
            reuse: "actual helper, new-instance plan replay measured separately, mandatory whole-file hash job",
            cache: "same process and warm worker after bootstrap; OS cache not evicted; new plan instance for every reuse",
            sourceIO: "actual completed FileHandle.read bytes/calls; NOT physical disk IO; same bench-only instrumentation in both modes",
            instrumentation: "read counters and basic telemetry checks included in both branches",
            sourcePreparation: "bounded-buffer deterministic generation, native writes and reference SHA256; excluded from comparisons",
            planWrites: "native filesystem completion plus production readback; no fsync guarantee",
            exactReferenceComparisonsIncludedInTimings: false,
            productionSourceVerificationIncluded: true, performanceGate: null,
            scope: "synthetic native temporary source and preparation only; no upload/network/Obsidian/device/end-to-end throughput proof" },
        setupPhases, medians, pairs, jobCounts: { manifest: PAIRS + 1, hash: PAIRS, nativeWorkers: factoryCalls },
        workerDiagnostics: diagnostics, bootstrapJob: jobRecords[0],
        retainedPlan: { records: 1, logicalBytes: retainedPlanLogicalBytes,
            allocatedBytes: retainedPlanAllocatedBytes, files: retainedPlanFiles },
        verification: { exactFileHash: true, exactAllChunkHashesOffsetsSizes: true,
            sourceMetadataUnchanged: true, wholeFileReadInBothModes: true, newInstancePreparedReplay: true,
            noReusePreparationOrSave: true, noWorkerFallbackOrRestart: true } }));
    completed = true;
}

async function buildBundles() {
    const worker = await build({ absWorkingDir: pluginDirectory, entryPoints: ["src/hash-worker-entry.ts"],
        bundle: true, platform: "node", format: "cjs", target: "node18", loader: { ".wasm": "binary" },
        write: false, minify: true, keepNames: true, logLevel: "silent",
        plugins: [{ name: "prepared-benchmark-native-read-counter", setup(builder) {
            builder.onResolve({ filter: /^node:fs\/promises$/ }, args => {
                if (args.namespace === "prepared-bench-io") return { path: args.path, external: true };
                if (args.importer.endsWith("hash-worker-entry.ts")) return { path: "native-read-counter", namespace: "prepared-bench-io" };
                return undefined;
            });
            builder.onLoad({ filter: /.*/, namespace: "prepared-bench-io" }, () => ({ contents: workerReadInstrumentation, loader: "js" }));
        } }],
    });
    const workerSource = worker.outputFiles[0].text;
    const fixture = await build({ absWorkingDir: pluginDirectory,
        stdin: { contents: `
            import * as fs from "node:fs/promises";
            import * as path from "node:path";
            import { Worker } from "node:worker_threads";
            import { strict as assert } from "node:assert";
            import { createHash } from "node:crypto";
            import { monitorEventLoopDelay, performance } from "node:perf_hooks";
            import { setImmediate as immediate, setTimeout as sleep } from "node:timers/promises";
            import { DesktopHashWorkerPool } from "./src/desktop-hash-workers";
            import { resolvePreparedDesktopManifest } from "./src/prepared-desktop-manifest";
            import { PreparedTransferPlan, PREPARED_TRANSFER_PATH } from "./src/transfer-plan";
            import { preparedScopeForTree } from "./src/transfer-plan-scope";
            const PAIRS = ${PAIRS}, SOURCE_BYTES = ${SOURCE_BYTES}, FEED_BYTES = ${FEED_BYTES};
            const DELAY_RESOLUTION_MS = ${DELAY_RESOLUTION_MS};
            const workerSource = ${JSON.stringify(workerSource)};
            (${fixtureWorker.toString()})().catch(error => {
                const code = typeof error?.code === "string" && /^[A-Z0-9_]{1,64}$/.test(error.code)
                    ? error.code : "WORKER_FAILURE";
                process.stderr.write(JSON.stringify({ error: "fixture-failed", code }));
                process.exitCode = 1;
            });
        `, sourcefile: "prepared-manifest-native-fixture.ts", resolveDir: pluginDirectory, loader: "ts" },
        bundle: true, platform: "node", format: "cjs", target: "node20", write: false, logLevel: "silent",
    });
    return { workerBytes: worker.outputFiles[0].contents.length, fixture: fixture.outputFiles[0].contents };
}

function runFixture(bundlePath, root) {
    return new Promise((resolve, reject) => {
        const child = spawn(process.execPath, [bundlePath, root], { stdio: ["ignore", "pipe", "pipe"] });
        let stdout = "";
        let stderr = "";
        let stoppedCode = null;
        const timeout = setTimeout(() => { stoppedCode = "WORKER_TIMEOUT"; child.kill("SIGKILL"); }, 180_000);
        child.stdout.on("data", chunk => {
            if (stoppedCode) return;
            stdout += chunk;
            if (stdout.length > 1024 * 1024) { stoppedCode = "WORKER_OUTPUT_LIMIT"; child.kill("SIGKILL"); }
        });
        child.stderr.on("data", chunk => { stderr = (stderr + chunk).slice(0, 4096); });
        child.once("error", error => { clearTimeout(timeout); reject(error); });
        child.once("close", code => {
            clearTimeout(timeout);
            if (code !== 0 || stoppedCode) {
                let diagnostic;
                try { diagnostic = JSON.parse(stderr); } catch { diagnostic = { code: "NO_WORKER_REPORT" }; }
                reject(Object.assign(new Error("prepared benchmark worker failed"), { code: stoppedCode ?? diagnostic.code }));
            } else {
                try { resolve(JSON.parse(stdout)); }
                catch { reject(Object.assign(new Error("invalid prepared benchmark report"), {
                    code: stdout.trim() ? "INVALID_WORKER_REPORT" : "EMPTY_WORKER_REPORT" })); }
            }
        });
    });
}

async function main() {
    const args = process.argv.slice(2);
    if (args.length > 1 || (args.length === 1 && args[0] !== "--check")) {
        throw Object.assign(new Error("invalid benchmark arguments"), { code: "INVALID_ARGUMENTS" });
    }
    const bundles = await buildBundles();
    if (args[0] === "--check") {
        process.stdout.write(JSON.stringify({ benchmark: "prepared-manifest-native-worker-sanity",
            check: "bundle-only", timingsRun: false, workerBundleBytes: bundles.workerBytes,
            fixtureBundleBytes: bundles.fixture.length }) + "\n");
        return;
    }
    benchmarkStage = "setup";
    const temporaryParent = await realpath("/tmp");
    const created = await mkdtemp(join(temporaryParent, "obsetync-prepared-manifest-bench-"));
    const root = await realpath(created);
    async function validateOwnedRoot() {
        const stat = await lstat(root);
        if (!stat.isDirectory() || stat.isSymbolicLink() || dirname(root) !== temporaryParent ||
            !/^obsetync-prepared-manifest-bench-[A-Za-z0-9]+$/.test(basename(root)) || await realpath(root) !== root) {
            throw Object.assign(new Error("invalid fixture ownership"), { code: "FIXTURE_OWNERSHIP" });
        }
    }
    let report;
    try {
        await validateOwnedRoot();
        const bundlePath = join(root, "benchmark.cjs");
        await writeFile(bundlePath, bundles.fixture);
        benchmarkStage = "native-fixture";
        report = await runFixture(bundlePath, root);
    } finally {
        await validateOwnedRoot();
        await rm(root, { recursive: true, force: true });
    }
    process.stdout.write(JSON.stringify({ ...report, cleanup: "owned source, plan and generated bundle removed" }, null, 2) + "\n");
}

main().catch(error => {
    const code = typeof error?.code === "string" && /^[A-Z0-9_]{1,64}$/.test(error.code)
        ? error.code : "BENCHMARK_FAILURE";
    process.stderr.write(JSON.stringify({ error: "prepared-manifest-sanity-failed", stage: benchmarkStage, code }) + "\n");
    process.exitCode = 1;
});
