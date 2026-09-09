// Bundled by bench-native-root.mjs. Only the Obsidian host shell is a port;
// engine, persistence, AEAD, WASM and the isolated Rust server are real.
import { strict as assert } from "node:assert";
import { readFile } from "node:fs/promises";
import { createHash, webcrypto } from "node:crypto";
import { pathToFileURL } from "node:url";
import { join } from "node:path";
import { TFile } from "obsidian";
import { ObsetyncApi } from "../../src/api";
import { ObsetyncSyncEngine } from "../../src/sync";
import { ObsetyncDesktopIO } from "../../src/platform";
import { HashWorkerFileDriftError } from "../../src/desktop-hash-workers";
import { BulkObjectKind } from "../../src/bulk-codec";
import { ObsetyncSyncBase } from "../../src/sync-base";
import { ObsetyncJournal } from "../../src/journal";
import { RootIntentStore } from "../../src/root-intent";
import { captureRootStreamScope } from "../../src/root-stream-scope";
import { rebuildRootTreeFromBase } from "../../src/root-tree-repair";
import { SerialSettingsWriter } from "../../src/settings-persistence";
import { configureHashTuning, getHashTuning, hashTuningForRuntime } from "../../src/hash-runtime";
import { configureTransientMemory, transientMemorySnapshot } from "../../src/transient-memory";
import { perfTrace } from "../../src/perf-trace";
import { disposeWorkScheduler, workSchedulerSnapshot } from "../../src/work-scheduler";
import { TREE_CANDIDATE_MUTATION_MAX_RELIABLE_STEPS_PER_TURN,
    TREE_CANDIDATE_MUTATION_STEP_UNITS } from "../../src/tree-candidate-mutation-job";
import { RootReviewPreempted } from "../../src/root-reviewed-queue";
import { createNativeRootIO } from "./native-root-io.mjs";
import { createNativeRootMetrics, nearestRankPercentile } from "./native-root-metrics.mjs";
import { sampleNativeRootPublication } from "./native-root-publication.ts";
import { NATIVE_ROOT_AUDIT_INJECTION_PREFIX,
    NATIVE_ROOT_AUDIT_SCENARIO } from "./native-root-audit-diagnostic.mjs";

const VAULT = "native-root-benchmark", BYTES = 4096, MAX_PROBES = 73;
const AUTO_TRIGGER_BOUND_MS = 5_000, AUTO_COMPLETION_BOUND_MS = 60_000;
const STATE_DIR = ".obsidian/plugins/obsetync";
const STATE_PATH = `${STATE_DIR}/native-benchmark-transport.json`;
const VERIFY_PATH = `${STATE_DIR}/native-benchmark-verify.json`;
const pathAt = index => `notes/p-${String(Math.floor(index / 256)).padStart(3, "0")}/n-${String(index).padStart(5, "0")}.md`;
const livePath = index => `live/n-${String(index).padStart(3, "0")}.md`;
const round = value => Math.round(value * 1000) / 1000;
function assertRunning() {
    if (globalThis.__nativeRootStopRequested) throw new Error("native benchmark interrupted");
}
async function waitForCondition(predicate, timeoutMs, label) {
    const started = performance.now();
    for (;;) {
        assertRunning();
        if (predicate()) return performance.now() - started;
        if (performance.now() - started > timeoutMs) throw new Error(`${label} exceeded ${timeoutMs}ms`);
        await new Promise(resolve => setTimeout(resolve, 10));
    }
}
function corpus(index, generation, live = false) {
    const header = `# Native root corpus\nkind=${live ? "live" : "bulk"};index=${index};generation=${generation}\n`;
    return new TextEncoder().encode(header + "abcdefghijklmnopqrstuvwxyz0123456789\n".repeat(120).slice(0, BYTES - header.length));
}
function histogram() {
    const limits = [16, 20, 32, 50, 64, 100, 128, 256, 512, 1024, 2048, 4096, 8192];
    const bins = Array(limits.length + 1).fill(0);
    let samples = 0, maximum = 0, total = 0;
    return { observe(value) { assert(Number.isFinite(value) && value >= 0); samples++; total += value;
        maximum = Math.max(maximum, value); let i = 0; while (i < limits.length && value > limits[i]) i++; bins[i]++; },
    snapshot() { let seen = 0, p95 = null;
        for (let i = 0; i < bins.length; i++) { seen += bins[i]; if (samples && seen >= Math.ceil(samples * 0.95)) { p95 = limits[i] ?? null; break; } }
        return { samples, maxMs: round(maximum), totalMs: round(total), p95UpperBoundMs: p95,
            upperBoundsMs: limits, bins: [...bins], overflow: bins.at(-1) }; } };
}
function percentileSummary(values) {
    return { samples: values.length, p50Ms: nearestRankPercentile(values, .5),
        p95Ms: nearestRankPercentile(values, .95), maxMs: values.length ? Math.max(...values) : null };
}
function ordinalSummary(values) {
    return { samples: values.length, min: values.length ? Math.min(...values) : null,
        p50: nearestRankPercentile(values, .5), p95: nearestRankPercentile(values, .95),
        max: values.length ? Math.max(...values) : null };
}
async function host(directory) {
    const native = await createNativeRootIO(directory);
    // Obsidian's millisecond metadata convention, applied consistently to real
    // lstat results; no invented cache/hash/stat success. Native counters still
    // include the original filesystem calls and validation overhead.
    const adapter = { ...native.adapter, async stat(path) {
        const value = await native.adapter.stat(path);
        return value && { ...value, mtime: Math.floor(value.mtime), ctime: Math.floor(value.ctime) };
    } };
    const files = new Map(), refs = new Set(); let active = null;
    const app = { vault: { adapter, getFiles: () => [...files.values()],
        on(event, callback) { const ref = { event, callback }; refs.add(ref); return ref; },
        offref(ref) { refs.delete(ref); },
    }, workspace: { getActiveFile: () => active, offref() {} } };
    return { native, adapter, app, files,
        async hydrate(path) {
            const stat = await adapter.stat(path); assert.equal(stat?.type, "file");
            const file = Object.assign(new TFile(), { path, stat }); files.set(path, file); return file;
        },
        async write(path, bytes) {
            const slash = path.lastIndexOf("/"); if (slash >= 0) await adapter.mkdir(path.slice(0, slash));
            await adapter.writeBinary(path, bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength));
            const stat = await adapter.stat(path); assert.equal(stat?.type, "file"); assert.equal(stat.size, bytes.length);
            const file = Object.assign(new TFile(), { path, stat }); files.set(path, file); return file;
        },
        setActive(file) { active = file; },
        callback(event, file) {
            const handlers = [...refs].filter(ref => ref.event === event); assert.equal(handlers.length, 1);
            return handlers[0].callback(file);
        },
    };
}
async function wasmModule(pluginDirectory) {
    const bytes = await readFile(join(pluginDirectory, "wasm/sync_core_simd_bg.wasm"));
    const exports = await import(pathToFileURL(join(pluginDirectory, "wasm/sync_core_simd.js")).href);
    const native = await exports.default({ module_or_path: bytes });
    assert(native.memory instanceof WebAssembly.Memory);
    return { wasm: { ...exports }, memory: native.memory, identity: {
        mode: "simd", bytes: bytes.length, sha256: createHash("sha256").update(bytes).digest("hex"),
    } };
}
function fixedTuning() {
    const tuning = { ...hashTuningForRuntime("desktop"), feedBytes: 512 * 1024,
        minFeedBytes: 512 * 1024, maxFeedBytes: 512 * 1024 };
    configureHashTuning(tuning, undefined); configureTransientMemory(tuning);
    perfTrace.setProfile({ runtime: "desktop", architecture: process.arch === "arm64" ? "arm64" : "x64",
        wasmMode: "simd", hashConcurrency: 1, readConcurrency: tuning.readConcurrency,
        networkConcurrency: tuning.networkConcurrency, feedBytes: tuning.feedBytes,
        batchBytes: tuning.maxBatchBytes, diffPageBytes: 0 });
    return getHashTuning();
}
async function seed(f, wasm, api, base, tree, count) {
    for (let first = 0; first < count; first += 256) {
        assertRunning();
        const records = [];
        for (let index = first; index < Math.min(count, first + 256); index++) {
            assertRunning();
            const bytes = corpus(index, 1), hash = wasm.wasm_hash(bytes), path = pathAt(index);
            const file = await f.write(path, bytes);
            base.setEntry(path, hash, file.stat.mtime, bytes.length);
            records.push({ kind: 0, hash, data: bytes });
        }
        await base.save(); await api.putObjects(records);
    }
    await rebuildRootTreeFromBase(base, tree, 1);
    const chunks = wasm.wasm_tree_committed_chunk_hashes(tree);
    for (let first = 0; first < chunks.length; first += 128) {
        assertRunning();
        const records = chunks.slice(first, first + 128).map(hash => {
            const data = wasm.wasm_tree_get_chunk(tree, hash); assert(data); assert.equal(wasm.wasm_hash(data), hash);
            return { kind: 2, hash, data };
        });
        await api.putObjects(records);
    }
    const root = tree.root_hash_hex();
    // Explicit baseline creation, OUTSIDE measurement. New vaults require v1.
    // Every content/index object exists on the real server before this root.
    await api.putRoot(VAULT, tree.root_bytes(), "");
    assert.equal(wasm.wasm_root_hash_from_bytes(await api.getRoot(VAULT)), root);
    base.setTreeBaseRoot(root); base.setLastSyncTimestamp(Date.now()); await base.checkpoint();
    return root;
}
async function queuePrepared(f, wasm, engine, journal, count) {
    for (let first = 0; first < count; first += 256) {
        assertRunning();
        const prepared = [];
        for (let index = first; index < Math.min(count, first + 256); index++) {
            assertRunning();
            const bytes = corpus(index, 2), path = pathAt(index), file = await f.write(path, bytes);
            prepared.push({ action: "modified", path, hash: wasm.wasm_hash(bytes), mtime: file.stat.mtime, size: bytes.length });
        }
        const ids = await Promise.all(prepared.map(change => journal.append({ action: change.action,
            path: change.path, ts: Date.now(), synced: false })));
        prepared.forEach((change, index) => engine.pendingChanges.add(change, ids[index]));
    }
}
async function verifyRemote(api, base, wasm, root, count) {
    assert.equal(wasm.wasm_root_hash_from_bytes(await api.getRoot(VAULT)), root);
    let cursor = null, seen = 0, pages = 0;
    const unique = new Set();
    do {
        assertRunning();
        const page = await api.getDiffPage(VAULT, "0".repeat(64), root, cursor);
        assert(page, "real server did not provide paged cold snapshot"); assert.equal(page.toRoot, root);
        for (const delta of page.deltas) {
            assert(!unique.has(delta.path)); unique.add(delta.path);
            assert.equal(delta.hash, base.getHash(delta.path)); seen++;
        }
        cursor = page.nextCursor; assert(++pages <= 256);
    } while (cursor);
    assert.equal(seen, count); return { pages, entries: seen, exactBaseHashes: true };
}

export async function measureNativeRoot(options, phase) {
    const { pluginDirectory, directory, count, scenario, binaryPath } = options;
    const auditDiagnostic = scenario === NATIVE_ROOT_AUDIT_SCENARIO;
    const liveScenario = scenario === "edits" || scenario === "auto-edits" || auditDiagnostic;
    const autoDiagnostic = scenario === "auto-edits";
    assert(scenario === "quiet" || liveScenario, "invalid native root scenario");
    if (auditDiagnostic) assert(count > NATIVE_ROOT_AUDIT_INJECTION_PREFIX,
        "audit-edits requires a full review larger than one materialization group");
    globalThis.window = globalThis; globalThis.crypto ??= webcrypto;
    // Deliberate HTTP-only baseline. This is not a failed WS fallback run.
    globalThis.WebSocket = undefined;
    const tuning = fixedTuning(), f = await host(directory), { wasm, memory, identity } = await wasmModule(pluginDirectory);
    const { startNativeRootServer } = await import(pathToFileURL(join(pluginDirectory, "scripts/lib/native-root-server.mjs")).href);
    let server, api, engine, tree, writer, heartbeat, liveWork, startWriter, startAutoWriter,
        startAuditWriter, runAuditInjection, base, journal;
    globalThis.__nativeRootStopNow = () => { engine?.stop(); };
    const metrics = createNativeRootMetrics();
    try {
        phase("server-start"); server = await startNativeRootServer({ binaryPath });
        globalThis.__nativeRootRequestUrl = server.requestUrl;
        const enrollment = await server.enroll("native-benchmark");
        let transport = { wireVersion: enrollment.wire_version, esPub: enrollment.Es_pub_initial,
            esPubValidUntil: enrollment.Es_pub_valid_until, lastOutgoingSeq: 0 };
        await f.adapter.mkdir(STATE_DIR);
        writer = new SerialSettingsWriter(async value => {
            const raw = JSON.stringify(value);
            await f.adapter.write(`${STATE_PATH}.next`, raw); await f.adapter.rename(`${STATE_PATH}.next`, STATE_PATH);
            assert.equal(await f.adapter.read(STATE_PATH), raw);
        });
        await writer.save(transport);
        api = new ObsetyncApi(server.syncUrl, enrollment.server_box_pub, enrollment.bearer_token,
            { get: () => ({ ...transport }), update: patch => { transport = { ...transport, ...patch }; return writer.save(transport); } }, "desktop");
        assert.equal((await api.ping()).ok, true);
        assert.equal((await api.negotiateTreeVersion(VAULT)).currentVersion, 1);
        assert(await api.negotiateRootOutcomes(true));
        base = new ObsetyncSyncBase(f.app); journal = new ObsetyncJournal(f.app);
        await base.load(); await journal.load(); tree = new wasm.WasmTree(VAULT, enrollment.device_id);
        tree.set_tree_version(1);
        phase("seed-v1"); const initialRoot = await seed(f, wasm, api, base, tree, count);
        const settings = { serverUrl: server.syncUrl, serverBoxPub: enrollment.server_box_pub,
            vaultId: VAULT, deviceId: enrollment.device_id, enrolled: true, bearerToken: enrollment.bearer_token };
        const captured = captureRootStreamScope(settings, api, api.baseUrl);
        engine = new ObsetyncSyncEngine(f.app, api, new ObsetyncDesktopIO(f.app), base, journal, wasm, tree,
            VAULT, 3600000, "sequential", () => {}, initialRoot, false, "native-benchmark", false, false, [],
            undefined, autoDiagnostic, null, undefined, undefined, { vaultId: VAULT, deviceId: enrollment.device_id,
                scopeHash: captured.scopeHash, assertApiScope: () => captured.assertCurrent(settings, api, api.baseUrl) });
        phase("actual-start"); await engine.start();
        assert.equal(engine.getLastError(), null); assert.equal(engine.getPendingChangeCount(), 0);
        phase("prepare-v2"); await queuePrepared(f, wasm, engine, journal, count);
        base.approveBulkChange(VAULT, count + MAX_PROBES, 0); await base.save();
        assert.equal(journal.unsyncedCount(), count);

        const sampleLimit = count + MAX_PROBES;
        const work = { plans: [], hostYieldsRequested: 0, hostYieldsCompleted: 0,
            materialize: [], materializeCalls: 0, materializeSamplesDropped: 0,
            sourceStatCalls: 0, materializationStatCalls: 0,
            dirtyClaimCalls: 0, dirtyClaimInputVisits: 0, maxDirtyClaimPaths: 0, dirtyClaimed: 0,
            dirtyTakeVisits: 0, dirtyRestoreVisits: 0, rootContinuations: 0,
            sourceDriftRetentionsAccepted: 0,
            contentCheckCalls: 0, contentCheckedObjects: 0, maxContentCheckObjects: 0,
            contentPutCalls: 0, contentPutObjects: 0, contentPutBytes: 0,
            maxContentPutObjects: 0, maxContentPutBytes: 0,
            candidateMutationJobs: 0, candidateMutationStepCalls: 0,
            candidateMutationActiveLoops: 0, candidateMutationStepCounts: [],
            candidateMutationLoopYieldsRequested: 0, candidateMutationLoopYieldsCompleted: 0,
            candidateMutationStepUnits: 0, candidateMutationPlanBarriers: 0,
            candidateMutationCompletedSteps: 0, maxCandidateMutationStepBudget: 0 };
        const diagnostics = { sourceDriftCountersVersion: 2, sourceOrGenerationDrifts: 0,
            retentionsAccepted: 0, terminalFailures: 0, unexpectedFailures: 0, explicitDrainCalls: 0,
            rootPublicationsObserved: 0, rootSamplesDropped: 0, publicationObserverOverlaps: 0,
            publicationObserverMisses: 0, rootApiAdmissionsObserved: 0, acceptedProbeCaptureMisses: 0,
            automaticPushCalls: 0, automaticPushCompleted: 0, automaticPushRejected: 0,
            callbacksWhileAutomaticPush: 0, automaticRetriggerSignals: 0,
            automaticCompletionWaitMs: 0, automaticCompletionElapsedMs: 0,
            automaticCompletionBoundMs: AUTO_COMPLETION_BOUND_MS,
            ...(auditDiagnostic ? {
                auditInitialReviewCalls: 0, auditInjectionCalls: 0,
                auditInitialReviewPreemptions: 0,
                auditPrefixReviewCalls: 0, auditWholeReviewRebuildCalls: 0,
                auditInjectionAfterMaterializationStats: 0,
                auditRootPublicationsAtInjection: 0, auditRootAdmissionsAtInjection: 0,
                auditRootAdmissionsAtCallbacksComplete: 0,
            } : {}) };
        const unexpectedFailureKinds = [];
        const wrap = (target, name, observe) => {
            const original = target[name]; target[name] = function (...args) { return observe(original.bind(this), args); };
        };
        let explicitPushInvocation = false, automaticPushStartedAt;
        const invokeExplicitPush = () => {
            diagnostics.explicitDrainCalls++; explicitPushInvocation = true;
            try { return engine.pushPending(); } finally { explicitPushInvocation = false; }
        };
        if (autoDiagnostic) wrap(engine, "pushPending", (original, args) => {
            if (explicitPushInvocation) return original(...args);
            diagnostics.automaticPushCalls++;
            automaticPushStartedAt ??= performance.now();
            let result;
            try { result = original(...args); }
            catch (error) { diagnostics.automaticPushRejected++; throw error; }
            void Promise.resolve(result).then(
                () => { diagnostics.automaticPushCompleted++; },
                () => { diagnostics.automaticPushRejected++; },
            );
            return result;
        });
        for (const name of ["begin_candidate_update_job", "begin_candidate_delete_job"]) {
            wrap(tree, name, (original, args) => {
                const token = original(...args); work.candidateMutationJobs++;
                work.candidateMutationStepCounts.push(0); work.candidateMutationActiveLoops++;
                return token;
            });
        }
        wrap(tree, "step_candidate_mutation_output_memory_v1_job", (original, args) => {
            assert.equal(work.candidateMutationActiveLoops, 1,
                "candidate mutation step escaped its measured ownership interval");
            const row = original(...args); work.candidateMutationStepCalls++;
            work.candidateMutationStepCounts[work.candidateMutationStepCounts.length - 1]++;
            work.maxCandidateMutationStepBudget = Math.max(work.maxCandidateMutationStepBudget, args[1]);
            work.candidateMutationStepUnits += row.units;
            if (row.phase === "plan ready") work.candidateMutationPlanBarriers++;
            if (row.done) work.candidateMutationCompletedSteps++;
            return row;
        });
        for (const name of ["finish_candidate_mutation_job_deferred", "cancel_candidate_mutation_job_deferred"]) {
            wrap(tree, name, (original, args) => {
                const result = original(...args);
                assert.equal(work.candidateMutationActiveLoops, 1,
                    "candidate mutation ownership ended outside its measured interval");
                work.candidateMutationActiveLoops--;
                return result;
            });
        }
        wrap(engine, "recordSyncFailure", (original, args) => {
            if (args[0] === "push" && args[1] instanceof HashWorkerFileDriftError) {
                diagnostics.sourceOrGenerationDrifts++; diagnostics.terminalFailures++;
            }
            else {
                diagnostics.unexpectedFailures++;
                if (unexpectedFailureKinds.length < 8) {
                    const error = args[1];
                    const origin = ["pull", "push", "scan", "full-scan"].includes(args[0]) ? args[0] : "other";
                    const rawName = error instanceof Error ? error.name : "NonError";
                    const name = /^[A-Za-z][A-Za-z0-9]{0,31}$/.test(rawName) ? rawName : "UnknownError";
                    const rawCode = error && typeof error === "object" && typeof error.code === "string" ? error.code : "NO_CODE";
                    const code = /^[A-Z0-9_]{1,32}$/.test(rawCode) ? rawCode : "UNKNOWN_CODE";
                    unexpectedFailureKinds.push(`${origin}:${name}:${code}`);
                }
            }
            return original(...args);
        });
        let materializationDepth = 0, initialAuditReviewActive = false;
        let auditInitialReviewStartedAt, auditInjectionAt, auditInitialReviewReturnedAt, auditCallbacksCompletedAt;
        let auditPrefixReviewStartedAt, auditPrefixReviewCompletedAt;
        let auditWholeReviewStartedAt, auditWholeReviewCompletedAt;
        wrap(engine.io, "stat", (original, args) => {
            work.sourceStatCalls++; if (materializationDepth) work.materializationStatCalls++; return original(...args);
        });
        wrap(engine, "materializeDirtyChanges", async (original, args) => {
            work.materializeCalls++;
            if (work.materialize.length < sampleLimit) work.materialize.push(args[0].length);
            else work.materializeSamplesDropped++;
            const wholeAuditReview = auditDiagnostic && args[0].length === count;
            const initialAuditReview = wholeAuditReview && diagnostics.auditInitialReviewCalls === 0;
            const prefixAuditReview = auditDiagnostic && args[0].length === 70 &&
                diagnostics.auditInjectionCalls === 1 && diagnostics.auditPrefixReviewCalls === 0;
            const rebuiltWholeAuditReview = wholeAuditReview && !initialAuditReview;
            if (initialAuditReview) {
                diagnostics.auditInitialReviewCalls++;
                assert.equal(initialAuditReviewActive, false, "initial review materialization overlapped itself");
                initialAuditReviewActive = true; auditInitialReviewStartedAt = performance.now();
            } else if (prefixAuditReview) {
                diagnostics.auditPrefixReviewCalls++; auditPrefixReviewStartedAt = performance.now();
            } else if (rebuiltWholeAuditReview) {
                diagnostics.auditWholeReviewRebuildCalls++; auditWholeReviewStartedAt = performance.now();
            }
            materializationDepth++;
            try { return await original(...args); }
            catch (error) {
                if (initialAuditReview && error instanceof RootReviewPreempted) {
                    diagnostics.auditInitialReviewPreemptions++;
                }
                throw error;
            }
            finally {
                materializationDepth--;
                if (initialAuditReview) {
                    initialAuditReviewActive = false; auditInitialReviewReturnedAt = performance.now();
                }
                else if (prefixAuditReview) auditPrefixReviewCompletedAt = performance.now();
                else if (rebuiltWholeAuditReview) auditWholeReviewCompletedAt = performance.now();
            }
        });
        wrap(engine, "waitForHeavyWork", async (original, args) => {
            if (args[0] === "root continuation") work.rootContinuations++;
            const result = await original(...args);
            if (auditDiagnostic && args[0] === "push metadata" && initialAuditReviewActive &&
                diagnostics.auditInjectionCalls === 0) {
                diagnostics.auditInjectionCalls++;
                diagnostics.auditInjectionAfterMaterializationStats = work.materializationStatCalls;
                diagnostics.auditRootPublicationsAtInjection = diagnostics.rootPublicationsObserved;
                diagnostics.auditRootAdmissionsAtInjection = diagnostics.rootApiAdmissionsObserved;
                assert.equal(work.materializationStatCalls, NATIVE_ROOT_AUDIT_INJECTION_PREFIX,
                    "first-review callback wave missed the first materialization boundary");
                assert.equal(diagnostics.rootPublicationsObserved, 0,
                    "first-review callback wave started after root publication");
                assert.equal(diagnostics.rootApiAdmissionsObserved, 0,
                    "first-review callback wave started after root API admission");
                assert.equal(typeof runAuditInjection, "function", "first-review callback owner is absent");
                auditInjectionAt = performance.now(); await runAuditInjection();
            }
            return result;
        });
        wrap(engine.pendingChanges, "claimHints", (original, args) => {
            work.dirtyClaimCalls++; work.dirtyClaimInputVisits += args[0].length;
            work.maxDirtyClaimPaths = Math.max(work.maxDirtyClaimPaths, args[0].length);
            const result = original(...args); if (result) work.dirtyClaimed += result.length; return result;
        });
        wrap(engine.pendingChanges, "take", (original, args) => {
            const result = original(...args); work.dirtyTakeVisits += result.length; return result;
        });
        wrap(engine.pendingChanges, "restore", (original, args) => { work.dirtyRestoreVisits += args[0].length; return original(...args); });
        wrap(api, "checkContent", (original, args) => {
            const hashes = args[0]; assert(Array.isArray(hashes));
            work.contentCheckCalls++; work.contentCheckedObjects += hashes.length;
            work.maxContentCheckObjects = Math.max(work.maxContentCheckObjects, hashes.length);
            return original(...args);
        });
        wrap(api, "putObjects", (original, args) => {
            const records = args[0]; assert(Array.isArray(records));
            const content = records.filter(record => record.kind !== BulkObjectKind.IndexChunk);
            if (content.length > 0) {
                const bytes = content.reduce((sum, record) => sum + record.data.byteLength, 0);
                work.contentPutCalls++; work.contentPutObjects += content.length; work.contentPutBytes += bytes;
                work.maxContentPutObjects = Math.max(work.maxContentPutObjects, content.length);
                work.maxContentPutBytes = Math.max(work.maxContentPutBytes, bytes);
            }
            return original(...args);
        });
        const probes = [], probesByPath = new Map(), roots = []; let activePublication = null;
        const firstCommit = new Promise(resolve => { startWriter = resolve; });
        const autoWriterStart = new Promise(resolve => { startAutoWriter = resolve; });
        const auditWriterStart = new Promise(resolve => { startAuditWriter = resolve; });
        wrap(journal, "append", async (original, args) => {
            const record = probesByPath.get(args[0].path)?.at(-1), id = await original(...args);
            if (record) { record.id = id; record.journalAt = performance.now(); record.epoch = journal.validatedEpoch; }
            return id;
        });
        wrap(journal, "acknowledgeOwned", async (original, args) => {
            await original(...args); const at = performance.now();
            for (const cut of args[1]) for (const probe of probesByPath.get(cut.path) ?? []) {
                if (probe.epoch === args[0] && probe.id <= cut.throughId && probe.ackAt === undefined) probe.ackAt = at;
            }
        });
        wrap(engine.rootRuntime, "publish", async (original, args) => {
            const candidate = args[0], row = { ...sampleNativeRootPublication(candidate), started: performance.now() };
            row.liveCuts = candidate.journalCuts.filter(cut => probesByPath.has(cut.path)).length;
            row.bulkCuts = row.cuts - row.liveCuts;
            const publication = { candidate, row };
            diagnostics.rootPublicationsObserved++;
            if (activePublication !== null) diagnostics.publicationObserverOverlaps++;
            activePublication = publication;
            if (roots.length < sampleLimit) roots.push(row); else diagnostics.rootSamplesDropped++;
            try { const result = await original(...args); row.result = result.status; return result; }
            finally {
                row.completed = performance.now();
                if (activePublication === publication) activePublication = null;
                metrics.sampleMemory(memory);
            }
        });
        wrap(api, "commitRootOutcome", async (original, args) => {
            const publication = activePublication;
            if (!publication) diagnostics.publicationObserverMisses++;
            const admissionOrdinal = ++diagnostics.rootApiAdmissionsObserved;
            if (publication) publication.row.apiAdmitted = performance.now();
            const response = original(...args); startWriter();
            const result = await response, at = performance.now();
            if (publication) { publication.row.reply = at; publication.row.status = result.status; }
            if (publication && result.status === "accepted") {
                for (const cut of publication.candidate.journalCuts) for (const probe of probesByPath.get(cut.path) ?? []) {
                    if (probe.epoch !== publication.candidate.journalEpoch || probe.id > cut.throughId || probe.acceptedAt !== undefined) continue;
                    const entry = publication.candidate.entries.find(entry => entry.path === cut.path);
                    const acceptedGeneration = probesByPath.get(cut.path).find(candidate => candidate.id === cut.throughId &&
                        candidate.epoch === publication.candidate.journalEpoch && candidate.hash === entry?.hash);
                    if (!acceptedGeneration) { diagnostics.acceptedProbeCaptureMisses++; continue; }
                    probe.acceptedAt = at; probe.exact = cut.throughId === probe.id && entry?.hash === probe.hash;
                    probe.coalesced = cut.throughId > probe.id;
                    probe.acceptedRootAdmissionOrdinal = admissionOrdinal;
                }
            }
            return result;
        });
        let legacyCalls = 0;
        wrap(api, "putRoot", (original, args) => { legacyCalls++; return original(...args); });
        const treeMethods = ["begin_candidate", "begin_candidate_update_job", "begin_candidate_delete_job",
            "step_candidate_mutation_output_memory_v1_job", "candidate_update_batch", "candidate_delete_batch", "candidate_root_hash_hex",
            "candidate_root_bytes", "commit_candidate", "abort_candidate", "rebuild_from_entries_in_version", "root_bytes"];
        metrics.wrapSync(tree, treeMethods, "tree");
        metrics.wrapSync(wasm, ["wasm_hash", "wasm_hash_batch", "wasm_root_hash_from_bytes", "wasm_tree_committed_chunk_hashes",
            "wasm_tree_candidate_chunk_hashes", "wasm_tree_new_candidate_chunk_hashes", "wasm_tree_chunk_byte_length", "wasm_tree_get_chunk"], "wasm");
        metrics.wrapAsync(api, ["checkContent", "putObjects", "commitRootOutcome", "queryRootOutcome", "negotiateRootOutcomes"], "api");
        metrics.wrapSync(wasm.Hasher.prototype, ["update", "finalize"], "hasher");
        metrics.wrapAsync(base, ["save", "commitRootPublication"], "base");
        metrics.wrapAsync(engine, ["materializeDirtyChanges"], "engine");
        metrics.wrapAsync(engine.io, ["stat", "readFile", "readFileIdentityVerified"], "sourceIO");
        metrics.wrapAsync(journal, ["append", "acknowledgeOwned"], "journal");
        const heartbeatGaps = histogram(); let previousHeartbeat = performance.now();
        const transientBefore = transientMemorySnapshot(); assert.equal(transientBefore.usedBytes, 0);
        // Seed/setup publications may have admitted low-priority segmented GC.
        // Join its exact IO before resetting the measured native counters.
        // Root requests can leave the coalesced transport-sequence writer on
        // its final actual-native publication. Join the current snapshot too;
        // save() is a barrier without closing later verification traffic.
        await Promise.all([base.drainMaintenance(), journal.drainMaintenance(),
            engine.rootRuntime.drainMaintenance(), writer.save(transport)]);
        f.native.resetCounters(); const serverBefore = server.snapshot();
        phase("measured-drain"); globalThis.__nativeRootWork = work; metrics.sampleMemory(memory);
        const started = performance.now();
        heartbeat = setInterval(() => {
            const now = performance.now(); heartbeatGaps.observe(now - previousHeartbeat); previousHeartbeat = now; metrics.sampleMemory(memory);
        }, 16);
        if (liveScenario) {
            liveWork = (async () => {
                await (autoDiagnostic ? autoWriterStart : auditDiagnostic ? auditWriterStart : firstCommit);
                const emit = async (index, generation) => {
                    assertRunning();
                    assert(probes.length < MAX_PROBES);
                    const path = livePath(index), bytes = corpus(index, generation, true), file = await f.write(path, bytes);
                    const probe = { hash: wasm.wasm_hash(bytes), callbackAt: performance.now(), generation,
                        lastAdmittedRootOrdinalAtCallback: diagnostics.rootApiAdmissionsObserved };
                    probes.push(probe); const prior = probesByPath.get(path) ?? []; prior.push(probe); probesByPath.set(path, prior);
                    const autoWasRunning = autoDiagnostic && engine.autoPushCoalescer?.snapshot().running === true;
                    f.setActive(file); await f.callback(generation === 1 ? "create" : "modify", file); probe.callbackDone = performance.now();
                    if (autoWasRunning) {
                        diagnostics.callbacksWhileAutomaticPush++;
                        if (engine.autoPushCoalescer?.snapshot().pending) diagnostics.automaticRetriggerSignals++;
                    }
                    assert(Number.isSafeInteger(probe.id));
                };
                // The bulk edits case begins after the first real root API
                // admission. The separate auto diagnostic begins after that
                // explicit backlog drain, so its first save can only be owned
                // by the production coalescer rather than the manual caller.
                if (autoDiagnostic) {
                    await emit(0, 1);
                    await waitForCondition(() => diagnostics.automaticPushCalls > 0,
                        AUTO_TRIGGER_BOUND_MS, "automatic save trigger");
                    assert.equal(engine.autoPushCoalescer?.snapshot().running, true,
                        "automatic push trigger settled before retrigger callbacks began");
                }
                for (let index = autoDiagnostic ? 1 : 0; index < 70; index++) await emit(index, 1);
                await emit(69, 2); await emit(69, 3); await emit(69, 4);
                if (auditDiagnostic) {
                    auditCallbacksCompletedAt = performance.now();
                    diagnostics.auditRootAdmissionsAtCallbacksComplete = diagnostics.rootApiAdmissionsObserved;
                    assert.equal(diagnostics.rootApiAdmissionsObserved, 0,
                        "first-review callback wave completed after root API admission");
                }
            })();
            if (auditDiagnostic) runAuditInjection = async () => { startAuditWriter(); await liveWork; };
            // Observe immediately, but retain/join the original writer below.
            void liveWork.catch(() => {});
        }
        await invokeExplicitPush();
        if (autoDiagnostic) startAutoWriter();
        assertRunning();
        if (liveWork) {
            if (auditDiagnostic) assert.equal(diagnostics.auditInjectionCalls, 1,
                "initial review completed without the first-review callback seam");
            assert(roots.length > 0, "drain failed before live writer admission"); await liveWork;
            if (autoDiagnostic) {
                assert(Number.isFinite(automaticPushStartedAt), "automatic save trigger was not observed");
                const elapsed = performance.now() - automaticPushStartedAt;
                assert(elapsed < AUTO_COMPLETION_BOUND_MS, "automatic save completion exceeded its bound");
                diagnostics.automaticCompletionWaitMs = await waitForCondition(() => {
                    const coalescer = engine.autoPushCoalescer?.snapshot();
                    return diagnostics.automaticPushCalls >= 2 &&
                        diagnostics.automaticPushCompleted === diagnostics.automaticPushCalls &&
                        coalescer?.pending === false && coalescer.running === false &&
                        engine.pushDrain === null && engine.getPendingChangeCount() === 0 &&
                        journal.unsyncedCount() === 0 && !engine.hasPendingRootWork();
                }, Math.max(1, AUTO_COMPLETION_BOUND_MS - elapsed), "automatic save completion");
                diagnostics.automaticCompletionElapsedMs = performance.now() - automaticPushStartedAt;
            } else if (engine.getPendingChangeCount()) await invokeExplicitPush();
        }
        // Root/base publications can admit low-priority segmented cleanup,
        // while transport sequence updates can leave one coalesced settings
        // write behind the last response. Drive the explicit cleanup passes
        // and join the settings publication before sampling actual native IO.
        // An already-scheduled empty maintenance turn may settle later; the
        // report claims IO quiescence, not scheduler-owner quiescence.
        await Promise.all([base.drainMaintenance(), journal.drainMaintenance(),
            engine.rootRuntime.drainMaintenance(), writer.save(transport)]);
        diagnostics.retentionsAccepted = work.sourceDriftRetentionsAccepted;
        diagnostics.sourceOrGenerationDrifts += diagnostics.retentionsAccepted;
        const durationMs = performance.now() - started;
        heartbeatGaps.observe(performance.now() - previousHeartbeat); clearInterval(heartbeat); heartbeat = undefined;
        globalThis.__nativeRootWork = undefined;
        metrics.sampleMemory(memory);
        const measured = { work, metrics: metrics.snapshot(), io: f.native.snapshot(),
            serverBefore, serverAfter: server.snapshot(), transientBefore, transientAfter: transientMemorySnapshot(),
            transientPeakScope: "since policy initialization, includes out-of-measurement seed; before/after snapshots retained",
            heartbeat: heartbeatGaps.snapshot() };
        metrics.restore();
        phase("verify-live-state");
        // A retained source drift continues inside the same drain and must not
        // become LastError. A terminal drift is historical and is not cleared
        // by a later explicit drain. Count both classes and still demand exact
        // empty queues/cold state. Never forgive IO/network/other errors.
        assert.equal(diagnostics.unexpectedFailures, 0,
            `unexpected engine failures: ${unexpectedFailureKinds.join(",")}`);
        assert.equal(diagnostics.sourceOrGenerationDrifts,
            diagnostics.retentionsAccepted + diagnostics.terminalFailures);
        assert.deepEqual({ rootSamples: diagnostics.rootSamplesDropped, overlap: diagnostics.publicationObserverOverlaps,
            missing: diagnostics.publicationObserverMisses, probe: diagnostics.acceptedProbeCaptureMisses,
            materialize: work.materializeSamplesDropped },
        { rootSamples: 0, overlap: 0, missing: 0, probe: 0, materialize: 0 },
        "incomplete benchmark observations");
        assert.equal(diagnostics.rootApiAdmissionsObserved, diagnostics.rootPublicationsObserved,
            "root publication/admission ordinal mismatch");
        if (autoDiagnostic) {
            assert.equal(diagnostics.explicitDrainCalls, 1, "auto diagnostic used an explicit retry drain");
            assert(diagnostics.automaticPushCalls >= 2, "save callbacks did not trigger and retrigger automatic push");
            assert.equal(diagnostics.automaticPushCompleted, diagnostics.automaticPushCalls,
                "automatic push owner did not complete");
            assert.equal(diagnostics.automaticPushRejected, 0, "automatic push rejected");
            assert(diagnostics.callbacksWhileAutomaticPush > 0 && diagnostics.automaticRetriggerSignals > 0,
                "callbacks did not retrigger the running engine coalescer");
            assert.equal(diagnostics.terminalFailures, 0, "auto diagnostic recorded a terminal push failure");
            assert(diagnostics.automaticCompletionElapsedMs <= diagnostics.automaticCompletionBoundMs,
                "automatic save completion exceeded its bound");
        }
        if (diagnostics.terminalFailures === 0) assert.equal(engine.getLastError(), null);
        if (scenario === "quiet") assert.equal(diagnostics.sourceOrGenerationDrifts, 0, "quiet source drift count");
        assert.equal(engine.getPendingChangeCount(), 0, "pending changes after drain");
        assert.equal(journal.unsyncedCount(), 0, "unsynced journal entries after drain");
        assert.equal(engine.hasPendingRootWork(), false, "pending root work after drain");
        assert.equal(legacyCalls, 0, "legacy root calls");
        assert.equal(work.hostYieldsRequested, work.hostYieldsCompleted);
        assert(work.candidateMutationJobs > 0); assert.equal(work.candidateMutationPlanBarriers, work.candidateMutationJobs);
        assert.equal(work.candidateMutationCompletedSteps, work.candidateMutationJobs);
        assert.equal(work.candidateMutationActiveLoops, 0);
        assert.equal(work.candidateMutationStepCounts.length, work.candidateMutationJobs);
        assert.equal(work.candidateMutationStepCounts.reduce((total, calls) => total + calls, 0),
            work.candidateMutationStepCalls);
        assert.equal(work.maxCandidateMutationStepBudget, TREE_CANDIDATE_MUTATION_STEP_UNITS);
        const v1ShapeBoundaryAllowance = work.candidateMutationJobs * Math.ceil(options.count / 1_000);
        assert(work.candidateMutationStepCalls <= work.candidateMutationPlanBarriers +
            Math.ceil(work.candidateMutationStepUnits / TREE_CANDIDATE_MUTATION_STEP_UNITS) +
            work.candidateMutationJobs + v1ShapeBoundaryAllowance,
        "candidate mutation native calls exceeded the bounded quantum quotient");
        const minimumCandidateLoopYields = work.candidateMutationStepCounts.reduce((total, calls) =>
            total + Math.floor(calls / TREE_CANDIDATE_MUTATION_MAX_RELIABLE_STEPS_PER_TURN), 0);
        assert(work.candidateMutationLoopYieldsRequested >= minimumCandidateLoopYields &&
            work.candidateMutationLoopYieldsRequested <= work.candidateMutationStepCalls,
        `candidate mutation loop cooperation outside bounds: ${work.candidateMutationLoopYieldsRequested}/${minimumCandidateLoopYields}`);
        assert.equal(work.candidateMutationLoopYieldsCompleted, work.candidateMutationLoopYieldsRequested);
        for (const field of ["hostYieldsFailed", "candidateMutationLoopYieldsFailed", "instrumentationErrors",
            "plansDropped", "batchSamplesDropped", "planCreatesFailed"]) {
            assert.equal(work[field] ?? 0, 0, `incomplete instrumentation: ${field}`);
        }
        assert.equal(work.planCreatesRequested, work.planCreatesCompleted);
        assert.equal(work.planCreatesCompleted, work.plans.length + (work.plansDropped ?? 0));
        assert.equal(measured.metrics.observerErrors, 0, "metrics observer errors");
        assert(Object.entries(measured.metrics.methods).every(([name, method]) => method.active === 0 &&
            method.calls === method.completed && method.errors ===
                (auditDiagnostic && name === "engine.materializeDirtyChanges" ? 1 : 0)));
        const activeIO = [];
        for (const [partition, group] of Object.entries(measured.io)) {
            if (group.inFlight) activeIO.push(`${partition}:adapter=${group.inFlight}`);
            for (const [operation, counters] of Object.entries(group.operations)) {
                if (counters.inFlight) activeIO.push(`${partition}:${operation}=${counters.inFlight}`);
            }
            if (group.native.inFlight) activeIO.push(`${partition}:native=${group.native.inFlight}`);
            for (const [operation, counters] of Object.entries(group.native.operations)) {
                if (counters.inFlight) activeIO.push(`${partition}:native.${operation}=${counters.inFlight}`);
            }
        }
        assert.equal(measured.io.total.inFlight, 0, `native IO still in flight: ${activeIO.join(",")}`);
        assert.equal(measured.transientAfter.usedBytes, 0, "transient admission still retained");
        assert.deepEqual(getHashTuning(), tuning, "fixed tuning changed during benchmark");
        assert.equal(diagnostics.rootPublicationsObserved, roots.length);
        assert(roots.length > 0 && roots.every(row => row.status === "accepted" && row.entries > 0 && row.entries <= 256));
        const expectedCount = count + (liveScenario ? 70 : 0);
        assert.equal(base.entryCount(), expectedCount); assert.equal(tree.total_files(), expectedCount);
        const finalRoot = tree.root_hash_hex(); assert.equal(base.treeBaseRoot, finalRoot);
        if (scenario === "quiet") {
            assert(roots.length >= Math.ceil(count / 256) && roots.length <= count,
                "quiet publication count escaped the bounded slicing range");
            assert.equal(roots.reduce((total, row) => total + row.entries, 0), count,
                "quiet publication slices lost or duplicated paths");
            assert.equal(work.plans.length, 1, "quiet drain rebuilt its full review plan");
            assert.equal(work.materialize[0], count); assert.equal(work.materialize.slice(1).reduce((a, b) => a + b, 0), count);
            assert.equal(work.materializationStatCalls, 2 * count);
            assert.equal(work.dirtyClaimed, count); assert.equal(work.dirtyTakeVisits, 0);
            assert.deepEqual({ checks: work.contentCheckedObjects, puts: work.contentPutObjects,
                bytes: work.contentPutBytes }, { checks: count, puts: count, bytes: count * BYTES },
            "quiet content-pack instrumentation lost or duplicated objects");
            assert.equal(work.contentCheckCalls, roots.length,
                "quiet root selection did not coalesce to one content check call");
            assert.equal(work.contentPutCalls, roots.length,
                "quiet root selection did not coalesce to one content PUT call");
            assert(work.maxContentCheckObjects <= tuning.maxBatchFiles &&
                work.maxContentPutObjects <= tuning.maxBatchFiles &&
                work.maxContentPutBytes <= tuning.maxBatchBytes,
            "quiet content pack exceeded configured count/byte limits");
        } else {
            assert.equal(probes.length, MAX_PROBES);
            assert(probes.every(probe => (probe.exact || probe.coalesced) && probe.journalAt >= probe.callbackAt &&
                probe.acceptedAt >= probe.journalAt && probe.ackAt >= probe.acceptedAt &&
                Number.isSafeInteger(probe.lastAdmittedRootOrdinalAtCallback) &&
                Number.isSafeInteger(probe.acceptedRootAdmissionOrdinal) &&
                probe.acceptedRootAdmissionOrdinal > probe.lastAdmittedRootOrdinalAtCallback));
            for (let index = 0; index < 70; index++) assert.equal(base.getHash(livePath(index)), wasm.wasm_hash(corpus(index, index === 69 ? 4 : 1, true)));
        }
        if (auditDiagnostic) {
            assert.equal(diagnostics.auditInitialReviewCalls, 1);
            assert.equal(diagnostics.auditInjectionCalls, 1);
            assert.equal(diagnostics.auditInitialReviewPreemptions, 1);
            assert.equal(diagnostics.auditPrefixReviewCalls, 1);
            assert.equal(diagnostics.auditWholeReviewRebuildCalls, 1);
            assert.equal(diagnostics.auditInjectionAfterMaterializationStats, NATIVE_ROOT_AUDIT_INJECTION_PREFIX);
            assert.deepEqual([diagnostics.auditRootPublicationsAtInjection,
                diagnostics.auditRootAdmissionsAtInjection,
                diagnostics.auditRootAdmissionsAtCallbacksComplete], [0, 0, 0]);
            assert(probes.every(probe => probe.lastAdmittedRootOrdinalAtCallback === 0),
                "first-review callback observed a root admission");
            assert(Number.isFinite(auditInitialReviewStartedAt) && Number.isFinite(auditInjectionAt) &&
                Number.isFinite(auditCallbacksCompletedAt) && Number.isFinite(auditInitialReviewReturnedAt) &&
                Number.isFinite(auditPrefixReviewStartedAt) && Number.isFinite(auditPrefixReviewCompletedAt) &&
                Number.isFinite(auditWholeReviewStartedAt) && Number.isFinite(auditWholeReviewCompletedAt));
            assert(auditInitialReviewStartedAt <= auditInjectionAt &&
                auditInjectionAt <= auditCallbacksCompletedAt &&
                auditCallbacksCompletedAt <= auditInitialReviewReturnedAt &&
                auditInitialReviewReturnedAt <= auditPrefixReviewStartedAt &&
                auditPrefixReviewStartedAt <= auditPrefixReviewCompletedAt &&
                auditPrefixReviewCompletedAt <= roots[0].apiAdmitted &&
                roots[1].completed <= auditWholeReviewStartedAt &&
                auditWholeReviewStartedAt <= auditWholeReviewCompletedAt &&
                auditWholeReviewCompletedAt <= roots[2].apiAdmitted,
            "first-review prefix/rebuild timing anchors are out of order");
            assert.deepEqual(roots.slice(0, 2).map(row => row.liveCuts), [64, 6],
                "first-review callbacks escaped the one-shot prefix");
            assert.deepEqual(roots.slice(0, 2).map(row => row.entries), [64, 6],
                "first-review prefix mixed unreviewed bulk work into an early root");
            assert(roots.slice(2).length > 0 && roots.slice(2).every(row => row.liveCuts === 0),
                "first-review callbacks leaked into the rebuilt static plan");
            assert.equal(roots.reduce((total, row) => total + row.liveCuts, 0), 70,
                "first-review live paths were lost or duplicated across roots");
            assert.equal(roots.slice(2).reduce((total, row) => total + row.bulkCuts, 0), count,
                "rebuilt static plan lost or duplicated bulk paths");
        }
        const sequence = engine.rootRuntime.lastSequence;
        const remote = await verifyRemote(api, base, wasm, finalRoot, expectedCount);
        assert.equal((await api.queryRootOutcome(VAULT)).last_sequence, sequence);
        const manifest = { schema: 1, scopeHash: await captured.scopeHash, root: finalRoot,
            vaultId: VAULT, deviceId: enrollment.device_id, count, scenario, expectedCount, sequence,
            transportReserved: transport.lastOutgoingSeq };
        await engine.stopAndDrain(); assert.equal(engine.getLifecycleSnapshot().operations.drained, true);
        await f.adapter.write(VERIFY_PATH, JSON.stringify(manifest));
        const exactProbes = probes.filter(probe => probe.exact);
        const callback = { probes: probes.length, exactPublished: exactProbes.length,
            supersededByAcceptedSuccessor: probes.filter(probe => probe.coalesced).length,
            callbackToJournal: percentileSummary(probes.map(probe => probe.journalAt - probe.callbackAt)),
            callbackToCaptureCompletion: percentileSummary(probes.map(probe => probe.callbackDone - probe.callbackAt)),
            callbackToAcceptedCut: percentileSummary(probes.map(probe => probe.acceptedAt - probe.callbackAt)),
            callbackToExactAccepted: percentileSummary(exactProbes.map(probe => probe.acceptedAt - probe.callbackAt)),
            callbackToExactAcceptedRootOpportunity: {
                samples: exactProbes.length,
                lastAdmittedRootOrdinalAtCallback: ordinalSummary(exactProbes.map(probe =>
                    probe.lastAdmittedRootOrdinalAtCallback)),
                exactAcceptedRootAdmissionOrdinal: ordinalSummary(exactProbes.map(probe =>
                    probe.acceptedRootAdmissionOrdinal)),
                rootAdmissionOpportunities: ordinalSummary(exactProbes.map(probe =>
                    probe.acceptedRootAdmissionOrdinal - probe.lastAdmittedRootOrdinalAtCallback)),
            },
            callbackToDurableAdapterAck: percentileSummary(probes.map(probe => probe.ackAt - probe.callbackAt)) };
        const audit = auditDiagnostic ? {
            injectionPhase: "initial-review-materialization",
            injectedAfterMaterializationStats: diagnostics.auditInjectionAfterMaterializationStats,
            initialReviewAttemptMs: auditInitialReviewReturnedAt - auditInitialReviewStartedAt,
            injectionToInitialReviewReturnMs: auditInitialReviewReturnedAt - auditInjectionAt,
            callbackToInitialReviewReturn: percentileSummary(probes.map(probe =>
                auditInitialReviewReturnedAt - probe.callbackAt)),
            initialReviewReturnToExactAccepted: percentileSummary(exactProbes.map(probe =>
                probe.acceptedAt - auditInitialReviewReturnedAt)),
            callbackToFirstRootAdmission: percentileSummary(probes.map(probe =>
                roots[0].apiAdmitted - probe.callbackAt)),
            callbackToPrefixCompleteAdmission: percentileSummary(probes.map(probe =>
                roots[1].apiAdmitted - probe.callbackAt)),
            prefixCompleteToWholeReviewCompleteMs: auditWholeReviewCompletedAt - roots[1].completed,
            wholeReviewMs: auditWholeReviewCompletedAt - auditWholeReviewStartedAt,
            liveCutsByRoot: roots.map(row => row.liveCuts),
            entriesByRoot: roots.map(row => row.entries),
        } : null;
        const wire = { requests: measured.serverAfter.requests - serverBefore.requests,
            requestBytes: measured.serverAfter.requestBytes - serverBefore.requestBytes,
            responseBytes: measured.serverAfter.responseBytes - serverBefore.responseBytes,
            counterScope: "Node requestUrl bridge body bytes; excludes HTTP headers/TCP framing" };
        const contentPacks = {
            checkCalls: work.contentCheckCalls,
            checkedObjects: work.contentCheckedObjects,
            maxCheckedObjects: work.maxContentCheckObjects,
            putCalls: work.contentPutCalls,
            putObjects: work.contentPutObjects,
            putBytes: work.contentPutBytes,
            maxPutObjects: work.maxContentPutObjects,
            maxPutBytes: work.maxContentPutBytes,
            averageCheckedObjects: work.contentCheckCalls ? work.contentCheckedObjects / work.contentCheckCalls : 0,
            averagePutObjects: work.contentPutCalls ? work.contentPutObjects / work.contentPutCalls : 0,
        };
        return { scenario, count, fileBytes: BYTES, durationMs, usefulBytes: count * BYTES,
            bulkFilesPerSecond: count / (durationMs / 1000), usefulMiBPerSecond: count * BYTES / 1048576 / (durationMs / 1000),
            tuning, hashExecution: { workers: 0, mode: "known source hashes; main-thread WASM metadata/intent hashing",
                serverValidatesUploadedContentHash: true, traceHashConcurrency: 1 },
            wasm: identity, roots: roots.map(row => ({ entries: row.entries, cuts: row.cuts,
                ...(auditDiagnostic ? { liveCuts: row.liveCuts, bulkCuts: row.bulkCuts } : {}),
                rootBytes: row.rootBytes,
                apiAdmittedAtMs: row.apiAdmitted - started, acceptedAtMs: row.reply - started,
                publicationMs: row.completed - row.started, requestMs: row.reply - row.apiAdmitted })),
            firstRootAcceptedMs: roots[0].reply - started,
            lastRootAcceptedMs: roots.at(-1).reply - started, callback,
            ...(audit ? { audit } : {}), wire, contentPacks, diagnostics, ...measured,
            scheduler: workSchedulerSnapshot().backend, remote, exactFinalRoot: finalRoot, lastSequence: sequence };
    } finally {
        if (heartbeat !== undefined) clearInterval(heartbeat);
        globalThis.__nativeRootWork = undefined;
        // These are joins of actual native work, not timeout-race completion.
        // Failure of one owner must not skip the independent server/native
        // joins. Tree release requires successful engine drain, not a race.
        const cleanupErrors = []; let engineDrained = !engine;
        if (liveWork) {
            startWriter?.(); startAutoWriter?.(); startAuditWriter?.();
            try { await liveWork; } catch (error) { cleanupErrors.push(error); }
        }
        try { await engine?.stopAndDrain(); engineDrained = true; } catch (error) { cleanupErrors.push(error); }
        try { await Promise.all([base?.closeAndDrain(), journal?.closeAndDrain()]); } catch (error) { cleanupErrors.push(error); }
        try { api?.closeDataLane(); } catch (error) { cleanupErrors.push(error); }
        try { await writer?.closeAndDrain(); } catch (error) { cleanupErrors.push(error); }
        try { metrics.restore(); if (engineDrained) tree?.free(); } catch (error) { cleanupErrors.push(error); }
        try { await server?.close(); } catch (error) { cleanupErrors.push(error); }
        delete globalThis.__nativeRootRequestUrl; delete globalThis.__nativeRootStopNow;
        disposeWorkScheduler();
        if (cleanupErrors.length) throw new AggregateError(cleanupErrors, "native benchmark owner cleanup failed");
    }
}

export async function coldVerifyNativeRoot(options, phase) {
    globalThis.window = globalThis; globalThis.crypto ??= webcrypto;
    fixedTuning(); const f = await host(options.directory), { wasm, memory } = await wasmModule(options.pluginDirectory);
    const manifest = JSON.parse(await f.adapter.read(VERIFY_PATH)); assert.equal(manifest.schema, 1);
    assert.equal(manifest.count, options.count); assert.equal(manifest.scenario, options.scenario);
    const base = new ObsetyncSyncBase(f.app), journal = new ObsetyncJournal(f.app);
    const intents = new RootIntentStore(f.adapter, manifest.scopeHash, bytes => wasm.wasm_hash(bytes));
    const tree = new wasm.WasmTree(manifest.vaultId, manifest.deviceId), metrics = createNativeRootMetrics();
    try {
        phase("cold-store-load"); await base.load(); await journal.load(); await intents.load();
        assert.equal(base.entryCount(), manifest.expectedCount); assert.equal(base.treeBaseRoot, manifest.root);
        assert.equal(journal.unsyncedCount(), 0); assert.equal(intents.pending(), null); assert.equal(intents.lastSequence, manifest.sequence);
        const transport = JSON.parse(await f.adapter.read(STATE_PATH)); assert(transport.lastOutgoingSeq >= manifest.transportReserved);
        phase("cold-source-verification"); let verified = 0;
        for (const path of base.allPaths()) {
            assertRunning();
            const source = new Uint8Array(await f.adapter.readBinary(path)), entry = base.getEntry(path), stat = await f.adapter.stat(path);
            assert.equal(wasm.wasm_hash(source), entry.hash); assert.equal(source.length, entry.size);
            assert.equal(stat.mtime, entry.mtime); assert.equal(stat.size, entry.size); verified++;
        }
        for (let i = 0; i < manifest.count; i++) assert.equal(base.getHash(pathAt(i)), wasm.wasm_hash(corpus(i, 2)));
        if (manifest.scenario === "edits" || manifest.scenario === "auto-edits" ||
            manifest.scenario === NATIVE_ROOT_AUDIT_SCENARIO) {
            for (let i = 0; i < 70; i++) assert.equal(base.getHash(livePath(i)),
                wasm.wasm_hash(corpus(i, i === 69 ? 4 : 1, true)));
        }
        phase("cold-wasm-rebuild"); metrics.wrapSync(tree, ["rebuild_from_entries_in_version"], "tree");
        metrics.sampleMemory(memory); const rebuildStarted = performance.now();
        await rebuildRootTreeFromBase(base, tree, 1);
        const rebuildWallMs = performance.now() - rebuildStarted; metrics.sampleMemory(memory);
        assert.equal(tree.root_hash_hex(), manifest.root); assert.equal(tree.total_files(), manifest.expectedCount);
        return { freshProcess: true, sourceHashesVerified: verified, exactRebuiltRoot: true,
            pendingJournal: 0, pendingRootIntent: false, lastSequence: intents.lastSequence,
            transportReserved: transport.lastOutgoingSeq, rebuildWallMs, rebuildMetrics: metrics.snapshot(), io: f.native.snapshot() };
    } finally {
        try {
            await Promise.all([intents.closeAndDrain(), base.closeAndDrain(), journal.closeAndDrain()]);
        } finally { metrics.restore(); tree.free(); disposeWorkScheduler(); }
    }
}

// Shared by the opt-in crash/recovery gate. Keep these low-level test-shell
// pieces in one place so both gates exercise the same real engine/WASM/native
// adapter setup without turning them into production APIs.
export {
    host as createNativeRootFixtureHost,
    wasmModule as loadNativeRootFixtureWasm,
    fixedTuning as configureNativeRootFixtureTuning,
    seed as seedNativeRootFixture,
    corpus as nativeRootFixtureCorpus,
    pathAt as nativeRootFixturePathAt,
    VAULT as NATIVE_ROOT_FIXTURE_VAULT,
};
