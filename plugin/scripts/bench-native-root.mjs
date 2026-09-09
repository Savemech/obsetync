import { strict as assert } from "node:assert";
import { build } from "esbuild";
import { lstat, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { basename, dirname, isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";
import { cpus, release } from "node:os";
import { createHash } from "node:crypto";
import { nativeRootInstrumentationPlugin } from "./lib/native-root-instrumentation.mjs";
import { runNativeRootChild } from "./lib/native-root-child.mjs";
import { NATIVE_ROOT_MAX_RUNS, nativeRootExecutionSchedule,
    nativeRootQualificationStatus, summarizeNativeRootResults } from "./lib/native-root-qualification.mjs";
import { NATIVE_ROOT_AUTO_SCENARIO, nativeRootAutoDiagnosticStatus,
    nativeRootAutoExecutionSchedule, summarizeNativeRootAutoResults } from "./lib/native-root-auto-diagnostic.mjs";
import { NATIVE_ROOT_AUDIT_SCENARIO, nativeRootAuditDiagnosticStatus,
    nativeRootAuditExecutionSchedule, summarizeNativeRootAuditResults } from "./lib/native-root-audit-diagnostic.mjs";

const pluginDirectory = fileURLToPath(new URL("../", import.meta.url));
const args = process.argv.slice(2), options = { count: 25000, runs: 3, scenario: "all", check: false,
    binaryPath: join(pluginDirectory, "../target/release/sync-server"), output: null };
if (args.length === 1 && args[0] === "--help") {
    process.stdout.write(`Usage: node scripts/bench-native-root.mjs [--smoke] [--check]
  [--case quiet|edits|auto-edits|audit-edits|all] [--files 32..25000] [--runs 1..${NATIVE_ROOT_MAX_RUNS}]
  [--binary /absolute/path/to/sync-server]
  [--output /tmp/obsetync-native-root-results-NAME.json]

Default: 25,000 unique 4-KiB notes, three diagnostic runs per case, real
packaged SIMD WASM + native files + release Rust server + transport-v2 HTTP.
--smoke uses 32 files and one run; --check only bundles (no server started).
An all-case run alternates quiet-first and edits-first pairs. Five or more
runs per case are required before the report is marked local-sampling-eligible.
The edits case emits 70 new-note callbacks and three superseding generations
after the first actual root API admission, without artificial network delay.
It reports exact-generation acceptance latency plus aggregate, path-free root
admission ordinals/opportunity distance; no per-probe records are retained.
The separate auto-edits diagnostic enables production autoSync/coalescer
timers and requires save-triggered push plus a callback-during-run retrigger;
it is not included in the required quiet+edits qualification.
The separate audit-edits diagnostic emits the same callback wave immediately
after the first 256-path cooperative cut of the initial whole review, before
any root publication or API admission. Its smoke corpus is 512 files. It is
also excluded from the required quiet+edits qualification.

v1 server/base seeding, source v2 hashing/journaling and final verification
are OUTSIDE measured engine.pushPending(). All v2 content is initially absent
on the server. A SECOND fresh process verifies persisted base/journal/intent,
actual source hashes and rebuilt WASM root. No production vault/service is used.

This is a Node host-shell integration/diagnostic gate, not an Obsidian device,
editor input-to-paint, mobile RSS, WS routing, power-loss or release qualification.
Client native completion is not fsync. Server uses its real durability policy.
Counters include fixture path-safety syscalls; worker/direct streams after
getFullPath are outside adapter byte counters. No workers are started in this
known-hash small-note corpus. Sampled WASM capacity is not allocator usage.
Fixed desktop tuning, normal per-operation perfTrace lag sampler, no governor.

The runner owns private /tmp/obsetync-native-root-* fixtures, stops/joins its
children and removes only those fixtures. --output is an exclusive, retained
path-free JSON report; an existing output is never overwritten.
`);
    process.exit(0);
}
const seen = new Set();
for (let i = 0; i < args.length; i++) {
    const arg = args[i]; assert(!seen.has(arg), "duplicate benchmark option"); seen.add(arg);
    if (arg === "--smoke") { options.count = 32; options.runs = 1; }
    else if (arg === "--check") options.check = true;
    else if (arg === "--case") { options.scenario = args[++i];
        assert(["quiet", "edits", NATIVE_ROOT_AUTO_SCENARIO, NATIVE_ROOT_AUDIT_SCENARIO, "all"].includes(options.scenario)); }
    else if (arg === "--files" || arg === "--runs") {
        const value = args[++i]; assert(/^\d+$/.test(value ?? ""));
        const n = Number(value); assert(Number.isSafeInteger(n));
        if (arg === "--files") { assert(n >= 32 && n <= 25000); options.count = n; }
        else { assert(n >= 1 && n <= NATIVE_ROOT_MAX_RUNS); options.runs = n; }
    } else if (arg === "--binary") {
        options.binaryPath = args[++i]; assert(typeof options.binaryPath === "string" && isAbsolute(options.binaryPath));
    } else if (arg === "--output") {
        options.output = args[++i]; assert(typeof options.output === "string" && dirname(options.output) === "/tmp" &&
            /^obsetync-native-root-results-[A-Za-z0-9_-]+\.json$/.test(basename(options.output)));
    } else throw new Error("unsupported benchmark option; use --help");
}
assert(!options.check || !options.output, "bundle-only check cannot produce a benchmark report");
assert(!seen.has("--smoke") || (!seen.has("--files") && !seen.has("--runs")), "--smoke cannot be combined with --files/--runs");
if (seen.has("--smoke") && options.scenario === NATIVE_ROOT_AUDIT_SCENARIO) options.count = 512;
assert(options.scenario !== NATIVE_ROOT_AUDIT_SCENARIO || options.count > 256,
    "audit-edits requires more than 256 files");
const owned = [];
let safeToCleanup = true;
async function directory(suffix) {
    const path = await mkdtemp(`/tmp/obsetync-native-root-${suffix}-`), info = await lstat(path);
    assert.equal(await realpath(path), path); assert.equal(info.uid, process.getuid()); assert.equal(info.mode & 0o077, 0);
    owned.push({ path, ino: info.ino, dev: info.dev }); return path;
}
async function cleanup(owner) {
    const info = await lstat(owner.path), resolved = await realpath(owner.path);
    assert.equal(resolved, owner.path); assert.equal(dirname(resolved), "/tmp");
    assert(/^obsetync-native-root-(run|vault)-[A-Za-z0-9]+$/.test(basename(resolved)));
    assert(info.isDirectory() && !info.isSymbolicLink()); assert.equal(info.uid, process.getuid());
    assert.equal(info.mode & 0o077, 0); assert.equal(info.ino, owner.ino); assert.equal(info.dev, owner.dev);
    await rm(resolved, { recursive: true, force: false });
}
async function runChild(bundle, params) {
    try {
        return await runNativeRootChild(bundle, params, { cwd: pluginDirectory,
            onStderr: chunk => { process.stderr.write(chunk); } });
    } catch (error) {
        if (error.outcome?.safeToCleanup === false) safeToCleanup = false;
        throw error;
    }
}

const results = [];
try {
    const runDirectory = await directory("run"), bundle = join(runDirectory, "fixture.cjs");
    await build({ absWorkingDir: pluginDirectory, bundle: true, platform: "node", format: "cjs", target: "node20", outfile: bundle,
        stdin: { resolveDir: pluginDirectory, loader: "js", contents: `
            import { measureNativeRoot, coldVerifyNativeRoot } from "./scripts/lib/native-root-fixture.mjs";
            const options = JSON.parse(process.argv[2]);
            let complete = false, lastPhase = "startup";
            const interrupt = () => {
                globalThis.__nativeRootStopRequested = true; process.exitCode = 1;
                try { globalThis.__nativeRootStopNow?.(); } catch { /* actual cleanup is still joined below */ }
            };
            process.on("SIGTERM", interrupt); process.on("SIGINT", interrupt);
            process.once("beforeExit", () => {
                if (!complete && !process.exitCode) { process.stderr.write("native benchmark unfinished\\n"); process.exitCode = 1; }
            });
            const phase = value => {
                if (globalThis.__nativeRootStopRequested) throw new Error("native benchmark interrupted");
                lastPhase = value; process.stderr.write(JSON.stringify({ phase: value, mode: options.mode,
                scenario: options.scenario, count: options.count, run: options.run }) + "\\n"); };
            // Plugin logs are not metrics and may include owned fixture paths.
            // Assertions still fail the strict child and identify the phase.
            console.log = console.warn = console.error = () => {};
            (async () => {
                const result = await (options.mode === "measure" ? measureNativeRoot : coldVerifyNativeRoot)(options, phase);
                if (globalThis.__nativeRootStopRequested) throw new Error("native benchmark interrupted");
                process.stdout.write(JSON.stringify(result) + "\\n"); complete = true;
            })().catch(error => {
                process.stderr.write(JSON.stringify({ error: error.name, message: String(error.message).slice(0, 1500),
                    phase: lastPhase, stack: String(error.stack).split("\\n").slice(1, 5) }) + "\\n"); process.exitCode = 1;
            });
        ` },
        plugins: [nativeRootInstrumentationPlugin(), { name: "native-root-obsidian-host-shell", setup(builder) {
            builder.onResolve({ filter: /^obsidian$/ }, () => ({ path: "obsidian", namespace: "native-root" }));
            builder.onLoad({ filter: /.*/, namespace: "native-root" }, () => ({ loader: "js", contents: `
                export class TAbstractFile {} export class TFile extends TAbstractFile {}
                export class Notice { setMessage() {} hide() {} }
                // Production callbacks/coalescer timers remain real. The
                // auto-edits case enables autoSync; other cases keep it off.
                // This legacy debounce shim adds no separate host timer.
                export function debounce(callback) { return callback; }
                export function requestUrl(...args) {
                    if (!globalThis.__nativeRootRequestUrl) throw new Error("native HTTP bridge absent");
                    return globalThis.__nativeRootRequestUrl(...args);
                }
                export const Platform = { isDesktop: true, isDesktopApp: true, isMobile: false, isMobileApp: false };
            ` }));
        } }],
    });
    if (options.check) {
        console.log("native-root benchmark bundle check passed; no server or corpus started");
    } else {
        const autoDiagnostic = options.scenario === NATIVE_ROOT_AUTO_SCENARIO;
        const auditDiagnostic = options.scenario === NATIVE_ROOT_AUDIT_SCENARIO;
        const schedule = autoDiagnostic
            ? nativeRootAutoExecutionSchedule(options.runs, NATIVE_ROOT_MAX_RUNS)
            : auditDiagnostic
                ? nativeRootAuditExecutionSchedule(options.runs, NATIVE_ROOT_MAX_RUNS)
                : nativeRootExecutionSchedule(options.scenario, options.runs);
        for (const execution of schedule) {
                const { scenario, run } = execution;
                const vaultDirectory = await directory("vault");
                const params = { directory: vaultDirectory, pluginDirectory, count: options.count, scenario, run,
                    binaryPath: options.binaryPath };
                const measured = await runChild(bundle, { ...params, mode: "measure" });
                const cold = await runChild(bundle, { ...params, mode: "verify" });
                results.push({ ...execution, ...measured, cold });
                const owner = owned.pop(); assert.equal(owner.path, vaultDirectory); await cleanup(owner);
        }
        const summary = autoDiagnostic
            ? summarizeNativeRootAutoResults(results)
            : auditDiagnostic
                ? summarizeNativeRootAuditResults(results)
                : summarizeNativeRootResults(results);
        const qualification = autoDiagnostic
            ? nativeRootAutoDiagnosticStatus()
            : auditDiagnostic
                ? nativeRootAuditDiagnosticStatus()
                : nativeRootQualificationStatus(options.scenario, options.runs, options.count, summary);
        const report = { benchmark: "native-node-root-drain-v1", capturedAt: new Date().toISOString(),
            host: { node: process.version, platform: process.platform, architecture: process.arch, osRelease: release(),
                cpuModel: cpus()[0]?.model ?? "unknown", logicalCpus: cpus().length },
            instrumentationBundleSha256: createHash("sha256").update(await readFile(bundle)).digest("hex"),
            execution: { requestedCase: options.scenario, runsPerCase: options.runs,
                schedule: schedule.map(({ scenario, run, pair, orderInPair, executionOrdinal }) =>
                    ({ scenario, run, pair, orderInPair, executionOrdinal })) },
            qualification,
            statistics: {
                p50: "interpolated median across isolated runs",
                p95: "nearest-rank across isolated runs; with at most 9 runs this is the maximum",
                perRunCallbackToAcceptedP95Ms:
                    "distribution across each run's internal callback-to-accepted-cut p95; not pooled callbacks",
                perRunCallbackToExactAcceptedP95Ms:
                    "distribution across each run's internal exact-generation callback p95; not pooled callbacks",
                rootAdmissionOpportunities:
                    "exact accepted root API admission ordinal minus the latest admitted ordinal at callback; next admitted root is 1; aggregate only",
            },
            corpus: { bulkFiles: options.count, bytesEach: 4096, distinctIndexAndGenerationInEachPayload: true,
                baselineGeneration: 1, measuredGeneration: 2, serverObjectsInitiallyMissing: true,
                seedAndPreparedJournalOutsideMeasurement: true, treeVersion: 1 },
            limitations: ["Node host-shell, not Obsidian/Electron bridge, editor input-to-paint or real mobile device",
                "HTTP-only baseline; no WebSocket switch, CRDT or worker qualification",
                "known-hash small-note drain, not full scan/startup/native large-file throughput",
                autoDiagnostic
                    ? "autoSync=true; one explicit initial backlog drain, then real coalescer timers own trigger/retrigger and joined completion"
                    : auditDiagnostic
                        ? "autoSync=false; callbacks enter after the first 256-path full-review cut and before any root admission; one joined drain"
                        : "autoSync=false; at most two explicit joined drains, including retry cost; not automatic recovery latency",
                "client native completion, no fsync/power-loss/server-restart proof; actual server durability unchanged",
                "fixture symlink/path-validation syscalls included and partitioned; native storage is owned /tmp",
                "sampled process memory and one WASM memory capacity, not allocator utilization or bounded RSS",
                "cooperative planner/yield and call wrappers add instrumentation overhead; no governor",
                "root-admission opportunities show scheduler placement, not network latency or device responsiveness",
                "fresh client process readback after orderly completion, not crash recovery",
                "one machine/corpus; no same-durability reference pipeline or 70-percent release gate comparison"],
            summary, results };
        const json = JSON.stringify(report, null, 2) + "\n";
        if (options.output) {
            await writeFile(options.output, json, { flag: "wx", mode: 0o600 });
            console.log(JSON.stringify({ output: options.output, summary }, null, 2));
        } else process.stdout.write(json);
    }
} finally {
    if (safeToCleanup) for (const owner of owned.reverse()) await cleanup(owner);
    else process.stderr.write(JSON.stringify({ error: "native benchmark owner not drained; fixtures retained",
        directories: owned.map(owner => owner.path) }) + "\n");
}
