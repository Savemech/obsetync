import { strict as assert } from "node:assert";
import { build } from "esbuild";
import { lstat, mkdtemp, realpath, rm } from "node:fs/promises";
import { basename, dirname, isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";
import { runNativeRootChild, NativeRootChildError } from "./lib/native-root-child.mjs";
import { startNativeRootServer } from "./lib/native-root-server.mjs";

const pluginDirectory = fileURLToPath(new URL("../", import.meta.url));
const args = process.argv.slice(2);
const options = { check: false, scenario: "objects-before-root", binaryPath: null };
const scenarios = new Set(["objects-before-root", "root-accepted-before-response"]);
if (args.length === 1 && args[0] === "--help") {
    process.stdout.write(`Usage: node scripts/test-native-root-crash-recovery.mjs [--check]
  [--case objects-before-root|root-accepted-before-response]
  [--binary /absolute/path/to/sync-server]

Opt-in, private integration test. It starts one real Rust server on random
127.0.0.1 ports, uses a private /tmp client vault, kills the first detached
client at the selected root-publication boundary, then verifies cold recovery
in a fresh client process. It never reads a real vault or production endpoint.
--check only bundles the child and starts no listener or server.
`);
    process.exit(0);
}
const seen = new Set();
for (let index = 0; index < args.length; index++) {
    const arg = args[index]; assert(!seen.has(arg), "duplicate crash-test option"); seen.add(arg);
    if (arg === "--check") options.check = true;
    else if (arg === "--case") {
        options.scenario = args[++index]; assert(scenarios.has(options.scenario), "unsupported crash-test case");
    } else if (arg === "--binary") {
        options.binaryPath = args[++index]; assert(typeof options.binaryPath === "string" && isAbsolute(options.binaryPath));
    } else throw new Error("unsupported crash-test option; use --help");
}

const owned = [];
let childSafeToCleanup = true;
async function ownedDirectory(suffix) {
    const path = await mkdtemp(`/tmp/obsetync-native-root-${suffix}-`), info = await lstat(path);
    assert.equal(await realpath(path), path); assert.equal(info.uid, process.getuid()); assert.equal(info.mode & 0o077, 0);
    owned.push({ path, ino: info.ino, dev: info.dev }); return path;
}
async function cleanup(owner) {
    const path = await realpath(owner.path), info = await lstat(owner.path);
    assert.equal(path, owner.path); assert.equal(dirname(path), "/tmp");
    assert(/^obsetync-native-root-(run|vault)-[A-Za-z0-9]+$/.test(basename(path)));
    assert(info.isDirectory() && !info.isSymbolicLink()); assert.equal(info.uid, process.getuid());
    assert.equal(info.mode & 0o077, 0); assert.equal(info.ino, owner.ino); assert.equal(info.dev, owner.dev);
    await rm(path, { recursive: true, force: false });
}
async function selectBinary() {
    if (options.binaryPath) return options.binaryPath;
    for (const candidate of [join(pluginDirectory, "../target/release/sync-server"),
        join(pluginDirectory, "../target/debug/sync-server")]) {
        try { if ((await lstat(candidate)).isFile()) return candidate; }
        catch (error) { if (error?.code !== "ENOENT") throw error; }
    }
    throw new Error("native crash test requires target/release or target/debug sync-server");
}
async function runChild(bundle, params, extra = {}) {
    try {
        return await runNativeRootChild(bundle, params, { cwd: pluginDirectory,
            timeoutMs: 180_000, graceMs: 5_000, ...extra });
    } catch (error) {
        if (error instanceof NativeRootChildError && error.outcome.safeToCleanup === false) childSafeToCleanup = false;
        throw error;
    }
}

let server;
try {
    const runDirectory = await ownedDirectory("run"), bundle = join(runDirectory, "crash-fixture.cjs");
    await build({ absWorkingDir: pluginDirectory, bundle: true, platform: "node", format: "cjs",
        target: "node20", outfile: bundle,
        stdin: { resolveDir: pluginDirectory, loader: "js", contents: `
            import { writeSync } from "node:fs";
            import { crashAfterObjectsBeforeRoot, crashAfterRootAcceptedBeforeLocalReceipt,
                recoverObjectsBeforeRoot, recoverRootAcceptedBeforeLocalReceipt } from "./scripts/lib/native-root-crash-fixture.mjs";
            const options = JSON.parse(process.argv[2]);
            const limit = 16 * 1024 * 1024;
            const allowed = new Set([options.serverUrl, options.adminUrl]);
            const requestPromise = promise => {
                for (const field of ["arrayBuffer", "text", "json"]) Object.defineProperty(promise, field,
                    { get: () => promise.then(response => response[field]) });
                return promise;
            };
            globalThis.__nativeRootRequestUrl = input => requestPromise((async () => {
                const params = typeof input === "string" ? { url: input } : input;
                let url; try { url = new URL(params?.url); } catch { throw new Error("invalid private fixture URL"); }
                if (!allowed.has(url.origin) || url.protocol !== "http:" || url.hostname !== "127.0.0.1" ||
                    url.username || url.password || url.hash) throw new Error("non-owned private fixture URL");
                if (params.body !== undefined && typeof params.body !== "string" && !(params.body instanceof ArrayBuffer)) {
                    throw new Error("invalid private fixture request body");
                }
                const bodyBytes = typeof params.body === "string" ? Buffer.byteLength(params.body) : params.body?.byteLength ?? 0;
                if (bodyBytes > limit) throw new Error("private fixture request exceeds limit");
                const headers = new Headers(params.headers);
                if (params.contentType && !headers.has("content-type")) headers.set("content-type", params.contentType);
                const abort = new AbortController(), timer = setTimeout(() => abort.abort(), 30_000);
                try {
                    const response = await fetch(url, { method: params.method ?? "GET", headers, body: params.body,
                        redirect: "manual", credentials: "omit", signal: abort.signal });
                    const declared = Number(response.headers.get("content-length"));
                    if (Number.isFinite(declared) && declared > limit) { await response.body?.cancel(); throw new Error("response exceeds limit"); }
                    const arrayBuffer = await response.arrayBuffer();
                    if (arrayBuffer.byteLength > limit) throw new Error("response exceeds limit");
                    if (params.throw !== false && response.status >= 400) throw new Error("private fixture HTTP failure");
                    let text, json, parsed = false;
                    return { status: response.status, headers: Object.fromEntries(response.headers), arrayBuffer,
                        get text() { return text ??= new TextDecoder().decode(arrayBuffer); },
                        get json() { if (!parsed) { json = JSON.parse(this.text); parsed = true; } return json; } };
                } finally { clearTimeout(timer); }
            })());
            let complete = false, lastPhase = "startup";
            const phase = value => { lastPhase = value; writeSync(2, JSON.stringify({ phase: value, mode: options.mode }) + "\\n"); };
            console.log = console.warn = console.error = () => {};
            process.once("beforeExit", () => {
                if (!complete && !process.exitCode) { process.stderr.write("native crash fixture unfinished\\n"); process.exitCode = 1; }
            });
            (async () => {
                const accepted = options.scenario === "root-accepted-before-response";
                const operation = options.mode === "crash"
                    ? (accepted ? crashAfterRootAcceptedBeforeLocalReceipt : crashAfterObjectsBeforeRoot)
                    : (accepted ? recoverRootAcceptedBeforeLocalReceipt : recoverObjectsBeforeRoot);
                const result = await operation(options, phase);
                process.stdout.write(JSON.stringify(result) + "\\n"); complete = true;
            })().catch(error => {
                process.stderr.write(JSON.stringify({ error: error.name, message: String(error.message).slice(0, 1000),
                    phase: lastPhase, stack: String(error.stack).split("\\n").slice(1, 4) }) + "\\n");
                process.exitCode = 1;
            });
        ` },
        plugins: [{ name: "native-root-crash-obsidian-shell", setup(builder) {
            builder.onResolve({ filter: /^obsidian$/ }, () => ({ path: "obsidian", namespace: "native-root-crash" }));
            builder.onLoad({ filter: /.*/, namespace: "native-root-crash" }, () => ({ loader: "js", contents: `
                export class TAbstractFile {} export class TFile extends TAbstractFile {}
                export class Notice { setMessage() {} hide() {} }
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
        console.log("native-root crash/recovery bundle check passed; no listener or server started");
    } else {
        const vaultDirectory = await ownedDirectory("vault");
        server = await startNativeRootServer({ binaryPath: await selectBinary() });
        const enrollment = await server.enroll("native-crash-recovery");
        const params = { directory: vaultDirectory, pluginDirectory, serverUrl: server.syncUrl,
            adminUrl: server.adminUrl, enrollment, scenario: options.scenario };
        const expectedBoundary = options.scenario === "root-accepted-before-response"
            ? "root-accepted-before-local-receipt" : "objects-acked-before-root";
        let boundarySeen = false, crashError;
        try {
            await runChild(bundle, { ...params, mode: "crash" }, {
                onStderr: chunk => { if (chunk.includes(`"phase":"${expectedBoundary}"`)) boundarySeen = true; },
            });
        } catch (error) { crashError = error; }
        assert(crashError instanceof NativeRootChildError); assert.equal(crashError.code, "CHILD_FAILED");
        assert.equal(crashError.outcome.signal, "SIGKILL"); assert.equal(crashError.outcome.exitCode, null);
        assert.equal(crashError.outcome.forcedGroupKill, false); assert.equal(crashError.outcome.safeToCleanup, true);
        assert.equal(crashError.outcome.groupExecutableMembers, 0); assert.equal(boundarySeen, true);
        const recovered = await runChild(bundle, { ...params, mode: "recover" }, {
            onStderr: chunk => { process.stderr.write(chunk); },
        });
        const acceptedBeforeCrash = options.scenario === "root-accepted-before-response";
        assert.deepEqual(recovered, { scenario: expectedBoundary, freshProcess: true, files: 32,
            recoveredSequence: acceptedBeforeCrash ? 1 : 2,
            cancellationSequence: acceptedBeforeCrash ? null : 1,
            commitSequence: acceptedBeforeCrash ? null : 2,
            sourceReads: acceptedBeforeCrash ? 0 : 1, uploadedRecords: 0, exactRoot: true, pendingJournal: 0,
            pendingRootIntent: false, ioInFlight: 0 });
        process.stdout.write(JSON.stringify({ test: "native-root-crash-recovery", passed: true,
            boundary: recovered.scenario, freshChild: recovered.freshProcess,
            server: server.snapshot().binary.profile, recoveredSequence: recovered.recoveredSequence }) + "\n");
    }
} finally {
    try { await server?.close(); }
    finally {
        if (childSafeToCleanup) for (const owner of owned.reverse()) await cleanup(owner);
        else process.stderr.write("native crash child group not drained; private fixtures retained\n");
    }
}
