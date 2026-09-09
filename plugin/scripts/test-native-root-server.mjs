import { strict as assert } from "node:assert";
import { build } from "esbuild";
import { lstat, mkdtemp, readFile, realpath, rename, rm, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { startNativeRootServer } from "./lib/native-root-server.mjs";

const pluginDirectory = fileURLToPath(new URL("../", import.meta.url));
const args = process.argv.slice(2);
assert(args.length === 0 || (args.length === 1 && args[0] === "--check"), "use --check for bundle-only verification");
const directory = await mkdtemp("/tmp/obsetync-native-api-smoke-");
let server, api, done = false, releaseHeldFetch = () => {};
const originalFetch = globalThis.fetch, originalSocket = globalThis.WebSocket;
const previousBridge = globalThis.__nativeRootRequestUrl;
const watchdog = setTimeout(() => { throw new Error("native API smoke exceeded60s"); }, 60000);
process.once("beforeExit", () => { if (!done && !process.exitCode) process.exitCode = 1; });
try {
    const output = join(directory, "api.cjs");
    const compiled = await build({ absWorkingDir: pluginDirectory, bundle: true, write: false,
        platform: "node", format: "cjs", target: "node20", outfile: output,
        stdin: { resolveDir: pluginDirectory, loader: "js", contents: `
            export { ObsetyncApi } from "./src/api";
            export { blake3 } from "@noble/hashes/blake3";
        ` },
        plugins: [{ name: "owned-loopback-obsidian-port", setup(builder) {
            builder.onResolve({ filter: /^obsidian$/ }, () => ({ path: "obsidian", namespace: "smoke" }));
            builder.onLoad({ filter: /.*/, namespace: "smoke" }, () => ({ loader: "js", contents: `
                export function requestUrl(...args) {
                    if (typeof globalThis.__nativeRootRequestUrl !== "function") throw new Error("native request bridge absent");
                    return globalThis.__nativeRootRequestUrl(...args);
                }
            ` }));
        } }],
    });
    assert.equal(compiled.outputFiles.length, 1); await writeFile(output, compiled.outputFiles[0].contents);
    if (args[0] === "--check") {
        console.log("native-root-server: actual API bundle check passed (no listener started)");
    } else {
        // This explicit mode tests real sealed HTTP, not WebSocket behavior.
        globalThis.WebSocket = undefined;
        server = await startNativeRootServer();
        globalThis.__nativeRootRequestUrl = server.requestUrl;
        const { ObsetyncApi, blake3 } = createRequire(import.meta.url)(output);
        const bundle = await server.enroll("native-smoke");
        let state = Object.freeze({ wireVersion: bundle.wire_version, esPub: bundle.Es_pub_initial,
            esPubValidUntil: bundle.Es_pub_valid_until, lastOutgoingSeq: 0 });
        const stateFile = join(directory, "transport.json");
        const persist = async next => {
            const raw = JSON.stringify(next); await writeFile(`${stateFile}.next`, raw);
            await rename(`${stateFile}.next`, stateFile);
            assert.equal(await readFile(stateFile, "utf8"), raw);
            state = Object.freeze(next);
        };
        await persist(state);
        const persistence = { get: () => ({ ...state }), update: patch => persist({ ...state, ...patch }) };
        api = new ObsetyncApi(server.syncUrl, bundle.server_box_pub, bundle.bearer_token, persistence, "desktop");
        assert.equal((await api.ping()).ok, true);
        const tree = await api.negotiateTreeVersion("native-smoke");
        assert.equal(tree.currentVersion, 1); assert.equal(tree.fleetReady, true);
        assert.equal(tree.enrolledDevices, 1);
        const caps = await api.negotiateRootOutcomes(true);
        assert(caps); assert.equal(caps.rootBytes, 512 * 1024);
        const stream = await api.queryRootOutcome("native-smoke");
        assert.equal(stream.status, "stream"); assert.equal(stream.last_sequence, 0);
        assert.equal(stream.current_root_hash, null); assert.equal(await api.getRoot("native-smoke"), null);
        const bytes = new TextEncoder().encode("native-loopback-content\n");
        const hash = Buffer.from(blake3(bytes)).toString("hex");
        assert.deepEqual(await api.checkContent([hash]), [hash]);
        await api.putContent(hash, bytes);
        assert.deepEqual(await api.checkContent([hash]), []);
        assert.deepEqual(await api.getContent(hash), bytes);
        assert(state.lastOutgoingSeq >= 4096);
        const previousReserved = state.lastOutgoingSeq;
        api.closeDataLane(); state = Object.freeze(JSON.parse(await readFile(stateFile, "utf8")));
        api = new ObsetyncApi(server.syncUrl, bundle.server_box_pub, bundle.bearer_token, persistence, "desktop");
        assert(await api.negotiateRootOutcomes(true));
        assert(state.lastOutgoingSeq > previousReserved, "new API reused old reserved transport sequence block");

        const beforeRefusal = server.snapshot().requests;
        for (const url of ["http://example.invalid/", "http://127.0.0.1:1/health",
            server.syncUrl.replace("http://", "http://owner:secret@"), `${server.syncUrl}/health#fragment`]) {
            await assert.rejects(server.requestUrl(url), /NON_OWNED_ENDPOINT/);
        }
        assert.equal(server.snapshot().requests, beforeRefusal);
        const redirected = await server.requestUrl(`${server.adminUrl}/`);
        assert.equal(redirected.status, 308, "bridge followed redirect");
        const text = await server.requestUrl(`${server.syncUrl}/health`).text;
        assert.deepEqual(JSON.parse(text), { ok: true });

        // Hold the actual fetch response before the adapter consumes its
        // native body. close() must join that owned request before teardown.
        let enter, release;
        const entered = new Promise(resolve => { enter = resolve; });
        const held = new Promise(resolve => { release = resolve; });
        releaseHeldFetch = release;
        globalThis.fetch = async (...parameters) => {
            const response = await originalFetch(...parameters); enter(); await held; return response;
        };
        const pending = server.requestUrl(`${server.syncUrl}/health`);
        await entered; let closed = false;
        const closing = server.close(); void closing.then(() => { closed = true; });
        assert.equal(server.close(), closing); await Promise.resolve();
        assert.equal(closed, false); assert.equal(server.snapshot().activeRequests, 1);
        await assert.rejects(server.requestUrl(`${server.syncUrl}/health`), /REQUEST_AFTER_CLOSE/);
        release(); await pending; await closing;
        assert.equal(closed, true); assert.equal(server.snapshot().activeRequests, 0);
        api.closeDataLane();
        console.log(JSON.stringify({ test: "actual-api-native-loopback-smoke", passed: true,
            coverage: ["owned loopback enrollment", "actual transport-v2 AEAD capabilities/root-stream/content",
                "persisted transport reservation reload", "foreign URL refusal", "no redirect following", "native request joined before close"],
            limitations: ["debug or explicitly supplied binary, no throughput claim", "HTTP-only, no root publication/WASM/device coverage",
                "Node filesystem state readback is not a process-crash durability test"],
            fixture: server.snapshot() }, null, 2));
    }
    done = true;
} finally {
    releaseHeldFetch();
    globalThis.fetch = originalFetch; globalThis.WebSocket = originalSocket;
    if (previousBridge === undefined) delete globalThis.__nativeRootRequestUrl;
    else globalThis.__nativeRootRequestUrl = previousBridge;
    api?.closeDataLane(); await server?.close();
    clearTimeout(watchdog);
    const resolved = await realpath(directory), info = await lstat(directory);
    assert.equal(resolved, directory); assert.equal(dirname(resolved), "/tmp");
    assert(basename(resolved).startsWith("obsetync-native-api-smoke-") && info.isDirectory() && !info.isSymbolicLink());
    await rm(resolved, { recursive: true, force: true });
}
