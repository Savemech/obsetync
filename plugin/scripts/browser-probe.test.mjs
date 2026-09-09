import { strict as assert } from "node:assert";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";
import test from "node:test";
import { build } from "esbuild";

const pluginDirectory = fileURLToPath(new URL("../", import.meta.url));
const protocolBuild = await build({
    absWorkingDir: pluginDirectory,
    entryPoints: ["src/browser-probe-protocol.ts"],
    bundle: true, platform: "node", format: "esm", target: "node20",
    write: false, logLevel: "warning",
});
const protocol = await import(`data:text/javascript;base64,${Buffer.from(protocolBuild.outputFiles[0].text).toString("base64")}`);
const [scalarModule, simdModule] = await Promise.all([
    readFile(new URL("../wasm/sync_core_bg.wasm", import.meta.url)),
    readFile(new URL("../wasm/sync_core_simd_bg.wasm", import.meta.url)),
]);

async function bundleProbe(minify) {
    // Match the actual production browser entry and bundler settings. No test
    // replacement for the worker, protocol or generated WASM glue is injected.
    const result = await build({
        absWorkingDir: pluginDirectory,
        entryPoints: ["src/browser-probe-entry.ts"],
        bundle: true, platform: "browser", format: "iife", target: "es2020",
        write: false, minify, keepNames: true, metafile: true, logLevel: "warning",
    });
    assert.equal(result.outputFiles.length, 1, "probe unexpectedly requires an external asset");
    for (const output of Object.values(result.metafile.outputs)) {
        assert.deepEqual(output.imports, [], "browser probe retained a runtime import");
    }
    assert.ok(Object.keys(result.metafile.inputs).some(path => path.endsWith("wasm/sync_core.js")));
    assert.ok(Object.keys(result.metafile.inputs).some(path => path.endsWith("wasm/sync_core_simd.js")));
    assert.ok(!Object.keys(result.metafile.inputs).some(path => path.endsWith(".wasm")),
        "probe bundle duplicated the embedded WASM payloads");
    return result.outputFiles[0].text;
}

async function runBundledProbe(source, { fixture = protocol.probeFixture(), scalar = scalarModule, simd = simdModule } = {}) {
    // Node only supplies the browser message boundary. This deliberately does
    // NOT test Blob URL startup, CSP, WebView cancellation or browser heap GC.
    // Fail closed if the real glue attempts to fetch an external WASM asset.
    const bridge = `
        const { parentPort } = require("node:worker_threads");
        globalThis.self = globalThis;
        globalThis.postMessage = (message, transfer) => parentPort.postMessage(message, transfer);
        globalThis.fetch = () => { throw new Error("unexpected external worker fetch"); };
        globalThis.importScripts = () => { throw new Error("unexpected external worker script"); };
        parentPort.on("message", data => globalThis.onmessage({ data }));
    `;
    const worker = new Worker(bridge + "\n" + source, { eval: true });
    // Exact, dedicated backings: never transfer a Buffer pool, a subarray's
    // larger backing, or the module bytes retained by the plugin/test process.
    const request = {
        type: "run",
        fixture: Uint8Array.from(fixture).buffer,
        scalar: Uint8Array.from(scalar).buffer,
        simd: Uint8Array.from(simd).buffer,
    };
    let deadline;
    try {
        return await new Promise((resolve, reject) => {
            let phase = "ready";
            let result;
            deadline = setTimeout(() => reject(new Error(`bundled worker stalled during ${phase}`)), 10_000);
            worker.on("error", reject);
            worker.on("messageerror", reject);
            worker.on("exit", code => {
                if (phase !== "done") reject(new Error(`bundled worker exited before completion: ${code}`));
            });
            worker.on("message", message => {
                try {
                    if (phase === "ready") {
                        assert.deepEqual(message, { type: "ready" });
                        phase = "result";
                        worker.postMessage(request, [request.fixture, request.scalar, request.simd]);
                        assert.equal(request.fixture.byteLength, 0, "fixture was cloned instead of transferred");
                        assert.equal(request.scalar.byteLength, 0, "scalar module copy was not transferred");
                        assert.equal(request.simd.byteLength, 0, "SIMD module copy was not transferred");
                        return;
                    }
                    if (phase === "result" && message?.type === "failed") {
                        assert.deepEqual(message, { type: "failed" }, "worker leaked raw failure details");
                        phase = "done";
                        resolve({ failed: true });
                        return;
                    }
                    if (phase === "result") {
                        assert.equal(message.type, "result");
                        assert.ok(message.fixture instanceof ArrayBuffer);
                        assert.ok(message.scalar instanceof ArrayBuffer);
                        assert.ok(message.simd instanceof ArrayBuffer);
                        assert.deepEqual(new Uint8Array(message.scalar), Uint8Array.from(scalar),
                            "worker did not return the exact scalar module copy");
                        assert.deepEqual(new Uint8Array(message.simd), Uint8Array.from(simd),
                            "worker did not return the exact SIMD module copy");
                        result = message;
                        phase = "released";
                        return;
                    }
                    assert.equal(phase, "released", "worker emitted an unexpected extra message");
                    assert.deepEqual(message, { type: "released", detached: true },
                        "worker retained transferred payload ownership after returning the result");
                    phase = "done";
                    resolve({ failed: false, result });
                } catch (error) { reject(error); }
            });
        });
    } finally {
        clearTimeout(deadline);
        // Node's acknowledged exit is a test cleanup mechanism; browser
        // Worker.terminate() is void and does not provide this acknowledgement.
        await worker.terminate();
    }
}

test("actual browser probe bundles hash scalar/SIMD and transfer both directions without external assets", async t => {
    const originalScalar = Uint8Array.from(scalarModule);
    const originalSimd = Uint8Array.from(simdModule);
    for (const minify of [false, true]) {
        await t.test(minify ? "production minified IIFE" : "development IIFE", async () => {
            const source = await bundleProbe(minify);
            const response = await runBundledProbe(source);
            assert.equal(response.failed, false);
            const result = response.result;
            assert.equal(result.scalarHash, protocol.PROBE_FIXTURE_HASH);
            assert.equal(result.simdValidated, true, "the packaging test requires a SIMD-capable Node runtime");
            assert.equal(result.simdHash, protocol.PROBE_FIXTURE_HASH);
            assert.equal(result.fixture.byteLength, protocol.PROBE_FIXTURE_BYTES);
            assert.deepEqual(new Uint8Array(result.fixture), protocol.probeFixture());
            for (const heap of [result.scalarHeapBytes, result.simdHeapBytes]) {
                assert.ok(Number.isSafeInteger(heap) && heap > 0 && heap <= protocol.PROBE_MAX_HEAP_BYTES);
            }
            assert.deepEqual(Uint8Array.from(scalarModule), originalScalar, "retained scalar bytes changed/detached");
            assert.deepEqual(Uint8Array.from(simdModule), originalSimd, "retained SIMD bytes changed/detached");
        });
    }
});

test("actual browser probe rejects malformed synthetic input without exception details", async () => {
    const response = await runBundledProbe(await bundleProbe(true), {
        fixture: new Uint8Array(protocol.PROBE_FIXTURE_BYTES - 1),
    });
    assert.deepEqual(response, { failed: true });
});

test("actual browser probe keeps scalar failure separate from working SIMD", async () => {
    const response = await runBundledProbe(await bundleProbe(true), { scalar: new Uint8Array([1, 2, 3]) });
    assert.equal(response.failed, false);
    assert.equal(response.result.scalarHash, null);
    assert.equal(response.result.scalarHeapBytes, null);
    assert.equal(response.result.simdValidated, true);
    assert.equal(response.result.simdHash, protocol.PROBE_FIXTURE_HASH);
    assert.deepEqual(new Uint8Array(response.result.fixture), protocol.probeFixture());
});
