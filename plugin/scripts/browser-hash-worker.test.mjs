import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { once } from "node:events";
import { Worker } from "node:worker_threads";
import test from "node:test";
import { build } from "esbuild";

const pluginDirectory = fileURLToPath(new URL("../", import.meta.url));
const protocolBuild = await build({
    absWorkingDir: pluginDirectory,
    entryPoints: ["src/browser-hash-worker-protocol.ts"],
    bundle: true, platform: "node", format: "esm", target: "node20",
    write: false, logLevel: "warning",
});
const protocol = await import(`data:text/javascript;base64,${Buffer.from(protocolBuild.outputFiles[0].text).toString("base64")}`);
const [scalarModule, simdModule] = await Promise.all([
    readFile(new URL("../wasm/sync_core_bg.wasm", import.meta.url)),
    readFile(new URL("../wasm/sync_core_simd_bg.wasm", import.meta.url)),
]);

async function bundleWorker() {
    const result = await build({
        absWorkingDir: pluginDirectory,
        entryPoints: ["src/browser-hash-worker-entry.ts"],
        bundle: true, platform: "browser", format: "iife", target: "es2020",
        write: false, minify: true, keepNames: true, metafile: true, logLevel: "warning",
    });
    assert.equal(result.outputFiles.length, 1);
    assert.ok(result.outputFiles[0].text.length <= protocol.BROWSER_HASH_WORKER_MAX_SOURCE_CHARS);
    for (const output of Object.values(result.metafile.outputs)) assert.deepEqual(output.imports, []);
    assert.ok(!Object.keys(result.metafile.inputs).some(path => path.endsWith(".wasm")),
        "browser worker duplicated embedded WASM binaries");
    return result.outputFiles[0].text;
}

function startWorker(source) {
    const bridge = `
        const { parentPort } = require("node:worker_threads");
        globalThis.self = globalThis;
        globalThis.postMessage = (message, transfer) => parentPort.postMessage(message, transfer);
        globalThis.fetch = () => { throw new Error("unexpected external worker fetch"); };
        globalThis.importScripts = () => { throw new Error("unexpected external worker script"); };
        parentPort.on("message", data => globalThis.onmessage({ data }));
    `;
    return new Worker(`${bridge}\n${source}`, { eval: true });
}

async function checkQualifiedWorker(simdBytes, expectedMode) {
    assert.ok(scalarModule.byteLength <= protocol.BROWSER_HASH_WORKER_MAX_MODULE_BYTES);
    assert.ok(simdModule.byteLength <= protocol.BROWSER_HASH_WORKER_MAX_MODULE_BYTES);
    const source = await bundleWorker();
    const worker = startWorker(source);
    const generation = 17;
    const fixture = protocol.browserHashQualificationFixture().buffer;
    const scalarWasm = Uint8Array.from(scalarModule).buffer;
    const simdWasm = Uint8Array.from(simdBytes).buffer;
    let deadline;
    try {
        const completed = new Promise((resolve, reject) => {
            let phase = "qualified";
            deadline = setTimeout(() => reject(new Error(`browser hash worker stalled during ${phase}`)), 10_000);
            worker.on("error", reject);
            worker.on("messageerror", reject);
            worker.on("message", message => {
                try {
                    assert.equal(message.schema, protocol.BROWSER_HASH_WORKER_SCHEMA);
                    assert.equal(message.generation, generation);
                    if (phase === "qualified") {
                        assert.equal(message.type, "qualified");
                        assert.equal(message.wasmMode, expectedMode);
                        assert.ok(message.wasmHeapBytes > 0 &&
                            message.wasmHeapBytes <= protocol.BROWSER_HASH_WORKER_MAX_HEAP_BYTES);
                        assert.deepEqual(new Uint8Array(message.qualification),
                            protocol.browserHashQualificationFixture());
                        assert.deepEqual(new Uint8Array(message.scalarWasm), Uint8Array.from(scalarModule));
                        assert.deepEqual(new Uint8Array(message.simdWasm), Uint8Array.from(simdBytes));
                        const bytes = protocol.browserHashQualificationFixture().buffer;
                        phase = "result";
                        worker.postMessage({ schema: protocol.BROWSER_HASH_WORKER_SCHEMA, type: "hash",
                            generation, jobId: 1, bytes, feedBytes: 64 * 1024, cpuSliceMs: 1 }, [bytes]);
                        assert.equal(bytes.byteLength, 0, "hash input was cloned instead of transferred");
                        return;
                    }
                    assert.equal(phase, "result");
                    assert.equal(message.type, "result");
                    assert.equal(message.jobId, 1);
                    assert.equal(message.hash, protocol.BROWSER_HASH_WORKER_QUALIFICATION_HASH);
                    assert.deepEqual(new Uint8Array(message.bytes), protocol.browserHashQualificationFixture());
                    assert.ok(message.wasmHeapBytes > 0 &&
                        message.wasmHeapBytes <= protocol.BROWSER_HASH_WORKER_MAX_HEAP_BYTES);
                    phase = "done";
                    resolve();
                } catch (error) { reject(error); }
            });
        });
        worker.postMessage({ schema: protocol.BROWSER_HASH_WORKER_SCHEMA, type: "init",
            generation, qualification: fixture, scalarWasm, simdWasm },
        [fixture, scalarWasm, simdWasm]);
        assert.equal(fixture.byteLength, 0);
        assert.equal(scalarWasm.byteLength, 0);
        assert.equal(simdWasm.byteLength, 0);
        await completed;
    } finally {
        clearTimeout(deadline);
        await worker.terminate();
    }
}

test("production browser worker qualifies and hashes with exact transferred ownership", async () => {
    await checkQualifiedWorker(simdModule, WebAssembly.validate(simdModule) ? "simd" : "scalar");
});

test("production browser worker qualifies using the scalar fallback", async () => {
    await checkQualifiedWorker(new Uint8Array(8), "scalar");
});

test("a failed self-test leaves the worker unavailable for file hashing", { timeout: 10000 }, async () => {
    const worker = startWorker(await bundleWorker());
    try {
        const qualification = protocol.browserHashQualificationFixture();
        qualification[0] ^= 1;
        const scalarWasm = Uint8Array.from(scalarModule).buffer;
        const simdWasm = Uint8Array.from(simdModule).buffer;
        const header = { schema: protocol.BROWSER_HASH_WORKER_SCHEMA, generation: 1 };
        const failed = once(worker, "message");
        worker.postMessage({ ...header, type: "init", qualification: qualification.buffer,
            scalarWasm, simdWasm }, [qualification.buffer, scalarWasm, simdWasm]);
        assert.equal((await failed)[0].reason, "INIT");
        const bytes = Uint8Array.of(42).buffer;
        const job = once(worker, "message");
        worker.postMessage({ ...header, type: "hash", jobId: 1, bytes,
            feedBytes: 65536, cpuSliceMs: 8 }, [bytes]);
        const [reply] = await job;
        assert.equal(reply.type, "failed");
        assert.equal(reply.reason, "PROTOCOL");
        assert.deepEqual(new Uint8Array(reply.bytes), Uint8Array.of(42));
    } finally {
        await worker.terminate();
    }
});
