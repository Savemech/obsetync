import { strict as assert } from "node:assert";
import { createRequire } from "node:module";
import { lstat, mkdtemp, readFile, realpath, rename, rm, writeFile } from "node:fs/promises";
import { basename, dirname, isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { startNativeRootServer } from "./lib/native-root-server.mjs";

const pluginDirectory = fileURLToPath(new URL("../", import.meta.url));
const fixturePrefix = "obsetync-native-ws-data-";
const largeFrameBytes = 60 * 1024;
const args = process.argv.slice(2);
let checkOnly = false, binaryPath;
for (let index = 0; index < args.length; index++) {
    if (args[index] === "--check" && !checkOnly) checkOnly = true;
    else if (args[index] === "--binary" && binaryPath === undefined && index + 1 < args.length) {
        binaryPath = args[++index];
    } else {
        assert.fail("usage: node scripts/test-native-ws-data.mjs [--check] [--binary /absolute/path]");
    }
}
if (binaryPath !== undefined) assert(isAbsolute(binaryPath), "--binary must be an absolute path");

async function bundleClient() {
    const compiled = await build({
        absWorkingDir: pluginDirectory,
        bundle: true,
        write: false,
        platform: "node",
        format: "cjs",
        target: "node20",
        outfile: "native-ws-data-client.cjs",
        stdin: {
            resolveDir: pluginDirectory,
            loader: "js",
            contents: `
                export { ObsetyncApi } from "./src/api";
                export { BulkObjectKind } from "./src/bulk-codec";
                export { WsDataCancelledError } from "./src/ws-data";
                export { blake3 } from "@noble/hashes/blake3";
            `,
        },
        plugins: [{ name: "owned-loopback-obsidian-port", setup(builder) {
            builder.onResolve({ filter: /^obsidian$/ }, () => ({ path: "obsidian", namespace: "native-ws" }));
            builder.onLoad({ filter: /.*/, namespace: "native-ws" }, () => ({ loader: "js", contents: `
                export function requestUrl(...args) {
                    if (typeof globalThis.__nativeWsRequestUrl !== "function") {
                        throw new Error("native WS request bridge absent");
                    }
                    return globalThis.__nativeWsRequestUrl(...args);
                }
            ` }));
        } }],
    });
    assert.equal(compiled.outputFiles.length, 1);
    return compiled.outputFiles[0].contents;
}

function binaryBytes(value) {
    if (value instanceof ArrayBuffer) return value.byteLength;
    if (ArrayBuffer.isView(value)) return value.byteLength;
    return null;
}

function installOwnedWebSocket(syncUrl) {
    const NativeWebSocket = globalThis.WebSocket;
    assert.equal(typeof NativeWebSocket, "function", "Node WebSocket is unavailable");
    const expected = new URL(syncUrl.replace(/^http:/, "ws:"));
    const sent = [], received = [], sockets = [];
    let rejectAfterAuth = 0, rejectedAuthConnections = 0;
    let nextLargeSend = null;

    class OwnedWebSocket {
        constructor(input) {
            const url = new URL(String(input));
            assert.equal(url.protocol, "ws:", "WS transport escaped plaintext loopback");
            assert.equal(url.hostname, "127.0.0.1", "WS transport escaped IPv4 loopback");
            assert.equal(url.origin, expected.origin, "WS transport escaped the owned sync listener");
            assert.equal(url.pathname, "/api/v1/ws-data", "WS transport escaped the data endpoint");
            assert.equal(url.username, ""); assert.equal(url.password, "");
            assert.equal(url.search, ""); assert.equal(url.hash, "");
            this.id = sockets.length + 1;
            this.inner = new NativeWebSocket(url.href);
            this.onopen = null; this.onmessage = null; this.onclose = null; this.onerror = null;
            let resolveClosed;
            this.closed = new Promise(resolve => { resolveClosed = resolve; });
            this.inner.onopen = event => this.onopen?.(event);
            this.inner.onmessage = event => {
                const bytes = binaryBytes(event.data);
                if (bytes !== null) received.push({ socket: this.id, bytes });
                this.onmessage?.(event);
            };
            this.inner.onerror = event => this.onerror?.(event);
            this.inner.onclose = event => { resolveClosed(); this.onclose?.(event); };
            sockets.push(this);
        }
        get readyState() { return this.inner.readyState; }
        get bufferedAmount() { return this.inner.bufferedAmount; }
        get binaryType() { return this.inner.binaryType; }
        set binaryType(value) { this.inner.binaryType = value; }
        send(value) {
            if (typeof value === "string") {
                if (rejectAfterAuth > 0) {
                    rejectAfterAuth--;
                    rejectedAuthConnections++;
                    queueMicrotask(() => this.inner.close());
                }
            } else {
                const bytes = binaryBytes(value);
                assert.notEqual(bytes, null, "client sent an unsupported WS body");
                sent.push({ socket: this.id, bytes });
                if (bytes > largeFrameBytes && nextLargeSend) {
                    const callback = nextLargeSend;
                    nextLargeSend = null;
                    queueMicrotask(callback);
                }
            }
            this.inner.send(value);
        }
        close(...parameters) { this.inner.close(...parameters); }
    }

    globalThis.WebSocket = OwnedWebSocket;
    return Object.freeze({
        sent,
        received,
        sockets,
        rejectOneAfterAuth() { rejectAfterAuth++; },
        onNextLargeSend(callback) {
            assert.equal(nextLargeSend, null, "large-send observer already armed");
            nextLargeSend = callback;
        },
        get rejectedAuthConnections() { return rejectedAuthConnections; },
        restore() { globalThis.WebSocket = NativeWebSocket; },
    });
}

function deterministicBytes(seed, length) {
    let state = seed >>> 0;
    const output = new Uint8Array(length);
    for (let index = 0; index < output.length; index++) {
        state ^= state << 13; state ^= state >>> 17; state ^= state << 5;
        output[index] = state & 0xff;
    }
    return output;
}

function makeRecord(BulkObjectKind, blake3, seed, length) {
    const data = deterministicBytes(seed, length);
    return { kind: BulkObjectKind.Content, hash: Buffer.from(blake3(data)).toString("hex"), data };
}

function frameShape(frames) {
    return frames.map(frame => frame.bytes > largeFrameBytes ? "L" : "s").join("");
}

async function waitFor(predicate, message, timeoutMs = 5000) {
    const deadline = Date.now() + timeoutMs;
    while (!(await predicate())) {
        if (Date.now() >= deadline) assert.fail(message);
        await new Promise(resolve => setTimeout(resolve, 10));
    }
}

async function removeFixture(directory) {
    const resolved = await realpath(directory), info = await lstat(directory);
    assert.equal(resolved, directory);
    assert.equal(dirname(resolved), "/tmp");
    assert(basename(resolved).startsWith(fixturePrefix) && info.isDirectory() && !info.isSymbolicLink());
    assert.equal(info.mode & 0o777, 0o700, "native WS fixture stopped being private");
    if (typeof process.getuid === "function") assert.equal(info.uid, process.getuid());
    await rm(resolved, { recursive: true, force: true });
}

const compiled = await bundleClient();
if (checkOnly) {
    console.log("native-ws-data: actual TypeScript client bundle check passed (no listener or temporary fixture started)");
} else {
    let directory, server, api, fallbackApi, wsObserver;
    let done = false;
    const previousBridge = globalThis.__nativeWsRequestUrl;
    const watchdog = setTimeout(() => { throw new Error("native WS-data integration exceeded 60s"); }, 60000);
    process.once("beforeExit", () => { if (!done && !process.exitCode) process.exitCode = 1; });
    try {
        directory = await mkdtemp(`/tmp/${fixturePrefix}`);
        const directoryInfo = await lstat(directory);
        assert.equal(directoryInfo.mode & 0o777, 0o700, "native WS fixture is not private");
        if (typeof process.getuid === "function") assert.equal(directoryInfo.uid, process.getuid());
        const modulePath = join(directory, "client.cjs");
        await writeFile(modulePath, compiled, { mode: 0o600 });
        server = await startNativeRootServer(binaryPath ? { binaryPath } : undefined);
        globalThis.__nativeWsRequestUrl = server.requestUrl;
        wsObserver = installOwnedWebSocket(server.syncUrl);
        const { ObsetyncApi, BulkObjectKind, WsDataCancelledError, blake3 } = createRequire(import.meta.url)(modulePath);
        const enrollment = await server.enroll("native-ws-data");

        let state = Object.freeze({
            wireVersion: enrollment.wire_version,
            esPub: enrollment.Es_pub_initial,
            esPubValidUntil: enrollment.Es_pub_valid_until,
            lastOutgoingSeq: 0,
        });
        let persistenceTail = Promise.resolve();
        const stateFile = join(directory, "transport.json");
        const persistence = {
            get: () => ({ ...state }),
            update: patch => {
                const ownedPatch = { ...patch };
                const task = persistenceTail.then(async () => {
                    const next = Object.freeze({ ...state, ...ownedPatch });
                    const raw = JSON.stringify(next);
                    await writeFile(`${stateFile}.next`, raw, { mode: 0o600 });
                    await rename(`${stateFile}.next`, stateFile);
                    assert.equal(await readFile(stateFile, "utf8"), raw);
                    state = next;
                });
                persistenceTail = task;
                return task;
            },
        };
        await persistence.update({});
        const createApi = () => new ObsetyncApi(
            server.syncUrl,
            enrollment.server_box_pub,
            enrollment.bearer_token,
            persistence,
            "desktop",
        );
        api = createApi();

        assert.deepEqual(await api.checkContent(["00".repeat(32)]), ["00".repeat(32)]);
        let diagnostics = await api.getBulkDiagnostics();
        assert.equal(diagnostics.wsDataEnabled, true);
        assert.equal(diagnostics.wsDataState, "ready");
        assert.equal(diagnostics.wsDataWireVersion, 2, "actual client/server did not negotiate OBW2");
        assert.equal(diagnostics.lastCarrier, "ws");

        // Both request and response exceed the OBW2 64 KiB fragment payload.
        const fragmented = makeRecord(BulkObjectKind, blake3, 0x91e10da5, 220_000);
        const putSentStart = wsObserver.sent.length;
        await api.putObjects([fragmented]);
        const putFrames = wsObserver.sent.slice(putSentStart);
        assert(putFrames.filter(frame => frame.bytes > largeFrameBytes).length >= 3,
            `fragmented PUT did not cross OBW2 boundaries: ${frameShape(putFrames)}`);
        const getReceivedStart = wsObserver.received.length;
        const fetched = await api.getObjectsOwned(BulkObjectKind.Content, [fragmented.hash]);
        try {
            assert.deepEqual(fetched.objects.get(fragmented.hash), fragmented.data);
        } finally { fetched.release(); }
        const getFrames = wsObserver.received.slice(getReceivedStart);
        assert(getFrames.filter(frame => frame.bytes > largeFrameBytes).length >= 3,
            `fragmented GET response did not cross OBW2 boundaries: ${frameShape(getFrames)}`);

        // These sizes have fragment shapes LLS and Ls. The scheduler must
        // interleave them as LLLss instead of draining either request first.
        const first = makeRecord(BulkObjectKind, blake3, 0x10203040, 150_000);
        const second = makeRecord(BulkObjectKind, blake3, 0x50607080, 70_000);
        const rejectedSecond = { ...second, hash: "22".repeat(32) };
        const concurrentStart = wsObserver.sent.length;
        const firstPut = api.putObjects([first]).then(() => null, error => error);
        const secondPut = api.putObjects([rejectedSecond]).then(() => null, error => error);
        const [firstOutcome, secondOutcome] = await Promise.all([firstPut, secondPut]);
        assert.equal(firstOutcome, null, "valid interleaved request received its peer's failure");
        assert(secondOutcome instanceof Error && /bad hash/.test(secondOutcome.message),
            "invalid interleaved request received its peer's success");
        const concurrentFrames = wsObserver.sent.slice(concurrentStart);
        assert.equal(frameShape(concurrentFrames), "LLLss",
            "concurrent OBW2 request IDs were not interleaved at fragment boundaries");
        assert.deepEqual(await api.checkContent([first.hash, second.hash]), [second.hash]);

        // A control request admitted after the first bulk fragment must pass
        // the remaining bulk tail at its next safe fragment boundary.
        const priority = makeRecord(BulkObjectKind, blake3, 0x0badcafe, 220_000);
        const missing = "11".repeat(32);
        let controlRequest;
        wsObserver.onNextLargeSend(() => { controlRequest = api.checkContent([missing]); });
        const priorityStart = wsObserver.sent.length;
        await api.putObjects([priority]);
        await waitFor(() => controlRequest !== undefined, "control priority request was not admitted");
        assert.deepEqual(await controlRequest, [missing]);
        const priorityFrames = wsObserver.sent.slice(priorityStart);
        const priorityShape = frameShape(priorityFrames);
        assert(priorityShape.indexOf("s") > priorityShape.indexOf("L") &&
            priorityShape.indexOf("s") < priorityShape.lastIndexOf("L"),
        `control request did not preempt the bulk tail: ${priorityShape}`);

        // AbortSignal is the public cancellation surface. Once a request ID
        // is on-wire, cancellation must be emitted as a control boundary and
        // must not poison the shared socket or publish an incomplete object.
        const cancelled = makeRecord(BulkObjectKind, blake3, 0xc001d00d, 180_000);
        const abort = new AbortController();
        wsObserver.onNextLargeSend(() => abort.abort());
        const cancelStart = wsObserver.sent.length;
        const cancelledOutcome = await api.putObjects(
            [cancelled], undefined, undefined, abort.signal,
        ).then(() => null, error => error);
        assert(cancelledOutcome instanceof WsDataCancelledError,
            `AbortSignal lost WS cancellation identity: ${cancelledOutcome?.constructor?.name}`);
        const cancelFrames = wsObserver.sent.slice(cancelStart);
        const cancelShape = frameShape(cancelFrames);
        assert(cancelShape.startsWith("Ls"), `CANCEL did not pass the fragmented tail: ${cancelShape}`);
        assert.deepEqual(await api.checkContent([cancelled.hash]), [cancelled.hash]);
        assert.equal((await api.getBulkDiagnostics()).wsDataState, "ready");

        // Tear down the native socket without retiring the API. The next
        // request after bounded reconnect backoff must mint a fresh session.
        const oldSocket = wsObserver.sockets.at(-1);
        const socketsBeforeLoss = wsObserver.sockets.length;
        oldSocket.close();
        await oldSocket.closed;
        await waitFor(() => api.getBulkDiagnostics().then(value => value.wsDataState === "backoff"),
            "socket loss did not enter reconnect backoff");
        await new Promise(resolve => setTimeout(resolve, 1100));
        assert.deepEqual(await api.checkContent([missing]), [missing]);
        assert.equal(wsObserver.sockets.length, socketsBeforeLoss + 1,
            "socket loss did not establish exactly one fresh WS session");
        diagnostics = await api.getBulkDiagnostics();
        assert.equal(diagnostics.wsDataWireVersion, 2);
        assert.equal(diagnostics.wsDataState, "ready");

        // Force the first auth socket to disappear before its OBW2 HELLO.
        // Production connect() must consume a new ticket/session for OBW1.
        api.closeDataLane(); api = null;
        wsObserver.rejectOneAfterAuth();
        const socketsBeforeDowngrade = wsObserver.sockets.length;
        fallbackApi = createApi();
        assert.deepEqual(await fallbackApi.checkContent([missing]), [missing]);
        diagnostics = await fallbackApi.getBulkDiagnostics();
        assert.equal(wsObserver.rejectedAuthConnections, 1);
        assert.equal(wsObserver.sockets.length, socketsBeforeDowngrade + 2,
            "OBW2 rejection did not create a fresh OBW1 socket");
        assert.equal(diagnostics.wsDataState, "ready");
        assert.equal(diagnostics.wsDataWireVersion, 1, "explicit downgrade did not settle on OBW1");
        assert.equal(diagnostics.lastCarrier, "ws");

        fallbackApi.closeDataLane(); fallbackApi = null;
        await Promise.all(wsObserver.sockets.map(socket => socket.closed));
        done = true;
        console.log(JSON.stringify({
            test: "actual-typescript-client-rust-server-obw2",
            passed: true,
            coverage: [
                "actual API/router OBW2 fragmented PUT and GET",
                "concurrent request-ID interleaving",
                "control and CANCEL fragment-boundary priority",
                "socket-loss fresh-session recovery",
                "fresh-session OBW1 fallback after rejected OBW2",
            ],
            websocket: {
                connections: wsObserver.sockets.length,
                clientBinaryFrames: wsObserver.sent.length,
                serverBinaryFrames: wsObserver.received.length,
                rejectedObw2AuthConnections: wsObserver.rejectedAuthConnections,
            },
            limitations: [
                "opt-in Node loopback integration gate; not an Obsidian WebView/device test",
                "frame sizes expose scheduling boundaries without decrypting or logging payloads, paths, tokens, or hashes",
                "functional gate only; no throughput claim",
            ],
            fixture: server.snapshot(),
        }, null, 2));
    } finally {
        api?.closeDataLane(); fallbackApi?.closeDataLane();
        if (wsObserver) {
            await Promise.allSettled(wsObserver.sockets.map(socket => socket.closed));
            wsObserver.restore();
        }
        if (previousBridge === undefined) delete globalThis.__nativeWsRequestUrl;
        else globalThis.__nativeWsRequestUrl = previousBridge;
        await server?.close();
        clearTimeout(watchdog);
        if (directory) await removeFixture(directory);
    }
}
