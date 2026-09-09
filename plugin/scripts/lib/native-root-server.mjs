import { strict as assert } from "node:assert";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { lstat, mkdtemp, readFile, realpath, rm } from "node:fs/promises";
import { basename, dirname, isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";

const DEFAULT_BINARY = fileURLToPath(new URL("../../../target/debug/sync-server", import.meta.url));
const FIXTURE_PREFIX = "obsetync-native-root-server-";
const HTTP_LIMIT = 16 * 1024 * 1024;
const SERVER_ENV = Object.freeze({ RUST_LOG: "off", RUST_BACKTRACE: "0" });

function failure(code) { return new Error(`native root server fixture: ${code}`); }
function requestPromise(promise) {
    // Obsidian exposes both await requestUrl(...) and await requestUrl(...).text.
    // Lazy access avoids parsing an unused encrypted response as JSON.
    for (const field of ["arrayBuffer", "text", "json"]) {
        Object.defineProperty(promise, field, { get: () => promise.then(response => response[field]) });
    }
    return promise;
}

function launch(binary, args, onLine = () => {}) {
    const child = spawn(binary, args, { env: SERVER_ENV, stdio: ["ignore", "pipe", "pipe"] });
    let outputBytes = 0, partial = "", launchError = false;
    const closed = new Promise(resolve => {
        child.once("error", () => { launchError = true; });
        child.once("close", (code, signal) => resolve({ code, signal, launchError }));
    });
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", data => {
        outputBytes += Buffer.byteLength(data);
        if (outputBytes > 64 * 1024) { child.kill("SIGTERM"); return; }
        partial += data;
        let end;
        while ((end = partial.indexOf("\n")) !== -1) {
            const line = partial.slice(0, end); partial = partial.slice(end + 1);
            onLine(line);
        }
    });
    // Even failed enrollment can log its code. Drain but never retain/print
    // server output; only exact startup address lines are inspected above.
    child.stderr.resume();
    return { child, closed };
}

async function stopChild(owner) {
    if (!owner) return;
    if (owner.child.exitCode !== null || owner.child.signalCode !== null) { await owner.closed; return; }
    owner.child.kill("SIGTERM");
    const force = setTimeout(() => owner.child.kill("SIGKILL"), 5000);
    try { await owner.closed; } finally { clearTimeout(force); }
}

async function removeFixture(directory) {
    const resolved = await realpath(directory), info = await lstat(directory);
    assert.equal(resolved, directory);
    assert.equal(dirname(resolved), "/tmp");
    assert(basename(resolved).startsWith(FIXTURE_PREFIX) && info.isDirectory() && !info.isSymbolicLink());
    await rm(resolved, { recursive: true, force: true });
}

/** Owns a real Rust server, its random loopback listeners and its temporary
 * native storage. Never consults production endpoint/data-dir environment
 * variables. The caller must stop its engine/API owners before close().
 * SIGTERM is a process teardown, not a crash-durability/flush assertion.
 * This helper models requestUrl with Node fetch, not an Obsidian native host. */
export async function startNativeRootServer({ binaryPath = DEFAULT_BINARY } = {}) {
    assert(isAbsolute(binaryPath), "native server binary must be an explicit absolute path");
    const binary = await realpath(binaryPath), binaryInfo = await lstat(binary);
    assert(binaryInfo.isFile(), "native server binary must be a regular file");
    const digest = createHash("sha256");
    for await (const chunk of createReadStream(binary)) digest.update(chunk);
    const binaryIdentity = Object.freeze({
        profile: binary.includes("/target/release/") ? "release" : binary.includes("/target/debug/") ? "debug" : "custom",
        bytes: binaryInfo.size, sha256: digest.digest("hex"),
    });
    const directory = await mkdtemp(`/tmp/${FIXTURE_PREFIX}`);
    let owner, closing = false, closePromise;
    // Last-resort cleanup on ordinary Node exit/uncaught failure. SIGKILL of
    // the harness itself is outside JavaScript lifetime guarantees.
    const onExit = () => owner?.child.kill("SIGKILL");
    process.once("exit", onExit);
    const active = new Set();
    const metrics = { requests: 0, syncRequests: 0, adminRequests: 0, requestBytes: 0, responseBytes: 0,
        maxRequestBytes: 0, maxResponseBytes: 0, refusedRequests: 0, failedRequests: 0 };
    const close = () => {
        if (closePromise) return closePromise;
        closing = true;
        closePromise = (async () => {
            await Promise.allSettled([...active]);
            await stopChild(owner);
            process.off("exit", onExit);
            await removeFixture(directory);
        })();
        return closePromise;
    };
    try {
        owner = launch(binary, ["init", "--data-dir", directory]);
        const initDeadline = setTimeout(() => owner.child.kill("SIGKILL"), 15000);
        let initialized;
        try { initialized = await owner.closed; } finally { clearTimeout(initDeadline); }
        if (initialized.launchError || initialized.code !== 0) throw failure("INIT_FAILED");
        const config = JSON.parse(await readFile(join(directory, "config.json"), "utf8"));
        if (config.data_dir !== directory) throw failure("DATA_DIRECTORY_MISMATCH");

        let syncUrl, adminUrl, readyResolve;
        const ready = new Promise(resolve => { readyResolve = resolve; });
        owner = launch(binary, ["run", "--data-dir", directory, "--bind-address", "127.0.0.1",
            "--sync-port", "0", "--admin-port", "0"], line => {
            const match = /^(Sync API:  |Admin GUI: )(http:\/\/127\.0\.0\.1:(\d+))(?: \(AEAD-encrypted payloads\))?$/.exec(line);
            if (!match) return;
            if (Number(match[3]) < 1 || Number(match[3]) > 65535) return;
            if (match[1] === "Sync API:  ") syncUrl = match[2]; else adminUrl = match[2];
            if (syncUrl && adminUrl && syncUrl !== adminUrl) readyResolve();
        });
        let startupTimer;
        try {
            await Promise.race([ready,
                owner.closed.then(() => { throw failure("EXIT_BEFORE_LISTENING"); }),
                new Promise((_, reject) => { startupTimer = setTimeout(() => reject(failure("STARTUP_TIMEOUT")), 15000); }),
            ]);
        } finally { clearTimeout(startupTimer); }
        const origins = new Set([syncUrl, adminUrl]);
        const requestUrl = input => {
            const task = (async () => {
                if (closing) throw failure("REQUEST_AFTER_CLOSE");
                const params = typeof input === "string" ? { url: input } : input;
                let url;
                try { url = new URL(params?.url); } catch { metrics.refusedRequests++; throw failure("INVALID_URL"); }
                if (!origins.has(url.origin) || url.protocol !== "http:" || url.hostname !== "127.0.0.1" ||
                    url.username || url.password || url.hash) {
                    metrics.refusedRequests++; throw failure("NON_OWNED_ENDPOINT");
                }
                if (params.body !== undefined && typeof params.body !== "string" && !(params.body instanceof ArrayBuffer)) {
                    metrics.refusedRequests++; throw failure("INVALID_REQUEST_BODY");
                }
                const bytes = typeof params.body === "string" ? Buffer.byteLength(params.body) : params.body?.byteLength ?? 0;
                if (bytes > HTTP_LIMIT) { metrics.refusedRequests++; throw failure("REQUEST_LIMIT"); }
                metrics.requests++; metrics[url.origin === syncUrl ? "syncRequests" : "adminRequests"]++;
                metrics.requestBytes += bytes; metrics.maxRequestBytes = Math.max(metrics.maxRequestBytes, bytes);
                const headers = new Headers(params.headers);
                if (params.contentType && !headers.has("content-type")) headers.set("content-type", params.contentType);
                const abort = new AbortController(), timer = setTimeout(() => abort.abort(), 30000);
                try {
                    // Never follow redirects, even onto another local port.
                    const response = await fetch(url, { method: params.method ?? "GET", headers, body: params.body,
                        redirect: "manual", credentials: "omit", signal: abort.signal });
                    const declared = Number(response.headers.get("content-length"));
                    if (Number.isFinite(declared) && declared > HTTP_LIMIT) {
                        await response.body?.cancel(); throw failure("RESPONSE_LIMIT");
                    }
                    const arrayBuffer = await response.arrayBuffer();
                    if (arrayBuffer.byteLength > HTTP_LIMIT) throw failure("RESPONSE_LIMIT");
                    metrics.responseBytes += arrayBuffer.byteLength;
                    metrics.maxResponseBytes = Math.max(metrics.maxResponseBytes, arrayBuffer.byteLength);
                    if (params.throw !== false && response.status >= 400) throw failure(`HTTP_${response.status}`);
                    let text, json, parsed = false;
                    return { status: response.status, headers: Object.fromEntries(response.headers), arrayBuffer,
                        get text() { return text ??= new TextDecoder().decode(arrayBuffer); },
                        get json() { if (!parsed) { json = JSON.parse(this.text); parsed = true; } return json; } };
                } catch {
                    metrics.failedRequests++; throw failure("HTTP_REQUEST_FAILED");
                } finally { clearTimeout(timer); }
            })();
            active.add(task); void task.then(() => active.delete(task), () => active.delete(task));
            return requestPromise(task);
        };
        const health = await requestUrl(`${syncUrl}/health`);
        if (health.status !== 200) throw failure("HEALTH_FAILED");
        const enroll = async (deviceName = "native-benchmark") => {
            if (typeof deviceName !== "string" || !/^[A-Za-z0-9_-]{1,64}$/.test(deviceName)) throw failure("INVALID_DEVICE_NAME");
            const page = await requestUrl({ url: `${adminUrl}/admin/devices/new`, method: "POST",
                contentType: "application/x-www-form-urlencoded", body: new URLSearchParams({ device_name: deviceName }).toString() });
            const code = /<code class="code">([A-Z]{4}-[0-9]{4})<\/code>/.exec(page.text)?.[1];
            if (!code) throw failure("ENROLLMENT_CODE_MISSING");
            const response = await requestUrl(`${adminUrl}/admin/enrollment/${code}`), bundle = response.json;
            if (bundle?.device_name !== deviceName || !/^[0-9a-f]{32}$/.test(bundle.device_id) ||
                !/^[0-9a-f]{64}$/.test(bundle.bearer_token) || bundle.wire_version !== "0x02" ||
                bundle.eph_endpoint !== "/api/v1/server-eph" || !Number.isSafeInteger(bundle.Es_pub_valid_until) ||
                bundle.Es_pub_valid_until <= Date.now() / 1000) throw failure("INVALID_ENROLLMENT");
            for (const key of [bundle.server_box_pub, bundle.Es_pub_initial]) {
                if (typeof key !== "string" || Buffer.from(key, "base64").byteLength !== 32 ||
                    Buffer.from(key, "base64").toString("base64") !== key) throw failure("INVALID_ENROLLMENT_KEY");
            }
            return Object.freeze({ ...bundle });
        };
        return Object.freeze({ syncUrl, adminUrl, requestUrl, enroll, close,
            snapshot: () => ({ ...metrics, activeRequests: active.size, closing, binary: binaryIdentity,
                transportHost: "Node fetch requestUrl bridge", listener: "IPv4 loopback, OS-assigned ports" }) });
    } catch (error) {
        await close(); throw error;
    }
}
