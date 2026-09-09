import { strict as assert } from "node:assert";
import type { App } from "obsidian";
import { exactArrayBuffer } from "./binary";
import {
    MOBILE_RESOURCE_RANGE_MAX_BYTES,
    MobileRangeReadError,
    type MobileRangeFetchResponse,
    type MobileRangeHost,
} from "./mobile-ranged-source";
import { ObsetyncDesktopIO, ObsetyncMobileIO, type PlatformIO } from "./platform";
import { transientMemorySnapshot } from "./transient-memory";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

function response(status: number, offset: number, bytes: Uint8Array, total: number,
    options: { range?: string; length?: string; type?: string; redirected?: boolean;
        encoding?: string; truncated?: boolean; oversizedBacking?: boolean;
        lazyRejectedBody?: boolean; onReader?: () => void; onCancel?: () => void } = {}): MobileRangeFetchResponse {
    const emitted = options.truncated ? bytes.subarray(0, Math.max(0, bytes.length - 1)) : bytes;
    const chunk = options.oversizedBacking
        ? new Uint8Array(new Uint8Array(bytes.length + 1).buffer, 0, bytes.length)
        : Uint8Array.from(emitted);
    const body = options.lazyRejectedBody ? {
        getReader() { options.onReader?.(); throw new Error("rejected body was read"); },
        async cancel() { options.onCancel?.(); },
    } as unknown as ReadableStream<Uint8Array> : new ReadableStream<Uint8Array>({
        start(controller) { if (chunk.length) controller.enqueue(chunk); controller.close(); },
    });
    return {
        status,
        type: options.type ?? "basic",
        redirected: options.redirected ?? false,
        headers: { get(name) {
            if (name === "content-range") return options.range ??
                `bytes ${offset}-${offset + bytes.length - 1}/${total}`;
            if (name === "content-length") return options.length ?? String(bytes.length);
            if (name === "content-encoding") return options.encoding ?? null;
            return null;
        } },
        body,
    };
}

function fixture() {
    const source = Uint8Array.from([10, 11, 12, 13, 14, 15, 16, 17]);
    let stat = { type: "file" as const, size: source.length, mtime: 7 };
    let resourceUrl = "app://local/vault/large.bin";
    let fetches = 0;
    let mode: "valid" | "200" | "opaque" | "redirect" | "bad-range" |
        "bad-length" | "encoded" | "truncated" | "oversized-backing" | "failed" |
        "wait-abort" = "valid";
    let bodyReaders = 0, bodyCancels = 0;
    let driftOnFetch = false;
    const requests: RequestInit[] = [];
    const host: MobileRangeHost = {
        available: () => true,
        async fetch(_url, init) {
            fetches++;
            requests.push(init);
            if (mode === "failed") throw new Error("private host path");
            if (mode === "wait-abort") {
                return await new Promise<never>((_resolve, reject) => {
                    init.signal?.addEventListener("abort", () => reject(new Error("private abort")), { once: true });
                });
            }
            const header = (init.headers as Record<string, string>).Range;
            const match = /^bytes=(\d+)-(\d+)$/.exec(header);
            assert(match);
            const offset = Number(match[1]), end = Number(match[2]);
            const bytes = source.slice(offset, end + 1);
            if (driftOnFetch) { driftOnFetch = false; stat = { ...stat, mtime: stat.mtime + 1 }; }
            return response(mode === "200" ? 200 : 206, offset, bytes, source.length, {
                type: mode === "opaque" ? "opaque" : undefined,
                redirected: mode === "redirect",
                range: mode === "bad-range" ? `bytes 0-0/${source.length + 1}` : undefined,
                length: mode === "bad-length" ? String(bytes.length + 1) : undefined,
                encoding: mode === "encoded" ? "gzip" : undefined,
                truncated: mode === "truncated",
                oversizedBacking: mode === "oversized-backing",
                lazyRejectedBody: ["200", "opaque", "redirect", "bad-range", "bad-length", "encoded"].includes(mode),
                onReader: () => { bodyReaders++; },
                onCancel: () => { bodyCancels++; },
            });
        },
    };
    const adapter = {
        stat: async () => stat,
        getResourcePath: () => resourceUrl,
    };
    const app = { vault: { adapter } } as unknown as App;
    const io = new ObsetyncMobileIO(app, host);
    return {
        io, source, requests,
        fetches: () => fetches,
        bodyReaders: () => bodyReaders,
        bodyCancels: () => bodyCancels,
        setMode: (value: typeof mode) => { mode = value; },
        drift: () => { stat = { ...stat, mtime: stat.mtime + 1 }; },
        driftOnNextFetch: () => { driftOnFetch = true; },
        remap: () => { resourceUrl += "?replacement"; },
    };
}

async function qualifiedReaderIsStrictAndOwned(): Promise<void> {
    const f = fixture();
    const baseline = transientMemorySnapshot().usedBytes;
    check(f.io.mobileRangeCapability().state === "unprobed", "mobile range gate guessed support");
    const reader = await f.io.openMobileRangeReader("large.bin", { size: f.source.length, mtime: 7 });
    check(reader !== null && f.fetches() === 1, "mobile reader was not qualified by one real range probe");
    check(f.io.mobileRangeCapability().state === "qualified", "successful probe was not diagnosed");
    const range = await reader!.read(2, 4);
    check(range.bytes.join(",") === "12,13,14,15", "qualified reader changed range bytes");
    check(transientMemorySnapshot().usedBytes > baseline, "returned range outlived its admission");
    range.release();
    range.release();
    check(transientMemorySnapshot().usedBytes === baseline, "range admission did not return to baseline");
    const request = f.requests[1];
    check(request.redirect === "error" && request.cache === "no-store" &&
        (request.headers as Record<string, string>).Range === "bytes=2-5",
    "resource request permitted redirect/cache drift or sent the wrong range");
    reader!.close();
    const closed = await reader!.read(0, 1).then(() => null, error => error);
    check(closed instanceof MobileRangeReadError && closed.code === "CLOSED", "closed reader performed IO");
}

async function invalidHostResponsesFailClosed(): Promise<void> {
    for (const mode of ["200", "opaque", "redirect", "bad-range", "bad-length", "encoded",
        "truncated", "oversized-backing", "failed"] as const) {
        const f = fixture();
        f.setMode(mode);
        const baseline = transientMemorySnapshot().usedBytes;
        const reader = await f.io.openMobileRangeReader("large.bin", { size: f.source.length, mtime: 7 });
        check(reader === null, `${mode} response became a ranged source`);
        const diagnosis = f.io.mobileRangeCapability();
        check(diagnosis.state === "rejected" && diagnosis.reason ===
            (mode === "failed" ? "UNAVAILABLE" : "PROTOCOL"), `${mode} lost fixed capability diagnosis`);
        check(transientMemorySnapshot().usedBytes === baseline, `${mode} leaked range admission`);
        if (!["truncated", "oversized-backing", "failed"].includes(mode)) {
            check(f.bodyReaders() === 0 && f.bodyCancels() === 1,
                `${mode} response body was read before header rejection`);
        }
        const before = f.fetches();
        check(await f.io.openMobileRangeReader("large.bin", { size: f.source.length, mtime: 7 }) === null &&
            f.fetches() === before, `${mode} rejection repeatedly probed the same runtime`);
    }
}

async function capabilityAndSourceDriftStayDistinct(): Promise<void> {
    const capability = fixture();
    const baseline = transientMemorySnapshot().usedBytes;
    const reader = await capability.io.openMobileRangeReader("large.bin", { size: 8, mtime: 7 });
    assert(reader);
    capability.setMode("200");
    const protocol = await reader.read(0, 2).then(() => null, error => error);
    check(protocol instanceof MobileRangeReadError && protocol.code === "PROTOCOL",
        "post-probe 200 response escaped capability-drift fence");
    check(capability.io.mobileRangeCapability().state === "rejected",
        "capability drift did not latch safe pending mode");
    check(capability.fetches() === 2, "capability drift retried or consumed a whole-file fallback");
    check(transientMemorySnapshot().usedBytes === baseline, "capability drift leaked range admission");

    for (const mutate of ["stat", "url"] as const) {
        const source = fixture();
        const current = await source.io.openMobileRangeReader("large.bin", { size: 8, mtime: 7 });
        assert(current);
        mutate === "stat" ? source.drift() : source.remap();
        const before = source.fetches();
        const drift = await current.read(0, 1).then(() => null, error => error);
        check(drift instanceof MobileRangeReadError && drift.code === "SOURCE_CHANGED",
            `${mutate} drift was mistaken for range capability loss`);
        check(source.fetches() === before && source.io.mobileRangeCapability().state === "qualified",
            `${mutate} drift fetched bytes or poisoned the host capability`);
    }

    const postRead = fixture();
    const postReadReader = await postRead.io.openMobileRangeReader("large.bin", { size: 8, mtime: 7 });
    assert(postReadReader);
    postRead.driftOnNextFetch();
    const postReadBaseline = transientMemorySnapshot().usedBytes;
    const drift = await postReadReader.read(0, 2).then(() => null, error => error);
    check(drift instanceof MobileRangeReadError && drift.code === "SOURCE_CHANGED" &&
        transientMemorySnapshot().usedBytes === postReadBaseline,
    "post-read source drift returned bytes or leaked their admission");
}

async function boundsAbortAndDesktopIsolation(): Promise<void> {
    const invalid = fixture();
    await assert.rejects(invalid.io.openMobileRangeReader("large.bin", { size: Number.NaN, mtime: 7 }), RangeError);
    check(invalid.fetches() === 0 && invalid.io.mobileRangeCapability().state === "unprobed",
        "invalid caller metadata poisoned the runtime capability");
    const f = fixture();
    const reader = await f.io.openMobileRangeReader("large.bin", { size: 8, mtime: 7 });
    assert(reader);
    for (const [offset, size] of [[-1, 1], [0, 0], [7, 2], [0, MOBILE_RESOURCE_RANGE_MAX_BYTES + 1]]) {
        await assert.rejects(reader.read(offset, size), RangeError);
    }
    check(f.fetches() === 1, "invalid range reached resource fetch");
    const controller = new AbortController(); controller.abort();
    const aborted = await reader.read(0, 1, controller.signal).then(() => null, error => error);
    check((aborted as Error)?.name === "AbortError" && f.fetches() === 1,
        "pre-aborted range reached resource fetch");

    const pending = fixture();
    const pendingReader = await pending.io.openMobileRangeReader("large.bin", { size: 8, mtime: 7 });
    assert(pendingReader);
    pending.setMode("wait-abort");
    const pendingBaseline = transientMemorySnapshot().usedBytes;
    const inFlightController = new AbortController();
    const inFlight = pendingReader.read(0, 1, inFlightController.signal).then(() => null, error => error);
    for (let turn = 0; turn < 20 && pending.fetches() < 2; turn++) await Promise.resolve();
    check(pending.fetches() === 2, "abort fixture did not enter the native request");
    inFlightController.abort();
    const inFlightError = await inFlight;
    check((inFlightError as Error)?.name === "AbortError" &&
        transientMemorySnapshot().usedBytes === pendingBaseline,
    "in-flight abort returned early or leaked its admitted native request");

    const app = { vault: { adapter: {} } } as unknown as App;
    const unavailable = new ObsetyncMobileIO(app);
    check(await unavailable.openMobileRangeReader("large.bin", { size: 8, mtime: 7 }) === null &&
        unavailable.mobileRangeCapability().reason === "UNAVAILABLE",
    "missing public resource API did not fail closed");
    check(await unavailable.readFileIdentityVerified("large.bin", { size: 8, mtime: 7 }) === null,
        "mobile IO inherited the desktop file-descriptor accelerator");
    const desktop: PlatformIO = new ObsetyncDesktopIO(app);
    check(desktop.openMobileRangeReader === undefined && desktop.mobileRangeCapability === undefined,
        "desktop IO exposed the mobile resource accelerator");

    let resolutions = 0;
    const escaped = new ObsetyncDesktopIO({ vault: { adapter: {
        getFullPath(path: string) { resolutions++; return path ? "/outside/note.md" : "/vault"; },
    } } } as unknown as App);
    await assert.rejects(escaped.readFileIdentityVerified("note.md", { size: 1, mtime: 1 }), /escaped the vault root/);
    check(resolutions === 2, "desktop containment check opened or repeatedly resolved an escaped source");
    await assert.rejects(escaped.readFileIdentityVerified("../outside.md", { size: 1, mtime: 1 }), RangeError);
    check(resolutions === 2, "unsafe relative source reached the desktop adapter");
}

async function run(): Promise<void> {
    const backing = new Uint8Array([9, 1, 2, 3, 8]);
    const exact = new Uint8Array(exactArrayBuffer(backing.subarray(1, 4)));
    check(exact.length === 3, "binary writer leaked bytes outside a Uint8Array view");
    check(exact.join(",") === "1,2,3", "binary writer changed the selected bytes");
    await qualifiedReaderIsStrictAndOwned();
    await invalidHostResponsesFailClosed();
    await capabilityAndSourceDriftStayDistinct();
    await boundsAbortAndDesktopIsolation();
    console.log(`platform.test: ${assertions} assertions passed`);
}

void run().catch(error => { console.error(error); process.exitCode = 1; });
