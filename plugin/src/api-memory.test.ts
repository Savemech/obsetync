import { strict as assert } from "node:assert";
import {
    ObsetyncApi,
    BulkObjectKind,
    parseHttpReceiveBackpressureDelay,
    type BulkUploadRecord,
} from "./api";
import {
    BULK_MAX_OBJECT_BYTES,
    bulkPackEncodedLength,
    decodeBulkUploadPack,
    encodeBulkUploadPack,
    type BulkCodecLimits,
} from "./bulk-codec";
import { ResourceBudget } from "./resource-budget";
import { TransportRouter } from "./transport-router";
import { WsDataFrameType } from "./ws-data-codec";
import { estimateTransportWorkset, reserveTransientScope } from "./transient-memory";

const MIB = 1024 * 1024;
const limits: BulkCodecLimits = { maxObjects: 256, maxBytes: 2 * MIB, maxObjectBytes: BULK_MAX_OBJECT_BYTES };
const hash = (value: number) => value.toString(16).padStart(64, "0");
const record = (value: number, bytes = 16): BulkUploadRecord => ({
    kind: BulkObjectKind.Content, hash: hash(value), data: new Uint8Array(bytes).fill(value),
});
const ack = (count: number) => {
    const data = new Uint8Array(8 + count);
    data.set(new TextEncoder().encode("OBK1"));
    new DataView(data.buffer).setUint32(4, count, true);
    return data;
};
const checkAck = (count: number) => {
    const data = new Uint8Array(8 + Math.ceil(count / 8));
    data.set(new TextEncoder().encode("OBA1"));
    new DataView(data.buffer).setUint32(4, count, true);
    return data;
};
function page(records: BulkUploadRecord[], count: number, cursor: number): Uint8Array {
    const pack = encodeBulkUploadPack(records, limits);
    const prefix = 12 + Math.ceil(count / 8);
    const data = new Uint8Array(prefix + pack.length);
    data.set(new TextEncoder().encode("OBD1"));
    new DataView(data.buffer).setUint32(4, count, true);
    new DataView(data.buffer).setUint32(8, cursor, true);
    for (let index = cursor; index < count; index++) data[12 + (index >> 3)] |= 1 << (index & 7);
    data.set(pack, prefix);
    return data;
}
function deferred<T>() {
    let resolve!: (value: T) => void;
    const promise = new Promise<T>((yes) => { resolve = yes; });
    return { promise, resolve };
}
async function turns(count = 15): Promise<void> {
    for (let index = 0; index < count; index++) await Promise.resolve();
}

interface Reply {
    body: Uint8Array;
    status?: number;
    wireStatus?: number;
    headers?: Record<string, string>;
}
function fakeApi(budget: ResourceBudget, runtime: "mobile" | "desktop" = "mobile") {
    const api = new ObsetyncApi("http://memory-test", "", "", undefined, runtime, budget);
    let currentBody: Uint8Array = new Uint8Array();
    let currentPath = "";
    const counts = { encrypt: 0, decrypt: 0, request: 0, sequence: 0 };
    let respond: (path: string, body: Uint8Array) => Reply | Promise<Reply> = () => ({ body: ack(1) });
    const internal = api as any;
    internal.bulkLimits = limits;
    internal.wsDataFrameBytes = null;
    internal.nextSequence = async () => ++counts.sequence;
    internal.getChannel = async () => ({
        encryptRequest: async (_method: string, path: string, body: Uint8Array) => {
            counts.encrypt++;
            currentPath = path;
            currentBody = body;
            return new Uint8Array(53 + body.length);
        },
        decryptResponse: async (_method: string, _path: string, _nonce: Uint8Array, wire: Uint8Array) => {
            counts.decrypt++;
            return { status: new DataView(wire.buffer, wire.byteOffset).getUint16(29, true), body: wire.subarray(31) };
        },
    });
    (globalThis as any).__obsetyncTestRequestUrl = async () => {
        counts.request++;
        const reply = await respond(currentPath, currentBody);
        const wire = new Uint8Array(reply.body.length + 31);
        new DataView(wire.buffer).setUint16(29, reply.status ?? 200, true);
        wire.set(reply.body, 31);
        return {
            status: reply.wireStatus ?? 200,
            headers: reply.headers ?? {},
            arrayBuffer: wire.buffer,
            json: null,
        };
    };
    return { api, internal, counts, reply: (callback: typeof respond) => { respond = callback; } };
}

function pressureCounter() {
    return {
        retries: 0,
        events: 0,
        increment(delta: { retries?: number; backpressureEvents?: number }) {
            this.retries += delta.retries ?? 0;
            this.events += delta.backpressureEvents ?? 0;
        },
        phase() { return () => {}; },
    };
}

const replaySafe = { receiveBackpressureReplay: "application-idempotent" };
const markedPressure = {
    wireStatus: 503,
    body: new Uint8Array(),
    headers: { "X-Obsetync-Backpressure": "receive-memory", "Retry-After": "0" },
};
const bulkCapabilities = new TextEncoder().encode(JSON.stringify({
    capabilities: ["bulk-http-v1"],
    limits: { bulk_request_bytes: 2 * MIB, bulk_objects: 256 },
}));

async function markedHttpReceiveBackpressureRetriesOnceWithFreshSequence(): Promise<void> {
    assert.equal(parseHttpReceiveBackpressureDelay(503, {
        "X-Obsetync-Backpressure": "receive-memory", "Retry-After": "1",
    }), 1_000);
    assert.equal(parseHttpReceiveBackpressureDelay(503, {
        "x-obsetync-backpressure": "receive-memory", "retry-after": "999999",
    }), 5_000);
    const invalidHeaders: Array<Record<string, string>> = [
        { "Retry-After": "1" },
        { "X-Obsetync-Backpressure": "receive-memory", "Retry-After": "1.5" },
        { "X-Obsetync-Backpressure": "other", "Retry-After": "1" },
    ];
    for (const headers of invalidHeaders) {
        assert.equal(parseHttpReceiveBackpressureDelay(503, headers), null);
    }

    const fake = fakeApi(new ResourceBudget({ capacityBytes: 4 * MIB }));
    let replies = 0;
    const pressure = pressureCounter();
    fake.reply(() => ++replies === 1 ? markedPressure : { body: new TextEncoder().encode("ok") });
    const response = await fake.internal.sealed(
        "POST", "/api/v1/probe", new Uint8Array(), pressure, undefined, replaySafe,
    );
    assert.equal(response.ok, true);
    assert.equal(fake.counts.request, 2, "marked receive pressure was not retried exactly once");
    assert.equal(fake.counts.sequence, 2, "receive retry reused a transport sequence");
    assert.deepEqual([pressure.retries, pressure.events], [1, 1],
        "receive retry did not report one backpressure/retry event");

    const proxy = fakeApi(new ResourceBudget({ capacityBytes: 4 * MIB }));
    proxy.reply(() => ({
        wireStatus: 503,
        body: new Uint8Array(),
        headers: { "Retry-After": "0" },
    }));
    const refused = await proxy.internal.sealed(
        "POST", "/api/v1/probe", new Uint8Array(), undefined, undefined, replaySafe,
    );
    assert.equal(refused.status, 503);
    assert.equal(proxy.counts.request, 1, "an unmarked proxy 503 was replayed");

    const saturated = fakeApi(new ResourceBudget({ capacityBytes: 4 * MIB }));
    const saturatedPressure = pressureCounter();
    saturated.reply(() => markedPressure);
    const stillBusy = await saturated.internal.sealed(
        "POST", "/api/v1/probe", new Uint8Array(), saturatedPressure, undefined, replaySafe,
    );
    assert.equal(stillBusy.status, 503);
    assert.equal(saturated.counts.request, 2, "receive pressure retried more than once");
    assert.deepEqual([saturatedPressure.retries, saturatedPressure.events], [1, 2],
        "every marked refusal must be counted while only the actual retry increments retries");
}

async function plaintextPressureCannotReplayLegacyRootMutations(): Promise<void> {
    const rollback = fakeApi(new ResourceBudget({ capacityBytes: 4 * MIB }));
    const rollbackPressure = pressureCounter();
    rollback.internal.negotiateTreeVersion = async () => undefined;
    rollback.reply(() => markedPressure);
    await assert.rejects(
        rollback.api.rollbackVault("vault", hash(1), rollbackPressure as any),
        /rollback failed: 503/,
    );
    assert.equal(rollback.counts.request, 1,
        "a plaintext marker replayed a non-idempotent rollback");
    assert.deepEqual([rollbackPressure.retries, rollbackPressure.events], [0, 1]);

    const root = fakeApi(new ResourceBudget({ capacityBytes: 4 * MIB }));
    const rootPressure = pressureCounter();
    root.internal.negotiateTreeVersion = async () => undefined;
    root.reply(() => markedPressure);
    await assert.rejects(
        root.api.putRoot("vault", new Uint8Array([1]), hash(0), rootPressure as any),
        /putRoot failed: 503/,
    );
    assert.equal(root.counts.request, 1,
        "a plaintext marker replayed a legacy root publication");
    assert.deepEqual([rootPressure.retries, rootPressure.events], [0, 1]);
}

async function cancellationDuringPressureDelayStopsRetryAndReleasesAdmission(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 4 * MIB });
    const cancelled = fakeApi(budget);
    const pressure = pressureCounter();
    const abort = new AbortController();
    cancelled.reply(() => markedPressure);
    const upload = cancelled.api.putObjects(
        [record(3)],
        pressure as any,
        undefined,
        abort.signal,
    );
    await turns();
    assert.equal(cancelled.counts.request, 1,
        "cancel test did not reach the admitted HTTP backoff");
    const rejected = assert.rejects(upload, (error: unknown) =>
        error instanceof Error && error.name === "AbortError");
    abort.abort();
    await rejected;
    await turns();
    assert.equal(cancelled.counts.request, 1,
        "an aborted backoff emitted a second encrypted request");
    assert.deepEqual([pressure.retries, pressure.events], [0, 1],
        "an aborted backoff was counted as an actual retry");
    assert.equal(budget.snapshot().usedBytes, 0,
        "cancelled backpressure retained the upload admission");
}

async function cancelledCapabilitiesWaiterDoesNotPoisonSharedNegotiation(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 4 * MIB });
    const shared = fakeApi(budget);
    shared.internal.bulkLimits = undefined;
    shared.internal.wsDataFrameBytes = undefined;
    let capabilityRequests = 0;
    let bulkPuts = 0;
    shared.reply((path) => {
        if (path === "/api/v1/capabilities") {
            capabilityRequests++;
            return capabilityRequests === 1 ? markedPressure : { body: bulkCapabilities };
        }
        if (path === "/api/v1/bulk/put") {
            bulkPuts++;
            return { body: ack(1) };
        }
        throw new Error(`unexpected shared-negotiation path: ${path}`);
    });

    const cancelled = new AbortController();
    const survivor = new AbortController();
    const first = shared.api.putObjects(
        [record(10)], pressureCounter() as any, undefined, cancelled.signal,
    );
    const second = shared.api.putObjects(
        [record(11)], pressureCounter() as any, undefined, survivor.signal,
    );
    await turns();
    assert.equal(capabilityRequests, 1, "callers did not join one capability single-flight");
    const firstRejected = assert.rejects(first, (error: unknown) =>
        error instanceof Error && error.name === "AbortError");
    cancelled.abort();
    await firstRejected;
    await second;

    assert.equal(capabilityRequests, 2,
        "cancelling one waiter restarted or cancelled the shared marked-pressure retry");
    assert.equal(bulkPuts, 1, "the surviving capability waiter did not continue its upload");
    assert.equal(shared.counts.request, 3,
        "shared negotiation emitted an unexpected request fan-out");
    assert.equal(budget.snapshot().usedBytes, 0,
        "shared negotiation or surviving upload leaked admission");
}

async function scopedUploadWaitsForAckAndDoesNotReserveGloballyAgain(): Promise<void> {
    const input = [record(1), record(2)];
    const workBytes = estimateTransportWorkset(bulkPackEncodedLength(input));
    const budget = new ResourceBudget({ capacityBytes: 32 + workBytes });
    const memory = await reserveTransientScope({ ownerBytes: 32, workBytes }, { budget });
    const fake = fakeApi(budget);
    const response = deferred<Reply>();
    fake.reply(() => response.promise);
    let finished = false;
    const upload = fake.api.putObjects(input, undefined, memory).then(() => { finished = true; });
    await turns();
    assert.equal(fake.counts.request, 1);
    assert.equal(finished, false);
    assert.equal(budget.snapshot().queuedRequests, 0, "caller-reserved upload must not queue on itself");
    assert.equal(memory.snapshot().work.usedBytes, workBytes);
    memory.close();
    assert.equal(budget.snapshot().usedBytes, 32 + workBytes, "close cannot release an unsettled native request");
    response.resolve({ body: ack(2) });
    await upload;
    assert.equal(budget.snapshot().usedBytes, 0);
}

async function admissionCountsBackingBuffersAndPrecedesEncryption(): Promise<void> {
    const backing = new Uint8Array(1024);
    const input = [{ ...record(1), data: backing.subarray(10, 20) },
        { ...record(2), data: backing.subarray(20, 30) }];
    const workBytes = estimateTransportWorkset(bulkPackEncodedLength(input));
    const budget = new ResourceBudget({ capacityBytes: 1024 + workBytes });
    const fake = fakeApi(budget);
    fake.reply(() => {
        assert.equal(budget.snapshot().usedBytes, 1024 + workBytes, "shared backing must be counted once, in full");
        return { body: ack(2) };
    });
    await fake.api.putObjects(input);
    assert.equal(budget.snapshot().usedBytes, 0);
    const tooSmall = fakeApi(new ResourceBudget({ capacityBytes: 20 + workBytes }));
    await assert.rejects(tooSmall.api.putObjects(input), /exceeds capacity/);
    assert.equal(tooSmall.counts.encrypt, 0);
}

async function rejectedPacksReleaseWorkspaceBeforeOrderedRetries(): Promise<void> {
    const input = [record(1), record(2), record(3), record(4)];
    const workBytes = estimateTransportWorkset(bulkPackEncodedLength(input));
    const budget = new ResourceBudget({ capacityBytes: 64 + workBytes });
    const memory = await reserveTransientScope({ ownerBytes: 64, workBytes }, { budget });
    const fake = fakeApi(budget);
    const order: number[][] = [];
    fake.reply((_path, body) => {
        const records = decodeBulkUploadPack(body, limits);
        order.push(records.map((item) => item.data[0]));
        assert.equal(memory.snapshot().work.activeReservations, 1);
        return records.length === 4 ? { wireStatus: 413, body: new Uint8Array() } : { body: ack(records.length) };
    });
    await fake.api.putObjects(input, undefined, memory);
    assert.deepEqual(order, [[1, 2, 3, 4], [1, 2], [3, 4]]);
    assert.equal(memory.snapshot().work.usedBytes, 0);
    assert.equal(memory.snapshot().work.peakUsedBytes, workBytes);
    memory.close();
    assert.equal(budget.snapshot().usedBytes, 0);
}

async function legacyFallbackAndMalformedAckReleaseAdmission(): Promise<void> {
    const input = [record(1), record(2)];
    const budget = new ResourceBudget({ capacityBytes: 4 * MIB });
    const fake = fakeApi(budget);
    const paths: string[] = [];
    fake.reply((path) => {
        paths.push(path);
        return path.endsWith("/bulk/put") ? { wireStatus: 404, body: new Uint8Array() } : { body: new Uint8Array() };
    });
    await fake.api.putObjects(input);
    assert.deepEqual(paths, ["/api/v1/bulk/put", `/api/v1/content/${hash(1)}`, `/api/v1/content/${hash(2)}`]);
    assert.equal(budget.snapshot().usedBytes, 0);
    fake.internal.bulkLimits = limits;
    fake.reply(() => ({ body: new Uint8Array([1]) }));
    await assert.rejects(fake.api.putObjects(input), /magic|truncated/);
    assert.equal(budget.snapshot().usedBytes, 0);
}

async function ownedDownloadRetainsAdmissionThroughApplyAndRejectsOversizeBeforeDecrypt(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 32 * MIB });
    const fake = fakeApi(budget);
    const input = [record(1, 32), record(2, 32)];
    fake.reply(() => ({ body: page(input, 2, 2) }));
    const owned = await fake.api.getObjectsOwned(BulkObjectKind.Content, input.map((item) => item.hash));
    assert.equal(owned.objects.size, 2);
    assert.equal(owned.memory.snapshot().work.usedBytes, 0);
    assert.ok(budget.snapshot().usedBytes > 0, "returned maps retain their global parent");
    const route = await fake.api.getBulkDiagnostics();
    assert.equal(route.lastCarrier, "http");
    assert.equal(route.lastCarrierReason, "ws-unavailable");
    assert.ok((route.lastCarrierPayloadBytes ?? Number.MAX_SAFE_INTEGER) <
        (route.lastCarrierExpectedUsefulBytes ?? 0),
    "bulk GET diagnostics confused its small hash request with expected response bytes");
    await owned.memory.run(64, () => assert.equal(owned.objects.get(hash(1))?.length, 32));
    owned.release();
    owned.release();
    assert.equal(owned.objects.size, 0);
    assert.equal(budget.snapshot().usedBytes, 0);
    fake.reply(() => ({ body: new Uint8Array(2 * MIB + 1) }));
    const before = fake.counts.decrypt;
    await assert.rejects(fake.api.getObjectsOwned(BulkObjectKind.Content, [hash(1)]), /admitted byte bound/);
    assert.equal(fake.counts.decrypt, before, "oversized native result must be rejected before crypto copies");
    assert.equal(budget.snapshot().usedBytes, 0);
}

async function oversizedWsPayloadHasAnExplicitRouteReason(): Promise<void> {
    const previousWebSocket = (globalThis as any).WebSocket;
    (globalThis as any).WebSocket = class {};
    try {
        const budget = new ResourceBudget({ capacityBytes: 4 * MIB });
        const fake = fakeApi(budget);
        fake.internal.wsDataFrameBytes = 64;
        fake.reply(() => ({ body: ack(1) }));
        await fake.api.putObjects([record(1, 128)]);
        const diagnostics = await fake.api.getBulkDiagnostics();
        assert.equal(diagnostics.lastCarrier, "http");
        assert.equal(diagnostics.lastCarrierReason, "ws-payload-too-large");
        assert.equal(fake.counts.request, 1);
    } finally {
        if (previousWebSocket === undefined) delete (globalThis as any).WebSocket;
        else (globalThis as any).WebSocket = previousWebSocket;
    }
}

async function legacyLargeObjectFitsMobileAndCumulativeRetentionIsBounded(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 32 * MIB });
    const fake = fakeApi(budget);
    fake.internal.bulkLimits = null;
    fake.reply(() => ({ body: new Uint8Array(4 * MIB) }));
    const owned = await fake.api.getObjectsOwned(BulkObjectKind.ContentChunk, [hash(1)]);
    assert.equal(owned.objects.get(hash(1))?.length, 4 * MIB);
    assert.ok(budget.snapshot().usedBytes <= 32 * MIB);
    owned.release();
    fake.reply(() => ({ body: new Uint8Array(4 * MIB + 1) }));
    const before = fake.counts.decrypt;
    await assert.rejects(fake.api.getObjectsOwned(BulkObjectKind.ContentChunk, [hash(1)]), /admitted byte bound/);
    assert.equal(fake.counts.decrypt, before);
    fake.internal.bulkLimits = limits;
    const input = [record(1, 768 * 1024), record(2, 768 * 1024), record(3, 768 * 1024)];
    fake.reply((_path, request) => {
        const cursor = new DataView(request.buffer, request.byteOffset).getUint32(9, true);
        return { body: cursor === 0 ? page(input.slice(0, 2), 3, 2) : page(input.slice(2), 3, 3) };
    });
    await assert.rejects(fake.api.getObjectsOwned(BulkObjectKind.Content, input.map((item) => item.hash)), /retention budget/);
    assert.equal(budget.snapshot().usedBytes, 0);
}

async function ownedUploadStillUsesTheAutomaticWsLane(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 4 * MIB });
    const fake = fakeApi(budget);
    fake.internal.wsDataFrameBytes = 2 * MIB;
    let wsCalls = 0;
    fake.internal.getDataLane = async () => ({
        request: async (_type: unknown, body: Uint8Array, _expected: unknown, _perf: unknown, work: any) => {
            wsCalls++;
            assert.ok(work && typeof work.track === "function");
            assert.ok(budget.snapshot().usedBytes > 0);
            return ack(decodeBulkUploadPack(body, limits).length);
        },
    });
    fake.reply(() => { throw new Error("healthy owned WS transfer unexpectedly fell back to HTTP"); });
    await fake.api.putObjects([record(1)]);
    assert.equal(wsCalls, 1);
    assert.equal(fake.counts.request, 0);
    const diagnostics = await fake.api.getBulkDiagnostics();
    assert.equal(diagnostics.lastCarrier, "ws");
    assert.equal(diagnostics.lastCarrierReason, "bulk-ws-learning");
    assert.equal(diagnostics.lastCarrierLane, "bulk");
    assert.ok((diagnostics.lastCarrierPayloadBytes ?? 0) > 0);
    assert.equal(budget.snapshot().usedBytes, 0);
}

async function productionRouterOwnsFallbackAndStopsWsPingPong(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 4 * MIB });
    const fake = fakeApi(budget);
    fake.internal.wsDataFrameBytes = 2 * MIB;
    let wsCalls = 0;
    fake.internal.getDataLane = async () => ({
        request: async () => {
            wsCalls++;
            throw new Error("socket unavailable");
        },
    });
    fake.reply(() => ({ body: ack(1) }));
    await fake.api.putObjects([record(1)]);
    await fake.api.putObjects([record(2)]);
    await fake.api.putObjects([record(3)]);
    assert.equal(wsCalls, 2, "open circuit kept probing WS for every pack");
    assert.equal(fake.counts.request, 3, "one WS failure multiplied HTTP fallback attempts");
    assert.equal(fake.internal.transportRouter.snapshot().circuit, "open");
    const diagnostics = await fake.api.getBulkDiagnostics();
    assert.equal(diagnostics.lastCarrier, "http");
    assert.equal(diagnostics.lastCarrierReason, "ws-circuit-open");
    assert.equal(diagnostics.lastCarrierLane, "bulk");
    assert.equal(budget.snapshot().usedBytes, 0);
}

async function callerCancellationNeverFallsBackToHttp(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 4 * MIB });
    const fake = fakeApi(budget);
    fake.internal.wsDataFrameBytes = 2 * MIB;
    let wsCalls = 0;
    fake.internal.getDataLane = async () => ({
        request: async (
            _type: unknown,
            _body: Uint8Array,
            _expected: unknown,
            _perf: unknown,
            _work: unknown,
            _maximum: unknown,
            _timeouts: unknown,
            signal: AbortSignal,
        ) => {
            wsCalls++;
            await new Promise<void>((_resolve, reject) => {
                signal.addEventListener("abort", () => reject(new Error("cancelled")), { once: true });
            });
            return ack(1);
        },
    });
    fake.reply(() => { throw new Error("cancelled WS request fell back to HTTP"); });
    const controller = new AbortController();
    const upload = fake.api.putObjects([record(1)], undefined, undefined, controller.signal);
    await turns();
    controller.abort();
    await assert.rejects(upload, /cancelled|aborted/i);
    assert.equal(wsCalls, 1);
    assert.equal(fake.counts.request, 0, "caller cancellation dispatched HTTP fallback");
    assert.equal(budget.snapshot().usedBytes, 0, "caller cancellation leaked upload scope");
}

async function semanticLanesOwnHalfOpenProbeSelection(): Promise<void> {
    let now = 0;
    const probeSizeThreshold = 100;
    const budget = new ResourceBudget({ capacityBytes: 4 * MIB });
    const fake = fakeApi(budget);
    fake.internal.wsDataFrameBytes = 2 * MIB;
    fake.internal.transportRouter = new TransportRouter({
        now: () => now,
        jitter: () => 0,
        failureThreshold: 1,
        openBaseMs: 100,
        openMaxMs: 1_000,
        recoverySuccessThreshold: 1,
        recoveryProbeMaxPayloadBytes: probeSizeThreshold,
    });
    const wsTypes: WsDataFrameType[] = [];
    const wsPayloadBytes: number[] = [];
    let failFirstWs = true;
    fake.internal.getDataLane = async () => ({
        request: async (type: WsDataFrameType, body: Uint8Array) => {
            wsTypes.push(type);
            wsPayloadBytes.push(body.byteLength);
            if (failFirstWs) {
                failFirstWs = false;
                throw new Error("open semantic-lane circuit");
            }
            if (type === WsDataFrameType.CheckObjects) {
                return checkAck(new DataView(body.buffer, body.byteOffset).getUint32(5, true));
            }
            return ack(decodeBulkUploadPack(body, limits).length);
        },
    });
    fake.reply((path, body) => {
        if (path.endsWith("/bulk/check")) {
            return { body: checkAck(new DataView(body.buffer, body.byteOffset).getUint32(5, true)) };
        }
        return { body: ack(decodeBulkUploadPack(body, limits).length) };
    });

    await fake.api.putObjects([record(1)]);
    assert.equal(fake.internal.transportRouter.snapshot().circuit, "open");
    now = 100;
    await fake.api.putObjects([record(2, 1)]);
    assert.deepEqual(wsTypes, [WsDataFrameType.PutPack],
        "tiny object pack became the half-open probe");
    assert.ok(bulkPackEncodedLength([record(2, 1)]) <= probeSizeThreshold,
        "bulk fixture was not below the legacy size threshold");

    const largeControl = [hash(1), hash(2), hash(3)];
    assert.deepEqual(await fake.api.checkContent(largeControl), []);
    assert.deepEqual(wsTypes, [WsDataFrameType.PutPack, WsDataFrameType.CheckObjects],
        "large CHECK control was rejected by the old size heuristic");
    assert.ok(wsPayloadBytes[1] > probeSizeThreshold,
        "control fixture was not above the legacy size threshold");
    assert.equal(fake.internal.transportRouter.snapshot().circuit, "closed",
        "successful semantic control probe did not close the circuit");

    await fake.api.putObjects([record(3, 1)]);
    assert.deepEqual(wsTypes,
        [WsDataFrameType.PutPack, WsDataFrameType.CheckObjects, WsDataFrameType.PutPack],
        "closed circuit stopped selecting ordinary bulk WS work");
    assert.equal(fake.counts.request, 2,
        "semantic probe selection duplicated or skipped the HTTP fallback owner");
    assert.equal(budget.snapshot().usedBytes, 0, "semantic lane routing leaked admission");
}

async function urgentFilePriorityIsBoundedExplicitAndReplaySafe(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 4 * MIB });
    const fake = fakeApi(budget);
    fake.internal.wsDataFrameBytes = 2 * MIB;
    const router = fake.internal.transportRouter as TransportRouter;
    const execute = router.execute.bind(router);
    const routed: string[] = [];
    router.execute = ((options: any) => {
        routed.push(options.lane);
        return execute(options);
    }) as typeof router.execute;
    const priorities: unknown[] = [];
    fake.internal.getDataLane = async () => ({
        request: async (...args: any[]) => {
            priorities.push(args[8]?.priority);
            return ack(decodeBulkUploadPack(args[1], limits).length);
        },
    });
    fake.reply(() => { throw new Error("healthy urgent WS unexpectedly used HTTP"); });
    await fake.api.putObjects([record(1)]);
    await fake.api.putObjects([record(2)], undefined, undefined, undefined,
        { priority: "urgent-file" });
    assert.deepEqual(routed, ["bulk", "urgent"], "explicit urgent-file priority did not reach router");
    assert.deepEqual(priorities, [undefined, "urgent-file"],
        "default PutPack or explicit urgent-file WS classification changed");

    const beforeCalls = priorities.length;
    await assert.rejects(fake.api.putObjects([record(3), record(4)], undefined, undefined,
        undefined, { priority: "urgent-file" }), /one bounded content object/);
    await assert.rejects(fake.api.putObjects([record(5, 64 * 1024)], undefined, undefined,
        undefined, { priority: "urgent-file" }), /one bounded content object/);
    await assert.rejects(fake.api.putObjects([{ ...record(6), kind: BulkObjectKind.Manifest }],
        undefined, undefined, undefined, { priority: "urgent-file" }), /one bounded content object/);
    assert.equal(priorities.length, beforeCalls, "invalid urgent pack reached transport");

    const replay = fakeApi(new ResourceBudget({ capacityBytes: 4 * MIB }));
    replay.internal.wsDataFrameBytes = 2 * MIB;
    const effects = new Set<string>();
    let wsAttempts = 0;
    replay.internal.getDataLane = async () => ({
        request: async (_type: unknown, body: Uint8Array) => {
            wsAttempts++;
            effects.add(decodeBulkUploadPack(body, limits)[0].hash);
            throw new Error("synthetic lost PutAck");
        },
    });
    replay.reply((_path, body) => {
        const decoded = decodeBulkUploadPack(body, limits);
        for (const item of decoded) effects.add(item.hash);
        return { body: ack(decoded.length) };
    });
    await replay.api.putObjects([record(7)], undefined, undefined, undefined,
        { priority: "urgent-file" });
    assert.equal(wsAttempts, 1, "lost ACK retried the urgent WS effect more than once");
    assert.equal(replay.counts.request, 1, "lost ACK did not use one replay-safe HTTP fallback");
    assert.deepEqual([...effects], [hash(7)], "content-addressed fallback duplicated the stored effect");
    assert.equal(budget.snapshot().usedBytes, 0, "urgent-file routing leaked admission");
}

async function desktopOwnedReceiveFitsARecoverySizedPool(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 32 * MIB });
    const fake = fakeApi(budget, "desktop");
    fake.internal.bulkLimits = { ...limits, maxBytes: 8 * MIB };
    let requestedBytes = 0;
    fake.reply((_path, body) => {
        requestedBytes = new DataView(body.buffer, body.byteOffset).getUint32(13, true);
        return { body: page([record(1)], 1, 1) };
    });
    const owned = await fake.api.getObjectsOwned(BulkObjectKind.Content, [hash(1)]);
    assert.ok(requestedBytes <= owned.memory.snapshot().ownerBytes);
    assert.ok(owned.memory.snapshot().totalBytes <= 32 * MIB);
    assert.equal(fake.internal.bulkLimits.maxBytes, 8 * MIB, "per-call admission must not rewrite capabilities");
    owned.release();
    assert.equal(budget.snapshot().usedBytes, 0);
}

void (async () => {
    try {
        await markedHttpReceiveBackpressureRetriesOnceWithFreshSequence();
        await plaintextPressureCannotReplayLegacyRootMutations();
        await cancellationDuringPressureDelayStopsRetryAndReleasesAdmission();
        await cancelledCapabilitiesWaiterDoesNotPoisonSharedNegotiation();
        await scopedUploadWaitsForAckAndDoesNotReserveGloballyAgain();
        await admissionCountsBackingBuffersAndPrecedesEncryption();
        await rejectedPacksReleaseWorkspaceBeforeOrderedRetries();
        await legacyFallbackAndMalformedAckReleaseAdmission();
        await ownedDownloadRetainsAdmissionThroughApplyAndRejectsOversizeBeforeDecrypt();
        await oversizedWsPayloadHasAnExplicitRouteReason();
        await legacyLargeObjectFitsMobileAndCumulativeRetentionIsBounded();
        await ownedUploadStillUsesTheAutomaticWsLane();
        await productionRouterOwnsFallbackAndStopsWsPingPong();
        await callerCancellationNeverFallsBackToHttp();
        await semanticLanesOwnHalfOpenProbeSelection();
        await urgentFilePriorityIsBoundedExplicitAndReplaySafe();
        await desktopOwnedReceiveFitsARecoverySizedPool();
        console.log("api-memory.test: 17 ownership/transport regression scenarios passed");
    } finally {
        delete (globalThis as any).__obsetyncTestRequestUrl;
    }
})().catch((error) => { setTimeout(() => { throw error; }, 0); });
