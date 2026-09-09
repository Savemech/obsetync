import {
    ObsetyncWsDataLane,
    WsDataRpcError,
    WsDataUnavailableError,
} from "./ws-data";
import {
    WsDataErrorCode,
    WsDataFrameType,
    decodeWsDataFrame,
    decodeWsDataFragment,
    encodeWsDataFrame,
    encodeWsDataFragment,
    initialWsDataV2Limits,
    wsDataFragmentCount,
    type WsDataFragment,
} from "./ws-data-codec";
import { PerfTrace } from "./perf-trace";
import { ResourceBudget } from "./resource-budget";
import { reserveTransientScope } from "./transient-memory";

const check = (condition: unknown, message: string) => {
    if (!condition) throw new Error(message);
};

let assertions = 0;
const ok = (condition: unknown, message: string) => {
    assertions++;
    check(condition, message);
};

class IdentitySession {
    async sealBytes(bytes: Uint8Array): Promise<Uint8Array> { return bytes.slice(); }
    async openBytes(bytes: Uint8Array): Promise<Uint8Array> { return bytes.slice(); }
}

class StatefulSealSession {
    private nextSequence = 0;
    private rpcSealTriggered = false;

    constructor(private readonly onRpcSeal: () => void) {}

    async sealBytes(bytes: Uint8Array): Promise<Uint8Array> {
        const sequence = this.nextSequence++;
        if (!this.rpcSealTriggered && bytes[4] === WsDataFrameType.PutPack) {
            this.rpcSealTriggered = true;
            this.onRpcSeal();
            await Promise.resolve();
        }
        const wire = new Uint8Array(4 + bytes.byteLength);
        new DataView(wire.buffer).setUint32(0, sequence, true);
        wire.set(bytes, 4);
        return wire;
    }

    async openBytes(bytes: Uint8Array): Promise<Uint8Array> { return bytes.slice(); }
}

function contiguousSequenceOpener(received: number[]): (wire: Uint8Array) => Uint8Array {
    return (wire) => {
        if (wire.byteLength < 4) throw new Error("test AEAD envelope truncated");
        const sequence = new DataView(wire.buffer, wire.byteOffset, 4).getUint32(0, true);
        if (sequence !== received.length) {
            throw new Error(`test server expected sequence ${received.length}, got ${sequence}`);
        }
        received.push(sequence);
        return wire.slice(4);
    };
}

type RpcFrame = ReturnType<typeof decodeWsDataFrame>;

class FakeSocket {
    binaryType = "";
    readyState = 0;
    bufferedAmount = 0;
    onopen: (() => void) | null = null;
    onmessage: ((event: { data: unknown }) => void) | null = null;
    onclose: (() => void) | null = null;
    onerror: (() => void) | null = null;
    readonly rpcFrames: RpcFrame[] = [];
    readonly cancelFrames: RpcFrame[] = [];
    maxHeld = 0;
    private held: RpcFrame[] = [];

    constructor(
        private readonly mode:
            | "echo"
            | "hold"
            | "drop-rpc"
            | "malformed-error"
            | "remote-error-once"
            | "cancel-ack"
            | "cancel-mismatch"
            | "cancel-oversize" = "echo",
        private readonly ackLimits = {
            maxInflightRequests: 2,
            maxPayloadBytes: 2 * 1024 * 1024,
            maxInflightBytes: 8 * 1024 * 1024,
        },
    ) {
        queueMicrotask(() => {
            this.readyState = 1;
            this.onopen?.();
        });
    }

    send(data: string | ArrayBuffer): void {
        if (typeof data === "string") return;
        const frame = decodeWsDataFrame(new Uint8Array(data), 4 * 1024 * 1024);
        if (frame.type === WsDataFrameType.Hello) {
            const ack = frame.payload.slice();
            ack.set(new TextEncoder().encode("OWA1"), 0);
            new DataView(ack.buffer).setUint16(6, this.ackLimits.maxInflightRequests, true);
            new DataView(ack.buffer).setUint32(8, this.ackLimits.maxPayloadBytes, true);
            new DataView(ack.buffer).setBigUint64(
                16,
                BigInt(this.ackLimits.maxInflightBytes),
                true,
            );
            this.respond(WsDataFrameType.HelloAck, 0, ack);
            return;
        }
        if (frame.type === WsDataFrameType.Cancel) {
            this.cancelFrames.push(frame);
            if (this.mode === "cancel-ack") {
                this.respond(
                    WsDataFrameType.Error,
                    frame.requestId,
                    errorPayload(WsDataErrorCode.Cancelled, 0, "cancelled"),
                );
            } else if (this.mode === "cancel-mismatch") {
                this.respond(WsDataFrameType.GetResult, frame.requestId, new Uint8Array([1]));
            } else if (this.mode === "cancel-oversize") {
                this.respond(
                    WsDataFrameType.CheckResult,
                    frame.requestId,
                    new Uint8Array(1025),
                );
            }
            return;
        }
        this.rpcFrames.push(frame);
        if (this.mode === "drop-rpc") {
            this.readyState = 3;
            queueMicrotask(() => this.onclose?.());
            return;
        }
        if (this.mode === "malformed-error") {
            this.respond(WsDataFrameType.Error, frame.requestId, new Uint8Array([0]));
            return;
        }
        if (this.mode === "remote-error-once" && this.rpcFrames.length === 1) {
            this.respond(
                WsDataFrameType.Error,
                frame.requestId,
                errorPayload(WsDataErrorCode.Busy, 25, "storage busy"),
            );
            return;
        }
        if (this.mode === "hold") {
            this.held.push(frame);
            this.maxHeld = Math.max(this.maxHeld, this.held.length);
            return;
        }
        if ((this.mode === "cancel-ack" || this.mode === "cancel-mismatch" ||
            this.mode === "cancel-oversize") && this.rpcFrames.length === 1) return;
        this.respond(responseType(frame.type), frame.requestId, frame.payload);
    }

    close(): void {
        this.readyState = 3;
    }

    releaseHeldInReverse(): void {
        for (const frame of this.held.splice(0).reverse()) {
            this.respond(responseType(frame.type), frame.requestId, frame.payload);
        }
    }

    private respond(type: WsDataFrameType, requestId: number, payload: Uint8Array): void {
        const response = encodeWsDataFrame(type, requestId, payload, 4 * 1024 * 1024);
        queueMicrotask(() => this.onmessage?.({ data: response.buffer }));
    }
}

class V2FakeSocket {
    binaryType = "";
    readyState = 0;
    bufferedAmount = 0;
    onopen: (() => void) | null = null;
    onmessage: ((event: { data: unknown }) => void) | null = null;
    onclose: (() => void) | null = null;
    onerror: (() => void) | null = null;
    readonly rpcFragments: WsDataFragment[] = [];
    readonly cancelFragments: WsDataFragment[] = [];
    onFirstRpcFragment: (() => void) | null = null;
    private readonly heldResponses: ArrayBuffer[] = [];
    private readonly limits: ReturnType<typeof initialWsDataV2Limits>;
    private readonly assembling = new Map<number, {
        type: WsDataFrameType;
        payload: Uint8Array;
        next: number;
        count: number;
    }>();

    constructor(
        private readonly malformedResponse = false,
        private readonly holdResponseTail = false,
        private readonly openClientFrame: (wire: Uint8Array) => Uint8Array = wire => wire,
        maxInflightRequests = 4,
    ) {
        this.limits = initialWsDataV2Limits({
            maxInflightRequests,
            maxPayloadBytes: 4 * 1024 * 1024,
            maxInflightBytes: 32 * 1024 * 1024,
        });
        queueMicrotask(() => {
            this.readyState = 1;
            this.onopen?.();
        });
    }

    send(data: string | ArrayBuffer): void {
        if (typeof data === "string") return;
        const fragment = decodeWsDataFragment(
            this.openClientFrame(new Uint8Array(data)),
            this.limits,
        );
        if (fragment.type === WsDataFrameType.Hello) {
            const ack = fragment.payload.slice();
            ack.set(new TextEncoder().encode("OWA2"), 0);
            this.respondLogical(WsDataFrameType.HelloAck, 0, ack);
            return;
        }
        if (fragment.type === WsDataFrameType.Cancel) {
            this.cancelFragments.push(fragment);
            this.respondLogical(
                WsDataFrameType.Error,
                fragment.requestId,
                errorPayload(WsDataErrorCode.Cancelled, 0, "cancelled"),
            );
            return;
        }
        this.rpcFragments.push(fragment);
        if (this.rpcFragments.length === 1) this.onFirstRpcFragment?.();
        let assembly = this.assembling.get(fragment.requestId);
        if (!assembly) {
            assembly = {
                type: fragment.type,
                payload: new Uint8Array(fragment.logicalLength),
                next: 0,
                count: fragment.fragmentCount,
            };
            this.assembling.set(fragment.requestId, assembly);
        }
        if (assembly.next !== fragment.fragmentIndex) throw new Error("test received gapped request");
        assembly.payload.set(fragment.payload, fragment.offset);
        assembly.next++;
        if (assembly.next !== assembly.count) return;
        this.assembling.delete(fragment.requestId);
        const type = responseType(assembly.type);
        if (this.malformedResponse) {
            const response = encodeWsDataFragment(type, fragment.requestId, assembly.payload, 0,
                this.limits);
            new DataView(response.buffer).setUint32(22, 1, true);
            queueMicrotask(() => this.onmessage?.({ data: response.buffer }));
            return;
        }
        this.respondLogical(type, fragment.requestId, assembly.payload);
    }

    close(): void {
        this.readyState = 3;
    }

    releaseResponseTail(): void {
        for (const data of this.heldResponses.splice(0)) {
            queueMicrotask(() => this.onmessage?.({ data }));
        }
    }

    private respondLogical(type: WsDataFrameType, requestId: number, payload: Uint8Array): void {
        const count = wsDataFragmentCount(payload.byteLength, this.limits.maxFragmentPayloadBytes);
        for (let index = 0; index < count; index++) {
            const response = encodeWsDataFragment(type, requestId, payload, index, this.limits);
            if (this.holdResponseTail && type !== WsDataFrameType.HelloAck &&
                type !== WsDataFrameType.Error && index > 0) {
                this.heldResponses.push(response.buffer as ArrayBuffer);
            } else {
                queueMicrotask(() => this.onmessage?.({ data: response.buffer }));
            }
        }
    }
}

class RejectV2Socket {
    binaryType = "";
    readyState = 0;
    bufferedAmount = 0;
    onopen: (() => void) | null = null;
    onmessage: ((event: { data: unknown }) => void) | null = null;
    onclose: (() => void) | null = null;
    onerror: (() => void) | null = null;

    constructor() {
        queueMicrotask(() => {
            this.readyState = 1;
            this.onopen?.();
        });
    }

    send(data: string | ArrayBuffer): void {
        if (typeof data === "string") return;
        this.readyState = 3;
        queueMicrotask(() => this.onclose?.());
    }

    close(): void {
        this.readyState = 3;
    }
}

function errorPayload(code: WsDataErrorCode, retryAfterMs: number, message: string): Uint8Array {
    const messageBytes = new TextEncoder().encode(message);
    const output = new Uint8Array(14 + messageBytes.byteLength);
    output.set(new TextEncoder().encode("OWE1"), 0);
    const view = new DataView(output.buffer);
    view.setUint16(4, code, true);
    view.setUint32(8, retryAfterMs, true);
    view.setUint16(12, messageBytes.byteLength, true);
    output.set(messageBytes, 14);
    return output;
}

async function eventually(predicate: () => boolean, message: string): Promise<void> {
    for (let attempt = 0; attempt < 50; attempt++) {
        if (predicate()) return;
        await new Promise<void>((resolve) => setTimeout(resolve, 0));
    }
    throw new Error(message);
}

function deferred<T>(): {
    promise: Promise<T>;
    resolve: (value: T) => void;
    reject: (error: unknown) => void;
} {
    let resolve!: (value: T) => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<T>((resolvePromise, rejectPromise) => {
        resolve = resolvePromise;
        reject = rejectPromise;
    });
    return { promise, resolve, reject };
}

function responseType(type: WsDataFrameType): WsDataFrameType {
    switch (type) {
        case WsDataFrameType.CheckObjects: return WsDataFrameType.CheckResult;
        case WsDataFrameType.PutPack: return WsDataFrameType.PutAck;
        case WsDataFrameType.GetPack: return WsDataFrameType.GetResult;
        default: throw new Error("unexpected request type");
    }
}

const openSession = async () => ({ ticket: "aa".repeat(32), session: new IdentitySession() });

async function multiplexesByRequestIdAndHonorsCredits(): Promise<void> {
    const sockets: FakeSocket[] = [];
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "desktop",
        advertisedPayloadBytes: 4 * 1024 * 1024,
        openSession,
        socketFactory: () => {
            const socket = new FakeSocket("hold");
            sockets.push(socket);
            return socket;
        },
    });
    const requests = [1, 2, 3, 4].map((value) => lane.request(
        value % 2 === 0 ? WsDataFrameType.GetPack : WsDataFrameType.CheckObjects,
        new Uint8Array([value]),
        value % 2 === 0 ? WsDataFrameType.GetResult : WsDataFrameType.CheckResult,
    ));
    await eventually(() => sockets[0]?.rpcFrames.length === 2, "initial credit window did not fill");
    ok(sockets[0]?.rpcFrames.length === 2, "concurrent callers exceeded two request credits");
    sockets[0].releaseHeldInReverse();
    await eventually(() => sockets[0]?.rpcFrames.length === 4, "released credits did not resume senders");
    ok(sockets[0]?.rpcFrames.length === 4, "credit waiters did not make progress");
    sockets[0].releaseHeldInReverse();
    const results = await Promise.all(requests);
    for (let index = 0; index < results.length; index++) {
        ok(results[index][0] === index + 1, "out-of-order response resolved the wrong request");
    }
    ok(sockets[0]?.maxHeld === 2, "negotiated request credits were not used");
    ok(lane.getLimits()?.maxInflightRequests === 2, "HELLO_ACK credits were ignored");
    lane.close();
}

async function remoteRpcErrorDoesNotPoisonTheSession(): Promise<void> {
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "desktop",
        advertisedPayloadBytes: 4 * 1024 * 1024,
        openSession,
        socketFactory: () => new FakeSocket("remote-error-once"),
    });
    let remote: WsDataRpcError | null = null;
    try {
        await lane.request(
            WsDataFrameType.CheckObjects,
            new Uint8Array([1]),
            WsDataFrameType.CheckResult,
        );
    } catch (error) {
        if (error instanceof WsDataRpcError) remote = error;
    }
    ok(remote?.remote.code === WsDataErrorCode.Busy, "structured RPC error was lost");
    ok(lane.getState() === "ready", "one valid RPC error closed a healthy session");
    const recovered = await lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([2]),
        WsDataFrameType.CheckResult,
    );
    ok(recovered[0] === 2, "session did not continue after a valid RPC error");
    lane.close();
}

async function localGovernorCapStaysBelowServerCredits(): Promise<void> {
    let localLimit = 1;
    let backpressureEvents = 0;
    const perf = {
        phase: () => () => {},
        addPhase: () => {},
        increment(delta: { backpressureEvents?: number }) {
            backpressureEvents += delta.backpressureEvents ?? 0;
        },
    } as any;
    let socket: FakeSocket | null = null;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "desktop",
        advertisedPayloadBytes: 4096,
        openSession,
        localRequestLimit: () => localLimit,
        socketFactory: () => {
            socket = new FakeSocket("hold", {
                maxInflightRequests: 4,
                maxPayloadBytes: 1024,
                maxInflightBytes: 4096,
            });
            return socket;
        },
    });
    const first = lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([1]),
        WsDataFrameType.CheckResult,
        perf,
    );
    const second = lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([2]),
        WsDataFrameType.CheckResult,
        perf,
    );
    await eventually(() => socket?.rpcFrames.length === 1, "local request cap did not engage");
    ok(
        (socket as FakeSocket | null)?.rpcFrames.length === 1,
        "server credits bypassed the local governor cap",
    );
    localLimit = 2;
    socket!.releaseHeldInReverse();
    await eventually(() => socket?.rpcFrames.length === 2, "raised local cap did not resume work");
    socket!.releaseHeldInReverse();
    await Promise.all([first, second]);
    ok(backpressureEvents === 0, "local governor saturation was misreported as server pressure");
    lane.close();
}

async function byteCreditsAndSocketWatermarksApplyBackpressure(): Promise<void> {
    let socket: FakeSocket | null = null;
    let backpressureEvents = 0;
    const perf = {
        phase: () => () => {},
        addPhase: () => {},
        increment(delta: { backpressureEvents?: number }) {
            backpressureEvents += delta.backpressureEvents ?? 0;
        },
    } as any;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "desktop",
        advertisedPayloadBytes: 4096,
        openSession,
        socketFactory: () => {
            socket = new FakeSocket("hold", {
                maxInflightRequests: 4,
                maxPayloadBytes: 1024,
                maxInflightBytes: 1500,
            });
            socket.bufferedAmount = 2000;
            return socket;
        },
    });
    const first = lane.request(
        WsDataFrameType.PutPack,
        new Uint8Array(800),
        WsDataFrameType.PutAck,
        perf,
    );
    const second = lane.request(
        WsDataFrameType.PutPack,
        new Uint8Array(800),
        WsDataFrameType.PutAck,
        perf,
    );
    await eventually(() => socket !== null, "backpressure socket was not constructed");
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
    ok(socket!.rpcFrames.length === 0, "high bufferedAmount did not pause the sender");
    socket!.bufferedAmount = 700;
    await eventually(() => socket!.rpcFrames.length === 1, "low watermark did not resume sender");
    ok(socket!.rpcFrames.length === 1, "in-flight byte budget admitted a second request");
    socket!.releaseHeldInReverse();
    await eventually(() => socket!.rpcFrames.length === 2, "byte credit release did not resume waiter");
    socket!.releaseHeldInReverse();
    await Promise.all([first, second]);
    ok(backpressureEvents === 2, "credit and socket pressure were not both measured once");
    lane.close();
}

async function malformedErrorRejectsPendingWithoutHanging(): Promise<void> {
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "desktop",
        advertisedPayloadBytes: 4 * 1024 * 1024,
        openSession,
        backoffStartMs: 1,
        socketFactory: () => new FakeSocket("malformed-error"),
    });
    const outcome = await Promise.race([
        lane.request(
            WsDataFrameType.CheckObjects,
            new Uint8Array([1]),
            WsDataFrameType.CheckResult,
        ).then(() => "resolved", () => "rejected"),
        new Promise<string>((resolve) => setTimeout(() => resolve("timed-out"), 250)),
    ]);
    ok(outcome === "rejected", "malformed ERROR left its pending Promise unresolved");
    ok(lane.getState() === "backoff", "malformed ERROR did not close the protocol session");
    lane.close();
}

async function socketLossRejectsUnknownResultAndReconnectsFresh(): Promise<void> {
    const sockets: FakeSocket[] = [];
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "desktop",
        advertisedPayloadBytes: 4 * 1024 * 1024,
        openSession,
        backoffStartMs: 1,
        socketFactory: () => {
            const socket = new FakeSocket(sockets.length === 0 ? "drop-rpc" : "echo");
            sockets.push(socket);
            return socket;
        },
    });
    let rejected = false;
    try {
        await lane.request(
            WsDataFrameType.PutPack,
            new Uint8Array([7]),
            WsDataFrameType.PutAck,
        );
    } catch (error) {
        rejected = error instanceof WsDataUnavailableError;
    }
    ok(rejected, "socket loss did not make the unacked result unknown");
    ok(lane.getState() === "backoff", "socket loss did not enter bounded backoff");
    await new Promise<void>((resolve) => setTimeout(resolve, 3));
    const result = await lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([9]),
        WsDataFrameType.CheckResult,
    );
    ok(result[0] === 9, "fresh session did not recover after socket loss");
    ok(sockets.length === 2, "reconnect reused the failed socket/session");
    lane.close();
}

async function liveTelemetryTracksCreditAckAndSessionReuse(): Promise<void> {
    let now = 0;
    const trace = new PerfTrace({ monotonicNow: () => now, monitorEventLoop: false });
    let socket: FakeSocket | null = null;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "desktop",
        advertisedPayloadBytes: 4096,
        openSession,
        localRequestLimit: () => 1,
        socketFactory: () => {
            socket = new FakeSocket("hold", {
                maxInflightRequests: 2, maxPayloadBytes: 1024, maxInflightBytes: 4096,
            });
            return socket;
        },
    });
    try {
        const first = trace.begin("push");
        const second = trace.begin("pull");
        const firstRequest = lane.request(
            WsDataFrameType.PutPack, new Uint8Array([1]), WsDataFrameType.PutAck, first);
        const secondRequest = lane.request(
            WsDataFrameType.GetPack, new Uint8Array([2]), WsDataFrameType.GetResult, second);
        await eventually(() => socket?.rpcFrames.length === 1, "first RPC did not send");
        now = 120;
        const active = trace.activeSnapshots();
        ok(active[0].activePhases.some(p => p.name === "ws_ack_wait"), "sent RPC lacks live ACK wait");
        ok(active[1].activePhases.some(p => p.name === "ws_credit_wait"), "queued RPC lacks live credit wait");
        socket!.releaseHeldInReverse();
        await firstRequest;
        first.finish();
        await eventually(() => socket?.rpcFrames.length === 2, "second RPC did not reuse session");
        now = 200;
        ok(trace.activeSnapshots()[0].activePhases.some(p => p.name === "ws_ack_wait"),
            "credit waiter did not transition to ACK wait");
        socket!.releaseHeldInReverse();
        await secondRequest;
        ok(trace.activeSnapshots()[0].activePhases.length === 0, "successful RPC leaked live phases");
        second.finish();
        const records = trace.recent();
        ok(records[1].wsFrameCount === 2, "reused session response attributed to original operation");
        ok(records[1].wireBytesSent > 0 && records[1].wireBytesReceived > 0,
            "reused session wire bytes missing");
        ok(records[1].phases.ws_credit_wait === 120, "credit wait duration incorrect");
        ok(records[1].phases.ws_ack_wait === 80, "ACK wait duration incorrect");
        ok(records[1].phases.decrypt !== undefined, "decrypt duration missing from RPC owner");

        const cancelled = trace.begin("push");
        const result = lane.request(
            WsDataFrameType.PutPack, new Uint8Array([3]), WsDataFrameType.PutAck, cancelled)
            .then(() => false, () => true);
        await eventually(() => socket?.rpcFrames.length === 3, "closing RPC did not send");
        lane.close();
        ok(await result, "socket close did not reject request");
        ok(trace.activeSnapshots()[0].activePhases.length === 0, "socket close leaked ACK/queue phase");
        cancelled.finish("cancelled");
    } finally {
        lane.close();
    }
}

async function closeUnderBufferPressureClearsLiveWaitImmediately(): Promise<void> {
    const trace = new PerfTrace({ monitorEventLoop: false });
    const perf = trace.begin("push");
    const receiveBudget = new ResourceBudget({ capacityBytes: 4096 });
    let socket: FakeSocket | null = null;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182", runtime: "desktop", advertisedPayloadBytes: 4096,
        openSession,
        memoryBudget: receiveBudget,
        socketFactory: () => {
            socket = new FakeSocket("echo", {
                maxInflightRequests: 1, maxPayloadBytes: 1024, maxInflightBytes: 4096,
            });
            socket.bufferedAmount = 8192;
            return socket;
        },
    });
    const result = lane.request(
        WsDataFrameType.PutPack, new Uint8Array([1]), WsDataFrameType.PutAck, perf)
        .then(() => false, () => true);
    try {
        await eventually(() => trace.activeSnapshots()[0].activePhases.some(
            p => p.name === "ws_buffer_wait"), "buffer pressure did not become visible");
        lane.close();
        ok(await result, "close under buffer pressure did not reject RPC");
        // No extra timer turn: a hidden tab may not run that timer for a minute.
        ok(trace.activeSnapshots()[0].activePhases.length === 0,
            "closed socket retained a live buffer wait until its timer resumed");
        ok(socket!.rpcFrames.length === 0, "buffered request was sent after closure");
        ok(receiveBudget.snapshot().usedBytes === 0, "close leaked receive reservation");
    } finally {
        lane.close();
        perf.finish("cancelled");
    }
}

async function oldHandshakeDecryptCannotReplaceNewSession(): Promise<void> {
    const sockets: FakeSocket[] = [];
    const barriers: Array<{ started: boolean; release?: () => void }> = [
        { started: false }, { started: false },
    ];
    let sessionIndex = 0;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182", runtime: "desktop", advertisedPayloadBytes: 4096,
        backoffStartMs: 1,
        openSession: async () => {
            const barrier = barriers[sessionIndex++];
            return {
                ticket: "aa".repeat(32),
                session: {
                    sealBytes: async (bytes: Uint8Array) => bytes.slice(),
                    openBytes: async (bytes: Uint8Array) => {
                        const frame = decodeWsDataFrame(bytes, 4096);
                        if (frame.type === WsDataFrameType.HelloAck) {
                            barrier.started = true;
                            await new Promise<void>(resolve => { barrier.release = resolve; });
                        }
                        return bytes.slice();
                    },
                },
            };
        },
        socketFactory: () => {
            const socket = new FakeSocket("echo", {
                maxInflightRequests: 1,
                maxPayloadBytes: sockets.length === 0 ? 1024 : 2048,
                maxInflightBytes: 8192,
            });
            sockets.push(socket);
            return socket;
        },
    });
    try {
        const first = lane.request(WsDataFrameType.CheckObjects,
            new Uint8Array([1]), WsDataFrameType.CheckResult).then(() => false, () => true);
        await eventually(() => barriers[0].started, "first handshake did not reach decrypt");
        sockets[0].onerror?.();
        ok(await first, "failed handshake did not reject first RPC");
        await new Promise<void>(resolve => setTimeout(resolve, 3));
        const second = lane.request(WsDataFrameType.CheckObjects,
            new Uint8Array([2]), WsDataFrameType.CheckResult)
            .then(value => ({ value, error: null }), error => ({ value: null, error }));
        await eventually(() => barriers[1].started, "new handshake did not reach decrypt");
        barriers[0].release!();
        await new Promise<void>(resolve => setTimeout(resolve, 0));
        ok(lane.getState() === "connecting" && lane.getLimits() === null,
            "old decrypted HELLO_ACK changed the new session state");
        barriers[1].release!();
        const outcome = await second;
        ok(outcome.error === null && outcome.value?.[0] === 2, "new session failed after stale decrypt");
        ok(lane.getLimits()?.maxPayloadBytes === 2048, "new session inherited stale limits");
    } finally {
        for (const barrier of barriers) barrier.release?.();
        lane.close();
    }
}

class HeldNativeSession extends IdentitySession {
    holdSeal = false;
    holdOpen = false;
    started = false;
    opens = 0;
    release: (() => void) | null = null;
    private async wait(): Promise<void> {
        this.started = true;
        await new Promise<void>((resolve) => { this.release = resolve; });
    }
    override async sealBytes(bytes: Uint8Array): Promise<Uint8Array> {
        if (this.holdSeal) { this.holdSeal = false; await this.wait(); }
        return super.sealBytes(bytes);
    }
    override async openBytes(bytes: Uint8Array): Promise<Uint8Array> {
        this.opens++;
        if (this.holdOpen) { this.holdOpen = false; await this.wait(); }
        return super.openBytes(bytes);
    }
}

async function scopedTimeoutKeepsActualNativeWorkCharged(phase: "encrypt" | "decrypt"): Promise<void> {
    const session = new HeldNativeSession();
    let socket!: FakeSocket;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182", runtime: "mobile", advertisedPayloadBytes: 2 * 1024 * 1024,
        requestTimeoutMs: 10,
        openSession: async () => ({ ticket: "memory", session }),
        socketFactory: () => (socket = new FakeSocket()),
    });
    await lane.request(WsDataFrameType.CheckObjects, new Uint8Array([1]), WsDataFrameType.CheckResult);
    const budget = new ResourceBudget({ capacityBytes: 256 });
    const memory = await reserveTransientScope({ ownerBytes: 128, workBytes: 128 }, { budget });
    session.holdSeal = phase === "encrypt";
    session.holdOpen = phase === "decrypt";
    let settled = false;
    const request = memory.run(128, (work) => lane.request(WsDataFrameType.CheckObjects,
        new Uint8Array([2]), WsDataFrameType.CheckResult, undefined, work, 1024))
        .then(() => { settled = true; return ""; }, (error) => {
            settled = true;
            return error instanceof Error ? error.message : String(error);
        });
    try {
        await eventually(() => session.started, `${phase} did not start`);
        await eventually(() => lane.getState() === "backoff", `${phase} request did not time out`);
        ok(!settled, `${phase} timeout released a scoped request before native completion`);
        ok(memory.snapshot().work.usedBytes === 128, `${phase} timeout freed its reusable child quota`);
        memory.close();
        ok(budget.snapshot().usedBytes === 256, `${phase} timeout/close freed the global parent`);
        const opens = session.opens;
        socket.onmessage?.({ data: new ArrayBuffer(1024) });
        ok(session.opens === opens, "late old-session frame reached decryption");
        session.release?.();
        const timeoutMessage = await request;
        ok(timeoutMessage.includes(phase === "encrypt" ? "send phase" : "ACK phase"),
            `${phase} timeout was not attributed to its transport phase`);
        ok(budget.snapshot().usedBytes === 0, `${phase} work did not release after actual completion`);
    } finally {
        session.release?.();
        lane.close();
        memory.close();
    }
}

async function rawReceiveBoundsPrecedeDecryptAndQueueRetention(): Promise<void> {
    const session = new HeldNativeSession();
    let socket!: FakeSocket;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182", runtime: "mobile", advertisedPayloadBytes: 2 * 1024 * 1024,
        openSession: async () => ({ ticket: "memory", session }),
        socketFactory: () => (socket = new FakeSocket("hold")),
    });
    const request = lane.request(WsDataFrameType.GetPack, new Uint8Array([1]),
        WsDataFrameType.GetResult, undefined, undefined, 1024).then(() => false, () => true);
    await eventually(() => socket?.rpcFrames.length === 1, "bounded request did not send");
    const opens = session.opens;
    socket.onmessage?.({ data: new ArrayBuffer(1024 + 18 + 28 + 1) });
    ok(await request, "oversized raw frame did not reject the lane");
    ok(session.opens === opens, "oversized raw frame allocated decrypted plaintext");
    lane.close();

    const delayed = new HeldNativeSession();
    let secondSocket!: FakeSocket;
    const second = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182", runtime: "mobile", advertisedPayloadBytes: 2 * 1024 * 1024,
        openSession: async () => ({ ticket: "memory", session: delayed }),
        socketFactory: () => (secondSocket = new FakeSocket("hold")),
    });
    let pendingSettled = false;
    const pending = second.request(WsDataFrameType.GetPack, new Uint8Array([1]),
        WsDataFrameType.GetResult, undefined, undefined, 1024).then(
        () => { pendingSettled = true; return false; },
        () => { pendingSettled = true; return true; },
    );
    await eventually(() => secondSocket?.rpcFrames.length === 1, "queue-bound request did not send");
    delayed.holdOpen = true;
    const frame = encodeWsDataFrame(WsDataFrameType.GetResult, secondSocket.rpcFrames[0].requestId,
        new Uint8Array(1024), 1024);
    secondSocket.onmessage?.({ data: frame.buffer });
    await eventually(() => delayed.started, "receive decrypt did not start");
    const previousOpens = delayed.opens;
    secondSocket.onmessage?.({ data: frame.buffer });
    await eventually(() => second.getState() === "backoff", "duplicate receive did not close lane");
    ok(!pendingSettled, "receive overflow released ownership while decrypt was live");
    delayed.release?.();
    ok(await pending, "unbounded duplicate receive queue was accepted");
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    ok(delayed.opens === previousOpens, "overflow frame entered the crypto queue");
    second.close();
}

async function phaseTimeoutsBoundCreditAndAckIndependently(): Promise<void> {
    let creditSocket!: FakeSocket;
    const creditLane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "mobile",
        advertisedPayloadBytes: 4096,
        openSession,
        socketFactory: () => (creditSocket = new FakeSocket("hold", {
            maxInflightRequests: 1,
            maxPayloadBytes: 4096,
            maxInflightBytes: 4096,
        })),
    });
    const first = creditLane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([1]),
        WsDataFrameType.CheckResult,
    );
    await eventually(() => creditSocket?.rpcFrames.length === 1, "credit owner did not send");
    const controller = new AbortController();
    const cancelledWaiter = creditLane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([9]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        undefined,
        { creditTimeoutMs: 1_000, sendTimeoutMs: 1_000, ackTimeoutMs: 1_000 },
        controller.signal,
    ).then(() => false, () => true);
    await eventually(() => (creditLane as any).creditWaiters.length === 1,
        "cancellable request did not reach credit wait");
    controller.abort();
    ok(await cancelledWaiter, "credit waiter ignored caller cancellation");
    ok((creditLane as any).creditWaiters.length === 0,
        "cancelled credit waiter retained queue ownership");
    ok(creditSocket.rpcFrames.length === 1, "cancelled credit waiter reached the wire");
    ok(creditSocket.cancelFrames.length === 0, "unsent credit waiter emitted CANCEL");
    let creditError = "";
    await creditLane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([2]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        undefined,
        { creditTimeoutMs: 1, sendTimeoutMs: 1_000, ackTimeoutMs: 1_000 },
    ).catch((error) => { creditError = error instanceof Error ? error.message : String(error); });
    ok(creditError.includes("credit phase timed out"),
        "credit deadline was not classified by phase");
    ok(creditLane.getState() === "ready", "credit timeout poisoned the usable socket");
    ok(creditSocket.rpcFrames.length === 1, "timed-out credit waiter sent a late request");
    creditSocket.releaseHeldInReverse();
    await first;
    creditLane.close();

    let ackSocket!: FakeSocket;
    const ackBudget = new ResourceBudget({ capacityBytes: 4096 });
    const ackLane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "mobile",
        advertisedPayloadBytes: 4096,
        openSession,
        memoryBudget: ackBudget,
        socketFactory: () => (ackSocket = new FakeSocket("hold", {
            maxInflightRequests: 2,
            maxPayloadBytes: 4096,
            maxInflightBytes: 4096,
        })),
    });
    let ackError = "";
    await ackLane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([3]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        1,
        { creditTimeoutMs: 1_000, sendTimeoutMs: 1_000, ackTimeoutMs: 50 },
    ).catch((error) => { ackError = error instanceof Error ? error.message : String(error); });
    ok(ackSocket.rpcFrames.length === 1, "ACK timeout fired before socket send");
    ok(ackError.includes("ACK phase timed out"),
        "ACK deadline was not classified by phase");
    ok(ackLane.getState() === "backoff", "ACK loss did not trip reconnect backoff");
    ok(ackBudget.snapshot().usedBytes === 0, "ACK timeout leaked receive reservation");
    ackLane.close();
}

async function callerCancellationPropagatesOnlyAfterSend(): Promise<void> {
    const receiveBudget = new ResourceBudget({ capacityBytes: 4096 });
    let socket!: FakeSocket;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "mobile",
        advertisedPayloadBytes: 4096,
        openSession,
        memoryBudget: receiveBudget,
        socketFactory: () => (socket = new FakeSocket("cancel-ack", {
            maxInflightRequests: 2,
            maxPayloadBytes: 4096,
            maxInflightBytes: 4096,
        })),
    });
    const controller = new AbortController();
    const request = lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([7]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        1,
        { creditTimeoutMs: 1_000, sendTimeoutMs: 1_000, ackTimeoutMs: 1_000 },
        controller.signal,
    ).then(() => "success", (error) => error instanceof Error ? error.name : String(error));
    await eventually(() => socket?.rpcFrames.length === 1, "cancellable RPC did not send");
    ok(receiveBudget.snapshot().usedBytes > 0,
        "initial receive allocation was not reserved before socket send");
    controller.abort();
    ok(await request === "WsDataCancelledError", "caller cancellation identity was lost");
    ok(socket.cancelFrames.length === 1, "sent RPC did not emit one CANCEL frame");
    ok(socket.cancelFrames[0].requestId === socket.rpcFrames[0].requestId,
        "CANCEL did not name the original request");
    ok(socket.cancelFrames[0].payload.byteLength === 0, "CANCEL carried an unexpected payload");
    ok(receiveBudget.snapshot().usedBytes === 0, "cancel ACK did not release receive reservation");
    ok(lane.getState() === "ready", "terminal cancel poisoned the shared lane");

    const next = await lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([8]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        1,
    );
    ok(next[0] === 8, "lane did not accept work after terminal cancellation");
    lane.close();
}

async function invalidCancelledTerminalClosesLane(): Promise<void> {
    for (const mode of ["cancel-mismatch", "cancel-oversize"] as const) {
        const receiveBudget = new ResourceBudget({ capacityBytes: 4096 });
        let socket!: FakeSocket;
        const lane = new ObsetyncWsDataLane({
            baseUrl: "http://server:27182",
            runtime: "mobile",
            advertisedPayloadBytes: 4096,
            openSession,
            memoryBudget: receiveBudget,
            socketFactory: () => (socket = new FakeSocket(mode, {
                maxInflightRequests: 2,
                maxPayloadBytes: 4096,
                maxInflightBytes: 4096,
            })),
        });
        const controller = new AbortController();
        const request = lane.request(
            WsDataFrameType.CheckObjects,
            new Uint8Array([7]),
            WsDataFrameType.CheckResult,
            undefined,
            undefined,
            1,
            { creditTimeoutMs: 1_000, sendTimeoutMs: 1_000, ackTimeoutMs: 1_000 },
            controller.signal,
        ).then(() => "success", (error) => error instanceof Error ? error.name : String(error));
        await eventually(() => socket?.rpcFrames.length === 1,
            `${mode} cancellable RPC did not send`);
        controller.abort();
        ok(await request === "WsDataCancelledError", `${mode} lost caller cancellation identity`);
        ok(socket.cancelFrames.length === 1, `${mode} did not send CANCEL`);
        ok(lane.getState() === "backoff", `${mode} terminal response did not close the lane`);
        ok(receiveBudget.snapshot().usedBytes === 0, `${mode} leaked receive reservation`);
        lane.close();
    }
}

async function cancelledResponseKeepsLeaseThroughDecryptFinalizer(): Promise<void> {
    const receiveBudget = new ResourceBudget({ capacityBytes: 4096 });
    const session = new HeldNativeSession();
    let socket!: FakeSocket;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "mobile",
        advertisedPayloadBytes: 4096,
        openSession: async () => ({ ticket: "cancel-decrypt", session }),
        memoryBudget: receiveBudget,
        socketFactory: () => (socket = new FakeSocket("cancel-ack", {
            maxInflightRequests: 2,
            maxPayloadBytes: 4096,
            maxInflightBytes: 4096,
        })),
    });
    const controller = new AbortController();
    let settled = false;
    const request = lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([9]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        1,
        { creditTimeoutMs: 1_000, sendTimeoutMs: 1_000, ackTimeoutMs: 1_000 },
        controller.signal,
    ).then(() => {
        settled = true;
        return "success";
    }, (error) => {
        settled = true;
        return error instanceof Error ? error.name : String(error);
    });
    await eventually(() => socket?.rpcFrames.length === 1, "decrypt cancellation RPC did not send");
    session.started = false;
    session.holdOpen = true;
    controller.abort();
    await eventually(() => session.started, "cancel terminal decrypt did not start");
    // A concurrent close resolves the tombstone drain promise. The actual
    // receive task must still keep the request's memory ownership alive.
    socket.onerror?.();
    ok(!settled, "cancelled request settled while terminal decrypt still owned buffers");
    ok(receiveBudget.snapshot().usedBytes > 0,
        "cancelled terminal decrypt released its receive reservation early");
    session.release?.();
    ok(await request === "WsDataCancelledError", "held terminal decrypt lost cancellation identity");
    ok(receiveBudget.snapshot().usedBytes === 0,
        "terminal decrypt finalizer did not release receive reservation");
    ok(lane.getState() === "backoff", "concurrent close did not retire the held lane");
    lane.close();
}

async function cancellationDuringSealPreservesV1Sequence(): Promise<void> {
    const receiveBudget = new ResourceBudget({ capacityBytes: 4096 });
    const session = new HeldNativeSession();
    let socket!: FakeSocket;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "mobile",
        advertisedPayloadBytes: 4096,
        openSession: async () => ({ ticket: "cancel-before-send", session }),
        memoryBudget: receiveBudget,
        socketFactory: () => (socket = new FakeSocket("echo", {
            maxInflightRequests: 2,
            maxPayloadBytes: 4096,
            maxInflightBytes: 4096,
        })),
    });
    await lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([1]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        1,
    );
    const before = socket.rpcFrames.length;
    session.started = false;
    session.holdSeal = true;
    const controller = new AbortController();
    const request = lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([2]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        1,
        { creditTimeoutMs: 1_000, sendTimeoutMs: 1_000, ackTimeoutMs: 1_000 },
        controller.signal,
    ).then(() => false, () => true);
    await eventually(() => session.started, "queued encrypt did not start");
    controller.abort();
    ok(receiveBudget.snapshot().usedBytes > 0,
        "cancel released receive ownership while native encrypt was live");
    session.release?.();
    ok(await request, "pre-send cancellation unexpectedly succeeded");
    ok(socket.rpcFrames.length === before + 1,
        "V1 ciphertext that consumed a sequence was not sent before CANCEL");
    ok(socket.cancelFrames.length === 1,
        "V1 seal-race cancellation did not emit its contiguous CANCEL");
    ok(receiveBudget.snapshot().usedBytes === 0, "seal-race cleanup leaked receive reservation");
    ok(lane.getState() === "ready", "V1 seal-race cancellation closed a healthy lane");
    lane.close();
}

async function v2FragmentsRoundTripWithinBoundedOwnership(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 2 * 1024 * 1024 });
    let socket!: V2FakeSocket;
    const opened: Array<number | undefined> = [];
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "mobile",
        advertisedPayloadBytes: 2 * 1024 * 1024,
        preferredWireVersion: 2,
        memoryBudget: budget,
        openSession: async (wire) => {
            opened.push(wire);
            return { ticket: `v${wire}`, session: new IdentitySession() };
        },
        socketFactory: () => (socket = new V2FakeSocket()),
    });
    const payload = new Uint8Array(2 * 64 * 1024 + 7);
    payload[0] = 1;
    payload[64 * 1024] = 2;
    payload[payload.length - 1] = 3;
    const response = await lane.request(
        WsDataFrameType.CheckObjects,
        payload,
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        payload.byteLength,
    );
    ok(opened.join(",") === "2", "OBW2 used more than one fresh session");
    ok(lane.getWireVersion() === 2, "OBW2 negotiation did not become active");
    ok(socket.rpcFragments.length === 3, "logical request was not split into 64 KiB fragments");
    ok(socket.rpcFragments.map(fragment => fragment.fragmentIndex).join(",") === "0,1,2",
        "OBW2 request fragments were not canonical and ordered");
    ok(response.byteLength === payload.byteLength && response[0] === 1 &&
        response[64 * 1024] === 2 && response[response.length - 1] === 3,
    "OBW2 response reassembly changed bytes");
    ok(budget.snapshot().usedBytes === 0, "OBW2 round trip leaked transient ownership");
    lane.close();
}

async function v2CancelPreemptsRemainingBulkFragments(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 2 * 1024 * 1024 });
    let socket!: V2FakeSocket;
    const controller = new AbortController();
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "mobile",
        advertisedPayloadBytes: 2 * 1024 * 1024,
        preferredWireVersion: 2,
        memoryBudget: budget,
        openSession: async () => ({ ticket: "priority", session: new IdentitySession() }),
        socketFactory: () => {
            socket = new V2FakeSocket();
            socket.onFirstRpcFragment = () => controller.abort();
            return socket;
        },
    });
    const payload = new Uint8Array(3 * 64 * 1024);
    const outcome = await lane.request(
        WsDataFrameType.PutPack,
        payload,
        WsDataFrameType.PutAck,
        undefined,
        undefined,
        1024,
        { creditTimeoutMs: 1_000, sendTimeoutMs: 1_000, ackTimeoutMs: 1_000 },
        controller.signal,
    ).then(() => "success", error => error instanceof Error ? error.name : String(error));
    ok(outcome === "WsDataCancelledError", "OBW2 priority cancellation identity changed");
    ok(socket.rpcFragments.length === 1, "bulk fragments passed a queued OBW2 CANCEL");
    ok(socket.cancelFragments.length === 1, "OBW2 CANCEL was not sent between bulk fragments");
    ok(socket.cancelFragments[0].payload.byteLength === 0 &&
        socket.cancelFragments[0].fragmentCount === 1,
    "OBW2 CANCEL was not a canonical zero-length control fragment");
    ok(budget.snapshot().usedBytes === 0, "OBW2 cancellation leaked transient ownership");
    lane.close();
}

async function cancelDuringSealSendsContiguousBoundaryThenCancel(): Promise<void> {
    const sequences: number[] = [];
    const controller = new AbortController();
    let socket!: V2FakeSocket;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "mobile",
        advertisedPayloadBytes: 2 * 1024 * 1024,
        preferredWireVersion: 2,
        openSession: async () => ({
            ticket: "stateful-cancel-seal",
            session: new StatefulSealSession(() => controller.abort()),
        }),
        socketFactory: () => (socket = new V2FakeSocket(
            false,
            false,
            contiguousSequenceOpener(sequences),
        )),
    });
    const outcome = await lane.request(
        WsDataFrameType.PutPack,
        new Uint8Array(2 * 64 * 1024),
        WsDataFrameType.PutAck,
        undefined,
        undefined,
        1024,
        { creditTimeoutMs: 1_000, sendTimeoutMs: 1_000, ackTimeoutMs: 1_000 },
        controller.signal,
    ).then(() => "success", error => error instanceof Error ? error.name : String(error));
    ok(outcome === "WsDataCancelledError", "seal-race cancellation identity changed");
    ok(sequences.join(",") === "0,1,2",
        `seal-race created an outbound AEAD sequence gap: ${sequences.join(",")}`);
    ok(socket.rpcFragments.length === 1 && socket.cancelFragments.length === 1,
        "sealed boundary and following CANCEL were not both delivered");
    const recovered = await lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([9]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        1,
    );
    ok(recovered[0] === 9 && sequences.join(",") === "0,1,2,3",
        "same socket did not remain sequence-contiguous after seal-race cancellation");
    ok(lane.getState() === "ready", "seal-race cancellation poisoned the healthy socket");
    lane.close();
}

async function closeDuringSealDropsStaleCiphertextAndReconnectsFresh(): Promise<void> {
    let now = 0;
    let opened = 0;
    let firstSocket!: V2FakeSocket;
    const firstSequences: number[] = [];
    const secondSequences: number[] = [];
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "mobile",
        advertisedPayloadBytes: 2 * 1024 * 1024,
        preferredWireVersion: 2,
        now: () => now,
        backoffStartMs: 1,
        openSession: async () => {
            opened++;
            return {
                ticket: `stateful-close-${opened}`,
                session: new StatefulSealSession(() => {
                    if (opened !== 1) return;
                    firstSocket.readyState = 3;
                    firstSocket.onclose?.();
                }),
            };
        },
        socketFactory: () => {
            if (opened === 1) {
                firstSocket = new V2FakeSocket(
                    false,
                    false,
                    contiguousSequenceOpener(firstSequences),
                );
                return firstSocket;
            }
            return new V2FakeSocket(
                false,
                false,
                contiguousSequenceOpener(secondSequences),
            );
        },
    });
    const failed = await lane.request(
        WsDataFrameType.PutPack,
        new Uint8Array(2 * 64 * 1024),
        WsDataFrameType.PutAck,
        undefined,
        undefined,
        1024,
    ).then(() => false, () => true);
    ok(failed, "socket loss during seal unexpectedly completed the RPC");
    ok(firstSequences.join(",") === "0",
        "ciphertext sealed by a retired session was sent on its stale socket");

    now = 2;
    const recovered = await lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([7]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        1,
    );
    ok(recovered[0] === 7 && opened === 2,
        "seal-race socket loss did not reconnect with a fresh session");
    ok(secondSequences.join(",") === "0,1",
        "replacement session did not restart its independent AEAD sequence");
    lane.close();
}

async function v2CheckPreemptsBulkAtBoundaryWithoutStarvingIt(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 8 * 1024 * 1024 });
    let socket!: V2FakeSocket;
    let checkRequest: Promise<Uint8Array> | undefined;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "desktop",
        advertisedPayloadBytes: 2 * 1024 * 1024,
        preferredWireVersion: 2,
        memoryBudget: budget,
        openSession: async () => ({ ticket: "semantic-priority", session: new IdentitySession() }),
        socketFactory: () => {
            socket = new V2FakeSocket();
            socket.onFirstRpcFragment = () => {
                // Hold the next boundary long enough for the concurrent
                // request continuation to enter the semantic scheduler.
                socket.bufferedAmount = 33 * 1024 * 1024;
                checkRequest = lane.request(
                    WsDataFrameType.CheckObjects,
                    new Uint8Array(6 * 64 * 1024),
                    WsDataFrameType.CheckResult,
                    undefined,
                    undefined,
                    6 * 64 * 1024,
                );
                setTimeout(() => { socket.bufferedAmount = 0; }, 0);
            };
            return socket;
        },
    });
    const bulkRequest = lane.request(
        WsDataFrameType.PutPack,
        new Uint8Array(3 * 64 * 1024),
        WsDataFrameType.PutAck,
        undefined,
        undefined,
        3 * 64 * 1024,
    );
    const olderBulkRequest = lane.request(
        WsDataFrameType.GetPack,
        new Uint8Array(2 * 64 * 1024),
        WsDataFrameType.GetResult,
        undefined,
        undefined,
        2 * 64 * 1024,
    );
    await eventually(
        () => socket?.rpcFragments.length >= 8,
        "OBW2 semantic scheduler did not emit enough adversarial fragments",
    );
    const order = socket.rpcFragments.slice(0, 8)
        .map(fragment => `${fragment.requestId}:${fragment.fragmentIndex}`)
        .join(",");
    ok(order === "1:0,2:0,3:0,3:1,3:2,3:3,1:1,3:4",
        `OBW2 semantic priority/fairness order changed: ${order}`);
    ok(socket.rpcFragments[2].type === WsDataFrameType.CheckObjects,
        "CheckObjects did not overtake the remaining bulk fragments");
    ok(socket.rpcFragments[6].type === WsDataFrameType.PutPack,
        "bounded control burst starved the bulk logical message");
    ok(socket.rpcFragments.filter(fragment => fragment.fragmentIndex === 0)
        .map(fragment => fragment.requestId).slice(0, 3).join(",") === "1,2,3",
    "semantic priority reordered canonical first request fragments");
    await Promise.all([bulkRequest, olderBulkRequest, checkRequest!]);
    ok(socket.rpcFragments.filter(fragment => fragment.requestId === 1).length === 3,
        "preempted bulk request did not finish every fragment");
    ok(budget.snapshot().usedBytes === 0, "semantic priority leaked transient ownership");
    lane.close();
}

async function v2UrgentFilePreemptsAtBoundaryButCannotStarveBulk(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 8 * 1024 * 1024 });
    let socket!: V2FakeSocket;
    const urgent: Array<Promise<Uint8Array>> = [];
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "desktop",
        advertisedPayloadBytes: 2 * 1024 * 1024,
        preferredWireVersion: 2,
        memoryBudget: budget,
        openSession: async () => ({ ticket: "urgent-file-priority", session: new IdentitySession() }),
        socketFactory: () => {
            socket = new V2FakeSocket(false, false, wire => wire, 8);
            socket.onFirstRpcFragment = () => {
                socket.bufferedAmount = 33 * 1024 * 1024;
                for (let index = 0; index < 5; index++) {
                    urgent.push(lane.request(
                        WsDataFrameType.PutPack,
                        new Uint8Array([index + 1]),
                        WsDataFrameType.PutAck,
                        undefined,
                        undefined,
                        1,
                        undefined,
                        undefined,
                        { priority: "urgent-file" },
                    ));
                }
                setTimeout(() => { socket.bufferedAmount = 0; }, 0);
            };
            return socket;
        },
    });
    const bulk = lane.request(
        WsDataFrameType.PutPack,
        new Uint8Array(3 * 64 * 1024),
        WsDataFrameType.PutAck,
        undefined,
        undefined,
        3 * 64 * 1024,
    );
    await eventually(() => socket?.rpcFragments.length >= 7,
        "urgent-file scheduler did not emit the adversarial prefix");
    const order = socket.rpcFragments.slice(0, 7)
        .map(fragment => `${fragment.requestId}:${fragment.fragmentIndex}`).join(",");
    ok(order === "1:0,2:0,3:0,4:0,5:0,1:1,6:0",
        `urgent-file priority/bounded burst changed: ${order}`);
    ok(socket.rpcFragments[1].type === WsDataFrameType.PutPack,
        "urgent-file PutPack did not overtake the fragmented bulk tail");
    ok(socket.rpcFragments[5].requestId === 1,
        "urgent-file control burst starved the bulk request");
    await Promise.all([bulk, ...urgent]);
    ok(socket.rpcFragments.filter(fragment => fragment.requestId === 1).length === 3,
        "preempted bulk request did not complete");
    ok(budget.snapshot().usedBytes === 0, "urgent-file priority leaked transient ownership");
    lane.close();
}

async function v2CancelReplacesPartialNormalResponseWithError(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 2 * 1024 * 1024 });
    let socket!: V2FakeSocket;
    const controller = new AbortController();
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "mobile",
        advertisedPayloadBytes: 2 * 1024 * 1024,
        preferredWireVersion: 2,
        memoryBudget: budget,
        openSession: async () => ({ ticket: "partial-cancel", session: new IdentitySession() }),
        socketFactory: () => (socket = new V2FakeSocket(false, true)),
    });
    const payload = new Uint8Array(64 * 1024 + 1);
    const request = lane.request(
        WsDataFrameType.CheckObjects,
        payload,
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        payload.byteLength,
        { creditTimeoutMs: 1_000, sendTimeoutMs: 1_000, ackTimeoutMs: 1_000 },
        controller.signal,
    ).then(() => "success", error => error instanceof Error ? error.name : String(error));
    await eventually(() => {
        const pending = [...((lane as any).pending as Map<number, any>).values()][0];
        return pending?.response?.nextFragmentIndex === 1;
    }, "OBW2 partial normal response was not retained");
    controller.abort();
    ok(await request === "WsDataCancelledError", "partial OBW2 response lost cancellation identity");
    ok(socket.cancelFragments.length === 1, "partial OBW2 response did not send CANCEL");
    ok(lane.getState() === "ready", "cancel Error could not replace partial normal response");
    ok(budget.snapshot().usedBytes === 0, "partial response cancellation leaked ownership");
    // A conforming server retires this tail. Keep the fixture explicit so a
    // future test can inject it as a protocol violation if needed.
    socket.releaseResponseTail();
    lane.close();
}

async function rejectedV2HelloDowngradesWithFreshV1Session(): Promise<void> {
    const versions: Array<number | undefined> = [];
    let socketCount = 0;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "mobile",
        advertisedPayloadBytes: 4096,
        preferredWireVersion: 2,
        openSession: async (wire) => {
            versions.push(wire);
            return { ticket: `fresh-${versions.length}`, session: new IdentitySession() };
        },
        socketFactory: () => socketCount++ === 0 ? new RejectV2Socket() : new FakeSocket("echo", {
            maxInflightRequests: 2,
            maxPayloadBytes: 4096,
            maxInflightBytes: 4096,
        }),
    });
    const response = await lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([4]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        1,
    );
    ok(response[0] === 4, "fresh OBW1 fallback did not complete the RPC");
    ok(versions.join(",") === "2,1", "OBW2 downgrade reused a ticket/session");
    ok(socketCount === 2, "OBW2 downgrade did not create a fresh socket");
    ok(lane.getWireVersion() === 1, "OBW2 rejection did not pin explicit OBW1 fallback");
    lane.close();
}

async function rejectedSessionMintDowngradesWithoutLeakingAttempt(): Promise<void> {
    const versions: Array<number | undefined> = [];
    let sockets = 0;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "mobile",
        advertisedPayloadBytes: 4096,
        preferredWireVersion: 2,
        openSession: (wire) => {
            versions.push(wire);
            if (wire === 2) throw new Error("unsupported session mint");
            return Promise.resolve({ ticket: "fresh-v1", session: new IdentitySession() });
        },
        socketFactory: () => {
            sockets++;
            return new FakeSocket("echo", {
                maxInflightRequests: 2,
                maxPayloadBytes: 4096,
                maxInflightBytes: 4096,
            });
        },
    });
    const response = await lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([9]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        1,
    );
    ok(response[0] === 9, "synchronous OBW2 session rejection blocked fresh OBW1 fallback");
    ok(versions.join(",") === "2,1", "session rejection did not use explicit fresh fallback");
    ok(sockets === 1 && lane.getWireVersion() === 1,
        "rejected session mint leaked a stale socket attempt");
    lane.close();
}

async function sessionMintTimeoutCannotCreateLateSocketABA(): Promise<void> {
    const lateV2 = deferred<{ ticket: string; session: IdentitySession }>();
    const versions: Array<number | undefined> = [];
    let openingWire: number | undefined;
    let now = 0;
    const sockets: FakeSocket[] = [];
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "mobile",
        advertisedPayloadBytes: 4096,
        preferredWireVersion: 2,
        connectTimeoutMs: 5,
        now: () => now,
        v2ReprobeBaseMs: 1,
        v2ReprobeMaxMs: 1,
        openSession: (wire) => {
            openingWire = wire;
            versions.push(wire);
            if (wire === 2) return lateV2.promise;
            return Promise.resolve({ ticket: "fresh-v1", session: new IdentitySession() });
        },
        socketFactory: () => {
            if (openingWire !== 1) throw new Error("timed-out OBW2 mint created a socket");
            const socket = new FakeSocket("echo", {
                maxInflightRequests: 2,
                maxPayloadBytes: 4096,
                maxInflightBytes: 4096,
            });
            sockets.push(socket);
            return socket;
        },
    });
    const first = await lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([7]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        1,
    );
    ok(first[0] === 7 && versions.join(",") === "2,1",
        "session mint timeout did not fall back with a fresh OBW1 session");
    ok(sockets.length === 1 && lane.getWireVersion() === 1,
        "session mint timeout constructed an OBW2 socket");

    now = 100;
    const second = await lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([8]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        1,
    );
    ok(second[0] === 8 && sockets.length === 1 && versions.join(",") === "2,1",
        "detached session mint allowed unbounded duplicate OBW2 probes");
    lateV2.resolve({ ticket: "late-v2", session: new IdentitySession() });
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    ok(sockets.length === 1 && lane.getWireVersion() === 1,
        "late timed-out session replaced the live fallback socket");
    lane.close();
}

async function detachedV1SessionMintKeepsOneBoundedAttempt(): Promise<void> {
    const firstSession = deferred<{ ticket: string; session: IdentitySession }>();
    let openCalls = 0;
    let socketCalls = 0;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "mobile",
        advertisedPayloadBytes: 4096,
        connectTimeoutMs: 5,
        backoffStartMs: 0,
        openSession: () => {
            openCalls++;
            if (openCalls === 1) return firstSession.promise;
            return Promise.resolve({ ticket: "fresh-after-stale", session: new IdentitySession() });
        },
        socketFactory: () => {
            socketCalls++;
            return new FakeSocket("echo", {
                maxInflightRequests: 2,
                maxPayloadBytes: 4096,
                maxInflightBytes: 4096,
            });
        },
    });
    const request = (value: number) => lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([value]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        1,
    );
    ok(await request(1).then(() => false, error => error instanceof WsDataUnavailableError),
        "pending OBW1 session mint escaped its connection deadline");
    ok(await request(2).then(() => false, error => error instanceof WsDataUnavailableError),
        "duplicate OBW1 mint was accepted while its detached predecessor remained pending");
    ok(openCalls === 1 && socketCalls === 0,
        "detached OBW1 session mint accumulated another Promise/socket");
    firstSession.resolve({ ticket: "stale-v1", session: new IdentitySession() });
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    const recovered = await request(3);
    ok(recovered[0] === 3 && openCalls === 2 && socketCalls === 1,
        "settled detached mint did not release the bounded attempt slot");
    lane.close();
}

async function closeCancelsPendingSessionMintAndTombstonesLateResult(): Promise<void> {
    const pendingSession = deferred<{ ticket: string; session: IdentitySession }>();
    let openCalls = 0;
    let socketCalls = 0;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "mobile",
        advertisedPayloadBytes: 4096,
        preferredWireVersion: 2,
        connectTimeoutMs: 10_000,
        openSession: () => {
            openCalls++;
            return pendingSession.promise;
        },
        socketFactory: () => {
            socketCalls++;
            return new V2FakeSocket();
        },
    });
    const request = lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([6]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        1,
    ).then(() => false, error => error instanceof WsDataUnavailableError);
    await eventually(() => openCalls === 1, "session mint did not start");
    lane.close();
    ok(await request,
        "close did not immediately reject the pending session mint");
    ok(socketCalls === 0 && lane.getState() === "off",
        "close constructed a socket while session mint was pending");
    pendingSession.resolve({ ticket: "late-after-stop", session: new IdentitySession() });
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    ok(socketCalls === 0 && lane.getState() === "off",
        "late session result resurrected a stopped lane");
}

async function downgradedV1ReprobesV2OnlyAtIdleCooldownBoundary(): Promise<void> {
    let now = 0;
    let openingWire: number | undefined;
    const versions: Array<number | undefined> = [];
    const sessions: IdentitySession[] = [];
    let v1Socket!: FakeSocket;
    let v2Socket!: V2FakeSocket;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "mobile",
        advertisedPayloadBytes: 4096,
        preferredWireVersion: 2,
        now: () => now,
        v2ReprobeBaseMs: 100,
        v2ReprobeMaxMs: 400,
        openSession: async (wire) => {
            openingWire = wire;
            versions.push(wire);
            const session = new IdentitySession();
            sessions.push(session);
            return { ticket: `reprobe-${versions.length}`, session };
        },
        socketFactory: () => {
            if (openingWire === 2 && versions.length === 1) return new RejectV2Socket();
            if (openingWire === 2) return (v2Socket = new V2FakeSocket());
            return (v1Socket = new FakeSocket("hold", {
                maxInflightRequests: 2,
                maxPayloadBytes: 4096,
                maxInflightBytes: 4096,
            }));
        },
    });
    const initial = lane.request(WsDataFrameType.CheckObjects, new Uint8Array([1]),
        WsDataFrameType.CheckResult, undefined, undefined, 1);
    await eventually(() => v1Socket?.rpcFrames.length === 1, "fallback OBW1 request did not send");
    v1Socket.releaseHeldInReverse();
    ok((await initial)[0] === 1, "initial OBW1 fallback failed");
    ok(versions.join(",") === "2,1", "initial downgrade did not use two fresh sessions");

    now = 99;
    const firstBusy = lane.request(WsDataFrameType.CheckObjects, new Uint8Array([2]),
        WsDataFrameType.CheckResult, undefined, undefined, 1);
    await eventually(() => v1Socket.rpcFrames.length === 2, "busy OBW1 request did not send");
    now = 10_000;
    const secondBusy = lane.request(WsDataFrameType.CheckObjects, new Uint8Array([3]),
        WsDataFrameType.CheckResult, undefined, undefined, 1);
    await eventually(() => v1Socket.rpcFrames.length === 3,
        "concurrent request did not remain on active OBW1 lane");
    ok(versions.join(",") === "2,1", "OBW2 re-probe interrupted active OBW1 requests");
    v1Socket.releaseHeldInReverse();
    ok((await firstBusy)[0] === 2 && (await secondBusy)[0] === 3,
        "busy OBW1 responses changed across the cooldown boundary");

    const staleV1Close = v1Socket.onclose;
    const upgrades = await Promise.all([
        lane.request(WsDataFrameType.CheckObjects, new Uint8Array([4]),
            WsDataFrameType.CheckResult, undefined, undefined, 1),
        lane.request(WsDataFrameType.CheckObjects, new Uint8Array([5]),
            WsDataFrameType.CheckResult, undefined, undefined, 1),
    ]);
    ok(upgrades[0][0] === 4 && upgrades[1][0] === 5 && v2Socket.rpcFragments.length === 2,
        "concurrent idle-boundary callers did not complete on one OBW2 lane");
    ok(versions.join(",") === "2,1,2", "OBW2 re-probe did not mint one fresh session");
    ok(new Set(sessions).size === 3, "OBW2 re-probe reused cryptographic session state");
    ok(lane.getWireVersion() === 2, "successful idle re-probe did not upgrade the lane");
    staleV1Close?.();
    const afterStaleClose = await lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([6]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        1,
    );
    ok(afterStaleClose[0] === 6 && lane.getWireVersion() === 2,
        "retired OBW1 close callback ABA-poisoned the replacement lane");
    lane.close();
}

async function repeatedV2ReprobeFailuresUseBoundedExponentialHold(): Promise<void> {
    let now = 0;
    let openingWire: number | undefined;
    const versions: Array<number | undefined> = [];
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "mobile",
        advertisedPayloadBytes: 4096,
        preferredWireVersion: 2,
        now: () => now,
        v2ReprobeBaseMs: 100,
        v2ReprobeMaxMs: 400,
        openSession: async (wire) => {
            openingWire = wire;
            versions.push(wire);
            return { ticket: `old-server-${versions.length}`, session: new IdentitySession() };
        },
        socketFactory: () => openingWire === 2
            ? new RejectV2Socket()
            : new FakeSocket("echo", {
                maxInflightRequests: 2,
                maxPayloadBytes: 4096,
                maxInflightBytes: 4096,
            }),
    });
    const request = (value: number) => lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([value]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        1,
    );
    ok((await request(1))[0] === 1 && versions.join(",") === "2,1",
        "old server did not receive initial fresh OBW1 fallback");
    now = -100;
    ok((await request(8))[0] === 8 && versions.join(",") === "2,1",
        "clock rollback bypassed the monotonic OBW2 hold");
    now = 100;
    ok((await request(2))[0] === 2 && versions.join(",") === "2,1,2,1",
        "first bounded OBW2 re-probe did not fall back freshly");
    now = 299;
    ok((await request(3))[0] === 3 && versions.join(",") === "2,1,2,1",
        "failed OBW2 re-probe ignored its doubled minimum hold");
    now = 300;
    ok((await request(4))[0] === 4 && versions.join(",") === "2,1,2,1,2,1",
        "second OBW2 re-probe did not occur at the exponential boundary");
    now = 699;
    ok((await request(5))[0] === 5 && versions.length === 6,
        "OBW2 re-probe escaped its capped hold interval");
    now = 700;
    ok((await request(6))[0] === 6 && versions.join(",") === "2,1,2,1,2,1,2,1",
        "capped OBW2 re-probe did not retain fresh-session fallback");
    now = 1_099;
    ok((await request(7))[0] === 7 && versions.length === 8,
        "OBW2 re-probe cap collapsed into request-by-request ping-pong");
    ok(lane.getWireVersion() === 1, "old server escaped the explicit OBW1 fallback lane");
    lane.close();
    now = 2_000;
    const stopped = await request(9).then(() => false, () => true);
    ok(stopped && versions.length === 8,
        "stopped cooldown lane started another session probe");
}

async function malformedV2FragmentFailsClosed(): Promise<void> {
    let socket!: V2FakeSocket;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182",
        runtime: "mobile",
        advertisedPayloadBytes: 4096,
        preferredWireVersion: 2,
        openSession: async () => ({ ticket: "malformed-v2", session: new IdentitySession() }),
        socketFactory: () => (socket = new V2FakeSocket(true)),
    });
    const failed = await lane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([5]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        1,
    ).then(() => false, () => true);
    ok(failed, "non-canonical OBW2 response was accepted");
    ok(socket.rpcFragments.length === 1, "malformed test did not receive its request");
    ok(lane.getState() === "backoff", "malformed OBW2 response did not close the lane");
    lane.close();
}

void (async () => {
    await multiplexesByRequestIdAndHonorsCredits();
    await remoteRpcErrorDoesNotPoisonTheSession();
    await localGovernorCapStaysBelowServerCredits();
    await byteCreditsAndSocketWatermarksApplyBackpressure();
    await malformedErrorRejectsPendingWithoutHanging();
    await socketLossRejectsUnknownResultAndReconnectsFresh();
    await liveTelemetryTracksCreditAckAndSessionReuse();
    await closeUnderBufferPressureClearsLiveWaitImmediately();
    await oldHandshakeDecryptCannotReplaceNewSession();
    await scopedTimeoutKeepsActualNativeWorkCharged("encrypt");
    await scopedTimeoutKeepsActualNativeWorkCharged("decrypt");
    await rawReceiveBoundsPrecedeDecryptAndQueueRetention();
    await phaseTimeoutsBoundCreditAndAckIndependently();
    await callerCancellationPropagatesOnlyAfterSend();
    await invalidCancelledTerminalClosesLane();
    await cancelledResponseKeepsLeaseThroughDecryptFinalizer();
    await cancellationDuringSealPreservesV1Sequence();
    await v2FragmentsRoundTripWithinBoundedOwnership();
    await v2CancelPreemptsRemainingBulkFragments();
    await cancelDuringSealSendsContiguousBoundaryThenCancel();
    await closeDuringSealDropsStaleCiphertextAndReconnectsFresh();
    await v2CheckPreemptsBulkAtBoundaryWithoutStarvingIt();
    await v2UrgentFilePreemptsAtBoundaryButCannotStarveBulk();
    await v2CancelReplacesPartialNormalResponseWithError();
    await rejectedV2HelloDowngradesWithFreshV1Session();
    await rejectedSessionMintDowngradesWithoutLeakingAttempt();
    await sessionMintTimeoutCannotCreateLateSocketABA();
    await detachedV1SessionMintKeepsOneBoundedAttempt();
    await closeCancelsPendingSessionMintAndTombstonesLateResult();
    await downgradedV1ReprobesV2OnlyAtIdleCooldownBoundary();
    await repeatedV2ReprobeFailuresUseBoundedExponentialHold();
    await malformedV2FragmentFailsClosed();
    console.log(`ws-data.test: ${assertions} assertions passed`);
})().catch((error) => {
    setTimeout(() => { throw error; }, 0);
});
