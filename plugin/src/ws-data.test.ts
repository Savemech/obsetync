import {
    ObsetyncWsDataLane,
    WsDataRpcError,
    WsDataUnavailableError,
} from "./ws-data";
import {
    WsDataErrorCode,
    WsDataFrameType,
    decodeWsDataFrame,
    encodeWsDataFrame,
} from "./ws-data-codec";
import { PerfTrace } from "./perf-trace";

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
    maxHeld = 0;
    private held: RpcFrame[] = [];

    constructor(
        private readonly mode:
            | "echo"
            | "hold"
            | "drop-rpc"
            | "malformed-error"
            | "remote-error-once" = "echo",
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
    let socket: FakeSocket | null = null;
    const lane = new ObsetyncWsDataLane({
        baseUrl: "http://server:27182", runtime: "desktop", advertisedPayloadBytes: 4096,
        openSession,
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
    console.log(`ws-data.test: ${assertions} assertions passed`);
})().catch((error) => {
    setTimeout(() => { throw error; }, 0);
});
