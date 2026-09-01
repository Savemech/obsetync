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

void (async () => {
    await multiplexesByRequestIdAndHonorsCredits();
    await remoteRpcErrorDoesNotPoisonTheSession();
    await localGovernorCapStaysBelowServerCredits();
    await byteCreditsAndSocketWatermarksApplyBackpressure();
    await malformedErrorRejectsPendingWithoutHanging();
    await socketLossRejectsUnknownResultAndReconnectsFresh();
    console.log(`ws-data.test: ${assertions} assertions passed`);
})().catch((error) => {
    setTimeout(() => { throw error; }, 0);
});
