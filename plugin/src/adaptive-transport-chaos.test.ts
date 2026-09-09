import {
    ObsetyncWsDataLane,
    WsDataUnavailableError,
} from "./ws-data";
import {
    WsDataErrorCode,
    WsDataFrameType,
    decodeWsDataFrame,
    encodeWsDataFrame,
} from "./ws-data-codec";
import {
    TransportRouter,
    type TransportAttemptMeasurements,
    type TransportPhaseTimeouts,
} from "./transport-router";

let assertions = 0;
function ok(condition: unknown, message: string): void {
    assertions++;
    if (!condition) throw new Error(message);
}

async function eventually(predicate: () => boolean, message: string): Promise<void> {
    for (let turn = 0; turn < 50; turn++) {
        if (predicate()) return;
        await new Promise<void>((resolve) => setTimeout(resolve, 0));
    }
    throw new Error(message);
}

async function drainMicrotasks(): Promise<void> {
    for (let turn = 0; turn < 16; turn++) await Promise.resolve();
}

class FakeClock {
    value = 0;
    now = (): number => this.value;
    advance(milliseconds: number): void { this.value += milliseconds; }
    set(milliseconds: number): void { this.value = milliseconds; }
}

class IdentitySession {
    async sealBytes(bytes: Uint8Array): Promise<Uint8Array> { return bytes.slice(); }
    async openBytes(bytes: Uint8Array): Promise<Uint8Array> { return bytes.slice(); }
}

type RpcFrame = ReturnType<typeof decodeWsDataFrame>;

function responseType(type: WsDataFrameType): WsDataFrameType {
    switch (type) {
        case WsDataFrameType.CheckObjects: return WsDataFrameType.CheckResult;
        case WsDataFrameType.PutPack: return WsDataFrameType.PutAck;
        case WsDataFrameType.GetPack: return WsDataFrameType.GetResult;
        default: throw new Error("chaos carrier received a non-RPC frame");
    }
}

function errorPayload(code: WsDataErrorCode, message: string): Uint8Array {
    const messageBytes = new TextEncoder().encode(message);
    const output = new Uint8Array(14 + messageBytes.byteLength);
    output.set(new TextEncoder().encode("OWE1"), 0);
    const view = new DataView(output.buffer);
    view.setUint16(4, code, true);
    view.setUint32(8, 0, true);
    view.setUint16(12, messageBytes.byteLength, true);
    output.set(messageBytes, 14);
    return output;
}

/** Deterministic protocol carrier. Responses exist only in `held` until the
 * test releases them, so response reorder, loss and old-session delivery do
 * not depend on wall-clock timing or a real socket. */
class ChaosSocket {
    binaryType = "";
    readyState = 0;
    bufferedAmount = 0;
    onopen: (() => void) | null = null;
    onmessage: ((event: { data: unknown }) => void) | null = null;
    onclose: (() => void) | null = null;
    onerror: (() => void) | null = null;
    readonly rpcFrames: RpcFrame[] = [];
    readonly cancelFrames: RpcFrame[] = [];
    readonly wireOrder: WsDataFrameType[] = [];
    private openScheduled = false;
    private readonly held = new Map<number, ArrayBuffer>();

    constructor(
        private readonly maxInflightRequests = 2,
        private readonly maxInflightBytes = 8 * 1024 * 1024,
        private readonly onRpc?: (frame: RpcFrame) => void,
        private readonly maxPayloadBytes = 1024,
    ) {}

    open(): void {
        if (this.openScheduled) return;
        this.openScheduled = true;
        queueMicrotask(() => {
            this.readyState = 1;
            this.onopen?.();
        });
    }

    send(data: string | ArrayBuffer): void {
        if (typeof data === "string") {
            return;
        }
        const frame = decodeWsDataFrame(new Uint8Array(data), 4 * 1024 * 1024);
        if (frame.type === WsDataFrameType.Hello) {
            const ack = frame.payload.slice();
            ack.set(new TextEncoder().encode("OWA1"), 0);
            const view = new DataView(ack.buffer, ack.byteOffset, ack.byteLength);
            view.setUint16(6, this.maxInflightRequests, true);
            view.setUint32(8, this.maxPayloadBytes, true);
            view.setBigUint64(16, BigInt(this.maxInflightBytes), true);
            this.deliver(this.encode(WsDataFrameType.HelloAck, 0, ack));
            return;
        }
        this.wireOrder.push(frame.type);
        if (frame.type === WsDataFrameType.Cancel) {
            this.cancelFrames.push(frame);
            this.deliver(this.encode(
                WsDataFrameType.Error,
                frame.requestId,
                errorPayload(WsDataErrorCode.Cancelled, "cancelled"),
            ));
            return;
        }
        this.rpcFrames.push(frame);
        this.onRpc?.(frame);
        this.held.set(frame.requestId,
            this.encode(responseType(frame.type), frame.requestId, frame.payload));
    }

    close(): void { this.readyState = 3; }

    takeHeld(requestId: number): ArrayBuffer {
        const response = this.held.get(requestId);
        if (!response) throw new Error(`chaos response ${requestId} is not held`);
        this.held.delete(requestId);
        return response;
    }

    release(requestId: number): void { this.deliver(this.takeHeld(requestId)); }

    releaseAllReverse(): void {
        for (const requestId of [...this.held.keys()].reverse()) this.release(requestId);
    }

    disconnect(): void {
        this.readyState = 3;
        queueMicrotask(() => this.onclose?.());
    }

    private encode(type: WsDataFrameType, requestId: number, payload: Uint8Array): ArrayBuffer {
        return encodeWsDataFrame(type, requestId, payload, 4 * 1024 * 1024).buffer as ArrayBuffer;
    }

    private deliver(data: ArrayBuffer): void {
        queueMicrotask(() => this.onmessage?.({ data }));
    }
}

const retryable = () => ({ retryable: true });

function lane(socket: ChaosSocket, ticket: string): ObsetyncWsDataLane {
    return new ObsetyncWsDataLane({
        baseUrl: "http://chaos.invalid",
        runtime: "desktop",
        advertisedPayloadBytes: 4 * 1024 * 1024,
        openSession: async () => ({ ticket, session: new IdentitySession() }),
        socketFactory: () => {
            socket.open();
            return socket;
        },
    });
}

async function reorderedFramesAndByteCreditsStaySingleOwner(): Promise<void> {
    const socket = new ChaosSocket(2, 1024);
    const dataLane = lane(socket, "reorder");
    const completions = [0, 0];
    try {
        const requests = [1, 2].map((value, index) => dataLane.request(
            WsDataFrameType.CheckObjects,
            new Uint8Array([value]),
            WsDataFrameType.CheckResult,
        ).finally(() => { completions[index]++; }));
        await eventually(() => socket.rpcFrames.length === 2,
            "reorder matrix did not fill its bounded credit window");
        socket.releaseAllReverse();
        const responses = await Promise.all(requests);
        ok(responses[0][0] === 1 && responses[1][0] === 2,
            "reordered WS responses crossed request ownership");
        ok(completions.join() === "1,1", "reordered WS request completed more than once");
        await drainMicrotasks();
    } finally { dataLane.close(); }

    const creditSocket = new ChaosSocket(4, 1500);
    const creditLane = lane(creditSocket, "byte-credit");
    try {
        const first = creditLane.request(WsDataFrameType.PutPack,
            new Uint8Array(800), WsDataFrameType.PutAck);
        const second = creditLane.request(WsDataFrameType.PutPack,
            new Uint8Array(800), WsDataFrameType.PutAck);
        await eventually(() => creditSocket.rpcFrames.length === 1,
            "byte-credit matrix did not send its first request");
        ok(creditSocket.rpcFrames.length === 1,
            "bounded in-flight bytes admitted a second WS request");
        creditSocket.release(creditSocket.rpcFrames[0].requestId);
        await eventually(() => creditSocket.rpcFrames.length === 2,
            "released byte credit did not resume the queued request");
        creditSocket.release(creditSocket.rpcFrames[1].requestId);
        await Promise.all([first, second]);
        ok(creditSocket.rpcFrames.length === 2,
            "byte backpressure duplicated or lost a logical RPC");
        await drainMicrotasks();
    } finally { creditLane.close(); }
}

async function cancelPreemptsAFullCreditWindow(): Promise<void> {
    const socket = new ChaosSocket(1, 1024);
    const dataLane = lane(socket, "cancel-priority");
    const controller = new AbortController();
    let bulkCompletions = 0, controlCompletions = 0;
    try {
        const bulk = dataLane.request(
            WsDataFrameType.PutPack,
            new Uint8Array([7]),
            WsDataFrameType.PutAck,
            undefined,
            undefined,
            1,
            { creditTimeoutMs: 1_000, sendTimeoutMs: 1_000, ackTimeoutMs: 1_000 },
            controller.signal,
        ).then(() => "resolved", error => error instanceof Error ? error.name : String(error))
            .finally(() => { bulkCompletions++; });
        await eventually(() => socket.rpcFrames.length === 1,
            "priority matrix did not send its bulk owner");
        const control = dataLane.request(
            WsDataFrameType.CheckObjects,
            new Uint8Array([8]),
            WsDataFrameType.CheckResult,
        ).finally(() => { controlCompletions++; });
        await Promise.resolve();
        ok(socket.rpcFrames.length === 1, "control bypassed the negotiated request credit");
        controller.abort();
        ok(await bulk === "WsDataCancelledError", "CANCEL changed caller-visible identity");
        await eventually(() => socket.rpcFrames.length === 2,
            "CANCEL did not release credit for control work");
        ok(socket.wireOrder.slice(0, 3).join() === [
            WsDataFrameType.PutPack,
            WsDataFrameType.Cancel,
            WsDataFrameType.CheckObjects,
        ].join(), "CANCEL/control priority escaped its bounded wire order");
        socket.release(socket.rpcFrames[1].requestId);
        const response = await control;
        ok(response[0] === 8 && bulkCompletions === 1 && controlCompletions === 1,
            "priority/cancellation duplicated or lost an RPC completion");
        ok(socket.cancelFrames.length === 1,
            "one cancelled sent RPC emitted more than one CANCEL");
    } finally { dataLane.close(); }
}

async function delayedRttAndBandwidthChooseTruthfulCarrier(): Promise<void> {
    const clock = new FakeClock();
    const selected: string[] = [];
    const router = new TransportRouter({
        now: clock.now,
        performanceMinSamples: 1,
        performanceProbeEvery: 1,
        performanceHysteresisRatio: 1.25,
        performanceHoldMs: 1_000,
    });
    const request = (semanticLane: "bulk" | "control") => router.execute({
        payloadBytes: 1_000,
        expectedUsefulBytes: 1_000,
        replay: "content-addressed" as const,
        lane: semanticLane,
        ws: async (_timeouts: TransportPhaseTimeouts, measured: TransportAttemptMeasurements) => {
            selected.push("ws");
            measured.addQueueWaitMs(100);
            measured.setUsefulBytes(1_000);
            clock.advance(300);
            return "ws";
        },
        http: async (measured: TransportAttemptMeasurements) => {
            selected.push("http");
            measured.setUsefulBytes(1_000);
            clock.advance(50);
            return "http";
        },
        classifyWsFailure: retryable,
    });
    await request("bulk");
    await request("bulk");
    ok(await request("bulk") === "http",
        "measured delayed/queued WS did not move bulk to faster HTTP");
    const bulk = router.snapshot();
    ok(bulk.bulkPreferenceEstablished && bulk.bulkPreferredCarrier === "http" &&
        bulk.wsBulkQueueWaitMs === 100 &&
        (bulk.httpBulkThroughputBytesPerSecond ?? 0) >
            (bulk.wsBulkThroughputBytesPerSecond ?? Number.MAX_VALUE),
    "bandwidth/backpressure diagnostics did not support the selected carrier");
    ok(await request("control") === "ws" && selected.join() === "ws,http,http,ws",
        "bulk preference delayed latency-oriented control work");
    const control = router.snapshot();
    ok(control.lastSelectedCarrier === "ws" &&
        control.lastSelectionReason === "interactive-ws" &&
        control.lastSelectionLane === "control" &&
        control.lastSelectionAtMs === 400,
    "route diagnostics did not truthfully publish the control decision");
}

async function silentHalfOpenTimeoutRecoversWsWithoutAProbeStampede(): Promise<void> {
    const clock = new FakeClock();
    const router = new TransportRouter({
        now: clock.now,
        jitter: () => 0,
        failureThreshold: 1,
        openBaseMs: 100,
        openMaxMs: 1_000,
        recoverySuccessThreshold: 2,
        recoveryProbeIntervalMs: 10,
        probeCreditTimeoutMs: 7,
        probeSendTimeoutMs: 8,
        probeAckTimeoutMs: 25,
    });
    const invocations: string[] = [];
    const invocationCounts = new Map<string, number>();
    const completions = new Map<string, number>();
    const recordInvocation = (carrier: "ws" | "http", id: string): void => {
        const key = `${carrier}:${id}`;
        invocations.push(key);
        invocationCounts.set(key, (invocationCounts.get(key) ?? 0) + 1);
    };
    const labels = new Map<number, string>([
        [31, "silent-probe"],
        [32, "probe-one"],
        [33, "probe-two"],
        [34, "healthy-bulk"],
    ]);
    const sockets: ChaosSocket[] = [];
    const dataLane = new ObsetyncWsDataLane({
        baseUrl: "http://chaos.invalid",
        runtime: "desktop",
        advertisedPayloadBytes: 4 * 1024 * 1024,
        openSession: async () => ({
            ticket: `half-open-${sockets.length}`,
            session: new IdentitySession(),
        }),
        backoffStartMs: 1,
        now: clock.now,
        socketFactory: () => {
            const socketIndex = sockets.length;
            let socket!: ChaosSocket;
            socket = new ChaosSocket(1, 1024, (frame) => {
                const id = labels.get(frame.payload[0]);
                if (!id) throw new Error(`unknown recovery payload ${frame.payload[0]}`);
                recordInvocation("ws", id);
                // The first socket silently drops its ACK. A replacement
                // session responds normally after the real lane timeout.
                if (socketIndex > 0) queueMicrotask(() => socket.release(frame.requestId));
            });
            sockets.push(socket);
            socket.open();
            return socket;
        },
    });
    const run = (
        id: string,
        semanticLane: "bulk" | "control" | "urgent",
        ws: (timeouts: TransportPhaseTimeouts) => Promise<string>,
    ) => router.execute({
        payloadBytes: 16,
        replay: "content-addressed" as const,
        lane: semanticLane,
        ws,
        http: async () => {
            recordInvocation("http", id);
            clock.advance(5);
            return `http:${id}`;
        },
        classifyWsFailure: retryable,
    }).finally(() => completions.set(id, (completions.get(id) ?? 0) + 1));

    const lost = await run("lost-ack", "urgent", async () => {
        recordInvocation("ws", "lost-ack");
        throw new WsDataUnavailableError("lost ACK");
    });
    ok(lost === "http:lost-ack" &&
        invocations.join() === "ws:lost-ack,http:lost-ack" &&
        invocationCounts.get("ws:lost-ack") === 1 &&
        invocationCounts.get("http:lost-ack") === 1 &&
        completions.get("lost-ack") === 1,
    "lost ACK escaped its exact WS/fallback invocation sequence");
    let snapshot = router.snapshot();
    ok(snapshot.circuit === "open" && snapshot.lastSelectedCarrier === "http" &&
        snapshot.lastSelectionReason === "ws-retryable-fallback" && snapshot.openForMs === 95,
    "lost-ACK diagnostics did not expose the fallback/open hold");

    ok(await run("open-bulk", "bulk", async () => "unexpected") === "http:open-bulk",
        "open circuit dispatched bulk onto WS");
    snapshot = router.snapshot();
    ok(snapshot.lastSelectionReason === "ws-circuit-open",
        "open-circuit HTTP selection reported a false performance preference");

    clock.set(100);
    let probeTimeouts: TransportPhaseTimeouts | undefined;
    const silentProbe = run("silent-probe", "control", async (timeouts) => {
        probeTimeouts = timeouts;
        const response = await dataLane.request(
            WsDataFrameType.CheckObjects,
            new Uint8Array([31]),
            WsDataFrameType.CheckResult,
            undefined,
            undefined,
            1,
            timeouts,
        );
        return `ws:${response[0]}`;
    });
    await eventually(() => router.snapshot().halfOpenInFlight &&
        sockets.length === 1 && sockets[0].rpcFrames.length === 1,
    "silent half-open probe did not reach the real WS ACK wait");
    let queuedCompletions = 0;
    const queuedBehindLostAck = dataLane.request(
        WsDataFrameType.CheckObjects,
        new Uint8Array([35]),
        WsDataFrameType.CheckResult,
        undefined,
        undefined,
        1,
        { creditTimeoutMs: 1_000, sendTimeoutMs: 1_000, ackTimeoutMs: 1_000 },
    ).then(
        () => ({ resolved: true, unavailable: false, message: "" }),
        error => ({
            resolved: false,
            unavailable: error instanceof WsDataUnavailableError,
            message: error instanceof Error ? error.message : String(error),
        }),
    )
        .finally(() => { queuedCompletions++; });
    await eventually(() => (dataLane as unknown as { creditWaiters: unknown[] })
        .creditWaiters.length === 1,
    "lost-ACK fixture did not queue a real credit waiter");
    const parallel = await run("parallel", "control", async () => "stampede");
    ok(parallel === "http:parallel" &&
        router.snapshot().lastSelectionReason === "ws-recovery-probe-busy",
    "parallel control request joined the half-open probe");
    clock.advance(4);
    ok(await silentProbe === "http:silent-probe",
        "silent half-open timeout lost its single HTTP fallback");
    const queuedOutcome = await queuedBehindLostAck;
    ok(!queuedOutcome.resolved && queuedOutcome.unavailable &&
        queuedOutcome.message.includes("ACK phase timed out") &&
        queuedCompletions === 1 && sockets[0].rpcFrames.length === 1,
    "ACK timeout failed to reject exactly one queued credit owner");
    ok(probeTimeouts?.creditTimeoutMs === 7 && probeTimeouts.sendTimeoutMs === 8 &&
        probeTimeouts.ackTimeoutMs === 25,
    "half-open timeout escaped its bounded phase policy");
    const ownership = dataLane as unknown as {
        pending: Map<number, unknown>;
        cancelled: Map<number, unknown>;
        activeRequests: number;
        activeBytes: number;
        creditWaiters: unknown[];
    };
    ok(dataLane.getState() === "backoff" && sockets[0].readyState === 3 &&
        ownership.pending.size === 0 &&
        ownership.cancelled.size === 0 && ownership.activeRequests === 0 &&
        ownership.activeBytes === 0 && ownership.creditWaiters.length === 0,
    "actual ACK timeout retained request, byte-credit, or reconnect ownership");
    snapshot = router.snapshot();
    ok(snapshot.circuit === "open" && snapshot.consecutiveWsFailures === 2 &&
        snapshot.halfOpenInFlight === false,
    "timed-out probe left false recovery ownership or failure diagnostics");

    clock.set(309);
    ok(await run("probe-one", "control", async (timeouts) => {
        const response = await dataLane.request(
            WsDataFrameType.CheckObjects, new Uint8Array([32]),
            WsDataFrameType.CheckResult,
            undefined, undefined, 1, timeouts,
        );
        clock.advance(5);
        return response[0] === 32 ? "ws:probe-one" : "unexpected";
    }) === "ws:probe-one", "first recovery probe failed");
    ok(sockets.length === 2 && dataLane.getState() === "ready" &&
        router.snapshot().circuit === "open" && router.snapshot().recoveryWsSuccesses === 1,
        "one recovery sample released the circuit without hysteresis");
    clock.set(324);
    ok(await run("probe-two", "urgent", async (timeouts) => {
        const response = await dataLane.request(
            WsDataFrameType.CheckObjects, new Uint8Array([33]),
            WsDataFrameType.CheckResult,
            undefined, undefined, 1, timeouts,
        );
        clock.advance(5);
        return response[0] === 33 ? "ws:probe-two" : "unexpected";
    }) === "ws:probe-two", "second recovery probe failed");
    ok(router.snapshot().circuit === "closed", "two bounded probes did not recover WS");
    ok(await run("healthy-bulk", "bulk", async (timeouts) => {
        const response = await dataLane.request(
            WsDataFrameType.CheckObjects, new Uint8Array([34]),
            WsDataFrameType.CheckResult,
            undefined, undefined, 1, timeouts,
        );
        clock.advance(5);
        return response[0] === 34 ? "ws:healthy-bulk" : "unexpected";
    }) === "ws:healthy-bulk", "healthy WS did not regain ordinary bulk work");
    snapshot = router.snapshot();
    ok(snapshot.lastSelectedCarrier === "ws" && snapshot.circuit === "closed" &&
        snapshot.wsFailures === 2 && snapshot.halfOpenInFlight === false,
    "WS→HTTP→WS diagnostics did not match the completed route sequence");
    ok([...completions.values()].every(count => count === 1),
        "chaos recovery completed a logical RPC more than once");
    ok(invocations.join() === [
        "ws:lost-ack", "http:lost-ack", "http:open-bulk",
        "ws:silent-probe", "http:parallel", "http:silent-probe",
        "ws:probe-one", "ws:probe-two", "ws:healthy-bulk",
    ].join() && [...invocationCounts.values()].every(count => count === 1),
    "recovery emitted an extra or missing carrier invocation");
    dataLane.close();
}

async function serverRestartCannotPublishAStaleSessionResult(): Promise<void> {
    const clock = new FakeClock();
    const router = new TransportRouter({
        now: clock.now,
        jitter: () => 0,
        failureThreshold: 1,
        openBaseMs: 100,
        openMaxMs: 1_000,
        recoverySuccessThreshold: 1,
    });
    const invocations: string[] = [];
    const sockets: ChaosSocket[] = [];
    const sameLane = new ObsetyncWsDataLane({
        baseUrl: "http://chaos.invalid",
        runtime: "desktop",
        advertisedPayloadBytes: 4 * 1024 * 1024,
        openSession: async () => ({
            ticket: `restart-${sockets.length}`,
            session: new IdentitySession(),
        }),
        backoffStartMs: 100,
        now: clock.now,
        socketFactory: () => {
            const id = sockets.length === 0 ? "restart-op" : "fresh-session";
            const socket = new ChaosSocket(1, 1024,
                () => invocations.push(`ws:${id}`));
            sockets.push(socket);
            socket.open();
            return socket;
        },
    });
    let completions = 0, freshCompletions = 0, httpCalls = 0;
    const routed = router.execute({
        payloadBytes: 1,
        replay: "content-addressed",
        lane: "control",
        ws: async (timeouts) => sameLane.request(
            WsDataFrameType.PutPack,
            new Uint8Array([41]),
            WsDataFrameType.PutAck,
            undefined,
            undefined,
            1,
            timeouts,
        ),
        http: async () => {
            httpCalls++;
            invocations.push("http:restart-op");
            return new Uint8Array([42]);
        },
        classifyWsFailure: retryable,
    }).finally(() => { completions++; });
    await eventually(() => sockets.length === 1 && sockets[0].rpcFrames.length === 1,
        "restart fixture did not reach the old session");
    const oldSocket = sockets[0];
    const oldFrame = oldSocket.rpcFrames[0];
    const staleResponse = oldSocket.takeHeld(oldFrame.requestId);
    const staleHandler = oldSocket.onmessage;
    oldSocket.disconnect();
    const result = await routed;
    ok(result[0] === 42 && httpCalls === 1 && completions === 1 &&
        invocations.join() === "ws:restart-op,http:restart-op",
    "restart fallback escaped its exact carrier/completion sequence");

    clock.set(100);
    try {
        let freshSettled = false;
        const recovered = router.execute({
            payloadBytes: 1,
            replay: "content-addressed",
            lane: "control",
            ws: async (timeouts) => sameLane.request(
                WsDataFrameType.CheckObjects,
                new Uint8Array([43]),
                WsDataFrameType.CheckResult,
                undefined,
                undefined,
                1,
                timeouts,
            ),
            http: async () => { httpCalls++; return new Uint8Array([0]); },
            classifyWsFailure: retryable,
        }).finally(() => {
            freshSettled = true;
            freshCompletions++;
        });
        await eventually(() => sockets.length === 2 && sockets[1].rpcFrames.length === 1,
            "fresh session did not send its recovery probe");
        // Deliver the old session's authenticated frame while the replacement
        // request is live. It must neither settle nor poison the current lane.
        staleHandler?.({ data: staleResponse });
        await drainMicrotasks();
        ok(!freshSettled && sameLane.getState() === "ready" &&
            invocations.join() === "ws:restart-op,http:restart-op,ws:fresh-session",
        "stale old-socket frame affected an active replacement request");
        sockets[1].release(sockets[1].rpcFrames[0].requestId);
        const fresh = await recovered;
        ok(fresh[0] === 43 && httpCalls === 1 && freshCompletions === 1 &&
            completions === 1 && sameLane.getState() === "ready",
            "fresh post-restart session did not own the recovered result");
        staleHandler?.({ data: staleResponse });
        await Promise.resolve();
        ok(sameLane.getState() === "ready" && router.snapshot().circuit === "closed" &&
            invocations.join() === "ws:restart-op,http:restart-op,ws:fresh-session",
            "stale pre-restart frame poisoned the replacement session diagnostics");
    } finally { sameLane.close(); }
}

async function runSuite(): Promise<void> {
    await reorderedFramesAndByteCreditsStaySingleOwner();
    await cancelPreemptsAFullCreditWindow();
    await delayedRttAndBandwidthChooseTruthfulCarrier();
    await silentHalfOpenTimeoutRecoversWsWithoutAProbeStampede();
    await serverRestartCannotPublishAStaleSessionResult();
    console.log(`adaptive-transport-chaos.test: ${assertions} assertions passed`);
}

async function withCompletionWatchdog<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
    let watchdog: ReturnType<typeof setTimeout> | undefined;
    try {
        return await Promise.race([
            promise,
            new Promise<never>((_resolve, reject) => {
                watchdog = setTimeout(() => reject(new Error(
                    `adaptive transport chaos suite exceeded ${timeoutMs}ms`,
                )), timeoutMs);
            }),
        ]);
    } finally {
        if (watchdog !== undefined) clearTimeout(watchdog);
    }
}

void withCompletionWatchdog(runSuite(), 2_000)
    .catch((error) => { setTimeout(() => { throw error; }, 0); });
