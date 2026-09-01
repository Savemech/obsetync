/**
 * Optional sealed WebSocket data lane.
 *
 * The lane owns a different ticket/session/socket from realtime so object
 * traffic cannot delay presence or root notifications. Every operation is
 * content-addressed and may be retried over bulk HTTP after any failure.
 */
import { exactArrayBuffer } from "./binary";
import type { PerfOperation } from "./perf-trace";
import {
    WsDataFrameType,
    decodeWsDataError,
    decodeWsDataFrame,
    decodeWsDataHelloAck,
    encodeWsDataFrame,
    encodeWsDataHello,
    initialWsDataLimits,
    type WsDataLimits,
    type WsDataRemoteError,
} from "./ws-data-codec";

const CONNECT_TIMEOUT_MS = 10_000;
const REQUEST_TIMEOUT_MS = 60_000;
const BACKOFF_START_MS = 1_000;
const BACKOFF_CAP_MS = 60_000;
const MAX_CREDIT_WAITERS = 16;
const SOCKET_OPEN = 1;

interface BinarySession {
    sealBytes(plaintext: Uint8Array): Promise<Uint8Array>;
    openBytes(frame: Uint8Array): Promise<Uint8Array>;
}

interface SocketLike {
    binaryType: string;
    readyState: number;
    bufferedAmount: number;
    onopen: ((event?: unknown) => void) | null;
    onmessage: ((event: { data: unknown }) => void) | null;
    onclose: ((event?: unknown) => void) | null;
    onerror: ((event?: unknown) => void) | null;
    send(data: string | ArrayBuffer): void;
    close(): void;
}

interface OpenedSession {
    ticket: string;
    session: BinarySession;
}

export interface WsDataLaneOptions {
    baseUrl: string;
    runtime: "desktop" | "mobile";
    advertisedPayloadBytes: number;
    openSession: () => Promise<OpenedSession>;
    socketFactory?: (url: string) => SocketLike;
    backoffStartMs?: number;
    /** Dynamic local cap; authenticated server credits remain the ceiling. */
    localRequestLimit?: () => number;
}

interface PendingRequest {
    expectedType: WsDataFrameType;
    payloadBytes: number;
    resolve: (payload: Uint8Array) => void;
    reject: (error: Error) => void;
    timer: ReturnType<typeof globalThis.setTimeout>;
}

interface CreditWaiter {
    resolve: () => void;
    reject: (error: Error) => void;
}

export type WsDataLaneState = "off" | "connecting" | "ready" | "backoff";

export class WsDataUnavailableError extends Error {}

export class WsDataRpcError extends Error {
    constructor(public readonly remote: WsDataRemoteError) {
        super(`ws-data RPC ${remote.code}: ${remote.message}`);
    }
}

export class ObsetyncWsDataLane {
    private socket: SocketLike | null = null;
    private session: BinarySession | null = null;
    private limits: WsDataLimits | null = null;
    private connectPromise: Promise<WsDataLimits> | null = null;
    private pending = new Map<number, PendingRequest>();
    private activeRequests = 0;
    private activeBytes = 0;
    private nextRequestId = 1;
    private creditWaiters: CreditWaiter[] = [];
    private txChain: Promise<void> = Promise.resolve();
    private rxChain: Promise<void> = Promise.resolve();
    private state: WsDataLaneState = "off";
    private stopped = false;
    private backoffMs: number;
    private backoffUntil = 0;
    private readonly requested: WsDataLimits;
    private readonly socketFactory: (url: string) => SocketLike;

    constructor(private readonly options: WsDataLaneOptions) {
        const requested = initialWsDataLimits(options.runtime, options.advertisedPayloadBytes);
        if (!requested) throw new WsDataUnavailableError("server advertised invalid WS limits");
        this.requested = requested;
        this.backoffMs = options.backoffStartMs ?? BACKOFF_START_MS;
        this.socketFactory = options.socketFactory ??
            ((url) => new WebSocket(url) as unknown as SocketLike);
    }

    getState(): WsDataLaneState {
        return this.state;
    }

    getLimits(): WsDataLimits | null {
        return this.limits ? { ...this.limits } : null;
    }

    close(): void {
        this.stopped = true;
        this.failCurrent(
            this.socket,
            new WsDataUnavailableError("WS data lane stopped"),
            false,
        );
        this.state = "off";
    }

    async request(
        type: WsDataFrameType.CheckObjects | WsDataFrameType.PutPack | WsDataFrameType.GetPack,
        payload: Uint8Array,
        expectedType:
            | WsDataFrameType.CheckResult
            | WsDataFrameType.PutAck
            | WsDataFrameType.GetResult,
        perf?: PerfOperation,
    ): Promise<Uint8Array> {
        const limits = await this.ensureConnected(perf);
        if (payload.byteLength > limits.maxPayloadBytes) {
            throw new WsDataUnavailableError("RPC payload exceeds negotiated WS frame limit");
        }
        await this.acquireCredit(payload.byteLength, limits, perf);
        const socket = this.socket;
        const session = this.session;
        if (!socket || !session || socket.readyState !== SOCKET_OPEN || this.state !== "ready") {
            this.releaseCredit(payload.byteLength);
            throw new WsDataUnavailableError("WS data lane disconnected before send");
        }
        if (this.nextRequestId > Number.MAX_SAFE_INTEGER) {
            this.releaseCredit(payload.byteLength);
            this.failCurrent(
                socket,
                new WsDataUnavailableError("WS data request id space exhausted"),
                true,
            );
            throw new WsDataUnavailableError("WS data request id space exhausted");
        }
        const requestId = this.nextRequestId++;
        const promise = new Promise<Uint8Array>((resolve, reject) => {
            const timer = globalThis.setTimeout(() => {
                this.failCurrent(
                    socket,
                    new WsDataUnavailableError("WS data RPC timed out; retry by content hash"),
                    true,
                );
            }, REQUEST_TIMEOUT_MS);
            this.pending.set(requestId, {
                expectedType,
                payloadBytes: payload.byteLength,
                resolve,
                reject,
                timer,
            });
        });

        this.txChain = this.txChain
            .then(async () => {
                await this.waitForBufferedAmount(socket, limits, perf);
                const plaintext = encodeWsDataFrame(
                    type,
                    requestId,
                    payload,
                    limits.maxPayloadBytes,
                );
                const sealed = await session.sealBytes(plaintext);
                if (this.socket !== socket || socket.readyState !== SOCKET_OPEN) {
                    throw new WsDataUnavailableError("WS data lane changed before send");
                }
                socket.send(exactArrayBuffer(sealed));
                perf?.increment({ wsFrameCount: 1 });
            })
            .catch((error) => {
                this.failCurrent(
                    socket,
                    error instanceof Error ? error : new Error(String(error)),
                    true,
                );
            });
        return promise;
    }

    private async ensureConnected(perf?: PerfOperation): Promise<WsDataLimits> {
        if (this.stopped) throw new WsDataUnavailableError("WS data lane is stopped");
        if (this.state === "ready" && this.limits && this.socket?.readyState === SOCKET_OPEN) {
            return this.limits;
        }
        if (Date.now() < this.backoffUntil) {
            throw new WsDataUnavailableError("WS data lane is in reconnect backoff");
        }
        if (this.connectPromise) return this.connectPromise;
        this.connectPromise = this.connect(perf);
        try {
            return await this.connectPromise;
        } finally {
            this.connectPromise = null;
        }
    }

    private connect(perf?: PerfOperation): Promise<WsDataLimits> {
        this.state = "connecting";
        return new Promise<WsDataLimits>((resolve, reject) => {
            let settled = false;
            let timeout: ReturnType<typeof globalThis.setTimeout> | null = null;
            const rejectConnect = (error: Error, socket: SocketLike | null) => {
                if (!settled) {
                    settled = true;
                    if (timeout !== null) globalThis.clearTimeout(timeout);
                    reject(error);
                }
                this.failCurrent(socket, error, true);
            };
            void this.options.openSession().then(({ ticket, session }) => {
                if (this.stopped) {
                    rejectConnect(new WsDataUnavailableError("WS data lane stopped"), null);
                    return;
                }
                let socket: SocketLike;
                try {
                    socket = this.socketFactory(this.wsUrl());
                } catch (error) {
                    rejectConnect(
                        error instanceof Error ? error : new Error(String(error)),
                        null,
                    );
                    return;
                }
                socket.binaryType = "arraybuffer";
                this.socket = socket;
                this.session = session;
                this.limits = null;
                this.txChain = Promise.resolve();
                this.rxChain = Promise.resolve();

                timeout = globalThis.setTimeout(() => {
                    rejectConnect(new WsDataUnavailableError("WS data handshake timed out"), socket);
                }, CONNECT_TIMEOUT_MS);

                socket.onopen = () => {
                    try {
                        socket.send(JSON.stringify({ v: 2, t: "auth", ticket }));
                    } catch (error) {
                        rejectConnect(
                            error instanceof Error ? error : new Error(String(error)),
                            socket,
                        );
                        return;
                    }
                    this.txChain = this.txChain.then(async () => {
                        const hello = encodeWsDataFrame(
                            WsDataFrameType.Hello,
                            0,
                            encodeWsDataHello(this.requested),
                            this.requested.maxPayloadBytes,
                        );
                        const sealed = await session.sealBytes(hello);
                        if (this.socket !== socket || socket.readyState !== SOCKET_OPEN) {
                            throw new WsDataUnavailableError("WS data socket closed during HELLO");
                        }
                        socket.send(exactArrayBuffer(sealed));
                        perf?.increment({ wsFrameCount: 1 });
                    }).catch((error) => rejectConnect(
                        error instanceof Error ? error : new Error(String(error)),
                        socket,
                    ));
                };

                socket.onmessage = (event) => {
                    if (!(event.data instanceof ArrayBuffer)) {
                        rejectConnect(
                            new WsDataUnavailableError("WS data server sent plaintext after auth"),
                            socket,
                        );
                        return;
                    }
                    const wire = new Uint8Array(event.data);
                    this.rxChain = this.rxChain.then(async () => {
                        const plaintext = await session.openBytes(wire);
                        perf?.increment({ wsFrameCount: 1 });
                        const maxPayload = this.limits?.maxPayloadBytes ??
                            this.requested.maxPayloadBytes;
                        const frame = decodeWsDataFrame(plaintext, maxPayload);
                        if (!this.limits) {
                            if (frame.type !== WsDataFrameType.HelloAck || frame.requestId !== 0) {
                                throw new WsDataUnavailableError("WS data expected HELLO_ACK");
                            }
                            const limits = decodeWsDataHelloAck(frame.payload, this.requested);
                            this.limits = limits;
                            this.state = "ready";
                            this.backoffMs = this.options.backoffStartMs ?? BACKOFF_START_MS;
                            this.backoffUntil = 0;
                            if (!settled) {
                                settled = true;
                                if (timeout !== null) globalThis.clearTimeout(timeout);
                                resolve(limits);
                            }
                            return;
                        }
                        this.handleResponse(frame);
                    }).catch((error) => rejectConnect(
                        error instanceof Error ? error : new Error(String(error)),
                        socket,
                    ));
                };
                socket.onclose = () => rejectConnect(
                    new WsDataUnavailableError("WS data socket closed"),
                    socket,
                );
                socket.onerror = () => rejectConnect(
                    new WsDataUnavailableError("WS data socket failed"),
                    socket,
                );
            }).catch((error) => rejectConnect(
                error instanceof Error ? error : new Error(String(error)),
                null,
            ));
        });
    }

    private handleResponse(frame: ReturnType<typeof decodeWsDataFrame>): void {
        if (frame.requestId === 0) {
            throw new WsDataUnavailableError("WS data response reused handshake id");
        }
        const pending = this.pending.get(frame.requestId);
        if (!pending) {
            throw new WsDataUnavailableError("WS data response has no matching request");
        }
        // Validate the entire response before removing it from `pending`.
        // Otherwise a malformed ERROR payload would clear its timer and make
        // the caller wait forever when the protocol failure closes the lane.
        const remoteError = frame.type === WsDataFrameType.Error
            ? decodeWsDataError(frame.payload)
            : null;
        if (frame.type !== WsDataFrameType.Error && frame.type !== pending.expectedType) {
            throw new WsDataUnavailableError("WS data response type mismatch");
        }
        this.pending.delete(frame.requestId);
        globalThis.clearTimeout(pending.timer);
        this.releaseCredit(pending.payloadBytes);
        if (remoteError) {
            pending.reject(new WsDataRpcError(remoteError));
            return;
        }
        // `openBytes` already returned a fresh owned plaintext allocation.
        // The subarray keeps that backing store alive, so another full-page
        // copy here would only double peak receive memory.
        pending.resolve(frame.payload);
    }

    private async acquireCredit(
        bytes: number,
        limits: WsDataLimits,
        perf?: PerfOperation,
    ): Promise<void> {
        let pressureReported = false;
        for (;;) {
            const configuredLimit = this.options.localRequestLimit?.() ??
                limits.maxInflightRequests;
            const requestLimit = Number.isFinite(configuredLimit)
                ? Math.max(1, Math.min(limits.maxInflightRequests, Math.trunc(configuredLimit)))
                : 1;
            if (this.activeRequests < requestLimit &&
                this.activeBytes + bytes <= limits.maxInflightBytes) {
                // Reserve synchronously before returning the Promise. Several
                // callers may wake in the same microtask turn; counting only
                // entries inserted into `pending` would let all of them pass.
                this.activeRequests++;
                this.activeBytes += bytes;
                return;
            }
            if (this.creditWaiters.length >= MAX_CREDIT_WAITERS) {
                throw new WsDataUnavailableError("WS data credit wait queue is full");
            }
            const serverCreditBound =
                this.activeRequests >= limits.maxInflightRequests ||
                this.activeBytes + bytes > limits.maxInflightBytes;
            if (serverCreditBound && !pressureReported) {
                pressureReported = true;
                perf?.increment({ backpressureEvents: 1 });
            }
            await new Promise<void>((resolve, reject) => {
                this.creditWaiters.push({ resolve, reject });
            });
            if (this.state !== "ready") {
                throw new WsDataUnavailableError("WS data lane closed while waiting for credit");
            }
        }
    }

    private releaseCredit(bytes: number): void {
        this.activeRequests = Math.max(0, this.activeRequests - 1);
        this.activeBytes = Math.max(0, this.activeBytes - bytes);
        this.wakeCreditWaiters();
    }

    private wakeCreditWaiters(): void {
        const waiters = this.creditWaiters.splice(0);
        for (const waiter of waiters) waiter.resolve();
    }

    private async waitForBufferedAmount(
        socket: SocketLike,
        limits: WsDataLimits,
        perf?: PerfOperation,
    ): Promise<void> {
        const high = limits.maxInflightBytes;
        const low = Math.floor(high / 2);
        if (socket.bufferedAmount <= high) return;
        perf?.increment({ backpressureEvents: 1 });
        while (socket.bufferedAmount > low) {
            if (this.socket !== socket || socket.readyState !== SOCKET_OPEN) {
                throw new WsDataUnavailableError("WS data socket closed under backpressure");
            }
            await new Promise<void>((resolve) => globalThis.setTimeout(resolve, 5));
        }
    }

    private failCurrent(socket: SocketLike | null, error: Error, backoff: boolean): void {
        if (socket && this.socket !== socket) return;
        const current = this.socket;
        this.socket = null;
        this.session = null;
        this.limits = null;
        if (current) {
            current.onopen = null;
            current.onmessage = null;
            current.onclose = null;
            current.onerror = null;
            try { current.close(); } catch { /* already closed */ }
        }
        for (const pending of this.pending.values()) {
            globalThis.clearTimeout(pending.timer);
            pending.reject(error);
        }
        this.pending.clear();
        this.activeRequests = 0;
        this.activeBytes = 0;
        const waiters = this.creditWaiters.splice(0);
        for (const waiter of waiters) waiter.reject(error);
        if (backoff && !this.stopped) {
            this.state = "backoff";
            this.backoffUntil = Date.now() + this.backoffMs;
            this.backoffMs = Math.min(this.backoffMs * 2, BACKOFF_CAP_MS);
        } else if (!this.stopped) {
            this.state = "off";
        }
    }

    private wsUrl(): string {
        const base = this.options.baseUrl
            .replace(/^https:/, "wss:")
            .replace(/^http:/, "ws:")
            .replace(/\/+$/, "");
        return `${base}/api/v1/ws-data`;
    }
}
