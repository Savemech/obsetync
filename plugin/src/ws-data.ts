/**
 * Optional sealed WebSocket data lane.
 *
 * The lane owns a different ticket/session/socket from realtime so object
 * traffic cannot delay presence or root notifications. Every operation is
 * content-addressed and may be retried over bulk HTTP after any failure.
 */
import { exactArrayBuffer } from "./binary";
import type { PerfOperation } from "./perf-trace";
import { yieldWork } from "./work-scheduler";
import {
    reserveTransientWorkset,
    TRANSPORT_ERROR_PAYLOAD_ALLOWANCE_BYTES,
    type TransientReservationBudget,
    type TransientWorkContext,
} from "./transient-memory";
import {
    WS_DATA_FRAME_HEADER_BYTES,
    WS_DATA_FRAGMENT_HEADER_BYTES,
    WsDataFrameType,
    decodeWsDataError,
    decodeWsDataFrame,
    decodeWsDataFragment,
    decodeWsDataHelloAck,
    decodeWsDataHelloAckV2,
    encodeWsDataFrame,
    encodeWsDataFragment,
    encodeWsDataHello,
    encodeWsDataHelloV2,
    initialWsDataLimits,
    initialWsDataV2Limits,
    wsDataFragmentCount,
    type WsDataLimits,
    type WsDataRemoteError,
    type WsDataV2Limits,
    type WsDataWireVersion,
} from "./ws-data-codec";

const CONNECT_TIMEOUT_MS = 10_000;
const REQUEST_TIMEOUT_MS = 60_000;
const BACKOFF_START_MS = 1_000;
const BACKOFF_CAP_MS = 60_000;
const MAX_CREDIT_WAITERS = 16;
const SOCKET_OPEN = 1;
const SEALED_FRAME_OVERHEAD_BYTES = WS_DATA_FRAME_HEADER_BYTES + 12 + 16;
const SEALED_FRAGMENT_OVERHEAD_BYTES = WS_DATA_FRAGMENT_HEADER_BYTES + 12 + 16;
const HANDSHAKE_PAYLOAD_BYTES = 24;
const HANDSHAKE_V2_PAYLOAD_BYTES = 32;
const MAX_CANCELLED_REQUESTS = 64;
const CANCEL_DRAIN_TIMEOUT_MS = 5_000;
const V2_REPROBE_BASE_MS = 60_000;
const V2_REPROBE_MAX_MS = 60 * 60_000;
const MAX_V2_CONTROL_FRAGMENT_BURST = 4;
export const WS_DATA_URGENT_FILE_MAX_PAYLOAD_BYTES = 64 * 1024;

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
    /** Every call must mint a fresh single-use ticket/session. */
    openSession: (wireVersion?: WsDataWireVersion) => Promise<OpenedSession>;
    preferredWireVersion?: WsDataWireVersion;
    /** Monotonic clock and OBW2 re-probe policy are injectable for deterministic recovery tests. */
    now?: () => number;
    v2ReprobeBaseMs?: number;
    v2ReprobeMaxMs?: number;
    socketFactory?: (url: string) => SocketLike;
    backoffStartMs?: number;
    /** Socket auth + sealed HELLO deadline. */
    connectTimeoutMs?: number;
    /** Injectable deadline for deterministic lifetime tests. */
    requestTimeoutMs?: number;
    /** Dynamic local cap; authenticated server credits remain the ceiling. */
    localRequestLimit?: () => number;
    /** Used by unscoped callers to reserve the response before socket send. */
    memoryBudget?: TransientReservationBudget;
}

export interface WsDataRequestTimeouts {
    /** Maximum time waiting for request/byte credits. */
    creditTimeoutMs: number;
    /** Maximum time queued behind earlier sends, socket pressure and encrypt. */
    sendTimeoutMs: number;
    /** Maximum time from socket send until the authenticated response. */
    ackTimeoutMs: number;
}

interface PendingRequest {
    expectedType: WsDataFrameType;
    payloadBytes: number;
    resolve: (payload: Uint8Array) => void;
    reject: (error: Error) => void;
    timer: ReturnType<typeof globalThis.setTimeout>;
    perf?: PerfOperation;
    maxResponseBytes: number;
    memory?: TransientWorkContext;
    nativeTasks: Promise<unknown>[];
    sent: boolean;
    sealing: boolean;
    response?: ResponseAssembly;
}

interface CancelledRequest {
    timer: ReturnType<typeof globalThis.setTimeout>;
    resolve: () => void;
    maxResponseBytes: number;
    expectedType: WsDataFrameType;
    memory?: TransientWorkContext;
    nativeTasks: Promise<unknown>[];
    response?: ResponseAssembly;
    perf?: PerfOperation;
}

interface ResponseAssembly {
    type: WsDataFrameType;
    logicalLength: number;
    fragmentCount: number;
    nextFragmentIndex: number;
    payload: Uint8Array;
}

interface ControlFrame {
    requestId: number;
    socket: SocketLike;
    resolve: () => void;
    reject: (error: Error) => void;
    perf?: PerfOperation;
}

interface V2OutboundRequest {
    requestId: number;
    type: WsDataFrameType.CheckObjects | WsDataFrameType.PutPack | WsDataFrameType.GetPack;
    payload: Uint8Array;
    schedulingLane: "control" | "urgent-file" | "bulk";
    socket: SocketLike;
    fragmentCount: number;
    nextFragmentIndex: number;
    perf?: PerfOperation;
    queued: boolean;
    onStart: () => void;
    onBufferWait: (waiting: boolean) => void;
    onFinalFragment: () => void;
    settle: (error?: Error) => void;
}

interface CreditWaiter {
    resolve: () => void;
    reject: (error: Error) => void;
}

export type WsDataLaneState = "off" | "connecting" | "ready" | "backoff";
export interface WsDataRequestOptions { priority?: "bulk" | "urgent-file" }

export class WsDataUnavailableError extends Error {}

export class WsDataCancelledError extends Error {
    constructor(message = "WS data request cancelled") {
        super(message);
        this.name = "WsDataCancelledError";
    }
}

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
    private cancelled = new Map<number, CancelledRequest>();
    private activeRequests = 0;
    private activeBytes = 0;
    private nextRequestId = 1;
    private creditWaiters: CreditWaiter[] = [];
    private txChain: Promise<void> = Promise.resolve();
    private rxChain: Promise<void> = Promise.resolve();
    private controlFrames: ControlFrame[] = [];
    private v2ControlRequests: V2OutboundRequest[] = [];
    private v2BulkRequests: V2OutboundRequest[] = [];
    private v2PumpSocket: SocketLike | null = null;
    private v2ControlBurst = 0;
    private state: WsDataLaneState = "off";
    private stopped = false;
    private backoffMs: number;
    private backoffUntil = 0;
    private cancelConnectAttempt: ((error: Error) => void) | null = null;
    private readonly abandonedSessionMints = new Set<WsDataWireVersion>();
    private readonly preferredWireVersion: WsDataWireVersion;
    private readonly now: () => number;
    private lastClockNow = 0;
    private readonly v2ReprobeBaseMs: number;
    private readonly v2ReprobeMaxMs: number;
    private v2ReprobeDelayMs: number;
    private nextV2ProbeAt = 0;
    private negotiatedV2: WsDataV2Limits | null = null;
    private readonly requested: WsDataLimits;
    private readonly socketFactory: (url: string) => SocketLike;

    constructor(private readonly options: WsDataLaneOptions) {
        const requested = initialWsDataLimits(options.runtime, options.advertisedPayloadBytes);
        if (!requested) throw new WsDataUnavailableError("server advertised invalid WS limits");
        this.requested = requested;
        if (options.requestTimeoutMs !== undefined &&
            (!Number.isSafeInteger(options.requestTimeoutMs) || options.requestTimeoutMs < 1)) {
            throw new RangeError("invalid WS request timeout");
        }
        if (options.connectTimeoutMs !== undefined &&
            (!Number.isSafeInteger(options.connectTimeoutMs) || options.connectTimeoutMs < 1)) {
            throw new RangeError("invalid WS connect timeout");
        }
        this.backoffMs = options.backoffStartMs ?? BACKOFF_START_MS;
        this.preferredWireVersion = options.preferredWireVersion ?? 1;
        this.now = options.now ?? (() => globalThis.performance?.now?.() ?? Date.now());
        this.v2ReprobeBaseMs = options.v2ReprobeBaseMs ?? V2_REPROBE_BASE_MS;
        this.v2ReprobeMaxMs = options.v2ReprobeMaxMs ?? V2_REPROBE_MAX_MS;
        if (!Number.isSafeInteger(this.v2ReprobeBaseMs) || this.v2ReprobeBaseMs < 1 ||
            !Number.isSafeInteger(this.v2ReprobeMaxMs) ||
            this.v2ReprobeMaxMs < this.v2ReprobeBaseMs) {
            throw new RangeError("invalid OBW2 re-probe interval");
        }
        this.v2ReprobeDelayMs = this.v2ReprobeBaseMs;
        this.socketFactory = options.socketFactory ??
            ((url) => new WebSocket(url) as unknown as SocketLike);
    }

    getState(): WsDataLaneState {
        return this.state;
    }

    getLimits(): WsDataLimits | null {
        return this.limits ? { ...this.limits } : null;
    }

    getWireVersion(): WsDataWireVersion | null {
        return this.limits ? (this.negotiatedV2 ? 2 : 1) : null;
    }

    close(): void {
        this.stopped = true;
        const error = new WsDataUnavailableError("WS data lane stopped");
        const cancelConnect = this.cancelConnectAttempt;
        if (cancelConnect) cancelConnect(error);
        else this.failCurrent(this.socket, error, false);
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
        memory?: TransientWorkContext,
        maxResponseBytes?: number,
        phaseTimeouts?: WsDataRequestTimeouts,
        signal?: AbortSignal,
        requestOptions?: WsDataRequestOptions,
    ): Promise<Uint8Array> {
        this.throwIfCancelled(signal);
        const requestedPriority = requestOptions?.priority ?? "bulk";
        if ((requestedPriority !== "bulk" && requestedPriority !== "urgent-file") ||
            (requestedPriority === "urgent-file" &&
                (type !== WsDataFrameType.PutPack ||
                    payload.byteLength > WS_DATA_URGENT_FILE_MAX_PAYLOAD_BYTES))) {
            throw new RangeError("invalid WS urgent-file request");
        }
        const schedulingLane = type === WsDataFrameType.CheckObjects
            ? "control" : requestedPriority;
        const defaultTimeout = this.options.requestTimeoutMs ?? REQUEST_TIMEOUT_MS;
        const timeouts = phaseTimeouts ?? {
            creditTimeoutMs: defaultTimeout,
            sendTimeoutMs: defaultTimeout,
            ackTimeoutMs: defaultTimeout,
        };
        for (const [name, value] of Object.entries(timeouts)) {
            if (!Number.isSafeInteger(value) || value < 1) {
                throw new RangeError(`invalid WS ${name}`);
            }
        }
        const endConnect = perf?.phase("ws_connect");
        let limits: WsDataLimits;
        try {
            limits = await this.waitWithSignal(this.ensureConnected(perf), signal);
        } finally {
            endConnect?.();
        }
        if (payload.byteLength > limits.maxPayloadBytes) {
            throw new WsDataUnavailableError("RPC payload exceeds negotiated WS frame limit");
        }
        if (maxResponseBytes !== undefined &&
            (!Number.isSafeInteger(maxResponseBytes) || maxResponseBytes < 0)) {
            throw new RangeError("invalid WS response byte bound");
        }
        const responseCap = Math.min(limits.maxPayloadBytes,
            Math.max(TRANSPORT_ERROR_PAYLOAD_ALLOWANCE_BYTES,
                maxResponseBytes ?? limits.maxPayloadBytes));
        // Browser/WebView creates the incoming ArrayBuffer before `onmessage`.
        // Reserve queued plaintext/sealed transmit buffers plus that initial
        // receive frame and its maximum opened plaintext before socket send.
        // Scoped production callers already own a larger complete workset.
        const queueReservationBytes = this.negotiatedV2
            ? 2 * (payload.byteLength + wsDataFragmentCount(
                payload.byteLength,
                this.negotiatedV2.maxFragmentPayloadBytes,
            ) * SEALED_FRAGMENT_OVERHEAD_BYTES) +
                2 * (responseCap + wsDataFragmentCount(
                    responseCap,
                    this.negotiatedV2.maxFragmentPayloadBytes,
                ) * SEALED_FRAGMENT_OVERHEAD_BYTES)
            : 2 * (payload.byteLength + SEALED_FRAME_OVERHEAD_BYTES + WS_DATA_FRAME_HEADER_BYTES) +
                2 * (responseCap + SEALED_FRAME_OVERHEAD_BYTES + WS_DATA_FRAME_HEADER_BYTES);
        const queueLease = memory ? null : await (
            this.options.memoryBudget?.reserve(queueReservationBytes, { signal }) ??
            reserveTransientWorkset(queueReservationBytes, { signal })
        );
        try {
        const endCredit = perf?.phase("ws_credit_wait");
        try {
            await this.acquireCredit(
                payload.byteLength,
                limits,
                perf,
                timeouts.creditTimeoutMs,
                signal,
            );
        } finally {
            endCredit?.();
        }
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
        const nativeTasks: Promise<unknown>[] = [];
        const endSendQueue = perf?.phase("ws_send_queue");
        let endAckWait: (() => void) | undefined;
        let endBufferWait: (() => void) | undefined;
        const finishWaits = () => {
            endSendQueue?.();
            endAckWait?.();
            endBufferWait?.();
        };
        const promise = new Promise<Uint8Array>((resolve, reject) => {
            const timer = globalThis.setTimeout(() => {
                this.failCurrent(
                    socket,
                    new WsDataUnavailableError("WS data send phase timed out; retry by content hash"),
                    true,
                );
            }, timeouts.sendTimeoutMs);
            this.pending.set(requestId, {
                expectedType,
                payloadBytes: payload.byteLength,
                resolve: (response) => { finishWaits(); resolve(response); },
                reject: (error) => { finishWaits(); reject(error); },
                timer,
                perf,
                maxResponseBytes: responseCap,
                memory,
                nativeTasks,
                sent: false,
                sealing: false,
            });
        });

        const cancel = () => this.cancelPending(
            requestId,
            socket,
            new WsDataCancelledError(),
            Math.min(CANCEL_DRAIN_TIMEOUT_MS, timeouts.ackTimeoutMs),
        );
        signal?.addEventListener("abort", cancel, { once: true });
        if (signal?.aborted) cancel();

        let sendTask: Promise<void>;
        const v2 = this.negotiatedV2;
        if (v2) {
            sendTask = this.enqueueV2Request({
                requestId,
                type,
                payload,
                schedulingLane,
                socket,
                fragmentCount: wsDataFragmentCount(
                    payload.byteLength,
                    v2.maxFragmentPayloadBytes,
                ),
                nextFragmentIndex: 0,
                perf,
                queued: true,
                onStart: () => endSendQueue?.(),
                onBufferWait: (waiting) => {
                    if (waiting) endBufferWait = perf?.phase("ws_buffer_wait");
                    else {
                        endBufferWait?.();
                        endBufferWait = undefined;
                    }
                },
                onFinalFragment: () => {
                    const pending = this.pending.get(requestId);
                    if (!pending) return;
                    endAckWait = perf?.phase("ws_ack_wait");
                    globalThis.clearTimeout(pending.timer);
                    pending.timer = globalThis.setTimeout(() => {
                        this.failCurrent(
                            socket,
                            new WsDataUnavailableError(
                                "WS data ACK phase timed out; retry by content hash",
                            ),
                            true,
                        );
                    }, timeouts.ackTimeoutMs);
                },
                settle: () => {},
            });
        } else {
            this.txChain = this.txChain
                .then(async () => {
                    endSendQueue?.();
                    await this.drainControlFrames(socket, session, limits, null);
                    if (this.socket !== socket || !this.pending.has(requestId)) return;
                    if (socket.bufferedAmount > limits.maxInflightBytes) {
                        endBufferWait = perf?.phase("ws_buffer_wait");
                    }
                    try {
                        await this.waitForBufferedAmount(socket, limits, requestId, perf);
                    } finally {
                        endBufferWait?.();
                    }
                    if (this.socket !== socket || !this.pending.has(requestId)) return;
                    const plaintext = encodeWsDataFrame(
                        type,
                        requestId,
                        payload,
                        limits.maxPayloadBytes,
                    );
                    const sealing = this.pending.get(requestId);
                    if (sealing) sealing.sealing = true;
                    const endEncrypt = perf?.phase("encrypt");
                    let sealed: Uint8Array;
                    try {
                        sealed = await session.sealBytes(plaintext);
                    } finally {
                        const current = this.pending.get(requestId);
                        if (current) current.sealing = false;
                        endEncrypt?.();
                    }
                    if (!this.pending.has(requestId)) {
                        this.sendCancelledSealedBoundary(
                            socket,
                            session,
                            requestId,
                            plaintext,
                            sealed,
                            perf,
                        );
                        await this.drainControlFrames(socket, session, limits, null);
                        return;
                    }
                    if (this.socket !== socket || socket.readyState !== SOCKET_OPEN) {
                        throw new WsDataUnavailableError("WS data lane changed before send");
                    }
                    const pending = this.pending.get(requestId);
                    if (pending && !pending.sent) pending.sent = true;
                    socket.send(exactArrayBuffer(sealed));
                    if (pending && this.pending.has(requestId)) {
                        endAckWait = perf?.phase("ws_ack_wait");
                        globalThis.clearTimeout(pending.timer);
                        pending.timer = globalThis.setTimeout(() => {
                            this.failCurrent(
                                socket,
                                new WsDataUnavailableError(
                                    "WS data ACK phase timed out; retry by content hash",
                                ),
                                true,
                            );
                        }, timeouts.ackTimeoutMs);
                    }
                    perf?.increment({
                        wsFrameCount: 1,
                        plaintextBytesSent: plaintext.byteLength,
                        wireBytesSent: sealed.byteLength,
                    });
                })
                .catch((error) => {
                    this.failCurrent(
                        socket,
                        error instanceof Error ? error : new Error(String(error)),
                        true,
                    );
                });
            sendTask = this.txChain;
        }
        nativeTasks.push(memory ? memory.track(() => sendTask) : sendTask);
        try {
            return await promise;
        } finally {
            signal?.removeEventListener("abort", cancel);
            // A disconnect/timeout can reject the RPC while WebCrypto is
            // still reading the old buffers. Do not reuse its child quota for
            // HTTP fallback until the actual queued native jobs have settled.
            // Cancellation can attach an authenticated receive task after the
            // caller promise has rejected. Drain batches until the tombstone
            // has made the task list stable; Promise.allSettled snapshots an
            // array and would otherwise miss tasks appended while awaiting it.
            let drainedTasks = 0;
            while (drainedTasks < nativeTasks.length) {
                const batch = nativeTasks.slice(drainedTasks);
                drainedTasks += batch.length;
                await Promise.allSettled(batch);
            }
        }
        } finally {
            queueLease?.release();
        }
    }

    private async ensureConnected(perf?: PerfOperation): Promise<WsDataLimits> {
        if (this.stopped) throw new WsDataUnavailableError("WS data lane is stopped");
        if (this.state === "ready" && this.limits && this.socket?.readyState === SOCKET_OPEN) {
            if (!this.shouldReprobeV2()) return this.limits;
            // Upgrade only at an idle request boundary. Closing the V1 socket
            // first prevents mixed-version frames and forces the next attempt
            // to mint an independent ticket/session/sequence space.
            this.failCurrent(
                this.socket,
                new WsDataUnavailableError("retiring idle OBW1 lane for bounded OBW2 probe"),
                false,
            );
        }
        if (this.clockNow() < this.backoffUntil) {
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

    private async connect(perf?: PerfOperation): Promise<WsDataLimits> {
        if (this.preferredWireVersion === 2 &&
            !this.abandonedSessionMints.has(2) &&
            this.clockNow() >= this.nextV2ProbeAt) {
            try {
                const limits = await this.connectAttempt(2, perf);
                this.nextV2ProbeAt = 0;
                this.v2ReprobeDelayMs = this.v2ReprobeBaseMs;
                return limits;
            } catch (error) {
                if (this.stopped) throw error;
                // An OBW2 attempt consumes its ticket and AEAD sequence even
                // when an old peer immediately closes. Retry OBW1 only with a
                // freshly minted session. Further probes use bounded
                // exponential hysteresis instead of permanent downgrade or
                // request-by-request version ping-pong.
                this.nextV2ProbeAt = Math.min(
                    Number.MAX_SAFE_INTEGER,
                    this.clockNow() + this.v2ReprobeDelayMs,
                );
                this.v2ReprobeDelayMs = Math.min(
                    this.v2ReprobeMaxMs,
                    this.v2ReprobeDelayMs * 2,
                );
                this.backoffUntil = 0;
            }
        }
        return this.connectAttempt(1, perf);
    }

    private clockNow(): number {
        const sampled = this.now();
        if (Number.isFinite(sampled) && sampled >= this.lastClockNow) {
            this.lastClockNow = sampled;
        }
        return this.lastClockNow;
    }

    private shouldReprobeV2(): boolean {
        return this.preferredWireVersion === 2 && this.negotiatedV2 === null &&
            !this.abandonedSessionMints.has(2) &&
            this.nextV2ProbeAt > 0 && this.clockNow() >= this.nextV2ProbeAt &&
            this.pending.size === 0 && this.cancelled.size === 0 &&
            this.controlFrames.length === 0 && this.activeRequests === 0;
    }

    private connectAttempt(
        wireVersion: WsDataWireVersion,
        perf?: PerfOperation,
    ): Promise<WsDataLimits> {
        if (this.abandonedSessionMints.has(wireVersion)) {
            return Promise.reject(new WsDataUnavailableError(
                `previous OBW${wireVersion} session mint is still pending`,
            ));
        }
        this.state = "connecting";
        return new Promise<WsDataLimits>((resolve, reject) => {
            let settled = false;
            let abandoned = false;
            let sessionMintSettled = false;
            let attemptSocket: SocketLike | null = null;
            // Per-connection accounting: old decrypt finalizers must not
            // subtract from a replacement socket's receive queue.
            let queuedReceiveBytes = 0;
            let queuedReceiveFrames = 0;
            let timeout: ReturnType<typeof globalThis.setTimeout> | null = null;
            let cancelAttempt!: (error: Error) => void;
            const clearAttempt = () => {
                if (timeout !== null) {
                    globalThis.clearTimeout(timeout);
                    timeout = null;
                }
                if (this.cancelConnectAttempt === cancelAttempt) {
                    this.cancelConnectAttempt = null;
                }
            };
            const rejectConnect = (error: Error, socket: SocketLike | null) => {
                if (!settled) {
                    settled = true;
                    abandoned = true;
                    if (!sessionMintSettled) {
                        // We cannot force an arbitrary host callback to cancel.
                        // Keep at most one detached mint per wire version and
                        // suppress another attempt until its Promise settles.
                        this.abandonedSessionMints.add(wireVersion);
                    }
                    clearAttempt();
                    reject(error);
                }
                this.failCurrent(socket, error, true);
            };
            cancelAttempt = (error) => rejectConnect(error, attemptSocket);
            this.cancelConnectAttempt = cancelAttempt;
            timeout = globalThis.setTimeout(() => {
                rejectConnect(
                    new WsDataUnavailableError("WS data session/handshake timed out"),
                    attemptSocket,
                );
            }, this.options.connectTimeoutMs ?? CONNECT_TIMEOUT_MS);

            let opened: Promise<OpenedSession>;
            try {
                opened = this.options.openSession(wireVersion);
            } catch (error) {
                sessionMintSettled = true;
                rejectConnect(
                    error instanceof Error ? error : new Error(String(error)),
                    null,
                );
                return;
            }
            void opened.then((openedSession) => {
                sessionMintSettled = true;
                this.abandonedSessionMints.delete(wireVersion);
                const { ticket, session } = openedSession;
                // A timeout/stop may win while ticket minting is still in
                // flight. Never construct a socket from that consumed,
                // cryptographically stale session after a replacement attempt.
                if (abandoned) return;
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
                attemptSocket = socket;
                socket.binaryType = "arraybuffer";
                this.socket = socket;
                this.session = session;
                this.limits = null;
                this.negotiatedV2 = null;
                this.txChain = Promise.resolve();
                this.rxChain = Promise.resolve();

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
                        const hello = wireVersion === 2
                            ? encodeWsDataFragment(
                                WsDataFrameType.Hello,
                                0,
                                encodeWsDataHelloV2(initialWsDataV2Limits(this.requested)),
                                0,
                                initialWsDataV2Limits(this.requested),
                            )
                            : encodeWsDataFrame(
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
                    if (this.socket !== socket) return;
                    if (!(event.data instanceof ArrayBuffer)) {
                        rejectConnect(
                            new WsDataUnavailableError("WS data server sent plaintext after auth"),
                            socket,
                        );
                        return;
                    }
                    const wire = new Uint8Array(event.data);
                    const requests = [...this.pending.values()];
                    const cancelled = [...this.cancelled.values()];
                    const handshake = !this.limits;
                    const owners = [...requests, ...cancelled];
                    const v2 = this.negotiatedV2;
                    const frameCaps = owners.map((request) => v2
                        ? Math.min(request.maxResponseBytes, v2.maxFragmentPayloadBytes) +
                            SEALED_FRAGMENT_OVERHEAD_BYTES
                        : request.maxResponseBytes + SEALED_FRAME_OVERHEAD_BYTES);
                    const queueCaps = owners.map((request) => v2
                        ? request.maxResponseBytes + wsDataFragmentCount(
                            request.maxResponseBytes,
                            v2.maxFragmentPayloadBytes,
                        ) * SEALED_FRAGMENT_OVERHEAD_BYTES
                        : request.maxResponseBytes + SEALED_FRAME_OVERHEAD_BYTES);
                    const frameCap = handshake
                        ? (wireVersion === 2
                            ? HANDSHAKE_V2_PAYLOAD_BYTES + SEALED_FRAGMENT_OVERHEAD_BYTES
                            : HANDSHAKE_PAYLOAD_BYTES + SEALED_FRAME_OVERHEAD_BYTES)
                        : Math.max(0, ...frameCaps);
                    const queueCap = handshake
                        ? frameCap
                        : queueCaps.reduce((sum, cap) => sum + cap, 0);
                    const queueCountCap = handshake
                        ? 1
                        : v2
                            ? owners.reduce((sum, request) => sum + wsDataFragmentCount(
                                request.maxResponseBytes,
                                v2.maxFragmentPayloadBytes,
                            ), 0)
                            : owners.length;
                    // Browser allocation precedes onmessage; bound all further
                    // queued retention/ciphertext copies before decryption.
                    if (wire.byteLength > frameCap || queuedReceiveBytes + wire.byteLength > queueCap ||
                        queuedReceiveFrames >= queueCountCap) {
                        rejectConnect(new WsDataUnavailableError(
                            "WS data receive queue exceeds its admitted byte/count bound"), socket);
                        return;
                    }
                    queuedReceiveBytes += wire.byteLength;
                    queuedReceiveFrames++;
                    let completedCancellation: CancelledRequest | null = null;
                    const receiveTask = this.rxChain.then(async () => {
                        if (this.socket !== socket) return;
                        const decryptStarted = globalThis.performance?.now?.() ?? Date.now();
                        const plaintext = await session.openBytes(wire);
                        // Decryption can finish after disconnect/reconnect. An
                        // old HELLO_ACK must not install limits in a new session.
                        if (this.socket !== socket) return;
                        const decryptMs = Math.max(0,
                            (globalThis.performance?.now?.() ?? Date.now()) - decryptStarted);
                        if (!this.limits) {
                            perf?.increment({ wsFrameCount: 1 });
                            if (wireVersion === 2) {
                                const requestedV2 = initialWsDataV2Limits(this.requested);
                                const fragment = decodeWsDataFragment(plaintext, requestedV2);
                                if (fragment.type !== WsDataFrameType.HelloAck ||
                                    fragment.requestId !== 0 || fragment.fragmentCount !== 1) {
                                    throw new WsDataUnavailableError("WS data expected OBW2 HELLO_ACK");
                                }
                                this.negotiatedV2 = decodeWsDataHelloAckV2(
                                    fragment.payload,
                                    requestedV2,
                                );
                                this.limits = this.negotiatedV2;
                            } else {
                                const frame = decodeWsDataFrame(
                                    plaintext,
                                    this.requested.maxPayloadBytes,
                                );
                                if (frame.type !== WsDataFrameType.HelloAck || frame.requestId !== 0) {
                                    throw new WsDataUnavailableError("WS data expected HELLO_ACK");
                                }
                                this.limits = decodeWsDataHelloAck(frame.payload, this.requested);
                            }
                            this.state = "ready";
                            this.backoffMs = this.options.backoffStartMs ?? BACKOFF_START_MS;
                            this.backoffUntil = 0;
                            if (!settled) {
                                settled = true;
                                clearAttempt();
                                resolve(this.limits);
                            }
                            return;
                        }
                        if (this.negotiatedV2) {
                            const fragment = decodeWsDataFragment(plaintext, this.negotiatedV2);
                            completedCancellation = this.handleResponseFragment(
                                fragment,
                                wire.byteLength,
                                plaintext.byteLength,
                                decryptMs,
                            );
                        } else {
                            const frame = decodeWsDataFrame(plaintext, this.limits.maxPayloadBytes);
                            completedCancellation = this.handleResponse(
                                frame,
                                wire.byteLength,
                                plaintext.byteLength,
                                decryptMs,
                            );
                        }
                    }).finally(() => {
                        queuedReceiveBytes -= wire.byteLength;
                        queuedReceiveFrames--;
                        completedCancellation?.resolve();
                    });
                    // The request id is encrypted. Until authenticated decode,
                    // retain each distinct pending workspace conservatively.
                    const tracked = new Map<TransientWorkContext, Promise<void>>();
                    for (const owner of [...requests, ...cancelled]) {
                        if (!owner.memory) {
                            owner.nativeTasks.push(receiveTask);
                            continue;
                        }
                        let task = tracked.get(owner.memory);
                        if (!task) {
                            task = owner.memory.track(() => receiveTask);
                            tracked.set(owner.memory, task);
                            void task.catch(() => {});
                        }
                        owner.nativeTasks.push(task);
                    }
                    this.rxChain = receiveTask.catch((error) => rejectConnect(
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
            }).catch((error) => {
                sessionMintSettled = true;
                this.abandonedSessionMints.delete(wireVersion);
                if (abandoned) return;
                rejectConnect(
                    error instanceof Error ? error : new Error(String(error)),
                    attemptSocket,
                );
            });
        });
    }

    private handleResponseFragment(
        fragment: ReturnType<typeof decodeWsDataFragment>,
        wireBytes: number,
        plaintextBytes: number,
        decryptMs: number,
    ): CancelledRequest | null {
        if (fragment.requestId === 0) {
            throw new WsDataUnavailableError("OBW2 response reused handshake id");
        }
        const pending = this.pending.get(fragment.requestId);
        const cancelled = this.cancelled.get(fragment.requestId);
        const owner = pending ?? cancelled;
        if (!owner) {
            throw new WsDataUnavailableError("OBW2 response has no matching request");
        }
        if (fragment.logicalLength > owner.maxResponseBytes) {
            throw new WsDataUnavailableError("OBW2 response exceeds its request byte bound");
        }
        if (fragment.type !== WsDataFrameType.Error && fragment.type !== owner.expectedType) {
            throw new WsDataUnavailableError("OBW2 response type mismatch");
        }
        let response = owner.response;
        if (fragment.fragmentIndex === 0) {
            // Cancellation can overtake a partly sent normal response. The
            // server then starts a canonical Error terminal at index zero for
            // the same request id; replacing only that partial normal stream
            // is the one permitted restart.
            if (response && (!cancelled || fragment.type !== WsDataFrameType.Error ||
                response.type === WsDataFrameType.Error)) {
                throw new WsDataUnavailableError("duplicate OBW2 first fragment");
            }
            // The request's transient scope was reserved before socket send;
            // allocate the declared logical response only after strict header,
            // request-id, type and per-request byte-bound validation.
            response = {
                type: fragment.type,
                logicalLength: fragment.logicalLength,
                fragmentCount: fragment.fragmentCount,
                nextFragmentIndex: 0,
                payload: new Uint8Array(fragment.logicalLength),
            };
            owner.response = response;
        }
        if (!response || response.type !== fragment.type ||
            response.logicalLength !== fragment.logicalLength ||
            response.fragmentCount !== fragment.fragmentCount ||
            response.nextFragmentIndex !== fragment.fragmentIndex ||
            fragment.offset + fragment.payload.byteLength > response.payload.byteLength) {
            throw new WsDataUnavailableError("OBW2 response fragment sequence mismatch");
        }
        response.payload.set(fragment.payload, fragment.offset);
        response.nextFragmentIndex++;
        owner.perf?.increment({
            wsFrameCount: 1,
            wireBytesReceived: wireBytes,
            plaintextBytesReceived: plaintextBytes,
        });
        owner.perf?.addPhase("decrypt", decryptMs);
        if (response.nextFragmentIndex !== response.fragmentCount) return null;
        owner.response = undefined;
        return this.handleResponse({
            type: response.type,
            requestId: fragment.requestId,
            payload: response.payload,
        }, 0, 0, 0, false);
    }

    private handleResponse(
        frame: ReturnType<typeof decodeWsDataFrame>,
        wireBytes: number,
        plaintextBytes: number,
        decryptMs: number,
        recordTelemetry = true,
    ): CancelledRequest | null {
        if (frame.requestId === 0) {
            throw new WsDataUnavailableError("WS data response reused handshake id");
        }
        const pending = this.pending.get(frame.requestId);
        if (!pending) {
            const cancelled = this.cancelled.get(frame.requestId);
            if (cancelled) {
                if (frame.payload.byteLength > cancelled.maxResponseBytes) {
                    throw new WsDataUnavailableError(
                        "cancelled WS response exceeds its request byte bound",
                    );
                }
                if (frame.type === WsDataFrameType.Error) {
                    decodeWsDataError(frame.payload);
                } else if (frame.type !== cancelled.expectedType) {
                    throw new WsDataUnavailableError(
                        "cancelled WS response type mismatch",
                    );
                }
                this.cancelled.delete(frame.requestId);
                globalThis.clearTimeout(cancelled.timer);
                return cancelled;
            }
            throw new WsDataUnavailableError("WS data response has no matching request");
        }
        if (frame.payload.byteLength > pending.maxResponseBytes) {
            throw new WsDataUnavailableError("WS response exceeds its request byte bound");
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
        // A session outlives its first operation. Attribute response telemetry
        // to the matched RPC, not the operation that opened this socket.
        if (recordTelemetry) {
            pending.perf?.increment({
                wsFrameCount: 1,
                wireBytesReceived: wireBytes,
                plaintextBytesReceived: plaintextBytes,
            });
            pending.perf?.addPhase("decrypt", decryptMs);
        }
        this.pending.delete(frame.requestId);
        globalThis.clearTimeout(pending.timer);
        this.releaseCredit(pending.payloadBytes);
        if (remoteError) {
            pending.reject(new WsDataRpcError(remoteError));
            return null;
        }
        // `openBytes` already returned a fresh owned plaintext allocation.
        // The subarray keeps that backing store alive, so another full-page
        // copy here would only double peak receive memory.
        pending.resolve(frame.payload);
        return null;
    }

    private async acquireCredit(
        bytes: number,
        limits: WsDataLimits,
        perf?: PerfOperation,
        timeoutMs = REQUEST_TIMEOUT_MS,
        signal?: AbortSignal,
    ): Promise<void> {
        let pressureReported = false;
        const clock = () => globalThis.performance?.now?.() ?? Date.now();
        const deadline = clock() + timeoutMs;
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
            const remaining = Math.ceil(deadline - clock());
            if (remaining <= 0) {
                throw new WsDataUnavailableError(
                    "WS data credit phase timed out; retry by content hash",
                );
            }
            await new Promise<void>((resolve, reject) => {
                let settled = false;
                let waiter!: CreditWaiter;
                const cleanup = () => signal?.removeEventListener("abort", abort);
                const abort = () => {
                    if (settled) return;
                    settled = true;
                    globalThis.clearTimeout(timer);
                    const index = this.creditWaiters.indexOf(waiter);
                    if (index >= 0) this.creditWaiters.splice(index, 1);
                    cleanup();
                    reject(new WsDataCancelledError());
                };
                const timer = globalThis.setTimeout(() => {
                    if (settled) return;
                    settled = true;
                    const index = this.creditWaiters.indexOf(waiter);
                    if (index >= 0) this.creditWaiters.splice(index, 1);
                    cleanup();
                    reject(new WsDataUnavailableError(
                        "WS data credit phase timed out; retry by content hash",
                    ));
                }, remaining);
                waiter = {
                    resolve: () => {
                        if (settled) return;
                        settled = true;
                        globalThis.clearTimeout(timer);
                        cleanup();
                        resolve();
                    },
                    reject: (error) => {
                        if (settled) return;
                        settled = true;
                        globalThis.clearTimeout(timer);
                        cleanup();
                        reject(error);
                    },
                };
                this.creditWaiters.push(waiter);
                signal?.addEventListener("abort", abort, { once: true });
                if (signal?.aborted) abort();
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
        requestId: number,
        perf?: PerfOperation,
    ): Promise<void> {
        const high = limits.maxInflightBytes;
        const low = Math.floor(high / 2);
        if (socket.bufferedAmount <= high) return;
        perf?.increment({ backpressureEvents: 1 });
        while (socket.bufferedAmount > low) {
            if (!this.pending.has(requestId)) return;
            if (this.socket !== socket || socket.readyState !== SOCKET_OPEN) {
                throw new WsDataUnavailableError("WS data socket closed under backpressure");
            }
            await new Promise<void>((resolve) => globalThis.setTimeout(resolve, 5));
        }
    }

    private enqueueV2Request(request: V2OutboundRequest): Promise<void> {
        let settled = false;
        const task = new Promise<void>((resolve, reject) => {
            request.settle = (error) => {
                if (settled) return;
                settled = true;
                if (error) reject(error);
                else resolve();
            };
        });
        if (request.schedulingLane !== "bulk") {
            this.v2ControlRequests.push(request);
        } else {
            this.v2BulkRequests.push(request);
        }
        this.scheduleV2Pump(request.socket);
        return task;
    }

    private scheduleV2Pump(socket: SocketLike): void {
        if (this.v2PumpSocket === socket) return;
        this.v2PumpSocket = socket;
        const pump = this.txChain.then(() => this.runV2Pump(socket));
        this.txChain = pump.catch((cause) => {
            this.failCurrent(
                socket,
                cause instanceof Error ? cause : new Error(String(cause)),
                true,
            );
        });
        void this.txChain.finally(() => {
            if (this.v2PumpSocket !== socket) return;
            this.v2PumpSocket = null;
            if (this.socket === socket &&
                (this.v2ControlRequests.length > 0 || this.v2BulkRequests.length > 0)) {
                this.scheduleV2Pump(socket);
            }
        });
    }

    private nextV2Request(): V2OutboundRequest | undefined {
        const controlPreferred = this.v2ControlRequests.length > 0 &&
            (this.v2BulkRequests.length === 0 ||
                this.v2ControlBurst < MAX_V2_CONTROL_FRAGMENT_BURST);
        let selectedQueue = controlPreferred
            ? this.v2ControlRequests
            : this.v2BulkRequests.length > 0
                ? this.v2BulkRequests
                : this.v2ControlRequests;
        const selected = selectedQueue[0];
        if (!selected) return undefined;

        // The server rejects a newly observed request id below the highest
        // first fragment it has seen. If semantic selection would start a new
        // logical request, gate it behind any older unstarted id. Already
        // started control/bulk tails retain bounded fairness and cannot be
        // starved by a stream of newly admitted requests.
        let firstQueue: V2OutboundRequest[] | undefined;
        let firstIndex = -1;
        let firstId = selected.nextFragmentIndex === 0
            ? selected.requestId
            : Number.MAX_SAFE_INTEGER;
        for (const queue of [this.v2ControlRequests, this.v2BulkRequests]) {
            for (let index = 0; index < queue.length; index++) {
                const request = queue[index];
                if (request.nextFragmentIndex === 0 && request.requestId < firstId) {
                    firstQueue = queue;
                    firstIndex = index;
                    firstId = request.requestId;
                }
            }
        }
        if (selected.nextFragmentIndex === 0 && firstQueue && firstId < selected.requestId) {
            selectedQueue = firstQueue;
            const [request] = selectedQueue.splice(firstIndex, 1);
            if (request.schedulingLane !== "bulk") {
                this.v2ControlBurst = Math.min(
                    MAX_V2_CONTROL_FRAGMENT_BURST,
                    this.v2ControlBurst + 1,
                );
            } else {
                this.v2ControlBurst = 0;
            }
            return request;
        }
        selectedQueue.shift();
        if (selected.schedulingLane !== "bulk") {
            this.v2ControlBurst = Math.min(
                MAX_V2_CONTROL_FRAGMENT_BURST,
                this.v2ControlBurst + 1,
            );
        } else {
            this.v2ControlBurst = 0;
        }
        return selected;
    }

    private async runV2Pump(socket: SocketLike): Promise<void> {
        for (;;) {
            const session = this.session;
            const limits = this.limits;
            const v2 = this.negotiatedV2;
            if (!session || !limits || !v2 || this.socket !== socket ||
                socket.readyState !== SOCKET_OPEN) {
                throw new WsDataUnavailableError("OBW2 send scheduler lost its session");
            }
            await this.drainControlFrames(socket, session, limits, v2);
            const request = this.nextV2Request();
            if (!request) return;
            try {
                if (!this.pending.has(request.requestId)) {
                    request.settle();
                    continue;
                }
                if (request.queued) {
                    request.queued = false;
                    request.onStart();
                }
                const waiting = socket.bufferedAmount > limits.maxInflightBytes;
                if (waiting) request.onBufferWait(true);
                try {
                    await this.waitForBufferedAmount(
                        socket,
                        limits,
                        request.requestId,
                        request.perf,
                    );
                } finally {
                    if (waiting) request.onBufferWait(false);
                }
                if (!this.pending.has(request.requestId)) {
                    await this.drainControlFrames(socket, session, limits, v2);
                    request.settle();
                    continue;
                }
                const plaintext = encodeWsDataFragment(
                    request.type,
                    request.requestId,
                    request.payload,
                    request.nextFragmentIndex,
                    v2,
                );
                const sealing = this.pending.get(request.requestId);
                if (sealing) sealing.sealing = true;
                const endEncrypt = request.perf?.phase("encrypt");
                let sealed: Uint8Array;
                try {
                    sealed = await session.sealBytes(plaintext);
                } finally {
                    const current = this.pending.get(request.requestId);
                    if (current) current.sealing = false;
                    endEncrypt?.();
                }
                if (!this.pending.has(request.requestId)) {
                    this.sendCancelledSealedBoundary(
                        socket,
                        session,
                        request.requestId,
                        plaintext,
                        sealed,
                        request.perf,
                    );
                    await this.drainControlFrames(socket, session, limits, v2);
                    request.settle();
                    continue;
                }
                if (this.socket !== socket || socket.readyState !== SOCKET_OPEN) {
                    throw new WsDataUnavailableError("WS data lane changed before send");
                }
                const pending = this.pending.get(request.requestId);
                if (pending && !pending.sent) pending.sent = true;
                socket.send(exactArrayBuffer(sealed));
                request.perf?.increment({
                    wsFrameCount: 1,
                    plaintextBytesSent: plaintext.byteLength,
                    wireBytesSent: sealed.byteLength,
                });
                request.nextFragmentIndex++;
                if (request.nextFragmentIndex === request.fragmentCount) {
                    request.onFinalFragment();
                    request.settle();
                } else if (request.type === WsDataFrameType.CheckObjects) {
                    this.v2ControlRequests.push(request);
                } else {
                    this.v2BulkRequests.push(request);
                }
            } catch (cause) {
                const error = cause instanceof Error ? cause : new Error(String(cause));
                request.settle(error);
                throw error;
            }
            await yieldWork({ perf: request.perf });
        }
    }

    private failV2Requests(error: Error): void {
        // The currently selected request is deliberately not settled here:
        // its send task continues to own any live WebCrypto/buffer wait until
        // that native operation returns and the pump observes the disconnect.
        for (const request of this.v2ControlRequests.splice(0)) request.settle(error);
        for (const request of this.v2BulkRequests.splice(0)) request.settle(error);
        this.v2ControlBurst = 0;
    }

    private sendCancelledSealedBoundary(
        socket: SocketLike,
        session: BinarySession,
        requestId: number,
        plaintext: Uint8Array,
        sealed: Uint8Array,
        perf?: PerfOperation,
    ): boolean {
        // sealBytes consumes the directional AEAD sequence before its native
        // promise returns. If cancellation won during that await, dropping the
        // ciphertext would make the following CANCEL (and every later frame)
        // undecryptable. Send this final unavoidable boundary only while the
        // exact session/socket is still current, then drain CANCEL next.
        if (!this.cancelled.has(requestId) || this.socket !== socket ||
            this.session !== session || socket.readyState !== SOCKET_OPEN) return false;
        socket.send(exactArrayBuffer(sealed));
        perf?.increment({
            wsFrameCount: 1,
            plaintextBytesSent: plaintext.byteLength,
            wireBytesSent: sealed.byteLength,
        });
        return true;
    }

    private failCurrent(socket: SocketLike | null, error: Error, backoff: boolean): void {
        if (socket && this.socket !== socket) return;
        const current = this.socket;
        this.socket = null;
        this.session = null;
        this.limits = null;
        this.negotiatedV2 = null;
        this.failV2Requests(error);
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
        for (const cancelled of this.cancelled.values()) {
            globalThis.clearTimeout(cancelled.timer);
            cancelled.resolve();
        }
        this.cancelled.clear();
        const controls = this.controlFrames.splice(0);
        for (const control of controls) control.reject(error);
        this.activeRequests = 0;
        this.activeBytes = 0;
        const waiters = this.creditWaiters.splice(0);
        for (const waiter of waiters) waiter.reject(error);
        if (backoff && !this.stopped) {
            this.state = "backoff";
            this.backoffUntil = Math.min(
                Number.MAX_SAFE_INTEGER,
                this.clockNow() + this.backoffMs,
            );
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

    private async drainControlFrames(
        socket: SocketLike,
        session: BinarySession,
        limits: WsDataLimits,
        v2: WsDataV2Limits | null,
    ): Promise<void> {
        for (;;) {
            const control = this.controlFrames.shift();
            if (!control) return;
            if (control.socket !== socket || this.socket !== socket ||
                socket.readyState !== SOCKET_OPEN || !this.cancelled.has(control.requestId)) {
                control.resolve();
                continue;
            }
            try {
                const plaintext = v2
                    ? encodeWsDataFragment(
                        WsDataFrameType.Cancel,
                        control.requestId,
                        new Uint8Array(),
                        0,
                        v2,
                    )
                    : encodeWsDataFrame(
                        WsDataFrameType.Cancel,
                        control.requestId,
                        new Uint8Array(),
                        limits.maxPayloadBytes,
                    );
                const sealed = await session.sealBytes(plaintext);
                if (this.socket !== socket || socket.readyState !== SOCKET_OPEN ||
                    !this.cancelled.has(control.requestId)) {
                    control.resolve();
                    continue;
                }
                socket.send(exactArrayBuffer(sealed));
                control.perf?.increment({
                    wsFrameCount: 1,
                    plaintextBytesSent: plaintext.byteLength,
                    wireBytesSent: sealed.byteLength,
                });
                control.resolve();
            } catch (cause) {
                const error = cause instanceof Error ? cause : new Error(String(cause));
                control.reject(error);
                throw error;
            }
        }
    }

    private cancelPending(
        requestId: number,
        socket: SocketLike,
        error: WsDataCancelledError,
        drainTimeoutMs: number,
    ): void {
        const pending = this.pending.get(requestId);
        if (!pending) return;
        this.pending.delete(requestId);
        globalThis.clearTimeout(pending.timer);
        this.releaseCredit(pending.payloadBytes);
        if ((pending.sent || pending.sealing) && this.socket === socket &&
            socket.readyState === SOCKET_OPEN) {
            if (this.cancelled.size >= MAX_CANCELLED_REQUESTS) {
                this.failCurrent(
                    socket,
                    new WsDataUnavailableError("WS data cancellation queue is full"),
                    true,
                );
            } else {
                let resolve!: () => void;
                const drained = new Promise<void>((done) => { resolve = done; });
                const timer = globalThis.setTimeout(() => {
                    if (!this.cancelled.has(requestId)) return;
                    this.failCurrent(
                        socket,
                        new WsDataUnavailableError("WS data cancellation did not drain"),
                        true,
                    );
                }, drainTimeoutMs);
                this.cancelled.set(requestId, {
                    timer,
                    resolve,
                    maxResponseBytes: pending.maxResponseBytes,
                    expectedType: pending.expectedType,
                    memory: pending.memory,
                    nativeTasks: pending.nativeTasks,
                    perf: pending.perf,
                    response: pending.response,
                });
                if (pending.memory) {
                    pending.nativeTasks.push(pending.memory.track(() => drained));
                } else {
                    pending.nativeTasks.push(drained);
                }
                let resolveControl!: () => void;
                let rejectControl!: (error: Error) => void;
                const controlTask = new Promise<void>((resolve, reject) => {
                    resolveControl = resolve;
                    rejectControl = reject;
                });
                this.controlFrames.push({
                    requestId,
                    socket,
                    resolve: resolveControl,
                    reject: rejectControl,
                    perf: pending.perf,
                });
                pending.nativeTasks.push(pending.memory
                    ? pending.memory.track(() => controlTask)
                    : controlTask);
                this.txChain = this.txChain.then(async () => {
                    const session = this.session;
                    const limits = this.limits;
                    if (!session || !limits) return;
                    await this.drainControlFrames(socket, session, limits, this.negotiatedV2);
                }).catch((cause) => this.failCurrent(
                    socket,
                    cause instanceof Error ? cause : new Error(String(cause)),
                    true,
                ));
            }
        }
        pending.reject(error);
    }

    private throwIfCancelled(signal?: AbortSignal): void {
        if (signal?.aborted) throw new WsDataCancelledError();
    }

    private waitWithSignal<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
        if (!signal) return promise;
        this.throwIfCancelled(signal);
        return new Promise<T>((resolve, reject) => {
            let settled = false;
            const abort = () => {
                if (settled) return;
                settled = true;
                reject(new WsDataCancelledError());
            };
            signal.addEventListener("abort", abort, { once: true });
            void promise.then((value) => {
                if (settled) return;
                settled = true;
                signal.removeEventListener("abort", abort);
                resolve(value);
            }, (error) => {
                if (settled) return;
                settled = true;
                signal.removeEventListener("abort", abort);
                reject(error);
            });
        });
    }
}
