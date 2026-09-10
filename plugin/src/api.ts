import { requestUrl, RequestUrlParam } from "obsidian";
import {
    ObsetyncSecureChannel,
    ObsetyncSecureTransportError,
    ObsetyncSessionStaleError,
    ObsetyncWsSession,
    extractRequestNonce,
    generateWsEphKeypair,
} from "./secure";
import { DurableSequenceAllocator } from "./transport-sequence";
import { validateFileDeltas } from "./delta-validation";
import { exactArrayBuffer } from "./binary";
import {
    isReenrollmentRequiredError,
    isUpgradeRequiredError,
    reenrollmentRequired,
    upgradeRequired,
} from "./transport-errors";
import type { PerfOperation, PerfPhase } from "./perf-trace";
import { getHashTuning } from "./hash-runtime";
import {
    estimateTransportWorkset,
    estimateRootOutcomeWorkset,
    reserveTransientScope,
    TRANSPORT_ERROR_PAYLOAD_ALLOWANCE_BYTES,
    transientMemorySnapshot,
    type TransientWorkContext,
    type TransientWorkScope,
    type TransientReservationBudget,
} from "./transient-memory";
import {
    BulkObjectKind,
    BulkUploadStatus,
    BULK_MAX_OBJECTS,
    BULK_MOBILE_MAX_BYTES,
    BULK_SERVER_MAX_BYTES,
    bulkBitmapBytes,
    bulkPackEncodedLength,
    decodeBulkCheckResponse,
    decodeBulkDownloadResponse,
    decodeBulkUploadAck,
    encodeBulkCheckRequest,
    encodeBulkGetRequest,
    encodeBulkUploadPack,
    negotiateBulkLimits,
    planBulkUploadSteps,
    splitBulkUploadRecords,
    type BulkCodecLimits,
    type BulkRuntime,
    type BulkUploadRecord,
} from "./bulk-codec";
import {
    ObsetyncWsDataLane,
    WS_DATA_URGENT_FILE_MAX_PAYLOAD_BYTES,
    WsDataCancelledError,
    WsDataRpcError,
} from "./ws-data";
import {
    WsDataErrorCode,
    WsDataFrameType,
    negotiateWsDataCapability,
    type WsDataWireVersion,
} from "./ws-data-codec";
import {
    TransportRouter,
    type TransportAttemptMeasurements,
    type TransportPhaseTimeouts,
} from "./transport-router";
import {
    decodeDiffPage,
    encodeDiffPageRequest,
    negotiateDiffPageLimits,
    type DiffPage,
    type DiffPageLimits,
} from "./diff-page-codec";
import {
    decodeTreeNegotiation,
    type TreeNegotiation,
} from "./tree-negotiation";
import {
    ROOT_CAPABILITY_MAX_BYTES, ROOT_QUERY_MAX_BYTES, ROOT_RESPONSE_MAX_BYTES,
    decodeRootOutcomeBytes, decodeRootOutcomeCapabilities, detachRootCommitIntent,
    detachRootCancelRequest, detachRootOutcomeIdentity, encodeRootCancel, encodeRootCommitIntent, encodeRootOutcomeQuery,
    parseRootProtocolJSON, validateRootScopeId,
    rootCommitIntentEncodedLength,
    type RootCancelRequest, type RootCommitIntent, type RootOutcome, type RootOutcomeCapabilities,
    type RootOutcomeIdentity, type RootTerminalOutcome,
} from "./root-outcome";

export { BulkObjectKind, type BulkUploadRecord } from "./bulk-codec";
export type { DiffPage } from "./diff-page-codec";
export type { TreeNegotiation } from "./tree-negotiation";

export interface FileDelta {
    action: "added" | "modified" | "deleted" | "renamed";
    path: string;
    old_path?: string;
    hash?: string;
    size?: number;
    /** Server-side mtime of the entry. Present on added/modified deltas from
     *  servers ≥ 1.4.0. Required to rebase the local Merkle tree to the exact
     *  server root after a pull (leaf hashes cover mtime); when absent the
     *  rebase still runs but root parity can't be byte-verified. */
    mtime_ms?: number;
}

/** One unmergeable same-file divergence from a server-side merge. The server
 *  keeps side A (its current) in the tree; side B's blob stays retrievable by
 *  `side_b_hash` so the losing device can preserve its version as a conflict
 *  copy. (This mirrors the server's actual JSON — an earlier version of this
 *  type described a resolution/preserved_as flow that never existed.) */
export interface PushConflict {
    path: string;
    base_hash: string;
    side_a_hash: string;
    side_b_hash: string;
}

/** One point of the vault's server-side root history. */
export interface HistoryEntry {
    root: string;
    parent: string | null;
    created_ms: number;
    device_id: string;
    total_files: number;
    tree_version?: 1 | 2;
    current: boolean;
}

export interface PushResult {
    accepted?: boolean;
    merged?: boolean;
    root_hash: string;
    conflicts?: PushConflict[];
    auto_resolved?: number;
    /** Same-file two-sided text edits line-merged server-side (server ≥ 1.5.0). */
    text_merged?: number;
}

export interface FileManifest {
    file_hash: string;
    total_size: number;
    chunks: Array<{ hash: string; offset: number; size: number }>;
}

/** Enrollment bundle returned by the admin UI's /admin/enrollment/{code} endpoint. */
export interface EnrollmentBundle {
    device_name: string;
    device_id: string;
    bearer_token: string;
    server_box_pub: string;
    wire_version: "0x02";
    eph_endpoint: string;
    Es_pub_initial: string;
    Es_pub_valid_until: number;
}

export interface TransportPersistentState {
    wireVersion: string;
    esPub: string;
    esPubValidUntil: number;
    /** Highest sequence durably reserved, not merely the last one sent. */
    lastOutgoingSeq: number;
}

export interface TransportPersistence {
    get(): TransportPersistentState;
    update(patch: Partial<TransportPersistentState>): Promise<void>;
}

interface FetchLike {
    status: number;
    ok: boolean;
    /** Present only for the server's explicit pre-decrypt memory refusal. */
    receiveBackpressureRetryAfterMs?: number;
    arrayBuffer(): Promise<ArrayBuffer>;
    json(): Promise<any>;
}

/** An HTTP admission refusal is plaintext, so automatic replay is opt-in and
 * reserved for operations whose application semantics are independently
 * idempotent (content addressing or a durable mutation identity). */
interface ReplaySafeSealedOptions {
    receiveBackpressureReplay: "application-idempotent";
    signal?: AbortSignal;
}

interface RootOutcomeHttpRequest {
    /** Exact for root commits; a strict protocol maximum for tiny query/cancel
     * bodies. The encoder is called only after complete memory admission. */
    admittedBytes: number;
    exactBytes?: number;
    encode(): Uint8Array;
    beforeSend?(): Promise<unknown>;
}

export interface OwnedObjects {
    objects: Map<string, Uint8Array>;
    /** Reuse this admitted work quota for serial verification/application. */
    memory: TransientWorkScope;
    /** Drop all references to objects before releasing their admission. */
    release(): void;
}

const BOUNDED_ERROR_BYTES = 64 * 1024;
const HTTP_RESPONSE_OVERHEAD_BYTES = 13 + 16 + 2;
const LEGACY_LARGE_OBJECT_BYTES = 4 * 1024 * 1024;
const HTTP_RECEIVE_BACKPRESSURE_HEADER = "x-obsetync-backpressure";
const HTTP_RECEIVE_BACKPRESSURE_VALUE = "receive-memory";
const MAX_HTTP_RECEIVE_RETRY_AFTER_MS = 5_000;
const REPLAY_SAFE_SEALED_OPTIONS: ReplaySafeSealedOptions = {
    receiveBackpressureReplay: "application-idempotent",
};

function replaySafeSealedOptions(signal?: AbortSignal): ReplaySafeSealedOptions {
    return signal ? { ...REPLAY_SAFE_SEALED_OPTIONS, signal } : REPLAY_SAFE_SEALED_OPTIONS;
}

/** Recognize only the application's explicit pre-decrypt refusal. An
 * arbitrary proxy 503 is ambiguous and must not cause an automatic replay.
 * Delta-seconds are capped so a forged/plaintext hint cannot park a sync
 * operation indefinitely. */
export function parseHttpReceiveBackpressureDelay(
    status: number,
    headers: Readonly<Record<string, string>> | undefined,
): number | null {
    if (status !== 503 || !headers) return null;
    let marker: string | undefined, retryAfter: string | undefined;
    for (const [name, value] of Object.entries(headers)) {
        if (name.toLowerCase() === HTTP_RECEIVE_BACKPRESSURE_HEADER) marker = value;
        else if (name.toLowerCase() === "retry-after") retryAfter = value;
    }
    if (marker?.trim().toLowerCase() !== HTTP_RECEIVE_BACKPRESSURE_VALUE ||
        retryAfter === undefined || !/^\d+$/.test(retryAfter.trim())) return null;
    const seconds = Number(retryAfter.trim());
    if (!Number.isSafeInteger(seconds) || seconds < 0) return null;
    return Math.min(MAX_HTTP_RECEIVE_RETRY_AFTER_MS, seconds * 1_000);
}

type BulkPutTransportOutcome =
    | { kind: "ack"; bytes: Uint8Array }
    | { kind: "legacy" }
    | { kind: "split" };

function uploadBackingBytes(records: readonly BulkUploadRecord[]): number {
    const buffers = new Set<ArrayBufferLike>();
    let bytes = 0;
    for (const record of records) {
        if (!buffers.has(record.data.buffer)) {
            buffers.add(record.data.buffer);
            bytes += record.data.buffer.byteLength;
        }
    }
    if (!Number.isSafeInteger(bytes)) throw new RangeError("upload source byte count overflow");
    return bytes;
}

function bulkTransportCancelled(): Error {
    const error = new Error("bulk transport cancelled");
    error.name = "AbortError";
    return error;
}

function throwIfBulkCancelled(signal?: AbortSignal): void {
    if (signal?.aborted) throw bulkTransportCancelled();
}

function waitForHttpReceiveBackpressure(delayMs: number, signal?: AbortSignal): Promise<void> {
    throwIfBulkCancelled(signal);
    return new Promise<void>((resolve, reject) => {
        let settled = false;
        const finish = (error?: Error): void => {
            if (settled) return;
            settled = true;
            signal?.removeEventListener("abort", onAbort);
            globalThis.clearTimeout(timer);
            if (error) reject(error);
            else resolve();
        };
        const onAbort = (): void => {
            finish(bulkTransportCancelled());
        };
        const timer = globalThis.setTimeout(() => finish(), delayMs);
        signal?.addEventListener("abort", onAbort, { once: true });
        // Abort dispatch is synchronous, but the signal may have changed
        // between the pre-check and listener registration.
        if (signal?.aborted) onAbort();
    });
}

/** Detach one caller's cancellation from shared single-flight work. The
 * underlying negotiation remains available to other waiters, while every
 * per-caller abort listener is removed on either settlement path. */
function waitForSharedTransport<T>(work: Promise<T>, signal?: AbortSignal): Promise<T> {
    throwIfBulkCancelled(signal);
    if (!signal) return work;
    return new Promise<T>((resolve, reject) => {
        let settled = false;
        const cleanup = (): boolean => {
            if (settled) return false;
            settled = true;
            signal.removeEventListener("abort", onAbort);
            return true;
        };
        const onAbort = (): void => {
            if (cleanup()) reject(bulkTransportCancelled());
        };
        signal.addEventListener("abort", onAbort, { once: true });
        work.then(
            (value) => { if (cleanup()) resolve(value); },
            (error) => { if (cleanup()) reject(error); },
        );
        if (signal.aborted) onAbort();
    });
}

function dataRpcTransportLane(
    type: WsDataFrameType.CheckObjects | WsDataFrameType.PutPack | WsDataFrameType.GetPack,
    putPriority: "bulk" | "urgent-file" = "bulk",
): "control" | "urgent" | "bulk" {
    // Classify by protocol meaning, never by encoded size. CHECK is the
    // bounded metadata/control operation allowed to prove WS recovery;
    // Object transfer packs remain bulk even when tiny unless the caller has
    // passed the explicit single-content/64-KiB urgent-file boundary below.
    switch (type) {
        case WsDataFrameType.CheckObjects: return "control";
        case WsDataFrameType.PutPack: return putPriority === "urgent-file" ? "urgent" : "bulk";
        case WsDataFrameType.GetPack: return "bulk";
        default: throw new Error("unclassified WS data RPC transport lane");
    }
}

export interface PutObjectsOptions { priority?: "bulk" | "urgent-file" }

function validatePutObjectsPriority(records: readonly BulkUploadRecord[],
    options?: PutObjectsOptions): "bulk" | "urgent-file" {
    const priority = options?.priority ?? "bulk";
    if (priority !== "bulk" && priority !== "urgent-file") {
        throw new RangeError("invalid bulk upload priority");
    }
    if (priority === "urgent-file" &&
        (records.length !== 1 || records[0].kind !== BulkObjectKind.Content ||
            bulkPackEncodedLength(records) > WS_DATA_URGENT_FILE_MAX_PAYLOAD_BYTES)) {
        throw new RangeError("urgent-file upload must be one bounded content object");
    }
    return priority;
}

const TRANSPORT_QUEUE_PHASES = new Set<PerfPhase>([
    "ws_credit_wait",
    "ws_send_queue",
    "ws_buffer_wait",
]);

function transportMonotonicNow(): number {
    try {
        const value = globalThis.performance?.now?.() ?? Date.now();
        return Number.isFinite(value) && value >= 0 ? value : Number.NaN;
    } catch {
        return Number.NaN;
    }
}

function transportSampleVisible(): boolean {
    try {
        return typeof document === "undefined" || document.visibilityState === "visible";
    } catch {
        return false;
    }
}

/** Add only the WS waits which are observable inside the data lane. The
 * router still owns total elapsed time and rejects impossible/overlapping
 * samples, so telemetry can never change transport correctness. */
function measureTransportQueues(
    perf: PerfOperation | undefined,
    measurements: TransportAttemptMeasurements,
): PerfOperation {
    return {
        operationId: perf?.operationId ?? "transport-sample",
        kind: perf?.kind ?? "push",
        setWorkload: (workload) => perf?.setWorkload(workload),
        setDiffPageBytes: (bytes) => perf?.setDiffPageBytes(bytes),
        increment: (delta) => perf?.increment(delta),
        phase: (name) => {
            const endPerf = perf?.phase(name);
            if (!TRANSPORT_QUEUE_PHASES.has(name)) return endPerf ?? (() => undefined);
            const started = transportMonotonicNow();
            let ended = false;
            return () => {
                if (ended) return;
                ended = true;
                endPerf?.();
                const finished = transportMonotonicNow();
                measurements.addQueueWaitMs(finished - started);
            };
        },
        addPhase: (name, durationMs) => perf?.addPhase(name, durationMs),
        observeEventLoopLag: (lagMs) => perf?.observeEventLoopLag(lagMs),
        observePeakBatchBytes: (bytes) => perf?.observePeakBatchBytes(bytes),
        setWasmChunks: (chunks) => perf?.setWasmChunks(chunks),
        setDemand: (demand) => perf?.setDemand(demand),
        finish: (outcome) => perf?.finish(outcome),
    };
}

async function withTransportSampleVisibility<T>(
    measurements: TransportAttemptMeasurements,
    work: () => Promise<T>,
): Promise<T> {
    let observedDocument: Document | null = null;
    const invalidate = () => measurements.invalidate();
    try {
        if (!transportSampleVisible()) invalidate();
        if (typeof document !== "undefined") {
            observedDocument = document;
            observedDocument.addEventListener("visibilitychange", invalidate);
        }
    } catch {
        invalidate();
    }
    try {
        return await work();
    } finally {
        if (!transportSampleVisible()) invalidate();
        try {
            observedDocument?.removeEventListener("visibilitychange", invalidate);
        } catch {
            invalidate();
        }
    }
}

/**
 * HTTP client for the ObsetyNC sync server.
 *
 * Transport uses the AEAD envelope defined in `secure.ts`:
 *   X25519 ECDH + HKDF-SHA256 + AES-256-GCM over plain HTTP. No TLS, no certs,
 *   no CA trust store involvement — the server's X25519 public key, learned
 *   at enrollment, is the only pinning.
 *
 * Identical code path on desktop (Electron) and mobile (iOS WKWebView) via
 * Obsidian's `requestUrl`. One allowlist, one transport, one bug surface.
 */
export class ObsetyncApi {
    private channel: ObsetyncSecureChannel | null = null;
    private channelPromise: Promise<ObsetyncSecureChannel> | null = null;
    private refreshPromise: Promise<void> | null = null;
    /** undefined = not negotiated this session; null = authenticated server
     *  does not offer a valid bulk-v1 path. */
    private bulkLimits: BulkCodecLimits | null | undefined;
    private capabilitiesPromise: Promise<BulkCodecLimits | null> | null = null;
    /** undefined = capabilities not fetched; null = no authenticated
     *  ws-data-v1 offer. */
    private wsDataFrameBytes: number | null | undefined;
    private wsDataWireVersion: WsDataWireVersion = 1;
    /** Authenticated binary diff page limits, independently negotiated from
     *  the bulk object path. */
    private diffPageLimits: DiffPageLimits | null | undefined;
    private wsDataLane: ObsetyncWsDataLane | null = null;
    private wsDataFallbackLogAfter = 0;
    private readonly transportRouter = new TransportRouter();
    private channelGeneration = 0;
    private readonly treeNegotiations = new Map<
        string,
        { generation: number; result: TreeNegotiation }
    >();
    private readonly treeNegotiationPromises = new Map<string, Promise<TreeNegotiation>>();
    private rootOutcomeOffer: { generation: number; result: RootOutcomeCapabilities | null } | null = null;
    private rootOutcomePromise: { generation: number; promise: Promise<RootOutcomeCapabilities | null> } | null = null;
    private readonly sequences: DurableSequenceAllocator;

    constructor(
        private readonly serverUrl: string,
        private readonly serverBoxPubBase64: string,
        private readonly bearerTokenHex: string,
        private readonly transportPersistence?: TransportPersistence,
        private readonly runtime: BulkRuntime = "desktop",
        private readonly memoryBudget?: TransientReservationBudget & {
            snapshot?(): { capacityBytes: number };
        },
    ) {
        // The sync port speaks plain HTTP — the AEAD envelope is the trust
        // boundary. Fold legacy https:// URLs down to http:// transparently
        // so users migrating from 1.0.x don't trip ERR_SSL_PROTOCOL_ERROR
        // after the server drops its cert stack.
        let u = serverUrl.replace(/\/$/, "");
        if (u.startsWith("https://")) {
            u = "http://" + u.slice("https://".length);
            console.warn(
                "[obsetync] rewrote legacy https:// server URL to http:// " +
                "(transport is plaintext HTTP + AEAD envelope)"
            );
        }
        this.serverUrl = u;
        this.sequences = new DurableSequenceAllocator({
            readReservedThrough: () =>
                this.requireTransportPersistence().get().lastOutgoingSeq,
            persistReservedThrough: async (value) => {
                await this.requireTransportPersistence().update({ lastOutgoingSeq: value });
            },
        });
    }

    /** Normalized server base URL — the WS channel derives ws:// from it. */
    get baseUrl(): string {
        return this.serverUrl;
    }

    /** Mint a single-use, short-TTL WebSocket ticket over the sealed channel,
     *  exchanging ephemeral X25519 pubkeys so both sides derive the sealed-
     *  frame session keys (wire v2). The ticket travels once, in the first
     *  (plaintext) auth frame — never in a URL. */
    async mintWsTicket(clientEphPubB64: string): Promise<{
        ticket: string;
        expires_at: number;
        server_eph_pub?: string;
    }> {
        const body = new TextEncoder().encode(
            JSON.stringify({ client_eph_pub: clientEphPubB64 }),
        );
        const res = await this.sealed("POST", "/api/v1/ws-ticket", body);
        if (!res.ok) throw new Error(`ws-ticket failed: ${res.status}`);
        return await res.json();
    }

    /** Lazily establish the ObsetyncSecureChannel. Called before the first encrypted
     *  request; subsequent requests reuse the same shared secret. */
    private async getChannel(perf?: PerfOperation): Promise<ObsetyncSecureChannel> {
        if (this.channel && !this.channel.isStale()) return this.channel;
        if (this.channelPromise) return this.channelPromise;
        if (this.refreshPromise) await this.refreshPromise;
        if (this.channel && !this.channel.isStale()) return this.channel;
        if (!this.serverBoxPubBase64) {
            throw reenrollmentRequired("ObsetyncApi: server box pubkey missing — re-enroll the device");
        }
        if (!this.bearerTokenHex) {
            throw reenrollmentRequired("ObsetyncApi: bearer token missing — re-enroll the device");
        }
        const persistence = this.requireTransportPersistence();
        this.channelPromise = (async () => {
            let state = persistence.get();
            if (state.wireVersion !== "0x02") {
                throw reenrollmentRequired(
                    "server transport upgraded to wire 0x02 — re-enroll this device",
                );
            }
            if (!state.esPub || Date.now() / 1000 >= state.esPubValidUntil - 3600) {
                await this.refreshServerEphemeral(perf);
                state = persistence.get();
            }
            this.channel = await ObsetyncSecureChannel.create(
                this.serverBoxPubBase64,
                this.bearerTokenHex,
                state.esPub,
                state.esPubValidUntil,
            );
            this.channelGeneration++;
            return this.channel;
        })();
        try {
            return await this.channelPromise;
        } finally {
            this.channelPromise = null;
        }
    }

    /**
     * Resolve all local/rotating transport state before a push starts touching
     * its candidate tree. This intentionally allocates no sequence and sends
     * no vault request; the first real request still authenticates normally.
     */
    async ensureTransportReady(perf?: PerfOperation): Promise<void> {
        await this.getChannel(perf);
    }

    /** Report Tree v2 support over the exact HTTP crypto session and learn the
     *  vault's active root format. Old servers either lack the endpoint or omit
     *  the personalized `tree` block; both safely resolve to v1. */
    async negotiateTreeVersion(
        vaultId: string,
        perf?: PerfOperation,
        force = false,
    ): Promise<TreeNegotiation> {
        await this.getChannel(perf);
        const generation = this.channelGeneration;
        const cached = this.treeNegotiations.get(vaultId);
        if (!force && cached?.generation === generation) return cached.result;
        const pending = this.treeNegotiationPromises.get(vaultId);
        if (!force && pending) return pending;

        const promise = (async (): Promise<TreeNegotiation> => {
            const report = new TextEncoder().encode(JSON.stringify({
                protocol_version: 1,
                vault_id: vaultId,
                capabilities: ["tree-v2"],
            }));
            const response = await this.sealed(
                "POST",
                "/api/v1/capabilities",
                report,
                perf,
                ROOT_CAPABILITY_MAX_BYTES,
                REPLAY_SAFE_SEALED_OPTIONS,
            );
            let result: TreeNegotiation;
            if (response.status === 404 || response.status === 405) {
                result = {
                    currentVersion: 1,
                    fleetReady: false,
                    readyDevices: 0,
                    enrolledDevices: 0,
                    activation: "blocked",
                };
            } else {
                if (!response.ok) {
                    throw new Error(`tree capability negotiation failed: ${response.status}`);
                }
                const bundle = await response.json();
                this.applyCapabilityBundle(bundle);
                result = decodeTreeNegotiation(bundle);
            }
            // `sealed` used the current channel synchronously; still fail
            // closed if a future refactor swaps it while the request awaits.
            if (this.channelGeneration === generation) {
                this.treeNegotiations.set(vaultId, { generation, result });
            }
            return result;
        })();
        this.treeNegotiationPromises.set(vaultId, promise);
        try {
            return await promise;
        } finally {
            if (this.treeNegotiationPromises.get(vaultId) === promise) {
                this.treeNegotiationPromises.delete(vaultId);
            }
        }
    }

    getTreeNegotiation(vaultId: string): TreeNegotiation | null {
        const cached = this.treeNegotiations.get(vaultId);
        return cached?.generation === this.channelGeneration ? cached.result : null;
    }

    /** The root envelope is authenticated by the sealed response and is the
     *  authoritative format observation. Keep the capability cache coherent
     *  when an admin switches a live vault after this channel negotiated. */
    observeTreeVersion(vaultId: string, currentVersion: 1 | 2): void {
        const cached = this.getTreeNegotiation(vaultId) ?? {
            currentVersion: 1 as const,
            fleetReady: false,
            readyDevices: 0,
            enrolledDevices: 0,
            activation: "blocked" as const,
        };
        if (cached.currentVersion === currentVersion) return;
        this.treeNegotiations.set(vaultId, {
            generation: this.channelGeneration,
            result: {
                ...cached,
                currentVersion,
                activation: currentVersion === 2
                    ? "active"
                    : cached.fleetReady ? "eligible" : "blocked",
            },
        });
    }

    // --- Root ---

    /** Both outcome and terminal-cancellation support are prerequisites for a
     * future durable coordinator. A cached authenticated offer is not proof of
     * current incarnation: force a fresh observation when recovering an intent,
     * and never rebind its original identity merely because this value changed. */
    async negotiateRootOutcomes(force = false, perf?: PerfOperation): Promise<RootOutcomeCapabilities | null> {
        await this.getChannel(perf);
        const generation = this.channelGeneration;
        if (force) this.rootOutcomeOffer = null;
        if (!force && this.rootOutcomeOffer?.generation === generation) {
            const result = this.rootOutcomeOffer.result;
            return result ? { ...result } : null;
        }
        if (!force && this.rootOutcomePromise?.generation === generation) {
            const result = await this.rootOutcomePromise.promise;
            return result ? { ...result } : null;
        }
        const pending = (async () => {
            const response = await this.sealed(
                "POST",
                "/api/v1/capabilities",
                new Uint8Array(),
                perf,
                ROOT_CAPABILITY_MAX_BYTES,
                REPLAY_SAFE_SEALED_OPTIONS,
            );
            if (response.status === 404 || response.status === 405) return null;
            if (!response.ok) throw new Error(`root outcome capability negotiation failed: ${response.status}`);
            return decodeRootOutcomeCapabilities(parseRootProtocolJSON(
                new Uint8Array(await response.arrayBuffer()), ROOT_CAPABILITY_MAX_BYTES,
            ));
        })();
        this.rootOutcomePromise = { generation, promise: pending };
        try {
            const result = await pending;
            if (this.rootOutcomePromise?.promise === pending && this.channelGeneration === generation) {
                this.rootOutcomeOffer = { generation, result };
            }
            return result ? { ...result } : null;
        } finally {
            if (this.rootOutcomePromise?.promise === pending) this.rootOutcomePromise = null;
        }
    }

    /** Additive, deliberately unused by the engine until durable intent/base/
     * journal coordination exists. Captures exact request and expected identity
     * before awaiting session work. Only transport counters/nonces may change
     * during sealed retries; no WS/legacy root fallback or identity rewrite. */
    async commitRootOutcome(vaultId: string, intent: RootCommitIntent, perf?: PerfOperation): Promise<RootTerminalOutcome> {
        const vault = validateRootScopeId(vaultId), captured = detachRootCommitIntent(intent);
        const expected = detachRootOutcomeIdentity({ sequence: captured.sequence,
            mutation_id: captured.mutation_id, request_hash: captured.request_hash });
        const requestBytes = rootCommitIntentEncodedLength(captured);
        const outcome = await this.rootOutcomeRequest(
            `root-commit/${encodeURIComponent(vault)}`,
            {
                admittedBytes: requestBytes,
                exactBytes: requestBytes,
                encode: () => encodeRootCommitIntent(captured),
                // Fresh tree-v2 submissions still need the current crypto
                // session's capability proof. Its response and a stale-session
                // server-eph refresh share this already-admitted workspace.
                // A 426 after this point stays an explicit failure; there is no
                // automatic application retry with a different identity.
                beforeSend: () => this.negotiateTreeVersion(vault, perf),
            },
            expected,
            perf,
        );
        if (outcome.status !== "accepted" && outcome.status !== "cancelled") {
            throw new Error("root commit returned a nonterminal outcome");
        }
        return outcome;
    }

    async queryRootOutcome(vaultId: string, identity?: RootOutcomeIdentity, perf?: PerfOperation): Promise<RootOutcome> {
        const vault = validateRootScopeId(vaultId);
        const captured = identity === undefined ? undefined : detachRootOutcomeIdentity(identity);
        return this.rootOutcomeRequest(
            `root-outcome/${encodeURIComponent(vault)}`,
            { admittedBytes: ROOT_QUERY_MAX_BYTES, encode: () => encodeRootOutcomeQuery(captured) },
            captured,
            perf,
        );
    }

    /** Conditional terminal operation, not transport abort. Current incarnation
     * authorizes cancellation; request_hash still names the ORIGINAL commit. */
    async cancelRootOutcome(vaultId: string, request: RootCancelRequest, perf?: PerfOperation): Promise<RootTerminalOutcome> {
        const vault = validateRootScopeId(vaultId), captured = detachRootCancelRequest(request);
        const expected = detachRootOutcomeIdentity({ sequence: captured.sequence,
            mutation_id: captured.mutation_id, request_hash: captured.request_hash });
        const outcome = await this.rootOutcomeRequest(
            `root-cancel/${encodeURIComponent(vault)}`,
            { admittedBytes: ROOT_QUERY_MAX_BYTES, encode: () => encodeRootCancel(captured) },
            expected,
            perf,
        );
        if (outcome.status !== "accepted" && outcome.status !== "cancelled") {
            throw new Error("root cancellation returned a nonterminal outcome");
        }
        return outcome;
    }

    private async rootOutcomeRequest(
        endpoint: string,
        request: RootOutcomeHttpRequest,
        expected?: RootOutcomeIdentity,
        perf?: PerfOperation,
    ): Promise<RootOutcome> {
        const plan = estimateRootOutcomeWorkset(request.admittedBytes, ROOT_RESPONSE_MAX_BYTES);
        const memory = await reserveTransientScope(plan, { budget: this.memoryBudget });
        try {
            return await memory.track(() => memory.run(plan.workBytes, async () => {
                await request.beforeSend?.();
                const body = request.encode();
                if (body.byteLength > request.admittedBytes ||
                    (request.exactBytes !== undefined && body.byteLength !== request.exactBytes)) {
                    throw new Error("root outcome body changed after memory admission");
                }
                // The native requestUrl receive allocation is outside this byte
                // cap. sendEncrypted enforces it before decrypt/copy; the codec
                // also bounds plaintext before JSON decoding and duplicate-key
                // validation. All transport retries reuse this one child quota.
                const response = await this.sealed(
                    "POST",
                    `/api/v1/${endpoint}`,
                    body,
                    perf,
                    ROOT_RESPONSE_MAX_BYTES,
                    REPLAY_SAFE_SEALED_OPTIONS,
                );
                if (!response.ok) throw new Error(`root outcome request failed: ${response.status}`);
                return decodeRootOutcomeBytes(new Uint8Array(await response.arrayBuffer()), expected);
            }));
        } finally {
            memory.close();
        }
    }

    private async treeSealed(
        vaultId: string,
        method: string,
        path: string,
        body: Uint8Array,
        perf?: PerfOperation,
        options?: ReplaySafeSealedOptions,
    ): Promise<FetchLike> {
        await this.negotiateTreeVersion(vaultId, perf);
        try {
            return await this.sealed(method, path, body, perf, undefined, options);
        } catch (error) {
            if (!isUpgradeRequiredError(error)) throw error;
            // The server may have restarted and intentionally forgotten its
            // process-local session proof while this client's channel stayed
            // alive. Re-report once on the same authenticated channel. A real
            // format mismatch still returns 426 on the one retry.
            this.treeNegotiations.delete(vaultId);
            await this.negotiateTreeVersion(vaultId, perf, true);
            return await this.sealed(method, path, body, perf, undefined, options);
        }
    }

    async getRoot(vaultId: string, perf?: PerfOperation): Promise<Uint8Array | null> {
        const path = `/api/v1/root/${vaultId}`;
        const res = await this.treeSealed(
            vaultId, "GET", path, new Uint8Array(), perf, REPLAY_SAFE_SEALED_OPTIONS,
        );
        if (res.status === 404) return null;
        if (!res.ok) throw new Error(`getRoot failed: ${res.status}`);
        return new Uint8Array(await res.arrayBuffer());
    }

    /** Fetch the immutable root that owns a paged-diff snapshot. */
    async getRootAt(
        vaultId: string,
        rootHash: string,
        perf?: PerfOperation,
    ): Promise<Uint8Array | null> {
        const path = `/api/v1/root/${vaultId}/${rootHash}`;
        const res = await this.treeSealed(
            vaultId, "GET", path, new Uint8Array(), perf, REPLAY_SAFE_SEALED_OPTIONS,
        );
        if (res.status === 404) return null;
        if (!res.ok) throw new Error(`getRootAt failed: ${res.status}`);
        return new Uint8Array(await res.arrayBuffer());
    }

    /** Recent root history for the rollback UI, newest first. */
    async getHistory(vaultId: string, perf?: PerfOperation): Promise<HistoryEntry[]> {
        const path = `/api/v1/history/${vaultId}`;
        const res = await this.treeSealed(
            vaultId, "GET", path, new Uint8Array(), perf, REPLAY_SAFE_SEALED_OPTIONS,
        );
        if (!res.ok) throw new Error(`getHistory failed: ${res.status}`);
        const body = await res.json();
        return (body?.roots ?? []) as HistoryEntry[];
    }

    /** Roll the vault's current root back to an earlier point in history.
     *  The server validates the hash exists; devices converge on their next
     *  pull. Deliberately bypasses the stale-tree guard server-side — this
     *  is the explicit, human-initiated revert. */
    async rollbackVault(
        vaultId: string,
        rootHash: string,
        perf?: PerfOperation,
    ): Promise<void> {
        const path = `/api/v1/rollback/${vaultId}`;
        // No automatic plaintext-marker replay: rollback deliberately bypasses
        // the stale-tree guard and has no application idempotency identity.
        const res = await this.treeSealed(
            vaultId,
            "POST",
            path,
            new TextEncoder().encode(rootHash),
            perf,
        );
        if (!res.ok) throw new Error(`rollback failed: ${res.status}`);
    }

    async putRoot(
        vaultId: string,
        rootBytes: Uint8Array,
        parentHash: string,
        perf?: PerfOperation,
    ): Promise<PushResult> {
        const path = `/api/v1/root/${vaultId}`;
        // Parent-root used to go as a header. With encryption the header would
        // be outside the AEAD envelope; prepend it to the body as a 64-char
        // ASCII hex prefix instead so the server authenticates it too.
        const header = new TextEncoder().encode(parentHash.padEnd(64, " "));
        const body = new Uint8Array(header.length + rootBytes.length);
        body.set(header, 0);
        body.set(rootBytes, header.length);
        // The legacy publication protocol has no durable mutation identity.
        // A marked 503 therefore remains ambiguous and is surfaced as-is.
        const res = await this.treeSealed(vaultId, "PUT", path, body, perf);
        if (!res.ok) throw new Error(`putRoot failed: ${res.status}`);
        return res.json();
    }

    // --- Diff ---

    async getDiff(
        vaultId: string,
        deviceRootHash: string,
        perf?: PerfOperation,
    ): Promise<FileDelta[] | null> {
        const path = `/api/v1/diff/${vaultId}`;
        // Same trick — device-root prepended to body instead of a header.
        const body = new TextEncoder().encode(deviceRootHash.padEnd(64, " "));
        const res = await this.treeSealed(
            vaultId, "POST", path, body, perf, REPLAY_SAFE_SEALED_OPTIONS,
        );
        if (res.status === 304) return null;
        // 404 = vault has no root on the server yet (fresh server, first
        // push hasn't landed). Treat as "nothing to pull" and let the push
        // path seed the vault.
        if (res.status === 404) return null;
        if (!res.ok) throw new Error(`getDiff failed: ${res.status}`);
        return validateFileDeltas(await res.json());
    }

    /** Fetch one bounded, snapshot-pinned binary diff page. `undefined`
     *  means the authenticated server lacks paged-diff-v1 and the caller
     *  should use the legacy JSON endpoint; `null` means the vault is empty. */
    async getDiffPage(
        vaultId: string,
        fromRootHash: string,
        toRootHash: string | null,
        cursor: Uint8Array | null,
        perf?: PerfOperation,
    ): Promise<DiffPage | null | undefined> {
        await this.getBulkLimits(perf);
        const limits = this.diffPageLimits;
        if (!limits) return undefined;
        perf?.setDiffPageBytes(limits.maxBytes);
        const path = `/api/v1/diff-page/${vaultId}`;
        const request = encodeDiffPageRequest(fromRootHash, toRootHash, cursor, limits);
        const response = await this.treeSealed(
            vaultId, "POST", path, request, perf, REPLAY_SAFE_SEALED_OPTIONS,
        );
        if (response.status === 404) return null;
        if (!response.ok) throw new Error(`getDiffPage failed: ${response.status}`);
        const bytes = new Uint8Array(await response.arrayBuffer());
        perf?.observePeakBatchBytes(bytes.byteLength);
        return decodeDiffPage(bytes, fromRootHash, toRootHash, cursor, limits);
    }

    async supportsPagedDiff(perf?: PerfOperation): Promise<boolean> {
        await this.getBulkLimits(perf);
        return this.diffPageLimits !== null && this.diffPageLimits !== undefined;
    }

    // --- Bounded bulk HTTP v1 ---

    /** Whether the authenticated server and this runtime share bulk-http-v1. */
    async supportsBulkHttp(perf?: PerfOperation): Promise<boolean> {
        return (await this.getBulkLimits(perf)) !== null;
    }

    async getBulkDiagnostics(): Promise<{
        enabled: boolean;
        requestBytes?: number;
        objects?: number;
        objectBytes?: number;
        wsDataEnabled?: boolean;
        wsDataState?: string;
        wsDataFrameBytes?: number;
        wsDataWireVersion?: number;
        wsCircuit?: string;
        wsCircuitOpenForMs?: number;
        lastCarrier?: "ws" | "http";
        lastCarrierReason?: string;
        lastCarrierLane?: "control" | "urgent" | "bulk";
        lastCarrierPayloadBytes?: number;
        lastCarrierExpectedUsefulBytes?: number;
        bulkPreferredCarrier?: "ws" | "http";
        bulkPreferenceEstablished?: boolean;
        wsBulkThroughputBytesPerSecond?: number;
        httpBulkThroughputBytesPerSecond?: number;
        wsBulkQueueWaitMs?: number;
        httpBulkQueueWaitMs?: number;
    }> {
        const limits = await this.getBulkLimits();
        const routing = this.transportRouter.snapshot();
        const routeDiagnostics = {
            lastCarrier: routing.lastSelectedCarrier ?? undefined,
            lastCarrierReason: routing.lastSelectionReason ?? undefined,
            lastCarrierLane: routing.lastSelectionLane ?? undefined,
            lastCarrierPayloadBytes: routing.lastSelectedCarrier === null
                ? undefined
                : routing.lastSelectionPayloadBytes,
            lastCarrierExpectedUsefulBytes: routing.lastSelectedCarrier === null
                ? undefined
                : routing.lastSelectionExpectedUsefulBytes,
            bulkPreferredCarrier: routing.bulkPreferredCarrier,
            bulkPreferenceEstablished: routing.bulkPreferenceEstablished,
            wsBulkThroughputBytesPerSecond: routing.wsBulkThroughputBytesPerSecond ?? undefined,
            httpBulkThroughputBytesPerSecond: routing.httpBulkThroughputBytesPerSecond ?? undefined,
            wsBulkQueueWaitMs: routing.wsBulkQueueWaitMs ?? undefined,
            httpBulkQueueWaitMs: routing.httpBulkQueueWaitMs ?? undefined,
        };
        return limits ? {
            enabled: true,
            requestBytes: limits.maxBytes,
            objects: limits.maxObjects,
            objectBytes: limits.maxObjectBytes,
            wsDataEnabled: this.wsDataFrameBytes !== null,
            wsDataState: this.wsDataLane?.getState() ?? "off",
            wsDataFrameBytes: this.wsDataFrameBytes ?? undefined,
            wsDataWireVersion: this.wsDataFrameBytes === null
                ? undefined
                : this.wsDataLane?.getWireVersion() ?? this.wsDataWireVersion,
            wsCircuit: routing.circuit,
            wsCircuitOpenForMs: routing.openForMs,
            ...routeDiagnostics,
        } : { enabled: false, ...routeDiagnostics };
    }

    /** Stop the optional transfer socket when the sync engine unloads. */
    closeDataLane(): void {
        this.wsDataLane?.close();
        this.wsDataLane = null;
    }

    /** A background/frozen host can retain a WebSocket object whose native
     * connection is already dead. Drop that carrier and its pre-suspension
     * performance corpus synchronously; the next bounded request establishes
     * a fresh session or uses the unchanged HTTP fallback. */
    invalidateDataTransportAfterResume(): void {
        this.closeDataLane();
        this.transportRouter.resetMeasurements();
    }

    /** Upload records in ordered, count/byte-bounded packs. Records above the
     *  v1 per-object cap use the unchanged single-object endpoint. Ordering is
     *  intentional: a manifest in the same call observes all preceding chunk
     *  ACKs before it is validated server-side. */
    async putObjects(
        records: readonly BulkUploadRecord[],
        perf?: PerfOperation,
        memory?: TransientWorkScope,
        signal?: AbortSignal,
        options?: PutObjectsOptions,
    ): Promise<void> {
        throwIfBulkCancelled(signal);
        const priority = validatePutObjectsPriority(records, options);
        if (records.length === 0) return;
        const limits = await this.getBulkLimits(perf, signal);
        const steps = limits ? planBulkUploadSteps(records, limits) :
            records.map((record) => ({ kind: "single" as const, record }));
        const ownerBytes = uploadBackingBytes(records);
        const maxPayloadBytes = steps.reduce((maximum, step) => Math.max(maximum,
            step.kind === "bulk" ? bulkPackEncodedLength(step.records) : step.record.data.byteLength,
        ), 0);
        // Bare callers have already allocated their input, so account for its
        // distinct backing buffers before making any transport copies. Batch
        // producers should pass a scope admitted before their source reads.
        const scope = memory ?? await reserveTransientScope({
            ownerBytes,
            workBytes: estimateTransportWorkset(maxPayloadBytes),
        }, { budget: this.memoryBudget, signal });
        try {
            if (scope.snapshot().ownerBytes < ownerBytes) {
                throw new RangeError("upload source exceeds its caller-owned memory allowance");
            }
            for (const step of steps) {
                if (step.kind === "bulk") {
                    if (this.bulkLimits === null) {
                        for (const record of step.records) {
                            await this.putObjectLegacy(record, perf, scope, signal);
                        }
                    } else {
                        await this.putBulkPack(step.records, limits!, perf, scope, signal, priority);
                    }
                } else {
                    await this.putObjectLegacy(step.record, perf, scope, signal);
                }
            }
        } finally {
            if (!memory) scope.close();
        }
    }

    /** An admitted receive/apply batch. The parent remains charged after this
     * method returns: the caller releases it only after verification and disk
     * application settle. Obsidian requestUrl still allocates its initial
     * native response before JavaScript can check its byte length. */
    async getObjectsOwned(
        kind: BulkObjectKind,
        hashes: readonly string[],
        perf?: PerfOperation,
        signal?: AbortSignal,
    ): Promise<OwnedObjects> {
        throwIfBulkCancelled(signal);
        const unique = [...new Set(hashes.map((hash) => hash.toLowerCase()))];
        if (unique.length > BULK_MAX_OBJECTS) {
            throw new RangeError("bulk get caller exceeded the bounded apply object cap");
        }
        const capacityBytes = this.memoryBudget?.snapshot?.().capacityBytes ??
            transientMemorySnapshot().capacityBytes;
        // The estimator is linear: retained plaintext plus six transport
        // allowances. Derive the per-batch cap before requesting any bytes so
        // recovery profiles can still receive on a smaller shared pool.
        const fittingRetention = Math.max(1,
            Math.floor((capacityBytes - estimateTransportWorkset(0)) / 7));
        const localBytes = Math.min(
            this.runtime === "mobile" ? BULK_MOBILE_MAX_BYTES : BULK_SERVER_MAX_BYTES,
            getHashTuning().maxBatchBytes,
            fittingRetention,
        );
        const retentionBytes = unique.length === 1 &&
            (kind === BulkObjectKind.ContentChunk || kind === BulkObjectKind.Manifest)
            ? Math.max(localBytes, LEGACY_LARGE_OBJECT_BYTES) : localBytes;
        const scope = await reserveTransientScope({
            ownerBytes: retentionBytes,
            workBytes: estimateTransportWorkset(retentionBytes),
        }, { budget: this.memoryBudget, signal });
        try {
            const objects = await scope.run(estimateTransportWorkset(retentionBytes),
                (work) => this.getObjectsInternal(
                    kind,
                    unique,
                    perf,
                    { work, retentionBytes },
                    signal,
                ), { signal });
            let released = false;
            return {
                objects,
                memory: scope,
                release: () => {
                    if (released) return;
                    released = true;
                    objects.clear();
                    scope.close();
                },
            };
        } catch (error) {
            scope.close();
            throw error;
        }
    }

    /** Download an ordered set through bounded cursor pages. The returned map
     *  owns copies independent of the decrypted response allocation. Callers
     *  still verify BLAKE3/manifest semantics before applying bytes to disk. */
    async getObjects(
        kind: BulkObjectKind,
        hashes: readonly string[],
        perf?: PerfOperation,
        signal?: AbortSignal,
    ): Promise<Map<string, Uint8Array>> {
        // Compatibility API: bare maps have no release boundary. Do not grant
        // and immediately release an admission behind this naked return type.
        return this.getObjectsInternal(kind, hashes, perf, undefined, signal);
    }

    private async getObjectsInternal(
        kind: BulkObjectKind,
        hashes: readonly string[],
        perf?: PerfOperation,
        memory?: { work: TransientWorkContext; retentionBytes: number },
        signal?: AbortSignal,
    ): Promise<Map<string, Uint8Array>> {
        throwIfBulkCancelled(signal);
        const unique = [...new Set(hashes.map((hash) => hash.toLowerCase()))];
        const output = new Map<string, Uint8Array>();
        if (unique.length === 0) return output;
        if (unique.length > BULK_MAX_OBJECTS) {
            throw new RangeError("bulk get caller exceeded the bounded apply object cap");
        }
        const localRetentionBytes = memory?.retentionBytes ?? (this.runtime === "mobile"
            ? BULK_MOBILE_MAX_BYTES
            : BULK_SERVER_MAX_BYTES);
        let retainedBytes = 0;
        const retain = (hash: string, data: Uint8Array): void => {
            const next = retainedBytes + data.byteLength;
            // One legacy FastCDC object (or a W4-sized manifest) may be up to
            // 4 MiB. It remains bounded and is applied immediately by the
            // large-file caller.
            const allowedSingleLegacyObject =
                unique.length === 1 &&
                (kind === BulkObjectKind.ContentChunk || kind === BulkObjectKind.Manifest) &&
                data.byteLength <= 4 * 1024 * 1024;
            if (next > localRetentionBytes && (memory || !allowedSingleLegacyObject)) {
                throw new RangeError("bulk get apply batch exceeded the local retention budget");
            }
            output.set(hash, data);
            retainedBytes = next;
        };
        const getLegacy = (hash: string): Promise<Uint8Array> => this.getObjectLegacy(
            kind, hash, perf, memory ? localRetentionBytes - retainedBytes : undefined, signal,
        );
        const negotiated = await this.getBulkLimits(perf, signal);
        const limits = negotiated && memory
            ? { ...negotiated, maxBytes: Math.min(negotiated.maxBytes, memory.retentionBytes) }
            : negotiated;
        if (!limits) {
            for (const hash of unique) {
                retain(hash, await getLegacy(hash));
            }
            return output;
        }

        const hashesPerRequest = Math.max(
            1,
            Math.min(limits.maxObjects, Math.floor((limits.maxBytes - 17) / 32)),
        );
        for (let offset = 0; offset < unique.length; offset += hashesPerRequest) {
            const group = unique.slice(offset, offset + hashesPerRequest);
            if (this.bulkLimits === null) {
                for (const hash of group) {
                    retain(hash, await getLegacy(hash));
                }
                continue;
            }
            let cursor = 0;
            while (cursor < group.length) {
                const wsBudget = Math.min(limits.maxBytes, this.wsDataFrameBytes ?? 0);
                const responseBudget = wsBudget > 0 ? wsBudget : limits.maxBytes;
                const request = encodeBulkGetRequest(
                    kind,
                    group,
                    cursor,
                    responseBudget,
                    limits.maxObjects,
                );
                const responseBytes = await this.routeDataRpc<Uint8Array | null>(
                    WsDataFrameType.GetPack,
                    request,
                    WsDataFrameType.GetResult,
                    (bytes) => bytes,
                    async () => {
                        const request = encodeBulkGetRequest(
                            kind,
                            group,
                            cursor,
                            limits.maxBytes,
                            limits.maxObjects,
                        );
                        const response = await this.sealed(
                            "POST",
                            "/api/v1/bulk/get",
                            request,
                            perf,
                            memory ? limits.maxBytes : undefined,
                            replaySafeSealedOptions(signal),
                        );
                        if (response.status === 404 || response.status === 405) {
                            this.bulkLimits = null;
                            this.wsDataFrameBytes = null;
                            this.closeDataLane();
                            for (const hash of group.slice(cursor)) {
                                retain(hash, await getLegacy(hash));
                            }
                            cursor = group.length;
                            return null;
                        }
                        if (response.status === 413) {
                            // A stored object may predate bulk-v1 and exceed its
                            // per-object cap (notably a 4 MiB FastCDC chunk). Preserve
                            // correctness through the stable single-object GET.
                            const hash = group[cursor];
                            retain(hash, await getLegacy(hash));
                            cursor++;
                            return null;
                        }
                        if (!response.ok) throw new Error(`bulk get failed: ${response.status}`);
                        return new Uint8Array(await response.arrayBuffer());
                    },
                    perf,
                    memory?.work,
                    responseBudget,
                    signal,
                );
                if (!responseBytes) continue;
                const page = decodeBulkDownloadResponse(
                    responseBytes,
                    group.length,
                    cursor,
                    limits,
                );
                if (page.nextCursor === cursor) {
                    throw new Error("bulk get made no cursor progress");
                }
                // Validate cumulative retained size before allocating owned
                // copies of this page, including a partially filled batch.
                if (memory && page.records.reduce((sum, record) => sum + record.data.byteLength,
                    retainedBytes) > localRetentionBytes) {
                    throw new RangeError("bulk get apply batch exceeded the local retention budget");
                }
                let searchFrom = cursor;
                for (const record of page.records) {
                    if (record.kind !== kind) throw new Error("bulk get returned the wrong object kind");
                    let requestIndex = searchFrom;
                    while (
                        requestIndex < page.nextCursor &&
                        group[requestIndex] !== record.hash
                    ) {
                        requestIndex++;
                    }
                    if (requestIndex >= page.nextCursor || output.has(record.hash)) {
                        throw new Error("bulk get returned an unexpected or duplicate hash");
                    }
                    retain(record.hash, record.data.slice());
                    searchFrom = requestIndex + 1;
                }
                cursor = page.nextCursor;
            }
            for (const hash of group) {
                if (!output.has(hash)) throw new Error(`bulk get ${hash}: object not found`);
            }
        }
        return output;
    }

    // --- Index chunks ---

    async getChunk(hash: string, perf?: PerfOperation): Promise<Uint8Array> {
        const res = await this.sealed("GET", `/api/v1/chunk/${hash}`, new Uint8Array(), perf,
            undefined, REPLAY_SAFE_SEALED_OPTIONS);
        if (!res.ok) throw new Error(`getChunk ${hash}: ${res.status}`);
        return new Uint8Array(await res.arrayBuffer());
    }

    async putChunk(hash: string, data: Uint8Array, perf?: PerfOperation): Promise<void> {
        const res = await this.sealed("PUT", `/api/v1/chunk/${hash}`, data, perf,
            undefined, REPLAY_SAFE_SEALED_OPTIONS);
        if (!res.ok) throw new Error(`putChunk ${hash}: ${res.status}`);
    }

    async checkChunks(hashes: string[], perf?: PerfOperation, signal?: AbortSignal,
        memory?: TransientWorkScope): Promise<string[]> {
        const bulk = await this.checkObjectsBulk(BulkObjectKind.IndexChunk, hashes, perf, signal, memory);
        if (bulk) return bulk;
        throwIfBulkCancelled(signal);
        const body = new TextEncoder().encode(JSON.stringify(hashes));
        const res = await this.sealed("POST", "/api/v1/chunks/check", body, perf,
            undefined, replaySafeSealedOptions(signal));
        if (!res.ok) throw new Error(`checkChunks: ${res.status}`);
        return (await res.json()).needed;
    }

    // --- Content (small files) ---

    async getContent(hash: string, perf?: PerfOperation): Promise<Uint8Array> {
        const res = await this.sealed("GET", `/api/v1/content/${hash}`, new Uint8Array(), perf,
            undefined, REPLAY_SAFE_SEALED_OPTIONS);
        if (!res.ok) throw new Error(`getContent ${hash}: ${res.status}`);
        return new Uint8Array(await res.arrayBuffer());
    }

    async putContent(hash: string, data: Uint8Array, perf?: PerfOperation): Promise<void> {
        const res = await this.sealed("PUT", `/api/v1/content/${hash}`, data, perf,
            undefined, REPLAY_SAFE_SEALED_OPTIONS);
        if (!res.ok) throw new Error(`putContent ${hash}: ${res.status}`);
    }

    async checkContent(hashes: string[], perf?: PerfOperation, signal?: AbortSignal,
        memory?: TransientWorkScope): Promise<string[]> {
        const bulk = await this.checkObjectsBulk(BulkObjectKind.Content, hashes, perf, signal, memory);
        if (bulk) return bulk;
        throwIfBulkCancelled(signal);
        const body = new TextEncoder().encode(JSON.stringify(hashes));
        const res = await this.sealed("POST", "/api/v1/content/check", body, perf,
            undefined, replaySafeSealedOptions(signal));
        if (!res.ok) throw new Error(`checkContent: ${res.status}`);
        return (await res.json()).needed;
    }

    // --- Content manifests (large files) ---

    async getManifest(hash: string, perf?: PerfOperation): Promise<FileManifest> {
        const res = await this.sealed(
            "GET",
            `/api/v1/content/manifest/${hash}`,
            new Uint8Array(),
            perf,
            undefined,
            REPLAY_SAFE_SEALED_OPTIONS,
        );
        if (!res.ok) throw new Error(`getManifest ${hash}: ${res.status}`);
        return res.json();
    }

    async putManifest(
        hash: string,
        manifest: FileManifest,
        perf?: PerfOperation,
    ): Promise<void> {
        const body = new TextEncoder().encode(JSON.stringify(manifest));
        const res = await this.sealed("PUT", `/api/v1/content/manifest/${hash}`, body, perf,
            undefined, REPLAY_SAFE_SEALED_OPTIONS);
        if (!res.ok) throw new Error(`putManifest ${hash}: ${res.status}`);
    }

    async checkManifests(
        hashes: string[],
        perf?: PerfOperation,
        signal?: AbortSignal,
        memory?: TransientWorkScope,
    ): Promise<string[]> {
        const bulk = await this.checkObjectsBulk(BulkObjectKind.Manifest, hashes, perf, signal, memory);
        if (bulk) return bulk;
        throwIfBulkCancelled(signal);
        const body = new TextEncoder().encode(JSON.stringify(hashes));
        const res = await this.sealed("POST", "/api/v1/content/manifests/check", body, perf,
            undefined, replaySafeSealedOptions(signal));
        if (!res.ok) throw new Error(`checkManifests: ${res.status}`);
        return (await res.json()).needed;
    }

    // --- Content sub-file chunks ---

    async getContentChunk(hash: string, perf?: PerfOperation): Promise<Uint8Array> {
        const res = await this.sealed(
            "GET",
            `/api/v1/content/chunk/${hash}`,
            new Uint8Array(),
            perf,
            undefined,
            REPLAY_SAFE_SEALED_OPTIONS,
        );
        if (!res.ok) throw new Error(`getContentChunk ${hash}: ${res.status}`);
        return new Uint8Array(await res.arrayBuffer());
    }

    async putContentChunk(
        hash: string,
        data: Uint8Array,
        perf?: PerfOperation,
    ): Promise<void> {
        const res = await this.sealed("PUT", `/api/v1/content/chunk/${hash}`, data, perf,
            undefined, REPLAY_SAFE_SEALED_OPTIONS);
        if (!res.ok) throw new Error(`putContentChunk ${hash}: ${res.status}`);
    }

    async checkContentChunks(
        hashes: string[],
        perf?: PerfOperation,
        signal?: AbortSignal,
        memory?: TransientWorkScope,
    ): Promise<string[]> {
        const bulk = await this.checkObjectsBulk(BulkObjectKind.ContentChunk, hashes, perf, signal, memory);
        if (bulk) return bulk;
        throwIfBulkCancelled(signal);
        const body = new TextEncoder().encode(JSON.stringify(hashes));
        const res = await this.sealed("POST", "/api/v1/content/chunks/check", body, perf,
            undefined, replaySafeSealedOptions(signal));
        if (!res.ok) throw new Error(`checkContentChunks: ${res.status}`);
        return (await res.json()).needed;
    }

    private async checkObjectsBulk(
        kind: BulkObjectKind,
        hashes: readonly string[],
        perf?: PerfOperation,
        signal?: AbortSignal,
        memory?: TransientWorkScope,
    ): Promise<string[] | null> {
        throwIfBulkCancelled(signal);
        if (hashes.length === 0) return [];
        const limits = await this.getBulkLimits(perf, signal);
        if (!limits) return null;
        const hashesPerRequest = Math.max(
            1,
            Math.min(limits.maxObjects, Math.floor((limits.maxBytes - 9) / 32)),
        );
        const needed: string[] = [];
        for (let offset = 0; offset < hashes.length; offset += hashesPerRequest) {
            const batch = hashes.slice(offset, offset + hashesPerRequest);
            const body = encodeBulkCheckRequest(kind, batch, limits.maxObjects);
            const responseBytes = Math.max(
                TRANSPORT_ERROR_PAYLOAD_ALLOWANCE_BYTES,
                8 + bulkBitmapBytes(batch.length),
            );
            const request = (work?: TransientWorkContext) => this.routeDataRpc<Uint8Array | null>(
                WsDataFrameType.CheckObjects,
                body,
                WsDataFrameType.CheckResult,
                (bytes) => bytes,
                async () => {
                    const response = await this.sealed("POST", "/api/v1/bulk/check", body, perf,
                        responseBytes, replaySafeSealedOptions(signal));
                    if (response.status === 404 || response.status === 405) {
                        this.bulkLimits = null;
                        this.wsDataFrameBytes = null;
                        this.diffPageLimits = null;
                        this.closeDataLane();
                        return null;
                    }
                    if (!response.ok) throw new Error(`bulk check failed: ${response.status}`);
                    return new Uint8Array(await response.arrayBuffer());
                },
                perf,
                work,
                responseBytes,
                signal,
            );
            const workBytes = estimateTransportWorkset(Math.max(body.byteLength, responseBytes));
            const dataResponse = memory
                ? await memory.run(workBytes, request, { signal })
                : await request();
            if (dataResponse) {
                needed.push(...decodeBulkCheckResponse(dataResponse, batch));
                continue;
            }
            return null;
        }
        return needed;
    }

    private async putBulkPack(
        records: readonly BulkUploadRecord[],
        limits: BulkCodecLimits,
        perf?: PerfOperation,
        memory?: TransientWorkScope,
        signal?: AbortSignal,
        priority: "bulk" | "urgent-file" = "bulk",
    ): Promise<void> {
        throwIfBulkCancelled(signal);
        if (!memory) throw new Error("bulk upload requires an admitted memory scope");
        const encodedBytes = bulkPackEncodedLength(records);
        if (encodedBytes > limits.maxBytes) throw new RangeError("bulk upload exceeds byte limit");
        const outcome = await memory.run(estimateTransportWorkset(encodedBytes),
            async (work): Promise<"done" | "legacy" | "split"> => {
                const body = encodeBulkUploadPack(records, limits);
                if (body.byteLength !== encodedBytes) throw new Error("bulk upload changed after admission");
                const retryDelays = [0, 25, 100];
                for (let attempt = 0; attempt < retryDelays.length; attempt++) {
                    if (retryDelays[attempt] > 0) {
                        await new Promise<void>((resolve) =>
                            globalThis.setTimeout(resolve, retryDelays[attempt]));
                    }
                    const transport = await this.routeDataRpc<BulkPutTransportOutcome>(
                        WsDataFrameType.PutPack,
                        body,
                        WsDataFrameType.PutAck,
                        (bytes) => ({ kind: "ack", bytes }),
                        async () => {
                            const response = await this.sealed(
                                "POST",
                                "/api/v1/bulk/put",
                                body,
                                perf,
                                BOUNDED_ERROR_BYTES,
                                replaySafeSealedOptions(signal),
                            );
                            if (response.status === 404 || response.status === 405) {
                                this.bulkLimits = null;
                                this.wsDataFrameBytes = null;
                                this.closeDataLane();
                                return { kind: "legacy" };
                            }
                            if (response.status === 413) {
                                perf?.increment({ retries: 1, backpressureEvents: 1 });
                                if (records.length === 1) {
                                    // A proxy or stale server limit can be lower than the
                                    // authenticated capability bundle. Leave the durable,
                                    // single-object path as the final compatibility lane.
                                    this.bulkLimits = null;
                                    this.wsDataFrameBytes = null;
                                    this.closeDataLane();
                                    return { kind: "legacy" };
                                }
                                const [first, second] = splitBulkUploadRecords(records);
                                const retryMaxBytes = Math.max(
                                    bulkPackEncodedLength(first),
                                    bulkPackEncodedLength(second),
                                );
                                if (this.bulkLimits) {
                                    this.bulkLimits = {
                                        ...this.bulkLimits,
                                        maxBytes: Math.min(this.bulkLimits.maxBytes, retryMaxBytes),
                                    };
                                }
                                console.warn(
                                    `[obsetync] bulk upload ${body.byteLength} B exceeded the effective ` +
                                    `HTTP limit; retrying ordered packs of at most ${retryMaxBytes} B`,
                                );
                                return { kind: "split" };
                            }
                            if (!response.ok) throw new Error(`bulk put failed: ${response.status}`);
                            return {
                                kind: "ack",
                                bytes: new Uint8Array(await response.arrayBuffer()),
                            };
                        },
                        perf,
                        work,
                        BOUNDED_ERROR_BYTES,
                        signal,
                        priority,
                    );
                    if (transport.kind === "legacy") return "legacy";
                    if (transport.kind === "split") return "split";
                    const statuses = decodeBulkUploadAck(transport.bytes, records.length);
                    const permanent = statuses.findIndex((status) =>
                        status === BulkUploadStatus.BadHash ||
                        status === BulkUploadStatus.RejectedLimit);
                    if (permanent >= 0) {
                        const reason = statuses[permanent] === BulkUploadStatus.BadHash
                            ? "bad hash"
                            : "rejected limit";
                        throw new Error(
                            `bulk put record ${permanent} (${records[permanent].hash}) failed: ${reason}`,
                        );
                    }
                    if (!statuses.includes(BulkUploadStatus.RetryableStorageError)) return "done";
                    if (attempt + 1 < retryDelays.length) {
                        perf?.increment({ retries: 1, backpressureEvents: 1 });
                        continue;
                    }
                    throw new Error("bulk put exhausted retries after a storage error");
                }
                throw new Error("bulk put did not produce an acknowledgement");
        }, { signal });
        // Do not retain the rejected encoded pack or its child quota while
        // recursively encoding retries or entering the legacy transport.
        if (outcome === "legacy") {
            for (const record of records) await this.putObjectLegacy(record, perf, memory, signal);
        } else if (outcome === "split") {
            const [first, second] = splitBulkUploadRecords(records);
            await this.putBulkPack(first, limits, perf, memory, signal, priority);
            await this.putBulkPack(second, limits, perf, memory, signal, priority);
        }
    }

    private async getBulkLimits(
        perf?: PerfOperation,
        signal?: AbortSignal,
    ): Promise<BulkCodecLimits | null> {
        throwIfBulkCancelled(signal);
        if (this.bulkLimits !== undefined) return this.bulkLimits;
        if (!this.capabilitiesPromise) {
            const pending = (async (): Promise<BulkCodecLimits | null> => {
                // A fresh/rotated server-eph bundle already carries capabilities.
                // Existing sessions upgraded in place discover them through this
                // one sealed endpoint without requiring re-enrollment. This
                // single-flight belongs to the API instance, not its first waiter.
                await this.getChannel(perf);
                if (this.bulkLimits !== undefined) return this.bulkLimits;
                const response = await this.sealed(
                    "POST",
                    "/api/v1/capabilities",
                    new Uint8Array(),
                    perf,
                    undefined,
                    REPLAY_SAFE_SEALED_OPTIONS,
                );
                if (response.status === 404 || response.status === 405) {
                    this.bulkLimits = null;
                    this.wsDataFrameBytes = null;
                    this.closeDataLane();
                    return null;
                }
                if (!response.ok) throw new Error(`capability negotiation failed: ${response.status}`);
                const bundle = await response.json();
                this.applyCapabilityBundle(bundle);
                return this.bulkLimits ?? null;
            })();
            this.capabilitiesPromise = pending;
            // Clear only when the shared work itself settles. A cancelled
            // waiter must not detach the still-live single-flight from future
            // callers, and both branches handle a fully abandoned rejection.
            void pending.then(
                () => { if (this.capabilitiesPromise === pending) this.capabilitiesPromise = null; },
                () => { if (this.capabilitiesPromise === pending) this.capabilitiesPromise = null; },
            );
        }
        return waitForSharedTransport(this.capabilitiesPromise, signal);
    }

    private applyCapabilityBundle(bundle: any): void {
        const previousFrameBytes = this.wsDataFrameBytes;
        const previousWireVersion = this.wsDataWireVersion;
        this.bulkLimits = negotiateBulkLimits(bundle, this.runtime);
        this.diffPageLimits = negotiateDiffPageLimits(bundle, this.runtime);
        const wsData = negotiateWsDataCapability(bundle);
        const frameBytes = wsData?.maxPayloadBytes ?? null;
        const wireVersion = wsData?.preferredWireVersion ?? 1;
        if (this.wsDataLane && (frameBytes === null || wireVersion !== this.wsDataWireVersion)) {
            this.closeDataLane();
        }
        this.wsDataFrameBytes = frameBytes;
        this.wsDataWireVersion = wireVersion;
        if (previousFrameBytes !== undefined &&
            (previousFrameBytes !== frameBytes || previousWireVersion !== wireVersion)) {
            this.transportRouter.resetMeasurements();
        }
    }

    private async getDataLane(
        perf?: PerfOperation,
        signal?: AbortSignal,
    ): Promise<ObsetyncWsDataLane | null> {
        await this.getBulkLimits(perf, signal);
        if (this.wsDataFrameBytes === null || this.wsDataFrameBytes === undefined ||
            typeof WebSocket === "undefined") {
            return null;
        }
        if (!this.wsDataLane) {
            this.wsDataLane = new ObsetyncWsDataLane({
                baseUrl: this.serverUrl,
                runtime: this.runtime,
                advertisedPayloadBytes: this.wsDataFrameBytes,
                preferredWireVersion: this.wsDataWireVersion,
                localRequestLimit: () => getHashTuning().networkConcurrency,
                memoryBudget: this.memoryBudget,
                openSession: async () => {
                    const keys = generateWsEphKeypair();
                    const minted = await this.mintWsTicket(keys.pubB64);
                    if (!minted.server_eph_pub) {
                        keys.priv.fill(0);
                        throw new Error("server omitted the WS data ephemeral key");
                    }
                    return {
                        ticket: minted.ticket,
                        session: await ObsetyncWsSession.create(
                            keys.priv,
                            minted.server_eph_pub,
                            minted.ticket,
                            "data-v1",
                        ),
                    };
                },
            });
        }
        return this.wsDataLane;
    }

    private async routeDataRpc<T>(
        type: WsDataFrameType.CheckObjects | WsDataFrameType.PutPack | WsDataFrameType.GetPack,
        payload: Uint8Array,
        expected:
            | WsDataFrameType.CheckResult
            | WsDataFrameType.PutAck
            | WsDataFrameType.GetResult,
        fromWs: (bytes: Uint8Array) => T,
        http: () => Promise<T>,
        perf?: PerfOperation,
        memory?: TransientWorkContext,
        maxResponseBytes?: number,
        signal?: AbortSignal,
        putPriority: "bulk" | "urgent-file" = "bulk",
    ): Promise<T> {
        throwIfBulkCancelled(signal);
        const wsAdvertised = this.wsDataFrameBytes !== undefined && this.wsDataFrameBytes !== null;
        const withinFrame = wsAdvertised && payload.byteLength <= this.wsDataFrameBytes!;
        const wsAvailable = withinFrame && typeof WebSocket !== "undefined";
        return this.transportRouter.execute({
            payloadBytes: payload.byteLength,
            expectedUsefulBytes: type === WsDataFrameType.GetPack
                ? maxResponseBytes ?? payload.byteLength
                : payload.byteLength,
            replay: "content-addressed",
            lane: dataRpcTransportLane(type, putPriority),
            wsUnavailableReason: wsAvailable
                ? undefined
                : wsAdvertised && typeof WebSocket !== "undefined"
                    ? "ws-payload-too-large"
                    : "ws-unavailable",
            sampleValid: transportSampleVisible,
            ws: wsAvailable ? async (
                timeouts: TransportPhaseTimeouts,
                measurements: TransportAttemptMeasurements,
            ) => withTransportSampleVisibility(measurements, async () => {
                const measuredPerf = measureTransportQueues(perf, measurements);
                const lane = await this.getDataLane(measuredPerf, signal);
                if (!lane) throw new Error("WS data lane became unavailable before dispatch");
                const response = await lane.request(
                    type,
                    payload,
                    expected,
                    measuredPerf,
                    memory,
                    maxResponseBytes,
                    timeouts,
                    signal,
                    putPriority === "urgent-file" ? { priority: "urgent-file" } : undefined,
                );
                measurements.setUsefulBytes(type === WsDataFrameType.GetPack
                    ? response.byteLength
                    : payload.byteLength);
                this.wsDataFallbackLogAfter = 0;
                return fromWs(response);
            }) : undefined,
            http: async (measurements) => withTransportSampleVisibility(measurements, async () => {
                throwIfBulkCancelled(signal);
                const result = await http();
                throwIfBulkCancelled(signal);
                if (result === null ||
                    (type === WsDataFrameType.PutPack &&
                        (result as BulkPutTransportOutcome).kind !== "ack")) {
                    measurements.invalidate();
                } else if (type === WsDataFrameType.GetPack && result instanceof Uint8Array) {
                    measurements.setUsefulBytes(result.byteLength);
                } else {
                    measurements.setUsefulBytes(payload.byteLength);
                }
                return result;
            }),
            classifyWsFailure: (error) => {
                if (signal?.aborted || error instanceof WsDataCancelledError) {
                    return { retryable: false };
                }
                if (error instanceof WsDataRpcError &&
                    error.remote.code === WsDataErrorCode.InvalidRequest) {
                    return { retryable: false };
                }
                return {
                    retryable: true,
                    retryAfterMs: error instanceof WsDataRpcError
                        ? error.remote.retryAfterMs
                        : undefined,
                };
            },
            onFallback: (error) => {
                perf?.increment({ retries: 1 });
                const now = Date.now();
                if (now >= this.wsDataFallbackLogAfter) {
                    const message = error instanceof Error ? error.message : String(error);
                    console.warn(
                        "[obsetync] ws-data unavailable; using bulk HTTP " +
                        "(further messages suppressed for 30s):",
                        message,
                    );
                    this.wsDataFallbackLogAfter = now + 30_000;
                }
            },
        });
    }

    private async putObjectLegacy(
        record: BulkUploadRecord,
        perf?: PerfOperation,
        memory?: TransientWorkScope,
        signal?: AbortSignal,
    ): Promise<void> {
        const path = this.objectPath(record.kind, record.hash);
        const put = async () => {
            throwIfBulkCancelled(signal);
            const response = await this.sealed("PUT", path, record.data, perf,
                memory ? BOUNDED_ERROR_BYTES : undefined, replaySafeSealedOptions(signal));
            throwIfBulkCancelled(signal);
            if (!response.ok) throw new Error(`put object ${record.hash}: ${response.status}`);
        };
        if (memory) {
            await memory.run(estimateTransportWorkset(record.data.byteLength), put, { signal });
        } else {
            await put();
        }
    }

    private async getObjectLegacy(
        kind: BulkObjectKind,
        hash: string,
        perf?: PerfOperation,
        maxResponseBytes?: number,
        signal?: AbortSignal,
    ): Promise<Uint8Array> {
        throwIfBulkCancelled(signal);
        const response = await this.sealed("GET", this.objectPath(kind, hash), new Uint8Array(), perf,
            maxResponseBytes, replaySafeSealedOptions(signal));
        throwIfBulkCancelled(signal);
        if (!response.ok) throw new Error(`get object ${hash}: ${response.status}`);
        return new Uint8Array(await response.arrayBuffer());
    }

    private objectPath(kind: BulkObjectKind, hash: string): string {
        switch (kind) {
            case BulkObjectKind.Content:
                return `/api/v1/content/${hash}`;
            case BulkObjectKind.ContentChunk:
                return `/api/v1/content/chunk/${hash}`;
            case BulkObjectKind.IndexChunk:
                return `/api/v1/chunk/${hash}`;
            case BulkObjectKind.Manifest:
                return `/api/v1/content/manifest/${hash}`;
            default:
                throw new Error("unknown bulk object kind");
        }
    }

    // --- Health / connectivity ---

    async ping(): Promise<{ serverUrl: string; ok: boolean; transport: string }> {
        // /health is the only plaintext route. Client calls it pre-enrollment
        // to verify the URL is reachable without needing serverBoxPub yet.
        try {
            const res = await requestUrl({
                url: `${this.serverUrl}/health`,
                method: "GET",
                throw: false,
            });
            return {
                serverUrl: this.serverUrl,
                ok: res.status >= 200 && res.status < 300,
                transport: "http + AEAD envelope",
            };
        } catch (e: any) {
            return { serverUrl: this.serverUrl, ok: false, transport: `error: ${e?.message ?? e}` };
        }
    }

    // --- Enrollment ---

    async claimEnrollment(code: string): Promise<EnrollmentBundle> {
        // Admin port is plain HTTP (enrollment UX). User runs it behind
        // whatever trust boundary they want (localhost, VPN, SSH tunnel).
        const adminUrl = this.serverUrl
            .replace(/^https:/, "http:")
            .replace(/:\d+$/, ":27183");
        const res = await requestUrl({
            url: `${adminUrl}/admin/enrollment/${code}`,
            throw: false,
        });
        const body = res.json;
        if (res.status < 200 || res.status >= 300) {
            throw new Error(`enrollment failed: ${body?.error ?? res.status}`);
        }
        return body;
    }

    // --- Internal: encrypted request/response ---

    /**
     * Seal `body` with the ObsetyncSecureChannel, POST it to `path`, unseal the
     * response. This is the single code path for every sync API call.
     *
     * Note: every route maps to POST on the wire even if the semantic method
     * is GET/PUT/DELETE. The semantic method is preserved in the AAD so the
     * server still routes correctly, but HTTP-level always POST avoids
     * issues with iOS's requestUrl not sending a body on GET.
     */
    private async sealed(
        method: string,
        path: string,
        body: Uint8Array,
        perf?: PerfOperation,
        maxResponseBytes?: number,
        options?: ReplaySafeSealedOptions,
    ): Promise<FetchLike> {
        if (maxResponseBytes !== undefined &&
            (!Number.isSafeInteger(maxResponseBytes) || maxResponseBytes < 0)) {
            throw new RangeError("invalid sealed response byte bound");
        }
        let refreshed = false;
        let replayRecovered = false;
        let receiveBackpressureRetried = false;
        let receiveBackpressureRetryPending = false;
        for (;;) {
            throwIfBulkCancelled(options?.signal);
            const channel = await this.getChannel(perf);
            const sequence = await this.nextSequence();
            // Neither channel establishment nor a durable sequence reservation
            // is cancellable. Re-check before allocating/encrypting the body or
            // issuing a native request.
            throwIfBulkCancelled(options?.signal);
            if (receiveBackpressureRetryPending) {
                receiveBackpressureRetryPending = false;
                perf?.increment({ retries: 1 });
            }
            let response: FetchLike;
            try {
                response = await this.sendEncrypted(
                    channel,
                    method,
                    path,
                    body,
                    sequence,
                    perf,
                    maxResponseBytes,
                );
            } catch (error) {
                throwIfBulkCancelled(options?.signal);
                if (error instanceof ObsetyncSessionStaleError) {
                    if (!refreshed) {
                        refreshed = true;
                        if (this.channel === channel || this.channel === null) {
                            this.channel = null;
                            perf?.increment({ retries: 1 });
                            await this.refreshServerEphemeral(perf);
                        }
                        continue;
                    }
                    throw reenrollmentRequired(
                        `server rejected refreshed transport keys for ${method} ${path} — ` +
                        "re-enroll device",
                    );
                }
                if (error instanceof ObsetyncSecureTransportError) {
                    throw new Error(`decrypt ${method} ${path}: ${error.message}`);
                }
                throw error;
            }

            if (response.receiveBackpressureRetryAfterMs !== undefined) {
                perf?.increment({ backpressureEvents: 1 });
                if (options?.receiveBackpressureReplay === "application-idempotent" &&
                    !receiveBackpressureRetried) {
                    receiveBackpressureRetried = true;
                    await waitForHttpReceiveBackpressure(
                        response.receiveBackpressureRetryAfterMs,
                        options.signal,
                    );
                    // The genuine server refusal happened before body
                    // receive/decrypt. The marker itself is plaintext, so this
                    // path is enabled only when duplicate application execution
                    // is harmless. Allocate a fresh sequence and envelope;
                    // never replay ciphertext.
                    receiveBackpressureRetryPending = true;
                    continue;
                }
            }

            throwIfBulkCancelled(options?.signal);

            if (response.status === 401 && !replayRecovered) {
                const error = await response.json().catch(() => null);
                if (error?.error === "replay" && Number.isSafeInteger(error.last_seen_seq)) {
                    replayRecovered = true;
                    perf?.increment({ retries: 1 });
                    await this.recoverSequence(error.last_seen_seq);
                    continue;
                }
            }
            if (response.status === 401 || response.status === 403) {
                throw reenrollmentRequired(
                    `server rejected this device (${response.status}) — re-enroll device`,
                );
            }
            if (response.status === 426) {
                const message = new TextDecoder().decode(
                    new Uint8Array(await response.arrayBuffer()),
                ).trim();
                throw upgradeRequired(
                    message ||
                    "server requires a newer Obsetync plugin; enrollment remains valid",
                );
            }
            return response;
        }
    }

    private async sendEncrypted(
        channel: ObsetyncSecureChannel,
        method: string,
        path: string,
        body: Uint8Array,
        sequence: number,
        perf?: PerfOperation,
        maxResponseBytes?: number,
    ): Promise<FetchLike> {
        const endEncrypt = perf?.phase("encrypt");
        let wireBody: Uint8Array;
        try {
            wireBody = await channel.encryptRequest(method, path, body, sequence);
        } finally {
            endEncrypt?.();
        }
        perf?.increment({
            plaintextBytesSent: body.length,
            wireBytesSent: wireBody.length,
            requestCount: 1,
        });
        const nonceReq = extractRequestNonce(wireBody);
        const params: RequestUrlParam = {
            url: `${this.serverUrl}${path}`,
            method: "POST",
            headers: {
                "Content-Type": "application/octet-stream",
                "X-Obsetync-Method": method,
            },
            body: exactArrayBuffer(wireBody),
            throw: false,
        };
        const endNetwork = perf?.phase("network");
        let wireResponse: Awaited<ReturnType<typeof requestUrl>>;
        try {
            wireResponse = await requestUrl(params);
        } finally {
            endNetwork?.();
        }
        perf?.increment({ wireBytesReceived: wireResponse.arrayBuffer.byteLength });
        // requestUrl has already materialized the native response. Reject
        // before ciphertext slicing, WebCrypto decryption or plaintext copies;
        // this API cannot impose a streaming native receive allocation cap.
        if (maxResponseBytes !== undefined && wireResponse.arrayBuffer.byteLength >
            maxResponseBytes + HTTP_RESPONSE_OVERHEAD_BYTES) {
            throw new RangeError("sealed response exceeds the admitted byte bound");
        }
        if (wireResponse.status !== 200) {
            // Reverse proxies can still emit their own plaintext failures;
            // the application server itself always responds with wire 200.
            return {
                status: wireResponse.status,
                ok: false,
                receiveBackpressureRetryAfterMs: parseHttpReceiveBackpressureDelay(
                    wireResponse.status,
                    wireResponse.headers,
                ) ?? undefined,
                arrayBuffer: async () => wireResponse.arrayBuffer,
                json: async () => wireResponse.json,
            };
        }

        const endDecrypt = perf?.phase("decrypt");
        let opened: Awaited<ReturnType<ObsetyncSecureChannel["decryptResponse"]>>;
        try {
            opened = await channel.decryptResponse(
                method,
                path,
                nonceReq,
                new Uint8Array(wireResponse.arrayBuffer),
            );
        } finally {
            endDecrypt?.();
        }
        const plaintext = opened.body;
        if (maxResponseBytes !== undefined && plaintext.byteLength > maxResponseBytes) {
            throw new RangeError("opened response exceeds the admitted byte bound");
        }
        perf?.increment({ plaintextBytesReceived: plaintext.length });
        return {
            status: opened.status,
            ok: opened.status >= 200 && opened.status < 300,
            arrayBuffer: async () => exactArrayBuffer(plaintext),
            json: async () => {
                const text = new TextDecoder().decode(plaintext);
                return text.length ? JSON.parse(text) : null;
            },
        };
    }

    private async refreshServerEphemeral(perf?: PerfOperation): Promise<void> {
        if (this.refreshPromise) return this.refreshPromise;
        this.refreshPromise = this.refreshServerEphemeralOnce(perf);
        try {
            await this.refreshPromise;
        } finally {
            this.refreshPromise = null;
        }
    }

    private async refreshServerEphemeralOnce(perf?: PerfOperation): Promise<void> {
        const persistence = this.requireTransportPersistence();
        const bootstrap = await ObsetyncSecureChannel.createBootstrap(
            this.serverBoxPubBase64,
        );
        let response: FetchLike;
        try {
            response = await this.sendEncrypted(
                bootstrap,
                "POST",
                "/api/v1/server-eph",
                new Uint8Array(),
                0,
                perf,
                ROOT_CAPABILITY_MAX_BYTES,
            );
        } catch (error) {
            if (error instanceof ObsetyncSecureTransportError) {
                throw reenrollmentRequired(
                    "server transport key no longer matches this enrollment — re-enroll device",
                );
            }
            throw error;
        }
        if (!response.ok) {
            // A proxy/server outage is retryable; re-enrollment cannot repair
            // an HTTP 5xx (or a route temporarily missing during a rollout).
            throw new Error(`server ephemeral refresh failed: ${response.status}`);
        }
        const bundle = await response.json();
        if (
            typeof bundle?.Es_pub !== "string" ||
            !Number.isSafeInteger(bundle?.valid_until) ||
            bundle.valid_until <= Date.now() / 1000
        ) {
            throw new Error("server returned an invalid transport-v2 ephemeral bundle");
        }
        this.applyCapabilityBundle(bundle);
        await persistence.update({
            esPub: bundle.Es_pub,
            esPubValidUntil: bundle.valid_until,
            wireVersion: "0x02",
        });
        this.channel = null;
    }

    private async nextSequence(): Promise<number> {
        try {
            return await this.sequences.next();
        } catch (error) {
            if (isReenrollmentRequiredError(error)) {
                throw reenrollmentRequired(
                    error instanceof Error ? error.message : String(error),
                );
            }
            throw error;
        }
    }

    private async recoverSequence(greatestSeen: number): Promise<void> {
        await this.sequences.recover(greatestSeen);
    }

    private requireTransportPersistence(): TransportPersistence {
        if (!this.transportPersistence) {
            throw reenrollmentRequired("transport-v2 state unavailable — re-enroll device");
        }
        return this.transportPersistence;
    }
}
