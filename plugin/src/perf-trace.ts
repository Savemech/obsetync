/**
 * Privacy-safe, bounded performance telemetry for sync operations.
 *
 * Records contain aggregate counts and durations only. Paths, filenames,
 * hashes, server URLs, errors, and payload bytes are deliberately absent from
 * every public input type, so callers cannot accidentally leak vault data into
 * the debug export.
 */

export type PerfOperationKind = "push" | "pull" | "scan" | "reconcile";
export type PerfOutcome = "success" | "error" | "cancelled";

export type PerfPhase =
    | "scheduler_wait"
    | "ws_connect"
    | "ws_credit_wait"
    | "ws_send_queue"
    | "ws_buffer_wait"
    | "ws_ack_wait"
    | "scan_batch"
    | "prepare_batch"
    | "enumerate"
    | "stat"
    | "read"
    | "hash"
    | "fastcdc"
    | "check"
    | "encrypt"
    | "decrypt"
    | "network"
    | "upload"
    | "download"
    | "apply"
    | "tree_update"
    | "tree_index_upload"
    | "root_commit"
    | "checkpoint";

export interface PerfPlatformProfile {
    runtime: "desktop" | "mobile" | "unknown";
    architecture: "arm64" | "x64" | "unknown";
    wasmMode: "scalar" | "simd" | "unknown";
    hashConcurrency: number;
    readConcurrency: number;
    networkConcurrency: number;
    feedBytes: number;
    batchBytes: number;
    diffPageBytes: number;
}

export const DEFAULT_PERF_PROFILE: PerfPlatformProfile = {
    runtime: "unknown",
    architecture: "unknown",
    wasmMode: "unknown",
    hashConcurrency: 1,
    readConcurrency: 1,
    networkConcurrency: 1,
    feedBytes: 65_536,
    batchBytes: 0,
    diffPageBytes: 0,
};

export function normalizePerfArchitecture(
    value: string | null | undefined,
): PerfPlatformProfile["architecture"] {
    const normalized = value?.trim().toLowerCase() ?? "";
    if (
        normalized === "arm64" ||
        normalized === "aarch64" ||
        normalized.includes("arm64") ||
        normalized.includes("aarch64")
    ) {
        return "arm64";
    }
    if (
        normalized === "x64" ||
        normalized === "x86_64" ||
        normalized === "amd64" ||
        normalized.includes("x86_64") ||
        normalized.includes("amd64")
    ) {
        return "x64";
    }
    return "unknown";
}

export interface PerfWorkload {
    filesTotal?: number;
    bytesTotal?: number;
    filesNeeded?: number;
    bytesNeeded?: number;
}

export interface PerfIncrement {
    filesCompleted?: number;
    bytesTransferred?: number;
    plaintextBytesSent?: number;
    plaintextBytesReceived?: number;
    wireBytesSent?: number;
    wireBytesReceived?: number;
    requestCount?: number;
    wsFrameCount?: number;
    retries?: number;
    backpressureEvents?: number;
    resumedPages?: number;
}

export interface PerfWasmChunks {
    before?: number;
    reachable?: number;
    after?: number;
}

export interface PerfOperationRecord {
    schemaVersion: 1;
    operationId: string;
    kind: PerfOperationKind;
    outcome: PerfOutcome;
    startedAt: number;
    finishedAt: number;
    durationMs: number;
    profile: PerfPlatformProfile;
    filesTotal: number | null;
    bytesTotal: number | null;
    filesNeeded: number | null;
    bytesNeeded: number | null;
    filesCompleted: number;
    bytesTransferred: number;
    plaintextBytesSent: number;
    plaintextBytesReceived: number;
    wireBytesSent: number;
    wireBytesReceived: number;
    requestCount: number;
    wsFrameCount: number;
    retries: number;
    backpressureEvents: number;
    resumedPages: number;
    dedupRatio: number | null;
    phases: Partial<Record<PerfPhase, number>>;
    eventLoopLagP95Ms: number | null;
    eventLoopLagSamples: number;
    /** Probe intervals crossing hidden/visible transitions are not UI lag. */
    eventLoopLagExcludedSamples?: number;
    peakBatchBytes: number;
    wasmChunksBefore: number | null;
    wasmChunksReachable: number | null;
    wasmChunksAfter: number | null;
}

/** Detached, path-free view of work that has not returned yet. */
export interface PerfActiveOperation {
    operationId: string;
    kind: PerfOperationKind;
    durationMs: number;
    sinceProgressMs: number;
    visible: boolean;
    filesTotal: number | null;
    filesCompleted: number;
    bytesTransferred: number;
    requestCount: number;
    wsFrameCount: number;
    retries: number;
    /** Wall duration of the continuous interval with at least one open span.
     * Concurrent spans overlap: these values must not be added to wall time. */
    activePhases: Array<{ name: PerfPhase; count: number; durationMs: number }>;
    phases: Partial<Record<PerfPhase, number>>;
    eventLoopLagP95Ms: number | null;
    eventLoopLagExcludedSamples: number;
}

export interface PerfOperation {
    readonly operationId: string;
    readonly kind: PerfOperationKind;
    setWorkload(workload: PerfWorkload): void;
    /** Negotiated plaintext cap for each binary diff page in this operation. */
    setDiffPageBytes(bytes: number): void;
    increment(delta: PerfIncrement): void;
    phase(name: PerfPhase): () => void;
    addPhase(name: PerfPhase, durationMs: number): void;
    observeEventLoopLag(lagMs: number): void;
    observePeakBatchBytes(bytes: number): void;
    setWasmChunks(chunks: PerfWasmChunks): void;
    finish(outcome?: PerfOutcome): void;
}

export interface PerfTraceOptions {
    maxRecords?: number;
    monotonicNow?: () => number;
    wallNow?: () => number;
    monitorEventLoop?: boolean;
    eventLoopIntervalMs?: number;
}

const LAG_BUCKET_UPPER_MS = [
    1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1_000, 2_000, 5_000, 10_000,
] as const;

const EXACT_PHASE_CALLS = 32;
const PHASE_SAMPLE_INTERVAL = 16;
const NOOP_PHASE_END = () => {};

/**
 * Deterministic sampling for per-file phases. The first 32 observations are
 * exact; the tail records one representative out of every 16 and returns the
 * number of observations it represents. Weights always sum to `total`, so a
 * caller can add `duration * weight` without touching PerfTrace for skipped
 * files. This keeps the hot path to a cheap integer branch.
 */
export function perfSampleWeight(index: number, total: number): number {
    if (!Number.isInteger(index) || !Number.isInteger(total) || index < 0 || index >= total) {
        return 0;
    }
    if (index < EXACT_PHASE_CALLS) return 1;
    const tailIndex = index - EXACT_PHASE_CALLS;
    if (tailIndex % PHASE_SAMPLE_INTERVAL !== 0) return 0;
    return Math.min(PHASE_SAMPLE_INTERVAL, total - index);
}

function finiteNonNegative(value: number, name: string): number {
    if (!Number.isFinite(value) || value < 0) {
        throw new RangeError(`${name} must be a finite non-negative number`);
    }
    return value;
}

function cloneProfile(profile: PerfPlatformProfile): PerfPlatformProfile {
    return { ...profile };
}

function validateProfile(profile: PerfPlatformProfile): PerfPlatformProfile {
    const copy = cloneProfile(profile);
    finiteNonNegative(copy.hashConcurrency, "hashConcurrency");
    finiteNonNegative(copy.readConcurrency, "readConcurrency");
    finiteNonNegative(copy.networkConcurrency, "networkConcurrency");
    finiteNonNegative(copy.feedBytes, "feedBytes");
    finiteNonNegative(copy.batchBytes, "batchBytes");
    finiteNonNegative(copy.diffPageBytes, "diffPageBytes");
    return copy;
}

function cloneRecord(record: PerfOperationRecord): PerfOperationRecord {
    return {
        ...record,
        profile: cloneProfile(record.profile),
        phases: { ...record.phases },
    };
}

class EventLoopLagHistogram {
    private readonly bins = new Uint32Array(LAG_BUCKET_UPPER_MS.length + 1);
    private samples = 0;

    observe(lagMs: number): void {
        finiteNonNegative(lagMs, "eventLoopLag");
        let index = LAG_BUCKET_UPPER_MS.findIndex((upper) => lagMs <= upper);
        if (index < 0) index = LAG_BUCKET_UPPER_MS.length;
        this.bins[index]++;
        this.samples++;
    }

    count(): number {
        return this.samples;
    }

    p95(): number | null {
        if (this.samples === 0) return null;
        const rank = Math.ceil(this.samples * 0.95);
        let cumulative = 0;
        for (let i = 0; i < this.bins.length; i++) {
            cumulative += this.bins[i];
            if (cumulative >= rank) {
                return i < LAG_BUCKET_UPPER_MS.length
                    ? LAG_BUCKET_UPPER_MS[i]
                    : 10_001;
            }
        }
        return 10_001;
    }
}

class PerfOperationHandle implements PerfOperation {
    readonly operationId: string;
    readonly kind: PerfOperationKind;

    private readonly startedAt: number;
    private readonly startedMono: number;
    private readonly phases: Partial<Record<PerfPhase, number>> = {};
    // One entry per phase kind, not per file/request: telemetry stays bounded
    // even when an operation has many concurrent spans of the same phase.
    private readonly activePhases = new Map<PerfPhase, { count: number; started: number }>();
    private readonly lag = new EventLoopLagHistogram();
    private readonly values: Required<PerfIncrement> = {
        filesCompleted: 0,
        bytesTransferred: 0,
        plaintextBytesSent: 0,
        plaintextBytesReceived: 0,
        wireBytesSent: 0,
        wireBytesReceived: 0,
        requestCount: 0,
        wsFrameCount: 0,
        retries: 0,
        backpressureEvents: 0,
        resumedPages: 0,
    };
    private workload: Required<PerfWorkload> = {
        filesTotal: 0,
        bytesTotal: 0,
        filesNeeded: 0,
        bytesNeeded: 0,
    };
    private workloadKnown = {
        filesTotal: false,
        bytesTotal: false,
        filesNeeded: false,
        bytesNeeded: false,
    };
    private peakBatchBytes = 0;
    private wasmChunks: PerfWasmChunks | null = null;
    private finished = false;
    private lagTimer: ReturnType<typeof setTimeout> | null = null;
    private nextLagProbeAt: number | null = null;
    private lagProbeVisible = true;
    private lagProbeVisibilityEpoch = 0;
    private excludedLagSamples = 0;
    private lastProgressMono: number;

    constructor(
        id: string,
        kind: PerfOperationKind,
        private readonly profile: PerfPlatformProfile,
        private readonly monotonicNow: () => number,
        private readonly wallNow: () => number,
        monitorEventLoop: boolean,
        private readonly eventLoopIntervalMs: number,
        private readonly isVisible: () => boolean,
        private readonly visibilityEpoch: () => number,
        private readonly onFinish: (record: PerfOperationRecord) => void,
    ) {
        this.operationId = id;
        this.kind = kind;
        this.startedAt = wallNow();
        this.startedMono = monotonicNow();
        this.lastProgressMono = this.startedMono;
        if (monitorEventLoop) this.scheduleLagProbe();
    }

    setWorkload(workload: PerfWorkload): void {
        if (this.finished) return;
        for (const key of Object.keys(workload) as Array<keyof PerfWorkload>) {
            const value = workload[key];
            if (value === undefined) continue;
            this.workload[key] = finiteNonNegative(value, key);
            this.workloadKnown[key] = true;
        }
    }

    setDiffPageBytes(bytes: number): void {
        if (this.finished) return;
        this.profile.diffPageBytes = finiteNonNegative(bytes, "diffPageBytes");
    }

    increment(delta: PerfIncrement): void {
        if (this.finished) return;
        if ((delta.filesCompleted ?? 0) > 0 || (delta.bytesTransferred ?? 0) > 0) {
            this.lastProgressMono = this.monotonicNow();
        }
        if (delta.filesCompleted !== undefined) {
            this.values.filesCompleted += finiteNonNegative(
                delta.filesCompleted,
                "filesCompleted",
            );
        }
        if (delta.bytesTransferred !== undefined) {
            this.values.bytesTransferred += finiteNonNegative(
                delta.bytesTransferred,
                "bytesTransferred",
            );
        }
        if (delta.plaintextBytesSent !== undefined) {
            this.values.plaintextBytesSent += finiteNonNegative(
                delta.plaintextBytesSent,
                "plaintextBytesSent",
            );
        }
        if (delta.plaintextBytesReceived !== undefined) {
            this.values.plaintextBytesReceived += finiteNonNegative(
                delta.plaintextBytesReceived,
                "plaintextBytesReceived",
            );
        }
        if (delta.wireBytesSent !== undefined) {
            this.values.wireBytesSent += finiteNonNegative(
                delta.wireBytesSent,
                "wireBytesSent",
            );
        }
        if (delta.wireBytesReceived !== undefined) {
            this.values.wireBytesReceived += finiteNonNegative(
                delta.wireBytesReceived,
                "wireBytesReceived",
            );
        }
        if (delta.requestCount !== undefined) {
            this.values.requestCount += finiteNonNegative(
                delta.requestCount,
                "requestCount",
            );
        }
        if (delta.wsFrameCount !== undefined) {
            this.values.wsFrameCount += finiteNonNegative(
                delta.wsFrameCount,
                "wsFrameCount",
            );
        }
        if (delta.retries !== undefined) {
            this.values.retries += finiteNonNegative(delta.retries, "retries");
        }
        if (delta.backpressureEvents !== undefined) {
            this.values.backpressureEvents += finiteNonNegative(
                delta.backpressureEvents,
                "backpressureEvents",
            );
        }
        if (delta.resumedPages !== undefined) {
            this.values.resumedPages += finiteNonNegative(
                delta.resumedPages,
                "resumedPages",
            );
        }
    }

    phase(name: PerfPhase): () => void {
        if (this.finished) return NOOP_PHASE_END;
        const started = this.monotonicNow();
        const active = this.activePhases.get(name);
        if (active) active.count++;
        else this.activePhases.set(name, { count: 1, started });
        let closed = false;
        return () => {
            if (closed || this.finished) return;
            closed = true;
            const active = this.activePhases.get(name);
            if (active && --active.count === 0) this.activePhases.delete(name);
            this.addPhase(name, Math.max(0, this.monotonicNow() - started));
        };
    }

    addPhase(name: PerfPhase, durationMs: number): void {
        if (this.finished) return;
        const value = finiteNonNegative(durationMs, `${name} duration`);
        this.phases[name] = (this.phases[name] ?? 0) + value;
    }

    observeEventLoopLag(lagMs: number): void {
        if (this.finished) return;
        if (!this.isVisible()) {
            finiteNonNegative(lagMs, "eventLoopLag");
            this.excludedLagSamples++;
            return;
        }
        this.lag.observe(lagMs);
    }

    snapshot(): PerfActiveOperation {
        const now = this.monotonicNow();
        return {
            operationId: this.operationId,
            kind: this.kind,
            durationMs: Math.max(0, now - this.startedMono),
            sinceProgressMs: Math.max(0, now - this.lastProgressMono),
            visible: this.isVisible(),
            filesTotal: this.workloadKnown.filesTotal ? this.workload.filesTotal : null,
            filesCompleted: this.values.filesCompleted,
            bytesTransferred: this.values.bytesTransferred,
            requestCount: this.values.requestCount,
            wsFrameCount: this.values.wsFrameCount,
            retries: this.values.retries,
            activePhases: Array.from(this.activePhases, ([name, active]) => ({
                name, count: active.count, durationMs: Math.max(0, now - active.started),
            })),
            phases: { ...this.phases },
            eventLoopLagP95Ms: this.lag.p95(),
            eventLoopLagExcludedSamples: this.excludedLagSamples,
        };
    }

    observePeakBatchBytes(bytes: number): void {
        if (this.finished) return;
        this.peakBatchBytes = Math.max(
            this.peakBatchBytes,
            finiteNonNegative(bytes, "peakBatchBytes"),
        );
    }

    setWasmChunks(chunks: PerfWasmChunks): void {
        if (this.finished) return;
        this.wasmChunks = {
            before: chunks.before === undefined
                ? undefined
                : finiteNonNegative(chunks.before, "wasmChunks.before"),
            reachable: chunks.reachable === undefined
                ? undefined
                : finiteNonNegative(chunks.reachable, "wasmChunks.reachable"),
            after: chunks.after === undefined
                ? undefined
                : finiteNonNegative(chunks.after, "wasmChunks.after"),
        };
    }

    finish(outcome: PerfOutcome = "success"): void {
        if (this.finished) return;
        const finishedMono = this.monotonicNow();
        if (
            this.lagTimer !== null && this.nextLagProbeAt !== null &&
            finishedMono >= this.nextLagProbeAt
        ) {
            // A synchronous phase can block the scheduled callback and then
            // finish before it runs. Preserve that terminal lag sample.
            this.observeScheduledLag(finishedMono);
        }
        this.finished = true;
        if (this.lagTimer !== null) {
            clearTimeout(this.lagTimer);
            this.lagTimer = null;
        }
        this.nextLagProbeAt = null;
        this.activePhases.clear();

        const finishedAt = this.wallNow();
        const durationMs = Math.max(0, finishedMono - this.startedMono);
        const bytesTotal = this.workloadKnown.bytesTotal ? this.workload.bytesTotal : null;
        const bytesNeeded = this.workloadKnown.bytesNeeded ? this.workload.bytesNeeded : null;
        const dedupRatio =
            bytesTotal !== null && bytesNeeded !== null && bytesTotal > 0
                ? Math.max(0, Math.min(1, 1 - bytesNeeded / bytesTotal))
                : null;

        this.onFinish({
            schemaVersion: 1,
            operationId: this.operationId,
            kind: this.kind,
            outcome,
            startedAt: this.startedAt,
            finishedAt,
            durationMs,
            profile: cloneProfile(this.profile),
            filesTotal: this.workloadKnown.filesTotal ? this.workload.filesTotal : null,
            bytesTotal,
            filesNeeded: this.workloadKnown.filesNeeded ? this.workload.filesNeeded : null,
            bytesNeeded,
            ...this.values,
            dedupRatio,
            phases: { ...this.phases },
            eventLoopLagP95Ms: this.lag.p95(),
            eventLoopLagSamples: this.lag.count(),
            eventLoopLagExcludedSamples: this.excludedLagSamples,
            peakBatchBytes: this.peakBatchBytes,
            wasmChunksBefore: this.wasmChunks?.before ?? null,
            wasmChunksReachable: this.wasmChunks?.reachable ?? null,
            wasmChunksAfter: this.wasmChunks?.after ?? null,
        });
    }

    private scheduleLagProbe(): void {
        this.nextLagProbeAt = this.monotonicNow() + this.eventLoopIntervalMs;
        this.lagProbeVisible = this.isVisible();
        this.lagProbeVisibilityEpoch = this.visibilityEpoch();
        const tick = () => {
            if (this.finished) return;
            const now = this.monotonicNow();
            this.observeScheduledLag(now);
            this.nextLagProbeAt = now + this.eventLoopIntervalMs;
            this.lagProbeVisible = this.isVisible();
            this.lagProbeVisibilityEpoch = this.visibilityEpoch();
            this.lagTimer = setTimeout(tick, this.eventLoopIntervalMs);
            (this.lagTimer as any)?.unref?.();
        };
        this.lagTimer = setTimeout(tick, this.eventLoopIntervalMs);
        (this.lagTimer as any)?.unref?.();
    }

    private observeScheduledLag(now: number): void {
        if (!this.lagProbeVisible || !this.isVisible() ||
            this.lagProbeVisibilityEpoch !== this.visibilityEpoch()) {
            this.excludedLagSamples++;
            return;
        }
        this.lag.observe(Math.max(0, now - this.nextLagProbeAt!));
    }

}

export class PerfTrace {
    private readonly maxRecords: number;
    private readonly monotonicNow: () => number;
    private readonly wallNow: () => number;
    private readonly monitorEventLoop: boolean;
    private readonly eventLoopIntervalMs: number;
    private readonly records: PerfOperationRecord[] = [];
    private readonly active = new Map<string, PerfOperationHandle>();
    private readonly listeners = new Set<(record: PerfOperationRecord) => void>();
    private profile = cloneProfile(DEFAULT_PERF_PROFILE);
    private sequence = 0;
    private visible = true;
    private visibilityGeneration = 0;

    constructor(options: PerfTraceOptions = {}) {
        this.maxRecords = options.maxRecords ?? 20;
        if (!Number.isInteger(this.maxRecords) || this.maxRecords <= 0) {
            throw new RangeError("maxRecords must be a positive integer");
        }
        this.monotonicNow =
            options.monotonicNow ??
            (() => globalThis.performance?.now?.() ?? Date.now());
        this.wallNow = options.wallNow ?? (() => Date.now());
        this.monitorEventLoop = options.monitorEventLoop ?? true;
        this.eventLoopIntervalMs = options.eventLoopIntervalMs ?? 250;
        if (!Number.isFinite(this.eventLoopIntervalMs) || this.eventLoopIntervalMs <= 0) {
            throw new RangeError("eventLoopIntervalMs must be positive");
        }
    }

    setProfile(profile: PerfPlatformProfile): void {
        this.profile = validateProfile(profile);
    }

    getProfile(): PerfPlatformProfile {
        return cloneProfile(this.profile);
    }

    /** Call on every visibility transition, including before startup work. */
    setVisible(visible: boolean): void {
        if (this.visible === visible) return;
        this.visible = visible;
        this.visibilityGeneration++;
    }

    begin(kind: PerfOperationKind): PerfOperation {
        const operationId =
            `${kind}-${Math.trunc(this.wallNow()).toString(36)}-${this.sequence++}`;
        const operation = new PerfOperationHandle(
            operationId,
            kind,
            cloneProfile(this.profile),
            this.monotonicNow,
            this.wallNow,
            this.monitorEventLoop,
            this.eventLoopIntervalMs,
            () => this.visible,
            () => this.visibilityGeneration,
            (record) => {
                this.active.delete(operationId);
                this.records.push(record);
                if (this.records.length > this.maxRecords) {
                    this.records.splice(0, this.records.length - this.maxRecords);
                }
                for (const listener of this.listeners) {
                    try {
                        listener(cloneRecord(record));
                    } catch (error) {
                        console.warn("[obsetync] performance listener failed:", error);
                    }
                }
            },
        );
        this.active.set(operationId, operation);
        return operation;
    }

    /** Observe completed aggregate records. One faulty listener is isolated. */
    subscribe(listener: (record: PerfOperationRecord) => void): () => void {
        this.listeners.add(listener);
        return () => this.listeners.delete(listener);
    }

    recent(): PerfOperationRecord[] {
        return this.records.map(cloneRecord);
    }

    activeSnapshots(limit = 5): PerfActiveOperation[] {
        const safeLimit = Number.isFinite(limit) ? Math.max(0, Math.trunc(limit)) : 0;
        const snapshots: PerfActiveOperation[] = [];
        for (const operation of this.active.values()) {
            if (snapshots.length >= safeLimit) break;
            snapshots.push(operation.snapshot());
        }
        return snapshots;
    }

    clear(): void {
        this.records.length = 0;
    }

    formatDebug(limit = 5): string[] {
        const safeLimit = Number.isFinite(limit) ? Math.max(0, Math.trunc(limit)) : 0;
        const p = this.profile;
        const lines = [
            `Profile:            ${p.runtime}/${p.architecture} · WASM ${p.wasmMode}`,
            `Limits:             hash ${p.hashConcurrency} · read ${p.readConcurrency} · ` +
                `network ${p.networkConcurrency} · feed ${formatBytes(p.feedBytes)} · ` +
                `batch ${p.batchBytes > 0 ? formatBytes(p.batchBytes) : "legacy/count-bound"}`,
            `Active operations:  ${this.active.size}`,
            `Visibility:         ${this.visible ? "visible" : "hidden"} · crossing/hidden lag probes excluded`,
        ];
        for (const active of this.activeSnapshots(safeLimit)) {
            lines.push(
                `  ${active.kind} running ${formatDuration(active.durationMs)}` +
                ` · files ${active.filesCompleted}/${active.filesTotal ?? "?"}` +
                ` · transfer ${formatBytes(active.bytesTransferred)}` +
                ` · no file/byte progress ${formatDuration(active.sinceProgressMs)}` +
                ` · req ${active.requestCount} · WS frames ${active.wsFrameCount}`,
            );
            const phases = active.activePhases.map((phase) =>
                `${phase.name} ${formatDuration(phase.durationMs)}` +
                (phase.count > 1 ? ` (${phase.count} spans)` : ""));
            lines.push(`    Active phases: ${phases.join(" · ") || "between instrumented phases"}`);
        }
        const recent = safeLimit > 0 ? this.records.slice(-safeLimit) : [];
        if (recent.length === 0) {
            lines.push("Recent operations:   (none)");
            return lines;
        }
        lines.push(`Recent operations:   ${recent.length}`);
        for (const record of recent) {
            const files =
                record.filesCompleted > 0 || record.filesTotal !== null
                    ? ` · files ${record.filesCompleted}/${record.filesTotal ?? "?"}`
                    : "";
            const bytes =
                record.bytesTransferred > 0
                    ? ` · transfer ${formatBytes(record.bytesTransferred)}`
                    : "";
            const lag =
                record.eventLoopLagP95Ms === null
                    ? ""
                    : ` · lag p95<=${record.eventLoopLagP95Ms}ms`;
            lines.push(
                `  ${record.kind} ${record.outcome} ${formatDuration(record.durationMs)}` +
                    `${files}${bytes} · req ${record.requestCount} · retry ${record.retries}` +
                    ` · pressure ${record.backpressureEvents}${lag}` +
                    ((record.eventLoopLagExcludedSamples ?? 0) > 0
                        ? ` · hidden/crossing lag samples skipped ${record.eventLoopLagExcludedSamples}`
                        : ""),
            );
        }
        return lines;
    }
}

function formatDuration(ms: number): string {
    if (ms < 1_000) return `${Math.round(ms)}ms`;
    if (ms < 60_000) return `${(ms / 1_000).toFixed(1)}s`;
    return `${(ms / 60_000).toFixed(1)}m`;
}

function formatBytes(bytes: number): string {
    if (bytes <= 0) return "0 B";
    if (bytes < 1_024) return `${Math.round(bytes)} B`;
    if (bytes < 1_048_576) return `${(bytes / 1_024).toFixed(1)} KiB`;
    return `${(bytes / 1_048_576).toFixed(1)} MiB`;
}

export const perfTrace = new PerfTrace();
