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
    resumedPages: number;
    dedupRatio: number | null;
    phases: Partial<Record<PerfPhase, number>>;
    eventLoopLagP95Ms: number | null;
    eventLoopLagSamples: number;
    peakBatchBytes: number;
    wasmChunksBefore: number | null;
    wasmChunksReachable: number | null;
    wasmChunksAfter: number | null;
}

export interface PerfOperation {
    readonly operationId: string;
    readonly kind: PerfOperationKind;
    setWorkload(workload: PerfWorkload): void;
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

    constructor(
        id: string,
        kind: PerfOperationKind,
        private readonly profile: PerfPlatformProfile,
        private readonly monotonicNow: () => number,
        private readonly wallNow: () => number,
        monitorEventLoop: boolean,
        private readonly eventLoopIntervalMs: number,
        private readonly onFinish: (record: PerfOperationRecord) => void,
    ) {
        this.operationId = id;
        this.kind = kind;
        this.startedAt = wallNow();
        this.startedMono = monotonicNow();
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

    increment(delta: PerfIncrement): void {
        if (this.finished) return;
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
        let closed = false;
        return () => {
            if (closed || this.finished) return;
            closed = true;
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
        this.lag.observe(lagMs);
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
        this.finished = true;
        if (this.lagTimer !== null) {
            clearTimeout(this.lagTimer);
            this.lagTimer = null;
        }

        const finishedAt = this.wallNow();
        const durationMs = Math.max(0, this.monotonicNow() - this.startedMono);
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
            peakBatchBytes: this.peakBatchBytes,
            wasmChunksBefore: this.wasmChunks?.before ?? null,
            wasmChunksReachable: this.wasmChunks?.reachable ?? null,
            wasmChunksAfter: this.wasmChunks?.after ?? null,
        });
    }

    private scheduleLagProbe(): void {
        let expected = this.monotonicNow() + this.eventLoopIntervalMs;
        const tick = () => {
            if (this.finished) return;
            const now = this.monotonicNow();
            this.observeEventLoopLag(Math.max(0, now - expected));
            expected = now + this.eventLoopIntervalMs;
            this.lagTimer = setTimeout(tick, this.eventLoopIntervalMs);
            (this.lagTimer as any)?.unref?.();
        };
        this.lagTimer = setTimeout(tick, this.eventLoopIntervalMs);
        (this.lagTimer as any)?.unref?.();
    }

}

export class PerfTrace {
    private readonly maxRecords: number;
    private readonly monotonicNow: () => number;
    private readonly wallNow: () => number;
    private readonly monitorEventLoop: boolean;
    private readonly eventLoopIntervalMs: number;
    private readonly records: PerfOperationRecord[] = [];
    private readonly active = new Set<string>();
    private profile = cloneProfile(DEFAULT_PERF_PROFILE);
    private sequence = 0;

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

    begin(kind: PerfOperationKind): PerfOperation {
        const operationId =
            `${kind}-${Math.trunc(this.wallNow()).toString(36)}-${this.sequence++}`;
        this.active.add(operationId);
        return new PerfOperationHandle(
            operationId,
            kind,
            cloneProfile(this.profile),
            this.monotonicNow,
            this.wallNow,
            this.monitorEventLoop,
            this.eventLoopIntervalMs,
            (record) => {
                this.active.delete(operationId);
                this.records.push(record);
                if (this.records.length > this.maxRecords) {
                    this.records.splice(0, this.records.length - this.maxRecords);
                }
            },
        );
    }

    recent(): PerfOperationRecord[] {
        return this.records.map(cloneRecord);
    }

    clear(): void {
        this.records.length = 0;
    }

    formatDebug(limit = 5): string[] {
        const safeLimit = Math.max(0, Math.trunc(limit));
        const p = this.profile;
        const lines = [
            `Profile:            ${p.runtime}/${p.architecture} · WASM ${p.wasmMode}`,
            `Limits:             hash ${p.hashConcurrency} · read ${p.readConcurrency} · ` +
                `network ${p.networkConcurrency} · feed ${formatBytes(p.feedBytes)} · ` +
                `batch ${p.batchBytes > 0 ? formatBytes(p.batchBytes) : "legacy/count-bound"}`,
            `Active operations:  ${this.active.size}`,
        ];
        const recent = this.records.slice(-safeLimit);
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
                    `${files}${bytes} · req ${record.requestCount} · retry ${record.retries}${lag}`,
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
