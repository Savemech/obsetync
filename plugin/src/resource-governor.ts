import type { HashRuntime, HashTuning } from "./hash-runtime";
import type { PerfOperationRecord } from "./perf-trace";

const KIB = 1024;
const MIB = 1024 * KIB;
const HINT_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const HEALTHY_WINDOWS_TO_CLEAR_HINT = 3;
const HEALTHY_LAG_MS = 16;
const OVERLOAD_LAG_MS = 100;
const GROWTH_RATIO = 1.10;
const PROBE_FLOOR_RATIO = 0.95;
const WINDOW_MIN_MS = 250;
const WINDOW_MAX_MS = 5_000;
const WINDOW_PROBE_COOLDOWN_MS = 3_000;
const WINDOW_PRESSURE_PROBE_COOLDOWN_MS = 6_000;
const WINDOW_HEALTHY_TO_PROBE = 3;
const RESOURCE_AXES = ["read", "hash", "network", "apply"] as const;

export type ResourceArchitecture = "arm64" | "x64" | "unknown";
export type ResourceOs = "ios" | "darwin" | "win32" | "linux" | "unknown";
export type ResourceProfileName = HashTuning["profile"];
export type ResourcePhase = "read" | "hash" | "upload";
export type ResourceAxis = typeof RESOURCE_AXES[number];
export type ResourceConcurrencyLimits = Record<ResourceAxis, number>;

export interface ResourceEnvironment {
    runtime: HashRuntime;
    architecture: ResourceArchitecture;
    os: ResourceOs;
    hardwareConcurrency: number;
    simdAvailable: boolean;
}

export interface ResourceRecoveryHint {
    schema: 1;
    penalty: number;
    updatedAt: number;
    reason: string;
}

export interface ResourceProfile {
    family: "ios-arm64" | "macos-arm64" | "windows-arm64" | "x86-desktop" | "generic";
    name: ResourceProfileName;
    tuning: HashTuning;
}

export interface ResourceMeasurement {
    operationKind?: "push" | "pull" | "scan" | "reconcile";
    outcome: "success" | "error" | "cancelled";
    durationMs: number;
    bytesTransferred: number;
    filesCompleted: number;
    eventLoopLagP95Ms: number | null;
    retries: number;
    backpressureEvents: number;
    peakBatchBytes: number;
    queueDepth?: number;
    averageFileBytes?: number;
    memoryPressure?: boolean;
    visible?: boolean;
    phaseThroughput?: Partial<Record<ResourcePhase, number>>;
}

/** Non-overlapping deltas supplied by the performance collector, not totals.
 * durationMs is elapsed wall time; activeDurationMs excludes lifecycle pauses.
 * Sequence is globally increasing for one collector/governor lifetime. */
export interface ResourceWindowMeasurement extends ResourceMeasurement {
    sequence: number;
    startedAtMs: number;
    endedAtMs: number;
    activeDurationMs: number;
    continuousVisible: boolean;
    /** Actual known backlog by stage. Missing demand never implies saturation. */
    demand?: Partial<ResourceConcurrencyLimits>;
    /** Actual runtime capabilities, e.g. hash=1 without hash workers. */
    maxConcurrency?: Partial<ResourceConcurrencyLimits>;
    /** Byte-lease accounting, not an RSS estimate. Only fully covered axes may
     * grow; missing/empty coverage means partial diagnostics, never permission. */
    budget?: { limitBytes: number; reservedBytes: number; coveredAxes?: ResourceAxis[] };
}

/** Dynamic buffers on these operation/stage paths are reserve-before-use in
 * the shared transient ledger. Coverage is deliberately intersected with
 * explicit live demand: operation kind alone never grants growth, and fixed
 * metadata/native/RSS allocations remain outside this claim. */
const ADMITTED_WINDOW_AXES: Record<
    NonNullable<ResourceMeasurement["operationKind"]>,
    readonly ResourceAxis[]
> = {
    scan: ["read", "hash"],
    push: ["read", "hash", "network"],
    // Pull/reconcile do not yet publish stage demand. Keep them ineligible
    // until their exact admitted spans provide comparable window evidence.
    pull: [],
    reconcile: [],
};

export function coveredResourceAxesForWindow(
    measurement: Pick<ResourceWindowMeasurement, "operationKind" | "demand">,
): ResourceAxis[] {
    if (!measurement.operationKind || !measurement.demand) return [];
    return ADMITTED_WINDOW_AXES[measurement.operationKind].filter((axis) => {
        const demand = measurement.demand?.[axis];
        return typeof demand === "number" && Number.isFinite(demand) && demand > 0;
    });
}

export interface ResourceGovernorSnapshot {
    family: ResourceProfile["family"];
    profile: ResourceProfileName;
    profileIndex: number;
    profileCount: number;
    decision: string;
    bottleneck: string;
    throughput: number | null;
    throughputUnit: "bytes/s" | "files/s" | null;
    eventLoopLagP95Ms: number | null;
    recoveryPenalty: number;
    visible: boolean;
    usefulWindows: number;
    phaseThroughput: Record<ResourcePhase, number | null>;
    controls: ResourceConcurrencyLimits;
    probeAxis: ResourceAxis | null;
    inputMode: "operations" | "active-windows";
}

export interface AdaptiveResourceGovernorOptions {
    now?: () => number;
    recoveryHint?: ResourceRecoveryHint | null;
    visible?: boolean;
    onProfileChange?: (profile: ResourceProfile, reason: string) => void;
    onRecoveryHintChange?: (hint: ResourceRecoveryHint | null) => void;
}

export interface ResourceProfileSet {
    profiles: ResourceProfile[];
    initialIndex: number;
}

function cores(value: number): number {
    return Number.isFinite(value) ? Math.max(1, Math.trunc(value)) : 1;
}

function tuning(
    runtime: HashRuntime,
    profile: ResourceProfileName,
    values: {
        hash: number;
        maxHash: number;
        network: number;
        read: number;
        feed: number;
        maxFeed: number;
        batch: number;
        files: number;
        transient: number;
        yieldMs: number;
    },
): HashTuning {
    return {
        runtime,
        profile,
        hashConcurrency: values.hash,
        maxHashConcurrency: values.maxHash,
        networkConcurrency: values.network,
        applyConcurrency: runtime === "mobile"
            ? Math.min(8, Math.max(2, values.network * 2))
            : Math.min(16, Math.max(2, values.network * 4)),
        feedBytes: values.feed,
        minFeedBytes: 64 * KIB,
        maxFeedBytes: values.maxFeed,
        maxBatchFiles: values.files,
        maxBatchBytes: values.batch,
        maxSingleBatchFileBytes: MIB,
        maxBatchHoldMs: runtime === "mobile" ? 4 : 8,
        readConcurrency: values.read,
        transientBudgetBytes: values.transient,
        yieldBudgetMs: values.yieldMs,
    };
}

function profile(
    family: ResourceProfile["family"],
    runtime: HashRuntime,
    name: ResourceProfileName,
    values: Parameters<typeof tuning>[2],
): ResourceProfile {
    return { family, name, tuning: tuning(runtime, name, values) };
}

/** Hardware chooses only a safe profile ladder. Runtime evidence chooses a rung. */
export function resourceProfilesFor(environment: ResourceEnvironment): ResourceProfileSet {
    const logicalCores = cores(environment.hardwareConcurrency);
    if (environment.runtime === "mobile" || environment.os === "ios") {
        const family: ResourceProfile["family"] = environment.os === "ios"
            ? "ios-arm64"
            : "generic";
        return {
            initialIndex: environment.simdAvailable ? 1 : 0,
            profiles: [
                profile(family, "mobile", "recovery", {
                    hash: 1, maxHash: 1, network: 1, read: 1,
                    feed: 128 * KIB, maxFeed: 128 * KIB, batch: MIB,
                    files: 32, transient: 32 * MIB, yieldMs: 8,
                }),
                profile(family, "mobile", "conservative", {
                    hash: 1, maxHash: 1, network: 4, read: 1,
                    feed: 256 * KIB, maxFeed: 256 * KIB, batch: MIB,
                    files: 64, transient: 48 * MIB, yieldMs: 8,
                }),
                profile(family, "mobile", "balanced", {
                    hash: 1, maxHash: 1, network: 6, read: 2,
                    feed: 256 * KIB, maxFeed: 256 * KIB, batch: 2 * MIB,
                    files: 64, transient: 64 * MIB, yieldMs: 10,
                }),
            ],
        };
    }

    if (environment.os === "darwin" && environment.architecture === "arm64") {
        const family = "macos-arm64" as const;
        const maximum = Math.min(4, Math.max(1, logicalCores - 1));
        return {
            initialIndex: environment.simdAvailable && logicalCores >= 4 ? 2 : 1,
            profiles: [
                profile(family, "desktop", "recovery", {
                    hash: 1, maxHash: maximum, network: 1, read: 1,
                    feed: 128 * KIB, maxFeed: 256 * KIB, batch: 2 * MIB,
                    files: 64, transient: 64 * MIB, yieldMs: 8,
                }),
                profile(family, "desktop", "conservative", {
                    hash: Math.min(2, maximum), maxHash: maximum, network: 2, read: 2,
                    feed: 256 * KIB, maxFeed: 512 * KIB, batch: 4 * MIB,
                    files: 128, transient: 96 * MIB, yieldMs: 10,
                }),
                profile(family, "desktop", "balanced", {
                    hash: maximum, maxHash: maximum, network: 4, read: maximum,
                    feed: 512 * KIB, maxFeed: MIB, batch: 8 * MIB,
                    files: 256, transient: 128 * MIB, yieldMs: 12,
                }),
                profile(family, "desktop", "throughput", {
                    hash: maximum, maxHash: maximum, network: 4, read: maximum,
                    feed: MIB, maxFeed: MIB, batch: 16 * MIB,
                    files: 256, transient: 192 * MIB, yieldMs: 12,
                }),
            ],
        };
    }

    if (environment.os === "win32" && environment.architecture === "arm64") {
        const family = "windows-arm64" as const;
        const maximum = Math.min(4, logicalCores);
        return {
            initialIndex: environment.simdAvailable && logicalCores >= 4 ? 1 : 0,
            profiles: [
                profile(family, "desktop", "recovery", {
                    hash: 1, maxHash: maximum, network: 1, read: 1,
                    feed: 128 * KIB, maxFeed: 256 * KIB, batch: 2 * MIB,
                    files: 64, transient: 64 * MIB, yieldMs: 8,
                }),
                profile(family, "desktop", "conservative", {
                    hash: Math.min(2, maximum), maxHash: maximum, network: 2, read: 2,
                    feed: 256 * KIB, maxFeed: 512 * KIB, batch: 4 * MIB,
                    files: 128, transient: 96 * MIB, yieldMs: 10,
                }),
                profile(family, "desktop", "balanced", {
                    hash: Math.min(3, maximum), maxHash: maximum, network: 3,
                    read: Math.min(3, maximum), feed: 512 * KIB, maxFeed: MIB,
                    batch: 8 * MIB, files: 192, transient: 128 * MIB, yieldMs: 12,
                }),
                profile(family, "desktop", "throughput", {
                    hash: maximum, maxHash: maximum, network: 4, read: maximum,
                    feed: MIB, maxFeed: MIB, batch: 16 * MIB,
                    files: 256, transient: 192 * MIB, yieldMs: 12,
                }),
            ],
        };
    }

    if (environment.architecture === "x64") {
        const family = "x86-desktop" as const;
        const maximum = Math.min(4, Math.max(1, logicalCores - 1));
        return {
            initialIndex: environment.simdAvailable && logicalCores >= 4 ? 2 : 1,
            profiles: [
                profile(family, "desktop", "recovery", {
                    hash: 1, maxHash: maximum, network: 1, read: 1,
                    feed: 128 * KIB, maxFeed: 256 * KIB, batch: 2 * MIB,
                    files: 64, transient: 64 * MIB, yieldMs: 8,
                }),
                profile(family, "desktop", "conservative", {
                    hash: Math.min(2, maximum), maxHash: maximum, network: 2, read: 2,
                    feed: 256 * KIB, maxFeed: 512 * KIB, batch: 4 * MIB,
                    files: 128, transient: 96 * MIB, yieldMs: 10,
                }),
                profile(family, "desktop", "balanced", {
                    hash: maximum, maxHash: maximum, network: 4, read: maximum,
                    feed: 512 * KIB, maxFeed: MIB, batch: 8 * MIB,
                    files: 256, transient: 128 * MIB, yieldMs: 12,
                }),
                profile(family, "desktop", "throughput", {
                    hash: maximum, maxHash: maximum, network: 4, read: maximum,
                    feed: MIB, maxFeed: MIB, batch: 16 * MIB,
                    files: 256, transient: 192 * MIB, yieldMs: 12,
                }),
            ],
        };
    }

    const family = "generic" as const;
    const maximum = Math.min(2, logicalCores);
    return {
        initialIndex: environment.simdAvailable ? 1 : 0,
        profiles: [
            profile(family, environment.runtime, "recovery", {
                hash: 1, maxHash: maximum, network: 1, read: 1,
                feed: 64 * KIB, maxFeed: 128 * KIB, batch: MIB,
                files: 32, transient: 32 * MIB, yieldMs: 8,
            }),
            profile(family, environment.runtime, "conservative", {
                hash: Math.min(2, maximum), maxHash: maximum, network: 2,
                read: Math.min(2, maximum), feed: 128 * KIB, maxFeed: 512 * KIB,
                batch: 2 * MIB, files: 64, transient: 64 * MIB, yieldMs: 10,
            }),
        ],
    };
}

function validHint(hint: ResourceRecoveryHint | null | undefined, now: number): boolean {
    return !!hint && hint.schema === 1 && Number.isInteger(hint.penalty) &&
        hint.penalty > 0 && hint.penalty <= 2 && Number.isFinite(hint.updatedAt) &&
        hint.updatedAt >= 0 && now - hint.updatedAt <= HINT_TTL_MS &&
        typeof hint.reason === "string";
}

function concurrencyLimits(value: HashTuning): ResourceConcurrencyLimits {
    return {
        read: value.readConcurrency,
        hash: value.hashConcurrency,
        network: value.networkConcurrency,
        apply: value.applyConcurrency,
    };
}

function withConcurrency(value: HashTuning, limits: ResourceConcurrencyLimits): HashTuning {
    return {
        ...value,
        readConcurrency: limits.read,
        hashConcurrency: limits.hash,
        networkConcurrency: limits.network,
        applyConcurrency: limits.apply,
    };
}

function clampConcurrency(value: HashTuning, caps: ResourceConcurrencyLimits): HashTuning {
    const limits = concurrencyLimits(value);
    for (const axis of RESOURCE_AXES) limits[axis] = Math.min(limits[axis], caps[axis]);
    return withConcurrency(value, limits);
}

export function measurementFromPerf(
    record: PerfOperationRecord,
    extras: Pick<ResourceMeasurement, "visible" | "memoryPressure" | "queueDepth" | "averageFileBytes"> = {},
): ResourceMeasurement {
    const sourceBytes = record.bytesTotal ?? record.bytesTransferred;
    const phaseThroughput: Partial<Record<ResourcePhase, number>> = {};
    const phaseInputs: Array<[ResourcePhase, number, number | undefined]> = [
        ["read", sourceBytes, record.phases.read],
        ["hash", sourceBytes, record.phases.hash],
        ["upload", record.bytesTransferred, record.phases.upload],
    ];
    for (const [phase, bytes, durationMs] of phaseInputs) {
        if (bytes > 0 && durationMs !== undefined && durationMs > 0) {
            phaseThroughput[phase] = bytes * 1000 / durationMs;
        }
    }
    return {
        outcome: record.outcome,
        operationKind: record.kind,
        durationMs: record.durationMs,
        bytesTransferred: record.bytesTransferred,
        filesCompleted: record.filesCompleted,
        eventLoopLagP95Ms: record.eventLoopLagP95Ms,
        retries: record.retries,
        backpressureEvents: record.backpressureEvents,
        peakBatchBytes: record.peakBatchBytes,
        phaseThroughput,
        ...extras,
    };
}

/**
 * Operation-window AIMD with hysteresis and probe rollback. It never uses
 * filenames, hashes, URLs, or content and therefore remains safe to export.
 */
export class AdaptiveResourceGovernor {
    private readonly profiles: ResourceProfile[];
    private readonly now: () => number;
    private readonly onProfileChange?: (profile: ResourceProfile, reason: string) => void;
    private readonly onRecoveryHintChange?: (hint: ResourceRecoveryHint | null) => void;
    private index: number;
    private visible: boolean;
    private hint: ResourceRecoveryHint | null;
    private healthyHintWindows = 0;
    private readonly previousThroughputs = new Map<string, number>();
    private throughputUnit: "bytes/s" | "files/s" | null = null;
    private growthWindows = 0;
    private growthKey: string | null = null;
    private probe: {
        from: number;
        baseline: number;
        remaining: number;
        key: string;
    } | null = null;
    private lastDecision = "initial platform profile";
    private lastBottleneck = "unmeasured";
    private lastThroughput: number | null = null;
    private lastLag: number | null = null;
    private usefulWindows = 0;
    private phaseRates: Record<ResourcePhase, number | null> = {
        read: null,
        hash: null,
        upload: null,
    };
    private windowTuning: HashTuning | null = null;
    private lastWindowSequence = -1;
    private lastWindowEndedAt = -Infinity;
    private windowHealthy = 0;
    private windowHealthyKey: string | null = null;
    private windowCooldownMs = WINDOW_PROBE_COOLDOWN_MS;
    private nextProbeAxis = 0;
    private windowCaps: ResourceConcurrencyLimits;
    private windowProbe: {
        axis: ResourceAxis;
        from: number;
        baseline: number;
        remaining: number;
        key: string;
    } | null = null;

    constructor(
        readonly environment: ResourceEnvironment,
        options: AdaptiveResourceGovernorOptions = {},
    ) {
        const selected = resourceProfilesFor(environment);
        this.profiles = selected.profiles;
        this.windowCaps = concurrencyLimits(this.profiles.at(-1)!.tuning);
        this.now = options.now ?? (() => Date.now());
        this.onProfileChange = options.onProfileChange;
        this.onRecoveryHintChange = options.onRecoveryHintChange;
        this.visible = options.visible ?? true;
        const now = this.now();
        this.hint = validHint(options.recoveryHint, now)
            ? { ...options.recoveryHint! }
            : null;
        this.index = Math.max(
            0,
            selected.initialIndex - (this.hint?.penalty ?? 0),
        );
        if (options.recoveryHint && !this.hint) {
            this.onRecoveryHintChange?.(null);
            this.lastDecision = "expired recovery hint cleared";
        } else if (this.hint) {
            this.lastDecision = `recovery penalty ${this.hint.penalty} applied`;
            this.lastBottleneck = this.hint.reason;
        }
    }

    current(): ResourceProfile {
        const current = this.profiles[this.index];
        return { ...current, tuning: { ...(this.windowTuning ?? current.tuning) } };
    }

    setVisible(visible: boolean): void {
        if (this.visible !== visible) {
            this.rollbackWindowProbe("visibility transition");
            this.windowHealthy = 0;
            this.windowHealthyKey = null;
            this.windowCooldownMs = WINDOW_PROBE_COOLDOWN_MS;
        }
        this.visible = visible;
        if (!visible && this.environment.runtime === "mobile") {
            this.lastBottleneck = "application hidden";
        }
    }

    recordSimdAvailability(available: boolean): void {
        if (this.environment.simdAvailable === available) return;
        this.environment.simdAvailable = available;
        if (this.windowTuning) {
            if (!available) {
                const next = { ...this.windowTuning };
                next.hashConcurrency = Math.min(next.hashConcurrency, 2);
                next.feedBytes = Math.min(next.feedBytes, 256 * KIB);
                this.resetWindowProbe();
                this.applyWindowTuning(next, "scalar WASM CPU fallback");
            }
            return;
        }
        if (!available && this.index > 1) {
            this.growthWindows = 0;
            this.growthKey = null;
            this.probe = null;
            this.lastBottleneck = "SIMD unavailable";
            this.changeProfile(this.index - 1, "scalar WASM conservative fallback");
        }
    }

    recordInterruption(phase: string): void {
        const reason = `previous renderer interruption during ${phase || "unknown"}`;
        const penalty = Math.min(2, Math.max(1, (this.hint?.penalty ?? 0) + 1));
        this.hint = { schema: 1, penalty, updatedAt: this.now(), reason };
        this.healthyHintWindows = 0;
        this.growthWindows = 0;
        this.growthKey = null;
        this.probe = null;
        this.resetWindowProbe();
        this.onRecoveryHintChange?.({ ...this.hint });
        const next = Math.max(0, this.index - 1);
        if (this.windowTuning && next === this.index) {
            this.applyWindowTuning(clampConcurrency(this.profiles[next].tuning, this.windowCaps), reason);
        }
        this.changeProfile(next, reason);
        this.lastBottleneck = reason;
    }

    observe(measurement: ResourceMeasurement): void {
        // Once the collector supplies window deltas, completed aggregates must
        // not count the same pressure/work a second time.
        if (this.windowTuning) return;
        if (!this.validMeasurement(measurement)) return;
        const visible = measurement.visible ?? this.visible;
        this.lastLag = measurement.eventLoopLagP95Ms;
        const usesBytes = measurement.bytesTransferred > 0;
        const work = usesBytes ? measurement.bytesTransferred : measurement.filesCompleted;
        if (work <= 0 || measurement.durationMs <= 0) {
            this.lastDecision = "idle window ignored";
            return;
        }
        this.usefulWindows++;
        const unit = usesBytes ? "bytes/s" : "files/s";
        const throughputKey = `${measurement.operationKind ?? "generic"}:${unit}`;
        const throughput = work * 1000 / measurement.durationMs;
        this.lastThroughput = throughput;
        this.updatePhaseRates(measurement.phaseThroughput);

        const overloaded = measurement.memoryPressure === true ||
            (measurement.eventLoopLagP95Ms ?? 0) > OVERLOAD_LAG_MS ||
            measurement.backpressureEvents > 0 || measurement.retries >= 2 ||
            measurement.peakBatchBytes > this.current().tuning.transientBudgetBytes;
        if (overloaded) {
            const reason = this.overloadReason(measurement);
            this.growthWindows = 0;
            this.growthKey = null;
            this.probe = null;
            this.previousThroughputs.set(throughputKey, throughput);
            this.throughputUnit = unit;
            this.lastBottleneck = reason;
            this.changeProfile(this.halvedProfileIndex(), `multiplicative decrease: ${reason}`);
            return;
        }

        const uiMeasuredOrTriviallyShort = measurement.eventLoopLagP95Ms !== null ||
            measurement.durationMs <= HEALTHY_LAG_MS;
        const uiHealthy = measurement.eventLoopLagP95Ms === null
            ? measurement.durationMs <= HEALTHY_LAG_MS
            : measurement.eventLoopLagP95Ms <= HEALTHY_LAG_MS;
        if (this.probe && this.probe.key === throughputKey && uiMeasuredOrTriviallyShort) {
            const regressed = measurement.outcome !== "success" || !visible || !uiHealthy ||
                throughput < this.probe.baseline * PROBE_FLOOR_RATIO;
            if (regressed) {
                const from = this.probe.from;
                this.probe = null;
                this.growthWindows = 0;
                this.growthKey = null;
                this.previousThroughputs.set(throughputKey, throughput);
                this.throughputUnit = unit;
                this.lastBottleneck = "additive probe regressed throughput or UI latency";
                this.changeProfile(from, "probe rollback: conservative profile was faster");
                return;
            }
            this.probe.remaining--;
            if (this.probe.remaining <= 0) this.probe = null;
        }

        const healthy = measurement.outcome === "success" && visible && uiHealthy &&
            measurement.retries === 0 && measurement.backpressureEvents === 0;
        if (!healthy) {
            this.growthWindows = 0;
            this.growthKey = null;
            this.lastBottleneck = !visible
                ? "application hidden"
                : !uiMeasuredOrTriviallyShort
                    ? "UI latency unmeasured"
                    : "non-healthy operation window";
            this.lastDecision = "profile held";
            this.previousThroughputs.set(throughputKey, throughput);
            this.throughputUnit = unit;
            return;
        }

        this.clearRecoveryHintAfterHealthyWindow();
        const previousThroughput = this.previousThroughputs.get(throughputKey);
        if (
            previousThroughput !== undefined &&
            throughput >= previousThroughput * GROWTH_RATIO
        ) {
            this.growthWindows = this.growthKey === throughputKey
                ? this.growthWindows + 1
                : 1;
            this.growthKey = throughputKey;
        } else {
            this.growthWindows = 0;
            this.growthKey = throughputKey;
        }
        const baseline = previousThroughput;
        this.previousThroughputs.set(throughputKey, throughput);
        this.throughputUnit = unit;
        this.lastBottleneck = this.bottleneckFrom(measurement);

        if (this.growthWindows >= 2 && this.index < this.profiles.length - 1) {
            const from = this.index;
            const probeBaseline = Math.max(baseline ?? throughput, throughput / GROWTH_RATIO);
            this.growthWindows = 0;
            this.growthKey = null;
            this.changeProfile(this.index + 1, "additive increase after two +10% windows");
            this.probe = { from, baseline: probeBaseline, remaining: 2, key: throughputKey };
        } else {
            this.lastDecision = "profile held after healthy window";
        }
    }

    /** Independent controls trained only by bounded, non-overlapping windows. */
    observeWindow(measurement: ResourceWindowMeasurement): void {
        if (!this.validWindow(measurement) || measurement.sequence <= this.lastWindowSequence) return;
        this.lastWindowSequence = measurement.sequence;
        // Concurrent operations may report the same wall interval. Never use
        // that time twice for hysteresis/cooldown or repeated pressure cuts.
        if (measurement.startedAtMs < this.lastWindowEndedAt) return;
        if (Number.isFinite(this.lastWindowEndedAt) &&
            measurement.startedAtMs - this.lastWindowEndedAt > WINDOW_MAX_MS) {
            this.rollbackWindowProbe("unobserved gap between windows");
            this.resetWindowProbe();
        }
        this.lastWindowEndedAt = measurement.endedAtMs;
        this.windowTuning ??= { ...this.current().tuning };
        const platformCaps = concurrencyLimits(this.profiles.at(-1)!.tuning);
        for (const axis of RESOURCE_AXES) {
            const supplied = measurement.maxConcurrency?.[axis];
            if (supplied !== undefined) this.windowCaps[axis] = Math.min(platformCaps[axis], supplied);
        }
        let bounded = clampConcurrency(this.windowTuning, this.windowCaps);
        const currentLimits = concurrencyLimits(this.windowTuning);
        const boundedLimits = concurrencyLimits(bounded);
        const capReduced = RESOURCE_AXES.some((axis) => boundedLimits[axis] < currentLimits[axis]);
        if (capReduced && this.windowProbe) {
            const probe = this.windowProbe;
            boundedLimits[probe.axis] = Math.min(boundedLimits[probe.axis], probe.from);
            bounded = withConcurrency(bounded, boundedLimits);
        }
        this.applyWindowTuning(bounded, "runtime capability cap");
        if (capReduced) this.resetWindowProbe();
        this.lastLag = measurement.eventLoopLagP95Ms;

        // Real lease pressure remains actionable with no progress and while
        // hidden. Delayed timers and transport retries during suspension do not.
        const budgetLimit = Math.min(
            this.windowTuning.transientBudgetBytes,
            measurement.budget?.limitBytes ?? Infinity,
        );
        const memoryPressure = measurement.memoryPressure === true ||
            (measurement.budget !== undefined && measurement.budget.reservedBytes > measurement.budget.limitBytes) ||
            measurement.peakBatchBytes > this.windowTuning.transientBudgetBytes;
        if (memoryPressure) {
            this.reduceWindowAxes(RESOURCE_AXES, "transient byte pressure", true);
            return;
        }
        const continuous = measurement.continuousVisible && this.visible &&
            measurement.visible !== false && measurement.durationMs <= WINDOW_MAX_MS &&
            measurement.activeDurationMs >= WINDOW_MIN_MS &&
            measurement.activeDurationMs >= measurement.durationMs * 0.9;
        if (!continuous) {
            this.rollbackWindowProbe("window interrupted or not continuously visible");
            this.resetWindowProbe();
            this.lastBottleneck = "suspended, hidden or unbounded window";
            this.lastDecision = "controls held without continuous foreground evidence";
            return;
        }
        this.windowCooldownMs = Math.max(0, this.windowCooldownMs - measurement.activeDurationMs);
        const cpuPressure = (measurement.eventLoopLagP95Ms ?? 0) > OVERLOAD_LAG_MS;
        const networkPressure = measurement.backpressureEvents > 0 || measurement.retries >= 2;
        if (cpuPressure || networkPressure) {
            const affected: ResourceAxis[] = [];
            if (cpuPressure) affected.push("read", "hash", "apply");
            if (networkPressure) affected.push("network");
            this.reduceWindowAxes(affected, cpuPressure
                ? "foreground UI lag >100ms" : "transport/server pressure");
            return;
        }

        const usesBytes = measurement.bytesTransferred > 0;
        const work = usesBytes ? measurement.bytesTransferred : measurement.filesCompleted;
        const healthy = measurement.outcome === "success" &&
            measurement.eventLoopLagP95Ms !== null &&
            measurement.eventLoopLagP95Ms <= HEALTHY_LAG_MS &&
            measurement.retries === 0 && measurement.backpressureEvents === 0;
        if (!healthy || work <= 0) {
            this.rollbackWindowProbe(!healthy ? "unhealthy or unmeasured UI window" : "no comparable work");
            this.windowHealthy = 0;
            this.windowHealthyKey = null;
            this.healthyHintWindows = 0;
            this.lastBottleneck = measurement.eventLoopLagP95Ms === null
                ? "UI latency unmeasured" : work <= 0 ? "no useful work" : "non-healthy window";
            this.lastDecision = "controls held";
            return;
        }

        this.usefulWindows++;
        const unit = usesBytes ? "bytes/s" : "files/s";
        const key = `${measurement.operationKind ?? "generic"}:${unit}`;
        const throughput = work * 1000 / measurement.activeDurationMs;
        this.lastThroughput = throughput;
        this.throughputUnit = unit;
        this.updatePhaseRates(measurement.phaseThroughput);
        this.clearRecoveryHintAfterHealthyWindow();
        this.lastBottleneck = this.bottleneckFrom(measurement);
        if (this.windowProbe) {
            const probe = this.windowProbe;
            if (!measurement.budget?.coveredAxes?.includes(probe.axis)) {
                this.rollbackWindowProbe("byte budget coverage no longer available");
                return;
            }
            if (probe.key !== key || (measurement.demand?.[probe.axis] ?? 0) <= 0) {
                this.rollbackWindowProbe("probe workload changed or backlog drained");
                return;
            }
            if (throughput < probe.baseline * PROBE_FLOOR_RATIO) {
                this.rollbackWindowProbe("probe regressed comparable throughput");
                return;
            }
            if (--probe.remaining === 0) {
                this.windowProbe = null;
                this.windowHealthy = 0;
                this.windowCooldownMs = WINDOW_PROBE_COOLDOWN_MS;
                this.lastDecision = `single-axis ${probe.axis} probe retained`;
            } else {
                this.lastDecision = `validating single-axis ${probe.axis} probe`;
            }
            return;
        }
        this.windowHealthy = this.windowHealthyKey === key
            ? Math.min(WINDOW_HEALTHY_TO_PROBE, this.windowHealthy + 1)
            : 1;
        this.windowHealthyKey = key;
        if (this.windowHealthy < WINDOW_HEALTHY_TO_PROBE || this.windowCooldownMs > 0) {
            this.lastDecision = "healthy window; probe cooldown/hysteresis";
            return;
        }
        if (!measurement.budget) {
            this.lastDecision = "controls held: byte headroom unmeasured";
            return;
        }
        const coveredAxes = measurement.budget.coveredAxes ?? [];
        if (coveredAxes.length === 0) {
            this.lastDecision = "controls held: byte budget coverage incomplete";
            return;
        }
        const limits = concurrencyLimits(this.windowTuning);
        for (let offset = 0; offset < RESOURCE_AXES.length; offset++) {
            const ordinal = (this.nextProbeAxis + offset) % RESOURCE_AXES.length;
            const axis = RESOURCE_AXES[ordinal];
            if (!coveredAxes.includes(axis)) continue;
            if ((measurement.demand?.[axis] ?? 0) <= 0 || limits[axis] >= this.windowCaps[axis]) continue;
            // Admission still belongs to the byte-budget allocator. This is a
            // conservative extra-slot estimate, not an RSS or allocation claim.
            const slotBytes = axis === "hash"
                ? this.windowTuning.feedBytes * 2
                : this.windowTuning.maxBatchBytes * 2;
            if (measurement.budget.reservedBytes + slotBytes > budgetLimit * 0.8) continue;
            const from = limits[axis];
            limits[axis]++;
            this.applyWindowTuning(withConcurrency(this.windowTuning, limits), `single-axis ${axis} probe +1`);
            this.windowProbe = { axis, from, baseline: throughput, remaining: 2, key };
            this.nextProbeAxis = (ordinal + 1) % RESOURCE_AXES.length;
            this.windowHealthy = 0;
            this.windowCooldownMs = WINDOW_PROBE_COOLDOWN_MS;
            return;
        }
        this.lastDecision = "controls held: no demanded, budget-covered axis with safe headroom";
    }

    private validWindow(value: ResourceWindowMeasurement): boolean {
        if (!this.validMeasurement(value) || !Number.isSafeInteger(value.sequence) || value.sequence < 0 ||
            !Number.isFinite(value.startedAtMs) || value.startedAtMs < 0 ||
            !Number.isFinite(value.endedAtMs) || value.endedAtMs < value.startedAtMs ||
            Math.abs(value.endedAtMs - value.startedAtMs - value.durationMs) > 1 ||
            !Number.isFinite(value.activeDurationMs) || value.activeDurationMs < 0 ||
            value.activeDurationMs > value.durationMs || typeof value.continuousVisible !== "boolean") return false;
        for (const axis of RESOURCE_AXES) {
            const demand = value.demand?.[axis];
            const cap = value.maxConcurrency?.[axis];
            if (demand !== undefined && (!Number.isFinite(demand) || demand < 0)) return false;
            if (cap !== undefined && (!Number.isSafeInteger(cap) || cap < 1)) return false;
        }
        return !value.budget || (Number.isFinite(value.budget.limitBytes) && value.budget.limitBytes > 0 &&
            Number.isFinite(value.budget.reservedBytes) && value.budget.reservedBytes >= 0 &&
            (value.budget.coveredAxes === undefined ||
                (Array.isArray(value.budget.coveredAxes) && value.budget.coveredAxes.length <= RESOURCE_AXES.length &&
                    value.budget.coveredAxes.every((axis) => RESOURCE_AXES.includes(axis)))));
    }

    private resetWindowProbe(cooldownMs = WINDOW_PROBE_COOLDOWN_MS): void {
        this.windowProbe = null;
        this.windowHealthy = 0;
        this.windowHealthyKey = null;
        this.windowCooldownMs = cooldownMs;
        this.healthyHintWindows = 0;
    }

    private rollbackWindowProbe(reason: string): void {
        const probe = this.windowProbe;
        if (!probe || !this.windowTuning) return;
        const limits = concurrencyLimits(this.windowTuning);
        limits[probe.axis] = Math.min(limits[probe.axis], probe.from);
        this.applyWindowTuning(withConcurrency(this.windowTuning, limits), `single-axis ${probe.axis} probe rollback: ${reason}`);
        this.resetWindowProbe(WINDOW_PRESSURE_PROBE_COOLDOWN_MS);
    }

    private reduceWindowAxes(axes: readonly ResourceAxis[], reason: string, memory = false): void {
        const current = this.windowTuning!;
        const limits = concurrencyLimits(current);
        if (this.windowProbe) {
            limits[this.windowProbe.axis] = Math.min(limits[this.windowProbe.axis], this.windowProbe.from);
        }
        for (const axis of axes) limits[axis] = Math.max(1, Math.ceil(limits[axis] / 2));
        const next = withConcurrency(current, limits);
        if (memory) {
            next.maxBatchBytes = Math.max(MIB, Math.floor(current.maxBatchBytes / 2));
            next.feedBytes = Math.max(current.minFeedBytes, Math.floor(current.feedBytes / 2));
        }
        // The configured byte ceiling is unchanged; pressure is not measured RSS.
        this.applyWindowTuning(next, `independent multiplicative decrease: ${reason}`);
        this.resetWindowProbe(WINDOW_PRESSURE_PROBE_COOLDOWN_MS);
        this.lastBottleneck = reason;
    }

    private applyWindowTuning(next: HashTuning, reason: string): void {
        const previous = this.windowTuning;
        if (previous && Object.keys(next).every((key) =>
            next[key as keyof HashTuning] === previous[key as keyof HashTuning])) return;
        this.windowTuning = next;
        try {
            this.onProfileChange?.(this.current(), reason);
            this.lastDecision = reason;
        } catch (error) {
            this.windowTuning = previous;
            this.lastDecision = `profile transition failed: ${reason}`;
            throw error;
        }
    }

    snapshot(): ResourceGovernorSnapshot {
        const current = this.profiles[this.index];
        return {
            family: current.family,
            profile: current.name,
            profileIndex: this.index,
            profileCount: this.profiles.length,
            decision: this.lastDecision,
            bottleneck: this.lastBottleneck,
            throughput: this.lastThroughput,
            throughputUnit: this.lastThroughput === null ? null : this.throughputUnit,
            eventLoopLagP95Ms: this.lastLag,
            recoveryPenalty: this.hint?.penalty ?? 0,
            visible: this.visible,
            usefulWindows: this.usefulWindows,
            phaseThroughput: { ...this.phaseRates },
            controls: concurrencyLimits(this.current().tuning),
            probeAxis: this.windowProbe?.axis ?? null,
            inputMode: this.windowTuning ? "active-windows" : "operations",
        };
    }

    private validMeasurement(value: ResourceMeasurement): boolean {
        return Number.isFinite(value.durationMs) && value.durationMs >= 0 &&
            Number.isFinite(value.bytesTransferred) && value.bytesTransferred >= 0 &&
            Number.isFinite(value.filesCompleted) && value.filesCompleted >= 0 &&
            (value.eventLoopLagP95Ms === null ||
                (Number.isFinite(value.eventLoopLagP95Ms) && value.eventLoopLagP95Ms >= 0)) &&
            Number.isFinite(value.retries) && value.retries >= 0 &&
            Number.isFinite(value.backpressureEvents) && value.backpressureEvents >= 0 &&
            Number.isFinite(value.peakBatchBytes) && value.peakBatchBytes >= 0;
    }

    private overloadReason(value: ResourceMeasurement): string {
        if (value.memoryPressure) return "memory pressure";
        if (value.peakBatchBytes > this.current().tuning.transientBudgetBytes) {
            return "transient memory budget exceeded";
        }
        if ((value.eventLoopLagP95Ms ?? 0) > OVERLOAD_LAG_MS) return "event-loop lag >100ms";
        if (value.backpressureEvents > 0) return "transport/server backpressure";
        return "repeated transport retries";
    }

    private bottleneckFrom(value: ResourceMeasurement): string {
        if ((value.queueDepth ?? 0) > this.current().tuning.hashConcurrency * 2) {
            return "hash/read queue depth";
        }
        if (
            value.averageFileBytes !== undefined && value.averageFileBytes < 64 * KIB &&
            value.filesCompleted > 0
        ) {
            return "small-file transaction overhead";
        }
        const measured = (Object.entries(this.phaseRates) as Array<[
            ResourcePhase,
            number | null,
        ]>).filter((entry): entry is [ResourcePhase, number] =>
            entry[1] !== null && Number.isFinite(entry[1]) && entry[1] > 0);
        if (measured.length > 0) {
            measured.sort((left, right) => left[1] - right[1]);
            return `${measured[0][0]} throughput`;
        }
        return "no pressure signal";
    }

    private updatePhaseRates(rates: ResourceMeasurement["phaseThroughput"]): void {
        if (!rates) return;
        for (const phase of ["read", "hash", "upload"] as const) {
            const next = rates[phase];
            if (next === undefined || !Number.isFinite(next) || next <= 0) continue;
            const previous = this.phaseRates[phase];
            this.phaseRates[phase] = previous === null
                ? next
                : previous * 0.75 + next * 0.25;
        }
    }

    private halvedProfileIndex(): number {
        const current = this.current().tuning;
        const targetHash = Math.max(1, Math.ceil(current.hashConcurrency / 2));
        const targetNetwork = Math.max(1, Math.ceil(current.networkConcurrency / 2));
        for (let candidate = this.index - 1; candidate >= 0; candidate--) {
            const tuning = this.profiles[candidate].tuning;
            if (
                tuning.hashConcurrency <= targetHash &&
                tuning.networkConcurrency <= targetNetwork
            ) {
                return candidate;
            }
        }
        return Math.max(0, this.index - 1);
    }

    private changeProfile(index: number, reason: string): void {
        const clamped = Math.max(0, Math.min(this.profiles.length - 1, index));
        if (clamped === this.index) {
            this.lastDecision = `minimum/maximum profile held: ${reason}`;
            return;
        }
        const previous = this.index;
        const previousWindowTuning = this.windowTuning;
        this.index = clamped;
        if (this.windowTuning) {
            this.windowTuning = clampConcurrency(this.profiles[clamped].tuning, this.windowCaps);
        }
        try {
            this.onProfileChange?.(this.current(), reason);
            this.lastDecision = reason;
        } catch (error) {
            this.index = previous;
            this.windowTuning = previousWindowTuning;
            this.lastDecision = `profile transition failed: ${reason}`;
            throw error;
        }
    }

    private clearRecoveryHintAfterHealthyWindow(): void {
        if (!this.hint) return;
        this.healthyHintWindows = Math.min(
            HEALTHY_WINDOWS_TO_CLEAR_HINT,
            this.healthyHintWindows + 1,
        );
        if (this.healthyHintWindows < HEALTHY_WINDOWS_TO_CLEAR_HINT) return;
        this.hint = null;
        this.healthyHintWindows = 0;
        this.onRecoveryHintChange?.(null);
    }
}

export type MobileLifecycleQuiesceReason = "hidden" | "pagehide" | "freeze" | "disposed";

/** Typed cancellation of one mobile lifecycle epoch. It is deliberately not
 * the engine-lifetime abort: authoritative work restores its dirty/journal
 * owner and a later visible epoch may retry it. */
export class MobileLifecycleQuiesceError extends Error {
    readonly name = "AbortError";
    constructor(readonly epoch: number, readonly lifecycleReason: MobileLifecycleQuiesceReason) {
        super(`mobile work quiesced: ${lifecycleReason}`);
    }
}

export interface ResourceVisibilityWork {
    readonly signal: AbortSignal;
    readonly epoch: number;
    release(): void;
}

/** Mobile-heavy-work lifecycle gate. New work waits while hidden. Work which
 * already owns a bounded read/hash/upload scope receives an epoch abort and
 * must release only after its actual async/native tail settles. Desktop never
 * receives lifecycle cancellation merely for being hidden. */
export class ResourceVisibilityGate {
    private visible: boolean;
    private disposed = false;
    private waiters = new Set<() => void>();
    private epoch = 1;
    private epochAbort = new AbortController();
    private activeWork = 0;
    private lastQuiesceReason: MobileLifecycleQuiesceReason | null = null;
    private composites = new WeakMap<AbortSignal, { epoch: number; signal: AbortSignal }>();
    private resumeListeners = new Set<() => void>();
    private quiescenceWaiters = new Set<() => void>();

    constructor(
        private readonly runtime: HashRuntime,
        visible = true,
    ) {
        this.visible = visible;
        if (runtime === "mobile" && !visible) {
            this.lastQuiesceReason = "hidden";
            this.epochAbort.abort(new MobileLifecycleQuiesceError(this.epoch, "hidden"));
        }
    }

    /** Returns true exactly once for a mobile hidden→visible epoch change. */
    setVisible(visible: boolean, reason: MobileLifecycleQuiesceReason = "hidden"): boolean {
        if (this.disposed) return false;
        if (this.runtime !== "mobile") {
            this.visible = visible;
            if (visible) this.release();
            return false;
        }
        if (!visible) {
            this.visible = false;
            if (!this.epochAbort.signal.aborted) {
                this.lastQuiesceReason = reason;
                this.epochAbort.abort(new MobileLifecycleQuiesceError(this.epoch, reason));
            }
            return false;
        }
        const resumed = !this.visible || this.epochAbort.signal.aborted;
        this.visible = visible;
        if (resumed) {
            this.epoch++;
            this.epochAbort = new AbortController();
            this.lastQuiesceReason = null;
            this.release();
            for (const listener of this.resumeListeners) {
                try { listener(); }
                catch (error) { console.warn("[obsetync] mobile lifecycle resume listener failed:", error); }
            }
        }
        return resumed;
    }

    isPaused(): boolean {
        return this.runtime === "mobile" && !this.visible && !this.disposed;
    }

    waitForHeavyWork(signal?: AbortSignal): Promise<void> {
        const aborted = () => signal?.reason ?? Object.assign(new Error("Visibility wait aborted"), { name: "AbortError" });
        if (signal?.aborted) return Promise.reject(aborted());
        if (!this.isPaused()) return Promise.resolve();
        return new Promise<void>((resolve, reject) => {
            const cleanup = () => {
                this.waiters.delete(resume);
                signal?.removeEventListener("abort", cancel);
            };
            const resume = () => { cleanup(); resolve(); };
            const cancel = () => { cleanup(); reject(aborted()); };
            this.waiters.add(resume);
            signal?.addEventListener("abort", cancel, { once: true });
            // This cancels only an unstarted visibility wait, not an IO owner.
            if (signal?.aborted) cancel();
        });
    }

    /** Link one bounded owner to the current mobile lifecycle epoch. */
    beginHeavyWork(parentSignal: AbortSignal): ResourceVisibilityWork {
        if (this.runtime !== "mobile") return { signal: parentSignal, epoch: this.epoch, release() {} };
        let composite = this.composites.get(parentSignal);
        if (!composite || composite.epoch !== this.epoch) {
            const lifecycle = this.epochAbort.signal, controller = new AbortController();
            let cleaned = false;
            const cleanup = () => {
                if (cleaned) return;
                cleaned = true;
                parentSignal.removeEventListener("abort", abortFromParent);
                lifecycle.removeEventListener("abort", abortFromLifecycle);
                this.composites.delete(parentSignal);
            };
            const abortFromParent = () => { controller.abort(parentSignal.reason); cleanup(); };
            const abortFromLifecycle = () => { controller.abort(lifecycle.reason); cleanup(); };
            if (parentSignal.aborted) abortFromParent();
            else if (lifecycle.aborted) abortFromLifecycle();
            else {
                parentSignal.addEventListener("abort", abortFromParent, { once: true });
                lifecycle.addEventListener("abort", abortFromLifecycle, { once: true });
            }
            composite = { epoch: this.epoch, signal: controller.signal };
            // Do not cache an already-aborted parent: a replacement engine in
            // the same visible epoch receives its own independent composite.
            if (!parentSignal.aborted) this.composites.set(parentSignal, composite);
        }
        const epoch = this.epoch, signal = composite.signal;
        this.activeWork++;
        let released = false;
        return {
            signal,
            epoch,
            release: () => {
                if (released) return;
                released = true;
                this.activeWork--;
                if (this.activeWork === 0) {
                    for (const resolve of this.quiescenceWaiters) resolve();
                    this.quiescenceWaiters.clear();
                }
            },
        };
    }

    waitForQuiescence(): Promise<void> {
        if (this.activeWork === 0) return Promise.resolve();
        return new Promise(resolve => this.quiescenceWaiters.add(resolve));
    }

    snapshot() {
        return { runtime: this.runtime, paused: this.isPaused(), epoch: this.epoch,
            activeWork: this.activeWork, lastQuiesceReason: this.lastQuiesceReason };
    }

    onResume(listener: () => void): () => void {
        if (this.disposed) return () => {};
        this.resumeListeners.add(listener);
        return () => { this.resumeListeners.delete(listener); };
    }

    dispose(): void {
        if (this.disposed) return;
        this.disposed = true;
        if (this.runtime === "mobile" && !this.epochAbort.signal.aborted) {
            this.lastQuiesceReason = "disposed";
            this.epochAbort.abort(new MobileLifecycleQuiesceError(this.epoch, "disposed"));
        }
        this.composites = new WeakMap();
        this.resumeListeners.clear();
        for (const resolve of this.quiescenceWaiters) resolve();
        this.quiescenceWaiters.clear();
        this.release();
    }

    private release(): void {
        for (const resolve of this.waiters) resolve();
        this.waiters.clear();
    }
}

export const RESOURCE_GOVERNOR_CONSTANTS = {
    hintTtlMs: HINT_TTL_MS,
    healthyWindowsToClearHint: HEALTHY_WINDOWS_TO_CLEAR_HINT,
    healthyLagMs: HEALTHY_LAG_MS,
    overloadLagMs: OVERLOAD_LAG_MS,
    windowMinMs: WINDOW_MIN_MS,
    windowMaxMs: WINDOW_MAX_MS,
    windowProbeCooldownMs: WINDOW_PROBE_COOLDOWN_MS,
    windowPressureProbeCooldownMs: WINDOW_PRESSURE_PROBE_COOLDOWN_MS,
    windowHealthyToProbe: WINDOW_HEALTHY_TO_PROBE,
} as const;
