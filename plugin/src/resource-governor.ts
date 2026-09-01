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

export type ResourceArchitecture = "arm64" | "x64" | "unknown";
export type ResourceOs = "ios" | "darwin" | "win32" | "linux" | "unknown";
export type ResourceProfileName = HashTuning["profile"];
export type ResourcePhase = "read" | "hash" | "upload";

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
    family: "a17-ios" | "m1-macos" | "snapdragon-windows" | "x86-desktop" | "generic";
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
            ? "a17-ios"
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
        const family = "m1-macos" as const;
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
        const family = "snapdragon-windows" as const;
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

    constructor(
        readonly environment: ResourceEnvironment,
        options: AdaptiveResourceGovernorOptions = {},
    ) {
        const selected = resourceProfilesFor(environment);
        this.profiles = selected.profiles;
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
        return { ...current, tuning: { ...current.tuning } };
    }

    setVisible(visible: boolean): void {
        this.visible = visible;
        if (!visible && this.environment.runtime === "mobile") {
            this.lastBottleneck = "application hidden";
        }
    }

    recordSimdAvailability(available: boolean): void {
        if (this.environment.simdAvailable === available) return;
        this.environment.simdAvailable = available;
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
        this.onRecoveryHintChange?.({ ...this.hint });
        const next = Math.max(0, this.index - 1);
        this.changeProfile(next, reason);
        this.lastBottleneck = reason;
    }

    observe(measurement: ResourceMeasurement): void {
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
        this.index = clamped;
        try {
            this.onProfileChange?.(this.current(), reason);
            this.lastDecision = reason;
        } catch (error) {
            this.index = previous;
            this.lastDecision = `profile transition failed: ${reason}`;
            throw error;
        }
    }

    private clearRecoveryHintAfterHealthyWindow(): void {
        if (!this.hint) return;
        this.healthyHintWindows++;
        if (this.healthyHintWindows < HEALTHY_WINDOWS_TO_CLEAR_HINT) return;
        this.hint = null;
        this.healthyHintWindows = 0;
        this.onRecoveryHintChange?.(null);
    }
}

/**
 * Mobile-heavy-work gate. Existing network/durability work is never aborted;
 * callers wait only before starting the next bounded batch.
 */
export class ResourceVisibilityGate {
    private visible: boolean;
    private disposed = false;
    private waiters = new Set<() => void>();

    constructor(
        private readonly runtime: HashRuntime,
        visible = true,
    ) {
        this.visible = visible;
    }

    setVisible(visible: boolean): void {
        this.visible = visible;
        if (visible) this.release();
    }

    isPaused(): boolean {
        return this.runtime === "mobile" && !this.visible && !this.disposed;
    }

    waitForHeavyWork(): Promise<void> {
        if (!this.isPaused()) return Promise.resolve();
        return new Promise<void>((resolve) => this.waiters.add(resolve));
    }

    dispose(): void {
        this.disposed = true;
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
} as const;
