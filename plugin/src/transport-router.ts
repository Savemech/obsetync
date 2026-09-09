/** Adaptive, replay-safe routing for the existing bulk HTTP and WS lanes.
 *
 * The router deliberately does not know how to encode an operation. Callers
 * must opt in with the content-addressed replay contract before a failed WS
 * attempt can cross over to HTTP. This keeps future non-idempotent mutations
 * out of the fallback path unless they acquire their own durable outcome ID.
 */

export type TransportCircuitState = "closed" | "open" | "half-open";

export interface TransportPhaseTimeouts {
    creditTimeoutMs: number;
    sendTimeoutMs: number;
    ackTimeoutMs: number;
}

export interface TransportFailureDisposition {
    retryable: boolean;
    retryAfterMs?: number;
}

export type TransportSemanticLane = "control" | "urgent" | "bulk";
export type TransportCarrier = "ws" | "http";
export type TransportSelectionReason =
    | "interactive-ws"
    | "bulk-ws-learning"
    | "bulk-ws-preferred"
    | "bulk-http-preferred"
    | "bulk-ws-probe"
    | "bulk-http-probe"
    | "ws-recovery-probe"
    | "ws-unavailable"
    | "ws-payload-too-large"
    | "ws-circuit-open"
    | "ws-recovery-probe-ineligible"
    | "ws-recovery-probe-busy"
    | "ws-retryable-fallback";

/** Attempt-local measurements. The router owns the wall/service interval;
 * callers add only queue wait which they can observe inside a transport and
 * actual useful bytes when response size differs from request size. Invalid
 * or visibility-crossing attempts can be excluded without changing outcome. */
export interface TransportAttemptMeasurements {
    addQueueWaitMs(durationMs: number): void;
    setUsefulBytes(bytes: number): void;
    invalidate(): void;
}

export interface TransportRouteRequest<T> {
    payloadBytes: number;
    /** Expected useful transfer size for carrier scoring. Actual confirmed
     * bytes are supplied through the attempt measurement sink. */
    expectedUsefulBytes?: number;
    replay: "content-addressed";
    /** Explicit semantic lane wins over the bounded payload fallback. Bulk is
     * never allowed to become a half-open health probe. */
    lane?: TransportSemanticLane;
    ws?: (timeouts: TransportPhaseTimeouts, measurements: TransportAttemptMeasurements) => Promise<T>;
    /** Diagnostics for an intentionally omitted optional WS carrier. */
    wsUnavailableReason?: "ws-unavailable" | "ws-payload-too-large";
    http: (measurements: TransportAttemptMeasurements) => Promise<T>;
    /** Checked before dispatch and after confirmed completion. False excludes
     * a performance sample, but never changes replay/fallback semantics. */
    sampleValid?: () => boolean;
    classifyWsFailure: (error: unknown) => TransportFailureDisposition;
    onFallback?: (error: unknown) => void;
}

export interface TransportRouterSnapshot {
    circuit: TransportCircuitState;
    consecutiveWsFailures: number;
    openForMs: number;
    halfOpenInFlight: boolean;
    recoveryWsSuccesses: number;
    nextProbeInMs: number;
    wsSuccesses: number;
    wsFailures: number;
    httpSelections: number;
    wsLatencyMs: number | null;
    httpLatencyMs: number | null;
    bulkPreferredCarrier: TransportCarrier;
    bulkPreferenceEstablished: boolean;
    bulkPreferenceHoldMs: number;
    wsBulkSamples: number;
    httpBulkSamples: number;
    wsBulkThroughputBytesPerSecond: number | null;
    httpBulkThroughputBytesPerSecond: number | null;
    wsBulkQueueWaitMs: number | null;
    httpBulkQueueWaitMs: number | null;
    wsControlSamples: number;
    httpControlSamples: number;
    invalidPerformanceSamples: number;
    lastSelectedCarrier: TransportCarrier | null;
    lastSelectionReason: TransportSelectionReason | null;
    lastSelectionLane: TransportSemanticLane | null;
    lastSelectionPayloadBytes: number;
    lastSelectionExpectedUsefulBytes: number;
    lastSelectionAtMs: number | null;
}

export interface TransportRouterOptions {
    now?: () => number;
    /** Injectable [0, 1) sample for deterministic tests. */
    jitter?: () => number;
    failureThreshold?: number;
    openBaseMs?: number;
    openMaxMs?: number;
    creditTimeoutMs?: number;
    sendTimeoutMs?: number;
    ackBaseTimeoutMs?: number;
    ackMinimumBytesPerSecond?: number;
    ackMaxTimeoutMs?: number;
    recoverySuccessThreshold?: number;
    recoveryProbeIntervalMs?: number;
    recoveryProbeMaxPayloadBytes?: number;
    probeCreditTimeoutMs?: number;
    probeSendTimeoutMs?: number;
    probeAckTimeoutMs?: number;
    performanceMinSamples?: number;
    performanceHysteresisRatio?: number;
    performanceHoldMs?: number;
    performanceProbeEvery?: number;
    performanceMaxSampleMs?: number;
    performanceMaxQueueWaitMs?: number;
    performanceMaxUsefulBytes?: number;
    performanceMaxThroughputBytesPerSecond?: number;
}

const DEFAULT_FAILURE_THRESHOLD = 2;
const DEFAULT_OPEN_BASE_MS = 1_000;
const DEFAULT_OPEN_MAX_MS = 60_000;
const DEFAULT_CREDIT_TIMEOUT_MS = 5_000;
const DEFAULT_SEND_TIMEOUT_MS = 10_000;
const DEFAULT_ACK_BASE_TIMEOUT_MS = 15_000;
const DEFAULT_ACK_MINIMUM_BYTES_PER_SECOND = 128 * 1024;
const DEFAULT_ACK_MAX_TIMEOUT_MS = 60_000;
const DEFAULT_RECOVERY_SUCCESS_THRESHOLD = 2;
const DEFAULT_RECOVERY_PROBE_INTERVAL_MS = 1_000;
const DEFAULT_RECOVERY_PROBE_MAX_PAYLOAD_BYTES = 64 * 1024;
const DEFAULT_PROBE_CREDIT_TIMEOUT_MS = 2_000;
const DEFAULT_PROBE_SEND_TIMEOUT_MS = 5_000;
const DEFAULT_PROBE_ACK_TIMEOUT_MS = 5_000;
const EWMA_OLD_WEIGHT = 0.75;
const OPEN_JITTER_RATIO = 0.2;
const DEFAULT_PERFORMANCE_MIN_SAMPLES = 3;
const DEFAULT_PERFORMANCE_HYSTERESIS_RATIO = 1.25;
const DEFAULT_PERFORMANCE_HOLD_MS = 30_000;
const DEFAULT_PERFORMANCE_PROBE_EVERY = 8;
const DEFAULT_PERFORMANCE_MAX_SAMPLE_MS = 120_000;
const DEFAULT_PERFORMANCE_MAX_QUEUE_WAIT_MS = 60_000;
const DEFAULT_PERFORMANCE_MAX_USEFUL_BYTES = 64 * 1024 * 1024;
const DEFAULT_PERFORMANCE_MAX_THROUGHPUT_BYTES_PER_SECOND = 2 * 1024 * 1024 * 1024;
const MAX_PERFORMANCE_SAMPLES = 64;

function positiveInteger(value: number, name: string): number {
    if (!Number.isSafeInteger(value) || value < 1) {
        throw new RangeError(`${name} must be a positive safe integer`);
    }
    return value;
}

function boundedRatio(value: number, name: string): number {
    if (!Number.isFinite(value) || value <= 1 || value > 4) {
        throw new RangeError(`${name} must be finite and in (1, 4]`);
    }
    return value;
}

function updateEwma(previous: number | null, sample: number): number {
    return previous === null
        ? sample
        : previous * EWMA_OLD_WEIGHT + sample * (1 - EWMA_OLD_WEIGHT);
}

function incrementBounded(value: number): number {
    return Math.min(Number.MAX_SAFE_INTEGER, value + 1);
}

function monotonicNow(): number {
    try {
        const value = globalThis.performance?.now?.();
        return typeof value === "number" && Number.isFinite(value) && value >= 0
            ? value
            : Date.now();
    } catch {
        return Date.now();
    }
}

interface CarrierPerformance {
    samples: number;
    throughputSamples: number;
    latencyMs: number | null;
    queueWaitMs: number | null;
    throughputBytesPerSecond: number | null;
}

interface LanePerformance {
    ws: CarrierPerformance;
    http: CarrierPerformance;
}

interface BulkCarrierSelection {
    carrier: TransportCarrier;
    reason: TransportSelectionReason;
}

function emptyCarrierPerformance(): CarrierPerformance {
    return { samples: 0, throughputSamples: 0, latencyMs: null,
        queueWaitMs: null, throughputBytesPerSecond: null };
}

function emptyLanePerformance(): LanePerformance {
    return { ws: emptyCarrierPerformance(), http: emptyCarrierPerformance() };
}

class AttemptMeasurements implements TransportAttemptMeasurements {
    queueWaitMs = 0;
    usefulBytes: number;
    valid = true;

    constructor(payloadBytes: number, private readonly maxQueueWaitMs: number,
        private readonly maxUsefulBytes: number) {
        this.usefulBytes = payloadBytes;
    }

    addQueueWaitMs(durationMs: number): void {
        if (!Number.isFinite(durationMs) || durationMs < 0 || durationMs > this.maxQueueWaitMs ||
            this.queueWaitMs > this.maxQueueWaitMs - durationMs) {
            this.valid = false;
            return;
        }
        this.queueWaitMs += durationMs;
    }

    setUsefulBytes(bytes: number): void {
        if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > this.maxUsefulBytes) {
            this.valid = false;
            return;
        }
        this.usefulBytes = bytes;
    }

    invalidate(): void { this.valid = false; }
}

/** One owner for WS selection, circuit state and the single HTTP fallback. */
export class TransportRouter {
    private readonly now: () => number;
    private readonly jitter: () => number;
    private readonly failureThreshold: number;
    private readonly openBaseMs: number;
    private readonly openMaxMs: number;
    private readonly creditTimeoutMs: number;
    private readonly sendTimeoutMs: number;
    private readonly ackBaseTimeoutMs: number;
    private readonly ackMinimumBytesPerSecond: number;
    private readonly ackMaxTimeoutMs: number;
    private readonly recoverySuccessThreshold: number;
    private readonly recoveryProbeIntervalMs: number;
    private readonly recoveryProbeMaxPayloadBytes: number;
    private readonly probeCreditTimeoutMs: number;
    private readonly probeSendTimeoutMs: number;
    private readonly probeAckTimeoutMs: number;
    private readonly performanceMinSamples: number;
    private readonly performanceHysteresisRatio: number;
    private readonly performanceHoldMs: number;
    private readonly performanceProbeEvery: number;
    private readonly performanceMaxSampleMs: number;
    private readonly performanceMaxQueueWaitMs: number;
    private readonly performanceMaxUsefulBytes: number;
    private readonly performanceMaxThroughputBytesPerSecond: number;
    private consecutiveWsFailures = 0;
    private openUntil = 0;
    private halfOpenInFlight = false;
    private wsSuccesses = 0;
    private wsFailures = 0;
    private httpSelections = 0;
    private wsLatencyMs: number | null = null;
    private httpLatencyMs: number | null = null;
    private recoveryWsSuccesses = 0;
    private performanceClock = 0;
    private performanceClockGeneration = 0;
    private controlPerformance = emptyLanePerformance();
    private bulkPerformance = emptyLanePerformance();
    private bulkPreferredCarrier: TransportCarrier = "ws";
    private bulkPreferenceEstablished = false;
    private bulkPreferenceHoldUntil = 0;
    private bulkRequestsSinceProbe = 0;
    private invalidPerformanceSamples = 0;
    private lastSelectedCarrier: TransportCarrier | null = null;
    private lastSelectionReason: TransportSelectionReason | null = null;
    private lastSelectionLane: TransportSemanticLane | null = null;
    private lastSelectionPayloadBytes = 0;
    private lastSelectionExpectedUsefulBytes = 0;
    private lastSelectionAtMs: number | null = null;

    constructor(options: TransportRouterOptions = {}) {
        this.now = options.now ?? monotonicNow;
        this.jitter = options.jitter ?? Math.random;
        this.failureThreshold = positiveInteger(
            options.failureThreshold ?? DEFAULT_FAILURE_THRESHOLD,
            "transport failure threshold",
        );
        this.openBaseMs = positiveInteger(options.openBaseMs ?? DEFAULT_OPEN_BASE_MS,
            "transport open base");
        this.openMaxMs = positiveInteger(options.openMaxMs ?? DEFAULT_OPEN_MAX_MS,
            "transport open maximum");
        if (this.openMaxMs < this.openBaseMs) {
            throw new RangeError("transport open maximum is below its base");
        }
        this.creditTimeoutMs = positiveInteger(
            options.creditTimeoutMs ?? DEFAULT_CREDIT_TIMEOUT_MS,
            "WS credit timeout",
        );
        this.sendTimeoutMs = positiveInteger(
            options.sendTimeoutMs ?? DEFAULT_SEND_TIMEOUT_MS,
            "WS send timeout",
        );
        this.ackBaseTimeoutMs = positiveInteger(
            options.ackBaseTimeoutMs ?? DEFAULT_ACK_BASE_TIMEOUT_MS,
            "WS ACK base timeout",
        );
        this.ackMinimumBytesPerSecond = positiveInteger(
            options.ackMinimumBytesPerSecond ?? DEFAULT_ACK_MINIMUM_BYTES_PER_SECOND,
            "WS ACK minimum throughput",
        );
        this.ackMaxTimeoutMs = positiveInteger(
            options.ackMaxTimeoutMs ?? DEFAULT_ACK_MAX_TIMEOUT_MS,
            "WS ACK maximum timeout",
        );
        if (this.ackMaxTimeoutMs < this.ackBaseTimeoutMs) {
            throw new RangeError("WS ACK maximum timeout is below its base");
        }
        this.recoverySuccessThreshold = positiveInteger(
            options.recoverySuccessThreshold ?? DEFAULT_RECOVERY_SUCCESS_THRESHOLD,
            "transport recovery success threshold",
        );
        this.recoveryProbeIntervalMs = positiveInteger(
            options.recoveryProbeIntervalMs ?? DEFAULT_RECOVERY_PROBE_INTERVAL_MS,
            "transport recovery probe interval",
        );
        this.recoveryProbeMaxPayloadBytes = positiveInteger(
            options.recoveryProbeMaxPayloadBytes ?? DEFAULT_RECOVERY_PROBE_MAX_PAYLOAD_BYTES,
            "transport recovery probe payload maximum",
        );
        this.probeCreditTimeoutMs = positiveInteger(
            options.probeCreditTimeoutMs ?? DEFAULT_PROBE_CREDIT_TIMEOUT_MS,
            "WS probe credit timeout",
        );
        this.probeSendTimeoutMs = positiveInteger(
            options.probeSendTimeoutMs ?? DEFAULT_PROBE_SEND_TIMEOUT_MS,
            "WS probe send timeout",
        );
        this.probeAckTimeoutMs = positiveInteger(
            options.probeAckTimeoutMs ?? DEFAULT_PROBE_ACK_TIMEOUT_MS,
            "WS probe ACK timeout",
        );
        this.performanceMinSamples = positiveInteger(
            options.performanceMinSamples ?? DEFAULT_PERFORMANCE_MIN_SAMPLES,
            "performance minimum samples",
        );
        if (this.performanceMinSamples > MAX_PERFORMANCE_SAMPLES) {
            throw new RangeError("performance minimum samples exceeds retained sample cap");
        }
        this.performanceHysteresisRatio = boundedRatio(
            options.performanceHysteresisRatio ?? DEFAULT_PERFORMANCE_HYSTERESIS_RATIO,
            "performance hysteresis ratio",
        );
        this.performanceHoldMs = positiveInteger(
            options.performanceHoldMs ?? DEFAULT_PERFORMANCE_HOLD_MS,
            "performance preference hold",
        );
        this.performanceProbeEvery = positiveInteger(
            options.performanceProbeEvery ?? DEFAULT_PERFORMANCE_PROBE_EVERY,
            "performance probe cadence",
        );
        this.performanceMaxSampleMs = positiveInteger(
            options.performanceMaxSampleMs ?? DEFAULT_PERFORMANCE_MAX_SAMPLE_MS,
            "performance sample duration cap",
        );
        this.performanceMaxQueueWaitMs = positiveInteger(
            options.performanceMaxQueueWaitMs ?? DEFAULT_PERFORMANCE_MAX_QUEUE_WAIT_MS,
            "performance queue wait cap",
        );
        if (this.performanceMaxQueueWaitMs > this.performanceMaxSampleMs) {
            throw new RangeError("performance queue wait cap exceeds sample duration cap");
        }
        this.performanceMaxUsefulBytes = positiveInteger(
            options.performanceMaxUsefulBytes ?? DEFAULT_PERFORMANCE_MAX_USEFUL_BYTES,
            "performance useful byte cap",
        );
        this.performanceMaxThroughputBytesPerSecond = positiveInteger(
            options.performanceMaxThroughputBytesPerSecond ??
                DEFAULT_PERFORMANCE_MAX_THROUGHPUT_BYTES_PER_SECOND,
            "performance throughput cap",
        );
    }

    timeoutsFor(payloadBytes: number): TransportPhaseTimeouts {
        if (!Number.isSafeInteger(payloadBytes) || payloadBytes < 0) {
            throw new RangeError("transport payload byte count is invalid");
        }
        const transferMs = Math.ceil(payloadBytes * 1_000 / this.ackMinimumBytesPerSecond);
        return {
            creditTimeoutMs: this.creditTimeoutMs,
            sendTimeoutMs: this.sendTimeoutMs,
            ackTimeoutMs: Math.min(this.ackMaxTimeoutMs, this.ackBaseTimeoutMs + transferMs),
        };
    }

    async execute<T>(request: TransportRouteRequest<T>): Promise<T> {
        if (request.replay !== "content-addressed") {
            throw new Error("transport fallback requires an explicit replay-safe operation");
        }
        const normalTimeouts = this.timeoutsFor(request.payloadBytes);
        const expectedUsefulBytes = request.expectedUsefulBytes ?? request.payloadBytes;
        if (!Number.isSafeInteger(expectedUsefulBytes) || expectedUsefulBytes < 0 ||
            expectedUsefulBytes > this.performanceMaxUsefulBytes) {
            throw new RangeError("transport expected useful byte count is invalid");
        }
        const policyNow = this.readPerformanceClock();
        const selection: BulkCarrierSelection = request.lane === "bulk"
            ? this.selectBulkCarrier(expectedUsefulBytes, policyNow)
            : { carrier: "ws", reason: "interactive-ws" };
        const wsMode = request.ws && selection.carrier === "ws"
            ? this.beginWsAttempt(this.probeEligible(request))
            : null;
        let retryableWsFailure = false;
        if (request.ws && wsMode) {
            this.recordSelection(
                "ws",
                wsMode === "half-open" ? "ws-recovery-probe" : selection.reason,
                request.lane,
                request.payloadBytes,
                expectedUsefulBytes,
                policyNow,
            );
            const timeouts = wsMode === "half-open"
                ? this.probeTimeoutsFor(normalTimeouts)
                : normalTimeouts;
            const started = this.readPerformanceClock();
            const clockGeneration = this.performanceClockGeneration;
            const sampleInitiallyValid = this.performanceSampleValid(request);
            const measurements = new AttemptMeasurements(
                request.payloadBytes,
                this.performanceMaxQueueWaitMs,
                this.performanceMaxUsefulBytes,
            );
            try {
                const result = await request.ws(timeouts, measurements);
                this.wsSuccesses = incrementBounded(this.wsSuccesses);
                this.recordPerformanceSample(
                    request.lane,
                    "ws",
                    started,
                    clockGeneration,
                    sampleInitiallyValid && this.performanceSampleValid(request),
                    measurements,
                );
                if (wsMode === "half-open") this.recordProbeSuccess();
                else this.closeCircuit();
                return result;
            } catch (error) {
                const disposition = request.classifyWsFailure(error);
                if (!disposition.retryable) throw error;
                this.wsFailures = incrementBounded(this.wsFailures);
                this.recordWsFailure(disposition.retryAfterMs);
                retryableWsFailure = true;
                request.onFallback?.(error);
            } finally {
                if (wsMode === "half-open") this.halfOpenInFlight = false;
            }
        }

        const started = this.readPerformanceClock();
        const httpReason = retryableWsFailure
            ? "ws-retryable-fallback"
            : request.ws === undefined
            ? request.wsUnavailableReason ?? "ws-unavailable"
            : selection.carrier === "http"
                ? selection.reason
                : this.httpReasonAfterUnavailableWs(request, started);
        this.recordSelection(
            "http",
            httpReason,
            request.lane,
            request.payloadBytes,
            expectedUsefulBytes,
            started,
        );
        this.httpSelections = incrementBounded(this.httpSelections);
        const clockGeneration = this.performanceClockGeneration;
        const sampleInitiallyValid = this.performanceSampleValid(request);
        const measurements = new AttemptMeasurements(
            request.payloadBytes,
            this.performanceMaxQueueWaitMs,
            this.performanceMaxUsefulBytes,
        );
        try {
            const result = await request.http(measurements);
            this.recordPerformanceSample(
                request.lane,
                "http",
                started,
                clockGeneration,
                sampleInitiallyValid && this.performanceSampleValid(request),
                measurements,
            );
            return result;
        } catch (error) {
            // Failed HTTP outcomes are not evidence for carrier speed. Existing
            // single-owner/fallback semantics intentionally propagate them.
            throw error;
        }
    }

    snapshot(): TransportRouterSnapshot {
        const observedNow = this.now();
        const now = Number.isFinite(observedNow) && observedNow >= 0
            ? observedNow
            : this.performanceClock;
        const circuit = this.circuitState(now);
        return {
            circuit,
            consecutiveWsFailures: this.consecutiveWsFailures,
            openForMs: circuit === "open" ? Math.max(0, this.openUntil - now) : 0,
            halfOpenInFlight: this.halfOpenInFlight,
            recoveryWsSuccesses: this.recoveryWsSuccesses,
            nextProbeInMs: circuit === "open" ? Math.max(0, this.openUntil - now) : 0,
            wsSuccesses: this.wsSuccesses,
            wsFailures: this.wsFailures,
            httpSelections: this.httpSelections,
            wsLatencyMs: this.wsLatencyMs,
            httpLatencyMs: this.httpLatencyMs,
            bulkPreferredCarrier: this.bulkPreferredCarrier,
            bulkPreferenceEstablished: this.bulkPreferenceEstablished,
            bulkPreferenceHoldMs: Math.max(0, this.bulkPreferenceHoldUntil - now),
            wsBulkSamples: this.bulkPerformance.ws.throughputSamples,
            httpBulkSamples: this.bulkPerformance.http.throughputSamples,
            wsBulkThroughputBytesPerSecond: this.bulkPerformance.ws.throughputBytesPerSecond,
            httpBulkThroughputBytesPerSecond: this.bulkPerformance.http.throughputBytesPerSecond,
            wsBulkQueueWaitMs: this.bulkPerformance.ws.queueWaitMs,
            httpBulkQueueWaitMs: this.bulkPerformance.http.queueWaitMs,
            wsControlSamples: this.controlPerformance.ws.samples,
            httpControlSamples: this.controlPerformance.http.samples,
            invalidPerformanceSamples: this.invalidPerformanceSamples,
            lastSelectedCarrier: this.lastSelectedCarrier,
            lastSelectionReason: this.lastSelectionReason,
            lastSelectionLane: this.lastSelectionLane,
            lastSelectionPayloadBytes: this.lastSelectionPayloadBytes,
            lastSelectionExpectedUsefulBytes: this.lastSelectionExpectedUsefulBytes,
            lastSelectionAtMs: this.lastSelectionAtMs,
        };
    }

    /** Forget only performance evidence. Circuit failures/backoff and
     * recovery ownership survive capability/session resets. */
    resetMeasurements(): void {
        this.performanceClockGeneration = incrementBounded(this.performanceClockGeneration);
        this.controlPerformance = emptyLanePerformance();
        this.bulkPerformance = emptyLanePerformance();
        this.bulkPreferredCarrier = "ws";
        this.bulkPreferenceEstablished = false;
        this.bulkPreferenceHoldUntil = 0;
        this.bulkRequestsSinceProbe = 0;
        this.wsLatencyMs = null;
        this.httpLatencyMs = null;
        this.lastSelectedCarrier = null;
        this.lastSelectionReason = null;
        this.lastSelectionLane = null;
        this.lastSelectionPayloadBytes = 0;
        this.lastSelectionExpectedUsefulBytes = 0;
        this.lastSelectionAtMs = null;
    }

    private readPerformanceClock(): number {
        const value = this.now();
        if (!Number.isFinite(value) || value < 0) {
            this.invalidatePerformanceSample();
            return this.performanceClock;
        }
        if (value < this.performanceClock) {
            // A monotonic source should never move backwards. Discard the
            // complete comparison corpus, while preserving circuit backoff.
            this.resetMeasurements();
        }
        this.performanceClock = value;
        return value;
    }

    private performanceSampleValid<T>(request: TransportRouteRequest<T>): boolean {
        try { return request.sampleValid?.() ?? true; }
        catch { return false; }
    }

    private invalidatePerformanceSample(): void {
        this.invalidPerformanceSamples = incrementBounded(this.invalidPerformanceSamples);
    }

    private recordPerformanceSample(
        lane: TransportSemanticLane | undefined,
        carrier: TransportCarrier,
        started: number,
        clockGeneration: number,
        externallyValid: boolean,
        measurements: AttemptMeasurements,
    ): void {
        const finished = this.readPerformanceClock();
        const durationMs = finished - started;
        if (!externallyValid || !measurements.valid ||
            clockGeneration !== this.performanceClockGeneration ||
            !Number.isFinite(durationMs) || durationMs <= 0 ||
            durationMs > this.performanceMaxSampleMs ||
            measurements.queueWaitMs > durationMs) {
            this.invalidatePerformanceSample();
            return;
        }
        const serviceMs = durationMs - measurements.queueWaitMs;
        let throughput: number | null = null;
        if (measurements.usefulBytes > 0) {
            if (serviceMs <= 0) {
                this.invalidatePerformanceSample();
                return;
            }
            throughput = measurements.usefulBytes * 1_000 / serviceMs;
            if (!Number.isFinite(throughput) || throughput <= 0 ||
                throughput > this.performanceMaxThroughputBytesPerSecond) {
                this.invalidatePerformanceSample();
                return;
            }
        }
        const group = lane === "bulk" ? this.bulkPerformance : this.controlPerformance;
        const target = group[carrier];
        target.samples = Math.min(MAX_PERFORMANCE_SAMPLES, target.samples + 1);
        target.latencyMs = updateEwma(target.latencyMs, durationMs);
        target.queueWaitMs = updateEwma(target.queueWaitMs, measurements.queueWaitMs);
        if (throughput !== null) {
            target.throughputSamples = Math.min(
                MAX_PERFORMANCE_SAMPLES,
                target.throughputSamples + 1,
            );
            target.throughputBytesPerSecond = updateEwma(
                target.throughputBytesPerSecond,
                throughput,
            );
        }
        if (carrier === "ws") this.wsLatencyMs = updateEwma(this.wsLatencyMs, durationMs);
        else this.httpLatencyMs = updateEwma(this.httpLatencyMs, durationMs);
    }

    private carrierScore(carrier: TransportCarrier, expectedUsefulBytes: number): number | null {
        const sample = this.bulkPerformance[carrier];
        const throughput = sample.throughputBytesPerSecond;
        const latency = sample.latencyMs;
        const queue = sample.queueWaitMs;
        if (sample.throughputSamples < this.performanceMinSamples ||
            throughput === null || latency === null || queue === null) return null;
        const serviceLatency = Math.max(0, latency - queue);
        const transfer = expectedUsefulBytes === 0
            ? 0
            : expectedUsefulBytes * 1_000 / throughput;
        const score = queue + Math.max(serviceLatency, transfer);
        return Number.isFinite(score) && score >= 0 ? score : null;
    }

    private selectBulkCarrier(expectedUsefulBytes: number, now: number): BulkCarrierSelection {
        this.bulkRequestsSinceProbe = Math.min(
            this.performanceProbeEvery,
            this.bulkRequestsSinceProbe + 1,
        );
        const wsScore = this.carrierScore("ws", expectedUsefulBytes);
        const httpScore = this.carrierScore("http", expectedUsefulBytes);
        if (wsScore === null) return { carrier: "ws", reason: "bulk-ws-learning" };
        if (httpScore === null) {
            if (this.bulkRequestsSinceProbe >= this.performanceProbeEvery) {
                this.bulkRequestsSinceProbe = 0;
                return { carrier: "http", reason: "bulk-http-probe" };
            }
            return { carrier: "ws", reason: "bulk-ws-learning" };
        }

        if (!this.bulkPreferenceEstablished) {
            this.bulkPreferenceEstablished = true;
        }
        if (now >= this.bulkPreferenceHoldUntil) {
            const preferredScore = this.bulkPreferredCarrier === "ws" ? wsScore : httpScore;
            const alternative: TransportCarrier = this.bulkPreferredCarrier === "ws" ? "http" : "ws";
            const alternativeScore = alternative === "ws" ? wsScore : httpScore;
            if (alternativeScore * this.performanceHysteresisRatio < preferredScore) {
                this.bulkPreferredCarrier = alternative;
                this.bulkPreferenceHoldUntil = now + this.performanceHoldMs;
                this.bulkRequestsSinceProbe = 0;
            }
        }

        if (this.bulkRequestsSinceProbe >= this.performanceProbeEvery) {
            this.bulkRequestsSinceProbe = 0;
            return this.bulkPreferredCarrier === "ws"
                ? { carrier: "http", reason: "bulk-http-probe" }
                : { carrier: "ws", reason: "bulk-ws-probe" };
        }
        return this.bulkPreferredCarrier === "ws"
            ? { carrier: "ws", reason: "bulk-ws-preferred" }
            : { carrier: "http", reason: "bulk-http-preferred" };
    }

    private httpReasonAfterUnavailableWs<T>(
        request: TransportRouteRequest<T>,
        now: number,
    ): TransportSelectionReason {
        if (this.openUntil > now) return "ws-circuit-open";
        if (this.consecutiveWsFailures >= this.failureThreshold) {
            if (!this.probeEligible(request)) return "ws-recovery-probe-ineligible";
            if (this.halfOpenInFlight) return "ws-recovery-probe-busy";
        }
        return "ws-retryable-fallback";
    }

    private recordSelection(
        carrier: TransportCarrier,
        reason: TransportSelectionReason,
        lane: TransportSemanticLane | undefined,
        payloadBytes: number,
        expectedUsefulBytes: number,
        now: number,
    ): void {
        this.lastSelectedCarrier = carrier;
        this.lastSelectionReason = reason;
        this.lastSelectionLane = lane ?? "control";
        this.lastSelectionPayloadBytes = payloadBytes;
        this.lastSelectionExpectedUsefulBytes = expectedUsefulBytes;
        this.lastSelectionAtMs = Number.isFinite(now) && now >= 0 ? now : null;
    }

    private beginWsAttempt(probeEligible: boolean): "closed" | "half-open" | null {
        const now = this.now();
        if (this.openUntil > now) return null;
        if (this.consecutiveWsFailures < this.failureThreshold) return "closed";
        if (!probeEligible) return null;
        if (this.halfOpenInFlight) return null;
        this.halfOpenInFlight = true;
        return "half-open";
    }

    private recordWsFailure(retryAfterMs?: number): void {
        this.recoveryWsSuccesses = 0;
        this.consecutiveWsFailures = Math.min(
            this.failureThreshold + 16,
            this.consecutiveWsFailures + 1,
        );
        if (this.consecutiveWsFailures < this.failureThreshold) return;
        const exponent = Math.min(16, this.consecutiveWsFailures - this.failureThreshold);
        const backoff = Math.min(this.openMaxMs, this.openBaseMs * (2 ** exponent));
        const sample = this.jitter();
        const normalized = Number.isFinite(sample) ? Math.max(0, Math.min(1, sample)) : 0;
        const jittered = Math.min(
            this.openMaxMs,
            backoff + Math.floor(backoff * OPEN_JITTER_RATIO * normalized),
        );
        const hinted = Number.isSafeInteger(retryAfterMs) && (retryAfterMs ?? 0) > 0
            ? Math.min(this.openMaxMs, retryAfterMs!)
            : 0;
        this.openUntil = this.now() + Math.max(jittered, hinted);
    }

    private probeEligible<T>(request: TransportRouteRequest<T>): boolean {
        if (request.lane) return request.lane !== "bulk";
        return request.payloadBytes <= this.recoveryProbeMaxPayloadBytes;
    }

    private probeTimeoutsFor(normal: TransportPhaseTimeouts): TransportPhaseTimeouts {
        return {
            creditTimeoutMs: Math.min(normal.creditTimeoutMs, this.probeCreditTimeoutMs),
            sendTimeoutMs: Math.min(normal.sendTimeoutMs, this.probeSendTimeoutMs),
            ackTimeoutMs: Math.min(normal.ackTimeoutMs, this.probeAckTimeoutMs),
        };
    }

    private recordProbeSuccess(): void {
        this.recoveryWsSuccesses = Math.min(
            this.recoverySuccessThreshold,
            this.recoveryWsSuccesses + 1,
        );
        if (this.recoveryWsSuccesses >= this.recoverySuccessThreshold) {
            this.closeCircuit();
            return;
        }
        this.openUntil = this.now() + this.recoveryProbeIntervalMs;
    }

    private closeCircuit(): void {
        this.consecutiveWsFailures = 0;
        this.recoveryWsSuccesses = 0;
        this.openUntil = 0;
    }

    private circuitState(now: number): TransportCircuitState {
        if (this.openUntil > now) return "open";
        if (this.consecutiveWsFailures >= this.failureThreshold) return "half-open";
        return "closed";
    }
}
