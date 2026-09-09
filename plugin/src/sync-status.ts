import type {
    PerfActiveOperation,
    PerfOperationKind,
    PerfOperationWindow,
    PerfPhase,
} from "./perf-trace";

const BLOCKER_AFTER_MS = 1_500;
const BETWEEN_PHASES_AFTER_MS = 5_000;
const MAX_RATE_OPERATIONS = 16;
const RATE_EWMA_NEW_WEIGHT = 0.4;
const MAX_CONSISTENT_RATE_RATIO = 4;
const ETA_MIN_SAMPLES = 2;
const MAX_ETA_MS = 7 * 24 * 60 * 60 * 1_000;
const MAX_STATUS_TEXT = 180;

interface PhasePresentation {
    label: string;
    priority: number;
}

/** Higher priority phases explain idle CPU/disk before ordinary work phases. */
const PHASE_PRESENTATION: Record<PerfPhase, PhasePresentation> = {
    scheduler_wait: { label: "waiting for scheduler", priority: 90 },
    ws_connect: { label: "connecting WebSocket", priority: 95 },
    ws_credit_wait: { label: "waiting for WS credit", priority: 110 },
    ws_send_queue: { label: "waiting for WS send", priority: 105 },
    ws_buffer_wait: { label: "waiting for socket buffer", priority: 108 },
    ws_ack_wait: { label: "awaiting server ACK", priority: 115 },
    scan_batch: { label: "scanning batch", priority: 25 },
    prepare_batch: { label: "preparing batch", priority: 30 },
    enumerate: { label: "listing vault files", priority: 35 },
    stat: { label: "reading file metadata", priority: 40 },
    read: { label: "reading file", priority: 45 },
    hash: { label: "hashing", priority: 50 },
    fastcdc: { label: "chunking file", priority: 50 },
    check: { label: "checking remote objects", priority: 75 },
    encrypt: { label: "encrypting batch", priority: 55 },
    decrypt: { label: "decrypting batch", priority: 55 },
    network: { label: "waiting for network", priority: 85 },
    upload: { label: "uploading batch", priority: 80 },
    download: { label: "downloading batch", priority: 80 },
    apply: { label: "applying remote files", priority: 60 },
    tree_update: { label: "updating tree", priority: 45 },
    tree_index_upload: { label: "uploading tree index", priority: 80 },
    root_commit: { label: "committing root", priority: 100 },
    checkpoint: { label: "saving resume checkpoint", priority: 65 },
};

const SAFETY_STATUS = /(?:sync off|sync ✗|sync ⚠|sync ⏸|re-enroll|review)/i;
const ACTION_STATUS = /(?:re-enroll|review)/i;
const PATH_LIKE_TEXT = /(?:[A-Za-z_.-][\\/]|[\\/][A-Za-z_.-]|\S+\.md\b)/i;
const RATE_PHASES = new Set<SelectedOperation["phaseKey"]>([
    "scan_batch", "prepare_batch", "enumerate", "stat", "read", "hash", "fastcdc",
    "check", "encrypt", "decrypt", "upload", "download", "apply", "tree_index_upload",
]);

export interface SyncStatusTruth {
    busy: boolean;
    pendingChanges: number;
    deferredChanges: number;
    rootPending: boolean;
    rootRecovering: boolean;
    rootRecoveryRequired: boolean;
    error: boolean;
}

interface SelectedOperation {
    operation: PerfActiveOperation;
    phase: PhasePresentation | null;
    phaseKey: PerfPhase | "visibility" | "between" | null;
    phaseDurationMs: number;
    priority: number;
}

interface RateState {
    fileRate: number | null;
    byteRate: number | null;
    fileSamples: number;
    byteSamples: number;
    lastSequence: number;
    kind: PerfOperationKind;
    phaseKey: SelectedOperation["phaseKey"];
}

export interface SyncStatusDiagnostics {
    trackedRates: number;
    activeRateSamples: number;
}

function duration(ms: number): string {
    if (!Number.isFinite(ms) || ms < 0) return "unknown";
    if (ms < 1_000) return `${Math.max(0, Math.round(ms))}ms`;
    if (ms < 60_000) return `${(ms / 1_000).toFixed(ms < 10_000 ? 1 : 0)}s`;
    if (ms < 3_600_000) return `${(ms / 60_000).toFixed(1)}m`;
    if (ms < 24 * 3_600_000) return `${(ms / 3_600_000).toFixed(1)}h`;
    if (ms <= 365 * 24 * 3_600_000) return `${(ms / (24 * 3_600_000)).toFixed(1)}d`;
    return ">1y";
}

function rate(value: number, unit: "f/s" | "B/s"): string {
    if (unit === "f/s" && value < 0.1) return "<0.1 f/s";
    if (unit === "B/s" && value < 1) return "<1 B/s";
    if (unit === "B/s") {
        if (value >= 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(1)} MiB/s`;
        if (value >= 1024) return `${(value / 1024).toFixed(1)} KiB/s`;
    }
    return `${value.toFixed(value >= 100 ? 0 : 1)} ${unit}`;
}

function direction(kind: PerfOperationKind): string {
    if (kind === "push") return "↑";
    if (kind === "pull") return "↓";
    return "⟳";
}

function finiteCount(value: number): number | null {
    return Number.isSafeInteger(value) && value >= 0 ? value : null;
}

function milestoneSummary(operation: PerfActiveOperation): string | null {
    const prepared = finiteCount(operation.preparedFiles);
    const confirmed = finiteCount(operation.serverConfirmedFiles);
    const committed = finiteCount(operation.rootCommittedPaths);
    const cuts = finiteCount(operation.rootCommittedCuts);
    const deletes = finiteCount(operation.trackedDeletesCommitted);
    const parts: string[] = [];
    if (prepared !== null && prepared > 0) parts.push(`prep ${prepared} volatile`);
    if (confirmed !== null && confirmed > 0) parts.push(`server ${confirmed} confirmed`);
    if ((committed !== null && committed > 0) || (cuts !== null && cuts > 0)) {
        parts.push(`root ${committed ?? "?"} ${committed === 1 ? "path" : "paths"}/` +
            `${cuts ?? "?"} ${cuts === 1 ? "cut" : "cuts"}`);
    }
    if (deletes !== null && deletes > 0) parts.push(`deletes ${deletes} committed`);
    return parts.length > 0 ? parts.join(" · ") : null;
}

function capStatus(text: string): string {
    if (text.length <= MAX_STATUS_TEXT) return text;
    let prefix = text.slice(0, MAX_STATUS_TEXT - 1);
    const last = prefix.charCodeAt(prefix.length - 1);
    if (last >= 0xd800 && last <= 0xdbff) prefix = prefix.slice(0, -1);
    return `${prefix}…`;
}

function safeBaseStatus(base: string): string {
    const normalized = base.slice(0, MAX_STATUS_TEXT * 2)
        .replace(/[\u0000-\u001f\u007f]+/g, " ")
        .replace(/\s+/g, " ")
        .trim();
    if (!PATH_LIKE_TEXT.test(normalized)) return capStatus(normalized || "sync …");
    if (/sync off/i.test(normalized)) return "sync off";
    if (/re-enroll/i.test(normalized)) return "sync ⚠ re-enroll";
    if (/review/i.test(normalized)) return "sync ⚠ review";
    if (/sync ✗/i.test(normalized)) return "sync ✗";
    if (/sync ⚠/i.test(normalized)) return "sync ⚠";
    if (/sync ⏸/i.test(normalized)) return "sync ⏸";
    return "sync …";
}

function phaseFor(operation: PerfActiveOperation): {
    phase: PhasePresentation | null;
    phaseKey: SelectedOperation["phaseKey"];
    phaseDurationMs: number;
    priority: number;
} {
    if (!operation.visible) {
        return {
            phase: { label: "paused by OS visibility; no progress", priority: 1_000 },
            phaseKey: "visibility",
            phaseDurationMs: operation.sinceProgressMs,
            priority: 1_000,
        };
    }
    let selected: {
        phase: PhasePresentation;
        phaseKey: PerfPhase;
        phaseDurationMs: number;
    } | null = null;
    for (const active of operation.activePhases) {
        const presentation = PHASE_PRESENTATION[active.name];
        if (!presentation) continue;
        if (!selected || presentation.priority > selected.phase.priority ||
            (presentation.priority === selected.phase.priority &&
                active.durationMs > selected.phaseDurationMs)) {
            selected = { phase: presentation, phaseKey: active.name, phaseDurationMs: active.durationMs };
        }
    }
    if (selected) return { ...selected, priority: selected.phase.priority };
    if (operation.sinceProgressMs >= BETWEEN_PHASES_AFTER_MS) {
        return {
            phase: { label: "waiting between batches", priority: 1 },
            phaseKey: "between",
            phaseDurationMs: operation.sinceProgressMs,
            priority: 1,
        };
    }
    return { phase: null, phaseKey: null, phaseDurationMs: 0, priority: 0 };
}

function selectOperation(operations: readonly PerfActiveOperation[]): SelectedOperation | null {
    let selected: SelectedOperation | null = null;
    for (const operation of operations) {
        const phase = phaseFor(operation);
        const candidate = { operation, ...phase };
        if (!selected || candidate.priority > selected.priority ||
            (candidate.priority === selected.priority &&
                (Number.isFinite(candidate.operation.durationMs) ? candidate.operation.durationMs : 0) >
                (Number.isFinite(selected.operation.durationMs) ? selected.operation.durationMs : 0))) {
            selected = candidate;
        }
    }
    return selected;
}

function smoothed(previous: number | null, sample: number): number {
    return previous === null
        ? sample
        : previous * (1 - RATE_EWMA_NEW_WEIGHT) + sample * RATE_EWMA_NEW_WEIGHT;
}

function updateRate(
    previousRate: number | null,
    previousSamples: number,
    sample: number | null,
): { value: number | null; samples: number } {
    if (sample === null || !Number.isFinite(sample) || sample <= 0) {
        return { value: null, samples: 0 };
    }
    if (previousRate === null || previousRate <= 0 ||
        sample / previousRate > MAX_CONSISTENT_RATE_RATIO ||
        previousRate / sample > MAX_CONSISTENT_RATE_RATIO) {
        return { value: sample, samples: 1 };
    }
    return {
        value: smoothed(previousRate, sample),
        samples: Math.min(Number.MAX_SAFE_INTEGER, previousSamples + 1),
    };
}

/**
 * Bounded, path-free status model. It consumes the same trusted performance
 * windows as the resource governor and owns no timers or promises. A window
 * that crossed a visibility transition/scheduler suspension clears its rate,
 * so a stale pre-suspension sample can never produce an ETA after resume.
 */
export class SyncStatusPresenter {
    private readonly rates = new Map<string, RateState>();

    observeWindow(window: PerfOperationWindow, operation?: PerfActiveOperation): void {
        const previous = this.rates.get(window.operationId);
        if (!Number.isSafeInteger(window.sequence) || window.sequence < 1) {
            if (previous) this.store(window.operationId, {
                ...previous, fileRate: null, byteRate: null, fileSamples: 0, byteSamples: 0,
            });
            return;
        }
        // Global perf sequence is strictly increasing. An older delayed
        // callback must not restore pre-suspension throughput or erase a newer
        // visible sample for this operation.
        if (previous && window.sequence <= previous.lastSequence) return;
        const phase = operation && operation.operationId === window.operationId &&
            operation.kind === window.kind ? phaseFor(operation).phaseKey : null;
        const empty: RateState = {
            fileRate: null,
            byteRate: null,
            fileSamples: 0,
            byteSamples: 0,
            lastSequence: window.sequence,
            kind: window.kind,
            phaseKey: phase,
        };
        if (!window.continuousVisible || !Number.isFinite(window.activeDurationMs) ||
            window.activeDurationMs < 250) {
            this.store(window.operationId, empty);
            return;
        }
        const seconds = window.activeDurationMs / 1_000;
        const fileSample = Number.isFinite(window.filesCompleted) && window.filesCompleted > 0
            ? window.filesCompleted / seconds : null;
        const byteSample = Number.isFinite(window.bytesTransferred) && window.bytesTransferred > 0
            ? window.bytesTransferred / seconds : null;
        if (fileSample === null && byteSample === null) {
            this.store(window.operationId, empty);
            return;
        }
        const sameContext = previous?.kind === window.kind && previous.phaseKey === phase;
        const files = updateRate(
            sameContext ? previous?.fileRate ?? null : null,
            sameContext ? previous?.fileSamples ?? 0 : 0,
            fileSample,
        );
        const bytes = updateRate(
            sameContext ? previous?.byteRate ?? null : null,
            sameContext ? previous?.byteSamples ?? 0 : 0,
            byteSample,
        );
        const next: RateState = {
            fileRate: files.value,
            byteRate: bytes.value,
            fileSamples: files.samples,
            byteSamples: bytes.samples,
            lastSequence: window.sequence,
            kind: window.kind,
            phaseKey: phase,
        };
        this.store(window.operationId, next);
    }

    private store(operationId: string, next: RateState): void {
        this.rates.delete(operationId);
        this.rates.set(operationId, next);
        while (this.rates.size > MAX_RATE_OPERATIONS) {
            const oldest = this.rates.keys().next().value as string | undefined;
            if (oldest === undefined) break;
            this.rates.delete(oldest);
        }
    }

    render(
        base: string,
        operations: readonly PerfActiveOperation[],
        truth?: SyncStatusTruth,
    ): string {
        const activeIds = new Set(operations.map(operation => operation.operationId));
        for (const operationId of this.rates.keys()) {
            if (!activeIds.has(operationId)) this.rates.delete(operationId);
        }

        const safeBase = safeBaseStatus(base);
        if (/sync off/i.test(safeBase)) return safeBase;
        if (ACTION_STATUS.test(safeBase)) return safeBase;
        if (truth?.error || /sync ✗/i.test(safeBase)) return "sync ✗";
        if (SAFETY_STATUS.test(safeBase)) return safeBase;

        const selected = selectOperation(operations);
        if (selected) return capStatus(this.renderOperation(selected));

        if (truth) {
            if (truth.rootRecovering || truth.rootRecoveryRequired || truth.rootPending) {
                return "sync ⏸ root recovery";
            }
            const deferred = finiteCount(truth.deferredChanges);
            if (deferred !== null && deferred > 0) {
                return `sync ⚠ ${deferred} pending`;
            }
            const pending = finiteCount(truth.pendingChanges);
            if (pending !== null && pending > 0) {
                return `sync … · ${pending} pending`;
            }
            // A callback owner/reconcile can temporarily outlive its perf span.
            // Never let an older root-equality-derived checkmark cover it.
            if (truth.busy) return "sync … · waiting for engine";
        }
        return safeBase;
    }

    diagnostics(activeOperationIds: readonly string[]): SyncStatusDiagnostics {
        const active = new Set(activeOperationIds);
        let activeRateSamples = 0;
        for (const [operationId, sample] of this.rates) {
            if (active.has(operationId)) {
                activeRateSamples = Math.min(
                    Number.MAX_SAFE_INTEGER,
                    activeRateSamples + Math.max(sample.fileSamples, sample.byteSamples),
                );
            }
        }
        return { trackedRates: this.rates.size, activeRateSamples };
    }

    private renderOperation(selected: SelectedOperation): string {
        const operation = selected.operation;
        const completed = finiteCount(operation.filesCompleted);
        const total = operation.filesTotal === null ? null : finiteCount(operation.filesTotal);
        const progress = completed !== null && total !== null && total >= completed
            ? `${completed}/${total}`
            : completed !== null && completed > 0 ? `${completed}/?` : "working";
        const parts = [`${direction(operation.kind)} ${progress}`];
        const currentRate = this.rates.get(operation.operationId);
        const rateIsFresh = operation.visible && Number.isFinite(operation.sinceProgressMs) &&
            operation.sinceProgressMs >= 0 && operation.sinceProgressMs < BLOCKER_AFTER_MS &&
            RATE_PHASES.has(selected.phaseKey) && currentRate?.kind === operation.kind &&
            currentRate.phaseKey === selected.phaseKey;
        if (currentRate && rateIsFresh) {
            if (currentRate.fileRate !== null) {
                parts.push(rate(currentRate.fileRate, "f/s"));
                if (currentRate.fileSamples >= ETA_MIN_SAMPLES && completed !== null &&
                    total !== null && completed < total) {
                    const etaMs = (total - completed) / currentRate.fileRate * 1_000;
                    if (Number.isFinite(etaMs) && etaMs > 0 && etaMs <= MAX_ETA_MS) {
                        parts.push(`ETA ${duration(etaMs)}`);
                    }
                }
            } else if (currentRate.byteRate !== null) {
                parts.push(`content ${rate(currentRate.byteRate, "B/s")}`);
            }
        }
        const milestones = milestoneSummary(operation);
        if (milestones) parts.push(milestones);
        if (selected.phase) {
            parts.push(operation.sinceProgressMs >= BLOCKER_AFTER_MS || !operation.visible
                ? `${selected.phase.label} ${duration(selected.phaseDurationMs)}`
                : selected.phase.label);
        }
        return parts.join(" · ");
    }
}

/** Stateless compatibility helper for focused consumers/tests without rates. */
export function renderActiveSyncStatus(
    base: string,
    operations: readonly PerfActiveOperation[],
    truth?: SyncStatusTruth,
): string {
    return new SyncStatusPresenter().render(base, operations, truth);
}
