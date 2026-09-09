import assert from "node:assert/strict";
import type {
    PerfActiveOperation,
    PerfOperationKind,
    PerfOperationWindow,
    PerfPhase,
} from "./perf-trace";
import {
    renderActiveSyncStatus,
    SyncStatusPresenter,
    type SyncStatusTruth,
} from "./sync-status";

function operation(options: {
    id?: string;
    kind?: PerfOperationKind;
    phase?: PerfPhase;
    phaseMs?: number;
    sinceProgressMs?: number;
    visible?: boolean;
    completed?: number;
    total?: number | null;
    prepared?: number;
    confirmed?: number;
    committed?: number;
    cuts?: number;
    deletes?: number;
} = {}): PerfActiveOperation {
    return {
        operationId: options.id ?? "push-1",
        kind: options.kind ?? "push",
        durationMs: 20_000,
        sinceProgressMs: options.sinceProgressMs ?? 2_000,
        visible: options.visible ?? true,
        filesTotal: options.total === undefined ? 100 : options.total,
        filesCompleted: options.completed ?? 10,
        bytesTransferred: 0,
        preparedFiles: options.prepared ?? 0,
        serverConfirmedFiles: options.confirmed ?? 0,
        rootCommittedPaths: options.committed ?? 0,
        rootCommittedCuts: options.cuts ?? 0,
        trackedDeletesCommitted: options.deletes ?? 0,
        requestCount: 1,
        wsFrameCount: 1,
        retries: 0,
        activePhases: options.phase ? [{
            name: options.phase,
            count: 1,
            durationMs: options.phaseMs ?? 2_000,
        }] : [],
        phases: {},
        eventLoopLagP95Ms: null,
        eventLoopLagExcludedSamples: 0,
    };
}

function window(options: {
    id?: string;
    kind?: PerfOperationKind;
    sequence?: number;
    files?: number;
    bytes?: number;
    durationMs?: number;
    visible?: boolean;
} = {}): PerfOperationWindow {
    const durationMs = options.durationMs ?? 1_000;
    return {
        sequence: options.sequence ?? 1,
        startedAtMs: 0,
        endedAtMs: durationMs,
        operationId: options.id ?? "push-1",
        kind: options.kind ?? "push",
        outcome: "success",
        durationMs,
        activeDurationMs: options.visible === false ? 0 : durationMs,
        continuousVisible: options.visible !== false,
        filesCompleted: options.files ?? 0,
        bytesTransferred: options.bytes ?? 0,
        retries: 0,
        backpressureEvents: 0,
        peakBatchBytes: 0,
        eventLoopLagP95Ms: null,
        demand: {},
    };
}

const idleTruth: SyncStatusTruth = {
    busy: false,
    pendingChanges: 0,
    deferredChanges: 0,
    rootPending: false,
    rootRecovering: false,
    rootRecoveryRequired: false,
    error: false,
};

assert.equal(renderActiveSyncStatus("↑ 10/100", []), "↑ 10/100");
assert.equal(
    renderActiveSyncStatus("↑ 10/100", [operation({
        phase: "ws_ack_wait", sinceProgressMs: 1_499,
    })]),
    "↑ 10/100 · awaiting server ACK",
);
assert.equal(
    renderActiveSyncStatus("↑ 10/100", [operation({ phase: "ws_ack_wait", phaseMs: 12_200 })]),
    "↑ 10/100 · awaiting server ACK 12s",
);
assert.equal(
    renderActiveSyncStatus("↑ 10/100", [operation({ phase: "ws_credit_wait", visible: false })]),
    "↑ 10/100 · paused by OS visibility; no progress 2.0s",
);
assert.equal(
    renderActiveSyncStatus("sync ↑", [
        operation({ phase: "hash", phaseMs: 9_000 }),
        operation({ id: "push-2", phase: "root_commit", phaseMs: 2_500 }),
    ]),
    "↑ 10/100 · committing root 2.5s",
);
assert.equal(
    renderActiveSyncStatus("sync ↑", [operation({ sinceProgressMs: 5_500 })]),
    "↑ 10/100 · waiting between batches 5.5s",
);
for (const [phase, blocker] of [
    ["scheduler_wait", "waiting for scheduler"],
    ["ws_send_queue", "waiting for WS send"],
    ["read", "reading file"],
    ["hash", "hashing"],
    ["check", "checking remote objects"],
    ["upload", "uploading batch"],
    ["apply", "applying remote files"],
    ["checkpoint", "saving resume checkpoint"],
] as const) {
    assert.match(
        renderActiveSyncStatus("sync ↑", [operation({ phase, phaseMs: 3_200 })]),
        new RegExp(`${blocker} 3\\.2s$`),
        `${phase} was not exposed as the exact timed blocker`,
    );
}
for (const protectedStatus of [
    "sync off", "sync ✗", "sync ⚠ review", "sync ⚠ re-enroll", "sync ⏸ root not published",
]) {
    assert.equal(
        renderActiveSyncStatus(protectedStatus, [operation({ phase: "ws_ack_wait" })]),
        protectedStatus,
    );
}

const presenter = new SyncStatusPresenter();
presenter.observeWindow(window({ sequence: 1, files: 10 }), operation({
    phase: "hash", phaseMs: 100, sinceProgressMs: 100,
}));
presenter.observeWindow(window({ sequence: 2, files: 10 }), operation({
    phase: "hash", phaseMs: 100, sinceProgressMs: 100,
}));
assert.equal(
    presenter.render("sync ✓", [operation({
        phase: "hash", phaseMs: 500, sinceProgressMs: 1_000, completed: 20,
    })], idleTruth),
    "↑ 20/100 · 10.0 f/s · ETA 8.0s · hashing",
    "an old checkmark hid current phase/progress or the trusted smoothed ETA",
);
assert.equal(
    presenter.render("sync ✓", [operation({
        phase: "ws_ack_wait", phaseMs: 2_000, sinceProgressMs: 2_000, completed: 20,
    })], idleTruth),
    "↑ 20/100 · awaiting server ACK 2.0s",
    "a stale rate/ETA survived a real no-progress blocker",
);

presenter.observeWindow(window({ sequence: 3, visible: false }), operation({ visible: false }));
assert.equal(
    presenter.render("sync ✓", [operation({
        phase: "hash", visible: false, sinceProgressMs: 12_000, completed: 20,
    })], idleTruth),
    "↑ 20/100 · paused by OS visibility; no progress 12s",
    "a suspended window was presented as throughput or ETA",
);
presenter.observeWindow(window({ sequence: 4, files: 10 }), operation({
    phase: "hash", phaseMs: 100, sinceProgressMs: 100,
}));
assert.equal(
    presenter.render("sync ✓", [operation({
        phase: "hash", phaseMs: 500, sinceProgressMs: 0, completed: 30,
    })], idleTruth),
    "↑ 30/100 · 10.0 f/s · hashing",
    "ETA resumed before two fresh post-suspension samples",
);

const byteRate = new SyncStatusPresenter();
byteRate.observeWindow(window({ id: "pull-1", kind: "pull",
    bytes: 2 * 1024 * 1024, durationMs: 2_000 }), operation({
    id: "pull-1", kind: "pull", phase: "download", phaseMs: 100, sinceProgressMs: 100,
}));
assert.equal(
    byteRate.render("sync ↓", [operation({ id: "pull-1", kind: "pull", total: null,
        completed: 0, phase: "download", phaseMs: 500, sinceProgressMs: 100 })], idleTruth),
    "↓ working · content 1.0 MiB/s · downloading batch",
    "content throughput was mislabeled as file or protocol throughput",
);

const outlier = new SyncStatusPresenter();
outlier.observeWindow(window({ sequence: 1, files: 1_000 }), operation({
    phase: "hash", phaseMs: 100, sinceProgressMs: 100,
}));
outlier.observeWindow(window({ sequence: 2, files: 10 }), operation({
    phase: "hash", phaseMs: 100, sinceProgressMs: 100,
}));
assert.equal(
    outlier.render("sync ↑", [operation({ phase: "hash", phaseMs: 100,
        sinceProgressMs: 100, completed: 20 })], idleTruth),
    "↑ 20/100 · 10.0 f/s · hashing",
    "one metadata-resolution burst poisoned the later ETA",
);

const ordered = new SyncStatusPresenter();
const hashSnapshot = operation({ phase: "hash", phaseMs: 100,
    sinceProgressMs: 100, completed: 20 });
ordered.observeWindow(window({ sequence: 10, files: 10 }), hashSnapshot);
ordered.observeWindow(window({ sequence: 11, files: 10 }), hashSnapshot);
ordered.observeWindow(window({ sequence: 9, visible: false }), operation({ visible: false }));
assert.match(
    ordered.render("sync ↑", [hashSnapshot], idleTruth),
    /10\.0 f\/s · ETA 8\.0s/,
    "an out-of-order hidden window erased newer visible throughput",
);
ordered.observeWindow(window({ sequence: 12, files: 0 }), hashSnapshot);
ordered.observeWindow(window({ sequence: 11, files: 1_000 }), hashSnapshot);
assert.equal(
    ordered.render("sync ↑", [hashSnapshot], idleTruth),
    "↑ 20/100 · hashing",
    "a counter-reset window retained rate or an older window resurrected it",
);

const phaseFence = new SyncStatusPresenter();
phaseFence.observeWindow(window({ sequence: 1, files: 10 }), hashSnapshot);
phaseFence.observeWindow(window({ sequence: 2, files: 10 }), hashSnapshot);
const secretOperation = operation({ id: "/private/vault/secret.md", phase: "root_commit",
    phaseMs: 200, sinceProgressMs: 100, completed: 20 });
assert.equal(
    phaseFence.render("sync ↑", [secretOperation, operation({ id: "push-1",
        phase: "root_commit", phaseMs: 300, sinceProgressMs: 100, completed: 20 })], idleTruth),
    "↑ 20/100 · committing root",
    "a prior hash rate survived a root phase change or an operation ID leaked",
);

assert.equal(
    renderActiveSyncStatus("sync ↑", [operation({ completed: 20, total: 10,
        phase: "hash", phaseMs: 100, sinceProgressMs: 100 })]),
    "↑ 20/? · hashing",
    "completed>total produced a negative ETA or false completion",
);
assert.equal(
    renderActiveSyncStatus("sync ↑", [operation({ completed: 10, prepared: 10,
        confirmed: 8, committed: 4, cuts: 1, deletes: 1,
        phase: "root_commit", phaseMs: 300, sinceProgressMs: 100 })]),
    "↑ 10/100 · prep 10 volatile · server 8 confirmed · root 4 paths/1 cut · deletes 1 committed · committing root",
    "durable milestones were collapsed into generic or falsely-synced progress",
);
assert.equal(
    renderActiveSyncStatus("sync ↑", [operation({ prepared: Number.NaN,
        confirmed: -1, committed: Number.POSITIVE_INFINITY, cuts: 0, deletes: 0,
        phase: "check", phaseMs: 100, sinceProgressMs: 100 })]),
    "↑ 10/100 · checking remote objects",
    "invalid milestone counters escaped into status text",
);
assert.equal(
    renderActiveSyncStatus("sync ↑", [operation({ completed: Number.POSITIVE_INFINITY,
        total: Number.NaN, phase: "hash", phaseMs: Number.POSITIVE_INFINITY })]),
    "↑ working · hashing unknown",
    "non-finite counters escaped into status text",
);
assert.equal(
    renderActiveSyncStatus("sync /private/vault/secret.md", []),
    "sync …",
    "a path-like base status leaked into the status bar",
);
assert.equal(
    renderActiveSyncStatus(`sync ⚠ review /private/vault/secret.md`, [], {
        ...idleTruth, error: true,
    }),
    "sync ⚠ review",
    "review lost precedence over an error or leaked a path",
);
assert.equal(
    renderActiveSyncStatus("sync ⏸ root not published", [], { ...idleTruth, error: true }),
    "sync ✗",
    "a stale pause hid the engine error state",
);
const capped = renderActiveSyncStatus(`sync … ${"x".repeat(1_000)}`, []);
assert.equal(capped.length, 180, "status text escaped its fixed display cap");
assert.ok(capped.endsWith("…"), "capped status did not signal truncation");

assert.equal(
    presenter.render("sync ✓", [], { ...idleTruth, pendingChanges: 3 }),
    "sync … · 3 pending",
    "root equality hid queued local changes",
);
assert.equal(
    presenter.render("sync ✓", [], { ...idleTruth, deferredChanges: 2 }),
    "sync ⚠ 2 pending",
    "root equality hid deferred work",
);
assert.equal(
    presenter.render("sync ✓", [], { ...idleTruth, busy: true }),
    "sync … · waiting for engine",
    "root equality hid an owner outside a perf span",
);
assert.equal(
    presenter.render("sync ✓", [], { ...idleTruth, rootPending: true }),
    "sync ⏸ root recovery",
    "root equality hid pending recovery",
);
assert.equal(
    presenter.render("sync ✓", [], { ...idleTruth, error: true }),
    "sync ✗",
    "root equality hid the engine error state",
);

const bounded = new SyncStatusPresenter();
for (let i = 0; i < 20; i++) bounded.observeWindow(window({
    id: `op-${i}`, sequence: i + 1, files: 1,
}));
assert.equal(bounded.diagnostics([]).trackedRates, 16, "status estimator retained unbounded operations");

console.log("sync-status.test: passed");
