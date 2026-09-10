import { App, TAbstractFile, TFile, debounce, Notice } from "obsidian";

import { yieldWork as yieldToUI, throwIfWorkAborted, type WorkLane } from "./work-scheduler";
import { EngineWorkTracker } from "./engine-work";
import { MaxLatencyCoalescer } from "./max-latency-coalescer";

/** A failed sibling cannot release this engine while an admitted native read
 * still borrows its WASM/runtime. Batches are already concurrency-bounded. */
async function joinStartedWork<T>(work: Promise<T>[]): Promise<T[]> {
    const settled = await Promise.allSettled(work);
    const values: T[] = [];
    for (const result of settled) {
        if (result.status === "rejected") throw result.reason;
        values.push(result.value);
    }
    return values;
}

/** Bytes → human-readable short form. Used in status/progress messages. */
function formatBytes(n: number): string {
    if (n >= 1_073_741_824) return `${(n / 1_073_741_824).toFixed(2)} GB`;
    if (n >= 1_048_576)     return `${(n / 1_048_576).toFixed(1)} MB`;
    if (n >= 1024)          return `${(n / 1024).toFixed(0)} KB`;
    return `${n} B`;
}

/** Files above this size skip WASM hashing during scan — push.ts hashes them
 *  during upload via FastCDC (wasm_chunk_file returns file_hash). This keeps
 *  WASM linear memory bounded to the ~4 MiB FastCDC window rather than the
 *  complete file. */
const LARGE_FILE_THRESHOLD = 1_048_576; // 1 MB
/** Root work may exceed these soft ceilings only to finish the dependency
 * component crossing the current ACK-safe content boundary. */
const ROOT_PUBLICATION_MAX_BOUNDARY_FILES = 32;
const ROOT_PUBLICATION_MAX_ELAPSED_MS = 50;
import { ObsetyncApi, PushConflict } from "./api";
import { conflictCopyPath } from "./conflict-path";
import { legacyConflictStagingKey, preserveVerifiedConflictCopy } from "./root-conflicts";
import { ObsetyncWsChannel, PresenceUpdate, WsState } from "./ws";
import { PlatformIO, type FileStat } from "./platform";
import { ObsetyncSyncBase } from "./sync-base";
import { ObsetyncJournal, type NewJournalEntry } from "./journal";
import type { PreparedTransferRuntime } from "./transfer-plan-scope";
import { retirablePreparedHints } from "./prepared-retirement";
import { perfSpan } from "./debug-log";
import { pull, PullTreeRebaseError, type PullResult } from "./pull";
import { drainTreeReachabilityRetirement,
    hasPendingTreeReachabilityRetirement } from "./tree-candidate-job";
import { drainTreeCandidateMutationRetirement,
    hasPendingTreeCandidateMutationRetirement } from "./tree-candidate-mutation-job";
import { push, hashFileStreaming, FileChange, WasmModule, WasmTree } from "./push";
import { SyncPriority } from "./settings";
import { sortByPriority, sortByPriorityCooperatively, type PrioritySortOptions } from "./priority-sort";
import { compileIgnore, type CompiledIgnore } from "./ignore";
import { PullEchoTracker } from "./pull-echo";
import { EditorPullGuard } from "./editor-pull-guard";
import { DIRTY_CLAIM_LIMIT, DirtyPathSet, type DirtyFileChange, type DirtyPathCapture } from "./dirty-set";
import { LocalEventGuards } from "./local-event-guards";
import { DeferredChangeTracker, type DeferredChangeSummary } from "./deferred-changes";
import { materializeFileChanges } from "./file-safety";
import { OperationCheckpoint } from "./operation-checkpoint";
import { exactArrayBuffer } from "./binary";
import { getHashTuning } from "./hash-runtime";
import { HashSourceGrowthError } from "./hash-source-budget";
import { reserveTransientScope, transientMemorySnapshot } from "./transient-memory";
import { ResourceBudgetClosedError } from "./resource-budget";
import {
    HashWorkerFileDriftError,
    HashWorkerPoolError,
    waitForHashWorkerCleanup,
    type DesktopHashWorkerPool,
} from "./desktop-hash-workers";
import type { BrowserHashWorkerRuntime } from "./browser-hash-workers";
import { repairSmallContent, repairLargeContent, validatedReconcileMissing,
    type ReconcileRepairStats } from "./reconcile-upload";
import { uploadIndexChunks } from "./index-upload";
import {
    automaticChangeNeedsReview,
    metadataScanNeedsReview,
    planMetadataScan,
} from "./scan-planner";
import { isSafeVaultPath } from "./delta-validation";
import { isReenrollmentRequiredError } from "./transport-errors";
import {
    perfSampleWeight,
    perfTrace,
    type PerfOperation,
    type PerfOutcome,
} from "./perf-trace";
import {
    buildReconcileContentIndex,
    sumIndexedContentBytes,
} from "./reconcile-content";
import {
    MobileLifecycleQuiesceError,
    type ResourceVisibilityGate,
    type ResourceVisibilityWork,
} from "./resource-governor";
import { RootSyncRuntime, type RootDowngradeAuthoritySnapshot, type RootSyncIdentity } from "./root-sync-runtime";
import type { RootBatchDeferrals } from "./root-batch";
import { runRootBatchDrain } from "./root-batch-drain";
import type { RootLocalOmission } from "./root-local-omissions";
import { RootReviewedQueue, RootReviewInvalidated, RootReviewPaused, RootReviewPreempted, ROOT_REVIEW_LIMITS,
    type RootReviewedQueuePorts, type RootReviewedSelection } from "./root-reviewed-queue";
import { hasPendingRootTreeRetirement, rebuildRootTreeFromBase,
    RootTreeOutputAdmissionDeniedError } from "./root-tree-repair";
import type { RootTreeResidentAdmission } from "./root-tree-resident-admission";
import { exportTreeRoot, TREE_ROOT_EXPORT_MAX_STEP_BYTES,
    TREE_ROOT_EXPORT_MAX_STEP_UNITS, TREE_ROOT_EXPORT_PAGE_BYTES } from "./tree-root-export-job";
import { candidateMutationRefusalAdmissionEpoch, candidateMutationRefusalCut,
    CandidateMutationOutputAdmissionDeniedError } from "./candidate-mutation-output-admission";

interface RecentLocalUpsertRecord {
    readonly token: symbol;
    hint?: DirtyFileChange;
}

/** Bounded volatile scheduling provenance. It never authorizes a read, ACK,
 * deletion or root mutation; the reviewed queue still establishes all of
 * those. Keeping it outside a review owner lets a safe rebuild remember which
 * pending upserts came from recent local callbacks. */
class RecentLocalUpserts {
    private readonly records = new Map<string, RecentLocalUpsertRecord>();

    constructor(private readonly maxPaths: number) {
        if (!Number.isSafeInteger(maxPaths) || maxPaths < 1 || maxPaths > ROOT_REVIEW_LIMITS.overlayPaths) {
            throw new RangeError("recent local upsert limit is invalid");
        }
    }

    begin(path: string): symbol {
        const token = Symbol();
        this.records.delete(path);
        this.records.set(path, { token });
        while (this.records.size > this.maxPaths) {
            const oldest = this.records.keys().next().value as string | undefined;
            if (oldest === undefined) throw new Error("recent local upsert eviction failed");
            this.records.delete(oldest);
        }
        return token;
    }

    complete(path: string, token: symbol, hint: DirtyFileChange): void {
        const record = this.records.get(path);
        if (record?.token !== token) return;
        if (hint.path !== path || (hint.action !== "created" && hint.action !== "modified")) {
            throw new Error("recent local upsert completion is invalid");
        }
        record.hint = { ...hint };
    }

    cancel(path: string, token: symbol): void {
        if (this.records.get(path)?.token === token) this.records.delete(path);
    }

    hasPending(dirty: DirtyPathCapture, inFlight: Pick<LocalEventGuards, "has">): boolean {
        for (const [path, record] of this.records) {
            if (record.hint === undefined) {
                if (inFlight.has(path)) return true;
                continue;
            }
            const current = dirty.get(path);
            if (current !== undefined && this.matches(current)) return true;
        }
        return false;
    }

    /** Initial-review preemption is stricter than lane selection: an in-flight
     * callback or a session-only no-ID hint cannot create prefix authority. */
    hasDurablePending(dirty: DirtyPathCapture): boolean {
        for (const record of this.records.values()) {
            const recent = record.hint;
            if (!recent || !Number.isSafeInteger(recent.journalId) || recent.journalId! <= 0) continue;
            const current = dirty.get(recent.path);
            if (current !== undefined && this.matches(current)) return true;
        }
        return false;
    }

    durablePaths(firstPath?: string | null): readonly string[] {
        const paths: string[] = [];
        const add = (path: string, record: RecentLocalUpsertRecord | undefined) => {
            if (record?.hint && Number.isSafeInteger(record.hint.journalId) && record.hint.journalId! > 0) paths.push(path);
        };
        if (firstPath) add(firstPath, this.records.get(firstPath));
        for (const [path, record] of this.records) {
            if (path !== firstPath) add(path, record);
        }
        return paths;
    }

    matches(hint: DirtyFileChange): boolean {
        const recent = this.records.get(hint.path)?.hint;
        return recent !== undefined && recent.action === hint.action && recent.journalId === hint.journalId &&
            recent.hash === hint.hash && recent.mtime === hint.mtime && recent.size === hint.size;
    }

    retire(hint: DirtyFileChange, hasNewerWork: (path: string) => boolean): void {
        if (!this.matches(hint) || hasNewerWork(hint.path)) return;
        this.records.delete(hint.path);
    }

    get size(): number { return this.records.size; }
}

export type SyncState = "idle" | "pulling" | "pushing" | "scanning" | "error";

export interface EngineLegacyDowngradeLease {
    readonly treeVersion: 1;
    readonly rootIntents: {
        pending(): RootDowngradeAuthoritySnapshot["pending"];
        readonly lastSequence: number;
    };
    /** Fails synchronously after revoke, retirement, replacement, or any
     * ownership invariant changing underneath this exact engine cut. */
    assertHeld(): void;
    /** Synchronously invalidate authority; native retirement is separate. */
    revoke(): void;
    /** Invalidate authority and dispose the quiesced root runtime exactly once. */
    closeAndDrain(): Promise<void>;
}

interface LegacyDowngradeFreezeState {
    readonly token: object;
    readonly runtime: RootSyncRuntime;
    readonly treeVersion: 1;
    readonly operationDrain: Promise<void>;
    readonly callbackDrain: Promise<void>;
    readonly rootDrain: Promise<void>;
    revoked: boolean;
    released: boolean;
    granted: boolean;
    retirement: Promise<void> | null;
}

export interface ReconcileResult {
    smallUploaded: number;
    largeUploaded: number;
    treeChunksUploaded: number;
    bytes: number;
    deferred: number;
    drifted: number;
    readErrors: number;
    uploadErrors: number;
}

export interface ReconcileSummary {
    ts: number;
    incomplete: boolean;
    /** Null when the check itself failed before an honest count was available. */
    result: ReconcileResult | null;
}

interface RootTreeAdmissionRefusal {
    tree: WasmTree;
    base: ReturnType<ObsetyncSyncBase["captureTreeEntries"]>;
    capacityBytes: number;
    usedBytes: number;
    activeLeases: number;
    closed: boolean;
    repairVersion: 1 | 2;
    treeVersion: number;
    committedRevision: number | undefined;
    candidateRevision: number | undefined;
    rootHash: string | null;
    totalFiles: number;
    error: RootTreeOutputAdmissionDeniedError;
}

/** Session-only suppression for one unchanged whole-cut V2 push refusal.
 * This is a scheduling witness, never durable state or memory-release proof. */
interface CandidateMutationAttemptWitness {
    tree: WasmTree;
    admission: RootTreeResidentAdmission;
    base: ReturnType<ObsetyncSyncBase["captureTreeEntries"]>;
    runtime: RootSyncRuntime;
    treeBaseRoot: string | null;
    journalEpoch: string;
    retryKey: string;
    retryEpoch: object;
    allowBulkChange: boolean;
    ignore: unknown;
    priority: SyncPriority;
    syncConfig: boolean;
    dependencyRevision: object;
    planningRevision: object;
    inputCount: number;
    treeVersion: number;
    committedRevision: number | undefined;
    candidateRevision: number | undefined;
    rootHash: string | null;
    totalFiles: number;
    ledgerBefore: ReturnType<RootTreeResidentAdmission["snapshot"]>;
    admissionMutationEpoch: number;
}

interface CandidateMutationAdmissionRefusal extends CandidateMutationAttemptWitness {
    dirty: DirtyPathCapture;
    ledger: ReturnType<RootTreeResidentAdmission["snapshot"]>;
    error: CandidateMutationOutputAdmissionDeniedError;
}

function sameResidentAdmissionSnapshot(
    a: ReturnType<RootTreeResidentAdmission["snapshot"]>,
    b: ReturnType<RootTreeResidentAdmission["snapshot"]>,
): boolean {
    const left = a.ledger, right = b.ledger;
    return a.scope === b.scope && a.residentTrees === b.residentTrees &&
        a.privateAttempts === b.privateAttempts && a.retiringOwners === b.retiringOwners &&
        a.privateBytes === b.privateBytes && a.residentBytes === b.residentBytes &&
        a.retiringBytes === b.retiringBytes && left.capacityBytes === right.capacityBytes &&
        left.usedBytes === right.usedBytes && left.peakUsedBytes === right.peakUsedBytes &&
        left.availableBytes === right.availableBytes &&
        left.overcommittedBytes === right.overcommittedBytes &&
        left.activeLeases === right.activeLeases && left.closed === right.closed &&
        left.refusedReservations === right.refusedReservations &&
        left.refusedGrowths === right.refusedGrowths;
}

function residentAdmissionAfterOneRefusal(
    before: ReturnType<RootTreeResidentAdmission["snapshot"]>,
    after: ReturnType<RootTreeResidentAdmission["snapshot"]>,
): boolean {
    const expectedRefusals = before.ledger.refusedReservations === Number.MAX_SAFE_INTEGER
        ? Number.MAX_SAFE_INTEGER : before.ledger.refusedReservations + 1;
    return before.scope === after.scope && before.residentTrees === after.residentTrees &&
        before.privateAttempts === after.privateAttempts && before.retiringOwners === after.retiringOwners &&
        before.privateBytes === after.privateBytes && before.residentBytes === after.residentBytes &&
        before.retiringBytes === after.retiringBytes &&
        before.ledger.capacityBytes === after.ledger.capacityBytes &&
        before.ledger.usedBytes === after.ledger.usedBytes &&
        before.ledger.peakUsedBytes === after.ledger.peakUsedBytes &&
        before.ledger.availableBytes === after.ledger.availableBytes &&
        before.ledger.overcommittedBytes === after.ledger.overcommittedBytes &&
        before.ledger.activeLeases === after.ledger.activeLeases &&
        before.ledger.closed === after.ledger.closed &&
        after.ledger.refusedReservations === expectedRefusals &&
        before.ledger.refusedGrowths === after.ledger.refusedGrowths;
}

/**
 * Core sync orchestrator. Coordinates pull, push, journal recovery,
 * metadata auditing, and live vault event tracking (D-005 4-layer system).
 */
export class ObsetyncSyncEngine {
    private state: SyncState = "idle";
    private localRootHash: string | null;
    /** One metadata-only record per dirty path. File bytes are read only when
     *  its push snapshot is actually processed. */
    private pendingChanges = new DirtyPathSet();
    /** Retry metadata only; every deferred generation stays in pendingChanges
     * and in its existing WAL record until a later successful publication. */
    private deferredChanges = new DeferredChangeTracker();
    private syncing = false;
    private syncTimer: number | null = null;
    private eventRefs: any[] = [];
    /** Most recent pull/push failure — surfaced by the debug panel. */
    private lastError: { ts: number; message: string; origin: string } | null = null;
    private lastRepairSummary: ReconcileSummary | null = null;
    /** Snapshots of observed remote / local roots for the debug panel. */
    private lastPullServerRoot: string | null = null;
    /** The server root this device's Merkle tree was last VERIFIABLY
     *  reconciled with — the honest putRoot parent (merge base). Distinct
     *  from `localRootHash`, which is the cached/adopted local fallback used
     *  before a verified base is available. Merely observing a newer server
     *  head must not advance either value: a deferred pull still has missing
     *  paths to request. Persisted in sync-base. */
    private treeBaseRoot: string | null = null;
    /** Set when the tree demonstrably diverged from the state pull applied.
     *  While set, pushes are refused (queued, not dropped) — publishing a
     *  root from an untrusted tree is how vaults get reverted. Cleared by a
     *  verified pull-rebase or a full rescan. */
    private pushBlocked = false;
    /** Terminal transport mismatch: retries cannot succeed until enrollment
     *  is reset, so startup scans and every automatic network loop pause. */
    private reenrollmentRequired = false;
    private reenrollmentNoticeShown = false;
    /** Automatic recovery found vault-sized or deletion-heavy work. */
    private bulkChangeReviewRequired = false;
    /** A persisted Full Rescan approval matched the current recovery plan.
     *  Keep it active across retry attempts in this engine session and clear
     *  the durable marker only after the corresponding publish commits. */
    private bulkChangeApprovalActive = false;
    /** Journal recovery may consume only a subset of an approved vault scan.
     *  Clear the marker after a completed full/metadata scan push, never after
     *  an earlier recovery-only batch. */
    private bulkChangeApprovalCompletesWithPush = false;
    /** Prevent late async completions from reviving an engine after reload. */
    private stopped = false;
    /** Content-authenticated expectations for adapter writes made by pull.
     *  Kept briefly after pull completion because Obsidian can emit the
     *  corresponding vault events asynchronously. */
    private pullEchoes = new PullEchoTracker();
    /** Legacy debounce compatibility/test seam; production scheduling is owned
     *  by autoPushCoalescer so continuous changes have a hard latency bound. */
    private debouncedPush: (() => void) | null = null;
    /** Short quiet batching with a hard upper latency under continuous edits. */
    private autoPushCoalescer: MaxLatencyCoalescer | null = null;
    /** Paths whose vault callbacks have started but whose durable journal
     *  append/hash classification has not finished yet. Pull consults this
     *  live set immediately before replacing remote content. */
    private localEventsInFlight = new LocalEventGuards();
    /** Recent local upserts retain only scalar generation hints. Freshest
     * eviction affects service priority, never durability or correctness. */
    private recentLocalUpserts = new RecentLocalUpserts(ROOT_REVIEW_LIMITS.overlayPaths);
    /** Closed from construction through local replay and tree negotiation.
     * Only explicit start activation may publish/apply or run a scan. */
    private startupReplayInProgress = true;
    private localPreparation: Promise<void> | null = null;
    private localPrepared = false;
    private listenersAttached = false;
    private startup: Promise<void> | null = null;
    private startupCompleted = false;
    private preparedScopeHash: string | null = null;
    private readonly hashWorkerAbort = new AbortController();
    private hashWorkerFallbackWarned = false;
    private operationWorkTracker?: EngineWorkTracker;
    private callbackWorkTracker?: EngineWorkTracker;
    private terminalDrain: Promise<void> | null = null;
    private captureHandedOff = false;
    private persistenceReload: Promise<void> | null = null;
    private persistenceReloadRequired = false;
    private readonly rootRuntime?: RootSyncRuntime;
    private rootRecoveryWork: Promise<void> | null = null;
    private rootOutputAdmissionRefusal: RootTreeAdmissionRefusal | null = null;
    /** Initial local publication is intentionally not a general recovery
     * request: a pull must still inspect the authenticated remote root before
     * choosing its bootstrap version. Cache only unchanged push admission. */
    private pushBootstrapAdmissionRefusal: RootTreeAdmissionRefusal | null = null;
    private pushBootstrapReportedRefusal: RootTreeOutputAdmissionDeniedError | null = null;
    private candidateMutationAdmissionRefusal: CandidateMutationAdmissionRefusal | null = null;
    /** Manual recovery requests invalidate even a marker installed by an
     * older attempt whose catch runs after the click. */
    private candidateMutationRetryEpoch: object = Object.freeze({});
    private rootRepairRequired = false;
    /** Authenticated target retained when a cross-format replacement cannot
     * publish yet. Null means rebuild in the wrapper's current format. */
    private rootRepairVersion: 1 | 2 | null = null;
    private rootRetirement: Promise<void> | null = null;
    private legacyDowngradeFreeze: LegacyDowngradeFreezeState | null = null;
    private legacyDowngradeFreezeWork: Promise<EngineLegacyDowngradeLease> | null = null;
    private legacyDowngradeAuthorityToken: object | null = null;
    private pushDrain: Promise<void> | null = null;
    private rootReviewedQueue: RootReviewedQueue | null = null;
    /** A preempted whole review remains owed across drain boundaries. A
     * separately reviewed recent prefix may improve first latency, but a
     * failed or deferred prefix cannot let continuous edits starve the audit. */
    private rootInitialReviewPreempted = false;
    private rootReviewLifecycleEpoch: number | null = null;
    private visibilityResumeUnsubscribe: (() => void) | null = null;
    private lifecycleResume: Promise<void> | null = null;
    private lifecycleResumeRequested = false;
    /** The notify WS and the mobile lifecycle gate observe the same host
     * foreground transition independently. Fence the bulk data carrier once
     * per mobile epoch without coalescing separate desktop notify resumes. */
    private dataTransportResumeEpoch: number | null = null;
    private fullScanLifecyclePending = false;
    private metadataScanLifecyclePending = false;
    private pullLifecycleEpoch: object = Object.freeze({});
    /** A sliced selection consumed static plan positions. Keep serving the
     * remaining reviewed plan and rebuild once it is exhausted. */
    private rootSliceNeedsReviewRebuild = false;
    private rootSliceUrgentTurn = false;
    private rootReviewFence: { base: ReturnType<ObsetyncSyncBase["captureTreeEntries"]> } | null = null;
    private rootPendingReason: string | null = null;
    private rootBatchSummary: { selected: number; cuts: number; metadataBytes: number; held: RootBatchDeferrals } | null = null;
    /** Finite same-renderer hint cleanup proof if retirement completed on disk
     * but returned an error. Process death has no surviving volatile hints. */
    private rootAttemptCut: { epoch: string; cuts: Array<{ path: string; throughId: number }> } | null = null;

    constructor(
        private app: App,
        private api: ObsetyncApi,
        private io: PlatformIO,
        private syncBase: ObsetyncSyncBase,
        private journal: ObsetyncJournal,
        private wasm: WasmModule,
        private tree: WasmTree,
        private vaultId: string,
        private syncInterval: number = 30000,
        private syncPriority: SyncPriority = "sequential",
        private onStatusUpdate: (text: string) => void = () => {},
        initialRootHash: string | null = null,
        private syncObsidianConfig: boolean = false,
        /** Human device name — stamped into conflict-copy filenames. */
        private deviceName: string = "device",
        /** Ph2 notify channel: server pushes "root changed" over WebSocket;
         *  polling drops to a slow safety-net cadence while it's alive. */
        private realtimeWs: boolean = true,
        /** Ph3: broadcast which file this device is looking at (receiving
         *  presence always works; this only gates SENDING ours). */
        private sharePresence: boolean = true,
        /** Slice 2: gitignore-style patterns for paths that never sync. */
        ignorePatterns: string[] = [],
        private operationCheckpoint?: OperationCheckpoint,
        private autoSync: boolean = true,
        private hashWorkers: DesktopHashWorkerPool | null = null,
        private visibilityGate?: ResourceVisibilityGate,
        private preparedTransfers?: PreparedTransferRuntime,
        rootIdentity?: RootSyncIdentity,
        private rootTreeResidentAdmission?: RootTreeResidentAdmission,
        private browserHash?: BrowserHashWorkerRuntime | null,
    ) {
        this.localRootHash = initialRootHash;
        this.ignore = compileIgnore(ignorePatterns);
        if (rootIdentity) this.rootRuntime = new RootSyncRuntime(rootIdentity, app.vault.adapter,
            api, io, wasm, syncBase, journal, () => this.waitForHeavyWork("conflict preservation"));
        this.visibilityResumeUnsubscribe = visibilityGate?.onResume(() => this.resumeMobileLifecycle()) ?? null;
    }

    /** Migration/semantic harnesses deliberately invoke the real callback on
     * an object restored without running the constructor. Priority provenance
     * is volatile, so lazy initialization is safe and keeps that path honest. */
    private recentLocalUpsertOwner(): RecentLocalUpserts {
        return this.recentLocalUpserts ??=
            new RecentLocalUpserts(ROOT_REVIEW_LIMITS.overlayPaths);
    }

    /** Compiled ignore matcher (Slice 2). Empty ⇒ nothing ignored. */
    private ignore: CompiledIgnore;

    /** The WS notify channel (null when disabled or before start()). */
    private wsChannel: ObsetyncWsChannel | null = null;
    /** Epoch-ms of the last completed pull — drives the slow-poll decision. */
    private lastPullDoneMs = 0;
    /** Ph3 presence: device(short) → latest update from the fleet. */
    private presence = new Map<string, PresenceUpdate & { ts: number }>();
    /** Path this device currently has open (what we advertise). */
    private myOpenFile: string | null = null;
    private presenceHeartbeat: number | null = null;
    private workspaceRefs: any[] = [];
    /** Throttle "X is editing this file" notices: `${device}:${file}` → ts. */
    private busyNoticeShown = new Map<string, number>();

    getState(): SyncState {
        return this.state;
    }

    // --- Debug accessors (used by the "Show debug info" panel) ----------------

    /** Hex root hash the client considers current for this vault.
     *  Prefers the engine's tracked `localRootHash` (seeded from the cached
     *  root file at startup and advanced only by an adopted pull or published
     *  push) over the WASM tree's in-memory hash. An observed but deferred
     *  pull head lives in `lastPullServerRoot` and is deliberately excluded. */
    getLocalRootHash(): string | null {
        if (this.localRootHash) return this.localRootHash;
        if (this.stopped && this.operationWorkTracker?.snapshot().drained) return null;
        try {
            const h = this.tree.root_hash_hex();
            return h && h.length > 0 ? h : null;
        } catch {
            return null;
        }
    }

    /** Count of files currently tracked in sync-base. */
    getSyncBaseCount(): number {
        try { return this.syncBase.entryCount(); } catch { return -1; }
    }

    /** Count of vault files Obsidian's cache reports right now (excluding .obsidian/). */
    getVaultFileCount(): number {
        try { return this.io.statBulk().size; } catch { return -1; }
    }

    /** Epoch-ms of the last successful push. 0 if never. */
    getLastSyncTimestamp(): number {
        try { return this.syncBase.lastSyncTimestamp; } catch { return 0; }
    }

    /** Most recent network/sync failure observed. */
    getLastError(): { ts: number; message: string; origin: string } | null {
        return this.lastError;
    }

    getLastRepairSummary(): ReconcileSummary | null {
        const summary = this.lastRepairSummary;
        return summary ? { ...summary, result: summary.result ? { ...summary.result } : null } : null;
    }

    /** Reconcile/replay can own the engine without changing the public phase label. */
    isBusy(): boolean {
        return this.startupReplayInProgress || this.syncing || this.state !== "idle" ||
            (this.operationWorkTracker?.snapshot().active ?? 0) > 0 ||
            (this.callbackWorkTracker?.snapshot().active ?? 0) > 0;
    }

    /** Aggregate diagnostic only; lifecycle admission/join uses the trackers,
     * never a status string or this snapshot as an ownership substitute. */
    getLifecycleSnapshot() {
        return {
            stopped: !!this.stopped,
            operations: this.operationWorkTracker?.snapshot() ?? { closed: false, active: 0, drained: false },
            callbacks: this.callbackWorkTracker?.snapshot() ?? { closed: false, active: 0, drained: false },
            captureHandedOff: !!this.captureHandedOff,
        };
    }

    /** Last remote root hash observed via pullRemote (for hash mismatch diagnosis). */
    getLastObservedServerRoot(): string | null {
        return this.lastPullServerRoot;
    }

    /** The verified base root pushes descend from (honest putRoot parent). */
    getTreeBaseRoot(): string | null {
        return this.treeBaseRoot;
    }

    /** The WASM tree's actual current root — NOT the observed server root. */
    getTreeRootHash(): string | null {
        // Main may free the tree after this exact boundary while capture is
        // still live. A stopped-but-undrained accepted ACK still needs its tree.
        if (this.stopped && this.operationWorkTracker?.snapshot().drained) return null;
        try {
            const h = this.tree.root_hash_hex();
            return h && h.length > 0 ? h : null;
        } catch {
            return null;
        }
    }

    /** File count inside the WASM tree (compare against sync-base count). */
    getTreeFileCount(): number {
        if (this.stopped && this.operationWorkTracker?.snapshot().drained) return -1;
        try { return this.tree.root_hash_hex() ? this.tree.total_files() : -1; } catch { return -1; }
    }

    isPushBlocked(): boolean {
        return this.pushBlocked;
    }

    isReenrollmentRequired(): boolean {
        return this.reenrollmentRequired;
    }

    isBulkChangeReviewRequired(): boolean {
        return this.bulkChangeReviewRequired;
    }

    /** Queued path count includes cooled/dependent changes. An in-flight
     * detached snapshot is separately represented by the pushing state. */
    getPendingChangeCount(): number {
        return this.pendingChanges.size;
    }

    getDeferredChangeSummary(): DeferredChangeSummary {
        return this.deferredChanges.summary();
    }

    /** Fixed-shape, path/credential-free diagnostic snapshot. An equal old
     * tree root does not settle a prepared root or its local repair tail. */
    getRootSyncStatus() {
        const state = this.rootRuntime?.snapshot();
        const batch = this.rootBatchSummary;
        return { enabled: !!this.rootRuntime, loaded: state?.loaded ?? false,
            pending: state?.pending ?? false, recoveryRequired: !!this.rootRepairRequired,
            recovering: !!this.rootRecoveryWork, reason: this.rootPendingReason ?? null,
            lastBatch: batch ? { selected: batch.selected, cuts: batch.cuts, metadataBytes: batch.metadataBytes,
                held: Object.fromEntries(Object.entries(batch.held).map(([reason, counts]) => [reason, { ...counts }])) } : null };
    }

    hasPendingRootWork(): boolean {
        return !!this.rootRecoveryWork || !!this.rootRepairRequired || !!this.rootRuntime?.snapshot().pending;
    }

    getBulkChangeApproval(): {
        approvedAt: number;
        observedChanges: number;
        changeLimit: number;
        trackedDeletionLimit: number;
        active: boolean;
    } | null {
        const approval = this.syncBase.bulkChangeApproval;
        return approval && approval.vaultId === this.vaultId
            ? {
                approvedAt: approval.approvedAt,
                observedChanges: approval.observedChanges,
                changeLimit: approval.changeLimit,
                trackedDeletionLimit: approval.trackedDeletionLimit,
                active: this.bulkChangeApprovalActive,
            }
            : null;
    }

    /** Capture/replay is independent of negotiation; it never opens network gates. */
    prepareLocal(): Promise<void> {
        if (this.stopped) return Promise.resolve();
        if (this.localPreparation) return this.localPreparation;
        if (this.localPrepared) return Promise.resolve();
        this.localPreparation = this.trackOperation(async () => {
            try {
                if (this.persistenceReload) await this.persistenceReload;
                if (this.persistenceReloadRequired) throw new Error("persistence recovery must complete before local preparation");
                if (!this.stopped) await this.prepareLocalInner();
            } finally { this.localPreparation = null; }
        }, undefined);
        return this.localPreparation;
    }

    /** Recovery after the predecessor's operation/callback cut. The new local
     * capture remains installed; no apply/scan/publish gate opens during load.
     * Store promotion is itself owned, so another replacement must join it. */
    reloadPersistence(): Promise<void> {
        if (this.stopped) return Promise.resolve();
        if (this.persistenceReload) return this.persistenceReload;
        if (!this.startupReplayInProgress || this.localPrepared || this.localPreparation ||
            this.startup || this.startupCompleted || this.operationWork().snapshot().active) {
            return Promise.reject(new Error("persistence reload requires an inactive startup-gated engine"));
        }
        this.persistenceReload = this.trackOperation(async () => {
            this.persistenceReloadRequired = true;
            try {
                await this.syncBase.load();
                if (this.stopped) return;
                await this.journal.load();
                if (!this.stopped) this.persistenceReloadRequired = false;
            } finally { this.persistenceReload = null; }
        }, undefined);
        return this.persistenceReload;
    }

    private async prepareLocalInner(): Promise<void> {
        this.startupReplayInProgress = true;
        let recoveryOrigin = "journal-replay";
        // Restore the verified base root persisted in lockstep with sync-base.
        // Null on first run and on pre-1.4.0 sync-base files — established by
        // the first verified pull below.
        this.treeBaseRoot = this.syncBase.treeBaseRoot;
        if (this.treeBaseRoot) {
            console.log(`[obsetync] tree base root: ${this.treeBaseRoot.slice(0, 16)}`);
        }

        try {
            // Retry replay without installing duplicate listeners. An attach
            // failure can be partial; remove only those registrations first.
            if (!this.listenersAttached) {
                console.log("[obsetync] step 1: attach vault listeners");
                try {
                    this.attachVaultListeners();
                    this.listenersAttached = true;
                } catch (error) {
                    this.autoPushCoalescer?.close();
                    this.autoPushCoalescer = null;
                    this.debouncedPush = null;
                    for (const ref of this.eventRefs) this.app.vault.offref(ref);
                    this.eventRefs = [];
                    throw error;
                }
            }
            console.log("[obsetync] step 2: restore local journal hints");
            await this.recoverFromJournal();
            throwIfWorkAborted(this.hashWorkerAbort.signal);
            recoveryOrigin = "prepared-plan";
            // Corrupt/incomplete preparation cannot become a fresh empty plan.
            // Local capture is already installed while bounded storage replays.
            await this.preparedTransfers?.plan.load();
            recoveryOrigin = "object-confirmations";
            await this.preparedTransfers?.confirmations?.load();
            recoveryOrigin = "root-intent";
            await this.rootRuntime?.load();
            if (this.rootRuntime) this.rootRepairRequired = this.rootRuntime.lastSequence > 0;
            throwIfWorkAborted(this.hashWorkerAbort.signal);
            if (this.stopped) return;
            this.localPrepared = true;
            if (this.lastError?.origin === "journal-replay" || this.lastError?.origin === "prepared-plan" ||
                this.lastError?.origin === "object-confirmations" ||
                this.lastError?.origin === "root-intent") this.lastError = null;
        } catch (error) {
            this.lastError = { ts: Date.now(), origin: recoveryOrigin, message: String((error as Error)?.message ?? error) };
            throw error;
        }
    }

    isStopped(): boolean { return this.stopped; }

    /** Activate only after the plugin has selected the negotiated tree format.
     * Concurrent/repeated starts share work; stop is terminal for this instance. */
    start(): Promise<void> {
        if (this.stopped || this.startupCompleted) return Promise.resolve();
        if (this.startup) return this.startup;
        this.startup = this.trackOperation(() => this.startInner().then(() => {
            if (!this.stopped && !this.reenrollmentRequired) this.startupCompleted = true;
        }, error => {
            this.startupReplayInProgress = true;
            this.stopRuntime();
            throw error;
        }).finally(() => { this.startup = null; }), undefined);
        return this.startup;
    }

    private async startInner(): Promise<void> {
        await this.prepareLocal();
        if (this.stopped) return;
        if (this.preparedTransfers) {
            const scope = await this.preparedTransfers.scopeForTree(this.tree.tree_version());
            throwIfWorkAborted(this.hashWorkerAbort.signal);
            if (this.stopped) return;
            this.preparedScopeHash = scope;
        }
        console.log("[obsetync] starting sync engine");
        await this.recoverRootBeforeWork();
        if (this.stopped) return;
        this.startupReplayInProgress = false;

        // Start the public health probe for diagnostics without delaying the
        // guarded startup pull.
        // Startup can return early on stop while this already-issued request
        // is still pending. Its engine owner remains until the real completion.
        const connectivity = this.runTracked(this.operationWork(), () => this.api.ping()).then(
            result => ({ result }), error => ({ error }),
        );

        // Startup sequence (D-005):
        // Local hints and the plugin's negotiated tree format are now ready.
        console.log("[obsetync] step 3: pull remote");
        await this.pullRemote();
        if (this.reenrollmentRequired || this.stopped) {
            console.warn("[obsetync] startup paused until this device is re-enrolled");
            return;
        }

        // Publishing remains a separate guarded operation after pull.
        console.log("[obsetync] step 4: publish recovered changes");
        if (this.pendingChanges.size > 0) await this.pushPending();
        if (this.reenrollmentRequired || this.stopped) return;

        // Partial metadata scan (Layer 3).
        console.log("[obsetync] step 5: metadata scan");
        await this.partialMtimeScan();

        try {
            const checked = await connectivity;
            if ("error" in checked) throw checked.error;
            const conn = checked.result;
            console.log(
                `[obsetync] ${conn.ok ? "✓ reachable" : "✗ unreachable"} at ${conn.serverUrl} | ${conn.transport}`
            );
        } catch (e) {
            console.warn("[obsetync] ✗ server unreachable:", e);
        }
        if (this.reenrollmentRequired || this.stopped) return;

        // Start periodic pull timer. While the WS notify channel is live,
        // frames trigger pulls within seconds and the timer degrades to a
        // slow safety net (4× the interval); the moment the socket drops,
        // full-cadence polling resumes automatically.
        console.log("[obsetync] ready");
        this.syncTimer = window.setInterval(() => {
            const wsLive = this.wsChannel?.isConnected() ?? false;
            if (wsLive && Date.now() - this.lastPullDoneMs < this.syncInterval * 4 - 500) {
                return; // notify channel owns the fast path right now
            }
            this.pullRemote().catch((e) =>
                console.error("[obsetync] periodic pull error:", e)
            );
        }, this.syncInterval);

        // Notify channel (Ph2) + presence (Ph3): "root changed" frames →
        // immediate pull; presence frames → fleet awareness map.
        if (this.realtimeWs) {
            this.wsChannel = new ObsetyncWsChannel(
                this.api,
                this.vaultId,
                () => {
                    this.pullRemote().catch((e) =>
                        console.error("[obsetync] ws-triggered pull error:", e)
                    );
                },
                (p) => this.handlePresence(p),
                () => {
                    // The WS channel has synchronously distrusted its
                    // pre-suspend session. Pull over the sealed HTTP safety
                    // path now; periodic polling also remains unsuppressed
                    // until a fresh WS `ready` proves the new session live.
                    this.invalidateDataTransportForForegroundResume();
                    this.pullRemote().catch((e) =>
                        console.error("[obsetync] foreground recovery pull error:", e)
                    );
                },
            );
            this.wsChannel.start();

            // Advertise which file we're looking at: on every active-leaf
            // change + a periodic refresh so the server-side TTL (90s)
            // doesn't expire us mid-edit.
            if (this.sharePresence) {
                this.workspaceRefs.push(
                    this.app.workspace.on("active-leaf-change", () => {
                        this.advertisePresence();
                    }),
                );
                this.presenceHeartbeat = window.setInterval(
                    () => this.advertisePresence(),
                    45_000,
                );
            }
        }
    }

    /** Send our current open file to the fleet (Ph3). */
    private advertisePresence(): void {
        if (this.stopped || !this.sharePresence || !this.wsChannel?.isConnected()) return;
        const file = this.app.workspace.getActiveFile()?.path ?? null;
        this.myOpenFile = file;
        this.wsChannel.sendPresence(file, file ? "active" : "idle");
    }

    /** Fold a fleet presence update into the map; nudge the user if someone
     *  else is actively in the file we currently have open. */
    private handlePresence(p: PresenceUpdate): void {
        if (this.stopped) return;
        if (p.state === "offline") {
            this.presence.delete(p.device);
        } else {
            this.presence.set(p.device, { ...p, ts: Date.now() });
        }

        if (
            p.state === "active" &&
            p.file &&
            this.myOpenFile &&
            p.file === this.myOpenFile
        ) {
            const key = `${p.device}:${p.file}`;
            const last = this.busyNoticeShown.get(key) ?? 0;
            if (Date.now() - last > 5 * 60_000) {
                this.busyNoticeShown.set(key, Date.now());
                new Notice(`Obsetync: ${p.name} is editing this file right now.`, 8000);
            }
        }
    }

    /** Live fleet presence (for the debug panel / status bar), stale-swept. */
    getPresence(): Array<PresenceUpdate & { ts: number }> {
        const now = Date.now();
        for (const [k, v] of this.presence) {
            if (now - v.ts > 120_000) this.presence.delete(k);
        }
        return [...this.presence.values()];
    }

    /** How many OTHER devices are active right now. */
    getActivePeerCount(): number {
        return this.getPresence().filter((p) => p.state === "active").length;
    }

    /** Terminal exclusive cut for the explicit 1.11.3 downgrade. Root intent
     * authority remains loaded until the returned lease is retired. */
    freezeForLegacyDowngrade(): Promise<EngineLegacyDowngradeLease> {
        if (this.legacyDowngradeFreezeWork) {
            if (this.legacyDowngradeFreeze?.revoked || this.legacyDowngradeFreeze?.released) {
                return Promise.reject(new Error("legacy downgrade authority is retired"));
            }
            return this.legacyDowngradeFreezeWork;
        }
        if (this.stopped) return Promise.reject(new Error("legacy downgrade requires a live engine"));
        const runtime = this.rootRuntime;
        const root = runtime?.snapshot();
        if (!runtime || !root?.loaded || root.closed) {
            return Promise.reject(new Error("legacy downgrade requires loaded root authority"));
        }
        let version: number;
        try { version = this.tree.tree_version(); }
        catch { return Promise.reject(new Error("legacy downgrade tree version is unavailable")); }
        if (version !== 1) return Promise.reject(new Error("legacy downgrade requires tree format v1"));
        const activeOperations = this.operationWorkTracker?.snapshot().active ?? 0;
        if (activeOperations === 0 && (root.pending || this.rootRepairRequired || this.rootRecoveryWork)) {
            return Promise.reject(new Error("legacy downgrade requires settled root authority"));
        }

        // No await/event-loop boundary precedes this complete admission cut.
        const operationDrain = this.operationWork().closeAndDrain();
        const callbackDrain = this.callbackWork().closeAndDrain();
        const rootDrain = runtime.quiesce();
        const state: LegacyDowngradeFreezeState = {
            token: Object.freeze({}), runtime, treeVersion: 1,
            operationDrain, callbackDrain, rootDrain,
            revoked: false, released: false, granted: false, retirement: null,
        };
        this.legacyDowngradeFreeze = state;
        this.legacyDowngradeAuthorityToken = state.token;
        this.visibilityResumeUnsubscribe?.();
        this.visibilityResumeUnsubscribe = null;
        this.stopped = true;
        this.hashWorkerAbort.abort();
        try { this.stopRuntime(); }
        catch (error) { console.warn("[obsetync] runtime stop failed:", error); }
        try { this.api.closeDataLane(); }
        catch (error) { console.warn("[obsetync] data lane close failed:", error); }
        this.detachVaultListeners();
        this.pullEchoes.clear();

        const assertHeld = () => {
            const operations = this.operationWork().snapshot();
            const callbacks = this.callbackWork().snapshot();
            const runtimeState = state.runtime.snapshot();
            if (this.legacyDowngradeFreeze !== state || this.legacyDowngradeAuthorityToken !== state.token ||
                state.revoked || state.released || !state.granted || !this.stopped ||
                !operations.drained || !callbacks.drained || this.listenersAttached || this.eventRefs.length !== 0 ||
                this.workspaceRefs.length !== 0 || this.syncTimer !== null || this.wsChannel !== null ||
                !runtimeState.loaded || !runtimeState.closed) {
                throw new Error("legacy downgrade authority changed");
            }
        };
        const work = Promise.all([operationDrain, callbackDrain, rootDrain]).then(() => {
            if (state.revoked || state.released || this.legacyDowngradeFreeze !== state) {
                throw new Error("legacy downgrade authority was revoked before acquisition");
            }
            const authority = runtime.captureDowngradeAuthority();
            if (this.tree.tree_version() !== state.treeVersion || authority.pending !== null ||
                this.rootRepairRequired || this.rootRecoveryWork) {
                throw new Error("legacy downgrade requires settled tree-v1 root authority");
            }
            state.granted = true;
            const rootIntents = Object.freeze({
                pending: () => { assertHeld(); return authority.pending; },
                get lastSequence() { assertHeld(); return authority.lastSequence; },
            });
            return Object.freeze({
                treeVersion: state.treeVersion,
                rootIntents,
                assertHeld,
                revoke: () => {
                    state.revoked = true;
                    if (this.legacyDowngradeAuthorityToken === state.token) this.legacyDowngradeAuthorityToken = null;
                },
                closeAndDrain: () => this.retireLegacyDowngradeFreeze(state),
            }) as EngineLegacyDowngradeLease;
        });
        this.legacyDowngradeFreezeWork = work;
        // A rejected acquisition has no external lease owner to dispose the
        // quiesced runtime, so retain and report its exact cleanup separately.
        void work.catch(() => this.retireLegacyDowngradeFreeze(state)).catch(error => {
            console.error("[obsetync] legacy downgrade freeze retirement failed:", error);
        });
        return work;
    }

    private retireLegacyDowngradeFreeze(state: LegacyDowngradeFreezeState): Promise<void> {
        if (state.retirement) return state.retirement;
        state.released = true;
        if (this.legacyDowngradeAuthorityToken === state.token) this.legacyDowngradeAuthorityToken = null;
        state.retirement = Promise.all([state.operationDrain, state.callbackDrain, state.rootDrain]).then(async () => {
            this.rootRetirement ??= state.runtime.dispose();
            await this.rootRetirement;
        });
        return state.retirement;
    }

    /** Close only runtime admission; edits stay captured throughout a slow
     * accepted HTTP transaction or replacement WASM/capability preparation. */
    quiesceAndDrain(): Promise<void> {
        const downgrade = this.legacyDowngradeFreeze;
        if (downgrade) {
            downgrade.revoked = true;
            return this.retireLegacyDowngradeFreeze(downgrade);
        }
        this.visibilityResumeUnsubscribe?.();
        this.visibilityResumeUnsubscribe = null;
        const drained = this.operationWork().closeAndDrain();
        const rootDrain = this.rootRuntime?.quiesce();
        if (!this.stopped) {
            this.stopped = true;
            this.hashWorkerAbort.abort();
            try { this.stopRuntime(); }
            catch (error) { console.warn("[obsetync] runtime stop failed:", error); }
            try { this.api.closeDataLane(); }
            catch (error) { console.warn("[obsetync] data lane close failed:", error); }
        }
        if (!this.rootRuntime) return drained;
        return this.rootRetirement ??= Promise.all([drained, rootDrain]).then(() => this.rootRuntime!.dispose());
    }

    /** Synchronous terminal admission/capture stop; admitted work is never
     * declared finished here. Use stopAndDrain before replacing shared stores. */
    stop(): void {
        void this.quiesceAndDrain();
        void this.callbackWork().closeAndDrain();
        this.detachVaultListeners();
        this.pullEchoes.clear();
    }

    stopAndDrain(): Promise<void> {
        this.stop();
        const downgradeRetirement = this.legacyDowngradeFreeze?.retirement;
        return this.terminalDrain ??= Promise.all([
            this.operationWork().closeAndDrain(), this.callbackWork().closeAndDrain(),
            downgradeRetirement, this.rootRetirement,
        ]).then(() => undefined);
    }

    /** Synchronous capture cut followed by a finite old-callback join. Local
     * hints, uncertain rename links and overlapping path generations belong to
     * the same physical vault, not a remote policy/tree/root generation.
     * Preconditions throw synchronously, before the caller can publish next. */
    handoffCaptureTo(next: ObsetyncSyncEngine): Promise<void> {
        if (this.captureHandedOff) throw new Error("capture ownership was already handed off");
        if (next === this || this.app !== next.app || this.syncBase !== next.syncBase || this.journal !== next.journal) {
            throw new Error("capture handoff requires the same physical vault and stores");
        }
        if (!this.operationWork().snapshot().drained) throw new Error("capture handoff requires drained operations");
        if (this.callbackWork().snapshot().closed) throw new Error("capture handoff requires live callback admission");
        const nextOperations = next.operationWork().snapshot();
        const nextCallbacks = next.callbackWork().snapshot();
        if (next.stopped || next.listenersAttached || next.eventRefs.length || next.localPrepared ||
            next.localPreparation || next.persistenceReload || next.startup || next.startupCompleted || nextOperations.closed ||
            nextOperations.active || nextCallbacks.closed || nextCallbacks.active ||
            next.pendingChanges.size || next.localEventsInFlight.size || next.recentLocalUpsertOwner().size || next.captureHandedOff) {
            throw new Error("capture handoff requires a pristine replacement engine");
        }
        // This inherited owner is registered BEFORE the new engine is exposed.
        // A second replacement/unload must also join the first old callback cut.
        const releaseIncoming = next.operationWork().enter();
        // Registration can synchronously dispatch a callback and then fail.
        // In that case next is never published, so old must carry its finite
        // borrowed callback cut into a later retry/terminal drain itself.
        const releaseRegistrationBridge = this.callbackWork().enter();
        next.pendingChanges = this.pendingChanges;
        next.deferredChanges = this.deferredChanges;
        next.localEventsInFlight = this.localEventsInFlight;
        next.recentLocalUpserts = this.recentLocalUpsertOwner();
        next.rootInitialReviewPreempted = this.rootInitialReviewPreempted;
        next.rootAttemptCut = this.rootAttemptCut;
        try {
            // Install first so a registration failure leaves old capture live.
            // There is no await/event-loop gap between installing and detaching.
            next.attachVaultListeners();
            next.listenersAttached = true;
        } catch (error) {
            next.detachVaultListeners();
            // A partially registered callback may already have entered. Keep
            // that ownership inherited until its real append/classification ends.
            void next.callbackWork().closeAndDrain().then(() => {
                releaseRegistrationBridge();
                releaseIncoming();
            });
            throw error;
        }
        this.captureHandedOff = true;
        releaseRegistrationBridge();
        const callbacksDrained = this.callbackWork().closeAndDrain();
        this.detachVaultListeners();
        // Delayed adapter echoes become conservative dirty hints. Never carry
        // old apply authority across the new engine's policy/tree activation.
        this.pullEchoes.clear();
        return callbacksDrained.finally(releaseIncoming);
    }

    private operationWork(): EngineWorkTracker {
        return this.operationWorkTracker ??= new EngineWorkTracker();
    }
    private callbackWork(): EngineWorkTracker {
        return this.callbackWorkTracker ??= new EngineWorkTracker();
    }
    private runTracked<T>(tracker: EngineWorkTracker, operation: () => Promise<T>): Promise<T> {
        const release = tracker.enter();
        try { return Promise.resolve(operation()).finally(release); }
        catch (error) { release(); return Promise.reject(error); }
    }
    private trackOperation<T>(operation: () => Promise<T>, inactive: T): Promise<T> {
        const tracker = this.operationWork();
        if (this.stopped || tracker.snapshot().closed) return Promise.resolve(inactive);
        return this.runTracked(tracker, operation);
    }
    private trackLocalEvent(operation: () => Promise<void>): Promise<void> {
        const tracker = this.callbackWork();
        if (tracker.snapshot().closed) return Promise.resolve();
        return this.runTracked(tracker, operation);
    }
    private detachVaultListeners(): void {
        this.autoPushCoalescer?.close();
        this.autoPushCoalescer = null;
        this.debouncedPush = null;
        const remaining = [];
        for (const ref of this.eventRefs) {
            try { this.app.vault.offref(ref); }
            catch (error) { remaining.push(ref); console.warn("[obsetync] vault listener detach failed:", error); }
        }
        this.eventRefs = remaining;
        this.listenersAttached = false;
    }

    /** Runtime activation may fail/retry; local capture remains installed. */
    private stopRuntime(): void {
        if (this.syncTimer) {
            window.clearInterval(this.syncTimer);
            this.syncTimer = null;
        }
        this.wsChannel?.stop();
        this.wsChannel = null;
        if (this.presenceHeartbeat !== null) {
            window.clearInterval(this.presenceHeartbeat);
            this.presenceHeartbeat = null;
        }
        for (const ref of this.workspaceRefs) {
            this.app.workspace.offref(ref);
        }
        this.workspaceRefs = [];
    }

    /** WS notify-channel state for the debug panel / status box. */
    getWsState(): WsState {
        return this.wsChannel?.getState() ?? "off";
    }

    /** ms since the last WS frame, -1 when never/off. */
    getWsLastFrameAgeMs(): number {
        return this.wsChannel?.lastFrameAgeMs() ?? -1;
    }

    /** Force a full sync cycle (pull → reconcile content → push pending).
     *
     * reconcileContent() is the missing piece that used to let the server and
     * client silently drift apart: sync-base said "everything's uploaded" but
     * the server had no content. We now verify, on every Sync Now, that the
     * server actually holds the content sync-base claims, and re-upload
     * anything missing. Cheap when the server is fully populated (one
     * checkContent call with N hashes), correct when it isn't. */
    forceSync(): Promise<void> {
        this.requestCandidateMutationRetry();
        this.rootOutputAdmissionRefusal = null;
        this.pushBootstrapAdmissionRefusal = null;
        this.pushBootstrapReportedRefusal = null;
        return this.trackOperation(() => this.forceSyncInner(), undefined);
    }

    private async forceSyncInner(): Promise<void> {
        if (this.startupReplayInProgress) {
            if (this.rootRuntime && this.localPrepared && !this.startup) await this.start();
            return;
        }
        if (this.reenrollmentRequired || this.stopped) {
            new Notice("Obsetync: re-enroll this device before syncing.", 10000);
            return;
        }
        if (this.bulkChangeReviewRequired) {
            new Notice(
                "Obsetync: automatic publishing is paused. Review ignores and run Full Rescan.",
                10000,
            );
            return;
        }
        // Another cycle already holds the engine (e.g. the startup
        // first-sync). Every sub-step below would silently yield to it and
        // forceSync would finish in ~1ms — reporting "complete" for work it
        // never did. Say what's actually happening instead.
        if (this.syncing) {
            console.log(
                `[obsetync] forceSync skipped: another sync in progress (state=${this.state})`
            );
            new Notice("Obsetync: sync already in progress — hang tight.");
            return;
        }
        const t0 = Date.now();
        console.log(
            `[obsetync] forceSync start: pending=${this.pendingChanges.size} ` +
            `localRoot=${this.localRootHash?.slice(0, 16) ?? "(none)"}`
        );
        const pullLifecycleEpoch = this.pullLifecycleEpoch;
        await this.pullRemote();
        if (this.pullLifecycleEpoch !== pullLifecycleEpoch) return;
        const t1 = Date.now();
        console.log(
            `[obsetync] forceSync: pull done in ${t1 - t0}ms, ` +
            `localRoot=${this.localRootHash?.slice(0, 16) ?? "(none)"}, ` +
            `pending=${this.pendingChanges.size}`
        );
        try {
            await this.reconcileContent();
        } catch (e: any) {
            throwIfWorkAborted(this.hashWorkerAbort.signal);
            if (e?.name === "AbortError" || e instanceof ResourceBudgetClosedError) throw e;
            console.error("[obsetync] reconcile error:", e);
            this.lastError = {
                ts: Date.now(),
                origin: "reconcile",
                message: String(e?.message ?? e),
            };
        }
        const t2 = Date.now();
        console.log(
            `[obsetync] forceSync: reconcile done in ${t2 - t1}ms, ` +
            `pending=${this.pendingChanges.size}`
        );
        await this.pushPending(false, true);
        if (this.state === "idle" && this.lastRepairSummary?.incomplete) {
            this.onStatusUpdate(this.pendingIdleStatus());
        }
        console.log(
            `[obsetync] forceSync end in ${Date.now() - t0}ms, ` +
            `pending=${this.pendingChanges.size}, ` +
            `localRoot=${this.localRootHash?.slice(0, 16) ?? "(none)"}`
        );
    }

    /**
     * Verify every file recorded in sync-base is actually present on the
     * server, upload whatever is missing. This exists because `sync-base` is
     * just a local cache of "what we believe the server has" — and the cache
     * can lie (server wiped, user restored from backup, migrated from TLS
     * server, etc.). Running it costs one `checkContent` for small files +
     * one `checkContentChunks` per large file the server already knows about,
     * plus real uploads for anything truly missing. O(1) network when the
     * server is fully populated; O(missing) otherwise.
     */
    reconcileContent(onProgress?: (msg: string) => void): Promise<ReconcileResult> {
        return this.trackOperation(() => this.reconcileContentInner(onProgress), {
            smallUploaded: 0, largeUploaded: 0, treeChunksUploaded: 0, bytes: 0,
            deferred: 0, drifted: 0, readErrors: 0, uploadErrors: 0,
        });
    }

    private async reconcileContentInner(onProgress?: (msg: string) => void): Promise<ReconcileResult> {
        const progress = onProgress ?? ((m: string) => this.onStatusUpdate(m));
        if (!this.syncing && !this.startupReplayInProgress) await this.recoverRootBeforeWork();

        // Guard against a concurrent push racing this — the coalescer fires
        // from live vault events and would otherwise share our WASM tree
        // handle while we bootstrap + inspect it.
        if (this.syncing || this.startupReplayInProgress) {
            console.log("[obsetync] reconcile skipped — another sync in progress");
            return { smallUploaded: 0, largeUploaded: 0, treeChunksUploaded: 0, bytes: 0,
                deferred: 0, drifted: 0, readErrors: 0, uploadErrors: 0 };
        }
        this.syncing = true;
        const perf = perfTrace.begin("reconcile");
        let perfOutcome: PerfOutcome = "success";
        const endSpan = perfSpan("sync.reconcile");
        let operationId: string | undefined;
        try {
            operationId = await this.operationCheckpoint?.begin(
                "reconcile",
                `${this.syncBase.entryCount()} tracked files`,
            );
            throwIfWorkAborted(this.hashWorkerAbort.signal);
            const result = await this._reconcileInner((message) => {
                progress(message);
                if (operationId) this.operationCheckpoint?.progress(operationId, message);
            }, perf);
            this.lastRepairSummary = { ts: Date.now(), incomplete: result.deferred > 0, result };
            if (result.deferred > 0) {
                perfOutcome = "error";
                this.lastError = { ts: Date.now(), origin: "reconcile",
                    message: `Content repair incomplete: ${result.deferred} deferred objects ` +
                        `(${result.drifted} drifted, ${result.readErrors} read errors, ${result.uploadErrors} upload errors)` };
            } else if (this.lastError?.origin === "reconcile") {
                this.lastError = null;
            }
            return result;
        } catch (error) {
            this.lastRepairSummary = { ts: Date.now(), incomplete: true, result: null };
            perfOutcome = this.stopped ? "cancelled" : "error";
            throw error;
        } finally {
            try {
                if (operationId) await this.operationCheckpoint?.complete(operationId);
            } finally {
                perf.finish(perfOutcome);
                endSpan();
                this.syncing = false;
            }
        }
    }

    private async _reconcileInner(
        progress: (msg: string) => void,
        perf?: PerfOperation,
    ): Promise<ReconcileResult> {
        // Populate an unexpectedly empty WASM tree, or finish a quarantined
        // repair, from sync-base so wasm_tree_chunk_hashes reflects the actual
        // index-chunk set the server should have. The replacement stays
        // private/admitted until validated; reconcile itself never publishes
        // a root.
        const endEnumerate = perf?.phase("enumerate");
        let contentIndex: ReturnType<typeof buildReconcileContentIndex>;
        try {
            const pendingRepair = this.rootRepairRequired ||
                hasPendingRootTreeRetirement(this.tree);
            if (pendingRepair ||
                (!this.tree.root_hash_hex() && this.syncBase.entryCount() > 0)) {
                const rebuildingTree = this.tree;
                const currentVersion = rebuildingTree.tree_version();
                if (currentVersion !== 1 && currentVersion !== 2) {
                    throw new Error(`local tree has unsupported version ${currentVersion}`);
                }
                const repairVersion: 1 | 2 = this.rootRepairVersion ?? currentVersion;
                try {
                    const rebuilt = await rebuildRootTreeFromBase(
                        this.syncBase,
                        rebuildingTree,
                        repairVersion,
                        this.hashWorkerAbort.signal,
                        {
                            cooperate: async (signal) => {
                                await yieldToUI({ signal });
                                if (signal === undefined) return;
                                await this.waitForHeavyWork("reconcile tree rebuild");
                            },
                            assertCurrent: () => {
                                if (this.tree !== rebuildingTree) {
                                    throw new Error("reconcile tree owner changed");
                                }
                                this.rootRuntime?.assertCurrentScope();
                            },
                            residentAdmission: this.rootTreeResidentAdmission,
                        },
                    );
                    this.treeBaseRoot = rebuilt.capturedBaseRoot;
                    this.localRootHash = rebuilt.capturedBaseRoot;
                    await this.saveCachedRoot();
                    this.rootRepairRequired = false;
                    this.rootRepairVersion = null;
                } catch (error) {
                    this.rootRepairRequired = true;
                    this.rootRepairVersion = repairVersion;
                    throw error;
                }
            }

            // --- Partition sync-base: small files (whole blobs) vs large (manifests+chunks).
            contentIndex = buildReconcileContentIndex(
                this.syncBase.allPaths().map((path) => {
                    const entry = this.syncBase.getEntry(path)!;
                    return { path, hash: entry.hash.toLowerCase(), size: entry.size };
                }),
                LARGE_FILE_THRESHOLD,
            );
            perf?.setWorkload({
                filesTotal: contentIndex.filesTotal,
                bytesTotal: contentIndex.bytesTotal,
            });
        } finally { endEnumerate?.(); }
        const { smallByHash, largeByHash } = contentIndex;

        const CHECK_BATCH = 1000;

        // --- Step 1: which tree chunks (index) is the server missing?
        const treeRootHash = this.tree.root_hash_hex();
        const treeFileCount = this.tree.total_files();
        const assertCommittedTreeCurrent = (): void => {
            throwIfWorkAborted(this.hashWorkerAbort.signal);
            if (this.tree.has_candidate() || this.tree.root_hash_hex() !== treeRootHash ||
                this.tree.total_files() !== treeFileCount) {
                throw new Error("reconcile tree changed during index repair");
            }
        };
        assertCommittedTreeCurrent();
        const treeHashes = this.wasm.wasm_tree_chunk_hashes(this.tree);
        perf?.setWasmChunks({ reachable: treeHashes.length });
        const endCheck = perf?.phase("check");
        let missingTreeChunks: string[] = [];

        // --- Step 2: which small-file contents is the server missing?
        const smallHashes = [...smallByHash.keys()];
        const missingSmall: string[] = [];
        const largeHashes = [...largeByHash.keys()];
        const missingLargeManifests: string[] = [];
        try {
            throwIfWorkAborted(this.hashWorkerAbort.signal);
            missingTreeChunks = treeHashes.length > 0
                ? validatedReconcileMissing(treeHashes, await this.api.checkChunks(treeHashes, perf))
                : [];
            for (let i = 0; i < smallHashes.length; i += CHECK_BATCH) {
                throwIfWorkAborted(this.hashWorkerAbort.signal);
                const batch = smallHashes.slice(i, i + CHECK_BATCH);
                const missing = await this.api.checkContent(batch, perf);
                missingSmall.push(...validatedReconcileMissing(batch, missing));
                progress(`reconcile: checked ${Math.min(i + CHECK_BATCH, smallHashes.length)}/${smallHashes.length}`);
            }

            // --- Step 3: which large-file manifests is the server missing?
            //
            // Before, we read + re-chunked + re-manifested every large file
            // unconditionally on every Sync Now. For a vault with big PDFs that
            // meant minutes of pointless disk reads and CPU — the "continuously
            // reuploading large files" symptom. The new bulk check lets us skip
            // straight past large files whose manifest is already on the server.
            for (let i = 0; i < largeHashes.length; i += CHECK_BATCH) {
                throwIfWorkAborted(this.hashWorkerAbort.signal);
                const batch = largeHashes.slice(i, i + CHECK_BATCH);
                const missing = await this.api.checkManifests(batch, perf);
                missingLargeManifests.push(...validatedReconcileMissing(batch, missing));
            }
            throwIfWorkAborted(this.hashWorkerAbort.signal);
        } finally { endCheck?.(); }

        const totalMissing =
            missingTreeChunks.length + missingSmall.length + missingLargeManifests.length;
        perf?.setWorkload({
            filesNeeded: missingSmall.length + missingLargeManifests.length,
        });

        console.log(
            `[obsetync] reconcile plan: ` +
            `tree-chunks ${treeHashes.length} checked / ${missingTreeChunks.length} missing, ` +
            `small ${smallHashes.length} checked / ${missingSmall.length} missing, ` +
            `large ${largeHashes.length} checked / ${missingLargeManifests.length} missing`
        );

        if (totalMissing === 0) {
            perf?.setWorkload({ bytesNeeded: 0 });
            progress("reconcile: server in parity");
            return { smallUploaded: 0, largeUploaded: 0, treeChunksUploaded: 0, bytes: 0,
                deferred: 0, drifted: 0, readErrors: 0, uploadErrors: 0 };
        }

        let smallUploaded = 0;
        let largeUploaded = 0;
        let treeChunksUploaded = 0;
        let bytes = 0;
        let contentBytesNeeded = sumIndexedContentBytes(missingSmall, smallByHash);
        let deferred = 0;
        let drifted = 0;
        let readErrors = 0;
        let uploadErrors = 0;
        const signal = this.hashWorkerAbort.signal;
        const addFailures = (stats: ReconcileRepairStats): void => {
            deferred += stats.deferred;
            drifted += stats.drifted;
            readErrors += stats.readErrors;
            uploadErrors += stats.uploadErrors;
        };

        // Copy each immutable index pack only after exact byte admission.
        // A failed index repair does not prevent independent content repair;
        // no root is published by this operation.
        const endTreeUpload = perf?.phase("tree_index_upload");
        try {
            await uploadIndexChunks({
                putObjects: async (records, operation, memory) => {
                    await this.api.putObjects(records, operation, memory);
                    treeChunksUploaded += records.length;
                    const transferred = records.reduce((sum, record) => sum + record.data.byteLength, 0);
                    bytes += transferred;
                    perf?.increment({ bytesTransferred: transferred });
                },
            }, this.wasm, this.tree, missingTreeChunks, perf,
            () => this.waitForHeavyWork("reconcile index upload"), signal,
            assertCommittedTreeCurrent);
        } catch (error) {
            throwIfWorkAborted(signal);
            if ((error as Error)?.name === "AbortError" || error instanceof ResourceBudgetClosedError) throw error;
            const incomplete = missingTreeChunks.length - treeChunksUploaded;
            deferred += incomplete;
            uploadErrors += incomplete;
        } finally { endTreeUpload?.(); }

        const small = await repairSmallContent(this.api, this.io, this.wasm,
            missingSmall.map(hash => ({ hash, ...smallByHash.get(hash)! })), {
                signal, perf,
                beforeHeavyBatch: () => this.waitForHeavyWork("reconcile content"),
                onProgress: (checked, stats) => {
                    const msg = `reconcile: ${checked}/${missingSmall.length} files · ${formatBytes(bytes + stats.bytes)} · ${stats.deferred} deferred`;
                    progress(msg);
                },
            });
        smallUploaded = small.uploaded;
        bytes += small.bytes;
        addFailures(small);

        for (let index = 0; index < missingLargeManifests.length; index++) {
            throwIfWorkAborted(signal);
            const hash = missingLargeManifests[index];
            const source = largeByHash.get(hash)!;
            const msg = `reconcile: large file ${index + 1}/${missingLargeManifests.length}`;
            progress(msg);
            const result = await repairLargeContent(this.api, this.io, this.wasm,
                { hash, ...source }, this.hashWorkers ?? undefined, {
                    signal, perf,
                    beforeHeavyBatch: () => this.waitForHeavyWork("large-file reconcile"),
                });
            largeUploaded += result.uploaded;
            bytes += result.bytes;
            contentBytesNeeded += result.neededBytes;
            addFailures(result);
        }

        const summary =
            `reconcile ${deferred ? "incomplete" : "done"}: ${smallUploaded} small, ${largeUploaded} large, ` +
            `${treeChunksUploaded} tree chunks, ${formatBytes(bytes)}; ${deferred} deferred ` +
            `(${drifted} drifted, ${readErrors} read errors, ${uploadErrors} upload errors)`;
        console.log(`[obsetync] ${summary}`);
        progress(summary);
        perf?.setWorkload({ bytesNeeded: contentBytesNeeded });
        return { smallUploaded, largeUploaded, treeChunksUploaded, bytes, deferred, drifted, readErrors, uploadErrors };
    }

    /** Force a full vault scan (Layer 4). Doubles as the recovery action for
     *  a diverged tree: the in-memory tree is rebuilt from sync-base (the
     *  state corresponding to treeBaseRoot) before scanning, so whatever
     *  in-memory drift caused a push block is discarded, the block lifted,
     *  and local differences re-queued from disk truth. */
    fullScan(): Promise<void> {
        this.requestCandidateMutationRetry();
        this.rootOutputAdmissionRefusal = null;
        this.pushBootstrapAdmissionRefusal = null;
        this.pushBootstrapReportedRefusal = null;
        return this.trackOperation(() => this.fullScanInner(), undefined);
    }

    private async fullScanInner(): Promise<void> {
        // A scan cannot substitute for an incomplete/failed durable replay.
        // Keep approval, checkpoints and the tree untouched until it succeeds.
        if (this.startupReplayInProgress) return;
        const pushWasBlocked = this.pushBlocked;
        const recoveredTreeAtEntry = Boolean(this.rootRuntime &&
            (this.rootRepairRequired || this.rootRecoveryWork ||
                hasPendingRootTreeRetirement(this.tree) || this.rootRuntime.pending() ||
                !this.rootRuntime.snapshot().loaded ||
                (this.getSyncBaseCount() > 0 && this.getTreeRootHash() === null)));
        if (!this.syncing) await this.recoverRootBeforeWork();
        if (this.reenrollmentRequired || this.stopped) {
            new Notice("Obsetync: re-enroll this device before running a rescan.", 10000);
            return;
        }
        if (this.syncing) {
            console.log(`[obsetync] full scan skipped: another sync is in progress (state=${this.state})`);
            new Notice("Obsetync: sync already in progress — retry the rescan when it finishes.");
            return;
        }
        // Full Rescan is the user's explicit confirmation after a bulk-change
        // safety stop. Restore the stop if the confirming scan itself fails.
        const bulkReviewWasRequired = this.bulkChangeReviewRequired;
        this.bulkChangeReviewRequired = false;
        // Own the tree for the complete scan. WS pulls and debounced pushes
        // must not mutate the same WASM handle between hash batches.
        this.syncing = true;
        this.state = "scanning";
        this.onStatusUpdate("sync ⟳");
        const perf = perfTrace.begin("scan");
        let perfOutcome: PerfOutcome = "success";
        console.log("[obsetync] full scan started");
        const endSpan = perfSpan("scan.full");
        let operationId: string | undefined;
        let scanFailed = false;
        let lifecycleQuiesced = false;
        let lifecycleWork: ResourceVisibilityWork | undefined;
        let workSignal = this.hashWorkerAbort.signal;

        try {
            operationId = await this.operationCheckpoint?.begin(
                "full-scan",
                `${this.getSyncBaseCount()} tracked files`,
            );
            await this.waitForHeavyWork("full scan", operationId);
            lifecycleWork = this.visibilityGate?.beginHeavyWork(this.hashWorkerAbort.signal);
            workSignal = lifecycleWork?.signal ?? this.hashWorkerAbort.signal;
            throwIfWorkAborted(workSignal);
            if (this.syncBase.clearDiffPageCheckpoint()) {
                // An operator-requested rebuild deliberately abandons any
                // interrupted snapshot cursor before deriving disk truth.
                await this.syncBase.checkpoint();
            }
            const endTree = perf.phase("tree_update");
            try {
                const currentVersion = this.tree.tree_version();
                if (currentVersion !== 1 && currentVersion !== 2) {
                    throw new Error(`local tree has unsupported version ${currentVersion}`);
                }
                const repairVersion: 1 | 2 = this.rootRepairVersion ?? currentVersion;
                if (this.rootRuntime && !recoveredTreeAtEntry) {
                    // The explicit rescan owns one complete base-derived
                    // replacement. Reuse root recovery so unresolved outcomes,
                    // V2 pre-output admission, refusal caching, old+new resident
                    // ownership and deferred retirement keep a single contract.
                    this.rootRepairRequired = true;
                    this.rootRepairVersion = repairVersion;
                    await this.recoverRootBeforeWork();
                } else if (!this.rootRuntime) {
                    // Compatibility engines without durable root outcomes still
                    // cross the bounded replacement helper instead of the old
                    // whole-vault JSON/direct-build callsite.
                    const rebuildingTree = this.tree;
                    try {
                        const rebuilt = await rebuildRootTreeFromBase(
                            this.syncBase,
                            rebuildingTree,
                            repairVersion,
                            workSignal,
                            {
                                cooperate: async (signal) => {
                                    await yieldToUI({ signal });
                                    if (signal === undefined) return;
                                    await this.waitForHeavyWork("full scan tree rebuild", operationId, workSignal);
                                },
                                assertCurrent: () => {
                                    if (this.tree !== rebuildingTree) {
                                        throw new Error("full scan tree owner changed");
                                    }
                                },
                                residentAdmission: this.rootTreeResidentAdmission,
                            },
                        );
                        this.treeBaseRoot = rebuilt.capturedBaseRoot;
                        this.localRootHash = rebuilt.capturedBaseRoot;
                        await this.saveCachedRoot();
                        this.rootRepairRequired = false;
                        this.rootRepairVersion = null;
                    } catch (error) {
                        this.rootRepairRequired = true;
                        this.rootRepairVersion = repairVersion;
                        throw error;
                    }
                }
            } finally {
                endTree();
            }

            // statBulk() reads all file stats from Obsidian's in-memory cache —
            // no async IPC calls, O(n) in-memory map construction.
            const endStat = perf.phase("stat");
            const statMap = this.io.statBulk();
            // Optionally include .obsidian/ — vault.getFiles() hides it by design.
            if (this.syncObsidianConfig) {
                const obsidianFiles = await this.io.listObsidianConfig();
                for (const [p, s] of obsidianFiles) statMap.set(p, s);
            }
            endStat();
            console.log(`[obsetync] full scan: ${statMap.size} files total`);

            // Phase 1: fast mtime+size filter (synchronous, no I/O).
            const endEnumerate = perf.phase("enumerate");
            const toHash: Array<{ path: string; stat: { mtime: number; size: number } }> = [];
            let visibleFiles = 0;
            let visibleBytes = 0;
            for (const [path, stat] of statMap) {
                if (this.isExcluded(path)) continue;
                visibleFiles++;
                visibleBytes += stat.size;
                const base = this.syncBase.getEntry(path);
                if (base && stat.mtime === base.mtime && stat.size === base.size) continue;
                toHash.push({ path, stat });
            }
            const deletedPaths = this.syncBase.allPaths().filter(
                (path) => !statMap.has(path) && !this.isExcluded(path),
            );
            endEnumerate();
            perf.setWorkload({ filesTotal: visibleFiles + deletedPaths.length, bytesTotal: visibleBytes });
            // Unchanged fingerprints are already resolved. Hashing progress
            // advances after each batch instead of remaining at zero until
            // the complete vault scan returns.
            perf.increment({ filesCompleted: visibleFiles - toHash.length });
            const plannedTotal = toHash.length + deletedPaths.length;
            console.log(
                `[obsetync] full scan: ${toHash.length} files need hashing, ` +
                `${deletedPaths.length} tracked deletions`,
            );

            // Persist the operator's approval BEFORE expensive hashing. If the
            // renderer/plugin restarts during this scan or its later push, the
            // next metadata audit may continue the same bounded plan without
            // asking for another click. A larger deletion plan cannot reuse it.
            if (
                bulkReviewWasRequired ||
                automaticChangeNeedsReview(
                    plannedTotal,
                    deletedPaths.length,
                    this.syncBase.entryCount(),
                )
            ) {
                // Journal recovery may already have queued paths before the
                // Full Rescan. Count it as a conservative upper bound; the
                // coalescing dirty set will usually make the real push smaller.
                const approvedTotal = Math.min(
                    Number.MAX_SAFE_INTEGER,
                    plannedTotal + this.pendingChanges.size,
                );
                const approval = this.syncBase.approveBulkChange(
                    this.vaultId,
                    approvedTotal,
                    deletedPaths.length,
                );
                await this.syncBase.checkpoint();
                this.bulkChangeApprovalActive = true;
                this.bulkChangeApprovalCompletesWithPush = true;
                console.log(
                    `[obsetync] persisted Full Rescan approval: ` +
                    `${approval.observedChanges} changes, ` +
                    `${approval.trackedDeletionLimit} tracked deletions`,
                );
            } else if (this.syncBase.bulkChangeApproval) {
                const resumed = await this.resumeBulkChangeApproval(
                    "full scan",
                    plannedTotal,
                    deletedPaths.length,
                );
                if (resumed) this.bulkChangeApprovalCompletesWithPush = true;
            }
            const hashableTotal = toHash.reduce(
                (count, item) => count + (item.stat.size < LARGE_FILE_THRESHOLD ? 1 : 0),
                0,
            );
            let hashOrdinal = 0;
            const hashSampleWeights = toHash.map((item) => {
                if (item.stat.size >= LARGE_FILE_THRESHOLD) return 0;
                return perfSampleWeight(hashOrdinal++, hashableTotal);
            });

            // Phase 2: read + hash in streaming batches. Only compact path
            // records accumulate; file bytes are released after each group.
            //
            // Key constraints:
            //   - Large files (≥ 1 MB) skip WASM hash entirely here. push.ts reads
            //     them during upload and gets file_hash from FastCDC. This keeps
            //     WASM linear memory bounded regardless of PDF/image sizes.
            //   - Platform read concurrency limits concurrent IPC reads. Large
            //     files are excluded above; mobile stays more conservative.
            //   - FLUSH_BATCH=500 periodically moves local arrays into the
            //     coalescing DirtyPathSet without touching the network/tree.
            //   - yieldToUI() every group lets Electron's audio/render callbacks run.
            const FLUSH_BATCH = 256;
            let pending: FileChange[] = [];
            let totalChanges = 0;
            let changedBytes = 0;
            let metadataRefreshes = 0;
            let metadataRefreshesSinceCheckpoint = 0;

            const flushPending = async () => {
                if (pending.length === 0) return;
                await this.queueScanChangesDurably(pending, workSignal);
                pending = [];
            };

            for (let i = 0; i < toHash.length;) {
                await this.waitForHeavyWork("full scan", operationId, workSignal);
                // Re-read after the visibility wait: short-window feedback may
                // change concurrency while this scan is still running.
                const batch = toHash.slice(i, i + getHashTuning().readConcurrency);
                perf.setDemand({ read: toHash.length - i, hash: toHash.length - i });
                perf.observePeakBatchBytes(batch.reduce((sum, item) => {
                    const residentBytes = this.io.getAbsolutePath(item.path)
                        ? Math.min(item.stat.size, getHashTuning().feedBytes)
                        : item.stat.size;
                    return sum + residentBytes;
                }, 0));
                const endBatch = perf.phase("scan_batch");
                const results = await joinStartedWork(
                    batch.map(async ({ path, stat }, batchIndex) => {
                        const base = this.syncBase.getEntry(path);
                        if (stat.size >= LARGE_FILE_THRESHOLD) {
                            // Skip WASM hash — push.ts will hash during upload.
                            // We know it changed because it passed the mtime+size filter.
                            return {
                                kind: "change" as const,
                                path,
                                stat,
                                hash: undefined as string | undefined,
                                base,
                            };
                        }
                        const itemIndex = i + batchIndex;
                        const sampleWeight = hashSampleWeights[itemIndex];
                        const stable = await this.hashStableFile(
                            path,
                            stat,
                            sampleWeight > 0 ? perf : undefined,
                            sampleWeight,
                            workSignal,
                        );
                        if (base && stable.hash === base.hash) {
                            return {
                                kind: "metadata" as const,
                                path,
                                stat: stable.stat,
                            };
                        }
                        return {
                            kind: "change" as const,
                            path,
                            stat: stable.stat,
                            hash: stable.hash,
                            base,
                        };
                    })
                ).finally(endBatch);

                for (const r of results) {
                    if (r.kind === "metadata") {
                        if (this.syncBase.refreshLocalMetadata(
                            r.path,
                            r.stat.mtime,
                            r.stat.size,
                        )) {
                            metadataRefreshes++;
                            metadataRefreshesSinceCheckpoint++;
                        }
                        continue;
                    }
                    const change: FileChange = {
                        action: r.base ? "modified" : "created",
                        path: r.path,
                        mtime: r.stat.mtime,
                        size: r.stat.size,
                    };
                    if (r.hash !== undefined) change.hash = r.hash;
                    pending.push(change);
                    totalChanges++;
                    changedBytes += r.stat.size;
                }
                perf.increment({ filesCompleted: batch.length });
                i += batch.length;
                perf.setDemand({});

                // Let Electron's audio/render callbacks run between every read group.
                await yieldToUI({ signal: workSignal, perf, lane: "maintenance" });

                // Tick every batch — the slow phase here is the HASHING, and
                // the old placement (inside the flush guard) meant vaults with
                // <500 changes showed "Scanning vault..." frozen to the end.
                const done = i;
                this.onStatusUpdate(`⟳ ${done}/${toHash.length}`);
                this.progressHeartbeat("fullScan", `${done}/${toHash.length} hashed, ${totalChanges} changed`);
                if (operationId) {
                    this.operationCheckpoint?.progress(
                        operationId,
                        `${done}/${toHash.length} hashed; ${totalChanges} changed`,
                    );
                }

                if (pending.length >= FLUSH_BATCH) {
                    await flushPending();
                }
                if (metadataRefreshesSinceCheckpoint >= FLUSH_BATCH) {
                    await this.syncBase.checkpoint();
                    metadataRefreshesSinceCheckpoint = 0;
                }
            }
            await flushPending();
            if (metadataRefreshesSinceCheckpoint > 0) {
                await this.syncBase.checkpoint();
            }

            // Phase 3: deletions — files in sync-base that no longer exist.
            for (const path of deletedPaths) {
                pending.push({ action: "deleted", path });
                totalChanges++;
            }
            await flushPending();
            perf.increment({ filesCompleted: deletedPaths.length });

            // Compact the bounded metadata checkpoints even when hashing found
            // no publishable content change. Otherwise a metadata-only rescan
            // leaves a large WAL and repeats the same stat drift on restart.
            if (metadataRefreshes > 0) await this.syncBase.save();

            perf.setWorkload({
                filesTotal: visibleFiles + deletedPaths.length,
                filesNeeded: totalChanges,
                bytesNeeded: changedBytes,
            });
            console.log(
                `[obsetync] full scan complete: ${totalChanges} changes, ` +
                `${metadataRefreshes} metadata-only entries refreshed`,
            );
            if (pushWasBlocked) {
                console.log("[obsetync] full scan completed after rebuilding the tree — push unblocked");
            }
            this.pushBlocked = false;
        } catch (error: any) {
            const lifecycleReason = workSignal.aborted &&
                workSignal.reason instanceof MobileLifecycleQuiesceError
                ? workSignal.reason : error instanceof MobileLifecycleQuiesceError ? error : null;
            lifecycleQuiesced = lifecycleReason !== null;
            scanFailed = !lifecycleQuiesced;
            perfOutcome = this.stopped || lifecycleQuiesced ? "cancelled" : "error";
            if (bulkReviewWasRequired) this.bulkChangeReviewRequired = true;
            if (pushWasBlocked) this.pushBlocked = true;
            if (lifecycleQuiesced) {
                this.fullScanLifecyclePending = true;
                this.onStatusUpdate("sync ⏸ mobile hidden");
                console.log(`[obsetync] full scan quiesced: ${lifecycleReason!.lifecycleReason}`);
                return;
            }
            this.lastError = {
                ts: Date.now(),
                origin: "full-scan",
                message: String(error?.message ?? error),
            };
            console.error("[obsetync] full scan failed:", error);
            throw error;
        } finally {
            try {
                if (operationId) await this.operationCheckpoint?.complete(operationId);
            } finally {
                lifecycleWork?.release();
                perf.finish(perfOutcome);
                endSpan();
                this.syncing = false;
                this.state = scanFailed ? "error" : "idle";
                if (!lifecycleQuiesced) this.onStatusUpdate(scanFailed ? "sync ✗" : this.pendingIdleStatus());
            }
        }

        // Publish once the scan has released exclusive ownership. push()
        // itself streams these path records in bounded file batches.
        if (this.pendingChanges.size > 0) {
            await this.pushPending(true, true);
        } else if (this.bulkChangeApprovalActive) {
            await this.completeBulkChangeApproval("scan found no content changes");
        }
    }

    // --- Private ---

    /**
     * iOS may suspend the renderer at any time once hidden. Stop only at a
     * bounded batch boundary: an already-issued durability request is allowed
     * to ACK, while the next read/hash/network allocation waits for visibility.
     */
    private async waitForHeavyWork(phase: string, operationId?: string, activeSignal?: AbortSignal): Promise<void> {
        throwIfWorkAborted(activeSignal ?? this.hashWorkerAbort.signal);
        this.rootRuntime?.assertCurrentScope();
        if (!this.visibilityGate?.isPaused()) return;
        // An owner from the epoch which just became hidden must unwind now;
        // only a future attempt may wait for the next visible epoch.
        if (activeSignal) throwIfWorkAborted(activeSignal);
        const detail = `paused while hidden before next ${phase} batch`;
        if (operationId) this.operationCheckpoint?.progress(operationId, detail);
        this.onStatusUpdate("sync ⏸");
        await this.visibilityGate.waitForHeavyWork(this.hashWorkerAbort.signal);
        if (this.stopped) throw new Error("sync engine stopped");
        this.rootRuntime?.assertCurrentScope();
    }

    /** One foreground cycle per mobile lifecycle epoch, including when the
     * realtime notify channel is disabled. Join the cancelled push tail first
     * so its dirty/journal restoration is visible to the new cycle. */
    private resumeMobileLifecycle(): void {
        if (this.stopped) return;
        // Synchronous invalidation is intentionally before every await and
        // before coalescing with an older resume tail: an apparently-open data
        // socket from this new host epoch must not remain observable.
        this.invalidateDataTransportForForegroundResume();
        if (this.lifecycleResume) {
            this.lifecycleResumeRequested = true;
            return;
        }
        this.lifecycleResumeRequested = false;
        const previousPush = this.pushDrain;
        const resume = Promise.resolve().then(async () => {
            await previousPush?.catch(() => {});
            await this.visibilityGate?.waitForQuiescence();
            if (this.stopped || this.startupReplayInProgress || this.visibilityGate?.isPaused()) return;
            if (this.fullScanLifecyclePending) {
                this.fullScanLifecyclePending = false;
                await this.fullScan();
                return;
            }
            if (this.metadataScanLifecyclePending) {
                this.metadataScanLifecyclePending = false;
                await this.partialMtimeScan();
                return;
            }
            await this.forceSync();
        }).catch(error => {
            if (!this.stopped) console.error("[obsetync] mobile foreground resume error:", error);
        }).finally(() => {
            if (this.lifecycleResume !== resume) return;
            this.lifecycleResume = null;
            if (!this.stopped && !this.visibilityGate?.isPaused() &&
                (this.lifecycleResumeRequested || this.fullScanLifecyclePending || this.metadataScanLifecyclePending)) {
                this.resumeMobileLifecycle();
            }
        });
        this.lifecycleResume = resume;
    }

    private invalidateDataTransportForForegroundResume(): void {
        if (this.stopped) return;
        const visibility = this.visibilityGate?.snapshot();
        if (visibility?.runtime === "mobile") {
            // The notify WS listener may run before the main visibility
            // listener advances the gate. A paused epoch N and its following
            // visible epoch N+1 therefore name the same foreground transition.
            const foregroundEpoch = visibility.epoch + (visibility.paused ? 1 : 0);
            if (this.dataTransportResumeEpoch === foregroundEpoch) return;
            this.dataTransportResumeEpoch = foregroundEpoch;
        }
        this.api.invalidateDataTransportAfterResume();
    }

    /** Prepared sources are isolated by Merkle format. A live authenticated
     * format transition must publish the new scope before later pushes can
     * consult or retain prepared hints. */
    private async refreshPreparedScopeForTree(tree: WasmTree, version: 1 | 2): Promise<void> {
        if (!this.preparedTransfers) return;
        const scope = await this.preparedTransfers.scopeForTree(version);
        throwIfWorkAborted(this.hashWorkerAbort.signal);
        if (this.tree !== tree || tree.tree_version() !== version) {
            throw new Error("prepared transfer tree format changed while deriving scope");
        }
        this.rootRuntime?.assertCurrentScope();
        this.preparedScopeHash = scope;
    }

    /** Prepare only the local baseline for this push. It deliberately does
     * not advance an honest server parent, localRootHash or cached root: those
     * become durable only after root acceptance. */
    private async prepareCommittedTreeForPush(operationId?: string): Promise<void> {
        const rebuildingTree = this.tree;
        if (rebuildingTree.root_hash_hex() && !hasPendingRootTreeRetirement(rebuildingTree)) {
            this.pushBootstrapAdmissionRefusal = null;
            this.pushBootstrapReportedRefusal = null;
            return;
        }
        const currentVersion = rebuildingTree.tree_version();
        if (currentVersion !== 1 && currentVersion !== 2) {
            throw new Error(`local tree has unsupported version ${currentVersion}`);
        }
        const repairVersion: 1 | 2 = currentVersion;
        const admission = this.rootTreeResidentAdmission;
        const refusal = this.pushBootstrapAdmissionRefusal;
        if (refusal && admission) {
            const ledger = admission.snapshot().ledger;
            if (refusal.tree === rebuildingTree && refusal.base.isCurrent() &&
                refusal.capacityBytes === ledger.capacityBytes && refusal.usedBytes === ledger.usedBytes &&
                refusal.activeLeases === ledger.activeLeases && refusal.closed === ledger.closed &&
                refusal.repairVersion === repairVersion && refusal.treeVersion === rebuildingTree.tree_version() &&
                refusal.committedRevision === rebuildingTree.committed_revision?.() &&
                refusal.candidateRevision === rebuildingTree.candidate_revision?.() &&
                refusal.rootHash === rebuildingTree.root_hash_hex() &&
                refusal.totalFiles === rebuildingTree.total_files()) {
                throw refusal.error;
            }
            this.pushBootstrapAdmissionRefusal = null;
            this.pushBootstrapReportedRefusal = null;
        }
        let witness: Omit<RootTreeAdmissionRefusal, "error"> | null = null;
        if (admission) {
            const ledger = admission.snapshot().ledger;
            witness = { tree: rebuildingTree, base: this.syncBase.captureTreeEntries(),
                capacityBytes: ledger.capacityBytes, usedBytes: ledger.usedBytes,
                activeLeases: ledger.activeLeases, closed: ledger.closed,
                repairVersion, treeVersion: rebuildingTree.tree_version(),
                committedRevision: rebuildingTree.committed_revision?.(),
                candidateRevision: rebuildingTree.candidate_revision?.(),
                rootHash: rebuildingTree.root_hash_hex(), totalFiles: rebuildingTree.total_files() };
        }
        try {
            await rebuildRootTreeFromBase(
                this.syncBase,
                rebuildingTree,
                repairVersion,
                this.hashWorkerAbort.signal,
                {
                    cooperate: async (signal) => {
                        await yieldToUI({ signal });
                        if (signal === undefined) return;
                        await this.waitForHeavyWork("push tree bootstrap", operationId);
                    },
                    assertCurrent: () => {
                        if (this.tree !== rebuildingTree) {
                            throw new Error("push tree owner changed");
                        }
                        this.rootRuntime?.assertCurrentScope();
                        this.rootReviewedQueue?.assertApplicable();
                    },
                    residentAdmission: admission,
                },
            );
            if (!rebuildingTree.root_hash_hex() || rebuildingTree.has_candidate()) {
                throw new Error("push tree bootstrap returned no committed root");
            }
            this.pushBootstrapAdmissionRefusal = null;
            this.pushBootstrapReportedRefusal = null;
        } catch (error) {
            if (error instanceof RootTreeOutputAdmissionDeniedError && admission && witness) {
                const ledger = admission.snapshot().ledger;
                const unchanged = witness.tree === this.tree && witness.base.isCurrent() &&
                    witness.capacityBytes === ledger.capacityBytes && witness.usedBytes === ledger.usedBytes &&
                    witness.activeLeases === ledger.activeLeases && witness.closed === ledger.closed &&
                    witness.repairVersion === rebuildingTree.tree_version() &&
                    witness.treeVersion === rebuildingTree.tree_version() &&
                    witness.committedRevision === rebuildingTree.committed_revision?.() &&
                    witness.candidateRevision === rebuildingTree.candidate_revision?.() &&
                    witness.rootHash === rebuildingTree.root_hash_hex() &&
                    witness.totalFiles === rebuildingTree.total_files() &&
                    error.capacityBytes === ledger.capacityBytes && error.usedBytes === ledger.usedBytes;
                this.pushBootstrapAdmissionRefusal = unchanged ? { ...witness, error } : null;
                if (!unchanged) this.pushBootstrapReportedRefusal = null;
            } else {
                this.pushBootstrapAdmissionRefusal = null;
                this.pushBootstrapReportedRefusal = null;
            }
            throw error;
        }
    }

    /** Capture remains live, but no pull/scan/new root can cross an unresolved
     * historical outcome. Rebuild from the CURRENT validated base, never the
     * historical candidate payload over a newer locally applied state. */
    private recoverRootBeforeWork(): Promise<void> {
        const runtime = this.rootRuntime;
        if (!runtime || this.stopped) return Promise.resolve();
        if (this.rootRecoveryWork) return this.rootRecoveryWork;
        const hasCandidateCleanup = () => hasPendingTreeCandidateMutationRetirement(this.tree) ||
            hasPendingTreeReachabilityRetirement(this.tree);
        const canSkipRecovery = () => {
            if (this.rootRepairRequired || hasPendingRootTreeRetirement(this.tree) ||
                !runtime.snapshot().loaded || runtime.pending()) return false;
            runtime.assertCurrentScope();
            // A fresh/legacy-upgraded device can have a fully validated
            // sync-base while its volatile WASM wrapper is still empty and
            // the durable root stream has sequence zero. Do not let that
            // state fall through to the synchronous push/pull bootstrap: the
            // first resident graph must cross the same pre-output admission
            // and deferred ownership boundary as every later replacement.
            return this.syncBase.entryCount() === 0 || Boolean(this.tree.root_hash_hex());
        };
        if (!hasCandidateCleanup() && canSkipRecovery()) return Promise.resolve();
        let admissionRefusalWitness: Omit<NonNullable<typeof this.rootOutputAdmissionRefusal>, "error"> | null = null;
        const recoveryTree = this.tree;
        this.rootRecoveryWork = Promise.resolve().then(async () => {
            // A failed pull/push helper may have returned while retaining the
            // wrapper's one native job slot and an exact candidate-abort debt.
            // Every ordinary entry funnels through this owner, so cleanup must
            // precede persistence reload, refusal probes and replacement work.
            await drainTreeCandidateMutationRetirement(recoveryTree);
            await drainTreeReachabilityRetirement(recoveryTree);
            if (this.tree !== recoveryTree) throw new Error("root recovery tree owner changed during cleanup");
            throwIfWorkAborted(this.hashWorkerAbort.signal);
            runtime.assertCurrentScope();
            if (canSkipRecovery()) return;
            const refusal = this.rootOutputAdmissionRefusal;
            const admission = this.rootTreeResidentAdmission;
            if (refusal && admission) {
                const ledger = admission.snapshot().ledger;
                if (refusal.tree === this.tree && refusal.base.isCurrent() &&
                    refusal.capacityBytes === ledger.capacityBytes && refusal.usedBytes === ledger.usedBytes &&
                    refusal.activeLeases === ledger.activeLeases && refusal.closed === ledger.closed &&
                    refusal.repairVersion === (this.rootRepairVersion ?? this.tree.tree_version()) &&
                    refusal.treeVersion === this.tree.tree_version() &&
                    refusal.committedRevision === this.tree.committed_revision?.() &&
                    refusal.candidateRevision === this.tree.candidate_revision?.() &&
                    refusal.rootHash === this.tree.root_hash_hex() &&
                    refusal.totalFiles === this.tree.total_files()) {
                    // Do not reload/feed/sort/plan every interval while none
                    // of the facts which can change admission have moved.
                    throw refusal.error;
                }
                this.rootOutputAdmissionRefusal = null;
            }
            this.onStatusUpdate("sync ⟳ root recovery");
            // No engine mutation is active here. Queue-owned local callbacks
            // remain captured across reload; no uncertain writer is reused.
            await this.syncBase.load();
            await this.journal.load();
            await runtime.reload();
            throwIfWorkAborted(this.hashWorkerAbort.signal);
            const pending = runtime.pending();
            const retirement = pending ? this.pendingChanges.captureRetirement(pending.intent.journalCuts) : null;
            const result = await runtime.resolvePending();
            if (result.status !== "idle" && result.status !== "accepted" && result.status !== "cancelled") {
                throw new Error(`root outcome remains pending: ${result.status}`);
            }
            if (result.status === "accepted" && pending && retirement) {
                this.pendingChanges.commitRetirement(retirement);
                this.deferredChanges.commitAcknowledged(pending.intent.journalCuts);
            }
            if (!pending && this.rootAttemptCut?.epoch === this.journal.validatedEpoch) {
                // Absence in this validated journal epoch can retire only the
                // exact old proven hint, never a newer hint carrying its ID.
                const stillPending = this.journal.capturePendingPaths();
                const acknowledged = this.rootAttemptCut.cuts.filter(cut => !stillPending.has(cut.path));
                this.pendingChanges.commitRetirement(this.pendingChanges.captureRetirement(acknowledged));
                this.deferredChanges.commitAcknowledged(acknowledged);
            }
            await this.waitForHeavyWork("root recovery");
            const repairingTree = this.tree;
            const currentTreeVersion = repairingTree.tree_version();
            if (currentTreeVersion !== 1 && currentTreeVersion !== 2) {
                throw new Error(`local tree has unsupported version ${currentTreeVersion}`);
            }
            const repairVersion: 1 | 2 = this.rootRepairVersion ?? currentTreeVersion;
            if (admission) {
                const ledger = admission.snapshot().ledger;
                admissionRefusalWitness = { tree: repairingTree,
                    base: this.syncBase.captureTreeEntries(), capacityBytes: ledger.capacityBytes,
                    usedBytes: ledger.usedBytes, activeLeases: ledger.activeLeases,
                    closed: ledger.closed, repairVersion, treeVersion: repairingTree.tree_version(),
                    committedRevision: repairingTree.committed_revision?.(),
                    candidateRevision: repairingTree.candidate_revision?.(),
                    rootHash: repairingTree.root_hash_hex(), totalFiles: repairingTree.total_files() };
            }
            const repaired = await rebuildRootTreeFromBase(this.syncBase, repairingTree,
                repairVersion, this.hashWorkerAbort.signal, { assertCurrent: () => {
                    if (this.tree !== repairingTree) throw new Error("root recovery tree owner changed");
                    runtime.assertCurrentScope();
                }, cooperate: async (signal) => {
                    await yieldToUI({ signal, lane: "interactive", deadlineMs: 50 });
                    // Cleanup is mandatory after cancellation and must not
                    // re-enter the stopped/hidden engine gate whose owner it
                    // is retiring.
                    if (signal === undefined) return;
                    await this.waitForHeavyWork("root recovery");
                }, residentAdmission: admission });
            // This is intentionally unconditional. A prior replacement may
            // have published the target graph and then failed while deriving
            // its prepared-transfer scope; the retry must repair that tail
            // before clearing rootRepairRequired even though versions match.
            await this.refreshPreparedScopeForTree(repairingTree, repairVersion);
            this.treeBaseRoot = repaired.capturedBaseRoot;
            // A receipt's M is historical; the next ordinary pull observes the
            // actual server state from the persisted honest parent.
            this.localRootHash = repaired.capturedBaseRoot;
            this.pushBlocked = false;
            // A parentless repair can be a local initial-push graph whose
            // deferred native finish succeeded before retirement failed. It
            // is not remote authority yet: caching it would make a restart
            // treat an unaccepted hash as a known server root. The accepted
            // push tail persists the cache after its durable root settlement.
            if (repaired.capturedBaseRoot !== null) await this.saveCachedRoot();
            this.rootRepairRequired = false;
            this.rootRepairVersion = null;
            this.rootAttemptCut = null;
            this.rootPendingReason = null;
            this.rootOutputAdmissionRefusal = null;
            runtime.assertCurrentScope();
            if (this.lastError?.origin === "root-recovery") this.lastError = null;
        }).catch(error => {
            this.rootRepairRequired = true;
            const repeatedAdmissionRefusal = error instanceof RootTreeOutputAdmissionDeniedError &&
                this.rootOutputAdmissionRefusal?.error === error;
            if (error instanceof RootTreeOutputAdmissionDeniedError && this.rootTreeResidentAdmission) {
                if (!repeatedAdmissionRefusal) {
                    const ledger = this.rootTreeResidentAdmission.snapshot().ledger;
                    const witness = admissionRefusalWitness;
                    const unchanged = witness && witness.tree === this.tree && witness.base.isCurrent() &&
                        witness.capacityBytes === ledger.capacityBytes && witness.usedBytes === ledger.usedBytes &&
                        witness.activeLeases === ledger.activeLeases && witness.closed === ledger.closed &&
                        witness.repairVersion === (this.rootRepairVersion ?? this.tree.tree_version()) &&
                        witness.treeVersion === this.tree.tree_version() &&
                        witness.committedRevision === this.tree.committed_revision?.() &&
                        witness.candidateRevision === this.tree.candidate_revision?.() &&
                        witness.rootHash === this.tree.root_hash_hex() &&
                        witness.totalFiles === this.tree.total_files() &&
                        error.capacityBytes === ledger.capacityBytes && error.usedBytes === ledger.usedBytes;
                    this.rootOutputAdmissionRefusal = unchanged ? { ...witness, error } : null;
                }
            } else if (!(error instanceof RootTreeOutputAdmissionDeniedError)) {
                this.rootOutputAdmissionRefusal = null;
            }
            if (!repeatedAdmissionRefusal) this.recordSyncFailure("root-recovery", error);
            throw error;
        }).finally(() => { this.rootRecoveryWork = null; });
        return this.rootRecoveryWork;
    }

    private pullRemote(): Promise<void> {
        return this.trackOperation(() => this.pullRemoteInner(), undefined);
    }

    private async pullRemoteInner(): Promise<void> {
        if (this.syncing || this.startupReplayInProgress || this.reenrollmentRequired || this.stopped) return;
        // A prior failed begin/mutation owns the wrapper's single native job
        // slot and may carry an exact candidate-abort debt. Retire it before
        // recovery, capability checks, network reads, or vault/base writes.
        // These registries retain their original uncancellable cooperation;
        // a failure leaves the same owner quarantined for the next retry.
        const preparingTree = this.tree;
        await drainTreeCandidateMutationRetirement(preparingTree);
        await drainTreeReachabilityRetirement(preparingTree);
        if (this.tree !== preparingTree || this.stopped) return;
        await this.recoverRootBeforeWork();
        if (this.syncing || this.stopped) return;
        this.syncing = true;
        this.state = "pulling";
        this.onStatusUpdate("sync ↓");
        const perf = perfTrace.begin("pull");
        let perfOutcome: PerfOutcome = "success";
        const endSpan = perfSpan("sync.pull");
        let operationId: string | undefined;
        let pullTreeVersion: 1 | 2 | null = null;
        let editorGuard: EditorPullGuard | null = null;
        let lifecycleWork: ResourceVisibilityWork | undefined;
        let workSignal = this.hashWorkerAbort.signal;
        let lifecycleQuiesced = false;

        // Normal operation progress belongs in the stable status bar. Notices
        // are reserved for actionable or terminal events, never batch ticks.
        let pullFailed = false;
        const progress = (msg: string) => {
            if (this.stopped) throw new Error("sync engine stopped");
            this.onStatusUpdate(`↓ ${msg}`);
            this.progressHeartbeat("pull", msg);
            if (operationId) this.operationCheckpoint?.progress(operationId, msg);
        };

        try {
            operationId = await this.operationCheckpoint?.begin(
                "pull",
                `base=${(this.treeBaseRoot ?? this.localRootHash)?.slice(0, 16) ?? "none"}`,
            );
            await this.waitForHeavyWork("pull", operationId);
            lifecycleWork = this.visibilityGate?.beginHeavyWork(this.hashWorkerAbort.signal);
            workSignal = lifecycleWork?.signal ?? this.hashWorkerAbort.signal;
            throwIfWorkAborted(workSignal);
            // Keep even the native root probe under this operation's cleanup
            // boundary: a throwing getter must not strand `syncing=true`.
            // A root-less wrapper must not be populated by pull.ts's
            // synchronous compatibility bootstrap. Pull advances disk/base
            // first, then this owner publishes one admitted replacement.
            const pullingTree = this.tree;
            const treeWasEmpty = !pullingTree.root_hash_hex();
            const currentTreeVersion = pullingTree.tree_version();
            if (currentTreeVersion !== 1 && currentTreeVersion !== 2) {
                throw new Error(`local tree has unsupported version ${currentTreeVersion}`);
            }
            pullTreeVersion = currentTreeVersion;
            editorGuard = await EditorPullGuard.open(this.app, () => {
                throwIfWorkAborted(workSignal);
                if (this.tree !== pullingTree || this.stopped) {
                    throw new Error("pull editor guard scope changed");
                }
                this.rootRuntime?.assertCurrentScope();
            });
            const result = await pull(
                this.api,
                this.io,
                this.syncBase,
                this.vaultId,
                // Diff from the VERIFIED base when we have one — never from a
                // merely-observed root that may be ahead of our applied state.
                this.treeBaseRoot ?? this.localRootHash,
                this.wasm,
                treeWasEmpty ? null : pullingTree,
                progress,
                // Vault events for paths the pull itself writes are echoes,
                // not user edits — register them before apply starts.
                (writes) => this.pullEchoes.register(writes),
                // Editor safety: paths with UNSYNCED local edits keep their
                // disk bytes — the queued push + server merge reconcile them.
                // Without this, the startup order (pull → journal recovery)
                // could overwrite last session's edits before recovery reads
                // them.
                this.unsyncedLocalPaths(editorGuard),
                // Slice 2: never fetch ignored paths; untrack them if purged.
                (p) => this.isExcluded(p),
                perf,
                () => this.waitForHeavyWork("pull", operationId, workSignal),
                !treeWasEmpty && this.rootTreeResidentAdmission ? {
                    signal: workSignal,
                    residentAdmission: this.rootTreeResidentAdmission,
                    cooperate: async () => {
                        await this.waitForHeavyWork("pull tree rebase", operationId, workSignal);
                        await yieldToUI({ signal: workSignal });
                    },
                    cooperateRetirement: async () => {
                        await yieldToUI({ lane: "interactive", deadlineMs: 50 });
                    },
                    assertCurrent: () => {
                        if (this.tree !== pullingTree) throw new Error("pull tree owner changed during rebase");
                        this.rootRuntime?.assertCurrentScope();
                    },
                } : undefined,
            );
            let observedVersion: 1 | 2 | null = null;
            if (result.newRootBytes) {
                const value = this.wasm.wasm_root_version_from_bytes(result.newRootBytes);
                if (value !== 1 && value !== 2) {
                    throw new Error(`server root has unsupported Tree version ${value ?? "missing"}`);
                }
                observedVersion = value;
            }
            const previousVersion = pullTreeVersion;
            if (treeWasEmpty || result.requiresTreeRebuild === true ||
                (observedVersion !== null && previousVersion !== observedVersion)) {
                const repairingTree = this.tree;
                const repairVersion = observedVersion ?? previousVersion;
                this.rootRepairVersion = repairVersion;
                try {
                    await rebuildRootTreeFromBase(
                        this.syncBase,
                        repairingTree,
                        repairVersion,
                        workSignal,
                        {
                            cooperate: async (signal) => {
                                await yieldToUI({ signal });
                                // Deferred retirement deliberately clears the
                                // operation signal. It must keep draining after
                                // stop/scope replacement instead of re-entering
                                // the cancelled engine visibility gate.
                                if (signal === undefined) return;
                                await this.waitForHeavyWork("pull tree rebuild", operationId, workSignal);
                            },
                            assertCurrent: () => {
                                if (this.tree !== repairingTree) throw new Error("pull tree owner changed");
                                this.rootRuntime?.assertCurrentScope();
                            },
                            residentAdmission: this.rootTreeResidentAdmission,
                        },
                    );
                    if (repairVersion !== previousVersion) {
                        await this.refreshPreparedScopeForTree(repairingTree, repairVersion);
                    }
                } catch (error) {
                    // The applied sync-base remains the only source for the
                    // next attempt. Fence all ordinary work until recovery
                    // reconstructs this volatile graph; never fetch/publish
                    // another root from an empty or old-format wrapper.
                    this.rootRepairRequired = true;
                    throw error;
                }
                this.rootRepairVersion = null;
                result.treeParity = result.newRootHash
                    ? this.getTreeRootHash() === result.newRootHash
                    : null;
                console.log(
                    `[obsetync] rebuilt ${treeWasEmpty ? "initial" :
                        result.requiresTreeRebuild ? "checkpoint-recovered" : "transitioned"} local Merkle graph ` +
                    `as Tree v${repairVersion}`,
                );
            }
            if (observedVersion !== null) {
                this.api.observeTreeVersion(this.vaultId, observedVersion);
            }
            await this.settlePullResult(result);
        } catch (e: any) {
            pullFailed = true;
            const lifecycleReason = workSignal.aborted &&
                workSignal.reason instanceof MobileLifecycleQuiesceError
                ? workSignal.reason : e instanceof MobileLifecycleQuiesceError ? e : null;
            lifecycleQuiesced = lifecycleReason !== null;
            perfOutcome = this.stopped || lifecycleQuiesced ? "cancelled" : "error";
            if (e instanceof PullTreeRebaseError && pullTreeVersion !== null) {
                // Disk/sync-base may already contain the remote delta while
                // the committed WASM tree still represents the old merge
                // base. No parity/count fallback may adopt a root from this
                // state; all later ordinary work must first rebuild from the
                // current validated base through the admitted replacement.
                this.rootRepairRequired = true;
                this.rootRepairVersion = pullTreeVersion;
                this.rootOutputAdmissionRefusal = null;
            }
            if (lifecycleQuiesced) {
                this.pullLifecycleEpoch = Object.freeze({});
                this.onStatusUpdate("sync ⏸ mobile hidden");
                console.log(`[obsetync] pull quiesced: ${lifecycleReason!.lifecycleReason}`);
            } else {
                console.error("[obsetync] pull error:", e);
                if (!this.stopped) this.recordSyncFailure("pull", e);
            }
        } finally {
            editorGuard?.close();
            try {
                if (operationId) await this.operationCheckpoint?.complete(operationId);
            } finally {
                lifecycleWork?.release();
                perf.finish(perfOutcome);
                endSpan();
                this.syncing = false;
                if (lifecycleQuiesced && !this.stopped) this.state = "idle";
            }
            if (!pullFailed && !this.stopped) {
                this.state = "idle";
                this.onStatusUpdate(
                    this.pendingIdleStatus(),
                );
                this.lastPullDoneMs = Date.now();
            }
            // Drain user edits that arrived while we were syncing.
            if (
                !this.reenrollmentRequired &&
                !this.stopped &&
                !this.bulkChangeReviewRequired &&
                this.hasRunnablePendingChanges()
            ) {
                this.autoPushCoalescer?.trigger();
            }
        }
    }

    /** Every path whose newest bytes exist only locally: queued-but-unpushed
     *  changes plus unsynced journal entries. Pull must not overwrite these. */
    private unsyncedLocalPaths(editorGuard?: { has(path: string): boolean }): { has(path: string): boolean } {
        let durableAtPullStart: { has(path: string): boolean };
        try {
            // O(1) immutable index capture; no journal walk/path Set per pull.
            durableAtPullStart = this.journal.capturePendingPaths();
        } catch {
            // An incomplete durable view cannot authorize overwriting any path.
            // Current-session hints alone cannot protect older offline changes.
            return { has: () => true };
        }
        return {
            has: (path: string) => {
                this.rootRuntime?.assertCurrentScope();
                return durableAtPullStart.has(path) ||
                    this.pendingChanges.has(path) || this.localEventsInFlight.has(path) ||
                    editorGuard?.has(path) === true;
            },
        };
    }

    /** Rate-limited progress line into the console/debug ring buffer, so
     *  long operations leave a visible trail in the debug panel even when
     *  nobody is watching the status bar (at most one line per 5s). */
    private lastHeartbeatMs = 0;
    private progressHeartbeat(op: string, msg: string): void {
        const now = Date.now();
        if (now - this.lastHeartbeatMs >= 5000) {
            this.lastHeartbeatMs = now;
            console.log(`[obsetync] ${op} progress: ${msg}`);
        }
    }

    private recordSyncFailure(origin: string, error: unknown): void {
        const message = error instanceof Error ? error.message : String(error);
        this.lastError = { ts: Date.now(), origin, message };
        this.state = "error";

        if (isReenrollmentRequiredError(error)) {
            this.reenrollmentRequired = true;
            this.onStatusUpdate("sync ⚠ re-enroll");
            if (!this.reenrollmentNoticeShown) {
                this.reenrollmentNoticeShown = true;
                new Notice(
                    "Obsetync: this server requires fresh enrollment. " +
                    "Automatic sync is paused; reset enrollment and enter a new code.",
                    15000,
                );
            }
            return;
        }

        this.onStatusUpdate("sync ✗");
    }

    private requireBulkChangeReview(
        source: string,
        total: number,
        trackedDeletions: number,
    ): void {
        this.bulkChangeReviewRequired = true;
        this.onStatusUpdate("sync ⚠ review");
        console.warn(
            `[obsetync] automatic ${source} publish paused: ${total} changes, ` +
            `${trackedDeletions} tracked deletions; review ignores and run Full Rescan`,
        );
        const deletionSummary = trackedDeletions > 0
            ? `, including ${trackedDeletions.toLocaleString()} tracked deletions`
            : "";
        new Notice(
            `Obsetync paused ${total.toLocaleString()} automatic changes${deletionSummary}. ` +
            "Review ignore patterns, then run Full Rescan to confirm.",
            15000,
        );
    }

    /** Activate a prior explicit approval only when the current plan stays
     *  inside its vault/count/deletion envelope. A mismatched marker is
     *  discarded before presenting a fresh review request. */
    private async resumeBulkChangeApproval(
        source: string,
        total: number,
        trackedDeletions: number,
    ): Promise<boolean> {
        const approval = this.syncBase.bulkChangeApproval;
        if (!approval) return false;
        if (this.syncBase.bulkChangeApprovalCovers(
            this.vaultId,
            total,
            trackedDeletions,
        )) {
            this.bulkChangeApprovalActive = true;
            this.bulkChangeReviewRequired = false;
            console.log(
                `[obsetync] resuming approved ${source}: ${total} changes, ` +
                `${trackedDeletions} tracked deletions ` +
                `(approved ${approval.observedChanges}, limit ${approval.changeLimit})`,
            );
            return true;
        }

        console.warn(
            `[obsetync] saved bulk approval no longer covers ${source}: ` +
            `${total} changes/${trackedDeletions} tracked deletions vs ` +
            `limit ${approval.changeLimit}/${approval.trackedDeletionLimit}; ` +
            `requesting a fresh review`,
        );
        if (this.syncBase.clearBulkChangeApproval()) {
            await this.syncBase.checkpoint();
        }
        this.bulkChangeApprovalActive = false;
        this.bulkChangeApprovalCompletesWithPush = false;
        return false;
    }

    private async completeBulkChangeApproval(reason: string): Promise<void> {
        this.bulkChangeApprovalActive = false;
        this.bulkChangeApprovalCompletesWithPush = false;
        this.bulkChangeReviewRequired = false;
        if (!this.syncBase.clearBulkChangeApproval()) return;
        await this.syncBase.save();
        console.log(`[obsetync] approved bulk recovery complete: ${reason}`);
    }

    /**
     * Decide what the pull result means for the verified base (D2/D3 core).
     *
     * - Exact parity (tree root == server root): adopt as treeBaseRoot,
     *   persist root bytes, clear any block. The only unconditional advance.
     * - Parity failed after applying deltas that carried server mtimes: the
     *   tree diverged from what we just applied — BLOCK pushes until a full
     *   rescan; publishing from this tree could revert other devices.
     * - Parity failed against a pre-1.4.0 server (no mtimes on the wire):
     *   exactness is unreachable (leaf hashes cover mtime). Adopt only when
     *   tree and sync-base agree on file count; otherwise block.
     * - Nothing applied + no parity: keep the current base. Merging from an
     *   older base is always safe — advancing past unapplied content is not
     *   (that's how sync state used to outrun reality).
     */
    private async adoptPullResult(result: {
        newRootHash: string | null;
        newRootBytes: Uint8Array | null;
        applied: number;
        treeParity: boolean | null;
        deltasHadMtime: boolean;
        deferredCount: number;
        localDeferredCount: number;
        remoteOmissionCount: number;
        downloaded: number;
    }): Promise<void> {
        // Files this pull couldn't fetch, plus policy-omitted remote upserts,
        // leave the tree missing content the server has. Adopting the server root as our base now would let a
        // later fast-forward read those gaps as deletions and propagate them
        // (the 2026-07-13 failure mode). Hold the base where it is — merges
        // from an older base always preserve the other side's changes — and
        // let the deferred files retry on the next pull.
        if (result.deferredCount > 0 || result.remoteOmissionCount > 0) {
            const fetchDeferred = result.deferredCount - result.localDeferredCount;
            console.warn(
                `[obsetync] pull deferred ${result.deferredCount} file(s) ` +
                `(${result.localDeferredCount} locally edited, ${fetchDeferred} unfetched) — ` +
                `${result.remoteOmissionCount} remote upsert(s) omitted by policy — ` +
                `base root NOT advanced`,
            );
            return;
        }

        const adopt = async (hash: string) => {
            const clearedDiffCheckpoint = this.syncBase.clearDiffPageCheckpoint();
            const clearedBaseFence = this.syncBase.setVerifiedBaseRequired(false);
            let baseChanged = false;
            if (this.treeBaseRoot !== hash) {
                this.treeBaseRoot = hash;
                this.syncBase.setTreeBaseRoot(hash);
                baseChanged = true;
            }
            if (baseChanged || clearedDiffCheckpoint || clearedBaseFence) {
                await this.syncBase.save();
            }
            if (this.pushBlocked) {
                console.log("[obsetync] tree re-verified against server — push unblocked");
                this.pushBlocked = false;
            }
            if (result.newRootBytes) {
                try {
                    const path = ".obsidian/plugins/obsetync/cached-root.bin";
                    await this.app.vault.adapter.writeBinary(
                        path,
                        exactArrayBuffer(result.newRootBytes),
                    );
                } catch (e) {
                    console.warn("[obsetync] failed to save cached root after pull:", e);
                }
            }
        };

        if (result.treeParity === true && result.newRootHash) {
            await adopt(result.newRootHash);
            return;
        }

        if (result.treeParity === false && result.newRootHash) {
            const treeCount = this.getTreeFileCount();
            const baseCount = this.syncBase.entryCount();
            const countsAgree = treeCount >= 0 && treeCount === baseCount;

            // A tree that still doesn't match the server AFTER the pull
            // actually DOWNLOADED content is a genuine content divergence —
            // block. But when the pull downloaded NOTHING (every delta verified
            // against local disk), the content is provably identical and the
            // only difference is metadata — mtime, which leaf hashes cover.
            // That's benign: fall through to the count-agreement check and
            // adopt, instead of nagging "Force full rescan" on a false alarm
            // that no rescan can fix (the mtimes just drift again).
            if (result.applied > 0 && result.deltasHadMtime && result.downloaded > 0) {
                if (this.syncBase.clearDiffPageCheckpoint()) {
                    await this.syncBase.checkpoint();
                }
                this.pushBlocked = true;
                console.error(
                    `[obsetync] tree root ${this.getTreeRootHash()?.slice(0, 16)} != ` +
                    `server root ${result.newRootHash.slice(0, 16)} after verified rebase — ` +
                    `pushes blocked, run "Full Rescan" to recover`,
                );
                new Notice(
                    "Obsetync: local index diverged from server — sync paused. " +
                    "Run 'Force full rescan' in settings to recover.",
                    10000,
                );
                return;
            }
            if (result.applied > 0 && result.deltasHadMtime && result.downloaded === 0) {
                console.log(
                    `[obsetync] tree hash differs from server but the pull downloaded 0 bytes ` +
                    `(content verified identical) — metadata-only drift, adopting server root`,
                );
            }

            // Pre-1.4.0 server (deltas without mtimes) or metadata-only root
            // drift: exact parity is unattainable. Content-wise we HAVE
            // applied everything the server reported, so the observed root is
            // an honest base — but only while tree and sync-base agree.
            if (countsAgree) {
                if (!this.treeBaseRoot || result.applied > 0) {
                    console.warn(
                        `[obsetync] adopting server root ${result.newRootHash.slice(0, 16)} as base ` +
                        `without byte parity (server deltas carried no mtimes); ` +
                        `tree=${treeCount} sync-base=${baseCount}`,
                    );
                    await adopt(result.newRootHash);
                } else if (this.syncBase.clearDiffPageCheckpoint()) {
                    await this.syncBase.checkpoint();
                }
                return;
            }

            this.pushBlocked = true;
            if (this.syncBase.clearDiffPageCheckpoint()) {
                await this.syncBase.checkpoint();
            }
            console.error(
                `[obsetync] tree/sync-base divergence: tree=${treeCount} files, ` +
                `sync-base=${baseCount} — pushes blocked`,
            );
            new Notice(
                "Obsetync: local index inconsistent — sync paused. " +
                "Run 'Force full rescan' in settings to recover.",
                10000,
            );
            return;
        }

        // No verifiable parity means the fixed snapshot must be replayed on
        // the next pull; retaining a completed cursor would skip that work.
        if (this.syncBase.clearDiffPageCheckpoint()) {
            await this.syncBase.checkpoint();
        }
    }

    /** Record an observed server head without turning it into a verified diff
     * base. A deferred/failed-parity first pull must replay from its previous
     * root (often ZERO_ROOT), otherwise its missing files disappear from the
     * next diff. `lastPullServerRoot` remains the diagnostic/push-safety
     * observation; `localRootHash` advances only with the adopted base. */
    private async settlePullResult(result: PullResult): Promise<void> {
        if (result.newRootHash) this.lastPullServerRoot = result.newRootHash;
        await this.adoptPullResult(result);
        if (result.newRootHash && this.treeBaseRoot === result.newRootHash) {
            this.localRootHash = result.newRootHash;
        }
    }

    private requestCandidateMutationRetry(): void {
        // Allocate the new epoch before publishing either field. A manual
        // request that cannot allocate changes no suppression state.
        const epoch = Object.freeze({});
        this.candidateMutationAdmissionRefusal = null;
        this.candidateMutationRetryEpoch = epoch;
    }

    private captureCandidateMutationAttemptWitness(
        allowBulkChange: boolean,
        retryKey: string,
        retryEpoch: object,
        inputCount: number,
    ): CandidateMutationAttemptWitness | null {
        try {
            const tree = this.tree, admission = this.rootTreeResidentAdmission;
            const runtime = this.rootRuntime, journalEpoch = this.journal.validatedEpoch;
            if (!admission || !runtime || !journalEpoch || inputCount < 1 ||
                inputCount > DIRTY_CLAIM_LIMIT || tree.tree_version() !== 2 || tree.has_candidate() ||
                hasPendingTreeCandidateMutationRetirement(tree) ||
                hasPendingTreeReachabilityRetirement(tree)) return null;
            runtime.assertCurrentScope();
            const committedRevision = tree.committed_revision?.();
            const candidateRevision = tree.candidate_revision?.();
            if (!Number.isSafeInteger(committedRevision) || committedRevision! < 0 ||
                !Number.isSafeInteger(candidateRevision) || candidateRevision! < 0) return null;
            const admissionMutationEpoch = admission.mutationEpoch;
            if (admissionMutationEpoch === null) return null;
            return {
                tree, admission, runtime, journalEpoch, retryKey, retryEpoch, inputCount,
                base: this.syncBase.captureTreeEntries(), treeBaseRoot: this.treeBaseRoot,
                allowBulkChange, ignore: this.ignore, priority: this.syncPriority,
                syncConfig: this.syncObsidianConfig,
                dependencyRevision: this.deferredChanges.dependencyRevision,
                planningRevision: this.deferredChanges.planningRevision,
                treeVersion: 2, committedRevision, candidateRevision,
                rootHash: tree.root_hash_hex(), totalFiles: tree.total_files(),
                ledgerBefore: admission.snapshot(),
                admissionMutationEpoch,
            };
        } catch {
            return null;
        }
    }

    private currentCandidateMutationAdmissionRefusal(
        allowBulkChange: boolean,
        retryKey: string,
    ): CandidateMutationAdmissionRefusal | null {
        const refusal = this.candidateMutationAdmissionRefusal;
        if (!refusal) return null;
        let current = false;
        try {
            const tree = this.tree, admission = this.rootTreeResidentAdmission;
            current = refusal.retryEpoch === this.candidateMutationRetryEpoch &&
                refusal.tree === tree && refusal.admission === admission &&
                refusal.runtime === this.rootRuntime && refusal.allowBulkChange === allowBulkChange &&
                refusal.ignore === this.ignore && refusal.priority === this.syncPriority &&
                refusal.syncConfig === this.syncObsidianConfig && refusal.retryKey === retryKey &&
                refusal.treeBaseRoot === this.treeBaseRoot &&
                refusal.journalEpoch === this.journal.validatedEpoch &&
                refusal.dependencyRevision === this.deferredChanges.dependencyRevision &&
                refusal.planningRevision === this.deferredChanges.planningRevision &&
                refusal.base.isCurrent() && this.pendingChanges.isCurrentCapture(refusal.dirty) &&
                refusal.dirty.size === refusal.inputCount && refusal.dirty.size > 0 &&
                refusal.dirty.size <= DIRTY_CLAIM_LIMIT &&
                tree.tree_version() === 2 && refusal.treeVersion === 2 &&
                tree.committed_revision?.() === refusal.committedRevision &&
                tree.candidate_revision?.() === refusal.candidateRevision &&
                tree.root_hash_hex() === refusal.rootHash && tree.total_files() === refusal.totalFiles &&
                tree.has_candidate() === false &&
                !hasPendingTreeCandidateMutationRetirement(tree) &&
                !hasPendingTreeReachabilityRetirement(tree) && admission !== undefined &&
                admission.mutationEpoch === refusal.admissionMutationEpoch &&
                sameResidentAdmissionSnapshot(admission.snapshot(), refusal.ledger);
        } catch {
            current = false;
        }
        if (!current) {
            this.candidateMutationAdmissionRefusal = null;
            return null;
        }
        return refusal;
    }

    private rememberCandidateMutationAdmissionRefusal(
        error: CandidateMutationOutputAdmissionDeniedError,
        attempt: CandidateMutationAttemptWitness | null,
    ): void {
        try {
            if (!attempt) return;
            const tree = this.tree, admission = this.rootTreeResidentAdmission;
            const runtime = this.rootRuntime, journalEpoch = this.journal.validatedEpoch;
            const dirty = this.pendingChanges.capture();
            const cut = candidateMutationRefusalCut(error);
            const refusalAdmissionEpoch = candidateMutationRefusalAdmissionEpoch(error);
            if (attempt.retryEpoch !== this.candidateMutationRetryEpoch || !admission || !runtime ||
                !journalEpoch || dirty.size !== attempt.inputCount || dirty.size < 1 ||
                dirty.size > DIRTY_CLAIM_LIMIT || !cut || refusalAdmissionEpoch === null || cut.tree !== tree ||
                cut.mutationInputCount !== attempt.inputCount ||
                attempt.tree !== tree || attempt.admission !== admission || attempt.runtime !== runtime ||
                attempt.ignore !== this.ignore || attempt.priority !== this.syncPriority ||
                attempt.syncConfig !== this.syncObsidianConfig || attempt.retryKey !== this.deferredRetryKey() ||
                attempt.treeBaseRoot !== this.treeBaseRoot || attempt.journalEpoch !== journalEpoch ||
                attempt.dependencyRevision !== this.deferredChanges.dependencyRevision ||
                attempt.planningRevision !== this.deferredChanges.planningRevision || !attempt.base.isCurrent() ||
                tree.tree_version() !== 2 || tree.has_candidate() ||
                hasPendingTreeCandidateMutationRetirement(tree) ||
                hasPendingTreeReachabilityRetirement(tree)) return;
            runtime.assertCurrentScope();
            const committedRevision = tree.committed_revision?.();
            const candidateRevision = tree.candidate_revision?.();
            const ledger = admission.snapshot();
            if (committedRevision !== attempt.committedRevision ||
                candidateRevision !== cut.candidateRevision + 1 ||
                tree.root_hash_hex() !== attempt.rootHash || tree.total_files() !== attempt.totalFiles ||
                refusalAdmissionEpoch !== attempt.admissionMutationEpoch + 1 ||
                admission.mutationEpoch !== refusalAdmissionEpoch ||
                error.capacityBytes !== attempt.ledgerBefore.ledger.capacityBytes ||
                error.usedBytes !== attempt.ledgerBefore.ledger.usedBytes ||
                !residentAdmissionAfterOneRefusal(attempt.ledgerBefore, ledger)) return;
            this.candidateMutationAdmissionRefusal = {
                ...attempt,
                dirty,
                candidateRevision,
                ledger,
                admissionMutationEpoch: refusalAdmissionEpoch,
                error,
            };
        } catch {
            // Suppression is optional. Never replace the original admission
            // refusal with a witness/getter/allocation failure.
            this.candidateMutationAdmissionRefusal = null;
        }
    }

    private deferredRetryKey(): string {
        const tuning = getHashTuning();
        const workerMode = this.hashWorkers?.stats().wasmMode ??
            this.browserHash?.pool.stats().wasmMode ?? "unavailable";
        const mobileRange = this.io.mobileRangeCapability?.();
        const mobileRangeKey = mobileRange
            ? `${mobileRange.state}:${mobileRange.reason ?? "none"}:${mobileRange.maxRangeBytes}`
            : "absent";
        // Admission uses the shared capacity, which may be lower than the
        // profile's requested limit. Occupancy changes do not invalidate a
        // source-size cooldown; an actual capacity/feed/worker change does.
        return `${tuning.runtime}:${transientMemorySnapshot().capacityBytes}:${tuning.maxFeedBytes}:` +
            `${workerMode}:${mobileRangeKey}`;
    }

    private hasRunnablePendingChanges(): boolean {
        if (this.pendingChanges.size === 0) return false;
        // A read-only captured iterator preserves live entry identities and
        // stops at the first runnable hint. Eligibility checks between roots
        // must not copy and rebuild the entire dirty queue twice per batch.
        return this.deferredChanges.hasRunnable(
            this.pendingChanges.iterate(), this.deferredRetryKey(), Date.now(),
        );
    }

    private pendingIdleStatus(): string {
        if (this.bulkChangeReviewRequired) return "sync ⚠ review";
        if (this.hasPendingRootWork()) return "sync ⏸ root recovery";
        if (this.rootPendingReason && this.pendingChanges.size > 0) return `sync ⏸ ${this.rootPendingReason}`;
        if (this.lastRepairSummary?.incomplete) {
            const count = this.lastRepairSummary.result?.deferred;
            return count ? `sync ⚠ repair ${count}` : "sync ⚠ repair pending";
        }
        const deferred = this.deferredChanges.summary();
        if (deferred.count > 0) return `sync ⚠ ${deferred.count} pending`;
        // Startup pulls precede journal recovery; even there an unchanged
        // committed root is not proof that queued local edits were published.
        if (this.pendingChanges.size > 0 || this.journal.unsyncedCount() > 0) return "sync …";
        return "sync ✓";
    }

    private pushPending(allowBulkChange = false, retryDeferred = false): Promise<void> {
        if (this.pushDrain) return this.pushDrain;
        if (retryDeferred) this.requestCandidateMutationRetry();
        // A manual sync drains the ready queue even with auto-sync disabled.
        // Keep one owner across real host turns, not a chain of detached
        // three-second debounce callbacks. A non-progress/pending outcome ends
        // this cycle; only a later trigger may retry it through recovery.
        let firstAttempt = true;
        // Queue owners may be rebuilt after a structural/base fence changes.
        // Keep the source-drift continuation budget on this whole drain so a
        // rebuild cannot reset it into an unbounded foreground retry loop.
        const driftRetryBudget = { remaining: ROOT_REVIEW_LIMITS.overlayRefreshes };
        // Defer the first attempt until the owner/promise is installed, so a
        // reentrant status/host callback joins rather than starts another loop.
        this.pushDrain = this.trackOperation(() => Promise.resolve().then(() => runRootBatchDrain({
            attempt: () => {
                const retry = firstAttempt && retryDeferred;
                firstAttempt = false;
                return this.pushPendingInner(allowBulkChange, retry, driftRetryBudget);
            },
            shouldContinue: () => !this.stopped && !this.startupReplayInProgress &&
                !this.reenrollmentRequired && !this.bulkChangeReviewRequired &&
                !this.rootRepairRequired && !this.pushBlocked &&
                (this.rootReviewedQueue?.hasRunnableReviewed() || this.hasRunnablePendingChanges()),
            cooperate: async () => {
                const lane = this.pendingSchedulerLane();
                await yieldToUI({ signal: this.hashWorkerAbort.signal, lane,
                    deadlineMs: lane === "urgent-file" ? 250 : undefined });
                await this.waitForHeavyWork("root continuation");
            },
        })), undefined).finally(() => {
            this.disposeRootReview();
            this.pushDrain = null;
        });
        return this.pushDrain;
    }

    private disposeRootReview(): void {
        this.rootReviewedQueue?.dispose();
        this.rootReviewedQueue = null;
        this.rootReviewLifecycleEpoch = null;
        this.rootReviewFence = null;
        this.rootSliceNeedsReviewRebuild = false;
        this.rootSliceUrgentTurn = false;
    }

    /** Full safety accounting, also used when a bounded live overlay changes
     * the reviewed cut. A byte/stat hint never grants deletion approval. */
    private async reviewPendingCounts(total: number, trackedDeletions: number, allowBulkChange: boolean): Promise<boolean> {
        const needsReview = automaticChangeNeedsReview(total, trackedDeletions, this.syncBase.entryCount());
        const covered = this.syncBase.bulkChangeApprovalCovers(this.vaultId, total, trackedDeletions);
        if (allowBulkChange && covered) this.bulkChangeApprovalActive = true;
        if (!needsReview || (this.bulkChangeApprovalActive && covered)) return true;
        if (await this.resumeBulkChangeApproval("materialized pending batch", total, trackedDeletions)) return true;
        this.requireBulkChangeReview("pending recovery", total, trackedDeletions);
        return false;
    }

    private async prepareReviewedRoot(allowBulkChange: boolean, retryDeferred: boolean,
        signal: AbortSignal = this.hashWorkerAbort.signal, lifecycleEpoch?: number): Promise<RootReviewedSelection | null> {
        if (this.rootReviewedQueue && lifecycleEpoch !== undefined &&
            this.rootReviewLifecycleEpoch !== lifecycleEpoch) this.disposeRootReview();
        // One rebuild for an observed structural/base change, never an
        // unbounded retry loop under continuous writes or admission failure.
        for (let attempt = 0; attempt < 2; attempt++) {
            try {
                if (!this.rootReviewedQueue) {
                    const epoch = this.journal.validatedEpoch;
                    if (!epoch) throw new Error("root review lacks a validated journal epoch");
                    const ignore = this.ignore, priority = this.syncPriority, config = this.syncObsidianConfig;
                    const treeVersion = this.tree.tree_version();
                    const retryKey = this.deferredRetryKey();
                    const fence = { base: this.syncBase.captureTreeEntries() };
                    this.rootReviewFence = fence;
                    const ports: RootReviewedQueuePorts = {
                        dirty: this.pendingChanges, deferred: this.deferredChanges,
                        materialize: (hints, omissions, afterCooperate) => this.materializeDirtyChanges(hints,
                            (path, reason) => { omissions.push({ path, reason }); }, signal, afterCooperate),
                        review: (total, deletions) => this.reviewPendingCounts(total, deletions, allowBulkChange),
                        tracked: path => this.syncBase.getEntry(path) !== null,
                        sort: (changes, options) => this.sortReviewedChanges(changes, priority, options),
                        cooperate: async () => {
                            const lane = this.pendingSchedulerLane();
                            await yieldToUI({ signal, lane,
                                deadlineMs: lane === "urgent-file" ? 250 : undefined });
                            await this.waitForHeavyWork("root planning", undefined, signal);
                        },
                        assertCurrent: () => {
                            throwIfWorkAborted(signal);
                            this.rootRuntime!.assertCurrentScope();
                            // Rebuilding alone cannot rebind old IDs to a new
                            // WAL epoch; persistence recovery must own that.
                            if (this.journal.validatedEpoch !== epoch) throw new Error("root review journal epoch changed");
                            if (this.rootReviewFence !== fence || !fence.base.isCurrent() || this.ignore !== ignore ||
                                this.syncPriority !== priority || this.syncObsidianConfig !== config || this.deferredRetryKey() !== retryKey) {
                                throw new RootReviewInvalidated("base/policy/retry scope changed");
                            }
                            if (this.tree.tree_version() !== treeVersion) throw new RootReviewInvalidated("tree format changed");
                        },
                        activePath: () => this.app.workspace.getActiveFile?.()?.path ?? null,
                        upsertInFlight: path => this.localEventsInFlight.has(path),
                        recentUpsert: hint => this.recentLocalUpsertOwner().matches(hint),
                        recentUpsertPaths: () => this.recentLocalUpsertOwner().durablePaths(
                            this.app.workspace.getActiveFile?.()?.path ?? null),
                        preemptInitialReview: () => {
                            const total = this.pendingChanges.size;
                            const policyAllowsPrefix = !automaticChangeNeedsReview(
                                total, total, this.syncBase.entryCount()) ||
                                this.syncBase.bulkChangeApprovalCovers(this.vaultId, total, 0);
                            return !this.rootInitialReviewPreempted && policyAllowsPrefix &&
                                this.recentLocalUpsertOwner().hasDurablePending(this.pendingChanges.capture());
                        },
                        authorizeRecentPrefix: total =>
                            !automaticChangeNeedsReview(total, total, this.syncBase.entryCount()) ||
                            this.syncBase.bulkChangeApprovalCovers(this.vaultId, total, 0),
                        retryKey, retryDeferred, signal,
                        memoryArbiter: this.preparedTransfers?.memoryArbiter,
                    };
                    try {
                        this.rootReviewedQueue = await RootReviewedQueue.create(ports);
                        // Only a completed ordinary owner repays the audit
                        // debt. Prefix failure and drain disposal do not.
                        this.rootInitialReviewPreempted = false;
                    } catch (error) {
                        if (!(error instanceof RootReviewPreempted)) throw error;
                        this.rootInitialReviewPreempted = true;
                        this.rootReviewedQueue = await RootReviewedQueue.createRecentPrefix(ports, error);
                    }
                    this.rootReviewLifecycleEpoch = lifecycleEpoch ?? null;
                }
                const reviewed = await this.rootReviewedQueue.next();
                if (!reviewed && this.rootSliceNeedsReviewRebuild) {
                    this.disposeRootReview();
                    const lane = this.pendingSchedulerLane();
                    await yieldToUI({ signal, lane,
                        deadlineMs: lane === "urgent-file" ? 250 : undefined });
                    await this.waitForHeavyWork("root review rebuild", undefined, signal);
                    continue;
                }
                return reviewed;
            } catch (error) {
                this.disposeRootReview();
                if (error instanceof RootReviewPaused) return null;
                if (!(error instanceof RootReviewInvalidated)) throw error;
                if (attempt === 1 || error.reason.includes("admission")) {
                    this.rootPendingReason = "root review changed/limits";
                    return null;
                }
            }
        }
        return null;
    }

    private sortReviewedChanges(changes: FileChange[], priority: SyncPriority,
        options: PrioritySortOptions): Promise<FileChange[]> {
        return sortByPriorityCooperatively(changes, priority, options);
    }

    private pendingSchedulerLane(): WorkLane {
        try {
            const dirty = this.pendingChanges.capture();
            if (this.recentLocalUpsertOwner().hasPending(dirty, this.localEventsInFlight)) return "urgent-file";
            const activePath = this.app.workspace.getActiveFile?.()?.path;
            return activePath &&
                (this.pendingChanges.has(activePath) || this.localEventsInFlight.has(activePath))
                ? "urgent-file"
                : "bulk";
        } catch {
            return "bulk";
        }
    }

    private schedulerLaneForChanges(changes: readonly DirtyFileChange[]): WorkLane {
        try {
            if (changes.some(change => this.recentLocalUpsertOwner().matches(change))) return "urgent-file";
            const activePath = this.app.workspace.getActiveFile?.()?.path;
            return activePath && changes.some(change => change.path === activePath)
                ? "urgent-file"
                : "bulk";
        } catch {
            return "bulk";
        }
    }

    private async pushPendingInner(allowBulkChange = false, retryDeferred = false,
        driftRetryBudget?: { remaining: number }): Promise<"continue" | void> {
        if (this.startupReplayInProgress) return;
        if (this.reenrollmentRequired || this.stopped) return;
        if (!this.syncing) await this.recoverRootBeforeWork();
        if (this.stopped) return;
        if (
            this.bulkChangeReviewRequired &&
            !allowBulkChange &&
            !this.bulkChangeApprovalActive &&
            !this.syncBase.bulkChangeApproval
        ) {
            console.warn(
                `[obsetync] push deferred: ${this.pendingChanges.size} changes await Full Rescan review`,
            );
            return;
        }
        if (this.syncing || this.pendingChanges.size === 0) {
            console.log(
                `[obsetync] pushPending early-return: syncing=${this.syncing}, ` +
                `pending=${this.pendingChanges.size}`
            );
            return;
        }

        // --- Publish guards -------------------------------------------------
        // Never publish a root we can't vouch for; changes stay queued.
        if (this.pushBlocked) {
            console.warn(
                `[obsetync] push refused: tree diverged from server ` +
                `(${this.pendingChanges.size} changes queued) — run Full Rescan`,
            );
            return;
        }
        // A device that has both local state and a KNOWN server root but no
        // verified base would have to fabricate its parent — exactly the lie
        // that reverted the vault. The server root counts whether it came from
        // a completed/partial pull (lastPullServerRoot), the cached/adopted
        // fallback (localRootHash), OR the durable remote-work fence. A pull
        // that keeps CRASHING before observing a head never sets
        // lastPullServerRoot, which used to let this guard
        // slip and spray empty-parent putRoots (→ 400 storm, incident
        // 2026-07-15). A genuinely-first push to an empty vault has neither
        // signal, so it still proceeds.
        const serverRootKnown = this.lastPullServerRoot ?? this.localRootHash;
        if (!this.treeBaseRoot && (serverRootKnown || this.syncBase.verifiedBaseRequired)) {
            console.warn(
                "[obsetync] push deferred: no verified base root yet " +
                "(waiting for a pull to reconcile the tree)",
            );
            return;
        }
        // Cheap structural invariant: tree and sync-base advance in lockstep
        // now; a widening gap means a rebase was missed somewhere.
        const treeCount = this.getTreeFileCount();
        if (treeCount >= 0) {
            const baseCount = this.syncBase.entryCount();
            if (treeCount !== baseCount) {
                this.pushBlocked = true;
                console.error(
                    `[obsetync] push refused: tree=${treeCount} files vs ` +
                    `sync-base=${baseCount} — run Full Rescan`,
                );
                new Notice(
                    "Obsetync: local index inconsistent — sync paused. " +
                    "Run 'Force full rescan' in settings to recover.",
                    10000,
                );
                return;
            }
        }
        const retryKey = this.deferredRetryKey();
        const capturedJournalEpoch = this.rootRuntime ? this.journal.validatedEpoch : null;
        const candidateMutationAttemptEpoch = this.candidateMutationRetryEpoch;
        if (!retryDeferred) {
            const refusal = this.currentCandidateMutationAdmissionRefusal(allowBulkChange, retryKey);
            if (refusal) {
                this.rootPendingReason = "candidate memory admission";
                this.onStatusUpdate("sync ⏸ candidate memory admission");
                return;
            }
        }
        let queued: DirtyFileChange[] = [];
        if (!this.rootReviewedQueue?.hasRunnableReviewed() &&
            !this.deferredChanges.hasRunnable(this.pendingChanges.iterate(), retryKey, Date.now(), retryDeferred)) {
            this.onStatusUpdate(this.pendingIdleStatus());
            return;
        }

        this.syncing = true;
        this.state = "pushing";
        const perf = perfTrace.begin("push");
        let perfOutcome: PerfOutcome = "success";
        const endSpan = perfSpan("sync.push");

        // Durable mode claims only a bounded selection from its reviewed cut;
        // legacy mode still detaches a coalesced snapshot. Both freshly stat
        // selected sources and verify bytes before publication.
        let operationId: string | undefined;
        let pushFailed = false;
        let lifecycleQuiesced = false;
        // A failed negotiation is not permission for a legacy retry loop.
        let rootMode: "durable" | "legacy" = this.rootRuntime ? "durable" : "legacy";
        let rootAttempted = false;
        let candidateMutationCompleteClaim = false;
        let candidateMutationAttempt: CandidateMutationAttemptWitness | null = null;
        const durablyPublishedPaths = new Set<string>();
        let lifecycleWork: ResourceVisibilityWork | undefined;
        let workSignal = this.hashWorkerAbort.signal;

        try {
            operationId = await this.operationCheckpoint?.begin(
                "push",
                `${this.pendingChanges.size} dirty paths awaiting materialization`,
            );
            throwIfWorkAborted(this.hashWorkerAbort.signal);
            if (this.preparedTransfers && !this.preparedTransfers.plan.snapshot().ready) {
                // A failed optional retirement may leave this writer poisoned.
                // Recover its verified cut before another preparation, never
                // silently reinterpret missing/corrupt data as an empty plan.
                await this.preparedTransfers.plan.load();
            }
            if (this.preparedTransfers?.confirmations && !this.preparedTransfers.confirmations.snapshot().ready) {
                await this.preparedTransfers.confirmations.load();
            }
            throwIfWorkAborted(this.hashWorkerAbort.signal);
            // Do this before materializing a potentially vault-sized queue.
            await this.api.ensureTransportReady(perf);
            if (this.rootRuntime) rootMode = await this.rootRuntime.selectMode(perf);
            await this.waitForHeavyWork("push planning", operationId);
            lifecycleWork = this.visibilityGate?.beginHeavyWork(this.hashWorkerAbort.signal);
            workSignal = lifecycleWork?.signal ?? this.hashWorkerAbort.signal;
            throwIfWorkAborted(workSignal);
            this.rootPendingReason = null;
            let batch: FileChange[];
            let reviewed: RootReviewedSelection | null = null;
            for (;;) {
                const endMaterialize = perf.phase("stat");
                try {
                    if (rootMode === "durable") {
                        reviewed = await this.prepareReviewedRoot(allowBulkChange, retryDeferred, workSignal,
                            lifecycleWork?.epoch);
                        if (!reviewed) {
                            this.rootPendingReason ??= "root dependencies/limits";
                            const summary = this.rootReviewedQueue?.snapshot();
                            if (summary) this.rootBatchSummary = { selected: 0, cuts: 0, metadataBytes: 0, held: summary.deferred };
                            return;
                        }
                        if (this.rootSliceUrgentTurn && reviewed.kind === "root") {
                            const pendingLane = this.pendingSchedulerLane();
                            if (this.schedulerLaneForChanges(reviewed.queued) !== "urgent-file" &&
                                pendingLane === "urgent-file") {
                                // The queue alternates urgent and bulk for
                                // starvation freedom. A preempted bulk owner is
                                // the one exception: discard this consumed
                                // static selection back to dirty state and take
                                // the already-reviewed urgent overlay NOW. The
                                // remaining static plan stays usable; one
                                // rebuild recovers consumed suffixes only after
                                // that plan is exhausted.
                                this.pendingChanges.restore(reviewed.queued);
                                reviewed = await this.prepareReviewedRoot(allowBulkChange, retryDeferred, workSignal,
                                    lifecycleWork?.epoch);
                                if (!reviewed) {
                                    this.rootPendingReason ??= "root dependencies/limits";
                                    return;
                                }
                            }
                            if (reviewed.kind !== "root" ||
                                this.schedulerLaneForChanges(reviewed.queued) === "urgent-file" ||
                                this.pendingSchedulerLane() !== "urgent-file") {
                                this.rootSliceUrgentTurn = false;
                            }
                        }
                        queued = reviewed.queued;
                        batch = reviewed.ready;
                    } else {
                        this.disposeRootReview();
                        queued = this.pendingChanges.take();
                        batch = sortByPriority(await this.materializeDirtyChanges(queued, undefined, workSignal), this.syncPriority);
                    }
                } finally {
                    endMaterialize();
                }
                if (this.stopped) {
                    perfOutcome = "cancelled";
                    this.pendingChanges.restore(queued);
                    return;
                }
                const trackedDeletions = batch.filter(
                    (change) =>
                        change.action === "deleted" &&
                        this.syncBase.getEntry(change.path) !== null,
                ).length;
                if (!reviewed && !await this.reviewPendingCounts(batch.length, trackedDeletions, allowBulkChange)) {
                    this.pendingChanges.restore(queued);
                    return;
                }
                // Safety review above always sees the full coalesced snapshot,
                // including cooled sources and any deletions coupled to them.
                if (reviewed?.kind === "local") {
                    // Explicit policy/folder no-ops need no server mutation. They
                    // still require their exact validated WAL generation, and may
                    // not split a linked rename or erase a newer volatile hint.
                    this.rootRuntime!.assertCurrentScope();
                    throwIfWorkAborted(workSignal);
                    this.rootReviewedQueue!.assertApplicable();
                    const selected = queued.filter(hint =>
                        !this.pendingChanges.has(hint.path) && !this.localEventsInFlight.has(hint.path));
                    this.pendingChanges.restore(queued);
                    if (selected.length) {
                        if (!capturedJournalEpoch) throw new Error("local omission lacks its validated journal epoch");
                        const paths = new Set(selected.map(hint => hint.path));
                        const cuts = selected.map(hint => ({ path: hint.path, throughId: hint.journalId! }))
                            .sort((a, b) => a.path < b.path ? -1 : a.path > b.path ? 1 : 0);
                        const retirement = this.pendingChanges.captureRetirement(cuts);
                        this.rootAttemptCut = { epoch: capturedJournalEpoch, cuts };
                        rootAttempted = true; // Ambiguous ACK must reload before ordinary work.
                        await this.journal.acknowledgeOwned(capturedJournalEpoch, cuts);
                        const retired = this.pendingChanges.commitRetirement(retirement);
                        const dependenciesWereCurrent = this.rootReviewedQueue!.dependenciesCurrent();
                        this.deferredChanges.commitAcknowledged(cuts);
                        for (const path of paths) durablyPublishedPaths.add(path);
                        for (const hint of selected) this.recentLocalUpsertOwner().retire(hint, path =>
                            this.pendingChanges.has(path) || this.localEventsInFlight.has(path));
                        this.rootAttemptCut = null;
                        this.rootReviewedQueue!.settled(new Set([...paths].filter(path => !this.pendingChanges.has(path))), [], dependenciesWereCurrent);
                        if (retired > 0) {
                            return "continue";
                        }
                        // An inherited ID can ACK an older WAL row, but cannot
                        // retire its newer no-ID hint. Do not loop on that alone;
                        // allow independent real root work below to proceed.
                    }
                    // The local cursor consumed this finite selection, even when
                    // an inherited watermark could not retire a newer hint. Seek
                    // independent work without restarting a full audit or making
                    // a no-progress continuation loop.
                    queued = [];
                    continue;
                }
                break;
            }
            // The reviewed owner already selected current runnable sources
            // with its global delete hold and finite manual-retry allowance.
            // Repartitioning this page alone would lose that global context
            // (or suppress a forced source after an unrelated earlier root).
            const selection = reviewed ? { ready: batch, deferred: [] } : this.deferredChanges.partition(
                queued, batch, retryKey, Date.now(), retryDeferred,
            );
            const transaction = reviewed ? { ...reviewed, ready: selection.ready } : null;
            const transactionQueued = transaction?.queued ?? queued;
            const transactionReady = transaction?.ready ?? selection.ready;
            if (transaction) {
                this.rootBatchSummary = { selected: transaction.ready.length, cuts: transaction.cutCount,
                    metadataBytes: transaction.estimatedMetadataBytes, held: transaction.deferred };
                if (!transaction.ready.length) {
                    this.pendingChanges.restore(queued);
                    this.rootPendingReason = "root dependencies/limits";
                    this.onStatusUpdate("sync ⏸ pending root dependencies/limits");
                    return;
                }
            }
            const workLane = this.schedulerLaneForChanges(transactionQueued);
            const rootSlice = transaction ? {
                maxSourceBytes: Math.max(1, getHashTuning().maxBatchBytes),
                maxElapsedMs: ROOT_PUBLICATION_MAX_ELAPSED_MS,
                maxBoundaryFiles: ROOT_PUBLICATION_MAX_BOUNDARY_FILES,
                // RootReviewedQueue has already consumed this complete
                // selection. Exact urgent suffix restoration is safe, but
                // routine byte/time cuts would still churn the review and its
                // final suffix rebuild. Batches/yields stay bounded, and an
                // urgent edit can preempt at every <=32-file ACK boundary.
                allowRoutineSplit: false,
                shouldPreempt: () => workLane !== "urgent-file" &&
                    this.pendingSchedulerLane() === "urgent-file",
            } : undefined;
            // A slice policy is present on every durable publication, but it
            // may still consume the complete reviewed claim. Capture that
            // claim here; push's post-materialization cut attestation is the
            // authority for whether a safe slice/late deferral actually made
            // the candidate mutation smaller. rememberCandidateMutation...()
            // compares the two counts before installing a global refusal.
            candidateMutationCompleteClaim = rootMode === "durable" && transaction !== null &&
                this.tree.tree_version() === 2 && transactionQueued.length > 0 &&
                transactionQueued.length <= DIRTY_CLAIM_LIMIT &&
                transactionQueued.length === transactionReady.length && this.pendingChanges.size === 0;
            this.operationCheckpoint?.progress(
                operationId ?? "",
                `${batch.length} coalesced paths materialized`,
            );
            console.log(
                `[obsetync] pushPending: ${batch.length} final path states — ` +
                `first 3 paths: ${batch.slice(0, 3).map(c => `${c.action}:${c.path}`).join(", ")}`
            );
            this.onStatusUpdate(`↑ 0/${batch.length}`);

            const preparedJournalCuts = new Map<string, number>();
            if (this.preparedTransfers || transaction) {
                for (const change of transactionQueued) if (change.journalId !== undefined) {
                    preparedJournalCuts.set(change.path, change.journalId);
                }
            }
            if (candidateMutationCompleteClaim) {
                candidateMutationAttempt = this.captureCandidateMutationAttemptWitness(
                    allowBulkChange,
                    retryKey,
                    candidateMutationAttemptEpoch,
                    transactionQueued.length,
                );
            }
            const result = transactionReady.length > 0 ? await push(
                this.api,
                this.io,
                this.syncBase,
                this.wasm,
                this.tree,
                this.vaultId,
                transactionReady,
                // HONEST parent: the base our tree state descends from — never
                // the last root merely observed on the server.
                this.treeBaseRoot,
                (text) => {
                    if (this.stopped) throw new Error("sync engine stopped");
                    this.onStatusUpdate(text);
                    if (operationId) this.operationCheckpoint?.progress(operationId, text);
                },
                perf,
                this.hashWorkers,
                () => this.waitForHeavyWork("push", operationId, workSignal),
                workSignal,
                this.preparedTransfers && this.preparedScopeHash ? {
                    plan: this.preparedTransfers.plan,
                    confirmations: rootMode === "durable" ? this.preparedTransfers.confirmations : undefined,
                    serverGeneration: rootMode === "durable" ? this.rootRuntime!.selectedServerGeneration : undefined,
                    scopeHash: this.preparedScopeHash,
                    journalThroughId: (path) => preparedJournalCuts.get(path),
                    assertApplicable: (path) => {
                        throwIfWorkAborted(workSignal);
                        this.rootReviewedQueue?.assertApplicable();
                        if (this.pendingChanges.has(path) || this.localEventsInFlight.has(path)) {
                            throw new HashWorkerFileDriftError("local generation changed during prepared transfer");
                        }
                    },
                    memoryArbiter: this.preparedTransfers.memoryArbiter,
                } : undefined,
                transaction ? {
                    groups: transaction.selectedGroups,
                    rootByteLimit: this.rootRuntime!.selectedRootByteLimit,
                    slice: rootSlice,
                    assertApplicable: (path) => {
                        throwIfWorkAborted(workSignal);
                        this.rootReviewedQueue!.assertApplicable();
                        if (this.pendingChanges.has(path) || this.localEventsInFlight.has(path)) {
                            throw new HashWorkerFileDriftError("local generation changed during root transaction");
                        }
                    },
                    publish: async (candidate, assertCandidateCurrent) => {
                        if (!capturedJournalEpoch) throw new Error("root publication lacks its validated journal epoch");
                        const cuts = candidate.entries.flatMap(entry => {
                            const throughId = preparedJournalCuts.get(entry.path);
                            return throughId === undefined ? [] : [{ path: entry.path, throughId }];
                        });
                        this.rootAttemptCut = { epoch: capturedJournalEpoch, cuts };
                        rootAttempted = true;
                        const owner = this.rootReviewedQueue!;
                        const paths = candidate.entries.map(entry => entry.path);
                        const outcome = await this.rootRuntime!.publish({ ...candidate,
                            journalEpoch: capturedJournalEpoch, journalCuts: cuts }, () => {
                            // Intent hashing/persistence and final negotiation
                            // yield after push's last source guard. Revalidate
                            // only NEW send admission, never accepted recovery.
                            assertCandidateCurrent();
                            owner.assertApplicable();
                            for (const path of paths) if (this.pendingChanges.has(path) || this.localEventsInFlight.has(path)) {
                                throw new HashWorkerFileDriftError("local generation changed before root dispatch");
                            }
                        });
                        if (outcome.status === "accepted") {
                            for (const entry of candidate.entries) durablyPublishedPaths.add(entry.path);
                        }
                        return outcome;
                    },
                } : undefined,
                {
                    prepare: () => this.prepareCommittedTreeForPush(operationId),
                },
                this.rootTreeResidentAdmission,
                workLane,
                this.browserHash,
                true,
            ) : { newRootHash: this.getTreeRootHash(), conflicts: [], published: false, deferred: [] };
            // A capability probe can become definitively rejected during this
            // attempt. Attach deferrals to the resulting capability state so
            // the next automatic readiness check does not immediately retry.
            const settlementRetryKey = this.deferredRetryKey();
            if (transaction) {
                const deferred = result.deferred ?? [];
                const heldPaths = new Set(deferred.map(change => change.path));
                const readyPaths = new Set(transactionReady.map(change => change.path));
                const continuationPaths = new Set<string>();
                for (const path of result.continuation?.paths ?? []) {
                    if (!readyPaths.has(path) || continuationPaths.has(path) || heldPaths.has(path) ||
                        durablyPublishedPaths.has(path)) {
                        throw new Error("push returned an invalid root continuation");
                    }
                    continuationPaths.add(path);
                }
                if (continuationPaths.size > 0) {
                    this.rootSliceNeedsReviewRebuild = true;
                    if (result.continuation?.reason === "urgent") this.rootSliceUrgentTurn = true;
                }
                if (result.rootSettlement && result.rootSettlement.status !== "accepted") {
                    this.disposeRootReview();
                    this.rootRepairRequired = true;
                    this.rootPendingReason = `root ${result.rootSettlement.status}`;
                    this.pendingChanges.restore(transactionQueued);
                    this.onStatusUpdate(`sync ⏸ root ${result.rootSettlement.status}`);
                    return;
                }
                if (!result.published) {
                    if (!this.rootSliceUrgentTurn) this.disposeRootReview();
                    this.rootPendingReason = continuationPaths.size > 0 ? null : "root not published";
                    const held = this.deferredChanges.settle(transactionQueued, transactionReady, deferred,
                        settlementRetryKey, Date.now());
                    this.pendingChanges.restore(transactionQueued);
                    // No root means no journal ACK, even for a local omission.
                    if (held.acknowledged.length) this.onStatusUpdate("sync ⏸ root not published");
                    return continuationPaths.size > 0 ? "continue" : undefined;
                }
                const covered = transactionQueued.filter(hint => durablyPublishedPaths.has(hint.path) || heldPaths.has(hint.path));
                const settlement = this.deferredChanges.settle(covered, transactionReady, deferred,
                    settlementRetryKey, Date.now());
                this.pendingChanges.restore(settlement.retained);
                this.pendingChanges.restore(transactionQueued.filter(hint => !durablyPublishedPaths.has(hint.path) && !heldPaths.has(hint.path)));
                const dependenciesWereCurrent = this.rootReviewedQueue!.dependenciesCurrent();
                this.deferredChanges.commitSettlement(settlement); // Exact ACK already completed inside root settlement.
                this.treeBaseRoot = this.syncBase.treeBaseRoot;
                this.localRootHash = result.newRootHash;
                this.rootReviewedQueue!.settled(durablyPublishedPaths, deferred, dependenciesWereCurrent);
                for (const hint of transactionQueued) if (durablyPublishedPaths.has(hint.path)) {
                    this.recentLocalUpsertOwner().retire(hint, path =>
                        this.pendingChanges.has(path) || this.localEventsInFlight.has(path));
                }
                // Only this verified accepted successor may advance the
                // captured base. Pull/reload/policy changes rebuild on entry.
                if (this.rootReviewFence) this.rootReviewFence.base = this.syncBase.captureTreeEntries();
                await this.saveCachedRoot();
                this.rootAttemptCut = null;
                if (result.preparedCleanup?.length && this.preparedTransfers) {
                    const retire = retirablePreparedHints(result.preparedCleanup, preparedJournalCuts, settlement);
                    try { if (retire.length) await this.preparedTransfers.plan.discardMany(retire); }
                    catch { console.warn("[obsetync] prepared hint cleanup awaits validated recovery"); }
                }
                if (this.bulkChangeApprovalActive && this.bulkChangeApprovalCompletesWithPush &&
                    this.deferredChanges.summary().count === 0 && this.pendingChanges.size === 0 && this.localEventsInFlight.size === 0) {
                    await this.completeBulkChangeApproval(`${transactionReady.length} root paths committed`);
                }
                return "continue";
            }
            const settlement = this.deferredChanges.settle(
                queued,
                batch,
                [...selection.deferred, ...(result.deferred ?? [])],
                settlementRetryKey,
                Date.now(),
            );
            this.pendingChanges.restore(settlement.retained);
            const published = result.published ?? ((result.deferred?.length ?? 0) === 0);
            if (published && result.newRootHash) {
                this.localRootHash = result.newRootHash;
            }
            // Unmergeable same-file divergences: the server kept the OTHER
            // side in the tree and our version lost. Preserve our bytes as a
            // conflict copy NOW — the next pull will overwrite the original
            // path with the winner. The copy then syncs out as a normal new
            // file, visible on every device.
            if (result.conflicts.length > 0) {
                await this.preserveConflictCopies(result.conflicts as PushConflict[], batch, workSignal, perf);
            }

            // Our just-pushed root is now in the server's history, so it is a
            // valid (and honest) base for the next push — on a fast-forward it
            // IS the server's current root; after a server-side merge the next
            // pull will converge us onto the merged root and re-adopt.
            if (published) {
                const ourRoot = this.getTreeRootHash();
                const newBase = ourRoot ?? result.newRootHash;
                if (newBase && this.treeBaseRoot !== newBase) {
                    this.treeBaseRoot = newBase;
                    this.syncBase.setTreeBaseRoot(newBase);
                    await this.syncBase.save();
                }

                // An all-deferred result did not publish a root or alter base.
                await this.saveCachedRoot();
            }

            await this.journal.acknowledge(settlement.acknowledged);
            this.deferredChanges.commitSettlement(settlement);
            const acknowledged = new Map(settlement.acknowledged.map(cut => [cut.path, cut.throughId]));
            const retainedPaths = new Set(settlement.retained.map(hint => hint.path));
            for (const hint of queued) if (!retainedPaths.has(hint.path) &&
                ((hint.journalId === undefined && published) || acknowledged.get(hint.path) === hint.journalId)) {
                this.recentLocalUpsertOwner().retire(hint, path =>
                    this.pendingChanges.has(path) || this.localEventsInFlight.has(path));
            }
            if (result.preparedCleanup?.length && this.preparedTransfers) {
                const retire = retirablePreparedHints(result.preparedCleanup, preparedJournalCuts, settlement);
                try { if (retire.length) await this.preparedTransfers.plan.discardMany(retire); }
                catch {
                    // Content/root/base/journal already committed. Cleanup is
                    // not permission to restore their acknowledged dirty hints.
                    console.warn("[obsetync] prepared hint retirement deferred; validated recovery required before reuse");
                }
            }
            if (
                this.bulkChangeApprovalActive &&
                this.bulkChangeApprovalCompletesWithPush &&
                this.deferredChanges.summary().count === 0 &&
                this.pendingChanges.size === 0 && this.localEventsInFlight.size === 0
            ) {
                await this.completeBulkChangeApproval(
                    `${batch.length} materialized paths committed`,
                );
            }
        } catch (e: any) {
            pushFailed = true;
            const lifecycleReason = workSignal.aborted &&
                workSignal.reason instanceof MobileLifecycleQuiesceError
                ? workSignal.reason : e instanceof MobileLifecycleQuiesceError ? e : null;
            const lifecycleQuiesce = lifecycleReason !== null;
            lifecycleQuiesced = lifecycleQuiesce;
            perfOutcome = this.stopped || lifecycleQuiesce ? "cancelled" : "error";
            const candidateMutationRefusal = e instanceof CandidateMutationOutputAdmissionDeniedError;
            const candidateMutationSawNewer = candidateMutationCompleteClaim &&
                queued.some(hint => this.pendingChanges.has(hint.path));
            const bootstrapRefusal = e instanceof RootTreeOutputAdmissionDeniedError &&
                this.pushBootstrapAdmissionRefusal?.error === e;
            const repeatedBootstrapRefusal = bootstrapRefusal &&
                this.pushBootstrapReportedRefusal === e;
            const restored = queued.filter(hint => !durablyPublishedPaths.has(hint.path));
            this.pendingChanges.restore(restored);
            let retainedReview = false;
            if (!this.stopped && e instanceof HashWorkerFileDriftError && rootMode === "durable" &&
                (driftRetryBudget?.remaining ?? 0) > 0 &&
                !rootAttempted && durablyPublishedPaths.size === 0) {
                try {
                    retainedReview = this.rootReviewedQueue?.retainFailedIndependentSelection(restored) ?? false;
                } catch {
                    // A concurrent scope/dependency/overlay change requires the
                    // ordinary full rebuild below; never preserve it by guess.
                }
            }
            if (!retainedReview) this.disposeRootReview();
            if (rootAttempted) this.rootRepairRequired = true;
            if (candidateMutationRefusal && candidateMutationCompleteClaim &&
                !candidateMutationSawNewer && !rootAttempted && durablyPublishedPaths.size === 0 &&
                restored.length === queued.length && this.pendingChanges.size === queued.length) {
                this.rememberCandidateMutationAdmissionRefusal(
                    e,
                    candidateMutationAttempt,
                );
                this.rootPendingReason = "candidate memory admission";
            }
            if (lifecycleQuiesce) {
                this.rootPendingReason = `mobile ${lifecycleReason!.lifecycleReason}`;
                this.onStatusUpdate("sync ⏸ mobile hidden");
            } else if (retainedReview) {
                console.warn("[obsetync] push source changed; retrying the bounded reviewed selection");
            } else {
                console.error("[obsetync] push error:", e);
            }
            if (!this.stopped && !lifecycleQuiesce && !retainedReview && !repeatedBootstrapRefusal) {
                this.recordSyncFailure("push", e);
            }
            if (bootstrapRefusal) this.pushBootstrapReportedRefusal = e;
            if (retainedReview) {
                driftRetryBudget!.remaining--;
                return "continue";
            }
        } finally {
            try {
                if (operationId) await this.operationCheckpoint?.complete(operationId);
            } finally {
                lifecycleWork?.release();
                perf.finish(perfOutcome);
                endSpan();
                this.syncing = false;
                if (lifecycleQuiesced && !this.stopped) this.state = "idle";
            }
            if (!pushFailed && !this.stopped) {
                this.state = "idle";
                this.onStatusUpdate(
                    this.pendingIdleStatus(),
                );
            }
            // Drain user edits that arrived while we were pushing.
            if (
                !this.reenrollmentRequired &&
                !this.stopped &&
                !this.bulkChangeReviewRequired &&
                rootMode !== "durable" &&
                this.hasRunnablePendingChanges()
            ) {
                this.autoPushCoalescer?.trigger();
            }
        }
    }

    /** Resolve a dirty hint to the path's final on-disk state. Metadata from
     *  an event is reused only if stat still agrees; bytes are never retained
     *  in the queue. */
    private async materializeDirtyChanges(changes: DirtyFileChange[],
        onOmission?: (path: string, reason: RootLocalOmission["reason"]) => void,
        signal: AbortSignal = this.hashWorkerAbort.signal,
        afterCooperate?: () => void): Promise<FileChange[]> {
        return materializeFileChanges(
            changes,
            (path) => this.io.stat(path),
            (path) => this.syncBase.getEntry(path) !== null,
            (path) => this.isExcluded(path),
            onOmission,
            async () => {
                await yieldToUI({ signal });
                await this.waitForHeavyWork("push metadata", undefined, signal);
                afterCooperate?.();
            },
            { readConcurrency: () => getHashTuning().readConcurrency, signal },
        );
    }

    /** Preserve OUR losing side of a legacy-root conflict from immutable
     * server objects. The exact pushed row supplies the expected size/hash;
     * neither a later local generation nor a whole-file fallback is accepted. */
    private async preserveConflictCopies(conflicts: PushConflict[], batch: readonly FileChange[],
        signal: AbortSignal, perf?: PerfOperation): Promise<void> {
        let preserved = 0;
        let failed = 0;
        const now = new Date();
        for (const c of conflicts) {
            if (!c.path || !c.side_b_hash) continue;
            const copyPath = conflictCopyPath(c.path, this.deviceName, now);
            try {
                const matching = batch.filter(change => change.path === c.path && change.action !== "deleted" &&
                    change.hash?.toLowerCase() === c.side_b_hash.toLowerCase());
                if (matching.length !== 1 || !Number.isSafeInteger(matching[0].size) || matching[0].size! < 0) {
                    throw new Error("legacy conflict does not match one exact pushed generation");
                }
                await preserveVerifiedConflictCopy(this.api, this.io, this.wasm, {
                    path: c.path,
                    copyPath,
                    hash: c.side_b_hash.toLowerCase(),
                    size: matching[0].size!,
                    stagingKey: await legacyConflictStagingKey(c.path, c.side_b_hash.toLowerCase()),
                }, { signal, perf });
                preserved++;
                console.log(
                    `[obsetync] conflict on ${c.path} — our version preserved as ${copyPath}`,
                );
            } catch (e) {
                failed++;
                console.error(`[obsetync] failed to preserve conflict copy for ${c.path}:`, e);
            }
        }
        if (preserved > 0) {
            new Notice(
                `Obsetync: ${preserved} conflict${preserved > 1 ? "s" : ""} — your version${
                    preserved > 1 ? "s were" : " was"
                } saved as "(conflict …)" cop${preserved > 1 ? "ies" : "y"} next to the file${
                    preserved > 1 ? "s" : ""
                }. Use the "Show sync conflicts" command to resolve.`,
                12000,
            );
        }
        if (failed > 0) {
            throw new Error(
                `could not preserve ${failed} conflict cop${failed === 1 ? "y" : "ies"}; ` +
                "original push remains journaled for retry",
            );
        }
    }

    /** Layer 2: metadata-only local replay, independent of server availability.
     * Publishing is deliberately a separate startup/Sync Now operation. */
    private async recoverFromJournal(): Promise<void> {
        const unsyncedCount = this.journal.unsyncedCount();
        if (unsyncedCount === 0) return;
        // Capture the same immutable index cut before any callback or await.
        // Iterator creation does not materialize the pending journal as an array.
        const unsynced = this.journal.iterateUnsynced();

        console.log(
            `[obsetync] recovering ${unsyncedCount} changes from journal`
        );
        const recovered = new DirtyPathSet();
        let processed = 0;
        for (const entry of unsynced) {
            throwIfWorkAborted(this.hashWorkerAbort.signal);
            // No reads or hashes during recovery: one final-state stat happens
            // immediately before push. The captured index shares immutable
            // metadata; coalesced hints and unresolved rename dependencies
            // still use metadata proportional to pending work.
            if (entry.action === "renamed" && entry.oldPath) {
                this.deferredChanges.registerLegacyRename(entry.oldPath, entry.path, entry.id);
            }
            recovered.add(
                {
                    action: entry.action === "created" ? "created" :
                        entry.action === "deleted" ? "deleted" : "modified",
                    path: entry.path,
                    ...(entry.hash !== undefined ? { hash: entry.hash } : {}),
                    ...(entry.mtime !== undefined ? { mtime: entry.mtime } : {}),
                    ...(entry.size !== undefined ? { size: entry.size } : {}),
                },
                entry.id,
            );
            if (entry.action === "renamed" && entry.oldPath) {
                recovered.add({ action: "deleted", path: entry.oldPath }, entry.id);
            }
            processed++;
            if (processed % 256 === 0 || processed === unsyncedCount) {
                this.onStatusUpdate(`⟳ journal ${processed}/${unsyncedCount}`);
                this.progressHeartbeat("journal", `${processed}/${unsyncedCount}`);
                await yieldToUI({ signal: this.hashWorkerAbort.signal, lane: "maintenance" });
            }
        }
        // Merge each unique old path once. Live callbacks may have completed
        // during the yields; preserve those newer hints and carry only the
        // older WAL watermark into their eventual successful publication.
        const hints = recovered.take();
        for (let offset = 0; offset < hints.length; offset += 256) {
            throwIfWorkAborted(this.hashWorkerAbort.signal);
            this.pendingChanges.restoreJournal(hints.slice(offset, offset + 256));
            await yieldToUI({ signal: this.hashWorkerAbort.signal, lane: "maintenance" });
        }
    }

    /** Layer 3: metadata audit — detect any stat drift and offline deletion. */
    private partialMtimeScan(): Promise<void> {
        return this.trackOperation(() => this.partialMtimeScanInner(), undefined);
    }

    private async partialMtimeScanInner(): Promise<void> {
        if (this.startupReplayInProgress || this.stopped || this.reenrollmentRequired || this.syncing) return;
        await this.recoverRootBeforeWork();
        if (this.stopped || this.reenrollmentRequired || this.syncing) return;
        const lastSync = this.syncBase.lastSyncTimestamp;
        if (lastSync === 0) return; // First ever sync — skip, let pull handle it.
        if (this.bulkChangeReviewRequired && !this.syncBase.bulkChangeApproval) {
            console.warn("[obsetync] metadata scan skipped while bulk changes await review");
            return;
        }

        const perf = perfTrace.begin("scan");
        let perfOutcome: PerfOutcome = "success";
        let shouldPush = false;
        let lifecycleWork: ResourceVisibilityWork | undefined;
        let workSignal = this.hashWorkerAbort.signal;
        let lifecycleQuiesced = false;
        // Metadata refreshes also mutate shared base. They may not overlap
        // push publication or a recovery reload between cooperative hash cuts.
        this.syncing = true;
        this.state = "scanning";
        try {
            await this.waitForHeavyWork("metadata scan");
            lifecycleWork = this.visibilityGate?.beginHeavyWork(this.hashWorkerAbort.signal);
            workSignal = lifecycleWork?.signal ?? this.hashWorkerAbort.signal;
            throwIfWorkAborted(workSignal);
            // Filter candidates from in-memory cache — no async stat calls.
            const endStat = perf.phase("stat");
            const allStats = this.io.statBulk();
            if (this.syncObsidianConfig) {
                const obsidianFiles = await this.io.listObsidianConfig();
                for (const [p, s] of obsidianFiles) allStats.set(p, s);
            }
            endStat();

            let visibleFiles = 0;
            let visibleBytes = 0;
            for (const [path, stat] of allStats) {
                if (this.isExcluded(path)) continue;
                visibleFiles++;
                visibleBytes += stat.size;
            }

            const endEnumerate = perf.phase("enumerate");
            const plan = planMetadataScan(
                allStats,
                this.syncBase.allPaths(),
                (path) => this.syncBase.getEntry(path),
                (path) => this.isExcluded(path),
            );
            endEnumerate();
            perf.setWorkload({ filesTotal: visibleFiles, bytesTotal: visibleBytes });

            const total = plan.toHash.length + plan.deleted.length;
            const needsBulkReview = metadataScanNeedsReview(
                plan,
                this.syncBase.entryCount(),
            );
            const approvalResumed = this.syncBase.bulkChangeApproval
                ? await this.resumeBulkChangeApproval(
                    "metadata scan",
                    total,
                    plan.deleted.length,
                )
                : false;
            if (approvalResumed) {
                this.bulkChangeApprovalCompletesWithPush = true;
            }

            if (needsBulkReview && !approvalResumed) {
                perf.setWorkload({
                    filesTotal: visibleFiles + plan.deleted.length,
                    filesNeeded: total,
                    bytesNeeded: plan.toHash.reduce((sum, item) => sum + item.stat.size, 0),
                });
                perf.increment({ filesCompleted: visibleFiles + plan.deleted.length });
                console.warn(
                    `[obsetync] metadata scan found ${plan.toHash.length} ` +
                    `files need hashing and ${plan.deleted.length} look deleted ` +
                    `(${total} total changes)`,
                );
                this.requireBulkChangeReview("metadata scan", total, plan.deleted.length);
                return;
            }

            let durablePending: FileChange[] = plan.deleted.map(path => ({ action: "deleted", path }));
            const flushDurablePending = async () => {
                if (durablePending.length === 0) return;
                await this.queueScanChangesDurably(durablePending, workSignal);
                durablePending = [];
            };
            await flushDurablePending();
            perf.setWorkload({ filesTotal: visibleFiles + plan.deleted.length });
            perf.increment({ filesCompleted: visibleFiles - plan.toHash.length + plan.deleted.length });

            let found = plan.deleted.length;
            let changedBytes = 0;
            let metadataRefreshes = 0;
            let metadataRefreshesSinceCheckpoint = 0;
            if (plan.toHash.length > 0) {
                const hashableTotal = plan.toHash.reduce(
                    (count, item) =>
                        count + (item.stat.size < LARGE_FILE_THRESHOLD ? 1 : 0),
                    0,
                );
                let hashOrdinal = 0;
                const hashSampleWeights = plan.toHash.map((item) => {
                    if (item.stat.size >= LARGE_FILE_THRESHOLD) return 0;
                    return perfSampleWeight(hashOrdinal++, hashableTotal);
                });
                for (let i = 0; i < plan.toHash.length;) {
                    await this.waitForHeavyWork("metadata scan", undefined, workSignal);
                    const batch = plan.toHash.slice(i, i + getHashTuning().readConcurrency);
                    perf.setDemand({ read: plan.toHash.length - i, hash: plan.toHash.length - i });
                    perf.observePeakBatchBytes(batch.reduce((sum, item) => {
                        const residentBytes = this.io.getAbsolutePath(item.path)
                            ? Math.min(item.stat.size, getHashTuning().feedBytes)
                            : item.stat.size;
                        return sum + residentBytes;
                    }, 0));
                    const endBatch = perf.phase("scan_batch");
                    const results = await joinStartedWork(
                        batch.map(async ({ path, stat }, batchIndex) => {
                            const knownHash = this.syncBase.getHash(path);
                            if (stat.size >= LARGE_FILE_THRESHOLD) {
                                return {
                                    kind: "change" as const,
                                    path,
                                    stat,
                                    hash: undefined as string | undefined,
                                    knownHash,
                                };
                            }
                            const itemIndex = i + batchIndex;
                            const sampleWeight = hashSampleWeights[itemIndex];
                            const stable = await this.hashStableFile(
                                path,
                                stat,
                                sampleWeight > 0 ? perf : undefined,
                                sampleWeight,
                                workSignal,
                            );
                            if (stable.hash === knownHash) {
                                return {
                                    kind: "metadata" as const,
                                    path,
                                    stat: stable.stat,
                                };
                            }
                            return {
                                kind: "change" as const,
                                path,
                                stat: stable.stat,
                                hash: stable.hash,
                                knownHash,
                            };
                        }),
                    ).finally(endBatch);
                    for (const result of results) {
                        if (result.kind === "metadata") {
                            if (this.syncBase.refreshLocalMetadata(
                                result.path,
                                result.stat.mtime,
                                result.stat.size,
                            )) {
                                metadataRefreshes++;
                                metadataRefreshesSinceCheckpoint++;
                            }
                            continue;
                        }
                        found++;
                        changedBytes += result.stat.size;
                        const change: FileChange = {
                            action: result.knownHash ? "modified" : "created",
                            path: result.path,
                            mtime: result.stat.mtime,
                            size: result.stat.size,
                        };
                        if (result.hash !== undefined) change.hash = result.hash;
                        durablePending.push(change);
                        if (durablePending.length >= 256) await flushDurablePending();
                    }
                    perf.increment({ filesCompleted: batch.length });
                    i += batch.length;
                    perf.setDemand({});
                    if (metadataRefreshesSinceCheckpoint >= 500) {
                        await this.syncBase.checkpoint();
                        metadataRefreshesSinceCheckpoint = 0;
                    }
                    await yieldToUI({ signal: workSignal, perf, lane: "maintenance" });
                    const done = i;
                    this.onStatusUpdate(`⟳ scan ${done}/${plan.toHash.length}`);
                    this.progressHeartbeat(
                        "mtimeScan",
                        `${done}/${plan.toHash.length}, ${found} changed`,
                    );
                }
            }
            await flushDurablePending();
            if (metadataRefreshesSinceCheckpoint > 0) {
                await this.syncBase.checkpoint();
            }
            if (metadataRefreshes > 0) await this.syncBase.save();

            perf.setWorkload({
                filesTotal: visibleFiles + plan.deleted.length,
                filesNeeded: found,
                bytesNeeded: changedBytes,
            });
            shouldPush = found > 0;
            if (shouldPush) {
                console.log(`[obsetync] metadata scan found ${found} unsynced changes`);
            }
            if (metadataRefreshes > 0) {
                console.log(
                    `[obsetync] metadata scan refreshed ${metadataRefreshes} ` +
                    `content-identical stat entries`,
                );
            }
        } catch (error) {
            const lifecycleReason = workSignal.aborted &&
                workSignal.reason instanceof MobileLifecycleQuiesceError
                ? workSignal.reason : error instanceof MobileLifecycleQuiesceError ? error : null;
            lifecycleQuiesced = lifecycleReason !== null;
            perfOutcome = this.stopped || lifecycleQuiesced ? "cancelled" : "error";
            if (lifecycleQuiesced) {
                this.metadataScanLifecyclePending = true;
                this.onStatusUpdate("sync ⏸ mobile hidden");
                return;
            }
            throw error;
        } finally {
            lifecycleWork?.release();
            try { perf.finish(perfOutcome); }
            finally {
                this.syncing = false;
                this.state = perfOutcome === "error" ? "error" : "idle";
            }
        }

        if (shouldPush || this.hasRunnablePendingChanges() || (this.bulkChangeApprovalActive && this.pendingChanges.size > 0)) {
            await this.pushPending(this.bulkChangeApprovalActive);
        } else if (this.bulkChangeApprovalActive) {
            await this.completeBulkChangeApproval("metadata scan found no content changes");
        }
    }

    /** Layer 1: attach live vault event listeners.
     *
     *  Events are journaled + queued even while a sync is in flight — the old
     *  `if (this.syncing) return` dropped genuine user edits made during a
     *  long pull. The one thing we must NOT queue is the pull's own disk
     *  writes echoing back as vault events; those are recognized only when
     *  the event's actual content matches PullEchoTracker's expected hash. */
    private async appendLocalJournal(entry: NewJournalEntry): Promise<number | undefined> {
        try {
            return await this.journal.append(entry);
        } catch (error) {
            // Keep the current-session dirty hint even when durable storage is
            // unavailable. The error is visible in diagnostics; a full scan
            // remains the recovery path after a crash.
            console.error(`[obsetync] change journal append failed for ${entry.path}:`, error);
            return undefined;
        }
    }

    /** Scan-discovered dirty state is real local work, not diagnostic progress.
     * Publish it to the authoritative journal in bounded cuts before exposing
     * it to the in-memory queue. A renderer restart can therefore publish the
     * completed scan prefix first and continue auditing the remaining suffix,
     * instead of returning to an all-or-nothing Full Rescan loop. */
    private async queueScanChangesDurably(changes: readonly FileChange[],
        signal: AbortSignal = this.hashWorkerAbort.signal): Promise<void> {
        for (let offset = 0; offset < changes.length; offset += 256) {
            throwIfWorkAborted(signal);
            const batch = changes.slice(offset, offset + 256);
            const ts = Date.now();
            const ids = await this.journal.appendBatch(batch.map(change => ({
                action: change.action,
                path: change.path,
                ts,
                synced: false,
                ...(change.hash !== undefined ? { hash: change.hash } : {}),
                ...(change.mtime !== undefined ? { mtime: change.mtime } : {}),
                ...(change.size !== undefined ? { size: change.size } : {}),
            })));
            if (ids.length !== batch.length) {
                throw new Error("scan journal returned an incomplete durable generation cut");
            }
            for (let index = 0; index < batch.length; index++) {
                this.pendingChanges.add(batch[index], ids[index]);
            }
        }
    }

    /** Hash an expected pull echo without materializing a large file twice on
     *  mobile. Desktop streams from fs. Mobile large-file writes use the
     *  just-recorded sync-base hash only when event stat exactly matches the
     *  post-write stat; otherwise the event stays dirty conservatively. */
    private async pullEchoHash(file: TFile, path = file.path): Promise<string | null> {
        try {
            if (this.stopped || file.path !== path) return null;
            const expected = { size: file.stat.size, mtime: file.stat.mtime };
            if (
                expected.size >= LARGE_FILE_THRESHOLD &&
                this.io.getAbsolutePath(path) === null
            ) {
                // This optional classification also catches a cancelled mobile
                // yield: an event with failed durable append MUST still reach
                // its session dirty hint, never disappear on quiescence.
                await yieldToUI({ signal: this.hashWorkerAbort.signal,
                    lane: "urgent-file", deadlineMs: 250 });
                if (this.stopped || file.path !== path || file.stat.size !== expected.size || file.stat.mtime !== expected.mtime) return null;
                const base = this.syncBase.getEntry(path);
                return base && base.mtime === expected.mtime && base.size === expected.size
                    ? base.hash
                    : null;
            }
            const hashed = await this.hashStableFile(path, expected);
            return !this.stopped && file.path === path ? hashed.hash : null;
        } catch (error) {
            console.warn(`[obsetync] pull-echo verification failed for ${path}:`, error);
            return null;
        }
    }

    /** Hash a stable pathname. Desktop workers receive only path/stat metadata
     * and return only digest/timings; a stat drift retries once with the fresh
     * fingerprint. Mobile and unavailable-worker paths keep the bounded
     * renderer implementation. */
    private async hashStableFile(
        path: string,
        expected: FileStat,
        perf?: PerfOperation,
        perfWeight = 1,
        signal: AbortSignal = this.hashWorkerAbort.signal,
    ): Promise<{ hash: string; stat: FileStat }> {
        let current = expected;
        for (let attempt = 0; attempt < 2; attempt++) {
            const absolutePath = this.io.getAbsolutePath(path);
            if (this.hashWorkers && absolutePath) {
                const feedBytes = getHashTuning().feedBytes;
                const memory = await reserveTransientScope({
                    ownerBytes: 2 * feedBytes + 64 * 1024,
                    workBytes: 64 * 1024,
                }, { signal });
                try {
                    let result;
                    try {
                        result = await this.hashWorkers.run({
                            absolutePath,
                            expectedSize: current.size,
                            expectedMtime: current.mtime,
                            mode: "hash",
                            feedBytes,
                        }, signal, memory);
                    } finally { memory.close(); }
                    if (result.mode !== "hash") throw new Error("hash worker mode mismatch");
                    perf?.addPhase("read", result.read_ms * perfWeight);
                    perf?.addPhase("hash", result.hash_ms * perfWeight);
                    return { hash: result.hash, stat: current };
                } catch (error) {
                    if (error instanceof HashWorkerFileDriftError) {
                        const refreshed = await this.io.stat(path);
                        if (refreshed && attempt === 0) {
                            current = refreshed;
                            continue;
                        }
                        throw error;
                    }
                    if ((error as Error)?.name === "AbortError") throw error;
                    if (!(await waitForHashWorkerCleanup(error))) throw error;
                    if (!this.hashWorkerFallbackWarned) {
                        this.hashWorkerFallbackWarned = true;
                        console.warn(
                            "[obsetync] desktop hash worker failed; using renderer fallback:",
                            error,
                        );
                    }
                }
            }

            let hash: string;
            try {
                hash = await hashFileStreaming(
                    path, this.io, this.wasm, perf, perfWeight, signal,
                    this.browserHash,
                );
            } catch (error) {
                // A note can grow during an uncancellable adapter read. Treat
                // this specific admission drift like the worker fingerprint
                // drift above; never retry/classify unrelated I/O or oversize
                // failures as absence or a successful hash.
                if (error instanceof HashSourceGrowthError && attempt === 0) {
                    const refreshed = await this.io.stat(path);
                    if (refreshed) {
                        current = refreshed;
                        continue;
                    }
                }
                throw error;
            }
            const after = await this.io.stat(path);
            if (
                after &&
                after.size === current.size &&
                Math.abs(after.mtime - current.mtime) <= 1
            ) {
                return { hash, stat: after };
            }
            if (after && attempt === 0) {
                current = after;
                continue;
            }
            throw new HashWorkerFileDriftError();
        }
        throw new HashWorkerFileDriftError();
    }

    private async queueLocalUpsert(file: TAbstractFile, action: "created" | "modified"): Promise<void> {
        if (!(file instanceof TFile)) return;
        const path = file.path;
        const stat = { mtime: file.stat.mtime, size: file.stat.size };
        if (this.isExcluded(path)) return;
        const guard = this.localEventsInFlight.acquire(path);
        const recentLocalUpserts = this.recentLocalUpsertOwner();
        let recentToken: symbol | undefined;
        let recentCompleted = false;
        try {
            const expectedEcho = !this.stopped && this.pullEchoes.expectsUpsert(path);
            if (!expectedEcho) recentToken = recentLocalUpserts.begin(path);
            const journalId = await this.appendLocalJournal({ action, path, ts: Date.now(), synced: false });
            const actualHash = expectedEcho && guard.isCurrent() && file.path === path
                ? await this.pullEchoHash(file, path) : null;
            if (!this.stopped && guard.isCurrent() && file.path === path && actualHash !== null &&
                this.pullEchoes.consumeUpsert(path, actualHash)) {
                if (journalId !== undefined) await this.journal.acknowledge([{ path, throughId: journalId }]);
                return;
            }
            const change: DirtyFileChange = { action, path, ...stat, journalId };
            if (guard.isCurrent() && file.path === path && actualHash !== null) change.hash = actualHash;
            if (guard.isCurrent()) {
                this.pendingChanges.add(change, journalId);
                recentToken ??= recentLocalUpserts.begin(path);
                recentLocalUpserts.complete(path, recentToken, change);
                recentCompleted = true;
            } else {
                // A superseded pull echo has no early recent token. Restoring
                // its stale dirty owner is conservative, but it must not
                // overwrite the newer callback's scheduling provenance.
                this.pendingChanges.restore([change]);
            }
            this.autoPushCoalescer?.trigger();
        } finally {
            if (recentToken && !recentCompleted) recentLocalUpserts.cancel(path, recentToken);
            guard.release();
        }
    }

    private attachVaultListeners(): void {
        const runAutomaticPush = async () => {
            if (this.autoSync) {
                try { await this.pushPending(); }
                catch (error) {
                    // Recovery/scope/cooperation can reject before the inner
                    // push catch. Automatic callbacks must own that rejection,
                    // not turn a safely pending intent into a renderer error.
                    if (!this.stopped && this.lastError?.origin !== "root-recovery") this.recordSyncFailure("push", error);
                    console.warn("[obsetync] automatic push remains pending:", error);
                }
            }
        };
        // Keep the legacy wrapper as an existing deterministic host-test seam.
        // Production events never call it; batching is owned by the bounded
        // coalescer below, so this compatibility delay adds no user latency.
        this.debouncedPush = debounce(() => { void runAutomaticPush(); }, 3_000, true);
        this.autoPushCoalescer?.close();
        this.autoPushCoalescer = new MaxLatencyCoalescer({
            quietMs: 150,
            maxLatencyMs: 750,
            run: runAutomaticPush,
        });
        const debouncedPush = () => this.autoPushCoalescer?.trigger();

        this.eventRefs.push(
            this.app.vault.on("modify", (file: TAbstractFile) => this.trackLocalEvent(() => this.queueLocalUpsert(file, "modified"))),
        );
        this.eventRefs.push(
            this.app.vault.on("create", (file: TAbstractFile) => this.trackLocalEvent(() => this.queueLocalUpsert(file, "created"))),
        );

        this.eventRefs.push(
            this.app.vault.on("delete", (file: TAbstractFile) => this.trackLocalEvent(async () => {
                if (!(file instanceof TFile)) return;
                const path = file.path;
                if (this.isExcluded(path) || (!this.stopped && this.pullEchoes.consumeDelete(path))) return;
                const guard = this.localEventsInFlight.acquire(path);
                try {
                    const journalId = await this.appendLocalJournal({
                        action: "deleted",
                        path,
                        ts: Date.now(),
                        synced: false,
                    });
                    const change: DirtyFileChange = { action: "deleted", path, journalId };
                    if (guard.isCurrent()) this.pendingChanges.add(change, journalId);
                    else this.pendingChanges.restore([change]);
                    debouncedPush();
                } finally {
                    guard.release();
                }
            }))
        );

        this.eventRefs.push(
            this.app.vault.on("rename", (file: TAbstractFile, oldPath: string) => this.trackLocalEvent(async () => {
                if (!(file instanceof TFile)) return;
                // TFile is mutable: another rename can change file.path while
                // this event's durable append is still awaiting the adapter.
                const newPath = file.path;
                const stat = { mtime: file.stat.mtime, size: file.stat.size };
                const oldExcluded = this.isExcluded(oldPath);
                const newExcluded = this.isExcluded(newPath);
                if (oldExcluded && newExcluded) return;
                const oldGuard = this.localEventsInFlight.acquire(oldPath);
                const newGuard = this.localEventsInFlight.acquire(newPath);
                try {
                    // A rename crossing out of sync scope is only a deletion;
                    // crossing in is only a creation. Never publish the ignored
                    // half of the move.
                    if (newExcluded) {
                        const journalId = await this.appendLocalJournal({
                            action: "deleted",
                            path: oldPath,
                            ts: Date.now(),
                            synced: false,
                        });
                        const change: DirtyFileChange = { action: "deleted", path: oldPath, journalId };
                        if (oldGuard.isCurrent()) this.pendingChanges.add(change, journalId);
                        else this.pendingChanges.restore([change]);
                        debouncedPush();
                        return;
                    }

                    const expectedEcho = !this.stopped && !oldExcluded &&
                        this.pullEchoes.expectsRename(oldPath, newPath);
                    let oldJournalId: number | undefined;
                    let newJournalId: number | undefined;
                    if (oldExcluded) {
                        newJournalId = await this.appendLocalJournal({
                            action: "created", path: newPath, ts: Date.now(), synced: false,
                        });
                    } else {
                        const registration = this.deferredChanges.registerLegacyRename(oldPath, newPath);
                        try {
                            [oldJournalId, newJournalId] = await this.journal.appendGroup([
                                { action: "deleted", path: oldPath, ts: Date.now(), synced: false },
                                { action: "created", path: newPath, ts: Date.now(), synced: false },
                            ]);
                        } catch (error) {
                            // Keep both session hints; never persist just the
                            // delete half or guess an ACK after append failure.
                            console.error("[obsetync] linked rename journal append failed:", error);
                        } finally {
                            registration.confirm(oldJournalId);
                        }
                    }
                    const actualHash = expectedEcho && newGuard.isCurrent() && file.path === newPath
                        ? await this.pullEchoHash(file, newPath) : null;
                    if (
                        !this.stopped && expectedEcho &&
                        oldGuard.isCurrent() && newGuard.isCurrent() &&
                        file.path === newPath &&
                        actualHash !== null &&
                        this.pullEchoes.consumeRename(oldPath, newPath, actualHash)
                    ) {
                        const watermarks: Array<{ path: string; throughId: number }> = [];
                        if (oldJournalId !== undefined) {
                            watermarks.push({ path: oldPath, throughId: oldJournalId });
                        }
                        if (newJournalId !== undefined) {
                            watermarks.push({ path: newPath, throughId: newJournalId });
                        }
                        await this.journal.acknowledge(watermarks);
                        this.deferredChanges.commitAcknowledged(watermarks);
                        return;
                    }
                    if (!oldExcluded) {
                        const change: DirtyFileChange = { action: "deleted", path: oldPath, journalId: oldJournalId };
                        if (oldGuard.isCurrent()) this.pendingChanges.add(change, oldJournalId);
                        else this.pendingChanges.restore([change]);
                    }
                    const change: DirtyFileChange = {
                        action: "created",
                        path: newPath,
                        ...stat,
                        journalId: newJournalId,
                    };
                    if (newGuard.isCurrent() && file.path === newPath && actualHash !== null) change.hash = actualHash;
                    if (newGuard.isCurrent()) this.pendingChanges.add(change, newJournalId);
                    else this.pendingChanges.restore([change]);
                    debouncedPush();
                } finally {
                    oldGuard.release();
                    newGuard.release();
                }
            }))
        );
    }

    private async saveCachedRoot(): Promise<void> {
        const path = ".obsidian/plugins/obsetync/cached-root.bin";
        try {
            const tree = this.tree;
            const revisionReader = tree.committed_revision;
            if (revisionReader !== undefined && typeof revisionReader !== "function") {
                throw new Error("committed root revision witness is invalid");
            }
            const readRevision = revisionReader === undefined ? undefined : () => {
                const revision = revisionReader.call(tree);
                if (!Number.isSafeInteger(revision) || revision < 0) {
                    throw new Error("committed root revision witness is invalid");
                }
                return revision;
            };
            const expectedRevision = readRevision?.();
            const expectedHash = tree.root_hash_hex();
            const expectedVersion = tree.tree_version();
            if (!expectedHash) return;
            if ((expectedVersion !== 1 && expectedVersion !== 2) || tree.has_candidate()) {
                throw new Error("committed root cache export requires a stable supported tree");
            }
            const assertCurrent = () => {
                if (this.tree !== tree || tree.has_candidate() || tree.tree_version() !== expectedVersion ||
                    (readRevision ? readRevision() !== expectedRevision : tree.root_hash_hex() !== expectedHash)) {
                    throw new Error("committed root changed during cache export");
                }
            };
            // For v1, offset metadata is <= arena/32 because every child adds
            // at least 128 bytes to the conservative arena. The export owner
            // is 2*arena + offsets + 2 pages and this cache consumer reserves
            // arena + 1 page, so reject an impossible Plan before scanning the
            // complete root. This is a capacity bound, not available-memory or
            // RSS measurement; admission still arbitrates concurrent owners.
            const capacity = transientMemorySnapshot().capacityBytes;
            const fixedPages = 3 * TREE_ROOT_EXPORT_PAGE_BYTES;
            const maxArenaBytes = Math.max(512, Math.min(0xffff_ffff,
                Math.floor(((Math.max(0, capacity - fixedPages) * 32) + 512) / 97)));
            const stepBytes = Math.max(1, Math.min(TREE_ROOT_EXPORT_MAX_STEP_BYTES,
                getHashTuning().feedBytes));
            const owned = await exportTreeRoot(tree, "committed", {
                cooperate: async () => {
                    await this.waitForHeavyWork("root cache export");
                    await yieldToUI({ signal: this.hashWorkerAbort.signal, lane: "maintenance" });
                },
                signal: this.hashWorkerAbort.signal,
                assertCurrent,
                expectedHash,
                expectedVersion,
                // The byte guard follows the governor while the independent
                // operation cap prevents a huge run of zero/small-byte schema
                // primitives inside one host task.
                stepUnits: TREE_ROOT_EXPORT_MAX_STEP_UNITS,
                stepBytes,
                maxArenaBytes,
                workBytes: maxLength => maxLength + TREE_ROOT_EXPORT_PAGE_BYTES,
                parse: bytes => {
                    const hash = this.wasm.wasm_root_hash_from_bytes(bytes);
                    const version = this.wasm.wasm_root_version_from_bytes(bytes);
                    if (!hash || (version !== 1 && version !== 2)) {
                        throw new Error("cached root export produced an invalid root");
                    }
                    return { hash, version };
                },
                legacy: () => tree.root_bytes(),
                requireIncremental: true,
            });
            try {
                const writeBytes = owned.bytes.byteLength + TREE_ROOT_EXPORT_PAGE_BYTES;
                await owned.memory.run(writeBytes, () => this.app.vault.adapter.writeBinary(
                    path,
                    exactArrayBuffer(owned.bytes),
                ));
            } finally {
                owned.release();
            }
            console.log("[obsetync] cached root saved");
        } catch (e) {
            console.warn("[obsetync] failed to save cached root:", e);
        }
    }

    private isSyncInternal(path: string): boolean {
        return (
            !isSafeVaultPath(path) ||
            path.startsWith(".obsidian/plugins/obsetync/") ||
            path === ".obsetync-crash.log"
        );
    }

    /** User-configured ignore (Slice 2). */
    isIgnored(path: string): boolean {
        return this.ignore.test(path);
    }

    /** A path that must never enter sync from THIS device: the plugin's own
     *  internal files, or a user-ignored path. Used at every write-detection
     *  chokepoint (vault events, full scan, metadata scan). Applying it in the
     *  full-scan delete-detection is what stops a local `cargo clean` from
     *  propagating target/ DELETIONS to the fleet — ignored paths that vanish
     *  from disk are simply not tracked, never deleted. */
    private isExcluded(path: string): boolean {
        return this.isSyncInternal(path) || this.isIgnored(path);
    }
}
