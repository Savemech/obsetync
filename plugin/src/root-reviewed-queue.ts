import { DIRTY_CLAIM_LIMIT, DirtyPathSet, type DirtyFileChange, type DirtyPathTracking } from "./dirty-set";
import { DeferredChangeTracker, DeferredDependencyCaptureError, DeferredPartitionError,
    type DeferredPartition, type RenameDependencySnapshot } from "./deferred-changes";
import type { FileChange, DeferredPushChange } from "./push";
import type { RootLocalOmission } from "./root-local-omissions";
import { createRootBatchPlan, type RootBatchPlan } from "./root-batch-plan";
import { selectRootBatch, ROOT_BATCH_LIMITS, rootBatchPathBytesSteps, emptyRootBatchDeferrals, type RootBatchDeferrals } from "./root-batch";
import { isSafeVaultPath } from "./delta-validation";
import { throwIfWorkAborted } from "./work-scheduler";
import { PrioritySortError, type PrioritySortOptions } from "./priority-sort";
import type { SyncMemoryArbiter, SyncMemoryLease } from "./sync-memory-arbiter";

// Deterministic review-ledger model. With an injected shared arbiter its full
// ceiling is leased before owner allocation and until dispose(); it remains
// neither measured JS heap nor an RSS bound.
export const ROOT_REVIEW_LIMITS = { paths: 65_536, metadataBytes: 8 * 1024 * 1024,
    rowBytes: 160, overlayPaths: 1024, urgentPaths: 64, initialRecentPaths: 128,
    overlayRefreshes: 4 } as const;
type Kind = "upsert" | "delete" | "tracked-delete" | "excluded" | "untracked-directory";
interface Row { hint: DirtyFileChange; kind: Kind }
function sameReviewedGeneration(a: DirtyFileChange, b: DirtyFileChange): boolean {
    return a.path === b.path && a.action === b.action && a.journalId === b.journalId &&
        a.hash === b.hash && a.mtime === b.mtime && a.size === b.size;
}
export class RootReviewInvalidated extends Error {
    constructor(readonly reason: string) { super(`root review invalidated: ${reason}`); this.name = "RootReviewInvalidated"; }
}
export class RootReviewPaused extends Error {
    constructor() { super("root review requires approval"); this.name = "RootReviewPaused"; }
}
const ROOT_REVIEW_PREEMPTION_AUTHORITY = Symbol("root-review-preemption-authority");
const rootReviewPreemptionAuthorities = new WeakSet<RootReviewPreempted>();
/** Scheduling signal only. The abandoned whole owner grants no authority;
 * the engine must build and review a separate bounded prefix before use. */
export class RootReviewPreempted extends Error {
    constructor(authority: symbol) {
        super("root review preempted by recent local upserts");
        if (authority !== ROOT_REVIEW_PREEMPTION_AUTHORITY) throw new Error("invalid root review preemption authority");
        this.name = "RootReviewPreempted";
    }
}
export interface RootReviewedQueuePorts {
    dirty: DirtyPathSet;
    deferred: DeferredChangeTracker;
    materialize(hints: DirtyFileChange[], omissions: RootLocalOmission[],
        afterCooperate?: () => void): Promise<FileChange[]>;
    review(total: number, trackedDeletions: number): Promise<boolean>;
    tracked(path: string): boolean;
    sort(changes: FileChange[], options: PrioritySortOptions): FileChange[] | Promise<FileChange[]>;
    cooperate(): Promise<void>;
    assertCurrent(): void;
    activePath(): string | null;
    /** Scheduling-only callback fence. A newer local upsert that has started
     * but has not reached its durable dirty row must not dispatch the older
     * reviewed generation merely because it is currently active/urgent. */
    upsertInFlight?(path: string): boolean;
    /** Volatile scheduling provenance owned by the engine. Rebuilds may use
     * it only to prioritize an already reviewed independent upsert; it grants
     * no source, dependency, deletion-review or journal authority. */
    recentUpsert?(hint: DirtyFileChange): boolean;
    /** Bounded recent paths only; the prefix resolves every path through its
     * own full dirty capture and exact recentUpsert predicate. */
    recentUpsertPaths?(): readonly string[];
    /** Checked only while constructing the initial whole owner. Returning true
     * abandons that owner before review/claim and requests a separately
     * classified one-shot recent prefix. */
    preemptInitialReview?(): boolean;
    /** Re-check the gross full-capture policy envelope after the one-shot
     * prefix installs its mutation tracker. */
    authorizeRecentPrefix?(totalDirtyPaths: number): boolean;
    retryKey: string;
    retryDeferred: boolean;
    signal?: AbortSignal;
    /** Phase-A injection seam. Omission preserves the legacy unwired caller. */
    memoryArbiter?: SyncMemoryArbiter;
}
export interface RootReviewedSelection {
    kind: "root" | "local";
    queued: DirtyFileChange[];
    ready: FileChange[];
    selectedGroups: readonly (readonly string[])[];
    estimatedMetadataBytes: number;
    cutCount: number;
    deferred: RootBatchDeferrals;
}
export interface RootReviewedQueueSnapshot {
    disposed: boolean;
    rows: number;
    urgent: number;
    cooling: number;
    /** Deterministic review charge only, not measured or admitted JS heap. */
    reviewMetadataBytes: number;
    /** Current static plan disposition, separate from live overlay counts. */
    deferred: RootBatchDeferrals;
}

/** One drain-local reviewed cut, not a durable transfer plan or ACK owner.
 * Ordinary independent upserts update a bounded coalesced overlay; structural
 * changes/new deletions require a new full audit. All selected sources are
 * freshly materialized after exact claims. Failed work remains in dirty/WAL.
 * The engine owns restoration, journal ACKs, outcome recovery and base fences. */
export class RootReviewedQueue {
    private readonly rows = new Map<string, Row>();
    private readonly ids = new Map<number, string | Set<string>>();
    private readonly urgent = new Set<string>();
    private readonly localOverlay = new Set<string>();
    private readonly cooling = new Set<string>();
    /** At most one explicit retry opportunity per initially cooled source.
     * Unrelated/local batches cannot consume another source's opportunity. */
    private readonly forcedPaths = new Set<string>();
    /** A fresh materialized fingerprint can make an old cooled dirty hint
     * normally eligible. Keep that finite opportunity with its exact row,
     * not by replacing the hint or granting it new journal authority. */
    private readonly reviewedRetryPaths = new Set<string>();
    private readonly localPaths: string[] = [];
    private localCursor = 0;
    private plan: RootBatchPlan | null = null;
    private total = 0;
    private deletes = 0;
    private charge = 0;
    private disposed = false;
    private rebuildAfterCooling = false;
    private preferUrgent = true;
    private driftRetries = 0;
    private readonly driftRetryRows = new Map<string, DirtyFileChange[]>();
    private dependencyRevision: object;
    /** Temporary cooldown/dependency fence for one preparation only. Own
     * accepted settlements may change this state after the plan exists. */
    private preparationRevision?: object;
    private constructor(private readonly ports: RootReviewedQueuePorts, private readonly tracking: DirtyPathTracking,
        private readonly recentPrefix: boolean, private initialPreemptible: boolean,
        private memoryLease?: SyncMemoryLease) {
        this.dependencyRevision = ports.deferred.dependencyRevision;
    }

    static async create(ports: RootReviewedQueuePorts): Promise<RootReviewedQueue> {
        return this.createInternal(ports, false);
    }

    /** Build a one-shot queue from exact completed recent upserts. The full
     * dirty capture is still scanned for shared journal IDs and tracked for
     * later mutations. Missing paths, deletes, links, cooling sources and
     * in-flight callbacks remain dirty for the mandatory whole rebuild. */
    static async createRecentPrefix(ports: RootReviewedQueuePorts,
        authority: RootReviewPreempted): Promise<RootReviewedQueue> {
        if (!rootReviewPreemptionAuthorities.delete(authority)) {
            throw new RootReviewInvalidated("recent upsert prefix lacks one-shot authority");
        }
        return this.createInternal(ports, true);
    }

    private static async createInternal(ports: RootReviewedQueuePorts, recentPrefix: boolean): Promise<RootReviewedQueue> {
        let memoryLease: SyncMemoryLease | undefined;
        let owner: RootReviewedQueue | undefined;
        try {
            if (ports.memoryArbiter) memoryLease = await ports.memoryArbiter.reserve("root-review",
                ROOT_REVIEW_LIMITS.metadataBytes, { signal: ports.signal });
            throwIfWorkAborted(ports.signal);
            const tracking = ports.dirty.captureTracking(ROOT_REVIEW_LIMITS.overlayPaths);
            owner = new RootReviewedQueue(ports, tracking, recentPrefix, !recentPrefix, memoryLease); memoryLease = undefined;
            owner.assertCurrent();
            if (tracking.capture.size > ROOT_REVIEW_LIMITS.paths) throw new RootReviewInvalidated("review metadata admission");
            if (recentPrefix && ports.authorizeRecentPrefix?.(tracking.capture.size) !== true) {
                throw new RootReviewInvalidated("recent upsert prefix policy changed");
            }
            const queued: DirtyFileChange[] = [];
            let visited = 0;
            if (recentPrefix) {
                const paths = ports.recentUpsertPaths?.();
                if (!Array.isArray(paths) || paths.length > ROOT_REVIEW_LIMITS.overlayPaths) {
                    throw new RootReviewInvalidated("recent upsert path admission");
                }
                const unique = new Set<string>();
                for (const path of paths) {
                    if (!isSafeVaultPath(path) || unique.has(path)) {
                        throw new RootReviewInvalidated("recent upsert path owner changed");
                    }
                    unique.add(path);
                    const hint = tracking.capture.get(path);
                    if (hint && ports.recentUpsert?.(hint) && queued.length < ROOT_REVIEW_LIMITS.initialRecentPaths) {
                        owner.put(hint, "excluded"); queued.push(hint);
                    }
                    if (++visited % 256 === 0) await owner.cooperate();
                }
            } else for (const hint of tracking.capture.iterate()) {
                owner.put(hint, "excluded"); queued.push(hint);
                if (++visited % 256 === 0) await owner.cooperate();
            }
            if (recentPrefix && !queued.length) throw new RootReviewInvalidated("recent upsert prefix is empty");
            const sharedPrefixIds = new Set<number>();
            if (recentPrefix) {
                const selectedIds = new Set(queued.flatMap(hint => hint.journalId === undefined ? [] : [hint.journalId]));
                const seenIds = new Set<number>();
                visited = 0;
                for (const hint of tracking.capture.iterate()) {
                    const id = hint.journalId;
                    if (id !== undefined && selectedIds.has(id)) {
                        if (seenIds.has(id)) sharedPrefixIds.add(id); else seenIds.add(id);
                    }
                    if (++visited % 256 === 0) await owner.cooperate();
                }
            }
            const omissions: RootLocalOmission[] = [];
            const materialized = await ports.materialize(queued, omissions,
                recentPrefix ? undefined : () => owner!.preemptInitialReview());
            owner.assertCurrent();
            // Provisional row kinds above are bookkeeping only. No review or
            // local omission is authorized until EVERY path is classified.
            const kinds = await owner.classify(queued, materialized, omissions);
            visited = 0;
            for (const [path, kind] of kinds) {
                owner.setKind(path, kind);
                if (++visited % 256 === 0) await owner.cooperate();
            }
            let plannedQueued = queued;
            let plannedMaterialized = materialized;
            if (recentPrefix) {
                const readyByPath = new Map(materialized.map(change => [change.path, change]));
                plannedQueued = queued.filter(hint => Number.isSafeInteger(hint.journalId) && hint.journalId! > 0 &&
                    owner!.rows.get(hint.path)?.kind === "upsert" &&
                    ports.recentUpsert?.(hint) && !ports.upsertInFlight?.(hint.path) &&
                    !owner!.linked(hint) && (hint.journalId === undefined || !sharedPrefixIds.has(hint.journalId)));
                const selected = new Set(plannedQueued.map(hint => hint.path));
                for (const path of [...owner.rows.keys()]) if (!selected.has(path)) owner.remove(path);
                plannedMaterialized = plannedQueued.map(hint => readyByPath.get(hint.path)!).filter(Boolean);
                if (!plannedQueued.length || plannedMaterialized.length !== plannedQueued.length) {
                    throw new RootReviewInvalidated("recent upsert prefix has no independent source");
                }
            }
            if (!await ports.review(owner.total, owner.deletes)) throw new RootReviewPaused();
            owner.assertCurrent();
            const preparation = owner.beginPreparation();
            try {
                const now = Date.now();
                const normal = await owner.partition(plannedQueued, plannedMaterialized, now, false);
                // Explicit retry makes sources eligible, not already successful.
                // Keep the known global delete hold until they are actually tried
                // in the same candidate or complete an accepted settlement.
                await owner.recordReviewedEligibility(plannedQueued, normal.ready, now,
                    recentPrefix ? false : ports.retryDeferred);
                let partition = !recentPrefix && ports.retryDeferred
                    ? await owner.partition(plannedQueued, plannedMaterialized, now, true) : normal;
                if (recentPrefix) {
                    const runnable = new Set(normal.ready.map(change => change.path));
                    plannedQueued = plannedQueued.filter(hint => runnable.has(hint.path) && !owner!.cooling.has(hint.path));
                    const selected = new Set(plannedQueued.map(hint => hint.path));
                    for (const path of [...owner.rows.keys()]) if (!selected.has(path)) owner.remove(path);
                    partition = { ready: normal.ready.filter(change => selected.has(change.path)), deferred: normal.deferred };
                    if (!plannedQueued.length) throw new RootReviewInvalidated("recent upsert prefix is not runnable");
                }
                for (const [path, row] of owner.rows) {
                    if (owner.isOmission(row.kind)) owner.localPaths.push(path);
                    else if (row.kind === "upsert" && owner.urgent.size < ROOT_REVIEW_LIMITS.overlayPaths &&
                        ports.recentUpsert?.(row.hint)) owner.urgent.add(path);
                    if (++visited % 256 === 0) await owner.cooperate();
                }
                const ready = await owner.sort(partition.ready);
                owner.assertCurrent();
                const dependencies = await owner.captureDependencies();
                owner.assertCurrent();
                owner.plan = await createRootBatchPlan(ready, plannedQueued, dependencies,
                    { cooperate: () => owner!.cooperate(), signal: ports.signal, memoryArbiter: ports.memoryArbiter,
                        memoryWait: false });
                owner.assertCurrent();
            } finally {
                owner.endPreparation(preparation);
            }
            owner.initialPreemptible = false;
            if (recentPrefix && owner.tracking.snapshot().pendingPaths > 0) {
                throw new RootReviewInvalidated("recent upsert prefix observed a newer dirty generation");
            }
            return owner;
        } catch (error) { owner?.dispose(); memoryLease?.release(); throw error; }
    }

    private isOmission(kind: Kind): boolean { return kind === "excluded" || kind === "untracked-directory"; }
    private async recordReviewedEligibility(hints: readonly DirtyFileChange[], normallyReady: readonly FileChange[],
        now: number, allowForced: boolean): Promise<void> {
        let visited = 0;
        for (const hint of hints) {
            if (this.rows.get(hint.path)?.kind === "upsert" &&
                !this.ports.deferred.hasRunnable([hint], this.ports.retryKey, now, false)) {
                // Fresh stat can invalidate the suppression fingerprint, but
                // it is not proof the previously refused source now fits.
                this.cooling.add(hint.path);
                if (allowForced) this.forcedPaths.add(hint.path);
            }
            if (++visited % 256 === 0) await this.cooperate();
        }
        for (const change of normallyReady) {
            const row = this.rows.get(change.path);
            if (row?.kind === "upsert" && this.cooling.has(change.path) &&
                !this.ports.deferred.hasRunnable([row.hint], this.ports.retryKey, now, false)) {
                this.reviewedRetryPaths.add(change.path);
            }
            if (++visited % 256 === 0) await this.cooperate();
        }
    }
    private kind(change: FileChange): Kind {
        return change.action !== "deleted" ? "upsert" : this.ports.tracked(change.path) ? "tracked-delete" : "delete";
    }
    /** Absence is not an exclusion. Exactly one positive classification must
     * cover each input path, including whole review, overlay and selected cut.
     * This also rejects malformed port output before it can become local ACK
     * permission or undercount the full deletion review. */
    private async classify(hints: readonly DirtyFileChange[], materialized: readonly FileChange[],
        omissions: readonly RootLocalOmission[]): Promise<Map<string, Kind>> {
        this.assertCurrent();
        if (!Array.isArray(hints) || !Array.isArray(materialized) || !Array.isArray(omissions)) {
            throw new RootReviewInvalidated("invalid classification collections");
        }
        const count = hints.length, readyCount = materialized.length, omittedCount = omissions.length;
        if (count > ROOT_REVIEW_LIMITS.paths || readyCount + omittedCount !== count) {
            throw new RootReviewInvalidated("incomplete or duplicate source classification");
        }
        const kinds = new Map<string, Kind | undefined>();
        let visited = 0;
        for (let index = 0; index < count; index++) {
            const path = hints[index]?.path;
            if (!isSafeVaultPath(path) || kinds.has(path)) throw new RootReviewInvalidated("invalid classification owner");
            kinds.set(path, undefined);
            if (++visited % 256 === 0) await this.cooperate();
        }
        const record = (path: string, kind: Kind) => {
            if (!isSafeVaultPath(path) || !kinds.has(path) || kinds.get(path) !== undefined) {
                throw new RootReviewInvalidated("duplicate or unowned source classification");
            }
            kinds.set(path, kind);
        };
        for (let index = 0; index < readyCount; index++) {
            const change = materialized[index];
            if (!change || typeof change !== "object" || !isSafeVaultPath(change.path) ||
                !["created", "modified", "deleted"].includes(change.action) ||
                (change.mtime !== undefined && !Number.isFinite(change.mtime)) ||
                (change.size !== undefined && (!Number.isSafeInteger(change.size) || change.size < 0)) ||
                (change.hash !== undefined && (typeof change.hash !== "string" || !/^[0-9a-f]{64}$/.test(change.hash))) ||
                "data" in change) throw new RootReviewInvalidated("invalid materialized classification");
            record(change.path, this.kind(change));
            if (++visited % 256 === 0) await this.cooperate();
        }
        for (let index = 0; index < omittedCount; index++) {
            const omission = omissions[index];
            if (!omission || (omission.reason !== "excluded" && omission.reason !== "untracked-directory")) {
                throw new RootReviewInvalidated("invalid explicit omission classification");
            }
            record(omission.path, omission.reason);
            if (++visited % 256 === 0) await this.cooperate();
        }
        if (hints.length !== count || materialized.length !== readyCount || omissions.length !== omittedCount) {
            throw new RootReviewInvalidated("classification collections changed");
        }
        this.assertCurrent();
        // Exact count + unique owned results proves no undefined row remains.
        return kinds as Map<string, Kind>;
    }
    private setKind(path: string, kind: Kind): void {
        const row = this.rows.get(path);
        if (!row) throw new RootReviewInvalidated("materialized path lacks reviewed owner");
        this.total += Number(!this.isOmission(kind)) - Number(!this.isOmission(row.kind));
        this.deletes += Number(kind === "tracked-delete") - Number(row.kind === "tracked-delete");
        row.kind = kind;
    }
    private put(hint: DirtyFileChange, kind: Kind): void {
        if (!isSafeVaultPath(hint.path)) throw new RootReviewInvalidated("unsafe review path");
        const previous = this.rows.get(hint.path);
        if (previous) this.remove(hint.path);
        const bytes = ROOT_REVIEW_LIMITS.rowBytes + hint.path.length * 2;
        if (this.rows.size >= ROOT_REVIEW_LIMITS.paths || bytes > ROOT_REVIEW_LIMITS.metadataBytes - this.charge) {
            throw new RootReviewInvalidated("review metadata admission");
        }
        this.charge += bytes;
        this.rows.set(hint.path, { hint, kind });
        this.total += Number(!this.isOmission(kind)); this.deletes += Number(kind === "tracked-delete");
        if (hint.journalId !== undefined) {
            const peer = this.ids.get(hint.journalId);
            if (peer === undefined) this.ids.set(hint.journalId, hint.path);
            else if (typeof peer === "string") this.ids.set(hint.journalId, new Set([peer, hint.path]));
            else peer.add(hint.path);
        }
    }
    private remove(path: string): void {
        const row = this.rows.get(path); if (!row) return;
        this.total -= Number(!this.isOmission(row.kind)); this.deletes -= Number(row.kind === "tracked-delete");
        this.charge -= ROOT_REVIEW_LIMITS.rowBytes + path.length * 2;
        this.rows.delete(path); this.urgent.delete(path); this.localOverlay.delete(path); this.forcedPaths.delete(path);
        this.reviewedRetryPaths.delete(path);
        if (row.hint.journalId !== undefined) {
            const peers = this.ids.get(row.hint.journalId);
            if (typeof peers === "string") this.ids.delete(row.hint.journalId);
            else if (peers) {
                peers.delete(path);
                if (peers.size === 1) this.ids.set(row.hint.journalId, peers.values().next().value!);
                else if (!peers.size) this.ids.delete(row.hint.journalId);
            }
        }
    }
    private linked(hint: DirtyFileChange): boolean {
        return this.ports.deferred.hasLinkedPath(hint.path) ||
            (hint.journalId !== undefined && this.ids.get(hint.journalId) instanceof Set);
    }
    private assertCurrent(): void {
        throwIfWorkAborted(this.ports.signal);
        if (this.disposed) throw new RootReviewInvalidated("disposed owner");
        this.ports.assertCurrent();
        if (this.dependencyRevision !== this.ports.deferred.dependencyRevision) throw new RootReviewInvalidated("rename dependency changed");
        if (this.preparationRevision && this.preparationRevision !== this.ports.deferred.planningRevision) {
            throw new RootReviewInvalidated("deferred planning state changed");
        }
        const state = this.tracking.snapshot();
        if (!state.active || state.overflow) throw new RootReviewInvalidated("dirty overlay overflow/owner change");
    }
    private async cooperate(): Promise<void> {
        this.assertCurrent(); await this.ports.cooperate(); this.assertCurrent();
        this.preemptInitialReview();
    }
    private preemptInitialReview(): void {
        this.assertCurrent();
        if (!this.initialPreemptible || !this.ports.preemptInitialReview?.()) return;
        this.initialPreemptible = false;
        const authority = new RootReviewPreempted(ROOT_REVIEW_PREEMPTION_AUTHORITY);
        rootReviewPreemptionAuthorities.add(authority);
        throw authority;
    }
    private beginPreparation(): object {
        this.assertCurrent();
        if (this.preparationRevision) throw new RootReviewInvalidated("nested deferred preparation");
        return this.preparationRevision = this.ports.deferred.planningRevision;
    }
    private endPreparation(revision: object): void {
        if (this.preparationRevision === revision) this.preparationRevision = undefined;
    }
    private async partition(queued: readonly DirtyFileChange[], materialized: readonly FileChange[],
        now: number, force: boolean): Promise<DeferredPartition> {
        try {
            return await this.ports.deferred.partitionCooperatively(
                queued, materialized, this.ports.retryKey, now, force,
                { cooperate: () => this.cooperate(), signal: this.ports.signal },
            );
        } catch (error) {
            if (error instanceof DeferredPartitionError) {
                throw new RootReviewInvalidated(error.code === "DEFERRED_PARTITION_ADMISSION"
                    ? "deferred partition admission" : "deferred planning state changed");
            }
            throw error;
        }
    }
    private async captureDependencies(): Promise<readonly RenameDependencySnapshot[]> {
        try {
            return await this.ports.deferred.captureDependenciesCooperatively({
                cooperate: () => this.cooperate(), signal: this.ports.signal,
            });
        } catch (error) {
            if (error instanceof DeferredDependencyCaptureError) {
                throw new RootReviewInvalidated(error.code === "DEFERRED_DEPENDENCIES_ADMISSION"
                    ? "deferred dependency admission" : "rename dependency changed");
            }
            throw error;
        }
    }
    private async sort(changes: FileChange[]): Promise<FileChange[]> {
        try {
            return await this.ports.sort(changes, {
                cooperate: () => this.cooperate(), signal: this.ports.signal,
            });
        } catch (error) {
            if (error instanceof PrioritySortError) {
                throw new RootReviewInvalidated(error.code === "PRIORITY_SORT_ADMISSION" ? "priority sort admission" :
                    error.code === "PRIORITY_SORT_RANDOM" ? "priority sort random source" : "priority sort input changed");
            }
            throw error;
        }
    }
    /** Check before dispatch. A changed structural witness never licenses a
     * group built from a different graph, even if selected paths look equal. */
    assertApplicable(): void { this.assertCurrent(); }
    dependenciesCurrent(): boolean { return this.dependencyRevision === this.ports.deferred.dependencyRevision; }
    /** Pending finite retry opportunities, not proof of source/group eligibility. */
    hasForcedRetries(): boolean { return !this.disposed && this.forcedPaths.size > 0; }
    /** Unlike the pre-stat hint gate, a completed review can know that the
     * source fingerprint changed. These finite slots keep the drain alive
     * across unrelated batches; only exact source claims consume them. */
    hasRunnableReviewed(): boolean {
        return !this.disposed && (this.forcedPaths.size > 0 || this.reviewedRetryPaths.size > 0);
    }

    /** A source/generation drift before root dispatch invalidates only the
     * selected independent upserts, not the reviewed static graph. The engine
     * restores their exact dirty owners first; retain this bounded selection
     * (and every other live overlay path) as urgent for the next invocation.
     * Structural/link changes and root-attempt recovery never use this seam. */
    retainFailedIndependentSelection(hints: readonly DirtyFileChange[]): boolean {
        this.assertCurrent();
        // Do not retry an entirely unchanged reviewed selection twice. A
        // mixed selection may carry unchanged peers beside a newly observed
        // generation, but both queue and whole-drain retry counts stay finite.
        if (this.driftRetries >= ROOT_REVIEW_LIMITS.overlayRefreshes || !Array.isArray(hints)) return false;
        const count = hints.length;
        if (!Number.isSafeInteger(count) || count === 0 || count > DIRTY_CLAIM_LIMIT) return false;
        const paths: string[] = [], rows: DirtyFileChange[] = [], unique = new Set<string>();
        let additional = 0;
        for (let index = 0; index < count; index++) {
            const hint = hints[index];
            const path = hint?.path, row = this.rows.get(path);
            if (!isSafeVaultPath(path) || unique.has(path) || !row || !sameReviewedGeneration(row.hint, hint) || row.kind !== "upsert" ||
                this.linked(row.hint) || this.cooling.has(path) || !this.ports.dirty.has(path)) return false;
            unique.add(path); paths.push(path); rows.push(row.hint);
            if (!this.urgent.has(path)) additional++;
        }
        if (hints.length !== count) return false;
        if (rows.every(row => this.driftRetryRows.get(row.path)?.some(seen => sameReviewedGeneration(seen, row)))) return false;
        if (this.urgent.size + this.localOverlay.size + additional > ROOT_REVIEW_LIMITS.overlayPaths) return false;
        for (const path of paths) this.urgent.add(path);
        for (const row of rows) {
            const seen = this.driftRetryRows.get(row.path);
            if (seen) seen.push(row); else this.driftRetryRows.set(row.path, [row]);
        }
        this.preferUrgent = true;
        this.driftRetries++;
        return true;
    }

    private async updateOverlay(): Promise<void> {
        this.assertCurrent();
        const delta = this.tracking.drain();
        if (!delta.active || delta.overflow) throw new RootReviewInvalidated("dirty overlay overflow/owner change");
        if (!delta.paths.length) return;
        const hints: DirtyFileChange[] = [];
        for (const path of delta.paths) {
            const hint = delta.capture.get(path), old = this.rows.get(path);
            if (old && this.linked(old.hint)) throw new RootReviewInvalidated("linked generation changed");
            if (!hint) { this.remove(path); continue; }
            hints.push(hint);
        }
        const omissions: RootLocalOmission[] = [], changes = await this.ports.materialize(hints, omissions);
        this.assertCurrent();
        const kinds = await this.classify(hints, changes, omissions);
        for (const hint of hints) {
            const kind = kinds.get(hint.path);
            if (!kind) throw new RootReviewInvalidated("unclassified overlay path");
            if (kind === "tracked-delete" && this.rows.get(hint.path)?.kind !== kind) throw new RootReviewInvalidated("new tracked deletion");
            this.put(hint, kind);
            if (this.linked(hint)) throw new RootReviewInvalidated("linked overlay generation");
            if (this.isOmission(kind)) this.localOverlay.add(hint.path);
            else if (kind === "upsert") this.urgent.add(hint.path);
            else throw new RootReviewInvalidated("overlay deletion needs graph rebuild");
        }
        if (this.urgent.size + this.localOverlay.size > ROOT_REVIEW_LIMITS.overlayPaths) throw new RootReviewInvalidated("pending overlay admission");
        if (!await this.ports.review(this.total, this.deletes)) throw new RootReviewPaused();
        this.assertCurrent();
        const preparation = this.beginPreparation();
        try {
            const now = Date.now();
            const normal = await this.partition(hints, changes, now, false);
            // A new row cannot inherit an old explicit retry slot. Its fresh
            // materialized fingerprint may establish its own normal eligibility.
            await this.recordReviewedEligibility(hints, normal.ready, now, false);
            this.assertCurrent();
        } finally {
            this.endPreparation(preparation);
        }
    }

    private claim(candidate: readonly DirtyFileChange[], groups: readonly (readonly string[])[], local = false): DirtyFileChange[] {
        const candidateByPath = new Map(candidate.map(hint => [hint.path, hint]));
        const grouped = new Set(groups.flat());
        const components = [
            ...groups.map(paths => ({ paths, structural: true })),
            ...candidate.filter(hint => !grouped.has(hint.path)).map(hint => ({ paths: [hint.path], structural: false })),
        ];
        const held = new Set<string>();
        for (const component of components) {
            const hints = component.paths.map(path => candidateByPath.get(path)!);
            if (hints.some(hint => !hint || this.rows.get(hint.path)?.hint !== hint)) continue;
            if (local || !hints.some(hint => this.ports.upsertInFlight?.(hint.path))) continue;
            const hint = hints[0], row = this.rows.get(hint.path);
            if (component.structural || hints.length !== 1 || row?.kind !== "upsert" ||
                this.linked(hint) || this.cooling.has(hint.path)) {
                throw new RootReviewInvalidated("in-flight reviewed component changed");
            }
            if (!this.urgent.has(hint.path)) {
                if (this.urgent.size + this.localOverlay.size >= ROOT_REVIEW_LIMITS.overlayPaths) {
                    throw new RootReviewInvalidated("pending overlay admission");
                }
                this.urgent.add(hint.path);
            }
            held.add(hint.path);
        }
        const available = candidate.filter(hint => !held.has(hint.path));
        const byPath = new Map(available.map(hint => [hint.path, hint]));
        const availableComponents = components.filter(component =>
            !component.paths.some(path => held.has(path)));
        // Positive local omissions need no source allocation/retry. Their
        // exact generation and nonlinked classification gates remain below.
        const runnable = (hint: DirtyFileChange) => local || this.ports.deferred.hasRunnable([hint], this.ports.retryKey,
            Date.now(), this.forcedPaths.has(hint.path) || this.reviewedRetryPaths.has(hint.path));
        const consume = (hint: DirtyFileChange) => {
            this.forcedPaths.delete(hint.path); this.reviewedRetryPaths.delete(hint.path);
        };
        const hasHeldDelete = !local && this.cooling.size > 0 &&
            available.some(hint => this.rows.get(hint.path)?.kind !== "upsert");
        const coversCooling = hasHeldDelete && this.cooling.size <= byPath.size &&
            [...this.cooling].every(path => byPath.has(path));
        if (coversCooling && available.every(hint => this.rows.get(hint.path)?.hint === hint && runnable(hint))) {
            // No partial source claim here: deletes are eligible only if the
            // SAME atomic candidate owns every currently cooled source. Push
            // must still hold all selected deletes/groups on source deferral.
            const claimed = this.ports.dirty.claimHints(available);
            if (!claimed) throw new RootReviewInvalidated("cooling retry candidate changed");
            for (const hint of claimed) consume(hint);
            return claimed;
        }
        const claimed: DirtyFileChange[] = [];
        try {
            for (const { paths } of availableComponents) {
                const hints = paths.map(path => byPath.get(path)!);
                if (hints.some(hint => this.rows.get(hint.path)?.hint !== hint)) continue;
                if (!local && hints.some(hint => this.cooling.size > 0 && this.rows.get(hint.path)?.kind !== "upsert")) continue;
                if (hints.some(hint => !runnable(hint))) continue;
                const selected = this.ports.dirty.claimHints(hints);
                if (selected) {
                    claimed.push(...selected);
                    if (!local) for (const hint of selected) consume(hint);
                }
            }
        } catch (error) { this.ports.dirty.restore(claimed); throw error; }
        return claimed;
    }
    private takeUrgent(): DirtyFileChange[] {
        const candidates: DirtyFileChange[] = [];
        const active = this.ports.activePath();
        const add = (path: string) => {
            // Keep the exact reviewed row in the urgent overlay until the
            // callback replaces it with its durable final generation. This is
            // priority only: source, journal and dependency authority remain
            // with the ordinary claim/materialization path below.
            if (this.ports.upsertInFlight?.(path)) return;
            const row = this.rows.get(path);
            if (row?.kind === "upsert" && !this.linked(row.hint)) candidates.push(row.hint);
            this.urgent.delete(path);
        };
        if (active && this.urgent.has(active)) add(active);
        for (const path of this.urgent) {
            if (candidates.length >= ROOT_REVIEW_LIMITS.urgentPaths) break;
            add(path);
        }
        // Independent overlay work still obeys root byte/cut limits.
        if (candidates.length === 0) return [];
        const batch = selectRootBatch(candidates, candidates);
        for (const hint of batch.retained) this.urgent.add(hint.path);
        return this.claim(batch.queued, []);
    }
    private async takeLocal(): Promise<DirtyFileChange[]> {
        const candidates: DirtyFileChange[] = [];
        const add = (path: string) => {
            const row = this.rows.get(path);
            if (row && this.isOmission(row.kind) && row.hint.journalId !== undefined && !this.linked(row.hint)) candidates.push(row.hint);
        };
        for (const path of this.localOverlay) {
            if (candidates.length >= 256) break;
            this.localOverlay.delete(path); add(path);
        }
        while (this.localCursor < this.localPaths.length && candidates.length < 256) {
            add(this.localPaths[this.localCursor++]);
            if (this.localCursor % 256 === 0) await this.cooperate();
        }
        return this.claim(candidates, [], true);
    }

    async next(): Promise<RootReviewedSelection | null> {
        if (this.rebuildAfterCooling) throw new RootReviewInvalidated("cooling completed; delete graph rebuild required");
        if (this.recentPrefix && this.tracking.snapshot().pendingPaths > 0) {
            throw new RootReviewInvalidated("recent upsert prefix observed a newer dirty generation");
        }
        let refreshes = 0;
        const refreshOverlay = async () => {
            if (++refreshes > ROOT_REVIEW_LIMITS.overlayRefreshes) {
                throw new RootReviewInvalidated("continuous dirty overlay changes");
            }
            await this.updateOverlay();
        };
        if (!this.recentPrefix) await this.updateOverlay();
        for (;;) {
            this.assertCurrent();
            // A late edit can invalidate the last old plan row while this
            // invocation yields. Re-drain before reusing/exhausting that plan,
            // but never spin forever if edits arrive after every host turn.
            if (this.tracking.snapshot().pendingPaths > 0) {
                if (this.recentPrefix) throw new RootReviewInvalidated("recent upsert prefix observed a newer dirty generation");
                await refreshOverlay(); continue;
            }
            let queued: DirtyFileChange[] = [], groups: readonly (readonly string[])[] = [], kind: "root" | "local" = "root";
            let readyOrder: readonly FileChange[] | null = null;
            if (this.preferUrgent) queued = this.takeUrgent();
            if (!queued.length) { queued = await this.takeLocal(); if (queued.length) kind = "local"; }
            if (!queued.length) {
                const batch = this.plan!.next();
                if (batch) {
                    queued = this.claim(batch.queued, batch.selectedGroups); groups = batch.selectedGroups;
                    if (!queued.length) { await this.cooperate(); continue; }
                    readyOrder = batch.ready;
                } else queued = this.takeUrgent();
            }
            if (!queued.length) {
                if (this.tracking.snapshot().pendingPaths > 0) {
                    if (this.recentPrefix) throw new RootReviewInvalidated("recent upsert prefix observed a newer dirty generation");
                    await refreshOverlay(); continue;
                }
                if (this.recentPrefix) {
                    throw new RootReviewInvalidated("recent upsert prefix exhausted; full review required");
                }
                return null;
            }
            try {
                if (readyOrder) {
                    // Graph claims retain dirty insertion order; source work
                    // retains the user's reviewed priority within this cut.
                    // This bounded lookup neither copies nor changes proofs.
                    const byPath = new Map(queued.map(hint => [hint.path, hint]));
                    const ordered: DirtyFileChange[] = [];
                    for (const change of readyOrder) {
                        const hint = byPath.get(change.path);
                        if (hint) { ordered.push(hint); byPath.delete(change.path); }
                    }
                    if (byPath.size) throw new RootReviewInvalidated("selected priority owner missing");
                    queued = ordered;
                }
                const omissions: RootLocalOmission[] = [];
                const ready = await this.ports.materialize(queued, omissions);
                this.assertCurrent();
                const kinds = await this.classify(queued, ready, omissions);
                for (const hint of queued) if (kinds.get(hint.path) !== this.rows.get(hint.path)?.kind) {
                    throw new RootReviewInvalidated("selected source classification changed");
                }
                const selectedPaths = new Set(queued.map(hint => hint.path));
                const selectedGroups = groups.filter(group => group.every(path => selectedPaths.has(path)));
                this.preferUrgent = !this.preferUrgent;
                const summary = this.plan!.snapshot();
                let estimatedMetadataBytes = kind === "local" ? 0 : ROOT_BATCH_LIMITS.headerBytes;
                if (kind === "root") for (const hint of queued) {
                    const scan = rootBatchPathBytesSteps(hint.path);
                    let step = scan.next(); while (!step.done) step = scan.next();
                    estimatedMetadataBytes += ROOT_BATCH_LIMITS.entryFixedBytes + step.value;
                    if (hint.journalId !== undefined) estimatedMetadataBytes += ROOT_BATCH_LIMITS.cutFixedBytes + step.value;
                }
                if (estimatedMetadataBytes > ROOT_BATCH_LIMITS.metadataBytes) throw new RootReviewInvalidated("selected root metadata admission");
                return { kind, queued, ready, selectedGroups, estimatedMetadataBytes,
                    cutCount: queued.filter(hint => hint.journalId !== undefined).length, deferred: summary.deferred };
            } catch (error) { this.ports.dirty.restore(queued); throw error; }
        }
    }

    /** ONLY after actual accepted/local ACK tail. Structural stability is
     * captured before this engine retires its own confirmed dependency links. */
    settled(paths: ReadonlySet<string>, deferred: readonly DeferredPushChange[], dependenciesWereCurrent: boolean): void {
        const hadCooling = this.cooling.size > 0;
        for (const path of paths) { this.remove(path); this.cooling.delete(path); }
        for (const value of deferred) {
            if (value.reason === "source-too-large" || value.reason === "range-unavailable") {
                this.cooling.add(value.path);
            }
        }
        // The static graph excluded deletes held by these sources. Reusing
        // it after the last source succeeds would falsely exhaust the queue.
        // Invalidate the NEXT selection, not the already-successful ACK tail.
        if (hadCooling && this.cooling.size === 0) this.rebuildAfterCooling = true;
        if (!dependenciesWereCurrent) { this.dispose(); return; }
        this.dependencyRevision = this.ports.deferred.dependencyRevision;
    }
    snapshot(): RootReviewedQueueSnapshot {
        return { disposed: this.disposed, rows: this.rows.size, urgent: this.urgent.size, cooling: this.cooling.size,
            reviewMetadataBytes: this.charge, deferred: this.plan?.snapshot().deferred ?? emptyRootBatchDeferrals() };
    }
    dispose(): void {
        if (this.disposed) return;
        this.disposed = true; this.tracking.dispose(); this.plan?.dispose(); this.plan = null;
        this.rows.clear(); this.ids.clear(); this.urgent.clear(); this.localOverlay.clear(); this.cooling.clear();
        this.forcedPaths.clear(); this.reviewedRetryPaths.clear(); this.driftRetryRows.clear(); this.localPaths.length = 0;
        this.charge = 0; this.total = 0; this.deletes = 0;
        this.memoryLease?.release(); this.memoryLease = undefined;
    }
}
