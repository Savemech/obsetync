import type { DirtyFileChange } from "./dirty-set";
import type { DeferredPushChange, FileChange } from "./push";
import { throwIfWorkAborted } from "./work-scheduler";

export const DEFERRED_CHANGE_RETRY_MS = 60_000;
export const DEFERRED_PARTITION_WORK_UNITS = 256;
export const DEFERRED_PARTITION_MAX_DEPENDENCIES = 65_536;
export const DEFERRED_DEPENDENCY_CAPTURE_WORK_UNITS = 256;
const DEFERRED_PARTITION_MAX_VISITED_PATHS = 2 * DEFERRED_PARTITION_MAX_DEPENDENCIES;
const DEFERRED_PARTITION_MAX_EDGE_VISITS = 2 * DEFERRED_PARTITION_MAX_DEPENDENCIES;

export interface DeferredPartition {
    ready: FileChange[];
    deferred: DeferredPushChange[];
}

export interface DeferredPartitionOptions {
    /** MUST yield to the actual host; Promise.resolve alone is not UI fairness. */
    cooperate(): Promise<void>;
    signal?: AbortSignal;
}

export class DeferredPartitionError extends Error {
    constructor(readonly code: "DEFERRED_PARTITION_CHANGED" | "DEFERRED_PARTITION_ADMISSION") {
        super(code === "DEFERRED_PARTITION_CHANGED"
            ? "deferred partition state changed"
            : "deferred partition dependency traversal admission rejected");
        this.name = "DeferredPartitionError";
    }
}

export interface DeferredDependencyCaptureOptions {
    /** MUST yield to the actual host; Promise.resolve alone is not UI fairness. */
    cooperate(): Promise<void>;
    signal?: AbortSignal;
    /** May lower, never raise, the root planner's dependency ceiling. */
    maxDependencies?: number;
}

export class DeferredDependencyCaptureError extends Error {
    constructor(readonly code: "DEFERRED_DEPENDENCIES_CHANGED" | "DEFERRED_DEPENDENCIES_ADMISSION") {
        super(code === "DEFERRED_DEPENDENCIES_CHANGED"
            ? "deferred dependencies changed during capture"
            : "deferred dependency snapshot admission rejected");
        this.name = "DeferredDependencyCaptureError";
    }
}

interface DeferredRecord {
    hint: DirtyFileChange;
    current: DirtyFileChange;
    detail: DeferredPushChange;
    retryKey: string;
    retryAt: number;
}

interface LegacyRenameLink {
    left: string;
    right: string;
    generation: number;
    pendingRegistrations: number;
    uncertain: boolean;
}

/** One detached current path-pair dependency, not historical journal rows.
 * Pending/uncertain registrations must not license splitting a rename. */
export interface RenameDependencySnapshot {
    readonly left: string;
    readonly right: string;
    readonly generation: number;
    readonly pending: boolean;
    readonly uncertain: boolean;
}

export interface LegacyRenameRegistration {
    /** Undefined means append outcome is ambiguous; retain the dependency
     * conservatively until journal recovery supplies its durable generation. */
    confirm(journalId?: number): void;
}

export interface DeferredSettlement {
    retained: DirtyFileChange[];
    acknowledged: Array<{ path: string; throughId: number }>;
}

function validGeneration(generation: number | undefined): void {
    if (generation !== undefined && (!Number.isSafeInteger(generation) || generation <= 0)) {
        throw new RangeError("invalid rename journal generation");
    }
}

export interface DeferredChangeSummary {
    count: number;
    sourceTooLarge: number;
    rangeUnavailable: number;
    dependentDeletes: number;
    /** Legacy rename rows can also retain an already-published destination. */
    dependentChanges: number;
    maxRequiredBytes: number;
    minCapacityBytes: number;
    nextRetryAt: number | null;
}

function compact(change: FileChange, journalId?: number): DirtyFileChange {
    return {
        path: change.path,
        action: change.action,
        journalId,
        ...(change.hash === undefined ? {} : { hash: change.hash }),
        ...(change.mtime === undefined ? {} : { mtime: change.mtime }),
        ...(change.size === undefined ? {} : { size: change.size }),
    };
}

function sameHint(left: DirtyFileChange, right: DirtyFileChange): boolean {
    return left.action === right.action && left.journalId === right.journalId &&
        left.hash === right.hash && left.mtime === right.mtime && left.size === right.size;
}

function sameDetail(left: DeferredPushChange, right: DeferredPushChange): boolean {
    return left.path === right.path && left.reason === right.reason &&
        left.requiredBytes === right.requiredBytes && left.capacityBytes === right.capacityBytes;
}

function sameRecord(left: DeferredRecord, right: DeferredRecord): boolean {
    return sameHint(left.hint, right.hint) && sameHint(left.current, right.current) &&
        sameDetail(left.detail, right.detail) && left.retryKey === right.retryKey && left.retryAt === right.retryAt;
}

/** Retain both halves of a legacy one-row rename: its destination watermark
 * would otherwise erase the only durable record for a withheld old path.
 * Explicit path links survive newer edits replacing that shared ID. */
function* includeLinkedGenerationSteps(
    queued: readonly DirtyFileChange[],
    hints: ReadonlyMap<string, DirtyFileChange>,
    held: Map<string, DeferredPushChange>,
    links: ReadonlyMap<string, ReadonlyMap<string, LegacyRenameLink>>,
    queuedLength = queued.length,
    enforceAdmission = true,
): Generator<void, void, void> {
    const byId = new Map<number, string[]>();
    for (let index = 0; index < queuedLength; index++) {
        const change = queued[index];
        if (change.journalId !== undefined) {
            const paths = byId.get(change.journalId) ?? [];
            paths.push(change.path);
            byId.set(change.journalId, paths);
        }
        yield;
    }
    const visit: string[] = [], visited = new Set<string>();
    for (const path of held.keys()) {
        if (!visited.has(path)) {
            if (enforceAdmission && visited.size >= DEFERRED_PARTITION_MAX_VISITED_PATHS) {
                throw new DeferredPartitionError("DEFERRED_PARTITION_ADMISSION");
            }
            visited.add(path);
            visit.push(path);
        }
        yield;
    }
    let edgeVisits = 0;
    const include = (next: string) => {
        if (visited.has(next)) return;
        if (enforceAdmission && visited.size >= DEFERRED_PARTITION_MAX_VISITED_PATHS) {
            throw new DeferredPartitionError("DEFERRED_PARTITION_ADMISSION");
        }
        visited.add(next);
        visit.push(next);
        if (hints.has(next)) held.set(next, {
            path: next, reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0,
        });
    };
    for (let index = 0; index < visit.length; index++) {
        const path = visit[index];
        yield;
        // Preserve the legacy order: explicit edges first, then peers sharing
        // the detached journal generation. Iterate each edge separately so a
        // high-degree vertex cannot hide one giant synchronous spread/copy.
        for (const next of links.get(path)?.keys() ?? []) {
            if (++edgeVisits > DEFERRED_PARTITION_MAX_EDGE_VISITS && enforceAdmission) {
                throw new DeferredPartitionError("DEFERRED_PARTITION_ADMISSION");
            }
            include(next);
            yield;
        }
        const id = hints.get(path)?.journalId;
        const peers = id === undefined ? undefined : byId.get(id);
        // A same-generation group only needs one expansion, even if a legacy
        // journal restored more than two hints with the same watermark.
        if (id !== undefined) byId.delete(id);
        for (const next of peers ?? []) {
            include(next);
            yield;
        }
    }
}

function exhaust<T>(steps: Generator<void, T, void>): T {
    for (;;) {
        const step = steps.next();
        if (step.done) return step.value;
    }
}

function* dependencyCaptureSteps(
    links: ReadonlyMap<string, ReadonlyMap<string, LegacyRenameLink>>,
    maxDependencies?: number,
): Generator<void, readonly RenameDependencySnapshot[], void> {
    const captured: RenameDependencySnapshot[] = [];
    let visitedPaths = 0, edgeVisits = 0;
    for (const [path, related] of links) {
        if (maxDependencies !== undefined && ++visitedPaths > 2 * maxDependencies) {
            throw new DeferredDependencyCaptureError("DEFERRED_DEPENDENCIES_ADMISSION");
        }
        // Count even an outer row whose reverse adjacency is subsequently all
        // skipped; long reverse-only tails must still cross host checkpoints.
        yield;
        for (const link of related.values()) {
            if (maxDependencies !== undefined && ++edgeVisits > 2 * maxDependencies) {
                throw new DeferredDependencyCaptureError("DEFERRED_DEPENDENCIES_ADMISSION");
            }
            if (path === link.left) {
                if (maxDependencies !== undefined && captured.length >= maxDependencies) {
                    throw new DeferredDependencyCaptureError("DEFERRED_DEPENDENCIES_ADMISSION");
                }
                captured.push(Object.freeze({ left: link.left, right: link.right,
                    generation: link.generation, pending: link.pendingRegistrations > 0,
                    uncertain: link.uncertain }));
            }
            yield;
        }
    }
    return Object.freeze(captured);
}

function dependencyCaptureLimit(value: number | undefined): number {
    if (value === undefined) return DEFERRED_PARTITION_MAX_DEPENDENCIES;
    if (!Number.isSafeInteger(value) || value < 1 || value > DEFERRED_PARTITION_MAX_DEPENDENCIES) {
        throw new DeferredDependencyCaptureError("DEFERRED_DEPENDENCIES_ADMISSION");
    }
    return value;
}

function* partitionSteps(
    queued: readonly DirtyFileChange[],
    materialized: readonly FileChange[],
    records: ReadonlyMap<string, DeferredRecord>,
    links: ReadonlyMap<string, ReadonlyMap<string, LegacyRenameLink>>,
    retryKey: string,
    now: number,
    force: boolean,
    queuedLength = queued.length,
    materializedLength = materialized.length,
): Generator<void, DeferredPartition, void> {
    const hints = new Map<string, DirtyFileChange>();
    for (let index = 0; index < queuedLength; index++) {
        const hint = queued[index];
        hints.set(hint.path, hint);
        yield;
    }
    const held = new Map<string, DeferredPushChange>();
    if (!force) {
        for (let index = 0; index < materializedLength; index++) {
            const change = materialized[index], previous = records.get(change.path), hint = hints.get(change.path);
            if (
                (previous?.detail.reason === "source-too-large" || previous?.detail.reason === "range-unavailable") && hint &&
                change.action !== "deleted" && sameHint(hint, previous.hint) &&
                sameHint(compact(change, hint.journalId), previous.current) &&
                previous.retryKey === retryKey &&
                (previous.detail.reason === "range-unavailable" || now < previous.retryAt)
            ) held.set(change.path, { ...previous.detail });
            yield;
        }
    }
    if (held.size > 0) {
        for (let index = 0; index < materializedLength; index++) {
            const change = materialized[index];
            if (change.action === "deleted" || records.get(change.path)?.detail.reason === "dependent-delete") {
                held.set(change.path, {
                    path: change.path, reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0,
                });
            }
            yield;
        }
        yield* includeLinkedGenerationSteps(queued, hints, held, links, queuedLength);
    }
    const ready: FileChange[] = [];
    for (let index = 0; index < materializedLength; index++) {
        const change = materialized[index];
        if (!held.has(change.path)) ready.push(change);
        yield;
    }
    const deferred: DeferredPushChange[] = [];
    for (const detail of held.values()) {
        deferred.push(detail);
        yield;
    }
    return { ready, deferred };
}

/** Session-only retry suppression, never a substitute for the existing WAL.
 * One compact record per still-pending path; no contents or attempt history.
 * Summaries have fixed shape and contain no paths, hashes, or journal IDs. */
export class DeferredChangeTracker {
    private readonly records = new Map<string, DeferredRecord>();
    /** One edge per path pair, not one record per historical rename. */
    private readonly legacyLinks = new Map<string, Map<string, LegacyRenameLink>>();
    private dependencyRevisionValue: object = Object.freeze({});
    private planningRevisionValue: object = Object.freeze({});
    /** Owner-local structural witness, not a journal generation or ACK. */
    get dependencyRevision(): object { return this.dependencyRevisionValue; }
    /** Owner-local witness for cooldown records plus structural dependencies.
     * Root review uses it only while preparing a cut, never for the cached
     * queue's whole lifetime: accepted settlements legitimately update it. */
    get planningRevision(): object { return this.planningRevisionValue; }
    hasLinkedPath(path: string): boolean { return this.legacyLinks.has(path); }
    private changedPlanning(): void { this.planningRevisionValue = Object.freeze({}); }
    private changedDependencies(): void {
        this.dependencyRevisionValue = Object.freeze({});
        this.changedPlanning();
    }
    /** Failed/abandoned settlements are collectible without retaining history. */
    private readonly settlements = new WeakMap<DeferredSettlement, Map<string, number>>();

    constructor(private readonly retryMs = DEFERRED_CHANGE_RETRY_MS) {
        if (!Number.isFinite(retryMs) || retryMs <= 0) throw new RangeError("deferred retry interval must be positive");
    }

    /** Old one-row rename journals need path links even if a newer edit has
     * replaced the destination's original journal ID in the dirty map. */
    registerLegacyRename(oldPath: string, newPath: string, journalId?: number): LegacyRenameRegistration {
        validGeneration(journalId);
        if (oldPath === newPath) return { confirm: () => {} };
        let link = this.legacyLinks.get(oldPath)?.get(newPath);
        if (!link) {
            link = { left: oldPath, right: newPath, generation: 0, pendingRegistrations: 0, uncertain: false };
            for (const [from, to] of [[oldPath, newPath], [newPath, oldPath]]) {
                const related = this.legacyLinks.get(from) ?? new Map<string, LegacyRenameLink>();
                related.set(to, link);
                this.legacyLinks.set(from, related);
            }
            this.changedDependencies();
        }
        if (journalId !== undefined) {
            const next = Math.max(link.generation, journalId);
            if (next !== link.generation) { link.generation = next; this.changedDependencies(); }
            return { confirm: () => {} };
        }
        const registered = link;
        registered.pendingRegistrations++;
        this.changedDependencies();
        let confirmed = false;
        return { confirm: (generation) => {
            if (confirmed) return;
            validGeneration(generation);
            confirmed = true;
            registered.pendingRegistrations--;
            if (generation === undefined) registered.uncertain = true;
            else registered.generation = Math.max(registered.generation, generation);
            this.changedDependencies();
        } };
    }

    /** Legacy synchronous compatibility snapshot. Reviewed selection uses the
     * admitted cooperative API below. Returned rows are detached from later
     * registrations/ACK retirement and remain proportional to current pairs. */
    captureDependencies(): readonly RenameDependencySnapshot[] {
        // Compatibility API intentionally keeps its previous uncapped result;
        // reviewed preparation uses the admitted cooperative path below.
        return exhaust(dependencyCaptureSteps(this.legacyLinks));
    }

    /** Capture a detached structural snapshot with bounded traversal. Cooldown
     * records are deliberately outside this standalone witness; the reviewed
     * owner additionally fences its whole preparation planningRevision. */
    async captureDependenciesCooperatively(
        options: DeferredDependencyCaptureOptions,
    ): Promise<readonly RenameDependencySnapshot[]> {
        if (typeof options?.cooperate !== "function") throw new TypeError("deferred dependency capture requires host cooperation");
        const cooperate = options.cooperate, signal = options.signal;
        const revision = this.dependencyRevisionValue, limit = dependencyCaptureLimit(options.maxDependencies);
        const assertCurrent = () => {
            throwIfWorkAborted(signal);
            if (this.dependencyRevisionValue !== revision) {
                throw new DeferredDependencyCaptureError("DEFERRED_DEPENDENCIES_CHANGED");
            }
        };
        const steps = dependencyCaptureSteps(this.legacyLinks, limit);
        let work = 0;
        assertCurrent();
        for (;;) {
            assertCurrent();
            const step = steps.next();
            if (step.done) {
                assertCurrent();
                return step.value;
            }
            if (++work < DEFERRED_DEPENDENCY_CAPTURE_WORK_UNITS) continue;
            assertCurrent();
            await cooperate();
            assertCurrent();
            work = 0;
        }
    }

    /** Read-only, short-circuit hint gate before network/stat work. Accept a
     * lazy captured view so checking an early runnable path does not detach,
     * clone or restore the whole backlog. A normal new event gets its own
     * generation; it must not inherit an old attachment's cooldown. The
     * all-cooled case still visits the supplied cut; this is not a cached
     * global readiness counter or deletion authorization. */
    hasRunnable(
        queued: Iterable<DirtyFileChange>,
        retryKey: string,
        now: number,
        force = false,
    ): boolean {
        let coolingSource = false, sawHint = false;
        for (const hint of queued) {
            sawHint = true;
            if (force) return true;
            const previous = this.records.get(hint.path);
            if (!previous || !sameHint(hint, previous.hint) || previous.retryKey !== retryKey) return true;
            if (previous.detail.reason === "source-too-large") {
                if (now >= previous.retryAt) return true;
                coolingSource = true;
            } else if (previous.detail.reason === "range-unavailable") {
                coolingSource = true;
            }
        }
        // Dependency-only states have nothing left to wait for.
        return sawHint && !coolingSource;
    }

    /** Call only AFTER the full materialized snapshot passed safety review.
     * In the absence of durable rename groups, withholding every delete is
     * conservative but prevents publishing the old half of an oversized move. */
    partition(
        queued: readonly DirtyFileChange[],
        materialized: readonly FileChange[],
        retryKey: string,
        now: number,
        force = false,
    ): DeferredPartition {
        return exhaust(partitionSteps(queued, materialized, this.records, this.legacyLinks, retryKey, now, force));
    }

    /** The same policy as partition(), advanced in bounded elementary units.
     * This is a read-only classifier: no partial result escapes abort, state
     * mutation, dependency admission failure or a rejected host yield. */
    async partitionCooperatively(
        queued: readonly DirtyFileChange[],
        materialized: readonly FileChange[],
        retryKey: string,
        now: number,
        force: boolean,
        options: DeferredPartitionOptions,
    ): Promise<DeferredPartition> {
        if (typeof options?.cooperate !== "function") throw new TypeError("deferred partition requires host cooperation");
        const cooperate = options.cooperate, signal = options.signal;
        const revision = this.planningRevisionValue, queuedLength = queued.length, materializedLength = materialized.length;
        const assertCurrent = () => {
            throwIfWorkAborted(signal);
            // Arrays and their scalar rows are caller-owned immutable cut
            // inputs. Length checks catch structural mutation without cloning
            // the whole review merely to cross each host task boundary.
            if (this.planningRevisionValue !== revision || queued.length !== queuedLength ||
                materialized.length !== materializedLength) {
                throw new DeferredPartitionError("DEFERRED_PARTITION_CHANGED");
            }
        };
        const steps = partitionSteps(queued, materialized, this.records, this.legacyLinks,
            retryKey, now, force, queuedLength, materializedLength);
        let work = 0;
        assertCurrent();
        for (;;) {
            assertCurrent();
            const step = steps.next();
            if (step.done) {
                assertCurrent();
                return step.value;
            }
            if (++work < DEFERRED_PARTITION_WORK_UNITS) continue;
            assertCurrent();
            await cooperate();
            assertCurrent();
            work = 0;
        }
    }

    /** Split exactly the detached generations, not the live dirty map. Caller
     * must use DirtyPathSet.restore(retained), which keeps any newer events.
     * An ACK failure may restore the original snapshot and retry safely. */
    settle(
        queued: readonly DirtyFileChange[],
        materialized: readonly FileChange[],
        deferred: readonly DeferredPushChange[],
        retryKey: string,
        now: number,
    ): DeferredSettlement {
        const hints = new Map(queued.map((hint) => [hint.path, hint]));
        const current = new Map(materialized.map((change) => [change.path, change]));
        const held = new Map<string, DeferredPushChange>();
        for (const detail of deferred) {
            if (!hints.has(detail.path) ||
                (detail.reason !== "source-too-large" && detail.reason !== "range-unavailable" &&
                    detail.reason !== "dependent-delete") ||
                !Number.isSafeInteger(detail.requiredBytes) || detail.requiredBytes < 0 ||
                !Number.isSafeInteger(detail.capacityBytes) || detail.capacityBytes < 0) {
                throw new Error("invalid deferred push outcome");
            }
            held.set(detail.path, { ...detail });
        }
        // Settlement may run after an accepted root and exact journal ACK. It
        // must preserve the pre-existing synchronous closure semantics rather
        // than introduce a new post-commit admission failure. Full-review
        // partition rejects the same oversized graph before any claim.
        exhaust(includeLinkedGenerationSteps(queued, hints, held, this.legacyLinks, queued.length, false));
        const retained: DirtyFileChange[] = [];
        const acknowledged: Array<{ path: string; throughId: number }> = [];
        let changed = false;
        const beforeRecordMutation = () => {
            if (changed) return;
            changed = true;
            this.changedPlanning();
        };
        for (const hint of queued) {
            const detail = held.get(hint.path);
            if (!detail) {
                if (this.records.has(hint.path)) {
                    beforeRecordMutation();
                    this.records.delete(hint.path);
                }
                if (hint.journalId !== undefined) acknowledged.push({ path: hint.path, throughId: hint.journalId });
                continue;
            }
            const refreshed = compact(current.get(hint.path) ?? hint, hint.journalId);
            const previous = this.records.get(hint.path);
            // Unrelated successful note uploads must not postpone an existing
            // attachment retry forever by repeatedly refreshing its cooldown.
            const preserveRetry = previous && sameHint(refreshed, previous.hint) &&
                previous.retryKey === retryKey && previous.detail.reason === detail.reason &&
                previous.retryAt > now;
            const stored = compact(refreshed, hint.journalId);
            const record: DeferredRecord = {
                hint: stored,
                current: stored,
                detail,
                retryKey,
                retryAt: detail.reason === "range-unavailable"
                    ? Number.POSITIVE_INFINITY
                    : preserveRetry ? previous.retryAt : now + this.retryMs,
            };
            if (!previous || !sameRecord(previous, record)) {
                beforeRecordMutation();
                this.records.set(hint.path, record);
            }
            retained.push(refreshed);
        }
        const settlement = { retained, acknowledged };
        this.settlements.set(settlement, new Map(acknowledged.map(({ path, throughId }) => [path, throughId])));
        return settlement;
    }

    /** Call only AFTER the corresponding journal ACK has completed. A root
     * result alone is not enough: save/conflict-copy/ACK failure restores the
     * detached hints and must retain the links needed by the next retry.
     * Recheck the current edge generation here because another rename may
     * have registered while this settlement was awaiting persistence. */
    commitSettlement(settlement: DeferredSettlement): void {
        const watermarks = this.settlements.get(settlement);
        if (!watermarks) return;
        this.settlements.delete(settlement);
        this.retireAcknowledgedLinks(watermarks);
    }

    /** Echo-only ACKs have no push settlement and must not alter deferred
     * records. Call only after the corresponding durable ACK succeeded. The
     * same generation/pending-registration fences apply to concurrent renames. */
    commitAcknowledged(acknowledged: readonly { path: string; throughId: number }[]): void {
        const watermarks = new Map<string, number>();
        for (const { path, throughId } of acknowledged) {
            if (!Number.isSafeInteger(throughId) || throughId <= 0) {
                throw new RangeError("invalid acknowledged rename generation");
            }
            watermarks.set(path, Math.max(watermarks.get(path) ?? 0, throughId));
        }
        this.retireAcknowledgedLinks(watermarks);
    }

    private retireAcknowledgedLinks(watermarks: ReadonlyMap<string, number>): void {
        const visited = new Set<LegacyRenameLink>();
        for (const path of watermarks.keys()) {
            for (const link of this.legacyLinks.get(path)?.values() ?? []) {
                if (visited.has(link)) continue;
                visited.add(link);
                if (link.pendingRegistrations > 0 || link.uncertain || link.generation === 0 ||
                    (watermarks.get(link.left) ?? 0) < link.generation ||
                    (watermarks.get(link.right) ?? 0) < link.generation) continue;
                for (const [from, to] of [[link.left, link.right], [link.right, link.left]]) {
                    const related = this.legacyLinks.get(from);
                    related?.delete(to);
                    if (related?.size === 0) this.legacyLinks.delete(from);
                }
                this.changedDependencies();
            }
        }
    }

    summary(): DeferredChangeSummary {
        const summary: DeferredChangeSummary = {
            count: this.records.size,
            sourceTooLarge: 0,
            rangeUnavailable: 0,
            dependentDeletes: 0,
            dependentChanges: 0,
            maxRequiredBytes: 0,
            minCapacityBytes: 0,
            nextRetryAt: null,
        };
        for (const record of this.records.values()) {
            if (record.detail.reason === "source-too-large") {
                summary.sourceTooLarge++;
                summary.maxRequiredBytes = Math.max(summary.maxRequiredBytes, record.detail.requiredBytes);
                summary.minCapacityBytes = summary.sourceTooLarge === 1
                    ? record.detail.capacityBytes
                    : Math.min(summary.minCapacityBytes, record.detail.capacityBytes);
                summary.nextRetryAt = summary.nextRetryAt === null ? record.retryAt : Math.min(summary.nextRetryAt, record.retryAt);
            } else if (record.detail.reason === "range-unavailable") {
                summary.rangeUnavailable++;
            } else if (record.hint.action === "deleted") {
                summary.dependentDeletes++;
            } else {
                summary.dependentChanges++;
            }
        }
        return summary;
    }
}
