import type { FileChange } from "./push";
import { isSafeVaultPath } from "./delta-validation";
import { PersistentPathIndex } from "./persistent-path-index";

/** A compact dirty-path record. File bytes are intentionally forbidden:
 *  holding event snapshots until a debounce fires is the main mobile-memory
 *  failure mode this structure replaces. */
export interface DirtyFileChange extends Omit<FileChange, "data"> {
    /** Highest durable local-journal record represented by this path. */
    journalId?: number;
}

export const DIRTY_RETIREMENT_LIMIT = 256;
export const DIRTY_CLAIM_LIMIT = 256;
export const DIRTY_TRACKING_LIMIT = 1024;
declare const retirementToken: unique symbol;
declare const captureToken: unique symbol;
/** Opaque, owner-local and valid only until this owner's next capture/commit. */
export interface DirtyRetirementToken { readonly [retirementToken]: true }
/** A stable, owner-local metadata view, not review or journal ACK authority.
 * Keep only the needed views: historical captures retain their AVL roots. */
export interface DirtyPathCapture {
    readonly [captureToken]: true;
    readonly size: number;
    iterate(): IterableIterator<DirtyFileChange>;
    get(path: string): DirtyFileChange | undefined;
}
export interface DirtyTrackingSnapshot {
    readonly pendingPaths: number;
    readonly limit: number;
    readonly overflow: boolean;
    readonly active: boolean;
}
export interface DirtyTrackingCut {
    readonly capture: DirtyPathCapture;
    readonly paths: readonly string[];
    readonly overflow: boolean;
    readonly active: boolean;
}
/** Bounded notification hints, never review, source or ACK authority. A caller
 * MUST abandon its reviewed session if overflow or !active, even when paths
 * is empty. One observer per owner; replacement invalidates the previous one. */
export interface DirtyPathTracking {
    readonly capture: DirtyPathCapture;
    drain(): DirtyTrackingCut;
    snapshot(): DirtyTrackingSnapshot;
    dispose(): void;
}
export interface DirtyJournalCut { path: string; throughId: number }
interface DirtyEntry {
    readonly change: DirtyFileChange;
    /** The final hint's own durable generation, never an inherited watermark. */
    readonly ownJournalId?: number;
}
interface DirtySlot {
    readonly entry: DirtyEntry;
    readonly orderKey: string;
}
interface DirtyTrackingState {
    readonly paths: Set<string>;
    readonly limit: number;
    overflow: boolean;
    active: boolean;
}
const NO_TRACKED_PATHS: readonly string[] = Object.freeze([]);
const durableId = (value: unknown): value is number => Number.isSafeInteger(value) &&
    (value as number) > 0 && (value as number) < Number.MAX_SAFE_INTEGER;

function compact(change: FileChange, journalId?: number): DirtyFileChange {
    // Copy only scalar hint metadata. Neither caller-owned objects nor bytes
    // become part of the live map or a captured retirement authorization.
    const result: DirtyFileChange = { action: change.action, path: change.path, journalId };
    if (change.hash !== undefined) result.hash = change.hash;
    if (change.mtime !== undefined) result.mtime = change.mtime;
    if (change.size !== undefined) result.size = change.size;
    return result;
}
function sameHint(a: DirtyFileChange, b: DirtyFileChange): boolean {
    return a.action === b.action && a.path === b.path && a.journalId === b.journalId &&
        a.hash === b.hash && a.mtime === b.mtime && a.size === b.size;
}

/**
 * Snapshot sync cares about the final state of each path, not every
 * intermediate editor event. This map coalesces an arbitrary event stream to
 * one record per path while retaining a journal watermark for crash recovery.
 */
export class DirtyPathSet {
    // Two immutable indexes keep O(1) captures without changing the previous
    // Map insertion order used by sequential sync. Live metadata remains O(n),
    // with O(log n) path-copy updates; this is not a bounded-heap claim.
    private changes = new PersistentPathIndex<DirtySlot>();
    private ordered = new PersistentPathIndex<DirtyEntry>();
    private nextOrder = 0;
    private captures = new WeakMap<DirtyPathCapture, PersistentPathIndex<DirtySlot>>();
    private tracking: DirtyTrackingState | null = null;
    // A take() record can carry its provenance back only to this owner and
    // only without changing its scalar metadata. Weak keys retain no history
    // after the caller drops a detached snapshot.
    private snapshots = new WeakMap<DirtyFileChange, DirtyEntry>();
    private retirement: { token: DirtyRetirementToken; entries: Array<{ path: string; entry: DirtyEntry }> } | null = null;

    add(change: FileChange, journalId?: number): void {
        const previous = this.changes.get(change.path)?.entry;
        const effectiveJournalId = Math.max(previous?.change.journalId ?? 0, journalId ?? 0) || undefined;
        // Deliberately do not retain change.data. Push reads the current file
        // after the debounce, which is both more correct and dramatically
        // cheaper than retaining every historical Uint8Array.
        this.setEntry(change.path, { change: compact(change, effectiveJournalId),
            ownJournalId: durableId(journalId) ? journalId : undefined });
    }

    addMany(changes: FileChange[]): void {
        for (const change of changes) this.add(change);
    }

    has(path: string): boolean {
        return this.changes.get(path) !== undefined;
    }

    paths(): IterableIterator<string> {
        return this.iteratePaths(this.ordered);
    }

    get size(): number {
        return this.changes.size;
    }

    /** Read-only captured iteration, in insertion order. Rows are detached
     * lazily; merely creating this iterator does not walk/copy the backlog. */
    iterate(): IterableIterator<DirtyFileChange> {
        return this.iterateEntries(this.ordered);
    }

    /** O(1) immutable capture. Read hints retain their exact owner's existing
     * provenance, but reading a no-ID hint never makes it journal-proven. */
    capture(): DirtyPathCapture {
        const changes = this.changes, ordered = this.ordered;
        const capture = Object.freeze({
            size: changes.size,
            iterate: () => this.iterateEntries(ordered),
            get: (path: string) => {
                const entry = changes.get(path)?.entry;
                return entry ? this.detach(entry) : undefined;
            },
        }) as DirtyPathCapture;
        this.captures.set(capture, changes);
        return capture;
    }

    /** O(1), owner-local whole-set identity check; reads no captured rows.
     * Unlike claim(), any changed root invalidates this witness, including an
     * equal-looking event or a take/claim followed by an unchanged restore.
     * This grants neither source-freshness proof nor journal ACK authority. */
    isCurrentCapture(capture: DirtyPathCapture): boolean {
        return this.captures.get(capture) === this.changes;
    }

    /** Install synchronously with an immutable capture: no mutation gap and
     * no caller callbacks. Repeated events coalesce by path until drain().
     * The bounded observer retains no per-event history or file bytes. */
    captureTracking(maxPaths = DIRTY_CLAIM_LIMIT): DirtyPathTracking {
        if (!Number.isSafeInteger(maxPaths) || maxPaths < 1 || maxPaths > DIRTY_TRACKING_LIMIT) {
            throw new RangeError("dirty tracking requires a bounded positive path limit");
        }
        const capture = this.capture();
        const state: DirtyTrackingState = { paths: new Set(), limit: maxPaths, overflow: false, active: true };
        const tracking: DirtyPathTracking = Object.freeze({
            capture,
            drain: (): DirtyTrackingCut => {
                const active = this.tracking === state && state.active;
                if (!active || state.overflow) {
                    // Do not manufacture a fresh apparently usable capture
                    // after invalidation. The original view is metadata only.
                    return Object.freeze({ capture, paths: NO_TRACKED_PATHS, overflow: state.overflow, active });
                }
                // Construct the complete bounded cut before clearing pending
                // notifications. Allocation failure leaves them observable.
                const current = this.capture(), paths = Object.freeze([...state.paths]);
                const cut = Object.freeze({ capture: current, paths, overflow: false, active: true });
                state.paths.clear();
                return cut;
            },
            snapshot: (): DirtyTrackingSnapshot => Object.freeze({ pendingPaths: state.paths.size,
                limit: state.limit, overflow: state.overflow, active: this.tracking === state && state.active }),
            dispose: () => {
                state.active = false; state.paths.clear();
                if (this.tracking === state) this.tracking = null;
            },
        });
        // Invalid creation cannot replace a still-valid observer. Both the
        // captured cut and its consumer object exist before the handoff.
        if (this.tracking) { this.tracking.active = false; this.tracking.paths.clear(); }
        this.tracking = state;
        return tracking;
    }

    /** Atomically detach a bounded selection ONLY if every captured entry is
     * still current. Selection is not review/ACK; the caller must restore the
     * returned exact objects on failure. New/equal-looking hints are stale,
     * even when their path and inherited journal watermark match the capture.
     * A stale or foreign capture returns null without removing any entry. */
    claim(capture: DirtyPathCapture, paths: readonly string[]): DirtyFileChange[] | null {
        if (!Array.isArray(paths)) {
            throw new RangeError("dirty claim exceeds its bounded path cut");
        }
        const length = paths.length;
        if (!Number.isSafeInteger(length) || length > DIRTY_CLAIM_LIMIT || length < 0) {
            throw new RangeError("dirty claim exceeds its bounded path cut");
        }
        // Detach/validate every caller-owned value before touching live state.
        // In particular a late invalid path cannot split a linked rename cut.
        const selected: string[] = [], unique = new Set<string>();
        for (let index = 0; index < length; index++) {
            const path = paths[index];
            if (!isSafeVaultPath(path) || unique.has(path)) {
                throw new RangeError("dirty claim requires unique safe paths");
            }
            unique.add(path); selected.push(path);
        }
        const captured = this.captures.get(capture);
        if (!captured) return null;
        const entries: Array<{ path: string; slot: DirtySlot }> = [];
        for (const path of selected) {
            const slot = captured.get(path), current = this.changes.get(path);
            if (!slot || !current || slot.entry !== current.entry) return null;
            // A take/restore preserves logical identity but reinserts at the
            // end; removal must use its CURRENT insertion-order slot.
            entries.push({ path, slot: current });
        }
        return this.detachClaimedEntries(entries);
    }

    /** Claim original owner-issued read/take hints without retaining their
     * whole historical AVL captures. References retain only those entries.
     * Copied, scalar-mutated, foreign or stale rows grant no selection; own
     * no-ID rows can be selected but never gain journal ACK provenance. */
    claimHints(hints: readonly DirtyFileChange[]): DirtyFileChange[] | null {
        if (!Array.isArray(hints)) throw new RangeError("dirty hint claim exceeds its bounded path cut");
        const length = hints.length;
        if (!Number.isSafeInteger(length) || length < 0 || length > DIRTY_CLAIM_LIMIT) {
            throw new RangeError("dirty hint claim exceeds its bounded path cut");
        }
        const selected: Array<{ change: DirtyFileChange; proof: DirtyEntry | undefined }> = [];
        const unique = new Set<string>();
        for (let index = 0; index < length; index++) {
            const raw = hints[index];
            if (!raw || typeof raw !== "object") throw new RangeError("dirty hint claim requires unique safe paths");
            const change = compact(raw, raw.journalId);
            if (!isSafeVaultPath(change.path) || unique.has(change.path)) {
                throw new RangeError("dirty hint claim requires unique safe paths");
            }
            unique.add(change.path); selected.push({ change, proof: this.snapshots.get(raw) });
        }
        // No caller-owned getter is read after this point. In particular a
        // later row's getter cannot invalidate an already-compared live path.
        const entries: Array<{ path: string; slot: DirtySlot }> = [];
        for (const { change, proof } of selected) {
            const current = this.changes.get(change.path);
            if (!proof || !sameHint(change, proof.change) || current?.entry !== proof) return null;
            entries.push({ path: change.path, slot: current });
        }
        return this.detachClaimedEntries(entries);
    }

    private detachClaimedEntries(entries: readonly { path: string; slot: DirtySlot }[]): DirtyFileChange[] {
        let changes = this.changes, ordered = this.ordered;
        const result: DirtyFileChange[] = [];
        for (const { path, slot } of entries) {
            result.push(this.detach(slot.entry));
            changes = changes.delete(path); ordered = ordered.delete(slot.orderKey);
        }
        // Publish both indexes only after the whole bounded cut is prepared.
        this.changes = changes; this.ordered = ordered;
        if (changes.size === 0) this.nextOrder = 0;
        return result;
    }

    /** Remove and return one stable snapshot for a push transaction. Events
     *  arriving afterwards populate a fresh map entry and are not lost. */
    take(): DirtyFileChange[] {
        const snapshot = [...this.iterate()];
        this.changes = new PersistentPathIndex<DirtySlot>();
        this.ordered = new PersistentPathIndex<DirtyEntry>();
        this.nextOrder = 0;
        return snapshot;
    }

    /** Restore a failed transaction without overwriting newer events that
     *  arrived for the same path while the request was in flight. */
    restore(snapshot: readonly DirtyFileChange[]): void {
        for (const raw of snapshot) {
            const change = compact(raw, raw.journalId);
            const proof = this.snapshots.get(raw);
            const trusted = proof !== undefined && sameHint(change, proof.change);
            const entry = trusted ? proof : { change };
            this.restoreEntry(entry, trusted);
        }
    }

    /** ONLY a validated journal replay may introduce durable provenance from
     * external hints. Live callbacks/scans use add/ordinary restore instead.
     * Existing newer hints retain their state and provenance, including none. */
    restoreJournal(snapshot: readonly DirtyFileChange[]): void {
        if (!Array.isArray(snapshot) || snapshot.length > DIRTY_RETIREMENT_LIMIT) {
            throw new RangeError("journal hint restore exceeds its bounded replay batch");
        }
        // Validate/detach the entire bounded batch before changing the map.
        const entries = snapshot.map(raw => {
            if (!isSafeVaultPath(raw?.path) || !durableId(raw?.journalId)) {
                throw new RangeError("journal hint restore requires a safe path and durable generation");
            }
            return { change: compact(raw, raw.journalId), ownJournalId: raw.journalId };
        });
        for (const entry of entries) this.restoreEntry(entry);
    }

    /** Capture BEFORE asynchronous root settlement; commit ONLY after its
     * exact epoch-bound journal ACK succeeds. A watermark by itself never
     * grants retirement of a newer unjournaled live/scan state.
     * One outstanding token retains at most 256 selected entries, not the
     * complete dirty backlog. A new capture invalidates the previous token. */
    captureRetirement(cuts: readonly DirtyJournalCut[]): DirtyRetirementToken {
        if (!Array.isArray(cuts) || cuts.length > DIRTY_RETIREMENT_LIMIT) {
            throw new RangeError("dirty retirement exceeds its bounded journal cut");
        }
        const entries: Array<{ path: string; entry: DirtyEntry }> = [];
        let previous: string | undefined;
        for (const cut of cuts) {
            const path = cut?.path, throughId = cut?.throughId;
            if (!isSafeVaultPath(path) || !durableId(throughId) ||
                (previous !== undefined && path <= previous)) {
                throw new RangeError("dirty retirement requires ordered unique journal paths and generations");
            }
            previous = path;
            const entry = this.changes.get(path)?.entry;
            if (entry?.ownJournalId !== undefined && entry.ownJournalId <= throughId &&
                durableId(entry.change.journalId) && entry.change.journalId <= throughId) {
                entries.push({ path, entry });
            }
        }
        const token = Object.freeze({}) as DirtyRetirementToken;
        this.retirement = { token, entries };
        return token;
    }

    /** No-op for stale, consumed or foreign-owner tokens. Actual object
     * identity protects even a later equal-looking hint with the same ID. */
    commitRetirement(token: DirtyRetirementToken): number {
        const capture = this.retirement;
        if (!capture || capture.token !== token) return 0;
        this.retirement = null;
        let removed = 0;
        for (const { path, entry } of capture.entries) {
            const current = this.changes.get(path);
            if (current?.entry !== entry) continue;
            this.changes = this.changes.delete(path);
            this.ordered = this.ordered.delete(current.orderKey);
            removed++;
        }
        if (this.changes.size === 0) this.nextOrder = 0;
        return removed;
    }

    private restoreEntry(entry: DirtyEntry, unchangedOwnedRestore = false): void {
        const change = entry.change;
        const newer = this.changes.get(change.path)?.entry;
        if (!newer) {
            this.setEntry(change.path, entry, !unchangedOwnedRestore);
            return;
        }
        const journalId = Math.max(newer.change.journalId ?? 0, change.journalId ?? 0) || undefined;
        if (journalId !== newer.change.journalId) {
            // Preserve the newer final state but carry the older durable
            // watermark so its WAL row is acknowledged by the eventual
            // successful retry (a scan-generated hint may have no id).
            this.setEntry(change.path, { ...newer, change: compact(newer.change, journalId) });
        }
    }

    private setEntry(path: string, entry: DirtyEntry, observe = true): void {
        const previous = this.changes.get(path);
        if (!previous && (!Number.isSafeInteger(this.nextOrder) || this.nextOrder >= Number.MAX_SAFE_INTEGER)) {
            throw new RangeError("dirty insertion sequence exhausted");
        }
        // Fixed-width safe-integer keys preserve chronological ordering. The
        // counter resets only on an empty live queue, never by copying it.
        const orderKey = previous?.orderKey ?? this.nextOrder.toString().padStart(16, "0");
        const changes = this.changes.set(path, { entry, orderKey });
        const ordered = this.ordered.set(orderKey, entry);
        this.changes = changes; this.ordered = ordered;
        if (!previous) this.nextOrder++;
        if (observe) this.observeMutation(path);
    }

    private observeMutation(path: string): void {
        const state = this.tracking;
        if (!state || !state.active || state.overflow || state.paths.has(path)) return;
        // Fail closed before any new tracking allocation/validation. If it
        // throws, do not swallow the error or leave a falsely valid session.
        // The already-published dirty entry itself remains authoritative.
        state.overflow = true;
        try {
            if (state.paths.size >= state.limit || !isSafeVaultPath(path)) return;
            state.paths.add(path);
            state.overflow = false;
        } finally {
            if (state.overflow) state.paths.clear();
        }
    }

    private detach(entry: DirtyEntry): DirtyFileChange {
        const change = compact(entry.change, entry.change.journalId);
        this.snapshots.set(change, entry);
        return change;
    }

    private *iterateEntries(ordered: PersistentPathIndex<DirtyEntry>): IterableIterator<DirtyFileChange> {
        for (const [, entry] of ordered.entries()) yield this.detach(entry);
    }

    private *iteratePaths(ordered: PersistentPathIndex<DirtyEntry>): IterableIterator<string> {
        for (const [, entry] of ordered.entries()) yield entry.change.path;
    }
}
