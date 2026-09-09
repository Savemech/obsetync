import type { JournalEntry } from "./journal";
import { JournalRecoveryError, validateJournalEntry } from "./journal-format";
import { isSafeVaultPath } from "./delta-validation";
import { PersistentPathIndex } from "./persistent-path-index";

type NormalJournalEntry = JournalEntry & { action: "created" | "modified" | "deleted" };

export type JournalRecord =
    | { readonly kind: "entry"; readonly entry: Readonly<NormalJournalEntry> }
    | { readonly kind: "rename"; readonly entry: Readonly<JournalEntry>;
        readonly oldPending: boolean; readonly newPending: boolean };

function invalid(message: string): never { throw new JournalRecoveryError("CORRUPT", message); }

/** Decimal padding preserves numeric generation order; JSON array identities
 * avoid delimiter collisions for valid paths containing punctuation/Unicode.
 * Equal legacy IDs on distinct paths have a stable, locale-independent tie. */
export function journalRecordKey(record: JournalRecord): string {
    return record.entry.id.toString().padStart(16, "0") + ":" +
        (record.kind === "entry" ? "e:" + JSON.stringify(record.entry.path)
            : "r:" + JSON.stringify([record.entry.oldPath, record.entry.path]));
}

function immutableEntry(value: unknown): Readonly<JournalEntry> {
    return Object.freeze(validateJournalEntry(value));
}

function sameEntry(left: Readonly<JournalEntry>, right: Readonly<JournalEntry>): boolean {
    return left.id === right.id && left.action === right.action && left.path === right.path &&
        left.oldPath === right.oldPath && left.ts === right.ts && left.synced === right.synced &&
        left.hash === right.hash && left.mtime === right.mtime && left.size === right.size;
}

function snapshotRecord(value: unknown): JournalRecord {
    if (!value || typeof value !== "object" || Array.isArray(value)) invalid("invalid journal snapshot record");
    const row = value as Record<string, unknown>;
    const entry = immutableEntry(row.entry);
    if (row.kind === "entry") {
        if (entry.action === "renamed" || Object.keys(row).some(key => key !== "kind" && key !== "entry")) {
            invalid("invalid ordinary journal snapshot record");
        }
        return Object.freeze({ kind: "entry", entry: entry as Readonly<NormalJournalEntry> });
    }
    if (row.kind === "rename") {
        if (entry.action !== "renamed" || typeof row.oldPending !== "boolean" || typeof row.newPending !== "boolean" ||
            (!row.oldPending && !row.newPending) ||
            Object.keys(row).some(key => key !== "kind" && key !== "entry" && key !== "oldPending" && key !== "newPending")) {
            invalid("invalid linked journal snapshot record");
        }
        return Object.freeze({ kind: "rename", entry, oldPending: row.oldPending, newPending: row.newPending });
    }
    throw new JournalRecoveryError("UNKNOWN_SCHEMA", "unsupported journal snapshot record kind");
}

function validateAcknowledgement(path: string, throughId: number): void {
    if (!isSafeVaultPath(path) || !Number.isSafeInteger(throughId) || throughId <= 0 ||
        throughId >= Number.MAX_SAFE_INTEGER) invalid("invalid journal acknowledgement watermark");
}

/**
 * Immutable pending-state index, not an event-history array. Normal rows are
 * coalesced per path; unacknowledged rename groups retain independent endpoint
 * flags until BOTH sides settle. A later normal edit never erases a group.
 *
 * Persistent AVL roots make snapshots O(1) and updates path-copy O(log n).
 * ACK walks only one normal pointer and that path's covered group IDs. Each
 * pending group occupies one ordered record and at most two endpoint pointers.
 * Memory is O(unique pending normal paths + unresolved groups), not a fixed
 * byte cap: pathological unacknowledged rename history still needs admission.
 * Captured roots/iterators retain reachable metadata, not file contents.
 */
export class JournalIndex {
    private ordered = new PersistentPathIndex<JournalRecord>();
    private normalByPath = new PersistentPathIndex<string>();
    private groupsByEndpoint = new PersistentPathIndex<PersistentPathIndex<true>>();

    /** Number of projected entries, not the number of rename endpoints. */
    get size(): number { return this.ordered.size; }

    /** Pending path membership without walking/materializing the whole queue. */
    hasPath(path: string): boolean {
        return this.normalByPath.get(path) !== undefined || this.groupsByEndpoint.get(path) !== undefined;
    }

    /** The wrapper owns global monotonic-ID validation. Legacy import may
     * encounter older normal hints: preserve a higher-ID existing hint; reject
     * contradictory values sharing one normal path/generation or group key. */
    append(value: JournalEntry): JournalIndex {
        const entry = immutableEntry(value);
        const record: JournalRecord = entry.action === "renamed"
            ? Object.freeze({ kind: "rename", entry, oldPending: true, newPending: true })
            : Object.freeze({ kind: "entry", entry: entry as Readonly<NormalJournalEntry> });
        const key = journalRecordKey(record);
        if (record.kind === "rename") {
            const previous = this.ordered.get(key);
            if (previous) {
                if (!sameEntry(previous.entry, entry)) invalid("contradicting journal rename generation");
                // Idempotent replay must not resurrect an already-ACKed half.
                return this;
            }
            return this.insertRecord(key, record);
        }
        const previousKey = this.normalByPath.get(entry.path);
        if (previousKey !== undefined) {
            const previous = this.ordered.get(previousKey)!;
            if (previous.entry.id > entry.id) return this;
            if (previous.entry.id === entry.id) {
                if (!sameEntry(previous.entry, entry)) invalid("contradicting journal path generation");
                return this;
            }
            return this.withState(this.ordered.delete(previousKey), this.normalByPath, this.groupsByEndpoint)
                .insertRecord(key, record);
        }
        return this.insertRecord(key, record);
    }

    /** Rebuild a sealed snapshot without coalescing malformed duplicate rows.
     * The storage wrapper validates physical order, epoch and sequence cut. */
    addSnapshotRecord(value: unknown): JournalIndex {
        const record = snapshotRecord(value);
        const key = journalRecordKey(record);
        if (this.ordered.get(key) !== undefined ||
            (record.kind === "entry" && this.normalByPath.get(record.entry.path) !== undefined)) {
            invalid("duplicate journal snapshot identity");
        }
        return this.insertRecord(key, record);
    }

    acknowledge(path: string, throughId: number): JournalIndex {
        let next: JournalIndex = this;
        for (const step of this.acknowledgeSteps(path, throughId)) next = step;
        return next;
    }

    /** Candidate-state iterator: one affected row per step, no whole-journal
     * scan or materialized ID list. A wrapper may yield every bounded batch.
     * It MUST serialize mutations and publish only the completed candidate
     * after its whole ACK transaction is durable, never an intermediate step.
     * The synchronous convenience API is O(k log n) for k covered groups on
     * one path; a hot endpoint can have arbitrarily many unresolved groups. */
    acknowledgeSteps(path: string, throughId: number): IterableIterator<JournalIndex> {
        validateAcknowledgement(path, throughId);
        return this.walkAcknowledgement(path, throughId);
    }

    /** Frozen records/entries may be handed directly to a captured snapshot
     * encoder. Iteration is stable even before its first next() across awaits. */
    *records(): IterableIterator<JournalRecord> {
        for (const [, record] of this.ordered.entries()) yield record;
    }

    /** Compatibility projection, always in generation order. Return detached
     * mutable objects: public callers must not mutate retained index values. */
    *entries(): IterableIterator<JournalEntry> {
        for (const record of this.records()) {
            if (record.kind === "entry" || (record.oldPending && record.newPending)) {
                yield { ...record.entry };
            } else {
                const { oldPath, ...entry } = record.entry;
                yield record.oldPending
                    ? { ...entry, action: "deleted", path: oldPath! }
                    : { ...entry, action: "modified" };
            }
        }
    }

    private *walkAcknowledgement(path: string, throughId: number): IterableIterator<JournalIndex> {
        let current: JournalIndex = this;
        const normalKey = this.normalByPath.get(path);
        if (normalKey !== undefined && this.ordered.get(normalKey)!.entry.id <= throughId) {
            current = current.withState(current.ordered.delete(normalKey), current.normalByPath.delete(path), current.groupsByEndpoint);
            yield current;
        }
        const groups = this.groupsByEndpoint.get(path);
        if (!groups) return;
        // Captured endpoint tree is immutable while candidate roots advance.
        for (const [key] of groups.entries()) {
            const record = current.ordered.get(key)!;
            if (record.entry.id > throughId) break;
            if (record.kind !== "rename") invalid("invalid journal endpoint index");
            const updated: JournalRecord = Object.freeze({
                ...record,
                oldPending: record.oldPending && record.entry.oldPath !== path,
                newPending: record.newPending && record.entry.path !== path,
            });
            const endpoints = current.removeEndpoint(path, key);
            current = current.withState(
                updated.oldPending || updated.newPending ? current.ordered.set(key, updated) : current.ordered.delete(key),
                current.normalByPath, endpoints,
            );
            yield current;
        }
    }

    private insertRecord(key: string, record: JournalRecord): JournalIndex {
        let normals = this.normalByPath;
        let endpoints = this.groupsByEndpoint;
        if (record.kind === "entry") normals = normals.set(record.entry.path, key);
        else {
            const add = (path: string) => {
                const groups = endpoints.get(path) ?? new PersistentPathIndex<true>();
                endpoints = endpoints.set(path, groups.set(key, true));
            };
            if (record.oldPending) add(record.entry.oldPath!);
            if (record.newPending) add(record.entry.path);
        }
        return this.withState(this.ordered.set(key, record), normals, endpoints);
    }

    private removeEndpoint(path: string, key: string): PersistentPathIndex<PersistentPathIndex<true>> {
        const remaining = this.groupsByEndpoint.get(path)!.delete(key);
        return remaining.size ? this.groupsByEndpoint.set(path, remaining) : this.groupsByEndpoint.delete(path);
    }

    private withState(
        ordered: PersistentPathIndex<JournalRecord>,
        normalByPath: PersistentPathIndex<string>,
        groupsByEndpoint: PersistentPathIndex<PersistentPathIndex<true>>,
    ): JournalIndex {
        if (ordered === this.ordered && normalByPath === this.normalByPath && groupsByEndpoint === this.groupsByEndpoint) return this;
        const next = new JournalIndex();
        next.ordered = ordered; next.normalByPath = normalByPath; next.groupsByEndpoint = groupsByEndpoint;
        return next;
    }
}
