import type { FileChange } from "./push";

/** A compact dirty-path record. File bytes are intentionally forbidden:
 *  holding event snapshots until a debounce fires is the main mobile-memory
 *  failure mode this structure replaces. */
export interface DirtyFileChange extends Omit<FileChange, "data"> {
    /** Highest durable local-journal record represented by this path. */
    journalId?: number;
}

/**
 * Snapshot sync cares about the final state of each path, not every
 * intermediate editor event. This map coalesces an arbitrary event stream to
 * one record per path while retaining a journal watermark for crash recovery.
 */
export class DirtyPathSet {
    private changes = new Map<string, DirtyFileChange>();

    add(change: FileChange, journalId?: number): void {
        const previous = this.changes.get(change.path);
        const effectiveJournalId = Math.max(previous?.journalId ?? 0, journalId ?? 0) || undefined;
        // Deliberately do not retain change.data. Push reads the current file
        // after the debounce, which is both more correct and dramatically
        // cheaper than retaining every historical Uint8Array.
        const compact: DirtyFileChange = {
            action: change.action,
            path: change.path,
            journalId: effectiveJournalId,
        };
        if (change.hash !== undefined) compact.hash = change.hash;
        if (change.mtime !== undefined) compact.mtime = change.mtime;
        if (change.size !== undefined) compact.size = change.size;
        this.changes.set(change.path, compact);
    }

    addMany(changes: FileChange[]): void {
        for (const change of changes) this.add(change);
    }

    has(path: string): boolean {
        return this.changes.has(path);
    }

    paths(): IterableIterator<string> {
        return this.changes.keys();
    }

    get size(): number {
        return this.changes.size;
    }

    /** Remove and return one stable snapshot for a push transaction. Events
     *  arriving afterwards populate a fresh map entry and are not lost. */
    take(): DirtyFileChange[] {
        const snapshot = [...this.changes.values()];
        this.changes.clear();
        return snapshot;
    }

    /** Restore a failed transaction without overwriting newer events that
     *  arrived for the same path while the request was in flight. */
    restore(snapshot: DirtyFileChange[]): void {
        for (const change of snapshot) {
            const newer = this.changes.get(change.path);
            if (!newer) {
                this.changes.set(change.path, change);
                continue;
            }
            const journalId = Math.max(newer.journalId ?? 0, change.journalId ?? 0) || undefined;
            if (journalId !== newer.journalId) {
                // Preserve the newer final state but carry the older durable
                // watermark so its WAL row is acknowledged by the eventual
                // successful retry (a scan-generated hint may have no id).
                this.changes.set(change.path, { ...newer, journalId });
            }
        }
    }
}
