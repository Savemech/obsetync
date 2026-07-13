import { App } from "obsidian";

interface BaseEntry {
    hash: string;
    /** Local on-disk mtime — used by the mtime scan to detect edits. */
    mtime: number;
    size: number;
    /** mtime the file carries in the SERVER's Merkle tree, when it differs
     *  from the local one (files written by pull get the puller's own disk
     *  mtime locally, but must mirror the server's leaf mtime in the tree —
     *  leaf hashes cover mtime, so rebuilding the tree from sync-base can
     *  only reproduce the server root if this is preserved). Absent means
     *  "same as `mtime`" (files this device pushed itself). */
    treeMtime?: number;
}

interface SyncBaseData {
    lastSyncTimestamp: number;
    entries: Record<string, BaseEntry>;
    /** The server root hash this device's Merkle tree was last reconciled
     *  with — the HONEST parent for putRoot (merge base), as opposed to the
     *  last root merely *observed* on the server. Persisted in lockstep with
     *  the entries it corresponds to. Null until the first verified
     *  pull-rebase or push. */
    treeBaseRoot?: string | null;
}

const SYNC_BASE_PATH = ".obsidian/plugins/obsetync/sync-base.json";

/**
 * Tracks the last-synced hash for every file — the "common ancestor" for conflict detection.
 * Persisted as a JSON file in the plugin data directory.
 */
export class ObsetyncSyncBase {
    private data: SyncBaseData = { lastSyncTimestamp: 0, entries: {} };
    private dirty = false;

    constructor(private app: App) {}

    async load(): Promise<void> {
        try {
            const raw = await this.app.vault.adapter.read(SYNC_BASE_PATH);
            this.data = JSON.parse(raw);
        } catch {
            this.data = { lastSyncTimestamp: 0, entries: {} };
        }
    }

    async save(): Promise<void> {
        if (!this.dirty) return;
        const dir = SYNC_BASE_PATH.substring(0, SYNC_BASE_PATH.lastIndexOf("/"));
        if (!(await this.app.vault.adapter.exists(dir))) {
            await this.app.vault.adapter.mkdir(dir);
        }
        await this.app.vault.adapter.write(
            SYNC_BASE_PATH,
            JSON.stringify(this.data)
        );
        this.dirty = false;
    }

    getHash(path: string): string | null {
        return this.data.entries[path]?.hash ?? null;
    }

    getEntry(path: string): BaseEntry | null {
        return this.data.entries[path] ?? null;
    }

    setEntry(
        path: string,
        hash: string,
        mtime: number,
        size: number,
        treeMtime?: number,
    ): void {
        const entry: BaseEntry = { hash, mtime, size };
        // Only store treeMtime when it genuinely differs — keeps the JSON
        // compact and "absent = same as mtime" unambiguous.
        if (treeMtime !== undefined && treeMtime !== mtime) {
            entry.treeMtime = treeMtime;
        }
        this.data.entries[path] = entry;
        this.dirty = true;
    }

    removeEntry(path: string): void {
        delete this.data.entries[path];
        this.dirty = true;
    }

    /** The mtime this path must carry in the Merkle tree (server parity). */
    getTreeMtime(path: string): number | null {
        const e = this.data.entries[path];
        if (!e) return null;
        return e.treeMtime ?? e.mtime;
    }

    get treeBaseRoot(): string | null {
        return this.data.treeBaseRoot ?? null;
    }

    setTreeBaseRoot(hash: string | null): void {
        if ((this.data.treeBaseRoot ?? null) === hash) return;
        this.data.treeBaseRoot = hash;
        this.dirty = true;
    }

    get lastSyncTimestamp(): number {
        return this.data.lastSyncTimestamp;
    }

    setLastSyncTimestamp(ts: number): void {
        this.data.lastSyncTimestamp = ts;
        this.dirty = true;
    }

    allPaths(): string[] {
        return Object.keys(this.data.entries);
    }

    entryCount(): number {
        return Object.keys(this.data.entries).length;
    }
}
