import { App } from "obsidian";

interface BaseEntry {
    hash: string;
    mtime: number;
    size: number;
}

interface SyncBaseData {
    lastSyncTimestamp: number;
    entries: Record<string, BaseEntry>;
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

    setEntry(path: string, hash: string, mtime: number, size: number): void {
        this.data.entries[path] = { hash, mtime, size };
        this.dirty = true;
    }

    removeEntry(path: string): void {
        delete this.data.entries[path];
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
