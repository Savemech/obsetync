import type { App } from "obsidian";

interface BaseEntry {
    hash: string;
    /** Local on-disk mtime — compared during the metadata audit. */
    mtime: number;
    size: number;
    /** Server-tree mtime when it differs from the local filesystem mtime. */
    treeMtime?: number;
}

interface SyncBaseData {
    lastSyncTimestamp: number;
    entries: Record<string, BaseEntry>;
    /** Last server root this exact local tree was verifiably based on. */
    treeBaseRoot?: string | null;
    /** Last fully-applied binary diff page. Kept in the same snapshot/WAL as
     *  entry mutations, so a cursor can never outrun local state. */
    diffPageCheckpoint?: DiffPageCheckpoint | null;
}

export interface DiffPageCheckpoint {
    version: 1;
    vaultId: string;
    fromRoot: string;
    toRoot: string;
    /** Null only when the final page has been applied. */
    nextCursorHex: string | null;
    complete: boolean;
    recordsSeen: number;
    filesApplied: number;
    bytesTotal: number;
    downloaded: number;
    bytesDownloaded: number;
    deltasHadMtime: boolean;
}

type SyncBaseOperation =
    | { op: "set"; path: string; entry: BaseEntry }
    | { op: "remove"; path: string }
    | { op: "timestamp"; value: number }
    | { op: "tree-root"; value: string | null }
    | { op: "diff-page"; value: DiffPageCheckpoint | null };

const SYNC_BASE_PATH = ".obsidian/plugins/obsetync/sync-base.json";
const SYNC_BASE_NEXT_PATH = `${SYNC_BASE_PATH}.next`;
const SYNC_BASE_BACKUP_PATH = `${SYNC_BASE_PATH}.bak`;
const SYNC_BASE_WAL_PATH = ".obsidian/plugins/obsetync/sync-base.wal.ndjson";

/**
 * Last-synced state with a small append-only checkpoint log.
 *
 * A 68k-file pull no longer has to choose between rewriting a huge JSON file
 * after every batch and losing all progress on an iOS process kill. Mutations
 * are appended to the WAL at batch boundaries; save() periodically compacts
 * them into an atomically rotated snapshot. WAL operations are idempotent.
 */
export class ObsetyncSyncBase {
    private data: SyncBaseData = { lastSyncTimestamp: 0, entries: {} };
    private dirty = false;
    private pendingOperations: SyncBaseOperation[] = [];
    private directoryEnsured = false;
    private walNeedsLeadingNewline = false;
    private writeChain: Promise<void> = Promise.resolve();
    private mutationVersion = 0;

    constructor(private app: App) {}

    async load(): Promise<void> {
        const loaded = await this.readFirstValidSnapshot([
            SYNC_BASE_NEXT_PATH,
            SYNC_BASE_PATH,
            SYNC_BASE_BACKUP_PATH,
        ]);
        this.data = loaded ?? { lastSyncTimestamp: 0, entries: {} };
        this.pendingOperations = [];
        this.dirty = false;

        try {
            const wal = await this.app.vault.adapter.read(SYNC_BASE_WAL_PATH);
            this.walNeedsLeadingNewline = wal.length > 0 && !wal.endsWith("\n");
            let applied = 0;
            for (const line of wal.split("\n")) {
                if (!line.trim()) continue;
                try {
                    this.applyOperation(JSON.parse(line) as SyncBaseOperation);
                    applied++;
                } catch {
                    // Preserve all parseable checkpoints after a torn final append.
                }
            }
            if (applied > 0) this.dirty = true;
        } catch {
            // No WAL yet.
            this.walNeedsLeadingNewline = false;
        }
    }

    /** Durably append mutations since the previous batch without rewriting
     *  the full sync-base snapshot. */
    async checkpoint(): Promise<void> {
        await this.enqueue(async () => {
            if (this.pendingOperations.length === 0) return;
            await this.ensureDirectory();
            await this.flushPendingInsideQueue();
        });
    }

    /** Compact current state into a recoverable rotated snapshot, then clear
     *  the idempotent WAL. */
    async save(): Promise<void> {
        await this.enqueue(async () => {
            if (!this.dirty && this.pendingOperations.length === 0) return;
            await this.ensureDirectory();
            await this.flushPendingInsideQueue();

            const snapshotVersion = this.mutationVersion;
            const encoded = JSON.stringify(this.data);
            const adapter = this.app.vault.adapter;
            await adapter.write(SYNC_BASE_NEXT_PATH, encoded);

            if (await adapter.exists(SYNC_BASE_BACKUP_PATH)) {
                await adapter.remove(SYNC_BASE_BACKUP_PATH);
            }
            if (await adapter.exists(SYNC_BASE_PATH)) {
                await adapter.rename(SYNC_BASE_PATH, SYNC_BASE_BACKUP_PATH);
            }
            try {
                await adapter.rename(SYNC_BASE_NEXT_PATH, SYNC_BASE_PATH);
            } catch (error) {
                if (!(await adapter.exists(SYNC_BASE_PATH)) && await adapter.exists(SYNC_BASE_BACKUP_PATH)) {
                    try { await adapter.rename(SYNC_BASE_BACKUP_PATH, SYNC_BASE_PATH); } catch { /* original error */ }
                }
                throw error;
            }

            // Replaying pre-snapshot WAL rows is harmless, so clear it only
            // after the new main snapshot is in place.
            await adapter.write(SYNC_BASE_WAL_PATH, "");
            this.walNeedsLeadingNewline = false;
            if (await adapter.exists(SYNC_BASE_BACKUP_PATH)) {
                await adapter.remove(SYNC_BASE_BACKUP_PATH);
            }

            // Mutations can arrive while adapter promises yield. Persist any
            // post-snapshot operations immediately after clearing the WAL.
            await this.flushPendingInsideQueue();
            this.dirty = this.mutationVersion !== snapshotVersion;
        });
    }

    getHash(path: string): string | null {
        return this.data.entries[path]?.hash ?? null;
    }

    getEntry(path: string): BaseEntry | null {
        return this.data.entries[path] ?? null;
    }

    setEntry(path: string, hash: string, mtime: number, size: number, treeMtime?: number): void {
        const entry: BaseEntry = { hash, mtime, size };
        if (treeMtime !== undefined && treeMtime !== mtime) entry.treeMtime = treeMtime;
        this.data.entries[path] = entry;
        this.record({ op: "set", path, entry });
    }

    removeEntry(path: string): void {
        delete this.data.entries[path];
        this.record({ op: "remove", path });
    }

    getTreeMtime(path: string): number | null {
        const entry = this.data.entries[path];
        return entry ? entry.treeMtime ?? entry.mtime : null;
    }

    get treeBaseRoot(): string | null {
        return this.data.treeBaseRoot ?? null;
    }

    setTreeBaseRoot(hash: string | null): void {
        if ((this.data.treeBaseRoot ?? null) === hash) return;
        this.data.treeBaseRoot = hash;
        this.record({ op: "tree-root", value: hash });
    }

    get diffPageCheckpoint(): DiffPageCheckpoint | null {
        const checkpoint = this.data.diffPageCheckpoint;
        return isDiffPageCheckpoint(checkpoint) ? { ...checkpoint } : null;
    }

    setDiffPageCheckpoint(checkpoint: DiffPageCheckpoint): void {
        if (!isDiffPageCheckpoint(checkpoint)) throw new Error("invalid diff page checkpoint");
        this.data.diffPageCheckpoint = { ...checkpoint };
        this.record({ op: "diff-page", value: { ...checkpoint } });
    }

    /** Returns whether a persisted marker was actually removed. */
    clearDiffPageCheckpoint(): boolean {
        if (!this.data.diffPageCheckpoint) return false;
        this.data.diffPageCheckpoint = null;
        this.record({ op: "diff-page", value: null });
        return true;
    }

    get lastSyncTimestamp(): number {
        return this.data.lastSyncTimestamp;
    }

    setLastSyncTimestamp(ts: number): void {
        this.data.lastSyncTimestamp = ts;
        this.record({ op: "timestamp", value: ts });
    }

    allPaths(): string[] {
        return Object.keys(this.data.entries);
    }

    entryCount(): number {
        return Object.keys(this.data.entries).length;
    }

    private record(operation: SyncBaseOperation): void {
        this.pendingOperations.push(operation);
        this.dirty = true;
        this.mutationVersion++;
    }

    private applyOperation(operation: SyncBaseOperation): void {
        if (!operation || typeof operation !== "object") throw new Error("invalid sync-base WAL row");
        switch (operation.op) {
            case "set":
                if (typeof operation.path !== "string" || !operation.entry) throw new Error("invalid set");
                this.data.entries[operation.path] = operation.entry;
                break;
            case "remove":
                if (typeof operation.path !== "string") throw new Error("invalid remove");
                delete this.data.entries[operation.path];
                break;
            case "timestamp":
                if (typeof operation.value !== "number") throw new Error("invalid timestamp");
                this.data.lastSyncTimestamp = operation.value;
                break;
            case "tree-root":
                if (operation.value !== null && typeof operation.value !== "string") {
                    throw new Error("invalid tree root");
                }
                this.data.treeBaseRoot = operation.value;
                break;
            case "diff-page":
                if (operation.value !== null && !isDiffPageCheckpoint(operation.value)) {
                    throw new Error("invalid diff page checkpoint");
                }
                this.data.diffPageCheckpoint = operation.value === null
                    ? null
                    : { ...operation.value };
                break;
            default:
                throw new Error("unknown sync-base WAL operation");
        }
    }

    private async readFirstValidSnapshot(paths: string[]): Promise<SyncBaseData | null> {
        for (const path of paths) {
            try {
                const parsed = JSON.parse(await this.app.vault.adapter.read(path)) as SyncBaseData;
                if (parsed && typeof parsed.lastSyncTimestamp === "number" && parsed.entries) {
                    return parsed;
                }
            } catch {
                // Try the staged snapshot or previous backup.
            }
        }
        return null;
    }

    private async flushPendingInsideQueue(): Promise<void> {
        // A vault callback can mutate sync-base while adapter.append() yields.
        // Drain until stable so save()/checkpoint() never resolve with a
        // mutation that happened during their final write still memory-only.
        while (this.pendingOperations.length > 0) {
            const operations = this.pendingOperations.splice(0);
            try {
                const lines = operations.map((operation) => JSON.stringify(operation)).join("\n");
                const prefix = this.walNeedsLeadingNewline ? "\n" : "";
                await this.app.vault.adapter.append(SYNC_BASE_WAL_PATH, `${prefix}${lines}\n`);
                this.walNeedsLeadingNewline = false;
            } catch (error) {
                this.pendingOperations = operations.concat(this.pendingOperations);
                throw error;
            }
        }
    }

    private async ensureDirectory(): Promise<void> {
        if (this.directoryEnsured) return;
        const dir = SYNC_BASE_PATH.substring(0, SYNC_BASE_PATH.lastIndexOf("/"));
        if (!(await this.app.vault.adapter.exists(dir))) await this.app.vault.adapter.mkdir(dir);
        this.directoryEnsured = true;
    }

    private enqueue<T>(operation: () => Promise<T>): Promise<T> {
        const result = this.writeChain.then(operation, operation);
        this.writeChain = result.then(() => undefined, () => undefined);
        return result;
    }
}

const ROOT_HASH = /^[0-9a-f]{64}$/;
const CURSOR_HEX = /^[0-9a-f]*$/;

function isDiffPageCheckpoint(value: unknown): value is DiffPageCheckpoint {
    if (!value || typeof value !== "object") return false;
    const row = value as Record<string, unknown>;
    const cursor = row.nextCursorHex;
    const counters = [
        row.recordsSeen,
        row.filesApplied,
        row.bytesTotal,
        row.downloaded,
        row.bytesDownloaded,
    ];
    return row.version === 1 &&
        typeof row.vaultId === "string" && row.vaultId.length > 0 && row.vaultId.length <= 4096 &&
        typeof row.fromRoot === "string" && ROOT_HASH.test(row.fromRoot) &&
        typeof row.toRoot === "string" && ROOT_HASH.test(row.toRoot) &&
        typeof row.complete === "boolean" &&
        typeof row.deltasHadMtime === "boolean" &&
        counters.every((counter) => Number.isSafeInteger(counter) && (counter as number) >= 0) &&
        ((row.complete === true && cursor === null) ||
            (row.complete === false && typeof cursor === "string" && cursor.length > 0 &&
                cursor.length <= 16_532 && cursor.length % 2 === 0 && CURSOR_HEX.test(cursor)));
}
