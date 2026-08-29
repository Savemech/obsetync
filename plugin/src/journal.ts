import { App } from "obsidian";

export interface JournalEntry {
    /** Monotonic local WAL position. Legacy records receive one when loaded. */
    id: number;
    action: "created" | "modified" | "deleted" | "renamed";
    path: string;
    oldPath?: string;
    ts: number;
    /** Kept on disk for compatibility with journals written before 1.10.1. */
    synced: boolean;
}

export type NewJournalEntry = Omit<JournalEntry, "id">;

interface JournalAcknowledgement {
    op: "ack";
    path: string;
    throughId: number;
}

const JOURNAL_PATH = ".obsidian/plugins/obsetync/change-journal.ndjson";
const COMPACT_EVERY = 10000;

/**
 * Durable write-ahead log for vault events.
 *
 * Appends are serialized and use DataAdapter.append(), so recording an event is
 * O(1) instead of reading and rewriting the entire journal. Acknowledgements
 * carry an exact per-path watermark: if a new edit arrives while an older push
 * is in flight, acknowledging that push cannot erase the newer edit.
 */
export class ObsetyncJournal {
    private entries: JournalEntry[] = [];
    private dirEnsured = false;
    private nextId = 1;
    private needsLeadingNewline = false;
    private appendsSinceCompact = 0;
    private writeChain: Promise<void> = Promise.resolve();

    constructor(private app: App) {}

    async load(): Promise<void> {
        await this.enqueue(async () => {
            let raw = "";
            try {
                raw = await this.app.vault.adapter.read(JOURNAL_PATH);
            } catch {
                this.entries = [];
                this.nextId = 1;
                this.needsLeadingNewline = false;
                return;
            }

            const loaded: JournalEntry[] = [];
            const acknowledgedThrough = new Map<string, number>();
            let fallbackId = 1;
            let recordsRead = 0;
            for (const line of raw.split("\n")) {
                if (!line.trim()) continue;
                try {
                    const parsed = JSON.parse(line) as Partial<JournalEntry & JournalAcknowledgement>;
                    recordsRead++;
                    if (parsed.op === "ack") {
                        if (
                            typeof parsed.path === "string" &&
                            typeof parsed.throughId === "number" &&
                            Number.isSafeInteger(parsed.throughId) &&
                            parsed.throughId > 0
                        ) {
                            acknowledgedThrough.set(
                                parsed.path,
                                Math.max(
                                    acknowledgedThrough.get(parsed.path) ?? 0,
                                    parsed.throughId,
                                ),
                            );
                            fallbackId = Math.max(fallbackId, parsed.throughId + 1);
                        }
                        continue;
                    }
                    if (
                        typeof parsed.path !== "string" ||
                        (parsed.action !== "created" &&
                            parsed.action !== "modified" &&
                            parsed.action !== "deleted" &&
                            parsed.action !== "renamed") ||
                        typeof parsed.ts !== "number"
                    ) {
                        continue;
                    }
                    const id =
                        typeof parsed.id === "number" && Number.isSafeInteger(parsed.id) && parsed.id > 0
                            ? parsed.id
                            : fallbackId;
                    fallbackId = Math.max(fallbackId + 1, id + 1);
                    // A persisted row represents pending work. `synced` was only
                    // ever an in-memory flag in older releases.
                    loaded.push({
                        id,
                        action: parsed.action as JournalEntry["action"],
                        path: parsed.path,
                        oldPath: typeof parsed.oldPath === "string" ? parsed.oldPath : undefined,
                        ts: parsed.ts,
                        synced: false,
                    });
                } catch {
                    // A hard process kill can tear the final append. Preserve
                    // every independently parseable row instead of discarding
                    // the complete WAL.
                }
            }

            this.entries = loaded.filter(
                (entry) => entry.id > (acknowledgedThrough.get(entry.path) ?? 0),
            );
            this.nextId = Math.max(fallbackId, ...loaded.map((entry) => entry.id + 1));
            this.needsLeadingNewline = raw.length > 0 && !raw.endsWith("\n");

            if (recordsRead >= COMPACT_EVERY) {
                const before = this.entries.length;
                this.compactLatestPerPath();
                if (this.entries.length < before || recordsRead > this.entries.length) {
                    await this.rewrite();
                }
            }
        });
    }

    /** Append an entry before scheduling any sync work for the file. */
    async append(entry: NewJournalEntry): Promise<number> {
        return this.enqueue(async () => {
            await this.ensureDir();
            const persisted: JournalEntry = {
                ...entry,
                id: this.nextId++,
                synced: false,
            };
            const prefix = this.needsLeadingNewline ? "\n" : "";
            await this.app.vault.adapter.append(
                JOURNAL_PATH,
                `${prefix}${JSON.stringify(persisted)}\n`,
            );
            this.needsLeadingNewline = false;
            this.entries.push(persisted);
            this.appendsSinceCompact++;

            if (this.appendsSinceCompact >= COMPACT_EVERY) {
                const before = this.entries.length;
                this.compactLatestPerPath();
                if (this.entries.length < before) await this.rewrite();
                this.appendsSinceCompact = 0;
            }
            return persisted.id;
        });
    }

    /** Return a stable copy of all pending records. */
    unsynced(): JournalEntry[] {
        return this.entries.map((entry) => ({ ...entry }));
    }

    /** Remove only records included in a successfully committed push.
     *  Acknowledgements are appended as WAL records before memory is changed,
     *  so this stays O(paths) instead of rewriting a large offline journal for
     *  every pull echo or push batch. A newer id on the path remains pending. */
    async acknowledge(watermarks: Array<{ path: string; throughId: number }>): Promise<void> {
        if (watermarks.length === 0) return;
        await this.enqueue(async () => {
            const byPath = new Map<string, number>();
            for (const watermark of watermarks) {
                byPath.set(
                    watermark.path,
                    Math.max(byPath.get(watermark.path) ?? 0, watermark.throughId),
                );
            }
            const remaining = this.entries.filter(
                (entry) => entry.id > (byPath.get(entry.path) ?? 0),
            );
            if (remaining.length === this.entries.length) return;
            await this.ensureDir();
            const acknowledgements: JournalAcknowledgement[] = [...byPath].map(
                ([path, throughId]) => ({ op: "ack", path, throughId }),
            );
            const prefix = this.needsLeadingNewline ? "\n" : "";
            await this.app.vault.adapter.append(
                JOURNAL_PATH,
                `${prefix}${acknowledgements.map((entry) => JSON.stringify(entry)).join("\n")}\n`,
            );
            this.needsLeadingNewline = false;
            this.entries = remaining;
            this.appendsSinceCompact += acknowledgements.length;
            if (this.appendsSinceCompact >= COMPACT_EVERY) {
                this.compactLatestPerPath();
                await this.rewrite();
                this.appendsSinceCompact = 0;
            }
        });
    }

    /** Explicitly discard the WAL, for reset/unlink flows only. */
    async clear(): Promise<void> {
        await this.enqueue(async () => {
            this.entries = [];
            this.needsLeadingNewline = false;
            this.appendsSinceCompact = 0;
            await this.ensureDir();
            await this.app.vault.adapter.write(JOURNAL_PATH, "");
        });
    }

    /** Coalesce duplicate paths without ever dropping a unique dirty path. */
    private compactLatestPerPath(): void {
        const latest = new Map<string, JournalEntry>();
        for (const entry of this.entries) {
            if (entry.action === "renamed" && entry.oldPath) {
                // Pre-1.10.1 journals encoded a rename as one row keyed only
                // by its destination. A later edit of that destination used
                // to replace the row during compaction and silently lose the
                // old-path deletion. Expand the legacy row into the two final
                // path states before coalescing.
                latest.set(entry.oldPath, {
                    ...entry,
                    action: "deleted",
                    path: entry.oldPath,
                    oldPath: undefined,
                });
                latest.set(entry.path, {
                    ...entry,
                    action: "modified",
                    oldPath: undefined,
                });
                continue;
            }
            latest.set(entry.path, entry);
        }
        this.entries = [...latest.values()].sort((a, b) => a.id - b.id);
    }

    private async rewrite(): Promise<void> {
        await this.ensureDir();
        const contents = this.entries.map((entry) => JSON.stringify(entry)).join("\n");
        await this.app.vault.adapter.write(JOURNAL_PATH, contents ? `${contents}\n` : "");
        this.needsLeadingNewline = false;
    }

    private async ensureDir(): Promise<void> {
        if (this.dirEnsured) return;
        const dir = JOURNAL_PATH.substring(0, JOURNAL_PATH.lastIndexOf("/"));
        if (!(await this.app.vault.adapter.exists(dir))) {
            await this.app.vault.adapter.mkdir(dir);
        }
        this.dirEnsured = true;
    }

    private enqueue<T>(operation: () => Promise<T>): Promise<T> {
        const result = this.writeChain.then(operation, operation);
        this.writeChain = result.then(
            () => undefined,
            () => undefined,
        );
        return result;
    }
}
