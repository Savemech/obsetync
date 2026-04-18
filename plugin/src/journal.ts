import { App } from "obsidian";

export interface JournalEntry {
    action: "created" | "modified" | "deleted" | "renamed";
    path: string;
    oldPath?: string;
    ts: number;
    synced: boolean;
}

const JOURNAL_PATH = ".obsidian/plugins/obsetync/change-journal.ndjson";
const MAX_ENTRIES = 10000;

/**
 * Persistent change journal — Layer 2 of the 4-layer change tracking (D-005).
 * Written to disk immediately on every vault event, survives app kill.
 *
 * Append-only: each new entry is appended as a single JSON line.
 * On truncate (after successful recovery), the file is cleared.
 * markSynced is in-memory only — on restart all entries appear unsynced,
 * which is fine since recoverFromJournal is idempotent.
 */
export class Journal {
    private entries: JournalEntry[] = [];
    private dirEnsured = false;

    constructor(private app: App) {}

    async load(): Promise<void> {
        try {
            const raw = await this.app.vault.adapter.read(JOURNAL_PATH);
            this.entries = raw
                .split("\n")
                .filter((line) => line.trim())
                .map((line) => JSON.parse(line));
        } catch {
            this.entries = [];
        }
    }

    /** Append an entry — must be called BEFORE any sync work on the file. */
    async append(entry: JournalEntry): Promise<void> {
        this.entries.push(entry);

        // Cap in-memory size to prevent unbounded growth.
        if (this.entries.length > MAX_ENTRIES) {
            this.entries = this.entries.slice(-MAX_ENTRIES);
        }

        await this.appendLine(JSON.stringify(entry));
    }

    /** Get all unsynced entries. */
    unsynced(): JournalEntry[] {
        return this.entries.filter((e) => !e.synced);
    }

    /** Mark a path as synced in memory (not flushed — transient state). */
    markSynced(path: string): void {
        for (const entry of this.entries) {
            if (entry.path === path && !entry.synced) {
                entry.synced = true;
            }
        }
    }

    /** Clear the journal after successful recovery. */
    async truncate(): Promise<void> {
        this.entries = [];
        await this.ensureDir();
        await this.app.vault.adapter.write(JOURNAL_PATH, "");
    }

    /** Append a single line to the journal file. */
    private async appendLine(line: string): Promise<void> {
        await this.ensureDir();
        // Read current content and append — Obsidian adapter has no appendFile API.
        let current = "";
        try {
            current = await this.app.vault.adapter.read(JOURNAL_PATH);
        } catch {
            // File doesn't exist yet — start fresh.
        }
        const separator = current && !current.endsWith("\n") ? "\n" : "";
        await this.app.vault.adapter.write(JOURNAL_PATH, current + separator + line);
    }

    /** Ensure the journal directory exists (checked once per session). */
    private async ensureDir(): Promise<void> {
        if (this.dirEnsured) return;
        const dir = JOURNAL_PATH.substring(0, JOURNAL_PATH.lastIndexOf("/"));
        if (!(await this.app.vault.adapter.exists(dir))) {
            await this.app.vault.adapter.mkdir(dir);
        }
        this.dirEnsured = true;
    }
}
