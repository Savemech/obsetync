import type { App } from "obsidian";
import { isSafeVaultPath } from "./delta-validation";
import { validateJournalEntry, JournalRecoveryError } from "./journal-format";
import { JournalIndex, journalRecordKey } from "./journal-index";
import { importLegacyJournal } from "./journal-legacy";
import { SegmentedStore, STORE_LIMITS, StoreRecoveryError, createMigrationProof } from "./segmented-store";
import { yieldWork } from "./work-scheduler";
import { assertLegacyDowngradeReadable } from "./legacy-downgrade";
export { JournalRecoveryError } from "./journal-format";
export { JOURNAL_PATHS } from "./journal-legacy";

export interface JournalEntry {
    /** Monotonic local mutation position, retained after compaction/clear. */
    id: number;
    action: "created" | "modified" | "deleted" | "renamed";
    path: string;
    oldPath?: string;
    ts: number;
    synced: boolean;
    /** Optional scan result. It is a source hint, never publication authority;
     * recovery reuses it only after an exact size/mtime restat. */
    hash?: string;
    mtime?: number;
    size?: number;
}

export type NewJournalEntry = Omit<JournalEntry, "id">;
export const JOURNAL_STORE_PATH = ".obsidian/plugins/obsetync/change-journal.store-v1";
const MAX_BATCH_REQUESTS = 256;
type Watermark = { path: string; throughId: number };
type Operation = { schema: 1; op: "append"; entry: JournalEntry } |
    { schema: 1; op: "ack"; path: string; throughId: number };
interface State { index: JournalIndex; nextId: number }
interface Request {
    kind: "load" | "append" | "append-batch" | "ack" | "compact" | "clear";
    entry?: NewJournalEntry;
    entries?: readonly NewJournalEntry[];
    watermarks?: Watermark[];
    expectedEpoch?: string;
    resolve(value?: any): void;
    reject(error: unknown): void;
    next: Request | null;
}
interface Prepared {
    request: Request;
    entry?: JournalEntry;
    entries?: JournalEntry[];
    watermarks?: Watermark[];
}

/** Immutable segmented journal with generation-safe coalescing and independent
 * rename endpoint ACKs. Adjacent pending requests share one verified head;
 * individual callers resolve only after their complete batch is published.
 *
 * Controlled frames and replay steps are bounded. Normal live rows coalesce
 * by path, but unresolved rename graph and awaiting authoritative callers are
 * not a hard RSS/backlog limit; no governor growth is licensed by this class. */
export class ObsetyncJournal {
    private index = new JournalIndex();
    private nextId = 1;
    private writable = false;
    private head: Request | null = null;
    private tail: Request | null = null;
    private draining = false;
    private readonly store: SegmentedStore;
    private closing = false;
    private drainWork: Promise<void> | null = null;
    private retirement?: Promise<void>;

    constructor(private app: App) {
        this.store = new SegmentedStore(app.vault.adapter, JOURNAL_STORE_PATH);
    }

    load(): Promise<void> { return this.enqueue({ kind: "load" }); }

    get validatedEpoch(): string | null { return this.writable ? this.store.validatedEpoch : null; }

    append(entry: NewJournalEntry): Promise<number> {
        try {
            const checked = validateJournalEntry({ ...entry, id: 1 });
            const { id: _id, ...detached } = checked;
            return this.enqueue({ kind: "append", entry: Object.freeze(detached) });
        } catch (error) { return Promise.reject(error); }
    }

    /** One bounded durable cut for scan-discovered changes. IDs and the public
     * index become visible only after the complete segmented-store head is
     * published and read back. This avoids one fs transaction per scanned
     * file without creating an unbounded caller promise queue. */
    appendBatch(entries: readonly NewJournalEntry[]): Promise<number[]> {
        try {
            if (!Array.isArray(entries) || entries.length > MAX_BATCH_REQUESTS) {
                throw new JournalRecoveryError("LIMIT", "journal append batch exceeds request ceiling");
            }
            if (entries.length === 0) return Promise.resolve([]);
            const detached = entries.map(entry => {
                const checked = validateJournalEntry({ ...entry, id: 1 });
                const { id: _id, ...value } = checked;
                return Object.freeze(value);
            });
            return this.enqueue({ kind: "append-batch", entries: Object.freeze(detached) });
        } catch (error) { return Promise.reject(error); }
    }

    /** One logical rename remains one durable row, including its independent
     * endpoint acknowledgement flags in every later snapshot. */
    async appendGroup(entries: readonly NewJournalEntry[]): Promise<number[]> {
        if (entries.length !== 2 || entries[0].action !== "deleted" || entries[1].action !== "created" ||
            entries[0].path === entries[1].path) throw new Error("journal group must be one linked rename");
        for (const entry of entries) validateJournalEntry({ ...entry, id: 1 });
        const id = await this.append({ action: "renamed", oldPath: entries[0].path, path: entries[1].path,
            ts: Math.max(entries[0].ts, entries[1].ts), synced: false });
        return [id, id];
    }

    /** Detached compatibility view. Prefer the captured iterator for replay. */
    unsynced(): JournalEntry[] { return [...this.iterateUnsynced()]; }
    iterateUnsynced(): IterableIterator<JournalEntry> { return this.index.entries(); }
    unsyncedCount(): number { return this.index.size; }
    capturePendingPaths(): { has(path: string): boolean } {
        // An unavailable journal must not license overwriting unknown offline
        // work. Otherwise capture a stable AVL root without an O(n) path Set.
        if (!this.writable) return { has: () => true };
        const captured = this.index;
        return { has: path => captured.hasPath(path) };
    }

    acknowledge(watermarks: Watermark[]): Promise<void> {
        try {
            if (watermarks.length === 0) return Promise.resolve();
            // Capture caller-owned values before any await. A newer callback
            // cannot replace the watermark of an older in-flight publication.
            const byPath = new Map<string, number>();
            for (const { path, throughId } of watermarks) {
                validateWatermark(path, throughId);
                byPath.set(path, Math.max(byPath.get(path) ?? 0, throughId));
            }
            return this.enqueue({ kind: "ack", watermarks: [...byPath].map(([path, throughId]) => ({ path, throughId })) });
        } catch (error) { return Promise.reject(error); }
    }

    /** Exact persisted publication cut, not a watermark reconstructed from
     * the current queue. Recheck epoch/issued IDs inside the journal writer so
     * a queued reload cannot turn an old ACK into authority over a new store. */
    acknowledgeOwned(expectedEpoch: string, watermarks: readonly Watermark[]): Promise<void> {
        try {
            if (!/^[0-9a-f]{32}$/.test(expectedEpoch) || !Array.isArray(watermarks) || watermarks.length > 256) {
                throw new JournalRecoveryError("CORRUPT", "invalid owned journal acknowledgement");
            }
            const detached: Watermark[] = [];
            let previous: string | null = null;
            for (const { path, throughId } of watermarks) {
                validateWatermark(path, throughId);
                if (previous !== null && path <= previous) throw new JournalRecoveryError("CORRUPT", "owned journal cut is not strictly ordered");
                previous = path; detached.push({ path, throughId });
            }
            // Even an empty cut must validate its epoch before settlement.
            return this.enqueue({ kind: "ack", watermarks: detached, expectedEpoch });
        } catch (error) { return Promise.reject(error); }
    }

    compact(): Promise<void> { return this.enqueue({ kind: "compact" }); }
    /** Explicit reset preserves the mutation high watermark and store epoch. */
    clear(): Promise<void> { return this.enqueue({ kind: "clear" }); }

    runMaintenance(): Promise<void> { return this.store.runMaintenance(); }
    drainMaintenance(): Promise<void> { return this.store.drainMaintenance(); }

    closeAndDrain(): Promise<void> {
        if (this.retirement) return this.retirement;
        this.closing = true;
        this.retirement = (async () => {
            await this.drainWork;
            await this.store.closeAndDrainMaintenance();
            this.writable = false;
        })();
        return this.retirement;
    }

    private async loadInsideQueue(): Promise<void> {
        this.writable = false;
        await assertLegacyDowngradeReadable(this.app.vault.adapter);
        const state: State = { index: new JournalIndex(), nextId: 1 };
        let previousKey: string | null = null;
        let retryInitialization = false;
        let present = false;
        try {
            present = await this.store.load(metadata => { state.nextId = validateMetadata(metadata); }, row => {
                const candidate = validateSnapshotRow(row, state.nextId);
                const key = journalRecordKey(candidate);
                if (previousKey !== null && key <= previousKey) throw new JournalRecoveryError("CORRUPT", "journal snapshot order differs");
                previousKey = key;
                state.index = state.index.addSnapshotRecord(candidate);
            }, async row => { await applyOperation(state, row); });
        } catch (error) {
            if (!(error instanceof StoreRecoveryError) || error.code !== "RECOVERY_REQUIRED") throw error;
            // Retry is opt-in, never a guessed empty epoch: the store verifies
            // the exact legacy proof and absence of an advance fence/heads.
            retryInitialization = true;
        }
        if (!present) {
            const imported = await importLegacyJournal(this.app.vault.adapter);
            state.index = new JournalIndex();
            state.nextId = imported.parsed.nextId;
            let processed = 0;
            for (const entry of imported.parsed.mutations) {
                state.index = state.index.append(entry);
                if (++processed % 256 === 0) await yieldWork();
            }
            for (const { path, throughId } of imported.parsed.acknowledgements) {
                await applyAcknowledgement(state, path, throughId);
            }
            const metadata = { schema: 1, kind: "journal", nextId: state.nextId };
            const proof = await createMigrationProof("journal-legacy-v1", imported.sourceSha256, metadata);
            const directory = JOURNAL_STORE_PATH.slice(0, JOURNAL_STORE_PATH.lastIndexOf("/"));
            if (!(await this.app.vault.adapter.exists(directory))) await this.app.vault.adapter.mkdir(directory);
            if (retryInitialization) await this.store.retryInitialization(state.index.records(), metadata, proof);
            else await this.store.initializeMigration(state.index.records(), metadata, proof);
        }
        this.index = state.index;
        this.nextId = state.nextId;
        this.writable = true;
    }

    private async mutateInsideQueue(requests: Request[]): Promise<void> {
        this.assertWritable();
        for (const request of requests) {
            if (request.expectedEpoch !== undefined && (request.expectedEpoch !== this.store.validatedEpoch ||
                request.watermarks!.some(cut => cut.throughId >= this.nextId))) {
                throw new JournalRecoveryError("RECOVERY_REQUIRED", "owned journal acknowledgement epoch or issued generation differs");
            }
        }
        // Maintenance before new writes, not after returning their IDs. A
        // rejected snapshot cannot ambiguously report a newly accepted append.
        if (this.store.walSegments >= STORE_LIMITS.compactAfterSegments) await this.snapshot(this.index);
        const state: State = { index: this.index, nextId: this.nextId };
        const prepared: Prepared[] = [];
        let processed = 0;
        for (const request of requests) {
            if (request.kind === "append" || request.kind === "append-batch") {
                const source = request.kind === "append" ? [request.entry!] : request.entries!;
                const entries: JournalEntry[] = [];
                for (const input of source) {
                    const entry = validateJournalEntry({ ...input, id: state.nextId });
                    state.index = state.index.append(entry);
                    state.nextId = entry.id + 1;
                    entries.push(entry);
                }
                prepared.push(request.kind === "append"
                    ? { request, entry: entries[0] }
                    : { request, entries });
            } else {
                const effective: Watermark[] = [];
                for (const watermark of request.watermarks!) {
                    const before = state.index;
                    const beforeId = state.nextId;
                    await applyAcknowledgement(state, watermark.path, watermark.throughId);
                    if (before !== state.index || beforeId !== state.nextId) effective.push(watermark);
                    if (++processed % 256 === 0) await yieldWork();
                }
                prepared.push({ request, watermarks: effective });
            }
            if (++processed % 256 === 0) await yieldWork();
        }
        function* operations(): IterableIterator<Operation> {
            for (const item of prepared) {
                if (item.entry) yield { schema: 1, op: "append", entry: item.entry };
                else if (item.entries) {
                    for (const entry of item.entries) yield { schema: 1, op: "append", entry };
                }
                else for (const watermark of item.watermarks!) yield { schema: 1, op: "ack", ...watermark };
            }
        }
        await this.store.commit(operations());
        // No public pending view/ID advances ahead of the verified head.
        this.index = state.index;
        this.nextId = state.nextId;
        for (const item of prepared) {
            item.request.resolve(item.entry?.id ?? item.entries?.map(entry => entry.id));
        }
    }

    private async snapshot(index: JournalIndex): Promise<void> {
        await this.store.snapshot(index.records(), { schema: 1, kind: "journal", nextId: this.nextId });
    }

    private enqueue<T>(work: Pick<Request, "kind" | "entry" | "entries" | "watermarks" | "expectedEpoch">): Promise<T> {
        if (this.closing) {
            return Promise.reject(new JournalRecoveryError("RECOVERY_REQUIRED", "journal is closing"));
        }
        const result = new Promise<T>((resolve, reject) => {
            const request: Request = { ...work, resolve, reject, next: null };
            if (this.tail) this.tail.next = request;
            else this.head = request;
            this.tail = request;
        });
        if (!this.draining) {
            this.draining = true;
            // Same-turn callbacks and callbacks arriving behind native IO can
            // share one bounded batch without a fixed debounce timer.
            const flight = Promise.resolve().then(() => this.drain());
            this.drainWork = flight;
            void flight.then(() => {
                if (this.drainWork === flight) this.drainWork = null;
            }, () => {
                if (this.drainWork === flight) this.drainWork = null;
            });
        }
        return result;
    }

    private take(): Request {
        const request = this.head!;
        this.head = request.next;
        if (!this.head) this.tail = null;
        request.next = null;
        return request;
    }

    private async drain(): Promise<void> {
        try {
            while (this.head) {
                const requests: Request[] = [];
                const first = this.take();
                requests.push(first);
                if (first.kind === "append" || first.kind === "append-batch" || first.kind === "ack") {
                    let rows = first.kind === "ack" ? first.watermarks!.length :
                        first.kind === "append-batch" ? first.entries!.length : 1;
                    while (this.head && requests.length < MAX_BATCH_REQUESTS &&
                        (this.head.kind === "append" || this.head.kind === "append-batch" ||
                            this.head.kind === "ack")) {
                        const nextRows = this.head.kind === "ack" ? this.head.watermarks!.length :
                            this.head.kind === "append-batch" ? this.head.entries!.length : 1;
                        if (rows + nextRows > STORE_LIMITS.rowsPerPage) break;
                        rows += nextRows;
                        requests.push(this.take());
                    }
                }
                try {
                    if (first.kind === "load") {
                        await this.loadInsideQueue();
                        first.resolve();
                    } else if (first.kind === "compact" || first.kind === "clear") {
                        this.assertWritable();
                        const index = first.kind === "clear" ? new JournalIndex() : this.index;
                        await this.snapshot(index);
                        this.index = index;
                        first.resolve();
                    } else await this.mutateInsideQueue(requests);
                } catch (failure) {
                    this.writable = false;
                    const error = failure instanceof StoreRecoveryError
                        ? new JournalRecoveryError(failure.code, failure.message) : failure;
                    for (const request of requests) request.reject(error);
                }
            }
        } finally { this.draining = false; }
    }

    private assertWritable(): void {
        if (!this.writable) throw new JournalRecoveryError("RECOVERY_REQUIRED", "journal requires validated recovery before further writes");
    }
}

function validateWatermark(path: unknown, throughId: unknown): asserts throughId is number {
    if (!isSafeVaultPath(path) || !Number.isSafeInteger(throughId) ||
        (throughId as number) <= 0 || (throughId as number) >= Number.MAX_SAFE_INTEGER) {
        throw new JournalRecoveryError("CORRUPT", "invalid journal acknowledgement watermark");
    }
}

function validateMetadata(value: any): number {
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new JournalRecoveryError("CORRUPT", "invalid journal metadata");
    if (value.schema !== 1) throw new JournalRecoveryError("UNKNOWN_SCHEMA", "unsupported journal metadata schema");
    if (value.kind !== "journal" || !Number.isSafeInteger(value.nextId) || value.nextId < 1) {
        throw new JournalRecoveryError("CORRUPT", "invalid journal mutation watermark");
    }
    return value.nextId;
}

function validateSnapshotRow(value: any, nextId: number): any {
    const entry = validateJournalEntry(value?.entry);
    if (entry.id >= nextId) throw new JournalRecoveryError("CORRUPT", "journal snapshot watermark trails a mutation");
    return value;
}

async function applyAcknowledgement(state: State, path: string, throughId: number): Promise<void> {
    validateWatermark(path, throughId);
    let processed = 0;
    for (const next of state.index.acknowledgeSteps(path, throughId)) {
        state.index = next;
        if (++processed % 256 === 0) await yieldWork();
    }
    state.nextId = Math.max(state.nextId, throughId + 1);
}

async function applyOperation(state: State, value: any): Promise<void> {
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new JournalRecoveryError("CORRUPT", "invalid journal WAL row");
    if (value.schema !== 1) throw new JournalRecoveryError("UNKNOWN_SCHEMA", "unsupported journal WAL schema");
    if (value.op === "append") {
        const entry = validateJournalEntry(value.entry);
        if (entry.id !== state.nextId) throw new JournalRecoveryError("CORRUPT", "journal mutation sequence moved backwards or skipped");
        state.index = state.index.append(entry);
        state.nextId = entry.id + 1;
    } else if (value.op === "ack") {
        await applyAcknowledgement(state, value.path, value.throughId);
    } else throw new JournalRecoveryError("UNKNOWN_SCHEMA", "unsupported journal operation");
}
