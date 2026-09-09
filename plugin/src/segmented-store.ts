import { checksumJournalUtf8 } from "./journal-format";
import { isMissingPathError } from "./file-safety";
import { yieldWork } from "./work-scheduler";

/** Portable, bounded immutable pages. Completion is adapter completion, not
 * fsync. The caller validates row semantics into a provisional index on load. */
export interface SegmentedStoreIO {
    exists(path: string): Promise<boolean>;
    read(path: string): Promise<string>;
    write(path: string, data: string): Promise<void>;
    mkdir(path: string): Promise<void>;
    rename(from: string, to: string): Promise<void>;
    remove(path: string): Promise<void>;
    stat?(path: string): Promise<{ type?: string; size: number } | null>;
}

export const STORE_LIMITS = {
    payloadBytes: 128 * 1024, frameBytes: 512 * 1024, headBytes: 128 * 1024,
    rowsPerPage: 256, snapshotPages: 8192, replaySegments: 2048,
    compactAfterSegments: 128,
} as const;

const DEFAULT_CLEANUP_JOBS = 64;
const MAX_CLEANUP_JOBS = 1024;

export interface SegmentedStoreOptions {
    yield?: () => Promise<void>;
    /** Admission for one bounded cleanup IO slice. The default uses the
     * scheduler's low-priority maintenance lane, independently of page IO. */
    maintenanceYield?: (signal: AbortSignal) => Promise<void>;
    /** Tests and lifecycle owners may drive runMaintenance() explicitly. */
    automaticMaintenance?: boolean;
    maxPendingCleanupJobs?: number;
}

export class StoreRecoveryError extends Error {
    constructor(readonly code: "CORRUPT" | "UNKNOWN_SCHEMA" | "READ_FAILED" |
        "RECOVERY_REQUIRED" | "LIMIT" | "CONTRADICTING_COPIES", message: string) {
        super(message); this.name = "StoreRecoveryError";
    }
}

export interface MigrationProof {
    schema: 1;
    /** Versioned domain/parser, not a filename or user-facing description. */
    sourceKind: string;
    /** SHA-256 of the caller's exact legacy-copy/role/absence manifest. */
    sourceSha256: string;
    /** SHA-256 of the exact detached JSON metadata cut. */
    metadataSha256: string;
}

const MIGRATION_RECORD_BYTES = 4096;
interface MigrationMarker {
    schema: 1;
    kind: "migration-init" | "migration-advanced";
    target: string;
    epoch: string;
    proof: MigrationProof;
}

export function segmentedMigrationPaths(directory: string): { intent: string; advanced: string } {
    return { intent: `${directory}.init-v1.json`, advanced: `${directory}.advanced-v1.json` };
}

function checkedProof(value: unknown): MigrationProof {
    if (!plain(value) || value.schema !== 1) {
        throw new StoreRecoveryError("UNKNOWN_SCHEMA", "unsupported migration proof schema");
    }
    if (typeof value.sourceKind !== "string" || !/^[a-z0-9][a-z0-9._/-]{0,63}$/.test(value.sourceKind) ||
        typeof value.sourceSha256 !== "string" || !/^[0-9a-f]{64}$/.test(value.sourceSha256) ||
        typeof value.metadataSha256 !== "string" || !/^[0-9a-f]{64}$/.test(value.metadataSha256)) fail("invalid migration proof");
    return { schema: 1, sourceKind: value.sourceKind, sourceSha256: value.sourceSha256,
        metadataSha256: value.metadataSha256 };
}

function metadataJSON(metadata: unknown): string {
    let raw: string | undefined;
    try { raw = JSON.stringify(metadata); } catch { fail("migration metadata cannot be encoded"); }
    if (raw === undefined) fail("migration metadata cannot be encoded");
    if (checksumJournalUtf8(raw).bytes > STORE_LIMITS.headBytes) {
        throw new StoreRecoveryError("LIMIT", "migration metadata exceeds bounded proof size");
    }
    return raw;
}

async function metadataDigest(raw: string): Promise<string> {
    try {
        const bytes = new TextEncoder().encode(raw);
        const digest = new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", bytes as BufferSource));
        return [...digest].map(byte => byte.toString(16).padStart(2, "0")).join("");
    } catch {
        throw new StoreRecoveryError("READ_FAILED", "migration proof hashing unavailable");
    }
}

/** Caller owns source quiescence and the stable rows corresponding to this
 * exact source manifest. This helper never reads or modifies legacy originals. */
export async function createMigrationProof(sourceKind: string, sourceSha256: string, metadata: unknown): Promise<MigrationProof> {
    const proof = checkedProof({ schema: 1, sourceKind, sourceSha256, metadataSha256: "0".repeat(64) });
    proof.metadataSha256 = await metadataDigest(metadataJSON(metadata));
    return proof;
}

interface StoreHead {
    schema: 1;
    kind: "head";
    epoch: string;
    generation: number;
    sequence: number;
    snapshot: { id: number; pages: number; cut: number; metadata: unknown };
    walStart: number;
    walEnd: number;
    nextSegment: number;
}

interface Page {
    schema: 1;
    kind: "snapshot" | "wal";
    epoch: string;
    id: number;
    part?: number;
    from: number;
    through: number;
    rows: unknown[];
}

interface CleanupTarget {
    kind: "snapshot" | "wal";
    epoch: string;
    id: number;
    start: number;
    end: number;
    next: number;
}

interface CleanupJob {
    targets: CleanupTarget[];
    target: number;
    failed: boolean;
    ready: boolean;
}

const integer = (value: unknown, min = 0): value is number =>
    typeof value === "number" && Number.isSafeInteger(value) && value >= min && value < Number.MAX_SAFE_INTEGER;
const plain = (value: unknown): value is Record<string, any> =>
    !!value && typeof value === "object" && !Array.isArray(value);
function fail(message: string): never { throw new StoreRecoveryError("CORRUPT", message); }

function seal(value: unknown, limit: number): string {
    const payload = JSON.stringify(value);
    const checksum = checksumJournalUtf8(payload);
    const encoded = JSON.stringify({ schema: 1, payload, ...checksum });
    if (checksumJournalUtf8(encoded).bytes > limit) throw new StoreRecoveryError("LIMIT", "storage frame exceeds bounded encoding");
    return encoded;
}

function unseal(raw: string, payloadLimit: number = STORE_LIMITS.payloadBytes): any {
    let frame: any;
    try { frame = JSON.parse(raw); } catch { fail("storage frame is incomplete or corrupt"); }
    if (!plain(frame)) fail("invalid storage frame");
    if (frame.schema !== 1) throw new StoreRecoveryError("UNKNOWN_SCHEMA", "unsupported storage frame schema");
    if (typeof frame.payload !== "string") fail("invalid storage payload");
    const checksum = checksumJournalUtf8(frame.payload);
    if (checksum.bytes !== frame.bytes || checksum.crc32 !== frame.crc32) fail("storage frame checksum differs");
    if (checksum.bytes > payloadLimit) throw new StoreRecoveryError("LIMIT", "sealed payload exceeds bounded decoding");
    let value: any;
    try { value = JSON.parse(frame.payload); } catch { fail("invalid sealed JSON payload"); }
    if (!plain(value)) fail("invalid sealed record");
    if (value.schema !== 1) throw new StoreRecoveryError("UNKNOWN_SCHEMA", "unsupported storage record schema");
    return value;
}

function validateHead(value: any): StoreHead {
    if (value.kind !== "head" || typeof value.epoch !== "string" || !/^[0-9a-f]{32}$/.test(value.epoch) ||
        !integer(value.generation, 1) || !integer(value.sequence) || !plain(value.snapshot) ||
        !Object.prototype.hasOwnProperty.call(value.snapshot, "metadata") ||
        !integer(value.snapshot.id, 1) || value.snapshot.id > value.generation ||
        !integer(value.snapshot.pages) || value.snapshot.pages > STORE_LIMITS.snapshotPages ||
        !integer(value.snapshot.cut) || value.snapshot.cut > value.sequence ||
        !integer(value.walStart, 1) || !integer(value.walEnd) || !integer(value.nextSegment, 1) ||
        value.nextSegment !== value.walEnd + 1 || value.walStart > value.nextSegment ||
        value.walEnd - value.walStart + 1 > STORE_LIMITS.replaySegments ||
        (value.walStart === value.nextSegment && value.sequence !== value.snapshot.cut)) fail("invalid storage head bounds");
    return value as StoreHead;
}

/** One head publishes a complete batch, including a cursor that follows its
 * entry mutations. Referenced pages are immutable. No listing or multi-year WAL
 * read is needed to recover. A failed publication poisons this writer until
 * validated reload; unreferenced staged objects never become acknowledged work.
 *
 * Superseded pages/segments are reclaimed only after a verified successor,
 * preserving both recoverable heads' transitive closures. Unpublished orphan
 * discovery is separate; this does not perform a directory-wide GC.
 * The caller's live index and in-memory pending mutations are separate budgets. */
export class SegmentedStore {
    private head: StoreHead | null = null;
    private writable = false;
    private absent = false;
    private cleanupDeferred = 0;
    private migration: MigrationMarker | null = null;
    private migrationAdvanced = false;
    private chain: Promise<void> = Promise.resolve();
    private readonly pause: () => Promise<void>;
    private readonly maintenancePause: (signal: AbortSignal) => Promise<void>;
    private readonly maintenanceAbort = new AbortController();
    private readonly automaticMaintenance: boolean;
    private readonly maxPendingCleanupJobs: number;
    private readonly cleanupJobs: CleanupJob[] = [];
    private maintenanceScheduled = false;
    private maintenanceClosed = false;
    private automaticMaintenanceFlight: Promise<void> | null = null;
    private explicitMaintenanceFlight: Promise<void> | null = null;
    private maintenanceRetirement: Promise<void> | null = null;

    constructor(private readonly io: SegmentedStoreIO, readonly directory: string,
        options: SegmentedStoreOptions = {}) {
        this.pause = options.yield ?? (() => yieldWork());
        this.maintenancePause = options.maintenanceYield ?? options.yield ??
            (signal => yieldWork({ lane: "maintenance", signal }));
        this.automaticMaintenance = options.automaticMaintenance ?? true;
        this.maxPendingCleanupJobs = options.maxPendingCleanupJobs ?? DEFAULT_CLEANUP_JOBS;
        if (!Number.isSafeInteger(this.maxPendingCleanupJobs) || this.maxPendingCleanupJobs < 1 ||
            this.maxPendingCleanupJobs > MAX_CLEANUP_JOBS) {
            throw new RangeError(`maxPendingCleanupJobs must be an integer between 1 and ${MAX_CLEANUP_JOBS}`);
        }
    }

    get walSegments(): number { return this.head ? this.head.walEnd - this.head.walStart + 1 : 0; }
    get sequence(): number { return this.head?.sequence ?? 0; }
    /** Only a completely validated/current writable publication supplies an
     * epoch for cross-store ownership. Never expose a stale cut after IO error. */
    get validatedEpoch(): string | null { return this.writable ? this.head?.epoch ?? null : null; }
    get deferredCleanupCount(): number { return this.cleanupDeferred; }
    get pendingCleanupJobs(): number { return this.cleanupJobs.length; }

    /** Retry every currently known cleanup job for at most one complete pass.
     * Persistent failures remain queued but do not hot-loop. */
    runMaintenance(): Promise<void> {
        if (this.maintenanceClosed) return Promise.resolve();
        if (this.explicitMaintenanceFlight) return this.explicitMaintenanceFlight;
        const flight = this.runExplicitMaintenancePass();
        this.explicitMaintenanceFlight = flight;
        void flight.then(() => {
            if (this.explicitMaintenanceFlight === flight) this.explicitMaintenanceFlight = null;
        }, () => {
            if (this.explicitMaintenanceFlight === flight) this.explicitMaintenanceFlight = null;
        });
        return flight;
    }

    /** Run one explicit retry pass, join every automatic cleanup flight that
     * it overlaps or causes to be rescheduled, then fence the serialized IO
     * writer. Unlike closeAndDrainMaintenance(), this keeps future cleanup
     * admission open for a long-lived store. */
    async drainMaintenance(): Promise<void> {
        if (this.maintenanceClosed) return;
        await this.runMaintenance();
        for (;;) {
            const automatic = this.automaticMaintenanceFlight;
            if (automatic) {
                await automatic.catch(() => {});
                continue;
            }
            await this.enqueue(async () => {});
            if (!this.automaticMaintenanceFlight) return;
        }
    }

    /** Stop new cleanup admission and join every already-started scheduler wait
     * and exact-path IO slice. The store's authoritative writer queue remains a
     * separate owner; production callers close it before invoking this hook. */
    closeAndDrainMaintenance(): Promise<void> {
        if (this.maintenanceRetirement) return this.maintenanceRetirement;
        this.maintenanceClosed = true;
        this.maintenanceAbort.abort();
        this.maintenanceRetirement = (async () => {
            const explicit = this.explicitMaintenanceFlight;
            const automatic = this.automaticMaintenanceFlight;
            await Promise.allSettled([explicit, automatic].filter((value): value is Promise<void> => value !== null));
            // Join an admitted slice already appended to the writer, plus any
            // authoritative operation accepted before its owning facade closed.
            await this.enqueue(async () => {});
        })();
        return this.maintenanceRetirement;
    }

    /** Callbacks build provisional state: do not expose it until this returns. */
    load(onMetadata: (metadata: unknown) => void | Promise<void>, onSnapshot: (row: unknown) => void | Promise<void>,
        onMutation: (row: unknown) => void | Promise<void>,
        onValidated?: () => void | Promise<void>): Promise<boolean> {
        return this.enqueue(async () => {
            this.writable = false;
            this.absent = false;
            const migration = await this.readMigrationState();
            this.migration = migration.intent; this.migrationAdvanced = migration.advanced;
            const copies: Array<{ suffix: string; raw: string; head: StoreHead }> = [];
            let incompleteStage = false;
            for (const suffix of ["", ".next", ".bak"]) {
                const raw = await this.readOptional(this.headPath(suffix), STORE_LIMITS.headBytes);
                if (raw === null) continue;
                try { copies.push({ suffix, raw, head: validateHead(unseal(raw)) }); }
                catch (error) {
                    if (suffix === ".next" && error instanceof StoreRecoveryError && error.code === "CORRUPT") {
                        incompleteStage = true; continue;
                    }
                    throw error;
                }
            }
            if (!copies.length) {
                if (this.migration || incompleteStage || await this.io.exists(this.directory)) {
                    throw new StoreRecoveryError("RECOVERY_REQUIRED", "storage directory has no validated publication; preserve it for recovery");
                }
                this.absent = true; this.head = null;
                return false;
            }
            copies.sort((left, right) => right.head.generation - left.head.generation);
            const selected = copies[0];
            for (const copy of copies) {
                if (copy.head.epoch !== selected.head.epoch ||
                    (copy.head.generation === selected.head.generation && copy.raw !== selected.raw)) {
                    throw new StoreRecoveryError("CONTRADICTING_COPIES", "storage heads disagree on identity or generation");
                }
            }
            const head = selected.head;
            if (this.migration) {
                if (copies.some(copy => copy.head.epoch !== this.migration!.epoch)) {
                    throw new StoreRecoveryError("CONTRADICTING_COPIES", "migration and head epochs differ");
                }
                if (!this.migrationAdvanced && (head.generation !== 1 || head.sequence !== 0 || head.snapshot.id !== 1 ||
                    await metadataDigest(metadataJSON(head.snapshot.metadata)) !== this.migration.proof.metadataSha256)) {
                    throw new StoreRecoveryError("RECOVERY_REQUIRED", "advanced migration head lacks its durable fence");
                }
            }
            // The callback owns its copy, never the live publication cache.
            await onMetadata(JSON.parse(JSON.stringify(head.snapshot.metadata)));
            for (let part = 0; part < head.snapshot.pages; part++) {
                const page = await this.readPage(this.pagePath(head, part), head, "snapshot", head.snapshot.id);
                if (page.part !== part || page.from !== head.snapshot.cut || page.through !== head.snapshot.cut) fail("snapshot page cut differs");
                for (const row of page.rows) await onSnapshot(row);
                await this.pause();
            }
            let expected = head.snapshot.cut + 1;
            for (let id = head.walStart; id <= head.walEnd; id++) {
                const page = await this.readPage(this.walPath(head.epoch, id), head, "wal", id);
                if (page.from !== expected || page.through !== expected + page.rows.length - 1) fail("WAL sequence continuity differs");
                for (const row of page.rows) await onMutation(row);
                expected = page.through + 1;
                await this.pause();
            }
            if (expected !== head.sequence + 1) fail("head sequence outruns referenced mutations");
            // Multi-row logical records need a final closure check before a
            // selected stage/backup can be promoted or exposed as writable.
            await onValidated?.();
            // Validate the entire selected closure before allowing a writer.
            // Re-publish a selected stage/backup so the next rotation starts
            // from that exact head, not a guessed filename predecessor.
            if (await this.readOptional(this.headPath(), STORE_LIMITS.headBytes) !== selected.raw) {
                // A selected stage can be the only newest publication. Never
                // rewrite it in place: a rejected write could destroy its cut.
                if (selected.suffix !== ".next") {
                    await this.writeVerified(this.headPath(".next"), selected.raw, STORE_LIMITS.headBytes);
                }
                await this.promote(selected.raw);
            }
            this.head = validateHead(unseal(selected.raw)); this.absent = false; this.writable = true;
            return true;
        });
    }

    initialize(rows: Iterable<unknown>, metadata: unknown): Promise<void> {
        return this.enqueue(async () => {
            if (!this.absent || this.head) throw new StoreRecoveryError("RECOVERY_REQUIRED", "initialize requires confirmed absent storage");
            try {
                const migration = await this.readMigrationState();
                if (migration.intent || await this.io.exists(this.directory)) {
                    throw new StoreRecoveryError("RECOVERY_REQUIRED", "initialize cannot replace existing migration evidence");
                }
                await this.io.mkdir(this.directory);
                const bytes = new Uint8Array(16);
                globalThis.crypto.getRandomValues(bytes);
                const epoch = [...bytes].map(byte => byte.toString(16).padStart(2, "0")).join("");
                const head: StoreHead = { schema: 1, kind: "head", epoch, generation: 1, sequence: 0,
                    snapshot: { id: 1, pages: 0, cut: 0, metadata }, walStart: 1, walEnd: 0, nextSegment: 1 };
                await this.writeSnapshot(head, rows);
                await this.publish(head);
                this.absent = false; this.writable = true;
            } catch (error) { this.writable = false; this.absent = false; throw error; }
        });
    }

    /** Opt-in first import. Persist source identity before creating the target
     * directory, so an interrupted first snapshot has evidence for exact retry.
     * The caller must already have ensured the sibling marker's parent exists. */
    initializeMigration(rows: Iterable<unknown>, metadata: unknown, proof: MigrationProof): Promise<void> {
        return this.initializeWithProof(rows, metadata, proof, false);
    }

    /** Explicit recovery only; never reinterpret an arbitrary unheaded directory
     * as empty. A valid head/stage belongs to load(), not a fresh import. */
    retryInitialization(rows: Iterable<unknown>, metadata: unknown, proof: MigrationProof): Promise<void> {
        return this.initializeWithProof(rows, metadata, proof, true);
    }

    private initializeWithProof(rows: Iterable<unknown>, metadata: unknown, proof: MigrationProof, retry: boolean): Promise<void> {
        // Snapshot the caller's cut synchronously, before this queue can yield.
        let rawMetadata: string;
        let detachedProof: MigrationProof;
        try { rawMetadata = metadataJSON(metadata); detachedProof = checkedProof(proof); }
        catch (error) { return Promise.reject(error); }
        return this.enqueue(async () => {
            const confirmedAbsent = this.absent && !this.head;
            this.writable = false; this.absent = false;
            try {
                if (await metadataDigest(rawMetadata) !== detachedProof.metadataSha256) {
                    throw new StoreRecoveryError("CONTRADICTING_COPIES", "migration metadata cut differs from proof");
                }
                const state = await this.readMigrationState();
                let intent = state.intent;
                if (retry) {
                    if (!intent || state.advanced) {
                        throw new StoreRecoveryError("RECOVERY_REQUIRED", "initialization retry lacks unadvanced migration evidence");
                    }
                    if (JSON.stringify(intent.proof) !== JSON.stringify(detachedProof)) {
                        throw new StoreRecoveryError("CONTRADICTING_COPIES", "migration source identity differs");
                    }
                } else {
                    if (!confirmedAbsent || intent || await this.io.exists(this.directory)) {
                        throw new StoreRecoveryError("RECOVERY_REQUIRED", "migration requires confirmed absent target and intent");
                    }
                    const bytes = new Uint8Array(16);
                    globalThis.crypto.getRandomValues(bytes);
                    intent = { schema: 1, kind: "migration-init", target: this.directory,
                        epoch: [...bytes].map(byte => byte.toString(16).padStart(2, "0")).join(""), proof: detachedProof };
                    await this.writeVerified(segmentedMigrationPaths(this.directory).intent,
                        seal(intent, MIGRATION_RECORD_BYTES), MIGRATION_RECORD_BYTES);
                }
                await this.assertInitialHeadsOnly();
                this.migration = intent; this.migrationAdvanced = false;
                if (!(await this.io.exists(this.directory))) await this.io.mkdir(this.directory);
                const head: StoreHead = { schema: 1, kind: "head", epoch: intent!.epoch, generation: 1, sequence: 0,
                    snapshot: { id: 1, pages: 0, cut: 0, metadata: JSON.parse(rawMetadata) },
                    walStart: 1, walEnd: 0, nextSegment: 1 };
                await this.writeSnapshot(head, rows, retry);
                // A concurrent writer is unsupported, but do not knowingly
                // overwrite a head that appeared while initial page IO yielded.
                await this.assertInitialHeadsOnly();
                validateHead(head);
                const encoded = seal(head, STORE_LIMITS.headBytes);
                if (retry) await this.writeInitialVerified(this.headPath(".next"), encoded, STORE_LIMITS.headBytes);
                else await this.writeVerified(this.headPath(".next"), encoded, STORE_LIMITS.headBytes);
                await this.promote(encoded);
                this.head = validateHead(unseal(encoded)); this.writable = true;
            } catch (error) { this.writable = false; throw error; }
        });
    }

    commit(rows: Iterable<unknown>): Promise<void> {
        return this.enqueue(async () => {
            this.assertWritable();
            try {
                const current = this.head!;
                const next: StoreHead = { ...current, generation: current.generation + 1 };
                let count = 0;
                for (const batch of this.batches(rows)) {
                    if (next.nextSegment - next.walStart >= STORE_LIMITS.replaySegments) throw new StoreRecoveryError("LIMIT", "WAL requires snapshot compaction");
                    const page: Page = { schema: 1, kind: "wal", epoch: next.epoch, id: next.nextSegment,
                        from: next.sequence + 1, through: next.sequence + batch.length, rows: batch };
                    const encoded = seal(page, STORE_LIMITS.frameBytes);
                    await this.ensureAdvanceFence();
                    await this.writeVerified(this.walPath(next.epoch, page.id), encoded, STORE_LIMITS.frameBytes);
                    next.sequence = page.through; next.walEnd = page.id; next.nextSegment++;
                    count += batch.length;
                    await this.pause();
                }
                if (count) await this.publish(next);
            } catch (error) { this.writable = false; throw error; }
        });
    }

    snapshot(rows: Iterable<unknown>, metadata: unknown): Promise<void> {
        return this.enqueue(async () => {
            this.assertWritable();
            try {
                const current = this.head!;
                await this.ensureAdvanceFence();
                const next: StoreHead = { ...current, generation: current.generation + 1,
                    snapshot: { id: current.generation + 1, pages: 0, cut: current.sequence, metadata },
                    walStart: current.nextSegment, walEnd: current.nextSegment - 1 };
                await this.writeSnapshot(next, rows);
                await this.publish(next);
            } catch (error) { this.writable = false; throw error; }
        });
    }

    private *batches(rows: Iterable<unknown>): Generator<unknown[]> {
        let batch: unknown[] = [];
        let bytes = 512;
        for (const row of rows) {
            const encoded = JSON.stringify(row);
            if (encoded === undefined) fail("storage row cannot be encoded");
            const size = checksumJournalUtf8(encoded).bytes + 1;
            if (size + 512 > STORE_LIMITS.payloadBytes) throw new StoreRecoveryError("LIMIT", "storage row exceeds page capacity");
            if (batch.length && (bytes + size > STORE_LIMITS.payloadBytes || batch.length >= STORE_LIMITS.rowsPerPage)) {
                yield batch; batch = []; bytes = 512;
            }
            batch.push(row); bytes += size;
        }
        if (batch.length) yield batch;
    }

    private async writeSnapshot(head: StoreHead, rows: Iterable<unknown>, initialRetry = false): Promise<void> {
        for (const batch of this.batches(rows)) {
            const part = head.snapshot.pages;
            if (part >= STORE_LIMITS.snapshotPages) throw new StoreRecoveryError("LIMIT", "snapshot exceeds page-count limit");
            const page: Page = { schema: 1, kind: "snapshot", epoch: head.epoch, id: head.snapshot.id,
                part, from: head.snapshot.cut, through: head.snapshot.cut, rows: batch };
            const encoded = seal(page, STORE_LIMITS.frameBytes);
            if (initialRetry) await this.writeInitialVerified(this.pagePath(head, part), encoded, STORE_LIMITS.frameBytes);
            else await this.writeVerified(this.pagePath(head, part), encoded, STORE_LIMITS.frameBytes);
            head.snapshot.pages++;
            await this.pause();
        }
    }

    private async readPage(path: string, head: StoreHead, kind: Page["kind"], id: number): Promise<Page> {
        const raw = await this.readOptional(path, STORE_LIMITS.frameBytes);
        if (raw === null) fail("referenced storage page is missing");
        const page = unseal(raw);
        if (page.kind !== kind || page.epoch !== head.epoch || page.id !== id ||
            !integer(page.from) || !integer(page.through) || !Array.isArray(page.rows) ||
            page.rows.length === 0 || page.rows.length > STORE_LIMITS.rowsPerPage ||
            checksumJournalUtf8(JSON.stringify(page)).bytes > STORE_LIMITS.payloadBytes) fail("invalid storage page bounds or identity");
        return page;
    }

    private async readMigrationState(): Promise<{ intent: MigrationMarker | null; advanced: boolean }> {
        const paths = segmentedMigrationPaths(this.directory);
        const read = async (path: string, kind: MigrationMarker["kind"]): Promise<MigrationMarker | null> => {
            const raw = await this.readOptional(path, MIGRATION_RECORD_BYTES);
            if (raw === null) return null;
            const value = unseal(raw, MIGRATION_RECORD_BYTES);
            if (value.kind !== kind || value.target !== this.directory || typeof value.epoch !== "string" ||
                !/^[0-9a-f]{32}$/.test(value.epoch)) fail("invalid migration marker identity");
            return { schema: 1, kind, target: value.target, epoch: value.epoch, proof: checkedProof(value.proof) };
        };
        const intent = await read(paths.intent, "migration-init");
        const advanced = await read(paths.advanced, "migration-advanced");
        if (advanced && (!intent || advanced.epoch !== intent.epoch ||
            JSON.stringify(advanced.proof) !== JSON.stringify(intent.proof))) {
            throw new StoreRecoveryError("CONTRADICTING_COPIES", "migration fence has no matching intent");
        }
        return { intent, advanced: advanced !== null };
    }

    private async ensureAdvanceFence(): Promise<void> {
        if (!this.migration || this.migrationAdvanced) return;
        const state = await this.readMigrationState();
        if (!state.intent || JSON.stringify(state.intent) !== JSON.stringify(this.migration)) {
            throw new StoreRecoveryError("CONTRADICTING_COPIES", "migration intent changed before advancement");
        }
        if (!state.advanced) {
            const fence: MigrationMarker = { ...this.migration, kind: "migration-advanced" };
            await this.writeVerified(segmentedMigrationPaths(this.directory).advanced,
                seal(fence, MIGRATION_RECORD_BYTES), MIGRATION_RECORD_BYTES);
        }
        this.migrationAdvanced = true;
    }

    private async assertInitialHeadsOnly(): Promise<void> {
        for (const suffix of ["", ".bak", ".next"]) {
            const raw = await this.readOptional(this.headPath(suffix), STORE_LIMITS.headBytes);
            if (raw === null) continue;
            try { validateHead(unseal(raw)); }
            catch (error) {
                if (suffix === ".next" && error instanceof StoreRecoveryError && error.code === "CORRUPT") continue;
                throw error;
            }
            throw new StoreRecoveryError("RECOVERY_REQUIRED", "existing publication requires validated load, not initialization retry");
        }
    }

    /** Only an exact already-validated page or a strict prefix of the expected
     * initial bytes may be reused/repaired. Never overwrite a different valid
     * page or unknown-schema evidence under the same migration proof. */
    private async writeInitialVerified(path: string, contents: string, limit: number): Promise<void> {
        const previous = await this.readOptional(path, limit);
        if (previous === contents) { unseal(previous); return; }
        if (previous !== null) {
            try {
                unseal(previous);
                throw new StoreRecoveryError("CONTRADICTING_COPIES", "initial page differs from proven source");
            } catch (error) {
                if (!(error instanceof StoreRecoveryError) || error.code !== "CORRUPT") throw error;
                if (!contents.startsWith(previous)) throw error;
            }
        }
        await this.writeVerified(path, contents, limit);
    }

    private async publish(head: StoreHead): Promise<void> {
        validateHead(head);
        // Only this previously known, bounded closure becomes a GC candidate.
        // Never discover deletion targets with an unbounded directory listing.
        const oldBackupRaw = await this.readOptional(this.headPath(".bak"), STORE_LIMITS.headBytes);
        const oldBackup = oldBackupRaw === null ? null : validateHead(unseal(oldBackupRaw));
        if (oldBackup && oldBackup.epoch !== head.epoch) fail("backup epoch changed before publication");
        const predecessor = this.head;
        const encoded = seal(head, STORE_LIMITS.headBytes);
        await this.writeVerified(this.headPath(".next"), encoded, STORE_LIMITS.headBytes);
        await this.promote(encoded);
        // Keep only the exact serialized publication, not caller-owned metadata
        // that might mutate while a later transaction is awaiting native IO.
        this.head = validateHead(unseal(encoded));
        if (oldBackup && predecessor) {
            try {
                // Do not prune against only a cached predecessor if an external
                // writer changed the actual backup. This is still a single-
                // writer store, not a cross-process locking protocol.
                const actualRaw = await this.readOptional(this.headPath(".bak"), STORE_LIMITS.headBytes);
                const actual = actualRaw === null ? null : validateHead(unseal(actualRaw));
                if (!actual || JSON.stringify(actual) !== JSON.stringify(predecessor)) {
                    this.noteDeferredCleanup(); return;
                }
                this.queueReclaim(oldBackup, [actual, this.head]);
            } catch {
                // Verification here is maintenance-only: the new head itself
                // has already been verified. Preserve every candidate file.
                this.noteDeferredCleanup();
            }
        }
    }

    /** Capture only the exact bounded subset proven unreachable at publication.
     * Snapshot generations and WAL ids only advance within an epoch, so a path
     * excluded from both verified retained heads cannot become reachable in a
     * later local publication. Jobs retain no metadata or expanded path list.
     * A fixed queue cap keeps a broken adapter from turning maintenance evidence
     * into unbounded heap. This remains a single-writer, not a locking, proof. */
    private queueReclaim(candidate: StoreHead, retained: readonly StoreHead[]): void {
        this.activateCleanupJobs(false);
        const targets: CleanupTarget[] = [];
        if (candidate.snapshot.pages > 0 &&
            !retained.some(head => head.epoch === candidate.epoch && head.snapshot.id === candidate.snapshot.id)) {
            targets.push({ kind: "snapshot", epoch: candidate.epoch, id: candidate.snapshot.id,
                start: 0, end: candidate.snapshot.pages - 1, next: 0 });
        }
        if (candidate.walStart <= candidate.walEnd) {
            const protectedRanges = retained.filter(head => head.epoch === candidate.epoch && head.walStart <= head.walEnd)
                .map(head => ({ start: Math.max(candidate.walStart, head.walStart),
                    end: Math.min(candidate.walEnd, head.walEnd) }))
                .filter(range => range.start <= range.end)
                .sort((left, right) => left.start - right.start);
            let next = candidate.walStart;
            for (const range of protectedRanges) {
                if (range.end < next) continue;
                if (range.start > next) {
                    targets.push({ kind: "wal", epoch: candidate.epoch, id: 0,
                        start: next, end: range.start - 1, next });
                }
                next = Math.max(next, range.end + 1);
                if (next > candidate.walEnd) break;
            }
            if (next <= candidate.walEnd) {
                targets.push({ kind: "wal", epoch: candidate.epoch, id: 0,
                    start: next, end: candidate.walEnd, next });
            }
        }
        if (targets.length) {
            if (this.cleanupJobs.length >= this.maxPendingCleanupJobs) {
                // Publication is already durable. Losing retry evidence is a
                // conservative leak, never permission to fail or delete more.
                this.noteDeferredCleanup();
            } else {
                this.cleanupJobs.push({ targets, target: 0, failed: false, ready: true });
            }
        }
        this.requestAutomaticMaintenance();
    }

    private activateCleanupJobs(schedule = true): void {
        for (const job of this.cleanupJobs) job.ready = true;
        if (schedule) this.requestAutomaticMaintenance();
    }

    private nextReadyCleanupJob(): CleanupJob | undefined {
        return this.cleanupJobs.find(job => job.ready);
    }

    /** The lane wait never owns the serialized writer. Each admitted path is
     * appended independently, allowing foreground work to interleave between
     * any two cleanup paths even for an explicitly requested full pass. */
    private async runExplicitMaintenancePass(): Promise<void> {
        const jobs = this.cleanupJobs.slice();
        for (const job of jobs) job.ready = true;
        for (const job of jobs) {
            let passFinished = false;
            while (!passFinished && !this.maintenanceClosed && this.cleanupJobs.includes(job)) {
                try { await this.maintenancePause(this.maintenanceAbort.signal); }
                catch {
                    if (this.maintenanceClosed) break;
                    await this.enqueue(async () => {
                        if (!this.maintenanceClosed && this.cleanupJobs.includes(job)) this.suspendCleanupJob(job);
                    });
                    break;
                }
                if (this.maintenanceClosed) break;
                await this.enqueue(async () => {
                    if (this.maintenanceClosed || !this.cleanupJobs.includes(job) || !job.ready) {
                        passFinished = true;
                        return;
                    }
                    passFinished = await this.runCleanupSlice(job);
                });
            }
        }
    }

    /** Scheduler admission happens outside the serialized writer. Foreground
     * publications can therefore pass while a low-priority turn is waiting;
     * only the admitted single-path IO slice joins the writer chain. */
    private requestAutomaticMaintenance(): void {
        if (!this.automaticMaintenance || this.maintenanceClosed || this.maintenanceScheduled ||
            !this.nextReadyCleanupJob()) return;
        this.maintenanceScheduled = true;
        const flight = this.dispatchAutomaticCleanupSlice();
        this.automaticMaintenanceFlight = flight;
        void flight.then(() => {
            if (this.automaticMaintenanceFlight === flight) this.automaticMaintenanceFlight = null;
            this.maintenanceScheduled = false;
            this.requestAutomaticMaintenance();
        }, () => {
            // Internal maintenance must never produce an unhandled rejection
            // or poison authoritative publication. Preserve the queued owner.
            if (this.automaticMaintenanceFlight === flight) this.automaticMaintenanceFlight = null;
            this.maintenanceScheduled = false;
            if (!this.maintenanceClosed) {
                const job = this.nextReadyCleanupJob();
                if (job) this.suspendCleanupJob(job);
            }
            this.requestAutomaticMaintenance();
        });
    }

    private async dispatchAutomaticCleanupSlice(): Promise<void> {
        try {
            await this.maintenancePause(this.maintenanceAbort.signal);
        } catch {
            if (this.maintenanceClosed) return;
            await this.enqueue(async () => {
                if (this.maintenanceClosed) return;
                const job = this.nextReadyCleanupJob();
                if (job) this.suspendCleanupJob(job);
            });
            return;
        }
        if (this.maintenanceClosed) return;
        await this.enqueue(async () => {
            if (this.maintenanceClosed) return;
            const job = this.nextReadyCleanupJob();
            if (job) await this.runCleanupSlice(job);
        });
    }

    /** One slice performs at most one exists/remove pair for one exact path. */
    private async runCleanupSlice(job: CleanupJob): Promise<boolean> {
        const target = job.targets[job.target];
        if (!target) { this.finishCleanupPass(job); return true; }
        const value = target.next++;
        const path = target.kind === "snapshot"
            ? `${this.directory}/page-${target.epoch}-${target.id}-${value}.json`
            : this.walPath(target.epoch, value);
        try {
            if (await this.io.exists(path)) await this.io.remove(path);
        } catch {
            job.failed = true;
            this.noteDeferredCleanup();
        }
        if (target.next > target.end) job.target++;
        if (job.target >= job.targets.length) {
            this.finishCleanupPass(job);
            return true;
        }
        return false;
    }

    private finishCleanupPass(job: CleanupJob): void {
        const index = this.cleanupJobs.indexOf(job);
        if (index < 0) return;
        this.cleanupJobs.splice(index, 1);
        if (!job.failed) return;
        for (const target of job.targets) target.next = target.start;
        job.target = 0;
        job.failed = false;
        job.ready = false;
        this.cleanupJobs.push(job);
    }

    private suspendCleanupJob(job: CleanupJob): void {
        const index = this.cleanupJobs.indexOf(job);
        if (index < 0) return;
        this.cleanupJobs.splice(index, 1);
        job.ready = false;
        this.cleanupJobs.push(job);
        this.noteDeferredCleanup();
    }

    private noteDeferredCleanup(): void {
        this.cleanupDeferred = Math.min(Number.MAX_SAFE_INTEGER, this.cleanupDeferred + 1);
    }

    private async promote(encoded: string): Promise<void> {
        const stage = this.headPath(".next");
        if (await this.readOptional(stage, STORE_LIMITS.headBytes) !== encoded) fail("head stage changed before replace");
        if (await this.io.exists(this.headPath())) {
            if (await this.io.exists(this.headPath(".bak"))) await this.io.remove(this.headPath(".bak"));
            await this.io.rename(this.headPath(), this.headPath(".bak"));
        }
        await this.io.rename(stage, this.headPath());
        if (await this.readOptional(this.headPath(), STORE_LIMITS.headBytes) !== encoded) fail("published head readback differs");
    }

    private async writeVerified(path: string, contents: string, limit: number): Promise<void> {
        await this.io.write(path, contents);
        if (await this.readOptional(path, limit) !== contents) fail("storage page readback differs");
        unseal(contents);
    }

    private async readOptional(path: string, limit: number): Promise<string | null> {
        try {
            if (!(await this.io.exists(path))) return null;
            if (this.io.stat) {
                const stat = await this.io.stat(path);
                if (!stat || (stat.type !== undefined && stat.type !== "file") || !integer(stat.size)) fail("invalid storage file metadata");
                if (stat.size > limit) throw new StoreRecoveryError("LIMIT", "storage file exceeds bounded read");
            }
            const raw = await this.io.read(path);
            if (checksumJournalUtf8(raw).bytes > limit) throw new StoreRecoveryError("LIMIT", "storage read exceeded admitted frame size");
            return raw;
        } catch (error) {
            if (error instanceof StoreRecoveryError) throw error;
            if (isMissingPathError(error)) {
                try { if (!(await this.io.exists(path))) return null; } catch { /* unavailable */ }
            }
            throw new StoreRecoveryError("READ_FAILED", "storage unavailable; refusing empty recovery");
        }
    }

    private headPath(suffix = ""): string { return `${this.directory}/head.json${suffix}`; }
    private pagePath(head: StoreHead, part: number): string { return `${this.directory}/page-${head.epoch}-${head.snapshot.id}-${part}.json`; }
    private walPath(epoch: string, id: number): string { return `${this.directory}/wal-${epoch}-${id}.json`; }
    private assertWritable(): void {
        if (!this.writable || !this.head) throw new StoreRecoveryError("RECOVERY_REQUIRED", "storage requires validated reload before writes");
    }
    private enqueue<T>(work: () => Promise<T>): Promise<T> {
        const result = this.chain.then(work, work);
        this.chain = result.then(() => undefined, () => undefined);
        return result;
    }
}
