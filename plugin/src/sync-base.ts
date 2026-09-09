import type { App } from "obsidian";
import { PersistentPathIndex } from "./persistent-path-index";
import { SegmentedStore, STORE_LIMITS, StoreRecoveryError, createMigrationProof } from "./segmented-store";
import { fingerprintLegacyCopies } from "./legacy-source";
import { isSafeVaultPath } from "./delta-validation";
import { isMissingPathError } from "./file-safety";
import { checksumJournalUtf8 } from "./journal-format";
import { yieldWork } from "./work-scheduler";
import { assertLegacyDowngradeReadable } from "./legacy-downgrade";

interface BaseEntry {
    hash: string;
    /** Local on-disk mtime — compared during the metadata audit. */
    mtime: number;
    size: number;
    /** Server-tree mtime when it differs from the local filesystem mtime. */
    treeMtime?: number;
}

interface SyncBaseData {
    /** v2 is a mandatory old-reader fence once root publication authority exists. */
    schema?: 1 | 2;
    lastSyncTimestamp: number;
    lastAppliedPublication?: RootPublicationMarker | null;
    /** Last server root this exact local tree was verifiably based on. */
    treeBaseRoot?: string | null;
    /** A pull proved that remote state exists but could not establish an
     *  honest parent. Persisted so restart cannot mistake that state for a
     *  genuinely empty server and publish with an empty parent. */
    verifiedBaseRequired?: boolean;
    /** Last fully-applied binary diff page. Kept in the same snapshot/WAL as
     *  entry mutations, so a cursor can never outrun local state. */
    diffPageCheckpoint?: DiffPageCheckpoint | null;
    /** One explicit operator approval for a vault-sized local recovery.
     *  It survives renderer/plugin restarts until the approved publish
     *  commits, but is bounded so it cannot bless a later, larger deletion. */
    bulkChangeApproval?: BulkChangeApproval | null;
}

export interface RootPublicationIdentity {
    scopeHash: string;
    sequence: number;
    mutationId: string;
    requestHash: string;
}

export type RootBasePublicationEntry =
    | { action: "upsert"; path: string; hash: string; mtime: number; size: number; treeMtime?: number }
    | { action: "delete"; path: string };

export interface RootBasePublication {
    identity: RootPublicationIdentity;
    /** The submitted local candidate, never a possibly merged server root. */
    candidateRoot: string;
    /** Strict lexical path order, unique, bounded independently of vault size. */
    entries: readonly RootBasePublicationEntry[];
    committedAt: number;
}

export interface RootPublicationMarker {
    identity: RootPublicationIdentity;
    candidateRoot: string;
    committedAt: number;
    /** SHA-256 binds the complete canonical publication, not only its root. */
    payloadHash: string;
}

export interface CapturedBaseTreeEntry {
    path: string;
    hash: string;
    /** Committed tree metadata, not a later local stat-only refresh. */
    mtime_ms: number;
    size: number;
}

export interface CapturedBaseTreeEntries {
    readonly entryCount: number;
    /** Honest ancestry may differ from the semantic root of these entries
     * after a partially applied pull. Never infer a newer parent from them. */
    readonly treeBaseRoot: string | null;
    entries(): IterableIterator<CapturedBaseTreeEntry>;
    /** Valid only for this owner and this exact validated working cut. */
    isCurrent(): boolean;
}

export const ROOT_BASE_PUBLICATION_LIMITS = {
    entries: 256, encodedBytes: 256 * 1024, queuedRequests: 4, queuedEncodedBytes: 1024 * 1024,
} as const;

export interface BulkChangeApproval {
    schema: 1;
    vaultId: string;
    approvedAt: number;
    observedChanges: number;
    changeLimit: number;
    trackedDeletionLimit: number;
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
    | { op: "base-fence"; value: boolean }
    | { op: "diff-page"; value: DiffPageCheckpoint | null }
    | { op: "bulk-approval"; value: BulkChangeApproval | null }
    | { schema: 2; op: "root-publication"; value: RootPublicationMarker };

interface PendingBaseOperation {
    version: number;
    operation: SyncBaseOperation;
    estimatedBytes: number;
}

export const SYNC_BASE_PENDING_LIMITS = Object.freeze({
    operations: 1024,
    estimatedBytes: 8 * 1024 * 1024,
} as const);

export interface SyncBasePendingAdmissionSnapshot {
    operations: number;
    estimatedBytes: number;
    maxOperations: number;
    maxEstimatedBytes: number;
}

export interface ObsetyncSyncBaseOptions {
    /** Tests/embedding may lower, but never raise, the production ceilings. */
    pendingAdmission?: {
        maxOperations?: number;
        maxEstimatedBytes?: number;
    };
}

const SYNC_BASE_PATH = ".obsidian/plugins/obsetync/sync-base.json";
const SYNC_BASE_NEXT_PATH = `${SYNC_BASE_PATH}.next`;
const SYNC_BASE_BACKUP_PATH = `${SYNC_BASE_PATH}.bak`;
const SYNC_BASE_WAL_PATH = ".obsidian/plugins/obsetync/sync-base.wal.ndjson";
export const SYNC_BASE_STORE_PATH = ".obsidian/plugins/obsetync/sync-base.store-v1";
const LEGACY_READ_LIMIT = 8 * 1024 * 1024;

/**
 * Committed state with bounded immutable WAL segments and snapshot pages.
 * A head publishes a complete checkpoint cut: a diff cursor cannot outlive
 * omitted entry rows. Captured AVL roots let snapshot IO yield without copying
 * the complete index or mixing concurrent later mutations into an older cut.
 * The live index is still O(vault), not a fully paged index or complete
 * ResourceBudget accounting. Pending mutation owners have separate fail-fast
 * count/estimated-byte admission, including rows retained by live writes.
 * Publication
 * keeps a second persistent AVL root sharing unchanged branches; it does not
 * copy the full vault, or establish a total heap/backlog bound by itself.
 */
export class ObsetyncSyncBase {
    private data: SyncBaseData = { lastSyncTimestamp: 0 };
    private entries = new PersistentPathIndex<BaseEntry>();
    private dirty = false;
    private pendingOperations: PendingBaseOperation[] = [];
    /** Includes both queued rows and rows detached into a live store.commit().
     * A write that yields does not make its retained operation graph free. */
    private pendingOperationOwners = 0;
    private pendingOperationEstimatedBytes = 0;
    private readonly maxPendingOperations: number;
    private readonly maxPendingOperationEstimatedBytes: number;
    /** Ordinary setters expose a working view immediately; only this immutable
     * cut can be checkpointed while a later publication barrier is pending. */
    private durable: BaseState = { data: { lastSyncTimestamp: 0 }, entries: new PersistentPathIndex<BaseEntry>() };
    private durableVersion = 0;
    private publicationBarriers: number[] = [];
    private queuedPublications = 0;
    private queuedPublicationBytes = 0;
    private directoryEnsured = false;
    private writeChain: Promise<void> = Promise.resolve();
    private mutationVersion = 0;
    private writable = false;
    private readonly store: SegmentedStore;
    private closing = false;
    private retirement?: Promise<void>;

    constructor(private app: App, options: ObsetyncSyncBaseOptions = {}) {
        this.maxPendingOperations = boundedPendingLimit(
            options.pendingAdmission?.maxOperations,
            SYNC_BASE_PENDING_LIMITS.operations,
            "pending operation count",
        );
        this.maxPendingOperationEstimatedBytes = boundedPendingLimit(
            options.pendingAdmission?.maxEstimatedBytes,
            SYNC_BASE_PENDING_LIMITS.estimatedBytes,
            "pending operation bytes",
        );
        this.store = new SegmentedStore(app.vault.adapter, SYNC_BASE_STORE_PATH);
    }

    async load(): Promise<void> {
        await this.enqueue(async () => {
            this.writable = false;
            const legacyDowngradeActive = await assertLegacyDowngradeReadable(this.app.vault.adapter);
            const state: BaseState = { data: { lastSyncTimestamp: 0 }, entries: new PersistentPathIndex<BaseEntry>() };
            let previousPath: string | null = null;
            let loaded = false;
            let retryInitialization = false;
            try {
                loaded = await this.store.load(metadata => { state.data = validateMetadata(metadata); }, row => {
                    if (!Array.isArray(row) || row.length !== 2 || !isSafeVaultPath(row[0]) ||
                        (previousPath !== null && row[0] <= previousPath)) throw new StoreRecoveryError("CORRUPT", "invalid snapshot entry order");
                    previousPath = row[0];
                    state.entries = state.entries.set(row[0], validateEntry(row[1]));
                }, row => applyBaseOperation(state, validateOperation(row)));
            } catch (error) {
                if (!(error instanceof StoreRecoveryError) || error.code !== "RECOVERY_REQUIRED") throw error;
                retryInitialization = true;
            }
            if (!loaded) {
                state.data = { lastSyncTimestamp: 0 };
                state.entries = new PersistentPathIndex<BaseEntry>();
                const sourceSha256 = await this.importLegacy(state, legacyDowngradeActive);
                // Originals remain untouched. Only a fully validated import
                // can create the new authoritative head and versioned pages.
                // Retry additionally requires its exact persisted init proof
                // and no advanced fence; absence is never a guessed reset.
                await this.ensureDirectory();
                const metadata = { ...state.data };
                const proof = await createMigrationProof("sync-base-legacy-v1", sourceSha256, metadata);
                if (retryInitialization) await this.store.retryInitialization(state.entries.entries(), metadata, proof);
                else await this.store.initializeMigration(state.entries.entries(), metadata, proof);
            }
            this.durable = state; this.durableVersion = 0;
            this.data = { ...state.data }; this.entries = state.entries;
            this.pendingOperations = [];
            this.pendingOperationOwners = 0;
            this.pendingOperationEstimatedBytes = 0;
            this.dirty = false; this.mutationVersion = 0;
            this.writable = true;
        });
    }

    runMaintenance(): Promise<void> { return this.store.runMaintenance(); }
    drainMaintenance(): Promise<void> { return this.store.drainMaintenance(); }

    closeAndDrain(): Promise<void> {
        if (this.retirement) return this.retirement;
        this.closing = true;
        this.retirement = (async () => {
            await this.writeChain;
            await this.store.closeAndDrainMaintenance();
            this.writable = false;
        })();
        return this.retirement;
    }

    /** One adapter-verified head publishes candidate entries, base root and
     * applied-intent identity. Accepted-result recovery must finish this cut
     * before journal ACK. It does not prove server acceptance itself.
     *
     * The v2 WAL operation fences existing v1 readers until compaction; every
     * subsequent snapshot carries metadata schema:2, preserving that fence.
     * Old binaries must not be used to discard/flatten this authority. Scope
     * changes require a future explicit reconciliation/migration, not reset. */
    commitRootPublication(publication: RootBasePublication): Promise<"applied" | "already-applied"> {
        let captured: RootBasePublication;
        let encoded: string;
        let bytes = 0;
        let ownsSlot = false;
        try {
            this.assertWritable();
            if (this.queuedPublications >= ROOT_BASE_PUBLICATION_LIMITS.queuedRequests) {
                throw new StoreRecoveryError("LIMIT", "root publication queue is full");
            }
            // Reserve before inspecting/copying caller metadata. Four slots
            // plus the per-request ceiling bound this API's retained snapshots;
            // ordinary setter backlog has independent count/byte admission
            // and must not be charged again as publication metadata.
            this.queuedPublications++; ownsSlot = true;
            captured = detachRootBasePublication(publication);
            encoded = JSON.stringify(captured);
            bytes = checksumJournalUtf8(encoded).bytes;
            if (bytes > ROOT_BASE_PUBLICATION_LIMITS.queuedEncodedBytes - this.queuedPublicationBytes) {
                throw new StoreRecoveryError("LIMIT", "root publication queued metadata exceeds bound");
            }
            this.queuedPublicationBytes += bytes;
        } catch (error) {
            if (ownsSlot) this.queuedPublications--;
            return Promise.reject(error);
        }
        // Register in the submission turn. An older queued checkpoint must not
        // flush newer ordinary setters ahead of this publication.
        const version = ++this.mutationVersion;
        this.publicationBarriers.push(version);
        return this.enqueue(async () => {
            try {
                this.assertWritable();
                const payloadHash = await rootPublicationDigest(encoded);
                const marker = validatePublicationMarker({ identity: captured.identity,
                    candidateRoot: captured.candidateRoot, committedAt: captured.committedAt, payloadHash });
                const previous = this.durable.data.lastAppliedPublication;
                if (previous) {
                    if (previous.identity.scopeHash !== marker.identity.scopeHash) {
                        throw new StoreRecoveryError("CONTRADICTING_COPIES", "root publication scope requires explicit reconciliation");
                    }
                    if (previous.identity.sequence === marker.identity.sequence) {
                        if (JSON.stringify(previous) !== JSON.stringify(marker)) {
                            throw new StoreRecoveryError("CONTRADICTING_COPIES", "root publication identity or payload differs");
                        }
                        this.removePublicationBarrier(version);
                        await this.flushPendingInsideQueue();
                        return "already-applied";
                    }
                    if (previous.identity.sequence > marker.identity.sequence) {
                        throw new StoreRecoveryError("CONTRADICTING_COPIES", "root publication is older than the applied cut");
                    }
                }
                const before = await this.flushPendingInsideQueue();
                if (this.store.walSegments >= STORE_LIMITS.compactAfterSegments) await this.snapshotInsideQueue(before);
                const operations: SyncBaseOperation[] = captured.entries.map(row => row.action === "delete"
                    ? { op: "remove", path: row.path }
                    : { op: "set", path: row.path, entry: validateEntry({ hash: row.hash, mtime: row.mtime,
                        size: row.size, ...(row.treeMtime === undefined ? {} : { treeMtime: row.treeMtime }) }) });
                operations.push({ schema: 2, op: "root-publication", value: marker });
                const next: BaseState = { data: { ...this.durable.data }, entries: this.durable.entries };
                for (const operation of operations) applyBaseOperation(next, operation);
                try { await this.store.commit(operations); }
                catch (error) { this.writable = false; throw error; }
                // Never expose a partial/unverified publication. Later setters
                // remain an overlay and are flushed strictly after this cut.
                this.durable = next; this.durableVersion = version;
                this.removePublicationBarrier(version);
                const visible: BaseState = { data: { ...next.data }, entries: next.entries };
                for (const { operation } of this.pendingOperations) applyBaseOperation(visible, operation);
                this.data = visible.data; this.entries = visible.entries;
                this.dirty = true;
                await this.flushPendingInsideQueue();
                return "applied";
            } finally {
                this.removePublicationBarrier(version);
                this.queuedPublications--; this.queuedPublicationBytes -= bytes;
            }
        });
    }

    /** A failed/ambiguous native write requires validated reload before this
     * marker may authorize coordinator settlement, even if an older copy is
     * still available in memory. */
    get lastAppliedPublication(): RootPublicationMarker | null {
        this.assertWritable();
        const marker = this.durable.data.lastAppliedPublication;
        return marker ? { ...marker, identity: { ...marker.identity } } : null;
    }

    private removePublicationBarrier(version: number): void {
        const index = this.publicationBarriers.indexOf(version);
        if (index >= 0) this.publicationBarriers.splice(index, 1);
    }

    /** Durably append mutations since the previous batch without rewriting
     *  the full sync-base snapshot. */
    async checkpoint(): Promise<void> {
        await this.enqueue(async () => {
            this.assertWritable();
            if (this.pendingOperations.length === 0) return;
            try {
                const captured = await this.flushPendingInsideQueue();
                if (this.store.walSegments >= STORE_LIMITS.compactAfterSegments) await this.snapshotInsideQueue(captured);
            } catch (error) { this.writable = false; throw error; }
        });
    }

    /** Publish a captured stable root as bounded pages at a verified WAL cut.
     * The store preserves current/backup closures and reclaims older pages. */
    async save(): Promise<void> {
        await this.enqueue(async () => {
            this.assertWritable();
            if (!this.dirty && this.pendingOperations.length === 0) return;
            try {
                const captured = await this.flushPendingInsideQueue();
                await this.snapshotInsideQueue(captured);
            } catch (error) { this.writable = false; throw error; }
        });
    }

    private async snapshotInsideQueue(captured: BaseState & { version: number }): Promise<void> {
        try {
            await this.store.snapshot(captured.entries.entries(), captured.data);
            await this.flushPendingInsideQueue();
            // Submission-only publication versions can be rejected or exact
            // no-ops. They must not make every later save rewrite the vault.
            this.dirty = this.durableVersion !== captured.version || this.pendingOperations.length > 0;
        } catch (error) { this.writable = false; throw error; }
    }

    getHash(path: string): string | null {
        return this.entries.get(path)?.hash ?? null;
    }

    getEntry(path: string): BaseEntry | null {
        const entry = this.entries.get(path);
        return entry ? { ...entry } : null;
    }

    setEntry(path: string, hash: string, mtime: number, size: number, treeMtime?: number): void {
        const entry: BaseEntry = { hash, mtime, size };
        if (treeMtime !== undefined && treeMtime !== mtime) entry.treeMtime = treeMtime;
        this.record({ op: "set", path, entry });
        this.entries = this.entries.set(path, Object.freeze(entry));
    }

    /** Refresh local stat metadata after hashing proved that content is still
     *  the committed content. Keep the old tree mtime: changing filesystem
     *  metadata alone must not fabricate a new server-tree entry. */
    refreshLocalMetadata(path: string, mtime: number, size: number): boolean {
        const entry = this.entries.get(path);
        if (!entry || (entry.mtime === mtime && entry.size === size)) return false;
        const treeMtime = entry.treeMtime ?? entry.mtime;
        this.setEntry(path, entry.hash, mtime, size, treeMtime);
        return true;
    }

    removeEntry(path: string): void {
        this.record({ op: "remove", path });
        this.entries = this.entries.delete(path);
    }

    getTreeMtime(path: string): number | null {
        const entry = this.entries.get(path);
        return entry ? entry.treeMtime ?? entry.mtime : null;
    }

    get treeBaseRoot(): string | null {
        return this.data.treeBaseRoot ?? null;
    }

    setTreeBaseRoot(hash: string | null): void {
        if ((this.data.treeBaseRoot ?? null) === hash && this.publicationBarriers.length === 0) return;
        this.record({ op: "tree-root", value: hash });
        this.data.treeBaseRoot = hash;
    }

    get verifiedBaseRequired(): boolean {
        return this.data.verifiedBaseRequired === true;
    }

    setVerifiedBaseRequired(required: boolean): boolean {
        if (this.verifiedBaseRequired === required) return false;
        this.record({ op: "base-fence", value: required });
        this.data.verifiedBaseRequired = required;
        return true;
    }

    get diffPageCheckpoint(): DiffPageCheckpoint | null {
        const checkpoint = this.data.diffPageCheckpoint;
        return isDiffPageCheckpoint(checkpoint) ? { ...checkpoint } : null;
    }

    setDiffPageCheckpoint(checkpoint: DiffPageCheckpoint): void {
        if (!isDiffPageCheckpoint(checkpoint)) throw new Error("invalid diff page checkpoint");
        this.record({ op: "diff-page", value: { ...checkpoint } });
        this.data.diffPageCheckpoint = { ...checkpoint };
    }

    /** Returns whether a persisted marker was actually removed. */
    clearDiffPageCheckpoint(): boolean {
        if (!this.data.diffPageCheckpoint) return false;
        this.record({ op: "diff-page", value: null });
        this.data.diffPageCheckpoint = null;
        return true;
    }

    get bulkChangeApproval(): BulkChangeApproval | null {
        const approval = this.data.bulkChangeApproval;
        return isBulkChangeApproval(approval) ? { ...approval } : null;
    }

    /** Persist the scope of an explicit Full Rescan before expensive hashing
     *  starts. A small count allowance tolerates ordinary edits made while the
     *  recovery is running; the deletion ceiling stays exact. */
    approveBulkChange(
        vaultId: string,
        total: number,
        trackedDeletions: number,
        approvedAt: number = Date.now(),
    ): BulkChangeApproval {
        if (!vaultId || vaultId.length > 4096 || !isNonNegativeSafeInteger(total) ||
            !isNonNegativeSafeInteger(trackedDeletions) || trackedDeletions > total) {
            throw new Error("invalid bulk change approval");
        }
        const growthAllowance = Math.max(100, Math.ceil(total * 0.01));
        const approval: BulkChangeApproval = {
            schema: 1,
            vaultId,
            approvedAt,
            observedChanges: total,
            changeLimit: Math.min(Number.MAX_SAFE_INTEGER, total + growthAllowance),
            trackedDeletionLimit: trackedDeletions,
        };
        this.record({ op: "bulk-approval", value: { ...approval } });
        this.data.bulkChangeApproval = approval;
        return { ...approval };
    }

    bulkChangeApprovalCovers(
        vaultId: string,
        total: number,
        trackedDeletions: number,
    ): boolean {
        const approval = this.bulkChangeApproval;
        return !!approval &&
            approval.vaultId === vaultId &&
            isNonNegativeSafeInteger(total) &&
            isNonNegativeSafeInteger(trackedDeletions) &&
            trackedDeletions <= approval.trackedDeletionLimit &&
            total <= approval.changeLimit;
    }

    clearBulkChangeApproval(): boolean {
        if (this.data.bulkChangeApproval == null) return false;
        this.record({ op: "bulk-approval", value: null });
        this.data.bulkChangeApproval = null;
        return true;
    }

    get lastSyncTimestamp(): number {
        return this.data.lastSyncTimestamp;
    }

    setLastSyncTimestamp(ts: number): void {
        this.record({ op: "timestamp", value: ts });
        this.data.lastSyncTimestamp = ts;
    }

    allPaths(): string[] {
        return [...this.entries.keys()];
    }

    entryCount(): number {
        return this.entries.size;
    }

    pendingAdmissionSnapshot(): SyncBasePendingAdmissionSnapshot {
        return {
            operations: this.pendingOperationOwners,
            estimatedBytes: this.pendingOperationEstimatedBytes,
            maxOperations: this.maxPendingOperations,
            maxEstimatedBytes: this.maxPendingOperationEstimatedBytes,
        };
    }

    /** O(1) immutable-index capture, with detached rows on each traversal.
     * The caller may yield between rows without mixing newer setters into the
     * cut, but MUST check isCurrent immediately before publishing a derived
     * graph. A reload, poisoned write, or even queued root publication makes
     * the witness stale. This is not a durable checkpoint or a paged index:
     * holding the capture retains its reachable AVL metadata until released. */
    captureTreeEntries(): CapturedBaseTreeEntries {
        this.assertWritable();
        if (this.queuedPublications !== 0 || this.publicationBarriers.length !== 0) {
            throw new StoreRecoveryError("RECOVERY_REQUIRED", "sync-base root publication is still pending");
        }
        const entries = this.entries, data = this.data, version = this.mutationVersion;
        return Object.freeze({
            entryCount: entries.size,
            treeBaseRoot: data.treeBaseRoot ?? null,
            *entries(): IterableIterator<CapturedBaseTreeEntry> {
                for (const [path, entry] of entries.entries()) {
                    yield { path, hash: entry.hash, mtime_ms: entry.treeMtime ?? entry.mtime, size: entry.size };
                }
            },
            isCurrent: () => this.writable && this.entries === entries && this.data === data &&
                this.mutationVersion === version && this.queuedPublications === 0 && this.publicationBarriers.length === 0,
        });
    }

    private record(operation: SyncBaseOperation): void {
        this.assertWritable();
        if (this.pendingOperationOwners >= this.maxPendingOperations) {
            throw new StoreRecoveryError("LIMIT", "sync-base pending operation queue is full");
        }
        const checked = validateOperation(operation);
        const estimatedBytes = estimatePendingOperationBytes(checked);
        if (estimatedBytes > this.maxPendingOperationEstimatedBytes -
            this.pendingOperationEstimatedBytes) {
            throw new StoreRecoveryError("LIMIT", "sync-base pending operation metadata exceeds bound");
        }
        this.pendingOperationOwners++;
        this.pendingOperationEstimatedBytes += estimatedBytes;
        this.pendingOperations.push({
            version: ++this.mutationVersion,
            operation: checked,
            estimatedBytes,
        });
        this.dirty = true;
    }

    private async importLegacy(state: BaseState, releasedPriority = false): Promise<string> {
        let snapshotRaw: string | null = null;
        const sources: Array<{ role: string; raw: string | null }> = [];
        const snapshots = releasedPriority
            ? [["snapshot-next", SYNC_BASE_NEXT_PATH], ["snapshot", SYNC_BASE_PATH], ["snapshot-backup", SYNC_BASE_BACKUP_PATH]]
            : [["snapshot", SYNC_BASE_PATH], ["snapshot-next", SYNC_BASE_NEXT_PATH], ["snapshot-backup", SYNC_BASE_BACKUP_PATH]];
        for (const [role, path] of snapshots) {
            const raw = await this.readLegacy(path);
            sources.push({ role, raw });
            if (raw === null) continue;
            // Once the released reader selected the first shallow-valid copy,
            // lower-priority crash remnants are fingerprint evidence only.
            // They cannot replace or invalidate the selected authority.
            if (releasedPriority && snapshotRaw !== null) continue;
            let parsed: any;
            try { parsed = JSON.parse(raw); }
            catch {
                if (releasedPriority) continue;
                throw new StoreRecoveryError("CORRUPT", "legacy snapshot is not valid JSON");
            }
            if (releasedPriority && (!parsed || typeof parsed.lastSyncTimestamp !== "number" || !parsed.entries)) continue;
            const metadata = validateMetadata(parsed, true);
            if (!parsed.entries || typeof parsed.entries !== "object" || Array.isArray(parsed.entries)) {
                throw new StoreRecoveryError("CORRUPT", "legacy snapshot has no entry map");
            }
            // Legacy filenames have no epoch/cut: different copies cannot be
            // ordered safely merely because one happens to be named .next.
            if (!releasedPriority && snapshotRaw !== null && snapshotRaw !== raw) {
                throw new StoreRecoveryError("CONTRADICTING_COPIES", "legacy snapshots differ without a recovery generation");
            }
            if (snapshotRaw === null) {
                snapshotRaw = raw; state.data = metadata;
                let count = 0;
                for (const path in parsed.entries) {
                    if (!Object.prototype.hasOwnProperty.call(parsed.entries, path)) continue;
                    if (!isSafeVaultPath(path)) throw new StoreRecoveryError("CORRUPT", "unsafe legacy entry path");
                    state.entries = state.entries.set(path, validateEntry(parsed.entries[path]));
                    if (++count % STORE_LIMITS.rowsPerPage === 0) await yieldWork();
                }
            }
        }
        const raw = await this.readLegacy(SYNC_BASE_WAL_PATH);
        sources.push({ role: "wal", raw });
        if (raw === null) return fingerprintLegacyCopies(sources);
        let count = 0;
        for (let offset = 0; offset < raw.length;) {
            const newline = raw.indexOf("\n", offset);
            const terminated = newline !== -1;
            const end = terminated ? newline : raw.length;
            const line = raw.slice(offset, end);
            offset = terminated ? end + 1 : end;
            if (!line.trim()) continue;
            let row: unknown;
            try { row = JSON.parse(line); }
            catch {
                if (releasedPriority) continue;
                if (!terminated && snapshotRaw === null) break;
                // A torn pre-snapshot WAL prefix can roll a newer snapshot
                // backwards; legacy has no sequence cut to prove otherwise.
                throw new StoreRecoveryError("CORRUPT", "legacy WAL is damaged or has an ambiguous snapshot boundary");
            }
            try { applyBaseOperation(state, validateOperation(row)); }
            catch (error) {
                if (!releasedPriority) throw error;
            }
            if (++count % STORE_LIMITS.rowsPerPage === 0) await yieldWork();
        }
        return fingerprintLegacyCopies(sources);
    }

    private async readLegacy(path: string): Promise<string | null> {
        const adapter = this.app.vault.adapter;
        try {
            if (!(await adapter.exists(path))) return null;
            if (typeof adapter.stat === "function") {
                const stat = await adapter.stat(path);
                if (!stat || stat.type !== "file") throw new StoreRecoveryError("READ_FAILED", "legacy storage is not a regular file");
                if (stat.size > LEGACY_READ_LIMIT) throw new StoreRecoveryError("LIMIT", "legacy import needs a bounded external migration");
            }
            const raw = await adapter.read(path);
            if (checksumJournalUtf8(raw).bytes > LEGACY_READ_LIMIT) throw new StoreRecoveryError("LIMIT", "legacy import exceeds portable read limit");
            return raw;
        } catch (error) {
            if (error instanceof StoreRecoveryError) throw error;
            if (isMissingPathError(error)) {
                try { if (!(await adapter.exists(path))) return null; } catch { /* unavailable */ }
            }
            throw new StoreRecoveryError("READ_FAILED", "legacy sync-base unavailable; refusing empty recovery");
        }
    }

    private async flushPendingInsideQueue(): Promise<BaseState & { version: number }> {
        // A vault callback can mutate sync-base while adapter.append() yields.
        // Drain until stable so save()/checkpoint() never resolve with a
        // mutation that happened during their final write still memory-only.
        while (this.pendingOperations.length > 0) {
            const barrier = this.publicationBarriers[0] ?? Infinity;
            let count = 0;
            while (count < this.pendingOperations.length && this.pendingOperations[count].version < barrier) count++;
            if (count === 0) break;
            const operations = this.pendingOperations.splice(0, count);
            const ownedBytes = operations.reduce((sum, row) => sum + row.estimatedBytes, 0);
            const finalVersion = operations[operations.length - 1].version;
            const next: BaseState = { data: { ...this.durable.data }, entries: this.durable.entries };
            for (const { operation } of operations) applyBaseOperation(next, operation);
            try {
                function* rows(): IterableIterator<SyncBaseOperation> {
                    for (const { operation } of operations) yield operation;
                }
                await this.store.commit(rows());
                this.durable = next;
                this.durableVersion = finalVersion;
                // store.commit() has consumed the generator. Drop the detached
                // wrappers before returning their admission to concurrent setters.
                operations.length = 0;
                this.pendingOperationOwners -= count;
                this.pendingOperationEstimatedBytes -= ownedBytes;
            } catch (error) {
                this.pendingOperations = operations.concat(this.pendingOperations);
                this.writable = false;
                throw error;
            }
        }
        // Capture in the same synchronous turn as the final empty-queue check.
        // A caller's await continuation can otherwise see newer, uncommitted
        // mutations and assign them the preceding durable sequence cut.
        return { entries: this.durable.entries, data: { ...this.durable.data }, version: this.durableVersion };
    }

    private async ensureDirectory(): Promise<void> {
        if (this.directoryEnsured) return;
        const dir = SYNC_BASE_PATH.substring(0, SYNC_BASE_PATH.lastIndexOf("/"));
        if (!(await this.app.vault.adapter.exists(dir))) await this.app.vault.adapter.mkdir(dir);
        this.directoryEnsured = true;
    }

    private enqueue<T>(operation: () => Promise<T>): Promise<T> {
        if (this.closing) {
            return Promise.reject(new StoreRecoveryError("RECOVERY_REQUIRED", "sync-base is closing"));
        }
        const result = this.writeChain.then(operation, operation);
        this.writeChain = result.then(() => undefined, () => undefined);
        return result;
    }

    private assertWritable(): void {
        if (!this.writable) throw new StoreRecoveryError("RECOVERY_REQUIRED", "sync-base requires validated recovery before mutation");
    }
}

interface BaseState { data: SyncBaseData; entries: PersistentPathIndex<BaseEntry> }

function invalid(message: string): never { throw new StoreRecoveryError("CORRUPT", message); }

function boundedPendingLimit(value: number | undefined, maximum: number, name: string): number {
    const selected = value ?? maximum;
    if (!Number.isSafeInteger(selected) || selected < 1 || selected > maximum) {
        throw new RangeError(`${name} must be between 1 and ${maximum}`);
    }
    return selected;
}

/** Conservative retained-JS estimate for one detached pending operation.
 * JSON punctuation plus a fixed object/array allowance intentionally make
 * this larger than the operation's visible UTF-16 string payload. */
function estimatePendingOperationBytes(operation: SyncBaseOperation): number {
    const serializedLength = JSON.stringify(operation).length;
    const overhead = 256;
    if (serializedLength > Math.floor((Number.MAX_SAFE_INTEGER - overhead) / 2)) {
        throw new StoreRecoveryError("LIMIT", "sync-base pending operation estimate overflow");
    }
    return overhead + serializedLength * 2;
}

function validateEntry(value: unknown): BaseEntry {
    if (!value || typeof value !== "object" || Array.isArray(value)) invalid("invalid sync-base entry");
    const entry = value as BaseEntry;
    if (typeof entry.hash !== "string" || !ROOT_HASH.test(entry.hash) ||
        !Number.isFinite(entry.mtime) || !isNonNegativeSafeInteger(entry.size) ||
        (entry.treeMtime !== undefined && !Number.isFinite(entry.treeMtime))) invalid("invalid sync-base entry fields");
    return Object.freeze({ hash: entry.hash, mtime: entry.mtime, size: entry.size,
        ...(entry.treeMtime === undefined ? {} : { treeMtime: entry.treeMtime }) });
}

function validateMetadata(value: unknown, legacy = false): SyncBaseData {
    if (!value || typeof value !== "object" || Array.isArray(value)) invalid("invalid sync-base metadata");
    const row = value as SyncBaseData & { schema?: unknown };
    if (row.schema !== undefined && row.schema !== 1 && row.schema !== 2) {
        throw new StoreRecoveryError("UNKNOWN_SCHEMA", "unsupported sync-base metadata schema");
    }
    const keys = ["schema", "lastSyncTimestamp", "treeBaseRoot", "verifiedBaseRequired", "diffPageCheckpoint", "bulkChangeApproval",
        ...(legacy ? ["entries"] : []), ...(row.schema === 2 ? ["lastAppliedPublication"] : [])];
    if (Object.keys(value).some(key => !keys.includes(key))) {
        throw new StoreRecoveryError("UNKNOWN_SCHEMA", "unsupported sync-base metadata field");
    }
    if (legacy && row.schema === 2) throw new StoreRecoveryError("UNKNOWN_SCHEMA", "root publication authority cannot be imported from legacy metadata");
    if (!isNonNegativeSafeInteger(row.lastSyncTimestamp) ||
        (row.treeBaseRoot != null && (typeof row.treeBaseRoot !== "string" || !ROOT_HASH.test(row.treeBaseRoot))) ||
        (row.verifiedBaseRequired !== undefined && typeof row.verifiedBaseRequired !== "boolean") ||
        (row.diffPageCheckpoint != null && !isDiffPageCheckpoint(row.diffPageCheckpoint)) ||
        (row.bulkChangeApproval != null && !isBulkChangeApproval(row.bulkChangeApproval))) invalid("invalid sync-base metadata fields");
    return { ...(row.schema === undefined ? {} : { schema: row.schema }),
        ...(row.schema === 2 ? { lastAppliedPublication: validatePublicationMarker(row.lastAppliedPublication) } : {}),
        lastSyncTimestamp: row.lastSyncTimestamp, treeBaseRoot: row.treeBaseRoot ?? null,
        verifiedBaseRequired: row.verifiedBaseRequired === true,
        diffPageCheckpoint: row.diffPageCheckpoint ? { ...row.diffPageCheckpoint } : null,
        bulkChangeApproval: row.bulkChangeApproval ? { ...row.bulkChangeApproval } : null };
}

function validateOperation(value: unknown): SyncBaseOperation {
    if (!value || typeof value !== "object" || Array.isArray(value)) invalid("invalid sync-base mutation");
    const row = value as SyncBaseOperation & { schema?: unknown };
    if (row.op === "root-publication") {
        if (row.schema !== 2) throw new StoreRecoveryError("UNKNOWN_SCHEMA", "unsupported root publication operation schema");
        exactObject(value, ["schema", "op", "value"], "invalid root publication operation fields");
        return { schema: 2, op: row.op, value: validatePublicationMarker(row.value) };
    }
    if (row.schema !== undefined && row.schema !== 1) throw new StoreRecoveryError("UNKNOWN_SCHEMA", "unsupported sync-base mutation schema");
    switch (row.op) {
        case "set":
            if (!isSafeVaultPath(row.path)) invalid("unsafe sync-base path");
            return { op: row.op, path: row.path, entry: validateEntry(row.entry) };
        case "remove":
            if (!isSafeVaultPath(row.path)) invalid("unsafe sync-base path");
            return { op: row.op, path: row.path };
        case "timestamp":
            if (!isNonNegativeSafeInteger(row.value)) invalid("invalid sync-base timestamp");
            return { op: row.op, value: row.value };
        case "tree-root":
            if (row.value !== null && (typeof row.value !== "string" || !ROOT_HASH.test(row.value))) invalid("invalid sync-base root");
            return { op: row.op, value: row.value };
        case "base-fence":
            if (typeof row.value !== "boolean") invalid("invalid sync-base verification fence");
            return { op: row.op, value: row.value };
        case "diff-page":
            if (row.value !== null && !isDiffPageCheckpoint(row.value)) invalid("invalid sync-base cursor");
            return { op: row.op, value: row.value === null ? null : { ...row.value } };
        case "bulk-approval":
            if (row.value !== null && !isBulkChangeApproval(row.value)) invalid("invalid sync-base approval");
            return { op: row.op, value: row.value === null ? null : { ...row.value } };
        default: throw new StoreRecoveryError("UNKNOWN_SCHEMA", "unsupported sync-base operation");
    }
}

function applyBaseOperation(state: BaseState, operation: SyncBaseOperation): void {
    switch (operation.op) {
        case "set": state.entries = state.entries.set(operation.path, operation.entry); break;
        case "remove": state.entries = state.entries.delete(operation.path); break;
        case "timestamp": state.data.lastSyncTimestamp = operation.value; break;
        case "tree-root": state.data.treeBaseRoot = operation.value; break;
        case "base-fence": state.data.verifiedBaseRequired = operation.value; break;
        case "diff-page": state.data.diffPageCheckpoint = operation.value; break;
        case "bulk-approval": state.data.bulkChangeApproval = operation.value; break;
        case "root-publication":
            if (state.data.lastAppliedPublication &&
                (state.data.lastAppliedPublication.identity.scopeHash !== operation.value.identity.scopeHash ||
                    state.data.lastAppliedPublication.identity.sequence >= operation.value.identity.sequence)) {
                invalid("root publication WAL sequence or scope contradicts the applied cut");
            }
            state.data.schema = 2;
            state.data.lastAppliedPublication = operation.value;
            state.data.treeBaseRoot = operation.value.candidateRoot;
            state.data.verifiedBaseRequired = false;
            state.data.lastSyncTimestamp = operation.value.committedAt;
            break;
    }
}

function exactObject(value: unknown, fields: readonly string[], message: string): asserts value is Record<string, any> {
    if (!value || typeof value !== "object" || Array.isArray(value) ||
        Object.keys(value).some(key => !fields.includes(key))) invalid(message);
}

function detachPublicationIdentity(value: unknown): RootPublicationIdentity {
    exactObject(value, ["scopeHash", "sequence", "mutationId", "requestHash"], "invalid root publication identity fields");
    if (typeof value.scopeHash !== "string" || !ROOT_HASH.test(value.scopeHash) ||
        !Number.isSafeInteger(value.sequence) || value.sequence < 1 ||
        typeof value.mutationId !== "string" || !/^[0-9a-f]{32}$/.test(value.mutationId) ||
        typeof value.requestHash !== "string" || !ROOT_HASH.test(value.requestHash)) invalid("invalid root publication identity");
    return { scopeHash: value.scopeHash, sequence: value.sequence, mutationId: value.mutationId, requestHash: value.requestHash };
}

function validatePublicationMarker(value: unknown): RootPublicationMarker {
    exactObject(value, ["identity", "candidateRoot", "committedAt", "payloadHash"], "invalid root publication marker fields");
    const identity = Object.freeze(detachPublicationIdentity(value.identity));
    if (typeof value.candidateRoot !== "string" || !ROOT_HASH.test(value.candidateRoot) ||
        !isNonNegativeSafeInteger(value.committedAt) ||
        typeof value.payloadHash !== "string" || !ROOT_HASH.test(value.payloadHash)) invalid("invalid root publication marker");
    return Object.freeze({ identity, candidateRoot: value.candidateRoot,
        committedAt: value.committedAt, payloadHash: value.payloadHash });
}

/** Shared strict boundary for intent persistence and local base publication.
 * Count/byte ceilings precede admission; no caller-owned row/object is retained.
 * This is bounded metadata, not proof of content/source/server acceptance. */
export function detachRootBasePublication(value: unknown): RootBasePublication {
    exactObject(value, ["identity", "candidateRoot", "entries", "committedAt"], "invalid root base publication fields");
    const identity = detachPublicationIdentity(value.identity);
    if (typeof value.candidateRoot !== "string" || !ROOT_HASH.test(value.candidateRoot) ||
        !isNonNegativeSafeInteger(value.committedAt)) invalid("invalid root base publication metadata");
    if (!Array.isArray(value.entries) || value.entries.length > ROOT_BASE_PUBLICATION_LIMITS.entries) {
        throw new StoreRecoveryError("LIMIT", "root publication entry count exceeds bound");
    }
    const entries: RootBasePublicationEntry[] = [];
    let previous: string | null = null;
    let rowBytes = 0;
    for (const row of value.entries) {
        exactObject(row, row?.action === "delete" ? ["action", "path"] :
            ["action", "path", "hash", "mtime", "size", "treeMtime"], "invalid root publication entry fields");
        if (!isSafeVaultPath(row.path) || (previous !== null && row.path <= previous)) invalid("invalid root publication entry order or path");
        previous = row.path;
        let detached: RootBasePublicationEntry;
        if (row.action === "delete") detached = { action: "delete", path: row.path };
        else if (row.action === "upsert") {
            const entry = validateEntry({ hash: row.hash, mtime: row.mtime, size: row.size,
                ...(row.treeMtime === undefined ? {} : { treeMtime: row.treeMtime }) });
            detached = { action: "upsert", path: row.path, ...entry };
        } else invalid("unsupported root publication entry action");
        rowBytes += checksumJournalUtf8(JSON.stringify(detached)).bytes;
        if (rowBytes > ROOT_BASE_PUBLICATION_LIMITS.encodedBytes) throw new StoreRecoveryError("LIMIT", "root publication metadata exceeds bound");
        entries.push(detached);
    }
    const captured = { identity, candidateRoot: value.candidateRoot, entries, committedAt: value.committedAt };
    if (checksumJournalUtf8(JSON.stringify(captured)).bytes > ROOT_BASE_PUBLICATION_LIMITS.encodedBytes) {
        throw new StoreRecoveryError("LIMIT", "root publication metadata exceeds bound");
    }
    return captured;
}

async function rootPublicationDigest(encoded: string): Promise<string> {
    const bytes = new TextEncoder().encode(`obsetync:root-base-publication:v1\0${encoded}`);
    const digest = new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", bytes));
    return [...digest].map(byte => byte.toString(16).padStart(2, "0")).join("");
}

const ROOT_HASH = /^[0-9a-f]{64}$/;
const CURSOR_HEX = /^[0-9a-f]*$/;

function isNonNegativeSafeInteger(value: unknown): value is number {
    return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function isBulkChangeApproval(value: unknown): value is BulkChangeApproval {
    if (!value || typeof value !== "object") return false;
    const row = value as Record<string, unknown>;
    return row.schema === 1 &&
        typeof row.vaultId === "string" && row.vaultId.length > 0 && row.vaultId.length <= 4096 &&
        isNonNegativeSafeInteger(row.approvedAt) &&
        isNonNegativeSafeInteger(row.observedChanges) &&
        isNonNegativeSafeInteger(row.changeLimit) &&
        isNonNegativeSafeInteger(row.trackedDeletionLimit) &&
        row.changeLimit >= row.observedChanges &&
        row.trackedDeletionLimit <= row.observedChanges;
}

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
