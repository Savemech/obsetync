import { isSafeVaultPath } from "./delta-validation";
import { SegmentedStore, STORE_LIMITS, StoreRecoveryError, createMigrationProof,
    type SegmentedStoreIO } from "./segmented-store";
import { SyncMemoryUnavailableError, type SyncMemoryArbiter, type SyncMemoryLease } from "./sync-memory-arbiter";
import { preparedManifestCloneBytes } from "./prepared-desktop-manifest";

export const PREPARED_TRANSFER_PATH = ".obsidian/plugins/obsetync/prepared-transfer.store-v1";
export const MOBILE_PREPARED_TRANSFER_PATH = ".obsidian/plugins/obsetync/prepared-mobile-transfer.store-v1";
export const PREPARED_TRANSFER_LIMITS = {
    records: 128, chunks: 16_384, retainedBytes: 8 * 1024 * 1024,
    queuedBytes: 8 * 1024 * 1024, queuedRequests: 32,
} as const;
/** Bounded SegmentedStore encode/seal/verified-write workspace: row JSON and
 * page payload, sealed frame plus previous/read-back copies, head frames, and
 * fixed codec bookkeeping. Deterministic managed charge, not measured RSS. */
export const PREPARED_STORE_OPERATION_BYTES =
    2 * STORE_LIMITS.payloadBytes + 4 * STORE_LIMITS.frameBytes +
    2 * STORE_LIMITS.headBytes + 64 * 1024;
type Limits = { [K in keyof typeof PREPARED_TRANSFER_LIMITS]: number };
const HASH_FORMAT = "blake3-256-fastcdc-v1";
const MAX_PATH_UNITS = 4096;
const CHUNK_BYTES = 4 * 1024 * 1024;
type StoreKind = "desktop" | "mobile";
const storePath = (kind: StoreKind) => kind === "desktop" ? PREPARED_TRANSFER_PATH : MOBILE_PREPARED_TRANSFER_PATH;
const fingerprintKind = (kind: StoreKind) => kind === "desktop" ? "desktop-v1" : "mobile-resource-v1";
const metadataKind = (kind: StoreKind) => kind === "desktop" ? "prepared-transfer" : "prepared-mobile-transfer";

export interface PreparedDesktopSource {
    size: number;
    mtime: number;
    fingerprint: { kind: "desktop-v1"; size: number; mtime: number;
        ctime: number; device: number; inode: number };
}
export interface PreparedMobileSource {
    size: number;
    mtime: number;
    /** Digest of a host-qualified resource identity/version, never a path/URL. */
    fingerprint: { kind: "mobile-resource-v1"; size: number; mtime: number; resourceVersion: string };
}
export type PreparedSource = PreparedDesktopSource | PreparedMobileSource;
export interface PreparedManifest {
    scopeHash: string;
    path: string;
    mutationId: number;
    journalThroughId?: number;
    baseRoot?: string | null;
    source: PreparedSource;
    manifest: { file_hash: string; total_size: number;
        chunks: Array<{ hash: string; offset: number; size: number }> };
}
export type PreparedManifestInput = Omit<PreparedManifest, "mutationId"> & {
    /** undefined: no CAS; null: expect absence. This is not a journal ACK. */
    expectedMutationId?: number | null;
};
export type PreparedRetentionResult = { retained: true; mutationId: number } |
    { retained: false; reason: "limit" | "stale" };
export interface PreparedDiscard {
    scopeHash: string;
    path: string;
    expectedMutationId: number;
}

export class TransferPlanError extends Error {
    constructor(readonly code: "CORRUPT" | "UNKNOWN_SCHEMA" | "READ_FAILED" |
        "RECOVERY_REQUIRED" | "CONTRADICTING_COPIES" | "LIMIT" | "CLOSED" | "ADMISSION_REQUIRED", message: string) {
        super(message); this.name = "TransferPlanError";
    }
}
interface Retained { value: PreparedManifest; bytes: number }
interface State { entries: Map<string, Retained>; nextId: number; bytes: number; chunks: number }
type Header = Omit<PreparedManifest, "manifest"> & { fileHash: string; totalSize: number; chunkCount: number };
type Row = { schema: 1; op: "prepared"; header: Header } |
    { schema: 1; op: "chunk"; mutationId: number; index: number; hash: string; offset: number; size: number } |
    { schema: 1; op: "seal"; mutationId: number } |
    { schema: 1; op: "discard"; scopeHash: string; path: string; expectedMutationId: number };

const emptyState = (): State => ({ entries: new Map(), nextId: 1, bytes: 0, chunks: 0 });
const keyOf = (scope: string, path: string): string => `${scope}:${path}`;
function failure(code: TransferPlanError["code"], message: string): never { throw new TransferPlanError(code, message); }
function plain(value: unknown): value is Record<string, any> {
    return !!value && typeof value === "object" && !Array.isArray(value);
}
function exact(value: unknown, keys: readonly string[], message: string): asserts value is Record<string, any> {
    if (!plain(value) || Object.keys(value).some(key => !keys.includes(key))) failure("CORRUPT", message);
}
const integer = (value: unknown, min = 0): value is number =>
    Number.isSafeInteger(value) && (value as number) >= min && (value as number) < Number.MAX_SAFE_INTEGER;
const time = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value) && value >= 0;
// Node's numeric fs.Stats may expose Windows file IDs above MAX_SAFE_INTEGER.
// They are still useful as opaque equality witnesses together with size/time.
const nativeIdentity = (value: unknown): value is number => time(value) && Number.isInteger(value);
const hash = (value: unknown): value is string => typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
function identity(scope: unknown, path: unknown): void {
    if (!hash(scope) || typeof path !== "string" || path.length > MAX_PATH_UNITS || !isSafeVaultPath(path)) {
        failure("CORRUPT", "invalid prepared-record identity");
    }
}
function generation(value: unknown, optional = false): void {
    if (!(optional && value === undefined) && !integer(value, 1)) failure("CORRUPT", "invalid prepared mutation generation");
}
function sourceCopy(value: unknown, storeKind: StoreKind): PreparedSource {
    exact(value, ["size", "mtime", "fingerprint"], "invalid prepared source fields");
    const { size, mtime, fingerprint } = value;
    if (!plain(fingerprint) || fingerprint.kind !== fingerprintKind(storeKind)) {
        failure("UNKNOWN_SCHEMA", "unsupported prepared fingerprint kind");
    }
    if (fingerprint.kind === "desktop-v1") {
        exact(fingerprint, ["kind", "size", "mtime", "ctime", "device", "inode"], "invalid prepared fingerprint fields");
        const copy = { kind: "desktop-v1" as const, size: fingerprint.size, mtime: fingerprint.mtime,
            ctime: fingerprint.ctime, device: fingerprint.device, inode: fingerprint.inode };
        if (!integer(size) || !time(mtime) || copy.size !== size || copy.mtime !== mtime ||
            !time(copy.ctime) || !nativeIdentity(copy.device) || !nativeIdentity(copy.inode)) {
            failure("CORRUPT", "invalid prepared source fingerprint");
        }
        return { size, mtime, fingerprint: copy };
    }
    exact(fingerprint, ["kind", "size", "mtime", "resourceVersion"], "invalid prepared fingerprint fields");
    if (!integer(size) || !time(mtime) || fingerprint.size !== size || fingerprint.mtime !== mtime ||
        !hash(fingerprint.resourceVersion)) failure("CORRUPT", "invalid prepared source fingerprint");
    return { size, mtime, fingerprint: { kind: "mobile-resource-v1", size, mtime,
        resourceVersion: fingerprint.resourceVersion } };
}
function metadata(state: State, storeKind: StoreKind): Record<string, unknown> {
    return { schema: 1, kind: metadataKind(storeKind), hashFormat: HASH_FORMAT,
        nextMutationId: state.nextId, records: state.entries.size, chunks: state.chunks };
}
/** A conservative ledger of owned metadata, not JS heap bytes or native RSS. */
function estimate(path: string, chunks: number): number { return 1024 + path.length * 2 + chunks * 192; }
function immutable(value: PreparedManifest): PreparedManifest {
    Object.freeze(value.source.fingerprint); Object.freeze(value.source);
    for (const chunk of value.manifest.chunks) Object.freeze(chunk);
    Object.freeze(value.manifest.chunks); Object.freeze(value.manifest);
    return Object.freeze(value);
}
function detached(value: PreparedManifest): PreparedManifest {
    const source: PreparedSource = value.source.fingerprint.kind === "desktop-v1"
        ? { ...value.source, fingerprint: { ...value.source.fingerprint } }
        : { ...value.source, fingerprint: { ...value.source.fingerprint } };
    return { ...value, source,
        manifest: { ...value.manifest, chunks: value.manifest.chunks.map(chunk => ({ ...chunk })) } };
}
function header(value: PreparedManifest): Header {
    const { manifest, ...fields } = value;
    return { ...fields, fileHash: manifest.file_hash, totalSize: manifest.total_size, chunkCount: manifest.chunks.length };
}
function* recordRows(value: PreparedManifest): IterableIterator<Row> {
    yield { schema: 1, op: "prepared", header: header(value) };
    for (let index = 0; index < value.manifest.chunks.length; index++) {
        yield { schema: 1, op: "chunk", mutationId: value.mutationId, index, ...value.manifest.chunks[index] };
    }
    yield { schema: 1, op: "seal", mutationId: value.mutationId };
}
function* snapshotRows(state: State): IterableIterator<Row> {
    // The key array is capped independently of chunk counts (128 by default).
    for (const key of [...state.entries.keys()].sort()) yield* recordRows(state.entries.get(key)!.value);
}
function chunkCopy(value: unknown, expectedOffset: number, totalSize: number): { hash: string; offset: number; size: number } {
    exact(value, ["hash", "offset", "size"], "invalid prepared chunk fields");
    const copy = { hash: value.hash, offset: value.offset, size: value.size };
    if (!hash(copy.hash) || !integer(copy.offset) || !integer(copy.size, 1) || copy.size > CHUNK_BYTES ||
        copy.offset !== expectedOffset || copy.offset > totalSize - copy.size) failure("CORRUPT", "invalid prepared chunk range");
    return copy;
}
function baseFields(value: Record<string, any>, storeKind: StoreKind): Omit<PreparedManifest, "manifest"> {
    identity(value.scopeHash, value.path); generation(value.mutationId); generation(value.journalThroughId, true);
    if (value.baseRoot !== undefined && value.baseRoot !== null && !hash(value.baseRoot)) failure("CORRUPT", "invalid prepared base-root hint");
    return { scopeHash: value.scopeHash, path: value.path, mutationId: value.mutationId,
        ...(value.journalThroughId === undefined ? {} : { journalThroughId: value.journalThroughId }),
        ...(value.baseRoot === undefined ? {} : { baseRoot: value.baseRoot }), source: sourceCopy(value.source, storeKind) };
}
function install(state: State, value: PreparedManifest, limits: Limits): boolean {
    const key = keyOf(value.scopeHash, value.path);
    const previous = state.entries.get(key);
    const bytes = estimate(value.path, value.manifest.chunks.length);
    const nextBytes = state.bytes - (previous?.bytes ?? 0) + bytes;
    const nextChunks = state.chunks - (previous?.value.manifest.chunks.length ?? 0) + value.manifest.chunks.length;
    if (nextBytes > limits.retainedBytes || nextChunks > limits.chunks ||
        state.entries.size + (previous ? 0 : 1) > limits.records) return false;
    state.entries.set(key, { value, bytes }); state.bytes = nextBytes; state.chunks = nextChunks;
    return true;
}
function remove(state: State, key: string): void {
    const previous = state.entries.get(key)!;
    state.entries.delete(key); state.bytes -= previous.bytes; state.chunks -= previous.value.manifest.chunks.length;
}
function olderJournal(previous: PreparedManifest | undefined, value: PreparedManifest): boolean {
    return previous?.journalThroughId !== undefined &&
        (value.journalThroughId === undefined || value.journalThroughId < previous.journalThroughId);
}

/** PREPARED HINTS ONLY. No uploaded-object, root transaction, sync-base or
 * journal-ACK authority exists here. Every reuse MUST fully hash the current
 * source again, even in the same session and with an identical fingerprint.
 *
 * Owned retained/queued metadata has explicit portable ceilings. With an
 * injected shared arbiter, retained decode state is leased for the loaded
 * lifetime, mutation copies get synchronous per-operation leases, and legacy
 * unadmitted lookup is disabled. Store page buffers and host RSS remain outside
 * this model; it is not an OOM guarantee. No authoritative journal is evicted. */
export class PreparedTransferPlan {
    private readonly limits: Limits;
    private readonly store: SegmentedStore;
    private state = emptyState();
    private ready = false;
    private closed = false;
    private queuedBytes = 0;
    private queuedRequests = 0;
    private chain: Promise<void> = Promise.resolve();
    private memoryLease?: SyncMemoryLease;
    private readonly lifetime = new AbortController();
    private retirement?: Promise<void>;

    constructor(private readonly io: SegmentedStoreIO, limits: Partial<Limits> = {},
        private readonly memoryArbiter?: SyncMemoryArbiter, private readonly storeKind: StoreKind = "desktop") {
        this.limits = { ...PREPARED_TRANSFER_LIMITS, ...limits };
        for (const key of Object.keys(this.limits) as Array<keyof Limits>) {
            if (!(key in PREPARED_TRANSFER_LIMITS) || !integer(this.limits[key], 1) ||
                this.limits[key] > PREPARED_TRANSFER_LIMITS[key]) failure("LIMIT", "invalid prepared metadata ceiling");
        }
        this.store = new SegmentedStore(io, storePath(storeKind));
    }

    load(): Promise<void> {
        return this.enqueue(256, async () => {
            this.assertOpen(); this.ready = false;
            this.state = emptyState(); this.memoryLease?.release(); this.memoryLease = undefined;
            let memoryLease: SyncMemoryLease | undefined, operationLease: SyncMemoryLease | undefined;
            try {
                if (this.memoryArbiter) memoryLease = await this.memoryArbiter.reserve("prepared-manifest",
                    this.limits.retainedBytes, { signal: this.lifetime.signal });
                this.assertOpen();
                if (this.memoryArbiter) {
                    operationLease = this.memoryArbiter.tryReserve("prepared-manifest", PREPARED_STORE_OPERATION_BYTES) ?? undefined;
                    if (!operationLease) throw new SyncMemoryUnavailableError("prepared-manifest", PREPARED_STORE_OPERATION_BYTES);
                }
                const reader = this.reader();
                let present = false;
                let retry = false;
                try { present = await this.store.load(reader.metadata, reader.snapshot, reader.mutation, reader.finish); }
                catch (error) {
                    if (!(error instanceof StoreRecoveryError) || error.code !== "RECOVERY_REQUIRED") throw error;
                    retry = true;
                }
                this.assertOpen();
                if (!present) {
                    const initial = emptyState();
                    const cut = metadata(initial, this.storeKind);
                    const path = storePath(this.storeKind);
                    const encoded = new TextEncoder().encode(JSON.stringify({
                        domain: this.storeKind === "desktop" ? "obsetync-prepared-empty-source-v1" :
                            "obsetync-prepared-mobile-empty-source-v1", target: path,
                    }));
                    const digest = new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", encoded));
                    const sourceHash = [...digest].map(byte => byte.toString(16).padStart(2, "0")).join("");
                    const proof = await createMigrationProof(this.storeKind === "desktop" ? "prepared-transfer-empty-v1" :
                        "prepared-mobile-transfer-empty-v1", sourceHash, cut);
                    this.assertOpen();
                    const parent = path.slice(0, path.lastIndexOf("/"));
                    if (!(await this.io.exists(parent))) await this.io.mkdir(parent);
                    this.assertOpen();
                    if (retry) await this.store.retryInitialization([], cut, proof);
                    else await this.store.initializeMigration([], cut, proof);
                    this.state = initial;
                } else this.state = reader.state;
                this.memoryLease = memoryLease; memoryLease = undefined;
                this.ready = !this.closed;
            } catch (error) { memoryLease?.release(); this.ready = false; throw this.error(error); }
            finally { operationLease?.release(); }
        });
    }

    lookup(scopeHash: string, path: string): PreparedManifest | null {
        this.assertReady(); identity(scopeHash, path);
        if (this.memoryArbiter) failure("ADMISSION_REQUIRED", "arbitrated prepared lookup requires admitted ownership");
        const record = this.state.entries.get(keyOf(scopeHash, path));
        return record ? detached(record.value) : null;
    }

    /** Immutable store-owned view for a consumer which performs its own exact
     * admission before traversing/cloning chunk rows. This method allocates no
     * O(chunks) structure and the view must never escape that admitted clone. */
    lookupUnownedViewForAdmission(scopeHash: string, path: string): PreparedManifest | null {
        this.assertReady(); identity(scopeHash, path);
        return this.state.entries.get(keyOf(scopeHash, path))?.value ?? null;
    }

    /** Bounded detached lookup for restart/mobile reuse. The caller owns release. */
    async lookupAdmitted(scopeHash: string, path: string, signal?: AbortSignal): Promise<{
        value: PreparedManifest; memoryOwner?: SyncMemoryLease;
    } | null> {
        this.assertReady(); identity(scopeHash, path);
        const record = this.state.entries.get(keyOf(scopeHash, path));
        if (!record) return null;
        let memoryOwner: SyncMemoryLease | undefined;
        try {
            if (this.memoryArbiter) memoryOwner = await this.memoryArbiter.reserve("prepared-manifest",
                preparedManifestCloneBytes(record.value.manifest.chunks.length), { signal, wait: false });
            this.assertReady();
            if (record !== this.state.entries.get(keyOf(scopeHash, path))) {
                throw new SyncMemoryUnavailableError("prepared-manifest", memoryOwner?.bytes ?? 1);
            }
            return { value: detached(record.value), ...(memoryOwner ? { memoryOwner } : {}) };
        } catch (error) { memoryOwner?.release(); throw error; }
    }

    retain(input: PreparedManifestInput): Promise<PreparedRetentionResult> {
        try {
            this.assertReady();
            exact(input, ["scopeHash", "path", "journalThroughId", "baseRoot", "source", "manifest", "expectedMutationId"], "invalid prepared input fields");
            identity(input.scopeHash, input.path);
            exact(input.manifest, ["file_hash", "total_size", "chunks"], "invalid prepared manifest fields");
            if (!Array.isArray(input.manifest.chunks)) failure("CORRUPT", "invalid prepared chunk list");
            const count = input.manifest.chunks.length;
            const bytes = estimate(input.path, count);
            // No owned chunk-array copy before all admission bounds pass.
            if (count > this.limits.chunks || bytes > this.limits.retainedBytes || !this.hasQueueRoom(bytes)) {
                return Promise.resolve({ retained: false, reason: "limit" });
            }
            let operationLease: SyncMemoryLease | undefined;
            if (this.memoryArbiter) {
                operationLease = this.memoryArbiter.tryReserve("prepared-manifest", bytes + PREPARED_STORE_OPERATION_BYTES) ?? undefined;
                if (!operationLease) return Promise.resolve({ retained: false, reason: "limit" });
            }
            return this.retainAdmitted(input, bytes, operationLease);
        } catch (error) { return Promise.reject(error); }
    }

    private async retainAdmitted(input: PreparedManifestInput, bytes: number,
        operationLease?: SyncMemoryLease): Promise<PreparedRetentionResult> {
        try {
            this.assertReady();
            const expected = input.expectedMutationId;
            if (expected !== undefined && expected !== null) generation(expected);
            const base = baseFields({ ...input, mutationId: 1 }, this.storeKind);
            if (!hash(input.manifest.file_hash) || input.manifest.total_size !== base.source.size) failure("CORRUPT", "prepared manifest differs from source");
            const value: PreparedManifest = { ...base, manifest: {
                file_hash: input.manifest.file_hash, total_size: input.manifest.total_size, chunks: [],
            } };
            let offset = 0;
            for (const chunk of input.manifest.chunks) {
                const copy = chunkCopy(chunk, offset, value.manifest.total_size);
                value.manifest.chunks.push(copy); offset += copy.size;
            }
            if (offset !== value.manifest.total_size) failure("CORRUPT", "prepared manifest is incomplete");
            immutable(value);
            return await this.enqueue(bytes, async () => {
                this.assertReady();
                const key = keyOf(value.scopeHash, value.path);
                const previous = this.state.entries.get(key)?.value;
                if ((expected === null && previous) || (expected !== undefined && expected !== null && previous?.mutationId !== expected) ||
                    olderJournal(previous, value)) return { retained: false, reason: "stale" };
                generation(this.state.nextId);
                const accepted = immutable({ ...value, mutationId: this.state.nextId });
                const candidate: State = { ...this.state, entries: new Map(this.state.entries), nextId: this.state.nextId + 1 };
                if (!install(candidate, accepted, this.limits)) return { retained: false, reason: "limit" };
                try {
                    await this.compactIfNeeded(); await this.store.commit(recordRows(accepted)); this.state = candidate;
                    return { retained: true, mutationId: accepted.mutationId };
                } catch (error) { this.ready = false; throw this.error(error); }
            });
        } finally { operationLease?.release(); }
    }

    discard(scopeHash: string, path: string, expectedMutationId: number): Promise<boolean> {
        return this.discardMany([{ scopeHash, path, expectedMutationId }]).then(count => count !== 0);
    }

    /** Hint cleanup only; callers decide whether authoritative work was ACKed.
     * Matching generations are removed together under one verified head. */
    discardMany(items: readonly PreparedDiscard[]): Promise<number> {
        let operationLease: SyncMemoryLease | undefined;
        try {
            this.assertReady();
            if (!Array.isArray(items)) failure("CORRUPT", "invalid prepared discard list");
            if (items.length > this.limits.records) failure("LIMIT", "prepared discard batch exceeds record ceiling");
            let bytes = 256;
            for (const item of items) {
                exact(item, ["scopeHash", "path", "expectedMutationId"], "invalid prepared discard fields");
                identity(item.scopeHash, item.path); generation(item.expectedMutationId);
                bytes += 256 + item.path.length * 2;
            }
            if (!this.hasQueueRoom(bytes)) failure("LIMIT", "prepared-stage queue is full");
            if (this.memoryArbiter) {
                operationLease = this.memoryArbiter.tryReserve("prepared-manifest", bytes + PREPARED_STORE_OPERATION_BYTES) ?? undefined;
                if (!operationLease) failure("LIMIT", "prepared discard memory admission unavailable");
            }
            // Strings are immutable; detach every caller-owned record before
            // the first await.
            const owned = items.map(item => ({
                scopeHash: item.scopeHash, path: item.path, expectedMutationId: item.expectedMutationId,
            }));
            const result = this.enqueue(bytes, async () => {
                this.assertReady();
                const candidate: State = { ...this.state, entries: new Map(this.state.entries) };
                const rows: Row[] = [];
                for (const item of owned) {
                    const key = keyOf(item.scopeHash, item.path);
                    if (candidate.entries.get(key)?.value.mutationId !== item.expectedMutationId) continue;
                    remove(candidate, key);
                    rows.push({ schema: 1, op: "discard", ...item });
                }
                if (!rows.length) return 0;
                try {
                    await this.compactIfNeeded();
                    await this.store.commit(rows);
                    this.state = candidate;
                    return rows.length;
                } catch (error) { this.ready = false; throw this.error(error); }
            });
            return result.finally(() => operationLease?.release());
        } catch (error) { operationLease?.release(); return Promise.reject(error); }
    }

    compact(): Promise<void> {
        let operationLease: SyncMemoryLease | undefined;
        if (this.memoryArbiter) {
            operationLease = this.memoryArbiter.tryReserve("prepared-manifest", PREPARED_STORE_OPERATION_BYTES) ?? undefined;
            if (!operationLease) return Promise.reject(new TransferPlanError("LIMIT", "prepared compact memory admission unavailable"));
        }
        const result = this.enqueue(256, async () => {
            this.assertReady();
            try { await this.store.snapshot(snapshotRows(this.state), metadata(this.state, this.storeKind)); }
            catch (error) { this.ready = false; throw this.error(error); }
        });
        return result.finally(() => operationLease?.release());
    }

    close(): void {
        if (this.closed) return;
        this.closed = true; this.ready = false; this.lifetime.abort();
        // Already-dispatched native publication may finish. Release owned
        // metadata only after it and queued rejected callers have settled.
        this.retirement = this.chain.then(async () => {
            await this.store.closeAndDrainMaintenance();
            this.state = emptyState(); this.memoryLease?.release(); this.memoryLease = undefined;
        });
    }

    closeAndDrain(): Promise<void> {
        this.close();
        return this.retirement ?? Promise.resolve();
    }

    snapshot() {
        return { ready: this.ready && !this.closed, closed: this.closed,
            records: this.state.entries.size, chunks: this.state.chunks, estimatedRetainedBytes: this.state.bytes,
            managedResidentBytes: this.memoryLease?.bytes ?? 0,
            queuedRequests: this.queuedRequests, estimatedQueuedBytes: this.queuedBytes, limits: { ...this.limits } };
    }

    private hasQueueRoom(bytes: number): boolean {
        return this.queuedRequests < this.limits.queuedRequests && bytes <= this.limits.queuedBytes - this.queuedBytes;
    }
    private enqueue<T>(bytes: number, operation: () => Promise<T>): Promise<T> {
        if (this.closed) return Promise.reject(new TransferPlanError("CLOSED", "prepared stage is closed"));
        if (!this.hasQueueRoom(bytes)) return Promise.reject(new TransferPlanError("LIMIT", "prepared-stage queue is full"));
        this.queuedRequests++; this.queuedBytes += bytes;
        const result = this.chain.then(operation).finally(() => { this.queuedRequests--; this.queuedBytes -= bytes; });
        this.chain = result.then(() => undefined, () => undefined);
        return result;
    }
    private assertOpen(): void { if (this.closed) failure("CLOSED", "prepared stage is closed"); }
    private assertReady(): void {
        this.assertOpen();
        if (!this.ready) failure("RECOVERY_REQUIRED", "prepared stage requires validated load");
    }
    private error(error: unknown): unknown {
        return error instanceof StoreRecoveryError ? new TransferPlanError(error.code, error.message) : error;
    }
    private async compactIfNeeded(): Promise<void> {
        if (this.store.walSegments >= STORE_LIMITS.compactAfterSegments) {
            await this.store.snapshot(snapshotRows(this.state), metadata(this.state, this.storeKind));
        }
    }

    private reader() {
        const state = emptyState();
        let snapshotRecords = 0, snapshotChunks = 0, expectedRecords = 0, expectedChunks = 0;
        let inWal = false, previousKey: string | null = null;
        const snapshotIds = new Set<number>();
        let pending: PreparedManifest | null = null;
        let expectedCount = 0, offset = 0;
        const finishSnapshot = () => {
            if (pending || snapshotRecords !== expectedRecords || snapshotChunks !== expectedChunks) failure("CORRUPT", "prepared snapshot closure is incomplete");
        };
        const consume = (raw: unknown, snapshot: boolean) => {
            if (!plain(raw) || raw.schema !== 1) failure("UNKNOWN_SCHEMA", "unsupported prepared row schema");
            if (raw.op === "prepared") {
                exact(raw, ["schema", "op", "header"], "invalid prepared header row");
                if (pending) failure("CORRUPT", "prepared manifest header interrupts another record");
                exact(raw.header, ["scopeHash", "path", "mutationId", "journalThroughId", "baseRoot", "source", "fileHash", "totalSize", "chunkCount"], "invalid prepared header");
                const h = raw.header;
                if (!integer(h.chunkCount) || h.chunkCount > this.limits.chunks || estimate(h.path ?? "", h.chunkCount) > this.limits.queuedBytes) failure("LIMIT", "prepared record exceeds metadata retention ceiling");
                const fields = baseFields(h, this.storeKind);
                if (!hash(h.fileHash) || h.totalSize !== fields.source.size) failure("CORRUPT", "prepared header differs from source");
                const key = keyOf(fields.scopeHash, fields.path);
                if (snapshot) {
                    if (fields.mutationId >= state.nextId || snapshotIds.has(fields.mutationId) ||
                        (previousKey !== null && key <= previousKey)) failure("CORRUPT", "prepared snapshot order or generation differs");
                    snapshotIds.add(fields.mutationId); previousKey = key;
                } else if (fields.mutationId !== state.nextId) failure("CORRUPT", "prepared mutation sequence differs");
                pending = { ...fields, manifest: { file_hash: h.fileHash, total_size: h.totalSize, chunks: [] } };
                expectedCount = h.chunkCount; offset = 0;
            } else if (raw.op === "chunk") {
                exact(raw, ["schema", "op", "mutationId", "index", "hash", "offset", "size"], "invalid prepared chunk row");
                if (!pending || raw.mutationId !== pending.mutationId || raw.index !== pending.manifest.chunks.length ||
                    raw.index >= expectedCount) failure("CORRUPT", "prepared chunk order or owner differs");
                const chunk = chunkCopy({ hash: raw.hash, offset: raw.offset, size: raw.size }, offset, pending.manifest.total_size);
                pending.manifest.chunks.push(chunk); offset += chunk.size;
            } else if (raw.op === "seal") {
                exact(raw, ["schema", "op", "mutationId"], "invalid prepared seal row");
                if (!pending || raw.mutationId !== pending.mutationId || pending.manifest.chunks.length !== expectedCount ||
                    offset !== pending.manifest.total_size) failure("CORRUPT", "prepared manifest seal differs");
                if (!snapshot && olderJournal(state.entries.get(keyOf(pending.scopeHash, pending.path))?.value, pending)) {
                    failure("CORRUPT", "prepared replacement moves its journal hint backwards");
                }
                if (!install(state, immutable(pending), this.limits)) failure("LIMIT", "prepared retained metadata exceeds ceiling");
                if (snapshot) { snapshotRecords++; snapshotChunks += expectedCount; }
                else state.nextId++;
                pending = null;
            } else if (raw.op === "discard") {
                exact(raw, ["schema", "op", "scopeHash", "path", "expectedMutationId"], "invalid prepared discard row");
                if (snapshot || pending) failure("CORRUPT", "prepared discard interrupts a manifest");
                identity(raw.scopeHash, raw.path); generation(raw.expectedMutationId);
                const key = keyOf(raw.scopeHash, raw.path);
                if (state.entries.get(key)?.value.mutationId !== raw.expectedMutationId) failure("CORRUPT", "prepared discard owner differs");
                remove(state, key);
            } else failure("UNKNOWN_SCHEMA", "unsupported prepared operation");
        };
        return {
            state,
            metadata: (raw: unknown) => {
                exact(raw, ["schema", "kind", "hashFormat", "nextMutationId", "records", "chunks"], "invalid prepared metadata");
                if (raw.schema !== 1 || raw.kind !== metadataKind(this.storeKind) || raw.hashFormat !== HASH_FORMAT) failure("UNKNOWN_SCHEMA", "unsupported prepared metadata schema");
                // The final valid issued ID can leave an exhausted high-water
                // mark. Recovery/lookup/discard remain valid; retain refuses it.
                if (!Number.isSafeInteger(raw.nextMutationId) || raw.nextMutationId < 1) failure("CORRUPT", "invalid prepared mutation watermark");
                if (!integer(raw.records) || !integer(raw.chunks)) failure("CORRUPT", "invalid prepared snapshot counts");
                if (raw.records > this.limits.records || raw.chunks > this.limits.chunks) failure("LIMIT", "prepared snapshot exceeds metadata ceiling");
                state.nextId = raw.nextMutationId; expectedRecords = raw.records; expectedChunks = raw.chunks;
            },
            snapshot: (row: unknown) => consume(row, true),
            mutation: (row: unknown) => {
                if (!inWal) { finishSnapshot(); inWal = true; }
                consume(row, false);
            },
            finish: () => { if (!inWal) finishSnapshot(); if (pending) failure("CORRUPT", "prepared manifest publication is incomplete"); },
        };
    }
}

/** Separate forward-additive store. Released desktop readers never open this
 * path and therefore remain able to downgrade after mobile preparation. */
export class MobilePreparedTransferPlan extends PreparedTransferPlan {
    constructor(io: SegmentedStoreIO, limits: Partial<Limits> = {}, memoryArbiter?: SyncMemoryArbiter) {
        super(io, limits, memoryArbiter, "mobile");
    }
}
