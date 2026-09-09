import { SegmentedStore, STORE_LIMITS, StoreRecoveryError, createMigrationProof,
    type SegmentedStoreIO } from "./segmented-store";

export const OBJECT_CONFIRMATION_PATH = ".obsidian/plugins/obsetync/object-confirmation.store-v1";
export const OBJECT_CONFIRMATION_LIMITS = {
    records: 65_536,
    retainedBytes: 8 * 1024 * 1024,
    batchRecords: 512,
    queuedBytes: 512 * 1024,
    queuedRequests: 32,
} as const;

export type ConfirmedObjectKind = "content" | "content-chunk" | "index-chunk";
export interface ConfirmedObject { kind: ConfirmedObjectKind; hash: string }
type Limits = { [K in keyof typeof OBJECT_CONFIRMATION_LIMITS]: number };
type Row = { schema: 1; op: "confirm"; kind: ConfirmedObjectKind; hash: string };
interface State {
    scopeHash: string | null;
    serverGeneration: string | null;
    values: Set<string>;
    retainedBytes: number;
}

export class ObjectConfirmationStoreError extends Error {
    constructor(readonly code: "CORRUPT" | "UNKNOWN_SCHEMA" | "READ_FAILED" |
        "RECOVERY_REQUIRED" | "CONTRADICTING_COPIES" | "LIMIT" | "CLOSED", message: string) {
        super(message); this.name = "ObjectConfirmationStoreError";
    }
}

const emptyState = (): State => ({ scopeHash: null, serverGeneration: null,
    values: new Set(), retainedBytes: 0 });
const plain = (value: unknown): value is Record<string, any> =>
    !!value && typeof value === "object" && !Array.isArray(value);
const hex = (value: unknown): value is string =>
    typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
const integer = (value: unknown, min = 0): value is number =>
    Number.isSafeInteger(value) && (value as number) >= min && (value as number) < Number.MAX_SAFE_INTEGER;
const kind = (value: unknown): value is ConfirmedObjectKind =>
    value === "content" || value === "content-chunk" || value === "index-chunk";
const keyOf = (value: ConfirmedObject): string => `${value.kind}:${value.hash}`;
const estimate = (value: ConfirmedObject): number => 96 + value.hash.length * 2;
function fail(code: ObjectConfirmationStoreError["code"], message: string): never {
    throw new ObjectConfirmationStoreError(code, message);
}
function exact(value: unknown, keys: readonly string[], message: string): asserts value is Record<string, any> {
    if (!plain(value) || Object.keys(value).some(key => !keys.includes(key))) fail("CORRUPT", message);
}
function identity(scopeHash: unknown, serverGeneration: unknown): void {
    if (!hex(scopeHash) || !hex(serverGeneration)) fail("CORRUPT", "invalid object-confirmation generation scope");
}
function object(value: unknown): ConfirmedObject {
    exact(value, ["kind", "hash"], "invalid confirmed object fields");
    if (!kind(value.kind) || !hex(value.hash)) fail("CORRUPT", "invalid confirmed object identity");
    return { kind: value.kind, hash: value.hash };
}
function metadata(state: State): Record<string, unknown> {
    return { schema: 1, kind: "object-confirmation", hashFormat: "blake3-256-v1",
        scopeHash: state.scopeHash, serverGeneration: state.serverGeneration,
        records: state.values.size, retainedBytes: state.retainedBytes };
}
function* rows(state: State): IterableIterator<Row> {
    for (const value of [...state.values].sort()) {
        const separator = value.indexOf(":");
        yield { schema: 1, op: "confirm", kind: value.slice(0, separator) as ConfirmedObjectKind,
            hash: value.slice(separator + 1) };
    }
}

/** Durable positive-presence hints. A hint is authority only inside the exact
 * authenticated preparation scope and observed server storage generation.
 * Missing/unknown is never persisted, and a generation change atomically
 * replaces the old set. The server still validates the candidate graph at
 * publication; this store is not a root, journal, or mutation acknowledgement. */
export class ObjectConfirmationStore {
    private readonly limits: Limits;
    private readonly store: SegmentedStore;
    private state = emptyState();
    private ready = false;
    private closed = false;
    private queuedBytes = 0;
    private queuedRequests = 0;
    private chain: Promise<void> = Promise.resolve();
    private retirement?: Promise<void>;

    constructor(private readonly io: SegmentedStoreIO, limits: Partial<Limits> = {}) {
        this.limits = { ...OBJECT_CONFIRMATION_LIMITS, ...limits };
        for (const name of Object.keys(this.limits) as Array<keyof Limits>) {
            if (!(name in OBJECT_CONFIRMATION_LIMITS) || !integer(this.limits[name], 1) ||
                this.limits[name] > OBJECT_CONFIRMATION_LIMITS[name]) fail("LIMIT", "invalid object-confirmation ceiling");
        }
        this.store = new SegmentedStore(io, OBJECT_CONFIRMATION_PATH);
    }

    load(): Promise<void> {
        return this.enqueue(256, async () => {
            this.assertOpen(); this.ready = false;
            try {
                const reader = this.reader();
                let present = false, retry = false;
                try { present = await this.store.load(reader.metadata, reader.snapshot, reader.mutation, reader.finish); }
                catch (error) {
                    if (!(error instanceof StoreRecoveryError) || error.code !== "RECOVERY_REQUIRED") throw error;
                    retry = true;
                }
                this.assertOpen();
                if (!present) {
                    const state = emptyState(), cut = metadata(state);
                    const source = new TextEncoder().encode(JSON.stringify({
                        domain: "obsetync-object-confirmation-empty-v1", target: OBJECT_CONFIRMATION_PATH,
                    }));
                    const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", source));
                    const sourceHash = [...digest].map(byte => byte.toString(16).padStart(2, "0")).join("");
                    const proof = await createMigrationProof("object-confirmation-empty-v1", sourceHash, cut);
                    const parent = OBJECT_CONFIRMATION_PATH.slice(0, OBJECT_CONFIRMATION_PATH.lastIndexOf("/"));
                    if (!(await this.io.exists(parent))) await this.io.mkdir(parent);
                    this.assertOpen();
                    if (retry) await this.store.retryInitialization([], cut, proof);
                    else await this.store.initializeMigration([], cut, proof);
                    this.state = state;
                } else this.state = reader.state;
                this.ready = !this.closed;
            } catch (error) { this.ready = false; throw this.error(error); }
        });
    }

    has(scopeHash: string, serverGeneration: string, value: ConfirmedObject): boolean {
        this.assertReady(); identity(scopeHash, serverGeneration);
        const checked = object(value);
        return this.state.scopeHash === scopeHash && this.state.serverGeneration === serverGeneration &&
            this.state.values.has(keyOf(checked));
    }

    /** Returns false when bounded retention is full; callers simply recheck or
     * reupload later. The server ACK has already succeeded, so retention
     * failure must never restore/clear journal work. */
    retain(scopeHash: string, serverGeneration: string, input: readonly ConfirmedObject[]): Promise<boolean> {
        try {
            this.assertReady(); identity(scopeHash, serverGeneration);
            if (!Array.isArray(input) || input.length > this.limits.batchRecords) {
                return Promise.resolve(false);
            }
            const seen = new Set<string>(), owned: ConfirmedObject[] = [];
            let bytes = 256;
            for (const raw of input) {
                const value = object(raw), key = keyOf(value);
                if (seen.has(key)) continue;
                seen.add(key); owned.push(value); bytes += estimate(value);
            }
            if (!owned.length) return Promise.resolve(true);
            if (bytes > this.limits.queuedBytes || !this.hasQueueRoom(bytes)) return Promise.resolve(false);
            return this.enqueue(bytes, async () => {
                this.assertReady();
                const sameGeneration = this.state.scopeHash === scopeHash &&
                    this.state.serverGeneration === serverGeneration;
                const candidate: State = sameGeneration ? { ...this.state, values: new Set(this.state.values) } : {
                    scopeHash, serverGeneration, values: new Set(), retainedBytes: 0,
                };
                const additions: Row[] = [];
                for (const value of owned) {
                    const key = keyOf(value);
                    if (candidate.values.has(key)) continue;
                    const cost = estimate(value);
                    if (candidate.values.size >= this.limits.records ||
                        candidate.retainedBytes > this.limits.retainedBytes - cost) return false;
                    candidate.values.add(key); candidate.retainedBytes += cost;
                    additions.push({ schema: 1, op: "confirm", ...value });
                }
                if (!additions.length) return true;
                try {
                    if (!sameGeneration) await this.store.snapshot(rows(candidate), metadata(candidate));
                    else {
                        if (this.store.walSegments >= STORE_LIMITS.compactAfterSegments) {
                            await this.store.snapshot(rows(this.state), metadata(this.state));
                        }
                        await this.store.commit(additions);
                    }
                    this.state = candidate;
                    return true;
                } catch (error) { this.ready = false; throw this.error(error); }
            });
        } catch (error) { return Promise.reject(error); }
    }

    /** Conservative retry fence. Any failed/indeterminate publication may
     * clear the exact observed generation so a later attempt rechecks remote
     * presence instead of repeating a stale positive hint forever. */
    invalidate(scopeHash: string, serverGeneration: string): Promise<boolean> {
        try {
            this.assertReady(); identity(scopeHash, serverGeneration);
            return this.enqueue(256, async () => {
                this.assertReady();
                if (this.state.scopeHash !== scopeHash || this.state.serverGeneration !== serverGeneration ||
                    this.state.values.size === 0) return false;
                const candidate: State = { scopeHash, serverGeneration, values: new Set(), retainedBytes: 0 };
                try {
                    await this.store.snapshot([], metadata(candidate));
                    this.state = candidate;
                    return true;
                } catch (error) { this.ready = false; throw this.error(error); }
            });
        } catch (error) { return Promise.reject(error); }
    }

    close(): void {
        if (this.closed) return;
        this.closed = true; this.ready = false;
        this.retirement = this.chain.then(async () => {
            await this.store.closeAndDrainMaintenance();
            this.state = emptyState();
        });
    }

    closeAndDrain(): Promise<void> {
        this.close();
        return this.retirement ?? Promise.resolve();
    }

    snapshot() {
        return { ready: this.ready && !this.closed, closed: this.closed,
            records: this.state.values.size, estimatedRetainedBytes: this.state.retainedBytes,
            generationBound: this.state.serverGeneration !== null,
            queuedRequests: this.queuedRequests, estimatedQueuedBytes: this.queuedBytes,
            limits: { ...this.limits } };
    }

    private reader() {
        const state = emptyState();
        let expectedRecords = 0, expectedBytes = 0, rowsSeen = 0, bytesSeen = 0;
        let metadataSeen = false;
        const consume = (raw: unknown, mutation: boolean) => {
            if (!metadataSeen) fail("CORRUPT", "object-confirmation rows precede metadata");
            exact(raw, ["schema", "op", "kind", "hash"], "invalid object-confirmation row");
            if (raw.schema !== 1 || raw.op !== "confirm") fail("UNKNOWN_SCHEMA", "unsupported object-confirmation row schema");
            const value = object({ kind: raw.kind, hash: raw.hash }), key = keyOf(value), cost = estimate(value);
            if (!state.scopeHash || !state.serverGeneration || state.values.has(key) ||
                state.values.size >= this.limits.records || state.retainedBytes > this.limits.retainedBytes - cost) {
                fail("CORRUPT", "invalid or duplicate object-confirmation row");
            }
            state.values.add(key); state.retainedBytes += cost; rowsSeen++; bytesSeen += cost;
            if (mutation) { expectedRecords++; expectedBytes += cost; }
        };
        return { state,
            metadata: (raw: unknown) => {
                exact(raw, ["schema", "kind", "hashFormat", "scopeHash", "serverGeneration", "records", "retainedBytes"],
                    "invalid object-confirmation metadata");
                if (raw.schema !== 1 || raw.kind !== "object-confirmation" || raw.hashFormat !== "blake3-256-v1") {
                    fail("UNKNOWN_SCHEMA", "unsupported object-confirmation schema");
                }
                if ((raw.scopeHash === null) !== (raw.serverGeneration === null)) fail("CORRUPT", "partial object-confirmation scope");
                if (raw.scopeHash !== null) identity(raw.scopeHash, raw.serverGeneration);
                if (!integer(raw.records) || raw.records > this.limits.records || !integer(raw.retainedBytes) ||
                    raw.retainedBytes > this.limits.retainedBytes) fail("LIMIT", "object-confirmation metadata exceeds ceiling");
                state.scopeHash = raw.scopeHash; state.serverGeneration = raw.serverGeneration;
                expectedRecords = raw.records; expectedBytes = raw.retainedBytes; metadataSeen = true;
            },
            snapshot: (raw: unknown) => consume(raw, false),
            mutation: (raw: unknown) => consume(raw, true),
            finish: () => {
                if (!metadataSeen || rowsSeen !== expectedRecords || bytesSeen !== expectedBytes) {
                    fail("CORRUPT", "object-confirmation closure is incomplete");
                }
            } };
    }

    private hasQueueRoom(bytes: number): boolean {
        return this.queuedRequests < this.limits.queuedRequests && bytes <= this.limits.queuedBytes - this.queuedBytes;
    }
    private enqueue<T>(bytes: number, operation: () => Promise<T>): Promise<T> {
        if (this.closed) return Promise.reject(new ObjectConfirmationStoreError("CLOSED", "object-confirmation store is closed"));
        if (!this.hasQueueRoom(bytes)) return Promise.reject(new ObjectConfirmationStoreError("LIMIT", "object-confirmation queue is full"));
        this.queuedRequests++; this.queuedBytes += bytes;
        const result = this.chain.then(operation).finally(() => { this.queuedRequests--; this.queuedBytes -= bytes; });
        this.chain = result.then(() => undefined, () => undefined);
        return result;
    }
    private assertOpen(): void { if (this.closed) fail("CLOSED", "object-confirmation store is closed"); }
    private assertReady(): void {
        this.assertOpen();
        if (!this.ready) fail("RECOVERY_REQUIRED", "object-confirmation store requires validated load");
    }
    private error(error: unknown): unknown {
        return error instanceof StoreRecoveryError ? new ObjectConfirmationStoreError(error.code, error.message) : error;
    }
}
