import { isSafeVaultPath } from "./delta-validation";
import { checksumJournalUtf8 } from "./journal-format";
import { SegmentedStore, STORE_LIMITS, StoreRecoveryError, createMigrationProof,
    type SegmentedStoreIO } from "./segmented-store";
import { detachRootBasePublication, type RootBasePublication,
    type RootPublicationIdentity } from "./sync-base";
import { detachRootCommitIntent, computeRootRequestHash, validateRootTerminalOutcome,
    type RootCommitIntent, type RootCommitRequest, type RootTerminalOutcome } from "./root-outcome";
import { legacyDowngradeRootSequenceFloor } from "./legacy-downgrade";

export const ROOT_INTENT_PATH = ".obsidian/plugins/obsetync/root-intent.store-v1";
export const ROOT_INTENT_LIMITS = {
    entries: 256, journalCuts: 256, conflictCopies: 256, rootPieceChars: 16 * 1024,
    encodedBytes: 1280 * 1024, queuedRequests: 4, queuedBytes: 8 * 1024 * 1024,
} as const;
type HashBytes = (bytes: Uint8Array) => string | Promise<string>;
type HashRequest = (vaultId: string, deviceId: string, request: RootCommitRequest) => Promise<string>;
export interface RootJournalCut { path: string; throughId: number }
export interface RootConflictCopyReceipt { path: string; copyPath: string; hash: string; size: number }
export interface StoredRootIntent {
    vaultId: string;
    deviceId: string;
    request: RootCommitIntent;
    publication: RootBasePublication;
    journalEpoch: string;
    journalCuts: readonly RootJournalCut[];
}
export interface PendingRootIntent {
    intent: StoredRootIntent;
    terminal: RootTerminalOutcome | null;
    readonly conflictCopies: readonly RootConflictCopyReceipt[];
}
export class RootIntentError extends Error {
    constructor(readonly code: "CORRUPT" | "UNKNOWN_SCHEMA" | "RECOVERY_REQUIRED" |
        "IDENTITY" | "LIMIT" | "CLOSED", message: string) {
        super(message); this.name = "RootIntentError";
    }
}
interface State { lastSequence: number; pending: PendingRootIntent | null }
interface Header {
    vaultId: string; deviceId: string; request: Omit<RootCommitIntent, "root">;
    publication: Omit<RootBasePublication, "entries">; journalEpoch: string;
    rootChars: number; entryCount: number; cutCount: number;
}
type Row = { schema: 1; op: "intent"; header: Header } |
    { schema: 1; op: "root"; index: number; data: string } |
    { schema: 1; op: "entry"; index: number; entry: RootBasePublication["entries"][number] } |
    { schema: 1; op: "cut"; index: number; cut: RootJournalCut } |
    { schema: 1; op: "seal"; identity: RootPublicationIdentity } |
    { schema: 1; op: "terminal"; terminal: RootTerminalOutcome } |
    { schema: 1; op: "conflict-copy"; identity: RootPublicationIdentity; receipt: RootConflictCopyReceipt } |
    { schema: 1; op: "retire"; identity: RootPublicationIdentity };
const empty = (): State => ({ lastSequence: 0, pending: null });
const hash = (v: unknown): v is string => typeof v === "string" && /^[0-9a-f]{64}$/.test(v);
const epoch = (v: unknown): v is string => typeof v === "string" && /^[0-9a-f]{32}$/.test(v);
const uint = (v: unknown, min = 0): v is number => Number.isSafeInteger(v) && (v as number) >= min;
function fail(code: RootIntentError["code"], message: string): never { throw new RootIntentError(code, message); }
function exact(v: unknown, keys: readonly string[]): asserts v is Record<string, any> {
    if (!v || typeof v !== "object" || Array.isArray(v) || Object.keys(v).length !== keys.length ||
        Object.keys(v).some(key => !keys.includes(key))) fail("CORRUPT", "invalid root-intent fields");
}
function identityCopy(v: unknown): RootPublicationIdentity {
    exact(v, ["scopeHash", "sequence", "mutationId", "requestHash"]);
    if (!hash(v.scopeHash) || !uint(v.sequence, 1) || !epoch(v.mutationId) || !hash(v.requestHash)) {
        fail("CORRUPT", "invalid root-intent identity");
    }
    return { scopeHash: v.scopeHash, sequence: v.sequence, mutationId: v.mutationId, requestHash: v.requestHash };
}
function sameIdentity(a: RootPublicationIdentity, b: RootPublicationIdentity): boolean {
    return a.scopeHash === b.scopeHash && a.sequence === b.sequence &&
        a.mutationId === b.mutationId && a.requestHash === b.requestHash;
}
function wireIdentity(intent: StoredRootIntent) {
    return { sequence: intent.request.sequence, mutation_id: intent.request.mutation_id,
        request_hash: intent.request.request_hash };
}
function terminalKey(terminal: RootTerminalOutcome): string {
    const { server_incarnation: _incarnation, ...receipt } = terminal;
    return JSON.stringify(receipt);
}
function copyConflictReceipt(raw: unknown): RootConflictCopyReceipt {
    exact(raw, ["path", "copyPath", "hash", "size"]);
    if (!isSafeVaultPath(raw.path) || !isSafeVaultPath(raw.copyPath) || raw.path === raw.copyPath ||
        !hash(raw.hash) || !uint(raw.size)) fail("CORRUPT", "invalid root conflict-copy receipt");
    return { path: raw.path, copyPath: raw.copyPath, hash: raw.hash, size: raw.size };
}
function validateConflictBinding(pending: PendingRootIntent, receipt: RootConflictCopyReceipt): void {
    const terminal = pending.terminal;
    if (!terminal || terminal.status !== "accepted" || !("merged" in terminal.result)) {
        fail("IDENTITY", "root conflict copy lacks an accepted merged outcome");
    }
    const conflict = terminal.result.conflicts.find(conflict => conflict.path === receipt.path);
    const entry = pending.intent.publication.entries.find(entry => entry.path === receipt.path);
    if (!conflict || conflict.side_b_hash !== receipt.hash || entry?.action !== "upsert" ||
        entry.hash !== receipt.hash || entry.size !== receipt.size) {
        fail("IDENTITY", "root conflict copy differs from its accepted publication");
    }
}
function addConflictReceipt(pending: PendingRootIntent, receipt: RootConflictCopyReceipt): PendingRootIntent {
    if (pending.conflictCopies.length >= ROOT_INTENT_LIMITS.conflictCopies) fail("LIMIT", "root conflict-copy ceiling exceeded");
    return { ...pending, conflictCopies: [...pending.conflictCopies, receipt]
        .sort((a, b) => a.path < b.path ? -1 : a.path > b.path ? 1 : 0) };
}
function assertRetirable(pending: PendingRootIntent): void {
    const terminal = pending.terminal;
    if (!terminal) fail("IDENTITY", "cannot retire an unresolved root intent");
    if (terminal.status === "accepted" && "merged" in terminal.result && terminal.result.conflicts.some(conflict =>
        !pending.conflictCopies.some(receipt => receipt.path === conflict.path && receipt.hash === conflict.side_b_hash))) {
        fail("IDENTITY", "cannot retire before every root conflict copy is recorded");
    }
}
function copyIntent(raw: unknown, scopeHash: string): StoredRootIntent {
    exact(raw, ["vaultId", "deviceId", "request", "publication", "journalEpoch", "journalCuts"]);
    for (const field of [raw.vaultId, raw.deviceId]) {
        if (typeof field !== "string" || !field.length || field.length > 128 ||
            new TextEncoder().encode(field).length > 128) fail("CORRUPT", "invalid root-intent wire owner");
    }
    if (!epoch(raw.journalEpoch)) fail("CORRUPT", "invalid root-intent journal epoch");
    if (!Array.isArray(raw.journalCuts) || raw.journalCuts.length > ROOT_INTENT_LIMITS.journalCuts) {
        fail("LIMIT", "root-intent journal cut exceeds ceiling");
    }
    const request = detachRootCommitIntent(raw.request);
    const publication = detachRootBasePublication(raw.publication);
    const id = publication.identity;
    if (id.scopeHash !== scopeHash || id.sequence !== request.sequence ||
        id.mutationId !== request.mutation_id || id.requestHash !== request.request_hash) {
        fail("IDENTITY", "root-intent publication and request identities differ");
    }
    const paths = new Set(publication.entries.map(entry => entry.path));
    const journalCuts: RootJournalCut[] = [];
    let previous: string | null = null;
    for (const cut of raw.journalCuts) {
        exact(cut, ["path", "throughId"]);
        if (typeof cut.path !== "string" || cut.path.length > 4096 || !isSafeVaultPath(cut.path) ||
            !uint(cut.throughId, 1) || cut.throughId === Number.MAX_SAFE_INTEGER ||
            !paths.has(cut.path) || (previous !== null && cut.path <= previous)) {
            fail("CORRUPT", "root-intent journal cut is unordered or unrelated to its publication");
        }
        previous = cut.path; journalCuts.push({ path: cut.path, throughId: cut.throughId });
    }
    const result = { vaultId: raw.vaultId, deviceId: raw.deviceId, request, publication,
        journalEpoch: raw.journalEpoch, journalCuts };
    encodedSize(result);
    return result;
}
function intentHeader(intent: StoredRootIntent): Header {
    const { root, ...request } = intent.request;
    const { entries, ...publication } = intent.publication;
    return { vaultId: intent.vaultId, deviceId: intent.deviceId, request, publication,
        journalEpoch: intent.journalEpoch, rootChars: root.length,
        entryCount: entries.length, cutCount: intent.journalCuts.length };
}
function* intentRows(intent: StoredRootIntent): IterableIterator<Row> {
    yield { schema: 1, op: "intent", header: intentHeader(intent) };
    for (let start = 0, index = 0; start < intent.request.root.length; start += ROOT_INTENT_LIMITS.rootPieceChars, index++) {
        yield { schema: 1, op: "root", index, data: intent.request.root.slice(start, start + ROOT_INTENT_LIMITS.rootPieceChars) };
    }
    for (let index = 0; index < intent.publication.entries.length; index++) {
        yield { schema: 1, op: "entry", index, entry: intent.publication.entries[index] };
    }
    for (let index = 0; index < intent.journalCuts.length; index++) {
        yield { schema: 1, op: "cut", index, cut: intent.journalCuts[index] };
    }
    yield { schema: 1, op: "seal", identity: intent.publication.identity };
}
function* snapshotRows(state: State): IterableIterator<Row> {
    if (!state.pending) return;
    yield* intentRows(state.pending.intent);
    if (state.pending.terminal) yield { schema: 1, op: "terminal", terminal: state.pending.terminal };
    for (const receipt of state.pending.conflictCopies) {
        yield { schema: 1, op: "conflict-copy", identity: state.pending.intent.publication.identity, receipt };
    }
}
function encodedSize(intent: StoredRootIntent): number {
    let bytes = 0;
    for (const row of intentRows(intent)) {
        bytes += checksumJournalUtf8(JSON.stringify(row)).bytes;
        if (bytes > ROOT_INTENT_LIMITS.encodedBytes) fail("LIMIT", "root-intent exceeds serialized ceiling");
    }
    return bytes;
}

/** Authoritative one-at-a-time root intents. Not yet an engine coordinator:
 * callers must finish accepted base/conflict/journal settlement before retire.
 * No method sends network requests or acknowledges a journal. Adapter-complete
 * publications inherit SegmentedStore's recovery boundary, not native fsync. */
export class RootIntentStore {
    private readonly store: SegmentedStore;
    private state = empty();
    private ready = false;
    private closed = false;
    private chain: Promise<void> = Promise.resolve();
    private queuedBytes = 0;
    private queuedRequests = 0;

    constructor(private readonly io: SegmentedStoreIO, readonly scopeHash: string,
        private readonly hashBytes: HashBytes, private readonly hashRequest?: HashRequest) {
        if (!hash(scopeHash)) fail("IDENTITY", "invalid root-intent scope");
        this.store = new SegmentedStore(io, ROOT_INTENT_PATH);
    }
    load(): Promise<void> {
        return this.enqueue(256, async () => {
            this.assertOpen(); this.ready = false;
            let downgradeFloor: number | null;
            try { downgradeFloor = await legacyDowngradeRootSequenceFloor(this.io); }
            catch { fail("RECOVERY_REQUIRED", "legacy downgrade root sequence requires recovery"); }
            if (downgradeFloor !== null && downgradeFloor > 0 && !(await this.io.exists(ROOT_INTENT_PATH))) {
                fail("RECOVERY_REQUIRED", "legacy downgrade root sequence store is unavailable");
            }
            const reader = this.reader();
            let present = false, retry = false;
            try { present = await this.store.load(reader.metadata, reader.snapshot, reader.mutation, reader.finish); }
            catch (error) {
                if (!(error instanceof StoreRecoveryError) || error.code !== "RECOVERY_REQUIRED") throw error;
                retry = true;
            }
            if (!present) {
                const state = empty(), metadata = this.metadata(state);
                const source = new TextEncoder().encode(JSON.stringify({ domain: "obsetync-root-intent-empty-v1",
                    target: ROOT_INTENT_PATH, scopeHash: this.scopeHash }));
                const digest = new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", source));
                const sourceHash = [...digest].map(byte => byte.toString(16).padStart(2, "0")).join("");
                const proof = await createMigrationProof("root-intent-empty-v1", sourceHash, metadata);
                const parent = ROOT_INTENT_PATH.slice(0, ROOT_INTENT_PATH.lastIndexOf("/"));
                this.assertOpen();
                if (!(await this.io.exists(parent))) await this.io.mkdir(parent);
                if (retry) await this.store.retryInitialization([], metadata, proof);
                else await this.store.initializeMigration([], metadata, proof);
                this.state = state;
            } else this.state = reader.state;
            if (downgradeFloor !== null && this.state.lastSequence < downgradeFloor) {
                fail("RECOVERY_REQUIRED", "legacy downgrade root sequence moved backwards");
            }
            this.ready = !this.closed;
        });
    }
    get lastSequence(): number { this.assertReady(); return this.state.lastSequence; }
    pending(): PendingRootIntent | null {
        this.assertReady();
        const current = this.state.pending;
        return current ? { intent: copyIntent(current.intent, this.scopeHash), terminal: current.terminal ?
            validateRootTerminalOutcome(current.terminal, wireIdentity(current.intent)) : null,
            conflictCopies: current.conflictCopies.map(receipt => ({ ...receipt })) } : null;
    }
    prepare(raw: StoredRootIntent): Promise<"prepared" | "already-prepared"> {
        try {
            this.assertReady();
            // Reserve the worst-case bounded input before detached ownership.
            this.assertQueueRoom(ROOT_INTENT_LIMITS.encodedBytes * 2);
            const intent = copyIntent(raw, this.scopeHash);
            const bytes = encodedSize(intent) * 2;
            return this.enqueue(bytes, async () => {
                this.assertReady();
                await this.verifyDigest(intent);
                const current = this.state.pending;
                if (current) {
                    if (JSON.stringify(current.intent) !== JSON.stringify(intent)) fail("IDENTITY", "another root intent is pending");
                    return "already-prepared";
                }
                if (intent.request.sequence !== this.state.lastSequence + 1 || this.state.lastSequence === Number.MAX_SAFE_INTEGER) {
                    fail("IDENTITY", "root-intent sequence is not the next local stream operation");
                }
                await this.publish(intentRows(intent), { lastSequence: intent.request.sequence,
                    pending: { intent, terminal: null, conflictCopies: [] } });
                return "prepared";
            });
        } catch (error) { return Promise.reject(error); }
    }
    recordTerminal(raw: RootTerminalOutcome): Promise<"recorded" | "already-recorded"> {
        try {
            this.assertReady();
            const current = this.state.pending;
            if (!current) fail("IDENTITY", "no root intent owns this terminal outcome");
            const terminal = validateRootTerminalOutcome(raw, wireIdentity(current.intent));
            const id = identityCopy(current.intent.publication.identity);
            const bytes = checksumJournalUtf8(JSON.stringify(terminal)).bytes * 2;
            return this.enqueue(bytes, async () => {
                this.assertReady();
                const pending = this.requirePending(id);
                if (pending.terminal) {
                    if (terminalKey(pending.terminal) !== terminalKey(terminal)) fail("IDENTITY", "root terminal outcome changed");
                    return "already-recorded";
                }
                await this.publish([{ schema: 1, op: "terminal", terminal }],
                    { ...this.state, pending: { ...pending, terminal } });
                return "recorded";
            });
        } catch (error) { return Promise.reject(error); }
    }
    /** Call only after the actual exclusive copy completes or existing content
     * is fully hash-verified. This records historical SDK-level completion, not
     * a filesystem probe, atomic visibility or native fsync guarantee. Later
     * edits to the copied file never turn this receipt into a rewrite command. */
    recordConflictCopy(rawIdentity: RootPublicationIdentity, rawReceipt: RootConflictCopyReceipt): Promise<"recorded" | "already-recorded"> {
        try {
            this.assertReady();
            // Two bounded paths, one identity and serialization ownership.
            this.assertQueueRoom(2 * (2 * 4096 * 4 + 1024));
            const identity = identityCopy(rawIdentity), receipt = copyConflictReceipt(rawReceipt);
            validateConflictBinding(this.requirePending(identity), receipt);
            const row: Row = { schema: 1, op: "conflict-copy", identity, receipt };
            const bytes = checksumJournalUtf8(JSON.stringify(row)).bytes * 2;
            return this.enqueue(bytes, async () => {
                this.assertReady();
                const pending = this.requirePending(identity);
                validateConflictBinding(pending, receipt);
                const previous = pending.conflictCopies.find(copy => copy.path === receipt.path);
                if (previous) {
                    if (JSON.stringify(previous) !== JSON.stringify(receipt)) fail("IDENTITY", "root conflict-copy receipt changed");
                    return "already-recorded";
                }
                const updated = addConflictReceipt(pending, receipt);
                await this.publish([row], { ...this.state, pending: updated });
                return "recorded";
            });
        } catch (error) { return Promise.reject(error); }
    }
    /** Accepted: call ONLY after durable base publication, verified conflict
     * copies and exact epoch-bound journal ACK. Cancelled: never ACK local work.
     * A durable receipt alone is deliberately not an automatic retirement. */
    retire(rawIdentity: RootPublicationIdentity): Promise<void> {
        try {
            const identity = identityCopy(rawIdentity);
            return this.enqueue(512, async () => {
                this.assertReady();
                const pending = this.requirePending(identity);
                assertRetirable(pending);
                await this.publish([{ schema: 1, op: "retire", identity }], { ...this.state, pending: null });
            });
        } catch (error) { return Promise.reject(error); }
    }
    compact(): Promise<void> {
        return this.enqueue(256, async () => {
            this.assertReady();
            try { await this.store.snapshot(snapshotRows(this.state), this.metadata(this.state)); }
            catch (error) { this.ready = false; throw error; }
        });
    }
    /** Join maintenance admitted by every facade operation before this call
     * without closing either the authoritative queue or future cleanup. */
    drainMaintenance(): Promise<void> {
        return this.enqueue(0, () => this.store.drainMaintenance());
    }
    async closeAndDrain(): Promise<void> {
        this.closed = true; this.ready = false;
        await this.chain;
        await this.store.closeAndDrainMaintenance();
        this.state = empty();
    }
    snapshot() {
        return { ready: this.ready && !this.closed, closed: this.closed, pending: !!this.state.pending,
            queuedRequests: this.queuedRequests, estimatedQueuedBytes: this.queuedBytes };
    }
    private metadata(state: State) {
        return { schema: 1, kind: "root-intent", scopeHash: this.scopeHash,
            lastSequence: state.lastSequence, pending: !!state.pending };
    }
    private async verifyDigest(intent: StoredRootIntent): Promise<void> {
        const { request_hash, ...request } = intent.request;
        const actual = this.hashRequest
            ? await this.hashRequest(intent.vaultId, intent.deviceId, request)
            : await computeRootRequestHash(intent.vaultId, intent.deviceId, request, this.hashBytes);
        if (actual !== request_hash) {
            fail("CORRUPT", "root-intent request digest differs from exact submitted bytes");
        }
    }
    private requirePending(identity: RootPublicationIdentity): PendingRootIntent {
        const pending = this.state.pending;
        if (!pending || !sameIdentity(identity, pending.intent.publication.identity)) fail("IDENTITY", "root-intent owner differs");
        return pending;
    }
    private async publish(rows: Iterable<Row>, state: State): Promise<void> {
        try {
            if (this.store.walSegments >= STORE_LIMITS.compactAfterSegments) {
                await this.store.snapshot(snapshotRows(this.state), this.metadata(this.state));
            }
            await this.store.commit(rows);
            this.state = state;
        } catch (error) { this.ready = false; throw error; }
    }
    private assertOpen(): void { if (this.closed) fail("CLOSED", "root-intent store is closed"); }
    private assertReady(): void {
        this.assertOpen();
        if (!this.ready) fail("RECOVERY_REQUIRED", "root-intent store requires validated reload");
    }
    private assertQueueRoom(bytes: number): void {
        if (this.queuedRequests >= ROOT_INTENT_LIMITS.queuedRequests ||
            bytes > ROOT_INTENT_LIMITS.queuedBytes - this.queuedBytes) fail("LIMIT", "root-intent queue is full");
    }
    private enqueue<T>(bytes: number, operation: () => Promise<T>): Promise<T> {
        try { this.assertOpen(); this.assertQueueRoom(bytes); }
        catch (error) { return Promise.reject(error); }
        this.queuedRequests++; this.queuedBytes += bytes;
        const result = this.chain.then(operation).finally(() => { this.queuedRequests--; this.queuedBytes -= bytes; });
        this.chain = result.then(() => undefined, () => undefined);
        return result;
    }
    private reader() {
        const state = empty();
        let expectedPending = false, snapshotDone = false, snapshotIntents = 0;
        let header: Header | null = null, root = "", rootIndex = 0;
        let entries: RootBasePublication["entries"][number][] = [], cuts: RootJournalCut[] = [];
        let assemblingBytes = 0;
        let previousSnapshotCopy: string | null = null;
        const finishSnapshot = () => {
            if (header || snapshotIntents !== Number(expectedPending) || !!state.pending !== expectedPending) {
                fail("CORRUPT", "root-intent snapshot closure differs");
            }
        };
        const consume = async (raw: unknown, snapshot: boolean): Promise<void> => {
            if (!raw || typeof raw !== "object" || (raw as any).schema !== 1) fail("UNKNOWN_SCHEMA", "unsupported root-intent row schema");
            const row = raw as any;
            if (row.op === "intent") {
                exact(row, ["schema", "op", "header"]);
                if (header || state.pending) fail("CORRUPT", "root intent overlaps an unsettled operation");
                exact(row.header, ["vaultId", "deviceId", "request", "publication", "journalEpoch", "rootChars", "entryCount", "cutCount"]);
                const h = row.header;
                exact(h.request, ["protocol_version", "server_incarnation", "sequence", "mutation_id", "parent_root", "request_hash"]);
                exact(h.publication, ["identity", "candidateRoot", "committedAt"]);
                if (!uint(h.rootChars, 4) || h.rootChars > Math.ceil(512 * 1024 / 3) * 4 ||
                    !uint(h.entryCount) || h.entryCount > ROOT_INTENT_LIMITS.entries ||
                    !uint(h.cutCount) || h.cutCount > ROOT_INTENT_LIMITS.journalCuts) fail("LIMIT", "root-intent header exceeds ceiling");
                // The complete validators run at the seal before promotion.
                header = h as Header; root = ""; rootIndex = 0; entries = []; cuts = [];
                assemblingBytes = checksumJournalUtf8(JSON.stringify(row)).bytes;
            } else if (row.op === "root" || row.op === "entry" || row.op === "cut") {
                if (!header) fail("CORRUPT", "root-intent body has no header");
                assemblingBytes += checksumJournalUtf8(JSON.stringify(row)).bytes;
                if (assemblingBytes > ROOT_INTENT_LIMITS.encodedBytes) fail("LIMIT", "root-intent body exceeds ceiling");
                if (row.op === "root") {
                    exact(row, ["schema", "op", "index", "data"]);
                    const length = Math.min(ROOT_INTENT_LIMITS.rootPieceChars, header.rootChars - root.length);
                    if (entries.length || cuts.length || length === 0 || row.index !== rootIndex ||
                        typeof row.data !== "string" || row.data.length !== length || !/^[A-Za-z0-9+/=]+$/.test(row.data)) {
                        fail("CORRUPT", "root-intent root pieces are incomplete or reordered");
                    }
                    root += row.data; rootIndex++;
                } else if (row.op === "entry") {
                    exact(row, ["schema", "op", "index", "entry"]);
                    if (root.length !== header.rootChars || cuts.length || row.index !== entries.length ||
                        entries.length >= header.entryCount) fail("CORRUPT", "root-intent base entries are reordered");
                    entries.push(row.entry);
                } else {
                    exact(row, ["schema", "op", "index", "cut"]);
                    if (root.length !== header.rootChars || entries.length !== header.entryCount ||
                        row.index !== cuts.length || cuts.length >= header.cutCount) fail("CORRUPT", "root-intent journal cuts are reordered");
                    cuts.push(row.cut);
                }
            } else if (row.op === "seal") {
                exact(row, ["schema", "op", "identity"]);
                if (!header || root.length !== header.rootChars || entries.length !== header.entryCount ||
                    cuts.length !== header.cutCount) fail("CORRUPT", "root-intent seal lacks its complete body");
                const intent = copyIntent({ vaultId: header.vaultId, deviceId: header.deviceId,
                    request: { ...header.request, root }, publication: { ...header.publication, entries },
                    journalEpoch: header.journalEpoch, journalCuts: cuts }, this.scopeHash);
                if (!sameIdentity(identityCopy(row.identity), intent.publication.identity) ||
                    intent.request.sequence !== state.lastSequence + (snapshot ? 0 : 1)) fail("CORRUPT", "root-intent sealed sequence differs");
                await this.verifyDigest(intent);
                state.lastSequence = intent.request.sequence; state.pending = { intent, terminal: null, conflictCopies: [] };
                if (snapshot) snapshotIntents++;
                header = null; root = ""; entries = []; cuts = [];
            } else if (row.op === "terminal") {
                exact(row, ["schema", "op", "terminal"]);
                if (header || !state.pending || state.pending.terminal) fail("CORRUPT", "root terminal row has no unresolved owner");
                state.pending.terminal = validateRootTerminalOutcome(row.terminal, wireIdentity(state.pending.intent));
            } else if (row.op === "conflict-copy") {
                exact(row, ["schema", "op", "identity", "receipt"]);
                if (header || !state.pending || !sameIdentity(identityCopy(row.identity), state.pending.intent.publication.identity)) {
                    fail("CORRUPT", "root conflict copy lacks pending ownership");
                }
                const receipt = copyConflictReceipt(row.receipt);
                validateConflictBinding(state.pending, receipt);
                if (state.pending.conflictCopies.some(copy => copy.path === receipt.path) ||
                    (snapshot && previousSnapshotCopy !== null && receipt.path <= previousSnapshotCopy)) {
                    fail("CORRUPT", "root conflict-copy rows are duplicated or reordered");
                }
                if (snapshot) previousSnapshotCopy = receipt.path;
                state.pending = addConflictReceipt(state.pending, receipt);
            } else if (row.op === "retire") {
                exact(row, ["schema", "op", "identity"]);
                if (snapshot || header || !state.pending?.terminal ||
                    !sameIdentity(identityCopy(row.identity), state.pending.intent.publication.identity)) fail("CORRUPT", "root retirement lacks terminal ownership");
                assertRetirable(state.pending);
                state.pending = null;
            } else fail("UNKNOWN_SCHEMA", "unsupported root-intent operation");
        };
        return { state,
            metadata: (raw: unknown) => {
                exact(raw, ["schema", "kind", "scopeHash", "lastSequence", "pending"]);
                if (raw.schema !== 1 || raw.kind !== "root-intent") fail("UNKNOWN_SCHEMA", "unsupported root-intent metadata schema");
                if (raw.scopeHash !== this.scopeHash) fail("IDENTITY", "root-intent store belongs to another scope");
                if (!uint(raw.lastSequence) || typeof raw.pending !== "boolean" ||
                    (raw.pending && raw.lastSequence === 0)) fail("CORRUPT", "invalid root-intent stream watermark");
                state.lastSequence = raw.lastSequence; expectedPending = raw.pending;
            },
            snapshot: (raw: unknown) => consume(raw, true),
            mutation: async (raw: unknown) => {
                if (!snapshotDone) { finishSnapshot(); snapshotDone = true; }
                await consume(raw, false);
            },
            finish: () => {
                if (!snapshotDone) finishSnapshot();
                if (header) fail("CORRUPT", "root intent has no closing seal");
            },
        };
    }
}
