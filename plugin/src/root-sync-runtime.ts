import type { ObsetyncApi } from "./api";
import type { PlatformIO } from "./platform";
import type { WasmModule } from "./push";
import type { ObsetyncSyncBase, RootBasePublicationEntry } from "./sync-base";
import type { ObsetyncJournal } from "./journal";
import type { SegmentedStoreIO } from "./segmented-store";
import type { PerfOperation } from "./perf-trace";
import { RootIntentStore, ROOT_INTENT_LIMITS, type PendingRootIntent, type RootJournalCut } from "./root-intent";
import { RootConflictPreserver } from "./root-conflicts";
import { RootSettlementCoordinator } from "./root-settlement";
import { RootRecoveryCoordinator, type RootRecoveryResult } from "./root-recovery";
import { computeRootRequestHash, createRootCommitIntent, ROOT_MAX_BYTES, decodeRootOutcome,
    rootCommitRootByteLength, type RootCommitRequest, type RootOutcomeCapabilities } from "./root-outcome";
import { withStreamingHashSource } from "./hash-source-budget";
import { yieldWork } from "./work-scheduler";
import { reserveTransientScope, type TransientReservationBudget } from "./transient-memory";
import type { OwnedTreeRootExport } from "./tree-root-export-job";
import { estimateRootIntentPreparationWorkset, ROOT_INTENT_HASH_FEED_BYTES } from "./root-publication-memory";

export interface RootSyncIdentity {
    readonly vaultId: string;
    readonly deviceId: string;
    readonly scopeHash: Promise<string>;
    /** Must bind actual immutable API credentials, not mutable transport hints. */
    assertApiScope(): void;
}
export interface RootCandidatePublication {
    /** Consumed on entry. publish() releases it on every exit, and always
     * before root-intent persistence, network dispatch or conflict work. */
    readonly rootExport: OwnedTreeRootExport;
    readonly candidateRoot: string;
    readonly treeVersion: number;
    readonly parentRoot: string;
    readonly entries: readonly RootBasePublicationEntry[];
    readonly journalEpoch: string;
    readonly journalCuts: readonly RootJournalCut[];
}

/** Detached authority cut exposed only after network/recovery admission has
 * been synchronously closed and every admitted root tail has drained. The
 * owning runtime deliberately remains resident until the downgrade lease is
 * retired, so callers never infer persistence from an already-disposed
 * in-memory store. */
export interface RootDowngradeAuthoritySnapshot {
    readonly pending: PendingRootIntent | null;
    readonly lastSequence: number;
}

/** Engine-owned bridge. Local load is called only after capture is installed.
 * The engine serializes all methods and joins its actual operation tracker
 * before dispose; this class never loads shared base/journal or repairs trees.
 * No fallback is selected after an intent has been prepared. */
export class RootSyncRuntime {
    private intents?: RootIntentStore;
    private conflicts?: RootConflictPreserver;
    private recovery?: RootRecoveryCoordinator;
    private initializing: Promise<void> | null = null;
    private closed = false;
    private caps: RootOutcomeCapabilities | null = null;
    private closeWork: Promise<void> | null = null;
    private quiesced = false;
    private disposeWork: Promise<void> | null = null;

    constructor(private readonly identity: RootSyncIdentity, private readonly storage: SegmentedStoreIO,
        private readonly api: ObsetyncApi, private readonly io: PlatformIO, private readonly wasm: WasmModule,
        private readonly base: ObsetyncSyncBase, private readonly journal: ObsetyncJournal,
        private readonly beforeHeavyBatch?: () => Promise<void>) {}

    load(): Promise<void> {
        if (this.initializing) return this.initializing;
        this.initializing = (async () => {
            const scopeHash = await this.identity.scopeHash;
            this.assertOpen();
            this.intents ??= new RootIntentStore(this.storage, scopeHash, bytes => this.hash(bytes),
                (vaultId, deviceId, request) => this.hashRequest(vaultId, deviceId, request));
            await this.intents.load();
            this.assertOpen();
            const guardedObjects = { getObjectsOwned: async (...args: Parameters<ObsetyncApi["getObjectsOwned"]>) => {
                this.assertOpen();
                await this.beforeHeavyBatch?.();
                this.assertDispatch();
                return this.api.getObjectsOwned(...args);
            } };
            this.conflicts = new RootConflictPreserver(this.intents, guardedObjects, this.io, this.wasm);
            this.recovery = new RootRecoveryCoordinator(this.intents,
                new RootSettlementCoordinator(this.intents, this.base, this.journal, this.conflicts), this.api,
                { scopeHash, vaultId: this.identity.vaultId, deviceId: this.identity.deviceId,
                    assertApiScope: () => { this.assertOpen(); this.identity.assertApiScope(); } });
        })().catch(error => {
            // A transient read/recovery error is not a permanent cached
            // initialization failure. The actual load tail has settled here;
            // a later explicit retry revalidates the same authoritative store.
            this.initializing = null;
            throw error;
        });
        return this.initializing;
    }
    /** Guards ordinary engine work too, including a fresh stream with no
     * pending intent. Already accepted settlement tails do not call this. */
    assertCurrentScope(): void { this.assertDispatch(); }
    pending(): PendingRootIntent | null { return this.requireStore().pending(); }
    async reload(): Promise<void> {
        this.assertOpen();
        if (!this.intents || !this.recovery) throw new Error("root runtime requires local initialization");
        await this.intents.load();
    }
    drainMaintenance(): Promise<void> { return this.requireStore().drainMaintenance(); }
    get lastSequence(): number { return this.requireStore().lastSequence; }
    get selectedRootByteLimit(): number {
        this.assertOpen();
        if (!this.caps) throw new Error("durable root publication lacks negotiated limits");
        return Math.min(ROOT_MAX_BYTES, this.caps.rootBytes);
    }
    get selectedServerGeneration(): string {
        this.assertOpen();
        if (!this.caps) throw new Error("durable root publication lacks negotiated server generation");
        return this.caps.serverIncarnation;
    }
    snapshot(): { loaded: boolean; pending: boolean; closed: boolean } {
        return { loaded: !!this.intents?.snapshot().ready, pending: this.intents?.snapshot().pending ?? false, closed: this.closed };
    }
    captureDowngradeAuthority(): RootDowngradeAuthoritySnapshot {
        if (!this.quiesced) throw new Error("root runtime must be fully quiesced before downgrade capture");
        const store = this.requireStore();
        return Object.freeze({ pending: store.pending(), lastSequence: store.lastSequence });
    }
    resolvePending(): Promise<RootRecoveryResult> {
        this.assertOpen();
        if (!this.recovery) throw new Error("root runtime is not loaded");
        return this.recovery.resolvePending();
    }

    /** Decide BEFORE preparing an intent. A transport/schema error is not an
     * unsupported offer. Never adopt a remote sequence after local state loss. */
    async selectMode(perf?: PerfOperation): Promise<"durable" | "legacy"> {
        const store = this.requireStore();
        if (store.pending()) throw new Error("root recovery must finish before publication selection");
        this.caps = null;
        this.assertDispatch();
        const caps = await this.api.negotiateRootOutcomes(true, perf);
        this.assertOpen();
        if (caps === null) return "legacy";
        if (!caps) throw new Error("invalid root capability selection");
        this.assertDispatch();
        const stream = decodeRootOutcome(await this.api.queryRootOutcome(this.identity.vaultId, undefined, perf));
        this.assertOpen();
        if (stream.status !== "stream" || stream.last_sequence !== store.lastSequence) {
            throw new Error("root stream high-water differs; explicit reconciliation is required");
        }
        if (stream.server_incarnation !== caps.serverIncarnation) {
            throw new Error("server restarted during root stream preflight; retry before preparing");
        }
        this.caps = caps;
        return "durable";
    }

    /** assertFreshSend belongs only to this newly prepared invocation. Recovery
     * and already-dispatched terminal bookkeeping must not depend on its old
     * selected sources or reviewed policy remaining current. */
    async publish(candidate: RootCandidatePublication, assertFreshSend?: () => void): Promise<RootRecoveryResult> {
        let owner: OwnedTreeRootExport | undefined = candidate.rootExport;
        try {
            const store = this.requireStore(), caps = this.caps;
            this.assertOpen();
            if (!caps || !this.recovery || store.pending()) throw new Error("durable root publication lacks an exclusive negotiated stream");
            this.caps = null; // One successful stream preflight authorizes one submission only.
            if (candidate.entries.length > ROOT_INTENT_LIMITS.entries || candidate.journalCuts.length > ROOT_INTENT_LIMITS.journalCuts) {
                throw new Error("candidate root bytes differ from the validated tree or exceed publication limits");
            }
            const sequence = store.lastSequence + 1;
            if (!Number.isSafeInteger(sequence) || sequence > caps.maxSequence) throw new Error("root stream sequence is exhausted");
            // Capture all bounded metadata before the first await. The helper
            // returns no raw byte/view alias, only the immutable base64 intent.
            const entries = candidate.entries.map(entry => ({ ...entry }));
            const journalCuts = candidate.journalCuts.map(cut => ({ ...cut }));
            const journalEpoch = candidate.journalEpoch, candidateRoot = candidate.candidateRoot;
            const treeVersion = candidate.treeVersion, parentRoot = candidate.parentRoot, committedAt = Date.now();
            const random = new Uint8Array(16); crypto.getRandomValues(random);
            const mutation = [...random].map(byte => byte.toString(16).padStart(2, "0")).join("");
            const request = await this.materializeIntent(owner, caps, { protocol_version: 1,
                server_incarnation: caps.serverIncarnation, sequence, mutation_id: mutation,
                parent_root: parentRoot, root: "" }, candidateRoot, treeVersion);
            // Exact lifetime boundary: both raw-root and digest-preimage helper
            // frames have unwound. Persistence/rehash/HTTP/conflicts must use
            // independent global admission, never wait behind this owner.
            owner.release(); owner = undefined;
            this.assertOpen();
            return this.recovery.prepareAndSend({ vaultId: this.identity.vaultId, deviceId: this.identity.deviceId,
                request, journalEpoch, journalCuts, publication: { identity: { scopeHash: store.scopeHash,
                    sequence, mutationId: mutation, requestHash: request.request_hash }, candidateRoot, committedAt, entries } }, assertFreshSend);
        } finally {
            owner?.release();
        }
    }

    /** Synchronous admission close. Both tails retain their actual promises;
     * a received acceptance may persist while conflict work remains pending. */
    quiesce(): Promise<void> {
        if (this.closeWork) return this.closeWork;
        this.closed = true;
        let resolve!: () => void, reject!: (reason: unknown) => void;
        this.closeWork = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
        try { Promise.all([this.recovery?.closeAndDrain(), this.conflicts?.closeAndDrain()]).then(() => {
            this.quiesced = true;
            resolve();
        }, reject); }
        catch (error) { reject(error); }
        return this.closeWork;
    }
    /** Only after the engine's operation/preparation drain. Repeated terminal
     * owners share the exact close rather than clearing the store twice. */
    dispose(): Promise<void> {
        return this.disposeWork ??= (async () => { await this.quiesce(); await this.intents?.closeAndDrain(); })();
    }
    private requireStore(): RootIntentStore {
        if (!this.intents?.snapshot().ready) throw new Error("root intent storage requires validated load");
        return this.intents;
    }
    private assertOpen(): void { if (this.closed) throw new Error("root runtime is closing"); }
    private assertDispatch(): void { this.assertOpen(); this.identity.assertApiScope(); this.assertOpen(); }
    private async materializeIntent(owner: OwnedTreeRootExport, caps: RootOutcomeCapabilities,
        request: RootCommitRequest, candidateRoot: string, treeVersion: number) {
        const length = owner.bytes.byteLength;
        if (length > ROOT_MAX_BYTES || length > caps.rootBytes) {
            throw new Error("candidate root bytes differ from the validated tree or exceed publication limits");
        }
        const plan = estimateRootIntentPreparationWorkset(length);
        const preparation = await reserveTransientScope(plan, { budget: owner.memory });
        try {
            return await preparation.track(async () => {
                const bytes = owner.bytes;
                if (this.wasm.wasm_root_hash_from_bytes(bytes) !== candidateRoot ||
                    this.wasm.wasm_root_version_from_bytes(bytes) !== treeVersion) {
                    throw new Error("candidate root bytes differ from the validated tree or exceed publication limits");
                }
                let binary = "";
                for (let start = 0; start < bytes.length; start += 16384) {
                    binary += String.fromCharCode(...bytes.subarray(start, start + 16384));
                }
                return createRootCommitIntent(this.identity.vaultId, this.identity.deviceId,
                    { ...request, root: btoa(binary) }, input => this.hash(input, preparation));
            });
        } finally {
            preparation.close();
        }
    }
    private async hashRequest(vaultId: string, deviceId: string, request: RootCommitRequest): Promise<string> {
        const plan = estimateRootIntentPreparationWorkset(rootCommitRootByteLength(request));
        const memory = await reserveTransientScope(plan);
        try {
            return await memory.track(() => computeRootRequestHash(vaultId, deviceId, request,
                bytes => this.hash(bytes, memory)));
        } finally {
            memory.close();
        }
    }
    private hash(bytes: Uint8Array, budget?: TransientReservationBudget): Promise<string> {
        return withStreamingHashSource({ feedBytes: ROOT_INTENT_HASH_FEED_BYTES, budget, consume: async () => {
            const hasher = new this.wasm.Hasher();
            try {
                for (let start = 0; start < bytes.length; start += ROOT_INTENT_HASH_FEED_BYTES) {
                    hasher.update(bytes.subarray(start, start + ROOT_INTENT_HASH_FEED_BYTES));
                    await yieldWork();
                }
                return hasher.finalize();
            } finally { hasher.free(); }
        } });
    }
}
