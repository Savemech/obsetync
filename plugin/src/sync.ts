import { App, TAbstractFile, TFile, debounce, Notice } from "obsidian";

/** Yield control to the JS event loop (audio, render, IPC callbacks). */
const yieldToUI = () => new Promise<void>(r => window.setTimeout(r, 0));

/** Bytes → human-readable short form. Used in status/progress messages. */
function formatBytes(n: number): string {
    if (n >= 1_073_741_824) return `${(n / 1_073_741_824).toFixed(2)} GB`;
    if (n >= 1_048_576)     return `${(n / 1_048_576).toFixed(1)} MB`;
    if (n >= 1024)          return `${(n / 1024).toFixed(0)} KB`;
    return `${n} B`;
}

/** Files above this size skip WASM hashing during scan — push.ts hashes them
 *  during upload via FastCDC (wasm_chunk_file returns file_hash). This keeps
 *  WASM linear memory bounded to the ~4 MiB FastCDC window rather than the
 *  complete file. */
const LARGE_FILE_THRESHOLD = 1_048_576; // 1 MB
import { ObsetyncApi, PushConflict } from "./api";
import { conflictCopyPath } from "./conflict-path";
import { ObsetyncWsChannel, PresenceUpdate, WsState } from "./ws";
import { PlatformIO, type FileStat } from "./platform";
import { ObsetyncSyncBase } from "./sync-base";
import { ObsetyncJournal, type NewJournalEntry } from "./journal";
import { perfSpan } from "./debug-log";
import { pull } from "./pull";
import { push, hashFileStreaming, streamingHash, chunkFileStreaming, FileChange, WasmModule, WasmTree } from "./push";
import { SyncPriority } from "./settings";
import { compileIgnore, type CompiledIgnore } from "./ignore";
import { PullEchoTracker } from "./pull-echo";
import { DirtyPathSet, type DirtyFileChange } from "./dirty-set";
import { OperationCheckpoint } from "./operation-checkpoint";
import { exactArrayBuffer } from "./binary";
import { getHashTuning, planByteBoundedBatches } from "./hash-runtime";
import {
    BulkObjectKind,
    type BulkUploadRecord,
} from "./bulk-codec";
import {
    HashWorkerFileDriftError,
    HashWorkerPoolError,
    type DesktopHashWorkerPool,
} from "./desktop-hash-workers";
import { reconcileDesktopLargeFile } from "./desktop-reconcile-upload";
import {
    automaticChangeNeedsReview,
    metadataScanNeedsReview,
    planMetadataScan,
} from "./scan-planner";
import { isSafeVaultPath } from "./delta-validation";
import { isReenrollmentRequiredError } from "./transport-errors";
import { alignTreeFormatFromRoot } from "./tree-format";
import {
    perfSampleWeight,
    perfTrace,
    type PerfOperation,
    type PerfOutcome,
} from "./perf-trace";
import {
    buildReconcileContentIndex,
    sumIndexedContentBytes,
} from "./reconcile-content";

export type SyncState = "idle" | "pulling" | "pushing" | "scanning" | "error";

/**
 * Core sync orchestrator. Coordinates pull, push, journal recovery,
 * metadata auditing, and live vault event tracking (D-005 4-layer system).
 */
export class ObsetyncSyncEngine {
    private state: SyncState = "idle";
    private localRootHash: string | null;
    /** One metadata-only record per dirty path. File bytes are read only when
     *  its push snapshot is actually processed. */
    private pendingChanges = new DirtyPathSet();
    private syncing = false;
    private syncTimer: number | null = null;
    private eventRefs: any[] = [];
    /** Most recent pull/push failure — surfaced by the debug panel. */
    private lastError: { ts: number; message: string; origin: string } | null = null;
    /** Snapshots of observed remote / local roots for the debug panel. */
    private lastPullServerRoot: string | null = null;
    /** The server root this device's Merkle tree was last VERIFIABLY
     *  reconciled with — the honest putRoot parent (merge base). Distinct
     *  from `localRootHash`, which is merely the last root observed on the
     *  server; conflating the two is what let a stale tree fast-forward the
     *  fleet back in time (incident 2026-07-13). Persisted in sync-base. */
    private treeBaseRoot: string | null = null;
    /** Set when the tree demonstrably diverged from the state pull applied.
     *  While set, pushes are refused (queued, not dropped) — publishing a
     *  root from an untrusted tree is how vaults get reverted. Cleared by a
     *  verified pull-rebase or a full rescan. */
    private pushBlocked = false;
    /** Terminal transport mismatch: retries cannot succeed until enrollment
     *  is reset, so startup scans and every automatic network loop pause. */
    private reenrollmentRequired = false;
    private reenrollmentNoticeShown = false;
    /** Automatic recovery found vault-sized or deletion-heavy work. */
    private bulkChangeReviewRequired = false;
    /** Prevent late async completions from reviving an engine after reload. */
    private stopped = false;
    /** Content-authenticated expectations for adapter writes made by pull.
     *  Kept briefly after pull completion because Obsidian can emit the
     *  corresponding vault events asynchronously. */
    private pullEchoes = new PullEchoTracker();
    /** debouncedPush from attachVaultListeners, kept so sync completion can
     *  drain user edits that arrived mid-sync. */
    private debouncedPush: (() => void) | null = null;
    /** Paths whose vault callbacks have started but whose durable journal
     *  append/hash classification has not finished yet. Pull consults this
     *  live set immediately before replacing remote content. */
    private localEventsInFlight = new Set<string>();
    private readonly hashWorkerAbort = new AbortController();
    private hashWorkerFallbackWarned = false;

    constructor(
        private app: App,
        private api: ObsetyncApi,
        private io: PlatformIO,
        private syncBase: ObsetyncSyncBase,
        private journal: ObsetyncJournal,
        private wasm: WasmModule,
        private tree: WasmTree,
        private vaultId: string,
        private syncInterval: number = 30000,
        private syncPriority: SyncPriority = "sequential",
        private onStatusUpdate: (text: string) => void = () => {},
        initialRootHash: string | null = null,
        private syncObsidianConfig: boolean = false,
        /** Human device name — stamped into conflict-copy filenames. */
        private deviceName: string = "device",
        /** Ph2 notify channel: server pushes "root changed" over WebSocket;
         *  polling drops to a slow safety-net cadence while it's alive. */
        private realtimeWs: boolean = true,
        /** Ph3: broadcast which file this device is looking at (receiving
         *  presence always works; this only gates SENDING ours). */
        private sharePresence: boolean = true,
        /** Slice 2: gitignore-style patterns for paths that never sync. */
        ignorePatterns: string[] = [],
        private operationCheckpoint?: OperationCheckpoint,
        private autoSync: boolean = true,
        private hashWorkers: DesktopHashWorkerPool | null = null,
    ) {
        this.localRootHash = initialRootHash;
        this.ignore = compileIgnore(ignorePatterns);
    }

    /** Compiled ignore matcher (Slice 2). Empty ⇒ nothing ignored. */
    private ignore: CompiledIgnore;

    /** The WS notify channel (null when disabled or before start()). */
    private wsChannel: ObsetyncWsChannel | null = null;
    /** Epoch-ms of the last completed pull — drives the slow-poll decision. */
    private lastPullDoneMs = 0;
    /** Ph3 presence: device(short) → latest update from the fleet. */
    private presence = new Map<string, PresenceUpdate & { ts: number }>();
    /** Path this device currently has open (what we advertise). */
    private myOpenFile: string | null = null;
    private presenceHeartbeat: number | null = null;
    private workspaceRefs: any[] = [];
    /** Throttle "X is editing this file" notices: `${device}:${file}` → ts. */
    private busyNoticeShown = new Map<string, number>();

    getState(): SyncState {
        return this.state;
    }

    // --- Debug accessors (used by the "Show debug info" panel) ----------------

    /** Hex root hash the client considers current for this vault.
     *  Prefers the engine's tracked `localRootHash` (seeded from the cached
     *  root file at startup + updated after every push/pull) over the WASM
     *  tree's in-memory hash, which is intentionally left empty until the
     *  first push bootstraps the tree from sync-base (see the comment in
     *  main.ts about `load_root` not populating child nodes). */
    getLocalRootHash(): string | null {
        if (this.localRootHash) return this.localRootHash;
        try {
            const h = this.tree.root_hash_hex();
            return h && h.length > 0 ? h : null;
        } catch {
            return null;
        }
    }

    /** Count of files currently tracked in sync-base. */
    getSyncBaseCount(): number {
        try { return this.syncBase.allPaths().length; } catch { return -1; }
    }

    /** Count of vault files Obsidian's cache reports right now (excluding .obsidian/). */
    getVaultFileCount(): number {
        try { return this.io.statBulk().size; } catch { return -1; }
    }

    /** Epoch-ms of the last successful push. 0 if never. */
    getLastSyncTimestamp(): number {
        try { return this.syncBase.lastSyncTimestamp; } catch { return 0; }
    }

    /** Most recent network/sync failure observed. */
    getLastError(): { ts: number; message: string; origin: string } | null {
        return this.lastError;
    }

    /** Last remote root hash observed via pullRemote (for hash mismatch diagnosis). */
    getLastObservedServerRoot(): string | null {
        return this.lastPullServerRoot;
    }

    /** The verified base root pushes descend from (honest putRoot parent). */
    getTreeBaseRoot(): string | null {
        return this.treeBaseRoot;
    }

    /** The WASM tree's actual current root — NOT the observed server root. */
    getTreeRootHash(): string | null {
        try {
            const h = this.tree.root_hash_hex();
            return h && h.length > 0 ? h : null;
        } catch {
            return null;
        }
    }

    /** File count inside the WASM tree (compare against sync-base count). */
    getTreeFileCount(): number {
        try { return this.tree.root_hash_hex() ? this.tree.total_files() : -1; } catch { return -1; }
    }

    isPushBlocked(): boolean {
        return this.pushBlocked;
    }

    isReenrollmentRequired(): boolean {
        return this.reenrollmentRequired;
    }

    isBulkChangeReviewRequired(): boolean {
        return this.bulkChangeReviewRequired;
    }

    /** Start the sync engine: run startup sequence, attach listeners, start timer. */
    async start(): Promise<void> {
        this.stopped = false;
        console.log("[obsetync] starting sync engine");

        // Restore the verified base root persisted in lockstep with sync-base.
        // Null on first run and on pre-1.4.0 sync-base files — established by
        // the first verified pull below.
        this.treeBaseRoot = this.syncBase.treeBaseRoot;
        if (this.treeBaseRoot) {
            console.log(`[obsetync] tree base root: ${this.treeBaseRoot.slice(0, 16)}`);
        }

        // Attach before the first network await. A first sync can run for
        // minutes on an old iPad; edits made during it must enter the journal
        // and the pull's live overwrite guard, not disappear in the startup
        // gap. Pull-generated adapter events are authenticated by pullEchoes.
        console.log("[obsetync] step 1: attach vault listeners");
        this.attachVaultListeners();

        // Start the public health probe for diagnostics without delaying the
        // guarded startup pull.
        const connectivity = this.api.ping();

        // Startup sequence (D-005):
        // 2. Pull remote changes.
        console.log("[obsetync] step 2: pull remote");
        await this.pullRemote();
        if (this.reenrollmentRequired || this.stopped) {
            console.warn("[obsetync] startup paused until this device is re-enrolled");
            return;
        }

        // 3. Recover from journal (Layer 2).
        console.log("[obsetync] step 3: recover from journal");
        await this.recoverFromJournal();
        if (this.reenrollmentRequired || this.stopped) return;

        // 4. Partial metadata scan (Layer 3).
        console.log("[obsetync] step 4: metadata scan");
        await this.partialMtimeScan();

        try {
            const conn = await connectivity;
            console.log(
                `[obsetync] ${conn.ok ? "✓ reachable" : "✗ unreachable"} at ${conn.serverUrl} | ${conn.transport}`
            );
        } catch (e) {
            console.warn("[obsetync] ✗ server unreachable:", e);
        }

        // 5. Start periodic pull timer. While the WS notify channel is live,
        // frames trigger pulls within seconds and the timer degrades to a
        // slow safety net (4× the interval); the moment the socket drops,
        // full-cadence polling resumes automatically.
        console.log("[obsetync] ready");
        this.syncTimer = window.setInterval(() => {
            const wsLive = this.wsChannel?.isConnected() ?? false;
            if (wsLive && Date.now() - this.lastPullDoneMs < this.syncInterval * 4 - 500) {
                return; // notify channel owns the fast path right now
            }
            this.pullRemote().catch((e) =>
                console.error("[obsetync] periodic pull error:", e)
            );
        }, this.syncInterval);

        // 6. Notify channel (Ph2) + presence (Ph3): "root changed" frames →
        // immediate pull; presence frames → fleet awareness map.
        if (this.realtimeWs) {
            this.wsChannel = new ObsetyncWsChannel(
                this.api,
                this.vaultId,
                () => {
                    this.pullRemote().catch((e) =>
                        console.error("[obsetync] ws-triggered pull error:", e)
                    );
                },
                (p) => this.handlePresence(p),
            );
            this.wsChannel.start();

            // Advertise which file we're looking at: on every active-leaf
            // change + a periodic refresh so the server-side TTL (90s)
            // doesn't expire us mid-edit.
            if (this.sharePresence) {
                this.workspaceRefs.push(
                    this.app.workspace.on("active-leaf-change", () => {
                        this.advertisePresence();
                    }),
                );
                this.presenceHeartbeat = window.setInterval(
                    () => this.advertisePresence(),
                    45_000,
                );
            }
        }
    }

    /** Send our current open file to the fleet (Ph3). */
    private advertisePresence(): void {
        if (!this.sharePresence || !this.wsChannel?.isConnected()) return;
        const file = this.app.workspace.getActiveFile()?.path ?? null;
        this.myOpenFile = file;
        this.wsChannel.sendPresence(file, file ? "active" : "idle");
    }

    /** Fold a fleet presence update into the map; nudge the user if someone
     *  else is actively in the file we currently have open. */
    private handlePresence(p: PresenceUpdate): void {
        if (p.state === "offline") {
            this.presence.delete(p.device);
        } else {
            this.presence.set(p.device, { ...p, ts: Date.now() });
        }

        if (
            p.state === "active" &&
            p.file &&
            this.myOpenFile &&
            p.file === this.myOpenFile
        ) {
            const key = `${p.device}:${p.file}`;
            const last = this.busyNoticeShown.get(key) ?? 0;
            if (Date.now() - last > 5 * 60_000) {
                this.busyNoticeShown.set(key, Date.now());
                new Notice(`Obsetync: ${p.name} is editing this file right now.`, 8000);
            }
        }
    }

    /** Live fleet presence (for the debug panel / status bar), stale-swept. */
    getPresence(): Array<PresenceUpdate & { ts: number }> {
        const now = Date.now();
        for (const [k, v] of this.presence) {
            if (now - v.ts > 120_000) this.presence.delete(k);
        }
        return [...this.presence.values()];
    }

    /** How many OTHER devices are active right now. */
    getActivePeerCount(): number {
        return this.getPresence().filter((p) => p.state === "active").length;
    }

    /** Stop the sync engine. */
    stop(): void {
        this.stopped = true;
        this.hashWorkerAbort.abort();
        if (this.syncTimer) {
            window.clearInterval(this.syncTimer);
            this.syncTimer = null;
        }
        this.wsChannel?.stop();
        this.wsChannel = null;
        this.api.closeDataLane();
        if (this.presenceHeartbeat !== null) {
            window.clearInterval(this.presenceHeartbeat);
            this.presenceHeartbeat = null;
        }
        for (const ref of this.workspaceRefs) {
            this.app.workspace.offref(ref);
        }
        this.workspaceRefs = [];
        for (const ref of this.eventRefs) {
            this.app.vault.offref(ref);
        }
        this.eventRefs = [];
        this.pullEchoes.clear();
    }

    /** WS notify-channel state for the debug panel / status box. */
    getWsState(): WsState {
        return this.wsChannel?.getState() ?? "off";
    }

    /** ms since the last WS frame, -1 when never/off. */
    getWsLastFrameAgeMs(): number {
        return this.wsChannel?.lastFrameAgeMs() ?? -1;
    }

    /** Force a full sync cycle (pull → reconcile content → push pending).
     *
     * reconcileContent() is the missing piece that used to let the server and
     * client silently drift apart: sync-base said "everything's uploaded" but
     * the server had no content. We now verify, on every Sync Now, that the
     * server actually holds the content sync-base claims, and re-upload
     * anything missing. Cheap when the server is fully populated (one
     * checkContent call with N hashes), correct when it isn't. */
    async forceSync(): Promise<void> {
        if (this.reenrollmentRequired || this.stopped) {
            new Notice("Obsetync: re-enroll this device before syncing.", 10000);
            return;
        }
        if (this.bulkChangeReviewRequired) {
            new Notice(
                "Obsetync: automatic publishing is paused. Review ignores and run Full Rescan.",
                10000,
            );
            return;
        }
        // Another cycle already holds the engine (e.g. the startup
        // first-sync). Every sub-step below would silently yield to it and
        // forceSync would finish in ~1ms — reporting "complete" for work it
        // never did. Say what's actually happening instead.
        if (this.syncing) {
            console.log(
                `[obsetync] forceSync skipped: another sync in progress (state=${this.state})`
            );
            new Notice("Obsetync: sync already in progress — hang tight.");
            return;
        }
        const t0 = Date.now();
        console.log(
            `[obsetync] forceSync start: pending=${this.pendingChanges.size} ` +
            `localRoot=${this.localRootHash?.slice(0, 16) ?? "(none)"}`
        );
        await this.pullRemote();
        const t1 = Date.now();
        console.log(
            `[obsetync] forceSync: pull done in ${t1 - t0}ms, ` +
            `localRoot=${this.localRootHash?.slice(0, 16) ?? "(none)"}, ` +
            `pending=${this.pendingChanges.size}`
        );
        try {
            await this.reconcileContent();
        } catch (e: any) {
            console.error("[obsetync] reconcile error:", e);
            this.lastError = {
                ts: Date.now(),
                origin: "reconcile",
                message: String(e?.message ?? e),
            };
        }
        const t2 = Date.now();
        console.log(
            `[obsetync] forceSync: reconcile done in ${t2 - t1}ms, ` +
            `pending=${this.pendingChanges.size}`
        );
        await this.pushPending();
        console.log(
            `[obsetync] forceSync end in ${Date.now() - t0}ms, ` +
            `pending=${this.pendingChanges.size}, ` +
            `localRoot=${this.localRootHash?.slice(0, 16) ?? "(none)"}`
        );
    }

    /**
     * Verify every file recorded in sync-base is actually present on the
     * server, upload whatever is missing. This exists because `sync-base` is
     * just a local cache of "what we believe the server has" — and the cache
     * can lie (server wiped, user restored from backup, migrated from TLS
     * server, etc.). Running it costs one `checkContent` for small files +
     * one `checkContentChunks` per large file the server already knows about,
     * plus real uploads for anything truly missing. O(1) network when the
     * server is fully populated; O(missing) otherwise.
     */
    async reconcileContent(onProgress?: (msg: string) => void): Promise<{
        smallUploaded: number;
        largeUploaded: number;
        treeChunksUploaded: number;
        bytes: number;
    }> {
        const progress = onProgress ?? ((m: string) => this.onStatusUpdate(m));

        // Guard against a concurrent push racing this — debouncedPush() fires
        // from live vault events and would otherwise share our WASM tree
        // handle while we bootstrap + inspect it.
        if (this.syncing) {
            console.log("[obsetync] reconcile skipped — another sync in progress");
            return { smallUploaded: 0, largeUploaded: 0, treeChunksUploaded: 0, bytes: 0 };
        }
        this.syncing = true;
        const perf = perfTrace.begin("reconcile");
        let perfOutcome: PerfOutcome = "success";
        const endSpan = perfSpan("sync.reconcile");
        const operationId = await this.operationCheckpoint?.begin(
            "reconcile",
            `${this.syncBase.entryCount()} tracked files`,
        );
        try {
            return await this._reconcileInner((message) => {
                progress(message);
                if (operationId) this.operationCheckpoint?.progress(operationId, message);
            }, perf);
        } catch (error) {
            perfOutcome = this.stopped ? "cancelled" : "error";
            throw error;
        } finally {
            try {
                if (operationId) await this.operationCheckpoint?.complete(operationId);
            } finally {
                perf.finish(perfOutcome);
                endSpan();
                this.syncing = false;
            }
        }
    }

    private async _reconcileInner(
        progress: (msg: string) => void,
        perf?: PerfOperation,
    ): Promise<{
        smallUploaded: number;
        largeUploaded: number;
        treeChunksUploaded: number;
        bytes: number;
    }> {
        // Populate the WASM tree from sync-base so wasm_tree_chunk_hashes
        // reflects the actual index-chunk set the server should have. Same
        // bootstrap push.ts does on first call.
        const endEnumerate = perf?.phase("enumerate");
        if (!this.tree.root_hash_hex()) {
            const paths = this.syncBase.allPaths();
            if (paths.length > 0) {
                const entries = paths.map(p => {
                    const e = this.syncBase.getEntry(p)!;
                    return {
                        path: p,
                        hash: e.hash,
                        mtime_ms: this.syncBase.getTreeMtime(p) ?? e.mtime,
                        size: e.size,
                    };
                });
                this.tree.build_from_entries(JSON.stringify(entries));
            }
        }

        // --- Partition sync-base: small files (whole blobs) vs large (manifests+chunks).
        const contentIndex = buildReconcileContentIndex(
            this.syncBase.allPaths().map((path) => {
                const entry = this.syncBase.getEntry(path)!;
                return { path, hash: entry.hash, size: entry.size };
            }),
            LARGE_FILE_THRESHOLD,
        );
        const { smallByHash, largeByHash } = contentIndex;
        perf?.setWorkload({
            filesTotal: contentIndex.filesTotal,
            bytesTotal: contentIndex.bytesTotal,
        });
        endEnumerate?.();

        const CHECK_BATCH = 1000;

        // --- Step 1: which tree chunks (index) is the server missing?
        const treeHashes = this.wasm.wasm_tree_chunk_hashes(this.tree);
        perf?.setWasmChunks({ reachable: treeHashes.length });
        const endCheck = perf?.phase("check");
        const missingTreeChunks = treeHashes.length > 0
            ? await this.api.checkChunks(treeHashes, perf)
            : [];

        // --- Step 2: which small-file contents is the server missing?
        const smallHashes = [...smallByHash.keys()];
        const missingSmall: string[] = [];
        for (let i = 0; i < smallHashes.length; i += CHECK_BATCH) {
            const batch = smallHashes.slice(i, i + CHECK_BATCH);
            const missing = await this.api.checkContent(batch, perf);
            missingSmall.push(...missing);
            progress(`reconcile: checked ${Math.min(i + CHECK_BATCH, smallHashes.length)}/${smallHashes.length}`);
        }

        // --- Step 3: which large-file manifests is the server missing?
        //
        // Before, we read + re-chunked + re-manifested every large file
        // unconditionally on every Sync Now. For a vault with big PDFs that
        // meant minutes of pointless disk reads and CPU — the "continuously
        // reuploading large files" symptom. The new bulk check lets us skip
        // straight past large files whose manifest is already on the server.
        const largeHashes = [...largeByHash.keys()];
        const missingLargeManifests: string[] = [];
        for (let i = 0; i < largeHashes.length; i += CHECK_BATCH) {
            const batch = largeHashes.slice(i, i + CHECK_BATCH);
            const missing = await this.api.checkManifests(batch, perf);
            missingLargeManifests.push(...missing);
        }
        endCheck?.();

        const totalMissing =
            missingTreeChunks.length + missingSmall.length + missingLargeManifests.length;
        perf?.setWorkload({
            filesNeeded: missingSmall.length + missingLargeManifests.length,
        });

        console.log(
            `[obsetync] reconcile plan: ` +
            `tree-chunks ${treeHashes.length} checked / ${missingTreeChunks.length} missing, ` +
            `small ${smallHashes.length} checked / ${missingSmall.length} missing, ` +
            `large ${largeHashes.length} checked / ${missingLargeManifests.length} missing`
        );

        if (totalMissing === 0) {
            perf?.setWorkload({ bytesNeeded: 0 });
            progress("reconcile: server in parity");
            return { smallUploaded: 0, largeUploaded: 0, treeChunksUploaded: 0, bytes: 0 };
        }

        const notice = totalMissing >= 20
            ? new Notice(
                `Reconcile: uploading ${missingSmall.length} small + ` +
                `${missingLargeManifests.length} large + ` +
                `${missingTreeChunks.length} tree chunks...`,
                0,
            )
            : null;

        let smallUploaded = 0;
        let largeUploaded = 0;
        let treeChunksUploaded = 0;
        let bytes = 0;
        let contentBytesNeeded = sumIndexedContentBytes(missingSmall, smallByHash);

        // --- Step 3: upload missing tree (index) chunks.
        const endTreeUpload = perf?.phase("tree_index_upload");
        const treeRecords: BulkUploadRecord[] = [];
        for (const hash of missingTreeChunks) {
            const chunkBytes = this.wasm.wasm_tree_get_chunk(this.tree, hash);
            if (chunkBytes) {
                treeRecords.push({
                    kind: BulkObjectKind.IndexChunk,
                    hash,
                    data: chunkBytes,
                });
                bytes += chunkBytes.length;
            }
        }
        await this.api.putObjects(treeRecords, perf);
        treeChunksUploaded += treeRecords.length;
        perf?.increment({
            bytesTransferred: treeRecords.reduce((sum, record) => sum + record.data.length, 0),
        });
        endTreeUpload?.();

        // --- Step 4: read/hash with bounded concurrency, then upload every
        // valid object in byte/count-bounded packs.
        const reconcileTuning = getHashTuning();
        const smallBatches = planByteBoundedBatches(
            missingSmall,
            (hash) => smallByHash.get(hash)?.size ?? 0,
            {
                maxFiles: reconcileTuning.maxBatchFiles,
                maxBytes: reconcileTuning.maxBatchBytes,
                maxSingleBytes: reconcileTuning.maxSingleBatchFileBytes,
                maxHoldMs: reconcileTuning.maxBatchHoldMs,
            },
        );
        let smallChecked = 0;
        for (const batch of smallBatches) {
            const records: BulkUploadRecord[] = [];
            for (let i = 0; i < batch.length; i += reconcileTuning.readConcurrency) {
                const group = batch.slice(i, i + reconcileTuning.readConcurrency);
                const prepared = await Promise.all(group.map(async (hash) => {
                    const source = smallByHash.get(hash);
                    if (!source) return null;
                    const { path } = source;
                    try {
                        const endRead = perf?.phase("read");
                        let data: Uint8Array;
                        try {
                            data = await this.io.readFile(path);
                        } finally {
                            endRead?.();
                        }
                        // Drift never uploads bytes under the sync-base hash.
                        const endHash = perf?.phase("hash");
                        let actual: string;
                        try {
                            actual = streamingHash(this.wasm, data);
                        } finally {
                            endHash?.();
                        }
                        if (actual !== hash) return null;
                        return {
                            kind: BulkObjectKind.Content,
                            hash,
                            data,
                        } satisfies BulkUploadRecord;
                    } catch (error) {
                        console.warn(`[obsetync] reconcile skipped ${path}:`, error);
                        return null;
                    }
                }));
                for (const record of prepared) {
                    if (record) records.push(record as BulkUploadRecord);
                }
            }
            const residentBytes = records.reduce((sum, record) => sum + record.data.length, 0);
            perf?.observePeakBatchBytes(residentBytes);
            if (records.length > 0) {
                try {
                    const endUpload = perf?.phase("upload");
                    try {
                        await this.api.putObjects(records, perf);
                    } finally {
                        endUpload?.();
                    }
                    smallUploaded += records.length;
                    bytes += residentBytes;
                    perf?.increment({
                        filesCompleted: records.length,
                        bytesTransferred: residentBytes,
                    });
                } catch (error) {
                    console.warn(
                        `[obsetync] reconcile skipped ${records.length} packed small object(s):`,
                        error,
                    );
                }
            }
            smallChecked += batch.length;
            const done = smallChecked;
            const msg = `reconcile: ${done}/${missingSmall.length} files · ${formatBytes(bytes)}`;
            progress(msg);
            notice?.setMessage(`Re-uploading: ${done}/${missingSmall.length} · ${formatBytes(bytes)}`);
            await yieldToUI();
        }

        // --- Step 5: large files — only those whose manifest is actually
        // missing on the server. Steady-state syncs hit zero of these and
        // the whole step is a no-op. When the server was wiped, we read +
        // re-chunk + upload only the missing ones.
        let largeIdx = 0;
        for (const hash of missingLargeManifests) {
            const source = largeByHash.get(hash);
            if (!source) continue;
            const { path } = source;
            largeIdx++;
            progress(`reconcile: large file ${largeIdx}/${missingLargeManifests.length}`);
            notice?.setMessage(`Re-uploading large file ${largeIdx}/${missingLargeManifests.length}`);
            try {
                const absolutePath = this.io.getAbsolutePath(path);
                const currentStat = absolutePath ? await this.io.stat(path) : null;
                if (
                    this.hashWorkers && absolutePath &&
                    (!currentStat || currentStat.size !== source.size)
                ) {
                    await yieldToUI();
                    continue;
                }
                if (
                    this.hashWorkers && absolutePath && currentStat &&
                    currentStat.size === source.size
                ) {
                    try {
                        const ranged = await reconcileDesktopLargeFile(
                            this.hashWorkers,
                            {
                                absolutePath,
                                expectedHash: hash,
                                expectedSize: currentStat.size,
                                expectedMtime: currentStat.mtime,
                                feedBytes: getHashTuning().feedBytes,
                            },
                            async (hashes) => {
                                const endChunkCheck = perf?.phase("check");
                                try {
                                    return await this.api.checkContentChunks([...hashes], perf);
                                } finally {
                                    endChunkCheck?.();
                                }
                            },
                            async (records) => {
                                const endUpload = perf?.phase("upload");
                                try {
                                    await this.api.putObjects(records, perf);
                                } finally {
                                    endUpload?.();
                                }
                            },
                            this.hashWorkerAbort.signal,
                        );
                        perf?.addPhase("read", ranged.workerReadMs);
                        perf?.addPhase("fastcdc", ranged.workerHashMs);
                        if (ranged.status === "drifted") {
                            await yieldToUI();
                            continue;
                        }
                        perf?.addPhase("read", ranged.rangeReadMs);
                        perf?.observePeakBatchBytes(ranged.peakBufferedBytes);
                        contentBytesNeeded += ranged.neededBytes;
                        bytes += ranged.uploadedBytes;
                        largeUploaded++;
                        perf?.increment({
                            filesCompleted: 1,
                            bytesTransferred: ranged.uploadedBytes,
                        });
                        await yieldToUI();
                        continue;
                    } catch (error) {
                        if (
                            error instanceof HashWorkerFileDriftError ||
                            (error instanceof HashWorkerPoolError && error.code === "CLOSED") ||
                            (error as Error)?.name === "AbortError"
                        ) {
                            throw error;
                        }
                        if (error instanceof HashWorkerPoolError) {
                            if (!this.hashWorkerFallbackWarned) {
                                this.hashWorkerFallbackWarned = true;
                                console.warn(
                                    "[obsetync] hash worker unavailable during reconcile; " +
                                    "using renderer fallback:",
                                    error,
                                );
                            }
                        } else {
                            throw error;
                        }
                    }
                }

                // Mobile or worker-unavailable fallback. DataAdapter has no
                // ranged-read API, so this remains one explicitly bounded
                // whole-file operation per large file.
                const endRead = perf?.phase("read");
                const data = await this.io.readFile(path);
                endRead?.();
                perf?.observePeakBatchBytes(data.length);
                const endChunk = perf?.phase("fastcdc");
                const info = await chunkFileStreaming(this.wasm, data);
                endChunk?.();
                if (info.file_hash !== hash) continue; // drifted — scan will pick up
                const chunkHashes = (info.chunks as any[]).map(c => c.hash);
                const endChunkCheck = perf?.phase("check");
                const missingChunks = chunkHashes.length > 0
                    ? await this.api.checkContentChunks(chunkHashes, perf)
                    : [];
                endChunkCheck?.();
                const missingSet = new Set(missingChunks);
                const accountedChunks = new Set<string>();
                for (const chunk of info.chunks as any[]) {
                    if (missingSet.has(chunk.hash) && !accountedChunks.has(chunk.hash)) {
                        accountedChunks.add(chunk.hash);
                        contentBytesNeeded += chunk.size;
                    }
                }
                let fileBytesUploaded = 0;
                const records: BulkUploadRecord[] = [];
                const queuedChunks = new Set<string>();
                for (const c of info.chunks as any[]) {
                    if (missingSet.has(c.hash) && !queuedChunks.has(c.hash)) {
                        queuedChunks.add(c.hash);
                        const chunkData = data.subarray(c.offset, c.offset + c.size);
                        records.push({
                            kind: BulkObjectKind.ContentChunk,
                            hash: c.hash,
                            data: chunkData,
                        });
                        fileBytesUploaded += chunkData.length;
                    }
                }
                records.push({
                    kind: BulkObjectKind.Manifest,
                    hash,
                    data: new TextEncoder().encode(JSON.stringify({
                        file_hash: hash,
                        total_size: info.total_size,
                        chunks: info.chunks,
                    })),
                });
                const endUpload = perf?.phase("upload");
                try {
                    await this.api.putObjects(records, perf);
                } finally {
                    endUpload?.();
                }
                bytes += fileBytesUploaded;
                largeUploaded++;
                perf?.increment({
                    filesCompleted: 1,
                    bytesTransferred: fileBytesUploaded,
                });
            } catch (e) {
                console.warn(`[obsetync] reconcile skipped large ${path}:`, e);
            }
            await yieldToUI();
        }

        notice?.hide();
        const summary =
            `reconcile done: ${smallUploaded} small, ${largeUploaded} large, ` +
            `${treeChunksUploaded} tree chunks, ${formatBytes(bytes)}`;
        console.log(`[obsetync] ${summary}`);
        progress(summary);

        // Deduplication is a content metric. Merkle index chunks and manifests
        // are transport overhead and intentionally stay out of bytesNeeded.
        perf?.setWorkload({ bytesNeeded: contentBytesNeeded });

        return { smallUploaded, largeUploaded, treeChunksUploaded, bytes };
    }

    /** Force a full vault scan (Layer 4). Doubles as the recovery action for
     *  a diverged tree: the in-memory tree is rebuilt from sync-base (the
     *  state corresponding to treeBaseRoot) before scanning, so whatever
     *  in-memory drift caused a push block is discarded, the block lifted,
     *  and local differences re-queued from disk truth. */
    async fullScan(): Promise<void> {
        if (this.reenrollmentRequired || this.stopped) {
            new Notice("Obsetync: re-enroll this device before running a rescan.", 10000);
            return;
        }
        if (this.syncing) {
            console.log(`[obsetync] full scan skipped: another sync is in progress (state=${this.state})`);
            new Notice("Obsetync: sync already in progress — retry the rescan when it finishes.");
            return;
        }
        // Full Rescan is the user's explicit confirmation after a bulk-change
        // safety stop. Restore the stop if the confirming scan itself fails.
        const bulkReviewWasRequired = this.bulkChangeReviewRequired;
        this.bulkChangeReviewRequired = false;
        // Own the tree for the complete scan. WS pulls and debounced pushes
        // must not mutate the same WASM handle between hash batches.
        this.syncing = true;
        this.state = "scanning";
        this.onStatusUpdate("sync ⟳");
        const perf = perfTrace.begin("scan");
        let perfOutcome: PerfOutcome = "success";
        const notice = new Notice("Scanning vault...", 0);
        console.log("[obsetync] full scan started");
        const endSpan = perfSpan("scan.full");
        const operationId = await this.operationCheckpoint?.begin(
            "full-scan",
            `${this.getVaultFileCount()} cached vault files`,
        );
        let scanFailed = false;

        try {
            if (this.syncBase.clearDiffPageCheckpoint()) {
                // An operator-requested rebuild deliberately abandons any
                // interrupted snapshot cursor before deriving disk truth.
                await this.syncBase.checkpoint();
            }
            const endTree = perf.phase("tree_update");
            const entries = this.syncBase.allPaths().map((p) => {
                const e = this.syncBase.getEntry(p)!;
                return {
                    path: p,
                    hash: e.hash,
                    mtime_ms: this.syncBase.getTreeMtime(p) ?? e.mtime,
                    size: e.size,
                };
            });
            this.tree.build_from_entries(JSON.stringify(entries));
            endTree();
            if (this.pushBlocked) {
                console.log("[obsetync] full scan rebuilt the tree — push unblocked");
                this.pushBlocked = false;
            }

            // statBulk() reads all file stats from Obsidian's in-memory cache —
            // no async IPC calls, O(n) in-memory map construction.
            const endStat = perf.phase("stat");
            const statMap = this.io.statBulk();
            // Optionally include .obsidian/ — vault.getFiles() hides it by design.
            if (this.syncObsidianConfig) {
                const obsidianFiles = await this.io.listObsidianConfig();
                for (const [p, s] of obsidianFiles) statMap.set(p, s);
            }
            endStat();
            console.log(`[obsetync] full scan: ${statMap.size} files total`);

            // Phase 1: fast mtime+size filter (synchronous, no I/O).
            const endEnumerate = perf.phase("enumerate");
            const toHash: Array<{ path: string; stat: { mtime: number; size: number } }> = [];
            let visibleFiles = 0;
            let visibleBytes = 0;
            for (const [path, stat] of statMap) {
                if (this.isExcluded(path)) continue;
                visibleFiles++;
                visibleBytes += stat.size;
                const base = this.syncBase.getEntry(path);
                if (base && stat.mtime === base.mtime && stat.size === base.size) continue;
                toHash.push({ path, stat });
            }
            endEnumerate();
            perf.setWorkload({ filesTotal: visibleFiles, bytesTotal: visibleBytes });
            console.log(`[obsetync] full scan: ${toHash.length} files need hashing`);
            const hashableTotal = toHash.reduce(
                (count, item) => count + (item.stat.size < LARGE_FILE_THRESHOLD ? 1 : 0),
                0,
            );
            let hashOrdinal = 0;
            const hashSampleWeights = toHash.map((item) => {
                if (item.stat.size >= LARGE_FILE_THRESHOLD) return 0;
                return perfSampleWeight(hashOrdinal++, hashableTotal);
            });

            // Phase 2: read + hash in streaming batches. Only compact path
            // records accumulate; file bytes are released after each group.
            //
            // Key constraints:
            //   - Large files (≥ 1 MB) skip WASM hash entirely here. push.ts reads
            //     them during upload and gets file_hash from FastCDC. This keeps
            //     WASM linear memory bounded regardless of PDF/image sizes.
            //   - Platform read concurrency limits concurrent IPC reads. Large
            //     files are excluded above; mobile stays more conservative.
            //   - FLUSH_BATCH=500 periodically moves local arrays into the
            //     coalescing DirtyPathSet without touching the network/tree.
            //   - yieldToUI() every group lets Electron's audio/render callbacks run.
            const READ_CONCURRENCY = getHashTuning().readConcurrency;
            const FLUSH_BATCH = 500;
            let pending: FileChange[] = [];
            let totalChanges = 0;
            let changedBytes = 0;

            const flushPending = async () => {
                if (pending.length === 0) return;
                this.pendingChanges.addMany(pending);
                pending = [];
            };

            for (let i = 0; i < toHash.length; i += READ_CONCURRENCY) {
                const batch = toHash.slice(i, i + READ_CONCURRENCY);
                perf.observePeakBatchBytes(batch.reduce((sum, item) => {
                    const residentBytes = this.io.getAbsolutePath(item.path)
                        ? Math.min(item.stat.size, getHashTuning().feedBytes)
                        : item.stat.size;
                    return sum + residentBytes;
                }, 0));
                const results = await Promise.all(
                    batch.map(async ({ path, stat }, batchIndex) => {
                        const base = this.syncBase.getEntry(path);
                        if (stat.size >= LARGE_FILE_THRESHOLD) {
                            // Skip WASM hash — push.ts will hash during upload.
                            // We know it changed because it passed the mtime+size filter.
                            return { path, stat, hash: undefined as string | undefined, base };
                        }
                        const itemIndex = i + batchIndex;
                        const sampleWeight = hashSampleWeights[itemIndex];
                        const stable = await this.hashStableFile(
                            path,
                            stat,
                            sampleWeight > 0 ? perf : undefined,
                            sampleWeight,
                        );
                        if (base && stable.hash === base.hash) return null; // unchanged
                        return { path, stat: stable.stat, hash: stable.hash, base };
                    })
                );

                for (const r of results) {
                    if (!r) continue;
                    const change: FileChange = {
                        action: r.base ? "modified" : "created",
                        path: r.path,
                        mtime: r.stat.mtime,
                        size: r.stat.size,
                    };
                    if (r.hash !== undefined) change.hash = r.hash;
                    pending.push(change);
                    totalChanges++;
                    changedBytes += r.stat.size;
                }

                // Let Electron's audio/render callbacks run between every read group.
                await yieldToUI();

                // Tick every batch — the slow phase here is the HASHING, and
                // the old placement (inside the flush guard) meant vaults with
                // <500 changes showed "Scanning vault..." frozen to the end.
                const done = Math.min(i + READ_CONCURRENCY, toHash.length);
                notice.setMessage(
                    `Obsetync: scanning ${done}/${toHash.length} · ${totalChanges} changed`,
                );
                this.onStatusUpdate(`⟳ ${done}/${toHash.length}`);
                this.progressHeartbeat("fullScan", `${done}/${toHash.length} hashed, ${totalChanges} changed`);
                if (operationId) {
                    this.operationCheckpoint?.progress(
                        operationId,
                        `${done}/${toHash.length} hashed; ${totalChanges} changed`,
                    );
                }

                if (pending.length >= FLUSH_BATCH) {
                    await flushPending();
                }
            }
            await flushPending();

            // Phase 3: deletions — files in sync-base that no longer exist.
            let deletedCount = 0;
            for (const path of this.syncBase.allPaths()) {
                if (!statMap.has(path) && !this.isExcluded(path)) {
                    pending.push({ action: "deleted", path });
                    totalChanges++;
                    deletedCount++;
                }
            }
            await flushPending();

            perf.setWorkload({
                filesTotal: visibleFiles + deletedCount,
                filesNeeded: totalChanges,
                bytesNeeded: changedBytes,
            });
            perf.increment({ filesCompleted: visibleFiles + deletedCount });

            console.log(`[obsetync] full scan complete: ${totalChanges} changes`);
        } catch (error: any) {
            scanFailed = true;
            perfOutcome = this.stopped ? "cancelled" : "error";
            if (bulkReviewWasRequired) this.bulkChangeReviewRequired = true;
            this.lastError = {
                ts: Date.now(),
                origin: "full-scan",
                message: String(error?.message ?? error),
            };
            console.error("[obsetync] full scan failed:", error);
            throw error;
        } finally {
            try {
                if (operationId) await this.operationCheckpoint?.complete(operationId);
            } finally {
                perf.finish(perfOutcome);
                endSpan();
                notice.hide();
                this.syncing = false;
                this.state = scanFailed ? "error" : "idle";
                this.onStatusUpdate(scanFailed ? "sync ✗" : "sync ✓");
            }
        }

        // Publish once the scan has released exclusive ownership. push()
        // itself streams these path records in bounded file batches.
        if (this.pendingChanges.size > 0) await this.pushPending(true);
    }

    // --- Private ---

    private async pullRemote(): Promise<void> {
        if (this.syncing || this.reenrollmentRequired || this.stopped) return;
        this.syncing = true;
        this.state = "pulling";
        this.onStatusUpdate("sync ↓");
        const perf = perfTrace.begin("pull");
        let perfOutcome: PerfOutcome = "success";
        const endSpan = perfSpan("sync.pull");
        const operationId = await this.operationCheckpoint?.begin(
            "pull",
            `base=${(this.treeBaseRoot ?? this.localRootHash)?.slice(0, 16) ?? "none"}`,
        );

        // Live progress: every tick lands in the status bar; a persistent
        // Notice appears only once REAL work is detected (first-sync or a
        // non-trivial delta), so idle 30s polls stay silent. A 31-minute
        // first-sync once ran with zero feedback because this callback was
        // simply never passed — pull's per-batch ticks all landed in void.
        // (ref-object because TS control-flow can't see the closure assign)
        const noticeRef: { n: Notice | null } = { n: null };
        let pullFailed = false;
        const progress = (msg: string) => {
            if (this.stopped) throw new Error("sync engine stopped");
            this.onStatusUpdate(`↓ ${msg}`);
            const isRealWork = /files applied|changes to apply|first sync/.test(msg);
            if (noticeRef.n) {
                noticeRef.n.setMessage(`Obsetync ↓ ${msg}`);
            } else if (isRealWork) {
                noticeRef.n = new Notice(`Obsetync ↓ ${msg}`, 0);
            }
            this.progressHeartbeat("pull", msg);
            if (operationId) this.operationCheckpoint?.progress(operationId, msg);
        };

        try {
            const result = await pull(
                this.api,
                this.io,
                this.syncBase,
                this.vaultId,
                // Diff from the VERIFIED base when we have one — never from a
                // merely-observed root that may be ahead of our applied state.
                this.treeBaseRoot ?? this.localRootHash,
                this.wasm,
                this.tree,
                progress,
                // Vault events for paths the pull itself writes are echoes,
                // not user edits — register them before apply starts.
                (writes) => this.pullEchoes.register(writes),
                // Editor safety: paths with UNSYNCED local edits keep their
                // disk bytes — the queued push + server merge reconcile them.
                // Without this, the startup order (pull → journal recovery)
                // could overwrite last session's edits before recovery reads
                // them.
                this.unsyncedLocalPaths(),
                // Slice 2: never fetch ignored paths; untrack them if purged.
                (p) => this.isExcluded(p),
                perf,
            );
            if (result.newRootBytes && this.wasm && this.tree) {
                const alignment = alignTreeFormatFromRoot(
                    this.wasm,
                    this.tree,
                    this.syncBase,
                    result.newRootBytes,
                );
                this.api.observeTreeVersion(this.vaultId, alignment.version);
                if (alignment.changed) {
                    result.treeParity = result.newRootHash
                        ? this.getTreeRootHash() === result.newRootHash
                        : null;
                    console.warn(
                        `[obsetync] rebuilt local Merkle graph as Tree v${alignment.version} ` +
                        "after authenticated server format transition",
                    );
                }
            }
            if (result.newRootHash) {
                this.localRootHash = result.newRootHash;
                this.lastPullServerRoot = result.newRootHash;
            }
            await this.adoptPullResult(result);
        } catch (e: any) {
            pullFailed = true;
            perfOutcome = this.stopped ? "cancelled" : "error";
            console.error("[obsetync] pull error:", e);
            if (!this.stopped) this.recordSyncFailure("pull", e);
        } finally {
            try {
                if (operationId) await this.operationCheckpoint?.complete(operationId);
            } finally {
                perf.finish(perfOutcome);
                endSpan();
                noticeRef.n?.hide();
                this.syncing = false;
            }
            if (!pullFailed && !this.stopped) {
                this.state = "idle";
                this.onStatusUpdate(
                    this.bulkChangeReviewRequired ? "sync ⚠ review" : "sync ✓",
                );
                this.lastPullDoneMs = Date.now();
            }
            // Drain user edits that arrived while we were syncing.
            if (
                !this.reenrollmentRequired &&
                !this.stopped &&
                !this.bulkChangeReviewRequired &&
                this.pendingChanges.size > 0
            ) {
                this.debouncedPush?.();
            }
        }
    }

    /** Every path whose newest bytes exist only locally: queued-but-unpushed
     *  changes plus unsynced journal entries. Pull must not overwrite these. */
    private unsyncedLocalPaths(): { has(path: string): boolean } {
        const durableAtPullStart = new Set<string>();
        try {
            for (const entry of this.journal.unsynced()) {
                durableAtPullStart.add(entry.path);
                if (entry.oldPath) durableAtPullStart.add(entry.oldPath);
            }
        } catch {
            // The live pending/in-flight sets still protect current-session edits.
        }
        return {
            has: (path: string) =>
                durableAtPullStart.has(path) ||
                this.pendingChanges.has(path) ||
                this.localEventsInFlight.has(path),
        };
    }

    /** Rate-limited progress line into the console/debug ring buffer, so
     *  long operations leave a visible trail in the debug panel even when
     *  nobody is watching the status bar (at most one line per 5s). */
    private lastHeartbeatMs = 0;
    private progressHeartbeat(op: string, msg: string): void {
        const now = Date.now();
        if (now - this.lastHeartbeatMs >= 5000) {
            this.lastHeartbeatMs = now;
            console.log(`[obsetync] ${op} progress: ${msg}`);
        }
    }

    private recordSyncFailure(origin: string, error: unknown): void {
        const message = error instanceof Error ? error.message : String(error);
        this.lastError = { ts: Date.now(), origin, message };
        this.state = "error";

        if (isReenrollmentRequiredError(error)) {
            this.reenrollmentRequired = true;
            this.onStatusUpdate("sync ⚠ re-enroll");
            if (!this.reenrollmentNoticeShown) {
                this.reenrollmentNoticeShown = true;
                new Notice(
                    "Obsetync: this server requires fresh enrollment. " +
                    "Automatic sync is paused; reset enrollment and enter a new code.",
                    15000,
                );
            }
            return;
        }

        this.onStatusUpdate("sync ✗");
    }

    private requireBulkChangeReview(
        source: string,
        total: number,
        trackedDeletions: number,
    ): void {
        this.bulkChangeReviewRequired = true;
        this.onStatusUpdate("sync ⚠ review");
        console.warn(
            `[obsetync] automatic ${source} publish paused: ${total} changes, ` +
            `${trackedDeletions} tracked deletions; review ignores and run Full Rescan`,
        );
        const deletionSummary = trackedDeletions > 0
            ? `, including ${trackedDeletions.toLocaleString()} tracked deletions`
            : "";
        new Notice(
            `Obsetync paused ${total.toLocaleString()} automatic changes${deletionSummary}. ` +
            "Review ignore patterns, then run Full Rescan to confirm.",
            15000,
        );
    }

    /**
     * Decide what the pull result means for the verified base (D2/D3 core).
     *
     * - Exact parity (tree root == server root): adopt as treeBaseRoot,
     *   persist root bytes, clear any block. The only unconditional advance.
     * - Parity failed after applying deltas that carried server mtimes: the
     *   tree diverged from what we just applied — BLOCK pushes until a full
     *   rescan; publishing from this tree could revert other devices.
     * - Parity failed against a pre-1.4.0 server (no mtimes on the wire):
     *   exactness is unreachable (leaf hashes cover mtime). Adopt only when
     *   tree and sync-base agree on file count; otherwise block.
     * - Nothing applied + no parity: keep the current base. Merging from an
     *   older base is always safe — advancing past unapplied content is not
     *   (that's how sync state used to outrun reality).
     */
    private async adoptPullResult(result: {
        newRootHash: string | null;
        newRootBytes: Uint8Array | null;
        applied: number;
        treeParity: boolean | null;
        deltasHadMtime: boolean;
        deferredCount: number;
        localDeferredCount: number;
        downloaded: number;
    }): Promise<void> {
        // Files this pull couldn't fetch leave the tree missing content the
        // server has. Adopting the server root as our base now would let a
        // later fast-forward read those gaps as deletions and propagate them
        // (the 2026-07-13 failure mode). Hold the base where it is — merges
        // from an older base always preserve the other side's changes — and
        // let the deferred files retry on the next pull.
        if (result.deferredCount > 0) {
            const fetchDeferred = result.deferredCount - result.localDeferredCount;
            console.warn(
                `[obsetync] pull deferred ${result.deferredCount} file(s) ` +
                `(${result.localDeferredCount} locally edited, ${fetchDeferred} unfetched) — ` +
                `base root NOT advanced`,
            );
            return;
        }

        const adopt = async (hash: string) => {
            const clearedDiffCheckpoint = this.syncBase.clearDiffPageCheckpoint();
            let baseChanged = false;
            if (this.treeBaseRoot !== hash) {
                this.treeBaseRoot = hash;
                this.syncBase.setTreeBaseRoot(hash);
                baseChanged = true;
            }
            if (baseChanged || clearedDiffCheckpoint) {
                await this.syncBase.save();
            }
            if (this.pushBlocked) {
                console.log("[obsetync] tree re-verified against server — push unblocked");
                this.pushBlocked = false;
            }
            if (result.newRootBytes) {
                try {
                    const path = ".obsidian/plugins/obsetync/cached-root.bin";
                    await this.app.vault.adapter.writeBinary(
                        path,
                        exactArrayBuffer(result.newRootBytes),
                    );
                } catch (e) {
                    console.warn("[obsetync] failed to save cached root after pull:", e);
                }
            }
        };

        if (result.treeParity === true && result.newRootHash) {
            await adopt(result.newRootHash);
            return;
        }

        if (result.treeParity === false && result.newRootHash) {
            const treeCount = this.getTreeFileCount();
            const baseCount = this.syncBase.entryCount();
            const countsAgree = treeCount >= 0 && treeCount === baseCount;

            // A tree that still doesn't match the server AFTER the pull
            // actually DOWNLOADED content is a genuine content divergence —
            // block. But when the pull downloaded NOTHING (every delta verified
            // against local disk), the content is provably identical and the
            // only difference is metadata — mtime, which leaf hashes cover.
            // That's benign: fall through to the count-agreement check and
            // adopt, instead of nagging "Force full rescan" on a false alarm
            // that no rescan can fix (the mtimes just drift again).
            if (result.applied > 0 && result.deltasHadMtime && result.downloaded > 0) {
                if (this.syncBase.clearDiffPageCheckpoint()) {
                    await this.syncBase.checkpoint();
                }
                this.pushBlocked = true;
                console.error(
                    `[obsetync] tree root ${this.getTreeRootHash()?.slice(0, 16)} != ` +
                    `server root ${result.newRootHash.slice(0, 16)} after verified rebase — ` +
                    `pushes blocked, run "Full Rescan" to recover`,
                );
                new Notice(
                    "Obsetync: local index diverged from server — sync paused. " +
                    "Run 'Force full rescan' in settings to recover.",
                    10000,
                );
                return;
            }
            if (result.applied > 0 && result.deltasHadMtime && result.downloaded === 0) {
                console.log(
                    `[obsetync] tree hash differs from server but the pull downloaded 0 bytes ` +
                    `(content verified identical) — metadata-only drift, adopting server root`,
                );
            }

            // Pre-1.4.0 server (deltas without mtimes) or metadata-only root
            // drift: exact parity is unattainable. Content-wise we HAVE
            // applied everything the server reported, so the observed root is
            // an honest base — but only while tree and sync-base agree.
            if (countsAgree) {
                if (!this.treeBaseRoot || result.applied > 0) {
                    console.warn(
                        `[obsetync] adopting server root ${result.newRootHash.slice(0, 16)} as base ` +
                        `without byte parity (server deltas carried no mtimes); ` +
                        `tree=${treeCount} sync-base=${baseCount}`,
                    );
                    await adopt(result.newRootHash);
                } else if (this.syncBase.clearDiffPageCheckpoint()) {
                    await this.syncBase.checkpoint();
                }
                return;
            }

            this.pushBlocked = true;
            if (this.syncBase.clearDiffPageCheckpoint()) {
                await this.syncBase.checkpoint();
            }
            console.error(
                `[obsetync] tree/sync-base divergence: tree=${treeCount} files, ` +
                `sync-base=${baseCount} — pushes blocked`,
            );
            new Notice(
                "Obsetync: local index inconsistent — sync paused. " +
                "Run 'Force full rescan' in settings to recover.",
                10000,
            );
            return;
        }

        // No verifiable parity means the fixed snapshot must be replayed on
        // the next pull; retaining a completed cursor would skip that work.
        if (this.syncBase.clearDiffPageCheckpoint()) {
            await this.syncBase.checkpoint();
        }
    }

    private async pushPending(allowBulkChange = false): Promise<void> {
        if (this.reenrollmentRequired || this.stopped) return;
        if (this.bulkChangeReviewRequired && !allowBulkChange) {
            console.warn(
                `[obsetync] push deferred: ${this.pendingChanges.size} changes await Full Rescan review`,
            );
            return;
        }
        if (this.syncing || this.pendingChanges.size === 0) {
            console.log(
                `[obsetync] pushPending early-return: syncing=${this.syncing}, ` +
                `pending=${this.pendingChanges.size}`
            );
            return;
        }

        // --- Publish guards -------------------------------------------------
        // Never publish a root we can't vouch for; changes stay queued.
        if (this.pushBlocked) {
            console.warn(
                `[obsetync] push refused: tree diverged from server ` +
                `(${this.pendingChanges.size} changes queued) — run Full Rescan`,
            );
            return;
        }
        // A device that has both local state and a KNOWN server root but no
        // verified base would have to fabricate its parent — exactly the lie
        // that reverted the vault. The server root counts whether it came from
        // a completed pull (lastPullServerRoot) OR merely the root we seeded
        // from cached-root.bin at startup (localRootHash): a pull that keeps
        // CRASHING never sets lastPullServerRoot, which used to let this guard
        // slip and spray empty-parent putRoots (→ 400 storm, incident
        // 2026-07-15). A genuinely-first push to an empty vault has neither
        // signal, so it still proceeds.
        const serverRootKnown = this.lastPullServerRoot ?? this.localRootHash;
        if (!this.treeBaseRoot && serverRootKnown && this.syncBase.entryCount() > 0) {
            console.warn(
                "[obsetync] push deferred: no verified base root yet " +
                "(waiting for a pull to reconcile the tree)",
            );
            return;
        }
        // Cheap structural invariant: tree and sync-base advance in lockstep
        // now; a widening gap means a rebase was missed somewhere.
        const treeCount = this.getTreeFileCount();
        if (treeCount >= 0) {
            const baseCount = this.syncBase.entryCount();
            if (treeCount !== baseCount) {
                this.pushBlocked = true;
                console.error(
                    `[obsetync] push refused: tree=${treeCount} files vs ` +
                    `sync-base=${baseCount} — run Full Rescan`,
                );
                new Notice(
                    "Obsetync: local index inconsistent — sync paused. " +
                    "Run 'Force full rescan' in settings to recover.",
                    10000,
                );
                return;
            }
        }

        this.syncing = true;
        this.state = "pushing";
        const perf = perfTrace.begin("push");
        let perfOutcome: PerfOutcome = "success";
        const endSpan = perfSpan("sync.push");

        // Atomically detach one coalesced snapshot. Files are materialized
        // from their CURRENT disk state now, so a modify→delete or
        // delete→create burst cannot upload an obsolete intermediate version.
        const queued = this.pendingChanges.take();
        const operationId = await this.operationCheckpoint?.begin(
            "push",
            `${queued.length} dirty paths awaiting materialization`,
        );
        let notice: Notice | null = null;
        let pushFailed = false;

        try {
            // Do this before materializing a potentially vault-sized queue.
            await this.api.ensureTransportReady(perf);
            const endMaterialize = perf.phase("stat");
            const batch = sortByPriority(
                await this.materializeDirtyChanges(queued),
                this.syncPriority,
            );
            endMaterialize();
            if (this.stopped) {
                perfOutcome = "cancelled";
                this.pendingChanges.restore(queued);
                return;
            }
            const trackedDeletions = batch.filter(
                (change) =>
                    change.action === "deleted" &&
                    this.syncBase.getEntry(change.path) !== null,
            ).length;
            if (
                !allowBulkChange &&
                automaticChangeNeedsReview(
                    batch.length,
                    trackedDeletions,
                    this.syncBase.entryCount(),
                )
            ) {
                this.pendingChanges.restore(queued);
                this.requireBulkChangeReview("pending recovery", batch.length, trackedDeletions);
                return;
            }
            this.operationCheckpoint?.progress(
                operationId ?? "",
                `${batch.length} coalesced paths materialized`,
            );
            console.log(
                `[obsetync] pushPending: ${batch.length} final path states — ` +
                `first 3 paths: ${batch.slice(0, 3).map(c => `${c.action}:${c.path}`).join(", ")}`
            );
            this.onStatusUpdate(`↑ 0/${batch.length}`);

            // Show a persistent notice for batches large enough to care about.
            notice = batch.length >= 5 ? new Notice(`↑ 0/${batch.length}`, 0) : null;
            const result = await push(
                this.api,
                this.io,
                this.syncBase,
                this.wasm,
                this.tree,
                this.vaultId,
                batch,
                // HONEST parent: the base our tree state descends from — never
                // the last root merely observed on the server.
                this.treeBaseRoot,
                (text) => {
                    if (this.stopped) throw new Error("sync engine stopped");
                    this.onStatusUpdate(text);
                    notice?.setMessage(text);
                    if (operationId) this.operationCheckpoint?.progress(operationId, text);
                },
                perf,
                this.hashWorkers,
            );
            if (result.newRootHash) {
                this.localRootHash = result.newRootHash;
            }
            // Unmergeable same-file divergences: the server kept the OTHER
            // side in the tree and our version lost. Preserve our bytes as a
            // conflict copy NOW — the next pull will overwrite the original
            // path with the winner. The copy then syncs out as a normal new
            // file, visible on every device.
            if (result.conflicts.length > 0) {
                await this.preserveConflictCopies(result.conflicts as PushConflict[]);
            }

            // Our just-pushed root is now in the server's history, so it is a
            // valid (and honest) base for the next push — on a fast-forward it
            // IS the server's current root; after a server-side merge the next
            // pull will converge us onto the merged root and re-adopt.
            const ourRoot = this.getTreeRootHash();
            const newBase = ourRoot ?? result.newRootHash;
            if (newBase && this.treeBaseRoot !== newBase) {
                this.treeBaseRoot = newBase;
                this.syncBase.setTreeBaseRoot(newBase);
                await this.syncBase.save();
            }

            // Persist the new root so the WASM tree can be restored on restart.
            await this.saveCachedRoot();

            await this.journal.acknowledge(
                queued
                    .filter((change) => change.journalId !== undefined)
                    .map((change) => ({ path: change.path, throughId: change.journalId! })),
            );
        } catch (e: any) {
            pushFailed = true;
            perfOutcome = this.stopped ? "cancelled" : "error";
            console.error("[obsetync] push error:", e);
            this.pendingChanges.restore(queued);
            if (!this.stopped) {
                this.recordSyncFailure("push", e);
                notice?.setMessage(
                    this.reenrollmentRequired ? "Obsetync: re-enrollment required" : "sync ✗ error",
                );
            }
        } finally {
            try {
                if (operationId) await this.operationCheckpoint?.complete(operationId);
            } finally {
                perf.finish(perfOutcome);
                endSpan();
                notice?.hide();
                this.syncing = false;
            }
            if (!pushFailed && !this.stopped) {
                this.state = "idle";
                this.onStatusUpdate(
                    this.bulkChangeReviewRequired ? "sync ⚠ review" : "sync ✓",
                );
            }
            // Drain user edits that arrived while we were pushing.
            if (
                !this.reenrollmentRequired &&
                !this.stopped &&
                !this.bulkChangeReviewRequired &&
                this.pendingChanges.size > 0
            ) {
                this.debouncedPush?.();
            }
        }
    }

    /** Resolve a dirty hint to the path's final on-disk state. Metadata from
     *  an event is reused only if stat still agrees; bytes are never retained
     *  in the queue. */
    private async materializeDirtyChanges(changes: DirtyFileChange[]): Promise<FileChange[]> {
        const materialized: FileChange[] = [];
        for (const change of changes) {
            if (this.isExcluded(change.path)) continue;
            const stat = await this.io.stat(change.path);
            if (!stat) {
                materialized.push({ action: "deleted", path: change.path });
                continue;
            }

            const current: FileChange = {
                action: this.syncBase.getEntry(change.path) ? "modified" : "created",
                path: change.path,
                mtime: stat.mtime,
                size: stat.size,
            };
            if (
                change.hash !== undefined &&
                change.mtime === stat.mtime &&
                change.size === stat.size
            ) {
                current.hash = change.hash;
            }
            materialized.push(current);
        }
        return materialized;
    }

    /** Preserve OUR losing side of unmergeable conflicts as sibling copies
     *  ("doc (conflict Laptop 2026-07-14 0132).md"). Content comes from the
     *  server by our own side_b hash — we uploaded that blob moments ago, so
     *  it is authoritative even if the local file changed since. Falls back
     *  to the local bytes when the blob fetch fails (e.g. large chunked
     *  files, which never text-merge and aren't blob-addressable). */
    private async preserveConflictCopies(conflicts: PushConflict[]): Promise<void> {
        let preserved = 0;
        let failed = 0;
        const now = new Date();
        for (const c of conflicts) {
            if (!c.path || !c.side_b_hash) continue;
            const copyPath = conflictCopyPath(c.path, this.deviceName, now);
            try {
                let bytes: Uint8Array | null = null;
                try {
                    bytes = await this.api.getContent(c.side_b_hash);
                } catch {
                    // Blob not fetchable (chunked large file) — use the local
                    // file, which still holds our losing bytes until the next
                    // pull applies the winner.
                    bytes = await this.io.readFile(c.path);
                }
                if (!bytes) continue;
                await this.io.writeFile(copyPath, bytes);
                preserved++;
                console.log(
                    `[obsetync] conflict on ${c.path} — our version preserved as ${copyPath}`,
                );
            } catch (e) {
                failed++;
                console.error(`[obsetync] failed to preserve conflict copy for ${c.path}:`, e);
            }
        }
        if (preserved > 0) {
            new Notice(
                `Obsetync: ${preserved} conflict${preserved > 1 ? "s" : ""} — your version${
                    preserved > 1 ? "s were" : " was"
                } saved as "(conflict …)" cop${preserved > 1 ? "ies" : "y"} next to the file${
                    preserved > 1 ? "s" : ""
                }. Use the "Show sync conflicts" command to resolve.`,
                12000,
            );
        }
        if (failed > 0) {
            throw new Error(
                `could not preserve ${failed} conflict cop${failed === 1 ? "y" : "ies"}; ` +
                "original push remains journaled for retry",
            );
        }
    }

    /** Layer 2: recover unsynced entries from the persistent journal. */
    private async recoverFromJournal(): Promise<void> {
        const unsynced = this.journal.unsynced();
        if (unsynced.length === 0) return;

        console.log(
            `[obsetync] recovering ${unsynced.length} changes from journal`
        );
        const notice =
            unsynced.length >= 20
                ? new Notice(`Obsetync: recovering ${unsynced.length} journaled changes…`, 0)
                : null;

        let processed = 0;
        for (const entry of unsynced) {
            // No reads or hashes during recovery: one final-state stat happens
            // immediately before push. This makes replay O(unique paths) in
            // memory and safe even after a very large offline edit burst.
            this.pendingChanges.add(
                {
                    action: entry.action === "created" ? "created" :
                        entry.action === "deleted" ? "deleted" : "modified",
                    path: entry.path,
                },
                entry.id,
            );
            if (entry.action === "renamed" && entry.oldPath) {
                this.pendingChanges.add({ action: "deleted", path: entry.oldPath }, entry.id);
            }
            processed++;
            this.onStatusUpdate(`⟳ journal ${processed}/${unsynced.length}`);
            notice?.setMessage(`Obsetync: journal recovery ${processed}/${unsynced.length}`);
            this.progressHeartbeat("journal", `${processed}/${unsynced.length}`);
        }
        notice?.hide();

        if (this.pendingChanges.size > 0) {
            await this.pushPending();
        }
    }

    /** Layer 3: metadata audit — detect any stat drift and offline deletion. */
    private async partialMtimeScan(): Promise<void> {
        const lastSync = this.syncBase.lastSyncTimestamp;
        if (lastSync === 0) return; // First ever sync — skip, let pull handle it.
        if (this.bulkChangeReviewRequired) {
            console.warn("[obsetync] metadata scan skipped while bulk changes await review");
            return;
        }

        const perf = perfTrace.begin("scan");
        let perfOutcome: PerfOutcome = "success";
        let shouldPush = false;
        try {
            // Filter candidates from in-memory cache — no async stat calls.
            const endStat = perf.phase("stat");
            const allStats = this.io.statBulk();
            if (this.syncObsidianConfig) {
                const obsidianFiles = await this.io.listObsidianConfig();
                for (const [p, s] of obsidianFiles) allStats.set(p, s);
            }
            endStat();

            let visibleFiles = 0;
            let visibleBytes = 0;
            for (const [path, stat] of allStats) {
                if (this.isExcluded(path)) continue;
                visibleFiles++;
                visibleBytes += stat.size;
            }

            const endEnumerate = perf.phase("enumerate");
            const plan = planMetadataScan(
                allStats,
                this.syncBase.allPaths(),
                (path) => this.syncBase.getEntry(path),
                (path) => this.isExcluded(path),
            );
            endEnumerate();
            perf.setWorkload({ filesTotal: visibleFiles, bytesTotal: visibleBytes });

            if (metadataScanNeedsReview(plan, this.syncBase.entryCount())) {
                const total = plan.toHash.length + plan.deleted.length;
                perf.setWorkload({
                    filesTotal: visibleFiles + plan.deleted.length,
                    filesNeeded: total,
                    bytesNeeded: plan.toHash.reduce((sum, item) => sum + item.stat.size, 0),
                });
                perf.increment({ filesCompleted: visibleFiles + plan.deleted.length });
                console.warn(
                    `[obsetync] metadata scan found ${plan.toHash.length} ` +
                    `files need hashing and ${plan.deleted.length} look deleted ` +
                    `(${total} total changes)`,
                );
                this.requireBulkChangeReview("metadata scan", total, plan.deleted.length);
                return;
            }

            for (const path of plan.deleted) {
                this.pendingChanges.add({ action: "deleted", path });
            }

            let found = plan.deleted.length;
            let changedBytes = 0;
            if (plan.toHash.length > 0) {
                const readConcurrency = getHashTuning().readConcurrency;
                const hashableTotal = plan.toHash.reduce(
                    (count, item) =>
                        count + (item.stat.size < LARGE_FILE_THRESHOLD ? 1 : 0),
                    0,
                );
                let hashOrdinal = 0;
                const hashSampleWeights = plan.toHash.map((item) => {
                    if (item.stat.size >= LARGE_FILE_THRESHOLD) return 0;
                    return perfSampleWeight(hashOrdinal++, hashableTotal);
                });
                const notice = plan.toHash.length >= 20
                    ? new Notice(
                        `Obsetync: checking ${plan.toHash.length} recently-touched files…`,
                        0,
                    )
                    : null;
                try {
                    for (let i = 0; i < plan.toHash.length; i += readConcurrency) {
                        const batch = plan.toHash.slice(i, i + readConcurrency);
                        perf.observePeakBatchBytes(batch.reduce((sum, item) => {
                            const residentBytes = this.io.getAbsolutePath(item.path)
                                ? Math.min(item.stat.size, getHashTuning().feedBytes)
                                : item.stat.size;
                            return sum + residentBytes;
                        }, 0));
                        const results = await Promise.all(
                            batch.map(async ({ path, stat }, batchIndex) => {
                                const knownHash = this.syncBase.getHash(path);
                                if (stat.size >= LARGE_FILE_THRESHOLD) {
                                    return {
                                        path,
                                        stat,
                                        hash: undefined as string | undefined,
                                        knownHash,
                                    };
                                }
                                const itemIndex = i + batchIndex;
                                const sampleWeight = hashSampleWeights[itemIndex];
                                const stable = await this.hashStableFile(
                                    path,
                                    stat,
                                    sampleWeight > 0 ? perf : undefined,
                                    sampleWeight,
                                );
                                if (stable.hash === knownHash) return null;
                                return {
                                    path,
                                    stat: stable.stat,
                                    hash: stable.hash,
                                    knownHash,
                                };
                            }),
                        );
                        for (const result of results) {
                            if (!result) continue;
                            found++;
                            changedBytes += result.stat.size;
                            const change: FileChange = {
                                action: result.knownHash ? "modified" : "created",
                                path: result.path,
                                mtime: result.stat.mtime,
                                size: result.stat.size,
                            };
                            if (result.hash !== undefined) change.hash = result.hash;
                            this.pendingChanges.add(change);
                        }
                        await yieldToUI();
                        const done = Math.min(i + readConcurrency, plan.toHash.length);
                        this.onStatusUpdate(`⟳ scan ${done}/${plan.toHash.length}`);
                        notice?.setMessage(
                            `Obsetync: metadata scan ${done}/${plan.toHash.length} ` +
                            `· ${found} changed`,
                        );
                        this.progressHeartbeat(
                            "mtimeScan",
                            `${done}/${plan.toHash.length}, ${found} changed`,
                        );
                    }
                } finally {
                    notice?.hide();
                }
            }

            perf.setWorkload({
                filesTotal: visibleFiles + plan.deleted.length,
                filesNeeded: found,
                bytesNeeded: changedBytes,
            });
            perf.increment({ filesCompleted: visibleFiles + plan.deleted.length });
            shouldPush = found > 0;
            if (shouldPush) {
                console.log(`[obsetync] metadata scan found ${found} unsynced changes`);
            }
        } catch (error) {
            perfOutcome = this.stopped ? "cancelled" : "error";
            throw error;
        } finally {
            perf.finish(perfOutcome);
        }

        if (shouldPush) await this.pushPending();
    }

    /** Layer 1: attach live vault event listeners.
     *
     *  Events are journaled + queued even while a sync is in flight — the old
     *  `if (this.syncing) return` dropped genuine user edits made during a
     *  long pull. The one thing we must NOT queue is the pull's own disk
     *  writes echoing back as vault events; those are recognized only when
     *  the event's actual content matches PullEchoTracker's expected hash. */
    private async appendLocalJournal(entry: NewJournalEntry): Promise<number | undefined> {
        try {
            return await this.journal.append(entry);
        } catch (error) {
            // Keep the current-session dirty hint even when durable storage is
            // unavailable. The error is visible in diagnostics; a full scan
            // remains the recovery path after a crash.
            console.error(`[obsetync] change journal append failed for ${entry.path}:`, error);
            return undefined;
        }
    }

    /** Hash an expected pull echo without materializing a large file twice on
     *  mobile. Desktop streams from fs. Mobile large-file writes use the
     *  just-recorded sync-base hash only when event stat exactly matches the
     *  post-write stat; otherwise the event stays dirty conservatively. */
    private async pullEchoHash(file: TFile): Promise<string | null> {
        if (
            file.stat.size >= LARGE_FILE_THRESHOLD &&
            this.io.getAbsolutePath(file.path) === null
        ) {
            // Adapter events can arrive just before applyContentDelta records
            // its post-write stat. Give that continuation one event-loop turn.
            await yieldToUI();
            const base = this.syncBase.getEntry(file.path);
            return base && base.mtime === file.stat.mtime && base.size === file.stat.size
                ? base.hash
                : null;
        }
        try {
            return (await this.hashStableFile(file.path, file.stat)).hash;
        } catch (error) {
            console.warn(`[obsetync] pull-echo verification failed for ${file.path}:`, error);
            return null;
        }
    }

    /** Hash a stable pathname. Desktop workers receive only path/stat metadata
     * and return only digest/timings; a stat drift retries once with the fresh
     * fingerprint. Mobile and unavailable-worker paths keep the bounded
     * renderer implementation. */
    private async hashStableFile(
        path: string,
        expected: FileStat,
        perf?: PerfOperation,
        perfWeight = 1,
    ): Promise<{ hash: string; stat: FileStat }> {
        let current = expected;
        for (let attempt = 0; attempt < 2; attempt++) {
            const absolutePath = this.io.getAbsolutePath(path);
            if (this.hashWorkers && absolutePath) {
                try {
                    const result = await this.hashWorkers.run({
                        absolutePath,
                        expectedSize: current.size,
                        expectedMtime: current.mtime,
                        mode: "hash",
                        feedBytes: getHashTuning().feedBytes,
                    }, this.hashWorkerAbort.signal);
                    if (result.mode !== "hash") throw new Error("hash worker mode mismatch");
                    perf?.addPhase("read", result.read_ms * perfWeight);
                    perf?.addPhase("hash", result.hash_ms * perfWeight);
                    return { hash: result.hash, stat: current };
                } catch (error) {
                    if (error instanceof HashWorkerFileDriftError) {
                        const refreshed = await this.io.stat(path);
                        if (refreshed && attempt === 0) {
                            current = refreshed;
                            continue;
                        }
                        throw error;
                    }
                    if ((error as Error)?.name === "AbortError") throw error;
                    if (!this.hashWorkerFallbackWarned) {
                        this.hashWorkerFallbackWarned = true;
                        console.warn(
                            "[obsetync] desktop hash worker failed; using renderer fallback:",
                            error,
                        );
                    }
                }
            }

            const hash = await hashFileStreaming(path, this.io, this.wasm, perf, perfWeight);
            const after = await this.io.stat(path);
            if (
                after &&
                after.size === current.size &&
                Math.abs(after.mtime - current.mtime) <= 1
            ) {
                return { hash, stat: after };
            }
            if (after && attempt === 0) {
                current = after;
                continue;
            }
            throw new HashWorkerFileDriftError();
        }
        throw new HashWorkerFileDriftError();
    }

    private attachVaultListeners(): void {
        this.debouncedPush = debounce(
            () => {
                if (this.autoSync) void this.pushPending();
            },
            3000,
            true
        );
        const debouncedPush = () => this.debouncedPush?.();

        this.eventRefs.push(
            this.app.vault.on("modify", async (file: TAbstractFile) => {
                if (!(file instanceof TFile)) return;
                if (this.isExcluded(file.path)) return;
                this.localEventsInFlight.add(file.path);
                try {
                    const expectedEcho = this.pullEchoes.expectsUpsert(file.path);
                    const journalId = await this.appendLocalJournal({
                        action: "modified",
                        path: file.path,
                        ts: Date.now(),
                        synced: false,
                    });
                    const actualHash = expectedEcho ? await this.pullEchoHash(file) : null;
                    if (
                        actualHash !== null &&
                        this.pullEchoes.consumeUpsert(file.path, actualHash)
                    ) {
                        if (journalId !== undefined) {
                            await this.journal.acknowledge([
                                { path: file.path, throughId: journalId },
                            ]);
                        }
                        return;
                    }
                    const change: FileChange = {
                        action: "modified",
                        path: file.path,
                        mtime: file.stat.mtime,
                        size: file.stat.size,
                    };
                    if (actualHash !== null) change.hash = actualHash;
                    this.pendingChanges.add(change, journalId);
                    debouncedPush();
                } finally {
                    this.localEventsInFlight.delete(file.path);
                }
            })
        );

        this.eventRefs.push(
            this.app.vault.on("create", async (file: TAbstractFile) => {
                if (!(file instanceof TFile)) return;
                if (this.isExcluded(file.path)) return;
                this.localEventsInFlight.add(file.path);
                try {
                    const expectedEcho = this.pullEchoes.expectsUpsert(file.path);
                    const journalId = await this.appendLocalJournal({
                        action: "created",
                        path: file.path,
                        ts: Date.now(),
                        synced: false,
                    });
                    const actualHash = expectedEcho ? await this.pullEchoHash(file) : null;
                    if (
                        actualHash !== null &&
                        this.pullEchoes.consumeUpsert(file.path, actualHash)
                    ) {
                        if (journalId !== undefined) {
                            await this.journal.acknowledge([
                                { path: file.path, throughId: journalId },
                            ]);
                        }
                        return;
                    }
                    const change: FileChange = {
                        action: "created",
                        path: file.path,
                        mtime: file.stat.mtime,
                        size: file.stat.size,
                    };
                    if (actualHash !== null) change.hash = actualHash;
                    this.pendingChanges.add(change, journalId);
                    debouncedPush();
                } finally {
                    this.localEventsInFlight.delete(file.path);
                }
            })
        );

        this.eventRefs.push(
            this.app.vault.on("delete", async (file: TAbstractFile) => {
                if (this.isExcluded(file.path) || this.pullEchoes.consumeDelete(file.path)) return;
                this.localEventsInFlight.add(file.path);
                try {
                    const journalId = await this.appendLocalJournal({
                        action: "deleted",
                        path: file.path,
                        ts: Date.now(),
                        synced: false,
                    });
                    this.pendingChanges.add({
                        action: "deleted",
                        path: file.path,
                    }, journalId);
                    debouncedPush();
                } finally {
                    this.localEventsInFlight.delete(file.path);
                }
            })
        );

        this.eventRefs.push(
            this.app.vault.on("rename", async (file: TAbstractFile, oldPath: string) => {
                if (!(file instanceof TFile)) return;
                const oldExcluded = this.isExcluded(oldPath);
                const newExcluded = this.isExcluded(file.path);
                if (oldExcluded && newExcluded) return;
                this.localEventsInFlight.add(oldPath);
                this.localEventsInFlight.add(file.path);
                try {
                    // A rename crossing out of sync scope is only a deletion;
                    // crossing in is only a creation. Never publish the ignored
                    // half of the move.
                    if (newExcluded) {
                        const journalId = await this.appendLocalJournal({
                            action: "deleted",
                            path: oldPath,
                            ts: Date.now(),
                            synced: false,
                        });
                        this.pendingChanges.add({ action: "deleted", path: oldPath }, journalId);
                        debouncedPush();
                        return;
                    }

                    const expectedEcho = !oldExcluded &&
                        this.pullEchoes.expectsRename(oldPath, file.path);
                    const oldJournalId = oldExcluded ? undefined : await this.appendLocalJournal({
                        action: "deleted",
                        path: oldPath,
                        ts: Date.now(),
                        synced: false,
                    });
                    const newJournalId = await this.appendLocalJournal({
                        action: "created",
                        path: file.path,
                        ts: Date.now(),
                        synced: false,
                    });
                    const actualHash = expectedEcho ? await this.pullEchoHash(file) : null;
                    if (
                        expectedEcho &&
                        actualHash !== null &&
                        this.pullEchoes.consumeRename(oldPath, file.path, actualHash)
                    ) {
                        const watermarks: Array<{ path: string; throughId: number }> = [];
                        if (oldJournalId !== undefined) {
                            watermarks.push({ path: oldPath, throughId: oldJournalId });
                        }
                        if (newJournalId !== undefined) {
                            watermarks.push({ path: file.path, throughId: newJournalId });
                        }
                        await this.journal.acknowledge(watermarks);
                        return;
                    }
                    if (!oldExcluded) {
                        this.pendingChanges.add(
                            { action: "deleted", path: oldPath },
                            oldJournalId,
                        );
                    }
                    const change: FileChange = {
                        action: "created",
                        path: file.path,
                        mtime: file.stat.mtime,
                        size: file.stat.size,
                    };
                    if (actualHash !== null) change.hash = actualHash;
                    this.pendingChanges.add(change, newJournalId);
                    debouncedPush();
                } finally {
                    this.localEventsInFlight.delete(oldPath);
                    this.localEventsInFlight.delete(file.path);
                }
            })
        );
    }

    private async saveCachedRoot(): Promise<void> {
        const rootBytes = this.tree.root_bytes();
        if (!rootBytes) return;
        const path = ".obsidian/plugins/obsetync/cached-root.bin";
        try {
            await this.app.vault.adapter.writeBinary(path, exactArrayBuffer(rootBytes));
            console.log("[obsetync] cached root saved");
        } catch (e) {
            console.warn("[obsetync] failed to save cached root:", e);
        }
    }

    private isSyncInternal(path: string): boolean {
        return (
            !isSafeVaultPath(path) ||
            path.startsWith(".obsidian/plugins/obsetync/") ||
            path === ".obsetync-crash.log"
        );
    }

    /** User-configured ignore (Slice 2). */
    isIgnored(path: string): boolean {
        return this.ignore.test(path);
    }

    /** A path that must never enter sync from THIS device: the plugin's own
     *  internal files, or a user-ignored path. Used at every write-detection
     *  chokepoint (vault events, full scan, metadata scan). Applying it in the
     *  full-scan delete-detection is what stops a local `cargo clean` from
     *  propagating target/ DELETIONS to the fleet — ignored paths that vanish
     *  from disk are simply not tracked, never deleted. */
    private isExcluded(path: string): boolean {
        return this.isSyncInternal(path) || this.isIgnored(path);
    }
}

function sortByPriority(changes: FileChange[], priority: SyncPriority): FileChange[] {
    switch (priority) {
        case "oldest":    return [...changes].sort((a, b) => (a.mtime ?? 0) - (b.mtime ?? 0));
        case "newest":    return [...changes].sort((a, b) => (b.mtime ?? 0) - (a.mtime ?? 0));
        case "smallest":  return [...changes].sort((a, b) => (a.size ?? 0) - (b.size ?? 0));
        case "biggest":   return [...changes].sort((a, b) => (b.size ?? 0) - (a.size ?? 0));
        case "alphabetic": return [...changes].sort((a, b) => a.path.localeCompare(b.path));
        case "random":    return [...changes].sort(() => Math.random() - 0.5);
        default:          return changes; // sequential — preserve insertion order, no copy needed
    }
}
