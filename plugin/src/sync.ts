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
 *  WASM linear memory bounded to ~1 MB per file regardless of vault content. */
const LARGE_FILE_THRESHOLD = 1_048_576; // 1 MB
import { ObsetyncApi, PushConflict } from "./api";
import { conflictCopyPath } from "./conflict-ui";
import { ObsetyncWsChannel, PresenceUpdate, WsState } from "./ws";
import { PlatformIO } from "./platform";
import { ObsetyncSyncBase } from "./sync-base";
import { ObsetyncJournal } from "./journal";
import { perfSpan } from "./debug-log";
import { pull } from "./pull";
import { push, hashFileStreaming, streamingHash, FileChange, WasmModule, WasmTree } from "./push";
import { SyncPriority } from "./settings";
import { compileIgnore, type CompiledIgnore } from "./ignore";

export type SyncState = "idle" | "pulling" | "pushing" | "scanning" | "error";

/**
 * Core sync orchestrator. Coordinates pull, push, journal recovery,
 * mtime scanning, and live vault event tracking (D-005 4-layer system).
 */
export class ObsetyncSyncEngine {
    private state: SyncState = "idle";
    private localRootHash: string | null;
    private pendingChanges: FileChange[] = [];
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
    /** Paths the in-flight pull is writing — vault events for them are our
     *  own echoes, not user edits, and must not be queued for push-back. */
    private expectedPullWrites = new Set<string>();
    /** debouncedPush from attachVaultListeners, kept so sync completion can
     *  drain user edits that arrived mid-sync. */
    private debouncedPush: (() => void) | null = null;

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

    /** Start the sync engine: run startup sequence, attach listeners, start timer. */
    async start(): Promise<void> {
        console.log("[obsetync] starting sync engine");

        // Restore the verified base root persisted in lockstep with sync-base.
        // Null on first run and on pre-1.4.0 sync-base files — established by
        // the first verified pull below.
        this.treeBaseRoot = this.syncBase.treeBaseRoot;
        if (this.treeBaseRoot) {
            console.log(`[obsetync] tree base root: ${this.treeBaseRoot.slice(0, 16)}`);
        }

        // Step 0: Verify connectivity. /health is the only plaintext route.
        try {
            const conn = await this.api.ping();
            console.log(
                `[obsetync] ${conn.ok ? "✓ reachable" : "✗ unreachable"} at ${conn.serverUrl} | ${conn.transport}`
            );
        } catch (e) {
            console.warn("[obsetync] ✗ server unreachable:", e);
        }

        // Startup sequence (D-005):
        // 1. Pull remote changes.
        console.log("[obsetync] step 1: pull remote");
        await this.pullRemote();

        // 2. Recover from journal (Layer 2).
        console.log("[obsetync] step 2: recover from journal");
        await this.recoverFromJournal();

        // 3. Partial mtime scan (Layer 3).
        console.log("[obsetync] step 3: mtime scan");
        await this.partialMtimeScan();

        // 4. Start live event listeners (Layer 1).
        console.log("[obsetync] step 4: attach vault listeners");
        this.attachVaultListeners();

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
        if (this.syncTimer) {
            window.clearInterval(this.syncTimer);
            this.syncTimer = null;
        }
        this.wsChannel?.stop();
        this.wsChannel = null;
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
            `[obsetync] forceSync start: pending=${this.pendingChanges.length} ` +
            `localRoot=${this.localRootHash?.slice(0, 16) ?? "(none)"}`
        );
        await this.pullRemote();
        const t1 = Date.now();
        console.log(
            `[obsetync] forceSync: pull done in ${t1 - t0}ms, ` +
            `localRoot=${this.localRootHash?.slice(0, 16) ?? "(none)"}, ` +
            `pending=${this.pendingChanges.length}`
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
            `pending=${this.pendingChanges.length}`
        );
        await this.pushPending();
        console.log(
            `[obsetync] forceSync end in ${Date.now() - t0}ms, ` +
            `pending=${this.pendingChanges.length}, ` +
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
        const endSpan = perfSpan("sync.reconcile");
        try {
            return await this._reconcileInner(progress);
        } finally {
            endSpan();
            this.syncing = false;
        }
    }

    private async _reconcileInner(progress: (msg: string) => void): Promise<{
        smallUploaded: number;
        largeUploaded: number;
        treeChunksUploaded: number;
        bytes: number;
    }> {
        // Populate the WASM tree from sync-base so wasm_tree_chunk_hashes
        // reflects the actual index-chunk set the server should have. Same
        // bootstrap push.ts does on first call.
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
        const smallHashToPath = new Map<string, string>(); // first path wins
        const largeHashToPath = new Map<string, string>();
        for (const path of this.syncBase.allPaths()) {
            const entry = this.syncBase.getEntry(path)!;
            if (entry.size >= LARGE_FILE_THRESHOLD) {
                if (!largeHashToPath.has(entry.hash)) largeHashToPath.set(entry.hash, path);
            } else {
                if (!smallHashToPath.has(entry.hash)) smallHashToPath.set(entry.hash, path);
            }
        }

        const CHECK_BATCH = 1000;

        // --- Step 1: which tree chunks (index) is the server missing?
        const treeHashes = this.wasm.wasm_tree_chunk_hashes(this.tree);
        const missingTreeChunks = treeHashes.length > 0
            ? await this.api.checkChunks(treeHashes)
            : [];

        // --- Step 2: which small-file contents is the server missing?
        const smallHashes = [...smallHashToPath.keys()];
        const missingSmall: string[] = [];
        for (let i = 0; i < smallHashes.length; i += CHECK_BATCH) {
            const batch = smallHashes.slice(i, i + CHECK_BATCH);
            const missing = await this.api.checkContent(batch);
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
        const largeHashes = [...largeHashToPath.keys()];
        const missingLargeManifests: string[] = [];
        for (let i = 0; i < largeHashes.length; i += CHECK_BATCH) {
            const batch = largeHashes.slice(i, i + CHECK_BATCH);
            const missing = await this.api.checkManifests(batch);
            missingLargeManifests.push(...missing);
        }

        const totalMissing =
            missingTreeChunks.length + missingSmall.length + missingLargeManifests.length;

        console.log(
            `[obsetync] reconcile plan: ` +
            `tree-chunks ${treeHashes.length} checked / ${missingTreeChunks.length} missing, ` +
            `small ${smallHashes.length} checked / ${missingSmall.length} missing, ` +
            `large ${largeHashes.length} checked / ${missingLargeManifests.length} missing`
        );

        if (totalMissing === 0) {
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

        // --- Step 3: upload missing tree (index) chunks.
        for (const hash of missingTreeChunks) {
            const chunkBytes = this.wasm.wasm_tree_get_chunk(this.tree, hash);
            if (chunkBytes) {
                await this.api.putChunk(hash, chunkBytes);
                treeChunksUploaded++;
                bytes += chunkBytes.length;
            }
        }

        // --- Step 4: upload missing small-file content. Concurrent within
        // bounded batches to keep peak memory sane.
        const UPLOAD_CONCURRENCY = 4;
        for (let i = 0; i < missingSmall.length; i += UPLOAD_CONCURRENCY) {
            const batch = missingSmall.slice(i, i + UPLOAD_CONCURRENCY);
            await Promise.all(batch.map(async hash => {
                const path = smallHashToPath.get(hash);
                if (!path) return;
                try {
                    const data = await this.io.readFile(path);
                    // If content drifted since sync-base recorded it, skip — the next
                    // scan cycle will detect the change via hash mismatch and push
                    // the new version. Don't upload bytes under the WRONG hash.
                    const actual = streamingHash(this.wasm, data);
                    if (actual !== hash) return;
                    await this.api.putContent(hash, data);
                    smallUploaded++;
                    bytes += data.length;
                } catch (e) {
                    console.warn(`[obsetync] reconcile skipped ${path}:`, e);
                }
            }));
            const done = Math.min(i + UPLOAD_CONCURRENCY, missingSmall.length);
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
            const path = largeHashToPath.get(hash);
            if (!path) continue;
            largeIdx++;
            progress(`reconcile: large file ${largeIdx}/${missingLargeManifests.length}`);
            notice?.setMessage(`Re-uploading large file ${largeIdx}/${missingLargeManifests.length}`);
            try {
                const data = await this.io.readFile(path);
                const actual = streamingHash(this.wasm, data);
                if (actual !== hash) continue; // drifted — scan will pick up
                const info = this.wasm.wasm_chunk_file(data);
                const chunkHashes = (info.chunks as any[]).map(c => c.hash);
                const missingChunks = chunkHashes.length > 0
                    ? await this.api.checkContentChunks(chunkHashes)
                    : [];
                const missingSet = new Set(missingChunks);
                for (const c of info.chunks as any[]) {
                    if (missingSet.has(c.hash)) {
                        const chunkData = this.wasm.wasm_get_file_chunk(data, c.offset, c.size);
                        await this.api.putContentChunk(c.hash, chunkData);
                        bytes += chunkData.length;
                    }
                }
                await this.api.putManifest(hash, {
                    file_hash: hash,
                    total_size: info.total_size,
                    chunks: info.chunks,
                });
                largeUploaded++;
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

        return { smallUploaded, largeUploaded, treeChunksUploaded, bytes };
    }

    /** Force a full vault scan (Layer 4). Doubles as the recovery action for
     *  a diverged tree: the in-memory tree is rebuilt from sync-base (the
     *  state corresponding to treeBaseRoot) before scanning, so whatever
     *  in-memory drift caused a push block is discarded, the block lifted,
     *  and local differences re-queued from disk truth. */
    async fullScan(): Promise<void> {
        this.state = "scanning";
        this.onStatusUpdate("sync ⟳");
        const notice = new Notice("Scanning vault...", 0);
        console.log("[obsetync] full scan started");
        const endSpan = perfSpan("scan.full");

        try {
            try {
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
                if (this.pushBlocked) {
                    console.log("[obsetync] full scan rebuilt the tree — push unblocked");
                    this.pushBlocked = false;
                }
            } catch (e) {
                console.error("[obsetync] full scan tree rebuild failed:", e);
            }

            // statBulk() reads all file stats from Obsidian's in-memory cache —
            // no async IPC calls, O(n) in-memory map construction.
            const statMap = this.io.statBulk();
            // Optionally include .obsidian/ — vault.getFiles() hides it by design.
            if (this.syncObsidianConfig) {
                const obsidianFiles = await this.io.listObsidianConfig();
                for (const [p, s] of obsidianFiles) statMap.set(p, s);
            }
            console.log(`[obsetync] full scan: ${statMap.size} files total`);

            // Phase 1: fast mtime+size filter (synchronous, no I/O).
            const toHash: Array<{ path: string; stat: { mtime: number; size: number } }> = [];
            for (const [path, stat] of statMap) {
                if (this.isExcluded(path)) continue;
                const base = this.syncBase.getEntry(path);
                if (base && stat.mtime === base.mtime && stat.size === base.size) continue;
                toHash.push({ path, stat });
            }
            console.log(`[obsetync] full scan: ${toHash.length} files need hashing`);

            // Phase 2: read + hash + push in streaming batches.
            //
            // Key constraints:
            //   - Large files (≥ 1 MB) skip WASM hash entirely here. push.ts reads
            //     them during upload and gets file_hash from FastCDC. This keeps
            //     WASM linear memory bounded regardless of PDF/image sizes.
            //   - READ_CONCURRENCY=4 limits concurrent IPC reads. 8 concurrent 200 MB
            //     PDFs = 1.6 GB peak; 4 = 800 MB — and hash is serial anyway.
            //   - FLUSH_BATCH=500 means ~20 putRoot calls for a 10k vault instead of
            //     200. Each flush holds syncing=true; fewer flushes = smoother UX.
            //   - yieldToUI() every group lets Electron's audio/render callbacks run.
            const READ_CONCURRENCY = 4;
            const FLUSH_BATCH = 500;
            let pending: FileChange[] = [];
            let totalChanges = 0;

            const flushPending = async () => {
                if (pending.length === 0) return;
                this.pendingChanges.push(...pending);
                pending = [];
                await this.pushPending();
            };

            for (let i = 0; i < toHash.length; i += READ_CONCURRENCY) {
                const batch = toHash.slice(i, i + READ_CONCURRENCY);
                const results = await Promise.all(
                    batch.map(async ({ path, stat }) => {
                        const base = this.syncBase.getEntry(path);
                        if (stat.size >= LARGE_FILE_THRESHOLD) {
                            // Skip WASM hash — push.ts will hash during upload.
                            // We know it changed because it passed the mtime+size filter.
                            return { path, stat, hash: undefined as string | undefined, base };
                        }
                        const hash = await hashFileStreaming(path, this.io, this.wasm);
                        if (base && hash === base.hash) return null; // unchanged
                        return { path, stat, hash, base };
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

                if (pending.length >= FLUSH_BATCH) {
                    await flushPending();
                }
            }
            await flushPending();

            // Phase 3: deletions — files in sync-base that no longer exist.
            for (const path of this.syncBase.allPaths()) {
                if (!statMap.has(path) && !this.isExcluded(path)) {
                    pending.push({ action: "deleted", path });
                    totalChanges++;
                }
            }
            await flushPending();

            console.log(`[obsetync] full scan complete: ${totalChanges} changes`);
        } finally {
            endSpan();
            notice.hide();
            this.state = "idle";
            this.onStatusUpdate("sync ✓");
        }
    }

    // --- Private ---

    private async pullRemote(): Promise<void> {
        if (this.syncing) return;
        this.syncing = true;
        this.state = "pulling";
        this.onStatusUpdate("sync ↓");
        const endSpan = perfSpan("sync.pull");

        // Live progress: every tick lands in the status bar; a persistent
        // Notice appears only once REAL work is detected (first-sync or a
        // non-trivial delta), so idle 30s polls stay silent. A 31-minute
        // first-sync once ran with zero feedback because this callback was
        // simply never passed — pull's per-batch ticks all landed in void.
        // (ref-object because TS control-flow can't see the closure assign)
        const noticeRef: { n: Notice | null } = { n: null };
        const progress = (msg: string) => {
            this.onStatusUpdate(`↓ ${msg}`);
            const isRealWork = /files applied|changes to apply|first sync/.test(msg);
            if (noticeRef.n) {
                noticeRef.n.setMessage(`Obsetync ↓ ${msg}`);
            } else if (isRealWork) {
                noticeRef.n = new Notice(`Obsetync ↓ ${msg}`, 0);
            }
            this.progressHeartbeat("pull", msg);
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
                (paths) => {
                    for (const p of paths) this.expectedPullWrites.add(p);
                },
                // Editor safety: paths with UNSYNCED local edits keep their
                // disk bytes — the queued push + server merge reconcile them.
                // Without this, the startup order (pull → journal recovery)
                // could overwrite last session's edits before recovery reads
                // them.
                this.unsyncedLocalPaths(),
                // Slice 2: never fetch ignored paths; untrack them if purged.
                (p) => this.isIgnored(p),
            );
            if (result.newRootHash) {
                this.localRootHash = result.newRootHash;
                this.lastPullServerRoot = result.newRootHash;
            }
            await this.adoptPullResult(result);
        } catch (e: any) {
            console.error("[obsetync] pull error:", e);
            this.lastError = { ts: Date.now(), origin: "pull", message: String(e?.message ?? e) };
            this.state = "error";
            this.onStatusUpdate("sync ✗");
        } finally {
            endSpan();
            noticeRef.n?.hide();
            this.expectedPullWrites.clear();
            this.syncing = false;
            if (this.state !== "error") {
                this.state = "idle";
                this.onStatusUpdate("sync ✓");
                this.lastPullDoneMs = Date.now();
            }
            // Drain user edits that arrived while we were syncing.
            if (this.pendingChanges.length > 0) this.debouncedPush?.();
        }
    }

    /** Every path whose newest bytes exist only locally: queued-but-unpushed
     *  changes plus unsynced journal entries. Pull must not overwrite these. */
    private unsyncedLocalPaths(): Set<string> {
        const paths = new Set<string>();
        for (const c of this.pendingChanges) paths.add(c.path);
        try {
            for (const e of this.journal.unsynced()) paths.add(e.path);
        } catch {
            // Journal unavailable — pending queue alone still protects live edits.
        }
        return paths;
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
        failedCount: number;
    }): Promise<void> {
        // Files this pull couldn't fetch leave the tree missing content the
        // server has. Adopting the server root as our base now would let a
        // later fast-forward read those gaps as deletions and propagate them
        // (the 2026-07-13 failure mode). Hold the base where it is — merges
        // from an older base always preserve the other side's changes — and
        // let the deferred files retry on the next pull.
        if (result.failedCount > 0) {
            console.warn(
                `[obsetync] pull deferred ${result.failedCount} unfetched file(s) — ` +
                `base root NOT advanced; retrying next pull`,
            );
            return;
        }

        const adopt = async (hash: string) => {
            if (this.treeBaseRoot !== hash) {
                this.treeBaseRoot = hash;
                this.syncBase.setTreeBaseRoot(hash);
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
                        result.newRootBytes.buffer as ArrayBuffer,
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
            const epsilon = Math.max(8, Math.ceil(baseCount * 0.005));
            const countsAgree = treeCount >= 0 && Math.abs(treeCount - baseCount) <= epsilon;

            if (result.applied > 0 && result.deltasHadMtime) {
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
                }
                return;
            }

            this.pushBlocked = true;
            console.error(
                `[obsetync] tree/sync-base divergence: tree=${treeCount} files, ` +
                `sync-base=${baseCount} (epsilon=${epsilon}) — pushes blocked`,
            );
            new Notice(
                "Obsetync: local index inconsistent — sync paused. " +
                "Run 'Force full rescan' in settings to recover.",
                10000,
            );
        }
    }

    private async pushPending(): Promise<void> {
        if (this.syncing || this.pendingChanges.length === 0) {
            console.log(
                `[obsetync] pushPending early-return: syncing=${this.syncing}, ` +
                `pending=${this.pendingChanges.length}`
            );
            return;
        }

        // --- Publish guards -------------------------------------------------
        // Never publish a root we can't vouch for; changes stay queued.
        if (this.pushBlocked) {
            console.warn(
                `[obsetync] push refused: tree diverged from server ` +
                `(${this.pendingChanges.length} changes queued) — run Full Rescan`,
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
            const epsilon = Math.max(8, Math.ceil(baseCount * 0.005));
            if (Math.abs(treeCount - baseCount) > epsilon) {
                this.pushBlocked = true;
                console.error(
                    `[obsetync] push refused: tree=${treeCount} files vs ` +
                    `sync-base=${baseCount} (epsilon=${epsilon}) — run Full Rescan`,
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
        const endSpan = perfSpan("sync.push");

        const batch = sortByPriority(this.pendingChanges.splice(0), this.syncPriority);
        console.log(
            `[obsetync] pushPending: ${batch.length} changes — ` +
            `first 3 paths: ${batch.slice(0, 3).map(c => `${c.action}:${c.path}`).join(", ")}`
        );
        this.onStatusUpdate(`↑ 0/${batch.length}`);

        // Show a persistent notice for batches large enough to care about.
        const notice = batch.length >= 5 ? new Notice(`↑ 0/${batch.length}`, 0) : null;

        try {
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
                    this.onStatusUpdate(text);
                    notice?.setMessage(text);
                }
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

            // Mark journal entries as synced.
            for (const change of batch) {
                this.journal.markSynced(change.path);
            }
        } catch (e: any) {
            console.error("[obsetync] push error:", e);
            this.lastError = { ts: Date.now(), origin: "push", message: String(e?.message ?? e) };
            this.pendingChanges.unshift(...batch);
            this.state = "error";
            this.onStatusUpdate("sync ✗");
            notice?.setMessage("sync ✗ error");
        } finally {
            endSpan();
            notice?.hide();
            this.syncing = false;
            if (this.state !== "error") {
                this.state = "idle";
                this.onStatusUpdate("sync ✓");
            }
            // Drain user edits that arrived while we were pushing.
            if (this.pendingChanges.length > 0) this.debouncedPush?.();
        }
    }

    /** Preserve OUR losing side of unmergeable conflicts as sibling copies
     *  ("doc (conflict Laptop 2026-07-14 0132).md"). Content comes from the
     *  server by our own side_b hash — we uploaded that blob moments ago, so
     *  it is authoritative even if the local file changed since. Falls back
     *  to the local bytes when the blob fetch fails (e.g. large chunked
     *  files, which never text-merge and aren't blob-addressable). */
    private async preserveConflictCopies(conflicts: PushConflict[]): Promise<void> {
        let preserved = 0;
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
            if (entry.action === "deleted") {
                this.pendingChanges.push({ action: "deleted", path: entry.path });
            } else {
                try {
                    const hash = await hashFileStreaming(entry.path, this.io, this.wasm);
                    this.pendingChanges.push({
                        action: entry.action === "created" ? "created" : "modified",
                        path: entry.path,
                        hash,
                    });
                } catch {
                    // File might have been deleted since journal entry.
                }
            }
            processed++;
            this.onStatusUpdate(`⟳ journal ${processed}/${unsynced.length}`);
            notice?.setMessage(`Obsetync: journal recovery ${processed}/${unsynced.length}`);
            this.progressHeartbeat("journal", `${processed}/${unsynced.length}`);
        }
        notice?.hide();

        if (this.pendingChanges.length > 0) {
            await this.pushPending();
        }
        await this.journal.truncate();
    }

    /** Layer 3: partial mtime scan — check files modified since last sync. */
    private async partialMtimeScan(): Promise<void> {
        const lastSync = this.syncBase.lastSyncTimestamp;
        if (lastSync === 0) return; // First ever sync — skip, let pull handle it.

        // Filter candidates from in-memory cache — no async stat calls.
        const allStats = this.io.statBulk();
        if (this.syncObsidianConfig) {
            const obsidianFiles = await this.io.listObsidianConfig();
            for (const [p, s] of obsidianFiles) allStats.set(p, s);
        }
        const toHash: Array<{ path: string; stat: { mtime: number; size: number } }> = [];
        for (const [path, stat] of allStats) {
            if (this.isExcluded(path)) continue;
            if (stat.mtime <= lastSync) continue;
            const base = this.syncBase.getEntry(path);
            if (base && stat.mtime === base.mtime && stat.size === base.size) continue;
            toHash.push({ path, stat });
        }

        if (toHash.length === 0) return;

        const READ_CONCURRENCY = 4;
        const notice =
            toHash.length >= 20
                ? new Notice(`Obsetync: checking ${toHash.length} recently-touched files…`, 0)
                : null;
        let found = 0;
        for (let i = 0; i < toHash.length; i += READ_CONCURRENCY) {
            const batch = toHash.slice(i, i + READ_CONCURRENCY);
            const results = await Promise.all(
                batch.map(async ({ path, stat }) => {
                    const knownHash = this.syncBase.getHash(path);
                    if (stat.size >= LARGE_FILE_THRESHOLD) {
                        return { path, stat, hash: undefined as string | undefined, knownHash };
                    }
                    const hash = await hashFileStreaming(path, this.io, this.wasm);
                    if (hash === knownHash) return null;
                    return { path, stat, hash, knownHash };
                })
            );
            for (const r of results) {
                if (!r) continue;
                found++;
                const change: FileChange = {
                    action: r.knownHash ? "modified" : "created",
                    path: r.path,
                    mtime: r.stat.mtime,
                    size: r.stat.size,
                };
                if (r.hash !== undefined) change.hash = r.hash;
                this.pendingChanges.push(change);
            }
            await yieldToUI();
            const done = Math.min(i + READ_CONCURRENCY, toHash.length);
            this.onStatusUpdate(`⟳ scan ${done}/${toHash.length}`);
            notice?.setMessage(`Obsetync: mtime scan ${done}/${toHash.length} · ${found} changed`);
            this.progressHeartbeat("mtimeScan", `${done}/${toHash.length}, ${found} changed`);
        }
        notice?.hide();

        if (found > 0) {
            console.log(`[obsetync] mtime scan found ${found} unsynced changes`);
            await this.pushPending();
        }
    }

    /** Layer 1: attach live vault event listeners.
     *
     *  Events are journaled + queued even while a sync is in flight — the old
     *  `if (this.syncing) return` dropped genuine user edits made during a
     *  long pull. The one thing we must NOT queue is the pull's own disk
     *  writes echoing back as vault events; those are recognized via
     *  `expectedPullWrites` (registered before applyDeltas touches disk). */
    private attachVaultListeners(): void {
        this.debouncedPush = debounce(
            () => this.pushPending(),
            3000,
            true
        );
        const debouncedPush = () => this.debouncedPush?.();

        /** True → this event is our own pull writing to disk; consume it. */
        const isPullEcho = (path: string): boolean => {
            if (!this.syncing || !this.expectedPullWrites.has(path)) return false;
            this.expectedPullWrites.delete(path);
            return true;
        };

        this.eventRefs.push(
            this.app.vault.on("modify", async (file: TAbstractFile) => {
                if (!(file instanceof TFile)) return;
                if (this.isExcluded(file.path) || isPullEcho(file.path)) return;
                await this.journal.append({
                    action: "modified",
                    path: file.path,
                    ts: Date.now(),
                    synced: false,
                });
                const data = await this.io.readFile(file.path);
                const hash = streamingHash(this.wasm, data);
                this.pendingChanges.push({
                    action: "modified",
                    path: file.path,
                    hash,
                    data,
                    mtime: file.stat.mtime,
                    size: file.stat.size,
                });
                debouncedPush();
            })
        );

        this.eventRefs.push(
            this.app.vault.on("create", async (file: TAbstractFile) => {
                if (!(file instanceof TFile)) return;
                if (this.isExcluded(file.path) || isPullEcho(file.path)) return;
                await this.journal.append({
                    action: "created",
                    path: file.path,
                    ts: Date.now(),
                    synced: false,
                });
                const data = await this.io.readFile(file.path);
                const hash = streamingHash(this.wasm, data);
                this.pendingChanges.push({
                    action: "created",
                    path: file.path,
                    hash,
                    data,
                    mtime: file.stat.mtime,
                    size: file.stat.size,
                });
                debouncedPush();
            })
        );

        this.eventRefs.push(
            this.app.vault.on("delete", async (file: TAbstractFile) => {
                if (this.isExcluded(file.path) || isPullEcho(file.path)) return;
                await this.journal.append({
                    action: "deleted",
                    path: file.path,
                    ts: Date.now(),
                    synced: false,
                });
                this.pendingChanges.push({
                    action: "deleted",
                    path: file.path,
                });
                debouncedPush();
            })
        );

        this.eventRefs.push(
            this.app.vault.on("rename", async (file: TAbstractFile, oldPath: string) => {
                if (!(file instanceof TFile)) return;
                if (this.isExcluded(file.path)) return;
                // A pull-applied rename echoes as one event with both paths.
                const echoNew = isPullEcho(file.path);
                const echoOld = isPullEcho(oldPath);
                if (echoNew || echoOld) return;
                await this.journal.append({
                    action: "deleted",
                    path: oldPath,
                    ts: Date.now(),
                    synced: false,
                });
                await this.journal.append({
                    action: "created",
                    path: file.path,
                    ts: Date.now(),
                    synced: false,
                });
                const data = await this.io.readFile(file.path);
                const hash = streamingHash(this.wasm, data);
                this.pendingChanges.push({ action: "deleted", path: oldPath });
                this.pendingChanges.push({
                    action: "created",
                    path: file.path,
                    hash,
                    data,
                    mtime: file.stat.mtime,
                    size: file.stat.size,
                });
                debouncedPush();
            })
        );
    }

    private async saveCachedRoot(): Promise<void> {
        const rootBytes = this.tree.root_bytes();
        if (!rootBytes) return;
        const path = ".obsidian/plugins/obsetync/cached-root.bin";
        try {
            await this.app.vault.adapter.writeBinary(path, rootBytes.buffer as ArrayBuffer);
            console.log("[obsetync] cached root saved");
        } catch (e) {
            console.warn("[obsetync] failed to save cached root:", e);
        }
    }

    private isSyncInternal(path: string): boolean {
        return (
            path.startsWith(".obsidian/plugins/obsetync/") ||
            path.includes("chunk-cache/")
        );
    }

    /** User-configured ignore (Slice 2). */
    isIgnored(path: string): boolean {
        return this.ignore.test(path);
    }

    /** A path that must never enter sync from THIS device: the plugin's own
     *  internal files, or a user-ignored path. Used at every write-detection
     *  chokepoint (vault events, full scan, mtime scan). Applying it in the
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
