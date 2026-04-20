import { App, TAbstractFile, TFile, debounce, Notice } from "obsidian";

/** Yield control to the JS event loop (audio, render, IPC callbacks). */
const yieldToUI = () => new Promise<void>(r => setTimeout(r, 0));

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
import { SyncApi } from "./api";
import { PlatformIO } from "./platform";
import { SyncBase } from "./sync-base";
import { Journal, JournalEntry } from "./journal";
import { pull } from "./pull";
import { push, hashFileStreaming, streamingHash, FileChange, WasmModule, WasmTree } from "./push";
import { SyncPriority } from "./settings";

export type SyncState = "idle" | "pulling" | "pushing" | "scanning" | "error";

/**
 * Core sync orchestrator. Coordinates pull, push, journal recovery,
 * mtime scanning, and live vault event tracking (D-005 4-layer system).
 */
export class SyncEngine {
    private state: SyncState = "idle";
    private localRootHash: string | null;
    private pendingChanges: FileChange[] = [];
    private syncing = false;
    private syncTimer: ReturnType<typeof setInterval> | null = null;
    private eventRefs: any[] = [];
    /** Most recent pull/push failure — surfaced by the debug panel. */
    private lastError: { ts: number; message: string; origin: string } | null = null;
    /** Snapshots of observed remote / local roots for the debug panel. */
    private lastPullServerRoot: string | null = null;

    constructor(
        private app: App,
        private api: SyncApi,
        private io: PlatformIO,
        private syncBase: SyncBase,
        private journal: Journal,
        private wasm: WasmModule,
        private tree: WasmTree,
        private vaultId: string,
        private syncInterval: number = 30000,
        private syncPriority: SyncPriority = "sequential",
        private onStatusUpdate: (text: string) => void = () => {},
        initialRootHash: string | null = null,
        private syncObsidianConfig: boolean = false,
    ) {
        this.localRootHash = initialRootHash;
    }

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

    /** Start the sync engine: run startup sequence, attach listeners, start timer. */
    async start(): Promise<void> {
        console.log("[obsetync] starting sync engine");

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

        // 5. Start periodic pull timer.
        console.log("[obsetync] ready");
        this.syncTimer = setInterval(() => {
            this.pullRemote().catch((e) =>
                console.error("[obsetync] periodic pull error:", e)
            );
        }, this.syncInterval);
    }

    /** Stop the sync engine. */
    stop(): void {
        if (this.syncTimer) {
            clearInterval(this.syncTimer);
            this.syncTimer = null;
        }
        for (const ref of this.eventRefs) {
            this.app.vault.offref(ref);
        }
        this.eventRefs = [];
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
        await this.pullRemote();
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
        await this.pushPending();
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

        // Populate the WASM tree from sync-base so wasm_tree_chunk_hashes
        // reflects the actual index-chunk set the server should have. Same
        // bootstrap push.ts does on first call.
        if (!this.tree.root_hash_hex()) {
            const paths = this.syncBase.allPaths();
            if (paths.length > 0) {
                const entries = paths.map(p => {
                    const e = this.syncBase.getEntry(p)!;
                    return { path: p, hash: e.hash, mtime_ms: e.mtime, size: e.size };
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

        // --- Step 1: which tree chunks (index) is the server missing?
        const treeHashes = this.wasm.wasm_tree_chunk_hashes(this.tree);
        const missingTreeChunks = treeHashes.length > 0
            ? await this.api.checkChunks(treeHashes)
            : [];

        // --- Step 2: which small-file contents is the server missing?
        const smallHashes = [...smallHashToPath.keys()];
        const CHECK_BATCH = 1000;
        const missingSmall: string[] = [];
        for (let i = 0; i < smallHashes.length; i += CHECK_BATCH) {
            const batch = smallHashes.slice(i, i + CHECK_BATCH);
            const missing = await this.api.checkContent(batch);
            missingSmall.push(...missing);
            progress(`reconcile: checked ${Math.min(i + CHECK_BATCH, smallHashes.length)}/${smallHashes.length}`);
        }

        const totalMissing = missingTreeChunks.length + missingSmall.length + largeHashToPath.size;
        if (totalMissing === 0) {
            progress("reconcile: server in parity");
            return { smallUploaded: 0, largeUploaded: 0, treeChunksUploaded: 0, bytes: 0 };
        }

        console.log(
            `[obsetync] reconcile: server missing ${missingSmall.length} small files, ` +
            `${missingTreeChunks.length} tree chunks; ${largeHashToPath.size} large files to verify`
        );

        const notice = totalMissing >= 20
            ? new Notice(`Re-uploading ${missingSmall.length} files to server...`, 0)
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

        // --- Step 5: large files — manifest + sub-file chunks.
        let largeIdx = 0;
        for (const [hash, path] of largeHashToPath) {
            largeIdx++;
            progress(`reconcile: large file ${largeIdx}/${largeHashToPath.size}`);
            notice?.setMessage(`Re-uploading large file ${largeIdx}/${largeHashToPath.size}`);
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

    /** Force a full vault scan (Layer 4). */
    async fullScan(): Promise<void> {
        this.state = "scanning";
        this.onStatusUpdate("sync ⟳");
        const notice = new Notice("Scanning vault...", 0);
        console.log("[obsetync] full scan started");

        try {
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
                if (this.isSyncInternal(path)) continue;
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

                if (pending.length >= FLUSH_BATCH) {
                    notice.setMessage(`Syncing... ${Math.min(i + READ_CONCURRENCY, toHash.length)}/${toHash.length}`);
                    await flushPending();
                }
            }
            await flushPending();

            // Phase 3: deletions — files in sync-base that no longer exist.
            for (const path of this.syncBase.allPaths()) {
                if (!statMap.has(path) && !this.isSyncInternal(path)) {
                    pending.push({ action: "deleted", path });
                    totalChanges++;
                }
            }
            await flushPending();

            console.log(`[obsetync] full scan complete: ${totalChanges} changes`);
        } finally {
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

        try {
            const result = await pull(
                this.api,
                this.io,
                this.syncBase,
                this.vaultId,
                this.localRootHash,
                this.wasm,
            );
            if (result.newRootHash) {
                this.localRootHash = result.newRootHash;
                this.lastPullServerRoot = result.newRootHash;
            }
            // Persist the raw root bytes so cached-root.bin seeds
            // localRootHash on the next plugin start — otherwise a fresh
            // iPhone would re-seed every launch.
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
        } catch (e: any) {
            console.error("[obsetync] pull error:", e);
            this.lastError = { ts: Date.now(), origin: "pull", message: String(e?.message ?? e) };
            this.state = "error";
            this.onStatusUpdate("sync ✗");
        } finally {
            this.syncing = false;
            if (this.state !== "error") {
                this.state = "idle";
                this.onStatusUpdate("sync ✓");
            }
        }
    }

    private async pushPending(): Promise<void> {
        if (this.syncing || this.pendingChanges.length === 0) return;
        this.syncing = true;
        this.state = "pushing";

        const batch = sortByPriority(this.pendingChanges.splice(0), this.syncPriority);
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
                this.localRootHash,
                (text) => {
                    this.onStatusUpdate(text);
                    notice?.setMessage(text);
                }
            );
            if (result.newRootHash) {
                this.localRootHash = result.newRootHash;
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
            notice?.hide();
            this.syncing = false;
            if (this.state !== "error") {
                this.state = "idle";
                this.onStatusUpdate("sync ✓");
            }
        }
    }

    /** Layer 2: recover unsynced entries from the persistent journal. */
    private async recoverFromJournal(): Promise<void> {
        const unsynced = this.journal.unsynced();
        if (unsynced.length === 0) return;

        console.log(
            `[obsetync] recovering ${unsynced.length} changes from journal`
        );

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
        }

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
            if (this.isSyncInternal(path)) continue;
            if (stat.mtime <= lastSync) continue;
            const base = this.syncBase.getEntry(path);
            if (base && stat.mtime === base.mtime && stat.size === base.size) continue;
            toHash.push({ path, stat });
        }

        if (toHash.length === 0) return;

        const READ_CONCURRENCY = 4;
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
        }

        if (found > 0) {
            console.log(`[obsetync] mtime scan found ${found} unsynced changes`);
            await this.pushPending();
        }
    }

    /** Layer 1: attach live vault event listeners. */
    private attachVaultListeners(): void {
        const debouncedPush = debounce(
            () => this.pushPending(),
            3000,
            true
        );

        this.eventRefs.push(
            this.app.vault.on("modify", async (file: TAbstractFile) => {
                if (!(file instanceof TFile)) return;
                if (this.syncing || this.isSyncInternal(file.path)) return;
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
                if (this.syncing || this.isSyncInternal(file.path)) return;
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
                if (this.syncing || this.isSyncInternal(file.path)) return;
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
                if (this.syncing || this.isSyncInternal(file.path)) return;
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
