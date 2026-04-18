import { App, TAbstractFile, TFile, debounce, Notice } from "obsidian";

/** Yield control to the JS event loop (audio, render, IPC callbacks). */
const yieldToUI = () => new Promise<void>(r => setTimeout(r, 0));

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

    /** Start the sync engine: run startup sequence, attach listeners, start timer. */
    async start(): Promise<void> {
        console.log("[obsetync] starting sync engine");

        // Step 0: Verify connectivity and log TLS details.
        try {
            const conn = await this.api.ping();
            console.log(
                `[obsetync] ✓ connected to ${conn.serverUrl} | ` +
                `${conn.tlsVersion} | cipher: ${conn.cipher} | ` +
                `device cert: ${conn.deviceCert ? "yes" : "no"} | ` +
                `server: ${conn.serverFingerprint}`
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

    /** Force a full sync cycle (pull then push pending). */
    async forceSync(): Promise<void> {
        await this.pullRemote();
        await this.pushPending();
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
                this.localRootHash
            );
            if (result.newRootHash) {
                this.localRootHash = result.newRootHash;
            }
        } catch (e) {
            console.error("[obsetync] pull error:", e);
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
        } catch (e) {
            console.error("[obsetync] push error:", e);
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
