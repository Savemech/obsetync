import { Plugin, Notice } from "obsidian";
import { createPlatformIO, PlatformIO } from "./platform";
import { SyncApi } from "./api";
import { SyncBase } from "./sync-base";
import { Journal } from "./journal";
import { SyncEngine } from "./sync";
import { SyncSettings, DEFAULT_SETTINGS, SyncSettingTab } from "./settings";
import { ConflictModal, findConflicts } from "./conflict-ui";
import { debugLog } from "./debug-log";
import type { WasmModule, WasmTree } from "./push";

export default class SyncPlugin extends Plugin {
    settings: SyncSettings = DEFAULT_SETTINGS;
    private io!: PlatformIO;
    private api!: SyncApi;
    private syncBase!: SyncBase;
    private journal!: Journal;
    private syncEngine!: SyncEngine;
    private wasm!: WasmModule;
    private tree!: WasmTree;
    private statusBarEl: HTMLElement | null = null;

    async onload(): Promise<void> {
        // Capture every subsequent `[obsetync] …` console line into a ring
        // buffer so the "Show debug info" panel can surface them later,
        // especially on iOS where there's no easy way to see console output.
        debugLog.install();

        await this.loadSettings();

        // Platform I/O.
        this.io = createPlatformIO(this.app);

        // Persistence layers.
        this.syncBase = new SyncBase(this.app);
        await this.syncBase.load();
        this.journal = new Journal(this.app);
        await this.journal.load();

        // Settings tab.
        this.addSettingTab(new SyncSettingTab(this.app, this));

        // Commands.
        this.addCommand({
            id: "sync-now",
            name: "Sync now",
            callback: () => this.syncNow(),
        });

        this.addCommand({
            id: "full-rescan",
            name: "Full vault rescan",
            callback: () => this.fullScan(),
        });

        this.addCommand({
            id: "show-conflicts",
            name: "Show sync conflicts",
            callback: () => this.showConflicts(),
        });

        // Status bar.
        this.statusBarEl = this.addStatusBarItem();
        this.updateStatusBar("sync ✓");

        // Start sync if enrolled.
        if (this.settings.enrolled && this.settings.serverUrl) {
            // Defer to let Obsidian finish loading.
            this.app.workspace.onLayoutReady(() => {
                this.initSync().catch((e) => {
                    console.error("[obsetync] init failed:", e);
                    this.updateStatusBar("sync ✗");
                });
            });
        }
    }

    onunload(): void {
        this.syncEngine?.stop();
        debugLog.uninstall();
    }

    /** Gathers a human-readable snapshot of plugin state + live diagnostics. */
    async getDebugInfo(): Promise<string> {
        const lines: string[] = [];
        const push = (s: string) => lines.push(s);
        const fmt = (ms: number) => (ms ? new Date(ms).toISOString() : "never");
        const trunc = (s: string | null | undefined, n = 16) =>
            !s ? "—" : s.length <= n ? s : s.slice(0, n) + "…";

        push(`=== ObsetyNC ${this.manifest.version} debug info ===`);
        push(`Captured: ${new Date().toISOString()}`);
        push("");

        push("--- Settings ---");
        push(`Server URL:        ${this.settings.serverUrl || "(unset)"}`);
        push(`Vault ID:          ${this.settings.vaultId || "(unset)"}`);
        push(`Device name:       ${this.settings.deviceName || "(unset)"}`);
        push(`Enrolled:          ${this.settings.enrolled}`);
        push(`Fingerprint:       ${trunc(this.settings.fingerprint, 24)}`);
        push(`Bearer token:      ${this.settings.bearerToken ? "present" : "MISSING"}`);
        push(`Device cert+key:   ${this.settings.certPem && this.settings.keyPem ? "present" : "missing"}`);
        push(`Sync interval:     ${this.settings.syncIntervalMs}ms`);
        push(`Sync priority:     ${this.settings.syncPriority}`);
        push(`Sync .obsidian/:   ${this.settings.syncObsidianConfig}`);
        push(`Auto-sync:         ${this.settings.autoSync}`);
        push("");

        push("--- Platform ---");
        const isNode = typeof (window as any).require === "function";
        push(`Transport mode:    ${isNode ? "Node.js https (desktop)" : "requestUrl (mobile)"}`);
        push(`WASM:              ${this.wasm ? "loaded" : "not loaded"}`);
        push(`Plugin id:         ${this.manifest.id}`);
        push(`Plugin version:    ${this.manifest.version}`);
        push("");

        if (this.syncEngine) {
            push("--- Sync state ---");
            try {
                push(`Engine state:      ${this.syncEngine.getState()}`);
                push(`Local root hash:   ${trunc(this.syncEngine.getLocalRootHash(), 24)}`);
                push(`Last server root:  ${trunc(this.syncEngine.getLastObservedServerRoot(), 24)}`);
                push(`sync-base entries: ${this.syncEngine.getSyncBaseCount()}`);
                push(`Vault file count:  ${this.syncEngine.getVaultFileCount()}`);
                push(`Last sync (ts):    ${fmt(this.syncEngine.getLastSyncTimestamp())}`);
                const err = this.syncEngine.getLastError();
                if (err) {
                    push(`Last error:        [${err.origin}] ${err.message}`);
                    push(`  at:              ${fmt(err.ts)}`);
                } else {
                    push(`Last error:        none`);
                }
            } catch (e: any) {
                push(`Sync state read failed: ${e?.message ?? e}`);
            }
            push("");
        } else {
            push("--- Sync state ---");
            push("Sync engine not initialized yet (check enrollment).");
            push("");
        }

        push("--- Live diagnostics ---");
        if (!this.api) {
            push("SyncApi not ready.");
        } else {
            try {
                push("ping() → ...");
                const p = await this.api.ping();
                push(`  TLS:              ${p.tlsVersion} / ${p.cipher}`);
                push(`  Server fp:        ${p.serverFingerprint}`);
                push(`  Device cert sent: ${p.deviceCert}`);
            } catch (e: any) {
                push(`  ping failed:      ${e?.message ?? e}`);
            }
            if (this.settings.vaultId) {
                try {
                    push(`getRoot("${this.settings.vaultId}") → ...`);
                    const rootBytes = await this.api.getRoot(this.settings.vaultId);
                    if (rootBytes === null) {
                        push(`  Server has no vault with this ID.`);
                    } else {
                        const hash = this.wasm?.wasm_root_hash_from_bytes(rootBytes) ?? null;
                        push(`  Server root hash: ${trunc(hash, 24)}`);
                        push(`  Root bytes:       ${rootBytes.length} B`);
                    }
                } catch (e: any) {
                    push(`  getRoot failed:   ${e?.message ?? e}`);
                }
            }
        }
        push("");

        push(`--- Recent log lines (up to ${debugLog.recent().length}) ---`);
        const logs = debugLog.recent();
        if (logs.length === 0) {
            push("(none yet)");
        } else {
            for (const line of logs) push(line);
        }

        return lines.join("\n");
    }

    async loadSettings(): Promise<void> {
        this.settings = Object.assign(
            {},
            DEFAULT_SETTINGS,
            await this.loadData()
        );
    }

    async saveSettings(): Promise<void> {
        await this.saveData(this.settings);
    }

    /** Enroll this device with the server using an enrollment code. */
    async enroll(code: string): Promise<void> {
        const tempApi = new SyncApi(this.settings.serverUrl);
        const result = await tempApi.claimEnrollment(code);

        this.settings.certPem      = result.cert_pem;
        this.settings.keyPem       = result.key_pem;
        this.settings.fingerprint  = result.fingerprint;
        this.settings.bearerToken  = result.bearer_token;
        this.settings.enrolled     = true;
        await this.saveSettings();

        // Re-initialize sync with the new credentials.
        await this.initSync();
    }

    async syncNow(): Promise<void> {
        if (!this.syncEngine) {
            new Notice("Sync not initialized. Check settings.");
            return;
        }
        try {
            await this.syncEngine.forceSync();
        } catch (e: any) {
            this.updateStatusBar("sync ✗");
            throw e;
        }
    }

    async fullScan(): Promise<void> {
        if (!this.syncEngine) {
            new Notice("Sync not initialized. Check settings.");
            return;
        }
        try {
            await this.syncEngine.fullScan();
        } catch (e: any) {
            this.updateStatusBar("sync ✗");
            throw e;
        }
    }

    private showConflicts(): void {
        const conflicts = findConflicts(this.io);
        if (conflicts.length === 0) {
            new Notice("No sync conflicts found.");
            return;
        }
        new ConflictModal(this.app, this.io, conflicts, () => {
            new Notice("All conflicts resolved.");
        }).open();
    }

    private async initSync(): Promise<void> {
        // Stop existing engine if re-initializing.
        this.syncEngine?.stop();

        // Create API client with mTLS credentials.
        this.api = new SyncApi(
            this.settings.serverUrl,
            this.settings.certPem,
            this.settings.keyPem,
            this.settings.bearerToken,
        );

        // Load WASM module.
        // TODO: implement proper WASM loading from plugin directory.
        // For now, create a stub that will be replaced when wasm-pack output is available.
        this.wasm = await this.loadWasm();

        // Create WASM tree.
        this.tree = new this.wasm.WasmTree(
            this.settings.vaultId,
            this.settings.deviceName
        );

        // Extract the cached root hash for X-Parent-Root on the first push after restart.
        // We do NOT call tree.load_root() here: load_root only stores the root node in
        // MemoryChunkStore, NOT its children (LeafChunk/InternalNode). Calling update_entry
        // on such a tree triggers update_tree → load_all_entries(store, child_hash) →
        // ChunkError::NotFound. The tree always bootstraps from sync-base on first push
        // (push.ts: if (!tree.root_hash_hex())), which correctly populates the full store.
        let cachedRootHash: string | null = null;
        const cachedRoot = await this.loadCachedRoot();
        if (cachedRoot) {
            try {
                cachedRootHash = this.wasm.wasm_root_hash_from_bytes(cachedRoot) ?? null;
                console.log("[obsetync] cached root hash:", cachedRootHash?.slice(0, 12));
            } catch (e) {
                console.warn("[obsetync] failed to read cached root hash:", e);
            }
        }

        // Create sync engine.
        this.syncEngine = new SyncEngine(
            this.app,
            this.api,
            this.io,
            this.syncBase,
            this.journal,
            this.wasm,
            this.tree,
            this.settings.vaultId,
            this.settings.syncIntervalMs,
            this.settings.syncPriority,
            (text) => this.updateStatusBar(text),
            cachedRootHash,
            this.settings.syncObsidianConfig,
        );

        // Start.
        this.updateStatusBar("sync ↓");
        await this.syncEngine.start();
        this.updateStatusBar("sync ✓");
    }

    private async loadWasm(): Promise<WasmModule> {
        // WASM files live flat alongside main.js — not under wasm/ — so that
        // BRAT and manual installs can drop a single folder into .obsidian/plugins/
        // without needing to preserve subdirectories.
        try {
            const wasmPath = this.app.vault.adapter.getResourcePath(
                ".obsidian/plugins/obsetync/sync_core.js"
            );
            const mod = await import(/* webpackIgnore: true */ wasmPath);
            await mod.default(); // Initialize WASM.
            return mod;
        } catch (e) {
            console.warn(
                "[obsetync] WASM not available, using stub. Build WASM with wasm-pack first.",
                e
            );
            return createWasmStub();
        }
    }

    private async loadCachedRoot(): Promise<Uint8Array | null> {
        const path = ".obsidian/plugins/obsetync/cached-root.bin";
        try {
            const buf = await this.app.vault.adapter.readBinary(path);
            return new Uint8Array(buf);
        } catch {
            return null;
        }
    }

    private updateStatusBar(text: string): void {
        this.statusBarEl?.setText(text);
    }
}

/** Stub WASM module for development when WASM isn't built yet. */
function createWasmStub(): WasmModule {
    return {
        wasm_hash(_data: Uint8Array): string {
            // Fallback: use a simple JS hash. Not blake3, just for dev.
            let hash = 0;
            for (let i = 0; i < _data.length; i++) {
                hash = (hash * 31 + _data[i]) | 0;
            }
            return hash.toString(16).padStart(64, "0");
        },
        wasm_should_chunk(size: number): boolean {
            return size >= 1_048_576;
        },
        wasm_chunk_file(_data: Uint8Array): any {
            return { file_hash: "stub", total_size: _data.length, chunks: [] };
        },
        wasm_get_file_chunk(
            data: Uint8Array,
            offset: number,
            size: number
        ): Uint8Array {
            return data.slice(offset, offset + size);
        },
        wasm_tree_get_chunk(_tree: any, _hash: string): Uint8Array | null {
            return null;
        },
        wasm_tree_chunk_hashes(_tree: any): string[] {
            return [];
        },
        wasm_root_hash_from_bytes(_bytes: Uint8Array): string | undefined {
            return undefined;
        },
        wasm_hash_batch(data: Uint8Array, offsets: Uint32Array, sizes: Uint32Array): string[] {
            return Array.from({ length: offsets.length }, (_, i) => {
                const start = offsets[i];
                const end   = start + sizes[i];
                let hash = 0;
                for (let b = start; b < end; b++) hash = (hash * 31 + data[b]) | 0;
                return hash.toString(16).padStart(64, "0");
            });
        },
        Hasher: class {
            private chunks: Uint8Array[] = [];
            update(chunk: Uint8Array): void { this.chunks.push(chunk); }
            finalize(): string {
                // Stub: XOR bytes for a deterministic (non-blake3) result.
                let h = 0;
                for (const c of this.chunks) for (const b of c) h = (h * 31 + b) | 0;
                return h.toString(16).padStart(64, "0");
            }
            free(): void { this.chunks = []; }
        } as any,
        WasmTree: class {
            constructor(_vaultId: string, _deviceId: string) {}
            load_root(_rootBytes: Uint8Array): void {}
            root_hash_hex(): string | null {
                return null;
            }
            root_bytes(): Uint8Array | null {
                return null;
            }
            total_files(): number {
                return 0;
            }
            update_entry(
                _path: string,
                _hash: string,
                _mtime: number,
                _size: number
            ): void {
                return;
            }
            delete_entry(_path: string): void {
                return;
            }
            build_from_entries(_entriesJson: string): void {
                return;
            }
            update_batch(_entriesJson: string): void {
                return;
            }
            delete_batch(_pathsJson: string): void {
                return;
            }
        } as any,
    };
}
