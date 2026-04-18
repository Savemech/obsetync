import { Plugin, Notice } from "obsidian";
import { createPlatformIO, PlatformIO } from "./platform";
import { SyncApi } from "./api";
import { SyncBase } from "./sync-base";
import { Journal } from "./journal";
import { SyncEngine } from "./sync";
import { SyncSettings, DEFAULT_SETTINGS, SyncSettingTab } from "./settings";
import { ConflictModal, findConflicts } from "./conflict-ui";
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
        // Attempt to load the WASM module from the plugin directory.
        // The wasm-pack output should be at plugin/wasm/sync_core.js + sync_core_bg.wasm.
        try {
            // In Obsidian's Electron environment, we can use dynamic import.
            const wasmPath = this.app.vault.adapter.getResourcePath(
                ".obsidian/plugins/obsetync/wasm/sync_core.js"
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
