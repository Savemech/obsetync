import { Plugin, Notice, Platform } from "obsidian";
import { createPlatformIO, PlatformIO } from "./platform";
import { ObsetyncApi } from "./api";
import { ObsetyncSyncBase } from "./sync-base";
import { ObsetyncJournal } from "./journal";
import { ObsetyncSyncEngine } from "./sync";
import { SyncSettings, DEFAULT_SETTINGS, ObsetyncSettingTab } from "./settings";
import { ObsetyncConflictModal, findConflicts } from "./conflict-ui";
import { debugLog, crashLog, perfSpan } from "./debug-log";
import { OperationCheckpoint } from "./operation-checkpoint";
import type { WasmModule, WasmTree } from "./push";
import { migrateLegacyDefaultIgnorePatterns } from "./ignore";
import {
    normalizePerfArchitecture,
    perfTrace,
} from "./perf-trace";
import { configureHashRuntime, type HashTuning } from "./hash-runtime";
import { createWasmLoader, type WasmSelection } from "./wasm-runtime";
import {
    createDesktopHashWorkerPool,
    type DesktopHashWorkerPool,
} from "./desktop-hash-workers";
import hashWorkerSource from "obsetync-hash-worker-source";

// Static import of the wasm-bindgen --target web glue. esbuild inlines this
// ES module into main.js at build time — no runtime `new Function(...)` or
// dynamic import() is ever executed. That's what unblocks iOS WKWebView,
// whose strict CSP (no unsafe-eval) rejects the old `new Function(glueText)`
// approach used by the --target no-modules output.
//
// `@ts-ignore` because the generated sync_core.d.ts is also part of the
// wasm/ artifacts but isn't guaranteed present during every build environment.
// The runtime shape matches WasmModule structurally.
// @ts-ignore
import initScalarWasm, * as ScalarWasmExports from "../wasm/sync_core";
// @ts-ignore
import initSimdWasm, * as SimdWasmExports from "../wasm/sync_core_simd";

// Static import of the WASM binary — esbuild's "binary" loader (configured
// in esbuild.config.mjs) turns this into a base64-embedded Uint8Array at
// build time. The plugin ships as a single self-contained main.js, with no
// separate scalar/SIMD binaries for BRAT/Obsidian to (fail to) deliver.
// @ts-ignore
import scalarWasmBytes from "../wasm/sync_core_bg.wasm";
// @ts-ignore
import simdWasmBytes from "../wasm/sync_core_simd_bg.wasm";
const embeddedScalarWasmBytes = scalarWasmBytes as unknown as Uint8Array;
const embeddedSimdWasmBytes = simdWasmBytes as unknown as Uint8Array;

function formatDebugBytes(bytes: number): string {
    if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(0)} MiB`;
    if (bytes >= 1024) return `${(bytes / 1024).toFixed(0)} KiB`;
    return `${bytes} B`;
}

export default class ObsetyncPlugin extends Plugin {
    settings: SyncSettings = DEFAULT_SETTINGS;
    private io!: PlatformIO;
    private api!: ObsetyncApi;
    private syncBase!: ObsetyncSyncBase;
    private journal!: ObsetyncJournal;
    private syncEngine!: ObsetyncSyncEngine;
    private wasm!: WasmModule;
    private tree!: WasmTree;
    private wasmLoader: (() => Promise<WasmSelection<WasmModule>>) | null = null;
    private hashWorkers: DesktopHashWorkerPool | null = null;
    private operationCheckpoint!: OperationCheckpoint;
    private statusBarEl: HTMLElement | null = null;

    async onload(): Promise<void> {
        const runtime = Platform.isMobile ? "mobile" : "desktop";
        const detectedArchitecture = normalizePerfArchitecture(
            Platform.isMobile
                ? "arm64"
                : (globalThis as any).process?.arch ??
                    (globalThis.navigator as any)?.userAgentData?.architecture,
        );
        const applyHashTuning = (tuning: HashTuning) => {
            const current = perfTrace.getProfile();
            perfTrace.setProfile({
                ...current,
                readConcurrency: tuning.readConcurrency,
                feedBytes: tuning.feedBytes,
                batchBytes: tuning.maxBatchBytes,
            });
        };
        const hashTuning = configureHashRuntime(runtime, applyHashTuning);
        perfTrace.setProfile({
            runtime,
            architecture: detectedArchitecture,
            wasmMode: "unknown",
            hashConcurrency: 1,
            readConcurrency: hashTuning.readConcurrency,
            networkConcurrency: 8,
            feedBytes: hashTuning.feedBytes,
            batchBytes: hashTuning.maxBatchBytes,
            diffPageBytes: 0,
        });

        // Capture every subsequent `[obsetync] …` console line into a ring
        // buffer so the "Show debug info" panel can surface them later,
        // especially on iOS where there's no easy way to see console output.
        debugLog.install();

        // Persist window-level errors to .obsetync-crash.log in the vault
        // root. This captures JavaScript failures; the separate durable
        // operation checkpoint diagnoses OS-level renderer kills/Jetsam.
        crashLog.install(this.app, this.manifest.version);

        await this.loadSettings();

        // Platform I/O.
        this.io = createPlatformIO(this.app);
        this.operationCheckpoint = new OperationCheckpoint(
            this.io,
            this.manifest.version,
        );
        await this.operationCheckpoint.initialize();

        // Persistence layers.
        this.syncBase = new ObsetyncSyncBase(this.app);
        await this.syncBase.load();
        this.journal = new ObsetyncJournal(this.app);
        await this.journal.load();

        // Settings tab.
        this.addSettingTab(new ObsetyncSettingTab(this.app, this));

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
        // Engine stop first (crash logger still armed while it runs), but
        // never let it skip the listener teardown below.
        try {
            this.syncEngine?.stop();
            void this.hashWorkers?.close();
            this.hashWorkers = null;
        } catch (e) {
            console.warn("[obsetync] engine stop failed during unload:", e);
        }
        crashLog.uninstall();
        debugLog.uninstall();
    }

    /** Expose the sync engine for the settings tab's status box. Returns null
     *  if not yet initialized (e.g., before enrollment). */
    syncEngineOrNull(): ObsetyncSyncEngine | null {
        return this.syncEngine ?? null;
    }

    /** Expose the API client for the settings tab (history/rollback UI). */
    apiOrNull(): ObsetyncApi | null {
        return this.api ?? null;
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
        push(`Device ID:         ${trunc(this.settings.deviceId, 24)}`);
        push(`Bearer token:      ${this.settings.bearerToken ? "present" : "MISSING"}`);
        push(`Server box pubkey: ${trunc(this.settings.serverBoxPub, 24)}`);
        push(`HTTP wire:         ${this.settings.wireVersion || "not enrolled for v2"}`);
        push(`Server eph valid:  ${this.settings.esPubValidUntil ? new Date(this.settings.esPubValidUntil * 1000).toISOString() : "missing"}`);
        push(`Sync interval:     ${this.settings.syncIntervalMs}ms`);
        push(`Sync priority:     ${this.settings.syncPriority}`);
        push(`Sync .obsidian/:   ${this.settings.syncObsidianConfig}`);
        push(`Ignore patterns:   ${this.settings.ignorePatterns.length} (${this.settings.ignorePatterns.slice(0, 4).join(", ")}${this.settings.ignorePatterns.length > 4 ? ", …" : ""})`);
        push(`Auto-sync:         ${this.settings.autoSync}`);
        push("");

        push("--- Platform ---");
        push(`Transport:         AEAD envelope over HTTP (X25519 + HKDF-SHA256 + AES-256-GCM)`);
        push(`WASM:              ${this.wasm ? `loaded (${perfTrace.getProfile().wasmMode})` : "not loaded"}`);
        const workerStats = this.hashWorkers?.stats();
        push(`Hash workers:      ${workerStats ? `${workerStats.ready}/${workerStats.workers} ready · ${workerStats.active} active · ${workerStats.queued} queued` : "renderer fallback"}`);
        push(`Plugin id:         ${this.manifest.id}`);
        push(`Plugin version:    ${this.manifest.version}`);
        push("");

        push("--- Performance (aggregate, path-free) ---");
        for (const line of perfTrace.formatDebug()) push(line);
        push("");

        push("--- Previous interruption ---");
        const interruption = this.operationCheckpoint?.getLastInterruption();
        if (interruption) {
            push(`Phase:             ${interruption.phase}`);
            push(`Last checkpoint:   ${fmt(interruption.updatedAt)}`);
            push(`Started:           ${fmt(interruption.startedAt)}`);
            push(`Plugin version:    ${interruption.pluginVersion}`);
            push(`Progress:          ${interruption.detail || "(none)"}`);
            push(
                "Meaning:           renderer stopped before this phase returned " +
                "(on iOS, usually memory-pressure/Jetsam)",
            );
        } else {
            push("No orphaned operation checkpoint found.");
        }
        push("");

        if (this.syncEngine) {
            push("--- Sync state ---");
            try {
                const treeRoot   = this.syncEngine.getTreeRootHash();
                const baseRoot   = this.syncEngine.getTreeBaseRoot();
                const serverRoot = this.syncEngine.getLastObservedServerRoot();
                // Truth = the TREE's own root vs the server, not one server-
                // derived value vs another (the pre-1.4.0 display compared
                // observed-vs-observed and read "in sync" while the tree
                // silently drifted for days).
                const inSync = !!treeRoot && treeRoot === serverRoot;
                const treeCount = this.syncEngine.getTreeFileCount();
                const baseCount = this.syncEngine.getSyncBaseCount();
                push(`Engine state:      ${this.syncEngine.getState()}`);
                push(`In sync (tree):    ${inSync ? "yes ✓" : "no"}`);
                push(`Push blocked:      ${this.syncEngine.isPushBlocked() ? "YES — run Full Rescan" : "no"}`);
                push(`Re-enroll needed:  ${this.syncEngine.isReenrollmentRequired() ? "YES — automatic sync paused" : "no"}`);
                push(`Bulk change review: ${this.syncEngine.isBulkChangeReviewRequired() ? "YES — run Full Rescan to confirm" : "no"}`);
                push(`Tree root hash:    ${trunc(treeRoot, 24)}`);
                push(`Tree base root:    ${trunc(baseRoot, 24)}`);
                push(`Last server root:  ${trunc(serverRoot, 24)}`);
                push(`Observed root:     ${trunc(this.syncEngine.getLocalRootHash(), 24)}`);
                push(`Tree files:        ${treeCount < 0 ? "(not bootstrapped)" : treeCount}`);
                push(`sync-base entries: ${baseCount}`);
                push(`Vault file count:  ${this.syncEngine.getVaultFileCount()}`);
                push(`Last sync (ts):    ${fmt(this.syncEngine.getLastSyncTimestamp())}`);
                const wsState = this.syncEngine.getWsState();
                const wsAge = this.syncEngine.getWsLastFrameAgeMs();
                push(
                    `Realtime (WS):     ${wsState}` +
                    (wsState === "connected" && wsAge >= 0
                        ? ` · last frame ${Math.round(wsAge / 1000)}s ago`
                        : ""),
                );
                const fleet = this.syncEngine.getPresence();
                if (fleet.length > 0) {
                    push(`Fleet presence:`);
                    for (const p of fleet) {
                        push(
                            `  ${p.name.padEnd(16)} ${p.state.padEnd(7)} ` +
                            `${p.file ?? "(no file)"}`
                        );
                    }
                } else {
                    push(`Fleet presence:    (nobody else online)`);
                }
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
            push("ObsetyncApi not ready.");
        } else {
            try {
                push("ping() → ...");
                const p = await this.api.ping();
                push(`  Server URL:       ${p.serverUrl}`);
                push(`  Reachable:        ${p.ok ? "yes" : "no"}`);
                push(`  Transport:        ${p.transport}`);
            } catch (e: any) {
                push(`  ping failed:      ${e?.message ?? e}`);
            }
            try {
                const bulk = await this.api.getBulkDiagnostics();
                push(
                    `  Bulk HTTP:        ${bulk.enabled
                        ? `v1 · ${bulk.objects} objects · ${formatDebugBytes(bulk.requestBytes ?? 0)} request cap`
                        : "server fallback"}`,
                );
                push(
                    `  WS data:          ${bulk.wsDataEnabled
                        ? `v1 · ${bulk.wsDataState ?? "off"} · ${formatDebugBytes(bulk.wsDataFrameBytes ?? 0)} payload cap`
                        : "bulk HTTP fallback"}`,
                );
            } catch (e: any) {
                push(`  Bulk HTTP:        negotiation failed: ${e?.message ?? e}`);
                push(`  WS data:          negotiation unavailable`);
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
        let settingsChanged = false;

        const migratedIgnores = migrateLegacyDefaultIgnorePatterns(this.settings.ignorePatterns);
        if (migratedIgnores !== this.settings.ignorePatterns) {
            this.settings.ignorePatterns = migratedIgnores;
            settingsChanged = true;
            console.log("[obsetync] added the atomic-save temp suffix to default ignores");
        }

        // Migration 1.0.x → 1.1.x: server is plain HTTP now (the AEAD
        // envelope is the trust boundary). Persist the rewrite so the
        // settings UI reflects reality instead of showing a stale https URL.
        if (this.settings.serverUrl.startsWith("https://")) {
            this.settings.serverUrl =
                "http://" + this.settings.serverUrl.slice("https://".length);
            settingsChanged = true;
            console.warn(
                "[obsetync] migrated server URL from https:// to http:// " +
                "(transport is plaintext HTTP + AEAD envelope)"
            );
        }
        if (settingsChanged) await this.saveSettings();
    }

    async saveSettings(): Promise<void> {
        await this.saveData(this.settings);
    }

    /** Enroll this device with the server using an enrollment code. */
    async enroll(code: string): Promise<void> {
        // Enrollment is over plain HTTP to the admin port. We pass empty
        // strings for box_pub + bearer_token since claimEnrollment doesn't
        // need an ObsetyncSecureChannel (admin endpoint is unauthenticated).
        const tempApi = new ObsetyncApi(this.settings.serverUrl, "", "");
        const result = await tempApi.claimEnrollment(code);

        this.settings.deviceId     = result.device_id;
        this.settings.bearerToken  = result.bearer_token;
        this.settings.serverBoxPub = result.server_box_pub;
        if (result.wire_version !== "0x02") {
            throw new Error(`unsupported enrollment wire version: ${result.wire_version ?? "missing"}`);
        }
        this.settings.wireVersion = result.wire_version;
        this.settings.esPub = result.Es_pub_initial;
        this.settings.esPubValidUntil = result.Es_pub_valid_until;
        this.settings.lastOutgoingSeq = 0;
        this.settings.enrolled     = true;
        await this.saveSettings();

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
        new ObsetyncConflictModal(this.app, this.io, conflicts, () => {
            new Notice("All conflicts resolved.");
        }).open();
    }

    private async initSync(): Promise<void> {
        // Spans the whole startup cost: WASM load + engine start (pull,
        // journal recovery, metadata scan) — finally, so a failed init still
        // closes its span and shows how long it ran before dying.
        const endSpan = perfSpan("init");
        try {
            await this.initSyncInner();
        } catch (error) {
            await this.hashWorkers?.close();
            this.hashWorkers = null;
            throw error;
        } finally {
            endSpan();
        }
    }

    private async initSyncInner(): Promise<void> {
        // Stop existing engine if re-initializing.
        this.syncEngine?.stop();
        await this.hashWorkers?.close();
        this.hashWorkers = null;

        if (
            !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(this.settings.vaultId) ||
            this.settings.vaultId === "." ||
            this.settings.vaultId === ".."
        ) {
            throw new Error(
                "Vault ID must be 1–128 ASCII letters, digits, dots, underscores, or hyphens " +
                "and must start with a letter or digit",
            );
        }

        // Create API client with the pinned server pubkey + bearer token.
        this.api = new ObsetyncApi(
            this.settings.serverUrl,
            this.settings.serverBoxPub,
            this.settings.bearerToken,
            {
                get: () => ({
                    wireVersion: this.settings.wireVersion,
                    esPub: this.settings.esPub,
                    esPubValidUntil: this.settings.esPubValidUntil,
                    lastOutgoingSeq: this.settings.lastOutgoingSeq,
                }),
                update: async (patch) => {
                    Object.assign(this.settings, patch);
                    await this.saveSettings();
                },
            },
            Platform.isMobile ? "mobile" : "desktop",
        );

        // Load the bundled WASM module. Sync must fail closed if this cannot
        // initialize: development hash stubs are not content-address compatible
        // with the server and must never participate in a real vault.
        this.wasm = await this.loadWasm();
        if (!Platform.isMobile) {
            this.hashWorkers = createDesktopHashWorkerPool(hashWorkerSource);
            if (this.hashWorkers) {
                perfTrace.setProfile({
                    ...perfTrace.getProfile(),
                    hashConcurrency: this.hashWorkers.workerCount,
                });
                console.log(
                    `[obsetync] desktop hash pool starting ` +
                    `(${this.hashWorkers.workerCount} SIMD workers)`,
                );
            } else {
                console.warn("[obsetync] desktop hash workers unavailable; using renderer hashing");
            }
        }

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
        this.syncEngine = new ObsetyncSyncEngine(
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
            this.settings.deviceName || "device",
            this.settings.realtimeWs,
            this.settings.sharePresence,
            this.settings.ignorePatterns,
            this.operationCheckpoint,
            this.settings.autoSync,
            this.hashWorkers,
        );

        // Start.
        this.updateStatusBar("sync ↓");
        await this.syncEngine.start();
        if (this.syncEngine.isReenrollmentRequired()) {
            this.updateStatusBar("sync ⚠ re-enroll");
        } else if (this.syncEngine.isBulkChangeReviewRequired()) {
            this.updateStatusBar("sync ⚠ review");
        } else {
            this.updateStatusBar("sync ✓");
        }
    }

    private async loadWasm(): Promise<WasmModule> {
        // Scalar and SIMD wasm-bindgen modules are both bundled into main.js.
        // Runtime validation rejects SIMD before instantiation on an older
        // WebView; an unexpected SIMD compile/init failure also falls back to
        // the independently generated universal module. The loader promise is
        // cached for this plugin session so concurrent startup paths cannot
        // initialize two module instances.
        const endSpan = perfSpan("wasm.load");
        try {
            this.wasmLoader ??= createWasmLoader<WasmModule>({
                scalar: {
                    mode: "scalar",
                    bytes: embeddedScalarWasmBytes,
                    exports: ScalarWasmExports as unknown as WasmModule,
                    initialize: async (bytes) => {
                        await initScalarWasm({ module_or_path: bytes });
                    },
                },
                simd: {
                    mode: "simd",
                    bytes: embeddedSimdWasmBytes,
                    exports: SimdWasmExports as unknown as WasmModule,
                    initialize: async (bytes) => {
                        await initSimdWasm({ module_or_path: bytes });
                    },
                },
                onSimdFallback: (reason) => {
                    console.warn(`[obsetync] SIMD WASM unavailable (${reason}); using scalar`);
                },
            });
            const selected = await this.wasmLoader();
            perfTrace.setProfile({
                ...perfTrace.getProfile(),
                wasmMode: selected.mode,
            });
            console.log(
                `[obsetync] WASM loaded (${selected.mode}, ${selected.bytes} bytes, inline)`,
            );
            return selected.exports;
        } catch (e: any) {
            const msg = e?.message ?? String(e);
            const name = e?.name ?? "Error";
            const stack = (e?.stack ?? "").split("\n").slice(0, 3).join(" | ");
            console.error(
                `[obsetync] WASM load failed; sync disabled. ` +
                `${name}: ${msg} (stack: ${stack})`
            );
            throw new Error(`Obsetync WASM initialization failed: ${msg}`);
        } finally {
            endSpan();
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
        // Presence suffix: how many OTHER devices are active right now (Ph3).
        const peers = this.syncEngine?.getActivePeerCount() ?? 0;
        this.statusBarEl?.setText(peers > 0 ? `${text} · 👥${peers}` : text);
    }
}
