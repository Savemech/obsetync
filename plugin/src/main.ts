import { App, Modal, Plugin, Notice, Platform, Setting, apiVersion } from "obsidian";
import { createPlatformIO, PlatformIO } from "./platform";
import { ObsetyncApi, type HistoryEntry } from "./api";
import { ObsetyncSyncBase } from "./sync-base";
import { ObsetyncJournal } from "./journal";
import { MobilePreparedTransferPlan, PreparedTransferPlan } from "./transfer-plan";
import { ObjectConfirmationStore } from "./object-confirmation-store";
import { preparedScopeForTree } from "./transfer-plan-scope";
import { captureRootStreamScope } from "./root-stream-scope";
import { ObsetyncSyncEngine, type EngineLegacyDowngradeLease } from "./sync";
import { RootIntentStore } from "./root-intent";
import {
    activateLegacyDowngrade,
    authorizeLegacyDowngradeCurrentImport,
    completeLegacyDowngradeCurrentImport,
    inspectLegacyDowngradeState,
    resumeLegacyDowngrade as resumeLegacyDowngradeTransition,
    type DowngradeAuthorityLease,
    LegacyDowngradeError,
    type LegacyDowngradeState,
} from "./legacy-downgrade";
import {
    LegacyDowngradeHostCoordinator,
    type LegacyDowngradeHostAction,
    type LegacyDowngradeGenerationToken,
    type LegacyDowngradeOperationContext,
    type LegacyDowngradeStoppedHost,
} from "./legacy-downgrade-host";
import {
    LEGACY_DOWNGRADE_TARGET,
    LegacyDowngradeConfirmationModal,
    legacyDowngradeStatus,
    type LegacyDowngradeConfirmationResult,
    type LegacyDowngradeConfirmationView,
} from "./legacy-downgrade-modal";
import { activatePreparedSync, publishStartupResult } from "./startup-activation";
import { awaitRuntimeRetirement, registerRuntimeRetirement, settleRuntimeOwners, startRuntimeOwner } from "./runtime-retirement";
import { SerialSettingsWriter } from "./settings-persistence";
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
import { configureHashTuning, getHashTuning, type HashTuning } from "./hash-runtime";
import { disposeWorkScheduler, workSchedulerSnapshot, yieldWork } from "./work-scheduler";
import { configureTransientMemory, transientMemorySnapshot, closeTransientMemory } from "./transient-memory";
import { createWasmLoader, type WasmSelection } from "./wasm-runtime";
import {
    formatWasmMemoryDebug,
    formatTreeChunkMemoryDebug,
    formatTreeMetadataMemoryDebug,
    formatReplacementInputMemoryDebug,
    formatReplacementV1EntriesMemoryDebug,
    formatReplacementV1GraphMemoryDebug,
    formatReplacementSortMemoryDebug,
    formatReplacementV2PlanningMemoryDebug,
    formatReplacementV2RangesMemoryDebug,
    formatReplacementV2PostSortMemoryDebug,
} from "./wasm-memory";
import { drainRootTreeRetirement } from "./root-tree-repair";
import { RootTreeResidentAdmission } from "./root-tree-resident-admission";
import { drainTreeReachabilityRetirement } from "./tree-candidate-job";
import { drainTreeCandidateMutationRetirement } from "./tree-candidate-mutation-job";
import {
    createDesktopHashWorkerPool,
    type DesktopHashWorkerPool,
    type HashWorkerDiagnostic,
} from "./desktop-hash-workers";
import hashWorkerSource from "obsetync-hash-worker-source";
import browserProbeSource from "obsetync-browser-probe-source";
import browserHashWorkerSource from "obsetync-browser-hash-worker-source";
import {
    createBrowserHashWorkerPool,
    type BrowserHashWorkerDiagnostic,
    type BrowserHashWorkerPool,
    type BrowserHashWorkerRuntime,
} from "./browser-hash-workers";
import { runBrowserCapabilityProbe, type BrowserCapabilityReport } from "./browser-capability-probe";
import { PROBE_FIXTURE_HASH } from "./browser-probe-protocol";
import { runtimeForHost } from "./host-runtime";
import { ObsetyncDebugModal } from "./debug-modal";
import {
    AdaptiveResourceGovernor,
    coveredResourceAxesForWindow,
    ResourceVisibilityGate,
    type ResourceEnvironment,
    type ResourceProfile,
} from "./resource-governor";
import { SyncStatusPresenter, type SyncStatusTruth } from "./sync-status";
import { BUNDLE_BUILD_IDENTITY } from "./build-identity";
import { compareBuildIdentity } from "./build-identity-contract";
import { SyncMemoryArbiter } from "./sync-memory-arbiter";
import { CurrentHostWorkOwner } from "./current-host-work";

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
const MIB = 1024 * 1024;
const MANAGED_SYNC_MEMORY_BYTES = { desktop: 192 * MIB, mobile: 80 * MIB } as const;

function cloneSyncSettings(settings: SyncSettings): SyncSettings {
    return { ...settings, ignorePatterns: [...settings.ignorePatterns] };
}

function hostStateForLegacyDowngrade(state: LegacyDowngradeState):
    "none" | "interrupted" | "active-awaiting-import" | "import-authorized" | "current-active" {
    if (state === "legacy-active") return "active-awaiting-import";
    if (state === "current-import-authorized") return "import-authorized";
    if (state === "current-import-complete") return "current-active";
    return state;
}

class ObsidianLegacyDowngradeModal extends Modal {
    private settled = false;
    private typedTarget = "";
    private understandsSyncStops = false;
    private understandsArchiveIsRetained = false;
    private confirmButton: HTMLButtonElement | null = null;

    constructor(app: App, private readonly view: LegacyDowngradeConfirmationView,
        private readonly settle: (result: LegacyDowngradeConfirmationResult | null) => void) {
        super(app);
    }

    onOpen(): void {
        this.titleEl.setText(this.view.title);
        this.contentEl.createEl("p", { text: this.view.warning });
        new Setting(this.contentEl).setName(`Type ${this.view.requiredTypedTarget} to continue`).addText(input => {
            input.onChange(value => { this.typedTarget = value.trim(); this.refresh(); });
        });
        new Setting(this.contentEl).setName("I understand sync will stay stopped").addToggle(toggle => {
            toggle.onChange(value => { this.understandsSyncStops = value; this.refresh(); });
        });
        new Setting(this.contentEl).setName("I understand the current archive is retained").addToggle(toggle => {
            toggle.onChange(value => { this.understandsArchiveIsRetained = value; this.refresh(); });
        });
        new Setting(this.contentEl).addButton(button => {
            button.setButtonText(this.view.confirmLabel).setCta().onClick(() => {
                if (!this.ready()) return;
                this.finish({ action: this.view.action, targetVersion: this.view.targetVersion,
                    typedTarget: this.typedTarget, understandsSyncStops: this.understandsSyncStops,
                    understandsArchiveIsRetained: this.understandsArchiveIsRetained, explicitlyConfirmed: true });
                this.close();
            });
            this.confirmButton = button.buttonEl;
            this.refresh();
        });
    }

    onClose(): void { this.contentEl.empty(); this.finish(null); }

    private ready(): boolean {
        return this.typedTarget === this.view.requiredTypedTarget && this.understandsSyncStops &&
            this.understandsArchiveIsRetained;
    }
    private refresh(): void { if (this.confirmButton) this.confirmButton.disabled = !this.ready(); }
    private finish(result: LegacyDowngradeConfirmationResult | null): void {
        if (this.settled) return;
        this.settled = true;
        this.settle(result);
    }
}

function formatDebugBytes(bytes: number): string {
    if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(0)} MiB`;
    if (bytes >= 1024) return `${(bytes / 1024).toFixed(0)} KiB`;
    return `${bytes} B`;
}

function formatDebugRate(value: number | null, unit: "bytes/s" | "files/s" | null): string {
    if (value === null || unit === null) return "unmeasured";
    if (unit === "bytes/s") return `${formatDebugBytes(value)}/s`;
    return `${value.toFixed(1)} files/s`;
}

function formatDebugDuration(ms: number): string {
    if (ms < 1_000) return `${Math.max(0, Math.round(ms))}ms`;
    if (ms < 60_000) return `${(ms / 1_000).toFixed(ms < 10_000 ? 1 : 0)}s`;
    return `${(ms / 60_000).toFixed(1)}m`;
}

function formatTransportReason(reason: string): string {
    switch (reason) {
        case "interactive-ws": return "latency-priority WS";
        case "bulk-ws-learning": return "collecting carrier measurements";
        case "bulk-ws-preferred": return "measured WS completion is preferred";
        case "bulk-http-preferred": return "measured HTTP completion is preferred";
        case "bulk-ws-probe": return "periodic WS comparison probe";
        case "bulk-http-probe": return "periodic HTTP comparison probe";
        case "ws-recovery-probe": return "bounded WS recovery probe";
        case "ws-unavailable": return "WS data carrier unavailable";
        case "ws-payload-too-large": return "payload exceeds the negotiated WS frame";
        case "ws-circuit-open": return "WS circuit is open";
        case "ws-recovery-probe-ineligible": return "bulk cannot own the WS recovery probe";
        case "ws-recovery-probe-busy": return "another WS recovery probe is active";
        case "ws-retryable-fallback": return "retryable WS failure fell back once";
        default: return reason;
    }
}

function rendererMemoryPressure(): boolean {
    const memory = (globalThis.performance as any)?.memory;
    return Number.isFinite(memory?.usedJSHeapSize) &&
        Number.isFinite(memory?.jsHeapSizeLimit) && memory.jsHeapSizeLimit > 0 &&
        memory.usedJSHeapSize / memory.jsHeapSizeLimit >= 0.85;
}

function detectResourceEnvironment(
    runtime: "desktop" | "mobile",
    architecture: ResourceEnvironment["architecture"],
): ResourceEnvironment {
    const rawPlatform = String((globalThis as any).process?.platform ?? "");
    const os: ResourceEnvironment["os"] = Platform.isIosApp
        ? "ios"
        : rawPlatform === "darwin" || rawPlatform === "win32" || rawPlatform === "linux"
            ? rawPlatform
            : "unknown";
    const reportedCores = globalThis.navigator?.hardwareConcurrency;
    const hardwareConcurrency = Number.isFinite(reportedCores)
        ? Math.max(1, Math.trunc(reportedCores))
        : 1;
    let simdAvailable = false;
    try {
        simdAvailable = WebAssembly.validate(embeddedSimdWasmBytes as BufferSource);
    } catch {
        // The profile ladder stays conservative until scalar WASM is loaded.
    }
    return { runtime, architecture, os, hardwareConcurrency, simdAvailable };
}

export default class ObsetyncPlugin extends Plugin {
    settings: SyncSettings = DEFAULT_SETTINGS;
    private io!: PlatformIO;
    private api!: ObsetyncApi;
    private syncBase!: ObsetyncSyncBase;
    private journal!: ObsetyncJournal;
    private preparedTransfers!: PreparedTransferPlan;
    private syncMemory!: SyncMemoryArbiter;
    private objectConfirmations!: ObjectConfirmationStore;
    private syncEngine!: ObsetyncSyncEngine;
    private wasm!: WasmModule;
    private tree: WasmTree | undefined;
    private wasmLoader: (() => Promise<WasmSelection<WasmModule>>) | null = null;
    private hashWorkers: DesktopHashWorkerPool | null = null;
    private browserHashWorkers: BrowserHashWorkerPool | null = null;
    private browserHashStartup: Promise<BrowserHashWorkerPool | null> | null = null;
    private browserHashWorkerDiagnostic: BrowserHashWorkerDiagnostic | null = null;
    private hashWorkerDiagnostic: HashWorkerDiagnostic | null = null;
    private hashWorkerGeneration = 0;
    private syncInitGeneration = 0;
    private operationCheckpoint!: OperationCheckpoint;
    private resourceGovernor!: AdaptiveResourceGovernor;
    /** Separate from the FIFO transient pool: long-lived V2 replacement-output
     * requested buffers can never wait for their own old graph to retire. */
    private readonly rootTreeResidentAdmission = new RootTreeResidentAdmission({
        capacityBytes: 32 * 1024 * 1024,
    });
    private visibilityGate!: ResourceVisibilityGate;
    private unsubscribePerf: (() => void) | null = null;
    private statusBarEl: HTMLElement | null = null;
    private statusBarText = "sync off";
    private readonly syncStatusPresenter = new SyncStatusPresenter();
    private statusRefreshTimer: ReturnType<typeof globalThis.setInterval> | null = null;
    private browserProbeReport: BrowserCapabilityReport | null = null;
    private browserProbePromise: Promise<string> | null = null;
    private browserProbeAbort: AbortController | null = null;
    private legacyDowngradeHost: LegacyDowngradeHostCoordinator | null = null;
    private legacyDowngradeStoppedHost: LegacyDowngradeStoppedHost | null = null;
    private legacyRootIntentOwner: RootIntentStore | null = null;
    private legacyDowngradeModal: ObsidianLegacyDowngradeModal | null = null;
    private legacyConfirmation: Promise<boolean> | null = null;
    private legacyRecoveryWork: Promise<void> | null = null;
    private currentHostWork = new CurrentHostWorkOwner();
    private conflictModal: ObsetyncConflictModal | null = null;
    private legacyUiInstalled = false;
    private currentPersistenceStartup: Promise<void> | null = null;
    private currentUiInstalled = false;
    private readonly settingsWork = new Set<Promise<void>>();
    private settingsMigrationPending = false;
    private lastSavedSettings: SyncSettings | null = null;
    private legacyRecoveryProjectionOnly = false;
    private unloaded = false;
    private loadWork: Promise<void> | null = null;
    private retirement: Promise<void> | null = null;
    /** Constructor failure can leave a native worker alive without a pool
     * return value. Keep its actual cleanup boundary across init generations. */
    private readonly failedWorkerConstruction = new Set<Promise<void>>();
    private readonly settingsWrites = new SerialSettingsWriter<SyncSettings>(snapshot => this.saveData(snapshot));
    private readonly retiredTrees = new WeakSet<WasmTree>();
    private readonly retiringTrees = new WeakMap<WasmTree, Promise<void>>();
    private treeRetirementFailed = false;

    onload(): Promise<void> {
        const loading = this.onloadInner();
        this.loadWork = loading;
        void loading.then(() => { if (this.loadWork === loading) this.loadWork = null; },
            () => { if (this.loadWork === loading) this.loadWork = null; });
        return loading;
    }

    private async onloadInner(): Promise<void> {
        // A previous plugin bundle may still be adopting an accepted root.
        // Do not open storage or replace its logger before that actual cut.
        await awaitRuntimeRetirement(this.app.vault);
        if (this.unloaded) return;
        const runtime = runtimeForHost(Platform);
        this.syncMemory = new SyncMemoryArbiter({
            capacityBytes: MANAGED_SYNC_MEMORY_BYTES[runtime],
            interactiveReserveBytes: 8 * MIB,
        });
        const detectedArchitecture = normalizePerfArchitecture(
            Platform.isIosApp
                ? "arm64"
                : (globalThis as any).process?.arch ??
                    (globalThis.navigator as any)?.userAgentData?.architecture,
        );

        // Capture every subsequent `[obsetync] …` console line into a ring
        // buffer so the "Show debug info" panel can surface them later,
        // especially on iOS where there's no easy way to see console output.
        debugLog.install();
        const buildComparison = compareBuildIdentity(this.manifest.version, BUNDLE_BUILD_IDENTITY);
        if (buildComparison.status === "version-mismatch") {
            console.error(`[obsetync] build identity ${buildComparison.diagnostic}`);
            new Notice(`ObsetyNC build identity ${buildComparison.diagnostic}. Reinstall the plugin assets together.`, 0);
        } else if (buildComparison.status !== "match") {
            console.warn(`[obsetync] build identity ${buildComparison.diagnostic}`);
        }

        // Persist window-level errors to .obsetync-crash.log in the vault
        // root. This captures JavaScript failures; the separate durable
        // operation checkpoint diagnoses OS-level renderer kills/Jetsam.
        crashLog.install(this.app, this.manifest.version);

        await this.loadSettings();
        if (this.unloaded) return;

        // Platform I/O.
        this.io = createPlatformIO(this.app);
        let durableLegacyState: LegacyDowngradeState;
        try {
            durableLegacyState = await inspectLegacyDowngradeState(this.app.vault.adapter);
        } catch (error) {
            this.statusBarEl = this.addStatusBarItem();
            this.updateStatusBar("sync ⚠ compatibility recovery");
            new Notice("ObsetyNC compatibility state requires recovery. Sync remains stopped; no files were reset.", 0);
            throw error;
        }
        if (this.unloaded) return;
        this.legacyDowngradeHost = new LegacyDowngradeHostCoordinator(
            hostStateForLegacyDowngrade(durableLegacyState),
        );
        this.legacyRecoveryProjectionOnly = durableLegacyState === "interrupted" ||
            durableLegacyState === "legacy-active" || durableLegacyState === "current-import-authorized";
        this.installLegacyDowngradeUi();
        if (this.settingsMigrationPending &&
            (durableLegacyState === "none" || durableLegacyState === "current-import-complete")) {
            await this.saveSettings();
            if (this.unloaded) return;
        }
        this.operationCheckpoint = new OperationCheckpoint(
            this.io,
            this.manifest.version,
        );
        const newInterruption = await this.operationCheckpoint.initialize();
        if (this.unloaded) return;

        const environment = detectResourceEnvironment(runtime, detectedArchitecture);
        const initiallyVisible = typeof document === "undefined" || !document.hidden;
        perfTrace.setVisible(initiallyVisible);
        this.visibilityGate = new ResourceVisibilityGate(runtime, initiallyVisible);
        this.resourceGovernor = new AdaptiveResourceGovernor(environment, {
            recoveryHint: this.settings.resourceRecoveryHint,
            visible: initiallyVisible,
            onProfileChange: (profile, reason) => this.applyResourceProfile(profile, reason),
            onRecoveryHintChange: (hint) => {
                if (this.unloaded) return;
                try { this.legacyDowngradeHost?.authorize("settings").assertCurrent(); }
                catch { return; }
                this.settings.resourceRecoveryHint = hint;
                void this.saveSettings().catch((error) => {
                    console.warn("[obsetync] failed to persist resource recovery hint:", error);
                });
            },
        });
        this.applyResourceProfile(this.resourceGovernor.current(), "startup selection");
        if (newInterruption) this.resourceGovernor.recordInterruption(newInterruption.phase);
        this.unsubscribePerf = perfTrace.subscribeWindows((window) => {
            const statusOperation = perfTrace.activeSnapshots(16)
                .find(operation => operation.operationId === window.operationId);
            this.syncStatusPresenter.observeWindow(window, statusOperation);
            const stats = this.hashWorkers?.stats();
            const browserStats = this.browserHashWorkers?.stats();
            const budget = transientMemorySnapshot();
            this.resourceGovernor.observeWindow({
                ...window,
                operationKind: window.kind,
                visible: typeof document === "undefined" || !document.hidden,
                queueDepth: stats?.queued ?? browserStats?.queued ?? 0,
                memoryPressure: rendererMemoryPressure(),
                maxConcurrency: { hash: stats && stats.wasmMode !== "unavailable" ? stats.capacity : 1 },
                // Only explicitly demanded stages whose dynamic buffers use the
                // shared reserve-before-use ledger may probe. Fixed metadata,
                // native heaps and RSS remain outside this narrower claim.
                budget: {
                    limitBytes: budget.capacityBytes,
                    reservedBytes: budget.usedBytes,
                    coveredAxes: coveredResourceAxesForWindow(window),
                },
            });
        });
        if (typeof document !== "undefined") {
            this.registerDomEvent(document, "visibilitychange", () => {
                const visible = !document.hidden;
                perfTrace.setVisible(visible);
                this.resourceGovernor.setVisible(visible);
                this.visibilityGate.setVisible(visible, "hidden");
                if (!visible) this.browserProbeAbort?.abort("hidden");
            });
            this.registerDomEvent(document, "freeze" as keyof DocumentEventMap, () => {
                perfTrace.setVisible(false);
                this.resourceGovernor.setVisible(false);
                this.visibilityGate.setVisible(false, "freeze");
                this.browserProbeAbort?.abort("freeze");
            });
        }
        if (typeof window !== "undefined") {
            this.registerDomEvent(window, "pagehide", () => {
                perfTrace.setVisible(false);
                this.resourceGovernor.setVisible(false);
                this.visibilityGate.setVisible(false, "pagehide");
                this.browserProbeAbort?.abort("pagehide");
            });
            this.registerDomEvent(window, "pageshow", () => {
                const visible = typeof document === "undefined" || !document.hidden;
                perfTrace.setVisible(visible);
                this.resourceGovernor.setVisible(visible);
                this.visibilityGate.setVisible(visible, "hidden");
            });
        }

        const legacySnapshot = this.legacyDowngradeHost.snapshot();
        if (legacySnapshot.state !== "none" && legacySnapshot.state !== "current-active") {
            this.renderStatusBar();
            return;
        }
        if (legacySnapshot.state === "current-active") {
            await this.validateCompletedLegacyRootAuthority();
            if (this.unloaded) return;
        }
        await this.loadCurrentPersistence(runtime);
        this.installCurrentRuntimeUi();
    }

    private installLegacyDowngradeUi(): void {
        if (this.legacyUiInstalled) return;
        this.legacyUiInstalled = true;
        this.statusBarEl = this.addStatusBarItem();
        this.statusRefreshTimer = globalThis.setInterval(() => this.renderStatusBar(), 1_000);
        this.addCommand({ id: "prepare-legacy-downgrade-1-11-3",
            name: `Prepare compatibility handoff to ${LEGACY_DOWNGRADE_TARGET}`,
            callback: () => { void this.prepareLegacyDowngrade().catch(error => this.reportLegacyDowngradeError(error)); } });
        this.addCommand({ id: "resume-legacy-downgrade-1-11-3",
            name: `Resume compatibility handoff to ${LEGACY_DOWNGRADE_TARGET}`,
            callback: () => { void this.resumeLegacyDowngrade().catch(error => this.reportLegacyDowngradeError(error)); } });
        this.addCommand({ id: "import-legacy-downgrade-1-11-3",
            name: `Import work from ${LEGACY_DOWNGRADE_TARGET}`,
            callback: () => { void this.importLegacyDowngrade().catch(error => this.reportLegacyDowngradeError(error)); } });
        this.updateStatusBar(this.settings.enrolled ? "sync …" : "sync off");
    }

    private loadCurrentPersistence(runtime = runtimeForHost(Platform), forceReload = false): Promise<void> {
        if (this.currentPersistenceStartup && !forceReload) return this.currentPersistenceStartup;
        const startup = (async () => {
            if (this.unloaded) return;
            if (forceReload) {
                await this.closePersistenceOwners([
                    this.preparedTransfers, this.objectConfirmations, this.syncBase, this.journal,
                ]);
            }
            const syncBase = new ObsetyncSyncBase(this.app);
            let journal: ObsetyncJournal | undefined;
            try {
                await syncBase.load();
                if (this.unloaded) {
                    await this.closePersistenceOwners([syncBase]);
                    return;
                }
                journal = new ObsetyncJournal(this.app);
                await journal.load();
                if (this.unloaded) {
                    await this.closePersistenceOwners([syncBase, journal]);
                    return;
                }
            } catch (error) {
                await Promise.allSettled([this.closePersistenceOwners([syncBase, journal])]);
                throw error;
            }
            if (!journal) throw new Error("journal owner was not initialized");
            const preparedTransfers = runtime === "mobile"
                ? new MobilePreparedTransferPlan(this.app.vault.adapter, {}, this.syncMemory)
                : new PreparedTransferPlan(this.app.vault.adapter, {}, this.syncMemory);
            const objectConfirmations = new ObjectConfirmationStore(this.app.vault.adapter);
            this.syncBase = syncBase;
            this.journal = journal;
            this.preparedTransfers = preparedTransfers;
            this.objectConfirmations = objectConfirmations;
        })();
        this.currentPersistenceStartup = startup;
        void startup.catch(() => {
            if (this.currentPersistenceStartup === startup) this.currentPersistenceStartup = null;
        });
        return startup;
    }

    private closePersistenceOwners(owners: readonly unknown[]): Promise<void> {
        return Promise.all(owners.map(async owner => {
            if (!owner || typeof owner !== "object") return;
            const lifecycle = owner as { closeAndDrain?: () => Promise<void>; close?: () => void };
            if (typeof lifecycle.closeAndDrain === "function") await lifecycle.closeAndDrain.call(owner);
            else lifecycle.close?.call(owner);
        })).then(() => undefined);
    }

    private installCurrentRuntimeUi(scheduleAutomaticSync = true): void {
        if (this.currentUiInstalled || this.unloaded) return;
        this.currentUiInstalled = true;
        this.addSettingTab(new ObsetyncSettingTab(this.app, this));
        this.addCommand({ id: "sync-now", name: "Sync now", callback: () => this.syncNow() });
        this.addCommand({ id: "full-rescan", name: "Full vault rescan", callback: () => this.fullScan() });
        this.addCommand({ id: "show-conflicts", name: "Show sync conflicts", callback: () => this.showConflicts() });
        this.addCommand({ id: "test-browser-worker-capabilities",
            name: "Test browser worker capabilities (synthetic data only)",
            callback: () => { void this.showBrowserCapabilityProbe(); } });
        if (scheduleAutomaticSync && this.settings.enrolled && this.settings.serverUrl) {
            this.app.workspace.onLayoutReady(() => {
                try { this.legacyDowngradeHost?.authorize("init").assertCurrent(); }
                catch { return; }
                this.initSync().catch((error) => {
                    console.error("[obsetync] init failed:", error);
                    this.updateStatusBar("sync ✗");
                });
            });
        }
    }

    private requireLegacyDowngradeHost(): LegacyDowngradeHostCoordinator {
        if (!this.legacyDowngradeHost) throw new Error("legacy compatibility state is not initialized");
        return this.legacyDowngradeHost;
    }

    private async confirmLegacyDowngrade(action: "prepare" | "resume" | "import"): Promise<boolean> {
        if (this.legacyConfirmation) throw new Error("another compatibility confirmation is already open");
        const request = new LegacyDowngradeConfirmationModal({ confirm: view => new Promise(resolve => {
            if (this.unloaded) { resolve(null); return; }
            const modal = new ObsidianLegacyDowngradeModal(this.app, view, result => {
                if (this.legacyDowngradeModal === modal) this.legacyDowngradeModal = null;
                resolve(result);
            });
            this.legacyDowngradeModal = modal;
            modal.open();
        }) }).request(action);
        this.legacyConfirmation = request;
        try { return await request; }
        finally { if (this.legacyConfirmation === request) this.legacyConfirmation = null; }
    }

    private async drainAdmittedSettings(): Promise<void> {
        await Promise.allSettled([...this.settingsWork]);
    }

    private runCurrentHostWork<T>(action: LegacyDowngradeHostAction,
        operation: (lease: { readonly signal: AbortSignal; assertCurrent(): void }) => T | PromiseLike<T>): Promise<T> {
        const hostToken = this.legacyDowngradeHost?.authorize(action);
        return this.currentHostWork.run(ownerLease => {
            const lease = {
                signal: ownerLease.signal,
                assertCurrent: () => {
                    ownerLease.assertCurrent();
                    hostToken?.assertCurrent();
                },
            };
            lease.assertCurrent();
            return operation(lease);
        });
    }

    private async drainCurrentHostAdmission(): Promise<void> {
        const owner = this.currentHostWork;
        const modal = this.conflictModal;
        modal?.revoke();
        const results = await Promise.allSettled([
            owner.closeAndDrain(),
            this.drainAdmittedSettings(),
            modal?.closeAndDrain() ?? Promise.resolve(),
        ]);
        if (this.conflictModal === modal) this.conflictModal = null;
        if (results.some(result => result.status === "rejected")) {
            throw new Error("current host work did not retire cleanly");
        }
    }

    private createLegacyStoppedHost(engineLease: EngineLegacyDowngradeLease | null): LegacyDowngradeStoppedHost {
        if (this.legacyDowngradeStoppedHost) throw new Error("legacy stopped host already exists");
        let retiring = false;
        let retired = false;
        let retirement: Promise<void> | null = null;
        let rootOwner: RootIntentStore | null | undefined;
        let rootRetired = false;
        let engineRetired = engineLease === null;
        const owner: LegacyDowngradeStoppedHost = {
            assertStopped: () => {
                if (retired || retiring) throw new Error("legacy stopped host is retiring or retired");
                if (engineLease && !engineRetired) engineLease.assertHeld();
                else if (this.syncEngine && !this.syncEngine.isStopped()) {
                    throw new Error("current sync engine restarted during compatibility handoff");
                }
            },
            retire: () => {
                if (retirement) return retirement;
                retiring = true;
                if (rootOwner === undefined) rootOwner = this.legacyRootIntentOwner;
                const work = (async () => {
                    const results = await Promise.allSettled([
                        rootRetired || !rootOwner ? Promise.resolve() : rootOwner.closeAndDrain().then(() => {
                            rootRetired = true;
                            if (this.legacyRootIntentOwner === rootOwner) this.legacyRootIntentOwner = null;
                        }),
                        engineRetired || !engineLease ? Promise.resolve() : engineLease.closeAndDrain().then(() => {
                            engineRetired = true;
                        }),
                    ]);
                    const failures = results.flatMap(result => result.status === "rejected" ? [result.reason] : []);
                    if (failures.length) throw new Error("legacy stopped-host retirement failed");
                    retiring = false;
                    retired = true;
                    if (this.legacyDowngradeStoppedHost === owner) this.legacyDowngradeStoppedHost = null;
                })();
                retirement = work.catch(error => {
                    retiring = false;
                    retirement = null;
                    throw error;
                });
                return retirement;
            },
        };
        this.legacyDowngradeStoppedHost = owner;
        return owner;
    }

    private legacyCoreLease(context: LegacyDowngradeOperationContext): DowngradeAuthorityLease {
        const stopped = this.legacyDowngradeStoppedHost;
        if (!stopped) throw new Error("legacy compatibility host is not stopped");
        return { assertHeld: () => { context.assertCurrent(); stopped.assertStopped(); } };
    }

    private reconcileLegacyDowngradeState(): Promise<void> {
        if (this.legacyRecoveryWork) return this.legacyRecoveryWork;
        if (this.unloaded) return Promise.resolve();
        // Revocation is synchronous: no settings/sync/init action can enter
        // between recovery being requested and the durable marker inspection.
        const retirement = this.legacyDowngradeHost?.unload() ?? Promise.resolve();
        const recovery = this.reconcileLegacyDowngradeStateInner(retirement);
        this.legacyRecoveryWork = recovery;
        void recovery.then(
            () => { if (this.legacyRecoveryWork === recovery) this.legacyRecoveryWork = null; },
            () => { if (this.legacyRecoveryWork === recovery) this.legacyRecoveryWork = null; },
        );
        return recovery;
    }

    private async reconcileLegacyDowngradeStateInner(retirement: Promise<void>): Promise<void> {
        await retirement;
        if (this.unloaded) return;
        const durable = await inspectLegacyDowngradeState(this.app.vault.adapter);
        if (this.unloaded) return;
        this.legacyDowngradeHost = new LegacyDowngradeHostCoordinator(hostStateForLegacyDowngrade(durable));
        this.legacyRecoveryProjectionOnly = durable === "interrupted" || durable === "legacy-active" ||
            durable === "current-import-authorized";
        if (durable !== "none" && durable !== "current-import-complete") {
            this.renderStatusBar();
            return;
        }
        if (durable === "current-import-complete") {
            try {
                await this.validateCompletedLegacyRootAuthority();
                await this.loadCurrentPersistence(runtimeForHost(Platform), true);
            } catch (error) {
                await this.legacyDowngradeHost.unload();
                if (this.unloaded) return;
                this.legacyDowngradeHost = new LegacyDowngradeHostCoordinator("import-authorized");
                this.legacyRecoveryProjectionOnly = true;
                this.renderStatusBar();
                throw error;
            }
        } else {
            await this.loadCurrentPersistence(runtimeForHost(Platform), false);
        }
        this.currentHostWork = new CurrentHostWorkOwner();
        this.installCurrentRuntimeUi(false);
        if (this.settings.enrolled && this.settings.serverUrl &&
            (!this.syncEngine || this.syncEngine.isStopped())) await this.initSync();
    }

    private async reconcileLegacyDowngradeFailure(error: unknown): Promise<never> {
        try { await this.reconcileLegacyDowngradeState(); }
        catch (recoveryError) {
            const code = recoveryError instanceof LegacyDowngradeError ? recoveryError.code :
                recoveryError instanceof Error ? recoveryError.name : "UNKNOWN";
            console.error(`[obsetync] compatibility recovery reinspection failed (${code})`);
        }
        throw error;
    }

    private async prepareLegacyDowngrade(): Promise<void> {
        if (!(await this.confirmLegacyDowngrade("prepare"))) return;
        const host = this.requireLegacyDowngradeHost();
        const engine = this.syncEngine;
        if (!engine || !this.syncBase || !this.journal) {
            throw new Error("sync must finish initializing before compatibility handoff");
        }
        let engineLease: EngineLegacyDowngradeLease | null = null;
        await host.prepare({
            quiesce: async context => {
                await this.drainCurrentHostAdmission();
                context.assertCurrent();
                engineLease = await engine.freezeForLegacyDowngrade();
                const stopped = this.createLegacyStoppedHost(engineLease);
                try {
                    context.assertCurrent();
                    stopped.assertStopped();
                    return stopped;
                } catch (error) {
                    await stopped.retire();
                    throw error;
                }
            },
            activate: async context => {
                if (!engineLease) throw new Error("legacy engine authority was not captured");
                const lease = this.legacyCoreLease(context);
                await activateLegacyDowngrade({ io: this.app.vault.adapter, journal: this.journal,
                    syncBase: this.syncBase, rootIntents: engineLease.rootIntents,
                    treeVersion: engineLease.treeVersion, lease });
            },
            handoff: async context => {
                context.assertCurrent();
                new Notice(`Compatibility files are ready. Install exactly ObsetyNC ${LEGACY_DOWNGRADE_TARGET}; current sync stays stopped.`, 0);
            },
        }).catch(error => this.reconcileLegacyDowngradeFailure(error));
        this.renderStatusBar();
    }

    private async resumeLegacyDowngrade(): Promise<void> {
        if (!(await this.confirmLegacyDowngrade("resume"))) return;
        const host = this.requireLegacyDowngradeHost();
        let engineLease: EngineLegacyDowngradeLease | null = null;
        await host.resume({
            quiesce: async context => {
                await this.drainCurrentHostAdmission();
                context.assertCurrent();
                if (this.syncEngine && !this.syncEngine.isStopped()) {
                    engineLease = await this.syncEngine.freezeForLegacyDowngrade();
                    return this.createLegacyStoppedHost(engineLease);
                }
                return this.createLegacyStoppedHost(null);
            },
            activate: async context => {
                const lease = this.legacyCoreLease(context);
                if (!this.legacyRecoveryProjectionOnly && this.syncBase && this.journal && this.syncEngine) {
                    engineLease ??= await this.syncEngine.freezeForLegacyDowngrade();
                    await activateLegacyDowngrade({ io: this.app.vault.adapter, journal: this.journal,
                        syncBase: this.syncBase, rootIntents: engineLease.rootIntents,
                        treeVersion: engineLease.treeVersion, lease });
                } else {
                    await resumeLegacyDowngradeTransition(this.app.vault.adapter, lease);
                }
            },
            handoff: async context => {
                context.assertCurrent();
                new Notice(`Compatibility files are ready. Install exactly ObsetyNC ${LEGACY_DOWNGRADE_TARGET}; current sync stays stopped.`, 0);
            },
        }).catch(error => this.reconcileLegacyDowngradeFailure(error));
        this.renderStatusBar();
    }

    private async createLocalRootIntentOwner(assertCurrent: () => void,
        signal?: AbortSignal): Promise<RootIntentStore> {
        const settings = { ...this.settings, ignorePatterns: [...this.settings.ignorePatterns] };
        const api = new ObsetyncApi(settings.serverUrl, settings.serverBoxPub, settings.bearerToken);
        const scope = captureRootStreamScope(settings, api, api.baseUrl);
        const selected = await this.loadWasm();
        assertCurrent();
        const scopeHash = await scope.scopeHash;
        assertCurrent();
        scope.assertCurrent(this.settings, api, api.baseUrl);
        const hashBytes = async (bytes: Uint8Array): Promise<string> => {
            const hasher = new selected.exports.Hasher();
            try {
                for (let offset = 0; offset < bytes.length; offset += 256 * 1024) {
                    assertCurrent();
                    hasher.update(bytes.subarray(offset, offset + 256 * 1024));
                    await yieldWork({ lane: "maintenance", signal });
                }
                assertCurrent();
                return hasher.finalize();
            } finally { hasher.free(); }
        };
        const owner = new RootIntentStore(this.app.vault.adapter, scopeHash, hashBytes);
        try {
            await owner.load();
            assertCurrent();
            return owner;
        } catch (error) {
            await owner.closeAndDrain().catch(() => {});
            throw error;
        }
    }

    private async loadLegacyRootIntentOwner(context: LegacyDowngradeOperationContext): Promise<RootIntentStore> {
        if (this.legacyRootIntentOwner) {
            await this.legacyRootIntentOwner.load();
            context.assertCurrent();
            return this.legacyRootIntentOwner;
        }
        const owner = await this.createLocalRootIntentOwner(() => context.assertCurrent(), context.signal);
        this.legacyRootIntentOwner = owner;
        return owner;
    }

    private async validateCompletedLegacyRootAuthority(): Promise<void> {
        const token = this.requireLegacyDowngradeHost().authorize("init");
        const owner = await this.createLocalRootIntentOwner(() => token.assertCurrent());
        try {
            token.assertCurrent();
            if (owner.pending() !== null) throw new Error("root publication is pending or uncertain");
        } finally { await owner.closeAndDrain(); }
        token.assertCurrent();
    }

    private async importLegacyDowngrade(): Promise<void> {
        if (!(await this.confirmLegacyDowngrade("import"))) return;
        const host = this.requireLegacyDowngradeHost();
        await host.importLegacy({
            quiesce: async context => {
                await this.drainCurrentHostAdmission();
                context.assertCurrent();
                if (this.syncEngine && !this.syncEngine.isStopped()) {
                    const lease = await this.syncEngine.freezeForLegacyDowngrade();
                    return this.createLegacyStoppedHost(lease);
                }
                return this.createLegacyStoppedHost(null);
            },
            importLegacy: async context => {
                const lease = this.legacyCoreLease(context);
                const rootOwner = await this.loadLegacyRootIntentOwner(context);
                if (rootOwner.pending() !== null) throw new Error("root publication is pending or uncertain");
                await authorizeLegacyDowngradeCurrentImport(this.app.vault.adapter, rootOwner.lastSequence, lease);
                await this.loadCurrentPersistence(runtimeForHost(Platform), true);
                context.assertCurrent();
                await completeLegacyDowngradeCurrentImport(this.app.vault.adapter, lease);
            },
        }).catch(error => this.reconcileLegacyDowngradeFailure(error));
        this.currentHostWork = new CurrentHostWorkOwner();
        this.installCurrentRuntimeUi(false);
        this.updateStatusBar("sync …");
        if (this.settings.enrolled && this.settings.serverUrl) await this.initSync();
        new Notice(`Compatibility work from ObsetyNC ${LEGACY_DOWNGRADE_TARGET} was imported; current sync may resume.`);
    }

    private reportLegacyDowngradeError(error: unknown): void {
        const code = error instanceof LegacyDowngradeError ? error.code :
            error instanceof Error ? error.name : "UNKNOWN";
        console.error(`[obsetync] compatibility transition failed (${code})`);
        new Notice(`ObsetyNC compatibility transition failed (${code}). Sync remains stopped; no archive was deleted.`, 0);
        this.renderStatusBar();
    }

    onunload(): void {
        if (this.unloaded) return;
        this.unloaded = true;
        this.legacyDowngradeModal?.close();
        this.legacyDowngradeModal = null;
        const legacyRetirement = this.legacyDowngradeHost?.unload() ?? Promise.resolve();
        const currentHostRetirement = this.drainCurrentHostAdmission();
        if (this.statusRefreshTimer !== null) globalThis.clearInterval(this.statusRefreshTimer);
        this.statusRefreshTimer = null;
        this.rootTreeResidentAdmission.close();
        this.syncInitGeneration++;
        this.browserProbeAbort?.abort();
        this.hashWorkerGeneration++;
        // Stop admission synchronously, but leave services needed by accepted
        // root/base/journal settlement alive until all actual owners finish.
        const owners = [
            startRuntimeOwner(() => legacyRetirement),
            startRuntimeOwner(() => currentHostRetirement),
            startRuntimeOwner(() => this.legacyRecoveryWork ?? Promise.resolve()),
            startRuntimeOwner(async () => {
                await this.syncEngine?.stopAndDrain();
                await this.browserHashStartup?.catch(() => null);
                this.browserHashWorkers?.close();
            }),
            startRuntimeOwner(() => this.hashWorkers?.closeAndDrainNative()),
            ...this.failedWorkerConstruction,
            this.settingsWrites.closeAndDrain(),
            this.loadWork?.catch(() => {}) ?? Promise.resolve(),
        ];
        this.unsubscribePerf?.();
        this.unsubscribePerf = null;
        const cleanup = settleRuntimeOwners(owners).then(async () => {
            this.hashWorkers = null;
            this.browserHashWorkers = null;
            this.browserHashStartup = null;
            await this.retireTree(this.tree);
            await this.closePersistenceOwners([
                this.preparedTransfers, this.objectConfirmations, this.syncBase, this.journal,
            ]);
            disposeWorkScheduler();
            closeTransientMemory();
            this.syncMemory?.close();
            this.visibilityGate?.dispose();
            crashLog.uninstall();
            debugLog.uninstall();
        });
        this.retirement = registerRuntimeRetirement(this.app.vault, cleanup);
        void this.retirement.catch(() => {
            console.error("[obsetync] runtime retirement failed; replacement remains blocked");
        });
    }

    /** Explicit diagnostic only. Passing never enables production workers or
     * resource-URL reads; unsupported hosts retain their existing safe path. */
    async showBrowserCapabilityProbe(): Promise<void> {
        this.legacyDowngradeHost?.authorize("sync").assertCurrent();
        const loading = new Notice("Testing synthetic worker/WASM capabilities…", 0);
        try {
            const text = await this.testBrowserCapabilities();
            if (!this.unloaded) new ObsetyncDebugModal(this.app, text).open();
        } catch (error) {
            new Notice((error as Error).message);
        } finally { loading.hide(); }
    }

    private async testBrowserCapabilities(): Promise<string> {
        if (this.browserProbePromise) return this.browserProbePromise;
        if (typeof document !== "undefined" && document.hidden) throw new Error("Keep Obsidian visible for this diagnostic.");
        if (this.syncEngine?.isBusy()) throw new Error("Run this diagnostic after the current sync operation finishes.");
        if (this.browserProbeReport && ((this.browserProbeReport.jobDispatched &&
            !this.browserProbeReport.cooperativeDone) || this.browserProbeReport.cleanupEvidence === "shutdown-unconfirmed")) {
            throw new Error("The previous probe did not confirm completion. Reload the plugin before another attempt.");
        }
        this.browserProbeAbort = new AbortController();
        const signal = this.browserProbeAbort.signal;
        this.browserProbePromise = (async () => {
            this.browserProbeReport = await runBrowserCapabilityProbe({
                source: browserProbeSource, scalar: embeddedScalarWasmBytes,
                simd: embeddedSimdWasmBytes, expectedHash: PROBE_FIXTURE_HASH, signal,
            });
            return this.formatBrowserProbe();
        })();
        try { return await this.browserProbePromise; }
        finally { this.browserProbePromise = null; this.browserProbeAbort = null; }
    }

    private formatBrowserProbe(): string {
        const report = this.browserProbeReport;
        return [
            "=== Obsetync synthetic browser capability probe ===",
            `Plugin: ${this.manifest.version} · Obsidian API: ${apiVersion}`,
            `Host: desktopApp=${Platform.isDesktopApp} mobileApp=${Platform.isMobileApp} iOS=${Platform.isIosApp} Android=${Platform.isAndroidApp} mobileUI=${Platform.isMobile}`,
            report ? JSON.stringify(report, null, 2) : "Not run (explicit command/settings button only).",
            "Workload: one synthetic 64 KiB buffer; embedded scalar/SIMD WASM copies.",
            "Vault reads/writes: none. Network/resource URL tests: none.",
            "Termination requested is not a native heap-release/RSS measurement.",
            "A passing probe does not enable production browser workers or prove mobile UI/memory gates.",
        ].join("\n");
    }

    /** Expose the sync engine for the settings tab's status box. Returns null
     *  if not yet initialized (e.g., before enrollment). */
    syncEngineOrNull(): ObsetyncSyncEngine | null {
        return this.syncEngine ?? null;
    }

    async getRootHistory(): Promise<HistoryEntry[]> {
        return this.runCurrentHostWork("sync", async lease => {
            const api = this.api;
            const vaultId = this.settings.vaultId;
            if (!api || !vaultId) throw new Error("Not enrolled yet — no history to show");
            const entries = await api.getHistory(vaultId);
            lease.assertCurrent();
            return entries;
        });
    }

    async rollbackRoot(root: string): Promise<void> {
        return this.runCurrentHostWork("sync", async lease => {
            const api = this.api;
            const engine = this.syncEngine;
            const vaultId = this.settings.vaultId;
            if (!api || !vaultId) throw new Error("Not enrolled yet — rollback unavailable");
            await api.rollbackVault(vaultId, root);
            lease.assertCurrent();
            await engine?.forceSync();
            lease.assertCurrent();
        });
    }

    /** Gathers a human-readable snapshot of plugin state + live diagnostics. */
    async getDebugInfo(): Promise<string> {
        return this.runCurrentHostWork("sync", lease =>
            this.getDebugInfoOwned(() => lease.assertCurrent()));
    }

    private async getDebugInfoOwned(assertCurrent: () => void): Promise<string> {
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
        for (const line of formatWasmMemoryDebug(this.wasm)) push(line);
        for (const line of formatTreeChunkMemoryDebug(this.tree)) push(line);
        for (const line of formatTreeMetadataMemoryDebug(this.tree)) push(line);
        for (const line of formatReplacementInputMemoryDebug(this.tree)) push(line);
        for (const line of formatReplacementV1EntriesMemoryDebug(this.tree)) push(line);
        for (const line of formatReplacementV1GraphMemoryDebug(this.tree)) push(line);
        for (const line of formatReplacementSortMemoryDebug(this.tree)) push(line);
        for (const line of formatReplacementV2PlanningMemoryDebug(this.tree)) push(line);
        for (const line of formatReplacementV2RangesMemoryDebug(this.tree)) push(line);
        for (const line of formatReplacementV2PostSortMemoryDebug(this.tree)) push(line);
        const workerStats = this.hashWorkers?.stats();
        const browserWorkerStats = this.browserHashWorkers?.stats();
        push(`Hash workers:      ${workerStats && workerStats.wasmMode !== "unavailable" ? `${workerStats.ready}/${workerStats.limit} ready · ${workerStats.workers}/${workerStats.capacity} resident · ${workerStats.active} active · ${workerStats.queued} queued` : browserWorkerStats?.state === "ready" ? `browser ${browserWorkerStats.wasmMode} · ${browserWorkerStats.active} active · ${browserWorkerStats.queued} queued · ${formatDebugBytes(browserWorkerStats.ownedBytes)} transferred` : "renderer fallback"}`);
        const workerDiagnostics = this.hashWorkers?.diagnostics();
        push(`Worker lifecycle:  ${this.hashWorkerDiagnostic?.reason ?? (runtimeForHost(Platform) === "mobile" ? "desktop pool not applicable" : "not attempted")} · ${workerStats?.restarting ?? 0} restarting`);
        if (workerDiagnostics) {
            push(`Worker attempts:   ${workerDiagnostics.startupAttempts} starts · ${workerDiagnostics.failureCount} failures · ${workerDiagnostics.restartCount} restarts · last failure ${workerDiagnostics.lastFailure?.reason ?? "none"}`);
            push(`Worker ownership:  ${workerDiagnostics.nativeOwnedWorkers} native · ${workerDiagnostics.nativeSpawnsInProgress} starting · drain ${workerDiagnostics.nativeDrainRequested ? workerDiagnostics.nativeDrained ? "complete" : "waiting" : "not requested"}`);
        }
        const browserWorkerDiagnostics = this.browserHashWorkers?.diagnostics();
        if (browserWorkerStats || this.browserHashWorkerDiagnostic) {
            const blocked = browserWorkerDiagnostics?.qualified === true &&
                browserWorkerStats?.fallbackSafe === false;
            const fallback = blocked ? "blocked" :
                browserWorkerDiagnostics?.qualified === true ? "allowed" : "renderer";
            push(`Browser worker:    ${this.browserHashWorkerDiagnostic?.reason ?? browserWorkerStats?.state ?? "not attempted"} · generation ${browserWorkerStats?.generation ?? 0} · fallback ${fallback}`);
        }
        if (browserWorkerDiagnostics) {
            push(`Browser ownership: ${formatDebugBytes(browserWorkerStats?.ownedBytes ?? 0)} active · ${formatDebugBytes(browserWorkerStats?.quarantinedPayloadBytes ?? 0)} quarantined · ${formatDebugBytes(browserWorkerDiagnostics.unconfirmedReleaseBytes)} unconfirmed · ${browserWorkerDiagnostics.cleanupEvidence}`);
        }
        if (this.failedWorkerConstruction.size) push(`Worker cleanup:    ${this.failedWorkerConstruction.size} failed construction lifetimes awaiting native completion`);
        const scheduler = workSchedulerSnapshot();
        push(`Work scheduler:    ${scheduler.backend} · ${scheduler.pendingJobs}/${scheduler.maxPendingJobs} queued ` +
            `(i ${scheduler.pendingByLane.interactive} · u ${scheduler.pendingByLane["urgent-file"]} · ` +
            `b ${scheduler.pendingByLane.bulk} · m ${scheduler.pendingByLane.maintenance}) · ` +
            `oldest ${Math.round(scheduler.oldestWaitMs)}ms · expired deadlines ${scheduler.expiredDeadlines} · ` +
            `fallbacks ${scheduler.fallbackCount}`);
        push(`Plugin id:         ${this.manifest.id}`);
        push(`Manifest version:  ${this.manifest.version}`);
        push(`Bundle identity:   ${BUNDLE_BUILD_IDENTITY.semver} · ${BUNDLE_BUILD_IDENTITY.gitCommit} · ` +
            `${BUNDLE_BUILD_IDENTITY.sourceState}/${BUNDLE_BUILD_IDENTITY.buildMode}`);
        push(`Protocol identity: ${BUNDLE_BUILD_IDENTITY.protocol.schema} · HTTP ${BUNDLE_BUILD_IDENTITY.protocol.httpWire} · ` +
            BUNDLE_BUILD_IDENTITY.protocol.capabilities.join(", "));
        push(`Build verification: ${compareBuildIdentity(this.manifest.version, BUNDLE_BUILD_IDENTITY).diagnostic}`);
        push(`Browser probe:     ${this.browserProbeReport?.code ?? "not run (opt-in only)"} · not a production capability grant`);
        if (this.browserProbeReport) push(this.formatBrowserProbe());
        const prepared = this.preparedTransfers?.snapshot();
        if (prepared) {
            push(`Prepared hints:    ${prepared.ready ? "ready" : prepared.closed ? "closed" : "not ready"} · ${prepared.records}/${prepared.limits.records} files · ${prepared.chunks}/${prepared.limits.chunks} chunks`);
            push(`Prepared metadata: ${prepared.estimatedRetainedBytes}/${prepared.limits.retainedBytes} estimated bytes · queue ${prepared.queuedRequests}/${prepared.limits.queuedRequests} · full hash required on reuse`);
        }
        const confirmations = this.objectConfirmations?.snapshot();
        if (confirmations) {
            push(`Object presence:   ${confirmations.ready ? "ready" : confirmations.closed ? "closed" : "not ready"} · ${confirmations.records}/${confirmations.limits.records} generation-bound confirmations`);
            push(`Presence metadata: ${confirmations.estimatedRetainedBytes}/${confirmations.limits.retainedBytes} estimated bytes · queue ${confirmations.queuedRequests}/${confirmations.limits.queuedRequests}`);
        }
        push("");

        push("--- Performance (aggregate, path-free) ---");
        const statusOperations = perfTrace.activeSnapshots(5);
        const currentStatus = this.presentStatus(statusOperations);
        const statusDiagnostics = this.syncStatusPresenter.diagnostics(
            statusOperations.map(operation => operation.operationId),
        );
        push(`Current status:      ${currentStatus}`);
        push(`Status estimator:   ${statusDiagnostics.trackedRates} bounded operation rate(s) · ` +
            `${statusDiagnostics.activeRateSamples} trusted active sample(s)`);
        for (const line of perfTrace.formatDebug()) push(line);
        push("");

        push("--- Adaptive resource governor ---");
        if (this.resourceGovernor) {
            const governor = this.resourceGovernor.snapshot();
            const tuning = getHashTuning();
            push(`Platform family:    ${governor.family}`);
            push(`Selected profile:   ${governor.profile} (${governor.profileIndex + 1}/${governor.profileCount})`);
            push(`Actual limits:      hash ${workerStats && workerStats.wasmMode !== "unavailable" ? workerStats.limit : 1} · read ${tuning.readConcurrency} · network ${tuning.networkConcurrency} · apply ${tuning.applyConcurrency}`);
            push(`Memory/yield:       ${formatDebugBytes(tuning.transientBudgetBytes)} policy target · ${tuning.yieldBudgetMs}ms CPU slice`);
            const budget = transientMemorySnapshot();
            push(`Workset ledger:     ${formatDebugBytes(budget.usedBytes)}/${formatDebugBytes(budget.capacityBytes)} reserved · peak ${formatDebugBytes(budget.peakUsedBytes)} · ${budget.queuedRequests} waiting`);
            const managed = this.syncMemory.snapshot();
            push(`Managed aggregate:  ${formatDebugBytes(managed.usedBytes)}/${formatDebugBytes(managed.capacityBytes)} reserved · ` +
                `peak ${formatDebugBytes(managed.peakUsedBytes)} · ${managed.queuedRequests} waiting`);
            const output = this.rootTreeResidentAdmission.snapshot();
            push(`V2 output ledger:   ${formatDebugBytes(output.ledger.usedBytes)}/` +
                `${formatDebugBytes(output.ledger.capacityBytes)} scoped requested buffers · ` +
                `private/resident/retiring ${formatDebugBytes(output.privateBytes)}/` +
                `${formatDebugBytes(output.residentBytes)}/${formatDebugBytes(output.retiringBytes)}`);
            push("Memory coverage:    admitted source/transfer/apply and V2 replacement/mutation output; excludes full tree metadata, fixed WASM/worker heaps and host/RSS");
            push(`Control input:      ${governor.inputMode} · probe ${governor.probeAxis ?? "none"} · growth limited to demanded admitted stages`);
            push(`Latest throughput:  ${formatDebugRate(governor.throughput, governor.throughputUnit)}`);
            push(
                `Phase throughput:   read ${formatDebugRate(governor.phaseThroughput.read, "bytes/s")} · ` +
                `hash ${formatDebugRate(governor.phaseThroughput.hash, "bytes/s")} · ` +
                `upload ${formatDebugRate(governor.phaseThroughput.upload, "bytes/s")}`,
            );
            push(`Latest UI lag:      ${governor.eventLoopLagP95Ms === null ? "unmeasured" : `p95<=${governor.eventLoopLagP95Ms}ms`}`);
            push(`Last bottleneck:    ${governor.bottleneck}`);
            push(`Last decision:      ${governor.decision}`);
            push(`Recovery penalty:   ${governor.recoveryPenalty}`);
            const lifecycle = this.visibilityGate?.snapshot();
            push(`Visibility gate:    ${lifecycle?.paused
                ? `mobile quiesced (${lifecycle.lastQuiesceReason ?? "hidden"}) · epoch ${lifecycle.epoch} · active ${lifecycle.activeWork}`
                : governor.visible ? `visible · epoch ${lifecycle?.epoch ?? 0} · active ${lifecycle?.activeWork ?? 0}` : "hidden (desktop continues)"}`);
        } else {
            push("Governor not initialized yet.");
        }
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
                const rootsMatch = !!treeRoot && treeRoot === serverRoot;
                const pending = this.syncEngine.getPendingChangeCount();
                const deferred = this.syncEngine.getDeferredChangeSummary();
                const treeCount = this.syncEngine.getTreeFileCount();
                const baseCount = this.syncEngine.getSyncBaseCount();
                push(`Engine state:      ${this.syncEngine.getState()}`);
                const lifecycle = this.syncEngine.getLifecycleSnapshot();
                push(`Engine lifetime:   ${lifecycle.stopped ? "quiescing/stopped" : "active"} · ${lifecycle.operations.active} operation owners · ${lifecycle.callbacks.active} callback owners · capture ${lifecycle.callbacks.closed ? "closed" : "open"}`);
                push(`Tree roots match:  ${rootsMatch ? "yes (does not include pending work)" : "no"}`);
                push(`Pending changes:   ${pending} queued · ${deferred.count} deferred`);
                const rootSync = this.syncEngine.getRootSyncStatus();
                push(`Root recovery:     ${rootSync.enabled ? `${rootSync.loaded ? "local store ready" : "local store not ready"} · intent ${rootSync.pending ? "pending" : "none"} · repair ${rootSync.recoveryRequired ? "required" : "none"}${rootSync.recovering ? " · active" : ""}` : "unavailable"}`);
                if (rootSync.reason) push(`Root pending:      ${rootSync.reason}`);
                if (rootSync.lastBatch) {
                    const batch = rootSync.lastBatch;
                    push(`Last root batch:   ${batch.selected} selected · ${batch.cuts} journal cuts · ${formatDebugBytes(batch.metadataBytes)} estimated metadata`);
                    push(`Root batch holds:  ${Object.entries(batch.held).map(([reason, counts]) => `${reason} ${counts.paths}`).join(" · ")}`);
                }
                const repair = this.syncEngine.getLastRepairSummary();
                if (repair) push(`Content repair:    ${repair.incomplete ? "incomplete" : "complete"} · ${repair.result?.deferred ?? "unknown"} deferred · checked ${fmt(repair.ts)}`);
                if (deferred.count > 0) {
                    push(`Deferred work:     ${deferred.sourceTooLarge} source worksets too large · ${deferred.rangeUnavailable} range capability unavailable · ${deferred.dependentDeletes} held deletions · ${deferred.dependentChanges} linked changes`);
                    if (deferred.sourceTooLarge > 0) {
                        push(`Admission needed:  up to ${formatDebugBytes(deferred.maxRequiredBytes)} · capacity ${formatDebugBytes(deferred.minCapacityBytes)}`);
                    }
                    push(`Next retry:        ${deferred.nextRetryAt === null ? "capability/source change" : fmt(deferred.nextRetryAt)} · Sync now retries immediately`);
                }
                push(`Push blocked:      ${this.syncEngine.isPushBlocked() ? "YES — run Full Rescan" : "no"}`);
                push(`Re-enroll needed:  ${this.syncEngine.isReenrollmentRequired() ? "YES — automatic sync paused" : "no"}`);
                push(`Bulk change review: ${this.syncEngine.isBulkChangeReviewRequired() ? "YES — run Full Rescan to confirm" : "no"}`);
                const bulkApproval = this.syncEngine.getBulkChangeApproval();
                push(
                    `Bulk resume approval: ${bulkApproval
                        ? `${bulkApproval.active ? "active" : "saved"} · ` +
                            `${bulkApproval.observedChanges} changes · ` +
                            `delete limit ${bulkApproval.trackedDeletionLimit} · ` +
                            `approved ${fmt(bulkApproval.approvedAt)}`
                        : "none"}`,
                );
                push(`Tree format:       v${this.tree?.tree_version?.() ?? "?"}`);
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
        const diagnosticApi = this.api;
        if (!diagnosticApi) {
            push("ObsetyncApi not ready.");
        } else {
            try {
                push("ping() → ...");
                assertCurrent();
                const p = await diagnosticApi.ping();
                assertCurrent();
                push(`  Server URL:       ${p.serverUrl}`);
                push(`  Reachable:        ${p.ok ? "yes" : "no"}`);
                push(`  Transport:        ${p.transport}`);
            } catch (e: any) {
                push(`  ping failed:      ${e?.message ?? e}`);
            }
            try {
                assertCurrent();
                const bulk = await diagnosticApi.getBulkDiagnostics();
                assertCurrent();
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
                if (bulk.enabled) {
                    const circuit = bulk.wsCircuit ?? "unknown";
                    const retry = circuit === "open"
                        ? ` · HTTP selected, WS probe in ${formatDebugDuration(bulk.wsCircuitOpenForMs ?? 0)}`
                        : circuit === "half-open"
                            ? " · one WS health probe allowed"
                            : "";
                    push(`  WS routing:       ${circuit}${retry}`);
                }
                if (bulk.lastCarrier) {
                    const requestBytes = bulk.lastCarrierPayloadBytes ?? 0;
                    const expectedUsefulBytes = bulk.lastCarrierExpectedUsefulBytes ?? requestBytes;
                    push(
                        `  Last route:       ${bulk.lastCarrier.toUpperCase()} · ` +
                        `${bulk.lastCarrierLane ?? "control"} · ` +
                        `request ${formatDebugBytes(requestBytes)}` +
                        (expectedUsefulBytes === requestBytes
                            ? ""
                            : ` · expected useful ${formatDebugBytes(expectedUsefulBytes)}`) +
                        ` · ` +
                        `${formatTransportReason(bulk.lastCarrierReason ?? "unknown reason")}`,
                    );
                } else {
                    push(`  Last route:       no routed transfer yet`);
                }
                push(
                    `  Bulk policy:      ${bulk.bulkPreferenceEstablished
                        ? `${(bulk.bulkPreferredCarrier ?? "ws").toUpperCase()} preferred`
                        : "learning (WS first)"}`,
                );
                if (bulk.wsBulkThroughputBytesPerSecond !== undefined ||
                    bulk.httpBulkThroughputBytesPerSecond !== undefined) {
                    push(
                        `  Bulk estimates:   WS ${formatDebugRate(
                            bulk.wsBulkThroughputBytesPerSecond ?? null,
                            bulk.wsBulkThroughputBytesPerSecond === undefined ? null : "bytes/s",
                        )} · queue ${bulk.wsBulkQueueWaitMs === undefined
                            ? "unmeasured"
                            : formatDebugDuration(bulk.wsBulkQueueWaitMs)}; ` +
                        `HTTP ${formatDebugRate(
                            bulk.httpBulkThroughputBytesPerSecond ?? null,
                            bulk.httpBulkThroughputBytesPerSecond === undefined ? null : "bytes/s",
                        )} · queue ${bulk.httpBulkQueueWaitMs === undefined
                            ? "unmeasured"
                            : formatDebugDuration(bulk.httpBulkQueueWaitMs)}`,
                    );
                }
            } catch (e: any) {
                push(`  Bulk HTTP:        negotiation failed: ${e?.message ?? e}`);
                push(`  WS data:          negotiation unavailable`);
            }
            if (this.settings.vaultId) {
                try {
                    assertCurrent();
                    const tree = await diagnosticApi.negotiateTreeVersion(this.settings.vaultId);
                    assertCurrent();
                    push(
                        `  Tree protocol:    v${tree.currentVersion} · ${tree.activation} · ` +
                        `fleet ${tree.readyDevices}/${tree.enrolledDevices}`,
                    );
                    push(`getRoot("${this.settings.vaultId}") → ...`);
                    const rootBytes = await diagnosticApi.getRoot(this.settings.vaultId);
                    assertCurrent();
                    if (rootBytes === null) {
                        push(`  Server has no vault with this ID.`);
                    } else {
                        const hash = this.wasm?.wasm_root_hash_from_bytes(rootBytes) ?? null;
                        const version = this.wasm?.wasm_root_version_from_bytes(rootBytes) ?? null;
                        push(`  Server root hash: ${trunc(hash, 24)}`);
                        push(`  Server tree:      v${version ?? "?"}`);
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
        this.lastSavedSettings = cloneSyncSettings(this.settings);
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
        this.settingsMigrationPending = settingsChanged;
    }

    async saveSettings(): Promise<void> {
        if (this.unloaded) return;
        try { this.legacyDowngradeHost?.authorize("settings").assertCurrent(); }
        catch (error) {
            if (this.lastSavedSettings) this.settings = cloneSyncSettings(this.lastSavedSettings);
            throw error;
        }
        const snapshot = cloneSyncSettings(this.settings);
        const work = this.settingsWrites.save(snapshot);
        this.settingsWork.add(work);
        try {
            await work;
            this.lastSavedSettings = snapshot;
            this.settingsMigrationPending = false;
        }
        finally { this.settingsWork.delete(work); }
    }

    authorizeSettingsMutation(): void {
        this.legacyDowngradeHost?.authorize("settings").assertCurrent();
    }

    /** Enroll this device with the server using an enrollment code. */
    async enroll(code: string): Promise<void> {
        return this.runCurrentHostWork("settings", lease =>
            this.enrollOwned(code, () => lease.assertCurrent()));
    }

    private async enrollOwned(code: string, assertCurrent: () => void): Promise<void> {
        // Enrollment is over plain HTTP to the admin port. We pass empty
        // strings for box_pub + bearer_token since claimEnrollment doesn't
        // need an ObsetyncSecureChannel (admin endpoint is unauthenticated).
        const tempApi = new ObsetyncApi(this.settings.serverUrl, "", "");
        const result = await tempApi.claimEnrollment(code);
        assertCurrent();

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
        assertCurrent();

        await this.initSync();
        assertCurrent();
    }

    async syncNow(): Promise<void> {
        const hostToken = this.legacyDowngradeHost?.authorize("sync");
        if ((!this.syncEngine || this.syncEngine.isStopped()) &&
            this.legacyDowngradeHost?.snapshot().state === "current-active" &&
            this.settings.enrolled && this.settings.serverUrl) {
            await this.initSync();
        }
        if (!this.syncEngine) {
            new Notice("Sync not initialized. Check settings.");
            return;
        }
        try {
            hostToken?.assertCurrent();
            await this.syncEngine.forceSync();
        } catch (e: any) {
            this.updateStatusBar("sync ✗");
            throw e;
        }
    }

    async fullScan(): Promise<void> {
        const hostToken = this.legacyDowngradeHost?.authorize("sync");
        if (!this.syncEngine) {
            new Notice("Sync not initialized. Check settings.");
            return;
        }
        try {
            hostToken?.assertCurrent();
            await this.syncEngine.fullScan();
        } catch (e: any) {
            this.updateStatusBar("sync ✗");
            throw e;
        }
    }

    private async showConflicts(): Promise<void> {
        const hostToken = this.legacyDowngradeHost?.authorize("sync");
        const previous = this.conflictModal;
        previous?.revoke();
        await previous?.closeAndDrain();
        if (this.conflictModal === previous) this.conflictModal = null;
        hostToken?.assertCurrent();
        const conflicts = findConflicts(this.io);
        if (conflicts.length === 0) {
            new Notice("No sync conflicts found.");
            return;
        }
        const modal = new ObsetyncConflictModal(this.app, this.io, conflicts, () => {
            if (this.conflictModal === modal) this.conflictModal = null;
            new Notice("All conflicts resolved.");
        });
        this.conflictModal = modal;
        modal.open();
    }

    private async initSync(): Promise<void> {
        if (this.unloaded) return;
        const hostToken = this.legacyDowngradeHost?.authorize("init");
        const generation = ++this.syncInitGeneration;
        // Spans the whole startup cost: WASM load + engine start (pull,
        // journal recovery, metadata scan) — finally, so a failed init still
        // closes its span and shows how long it ran before dying.
        const endSpan = perfSpan("init");
        try {
            await this.initSyncInner(generation, hostToken);
        } catch (error) {
            // An older failed initialization must not close its replacement's pool.
            try { hostToken?.assertCurrent(); }
            catch { return; }
            if (this.unloaded || generation !== this.syncInitGeneration) return;
            const workers = this.hashWorkers;
            await workers?.closeAndDrainNative();
            await this.syncEngine?.quiesceAndDrain();
            if (this.hashWorkers === workers) this.hashWorkers = null;
            if (this.unloaded || generation !== this.syncInitGeneration) return;
            throw error;
        } finally {
            endSpan();
        }
    }

    /** One browser worker per plugin lifetime. Browser termination has no
     * native release acknowledgement, so re-init reuses this owner instead of
     * stacking a replacement heap over an unconfirmed predecessor. */
    private ensureBrowserHashWorkers(): Promise<BrowserHashWorkerPool | null> {
        if (this.browserHashStartup) return this.browserHashStartup;
        this.browserHashStartup = (async () => {
            const pool = createBrowserHashWorkerPool(browserHashWorkerSource, {
                scalarWasm: embeddedScalarWasmBytes.slice().buffer as ArrayBuffer,
                simdWasm: embeddedSimdWasmBytes.slice().buffer as ArrayBuffer,
                onDiagnostic: event => {
                    if (this.unloaded) return;
                    this.browserHashWorkerDiagnostic = event;
                    if (event.kind === "failure" || event.kind === "unavailable") {
                        const worker = this.browserHashWorkers;
                        const blocked = worker?.diagnostics().qualified === true &&
                            worker.stats().fallbackSafe === false;
                        console.warn(`[obsetync] browser hash worker ${event.reason} · fallback ${blocked ? "blocked" : "renderer"}`);
                    }
                },
            });
            this.browserHashWorkers = pool;
            if (!pool) return null;
            try { await pool.ready(); }
            catch { /* Synthetic qualification failure keeps renderer hashing for this load. */ }
            return pool;
        })();
        return this.browserHashStartup;
    }

    private async initSyncInner(generation: number, hostToken?: LegacyDowngradeGenerationToken): Promise<void> {
        const isCurrent = () => {
            if (this.unloaded || generation !== this.syncInitGeneration) return false;
            try { hostToken?.assertCurrent(); return true; }
            catch { return false; }
        };
        if (!isCurrent()) return;
        // Capture stays active while old accepted transactions/native owners
        // drain. A bounded close timeout is not permission to share its base.
        const previousEngine = this.syncEngine;
        const previousEngineWasStopped = previousEngine?.isStopped() ?? false;
        const previousWorkers = this.hashWorkers;
        const previousTree = this.tree;
        this.hashWorkerGeneration++;
        if (previousEngine || previousWorkers || this.failedWorkerConstruction.size) {
            this.updateStatusBar("sync ⏳ restarting");
        }
        await settleRuntimeOwners([
            startRuntimeOwner(() => previousEngine?.quiesceAndDrain()),
            startRuntimeOwner(() => previousWorkers?.closeAndDrainNative()),
            ...this.failedWorkerConstruction,
        ]);
        if (!isCurrent()) return;
        await this.retireTree(previousTree);
        if (!isCurrent()) return;
        this.hashWorkers = null;
        this.hashWorkerDiagnostic = null;

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

        // All scope/policy primitives belong to this initialization cut. UI
        // settings remain mutable while WASM/cache/negotiation are awaited.
        const syncSettings = { ...this.settings, ignorePatterns: [...this.settings.ignorePatterns] };
        // Create API client with the pinned server pubkey + bearer token.
        const api = new ObsetyncApi(
            syncSettings.serverUrl,
            syncSettings.serverBoxPub,
            syncSettings.bearerToken,
            {
                get: () => ({
                    wireVersion: this.settings.wireVersion,
                    esPub: this.settings.esPub,
                    esPubValidUntil: this.settings.esPubValidUntil,
                    lastOutgoingSeq: this.settings.lastOutgoingSeq,
                }),
                update: async (patch) => {
                    if (!isCurrent()) return;
                    Object.assign(this.settings, patch);
                    await this.saveSettings();
                },
            },
            runtimeForHost(Platform),
        );
        this.api = api;
        const rootScope = captureRootStreamScope(syncSettings, api, api.baseUrl);
        const assertRuntimeScope = () => {
            if (!isCurrent()) throw new Error("root runtime generation changed");
            rootScope.assertCurrent(this.settings, this.api, this.api.baseUrl);
        };
        // Capability I/O overlaps WASM compilation/worker startup. The result
        // selects the active tree format before the engine can build a local
        // candidate; failure remains retryable through the normal first root
        // request and never falls back by mutating enrollment state.
        const treeNegotiationPending = api
            .negotiateTreeVersion(rootScope.vaultId)
            .then(
                (result) => ({ result, error: null as unknown }),
                (error) => ({ result: null, error }),
            );

        // Load the bundled WASM module. Sync must fail closed if this cannot
        // initialize: development hash stubs are not content-address compatible
        // with the server and must never participate in a real vault.
        const wasmReady = await publishStartupResult(this.loadWasm(), isCurrent, (selected) => {
            this.wasm = selected.exports;
            this.resourceGovernor?.recordSimdAvailability(selected.mode === "simd");
            perfTrace.setProfile({ ...perfTrace.getProfile(), wasmMode: selected.mode });
            console.log(`[obsetync] WASM loaded (${selected.mode}, ${selected.bytes} bytes, inline)`);
        });
        if (!wasmReady || !isCurrent()) return;
        assertRuntimeScope();
        const wasm = wasmReady.value.exports;
        if (runtimeForHost(Platform) === "desktop" && wasmReady.value.mode === "simd") {
            const tuning = getHashTuning();
            const generation = ++this.hashWorkerGeneration;
            this.hashWorkers = createDesktopHashWorkerPool(hashWorkerSource, {
                initialWorkers: tuning.hashConcurrency,
                maxWorkers: tuning.maxHashConcurrency,
                onConstructionCleanup: (pending) => {
                    this.failedWorkerConstruction.add(pending);
                    void pending.then(() => this.failedWorkerConstruction.delete(pending), () => {});
                },
                onDiagnostic: (event) => {
                    if (!isCurrent() || generation !== this.hashWorkerGeneration) return;
                    this.hashWorkerDiagnostic = event;
                    if (event.kind === "failure" || event.kind === "unavailable") {
                        console.warn(`[obsetync] hash worker ${event.reason} · slot ${event.slot ?? "none"} · retry ${event.retrying ? "scheduled" : "no"}`);
                    }
                },
            });
            if (this.hashWorkers) {
                perfTrace.setProfile({
                    ...perfTrace.getProfile(),
                    hashConcurrency: this.hashWorkers.stats().limit,
                });
                console.log(
                    `[obsetync] desktop hash pool starting ` +
                    `(${this.hashWorkers.stats().limit}/${this.hashWorkers.workerCount} SIMD workers)`,
                );
            } else {
                console.warn("[obsetync] desktop hash workers unavailable; using renderer hashing");
            }
        } else if (runtimeForHost(Platform) === "desktop") {
            console.log("[obsetync] scalar WASM selected; using renderer hashing");
        }
        let browserHash: BrowserHashWorkerRuntime | null = null;
        if (runtimeForHost(Platform) === "mobile") {
            const pool = await this.ensureBrowserHashWorkers();
            if (!isCurrent()) return;
            if (pool && (pool.stats().state === "ready" || pool.diagnostics().qualified)) {
                const assertCurrent = () => {
                    if (!isCurrent() || this.browserHashWorkers !== pool) {
                        throw new Error("browser hash worker generation changed");
                    }
                };
                browserHash = { pool, assertCurrent };
                if (pool.stats().state === "ready") {
                    console.log(`[obsetync] browser hash worker ready (${pool.stats().wasmMode})`);
                }
            } else {
                console.warn("[obsetync] browser hash worker unavailable; using renderer hashing");
            }
        }
        if (this.failedWorkerConstruction.size) {
            await settleRuntimeOwners([...this.failedWorkerConstruction]);
            if (!isCurrent()) return;
        }
        assertRuntimeScope();
        const hashWorkers = this.hashWorkers;

        // Create WASM tree.
        const tree = new wasm.WasmTree(
            rootScope.vaultId,
            syncSettings.deviceName
        );
        this.tree = tree;

        // Extract the cached root hash for X-Parent-Root on the first push after restart.
        // We do NOT call tree.load_root() here: load_root only stores the root node in
        // MemoryChunkStore, NOT its children (LeafChunk/InternalNode). Calling update_entry
        // on such a tree triggers update_tree → load_all_entries(store, child_hash) →
        // ChunkError::NotFound. The tree always bootstraps from sync-base on first push
        // (push.ts: if (!tree.root_hash_hex())), which correctly populates the full store.
        let cachedRootHash: string | null = null;
        let cachedTreeVersion: 1 | 2 = 1;
        const cachedRoot = await this.loadCachedRoot();
        if (!isCurrent()) return;
        assertRuntimeScope();
        if (cachedRoot) {
            try {
                cachedRootHash = wasm.wasm_root_hash_from_bytes(cachedRoot) ?? null;
                const version = wasm.wasm_root_version_from_bytes(cachedRoot);
                if (version !== 1 && version !== 2) {
                    throw new Error(`unsupported cached tree version ${version}`);
                }
                cachedTreeVersion = version;
                tree.set_tree_version(version);
                console.log("[obsetync] cached root hash:", cachedRootHash?.slice(0, 12));
            } catch (e) {
                console.warn("[obsetync] failed to read cached root hash:", e);
            }
        }
        // Construct/capture locally before awaiting the already-running capability
        // request. A slow/offline negotiation must not leave edits unjournaled.
        const engine = new ObsetyncSyncEngine(
            this.app,
            api,
            this.io,
            this.syncBase,
            this.journal,
            wasm,
            tree,
            rootScope.vaultId,
            syncSettings.syncIntervalMs,
            syncSettings.syncPriority,
            (text) => { if (isCurrent()) this.updateStatusBar(text); },
            cachedRootHash,
            syncSettings.syncObsidianConfig,
            syncSettings.deviceName || "device",
            syncSettings.realtimeWs,
            syncSettings.sharePresence,
            syncSettings.ignorePatterns,
            this.operationCheckpoint,
            syncSettings.autoSync,
            hashWorkers,
            this.visibilityGate,
            {
                plan: this.preparedTransfers,
                memoryArbiter: this.syncMemory,
                confirmations: this.objectConfirmations,
                scopeForTree: preparedScopeForTree(syncSettings),
            },
            { vaultId: rootScope.vaultId, deviceId: rootScope.deviceId, scopeHash: rootScope.scopeHash,
                assertApiScope: assertRuntimeScope },
            this.rootTreeResidentAdmission,
            browserHash,
        );
        // A normally running predecessor hands over its finite callback cut.
        // A predecessor already terminally frozen for compatibility cannot
        // accept a handoff, so the replacement starts from reloaded durable
        // state after the compatibility owner has retired.
        const captureHandoff = previousEngine && !previousEngineWasStopped
            ? previousEngine.handoffCaptureTo(engine)
            : undefined;
        this.syncEngine = engine;
        await captureHandoff;
        if (!isCurrent()) return;
        if (previousEngine) {
            // Failed old publication may have poisoned a writer. Reload only
            // after every old callback/echo ACK, while new capture stays live.
            await engine.reloadPersistence();
            if (!isCurrent()) return;
        }

        this.updateStatusBar("sync ↓");
        const activated = await activatePreparedSync({
            engine,
            negotiation: treeNegotiationPending,
            isCurrent: () => isCurrent() && this.syncEngine === engine,
            selectTree: (negotiatedTree) => {
                assertRuntimeScope();
                if (negotiatedTree.result) {
                    tree.set_tree_version(negotiatedTree.result.currentVersion);
                    console.log(
                        `[obsetync] Tree v${negotiatedTree.result.currentVersion} negotiated ` +
                        `(${negotiatedTree.result.activation}, fleet ` +
                        `${negotiatedTree.result.readyDevices}/${negotiatedTree.result.enrolledDevices})`,
                    );
                } else {
                    tree.set_tree_version(cachedTreeVersion);
                    console.warn(
                        `[obsetync] Tree negotiation deferred; using cached/default v${cachedTreeVersion}:`,
                        negotiatedTree.error,
                    );
                }
            },
        });
        if (!activated || !isCurrent()) return;
        if (engine.isReenrollmentRequired()) {
            this.updateStatusBar("sync ⚠ re-enroll");
        } else if (engine.isBulkChangeReviewRequired()) {
            this.updateStatusBar("sync ⚠ review");
        }
        // Otherwise keep the engine-owned status: start() can return with
        // deferred or in-flight work, not only a completely synced vault.
    }

    /** Called only after every operation that can use this tree has drained.
     * Live capture uses shared journal/guards/Hasher, not the retired tree.
     * Mark before calling free: a throwing destructor must not be retried as
     * an unproved second native free. Missing disposal is a lifecycle error. */
    private async retireTree(tree: WasmTree | undefined): Promise<void> {
        if (this.treeRetirementFailed) throw new Error("WASM tree retirement requires runtime recovery");
        if (!tree || this.retiredTrees.has(tree)) return;
        const pending = this.retiringTrees.get(tree);
        if (pending) return pending;
        const retiring = (async () => {
            // A failed replacement cleanup retains its exact wrapper and
            // admission. Do not free that owner or close the scheduler/pool
            // until its cooperative native retirement has really completed.
            await drainTreeReachabilityRetirement(tree);
            await drainTreeCandidateMutationRetirement(tree);
            await drainRootTreeRetirement(tree);
            if (typeof tree.free !== "function") throw new Error("WASM tree disposal is unavailable");
            if (this.tree === tree) this.tree = undefined;
            this.retiredTrees.add(tree);
            try {
                tree.free();
                this.rootTreeResidentAdmission.releaseResidentAfterFree(tree);
            }
            catch (error) { this.treeRetirementFailed = true; throw error; }
        })();
        this.retiringTrees.set(tree, retiring);
        try { await retiring; }
        finally { this.retiringTrees.delete(tree); }
    }

    private async loadWasm(): Promise<WasmSelection<WasmModule>> {
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
                    // This is one session-shared loader, not one generation's
                    // runtime selection. It never updates governor/profile here.
                    if (!this.unloaded) console.warn(`[obsetync] SIMD WASM unavailable (${reason}); using scalar`);
                },
            });
            // Compilation can finish after replacement/unload. Only the guarded
            // publisher in initSyncInner may install exports or mutate profiles.
            return await this.wasmLoader();
        } catch (e: any) {
            const msg = e?.message ?? String(e);
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
        this.statusBarText = text;
        this.renderStatusBar();
    }

    private renderStatusBar(): void {
        const legacy = this.legacyDowngradeHost?.snapshot();
        if (legacy && legacy.state !== "none" && legacy.state !== "current-active") {
            this.statusBarEl?.setText(legacyDowngradeStatus(legacy).text);
            return;
        }
        // Presence suffix: how many OTHER devices are active right now (Ph3).
        const peers = this.syncEngine?.getActivePeerCount() ?? 0;
        const active = this.presentStatus(perfTrace.activeSnapshots(5));
        this.statusBarEl?.setText(peers > 0 ? `${active} · 👥${peers}` : active);
    }

    private presentStatus(operations = perfTrace.activeSnapshots(5)): string {
        return this.syncStatusPresenter.render(
            this.statusBarText,
            operations,
            this.syncStatusTruth(),
        );
    }

    private syncStatusTruth(): SyncStatusTruth | undefined {
        const engine = this.syncEngine;
        if (!engine) return undefined;
        try {
            const root = engine.getRootSyncStatus();
            return {
                busy: engine.isBusy(),
                pendingChanges: engine.getPendingChangeCount(),
                // Pending count already includes cooled/dependent paths. The
                // detailed deferred summary walks its records and belongs in
                // the on-demand debug export, not this one-second UI path.
                deferredChanges: 0,
                rootPending: root.pending,
                rootRecovering: root.recovering,
                rootRecoveryRequired: root.recoveryRequired,
                error: engine.getState() === "error",
            };
        } catch {
            // Status rendering is diagnostic only and must not break its sole
            // refresh interval while an engine generation is retiring.
            return { busy: true, pendingChanges: 0, deferredChanges: 0,
                rootPending: false, rootRecovering: false,
                rootRecoveryRequired: false, error: false };
        }
    }

    private applyResourceProfile(profile: ResourceProfile, reason: string): void {
        const applyHashTuning = (tuning: HashTuning) => {
            const current = perfTrace.getProfile();
            const workers = this.hashWorkers?.stats();
            const actualHashConcurrency = workers && workers.wasmMode !== "unavailable" ? workers.limit : 1;
            perfTrace.setProfile({
                ...current,
                hashConcurrency: actualHashConcurrency,
                readConcurrency: tuning.readConcurrency,
                networkConcurrency: tuning.networkConcurrency,
                feedBytes: tuning.feedBytes,
                batchBytes: tuning.maxBatchBytes,
            });
        };
        if (this.hashWorkers) {
            this.hashWorkers.setActiveWorkerLimit(
                Math.min(profile.tuning.hashConcurrency, this.hashWorkers.workerCount),
            );
        }
        // A network-only control change must not undo a feed-size reduction
        // learned independently by the cooperative hashing loop.
        const tuning = this.resourceGovernor?.snapshot().inputMode === "active-windows"
            ? { ...profile.tuning, feedBytes: Math.min(getHashTuning().feedBytes, profile.tuning.feedBytes) }
            : profile.tuning;
        const selected = configureHashTuning(tuning, applyHashTuning);
        configureTransientMemory(selected, this.syncMemory);
        // Same profile target, independent ledger: these capacities are not
        // additive evidence of a global heap/RSS ceiling.
        this.rootTreeResidentAdmission.setCapacity(selected.transientBudgetBytes);
        const workers = this.hashWorkers?.stats();
        perfTrace.setProfile({
            ...perfTrace.getProfile(),
            runtime: selected.runtime,
            architecture: this.resourceGovernor?.environment.architecture ?? "unknown",
            hashConcurrency: workers && workers.wasmMode !== "unavailable" ? workers.limit : 1,
            readConcurrency: selected.readConcurrency,
            networkConcurrency: selected.networkConcurrency,
            feedBytes: selected.feedBytes,
            batchBytes: selected.maxBatchBytes,
        });
        console.log(
            `[obsetync] resource profile ${profile.family}/${profile.name}: ` +
            `hash=${selected.hashConcurrency} read=${selected.readConcurrency} ` +
            `network=${selected.networkConcurrency} batch=${formatDebugBytes(selected.maxBatchBytes)} ` +
            `(${reason})`,
        );
    }
}
