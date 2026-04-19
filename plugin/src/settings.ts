import { App, PluginSettingTab, Setting, Notice } from "obsidian";
import type SyncPlugin from "./main";
import { DebugInfoModal } from "./debug-modal";

export type SyncPriority =
    | "sequential"   // as found (default)
    | "oldest"       // mtime ascending
    | "newest"       // mtime descending
    | "smallest"     // size ascending
    | "biggest"      // size descending
    | "alphabetic"   // path A→Z
    | "random";      // shuffled

export interface SyncSettings {
    serverUrl: string;
    vaultId: string;
    deviceName: string;
    syncIntervalMs: number;
    autoSync: boolean;
    syncPriority: SyncPriority;
    syncObsidianConfig: boolean;
    enrolled: boolean;
    deviceId: string;
    bearerToken: string;
    serverBoxPub: string;
}

export const DEFAULT_SETTINGS: SyncSettings = {
    serverUrl: "",
    vaultId: "",
    deviceName: "",
    syncIntervalMs: 30000,
    autoSync: true,
    syncPriority: "sequential",
    syncObsidianConfig: false,
    enrolled: false,
    deviceId: "",
    bearerToken: "",
    serverBoxPub: "",
};

export class SyncSettingTab extends PluginSettingTab {
    constructor(app: App, private plugin: SyncPlugin) {
        super(app, plugin);
    }

    display(): void {
        const { containerEl } = this;
        containerEl.empty();

        containerEl.createEl("h2", { text: "ObsetyNC Sync" });

        // Connection status.
        const statusEl = containerEl.createDiv({ cls: "obsetync-status" });
        if (this.plugin.settings.enrolled) {
            statusEl.createEl("span", {
                text: "Connected",
                cls: "obsetync-connected",
            });
            statusEl.createEl("span", {
                text: ` — ${this.plugin.settings.deviceName}`,
            });
        } else {
            statusEl.createEl("span", {
                text: "Not enrolled",
                cls: "obsetync-disconnected",
            });
        }

        // Always-visible sync status — shows non-sensitive snapshot of what
        // the engine knows: state, last sync time, truncated root hashes,
        // file counts, last error. No fingerprints, tokens, or cert bytes.
        this.renderStatusBox(containerEl);

        // Server URL.
        new Setting(containerEl)
            .setName("Server URL")
            .setDesc("Your ObsetyNC server address (e.g., http://localhost:27182)")
            .addText((text) =>
                text
                    .setPlaceholder("https://sync.example.com:27182")
                    .setValue(this.plugin.settings.serverUrl)
                    .onChange(async (value) => {
                        this.plugin.settings.serverUrl = value;
                        await this.plugin.saveSettings();
                    })
            );

        // Vault ID.
        new Setting(containerEl)
            .setName("Vault ID")
            .setDesc("Unique identifier for this vault on the server.")
            .addText((text) =>
                text
                    .setPlaceholder("my-vault")
                    .setValue(this.plugin.settings.vaultId)
                    .onChange(async (value) => {
                        this.plugin.settings.vaultId = value;
                        await this.plugin.saveSettings();
                    })
            );

        // Device name.
        new Setting(containerEl)
            .setName("Device Name")
            .setDesc("Human-readable name for this device.")
            .addText((text) =>
                text
                    .setPlaceholder("Desktop Home")
                    .setValue(this.plugin.settings.deviceName)
                    .onChange(async (value) => {
                        this.plugin.settings.deviceName = value;
                        await this.plugin.saveSettings();
                    })
            );

        // Enrollment.
        const enrollDiv = containerEl.createDiv();
        enrollDiv.createEl("h3", { text: "Enrollment" });

        if (!this.plugin.settings.enrolled) {
            let enrollCode = "";
            new Setting(enrollDiv)
                .setName("Enrollment Code")
                .setDesc(
                    "Enter the code from the server admin GUI to connect this device."
                )
                .addText((text) =>
                    text
                        .setPlaceholder("AXBR-7742")
                        .onChange((value) => {
                            enrollCode = value;
                        })
                )
                .addButton((btn) =>
                    btn.setButtonText("Enroll").onClick(async () => {
                        if (!enrollCode || !this.plugin.settings.serverUrl) {
                            new Notice("Enter server URL and enrollment code first.");
                            return;
                        }
                        try {
                            await this.plugin.enroll(enrollCode);
                            new Notice("Enrolled successfully!");
                            this.display(); // Refresh UI.
                        } catch (e: any) {
                            new Notice(`Enrollment failed: ${e.message}`);
                        }
                    })
                );
        } else {
            new Setting(enrollDiv)
                .setName("Reset enrollment")
                .setDesc(
                    "Clear this device's cert, key, and bearer token so you can enroll " +
                    "against a new server or re-enroll with a fresh code. Does NOT delete " +
                    "any notes — only the credentials in plugin settings."
                )
                .addButton((btn) =>
                    btn
                        .setButtonText("Reset enrollment")
                        .setWarning()
                        .onClick(async () => {
                            this.plugin.settings.enrolled    = false;
                            this.plugin.settings.deviceId     = "";
                            this.plugin.settings.bearerToken  = "";
                            this.plugin.settings.serverBoxPub = "";
                            await this.plugin.saveSettings();
                            new Notice("Enrollment reset. Enter a new code to re-enroll.");
                            this.display(); // Refresh UI — enrollment input reappears.
                        })
                );
        }

        // Sync settings.
        new Setting(containerEl)
            .setName("Sync Interval")
            .setDesc("How often to check for remote changes (seconds).")
            .addText((text) =>
                text
                    .setValue(String(this.plugin.settings.syncIntervalMs / 1000))
                    .onChange(async (value) => {
                        const secs = parseInt(value);
                        if (!isNaN(secs) && secs >= 5) {
                            this.plugin.settings.syncIntervalMs = secs * 1000;
                            await this.plugin.saveSettings();
                        }
                    })
            );

        new Setting(containerEl)
            .setName("Auto-Sync")
            .setDesc("Automatically sync when files change.")
            .addToggle((toggle) =>
                toggle
                    .setValue(this.plugin.settings.autoSync)
                    .onChange(async (value) => {
                        this.plugin.settings.autoSync = value;
                        await this.plugin.saveSettings();
                    })
            );

        new Setting(containerEl)
            .setName("Sync Obsidian config (.obsidian/)")
            .setDesc(
                "Include your .obsidian/ folder in sync — themes, plugin settings, " +
                "hotkeys, snippets, and templates will be identical across all your devices. " +
                "Off by default because .obsidian/ also contains plugin caches (e.g. Omnisearch " +
                "builds a full-text index of every note, often 200–500 MB) that waste bandwidth " +
                "and regenerate automatically on each device anyway. " +
                "The plugin's own state files (sync-base, journal, cached-root) are always " +
                "excluded regardless of this setting."
            )
            .addToggle((toggle) =>
                toggle
                    .setValue(this.plugin.settings.syncObsidianConfig)
                    .onChange(async (value) => {
                        this.plugin.settings.syncObsidianConfig = value;
                        await this.plugin.saveSettings();
                    })
            );

        new Setting(containerEl)
            .setName("Sync Priority")
            .setDesc("Order in which files are uploaded during a sync.")
            .addDropdown((drop) =>
                drop
                    .addOption("sequential", "Sequential (default)")
                    .addOption("newest", "Newest first")
                    .addOption("oldest", "Oldest first")
                    .addOption("smallest", "Smallest first")
                    .addOption("biggest", "Biggest first")
                    .addOption("alphabetic", "Alphabetic (A→Z)")
                    .addOption("random", "Random")
                    .setValue(this.plugin.settings.syncPriority)
                    .onChange(async (value) => {
                        this.plugin.settings.syncPriority = value as SyncPriority;
                        await this.plugin.saveSettings();
                    })
            );

        // Actions.
        containerEl.createEl("h3", { text: "Actions" });

        new Setting(containerEl)
            .setName("Sync Now")
            .setDesc("Force an immediate sync cycle.")
            .addButton((btn) =>
                btn.setButtonText("Sync Now").onClick(async () => {
                    const startedAt = Date.now();
                    try {
                        await this.plugin.syncNow();
                        const engine = this.plugin.syncEngineOrNull();
                        const err = engine?.getLastError();
                        if (err && err.ts >= startedAt) {
                            new Notice(
                                `Sync had errors: [${err.origin}] ${err.message.slice(0, 80)}${err.message.length > 80 ? "…" : ""}`
                            );
                        } else {
                            // Distinguish "nothing to do" from "applied changes"
                            // so the user isn't left wondering whether the
                            // click did anything.
                            const localRoot  = engine?.getLocalRootHash() ?? null;
                            const serverRoot = engine?.getLastObservedServerRoot() ?? null;
                            const inSync = !!localRoot && localRoot === serverRoot;
                            new Notice(inSync ? "Already up to date." : "Sync complete.");
                        }
                    } catch (e: any) {
                        new Notice(`Sync failed: ${e.message}`);
                    } finally {
                        this.display(); // refresh status box
                    }
                })
            );

        new Setting(containerEl)
            .setName("Show debug info")
            .setDesc(
                "Dump current settings, sync state, live ping/getRoot results, and recent " +
                "[obsetync] log lines. Share this when asking for help or diagnosing iOS issues."
            )
            .addButton((btn) =>
                btn.setButtonText("Show debug info").onClick(async () => {
                    const loading = new Notice("Gathering debug info…", 0);
                    try {
                        const text = await this.plugin.getDebugInfo();
                        loading.hide();
                        new DebugInfoModal(this.app, text).open();
                    } catch (e: any) {
                        loading.hide();
                        new Notice(`Debug info failed: ${e?.message ?? e}`);
                    }
                })
            );

        new Setting(containerEl)
            .setName("Full Rescan")
            .setDesc(
                "Scan all files and sync any untracked changes. May be slow on large vaults."
            )
            .addButton((btn) =>
                btn.setButtonText("Full Rescan").onClick(async () => {
                    const startedAt = Date.now();
                    try {
                        await this.plugin.fullScan();
                        const err = this.plugin.syncEngineOrNull()?.getLastError();
                        if (err && err.ts >= startedAt) {
                            new Notice(`Rescan had errors: [${err.origin}] ${err.message.slice(0, 80)}${err.message.length > 80 ? "…" : ""}`);
                        } else {
                            new Notice("Rescan complete.");
                        }
                    } catch (e: any) {
                        new Notice(`Rescan failed: ${e.message}`);
                    } finally {
                        this.display();
                    }
                })
            );

        new Setting(containerEl)
            .setName("Reconcile with server")
            .setDesc(
                "Verify the server actually holds every file in sync-base, " +
                "re-upload anything missing. Use after a server wipe or " +
                "when you suspect the server drifted from the client cache."
            )
            .addButton((btn) =>
                btn.setButtonText("Reconcile with server").onClick(async () => {
                    const engine = this.plugin.syncEngineOrNull();
                    if (!engine) {
                        new Notice("Sync engine not ready.");
                        return;
                    }
                    const startedAt = Date.now();
                    try {
                        const r = await engine.reconcileContent();
                        const total = r.smallUploaded + r.largeUploaded + r.treeChunksUploaded;
                        if (total === 0) {
                            new Notice("Server already has all content.");
                        } else {
                            new Notice(
                                `Uploaded ${r.smallUploaded} files, ` +
                                `${r.largeUploaded} large files, ` +
                                `${r.treeChunksUploaded} tree chunks.`
                            );
                        }
                        const err = engine.getLastError();
                        if (err && err.ts >= startedAt) {
                            new Notice(
                                `Reconcile had errors: [${err.origin}] ${err.message.slice(0, 80)}${err.message.length > 80 ? "…" : ""}`
                            );
                        }
                    } catch (e: any) {
                        new Notice(`Reconcile failed: ${e.message}`);
                    } finally {
                        this.display();
                    }
                })
            );
    }

    /** Non-sensitive status snapshot rendered inline in the settings tab. */
    private renderStatusBox(containerEl: HTMLElement): void {
        const box = containerEl.createDiv();
        box.setAttribute(
            "style",
            "margin: 12px 0 20px 0; padding: 10px 14px; " +
                "border: 1px solid var(--background-modifier-border); " +
                "border-radius: 6px; background: var(--background-primary-alt); " +
                "font-size: 12px; font-family: var(--font-monospace); line-height: 1.6;"
        );

        const engine = this.plugin.syncEngineOrNull();
        const t = (s: string | null | undefined, n = 16) =>
            !s ? "—" : s.length <= n ? s : s.slice(0, n) + "…";
        const relTime = (ms: number) => {
            if (!ms) return "never";
            const ago = Date.now() - ms;
            if (ago < 60_000) return "just now";
            if (ago < 3_600_000) return `${Math.floor(ago / 60_000)} min ago`;
            if (ago < 86_400_000) return `${Math.floor(ago / 3_600_000)} h ago`;
            return `${Math.floor(ago / 86_400_000)} d ago`;
        };

        const row = (label: string, value: string) => {
            const line = box.createDiv();
            const l = line.createSpan({ text: `${label.padEnd(14, " ")} ` });
            l.setAttribute("style", "color: var(--text-muted);");
            line.createSpan({ text: value });
        };

        const localRoot  = engine?.getLocalRootHash() ?? null;
        const serverRoot = engine?.getLastObservedServerRoot() ?? null;
        const inSync     = !!localRoot && !!serverRoot && localRoot === serverRoot;
        const syncLabel  = inSync
            ? "✓ in sync"
            : engine?.getState() ?? "not-initialized";

        row("Sync",         syncLabel);
        row("Last sync",    relTime(engine?.getLastSyncTimestamp() ?? 0));
        row("Local root",   t(localRoot));
        row("Server root",  t(serverRoot));
        row("sync-base",    `${engine?.getSyncBaseCount() ?? 0} entries`);
        row("Vault files",  `${engine?.getVaultFileCount() ?? 0}`);
        row("Enrolled",     this.plugin.settings.enrolled ? "yes" : "no");
        row("Bearer token", this.plugin.settings.bearerToken ? "present" : "missing");

        const err = engine?.getLastError();
        if (err) {
            const errLine = box.createDiv();
            errLine.setAttribute("style", "color: var(--text-error); margin-top: 4px;");
            errLine.setText(
                `Last error:   [${err.origin}] ${err.message.slice(0, 120)}${
                    err.message.length > 120 ? "…" : ""
                } (${relTime(err.ts)})`
            );
        }

        const refresh = box.createEl("button", { text: "↻ Refresh" });
        refresh.setAttribute(
            "style",
            "margin-top: 8px; padding: 4px 10px; font-size: 11px; cursor: pointer;"
        );
        refresh.onclick = () => this.display();
    }
}
