import { App, PluginSettingTab, Setting, Notice } from "obsidian";
import type SyncPlugin from "./main";

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
    certPem: string;
    keyPem: string;
    fingerprint: string;
    bearerToken: string;
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
    certPem: "",
    keyPem: "",
    fingerprint: "",
    bearerToken: "",
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
        if (!this.plugin.settings.enrolled) {
            const enrollDiv = containerEl.createDiv();
            enrollDiv.createEl("h3", { text: "Enrollment" });

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
                    try {
                        await this.plugin.syncNow();
                        new Notice("Sync complete.");
                    } catch (e: any) {
                        new Notice(`Sync failed: ${e.message}`);
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
                    try {
                        await this.plugin.fullScan();
                        new Notice("Rescan complete.");
                    } catch (e: any) {
                        new Notice(`Rescan failed: ${e.message}`);
                    }
                })
            );
    }
}
