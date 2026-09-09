import assert from "node:assert/strict";
import test from "node:test";
import { build } from "esbuild";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const controls = new Map();

class Element {
    empty() {}
    createDiv() { return new Element(); }
    createEl() { return new Element(); }
    createSpan() { return new Element(); }
    setAttribute() {}
}

class Control {
    constructor(owner, kind) { this.owner = owner; this.kind = kind; this.inputEl = { rows: 0, addClass() {} }; }
    setPlaceholder() { return this; }
    setValue() { return this; }
    addOption() { return this; }
    setButtonText() { return this; }
    setWarning() { return this; }
    setCta() { return this; }
    removeCta() { return this; }
    onChange(callback) { controls.set(`${this.owner.name}:${this.kind}`, callback); return this; }
    onClick(callback) { controls.set(`${this.owner.name}:${this.kind}`, callback); return this; }
}

const bundled = await build({
    absWorkingDir: root,
    entryPoints: ["src/settings.ts"],
    bundle: true,
    platform: "node",
    format: "esm",
    target: "node20",
    write: false,
    logLevel: "silent",
    plugins: [{
        name: "settings-ui-fixture",
        setup(builder) {
            builder.onResolve({ filter: /^obsidian$/ }, () => ({ path: "obsidian", namespace: "fixture" }));
            builder.onResolve({ filter: /^\.\/debug-modal$/ }, () => ({ path: "debug-modal", namespace: "fixture" }));
            builder.onLoad({ filter: /^obsidian$/, namespace: "fixture" }, () => ({ contents: `
                export class App {}
                export class PluginSettingTab {
                    constructor(app, plugin) { this.app = app; this.plugin = plugin; this.containerEl = app.containerEl; }
                }
                export class Setting {
                    constructor(container) { this.container = container; this.name = ""; }
                    setName(name) { this.name = name; return this; }
                    setDesc() { return this; }
                    setHeading() { return this; }
                    addText(callback) { callback(new globalThis.__settingsFence.Control(this, "text")); return this; }
                    addTextArea(callback) { callback(new globalThis.__settingsFence.Control(this, "textarea")); return this; }
                    addToggle(callback) { callback(new globalThis.__settingsFence.Control(this, "toggle")); return this; }
                    addDropdown(callback) { callback(new globalThis.__settingsFence.Control(this, "dropdown")); return this; }
                    addButton(callback) { callback(new globalThis.__settingsFence.Control(this, "button")); return this; }
                }
                export class Notice { constructor(message) { globalThis.__settingsFence.notices.push(String(message)); } }
            `, loader: "js" }));
            builder.onLoad({ filter: /^debug-modal$/, namespace: "fixture" }, () => ({
                contents: "export class ObsetyncDebugModal { open() {} }", loader: "js",
            }));
        },
    }],
});

globalThis.__settingsFence = { Control, notices: [] };
globalThis.window ??= { setTimeout() {} };
const source = Buffer.from(bundled.outputFiles[0].contents).toString("base64");
const { ObsetyncSettingTab, DEFAULT_SETTINGS } = await import(`data:text/javascript;base64,${source}`);

function fixture(enrolled = true) {
    controls.clear();
    let exposeEngine = false;
    let blocked = true;
    const effects = { authorized: 0, saves: 0, stops: 0, enrolls: 0, syncs: 0,
        scans: 0, reconciles: 0, histories: 0, rollbacks: 0, forces: 0 };
    const engine = {
        stop() { effects.stops++; },
        async reconcileContent() { effects.reconciles++; return { smallUploaded: 0, largeUploaded: 0, treeChunksUploaded: 0 }; },
        async forceSync() { effects.forces++; },
        getLastError: () => null, isReenrollmentRequired: () => false, hasPendingRootWork: () => false,
        isBulkChangeReviewRequired: () => false, isPushBlocked: () => false, getLastRepairSummary: () => null,
        getPendingChangeCount: () => 0, getDeferredChangeSummary: () => ({ count: 0 }), isBusy: () => false,
        getTreeRootHash: () => "root", getTreeBaseRoot: () => "root", getLastObservedServerRoot: () => "root",
        getState: () => "idle", getLastSyncTimestamp: () => 0, getSyncBaseCount: () => 0, getVaultFileCount: () => 0,
    };
    const plugin = {
        settings: { ...structuredClone(DEFAULT_SETTINGS), enrolled, serverUrl: "http://fixture", vaultId: "vault" },
        authorizeSettingsMutation() {
            effects.authorized++;
            if (blocked) throw new Error("handoff blocked");
        },
        async saveSettings() { effects.saves++; },
        async enroll() { effects.enrolls++; },
        async syncNow() { effects.syncs++; },
        async fullScan() { effects.scans++; },
        async getRootHistory() { effects.histories++; return [{ created_ms: 1, current: false, total_files: 1,
            device_id: "device-123456789", root: "a".repeat(64) }]; },
        async rollbackRoot() { effects.rollbacks++; effects.forces++; },
        syncEngineOrNull: () => exposeEngine ? engine : null,
        async getDebugInfo() { return ""; },
        async showBrowserCapabilityProbe() {},
    };
    const tab = new ObsetyncSettingTab({ containerEl: new Element() }, plugin);
    tab.display();
    exposeEngine = true;
    return { plugin, tab, effects, setBlocked(value) { blocked = value; } };
}

async function invoke(key, value) {
    const callback = controls.get(key);
    assert.equal(typeof callback, "function", `missing actual settings callback ${key}`);
    try { await callback(value); } catch (error) { assert.match(String(error), /handoff blocked/); }
}

test("blocked handoff fences every settings mutation before memory or engine side effects", async () => {
    const { plugin, effects } = fixture(true);
    const before = structuredClone(plugin.settings);
    for (const [key, value] of [
        ["Server URL:text", "http://changed"], ["Vault ID:text", "changed"], ["Device Name:text", "changed"],
        ["Sync Interval:text", "9"], ["Auto-Sync:toggle", false], ["Sync Obsidian config (.obsidian/):toggle", true],
        ["Ignore patterns:textarea", "changed/**"], ["Realtime sync:toggle", false], ["Share presence:toggle", false],
        ["Sync Priority:dropdown", "newest"], ["Reset enrollment:button", undefined],
    ]) await invoke(key, value);
    assert.deepEqual(plugin.settings, before);
    assert.equal(effects.saves, 0); assert.equal(effects.stops, 0);
    assert.equal(effects.authorized, 11);
});

test("blocked handoff fences enrollment and direct sync/server mutation actions", async () => {
    const enrolled = fixture(false);
    await invoke("Enrollment Code:text", "CODE");
    await invoke("Enrollment Code:button");
    assert.equal(enrolled.effects.enrolls, 0);

    const active = fixture(true);
    await invoke("Sync Now:button");
    await invoke("Show debug info:button");
    await invoke("Full Rescan:button");
    await invoke("Reconcile with server:button");
    await invoke("Server root history:button");
    assert.equal(active.effects.histories, 0, "history cannot open a current transport after handoff");
    assert.equal(active.effects.syncs, 0); assert.equal(active.effects.scans, 0);
    assert.equal(active.effects.reconciles, 0); assert.equal(active.effects.rollbacks, 0);
    assert.equal(active.effects.forces, 0);

    const rollback = fixture(true);
    rollback.setBlocked(false);
    await invoke("Server root history:button");
    assert.equal(rollback.effects.histories, 1);
    const rollbackButton = [...controls.keys()].find(key => key.startsWith("1970-") && key.endsWith(":button"));
    assert(rollbackButton, "actual history renderer did not create its rollback callback");
    await invoke(rollbackButton);
    rollback.setBlocked(true);
    await invoke(rollbackButton);
    assert.equal(rollback.effects.rollbacks, 0);
    assert.equal(rollback.effects.forces, 0);
});
