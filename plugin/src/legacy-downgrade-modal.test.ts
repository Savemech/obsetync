import { strict as assert } from "node:assert";
import {
    LEGACY_DOWNGRADE_TARGET,
    LegacyDowngradeConfirmationModal,
    isExplicitLegacyDowngradeConfirmation,
    legacyDowngradeConfirmation,
    legacyDowngradeStatus,
    type LegacyDowngradeConfirmationResult,
} from "./legacy-downgrade-modal";
import type { LegacyDowngradeHostState } from "./legacy-downgrade-host";

const complete = (action: "prepare" | "resume" | "import"): LegacyDowngradeConfirmationResult => ({
    action, targetVersion: "1.11.3", typedTarget: "1.11.3", understandsSyncStops: true,
    understandsArchiveIsRetained: true, explicitlyConfirmed: true,
});

async function run(): Promise<void> {
    for (const action of ["prepare", "resume", "import"] as const) {
        const view = legacyDowngradeConfirmation(action);
        assert.equal(view.targetVersion, LEGACY_DOWNGRADE_TARGET);
        assert.equal(view.requiredTypedTarget, "1.11.3");
        assert.match(view.warning, /archived and retained/i);
        assert.match(view.warning, /never resets or deletes/i);
        assert.match(view.warning, /Sync remains stopped/i);
        assert.equal(isExplicitLegacyDowngradeConfirmation(view, complete(action)), true);
        for (const mutation of [
            { typedTarget: "1.11.4" }, { targetVersion: "latest" },
            { understandsSyncStops: false }, { understandsArchiveIsRetained: false },
            { explicitlyConfirmed: false }, { action: action === "prepare" ? "resume" : "prepare" },
        ]) assert.equal(isExplicitLegacyDowngradeConfirmation(view, { ...complete(action), ...mutation } as any), false);
    }

    let observed = "";
    const modal = new LegacyDowngradeConfirmationModal({ async confirm(view) {
        observed = view.confirmLabel; return complete(view.action);
    } });
    assert.equal(await modal.request("resume"), true);
    assert.equal(observed, "Resume 1.11.3");
    assert.equal(await new LegacyDowngradeConfirmationModal({ async confirm() { return null; } }).request("import"), false);

    const states: LegacyDowngradeHostState[] = ["none", "interrupted", "active-awaiting-import", "quiescing",
        "activating", "legacy-handoff", "import-authorized", "current-active", "retired"];
    for (const state of states) {
        const status = legacyDowngradeStatus({ state, generation: 1, operation: null,
            stopped: state !== "none" && state !== "current-active", unloaded: state === "retired" });
        assert(status.text.length > 0 && status.text.length <= 180, `${state} status is unbounded`);
        assert(!status.text.includes("/Users/") && !status.text.includes(".obsidian"), `${state} status leaked a path`);
        if (state === "active-awaiting-import" || state === "legacy-handoff") assert.match(status.text, /stopped/i);
    }
    console.log("legacy-downgrade-modal.test: exact-target/confirmation/status assertions passed");
}
void run().catch(error => { setTimeout(() => { throw error; }, 0); });
