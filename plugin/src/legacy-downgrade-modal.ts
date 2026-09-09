import type { LegacyDowngradeHostSnapshot } from "./legacy-downgrade-host";

export const LEGACY_DOWNGRADE_TARGET = "1.11.3" as const;
export type LegacyDowngradeConfirmationAction = "prepare" | "resume" | "import";

export interface LegacyDowngradeConfirmationView {
    readonly action: LegacyDowngradeConfirmationAction;
    readonly title: string;
    readonly targetVersion: typeof LEGACY_DOWNGRADE_TARGET;
    readonly warning: string;
    readonly confirmLabel: string;
    readonly requiredTypedTarget: typeof LEGACY_DOWNGRADE_TARGET;
}

export interface LegacyDowngradeConfirmationResult {
    action: LegacyDowngradeConfirmationAction;
    targetVersion: string;
    typedTarget: string;
    understandsSyncStops: boolean;
    understandsArchiveIsRetained: boolean;
    explicitlyConfirmed: boolean;
}

export interface LegacyDowngradeModalPort {
    confirm(view: LegacyDowngradeConfirmationView): Promise<LegacyDowngradeConfirmationResult | null>;
}

const ARCHIVE_WARNING =
    "Authoritative current-format data is archived and retained for recovery. This action never resets or deletes it.";

export function legacyDowngradeConfirmation(action: LegacyDowngradeConfirmationAction): LegacyDowngradeConfirmationView {
    const verb = action === "prepare" ? "Prepare" : action === "resume" ? "Resume" : "Import";
    return Object.freeze({
        action,
        title: `${verb} compatibility handoff to ObsetyNC ${LEGACY_DOWNGRADE_TARGET}`,
        targetVersion: LEGACY_DOWNGRADE_TARGET,
        warning: `${ARCHIVE_WARNING} Sync remains stopped until the explicit compatibility workflow completes.`,
        confirmLabel: `${verb} ${LEGACY_DOWNGRADE_TARGET}`,
        requiredTypedTarget: LEGACY_DOWNGRADE_TARGET,
    });
}

export function isExplicitLegacyDowngradeConfirmation(view: LegacyDowngradeConfirmationView,
    result: LegacyDowngradeConfirmationResult | null): boolean {
    return result !== null && result.action === view.action &&
        result.targetVersion === LEGACY_DOWNGRADE_TARGET && result.typedTarget === LEGACY_DOWNGRADE_TARGET &&
        result.understandsSyncStops === true && result.understandsArchiveIsRetained === true &&
        result.explicitlyConfirmed === true;
}

export class LegacyDowngradeConfirmationModal {
    constructor(private readonly port: LegacyDowngradeModalPort) {}

    async request(action: LegacyDowngradeConfirmationAction): Promise<boolean> {
        const view = legacyDowngradeConfirmation(action);
        return isExplicitLegacyDowngradeConfirmation(view, await this.port.confirm(view));
    }
}

export interface LegacyDowngradeStatusView {
    readonly tone: "idle" | "warning" | "busy" | "stopped";
    readonly text: string;
}

/** Path-free bounded status copy for a later Obsidian modal/status-bar wire. */
export function legacyDowngradeStatus(snapshot: LegacyDowngradeHostSnapshot): LegacyDowngradeStatusView {
    switch (snapshot.state) {
        case "none": return { tone: "idle", text: "Compatibility handoff is not active." };
        case "interrupted": return { tone: "warning", text: `Compatibility handoff to ${LEGACY_DOWNGRADE_TARGET} was interrupted; explicit Resume is required.` };
        case "active-awaiting-import": return { tone: "stopped", text: `Sync is stopped for ${LEGACY_DOWNGRADE_TARGET}; current-format import still requires explicit confirmation.` };
        case "quiescing": return { tone: "busy", text: "Stopping sync and joining active work…" };
        case "activating": return { tone: "busy", text: `Preparing the retained ${LEGACY_DOWNGRADE_TARGET} compatibility projection…` };
        case "legacy-handoff": return { tone: "stopped", text: `Compatibility projection is ready. Sync stays stopped for handoff to ${LEGACY_DOWNGRADE_TARGET}.` };
        case "import-authorized": return { tone: "busy", text: "Importing retained compatibility work under explicit authorization…" };
        case "current-active": return { tone: "idle", text: "Compatibility import is complete; current sync may run." };
        case "retired": return { tone: "stopped", text: "This compatibility coordinator is retired." };
    }
}
