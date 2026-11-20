import type { LegacyDowngradeHostState, LegacyDowngradeOperation } from "./legacy-downgrade-host";

export const LEGACY_DOWNGRADE_TARGET = "1.11.3" as const;

export interface LegacyDowngradeConfirmationView {
    readonly action: LegacyDowngradeOperation;
    readonly title: string;
    readonly targetVersion: typeof LEGACY_DOWNGRADE_TARGET;
    readonly warning: string;
    readonly confirmLabel: string;
    readonly requiredTypedTarget: typeof LEGACY_DOWNGRADE_TARGET;
}

export interface LegacyDowngradeConfirmationResult {
    action: LegacyDowngradeOperation;
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

export function legacyDowngradeConfirmation(action: LegacyDowngradeOperation): LegacyDowngradeConfirmationView {
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

    async request(action: LegacyDowngradeOperation): Promise<boolean> {
        const view = legacyDowngradeConfirmation(action);
        return isExplicitLegacyDowngradeConfirmation(view, await this.port.confirm(view));
    }
}

export function legacyDowngradeStatus(state: LegacyDowngradeHostState): string {
    switch (state) {
        case "none": return "Compatibility handoff is not active.";
        case "interrupted": return `Compatibility handoff to ${LEGACY_DOWNGRADE_TARGET} was interrupted; explicit Resume is required.`;
        case "active-awaiting-import": return `Sync is stopped for ${LEGACY_DOWNGRADE_TARGET}; current-format import still requires explicit confirmation.`;
        case "quiescing": return "Stopping sync and joining active work…";
        case "activating": return `Preparing the retained ${LEGACY_DOWNGRADE_TARGET} compatibility projection…`;
        case "legacy-handoff": return `Compatibility projection is ready. Sync stays stopped for handoff to ${LEGACY_DOWNGRADE_TARGET}.`;
        case "import-authorized": return "Importing retained compatibility work under explicit authorization…";
        case "current-active": return "Compatibility import is complete; current sync may run.";
        case "retired": return "This compatibility coordinator is retired.";
    }
}
