/** A durable enrollment/transport mismatch cannot recover by retrying. */
export class ObsetyncReenrollmentRequiredError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "ObsetyncReenrollmentRequiredError";
    }
}

export function reenrollmentRequired(message: string): ObsetyncReenrollmentRequiredError {
    return new ObsetyncReenrollmentRequiredError(message);
}

/** The enrolled device is valid, but its running plugin lacks the active
 *  vault data format. This must never be presented as an enrollment failure. */
export class ObsetyncUpgradeRequiredError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "ObsetyncUpgradeRequiredError";
    }
}

/** A typed server refusal takes precedence over advice in its message.
 * Bundles and realms may have separate copies of the error constructors. */
function transportRequirement(error: unknown): "upgrade" | "reenroll" | null {
    if (error instanceof ObsetyncUpgradeRequiredError) return "upgrade";
    if (error instanceof ObsetyncReenrollmentRequiredError) return "reenroll";
    let message: unknown = error;
    if (typeof error === "object" && error !== null) {
        const name = "name" in error ? error.name : undefined;
        if (name === "ObsetyncUpgradeRequiredError") return "upgrade";
        if (name === "ObsetyncReenrollmentRequiredError") return "reenroll";
        message = "message" in error ? error.message : undefined;
    }
    if (typeof message !== "string") return null;
    if (/upgrade required|update\/reload the Obsetync plugin/i.test(message)) return "upgrade";
    if (/re-?enroll(?:ment)?(?: (?:this|the))? device/i.test(message)) return "reenroll";
    return null;
}

export function isReenrollmentRequiredError(error: unknown): boolean {
    return transportRequirement(error) === "reenroll";
}

export function isUpgradeRequiredError(error: unknown): boolean {
    return transportRequirement(error) === "upgrade";
}

export function upgradeRequired(message: string): ObsetyncUpgradeRequiredError {
    return new ObsetyncUpgradeRequiredError(message);
}
