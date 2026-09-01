/** A durable enrollment/transport mismatch cannot recover by retrying. */
export class ObsetyncReenrollmentRequiredError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "ObsetyncReenrollmentRequiredError";
    }
}

/**
 * Keep the classifier tolerant of errors crossing bundle/realm boundaries and
 * of older helpers that still surface the same terminal condition as a plain
 * Error. Transient network and server failures must not match this.
 */
export function isReenrollmentRequiredError(error: unknown): boolean {
    if (error instanceof ObsetyncReenrollmentRequiredError) return true;
    const message = error instanceof Error ? error.message : String(error ?? "");
    return /re-?enroll(?:ment)?(?: (?:this|the))? device/i.test(message);
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

export function isUpgradeRequiredError(error: unknown): boolean {
    if (error instanceof ObsetyncUpgradeRequiredError) return true;
    const message = error instanceof Error ? error.message : String(error ?? "");
    return /upgrade required|update\/reload the Obsetync plugin/i.test(message);
}

export function upgradeRequired(message: string): ObsetyncUpgradeRequiredError {
    return new ObsetyncUpgradeRequiredError(message);
}
