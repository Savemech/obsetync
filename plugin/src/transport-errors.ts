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
