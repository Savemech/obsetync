export interface RootBatchDrainOptions {
    /** Include the actual operation and its cleanup/accepted tail in this
     * promise. Return "continue" only after progress permits another batch. */
    attempt: () => Promise<"continue" | void>;
    /** Caller-owned eligibility/stop policy; never consulted before attempt 1. */
    shouldContinue: () => boolean;
    /** A real host cooperation point, not a resolved-promise busy loop. */
    cooperate: () => Promise<void>;
}

/** One serial, iteratively owned tail. The caller owns initial admission,
 * cross-invocation exclusion, cancellation policy and tracking this complete
 * promise through actual settlement. No timer, retry or virtual cancellation
 * is introduced here. Errors propagate unchanged; no-progress ends the drain.
 *
 * Callback references are captured before the first await. Eligibility is
 * checked on both sides of host cooperation so a stop/visibility change while
 * yielding cannot admit the next batch. An already started attempt is always
 * awaited, including an accepted publication's durable bookkeeping tail. */
export async function runRootBatchDrain({ attempt, shouldContinue, cooperate }: RootBatchDrainOptions): Promise<void> {
    while (await attempt() === "continue") {
        if (!shouldContinue()) return;
        await cooperate();
        if (!shouldContinue()) return;
    }
}
