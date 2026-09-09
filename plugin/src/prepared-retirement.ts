export interface PreparedRetirement {
    scopeHash: string;
    path: string;
    expectedMutationId: number;
}

/** Call only AFTER durable journal ACK. A published destination may still be
 * retained with an incomplete rename peer; merely calling acknowledge([])
 * does not make that preparation complete. Keep the candidate map small and
 * walk the existing settlement once, without another vault-sized path index. */
export function retirablePreparedHints(
    candidates: readonly PreparedRetirement[],
    journalCuts: ReadonlyMap<string, number>,
    settlement: { acknowledged: readonly { path: string; throughId: number }[];
        retained: readonly { path: string }[] },
): PreparedRetirement[] {
    const pending = new Map(candidates.map(value => [value.path, value]));
    const complete = new Set<string>();
    for (const path of pending.keys()) if (!journalCuts.has(path)) complete.add(path);
    for (const acknowledgement of settlement.acknowledged) {
        const cut = journalCuts.get(acknowledgement.path);
        if (pending.has(acknowledgement.path) && cut !== undefined && acknowledgement.throughId >= cut) {
            complete.add(acknowledgement.path);
        }
    }
    for (const retained of settlement.retained) complete.delete(retained.path);
    return [...complete].map(path => ({ ...pending.get(path)! }));
}
