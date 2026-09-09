import type { ObsetyncJournal } from "./journal";
import type { ObsetyncSyncBase } from "./sync-base";
import type { RootIntentStore } from "./root-intent";
import type { RootConflictPreserver } from "./root-conflicts";

export type RootSettlement = { status: "idle" | "unresolved" | "cancelled" | "conflicts-pending" } |
    { status: "accepted"; candidateRoot: string; observedRoot: string };

/** Local terminal bookkeeping, invoked by the engine-owned root runtime.
 * No request retry, queue reconstruction, source reads or tree mutation occurs
 * here. Conflict-bearing receipts require recorded deterministic preservation;
 * without a preservation provider, unresolved copies remain pending.
 *
 * The owner must join settle() before closing/replacing any persistence handle.
 * Concurrent callers share one actual operation, with no unbounded task queue.
 * A failure after any local write is recovered by validated reload + replay. */
export class RootSettlementCoordinator {
    private active: Promise<RootSettlement> | null = null;
    constructor(private readonly intents: RootIntentStore, private readonly base: ObsetyncSyncBase,
        private readonly journal: ObsetyncJournal,
        private readonly conflicts?: Pick<RootConflictPreserver, "preserve">) {}

    settle(): Promise<RootSettlement> {
        if (this.active) return this.active;
        const operation = this.settleInside().finally(() => { if (this.active === operation) this.active = null; });
        this.active = operation;
        return operation;
    }
    private async settleInside(): Promise<RootSettlement> {
        const pending = this.intents.pending();
        if (!pending) return { status: "idle" };
        const { intent, terminal } = pending;
        if (!terminal) return { status: "unresolved" };
        if (terminal.status === "cancelled") {
            // Cancellation consumes only the server stream identity. It must
            // not wait for or mutate a journal that may need separate recovery.
            await this.intents.retire(intent.publication.identity);
            return { status: "cancelled" };
        }
        if (this.journal.validatedEpoch !== intent.journalEpoch) {
            throw new Error("Root settlement requires its original validated journal epoch");
        }
        const expectedCopies = "merged" in terminal.result ? terminal.result.conflicts : [];
        const hasCopies = (copies: typeof pending.conflictCopies) => expectedCopies.every(conflict =>
            copies.some(copy => copy.path === conflict.path && copy.hash === conflict.side_b_hash));
        if (!hasCopies(pending.conflictCopies) && !this.conflicts) {
            return { status: "conflicts-pending" };
        }
        await this.base.commitRootPublication(intent.publication);
        if (!hasCopies(pending.conflictCopies)) await this.conflicts!.preserve();
        const current = this.intents.pending();
        if (!current || current.intent.request.sequence !== intent.request.sequence ||
            current.intent.request.mutation_id !== intent.request.mutation_id ||
            current.intent.publication.identity.scopeHash !== intent.publication.identity.scopeHash ||
            current.intent.request.request_hash !== intent.request.request_hash || !hasCopies(current.conflictCopies)) {
            throw new Error("Root settlement lacks its exact recorded conflict preservation");
        }
        await this.journal.acknowledgeOwned(intent.journalEpoch, intent.journalCuts);
        await this.intents.retire(intent.publication.identity);
        return { status: "accepted", candidateRoot: intent.publication.candidateRoot,
            observedRoot: terminal.result.root_hash };
    }
}
