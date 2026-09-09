import type { ObsetyncApi } from "./api";
import type { RootIntentStore, PendingRootIntent, StoredRootIntent } from "./root-intent";
import type { RootSettlement, RootSettlementCoordinator } from "./root-settlement";
import { decodeRootOutcome, encodeRootCancel, encodeRootOutcomeQuery, rootCommitIntentEncodedLength,
    validateRootScopeId, validateRootTerminalOutcome,
    type RootOutcomeCapabilities, type RootOutcomeIdentity, type RootTerminalOutcome } from "./root-outcome";

export type RootRecoveryApi = Pick<ObsetyncApi, "negotiateRootOutcomes" | "queryRootOutcome" |
    "cancelRootOutcome" | "commitRootOutcome">;
export interface RootRecoveryScope {
    readonly scopeHash: string;
    readonly vaultId: string;
    readonly deviceId: string;
}
export interface RootRecoveryOptions extends RootRecoveryScope {
    /** Bind this immutable API credential owner to the expected scope. The API
     * does not expose an authenticated device getter. The engine owner
     * must supply this check; an outcome is never evidence of another device's
     * authorization. Called before admission to each new network request. */
    assertApiScope: (expected: Readonly<RootRecoveryScope>) => void;
}
export type RootRecoveryResult = RootSettlement | {
    status: "deferred";
    reason: "unsupported" | "expired" | "stream-diverged" | "limits" | "closing";
};
export class RootRecoveryError extends Error {
    constructor(readonly code: "ROOT_RECOVERY_CLOSED" | "ROOT_RECOVERY_BUSY" | "ROOT_RECOVERY_SCOPE" |
        "ROOT_RECOVERY_OWNER", message: string) {
        super(message); this.name = "RootRecoveryError";
    }
}
function identity(intent: StoredRootIntent): RootOutcomeIdentity {
    return { sequence: intent.request.sequence, mutation_id: intent.request.mutation_id,
        request_hash: intent.request.request_hash };
}
const deferred = (reason: Extract<RootRecoveryResult, { status: "deferred" }>["reason"]): RootRecoveryResult =>
    ({ status: "deferred", reason });

/** One exclusive stream owner over already loaded stores. These methods do not
 * load/reset storage, discover a new sequence, rebuild a candidate, or activate
 * the production engine. The caller must not concurrently prepare/retire via
 * another owner; closeAndDrain must finish before replacement closes stores.
 *
 * Recovery deliberately resolves unknown outcomes by a conditional terminal
 * cancellation. This can return an already accepted winner, never a rollback.
 * Cancelled edits stay in the journal for subsequent ordinary reconciliation.
 * Conflicted acceptance remains owned by the settlement coordinator.
 *
 * One active owner, no queue/timers/caller-promise races. Closing stops new work
 * and subsequent network dispatches in admitted work. A received receipt and
 * its actual durable bookkeeping tail cannot be abandoned by user stop.
 * This is adapter completion, not native fsync/RSS
 * or a proof that historical accepted objects still exist on the server. */
export class RootRecoveryCoordinator {
    private readonly scope: Readonly<RootRecoveryScope>;
    private readonly assertApiScope: RootRecoveryOptions["assertApiScope"];
    private active: Promise<RootRecoveryResult> | null = null;
    private closed = false;
    private drain: Promise<void> | null = null;
    private resolveDrain: (() => void) | null = null;

    constructor(private readonly intents: RootIntentStore,
        private readonly settlement: Pick<RootSettlementCoordinator, "settle">,
        private readonly api: RootRecoveryApi, options: RootRecoveryOptions) {
        const { scopeHash, vaultId, deviceId, assertApiScope } = options;
        if (!/^[0-9a-f]{64}$/.test(scopeHash) || scopeHash !== intents.scopeHash ||
            typeof assertApiScope !== "function") this.scopeError();
        validateRootScopeId(vaultId); validateRootScopeId(deviceId);
        this.scope = Object.freeze({ scopeHash, vaultId, deviceId });
        this.assertApiScope = assertApiScope;
    }

    /** Own prepare rather than accepting an externally reusable "prepared"
     * token: after a crash/lost response an existing intent is NOT proof that it
     * was never sent. Only this call's new verified prepare permits initial send.
     * RootIntentStore captures the bounded submitted payload synchronously.
     * The invocation-local guard rechecks reviewed source/policy ownership at
     * the LAST initial-send boundary, after prepare and negotiation. It never
     * guards resolution of an existing intent or a received terminal tail. A
     * guard rejection retains the durable intent for ordinary recovery. */
    prepareAndSend(raw: StoredRootIntent, assertFreshSend?: () => void): Promise<RootRecoveryResult> {
        return this.admit(async () => {
            this.assertScope(raw);
            this.checkApiScope();
            // Capture the checked owner primitives. The store's synchronous
            // validator owns all remaining nested arrays/bytes before our await.
            const captured = { ...raw, vaultId: this.scope.vaultId, deviceId: this.scope.deviceId,
                publication: { ...raw.publication, identity: { ...raw.publication.identity,
                    scopeHash: this.scope.scopeHash } } };
            const prepared = await this.intents.prepare(captured);
            const pending = this.requirePending();
            if (pending.terminal) return this.settlePending(pending);
            const capabilities = await this.negotiate();
            if (capabilities === "closing" || this.closed) return deferred("closing");
            if (!capabilities) return deferred("unsupported");
            if (prepared === "already-prepared" ||
                pending.intent.request.server_incarnation !== capabilities.serverIncarnation) {
                return this.resolveNetwork(pending, capabilities);
            }
            return this.sendPrepared(pending, capabilities, assertFreshSend);
        });
    }

    resolvePending(): Promise<RootRecoveryResult> {
        return this.admit(async () => {
            const pending = this.intents.pending();
            if (!pending) return { status: "idle" };
            this.assertScope(pending.intent);
            if (pending.terminal) return this.settlePending(pending);
            const capabilities = await this.negotiate();
            if (capabilities === "closing" || this.closed) return deferred("closing");
            if (!capabilities) return deferred("unsupported");
            return this.resolveNetwork(pending, capabilities);
        });
    }

    /** Stable actual-work drain, including rejected native requests/writes and
     * the accepted terminal ACK tail. A hung native owner stays a blocker. */
    closeAndDrain(): Promise<void> {
        if (this.drain) return this.drain;
        this.closed = true;
        this.drain = new Promise<void>(resolve => { this.resolveDrain = resolve; });
        if (!this.active) this.finishDrain();
        return this.drain;
    }
    snapshot(): { closed: boolean; active: number; drained: boolean } {
        return { closed: this.closed, active: this.active ? 1 : 0, drained: this.closed && !this.active };
    }

    private admit(work: () => Promise<RootRecoveryResult>): Promise<RootRecoveryResult> {
        if (this.closed) return Promise.reject(new RootRecoveryError("ROOT_RECOVERY_CLOSED", "Root recovery is closed"));
        if (this.active) return Promise.reject(new RootRecoveryError("ROOT_RECOVERY_BUSY", "Root recovery is busy"));
        let resolve!: (value: RootRecoveryResult) => void, reject!: (reason: unknown) => void;
        const operation = new Promise<RootRecoveryResult>((yes, no) => { resolve = yes; reject = no; });
        // Register before invoking even synchronous callbacks or input getters.
        this.active = operation;
        const finish = () => { this.active = null; if (this.closed) this.finishDrain(); };
        try {
            work().then(value => { finish(); resolve(value); }, error => { finish(); reject(error); });
        } catch (error) { finish(); reject(error); }
        return operation;
    }
    private finishDrain(): void { this.resolveDrain?.(); this.resolveDrain = null; }
    private scopeError(): never {
        throw new RootRecoveryError("ROOT_RECOVERY_SCOPE", "Root recovery scope differs from its owner");
    }
    private assertScope(intent: StoredRootIntent): void {
        if (!intent || intent.vaultId !== this.scope.vaultId || intent.deviceId !== this.scope.deviceId ||
            intent.publication?.identity?.scopeHash !== this.scope.scopeHash) this.scopeError();
    }
    private checkApiScope(): void { this.assertApiScope(this.scope); }
    private canDispatchNetwork(): boolean {
        if (this.closed) return false;
        this.checkApiScope();
        // A scope/lifecycle hook may close synchronously. There is no await
        // between this final check and the native API method's admission.
        return !this.closed;
    }
    private requirePending(expected?: StoredRootIntent): PendingRootIntent {
        const current = this.intents.pending();
        if (!current) throw new RootRecoveryError("ROOT_RECOVERY_OWNER", "Root recovery lost its pending owner");
        this.assertScope(current.intent);
        if (expected && (current.intent.request.sequence !== expected.request.sequence ||
            current.intent.request.mutation_id !== expected.request.mutation_id ||
            current.intent.request.request_hash !== expected.request.request_hash)) {
            throw new RootRecoveryError("ROOT_RECOVERY_OWNER", "Root recovery pending identity changed");
        }
        return current;
    }
    private negotiate(): Promise<RootOutcomeCapabilities | null | "closing"> {
        if (!this.canDispatchNetwork()) return Promise.resolve("closing");
        // ObsetyncApi returns null unless BOTH outcome and cancel were offered.
        // A force observation does not replace the original intent incarnation.
        return this.api.negotiateRootOutcomes(true);
    }
    private async sendPrepared(pending: PendingRootIntent, caps: RootOutcomeCapabilities,
        assertFreshSend?: () => void): Promise<RootRecoveryResult> {
        if (this.closed) return deferred("closing");
        const { request } = pending.intent;
        const encodedBytes = rootCommitIntentEncodedLength(request);
        const padding = request.root.endsWith("==") ? 2 : request.root.endsWith("=") ? 1 : 0;
        if (request.sequence > caps.maxSequence || encodedBytes > caps.commitBytes ||
            request.root.length / 4 * 3 - padding > caps.rootBytes) return deferred("limits");
        this.requirePending(pending.intent);
        if (!this.canDispatchNetwork()) return deferred("closing");
        assertFreshSend?.();
        // A synchronous owner hook can initiate retirement too. No await or
        // further user hook may separate this check from initial API admission.
        if (this.closed) return deferred("closing");
        // API transport retries capture these exact bytes/identity; this layer
        // never retries a failed application send. The next call must query.
        const terminal = await this.api.commitRootOutcome(this.scope.vaultId, request);
        return this.recordAndSettle(pending, terminal);
    }
    private async resolveNetwork(pending: PendingRootIntent, caps: RootOutcomeCapabilities): Promise<RootRecoveryResult> {
        if (this.closed) return deferred("closing");
        const expected = identity(pending.intent);
        if (expected.sequence > caps.maxSequence || encodeRootOutcomeQuery(expected).byteLength > caps.queryBytes) {
            return deferred("limits");
        }
        this.requirePending(pending.intent);
        if (!this.canDispatchNetwork()) return deferred("closing");
        // Validate again at this boundary so alternate test/runtime ports cannot
        // bypass the identity contract enforced by ObsetyncApi's wire decoder.
        const outcome = decodeRootOutcome(await this.api.queryRootOutcome(this.scope.vaultId, expected), expected);
        if (outcome.status === "accepted" || outcome.status === "cancelled") {
            return this.recordAndSettle(pending, outcome);
        }
        if (this.closed) return deferred("closing");
        if (outcome.status === "expired") return deferred("expired");
        if (outcome.status !== "unknown" || outcome.last_sequence + 1 !== expected.sequence) {
            return deferred("stream-diverged");
        }
        // Query may observe a restart AFTER capability negotiation. Cancellation
        // names that current observation; only its incarnation changes. Another
        // restart may reject the request, leaving the original durable intent.
        const cancel = { protocol_version: 1 as const, server_incarnation: outcome.server_incarnation, ...expected };
        if (encodeRootCancel(cancel).byteLength > caps.cancelBytes) return deferred("limits");
        this.requirePending(pending.intent);
        if (!this.canDispatchNetwork()) return deferred("closing");
        const winner = await this.api.cancelRootOutcome(this.scope.vaultId, cancel);
        return this.recordAndSettle(pending, winner);
    }
    private async recordAndSettle(pending: PendingRootIntent, raw: RootTerminalOutcome): Promise<RootRecoveryResult> {
        const terminal = validateRootTerminalOutcome(raw, identity(pending.intent));
        this.requirePending(pending.intent);
        await this.intents.recordTerminal(terminal);
        // No closed/signal check here: actual terminal persistence MUST precede
        // every local base/ACK effect, and user stop cannot drop that tail.
        return this.settlePending(pending);
    }
    private settlePending(pending: PendingRootIntent): Promise<RootSettlement> {
        this.requirePending(pending.intent);
        return this.settlement.settle();
    }
}
