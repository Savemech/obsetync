import {
    NativeResidentLedger,
    type NativeResidentLedgerOptions,
    type NativeResidentLedgerSnapshot,
    type NativeResidentLease,
} from "./resident-memory";
import {
    takeV2TreeOutputSettlementProof,
    type V2TreeOutputSettlementOutcome,
    type V2TreeOutputSettlementProof,
} from "./tree-output-settlement";

/** Requested buffers for a V2 replacement, candidate-root clone or explicitly
 * admitted mutation output, NOT a complete native graph/heap/RSS quota. Unadmitted
 * push/pull allocations are outside this component. Attached mutation output
 * is absorbed into one aggregate tree lease and conservatively survives
 * candidate abort/commit until exact settlement, full replacement retirement
 * or proven free; a revision change is never release proof. */
export interface RootTreeResidentPlan {
    readonly peakBytes: number;
    readonly residentBytes: number;
}

const attemptBrand: unique symbol = Symbol("RootTreeResidentAttempt");
const retirementBrand: unique symbol = Symbol("RootTreeResidentRetirement");
const settlementBrand: unique symbol = Symbol("RootTreeResidentSettlement");
const ownerBrand = Symbol("RootTreeResidentAdmissionOwner");

export interface RootTreeResidentAttempt { readonly [attemptBrand]: true }
export interface RootTreeResidentRetirement { readonly [retirementBrand]: true }
export interface RootTreeResidentSettlement { readonly [settlementBrand]: true }

export class RootTreeResidentStateError extends Error {
    constructor() {
        super("invalid root tree resident admission ownership or state");
        this.name = "RootTreeResidentStateError";
    }
}

export interface RootTreeResidentAdmissionSnapshot {
    scope: "v2-tree-output-requested-buffers";
    ledger: NativeResidentLedgerSnapshot;
    residentTrees: number;
    privateAttempts: number;
    retiringOwners: number;
    privateBytes: number;
    residentBytes: number;
    retiringBytes: number;
}

interface AttemptState {
    tree: object;
    kind: "replacement" | "candidate-mutation" | "candidate-root";
    phase: "private" | "ready" | "published" | "detached" | "released";
    residentBytes: number;
    identityBytes: number;
    lease: NativeResidentLease | null;
    retirement: { state: RetirementState; token: RootTreeResidentRetirement };
}

interface RetirementState {
    tree: object;
    active: boolean;
    lease: NativeResidentLease | null;
    bytes: number;
}

interface SettlementState {
    tree: object;
    outcome: V2TreeOutputSettlementOutcome;
    active: boolean;
    admissionEpoch: number | null;
    exactGraphEligible: boolean;
    resident: NativeResidentLease | null;
    residentBytes: number;
    committedRevision: number;
    candidateRevision: number;
    candidateRoot: AttemptState | undefined;
    committedRoot: AttemptState | undefined;
}

interface TreeState {
    hasResident: boolean;
    resident: NativeResidentLease | null;
    residentBytes: number;
    attempt?: AttemptState;
    retirement?: RetirementState;
    settlement?: SettlementState;
    // Two bounded physical clone owners, not a history of root generations.
    // Their charges are already included in the aggregate resident lease.
    candidateRoot?: AttemptState;
    committedRoot?: AttemptState;
    completeV2?: boolean;
    completeCommittedRevision?: number;
    completeCandidateRevision?: number;
}

function treeOwner(tree: object): void {
    if (tree === null || (typeof tree !== "object" && typeof tree !== "function")) {
        throw new RootTreeResidentStateError();
    }
}

function detachedPlan(value: RootTreeResidentPlan): RootTreeResidentPlan {
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
        throw new TypeError("invalid root tree resident output plan");
    }
    const keys = Reflect.ownKeys(value);
    const peak = Object.getOwnPropertyDescriptor(value, "peakBytes");
    const resident = Object.getOwnPropertyDescriptor(value, "residentBytes");
    if (keys.length !== 2 || !peak || !("value" in peak) || !resident || !("value" in resident) ||
        !Number.isSafeInteger(peak.value) || peak.value < 0 ||
        !Number.isSafeInteger(resident.value) || resident.value < 0 || resident.value > peak.value) {
        throw new TypeError("invalid root tree resident output plan");
    }
    return { peakBytes: peak.value, residentBytes: resident.value };
}

function revision(value: number): number {
    if (!Number.isSafeInteger(value) || value < 0) {
        throw new RangeError("tree revision must be a non-negative safe integer");
    }
    return value;
}

/** One fail-fast, caller-proven output ownership ledger. All transitions are
 * synchronous; there are no callbacks, waiting queues, finalizers or native
 * calls. The caller alone supplies the Ready/retired/successfully-freed proof.
 * A zero-byte owner still requires every state transition. */
export class RootTreeResidentAdmission {
    private readonly ledger: NativeResidentLedger;
    // Strong keys deliberately quarantine a wrapper even if its caller drops
    // every other reference while native cleanup remains failed or ambiguous.
    private readonly trees = new Map<object, TreeState>();
    private readonly attempts = new WeakMap<object, AttemptState>();
    private readonly retirements = new WeakMap<object, RetirementState>();
    private readonly settlements = new WeakMap<object, SettlementState>();
    private readonly freed = new WeakSet<object>();
    private residentTrees = 0;
    private privateAttempts = 0;
    private retiringOwners = 0;
    private privateBytes = 0;
    private residentBytes = 0;
    private retiringBytes = 0;
    /** Monotonic, owner-local accounting witness. It changes before every
     * operation which may alter charged ownership/capacity, including a
     * refused reserve, so an equal-looking reserve/release ABA cannot validate
     * stale work. Revision provenance and no-op settlement do not perturb it. */
    private mutationEpochValue: number | null = 0;

    constructor(options: NativeResidentLedgerOptions) {
        this.ledger = new NativeResidentLedger(options);
    }

    get mutationEpoch(): number | null { return this.mutationEpochValue; }

    private changing(): void {
        // Allocation-free and non-throwing: post-native publish/attach are
        // move-only transitions. Exhaustion permanently disables exact witness
        // caching instead of wrapping or interrupting ownership cleanup.
        const current = this.mutationEpochValue;
        if (current !== null) this.mutationEpochValue = current === Number.MAX_SAFE_INTEGER
            ? null : current + 1;
    }

    /** Reserve BEFORE native output allocation. Refusal changes no ownership.
     * Even a zero-byte plan is refused after close. Old resident bytes remain
     * charged alongside the complete new peak; admission never waits on them. */
    reserve(tree: object, input: RootTreeResidentPlan): RootTreeResidentAttempt | null {
        return this.reserveOutput(tree, input, "replacement");
    }

    /** Reserve a disjoint candidate-mutation output cohort BEFORE its native
     * output starts. Existing resident/replacement cohorts remain charged.
     * The caller must not include already-covered shared resident buffers. */
    reserveCandidateMutation(tree: object, input: RootTreeResidentPlan): RootTreeResidentAttempt | null {
        return this.reserveOutput(tree, input, "candidate-mutation");
    }

    /** Exact requested root clone C = identity + endpoint bytes. This is
     * disjoint from existing resident owners; a scalar epoch baseline has no
     * allocation charge here. Caller must reserve before native clone resume. */
    reserveCandidateRoot(tree: object, totalBytes: number, identityBytes: number): RootTreeResidentAttempt | null {
        if (!Number.isSafeInteger(totalBytes) || totalBytes < 0 ||
            !Number.isSafeInteger(identityBytes) || identityBytes < 0 || identityBytes > totalBytes) {
            throw new RangeError("invalid candidate root requested bytes");
        }
        return this.reserveOutput(tree, { peakBytes: totalBytes, residentBytes: totalBytes },
            "candidate-root", identityBytes);
    }

    private reserveOutput(tree: object, input: RootTreeResidentPlan,
        kind: AttemptState["kind"], identityBytes = 0): RootTreeResidentAttempt | null {
        treeOwner(tree);
        const plan = detachedPlan(input);
        let record = this.trees.get(tree);
        if (this.freed.has(tree) || record?.attempt || record?.retirement || record?.settlement ||
            (kind === "candidate-root" && record?.candidateRoot)) {
            throw new RootTreeResidentStateError();
        }
        if (this.ledger.snapshot().closed) return null;
        // Preallocate the eventual retirement capability before admission/native
        // work, so publish/detach only move existing ownership references.
        const retirement = this.newRetirement(tree);
        const state: AttemptState = { tree, kind, phase: "private", residentBytes: plan.residentBytes, identityBytes,
            lease: null, retirement };
        const token = Object.freeze({ [attemptBrand]: true as const, [ownerBrand]: this });
        record ??= { hasResident: false, resident: null, residentBytes: 0,
            candidateRoot: undefined, committedRoot: undefined };
        this.attempts.set(token, state);
        this.changing();
        const lease = plan.peakBytes === 0 ? null : this.ledger.tryReserve(plan.peakBytes);
        if (plan.peakBytes !== 0 && lease === null) return null;
        try {
            this.trees.set(tree, record);
        } catch (error) {
            lease?.release();
            throw error;
        }
        state.lease = lease;
        record.attempt = state;
        this.privateAttempts++;
        this.privateBytes += plan.peakBytes;
        return token;
    }

    /** Caller proves build Ready AND the peak-only requested buffers have
     * actually been released. Merely knowing the eventual size is not proof. */
    ready(token: RootTreeResidentAttempt): void {
        const state = this.activeAttempt(token);
        if (state.phase !== "private" || state.kind !== "replacement") throw new RootTreeResidentStateError();
        this.changing();
        this.readyOutput(state, state.residentBytes);
    }

    /** Caller proves mutation Ready and supplies its native actual retained
     * requested output, which may be smaller than the plan after deduplication.
     * This never admits extra output or trusts the planned target as actual. */
    readyCandidateMutation(token: RootTreeResidentAttempt, actualResidentBytes: number): void {
        const state = this.activeAttempt(token);
        if (state.phase !== "private" || state.kind !== "candidate-mutation") {
            throw new RootTreeResidentStateError();
        }
        if (!Number.isSafeInteger(actualResidentBytes) || actualResidentBytes < 0 ||
            actualResidentBytes > state.residentBytes) {
            throw new RangeError("invalid candidate mutation resident bytes");
        }
        this.changing();
        this.readyOutput(state, actualResidentBytes);
    }

    /** The fallible native clone resume returned successfully and retained
     * exactly the admitted request. No shrink is authorized at this boundary. */
    readyCandidateRoot(token: RootTreeResidentAttempt): void {
        const state = this.activeAttempt(token);
        if (state.phase !== "private" || state.kind !== "candidate-root") throw new RootTreeResidentStateError();
        this.changing();
        state.phase = "ready";
    }

    private readyOutput(state: AttemptState, residentBytes: number): void {
        const before = state.lease?.bytes ?? 0;
        if (residentBytes === 0) {
            state.lease?.release();
            state.lease = null;
        } else if (!state.lease || !this.ledger.tryResize(state.lease, residentBytes)) {
            throw new RootTreeResidentStateError();
        }
        this.privateBytes -= before - residentBytes;
        state.residentBytes = residentBytes;
        state.phase = "ready";
    }

    /** Caller proves covered output allocation never began (for example, no
     * successful native resume). A generic build/cancellation error is NOT this
     * proof. Pre-Ready only; an existing resident is never released here. */
    releaseBeforeOutput(token: RootTreeResidentAttempt): void {
        const state = this.activeAttempt(token);
        const record = this.trees.get(state.tree)!;
        if (state.phase !== "private") throw new RootTreeResidentStateError();
        this.changing();
        const bytes = state.lease?.bytes ?? 0;
        state.lease?.release();
        state.lease = null;
        state.phase = "released";
        record.attempt = undefined;
        this.privateAttempts--;
        this.privateBytes -= bytes;
        this.attempts.delete(token);
        this.retirements.delete(state.retirement.token);
        if (!record.hasResident) this.trees.delete(state.tree);
    }

    /** Call immediately after successful native publication, before fallible
     * summary/scope checks. New output becomes resident even if those reject.
     * The returned prior-owner capability MUST be retired, including when it
     * holds zero bytes. No re-reservation, capability allocation, or shrink occurs. */
    publish(token: RootTreeResidentAttempt): RootTreeResidentRetirement {
        const state = this.activeAttempt(token);
        const record = this.trees.get(state.tree)!;
        if (state.phase !== "ready" || state.kind !== "replacement") throw new RootTreeResidentStateError();
        this.changing();
        const retirement = state.retirement;
        retirement.state.lease = record.resident;
        retirement.state.bytes = record.residentBytes;
        retirement.state.active = true;
        const previousBytes = record.residentBytes;
        const nextBytes = state.lease?.bytes ?? 0;
        if (!record.hasResident) this.residentTrees++;
        this.privateAttempts--;
        this.privateBytes -= nextBytes;
        this.residentBytes += nextBytes - previousBytes;
        this.retiringBytes += previousBytes;
        this.retiringOwners++;
        record.hasResident = true;
        record.completeV2 = false;
        record.completeCommittedRevision = undefined;
        record.completeCandidateRevision = undefined;
        record.resident = state.lease;
        record.residentBytes = nextBytes;
        // Native replacement moved the entire prior tree into retirement.
        // Its root clone charges travel inside the same aggregate lease.
        record.candidateRoot = undefined;
        record.committedRoot = undefined;
        record.attempt = undefined;
        record.retirement = retirement.state;
        state.lease = null;
        state.phase = "published";
        this.attempts.delete(token);
        return retirement.token;
    }

    /** Call immediately after successful native mutation finish, before any
     * fallible scope/summary callback. Adds the Ready cohort to the SAME shared
     * tree store without retiring/releasing prior cohorts. Even if the outer
     * candidate later aborts/commits, no per-candidate release is authorized.
     * This consumes the attempt without allocating a capability or array. */
    attachCandidateMutation(token: RootTreeResidentAttempt): void {
        const state = this.activeAttempt(token);
        if (state.phase !== "ready" || state.kind !== "candidate-mutation") {
            throw new RootTreeResidentStateError();
        }
        this.attachOutput(token, state);
    }

    /** Tree v1 mutation replaces the physical candidate-root clone while its
     * staged immutable chunks are additive. Attach both under the admitted
     * cohort, then retire the prior root slot and reuse this exact preallocated
     * attempt state as the new slot. If no prior root was tracked (for example
     * adoption of an already-open legacy wrapper), retain the whole cohort
     * additively: that is conservative and never invents proof. */
    attachV1CandidateMutation(token: RootTreeResidentAttempt, newRootBytes: number): void {
        const state = this.activeAttempt(token);
        const record = this.trees.get(state.tree)!;
        if (state.phase !== "ready" || state.kind !== "candidate-mutation" ||
            !Number.isSafeInteger(newRootBytes) || newRootBytes < 0 ||
            newRootBytes > state.residentBytes) {
            throw new RootTreeResidentStateError();
        }
        const oldRoot = record.candidateRoot;
        this.attachOutput(token, state);
        if (!oldRoot) return;
        this.shrinkResident(record, record.residentBytes - oldRoot.residentBytes);
        this.changing();
        oldRoot.phase = "released";
        state.residentBytes = newRootBytes;
        state.identityBytes = Math.min(oldRoot.identityBytes, newRootBytes);
        record.candidateRoot = state;
    }

    /** Immediately after native finish publishes the cloned candidate. The
     * preallocated slot survives mutation and is terminally consumed by the
     * ordinary settlement ticket, including a malformed/missing exact proof. */
    attachCandidateRoot(token: RootTreeResidentAttempt): void {
        const state = this.activeAttempt(token);
        const record = this.trees.get(state.tree)!;
        if (state.phase !== "ready" || state.kind !== "candidate-root" || record.candidateRoot) {
            throw new RootTreeResidentStateError();
        }
        this.attachOutput(token, state);
        record.candidateRoot = state;
    }

    private attachOutput(token: RootTreeResidentAttempt, state: AttemptState): void {
        const record = this.trees.get(state.tree)!;
        this.changing();
        const bytes = state.lease?.bytes ?? 0;
        if (state.lease && record.resident) this.ledger.absorb(record.resident, state.lease);
        else if (state.lease) record.resident = state.lease;
        if (!record.hasResident) this.residentTrees++;
        record.hasResident = true;
        record.residentBytes += bytes;
        record.attempt = undefined;
        this.privateAttempts--;
        this.privateBytes -= bytes;
        this.residentBytes += bytes;
        state.lease = null;
        state.phase = "published";
        this.attempts.delete(token);
        this.retirements.delete(state.retirement.token);
    }

    /** Detach the complete private/Ready allowance for native cancellation.
     * This does NOT release bytes, and does not disturb the old resident. */
    detachPrivate(token: RootTreeResidentAttempt): RootTreeResidentRetirement {
        const state = this.activeAttempt(token);
        const record = this.trees.get(state.tree)!;
        this.changing();
        const retirement = state.retirement;
        const bytes = state.lease?.bytes ?? 0;
        retirement.state.lease = state.lease;
        retirement.state.bytes = bytes;
        retirement.state.active = true;
        this.privateAttempts--;
        this.privateBytes -= bytes;
        this.retiringOwners++;
        this.retiringBytes += bytes;
        record.attempt = undefined;
        record.retirement = retirement.state;
        state.lease = null;
        state.phase = "detached";
        this.attempts.delete(token);
        return retirement.token;
    }

    /** Caller confirms actual native retirement completion. Never call merely
     * because cancellation was requested, its wait rejected, or scope changed. */
    releaseRetired(token: RootTreeResidentRetirement): void {
        const state = this.retirements.get(token);
        const record = state && this.trees.get(state.tree);
        if (!state?.active || !record || record.retirement !== state) throw new RootTreeResidentStateError();
        this.changing();
        const bytes = state.bytes;
        state.lease?.release();
        state.lease = null;
        state.bytes = 0;
        state.active = false;
        this.retirements.delete(token);
        record.retirement = undefined;
        this.retiringBytes -= bytes;
        this.retiringOwners--;
        if (!record.hasResident) this.trees.delete(state.tree);
    }

    /** Only AFTER the caller's native free returned successfully, with all
     * private/retiring jobs already drained. Throwing/ambiguous free leaves the
     * charge untouched by not calling this method. A freed wrapper never reopens. */
    releaseResidentAfterFree(tree: object): void {
        treeOwner(tree);
        const record = this.trees.get(tree);
        if (this.freed.has(tree) || record?.attempt || record?.retirement || record?.settlement) {
            throw new RootTreeResidentStateError();
        }
        this.changing();
        this.freed.add(tree);
        if (record?.hasResident) {
            const bytes = record.residentBytes;
            record.resident?.release();
            this.residentBytes -= bytes;
            this.residentTrees--;
        }
        this.trees.delete(tree);
    }

    /** Establish complete admitted V2 graph provenance only after the caller
     * has finished replacement retirement and revalidated the published
     * wrapper/root/base scope. Failure to mark merely disables settlement. */
    markV2GraphComplete(tree: object, committedRevision: number, candidateRevision: number): void {
        treeOwner(tree);
        const committed = revision(committedRevision), candidate = revision(candidateRevision);
        const record = this.trees.get(tree);
        if (!record?.hasResident || record.attempt || record.retirement || record.settlement ||
            (record.residentBytes > 0 && (!record.resident || record.resident.bytes !== record.residentBytes)) ||
            (record.residentBytes === 0 && record.resident !== null)) {
            throw new RootTreeResidentStateError();
        }
        record.completeV2 = true;
        record.completeCommittedRevision = committed;
        record.completeCandidateRevision = candidate;
    }

    /** Advance the proof only across an observed no-op or one exact candidate
     * revision. Direct/unadmitted mutation creates a larger mismatch and
     * permanently disables shrink until another admitted replacement. */
    advanceV2CandidateRevision(tree: object, committedRevision: number, candidateRevision: number): boolean {
        treeOwner(tree);
        const committed = revision(committedRevision), candidate = revision(candidateRevision);
        const record = this.trees.get(tree);
        if (!record?.completeV2 || record.attempt || record.retirement || record.settlement) return false;
        const previousCommitted = record.completeCommittedRevision;
        const previousCandidate = record.completeCandidateRevision;
        const valid = committed === previousCommitted && previousCandidate !== undefined &&
            (candidate === previousCandidate ||
                (previousCandidate < Number.MAX_SAFE_INTEGER && candidate === previousCandidate + 1));
        if (!valid) {
            this.clearV2Provenance(record);
            return false;
        }
        record.completeCommittedRevision = committed;
        record.completeCandidateRevision = candidate;
        return true;
    }

    /** Explicitly invalidate after a legacy/direct native transition which did
     * not return an authority-bearing settlement report. */
    invalidateV2Graph(tree: object): void {
        treeOwner(tree);
        const record = this.trees.get(tree);
        if (!record) return;
        this.clearV2Provenance(record);
    }

    /** Allocate a one-use host ticket before the irreversible native call.
     * A cloned candidate requires a terminal ticket even without complete
     * graph provenance. Such a ticket cannot authorize an exact graph shrink. */
    prepareV2Settlement(tree: object, outcome: V2TreeOutputSettlementOutcome,
        committedRevision: number, candidateRevision: number): RootTreeResidentSettlement | null {
        treeOwner(tree);
        if (outcome !== "commit" && outcome !== "abort") throw new TypeError("invalid settlement outcome");
        const committed = revision(committedRevision), candidate = revision(candidateRevision);
        const record = this.trees.get(tree);
        if (!record || record.attempt || record.retirement || record.settlement) return null;
        if (record.completeV2 && (record.completeCommittedRevision !== committed ||
            record.completeCandidateRevision !== candidate)) {
            this.clearV2Provenance(record);
        }
        const exactGraphEligible = record.completeV2 === true && this.mutationEpochValue !== null;
        if (!exactGraphEligible && !record.candidateRoot && !record.committedRoot) return null;
        const token = Object.freeze({ [settlementBrand]: true as const, [ownerBrand]: this });
        const state: SettlementState = { tree, outcome, active: true, admissionEpoch: 0,
            exactGraphEligible, candidateRoot: record.candidateRoot, committedRoot: record.committedRoot,
            resident: record.resident, residentBytes: record.residentBytes,
            committedRevision: committed, candidateRevision: candidate };
        this.settlements.set(token, state);
        const epoch = this.mutationEpochValue;
        state.admissionEpoch = epoch;
        record.settlement = state;
        return token;
    }

    /** The atomic native call threw before settlement. Preserve the old proven
     * graph, but consume this attempt ticket. */
    cancelV2SettlementBeforeSweep(token: RootTreeResidentSettlement): void {
        const state = this.activeSettlement(token);
        const record = this.trees.get(state.tree)!;
        state.active = false;
        record.settlement = undefined;
        this.settlements.delete(token);
    }

    /** Native completed but no exact receipt survived parsing/allocation.
     * Release only the root clone known to have been destroyed by that native
     * terminal outcome; keep all other charges and invalidate graph provenance. */
    abandonV2SettlementAfterSweep(token: RootTreeResidentSettlement): void {
        const state = this.activeSettlement(token);
        const record = this.trees.get(state.tree)!;
        state.active = false;
        record.settlement = undefined;
        this.settlements.delete(token);
        this.terminalRoot(record, state.outcome);
        this.clearV2Provenance(record);
    }

    /** Consume a native proof and shrink the aggregate resident lease in O(1).
     * False applies only the conservative root terminal transition and
     * invalidates graph provenance. Proof/epoch validation precedes any change. */
    settleAfterV2Sweep(token: RootTreeResidentSettlement,
        proof: V2TreeOutputSettlementProof): boolean {
        const proofState = takeV2TreeOutputSettlementProof(proof);
        const state = this.settlements.get(token);
        const record = state && this.trees.get(state.tree);
        if (!state?.active || !record || record.settlement !== state) return false;
        state.active = false;
        record.settlement = undefined;
        this.settlements.delete(token);
        const survivingRoot = state.outcome === "commit" ? record.candidateRoot : record.committedRoot;
        const identityBytes = survivingRoot?.identityBytes ?? 0;
        const droppedRoot = state.outcome === "commit" ? record.committedRoot : record.candidateRoot;
        const conservativeTarget = record.residentBytes - (droppedRoot?.residentBytes ?? 0);
        const target = proofState ? proofState.residentAdmissionBytes + identityBytes : undefined;
        const valid = state.exactGraphEligible && proofState !== null && proofState.tree === state.tree &&
            proofState.outcome === state.outcome &&
            proofState.beforeCommittedRevision === state.committedRevision &&
            proofState.beforeCandidateRevision === state.candidateRevision &&
            record.completeV2 === true &&
            record.completeCommittedRevision === state.committedRevision &&
            record.completeCandidateRevision === state.candidateRevision &&
            state.admissionEpoch !== null && this.mutationEpochValue === state.admissionEpoch &&
            record.resident === state.resident && record.residentBytes === state.residentBytes &&
            record.candidateRoot === state.candidateRoot && record.committedRoot === state.committedRoot &&
            Number.isSafeInteger(target) && target! >= identityBytes && target! <= conservativeTarget &&
            ((state.residentBytes === 0 && state.resident === null) ||
                (state.residentBytes > 0 && state.resident?.bytes === state.residentBytes));
        if (!valid) {
            this.terminalRoot(record, state.outcome);
            this.clearV2Provenance(record);
            return false;
        }
        try {
            this.terminalRoot(record, state.outcome);
            this.shrinkResident(record, target!);
            if (record.committedRoot) record.committedRoot.residentBytes = identityBytes;
            record.completeCommittedRevision = proofState.committedRevision;
            record.completeCandidateRevision = proofState.candidateRevision;
            record.completeV2 = true;
            return true;
        } catch {
            this.clearV2Provenance(record);
            return false;
        }
    }

    /** Caller proved the exact native terminal outcome, not merely a revision
     * change. Both slots are preallocated; no new owner or history is built. */
    private terminalRoot(record: TreeState, outcome: V2TreeOutputSettlementOutcome): void {
        const candidate = record.candidateRoot;
        const dropped = outcome === "commit" ? record.committedRoot : candidate;
        if (!candidate && !dropped) return;
        this.shrinkResident(record, record.residentBytes - (dropped?.residentBytes ?? 0));
        this.changing();
        if (dropped) dropped.phase = "released";
        if (outcome === "commit") record.committedRoot = candidate;
        record.candidateRoot = undefined;
    }

    private shrinkResident(record: TreeState, target: number): void {
        if (!Number.isSafeInteger(target) || target < 0 || target > record.residentBytes) {
            throw new RootTreeResidentStateError();
        }
        if (target === record.residentBytes) return;
        this.changing();
        if (target === 0) {
            record.resident?.release();
            record.resident = null;
        } else if (!record.resident || !this.ledger.tryResize(record.resident, target)) {
            throw new RootTreeResidentStateError();
        }
        this.residentBytes -= record.residentBytes - target;
        record.residentBytes = target;
    }

    setCapacity(capacityBytes: number): void { this.changing(); this.ledger.setCapacity(capacityBytes); }
    close(): void { this.changing(); this.ledger.close(); }

    /** Aggregate and O(1); no tree identity, path, hash or token is exposed. */
    snapshot(): RootTreeResidentAdmissionSnapshot {
        return { scope: "v2-tree-output-requested-buffers", ledger: this.ledger.snapshot(),
            residentTrees: this.residentTrees, privateAttempts: this.privateAttempts,
            retiringOwners: this.retiringOwners, privateBytes: this.privateBytes,
            residentBytes: this.residentBytes, retiringBytes: this.retiringBytes };
    }

    private activeAttempt(token: RootTreeResidentAttempt): AttemptState {
        const state = this.attempts.get(token);
        const record = state && this.trees.get(state.tree);
        if (!state || !record || record.attempt !== state || record.retirement || record.settlement ||
            (state.phase !== "private" && state.phase !== "ready")) throw new RootTreeResidentStateError();
        return state;
    }

    private activeSettlement(token: RootTreeResidentSettlement): SettlementState {
        const state = this.settlements.get(token);
        const record = state && this.trees.get(state.tree);
        if (!state?.active || !record || record.settlement !== state || record.attempt || record.retirement) {
            throw new RootTreeResidentStateError();
        }
        return state;
    }

    private clearV2Provenance(record: TreeState): void {
        record.completeV2 = false;
        record.completeCommittedRevision = undefined;
        record.completeCandidateRevision = undefined;
    }

    private newRetirement(tree: object): {
        state: RetirementState; token: RootTreeResidentRetirement;
    } {
        const state: RetirementState = { tree, active: false, lease: null, bytes: 0 };
        const token = Object.freeze({ [retirementBrand]: true as const, [ownerBrand]: this });
        this.retirements.set(token, state);
        return { state, token };
    }

}
