import type {
    CandidateMutationOutputMemoryPlan,
    CandidateMutationOutputMemoryReady,
    CandidateMutationOutputOwner,
} from "./tree-candidate-mutation-job";
import {
    RootTreeResidentAdmission,
    RootTreeResidentStateError,
    type RootTreeResidentAttempt,
    type RootTreeResidentRetirement,
} from "./root-tree-resident-admission";

/** Fail-fast refusal of the scoped candidate-mutation output allowance.
 * This is not an assertion about total native heap, linear memory, or RSS. */
export class CandidateMutationOutputAdmissionDeniedError extends Error {
    readonly code = "CANDIDATE_MUTATION_OUTPUT_ADMISSION_DENIED";

    constructor(
        readonly requestedBytes: number,
        readonly capacityBytes: number,
        readonly usedBytes: number,
    ) {
        super(`Candidate mutation output requires ${requestedBytes} requested bytes; ` +
            `${Math.max(0, capacityBytes - usedBytes)} are currently available`);
        this.name = "CandidateMutationOutputAdmissionDeniedError";
    }
}

/** Non-enumerable-by-construction refusal provenance. */
const refusalAdmissionEpochs = new WeakMap<CandidateMutationOutputAdmissionDeniedError, number>();

export function candidateMutationRefusalAdmissionEpoch(
    error: CandidateMutationOutputAdmissionDeniedError,
): number | null {
    return refusalAdmissionEpochs.get(error) ?? null;
}

/** Kept out of the Error's public shape: only push(), after late source
 * admission/dependency closure is known, may attest the exact candidate cut
 * which reached the mutation phase. */
interface CandidateMutationRefusalCutAttestation {
    readonly tree: object;
    readonly mutationInputCount: number;
    readonly candidateRevision: number;
}

const refusalMutationCuts = new WeakMap<
    CandidateMutationOutputAdmissionDeniedError,
    CandidateMutationRefusalCutAttestation
>();

export function attestCandidateMutationRefusalCut(
    error: CandidateMutationOutputAdmissionDeniedError,
    tree: object,
    mutationInputCount: number,
    candidateRevision: number,
): void {
    if (!Number.isSafeInteger(mutationInputCount) || mutationInputCount < 1 ||
        !Number.isSafeInteger(candidateRevision) || candidateRevision < 0 ||
        tree === null || (typeof tree !== "object" && typeof tree !== "function") ||
        refusalMutationCuts.has(error)) {
        throw new TypeError("invalid candidate mutation refusal cut attestation");
    }
    refusalMutationCuts.set(error, Object.freeze({ tree, mutationInputCount, candidateRevision }));
}

export function candidateMutationRefusalCut(
    error: CandidateMutationOutputAdmissionDeniedError,
): CandidateMutationRefusalCutAttestation | null {
    return refusalMutationCuts.get(error) ?? null;
}

type State = "private" | "ready" | "published" | "detached" | "released" | "retired";

/** Bind the generic mutation driver to one engine-owned resident ledger. The
 * returned callbacks are deliberately synchronous and idempotent for cleanup
 * replay. Candidate output remains conservatively resident after publication;
 * only whole-tree replacement/retirement or proven wrapper free releases it. */
export function admitCandidateMutationOutput(
    admission: RootTreeResidentAdmission,
    tree: object,
    plan: CandidateMutationOutputMemoryPlan,
): CandidateMutationOutputOwner {
    // Allocate the complete JS handoff before reserve. Once the ledger grants
    // bytes, only infallible local assignments remain before this owner is
    // returned to the driver; an allocation failure cannot strand an attempt
    // that the caller never received.
    let token: RootTreeResidentAttempt | undefined;
    let state: State = "private";
    let actualResidentBytes: number | undefined;
    let actualRootBytes: number | undefined;
    let retirement: RootTreeResidentRetirement | undefined;
    const requireState = (...allowed: State[]): void => {
        if (!allowed.includes(state)) throw new RootTreeResidentStateError();
    };
    const attemptToken = (): RootTreeResidentAttempt => {
        if (!token) throw new RootTreeResidentStateError();
        return token;
    };
    const owner: CandidateMutationOutputOwner = {
        releaseBeforeOutput(): void {
            if (state === "released") return;
            requireState("private");
            admission.releaseBeforeOutput(attemptToken());
            state = "released";
        },

        ready(ready: CandidateMutationOutputMemoryReady): void {
            if (state === "ready") {
                if (actualResidentBytes !== ready.residentAdmissionBytes ||
                    actualRootBytes !== ready.rangeEndpointResidentRequestedBytes) {
                    throw new RootTreeResidentStateError();
                }
                return;
            }
            requireState("private");
            admission.readyCandidateMutation(attemptToken(), ready.residentAdmissionBytes);
            actualResidentBytes = ready.residentAdmissionBytes;
            actualRootBytes = ready.rangeEndpointResidentRequestedBytes;
            state = "ready";
        },

        published(): void {
            if (state === "published") return;
            requireState("ready");
            if (plan.scope === "v1-candidate-mutation-output") {
                if (actualRootBytes === undefined) throw new RootTreeResidentStateError();
                admission.attachV1CandidateMutation(attemptToken(), actualRootBytes);
            } else {
                admission.attachCandidateMutation(attemptToken());
            }
            state = "published";
        },

        detached(): void {
            if (state === "detached") return;
            requireState("private", "ready");
            retirement = admission.detachPrivate(attemptToken());
            state = "detached";
        },

        retired(): void {
            // A published cohort now belongs to the shared tree store. Draining
            // the residual cursor is not proof that those immutable nodes died.
            if (state === "published" || state === "released" || state === "retired") return;
            requireState("detached");
            if (!retirement) throw new RootTreeResidentStateError();
            admission.releaseRetired(retirement);
            retirement = undefined;
            state = "retired";
        },
    };
    const attempt = admission.reserveCandidateMutation(tree, {
        peakBytes: plan.peakAdmissionBytes,
        residentBytes: plan.residentAdmissionBytes,
    });
    if (!attempt) {
        const snapshot = admission.snapshot().ledger;
        const error = new CandidateMutationOutputAdmissionDeniedError(
            plan.peakAdmissionBytes,
            snapshot.capacityBytes,
            snapshot.usedBytes,
        );
        const epoch = admission.mutationEpoch;
        try { if (epoch !== null) refusalAdmissionEpochs.set(error, epoch); }
        catch { /* Suppression provenance is optional; preserve the refusal. */ }
        throw error;
    }
    token = attempt;
    return owner;
}
