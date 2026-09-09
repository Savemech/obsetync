import type { CandidateOpenMemoryOwner, CandidateOpenMemoryPlan } from "./tree-candidate-job";
import {
    RootTreeResidentAdmission,
    RootTreeResidentStateError,
    type RootTreeResidentAttempt,
    type RootTreeResidentRetirement,
} from "./root-tree-resident-admission";

export class CandidateOpenRootAdmissionDeniedError extends Error {
    readonly code = "CANDIDATE_OPEN_ROOT_ADMISSION_DENIED";
    constructor(
        readonly requestedBytes: number,
        readonly capacityBytes: number,
        readonly usedBytes: number,
    ) {
        super(`candidate root needs ${requestedBytes} resident bytes; ` +
            `${usedBytes}/${capacityBytes} bytes are already reserved`);
        this.name = "CandidateOpenRootAdmissionDeniedError";
    }
}

type State = "private" | "ready" | "published" | "detached" | "released" | "retired";

/** Bind one exact native candidate-root request to the shared resident ledger.
 * All callbacks are synchronous and idempotent for cleanup replay. */
export function admitCandidateOpenRoot(
    admission: RootTreeResidentAdmission,
    tree: object,
    plan: CandidateOpenMemoryPlan,
): CandidateOpenMemoryOwner {
    let token: RootTreeResidentAttempt | undefined;
    let retirement: RootTreeResidentRetirement | undefined;
    let state: State = "private";
    const requireState = (...allowed: State[]): void => {
        if (!allowed.includes(state)) throw new RootTreeResidentStateError();
    };
    const attempt = (): RootTreeResidentAttempt => {
        if (!token) throw new RootTreeResidentStateError();
        return token;
    };
    const owner: CandidateOpenMemoryOwner = {
        releaseBeforeOutput(): void {
            if (state === "released") return;
            requireState("private");
            admission.releaseBeforeOutput(attempt());
            state = "released";
        },
        ready(): void {
            if (state === "ready") return;
            requireState("private");
            admission.readyCandidateRoot(attempt());
            state = "ready";
        },
        published(): void {
            if (state === "published") return;
            requireState("ready");
            admission.attachCandidateRoot(attempt());
            state = "published";
        },
        detached(): void {
            if (state === "detached") return;
            requireState("private", "ready");
            retirement = admission.detachPrivate(attempt());
            state = "detached";
        },
        retired(): void {
            if (state === "published" || state === "released" || state === "retired") return;
            requireState("detached");
            if (!retirement) throw new RootTreeResidentStateError();
            admission.releaseRetired(retirement);
            retirement = undefined;
            state = "retired";
        },
    };

    const reserved = admission.reserveCandidateRoot(
        tree,
        plan.rootStringRequestedBytes,
        plan.rootIdentityRequestedBytes,
    );
    if (!reserved) {
        const snapshot = admission.snapshot().ledger;
        throw new CandidateOpenRootAdmissionDeniedError(
            plan.rootStringRequestedBytes,
            snapshot.capacityBytes,
            snapshot.usedBytes,
        );
    }
    token = reserved;
    return owner;
}
