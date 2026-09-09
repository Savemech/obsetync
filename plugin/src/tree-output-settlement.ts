/** Atomic V2 candidate commit/abort settlement adapter.
 *
 * A proof is created only from the direct successful return of the additive
 * native ABI and is kept in a module-local WeakMap. Malformed post-success
 * output never turns the already-completed native transition into an error;
 * callers retain their conservative accounting instead.
 */

import type {
    RootTreeResidentSettlement,
} from "./root-tree-resident-admission";

export type V2TreeOutputSettlementOutcome = "commit" | "abort";

export interface CandidateSettlementGcStats {
    before: number;
    reachable: number;
    removed: number;
    after: number;
    bytes_removed: number;
}

export interface V2CandidateSettlementWitness {
    readonly tree: object;
    readonly treeVersion: 2;
    readonly committedRevision: number;
    readonly candidateRevision: number;
}

interface CandidateRootSettlementWitness {
    readonly committedRevision: number;
    readonly candidateRevision: number;
}

const proofBrand: unique symbol = Symbol("V2TreeOutputSettlementProof");
export interface V2TreeOutputSettlementProof { readonly [proofBrand]: true }

export interface V2TreeOutputSettlementProofState {
    readonly tree: object;
    readonly outcome: V2TreeOutputSettlementOutcome;
    readonly beforeCommittedRevision: number;
    readonly beforeCandidateRevision: number;
    readonly committedRevision: number;
    readonly candidateRevision: number;
    readonly residentAdmissionBytes: number;
}

interface CandidateSettlementTree {
    tree_version(): number;
    committed_revision?: () => number;
    candidate_revision?: () => number;
    has_candidate(): boolean;
    commit_candidate(): unknown;
    abort_candidate(): unknown;
    commit_candidate_output_settlement_v1?: () => unknown;
    abort_candidate_output_settlement_v1?: () => unknown;
}

export interface CandidateSettlementCall {
    readonly atomicV2: boolean;
    readonly gcStats: CandidateSettlementGcStats | null;
    readonly proof: V2TreeOutputSettlementProof | null;
}

interface ResidentSettlementAdmission {
    prepareV2Settlement(tree: object, outcome: V2TreeOutputSettlementOutcome,
        committedRevision: number, candidateRevision: number): RootTreeResidentSettlement | null;
    cancelV2SettlementBeforeSweep(token: RootTreeResidentSettlement): void;
    abandonV2SettlementAfterSweep(token: RootTreeResidentSettlement): void;
    settleAfterV2Sweep(token: RootTreeResidentSettlement, proof: V2TreeOutputSettlementProof): boolean;
    invalidateV2Graph(tree: object): void;
}

const proofs = new WeakMap<object, V2TreeOutputSettlementProofState>();

function count(value: unknown): number {
    if (!Number.isSafeInteger(value) || (value as number) < 0) {
        throw new TypeError("invalid V2 output settlement count");
    }
    return value as number;
}

function exactRecord(value: unknown, keys: readonly string[]): Record<string, unknown> {
    if (value === null || (typeof value !== "object" && typeof value !== "function")) {
        throw new TypeError("invalid V2 output settlement report");
    }
    const actual = Reflect.ownKeys(value);
    if (actual.length !== keys.length || actual.some(key => typeof key !== "string" || !keys.includes(key))) {
        throw new TypeError("invalid V2 output settlement report shape");
    }
    const result: Record<string, unknown> = Object.create(null);
    for (const key of keys) {
        const descriptor = Object.getOwnPropertyDescriptor(value, key);
        if (!descriptor || !("value" in descriptor)) {
            throw new TypeError("invalid V2 output settlement report field");
        }
        result[key] = descriptor.value;
    }
    return result;
}

const REPORT_KEYS = ["schema", "scope", "outcome", "treeVersion", "before", "reachable",
    "removed", "after", "bytesRemoved", "committedRevision", "candidateRevision",
    "countersValid", "nodePayloadBytes", "rangeEndpointResidentRequestedBytes",
    "residentAdmissionBytes"] as const;

function readRevision(reader: (() => number) | undefined, tree: CandidateSettlementTree): number {
    if (typeof reader !== "function") throw new TypeError("V2 output settlement revision API is missing");
    return count(reader.call(tree));
}

/** Best-effort pre-native witness. Null simply disables accounting shrink. */
export function captureV2CandidateSettlementWitness(
    tree: CandidateSettlementTree,
): V2CandidateSettlementWitness | null {
    try {
        if (tree.tree_version() !== 2 || tree.has_candidate() !== true) return null;
        return Object.freeze({
            tree,
            treeVersion: 2 as const,
            committedRevision: readRevision(tree.committed_revision, tree),
            candidateRevision: readRevision(tree.candidate_revision, tree),
        });
    } catch {
        return null;
    }
}

/** Best-effort revision fence for the format-neutral candidate-root owner.
 * Tree v1 has no exact stable-graph settlement report, but its cloned root is
 * still known to be destroyed or promoted by a successful terminal call. */
function captureCandidateRootSettlementWitness(
    tree: CandidateSettlementTree,
): CandidateRootSettlementWitness | null {
    try {
        const version = tree.tree_version();
        if ((version !== 1 && version !== 2) || tree.has_candidate() !== true) return null;
        return {
            committedRevision: readRevision(tree.committed_revision, tree),
            candidateRevision: readRevision(tree.candidate_revision, tree),
        };
    } catch {
        return null;
    }
}

function parseLegacyGcStats(value: unknown, target: CandidateSettlementGcStats): boolean {
    const row = exactRecord(value, ["before", "reachable", "removed", "after", "bytes_removed"]);
    target.before = count(row.before);
    target.reachable = count(row.reachable);
    target.removed = count(row.removed);
    target.after = count(row.after);
    target.bytes_removed = count(row.bytes_removed);
    return target.after === target.reachable && target.removed === target.before - target.after;
}

/** Invoke exactly one candidate outcome. A thrown native call made no
 * settlement under the additive ABI. Once it returns, all parsing is
 * best-effort and this function itself no longer throws. */
export function settleTreeCandidateOutput(
    tree: CandidateSettlementTree,
    outcome: V2TreeOutputSettlementOutcome,
    witness: V2CandidateSettlementWitness | null = null,
    assertBeforeNative?: () => void,
): CandidateSettlementCall {
    if (outcome !== "commit" && outcome !== "abort") {
        throw new TypeError("invalid V2 output settlement outcome");
    }
    const commitAtomic = tree.commit_candidate_output_settlement_v1;
    const abortAtomic = tree.abort_candidate_output_settlement_v1;
    const claimed = commitAtomic !== undefined || abortAtomic !== undefined;
    if (claimed && (typeof commitAtomic !== "function" || typeof abortAtomic !== "function")) {
        throw new TypeError("WASM tree exposes an incomplete V2 output settlement API");
    }
    const version = tree.tree_version();
    if (version !== 2 || !claimed) {
        const result = { atomicV2: false, gcStats: null, proof: null } as {
            atomicV2: boolean; gcStats: CandidateSettlementGcStats | null;
            proof: V2TreeOutputSettlementProof | null;
        };
        const gc = { before: 0, reachable: 0, removed: 0, after: 0, bytes_removed: 0 };
        const legacy = outcome === "commit" ? tree.commit_candidate : tree.abort_candidate;
        if (typeof legacy !== "function") throw new TypeError("candidate settlement method is missing");
        assertBeforeNative?.();
        const raw = legacy.call(tree);
        try { if (parseLegacyGcStats(raw, gc)) result.gcStats = gc; } catch { /* diagnostics only */ }
        return result;
    }

    let beforeCommitted: number | null = null, beforeCandidate: number | null = null;
    try {
        beforeCommitted = readRevision(tree.committed_revision, tree);
        beforeCandidate = readRevision(tree.candidate_revision, tree);
        if (tree.has_candidate() !== true || (witness && (witness.tree !== tree ||
            witness.committedRevision !== beforeCommitted || witness.candidateRevision !== beforeCandidate))) {
            throw new TypeError("V2 output settlement witness is stale");
        }
    } catch (error) {
        if (witness) throw error;
    }

    // Allocate every host capability/result owner before the irreversible
    // native call. The mutable records are private and published only after a
    // complete exact report validates.
    const token = Object.freeze({ [proofBrand]: true as const }) as V2TreeOutputSettlementProof;
    const state = {
        tree,
        outcome,
        beforeCommittedRevision: beforeCommitted ?? -1,
        beforeCandidateRevision: beforeCandidate ?? -1,
        committedRevision: -1,
        candidateRevision: -1,
        residentAdmissionBytes: -1,
    } as V2TreeOutputSettlementProofState;
    const result = { atomicV2: true, gcStats: null, proof: null } as {
        atomicV2: boolean; gcStats: CandidateSettlementGcStats | null;
        proof: V2TreeOutputSettlementProof | null;
    };
    const gc = { before: 0, reachable: 0, removed: 0, after: 0, bytes_removed: 0 };
    proofs.set(token, state);
    let raw: unknown;
    try {
        // This is deliberately the final observable host callback before the
        // already-captured native method. Outer owners can pin their exact
        // candidate revision here after witness/ticket preparation, closing
        // a reentrant getter/admission ABA without adding a host turn.
        assertBeforeNative?.();
        raw = (outcome === "commit" ? commitAtomic! : abortAtomic!).call(tree);
    } catch (error) {
        proofs.delete(token);
        throw error;
    }

    try {
        const row = exactRecord(raw, REPORT_KEYS);
        const before = count(row.before), reachable = count(row.reachable);
        const removed = count(row.removed), after = count(row.after);
        const bytesRemoved = count(row.bytesRemoved);
        const committedRevision = count(row.committedRevision);
        const candidateRevision = count(row.candidateRevision);
        const nodePayload = count(row.nodePayloadBytes);
        const endpoint = count(row.rangeEndpointResidentRequestedBytes);
        const resident = count(row.residentAdmissionBytes);
        const expectedCommitted = outcome === "commit" ? (beforeCommitted ?? -2) + 1 : beforeCommitted;
        const expectedCandidate = (beforeCandidate ?? -2) + 1;
        if (row.schema !== 1 || row.scope !== "v2-stable-tree-output" || row.outcome !== outcome ||
            row.treeVersion !== 2 || row.countersValid !== true || after !== reachable ||
            removed !== before - after || resident !== nodePayload + endpoint ||
            committedRevision !== expectedCommitted || candidateRevision !== expectedCandidate ||
            tree.tree_version() !== 2 || tree.has_candidate() !== false ||
            readRevision(tree.committed_revision, tree) !== committedRevision ||
            readRevision(tree.candidate_revision, tree) !== candidateRevision) {
            throw new TypeError("inconsistent V2 output settlement report");
        }
        gc.before = before; gc.reachable = reachable; gc.removed = removed;
        gc.after = after; gc.bytes_removed = bytesRemoved;
        Object.assign(state, { committedRevision, candidateRevision, residentAdmissionBytes: resident });
        result.gcStats = gc;
        result.proof = token;
        return result;
    } catch {
        proofs.delete(token);
        return result;
    }
}

/** Complete the host accounting handoff around one native outcome. Nothing in
 * the post-success half may reclassify an already-completed commit/abort as a
 * transaction failure; missing provenance simply retains the old charge. */
export function settleTreeCandidateOutputWithAdmission(
    tree: CandidateSettlementTree,
    outcome: V2TreeOutputSettlementOutcome,
    admission?: ResidentSettlementAdmission,
    assertBeforeNative?: () => void,
): CandidateSettlementCall {
    const witness = admission ? captureV2CandidateSettlementWitness(tree) : null;
    const rootWitness = admission ? witness ?? captureCandidateRootSettlementWitness(tree) : null;
    const ticket = rootWitness && admission
        ? admission.prepareV2Settlement(tree, outcome,
            rootWitness.committedRevision, rootWitness.candidateRevision)
        : null;
    let result: CandidateSettlementCall;
    try {
        result = settleTreeCandidateOutput(tree, outcome, witness, assertBeforeNative);
    } catch (error) {
        if (ticket && admission) {
            try { admission.cancelV2SettlementBeforeSweep(ticket); } catch { /* primary native error wins */ }
        }
        throw error;
    }
    if (!admission) return result;
    try {
        if (ticket && result.proof) admission.settleAfterV2Sweep(ticket, result.proof);
        else if (ticket) admission.abandonV2SettlementAfterSweep(ticket);
        else admission.invalidateV2Graph(tree);
    } catch {
        // Native already returned successfully. Preserve its outcome and every
        // conservative byte charge; cleanup of a still-active ticket is also
        // best-effort because it grants no release by itself.
        if (ticket) {
            try { admission.abandonV2SettlementAfterSweep(ticket); } catch { /* retain charge */ }
        }
        try { admission.invalidateV2Graph(tree); } catch { /* retain charge */ }
    }
    return result;
}

/** Consume exactly once. A foreign/copied/replayed token has no state. */
export function takeV2TreeOutputSettlementProof(
    proof: V2TreeOutputSettlementProof,
): V2TreeOutputSettlementProofState | null {
    if ((typeof proof !== "object" && typeof proof !== "function") || proof === null) return null;
    const state = proofs.get(proof) ?? null;
    proofs.delete(proof);
    return state;
}
