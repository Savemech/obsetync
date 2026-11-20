/** Atomic candidate commit/abort settlement adapter.
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

export interface CandidateSettlementWitness {
    readonly tree: object;
    readonly treeVersion: 1 | 2;
    readonly committedRevision: number;
    readonly candidateRevision: number;
}

const proofBrand: unique symbol = Symbol("V2TreeOutputSettlementProof");
export interface V2TreeOutputSettlementProof { readonly [proofBrand]: true }

export interface V2TreeOutputSettlementProofState {
    readonly tree: object;
    readonly treeVersion: 1 | 2;
    readonly outcome: V2TreeOutputSettlementOutcome;
    readonly beforeCommittedRevision: number;
    readonly beforeCandidateRevision: number;
    readonly committedRevision: number;
    readonly candidateRevision: number;
    readonly residentAdmissionBytes: number;
    readonly rootRequestedBytes: number;
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
export function captureCandidateSettlementWitness(
    tree: CandidateSettlementTree,
): CandidateSettlementWitness | null {
    try {
        const treeVersion = tree.tree_version();
        if ((treeVersion !== 1 && treeVersion !== 2) || tree.has_candidate() !== true) return null;
        return Object.freeze({
            tree,
            treeVersion,
            committedRevision: readRevision(tree.committed_revision, tree),
            candidateRevision: readRevision(tree.candidate_revision, tree),
        });
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
    witness: CandidateSettlementWitness | null = null,
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
    if (witness && (witness.tree !== tree || witness.treeVersion !== version)) {
        throw new TypeError("candidate settlement witness is stale");
    }
    if ((version !== 1 && version !== 2) || !claimed) {
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

    const before = captureCandidateSettlementWitness(tree);
    if (witness && (!before || before.treeVersion !== version ||
        witness.committedRevision !== before.committedRevision ||
        witness.candidateRevision !== before.candidateRevision)) {
        throw new TypeError("candidate settlement witness is stale");
    }

    // Allocate every host capability/result owner before the irreversible
    // native call. The mutable records are private and published only after a
    // complete exact report validates.
    const token = Object.freeze({ [proofBrand]: true as const }) as V2TreeOutputSettlementProof;
    const state = {
        tree,
        treeVersion: version,
        outcome,
        beforeCommittedRevision: before?.committedRevision ?? -1,
        beforeCandidateRevision: before?.candidateRevision ?? -1,
        committedRevision: -1,
        candidateRevision: -1,
        residentAdmissionBytes: -1,
        rootRequestedBytes: -1,
    } as V2TreeOutputSettlementProofState;
    const result = { atomicV2: version === 2, gcStats: null, proof: null } as {
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
        if (!before || before.treeVersion !== version) {
            proofs.delete(token);
            return result;
        }
        const row = exactRecord(raw, REPORT_KEYS);
        const beforeCount = count(row.before), reachable = count(row.reachable);
        const removed = count(row.removed), after = count(row.after);
        const bytesRemoved = count(row.bytesRemoved);
        const committedRevision = count(row.committedRevision);
        const candidateRevision = count(row.candidateRevision);
        const nodePayload = count(row.nodePayloadBytes);
        const endpoint = count(row.rangeEndpointResidentRequestedBytes);
        const resident = count(row.residentAdmissionBytes);
        const expectedCommitted = outcome === "commit" ? before.committedRevision + 1 : before.committedRevision;
        const expectedCandidate = before.candidateRevision + 1;
        if (row.schema !== 1 || row.scope !== `v${version}-stable-tree-output` || row.outcome !== outcome ||
            row.treeVersion !== version || row.countersValid !== true || after !== reachable ||
            removed !== beforeCount - after || resident !== nodePayload + endpoint ||
            committedRevision !== expectedCommitted || candidateRevision !== expectedCandidate ||
            tree.tree_version() !== version || tree.has_candidate() !== false ||
            readRevision(tree.committed_revision, tree) !== committedRevision ||
            readRevision(tree.candidate_revision, tree) !== candidateRevision) {
            throw new TypeError("inconsistent V2 output settlement report");
        }
        gc.before = beforeCount; gc.reachable = reachable; gc.removed = removed;
        gc.after = after; gc.bytes_removed = bytesRemoved;
        Object.assign(state, { committedRevision, candidateRevision, residentAdmissionBytes: resident,
            rootRequestedBytes: endpoint });
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
    const witness = admission ? captureCandidateSettlementWitness(tree) : null;
    const ticket = witness && admission
        ? admission.prepareV2Settlement(tree, outcome,
            witness.committedRevision, witness.candidateRevision)
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
