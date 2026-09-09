import { DIRTY_RETIREMENT_LIMIT, type DirtyFileChange, type DirtyJournalCut } from "./dirty-set";
import type { RenameDependencySnapshot } from "./deferred-changes";
import type { FileChangeOmissionReason } from "./file-safety";
import { isSafeVaultPath } from "./delta-validation";

export interface RootLocalOmission {
    readonly path: string;
    readonly reason: FileChangeOmissionReason;
}
export interface RootLocalOmissionSelection {
    /** Original detached take() objects, not cloned provenance. The caller
     * must keep their scalar metadata unchanged through restore/capture. */
    selected: DirtyFileChange[];
    /** Detached, strictly path-ordered, exact positive journal watermarks. */
    cuts: DirtyJournalCut[];
    /** Selected counts only. No paths, hashes or journal IDs in diagnostics. */
    counts: { excluded: number; untrackedDirectory: number };
}
export class RootLocalOmissionError extends Error {
    readonly code = "ROOT_LOCAL_OMISSION_INPUT";
    constructor() { super("Invalid root local omission selection input"); this.name = "RootLocalOmissionError"; }
}
function invalid(): never { throw new RootLocalOmissionError(); }
function object(value: unknown, fields: readonly string[]): asserts value is Record<string, unknown> {
    if (!value || typeof value !== "object" || Array.isArray(value) || Object.keys(value).some(field => !fields.includes(field))) invalid();
}
function generation(value: unknown): value is number {
    return Number.isSafeInteger(value) && (value as number) > 0 && (value as number) < Number.MAX_SAFE_INTEGER;
}
function validateHint(value: unknown): asserts value is DirtyFileChange {
    object(value, ["path", "action", "journalId", "hash", "mtime", "size"]);
    if (!isSafeVaultPath(value.path) ||
        (value.action !== "created" && value.action !== "modified" && value.action !== "deleted") ||
        (value.journalId !== undefined && !generation(value.journalId)) ||
        (value.hash !== undefined && (typeof value.hash !== "string" || !/^[0-9a-f]{64}$/.test(value.hash))) ||
        (value.mtime !== undefined && !Number.isFinite(value.mtime)) ||
        (value.size !== undefined && (!Number.isSafeInteger(value.size) || (value.size as number) < 0))) invalid();
    // Non-enumerable file data must not bypass the detached metadata contract.
    if ("data" in value) invalid();
}

/** Pure bounded selection, never journal ACK or source/deletion authority.
 * Call only with explicit observations from a FULL successful materialization
 * that passed bulk review. An absent ready row alone proves nothing.
 *
 * Any current rename endpoint is withheld, even when its peer is not queued
 * or the registration is already confirmed. Equal IDs anywhere in the FULL
 * queued snapshot likewise withhold all members. No dependency is split.
 *
 * Select at most 256 in input queue order, then sort only that bounded cut.
 * Full-input maps are O(current backlog/edges), not a historical or hard RSS
 * bound. All caller inputs are read-only; selected objects preserve owner-local
 * DirtyPathSet take/restore provenance. The positive ID is only a WAL cut: it
 * may be an inherited watermark on a newer no-ID hint and MUST NOT by itself
 * authorize removing that hint. Restore/captureRetirement plus current scope,
 * dependency/live guards and acknowledgeOwned are still the caller's job. */
export function selectRootLocalOmissions(
    queued: readonly DirtyFileChange[],
    explicitOmissions: readonly RootLocalOmission[],
    dependencies: readonly RenameDependencySnapshot[] = [],
): RootLocalOmissionSelection {
    if (!Array.isArray(queued) || !Array.isArray(explicitOmissions) || !Array.isArray(dependencies)) invalid();
    const hints = new Map<string, DirtyFileChange>(), idCounts = new Map<number, number>();
    for (const hint of queued) {
        validateHint(hint);
        if (hints.has(hint.path)) invalid();
        hints.set(hint.path, hint);
        if (hint.journalId !== undefined) idCounts.set(hint.journalId, (idCounts.get(hint.journalId) ?? 0) + 1);
    }
    const reasons = new Map<string, FileChangeOmissionReason>();
    for (const omission of explicitOmissions) {
        object(omission, ["path", "reason"]);
        if (!isSafeVaultPath(omission.path) || !hints.has(omission.path) || reasons.has(omission.path) ||
            (omission.reason !== "excluded" && omission.reason !== "untracked-directory")) invalid();
        reasons.set(omission.path, omission.reason);
    }
    const blocked = new Set<string>(), edges = new Map<string, Set<string>>();
    for (const edge of dependencies) {
        object(edge, ["left", "right", "generation", "pending", "uncertain"]);
        if (!isSafeVaultPath(edge.left) || !isSafeVaultPath(edge.right) || edge.left === edge.right ||
            !Number.isSafeInteger(edge.generation) || (edge.generation as number) < 0 || (edge.generation as number) >= Number.MAX_SAFE_INTEGER ||
            typeof edge.pending !== "boolean" || typeof edge.uncertain !== "boolean") invalid();
        const left = edge.left < edge.right ? edge.left : edge.right, right = edge.left < edge.right ? edge.right : edge.left;
        const related = edges.get(left) ?? new Set<string>();
        if (related.has(right)) invalid();
        related.add(right); edges.set(left, related);
        blocked.add(left); blocked.add(right);
    }
    const selected: DirtyFileChange[] = [];
    for (const hint of queued) {
        if (selected.length === DIRTY_RETIREMENT_LIMIT) break;
        if (!reasons.has(hint.path) || blocked.has(hint.path) || hint.journalId === undefined || idCounts.get(hint.journalId) !== 1) continue;
        selected.push(hint);
    }
    selected.sort((left, right) => left.path < right.path ? -1 : left.path > right.path ? 1 : 0);
    const counts = { excluded: 0, untrackedDirectory: 0 };
    const cuts = selected.map(hint => {
        if (reasons.get(hint.path) === "excluded") counts.excluded++;
        else counts.untrackedDirectory++;
        return { path: hint.path, throughId: hint.journalId! };
    });
    return { selected, cuts, counts };
}
