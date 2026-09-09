import { strict as assert } from "node:assert";
import { DirtyPathSet, DIRTY_RETIREMENT_LIMIT, type DirtyFileChange } from "./dirty-set";
import { selectRootLocalOmissions, RootLocalOmissionError, type RootLocalOmission } from "./root-local-omissions";
import type { RenameDependencySnapshot } from "./deferred-changes";

let assertions = 0;
function check(value: unknown, message: string): void { assertions++; assert.ok(value, message); }
const hint = (path: string, journalId?: number): DirtyFileChange => ({ path, action: "modified", journalId,
    hash: "a".repeat(64), mtime: 1, size: 1 });
const omitted = (path: string, reason: RootLocalOmission["reason"] = "excluded"): RootLocalOmission => ({ path, reason });
const edge = (left: string, right: string, patch: Partial<RenameDependencySnapshot> = {}): RenameDependencySnapshot =>
    ({ left, right, generation: 1, pending: false, uncertain: false, ...patch });
function invalid(queued: unknown, omissions: unknown, dependencies: unknown = []): void {
    let error: unknown;
    try { selectRootLocalOmissions(queued as DirtyFileChange[], omissions as RootLocalOmission[], dependencies as RenameDependencySnapshot[]); }
    catch (caught) { error = caught; }
    check(error instanceof RootLocalOmissionError && error.code === "ROOT_LOCAL_OMISSION_INPUT", "malformed omission input did not fail closed with its fixed error family");
    check(String(error) === "RootLocalOmissionError: Invalid root local omission selection input", "malformed omission diagnostics exposed input paths/IDs");
}

function explicitSelectionAndOriginalOwnership() {
    const dirty = new DirtyPathSet();
    dirty.add(hint("z-dir"), 3); dirty.add(hint("a.md"), 1); dirty.add(hint("real.md"), 2); dirty.add(hint("no-id.md"));
    const queued = dirty.take();
    const observations = [omitted("z-dir", "untracked-directory"), omitted("a.md"), omitted("no-id.md")];
    const selected = selectRootLocalOmissions(queued, observations);
    check(selected.selected.length === 2 && selected.selected[0] === queued[1] && selected.selected[1] === queued[0], "selection cloned provenance or failed strict lexical order");
    check(JSON.stringify(selected.cuts) === JSON.stringify([{ path: "a.md", throughId: 1 }, { path: "z-dir", throughId: 3 }]), "omission selection invented or widened journal cuts");
    check(selected.counts.excluded === 1 && selected.counts.untrackedDirectory === 1 && Object.keys(selected.counts).length === 2, "reason counters include unselected input or path data");
    check(queued.length === 4 && queued[0].path === "z-dir" && observations[0].path === "z-dir", "selector changed its caller-owned arrays");
    check(selectRootLocalOmissions(queued, []).selected.length === 0, "absence from ready was inferred without explicit classification");
    check(dirty.size === 0, "pure selector mutated dirty state");
    dirty.restore(selected.selected);
    // Only this explicit simulation of a successful exact owned journal ACK
    // permits commit. Selection itself never creates a retirement token.
    const token = dirty.captureRetirement(selected.cuts);
    check(dirty.commitRetirement(token) === 2 && dirty.size === 0, "original take provenance did not survive selection/restore");
    (observations[0] as { path: string }).path = "caller-mutated";
    check(selected.cuts[1].path === "z-dir", "returned cuts alias mutable explicit observations");
}

function completeQueueAndEveryDependencyAreConservative() {
    const queued = [hint("a.md", 1), hint("b.md", 1), hint("c.md", 2), hint("d.md", 3)];
    const shared = selectRootLocalOmissions(queued, [omitted("a.md"), omitted("c.md")]);
    check(shared.selected.length === 1 && shared.selected[0] === queued[2], "same-ID peer outside explicit omissions failed to hold its group");
    const states = [ {}, { pending: true }, { uncertain: true }, { pending: true, uncertain: true }, { generation: 0 } ];
    for (const state of states) {
        const selected = selectRootLocalOmissions(queued, [omitted("c.md"), omitted("d.md")], [edge("c.md", "not-queued.md", state)]);
        check(selected.selected.length === 1 && selected.selected[0] === queued[3], "confirmed/pending/uncertain or nonqueued dependency licensed an endpoint ACK");
    }
    const chain = selectRootLocalOmissions(queued, [omitted("c.md"), omitted("d.md")],
        [edge("c.md", "middle.md"), edge("middle.md", "d.md")]);
    check(chain.selected.length === 0, "transitive rename endpoints were split by omission selection");
    const laterShared = Array.from({ length: 258 }, (_, index) => hint(`${index.toString().padStart(3, "0")}.md`, index + 1));
    laterShared[257].journalId = 1;
    const bounded = selectRootLocalOmissions(laterShared, laterShared.map(value => omitted(value.path)));
    check(bounded.selected.length === 256 && !bounded.selected.includes(laterShared[0]) && !bounded.selected.includes(laterShared[257]), "selection stopped examining the full queue before a late same-ID peer");
}

function boundedCutsAndValidation() {
    const queued = Array.from({ length: 300 }, (_, index) => hint(`note-${(299 - index).toString().padStart(3, "0")}.md`, index + 1));
    const observed = queued.map((value, index) => omitted(value.path, index % 2 ? "excluded" : "untracked-directory"));
    const selected = selectRootLocalOmissions(queued, observed);
    check(selected.selected.length === DIRTY_RETIREMENT_LIMIT && selected.cuts.length === 256, "omission cut exceeded the retirement ceiling");
    check(selected.selected.every(value => queued.slice(0, 256).includes(value)) && selected.cuts.every((cut, index) => !index || selected.cuts[index - 1].path < cut.path),
        "selection failed queue-order cap or sorted/unique exact cuts");
    check(selected.counts.excluded === 128 && selected.counts.untrackedDirectory === 128, "bounded reason counters counted the withheld tail");
    const good = hint("safe.md", 1);
    for (const rows of [null, {}, [null], [good, good], [{ ...good, path: "../private.md" }], [{ ...good, path: "a\\b.md" }],
        [{ ...good, action: "renamed" }], [{ ...good, hash: "invalid" }], [{ ...good, mtime: NaN }], [{ ...good, size: -1 }],
        [{ ...good, data: undefined }], [{ ...good, secret: "not-supported" }]]) invalid(rows, []);
    const hiddenData = Object.defineProperty({ ...good }, "data", { value: new Uint8Array(1) }); invalid([hiddenData], []);
    for (const id of [0, -1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER, "1", null]) invalid([{ ...good, journalId: id }], []);
    for (const rows of [null, {}, [null], [omitted("absent.md")], [{ path: "safe.md", reason: "missing" }],
        [{ path: "safe.md" }], [{ reason: "excluded" }], [omitted("safe.md"), omitted("safe.md")],
        [{ ...omitted("safe.md"), extra: true }]]) invalid([good], rows);
    for (const edges of [null, {}, [null], [edge("../unsafe", "safe.md")], [edge("safe.md", "safe.md")],
        [edge("safe.md", "peer.md", { generation: -1 })], [edge("safe.md", "peer.md", { generation: Number.MAX_SAFE_INTEGER })],
        [edge("safe.md", "peer.md", { pending: undefined } as any)], [edge("safe.md", "peer.md", { uncertain: "false" } as any)],
        [{ ...edge("safe.md", "peer.md"), extra: 1 }], [edge("safe.md", "peer.md"), edge("peer.md", "safe.md")]]) invalid([good], [omitted("safe.md")], edges);
    // Input validation must still inspect the tail after the selectable cut is
    // full; malformed data cannot be silently hidden behind the size ceiling.
    invalid([...queued, { ...hint("late.md", 500), journalId: 0 }], observed);
}

function newerAndUnprovenHintsNeverGainRetirement() {
    for (const mode of ["new-before-capture", "new-after-capture", "inherited-id", "mutated-take", "foreign-owner"] as const) {
        const owner = new DirtyPathSet(); owner.add(hint("a.md"), 7);
        if (mode === "inherited-id") owner.add({ ...hint("a.md"), mtime: 2 });
        const queued = owner.take(), selected = selectRootLocalOmissions(queued, [omitted("a.md")]);
        check(selected.selected[0] === queued[0], "selector cloned an omission hint");
        if (mode === "new-before-capture") owner.add({ ...hint("a.md"), mtime: 2 });
        if (mode === "mutated-take") selected.selected[0].mtime = 2;
        const target = mode === "foreign-owner" ? new DirtyPathSet() : owner;
        target.restore(selected.selected); const token = target.captureRetirement(selected.cuts);
        if (mode === "new-after-capture") target.add({ ...hint("a.md"), mtime: 2 }, 8);
        check(target.commitRetirement(token) === 0 && target.size === 1, `${mode} gained old-ACK authority from a positive carried ID`);
        check(selected.cuts[0].throughId === 7, "omission cut was rewritten to a newer generation");
    }
    const noId = hint("no-id.md"), selection = selectRootLocalOmissions([noId], [omitted(noId.path)]);
    check(selection.selected.length === 0 && selection.cuts.length === 0 && selection.counts.excluded === 0, "unjournaled hint was granted an omission cut");
}

try {
    explicitSelectionAndOriginalOwnership(); completeQueueAndEveryDependencyAreConservative(); boundedCutsAndValidation();
    newerAndUnprovenHintsNeverGainRetirement();
    console.log(`root-local-omissions: ${assertions} assertions passed`);
} catch (error) { console.error(error); process.exitCode = 1; }
