import { strict as assert } from "node:assert";
import { DeferredChangeTracker } from "./deferred-changes";
import { DirtyPathSet, type DirtyFileChange } from "./dirty-set";
import type { FileChange } from "./push";
import { selectRootBatch, ROOT_BATCH_LIMITS, type RootBatchDependency, type RootBatchSelection } from "./root-batch";
import { detachRootBasePublication } from "./sync-base";

let assertions = 0;
const check = (value: unknown, message: string) => { assertions++; assert.ok(value, message); };
const same = (actual: unknown, expected: unknown, message: string) => { assertions++; assert.deepEqual(actual, expected, message); };
const fails = (run: () => unknown, message: string) => { assertions++; assert.throws(run, /root batch/, message); };
const paths = (changes: readonly FileChange[]) => changes.map(change => change.path);
const hint = (path: string, journalId?: number, action: FileChange["action"] = "modified"): DirtyFileChange =>
    ({ path, action, ...(journalId === undefined ? {} : { journalId }), size: 10, mtime: 20 });
const edge = (left: string, right: string, generation = 1): RootBatchDependency =>
    ({ left, right, generation, pending: false, uncertain: false });
function completePartition(result: RootBatchSelection, queued: readonly DirtyFileChange[], ready: readonly FileChange[]): void {
    const selected = new Set(paths(result.queued)), held = new Set(paths(result.retained));
    check(selected.size === result.queued.length && held.size === result.retained.length, "duplicate disposition path");
    check([...selected].every(path => !held.has(path)), "selected path also retained");
    same([...selected, ...held].sort(), paths(queued).sort(), "queued path was lost or invented");
    same([...paths(result.ready), ...paths(result.unselected)].sort(), paths(ready).sort(), "materialized path was lost or invented");
    same(paths(result.ready).sort(), [...selected].sort(), "selected ready and queued paths differ");
    check(result.ready.length <= 256 && result.queued.length <= 256 && result.cutCount <= 256, "selection exceeded hard path/cut cap");
    check(result.estimatedMetadataBytes <= ROOT_BATCH_LIMITS.metadataBytes, "selection exceeded hard metadata cap");
    check(Object.values(result.deferred).reduce((sum, item) => sum + item.paths, 0) === result.retained.length,
        "aggregate deferrals do not cover exact retained paths");
}

function chainsCyclesAndPriority(): void {
    const queued = [hint("c.md", 3), hint("b.md", 2), hint("a.md", 1), hint("independent.md", 9)];
    const dependencies = [edge("a.md", "b.md", 1), edge("b.md", "c.md", 2), edge("c.md", "a.md", 1)];
    const ready = [queued[2], queued[3], queued[0], queued[1]];
    const selected = selectRootBatch(ready, queued, dependencies, { maxPaths: 3 });
    same(paths(selected.ready), ["a.md", "c.md", "b.md"], "component changed materialized priority order");
    same(paths(selected.queued), ["c.md", "b.md", "a.md"], "selected queued snapshot order changed");
    same(selected.selectedGroups, [["a.md", "c.md", "b.md"]], "transitive cycle was not one disjoint dependency group");
    same(selected.deferred["transaction-limit"], { components: 1, paths: 1 }, "capacity deferral was confused with oversized component");
    completePartition(selected, queued, ready);
    const oversized = selectRootBatch(ready, queued, dependencies, { maxPaths: 2 });
    same(paths(oversized.ready), ["independent.md"], "oversized chain blocked independent work or split its member");
    same(oversized.deferred["component-limit"], { components: 1, paths: 3 }, "oversized component was not explicit");
    completePartition(oversized, queued, ready);
}

function groupsFromSharedIdsAndNewerRenames(): void {
    const sameIds = [hint("old.md", 2, "deleted"), hint("new.md", 2), hint("other.md", 3)];
    const limited = selectRootBatch(sameIds, sameIds, [], { maxPaths: 1 });
    same(paths(limited.ready), ["other.md"], "equal journal ID group was split");
    same(limited.deferred["component-limit"], { components: 1, paths: 2 }, "equal-ID group not counted once");
    const tracker = new DeferredChangeTracker(); tracker.registerLegacyRename("old.md", "new.md", 2);
    const newer = [sameIds[0], hint("new.md", 5), sameIds[2]];
    const selected = selectRootBatch(newer, newer, tracker.captureDependencies(), { maxPaths: 2 });
    same(paths(selected.ready), ["old.md", "new.md"], "new edit replacing destination ID lost explicit link");
    same(selected.selectedGroups, [["old.md", "new.md"]], "new-ID destination was treated as independent");
    const stale = [hint("old.md", 1, "deleted"), hint("new.md", 5), sameIds[2]];
    const staleSelected = selectRootBatch(stale, stale, tracker.captureDependencies());
    same(paths(staleSelected.ready), ["other.md"], "newer rename was applied to a detached older source cut");
    same(staleSelected.deferred["stale-capture"], { components: 1, paths: 2 }, "stale capture has no explicit disposition");
    completePartition(staleSelected, stale, stale);
}

function missingAndUncertainPeers(): void {
    const queued = [hint("a.md", 3), hint("b.md", 4), hint("other.md", 5)];
    for (const ready of [[queued[0], queued[2]], [queued[1], queued[2]], [queued[2]]]) {
        const selected = selectRootBatch(ready, queued, [edge("a.md", "b.md", 3)]);
        same(paths(selected.ready), ["other.md"], "omitted/cooling/ignored materialized peer allowed a partial group");
        same(selected.deferred["missing-peer"], { components: 1, paths: 2 }, "missing materialized member was not explicit");
        completePartition(selected, queued, ready);
    }
    const throughMissing = selectRootBatch(queued, queued,
        [edge("a.md", "missing.md", 1), edge("missing.md", "b.md", 1)]);
    same(paths(throughMissing.ready), ["other.md"], "non-queued intermediate path broke dependency closure");
    same(throughMissing.deferred["missing-peer"], { components: 1, paths: 2 }, "external missing peer invented queued work");
    const tracker = new DeferredChangeTracker();
    const pending = tracker.registerLegacyRename("a.md", "b.md");
    const captured = tracker.captureDependencies();
    pending.confirm(3);
    same(paths(selectRootBatch(queued, queued, captured).ready), ["other.md"], "late confirmation rewrote a pending snapshot");
    same(paths(selectRootBatch(queued, queued, tracker.captureDependencies()).ready), paths(queued), "known confirmed group remained needlessly held");
    const uncertain = tracker.registerLegacyRename("a.md", "b.md"); uncertain.confirm();
    const held = selectRootBatch(queued, queued, tracker.captureDependencies());
    same(paths(held.ready), ["other.md"], "ambiguous append licensed grouped publication");
    same(held.deferred["unresolved-dependency"], { components: 1, paths: 2 }, "uncertain group has no explicit disposition");
    completePartition(held, queued, queued);
}

function countAndMetadataBounds(): void {
    const queued = [hint("a.md", 1), hint("b.md", 2), hint("scan.md")];
    const cuts = selectRootBatch(queued, queued, [], { maxPaths: 3, maxCuts: 1 });
    same(paths(cuts.ready), ["a.md", "scan.md"], "cut limit either split ownership or excluded no-cut scan work");
    check(cuts.cutCount === 1, "scan-only hint acquired a synthetic durable cut");
    const first = selectRootBatch([queued[0]], [queued[0]]);
    const exact = selectRootBatch(queued, queued, [], { maxMetadataBytes: first.estimatedMetadataBytes });
    same(paths(exact.ready), ["a.md"], "exact conservative byte cut did not select one complete row");
    const tooSmall = selectRootBatch([queued[0]], [queued[0]], [], { maxMetadataBytes: first.estimatedMetadataBytes - 1 });
    same(paths(tooSmall.ready), [], "one-byte-too-small budget admitted a singleton");
    same(tooSmall.deferred["component-limit"], { components: 1, paths: 1 }, "oversized singleton lacked explicit deferral");
    check(tooSmall.estimatedMetadataBytes === 0, "empty selection pretended to retain publication metadata");
    const long = hint(`${"é".repeat(3000)}.md`, 1);
    const longPeer = hint(`${"ø".repeat(3000)}.md`, 2);
    const independent = hint("small.md", 3);
    const longGroup = selectRootBatch([long, independent, longPeer], [long, longPeer, independent],
        [edge(long.path, longPeer.path, 1)], { maxMetadataBytes: 20_000 });
    same(paths(longGroup.ready), ["small.md"], "multibyte metadata group was split or ignored byte ceiling");
    same(longGroup.deferred["component-limit"], { components: 1, paths: 2 }, "multibyte group limit not explicit");
    completePartition(longGroup, [long, longPeer, independent], [long, independent, longPeer]);
}

function estimateCoversActualPublication(): void {
    const names = ["plain.md", 'quote"name.md', "ユニコード/😀.md", "lonely-\ud800.md", `${"x".repeat(4090)}.md`];
    const queued = names.map((name, index) => hint(name, index + 1));
    const selected = selectRootBatch(queued, queued);
    const publication = detachRootBasePublication({ identity: { scopeHash: "a".repeat(64), sequence: Number.MAX_SAFE_INTEGER,
        mutationId: "b".repeat(32), requestHash: "c".repeat(64) }, candidateRoot: "d".repeat(64), committedAt: Number.MAX_SAFE_INTEGER,
        entries: [...queued].sort((a, b) => a.path < b.path ? -1 : 1).map(change => ({ action: "upsert", path: change.path,
            hash: "f".repeat(64), mtime: -Number.MAX_VALUE, treeMtime: 1.2345678901234567e-6, size: Number.MAX_SAFE_INTEGER })) });
    const cuts = queued.map(change => ({ path: change.path, throughId: Number.MAX_SAFE_INTEGER - 1 }));
    const actual = new TextEncoder().encode(JSON.stringify(publication)).byteLength +
        new TextEncoder().encode(JSON.stringify(cuts)).byteLength;
    check(actual < selected.estimatedMetadataBytes, "conservative estimate undercounted canonical publication plus cuts");
    completePartition(selected, queued, queued);
}

function detachedMetadataAndRuntimeDeferralContract(): void {
    const queued = [hint("old.md", 2, "deleted"), hint("new.md", 3), hint("note.md", 4)];
    const ready = queued.map(change => ({ ...change }));
    Object.defineProperty(ready[1], "data", { get() { throw new Error("source bytes must not be read"); } });
    const result = selectRootBatch(ready, queued, [edge("old.md", "new.md", 2)]);
    ready[1].size = 999;
    check(result.queued[0] === queued[0] && result.ready[1].size === 10,
        "selection lost original hint provenance or retained mutable materialized input");
    check(result.ready.every(row => !("data" in row)) && result.queued.every(row => !("data" in row)), "selector retained file bytes");
    assertions++; assert.throws(() => (result.selectedGroups[0] as any).push("outsider"), TypeError);
    // The push integration must run this closure BEFORE candidate updates or
    // deletes, including a held upsert if another upsert in its group defers.
    const held = new Set(["new.md"]);
    for (const group of result.selectedGroups) if (group.some(path => held.has(path))) for (const path of group) held.add(path);
    same(paths(result.ready.filter(change => !held.has(change.path))), ["note.md"],
        "selected component membership could not preserve independent work after late source deferral");
    const chain = [hint("a.md", 1), hint("b.md", 2), hint("c.md", 3), hint("note.md", 4)];
    const chainSelection = selectRootBatch(chain, chain, [edge("a.md", "b.md", 1), edge("b.md", "c.md", 2)]);
    const late = new Set(["b.md"]);
    for (const group of chainSelection.selectedGroups) if (group.some(path => late.has(path))) for (const path of group) late.add(path);
    same([...late].sort(), ["a.md", "b.md", "c.md"], "one-pass runtime closure missed a linked upsert");
}

function preservesDirtyOwnerProvenance(): void {
    const dirty = new DirtyPathSet(); dirty.add(hint("a.md"), 1); dirty.add(hint("b.md"), 2);
    const queued = dirty.take(), selection = selectRootBatch(queued, queued, [], { maxPaths: 1 });
    check(selection.queued[0] === queued[0] && selection.retained[0] === queued[1],
        "selected/unselected queued rows were copied away from their owner-local proof");
    dirty.restore(selection.retained);
    check(dirty.commitRetirement(dirty.captureRetirement([{ path: "b.md", throughId: 2 }])) === 1,
        "unselected restoration lost actual durable provenance");
    dirty.restore(selection.queued);
    check(dirty.commitRetirement(dirty.captureRetirement([{ path: "a.md", throughId: 1 }])) === 1,
        "failed selected restoration lost actual durable provenance");
    // A new unjournaled state remains unowned even when restoration carries an
    // older durable watermark. Selection must not manufacture provenance.
    dirty.add(hint("a.md"), 3);
    const old = dirty.take(); dirty.add({ ...hint("a.md"), mtime: 99 }); dirty.restore(old);
    const unowned = dirty.take(), selected = selectRootBatch(unowned, unowned);
    dirty.restore(selected.queued);
    check(dirty.commitRetirement(dirty.captureRetirement([{ path: "a.md", throughId: 3 }])) === 0 && dirty.size === 1,
        "selection converted an inherited journal watermark into ownership of a newer live state");
}

function largeDeterministicBacklog(): void {
    const queued = Array.from({ length: 25_000 }, (_, index) => hint(`notes/${String(index).padStart(5, "0")}.md`, index + 1));
    const ready = [...queued].reverse();
    const first = selectRootBatch(ready, queued), second = selectRootBatch(ready, queued);
    check(first.ready.length === 256 && first.queued.length === 256 && first.retained.length === 24_744,
        "25k backlog selection was not bounded to one transaction");
    same(paths(first.ready), paths(ready.slice(0, 256)), "25k selection changed priority order");
    same(first, second, "same stable snapshot produced nondeterministic selection");
    same(first.selectedGroups, [], "independent backlog invented path-group metadata");
    same(first.deferred["transaction-limit"], { components: 24_744, paths: 24_744 }, "25k unselected remainder lost explicit accounting");
    completePartition(first, queued, ready);
    const oversized = Array.from({ length: 300 }, (_, index) => queued[index]);
    const dependencies = oversized.slice(1).map((change, index) => edge(oversized[index].path, change.path, index + 1));
    const selected = selectRootBatch(queued, queued, dependencies);
    same(paths(selected.ready), paths(queued.slice(300, 556)), "oversized leading dependency chain blocked later independent work");
    same(selected.deferred["component-limit"], { components: 1, paths: 300 }, "large chain was partially admitted");
    completePartition(selected, queued, queued);
}

function invalidInputDoesNotInventWork(): void {
    const a = hint("a.md", 1), b = hint("b.md", 2);
    for (const value of [0, -1, 257, NaN, Infinity, 1.5]) {
        fails(() => selectRootBatch([a], [a], [], { maxPaths: value }), "invalid path limit accepted");
        fails(() => selectRootBatch([a], [a], [], { maxCuts: value }), "invalid cut limit accepted");
    }
    for (const value of [0, -1, ROOT_BATCH_LIMITS.metadataBytes + 1, NaN, Infinity, 1.5]) {
        fails(() => selectRootBatch([a], [a], [], { maxMetadataBytes: value }), "invalid metadata limit accepted");
    }
    fails(() => selectRootBatch([a, a], [a]), "duplicate materialized owner accepted");
    fails(() => selectRootBatch([a], [a, a]), "duplicate queued owner accepted");
    fails(() => selectRootBatch([a, b], [a]), "materialized path with no queued owner accepted");
    fails(() => selectRootBatch([{ ...a, path: "../outside" }], [a]), "unsafe materialized path accepted");
    fails(() => selectRootBatch([a], [{ ...a, journalId: Number.MAX_SAFE_INTEGER }]), "unacknowledgeable journal generation accepted");
    fails(() => selectRootBatch([a], [a], [{ ...edge("a.md", "b.md"), generation: NaN }]), "invalid dependency generation accepted");
    fails(() => selectRootBatch([a], [a], [{ ...edge("a.md", "b.md"), pending: undefined } as any]), "unknown dependency state accepted");
    fails(() => selectRootBatch([{ ...a, size: NaN }], [a]), "invalid metadata size accepted");
    fails(() => selectRootBatch([{ ...a, hash: "not-a-hash" }], [a]), "unbounded/invalid hash hint accepted");
    fails(() => selectRootBatch([a], [{ ...a, data: new Uint8Array(1) } as any]), "queued file data was retained");
    const empty = selectRootBatch([], [], [edge("outside-a", "outside-b")]);
    same(empty.ready, [], "irrelevant known links created ready work");
    check(Object.values(empty.deferred).every(item => item.paths === 0 && item.components === 0), "irrelevant links invented deferred backlog");
}

function run() {
    chainsCyclesAndPriority(); groupsFromSharedIdsAndNewerRenames(); missingAndUncertainPeers();
    countAndMetadataBounds(); estimateCoversActualPublication(); detachedMetadataAndRuntimeDeferralContract(); preservesDirtyOwnerProvenance();
    largeDeterministicBacklog(); invalidInputDoesNotInventWork();
    console.log(`root-batch: ${assertions} assertions passed`);
}
try { run(); } catch (error) { console.error(error); process.exitCode = 1; }
