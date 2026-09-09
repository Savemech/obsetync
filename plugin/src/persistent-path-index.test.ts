import { strict as assert } from "node:assert";
import { PersistentPathIndex } from "./persistent-path-index";

/** Test-only inspection checks AVL structure and structural sharing without
 * adding a production introspection API or relying on timing thresholds. */
interface InspectNode<V> {
    readonly key: string;
    readonly value: V;
    readonly left: InspectNode<V> | null;
    readonly right: InspectNode<V> | null;
    readonly height: number;
    readonly size: number;
}
const rootOf = <V>(index: PersistentPathIndex<V>) =>
    (index as unknown as { root: InspectNode<V> | null }).root;

function inspect<V>(root: InspectNode<V> | null, lower?: string, upper?: string): { height: number; size: number } {
    if (!root) return { height: 0, size: 0 };
    assert.ok(lower === undefined || lower < root.key, "left/right ordering is invalid");
    assert.ok(upper === undefined || root.key < upper, "left/right ordering is invalid");
    const left = inspect(root.left, lower, root.key);
    const right = inspect(root.right, root.key, upper);
    assert.ok(Math.abs(left.height - right.height) <= 1, "AVL balance exceeded one level");
    assert.equal(root.height, 1 + Math.max(left.height, right.height));
    assert.equal(root.size, left.size + right.size + 1);
    return { height: root.height, size: root.size };
}

function ordered<V>(reference: ReadonlyMap<string, V>): Array<[string, V]> {
    return [...reference].sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0);
}

function verify<V>(index: PersistentPathIndex<V>, reference: ReadonlyMap<string, V>): void {
    assert.equal(index.size, reference.size);
    assert.deepEqual([...index.entries()], ordered(reference));
    assert.deepEqual([...index.keys()], ordered(reference).map(([key]) => key));
    assert.equal(inspect(rootOf(index)).size, reference.size);
    for (const [key, value] of reference) assert.equal(index.get(key), value);
}

function collect<V>(root: InspectNode<V> | null, into = new Set<InspectNode<V>>()): Set<InspectNode<V>> {
    if (root) { into.add(root); collect(root.left, into); collect(root.right, into); }
    return into;
}

function basicsAndRotations(): void {
    const empty = new PersistentPathIndex<number | undefined>();
    assert.equal(empty.size, 0);
    assert.equal(empty.get("missing"), undefined);
    assert.equal(empty.delete("missing"), empty);
    let index = empty;
    const reference = new Map<string, number | undefined>();
    for (const [position, key] of ["", "__proto__", "constructor", "toString", "Z", "a", "é", "Ω", "😀"].entries()) {
        reference.set(key, position);
        index = index.set(key, position);
    }
    index = index.set("undefined-value", undefined);
    reference.set("undefined-value", undefined);
    verify(index, reference);
    assert.equal(index.set("undefined-value", undefined), index);
    assert.equal(index.set("a", index.get("a")), index, "same-value overwrite copied a path unnecessarily");
    const overwritten = index.set("a", 999);
    assert.equal(overwritten.size, index.size);
    assert.notEqual(index.get("a"), 999);
    reference.set("a", 999);
    verify(overwritten, reference);

    for (const sequence of [["c", "b", "a"], ["a", "b", "c"], ["c", "a", "b"], ["a", "c", "b"]]) {
        let tree = new PersistentPathIndex<string>();
        for (const key of sequence) tree = tree.set(key, key);
        assert.equal(rootOf(tree)?.key, "b", "single/double rotation selected the wrong root");
        verify(tree, new Map(sequence.map(key => [key, key])));
    }
}

function orderedInsertionDeletionAndSharing(): void {
    let index = new PersistentPathIndex<number>();
    const reference = new Map<string, number>();
    for (let i = 0; i < 2048; i++) {
        const key = String(i).padStart(5, "0");
        index = index.set(key, i); reference.set(key, i);
    }
    verify(index, reference);
    assert.ok(rootOf(index)!.height <= Math.ceil(1.45 * Math.log2(index.size + 2)));
    const captured = index;
    const capturedEntries = [...captured.entries()];
    const previousNodes = collect(rootOf(index));
    const changed = index.set("01000", -1);
    const created = [...collect(rootOf(changed))].filter(candidate => !previousNodes.has(candidate)).length;
    assert.ok(created <= rootOf(index)!.height, "overwrite copied more than its search path");
    assert.ok(created < index.size / 16, "single update copied the whole tree");
    assert.deepEqual([...captured.entries()], capturedEntries);

    // Odd removals create holes; descending evens exercise two-child removals,
    // deletion rebalance cascades, root replacement and the empty transition.
    const removalOrder = [
        ...Array.from({ length: 1024 }, (_, i) => i * 2 + 1),
        ...Array.from({ length: 1024 }, (_, i) => 2046 - i * 2),
    ];
    for (let position = 0; position < removalOrder.length; position++) {
        const key = String(removalOrder[position]).padStart(5, "0");
        const before = index;
        index = index.delete(key); reference.delete(key);
        assert.equal(before.size, index.size + 1);
        if (position % 31 === 0) verify(index, reference);
    }
    verify(index, reference);
    assert.equal(index.size, 0);
    assert.deepEqual([...captured.entries()], capturedEntries, "deletes mutated an older root");
}

async function randomizedReferenceAndCapturedIteration(): Promise<void> {
    let state = 0x5eeda11;
    const random = () => {
        state ^= state << 13; state ^= state >>> 17; state ^= state << 5;
        return state >>> 0;
    };
    let index = new PersistentPathIndex<Readonly<{ value: number }>>();
    const reference = new Map<string, Readonly<{ value: number }>>();
    const snapshots: Array<{ index: typeof index; expected: Array<[string, Readonly<{ value: number }>]> }> = [];
    for (let operation = 0; operation < 5000; operation++) {
        const key = `folder/${String(random() % 1024).padStart(4, "0")}.md`;
        const previous = index;
        if (random() % 3 !== 0) {
            const value = Object.freeze({ value: random() });
            index = index.set(key, value); reference.set(key, value);
        } else {
            index = index.delete(key); reference.delete(key);
        }
        assert.equal(index.size, reference.size);
        assert.equal(index.get(key), reference.get(key));
        if (operation % 25 === 0) {
            verify(index, reference);
            const beforeNodes = collect(rootOf(previous));
            const created = [...collect(rootOf(index))].filter(candidate => !beforeNodes.has(candidate)).length;
            assert.ok(created <= 3 * (rootOf(previous)?.height ?? 0) + 5,
                "update allocated more than a logarithmic path and rotations");
        }
        if (operation % 1000 === 999) snapshots.push({ index, expected: ordered(reference) });
    }
    verify(index, reference);
    for (const snapshot of snapshots) {
        assert.deepEqual([...snapshot.index.entries()], snapshot.expected, "later updates mutated a captured root");
        inspect(rootOf(snapshot.index));
    }

    const expected = [...index.entries()];
    const iterator = index.entries();
    const first = iterator.next();
    assert.equal(first.done, false);
    await Promise.resolve();
    index = index.delete(first.value![0]).set("new-during-snapshot.md", Object.freeze({ value: 1 }));
    await Promise.resolve();
    const remainder = [...iterator];
    assert.deepEqual([first.value, ...remainder], expected,
        "in-progress snapshot iterator observed later mutations across await");
    assert.ok(!remainder.some(([key]) => key === "new-during-snapshot.md"));
}

async function run(): Promise<void> {
    basicsAndRotations();
    orderedInsertionDeletionAndSharing();
    await randomizedReferenceAndCapturedIteration();
    console.log("persistent-path-index.test: AVL rotations/deletes, sharing, 5000 reference operations and captured iteration passed");
}

void run().catch(error => { console.error(error); process.exitCode = 1; });
