import { strict as assert } from "node:assert";
import { adaptiveBatches } from "./adaptive-batches";
import { planByteBoundedBatches, type BatchLimits } from "./hash-runtime";

const defaults: BatchLimits = { maxFiles: 4, maxBytes: 10, maxSingleBytes: 8 };

function stableLimitsMatchExistingGrouping(): void {
    const cases = [
        [], [0], [1, 2, 3, 4, 5], [4, 4, 4, 20, 1, 1, 1, 1],
        [0, 0, 0, 0, 0, 0], [8, 0, 9, 10, 11, 0, 1, 0],
    ];
    for (const items of cases) {
        const actual = [...adaptiveBatches(items, (size) => size, () => defaults, () => 0)];
        assert.deepEqual(actual, planByteBoundedBatches(items, (size) => size, defaults, () => 0));
        assert.deepEqual(actual.flat(), items);
        for (const batch of actual) {
            assert.ok(batch.length > 0 && batch.length <= defaults.maxFiles);
            const bytes = batch.reduce((sum, size) => sum + size, 0);
            assert.ok(bytes <= defaults.maxBytes || batch.length === 1);
            assert.ok(batch.length === 1 || batch.every((size) => size <= defaults.maxSingleBytes));
        }
    }
}

function limitsRefreshOnlyWhenNextBatchIsRequested(): void {
    let limits = { maxFiles: 4, maxBytes: 12, maxSingleBytes: 8 };
    let snapshots = 0;
    const items = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    const iterator = adaptiveBatches(items, () => 3, () => { snapshots++; return limits; }, () => 0);
    assert.equal(snapshots, 0, "constructing the iterator must not traverse or plan");
    const first = iterator.next().value;
    assert.deepEqual(first, [0, 1, 2, 3]);
    assert.equal(snapshots, 1);
    limits = { maxFiles: 1, maxBytes: 3, maxSingleBytes: 3 };
    const second = iterator.next().value;
    assert.deepEqual(second, [4]);
    limits = { maxFiles: 10, maxBytes: 30, maxSingleBytes: 8 };
    const third = iterator.next().value;
    assert.deepEqual(third, [5, 6, 7, 8, 9]);
    assert.equal(iterator.next().done, true);
    assert.equal(snapshots, 3);
    assert.deepEqual([first, second, third].flat(), items);
}

function pendingLookaheadUsesNewLimitsAndHoldClock(): void {
    let limits: BatchLimits = { maxFiles: 10, maxBytes: 5, maxSingleBytes: 5 };
    const inspected: Array<{ item: number; maxBytes: number }> = [];
    const iterator = adaptiveBatches([3, 4, 5], (item, current) => {
        inspected.push({ item, maxBytes: current.maxBytes });
        return item;
    }, () => limits, () => 0);
    assert.deepEqual(iterator.next().value, [3]);
    limits = { maxFiles: 10, maxBytes: 9, maxSingleBytes: 9 };
    assert.deepEqual(iterator.next().value, [4, 5]);
    assert.equal(iterator.next().done, true);
    assert.deepEqual(inspected, [
        { item: 3, maxBytes: 5 }, { item: 4, maxBytes: 5 },
        { item: 4, maxBytes: 9 }, { item: 5, maxBytes: 9 },
    ]);

    const observations = [0, 3, 11, 100, 103];
    const held = adaptiveBatches([1, 2, 3, 4], (item) => item, () => ({
        maxFiles: 10, maxBytes: 100, maxSingleBytes: 100, maxHoldMs: 10,
    }), () => observations.shift()!);
    assert.deepEqual(held.next().value, [1, 2]);
    // Consumer work took 89 ms. That delay must not age the unstarted batch.
    assert.deepEqual(held.next().value, [3, 4]);
    assert.equal(held.next().done, true);
    assert.equal(observations.length, 0);
}

function oversizeAndUnknownRemainSingletonsAsLimitsChange(): void {
    const items = [1, undefined, 20, 2, 3];
    let limits = { maxFiles: 4, maxBytes: 10, maxSingleBytes: 8 };
    const iterator = adaptiveBatches(items, (size, current) =>
        size === undefined ? current.maxSingleBytes + 1 : size,
    () => limits, () => 0);
    assert.deepEqual(iterator.next().value, [1]);
    limits = { maxFiles: 10, maxBytes: 100, maxSingleBytes: 50 };
    assert.deepEqual(iterator.next().value, [undefined], "unknown-size marker was stale after growth");
    limits = { maxFiles: 2, maxBytes: 10, maxSingleBytes: 8 };
    assert.deepEqual(iterator.next().value, [20]);
    assert.deepEqual(iterator.next().value, [2, 3]);
    assert.equal(iterator.next().done, true);
}

function earlyBreakDoesNotVisitOrCopyTheRemainingTail(): void {
    let visited = 0;
    const items = new Proxy(Array.from({ length: 100_000 }, (_, index) => index), {
        get(target, property, receiver) {
            if (property === "slice") throw new Error("must not copy the remaining tail");
            if (typeof property === "string" && /^\d+$/.test(property)) visited++;
            return Reflect.get(target, property, receiver);
        },
    });
    for (const batch of adaptiveBatches(items, () => 1, () => ({
        maxFiles: 3, maxBytes: 100, maxSingleBytes: 100,
    }), () => 0)) {
        assert.deepEqual(batch, [0, 1, 2]);
        break;
    }
    assert.equal(visited, 3, "early break traversed unseen input");
    let noWork = 0;
    assert.deepEqual([...adaptiveBatches([], () => { noWork++; return 0; }, () => {
        noWork++;
        return defaults;
    })], []);
    assert.equal(noWork, 0, "empty iteration should not start planning");
}

function invalidLimitsAndSizesFailAtTheirBatch(): void {
    const invalid: Partial<BatchLimits>[] = [
        { maxFiles: 0 }, { maxFiles: -1 }, { maxFiles: 1.5 }, { maxFiles: Infinity },
        { maxBytes: 0 }, { maxBytes: -1 }, { maxBytes: NaN }, { maxBytes: Infinity },
        { maxSingleBytes: 0 }, { maxSingleBytes: NaN }, { maxSingleBytes: Infinity },
        { maxHoldMs: 0 }, { maxHoldMs: -1 }, { maxHoldMs: NaN }, { maxHoldMs: Infinity },
    ];
    for (const bad of invalid) {
        assert.throws(() => adaptiveBatches([1], (size) => size, () => ({ ...defaults, ...bad })).next(), RangeError);
    }
    for (const size of [-1, NaN, Infinity, -Infinity]) {
        assert.throws(() => adaptiveBatches([size], (value) => value, () => defaults).next(), RangeError);
    }
    let limits: BatchLimits = { ...defaults, maxFiles: 1 };
    const iterator = adaptiveBatches([1, 2], (size) => size, () => limits);
    assert.deepEqual(iterator.next().value, [1]);
    limits = { ...limits, maxBytes: 0 };
    assert.throws(() => iterator.next(), RangeError);

    const reused = { ...defaults, maxFiles: 2 };
    const snapshot = adaptiveBatches([1, 2, 3], (size) => {
        reused.maxFiles = 1;
        return size;
    }, () => reused, () => 0);
    assert.deepEqual(snapshot.next().value, [1, 2], "mutable source changed limits inside a batch");
    assert.deepEqual(snapshot.next().value, [3]);
}

stableLimitsMatchExistingGrouping();
limitsRefreshOnlyWhenNextBatchIsRequested();
pendingLookaheadUsesNewLimitsAndHoldClock();
oversizeAndUnknownRemainSingletonsAsLimitsChange();
earlyBreakDoesNotVisitOrCopyTheRemainingTail();
invalidLimitsAndSizesFailAtTheirBatch();
console.log("adaptive-batches.test: lazy bounded grouping, adaptive limits and validation passed");
