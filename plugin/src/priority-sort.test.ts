import { strict as assert } from "node:assert";
import type { FileChange } from "./push";
import type { SyncPriority } from "./settings";
import { PRIORITY_SORT_MAX_CHANGES, PRIORITY_SORT_WORK_UNITS, PrioritySortError, sortByPriority,
    sortByPriorityCooperatively } from "./priority-sort";

type Tagged = FileChange & { tag: number };
const row = (tag: number, path: string, mtime?: number, size?: number): Tagged => ({
    tag, path, action: "modified", ...(mtime === undefined ? {} : { mtime }),
    ...(size === undefined ? {} : { size }),
});
const tags = (values: readonly FileChange[]) => values.map(item => (item as Tagged).tag);

async function deterministicModesMatchStableCompatibilityOrder(): Promise<void> {
    const priorities: SyncPriority[] = ["oldest", "newest", "smallest", "biggest", "alphabetic"];
    const input = Array.from({ length: 600 }, (_, index) => row(index,
        [`z/${String(599 - index).padStart(4, "0")}.md`, "ä.md", "Å.md", "a.md", "á.md"][index % 5],
        index % 7 === 0 ? undefined : index % 11, index % 9 === 0 ? undefined : index % 13));
    input[0].mtime = -Number.MAX_VALUE; input[1].mtime = Number.MAX_VALUE;
    input[0].size = 0; input[1].size = Number.MAX_VALUE;
    const original = [...input], references = new Set<FileChange>(input);
    const expectedYields = Math.floor(input.length * (1 + Math.ceil(Math.log2(input.length))) /
        PRIORITY_SORT_WORK_UNITS);
    for (const priority of priorities) {
        let yields = 0;
        const expected = sortByPriority(input, priority);
        const actual = await sortByPriorityCooperatively(input, priority, { cooperate: async () => { yields++; } });
        assert.deepEqual(tags(actual), tags(expected), `${priority} parity drifted`);
        assert.notEqual(actual, input, `${priority} returned the caller array`);
        assert.ok(actual.every(item => references.has(item)), `${priority} cloned caller rows`);
        assert.equal(yields, expectedYields, `${priority} did not account for every copy/merge/tail row`);
        assert.deepEqual(input, original, `${priority} mutated caller order`);
    }

    const ties = [row(1, "same.md", 7, 9), row(2, "same.md", 7, 9), row(3, "same.md", 7, 9)];
    for (const priority of priorities) {
        assert.deepEqual(tags(await sortByPriorityCooperatively(ties, priority,
            { cooperate: async () => {} })), [1, 2, 3], `${priority} was not stable on ties`);
    }
    let smallYields = 0;
    await sortByPriorityCooperatively(ties.slice(0, 2), "oldest", { cooperate: async () => { smallYields++; } });
    assert.equal(smallYields, 0, "small deterministic sort paid an unnecessary host wait");

    const empty: FileChange[] = [], single = [row(7, "μονο-📝.md")];
    const emptyResult = await sortByPriorityCooperatively(empty, "alphabetic", { cooperate: async () => {} });
    const singleResult = await sortByPriorityCooperatively(single, "newest", { cooperate: async () => {} });
    assert.deepEqual(emptyResult, []); assert.notEqual(emptyResult, empty);
    assert.deepEqual(singleResult, single); assert.notEqual(singleResult, single);
    const unicode = ["Ω.md", "😀.md", "e\u0301.md", "é.md", "Z.md", "z.md"].map((path, index) => row(index, path));
    assert.deepEqual(tags(await sortByPriorityCooperatively(unicode, "alphabetic", { cooperate: async () => {} })),
        tags(sortByPriority(unicode, "alphabetic")), "Unicode localeCompare parity drifted");
}

async function largeDuplicateHeavyCorpusIsBoundedAndStable(): Promise<void> {
    const count = 25_000;
    const input = Array.from({ length: count }, (_, index) =>
        row(index, `large/${String(count - index).padStart(5, "0")}.md`, index % 17, index % 31));
    let yields = 0;
    const result = await sortByPriorityCooperatively(input, "oldest", { cooperate: async () => { yields++; } });
    const expected = sortByPriority(input, "oldest");
    assert.deepEqual(tags(result), tags(expected));
    assert.equal(new Set(result).size, count); assert.equal(result.length, count);
    assert.equal(yields, Math.floor(count * (1 + Math.ceil(Math.log2(count))) / PRIORITY_SORT_WORK_UNITS));
}

async function sequentialAndRandomStayBounded(): Promise<void> {
    const input = [row(1, "c.md"), row(2, "b.md"), row(3, "a.md")];
    let yields = 0;
    assert.equal(await sortByPriorityCooperatively(input, "sequential",
        { cooperate: async () => { yields++; } }), input, "sequential lost its no-copy contract");
    assert.equal(yields, 0);

    const nativeRandom = Math.random;
    const seeded = () => { let state = 0x12345678; return () => {
        state ^= state << 13; state ^= state >>> 17; state ^= state << 5;
        return (state >>> 0) / 0x1_0000_0000;
    }; };
    const referenceShuffle = (values: FileChange[], random: () => number): FileChange[] => {
        const result = [...values];
        for (let index = result.length - 1; index > 0; index--) {
            const selected = Math.floor(random() * (index + 1));
            [result[index], result[selected]] = [result[selected], result[index]];
        }
        return result;
    };
    try {
        const expected = referenceShuffle(input, seeded());
        Math.random = seeded(); const actual = await sortByPriorityCooperatively(input, "random",
            { cooperate: async () => { yields++; } });
        assert.deepEqual(tags(actual), tags(expected));
        assert.notEqual(actual, input); assert.equal(yields, 0);
        assert.deepEqual(tags(input), [1, 2, 3], "random priority mutated caller order");

        const hostile = [row(4, "d.md"), row(5, "e.md")]; let iteratorUsed = false;
        Object.defineProperty(hostile, Symbol.iterator, { value: function* () {
            iteratorUsed = true;
            yield row(6, "injected-1.md"); yield row(7, "injected-2.md"); yield row(8, "injected-3.md");
        } });
        Math.random = seeded();
        const bounded = await sortByPriorityCooperatively(hostile, "random", { cooperate: async () => {} });
        assert.equal(iteratorUsed, false, "random cooperative copy trusted a caller iterator after admission");
        assert.equal(bounded.length, 2); assert.deepEqual(new Set(bounded), new Set([hostile[0], hostile[1]]));

        const count = 25_000, large = Array.from({ length: count }, (_, index) => row(index, `random-${index}.md`));
        let draws = 0; yields = 0;
        const random = seeded(); Math.random = () => { draws++; return random(); };
        const shuffled = await sortByPriorityCooperatively(large, "random", { cooperate: async () => { yields++; } });
        assert.equal(draws, count - 1, "Fisher-Yates did not consume one draw per swap");
        assert.equal(yields, Math.floor((count * 2 - 1) / PRIORITY_SORT_WORK_UNITS),
            "random copy/swap work was not cooperatively bounded");
        assert.equal(shuffled.length, count); assert.equal(new Set(shuffled).size, count);
        assert.deepEqual(new Set(shuffled), new Set(large), "random priority lost or invented rows");

        for (const invalid of [-Number.EPSILON, 1, Number.NaN, Number.POSITIVE_INFINITY]) {
            Math.random = () => invalid;
            await assert.rejects(sortByPriorityCooperatively(input, "random", { cooperate: async () => {} }),
                (error: unknown) => error instanceof PrioritySortError && error.code === "PRIORITY_SORT_RANDOM");
            assert.deepEqual(tags(input), [1, 2, 3], "invalid random sample mutated caller order");
        }
        const randomFailure = new Error("random source failed"); Math.random = () => { throw randomFailure; };
        await assert.rejects(sortByPriorityCooperatively(input, "random", { cooperate: async () => {} }),
            error => error === randomFailure);

        for (const count of [0, 1, 128, 129]) {
            let boundaryDraws = 0, boundaryYields = 0; Math.random = () => { boundaryDraws++; return 0; };
            const boundary = Array.from({ length: count }, (_, index) => row(index, `boundary-${index}.md`));
            const result = await sortByPriorityCooperatively(boundary, "random",
                { cooperate: async () => { boundaryYields++; } });
            assert.equal(result.length, count); assert.equal(boundaryDraws, Math.max(0, count - 1));
            assert.equal(boundaryYields, Math.floor(Math.max(0, count * 2 - 1) / PRIORITY_SORT_WORK_UNITS));
        }
        for (const valid of [0, 1 - Number.EPSILON]) {
            Math.random = () => valid;
            assert.deepEqual(new Set(await sortByPriorityCooperatively(input, "random", { cooperate: async () => {} })),
                new Set(input), `valid boundary sample ${valid} did not produce a permutation`);
        }

        const permutations = new Set<string>();
        for (let atFour = 0; atFour < 4; atFour++) for (let atThree = 0; atThree < 3; atThree++) {
            for (let atTwo = 0; atTwo < 2; atTwo++) {
                const choices = [atFour, atThree, atTwo]; let draw = 0;
                Math.random = () => (choices[draw] + 0.5) / (4 - draw++);
                permutations.add(tags(await sortByPriorityCooperatively(
                    [row(0, "0.md"), row(1, "1.md"), row(2, "2.md"), row(3, "3.md")],
                    "random", { cooperate: async () => {} })).join(","));
                assert.equal(draw, 3);
            }
        }
        assert.equal(permutations.size, 24, "Fisher-Yates choices did not cover every four-row permutation");

        const captured = Array.from({ length: 300 }, (_, index) => row(index, `captured-${index}.md`));
        let capturedDraws = 0, capturedYields = 0;
        Math.random = () => { capturedDraws++; return 0.5; };
        await sortByPriorityCooperatively(captured, "random", { cooperate: async () => {
            if (++capturedYields === 1) Math.random = () => { throw new Error("replacement RNG used"); };
        } });
        assert.equal(capturedDraws, 299, "shuffle did not retain its captured RNG");
        assert.equal(capturedYields, 2);
    } finally { Math.random = nativeRandom; }
}

async function mutationAbortFailureAndAdmissionFailClosed(): Promise<void> {
    const make = (count = 300) => Array.from({ length: count }, (_, index) =>
        row(index, `note-${String(count - index).padStart(4, "0")}.md`, index, count - index));
    const changed = make(); let mutated = false;
    await assert.rejects(sortByPriorityCooperatively(changed, "oldest", { cooperate: async () => {
        if (!mutated) { mutated = true; changed.push(row(999, "late.md")); }
    } }), (error: unknown) => error instanceof PrioritySortError && error.code === "PRIORITY_SORT_CHANGED");
    assert.equal(mutated, true);

    const before = new AbortController(); before.abort(new Error("before priority sort"));
    await assert.rejects(sortByPriorityCooperatively(make(), "oldest",
        { cooperate: async () => {}, signal: before.signal }), /before priority sort/);
    const during = new AbortController();
    await assert.rejects(sortByPriorityCooperatively(make(), "newest", {
        cooperate: async () => { during.abort(new Error("during priority sort")); }, signal: during.signal,
    }), /during priority sort/);
    const duringShuffle = new AbortController(); let shuffleYields = 0;
    await assert.rejects(sortByPriorityCooperatively(make(), "random", {
        cooperate: async () => {
            if (++shuffleYields === 2) duringShuffle.abort(new Error("during priority shuffle"));
        }, signal: duringShuffle.signal,
    }), /during priority shuffle/);
    assert.equal(shuffleYields, 2, "random abort did not reach the shuffle phase");
    const failure = new Error("priority host yield failed");
    await assert.rejects(sortByPriorityCooperatively(make(), "smallest",
        { cooperate: async () => { throw failure; } }), error => error === failure);

    assert.equal((await sortByPriorityCooperatively(make(2), "biggest",
        { cooperate: async () => {}, maxChanges: 2 })).length, 2);
    await assert.rejects(sortByPriorityCooperatively(make(3), "alphabetic",
        { cooperate: async () => {}, maxChanges: 2 }),
    (error: unknown) => error instanceof PrioritySortError && error.code === "PRIORITY_SORT_ADMISSION");
    for (const maxChanges of [0, -1, 1.5, PRIORITY_SORT_MAX_CHANGES + 1]) {
        await assert.rejects(sortByPriorityCooperatively(make(1), "oldest", { cooperate: async () => {}, maxChanges }),
            (error: unknown) => error instanceof PrioritySortError && error.code === "PRIORITY_SORT_ADMISSION");
    }
}

let completed = false;
process.once("beforeExit", () => {
    if (!completed && !process.exitCode) { console.error("priority-sort suite did not finish"); process.exitCode = 1; }
});
async function run(): Promise<void> {
    await deterministicModesMatchStableCompatibilityOrder();
    await largeDuplicateHeavyCorpusIsBoundedAndStable();
    await sequentialAndRandomStayBounded();
    await mutationAbortFailureAndAdmissionFailClosed();
    completed = true;
    console.log("priority-sort.test: stable deterministic parity, cooperative Fisher-Yates and gates passed");
}
void run().catch(error => { console.error(error); process.exitCode = 1; });
