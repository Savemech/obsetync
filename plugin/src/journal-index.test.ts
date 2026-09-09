import { strict as assert } from "node:assert";
import { JournalIndex, type JournalRecord } from "./journal-index";
import { JournalRecoveryError } from "./journal-format";
import type { JournalEntry } from "./journal";

const normal = (id: number, path = "note.md", action: "created" | "modified" | "deleted" = "modified"): JournalEntry =>
    ({ id, action, path, ts: id, synced: false });
const rename = (id: number, oldPath = "old.md", path = "new.md"): JournalEntry =>
    ({ id, action: "renamed", path, oldPath, ts: id, synced: false });
const ordinary = (entry: JournalEntry): JournalRecord => ({ kind: "entry", entry: entry as any });
const group = (entry: JournalEntry, oldPending = true, newPending = true): JournalRecord =>
    ({ kind: "rename", entry, oldPending, newPending });

function corrupted(work: () => unknown): void {
    assert.throws(work, error => error instanceof JournalRecoveryError && error.code === "CORRUPT");
}

function rebuild(index: JournalIndex): JournalIndex {
    let rebuilt = new JournalIndex();
    for (const record of JSON.parse(JSON.stringify([...index.records()]))) {
        rebuilt = rebuilt.addSnapshotRecord(record);
    }
    assert.deepEqual([...rebuilt.records()], [...index.records()]);
    assert.deepEqual([...rebuilt.entries()], [...index.entries()]);
    return rebuilt;
}

function modificationStormAndGenerationSafety(): void {
    const empty = new JournalIndex();
    let index = empty;
    for (let id = 1; id <= 10_000; id++) {
        index = index.append(normal(id));
        assert.equal(index.size, 1, "normal editor-event history grew instead of coalescing by path");
    }
    assert.equal(empty.size, 0);
    assert.deepEqual([...index.entries()], [normal(10_000)]);
    assert.equal(index.append(normal(100)), index, "older normal ingestion replaced the latest generation");
    assert.equal(index.append(normal(10_000)), index, "identical replay created a different state");
    corrupted(() => index.append(normal(10_000, "note.md", "deleted")));
    assert.equal(index.acknowledge("note.md", 9999), index, "old ACK consumed a newer path state");
    assert.equal(index.acknowledge("absent.md", 10_000), index);
    assert.equal(index.acknowledge("note.md", 10_000).size, 0);
    assert.equal(index.size, 1, "ACK mutated the captured pending root");

    const hash = "ab".repeat(32);
    const hinted = { ...normal(1, "scanned.md"), hash, mtime: 12.5, size: 34 };
    const hintedIndex = new JournalIndex().append(hinted);
    assert.equal(hintedIndex.append({ ...hinted }), hintedIndex,
        "identical durable scan hint did not replay idempotently");
    corrupted(() => hintedIndex.append({ ...hinted, hash: "cd".repeat(32) }));
    corrupted(() => hintedIndex.append(normal(1, "scanned.md")));
}

function linkedGroupsAndPartialSnapshotRoundTrip(): void {
    let index = new JournalIndex().append(rename(1)).append(normal(2, "new.md"));
    assert.equal(index.size, 2, "newer normal destination edit erased a pending rename group");
    assert.deepEqual([...index.entries()], [rename(1), normal(2, "new.md")]);
    index = rebuild(index);
    const oldDone = rebuild(index.acknowledge("old.md", 1));
    assert.deepEqual([...oldDone.entries()], [normal(1, "new.md"), normal(2, "new.md")]);
    assert.deepEqual([...oldDone.records()][0], group(rename(1), false, true));
    assert.equal(oldDone.append(rename(1)), oldDone, "duplicate rename replay resurrected an ACKed source");
    assert.deepEqual([...oldDone.acknowledge("new.md", 1).entries()], [normal(2, "new.md")]);
    assert.equal(oldDone.acknowledge("new.md", 2).size, 0);

    const newDone = rebuild(index.acknowledge("new.md", 2));
    assert(index.hasPath("old.md") && index.hasPath("new.md"));
    assert(newDone.hasPath("old.md") && !newDone.hasPath("new.md"), "path membership resurrected an acknowledged endpoint");
    assert(!oldDone.hasPath("old.md") && oldDone.hasPath("new.md"));
    assert(!index.hasPath("unrelated.md"));
    assert.deepEqual([...newDone.entries()], [normal(1, "old.md", "deleted")],
        "destination ACK erased the unacknowledged source or resurrected destination edits");
    assert.deepEqual([...newDone.records()], [group(rename(1), true, false)]);
    assert.equal(newDone.acknowledge("old.md", 1).size, 0);
    assert.equal(index.size, 2, "partial ACK mutated its original root");

    const chain = new JournalIndex().append(rename(1, "a.md", "b.md"))
        .append(rename(2, "b.md", "c.md")).append(normal(3, "b.md", "created"));
    assert.equal(chain.size, 3, "coalescing flattened a rename chain's logical groups");
    assert.deepEqual([...chain.acknowledge("b.md", 1).records()], [
        group(rename(1, "a.md", "b.md"), true, false),
        group(rename(2, "b.md", "c.md")), ordinary(normal(3, "b.md", "created")),
    ]);
    rebuild(chain.acknowledge("b.md", 2));
}

function renewedPairAndEqualLegacyGenerations(): void {
    let index = new JournalIndex().append(rename(1)).acknowledge("old.md", 1)
        .append(rename(2, "new.md", "old.md")).append(rename(3));
    index = index.acknowledge("new.md", 1);
    assert.deepEqual([...index.records()], [group(rename(2, "new.md", "old.md")), group(rename(3))]);
    assert.equal(index.acknowledge("old.md", 1), index, "old ACK consumed a renewed pair's endpoint");
    const expected = [...index.entries()];
    assert.deepEqual([...rebuild(index).entries()], expected);

    const entries = [normal(7, "Ω/😀.md"), normal(7, "a:b.md"), normal(7, "a\"b.md"), normal(7, "a/b.md"),
        normal(10, "ten.md"), normal(2, "two.md"), normal(9007199254740990, "last.md")];
    const make = (items: JournalEntry[]) => {
        let result = new JournalIndex();
        for (const entry of items) result = result.addSnapshotRecord(ordinary(entry));
        return result;
    };
    const forward = make(entries);
    const reverse = make([...entries].reverse());
    assert.deepEqual([...forward.records()], [...reverse.records()], "equal-ID legacy paths have insertion-dependent order");
    assert.deepEqual([...forward.entries()].map(entry => entry.id), [2, 7, 7, 7, 7, 10, 9007199254740990]);
    assert.equal(forward.acknowledge("a:b.md", 7).size, entries.length - 1,
        "equal legacy IDs linked independent normal paths accidentally");
}

function strictSnapshotValidationAndOwnership(): void {
    const input = rename(1);
    const index = new JournalIndex().append(input);
    input.path = "caller-mutated.md";
    const record = [...index.records()][0];
    assert(Object.isFrozen(record) && Object.isFrozen(record.entry), "retained snapshot values are mutable");
    assert.equal(Reflect.set(record.entry, "path", "escaped.md"), false);
    assert.equal(Reflect.set(record, "oldPending", false), false);
    const projected = [...index.entries()][0];
    projected.oldPath = "projection-mutated.md";
    assert.deepEqual([...index.entries()], [rename(1)], "projection/input aliases escaped into retained state");

    corrupted(() => index.addSnapshotRecord(group(rename(1))));
    corrupted(() => index.addSnapshotRecord(group(rename(1), false, true)));
    corrupted(() => index.append({ ...rename(1), ts: 2 }));
    const ordinaryIndex = new JournalIndex().addSnapshotRecord(ordinary(normal(1)));
    corrupted(() => ordinaryIndex.addSnapshotRecord(ordinary(normal(2))));
    for (const bad of [
        null, [], {}, { kind: "entry", entry: rename(1) },
        { kind: "rename", entry: normal(1), oldPending: true, newPending: true },
        { ...group(rename(1)), oldPending: 1 }, group(rename(1), false, false),
        { ...group(rename(1)), unexpected: true },
        { kind: "entry", entry: normal(1), oldPending: true },
        { kind: "entry", entry: normal(0) },
    ]) corrupted(() => new JournalIndex().addSnapshotRecord(bad));
    assert.throws(() => new JournalIndex().addSnapshotRecord({ kind: "future", entry: normal(1) }),
        error => error instanceof JournalRecoveryError && error.code === "UNKNOWN_SCHEMA");
    for (const [path, through] of [["../escape.md", 1], ["safe.md", 0], ["safe.md", Infinity],
        ["safe.md", Number.MAX_SAFE_INTEGER]] as const) {
        corrupted(() => index.acknowledgeSteps(path, through));
    }
}

async function capturedIteratorsAndIndexedAckSteps(): Promise<void> {
    let index = new JournalIndex();
    for (let id = 1; id <= 500; id++) index = index.append(normal(id, `normal-${id}.md`));
    index = index.append(normal(600, "hot.md"));
    for (let id = 1000; id < 1500; id++) index = index.append(rename(id, `left-${id}.md`, `right-${id}.md`));
    for (let id = 2000; id < 2300; id++) index = index.append(rename(id, "hot.md", `destination-${id}.md`));
    const captured = index;
    const entriesExpected = [...captured.entries()];
    const entries = captured.entries();
    const records = captured.records();
    const first = entries.next();
    await Promise.resolve();
    index = index.append(normal(3000, "new-during-snapshot.md")).acknowledge("normal-1.md", 1);
    await Promise.resolve();
    assert.deepEqual([first.value, ...entries], entriesExpected);
    assert.deepEqual([...records], [...captured.records()], "unstarted captured iterator observed later roots");

    // White-box guards check algorithmic scope, not timing: one path ACK must
    // not enumerate the whole ordered journal or all endpoint buckets.
    const internals = captured as any;
    const originalOrderedEntries = internals.ordered.entries;
    const originalEndpointEntries = internals.groupsByEndpoint.entries;
    internals.ordered.entries = () => { throw new Error("ACK scanned all journal records"); };
    internals.groupsByEndpoint.entries = () => { throw new Error("ACK scanned all endpoint buckets"); };
    let candidate = captured;
    let steps = 0;
    try {
        for (const next of captured.acknowledgeSteps("hot.md", 2049)) {
            candidate = next;
            steps++;
            if (steps % 16 === 0) await Promise.resolve();
        }
    } finally {
        internals.ordered.entries = originalOrderedEntries;
        internals.groupsByEndpoint.entries = originalEndpointEntries;
    }
    assert.equal(steps, 51, "ACK did not stop after one normal and 50 covered path groups");
    assert.equal(candidate.size, captured.size - 1, "half-ACK unexpectedly removed a whole group");
    assert.equal([...candidate.records()].filter(row => row.kind === "rename" && !row.oldPending).length, 50);
    assert.deepEqual([...captured.entries()], entriesExpected, "incremental ACK mutated its captured source root");
    assert.deepEqual([...candidate.records()], [...captured.acknowledge("hot.md", 2049).records()]);
    assert([...index.entries()].some(entry => entry.id === 3000), "independent new root was mutated by an older ACK iterator");
    rebuild(candidate);
}

function recordOrder(left: JournalRecord, right: JournalRecord): number {
    const numeric = left.entry.id - right.entry.id;
    if (numeric) return numeric;
    const tie = (record: JournalRecord) => record.kind === "entry" ? "e:" + JSON.stringify(record.entry.path)
        : "r:" + JSON.stringify([record.entry.oldPath, record.entry.path]);
    return tie(left) < tie(right) ? -1 : tie(left) > tie(right) ? 1 : 0;
}

function randomizedIndependentReference(): void {
    let state = 0x6a6f7572;
    const random = () => { state ^= state << 13; state ^= state >>> 17; state ^= state << 5; return state >>> 0; };
    const normals = new Map<string, JournalRecord>();
    const groups = new Map<number, JournalRecord & { kind: "rename" }>();
    let index = new JournalIndex();
    let nextId = 1;
    const compare = () => {
        const expected = [...normals.values(), ...groups.values()].sort(recordOrder);
        assert.equal(index.size, expected.length);
        assert.deepEqual([...index.records()], expected);
        assert.equal([...index.entries()].length, expected.length);
    };
    for (let step = 0; step < 3000; step++) {
        const path = `path-${random() % 64}.md`;
        const action = random() % 10;
        if (action < 5) {
            const entry = normal(nextId++, path, ["created", "modified", "deleted"][random() % 3] as any);
            index = index.append(entry);
            normals.set(path, ordinary(entry));
        } else if (action < 8) {
            const oldPath = `source-${random() % 32}.md`;
            const entry = rename(nextId++, oldPath, path);
            index = index.append(entry);
            groups.set(entry.id, group(entry) as JournalRecord & { kind: "rename" });
        } else {
            const ackPath = random() % 2 ? path : `source-${random() % 32}.md`;
            const through = 1 + random() % Math.max(1, nextId - 1);
            index = index.acknowledge(ackPath, through);
            if ((normals.get(ackPath)?.entry.id ?? Infinity) <= through) normals.delete(ackPath);
            // Reference deliberately scans independent Maps; production must
            // obtain the same state via its direct path/endpoint indexes.
            for (const [id, previous] of groups) {
                if (id > through) continue;
                const updated = { ...previous, oldPending: previous.oldPending && previous.entry.oldPath !== ackPath,
                    newPending: previous.newPending && previous.entry.path !== ackPath };
                if (!updated.oldPending && !updated.newPending) groups.delete(id);
                else groups.set(id, updated);
            }
        }
        if (step % 23 === 0) compare();
        if (step % 500 === 499) index = rebuild(index);
    }
    compare();
}

void (async () => {
    modificationStormAndGenerationSafety();
    linkedGroupsAndPartialSnapshotRoundTrip();
    renewedPairAndEqualLegacyGenerations();
    strictSnapshotValidationAndOwnership();
    await capturedIteratorsAndIndexedAckSteps();
    randomizedIndependentReference();
    console.log("journal-index.test: 10000 edits, linked/partial ACKs, snapshot validation, indexed ACK steps and 3000 reference operations passed");
})().catch(error => { console.error(error); process.exitCode = 1; });
