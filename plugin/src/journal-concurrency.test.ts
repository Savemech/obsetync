import { strict as assert } from "node:assert";
import { ObsetyncJournal, JOURNAL_STORE_PATH } from "./journal";
import type { JournalIndex } from "./journal-index";
import { MemorySegmentedIO } from "./segmented-store-test-io";

let assertions = 0;
const check = (condition: unknown, message: string): void => {
    assertions++;
    assert.ok(condition, message);
};
const same = (actual: unknown, expected: unknown, message: string): void => {
    assertions++;
    assert.deepEqual(actual, expected, message);
};
function deferred() {
    let resolve!: () => void;
    const promise = new Promise<void>(done => { resolve = done; });
    return { promise, resolve };
}

/** Actual journal/index/store with synthetic in-memory IO. The observation
 * wrapper below counts original iterator results; it neither implements ACKs
 * nor replaces scheduling, persistence, or the wrapper's request queue. */
async function hotEndpointAcknowledgementKeepsLaterAppend(): Promise<void> {
    const adapter = new MemorySegmentedIO();
    const app = { vault: { adapter } } as any;
    const journal = new ObsetyncJournal(app);
    await journal.load();
    const groupCount = 300;
    const ids = await Promise.all(Array.from({ length: groupCount }, (_, index) => journal.appendGroup([
        { action: "deleted", path: "hot.md", ts: index, synced: false },
        { action: "created", path: `destination-${index}.md`, ts: index, synced: false },
    ])));
    same(ids, Array.from({ length: groupCount }, (_, index) => [index + 1, index + 1]),
        "linked group fixture did not retain monotonic shared endpoint generations");

    const throughId = ids.at(-1)![0];
    const headPath = `${JOURNAL_STORE_PATH}/head.json`;
    const headBefore = adapter.files.get(headPath);
    const eventsBefore = adapter.events.length;
    const oldPaths = journal.capturePendingPaths();
    const captured = (journal as unknown as { index: JournalIndex }).index;
    const originalSteps = captured.acknowledgeSteps;
    const reachedCooperativeBoundary = deferred();
    let steps = 0;
    captured.acknowledgeSteps = function* (path, cut) {
        for (const candidate of originalSteps.call(captured, path, cut)) {
            if (++steps === 256) reachedCooperativeBoundary.resolve();
            yield candidate;
        }
    };

    let ackSettled = false;
    const acknowledgement = journal.acknowledge([{ path: "hot.md", throughId }])
        .then(() => { ackSettled = true; });
    let lateAppend: Promise<number> | undefined;
    try {
        await reachedCooperativeBoundary.promise;
        // A missing cooperative yield would run all 300 iterator steps before
        // this microtask resumes, even if later storage IO happened to yield.
        check(steps === 256 && !ackSettled, "large endpoint ACK did not yield inside its candidate walk");
        check(adapter.events.length === eventsBefore && adapter.files.get(headPath) === headBefore,
            "ACK publication began before the candidate's cooperative boundary");
        check(journal.unsyncedCount() === groupCount && journal.unsynced().every(row => row.action === "renamed"),
            "provisional half-ACK candidates escaped into the public journal before verified publication");
        check(oldPaths.has("hot.md"), "in-flight candidate changed an existing durable path guard");

        lateAppend = journal.append({ action: "modified", path: "hot.md", ts: groupCount + 1, synced: false });
        await acknowledgement;
        const newestId = await lateAppend;
        check(newestId === throughId + 1, "append arriving during ACK yield reused or skipped a generation");
        const expected = [
            ...Array.from({ length: groupCount }, (_, index) => ({
                path: `destination-${index}.md`, id: index + 1, action: "modified",
            })),
            { path: "hot.md", id: newestId, action: "modified" },
        ];
        const projected = (value: ObsetyncJournal) => value.unsynced()
            .map(({ path, id, action }) => ({ path, id, action }));
        same(projected(journal), expected,
            "ACK consumed the later hot-path append or lost an unacknowledged destination half");
        check(journal.unsynced().every(row => row.oldPath === undefined),
            "half-ACK projection resurrected linked source endpoints");

        await journal.compact();
        const restored = new ObsetyncJournal(app);
        await restored.load();
        same(projected(restored), expected, "compaction/reload changed partial rename state or the newer append");
        await restored.acknowledge([{ path: "hot.md", throughId }]);
        same(projected(restored), expected, "replayed old ACK consumed the newer hot-path generation");
        const nextId = await restored.append({ action: "modified", path: "after-reload.md", ts: groupCount + 2, synced: false });
        check(nextId === newestId + 1, "snapshot/reload did not preserve the issued mutation watermark");
    } finally {
        captured.acknowledgeSteps = originalSteps;
        await Promise.allSettled([acknowledgement, ...(lateAppend ? [lateAppend] : [])]);
    }
}

const watchdog = setTimeout(() => { throw new Error("journal concurrency test did not settle"); }, 10_000);
void hotEndpointAcknowledgementKeepsLaterAppend()
    .then(() => console.log(`journal-concurrency.test: ${assertions} assertions passed`))
    .catch(error => { console.error(error); process.exitCode = 1; })
    .finally(() => clearTimeout(watchdog));
