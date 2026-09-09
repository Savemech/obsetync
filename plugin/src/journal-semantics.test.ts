import { TFile } from "obsidian";
import { ObsetyncSyncEngine } from "./sync";
import { ObsetyncJournal, type JournalEntry } from "./journal";
import { DirtyPathSet, type DirtyFileChange } from "./dirty-set";
import { DeferredChangeTracker } from "./deferred-changes";
import { LocalEventGuards } from "./local-event-guards";
import { MemorySegmentedIO } from "./segmented-store-test-io";

/** Semantic migration tests deliberately use actual callback/recovery methods.
 * Full segmented-storage IO is synthetic; no real vault, transport or hashing
 * runs. Deliberately no legacy append(): production must use immutable pages. */

let assertions = 0;
function check(condition: unknown, message: string): asserts condition {
    assertions++;
    if (!condition) throw new Error(message);
}

const fileAt = (path: string): TFile => Object.assign(new TFile(), {
    path, stat: { size: 3, mtime: 1 },
});

async function fixture() {
    const adapter = new MemorySegmentedIO();
    const callbacks = new Map<string, (...args: any[]) => Promise<void>>();
    const app = { vault: { adapter, on: (event: string, callback: (...args: any[]) => Promise<void>) => {
        callbacks.set(event, callback);
        return {};
    } } };
    const journal = new ObsetyncJournal(app as any);
    await journal.load();
    const engine = Object.create(ObsetyncSyncEngine.prototype) as any;
    Object.assign(engine, {
        app, journal, eventRefs: [], autoSync: false,
        pendingChanges: new DirtyPathSet(), deferredChanges: new DeferredChangeTracker(),
        localEventsInFlight: new LocalEventGuards(),
        pullEchoes: { expectsRename: () => false, expectsUpsert: () => false },
        isExcluded: () => false,
    });
    const testGlobal = globalThis as any;
    const previous = testGlobal.__obsetyncTestDebounce;
    try {
        testGlobal.__obsetyncTestDebounce = () => () => {};
        engine.attachVaultListeners();
    } finally {
        if (previous === undefined) delete testGlobal.__obsetyncTestDebounce;
        else testGlobal.__obsetyncTestDebounce = previous;
    }
    return { app, adapter, callbacks, journal, engine };
}

async function recover(f: Awaited<ReturnType<typeof fixture>>) {
    const journal = new ObsetyncJournal(f.app as any);
    await journal.load();
    const engine = Object.create(ObsetyncSyncEngine.prototype) as any;
    Object.assign(engine, {
        journal, pendingChanges: new DirtyPathSet(), deferredChanges: new DeferredChangeTracker(),
        hashWorkerAbort: new AbortController(),
        onStatusUpdate: () => {}, progressHeartbeat: () => {},
    });
    await engine.recoverFromJournal();
    return { journal, engine, queued: engine.pendingChanges.take() as DirtyFileChange[] };
}

async function appendRename(journal: ObsetyncJournal, oldPath = "old.md", path = "new.md") {
    const ids = await journal.appendGroup([
        { action: "deleted", path: oldPath, ts: 1, synced: false },
        { action: "created", path, ts: 1, synced: false },
    ]);
    check(ids.length === 2 && ids[0] === ids[1], "one logical rename must retain one shared generation");
    return ids[0];
}

async function destinationEchoCannotErasePendingSource(): Promise<void> {
    const f = await fixture();
    const file = fileAt("new.md");
    await f.callbacks.get("rename")!(file, "old.md");
    // This is the real queueLocalUpsert direct, single-path echo ACK callsite,
    // not DeferredChangeTracker's two-endpoint push settlement.
    f.engine.pullEchoes.expectsUpsert = () => true;
    f.engine.pullEchoes.consumeUpsert = () => true;
    f.engine.pullEchoHash = async () => "authenticated-expected-hash";
    await f.callbacks.get("modify")!(file);
    check(f.engine.pendingChanges.has("old.md"), "fixture lost the current-session source hint");
    const restarted = await recover(f);
    check(restarted.queued.some(hint => hint.path === "old.md" && hint.action === "deleted"),
        "destination echo ACK erased the only durable source deletion; restart lost pending rename work");
    check(!restarted.queued.some(hint => hint.path === "new.md"),
        "recovery resurrected the already-acknowledged destination endpoint");
}

async function halfAcknowledgementSurvivesRestart(acknowledgedPath: string, compactFirst: boolean): Promise<void> {
    const f = await fixture();
    const id = await appendRename(f.journal);
    if (compactFirst) await f.journal.compact();
    await f.journal.acknowledge([{ path: acknowledgedPath, throughId: id }]);
    const remainingPath = acknowledgedPath === "old.md" ? "new.md" : "old.md";
    let restarted = await recover(f);
    check(restarted.queued.length === 1 && restarted.queued[0].path === remainingPath,
        `half ACK (${acknowledgedPath}, compact=${compactFirst}) lost its peer or resurrected the acknowledged endpoint`);
    check(restarted.queued[0].journalId === id, "partial rename recovery changed the surviving endpoint generation");
    await restarted.journal.compact();
    restarted = await recover(f);
    check(restarted.queued.length === 1 && restarted.queued[0].path === remainingPath,
        "compaction forgot a persisted partial rename ACK");
    await restarted.journal.acknowledge([{ path: remainingPath, throughId: id }]);
    check((await recover(f)).queued.length === 0, "separate endpoint ACK calls could not complete the rename");
}

async function compactionPreservesUnacknowledgedDependency(compactFirst: boolean): Promise<void> {
    const f = await fixture();
    await f.callbacks.get("rename")!(fileAt("new.md"), "old.md");
    await f.callbacks.get("modify")!(fileAt("new.md"));
    if (compactFirst) await f.journal.compact();
    const restarted = await recover(f);
    const result = restarted.engine.deferredChanges.settle(restarted.queued, restarted.queued, [{
        path: "old.md", reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0,
    }], "synthetic-budget", 0);
    check(result.acknowledged.length === 0 && result.retained.length === 2,
        `compaction=${compactFirst} discarded an unacknowledged rename link after a newer destination edit`);
}

async function coalescedRecoveryUsesGenerationOrder(): Promise<void> {
    const f = await fixture();
    await f.callbacks.get("rename")!(fileAt("b.md"), "a.md");
    await f.callbacks.get("rename")!(fileAt("c.md"), "b.md");
    await f.callbacks.get("create")!(fileAt("b.md"));
    await f.journal.compact();
    const recovered = await recover(f);
    const byPath = new Map(recovered.queued.map(hint => [hint.path, hint]));
    check(byPath.size === 3, "coalescing discarded a surviving rename-chain path");
    check(byPath.get("a.md")?.action === "deleted" && byPath.get("a.md")?.journalId === 1,
        "oldest source deletion changed during recovery");
    check(byPath.get("b.md")?.action === "created" && byPath.get("b.md")?.journalId === 3,
        "path-index projection let an older rename overwrite a later path recreation");
    check(byPath.get("c.md")?.journalId === 2, "rename-chain destination lost its generation");
}

async function olderAckCannotConsumeRenewedPair(): Promise<void> {
    const f = await fixture();
    const first = await appendRename(f.journal);
    await f.journal.acknowledge([{ path: "old.md", throughId: first }]);
    await appendRename(f.journal, "new.md", "old.md");
    const newest = await appendRename(f.journal);
    await f.journal.acknowledge([{ path: "new.md", throughId: first }]);
    await f.journal.compact();
    const restarted = await recover(f);
    check(restarted.queued.length === 2 && restarted.queued.every(hint => hint.journalId === newest),
        "an older partial ACK consumed a renewed pair or restored stale generations");
    check(restarted.queued.find(hint => hint.path === "old.md")?.action === "deleted",
        "newest same-pair source is no longer a deletion");
}

async function publicSnapshotDoesNotExposeMutableIndexValues(): Promise<void> {
    const f = await fixture();
    const id = await appendRename(f.journal);
    const exported = f.journal.unsynced();
    exported[0].path = "mutated.md";
    exported[0].oldPath = "mutated-old.md";
    exported.push({ id: id + 100, action: "deleted", path: "invented.md", ts: 1, synced: false } as JournalEntry);
    const pending = f.journal.unsynced();
    check(pending.length === 1 && pending[0].path === "new.md" && pending[0].oldPath === "old.md",
        "unsynced() exposed mutable retained index/group state");
    await f.journal.clear();
    const restarted = await recover(f);
    check(restarted.queued.length === 0, "clear left a logical group pending");
    check(await restarted.journal.append({ action: "modified", path: "later.md", ts: 2, synced: false }) === id + 1,
        "clear reset the journal generation high-water mark");
}

const cases: Array<[string, () => Promise<void>]> = [
    ["actual destination echo keeps source pending", destinationEchoCannotErasePendingSource],
    ...[false, true].flatMap(compactFirst => [
        [`source-only ACK, compact=${compactFirst}`, () => halfAcknowledgementSurvivesRestart("old.md", compactFirst)],
        [`destination-only ACK, compact=${compactFirst}`, () => halfAcknowledgementSurvivesRestart("new.md", compactFirst)],
        [`logical dependency, compact=${compactFirst}`, () => compactionPreservesUnacknowledgedDependency(compactFirst)],
    ] as Array<[string, () => Promise<void>]>),
    ["coalesced generation order", coalescedRecoveryUsesGenerationOrder],
    ["older ACK cannot consume renewed pair", olderAckCannotConsumeRenewedPair],
    ["snapshot ownership and clear high-water mark", publicSnapshotDoesNotExposeMutableIndexValues],
];

void (async () => {
    const failures: string[] = [];
    for (const [name, run] of cases) {
        try { await run(); console.log(`journal-semantics.test PASS: ${name}`); }
        catch (error) {
            failures.push(name);
            console.error(`journal-semantics.test FAIL: ${name}`, error);
        }
    }
    console.log(`journal-semantics.test: ${cases.length - failures.length}/${cases.length} cases, ${assertions} assertions checked`);
    if (failures.length) throw new Error(`journal semantic migration gaps: ${failures.join("; ")}`);
})().catch(error => { console.error(error); process.exitCode = 1; });
