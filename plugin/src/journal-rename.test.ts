import { TFile } from "obsidian";
import { ObsetyncSyncEngine } from "./sync";
import { ObsetyncJournal } from "./journal";
import { DirtyPathSet, type DirtyFileChange } from "./dirty-set";
import { DeferredChangeTracker } from "./deferred-changes";
import { LocalEventGuards } from "./local-event-guards";
import type { DeferredPushChange } from "./push";
import { MemorySegmentedIO } from "./segmented-store-test-io";

/** Real bounded-storage adapter surface. Count immutable WAL writes, not
 * rotating heads; failure injection leaves an unreferenced torn segment. */
class RenameJournalIO extends MemorySegmentedIO {
    readonly walWrites: Array<{ path: string; raw: string }> = [];
    partialAppend = false;

    override async write(path: string, raw: string): Promise<void> {
        if (/\/wal-[^/]+\.json$/.test(path)) {
            this.walWrites.push({ path, raw });
            if (this.partialAppend) {
                await super.write(path, raw.slice(0, 15));
                throw new Error("injected linked WAL segment write failure");
            }
        }
        await super.write(path, raw);
    }
}

const segmentRows = (raw: string): Array<{ schema: number; op: string; entry?: {
    action: string; path: string; oldPath?: string;
} }> => JSON.parse(JSON.parse(raw).payload).rows;

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

/** In-memory adapter and captured actual callback; no native Obsidian vault or
 * renderer lifecycle is emulated. Only the application debounce is replaced. */
async function fixture() {
    const adapter = new RenameJournalIO();
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
        isExcluded: (path: string) => path.startsWith("ignored/"),
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
    return { app, engine, journal, files: adapter.files, callbacks, appends: () => adapter.walWrites.length,
        walWrites: adapter.walWrites, failAppend: () => { adapter.partialAppend = true; } };
}

const fileAt = (path: string): TFile => Object.assign(new TFile(), { path, stat: { size: 3, mtime: 1 } });

async function enginePersistsOneRenameAndSnapshotsTheEventPath(): Promise<void> {
    const f = await fixture();
    const file = fileAt("new.md");
    const pending = f.callbacks.get("rename")!(file, "old.md");
    file.path = "renamed-again.md";
    await pending;
    check(f.appends() === 1, "actual engine callback persisted two separate rename rows");
    const persistedRows = segmentRows(f.files.get(f.walWrites[0].path)!);
    check(persistedRows.length === 1 && persistedRows[0].schema === 1 && persistedRows[0].op === "append" &&
        persistedRows[0].entry?.action === "renamed" && persistedRows[0].entry.oldPath === "old.md" &&
        persistedRows[0].entry.path === "new.md",
        "one rename callback did not publish exactly one linked row in one immutable WAL segment");
    const row = f.journal.unsynced()[0];
    check(row.action === "renamed" && row.oldPath === "old.md" && row.path === "new.md",
        "mutable TFile.path rewrote the durable identity of an earlier rename");
    const queued = f.engine.pendingChanges.take();
    check(queued.length === 2 && queued.every((hint: any) => hint.journalId === row.id), "rename halves did not share one generation");
    check(queued.some((hint: any) => hint.path === "new.md") &&
        !queued.some((hint: any) => hint.path === "renamed-again.md"), "rename queued a later event's destination");
    check(f.engine.localEventsInFlight.size === 0, "renamed TFile leaked its old in-flight guard");
    const changed = queued.map((hint: any) => hint.path === "new.md" ? { ...hint, journalId: row.id + 1 } : hint);
    const held = f.engine.deferredChanges.settle(changed, changed, [{
        path: "old.md", reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0,
    }], "test", 0);
    check(held.retained.length === 2 && held.acknowledged.length === 0,
        "actual rename callback did not register durability links across newer destination edits");
}

async function ignoredHalvesRemainIndependentAndPartialGroupsKeepBothHints(): Promise<void> {
    const incoming = await fixture();
    await incoming.callbacks.get("rename")!(fileAt("new.md"), "ignored/old.md");
    check(incoming.journal.unsynced().length === 1 && incoming.journal.unsynced()[0].action === "created",
        "rename entering scope persisted its ignored old path");
    const outgoing = await fixture();
    await outgoing.callbacks.get("rename")!(fileAt("ignored/new.md"), "old.md");
    check(outgoing.journal.unsynced().length === 1 && outgoing.journal.unsynced()[0].action === "deleted",
        "rename leaving scope persisted its ignored new path");

    const failed = await fixture();
    failed.failAppend();
    await failed.callbacks.get("rename")!(fileAt("new.md"), "old.md");
    const queued = failed.engine.pendingChanges.take();
    check(queued.length === 2 && queued.every((hint: any) => hint.journalId === undefined),
        "partial append lost one session hint or invented a durable watermark");
    check(failed.journal.unsynced().length === 0, "partial append published a half-rename in memory");
    const attempted = failed.walWrites.flatMap(write => segmentRows(write.raw));
    check(failed.appends() === 1 && attempted.length === 1 && attempted[0].op === "append" &&
        attempted[0].entry?.action === "renamed" && failed.files.get(failed.walWrites[0].path)?.length === 15,
        "actual callback persisted the old-path deletion separately instead of a single torn linked segment");
}

const oversizedMove: DeferredPushChange[] = [
    { path: "b.md", reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0 },
    { path: "large.bin", reason: "source-too-large", requiredBytes: 1000, capacityBytes: 100 },
];

/** Actual metadata-only replay: no synthetic remote transport or push. */
async function recover(f: Awaited<ReturnType<typeof fixture>>) {
    const journal = new ObsetyncJournal(f.app as any);
    await journal.load();
    const engine = Object.create(ObsetyncSyncEngine.prototype) as any;
    let handoffs = 0;
    Object.assign(engine, {
        journal, pendingChanges: new DirtyPathSet(), deferredChanges: new DeferredChangeTracker(),
        hashWorkerAbort: new AbortController(),
        onStatusUpdate: () => {}, progressHeartbeat: () => {},
        pushPending: async () => { handoffs++; },
    });
    await engine.recoverFromJournal();
    check(handoffs === 0, "local journal replay unexpectedly entered network push");
    return { journal, engine, queued: engine.pendingChanges.take() as DirtyFileChange[] };
}

async function olderPushCannotRetireAnInflightRenamesDependency(): Promise<void> {
    const f = await fixture();
    await f.callbacks.get("modify")!(fileAt("b.md"));
    const detached = f.engine.pendingChanges.take() as DirtyFileChange[];
    check(detached.length === 1 && detached[0].journalId === 1, "fixture did not detach b.md@1");

    const moved = fileAt("c.md");
    await f.callbacks.get("rename")!(moved, "b.md");
    await f.callbacks.get("modify")!(moved);
    await f.callbacks.get("create")!(fileAt("large.bin"));

    // Actual callback registration happened during an older push. Only that
    // detached b@1 is acknowledged; the new b→c@2 dependency must survive.
    const oldResult = f.engine.deferredChanges.settle(detached, detached, [], "mobile", 0);
    await f.journal.acknowledge(oldResult.acknowledged);
    f.engine.deferredChanges.commitSettlement(oldResult);
    const next = f.engine.pendingChanges.take() as DirtyFileChange[];
    check(next.find((hint) => hint.path === "b.md")?.journalId === 2, "source half lost rename generation 2");
    check(next.find((hint) => hint.path === "c.md")?.journalId === 3, "modify callback did not replace destination with generation 3");
    check(next.find((hint) => hint.path === "large.bin")?.journalId === 4, "fixture did not produce oversized generation 4");
    const result = f.engine.deferredChanges.settle(next, next, oversizedMove, "mobile", 1);
    check(result.acknowledged.length === 0 && result.retained.length === 3,
        "old push completion retired the newer link and acknowledged c@3 without b@2");
    f.engine.pendingChanges.restore(result.retained);
    await f.journal.acknowledge(result.acknowledged);
    f.engine.deferredChanges.commitSettlement(result);

    const restarted = await recover(f);
    check(restarted.journal.unsynced().map((row) => row.id).join(",") === "2,3,4",
        "restart lost the sole durable rename row or kept acknowledged b@1");
    check(restarted.queued.find((hint) => hint.path === "b.md")?.action === "deleted",
        "actual journal recovery lost the pending old-path deletion");
    check(restarted.queued.find((hint) => hint.path === "c.md")?.journalId === 3,
        "actual journal recovery replaced a newer destination edit with the rename hint");
    const afterRestart = restarted.engine.deferredChanges.settle(restarted.queued, restarted.queued, oversizedMove, "mobile", 2);
    check(afterRestart.acknowledged.length === 0 && afterRestart.retained.length === 3,
        "recovery failed to restore the rename dependency with its durable generation");

    const accepted = restarted.engine.deferredChanges.settle(afterRestart.retained, afterRestart.retained, [], "desktop", 3);
    await restarted.journal.acknowledge(accepted.acknowledged);
    restarted.engine.deferredChanges.commitSettlement(accepted);
    const committed = new ObsetyncJournal(f.app as any);
    await committed.load();
    check(committed.unsynced().length === 0, "successful retry could not durably acknowledge the entire rename");
}

async function failureBeforeAckKeepsLinksForNewerDestinationRetry(): Promise<void> {
    const f = await fixture();
    const moved = fileAt("c.md");
    await f.callbacks.get("rename")!(moved, "b.md");
    const detached = f.engine.pendingChanges.take() as DirtyFileChange[];
    const prepared = f.engine.deferredChanges.settle(detached, detached, [], "mobile", 0);
    check(prepared.acknowledged.length === 2, "fixture did not prepare a successful two-half rename");
    // This is pushPending's saveCachedRoot/conflict-copy failure boundary:
    // settle already ran, but the journal ACK has not. Its catch restores the
    // detached snapshot, and a normal modify may then replace the destination.
    f.engine.pendingChanges.restore(detached);
    await f.callbacks.get("modify")!(moved);
    await f.callbacks.get("create")!(fileAt("large.bin"));
    const retry = f.engine.pendingChanges.take() as DirtyFileChange[];
    const held = f.engine.deferredChanges.settle(retry, retry, oversizedMove, "mobile", 1);
    check(held.acknowledged.length === 0 && held.retained.length === 3,
        "pre-ACK failure retired the rename dependency before its durable completion");
    await f.journal.acknowledge(held.acknowledged);
    f.engine.deferredChanges.commitSettlement(held);
    const restarted = await recover(f);
    check(restarted.journal.unsynced().map((row) => row.id).join(",") === "1,2,3",
        "retry after pre-ACK failure erased the original rename row");
    check(restarted.queued.find((hint) => hint.path === "b.md")?.journalId === 1,
        "restart could not recover the held deletion after pre-ACK failure");
    check(restarted.queued.find((hint) => hint.path === "c.md")?.journalId === 2,
        "restart lost the newer destination modification after pre-ACK failure");
}

function probeOldRenameEdge(engine: any): boolean {
    const hints: DirtyFileChange[] = [
        { path: "old.md", action: "deleted", journalId: 10 },
        { path: "new.md", action: "modified", journalId: 11 },
    ];
    const result = engine.deferredChanges.settle(hints, hints, [{
        path: "old.md", reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0,
    }], "test", 0);
    return result.acknowledged.some((ack: any) => ack.path === "new.md");
}

async function acknowledgedEchoRetiresOnlyItsOldLink(): Promise<void> {
    const f = await fixture();
    f.engine.pullEchoes.expectsRename = () => true;
    f.engine.pullEchoes.consumeRename = () => true;
    f.engine.pullEchoHash = async () => "remote-hash";
    const unrelated = [{ path: "old.md", action: "modified" as const, journalId: 20 }];
    f.engine.deferredChanges.settle(unrelated, unrelated, [{
        path: "old.md", reason: "source-too-large", requiredBytes: 100, capacityBytes: 10,
    }], "test", 0);
    await f.callbacks.get("rename")!(fileAt("new.md"), "old.md");
    check(f.journal.unsynced().length === 0, "authenticated rename echo was not durably acknowledged");
    check(f.engine.deferredChanges.summary().count === 1, "echo-only link cleanup mutated deferred records");
    check(probeOldRenameEdge(f.engine), "successful echo left a dependency on later independent path reuse");
}

async function newerRenameSurvivesAnOlderEchoAck(): Promise<void> {
    const f = await fixture();
    f.engine.pullEchoes.expectsRename = () => true;
    f.engine.pullEchoes.consumeRename = () => true;
    f.engine.pullEchoHash = async () => "remote-hash";
    let ackEntered!: () => void;
    const entered = new Promise<void>(resolve => { ackEntered = resolve; });
    let finishAck!: () => void;
    const gate = new Promise<void>(resolve => { finishAck = resolve; });
    const acknowledge = f.journal.acknowledge.bind(f.journal);
    f.journal.acknowledge = async watermarks => { ackEntered(); await gate; await acknowledge(watermarks); };
    const oldEcho = f.callbacks.get("rename")!(fileAt("new.md"), "old.md");
    await entered;
    f.engine.pullEchoes.expectsRename = () => false;
    await f.callbacks.get("rename")!(fileAt("old.md"), "new.md");
    await f.callbacks.get("modify")!(fileAt("old.md"));
    finishAck();
    await oldEcho;
    check(f.journal.unsynced().map(row => row.id).join() === "2,3", "older echo ACK erased newer rename/edit generations");
    const hints = f.engine.pendingChanges.take();
    const result = f.engine.deferredChanges.settle(hints, hints, [{
        path: "new.md", reason: "dependent-delete", requiredBytes: 0, capacityBytes: 0,
    }], "test", 0);
    check(result.retained.length === 2 && result.acknowledged.length === 0,
        "older echo ACK retired a concurrently renewed rename dependency");
}

async function failedEchoAckKeepsItsLink(): Promise<void> {
    const f = await fixture();
    f.engine.pullEchoes.expectsRename = () => true;
    f.engine.pullEchoes.consumeRename = () => true;
    f.engine.pullEchoHash = async () => { f.failAppend(); return "remote-hash"; };
    let failed = false;
    try { await f.callbacks.get("rename")!(fileAt("new.md"), "old.md"); } catch { failed = true; }
    check(failed && f.journal.unsynced().length === 1, "failed echo ACK lost the durable rename");
    check(!probeOldRenameEdge(f.engine), "failed echo ACK retired its recovery dependency");
}

void enginePersistsOneRenameAndSnapshotsTheEventPath()
    .then(ignoredHalvesRemainIndependentAndPartialGroupsKeepBothHints)
    .then(olderPushCannotRetireAnInflightRenamesDependency)
    .then(failureBeforeAckKeepsLinksForNewerDestinationRetry)
    .then(acknowledgedEchoRetiresOnlyItsOldLink)
    .then(newerRenameSurvivesAnOlderEchoAck)
    .then(failedEchoAckKeepsItsLink)
    .then(() => console.log(`journal-rename.test: ${assertions} assertions passed`))
    .catch((error) => { console.error(error); process.exitCode = 1; });
