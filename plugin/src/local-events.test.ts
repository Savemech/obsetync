import { TFile, TAbstractFile } from "obsidian";
import { ObsetyncSyncEngine } from "./sync";
import { DirtyPathSet } from "./dirty-set";
import { DeferredChangeTracker } from "./deferred-changes";
import { LocalEventGuards } from "./local-event-guards";

let assertions = 0;
const check = (condition: unknown, message: string): void => {
    assertions++;
    if (!condition) throw new Error(message);
};
function deferred<T = void>() {
    let resolve!: (value: T) => void;
    const promise = new Promise<T>(done => { resolve = done; });
    return { promise, resolve };
}
const fileAt = (path: string) => Object.assign(new TFile(), { path, stat: { size: 1, mtime: 1 } });

function fixture() {
    const callbacks = new Map<string, (...args: any[]) => Promise<void>>();
    const rows: any[] = [];
    const acknowledgements: any[][] = [];
    const engine = Object.create(ObsetyncSyncEngine.prototype) as any;
    let nextId = 1;
    let durable = new Map<string, number>();
    Object.assign(engine, {
        app: { vault: { on: (name: string, callback: any) => { callbacks.set(name, callback); return {}; } } },
        journal: {
            append: async (entry: any) => {
                const id = nextId++;
                rows.push({ ...entry, id });
                durable = new Map(durable).set(entry.path, id);
                if (entry.oldPath) durable.set(entry.oldPath, id);
                return id;
            },
            unsynced: () => { throw new Error("local guard must not materialize the journal array"); },
            unsyncedCount: () => rows.length,
            iterateUnsynced: () => { throw new Error("local guard must not iterate the journal"); },
            capturePendingPaths: () => {
                const captured = durable;
                return { has: (path: string) => captured.has(path) };
            },
            acknowledge: async (watermarks: any[]) => {
                acknowledgements.push(watermarks.map(value => ({ ...value })));
                const next = new Map(durable);
                for (const { path, throughId } of watermarks) {
                    const id = next.get(path);
                    if (id !== undefined && id <= throughId) next.delete(path);
                }
                durable = next;
            },
        },
        eventRefs: [], autoSync: false, pendingChanges: new DirtyPathSet(),
        deferredChanges: new DeferredChangeTracker(), localEventsInFlight: new LocalEventGuards(),
        hashWorkerAbort: new AbortController(),
        pullEchoes: { expectsUpsert: () => false, consumeUpsert: () => false, consumeDelete: () => false },
        isExcluded: () => false,
    });
    const globals = globalThis as any;
    const previous = globals.__obsetyncTestDebounce;
    try { globals.__obsetyncTestDebounce = (callback: () => void) => callback; engine.attachVaultListeners(); }
    finally {
        if (previous === undefined) delete globals.__obsetyncTestDebounce;
        else globals.__obsetyncTestDebounce = previous;
    }
    return { engine, callbacks, rows, acknowledgements };
}

function guardRefcountsAndOverflowAreBounded(): void {
    const guards = new LocalEventGuards(1);
    const first = guards.acquire("same");
    const second = guards.acquire("same");
    check(!first.isCurrent() && second.isCurrent(), "overlap did not supersede earlier echo/hash token");
    first.release(); first.release();
    check(guards.has("same") && guards.size === 1, "earlier callback release removed overlapping protection");
    const overflow = guards.acquire("other");
    check(guards.size === 2 && guards.has("untracked") && !second.isCurrent(), "overflow did not fail closed within bounded metadata");
    second.release();
    check(guards.has("same"), "active overflow lost global overwrite protection");
    overflow.release(); overflow.release();
    check(guards.size === 0 && !guards.has("same"), "completed overlap/overflow leaked a guard");
}

async function overlapKeepsGuardUntilBothCallbacksSettle(): Promise<void> {
    const f = fixture();
    const firstAppend = deferred<number>();
    const secondAppend = deferred<number>();
    let appends = 0;
    f.engine.journal.append = () => ++appends === 1 ? firstAppend.promise : secondAppend.promise;
    const file = fileAt("note.md");
    const first = f.callbacks.get("modify")!(file);
    const second = f.callbacks.get("modify")!(file);
    firstAppend.resolve(1);
    await first;
    check(f.engine.localEventsInFlight.has("note.md"), "actual earlier callback removed second event guard");
    f.engine.pendingChanges.take();
    check(f.engine.unsyncedLocalPaths().has("note.md"), "pull lost live protection after detaching older pending hint");
    secondAppend.resolve(2);
    await second;
    check(f.engine.localEventsInFlight.size === 0, "actual overlapping callbacks leaked refcount");
    check(f.engine.pendingChanges.take()[0].journalId === 2, "newer callback lost its durable generation");
}

async function durablePullGuardKeepsOldCutAndNewLiveChanges(): Promise<void> {
    const f = fixture();
    await f.callbacks.get("modify")!(fileAt("durable.md"));
    f.engine.pendingChanges.take();
    const paths = f.engine.unsyncedLocalPaths();
    check(paths.has("durable.md") && !paths.has("unrelated.md"),
        "pull guard did not use the captured non-iterating journal index");
    await f.engine.journal.acknowledge([{ path: "durable.md", throughId: 1 }]);
    check(paths.has("durable.md") && !f.engine.unsyncedLocalPaths().has("durable.md"),
        "later ACK mutated an already-captured pull guard");

    const appended = deferred<number>();
    f.engine.journal.append = () => appended.promise;
    const live = f.callbacks.get("modify")!(fileAt("new-live.md"));
    check(paths.has("new-live.md") && paths.has("durable.md"),
        "captured durable cut excluded a new in-flight callback");
    appended.resolve(2);
    await live;
    check(paths.has("new-live.md") && !paths.has("unrelated.md"),
        "completed callback lost live pending protection alongside captured durable paths");
}

function failedJournalCaptureProtectsEveryPath(): void {
    const f = fixture();
    f.engine.journal.capturePendingPaths = () => { throw new Error("injected unavailable durable protection"); };
    const paths = f.engine.unsyncedLocalPaths();
    check(paths.has("offline.md") && paths.has("any/deep/path.md"),
        "failed journal capture fell back to an incomplete current-session path set");
    f.engine.journal.capturePendingPaths = () => ({ has: () => false });
    check(paths.has("still-protected.md") && !f.engine.unsyncedLocalPaths().has("clean.md"),
        "failed captured cut changed after recovery or recovery remained globally blocked");
}

async function mutablePathsNeverMoveAnEarlierEventOrAck(): Promise<void> {
    for (const event of ["modify", "create", "delete"]) {
        const f = fixture();
        const gate = deferred<number>();
        f.engine.journal.append = (entry: any) => { f.rows.push({ ...entry }); return gate.promise; };
        f.engine.pullEchoes.expectsUpsert = () => true;
        f.engine.pullEchoes.consumeUpsert = () => true;
        let hashes = 0;
        f.engine.pullEchoHash = async () => { hashes++; return "echo"; };
        const file = fileAt("before.md");
        const pending = f.callbacks.get(event)!(file);
        file.path = "after.md";
        file.stat = { ...file.stat, mtime: 2, size: 2 };
        gate.resolve(7);
        await pending;
        const hints = f.engine.pendingChanges.take();
        check(f.rows[0].path === "before.md" && hints.length === 1 && hints[0].path === "before.md",
            `${event} followed a mutated TFile.path after await`);
        check(hints[0].journalId === 7 && f.acknowledgements.length === 0 && hashes === 0,
            `${event} authenticated/acknowledged a newer pathname`);
        check(!f.engine.localEventsInFlight.has("before.md") && f.engine.localEventsInFlight.size === 0,
            `${event} leaked captured path guard`);
    }
}

async function oldHashCompletionPreservesNewerHint(): Promise<void> {
    const f = fixture();
    const enteredOldHash = deferred();
    const finishOldHash = deferred<string>();
    let hashes = 0;
    f.engine.pullEchoes.expectsUpsert = () => true;
    f.engine.pullEchoes.consumeUpsert = (_path: string, hash: string) => hash === "old-echo";
    f.engine.pullEchoHash = async () => {
        hashes++;
        if (hashes === 1) { enteredOldHash.resolve(); return finishOldHash.promise; }
        return "new-local-hash";
    };
    const file = fileAt("note.md");
    const old = f.callbacks.get("modify")!(file);
    await enteredOldHash.promise;
    file.stat = { ...file.stat, mtime: 2, size: 2 };
    await f.callbacks.get("modify")!(file);
    check(f.engine.localEventsInFlight.has("note.md"), "newer callback released older hash consumer's guard");
    finishOldHash.resolve("old-echo");
    await old;
    const hint = f.engine.pendingChanges.take()[0];
    check(hint.journalId === 2 && hint.hash === "new-local-hash" && hint.mtime === 2 && hint.size === 2,
        "late old echo/hash completion overwrote newer dirty generation");
    check(f.acknowledgements.length === 0, "superseded echo completion acknowledged a dirty generation");
}

async function ackUsesOnlyItsOwnCapturedWatermark(): Promise<void> {
    const f = fixture();
    f.engine.pullEchoes.expectsUpsert = () => true;
    f.engine.pullEchoes.consumeUpsert = () => true;
    f.engine.pullEchoHash = async () => "echo";
    const enteredAck = deferred();
    const finishAck = deferred();
    f.engine.journal.acknowledge = async (watermarks: any[]) => {
        f.acknowledgements.push(watermarks.map(value => ({ ...value })));
        enteredAck.resolve();
        await finishAck.promise;
    };
    const file = fileAt("note.md");
    const old = f.callbacks.get("modify")!(file);
    await enteredAck.promise;
    f.engine.pullEchoes.expectsUpsert = () => false;
    await f.callbacks.get("modify")!(file);
    finishAck.resolve();
    await old;
    check(f.acknowledgements[0][0].path === "note.md" && f.acknowledgements[0][0].throughId === 1,
        "echo ACK absorbed a later event watermark");
    check(f.engine.pendingChanges.take()[0].journalId === 2, "echo ACK completion removed newer pending hint");
}

async function hashVerifierNeverFollowsRenamedFile(): Promise<void> {
    const f = fixture();
    const started = deferred();
    const finished = deferred();
    let hashedPath = "";
    f.engine.io = { getAbsolutePath: () => "/fixture/source" };
    f.engine.hashStableFile = async (path: string) => { hashedPath = path; started.resolve(); await finished.promise; return { hash: "echo" }; };
    const file = fileAt("before.md");
    const hashing = f.engine.pullEchoHash(file);
    await started.promise;
    file.path = "after.md";
    finished.resolve();
    check(await hashing === null && hashedPath === "before.md", "echo verifier followed mutable path through native hash await");
    let folderWrites = 0;
    f.engine.journal.append = async () => { folderWrites++; return 1; };
    const folder = Object.assign(Object.create(TAbstractFile.prototype), { path: "folder" });
    await f.callbacks.get("create")!(folder);
    await f.callbacks.get("delete")!(folder);
    check(folderWrites === 0, "callback migration started journaling/reading folders");
}

guardRefcountsAndOverflowAreBounded();
const watchdog = setTimeout(() => { throw new Error("local event callback test did not settle"); }, 10_000);
void overlapKeepsGuardUntilBothCallbacksSettle()
    .then(mutablePathsNeverMoveAnEarlierEventOrAck)
    .then(oldHashCompletionPreservesNewerHint)
    .then(ackUsesOnlyItsOwnCapturedWatermark)
    .then(hashVerifierNeverFollowsRenamedFile)
    .then(durablePullGuardKeepsOldCutAndNewLiveChanges)
    .then(failedJournalCaptureProtectsEveryPath)
    .then(() => console.log(`local-events.test: ${assertions} assertions passed`))
    .catch(error => { console.error(error); process.exitCode = 1; })
    .finally(() => clearTimeout(watchdog));
