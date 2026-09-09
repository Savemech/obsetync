import { DirtyPathSet, DIRTY_RETIREMENT_LIMIT, DIRTY_CLAIM_LIMIT, DIRTY_TRACKING_LIMIT, type DirtyFileChange,
    type DirtyPathCapture, type DirtyRetirementToken } from "./dirty-set";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

const dirty = new DirtyPathSet();
dirty.add({ action: "modified", path: "a.md", data: new Uint8Array(1024) }, 1);
dirty.add({ action: "deleted", path: "a.md" }, 2);
check(dirty.size === 1, "same path was not coalesced");
let batch = dirty.take();
check(batch[0].action === "deleted", "modify then delete must finish deleted");
check(!("data" in batch[0]), "dirty set retained file bytes");
check(batch[0].journalId === 2, "latest journal watermark was lost");

dirty.add({ action: "deleted", path: "b.md" }, 3);
dirty.add({ action: "created", path: "b.md", hash: "new" }, 4);
batch = dirty.take();
check(batch.length === 1 && batch[0].action === "created", "delete then create must finish created");
check(batch[0].hash === "new", "latest upsert metadata was lost");

dirty.add({ action: "modified", path: "race.md", hash: "old" }, 5);
const inFlight = dirty.take();
dirty.add({ action: "modified", path: "race.md", hash: "new" }, 6);
dirty.restore(inFlight);
batch = dirty.take();
check(batch[0].hash === "new", "failed push overwrote a newer in-flight event");
check(batch[0].journalId === 6, "newer journal watermark was overwritten");

dirty.add({ action: "modified", path: "scan-race.md", hash: "journaled" }, 7);
const failedJournaled = dirty.take();
dirty.add({ action: "modified", path: "scan-race.md", hash: "scan" });
dirty.restore(failedJournaled);
batch = dirty.take();
check(batch[0].hash === "scan", "restore replaced a newer scan result");
check(batch[0].journalId === 7, "restore orphaned an older durable journal row");

function rejects(work: () => unknown, message: string) {
    let failed = false; try { work(); } catch { failed = true; } check(failed, message);
}
const hint = (path = "note.md", hash = "old"): DirtyFileChange => ({ action: "modified", path, hash });
const cut = (path = "note.md", throughId = 7) => ({ path, throughId });
function inspect(set: DirtyPathSet): DirtyFileChange[] {
    const values = set.take(); set.restore(values); return values;
}

function retirementCuts() {
    const set = new DirtyPathSet();
    set.add(hint(), 7); set.add(hint("unrelated.md"), 7);
    const token = set.captureRetirement([cut()]);
    check(Object.isFrozen(token) && Object.keys(token).length === 0, "retirement token exposed mutable paths/state");
    check(set.commitRetirement(token) === 1 && !set.has("note.md") && set.has("unrelated.md"), "exact ACK retired unrelated paths");
    check(set.commitRetirement(token) === 0, "retirement token could be committed twice");
    check(set.commitRetirement({} as DirtyRetirementToken) === 0, "forged token granted retirement");
    set.add(hint(), 8);
    check(set.commitRetirement(set.captureRetirement([cut()])) === 0 && set.has("note.md"), "older cut retired newer durable generation");
    const previous = set.captureRetirement([cut("note.md", 8)]);
    const next = set.captureRetirement([cut("unrelated.md", 7)]);
    check(set.commitRetirement(previous) === 0 && set.has("note.md"), "a replaced capture retained authority");
    const other = new DirtyPathSet(); other.add(hint("unrelated.md"), 7);
    const own = other.captureRetirement([cut("unrelated.md")]);
    check(other.commitRetirement(next) === 0 && other.has("unrelated.md"), "foreign token removed another owner's hint");
    check(other.commitRetirement(own) === 1, "foreign token invalidated this owner's current capture");
    check(set.commitRetirement(next) === 1, "current token stopped working after a foreign commit attempt");
    const detachedCuts = [cut("note.md", 8)];
    const detached = set.captureRetirement(detachedCuts);
    detachedCuts[0].path = "other.md"; detachedCuts[0].throughId = 1000;
    check(set.commitRetirement(detached) === 1, "mutable caller cut changed captured retirement");
}

function laterEvents() {
    for (const mode of ["new-id", "no-id", "same-id", "delete"] as const) {
        const set = new DirtyPathSet(); set.add(hint(), 7);
        const token = set.captureRetirement([cut()]);
        const next = mode === "delete" ? { action: "deleted" as const, path: "note.md" } : hint("note.md", "new");
        set.add(next, mode === "no-id" ? undefined : mode === "new-id" ? 8 : 7);
        check(set.commitRetirement(token) === 0 && set.has("note.md"), `later ${mode} event was retired by an old capture`);
        check(inspect(set)[0].action === next.action && inspect(set)[0].hash === next.hash, `later ${mode} state changed`);
    }
    const set = new DirtyPathSet();
    set.add(hint(), 7); set.add(hint("note.md", "scan"));
    check(inspect(set)[0].journalId === 7, "scan hint lost inherited recovery watermark");
    check(set.commitRetirement(set.captureRetirement([cut()])) === 0, "pre-capture no-ID scan inherited ACK authority");
    const embedded = { ...hint(), journalId: 7 };
    const noOwnID = new DirtyPathSet(); noOwnID.add(embedded);
    check(noOwnID.commitRetirement(noOwnID.captureRetirement([cut()])) === 0, "embedded watermark became an explicit add generation");
    const carriedNewer = new DirtyPathSet(); carriedNewer.add(hint(), 8); carriedNewer.add(hint("note.md", "own-seven"), 7);
    check(carriedNewer.commitRetirement(carriedNewer.captureRetirement([cut()])) === 0, "retirement orphaned a higher carried WAL watermark");
}

function trustedTakeRestore() {
    const set = new DirtyPathSet(); set.add(hint(), 7);
    const beforeTake = set.captureRetirement([cut()]), snapshot = set.take();
    set.restore(snapshot);
    check(set.commitRetirement(beforeTake) === 1, "unchanged own take/restore lost its exact state identity");
    set.add(hint(), 7);
    const failed = set.take(); set.add(hint("note.md", "scan")); set.restore(failed);
    const restored = inspect(set)[0];
    check(restored.hash === "scan" && restored.journalId === 7, "failed restore changed a newer unjournaled hint");
    check(set.commitRetirement(set.captureRetirement([cut()])) === 0, "restore carried old provenance into a newer no-ID state");

    for (const field of ["action", "hash", "mtime", "size", "journalId", "path"] as const) {
        const owner = new DirtyPathSet(); owner.add({ ...hint(), mtime: 10, size: 20 }, 7);
        const values = owner.take(), value = values[0];
        if (field === "action") value.action = "deleted";
        else if (field === "hash") value.hash = "changed";
        else if (field === "path") value.path = "changed.md";
        else value[field] = 8;
        owner.restore(values);
        check(owner.commitRetirement(owner.captureRetirement([cut(value.path, 10)])) === 0,
            `caller-modified ${field} preserved stale take provenance`);
        value.path = "external.md"; value.hash = "again";
        const live = owner.take()[0];
        check(live.path !== value.path && live.hash !== value.hash, `restored ${field} record remained caller-owned`);
    }
    const origin = new DirtyPathSet(); origin.add(hint(), 7);
    const foreign = new DirtyPathSet(); foreign.restore(origin.take());
    check(foreign.commitRetirement(foreign.captureRetirement([cut()])) === 0, "another dirty owner's take token granted provenance");
    const raw = { ...hint(), journalId: 7, data: new Uint8Array(100) };
    const external = new DirtyPathSet(); external.restore([raw]);
    raw.path = "external.md"; raw.hash = "mutated"; raw.data[0] = 9;
    const value = inspect(external)[0];
    check(value.path === "note.md" && value.hash === "old" && !("data" in value), "ordinary restore retained caller bytes/object");
    check(external.commitRetirement(external.captureRetirement([cut()])) === 0, "ordinary external restore fabricated durable provenance");
}

function replayAndRenameCuts() {
    const set = new DirtyPathSet();
    const replay = [{ ...hint(), journalId: 7, data: new Uint8Array(100) }];
    set.restoreJournal(replay);
    replay[0].hash = "caller-changed"; replay[0].journalId = 1000;
    check(inspect(set)[0].hash === "old" && inspect(set)[0].journalId === 7 && !("data" in inspect(set)[0]),
        "journal replay retained caller metadata or bytes");
    check(set.commitRetirement(set.captureRetirement([cut()])) === 1, "proven replay could not retire after its exact ACK");
    set.add(hint("note.md", "live"));
    set.restoreJournal([{ ...hint("note.md", "replayed"), journalId: 7 }]);
    check(inspect(set)[0].hash === "live" && inspect(set)[0].journalId === 7, "journal replay overwrote a newer live state");
    check(set.commitRetirement(set.captureRetirement([cut()])) === 0, "journal replay conferred old authority on a newer no-ID hint");
    const newer = new DirtyPathSet(); newer.add(hint("note.md", "newer"), 8);
    const token = newer.captureRetirement([cut("note.md", 8)]);
    newer.restoreJournal([{ ...hint(), journalId: 7 }]);
    check(newer.commitRetirement(token) === 1, "no-op older replay changed a captured current generation");

    const rename = new DirtyPathSet();
    rename.restoreJournal([{ action: "deleted", path: "a.md", journalId: 9 },
        { action: "created", path: "b.md", journalId: 9 }]);
    const oldOnly = rename.captureRetirement([cut("a.md", 9)]);
    check(rename.commitRetirement(oldOnly) === 1 && rename.has("b.md"), "shared rename ID retired an uncaptured endpoint");
    const newOnly = rename.captureRetirement([cut("b.md", 9)]);
    rename.add(hint("b.md", "newer scan"));
    check(rename.commitRetirement(newOnly) === 0 && rename.has("b.md"), "rename ACK erased a newer target state");
    check(rename.commitRetirement(rename.captureRetirement([cut("b.md", 9)])) === 0, "rename watermark gave a later scan retirement authority");
}

function boundsAndAtomicValidation() {
    const set = new DirtyPathSet(); set.add(hint(), 7);
    const valid = set.captureRetirement([cut()]);
    const invalid = [
        [cut("../unsafe.md")], [cut("a".repeat(4097))], [cut("note.md", 0)], [cut("note.md", -1)],
        [cut("note.md", 1.5)], [cut("note.md", NaN)], [cut("note.md", Infinity)],
        [cut("note.md", Number.MAX_SAFE_INTEGER)], [cut(), cut()], [cut("z.md"), cut("a.md")],
        Array.from({ length: DIRTY_RETIREMENT_LIMIT + 1 }, (_, index) => cut(`n${index}`)),
    ];
    for (const cuts of invalid) rejects(() => set.captureRetirement(cuts), "invalid/oversized retirement cut was admitted");
    check(set.commitRetirement(valid) === 1, "invalid capture replaced a previously valid bounded token");
    set.add(hint(), 7);
    rejects(() => set.restoreJournal([{ ...hint("first.md"), journalId: 8 }, { ...hint("bad.md") }]), "unproven replay hint was admitted");
    check(!set.has("first.md") && set.size === 1, "invalid replay partially mutated the dirty map");
    rejects(() => set.restoreJournal(Array.from({ length: 257 }, () => ({ ...hint(), journalId: 7 }))), "replay batch exceeded bounded admission");
    const full = new DirtyPathSet(), cuts = Array.from({ length: 256 }, (_, index) => cut(`n${index.toString().padStart(3, "0")}`));
    full.restoreJournal(cuts.map(({ path, throughId }) => ({ ...hint(path), journalId: throughId })));
    check(full.commitRetirement(full.captureRetirement(cuts)) === 256 && full.size === 0, "maximum bounded retirement failed");
    check(full.commitRetirement(full.captureRetirement([])) === 0, "empty journal cut changed state");
}

function immutableReadCaptures() {
    const set = new DirtyPathSet();
    const input = { ...hint("z.md", "z-old"), data: new Uint8Array(1024) };
    set.add(input, 7); set.add(hint("a.md", "a-old"), 8); set.add(hint("m.md", "m-old"), 9);
    const capture = set.capture(), iterator = set.iterate(), paths = set.paths();
    check(Object.isFrozen(capture) && capture.size === 3, "capture is mutable or has the wrong stable size");
    check(capture.get("absent.md") === undefined, "capture invented a missing path");
    input.hash = "caller-mutated"; input.path = "caller.md"; input.data[0] = 100;
    set.add(hint("z.md", "z-new"), 10); set.add(hint("new.md"), 11);
    const oldRows = [...capture.iterate()];
    check(oldRows.map(row => row.path).join() === "z.md,a.md,m.md", "capture changed sequential insertion order");
    check(oldRows[0].hash === "z-old" && oldRows[0].journalId === 7 && !("data" in oldRows[0]),
        "capture exposed later/caller-owned state or file bytes");
    check([...iterator].map(row => row.hash).join() === "z-old,a-old,m-old", "read iterator did not capture before its first next()");
    check([...paths].join() === "z.md,a.md,m.md", "path iterator did not retain its original order/cut");
    check([...set.iterate()].map(row => row.path).join() === "z.md,a.md,m.md,new.md", "updating an existing path moved its insertion position");
    check(capture.size === 3 && set.size === 4, "captured size tracked the live queue");

    const first = capture.get("z.md")!, again = capture.get("z.md")!;
    check(first !== again && first !== oldRows[0], "capture shared a caller-mutable row between reads");
    first.hash = "changed"; first.path = "elsewhere.md"; first.journalId = 999;
    check(capture.get("z.md")!.hash === "z-old" && capture.get("z.md")!.journalId === 7,
        "mutating a read row changed the captured entry");
    check(set.capture().get("z.md")!.hash === "z-new", "mutating an old read row changed the live queue");
    rejects(() => Object.assign(capture, { size: 0 }), "capture size could be changed");
    rejects(() => Object.assign(capture, { get: () => undefined }), "capture lookup could be replaced");
    check(set.claim({ ...capture } as DirtyPathCapture, ["a.md"]) === null && set.has("a.md"),
        "a copied public capture forged owner authority");
    check(set.claim(capture, ["z.md"]) === null, "equal path in an old capture selected a later generation");
    check(set.claim(capture, ["a.md"])?.[0].hash === "a-old", "unrelated live mutations invalidated an unchanged captured entry");
    check(capture.get("a.md")!.hash === "a-old" && !set.has("a.md"), "claim mutated the historical capture");
    check(set.claim(capture, ["a.md"]) === null, "a detached captured entry was claimed twice");
}

function wholeSetCaptureWitnessIsOwnerLocalAndReadOnly() {
    const set = new DirtyPathSet(), empty = set.capture();
    check(set.isCurrentCapture(empty) && set.isCurrentCapture(set.capture()),
        "empty/multiple captures of the current root were rejected");
    const foreign = new DirtyPathSet();
    check(!set.isCurrentCapture(foreign.capture()) && !foreign.isCurrentCapture(empty),
        "equal empty roots from another owner forged a current capture");
    const hostile = new Proxy(empty, {
        get() { throw new Error("capture witness inspected caller properties"); },
        ownKeys() { throw new Error("capture witness enumerated caller properties"); },
    });
    for (const invalid of [null, undefined, {}, { ...empty }, Object.create(empty), hostile]) {
        check(!set.isCurrentCapture(invalid as DirtyPathCapture), "unissued capture was accepted");
    }
    set.add({ ...hint(), mtime: 1, size: 2 }, 7);
    const capture = set.capture(), second = set.capture();
    check(!set.isCurrentCapture(empty), "new entry left the empty whole-set witness current");
    const tracking = set.captureTracking();
    const row = capture.get("note.md")!; row.hash = "caller-edited";
    [...capture.iterate()]; [...set.iterate()]; [...set.paths()];
    tracking.drain(); tracking.dispose();
    set.restore([{ ...hint(), journalId: 6 }]);
    set.restoreJournal([{ ...hint(), journalId: 6 }]);
    check(set.claim(capture, [])?.length === 0 && set.claimHints([])?.length === 0,
        "empty claims unexpectedly failed");
    check(set.claim(capture, ["missing.md"]) === null, "missing path was claimed");
    rejects(() => set.restoreJournal([{ ...hint("new.md"), journalId: 8 }, hint("bad.md")]),
        "invalid replay batch was admitted");
    check(set.commitRetirement(set.captureRetirement([cut("missing.md")])) === 0,
        "missing path acquired retirement authority");
    check(set.isCurrentCapture(capture) && set.isCurrentCapture(second) &&
        set.isCurrentCapture(tracking.capture), "read/no-op operations invalidated a current root");
    set.add(hint("unrelated.md"), 8);
    check(!set.isCurrentCapture(capture), "unrelated path change preserved a whole-set witness");
    check(set.claim(capture, ["note.md"])?.length === 1,
        "whole-set witness changed existing per-entry claim semantics");
}

function wholeSetCaptureWitnessNeverResurrects() {
    const transitions: Array<[string, (set: DirtyPathSet) => void]> = [
        ["equal-looking event", set => set.add({ ...hint(), mtime: 1, size: 2 }, 7)],
        ["new durable ID", set => set.add({ ...hint(), mtime: 1, size: 2 }, 8)],
        ["new no-ID event", set => set.add({ ...hint(), mtime: 1, size: 2 })],
        ["new path", set => set.add(hint("new.md"), 8)],
        ["replayed watermark", set => set.restoreJournal([{ ...hint(), journalId: 8 }])],
        ["restored watermark", set => set.restore([{ ...hint(), journalId: 8 }])],
        ["take", set => { set.take(); }],
        ["take/restore", set => { const taken = set.take(); set.restore(taken); }],
        ["claim/restore", set => { const claimed = set.claim(set.capture(), ["note.md"])!; set.restore(claimed); }],
        ["hint claim/restore", set => { const claimed = set.claimHints([...set.iterate()])!; set.restore(claimed); }],
        ["ACK", set => { check(set.commitRetirement(set.captureRetirement([cut()])) === 1, "expected ACK did not remove entry"); }],
    ];
    for (const [name, transition] of transitions) {
        const set = new DirtyPathSet(); set.add({ ...hint(), mtime: 1, size: 2 }, 7);
        const before = set.capture();
        transition(set);
        check(!set.isCurrentCapture(before), `${name} left the previous whole-set root current`);
        check(set.isCurrentCapture(set.capture()), `${name} prevented a fresh current witness`);
    }
    const empty = new DirtyPathSet(), before = empty.capture();
    empty.take();
    check(!empty.isCurrentCapture(before), "replacing an empty root resurrected its old capture");
}

function atomicBoundedClaims() {
    const set = new DirtyPathSet();
    set.add(hint("a.md"), 7); set.add(hint("b.md"), 7); set.add(hint("c.md"), 8);
    const capture = set.capture();
    set.add(hint("b.md", "later"), 9);
    check(set.claim(capture, ["a.md", "b.md"]) === null && set.size === 3 && set.has("a.md"),
        "stale second rename endpoint partially detached the first endpoint");
    check(set.claim(capture, ["a.md", "missing.md"]) === null && set.has("a.md"),
        "a missing captured endpoint partially detached another path");
    const before = set.captureRetirement([cut("a.md")]);
    const selected = set.claim(capture, ["c.md", "a.md"]);
    check(selected?.map(row => row.path).join() === "c.md,a.md" && set.size === 1 && set.has("b.md"),
        "claim lost caller selection order or removed unselected work");
    set.restore(selected!);
    check([...set.paths()].join() === "b.md,c.md,a.md", "claim restore did not append missing paths in restore order");
    check(set.commitRetirement(before) === 1 && !set.has("a.md"), "exact claimed/restored entry lost its original retirement identity");
    const recaptured = set.capture();
    check(set.claim(recaptured, [])?.length === 0 && set.size === 2, "empty valid claim changed live work");
    check(set.claim({} as DirtyPathCapture, []) === null, "empty foreign claim was accepted");
    const foreign = new DirtyPathSet(); foreign.add(hint("b.md", "later"), 9);
    check(foreign.claim(recaptured, ["b.md"]) === null && foreign.has("b.md"), "foreign capture matched equal-looking owner state");
    check(set.claim(null as unknown as DirtyPathCapture, ["b.md"]) === null, "null capture did not fail closed");

    // A stable entry may have a different CURRENT order slot after a complete
    // take/restore. Claim must delete that slot, not its captured old position.
    const reorder = new DirtyPathSet(); reorder.add(hint("z.md"), 7); reorder.add(hint("a.md"), 8);
    const oldCut = reorder.capture(), taken = reorder.take();
    reorder.restore([taken[1], taken[0]]);
    check([...reorder.paths()].join() === "a.md,z.md", "take/restore did not preserve supplied restore order");
    const claimed = reorder.claim(oldCut, ["z.md"]);
    check(claimed?.length === 1 && [...reorder.paths()].join() === "a.md" && reorder.size === 1,
        "claim deleted a stale order slot after trusted reinsertion");
    reorder.restore(claimed!);
    check([...reorder.paths()].join() === "a.md,z.md", "claim left duplicate order rows after reinsertion");
    check(reorder.take().map(row => row.path).join() === "a.md,z.md", "take and lookup indexes diverged");

    const full = new DirtyPathSet();
    const boundedPaths = Array.from({ length: DIRTY_CLAIM_LIMIT }, (_, index) => `n${index.toString().padStart(3, "0")}.md`);
    for (const path of boundedPaths) full.add(hint(path), 7);
    const maximum = full.claim(full.capture(), boundedPaths);
    check(maximum?.length === DIRTY_CLAIM_LIMIT && full.size === 0, "maximum bounded claim failed");
    full.restore(maximum!);
    check(full.commitRetirement(full.captureRetirement(boundedPaths.map(path => cut(path)))) === DIRTY_CLAIM_LIMIT,
        "maximum claim/restore changed durable provenance");
}

function claimGenerationAndReadProvenance() {
    for (const mode of ["new-id", "no-id", "same-id", "equal-hint", "delete"] as const) {
        const set = new DirtyPathSet(); set.add(hint(), 7);
        const capture = set.capture();
        set.add(mode === "delete" ? { action: "deleted", path: "note.md" } : hint("note.md", mode === "equal-hint" ? "old" : "new"),
            mode === "no-id" ? undefined : mode === "new-id" ? 8 : 7);
        check(set.claim(capture, ["note.md"]) === null && set.size === 1, `claim accepted later ${mode} identity`);
    }
    const noID = new DirtyPathSet(); noID.add(hint(), 7); noID.add(hint("note.md", "scan"));
    const claimedScan = noID.claim(noID.capture(), ["note.md"]);
    check(claimedScan?.[0].journalId === 7 && noID.size === 0, "claim dropped an inherited recovery watermark");
    noID.restore(claimedScan!);
    check(noID.commitRetirement(noID.captureRetirement([cut()])) === 0,
        "claim/restore promoted inherited no-ID watermark to own generation");

    for (const reader of ["get", "iterate", "readonly-iterate"] as const) {
        const set = new DirtyPathSet(); set.add(hint(), 7);
        const capture = set.capture();
        const row = reader === "get" ? capture.get("note.md")! :
            reader === "iterate" ? capture.iterate().next().value! : set.iterate().next().value!;
        set.take(); set.restore([row]);
        check(set.commitRetirement(set.captureRetirement([cut()])) === 1, `${reader} did not retain exact own read provenance`);
        const scan = new DirtyPathSet(); scan.add(hint(), 7); scan.add(hint("note.md", "scan"));
        const read = scan.capture().get("note.md")!; scan.take(); scan.restore([read]);
        check(scan.commitRetirement(scan.captureRetirement([cut()])) === 0, `${reader} fabricated proof for unjournaled state`);
    }
    for (const field of ["action", "hash", "mtime", "size", "journalId", "path"] as const) {
        const set = new DirtyPathSet(); set.add({ ...hint(), mtime: 10, size: 20 }, 7);
        const row = set.capture().get("note.md")!;
        set.take();
        if (field === "action") row.action = "deleted";
        else if (field === "hash") row.hash = "changed";
        else if (field === "path") row.path = "changed.md";
        else row[field] = 8;
        set.restore([row]);
        check(set.commitRetirement(set.captureRetirement([cut(row.path, 10)])) === 0,
            `mutated capture ${field} retained own durable provenance`);
    }
    const origin = new DirtyPathSet(); origin.add(hint(), 7);
    const foreign = new DirtyPathSet(); foreign.restore([...origin.capture().iterate()]);
    check(foreign.commitRetirement(foreign.captureRetirement([cut()])) === 0, "foreign read row transferred durable provenance");

    const live = new DirtyPathSet(); live.add(hint(), 7);
    const selected = live.claim(live.capture(), ["note.md"]);
    live.add(hint("note.md", "newer-scan")); live.restore(selected!);
    check(live.capture().get("note.md")!.hash === "newer-scan" && live.capture().get("note.md")!.journalId === 7,
        "failed claim restore overwrote newer scan or lost old WAL ownership");
    check(live.commitRetirement(live.captureRetirement([cut()])) === 0, "claim failure bestowed old authority on newer scan");

    const replay = new DirtyPathSet(); replay.restoreJournal([{ ...hint(), journalId: 7 }]);
    const replayClaim = replay.claim(replay.capture(), ["note.md"]);
    replay.restore(replayClaim!);
    check(replay.commitRetirement(replay.captureRetirement([cut()])) === 1, "claim lost validated journal replay provenance");
}

function invalidClaimsDoNotMutate() {
    const set = new DirtyPathSet(); set.add(hint("a.md"), 7); set.add(hint("b.md"), 8);
    const capture = set.capture();
    const invalid: unknown[] = [
        null, {}, "a.md", ["a.md", "a.md"], ["a.md", "../unsafe.md"], ["a.md", ""],
        ["a.md", "x".repeat(4097)], ["a.md", 1], ["a.md", null], new Array(2),
        Array.from({ length: DIRTY_CLAIM_LIMIT + 1 }, (_, index) => `n${index}`),
    ];
    for (const paths of invalid) {
        rejects(() => set.claim(capture, paths as string[]), "malformed claim was admitted");
        check(set.size === 2 && [...set.paths()].join() === "a.md,b.md", "invalid claim partially changed the live queue/order");
    }
    const requested = ["a.md", "b.md"];
    const selected = set.claim(capture, requested)!;
    requested[0] = "elsewhere.md"; requested.length = 0;
    check(selected.map(row => row.path).join() === "a.md,b.md" && set.size === 0, "claim retained caller-owned path array");
    set.restore(selected);
    const newer = set.capture();
    const reentrant = ["a.md", "b.md"];
    Object.defineProperty(reentrant, 1, { get: () => {
        set.add(hint("b.md", "reentrant"), 9); return "b.md";
    } });
    check(set.claim(newer, reentrant) === null && set.has("a.md") && set.capture().get("b.md")!.hash === "reentrant",
        "caller getter mutation escaped all-or-none current-entry comparison");

    const overflow = new DirtyPathSet(); overflow.add(hint(), 7);
    (overflow as unknown as { nextOrder: number }).nextOrder = Number.MAX_SAFE_INTEGER;
    rejects(() => overflow.add(hint("another.md"), 8), "unsafe insertion order counter was reused");
    check(overflow.size === 1 && [...overflow.paths()].join() === "note.md", "sequence exhaustion partially published an index");
    overflow.add(hint("note.md", "updated"), 8);
    check(overflow.capture().get("note.md")!.hash === "updated", "sequence exhaustion blocked an existing slot update");
    overflow.take(); overflow.add(hint("new.md"), 9);
    check([...overflow.paths()].join() === "new.md", "empty queue did not safely reset insertion sequence");
}

function callerIterationCannotExpandClaim() {
    const set = new DirtyPathSet();
    set.add(hint("a.md"), 7); set.add(hint("b.md"), 8); set.add(hint("c.md"), 9);
    const custom = ["a.md", "b.md"];
    custom[Symbol.iterator] = function* () {
        throw new Error("caller iterator must not control bounded claim validation");
    };
    const selected = set.claim(set.capture(), custom)!;
    check(selected.length === 2 && set.size === 1 && set.has("c.md"), "custom iterator expanded or controlled claim admission");
    set.restore(selected);
    const growing = ["a.md", "b.md"];
    Object.defineProperty(growing, 0, { get: () => {
        for (let index = 0; index < 1_000; index++) growing.push("c.md");
        return "a.md";
    } });
    const bounded = set.claim(set.capture(), growing)!;
    check(growing.length === 1_002 && bounded.length === 2 && set.size === 1 && set.has("c.md"),
        "dynamic array length expanded the original bounded indexed cut");
    set.restore(bounded);
    const shrinking = ["a.md", "b.md"];
    Object.defineProperty(shrinking, 0, { get: () => { shrinking.length = 1; return "a.md"; } });
    rejects(() => set.claim(set.capture(), shrinking), "shrinking caller array produced a partially validated cut");
    check(set.size === 3 && set.has("a.md") && set.has("b.md"), "shrinking caller input partially claimed live entries");
}

function largeLazyStructuralCapture() {
    const set = new DirtyPathSet();
    const pathAt = (index: number) => `n${index.toString().padStart(5, "0")}.md`;
    // Reverse insertion makes accidental path-sorted iteration observable.
    for (let index = 24_999; index >= 0; index--) set.add(hint(pathAt(index)), index + 1);
    const privateSet = set as unknown as {
        snapshots: WeakMap<DirtyFileChange, unknown>;
        changes: { root: TestNode | null };
        ordered: { root: TestNode | null };
    };
    const oldPathRoot = privateSet.changes.root, oldOrderRoot = privateSet.ordered.root;
    let detached = 0;
    const remember = privateSet.snapshots.set;
    privateSet.snapshots.set = function (key, value) { detached++; return remember.call(this, key, value); };
    const tracking = set.captureTracking();
    check(detached === 0 && tracking.capture.size === 25_000 && tracking.snapshot().pendingPaths === 0,
        "observer installation copied the historical 25k backlog");
    const captured = set.capture(), iterator = captured.iterate(), direct = set.iterate();
    check(captured.size === 25_000 && detached === 0, "capture/iterator eagerly detached the 25k backlog");
    const guardedIndexes = [privateSet.changes, privateSet.ordered].map(index => ({
        index, descriptor: Object.getOwnPropertyDescriptor(index, "root")!,
    }));
    try {
        for (const { index } of guardedIndexes) Object.defineProperty(index, "root", {
            configurable: true, get() { throw new Error("whole-set witness traversed an AVL root"); },
        });
        for (let count = 0; count < 1_000; count++) {
            if (!set.isCurrentCapture(captured)) throw new Error("unchanged 25k capture was rejected");
        }
        check(detached === 0, "whole-set witness materialized captured rows");
    } finally {
        for (const { index, descriptor } of guardedIndexes) Object.defineProperty(index, "root", descriptor);
    }
    check(iterator.next().value!.path === pathAt(24_999) && detached === 1, "captured iteration was not lazy/in insertion order");
    check(direct.next().value!.path === pathAt(24_999) && detached === 2, "readonly iteration eagerly materialized queue rows");
    check(captured.get(pathAt(100))!.journalId === 101 && detached === 3, "point lookup detached more than one row");
    set.add(hint(pathAt(12_345), "new"), 30_000);
    const changed = tracking.drain();
    check(changed.paths.join() === pathAt(12_345) && detached === 3,
        "one observed change/drain materialized unrelated backlog hints");
    check(captured.get(pathAt(12_345))!.hash === "old" && set.capture().get(pathAt(12_345))!.hash === "new",
        "large capture did not retain its immutable source cut");
    for (const [oldRoot, newRoot] of [[oldPathRoot, privateSet.changes.root], [oldOrderRoot, privateSet.ordered.root]]) {
        const previous = treeNodes(oldRoot), current = treeNodes(newRoot);
        let copied = 0;
        for (const node of current) if (!previous.has(node)) copied++;
        check(previous.size === 25_000 && current.size === 25_000 && copied > 0 && copied < 64,
            "single update copied the backlog instead of an AVL path");
    }
    const selectedPaths = Array.from({ length: DIRTY_CLAIM_LIMIT }, (_, index) => pathAt(index));
    const beforeClaim = detached, selected = set.claim(captured, selectedPaths);
    check(selected?.length === DIRTY_CLAIM_LIMIT && detached - beforeClaim === DIRTY_CLAIM_LIMIT && set.size === 25_000 - DIRTY_CLAIM_LIMIT,
        "bounded claim walked/detached unrelated 25k backlog hints");
    check(captured.size === 25_000 && captured.get(pathAt(0)) !== undefined && !set.has(pathAt(0)),
        "bounded claim mutated its captured root");
    set.restore(selected!);
    check(set.size === 25_000 && set.claim(captured, [pathAt(12_345)]) === null,
        "bounded restore lost live work or made a later update current in an old capture");
    check([...set.paths()].slice(-DIRTY_CLAIM_LIMIT).join() === selectedPaths.join(),
        "large bounded restore changed chronological insertion semantics");
}

interface TestNode { left: TestNode | null; right: TestNode | null }
function treeNodes(root: TestNode | null): Set<TestNode> {
    const seen = new Set<TestNode>(), pending = root ? [root] : [];
    while (pending.length > 0) {
        const node = pending.pop()!; seen.add(node);
        if (node.left) pending.push(node.left);
        if (node.right) pending.push(node.right);
    }
    return seen;
}

function trackingCapturesAndCoalesces() {
    const set = new DirtyPathSet(); set.add(hint("old.md"), 7);
    const tracking = set.captureTracking();
    check(Object.isFrozen(tracking) && tracking.capture.size === 1 && tracking.snapshot().limit === 256,
        "tracking did not install with a stable default-bounded capture");
    check(tracking.snapshot().active && !tracking.snapshot().overflow && tracking.snapshot().pendingPaths === 0,
        "initial captured backlog was reported as new mutations");
    set.add(hint("new.md", "first"), 8);
    for (let index = 0; index < 5_000; index++) set.add(hint("old.md", `update-${index}`));
    check(tracking.snapshot().pendingPaths === 2 && !tracking.snapshot().overflow,
        "typing retained event history instead of coalesced paths");
    const cut = tracking.drain();
    check(Object.isFrozen(cut) && Object.isFrozen(cut.paths) && cut.active && !cut.overflow,
        "drained tracking cut was mutable or invalid");
    check(cut.paths.join() === "new.md,old.md" && cut.capture.size === 2 &&
        cut.capture.get("old.md")!.hash === "update-4999", "drain did not capture the latest state alongside exact changed paths");
    check(tracking.capture.size === 1 && tracking.capture.get("old.md")!.hash === "old",
        "drain changed the original capture");
    check(tracking.snapshot().pendingPaths === 0 && tracking.drain().paths.length === 0,
        "drain retained previously consumed notifications");
    rejects(() => (cut.paths as string[]).push("forged.md"), "caller could mutate a drained changed-path cut");
    rejects(() => Object.assign(tracking, { capture: set.capture() }), "caller could replace the tracking capture");
    const row = cut.capture.get("old.md")!; row.hash = "caller-edited";
    check(set.capture().get("old.md")!.hash === "update-4999", "drain exposed mutable live entry metadata");
    set.add(hint("new.md", "first"), 8); // Equal metadata is still a new identity.
    const next = tracking.drain();
    check(next.paths.join() === "new.md" && next.capture.get("new.md")!.hash === "first",
        "equal-looking later event was omitted from tracking");
    check(set.claim(cut.capture, ["new.md"]) === null, "old tracked capture claimed an equal-looking newer generation");
    check(Object.keys(tracking.snapshot()).sort().join() === "active,limit,overflow,pendingPaths" &&
        Object.isFrozen(tracking.snapshot()), "tracking diagnostics exposed paths/records or mutable observer state");
}

function trackingDistinguishesOwnedRestores() {
    const set = new DirtyPathSet(); set.add(hint(), 7);
    const tracking = set.captureTracking();
    const taken = set.take(); set.restore(taken);
    check(tracking.drain().paths.length === 0, "unchanged own take/restore looked like a new external edit");
    const claimed = set.claim(tracking.capture, ["note.md"])!; set.restore(claimed);
    check(tracking.drain().paths.length === 0, "unchanged own claim/restore looked like a new external edit");
    const read = set.capture().get("note.md")!; set.take(); set.restore([read]);
    check(tracking.drain().paths.length === 0, "unchanged own read-proof restore looked like a new external edit");
    check(set.commitRetirement(set.captureRetirement([cut()])) === 1 && tracking.drain().paths.length === 0,
        "owned ACK removal was reported as new local work");

    const external = { ...hint("external.md"), journalId: 8, data: new Uint8Array(100) };
    set.restore([external]); external.hash = "caller-edited";
    const created = tracking.drain();
    check(created.paths.join() === "external.md" && created.capture.get("external.md")!.hash === "old" &&
        !("data" in created.capture.get("external.md")!), "ordinary external restore was not tracked/detached");
    set.restore([{ ...hint("external.md", "ignored-older"), journalId: 7 }]);
    check(tracking.drain().paths.length === 0, "no-op older restore caused a false mutation");
    set.restore([{ ...hint("external.md", "ignored-newer-watermark"), journalId: 9 }]);
    const watermark = tracking.drain();
    check(watermark.paths.join() === "external.md" && watermark.capture.get("external.md")!.journalId === 9 &&
        watermark.capture.get("external.md")!.hash === "old", "carried watermark change failed to invalidate its current row");
    check(set.commitRetirement(set.captureRetirement([cut("external.md", 9)])) === 0,
        "observed ordinary restore gained durable provenance");

    set.add(hint("race.md"), 10); tracking.drain();
    const old = set.claim(set.capture(), ["race.md"])!;
    set.add(hint("race.md", "new-scan")); tracking.drain();
    set.restore(old);
    const carried = tracking.drain();
    check(carried.paths.join() === "race.md" && carried.capture.get("race.md")!.hash === "new-scan" &&
        carried.capture.get("race.md")!.journalId === 10, "trusted old restore silently changed newer no-ID watermark");
    check(set.commitRetirement(set.captureRetirement([cut("race.md", 10)])) === 0,
        "tracking a carried watermark promoted newer scan ownership");
    set.restore(old);
    check(tracking.drain().paths.length === 0, "repeating a no-op trusted restore generated a mutation");

    const modified = set.claim(set.capture(), ["race.md"])!;
    modified[0].hash = "changed-owned-input"; set.restore(modified);
    check(tracking.drain().paths.join() === "race.md", "modified own detached input was suppressed as a trusted restore");
    const other = new DirtyPathSet(); other.add(hint("foreign.md"), 11);
    set.restore([...other.iterate()]);
    check(tracking.drain().paths.join() === "foreign.md", "foreign owner proof was incorrectly treated as own restoration");
    check(set.commitRetirement(set.captureRetirement([cut("foreign.md", 11)])) === 0,
        "observed foreign restore gained another owner's provenance");
}

function trackingReplayAndFailedMutation() {
    const set = new DirtyPathSet(), tracking = set.captureTracking();
    set.restoreJournal([{ ...hint(), journalId: 7 }]);
    check(tracking.drain().paths.join() === "note.md", "journal replay inserted unobserved work");
    set.restoreJournal([{ ...hint(), journalId: 6 }]);
    check(tracking.drain().paths.length === 0, "no-op journal replay caused a false mutation");
    set.restoreJournal([{ ...hint(), journalId: 8 }]);
    check(tracking.drain().paths.join() === "note.md", "higher journal watermark did not invalidate the captured row");
    check(set.commitRetirement(set.captureRetirement([cut("note.md", 8)])) === 1,
        "tracking changed trusted replay retirement semantics");
    check(tracking.drain().paths.length === 0, "replay ACK removal was reported as new work");
    set.add(hint("scan.md", "live-no-id")); tracking.drain();
    set.restoreJournal([{ ...hint("scan.md", "old-journal"), journalId: 9 }]);
    const merged = tracking.drain();
    check(merged.paths.join() === "scan.md" && merged.capture.get("scan.md")!.hash === "live-no-id",
        "replay did not track carried watermark while retaining newer live bytes");
    check(set.commitRetirement(set.captureRetirement([cut("scan.md", 9)])) === 0, "observed replay promoted a newer live no-ID hint");
    rejects(() => set.restoreJournal([{ ...hint("valid.md"), journalId: 10 }, hint("bad.md")]),
        "invalid replay batch was accepted while observing");
    check(!set.has("valid.md") && tracking.drain().paths.length === 0, "failed replay emitted phantom mutation or partial dirty state");
    (set as unknown as { nextOrder: number }).nextOrder = Number.MAX_SAFE_INTEGER;
    rejects(() => set.add(hint("unallocated.md"), 10), "failed insertion was admitted");
    check(tracking.drain().paths.length === 0 && !set.has("unallocated.md"), "failed dirty insertion produced an observer notification");

    const failure = new DirtyPathSet(), observer = failure.captureTracking();
    failure.add(hint("previous.md"), 11);
    const state = (failure as unknown as { tracking: { paths: Set<string> } }).tracking;
    state.paths.add = () => { throw new Error("tracking allocation failed"); };
    rejects(() => failure.add(hint("durable.md"), 12), "tracking failure was swallowed as a valid update");
    check(failure.has("durable.md") && observer.snapshot().overflow && observer.snapshot().pendingPaths === 0 && observer.drain().overflow,
        "tracking failure dropped authoritative work or left the old session valid");
    const recovered = failure.captureTracking();
    check(recovered.capture.get("durable.md")!.journalId === 12 && recovered.snapshot().active && !recovered.snapshot().overflow,
        "new full capture failed to recover work after tracking allocation failure");
}

function trackingOverflowAndLifecycle() {
    const set = new DirtyPathSet(), tracking = set.captureTracking(2);
    set.add(hint("a.md"), 1); set.add(hint("b.md"), 2);
    for (let index = 0; index < 2_000; index++) set.add(hint("a.md", `${index}`));
    check(tracking.snapshot().pendingPaths === 2 && !tracking.snapshot().overflow,
        "duplicate changes overflowed a full coalescing observer");
    set.add(hint("c.md"), 3);
    check(tracking.snapshot().overflow && tracking.snapshot().pendingPaths === 0 && set.size === 3,
        "capacity overflow dropped dirty work or retained a misleading partial delta");
    for (let index = 0; index < 1_000; index++) set.add(hint(`later-${index}.md`), index + 4);
    const invalid = tracking.drain();
    check(invalid.active && invalid.overflow && invalid.paths.length === 0 && invalid.capture === tracking.capture && set.size === 1_003,
        "overflow was cleared by drain or fabricated a usable replacement cut");
    check(tracking.drain().overflow && tracking.snapshot().pendingPaths === 0,
        "overflow was not sticky or continued retaining path history");
    const replacement = set.captureTracking(1);
    check(replacement.capture.size === 1_003 && replacement.snapshot().active && !replacement.snapshot().overflow,
        "replacement capture failed to preserve the full authoritative queue");
    check(!tracking.snapshot().active && !tracking.drain().active, "replacement left the old observer active");
    tracking.dispose(); set.add(hint("after-replace.md"), 2_000);
    check(replacement.drain().paths.join() === "after-replace.md", "disposing a stale observer detached its replacement");
    replacement.dispose(); replacement.dispose();
    set.add(hint("after-dispose.md"), 2_001);
    check(!replacement.snapshot().active && replacement.snapshot().pendingPaths === 0 && set.has("after-dispose.md"),
        "disposed observer retained notifications or prevented new dirty work");
    check(replacement.drain().capture === replacement.capture && !replacement.drain().active,
        "stale observer drain manufactured a fresh apparently active capture");

    const current = set.captureTracking(3); set.add(hint("pending.md"), 2_002);
    for (const limit of [0, -1, NaN, Infinity, 1.5, 1_025, Number.MAX_SAFE_INTEGER]) {
        rejects(() => set.captureTracking(limit), "invalid tracking bound accepted");
        check(current.snapshot().active && current.snapshot().pendingPaths === 1,
            "invalid replacement lost the existing observer or its pending delta");
    }
    const anotherOwner = new DirtyPathSet(), another = anotherOwner.captureTracking();
    anotherOwner.add(hint("elsewhere.md"), 1); another.dispose();
    check(current.drain().paths.join() === "pending.md", "another dirty owner interfered with this observer");
    const bounded = set.captureTracking(DIRTY_TRACKING_LIMIT);
    for (let index = 0; index < DIRTY_TRACKING_LIMIT; index++) set.add(hint(`bounded-${index}.md`));
    check(bounded.snapshot().pendingPaths === DIRTY_TRACKING_LIMIT && bounded.drain().paths.length === DIRTY_TRACKING_LIMIT,
        "maximum observer bound could not be drained as one bounded metadata cut");

    const unsafe = new DirtyPathSet(), unsafeTracking = unsafe.captureTracking();
    unsafe.add(hint("../unsupported.md"));
    check(unsafe.has("../unsupported.md") && unsafeTracking.snapshot().overflow && unsafeTracking.drain().paths.length === 0,
        "unrepresentable observed path was dropped or treated as a valid review delta");
}

function claimOriginalHintsWithoutHistoricalRoots() {
    const set = new DirtyPathSet(); set.add(hint("a.md"), 7); set.add(hint("b.md"), 7); set.add(hint("c.md"), 8);
    const rows = [...set.iterate()];
    const token = set.captureRetirement([cut("a.md")]);
    const selected = set.claimHints([rows[1], rows[0]])!;
    check(selected.map(row => row.path).join() === "b.md,a.md" && set.size === 1 && set.has("c.md"),
        "owner hint claim changed order or removed unselected work");
    check(selected[0] !== rows[1] && selected[1] !== rows[0], "hint claim returned caller-owned mutable rows");
    set.restore(selected);
    check(set.commitRetirement(token) === 1, "hint claim/restore lost exact original retirement identity");
    check(set.claimHints([rows[0], rows[1]]) === null && set.has("b.md"), "missing first claimed hint allowed partial removal");
    set.add(hint("b.md"), 7);
    check(set.claimHints([rows[1]]) === null && set.has("b.md"), "equal-looking newer entry was claimed by an old read proof");
    const current = set.capture().get("b.md")!;
    check(set.claimHints([{ ...current }]) === null && set.has("b.md"), "copied scalar hint forged owner proof");
    const foreign = new DirtyPathSet(); foreign.add(hint("b.md"), 7);
    check(foreign.claimHints([current]) === null && foreign.has("b.md"), "foreign hint matched another owner's equal state");
    const noID = new DirtyPathSet(); noID.add(hint(), 7); noID.add(hint("note.md", "scan"));
    const scan = noID.capture().get("note.md")!, scanClaim = noID.claimHints([scan])!;
    check(scanClaim.length === 1 && scanClaim[0].journalId === 7, "no-ID hint claim lost its inherited WAL watermark");
    noID.restore(scanClaim);
    check(noID.commitRetirement(noID.captureRetirement([cut()])) === 0, "hint claim granted ACK authority to inherited no-ID state");
    check(set.claimHints([])?.length === 0 && set.size === 2, "empty hint claim changed state");

    for (const field of ["action", "hash", "mtime", "size", "journalId", "path"] as const) {
        const owner = new DirtyPathSet(); owner.add({ ...hint(), mtime: 10, size: 20 }, 7);
        const row = owner.capture().get("note.md")!;
        if (field === "action") row.action = "deleted";
        else if (field === "hash") row.hash = "changed";
        else if (field === "path") row.path = "changed.md";
        else row[field] = 8;
        check(owner.claimHints([row]) === null && owner.has("note.md"), `scalar-mutated ${field} hint retained claim authority`);
    }
    const plain = new DirtyPathSet(); plain.add(hint(), 7);
    check(plain.claimHints([{ ...hint(), journalId: 7 }]) === null && plain.size === 1, "unproven external hint granted a claim");
    for (const invalid of [null, {}, [null], [hint("../unsafe.md")], new Array(2),
        [current, current], Array.from({ length: 257 }, () => current)]) {
        rejects(() => set.claimHints(invalid as DirtyFileChange[]), "malformed hint claim was admitted");
        check(set.size === 2 && set.has("b.md") && set.has("c.md"), "malformed hint claim partially removed live work");
    }
    const fresh = [...set.iterate()], custom = [fresh[0], fresh[1]];
    custom[Symbol.iterator] = function* () { throw new Error("custom hint iterator must not be used"); };
    const limited = set.claimHints(custom)!;
    check(limited.length === 2 && set.size === 0, "custom array iterator controlled bounded hint claim");
    set.restore(limited);
    const growing = [...set.iterate()];
    const first = growing[0];
    Object.defineProperty(growing, 0, { get: () => {
        for (let index = 0; index < 1_000; index++) growing.push(first);
        return first;
    } });
    const bounded = set.claimHints(growing)!;
    check(growing.length === 1_002 && bounded.length === 2 && set.size === 0,
        "dynamic array growth expanded hint-claim admission");
    set.restore(bounded);

    const race = new DirtyPathSet(); race.add(hint("a.md"), 1); race.add(hint("b.md"), 2);
    const raceRows = [...race.iterate()];
    Object.defineProperty(raceRows[1], "hash", { get: () => { race.add(hint("a.md", "newer"), 3); return "old"; } });
    check(race.claimHints(raceRows) === null && race.has("a.md") && race.has("b.md") &&
        race.capture().get("a.md")!.hash === "newer", "later input getter bypassed all-or-none current-entry comparison");

    const max = new DirtyPathSet();
    for (let index = 0; index < 256; index++) max.add(hint(`n${index.toString().padStart(3, "0")}`), index + 1);
    const hints = [...max.iterate()], claimed = max.claimHints(hints)!;
    check(claimed.length === 256 && max.size === 0, "maximum exact hint cut failed");
    max.restore(claimed);
    check(max.commitRetirement(max.captureRetirement(hints.map(row => cut(row.path, row.journalId!)))) === 256,
        "maximum hint claim/restore changed durable provenance");
}

retirementCuts(); laterEvents(); trustedTakeRestore(); replayAndRenameCuts(); boundsAndAtomicValidation();
immutableReadCaptures(); atomicBoundedClaims(); claimGenerationAndReadProvenance(); invalidClaimsDoNotMutate();
wholeSetCaptureWitnessIsOwnerLocalAndReadOnly(); wholeSetCaptureWitnessNeverResurrects();
callerIterationCannotExpandClaim(); largeLazyStructuralCapture();
trackingCapturesAndCoalesces(); trackingDistinguishesOwnedRestores(); trackingReplayAndFailedMutation(); trackingOverflowAndLifecycle();
claimOriginalHintsWithoutHistoricalRoots();
console.log(`dirty-set.test: ${assertions} assertions passed`);
