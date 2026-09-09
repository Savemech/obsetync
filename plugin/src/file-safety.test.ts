import {
    collectAdapterFileStats,
    isMissingPathError,
    materializeFileChanges,
    PathIsDirectoryError,
    readFileStat,
    type AdapterPathStat,
    type FileChangeOmissionReason,
    type FileStat,
} from "./file-safety";
import { DirtyPathSet, type DirtyFileChange } from "./dirty-set";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

const file = (size = 10, mtime = 20): AdapterPathStat => ({ type: "file", size, mtime });
const folder = (): AdapterPathStat => ({ type: "folder", size: 0, mtime: 20 });
const ioError = (code: string): Error & { code: string } =>
    Object.assign(new Error(`simulated ${code}`), { code });

async function rejectsWith(
    promise: Promise<unknown>,
    expected: unknown,
    message: string,
): Promise<void> {
    let thrown: unknown;
    try { await promise; } catch (error) { thrown = error; }
    check(thrown === expected, message);
}

async function distinctStatOutcomes(): Promise<void> {
    const regular = await readFileStat("note.md", async () => file());
    check(regular?.size === 10 && regular.mtime === 20, "regular file metadata changed");
    check(await readFileStat("missing.md", async () => null) === null, "explicit absence was rejected");
    check(
        await readFileStat("missing.md", async () => { throw ioError("ENOENT"); }) === null,
        "ENOENT was not treated as confirmed absence",
    );
    let directory: unknown;
    try { await readFileStat("notes", async () => folder()); } catch (error) { directory = error; }
    check(directory instanceof PathIsDirectoryError, "folder type was discarded");
    check((directory as PathIsDirectoryError).path === "notes", "directory error lost its path");
    for (const code of ["EIO", "EACCES", "EPERM", "ENOTDIR", "EISDIR"]) {
        const error = ioError(code);
        await rejectsWith(
            readFileStat("note.md", async () => { throw error; }),
            error,
            `${code} became absent-file metadata`,
        );
    }
    const messageOnly = new Error("ENOENT: cannot access file");
    await rejectsWith(
        readFileStat("note.md", async () => { throw messageOnly; }),
        messageOnly,
        "an ambiguous error message was treated as confirmed deletion",
    );
    check(!isMissingPathError(null), "null is not an ENOENT error");
    for (const invalid of [undefined, { ...file(), size: NaN }, { ...file(), type: "unknown" }]) {
        let rejected = false;
        try { await readFileStat("bad", async () => invalid as AdapterPathStat); } catch { rejected = true; }
        check(rejected, "invalid adapter metadata became file/missing state");
    }
}

class ListingAdapter {
    stats = new Map<string, AdapterPathStat>([
        [".obsidian", folder()],
        [".obsidian/app.json", file()],
        [".obsidian/themes", folder()],
        [".obsidian/themes/theme.css", file(30)],
    ]);
    listings = new Map<string, { files: string[]; folders: string[] }>([
        [".obsidian", { files: [".obsidian/app.json"], folders: [".obsidian/themes"] }],
        [".obsidian/themes", { files: [".obsidian/themes/theme.css"], folders: [] }],
    ]);
    statErrors = new Map<string, Error>();
    listErrors = new Map<string, Error>();
    async stat(path: string): Promise<AdapterPathStat | null> {
        const error = this.statErrors.get(path);
        if (error) throw error;
        return this.stats.get(path) ?? null;
    }
    async list(path: string): Promise<{ files: string[]; folders: string[] }> {
        const error = this.listErrors.get(path);
        if (error) throw error;
        const listing = this.listings.get(path);
        if (!listing) throw ioError("ENOENT");
        return listing;
    }
}

async function listingsFailClosed(): Promise<void> {
    const complete = await collectAdapterFileStats(".obsidian", new ListingAdapter());
    check(complete.size === 2, "recursive listing omitted a file or included a folder");
    check(complete.get(".obsidian/themes/theme.css")?.size === 30, "nested file stat was lost");

    for (const code of ["EIO", "EACCES", "ENOENT"]) {
        const adapter = new ListingAdapter();
        const error = ioError(code);
        adapter.listErrors.set(".obsidian/themes", error);
        await rejectsWith(
            collectAdapterFileStats(".obsidian", adapter), error,
            `nested ${code} returned a partial authoritative snapshot`,
        );
    }
    for (const code of ["EIO", "EACCES"]) {
        const adapter = new ListingAdapter();
        const error = ioError(code);
        adapter.statErrors.set(".obsidian/themes/theme.css", error);
        await rejectsWith(
            collectAdapterFileStats(".obsidian", adapter), error,
            `listed file ${code} was silently omitted`,
        );
        adapter.statErrors.set(".obsidian", error);
        await rejectsWith(
            collectAdapterFileStats(".obsidian", adapter), error,
            `root ${code} became an empty configuration tree`,
        );
    }

    const moved = new ListingAdapter();
    moved.stats.delete(".obsidian/themes/theme.css");
    let rejected = false;
    try { await collectAdapterFileStats(".obsidian", moved); } catch { rejected = true; }
    check(rejected, "disappearing listed file produced a truncated authoritative snapshot");

    const missingRoot = new ListingAdapter();
    missingRoot.stats.delete(".obsidian");
    check((await collectAdapterFileStats(".obsidian", missingRoot)).size === 0, "confirmed missing root failed");
    missingRoot.statErrors.set(".obsidian", ioError("ENOENT"));
    check((await collectAdapterFileStats(".obsidian", missingRoot)).size === 0, "root ENOENT failed");
}

async function dirtyMaterialization(): Promise<void> {
    const states = new Map<string, AdapterPathStat>([
        ["stale-folder", folder()], ["replaced.md", folder()],
        ["existing.md", file()], ["resized.md", file(11)],
        ["created.md", file()],
    ]);
    let statsRead = 0;
    const changes = await materializeFileChanges([
        { action: "deleted", path: "stale-folder" },
        { action: "modified", path: "replaced.md" },
        { action: "deleted", path: "existing.md", hash: "same", size: 10, mtime: 20 },
        { action: "modified", path: "resized.md", hash: "stale", size: 10, mtime: 20 },
        { action: "modified", path: "missing.md" },
        { action: "deleted", path: "created.md" },
        { action: "modified", path: "ignored.md" },
    ], (path) => {
        statsRead++;
        return readFileStat(path, async () => states.get(path) ?? null);
    }, (path) => !["stale-folder", "created.md"].includes(path), (path) => path === "ignored.md");
    check(statsRead === 6, "excluded path was inspected");
    check(!changes.some((change) => change.path === "stale-folder"), "untracked folder was published");
    check(changes.find((change) => change.path === "replaced.md")?.action === "deleted", "file→folder lost tracked removal");
    check(changes.find((change) => change.path === "missing.md")?.action === "deleted", "confirmed removal was lost");
    check(changes.find((change) => change.path === "existing.md")?.action === "modified", "stale delete overrode current file");
    check(changes.find((change) => change.path === "existing.md")?.hash === "same", "unchanged hash hint was discarded");
    check(changes.find((change) => change.path === "resized.md")?.hash === undefined, "stale hash survived size drift");
    check(changes.find((change) => change.path === "created.md")?.action === "created", "delete→create final state was lost");
}

async function failedStatsPreserveNewerGeneration(): Promise<void> {
    const dirty = new DirtyPathSet();
    dirty.add({ action: "modified", path: "race.md", hash: "old", size: 10, mtime: 20 }, 7);
    const snapshot = dirty.take();
    const error = ioError("EIO");
    const materializing = materializeFileChanges(snapshot, async () => {
        dirty.add({ action: "created", path: "race.md", hash: "new", size: 11, mtime: 21 }, 8);
        throw error;
    }, () => true, () => false);
    await rejectsWith(materializing, error, "stat failure was converted to a publishable change");
    dirty.restore(snapshot);
    const retried = dirty.take();
    check(retried.length === 1, "failed materialization duplicated dirty paths");
    check(retried[0].hash === "new" && retried[0].action === "created", "retry overwrote newer file state");
    check(retried[0].journalId === 8, "retry cleared the newer journal watermark");
}

async function explicitMaterializationOmissions(): Promise<void> {
    const omissions: Array<{ path: string; reason: FileChangeOmissionReason }> = [];
    const observe = (path: string, reason: FileChangeOmissionReason) => { omissions.push({ path, reason }); };
    let reads = 0, trackedLookups = 0;
    const excluded = await materializeFileChanges([{ action: "modified", path: "excluded.md" }], async () => {
        reads++; throw ioError("EIO");
    }, () => { trackedLookups++; return true; }, () => true, observe);
    check(excluded.length === 0 && reads === 0 && trackedLookups === 0, "excluded omission inspected source bytes/stat or base");
    check(omissions.length === 1 && omissions[0].path === "excluded.md" && omissions[0].reason === "excluded", "positive exclusion was not classified explicitly");
    omissions.length = 0;
    const directories = await materializeFileChanges([
        { action: "modified", path: "tracked.md" }, { action: "created", path: "untracked-dir" },
    ], async path => { throw new PathIsDirectoryError(path); }, path => path === "tracked.md", () => false, observe);
    check(directories.length === 1 && directories[0].path === "tracked.md" && directories[0].action === "deleted", "tracked file-to-directory removal changed");
    check(omissions.length === 1 && omissions[0].path === "untracked-dir" && omissions[0].reason === "untracked-directory", "typed untracked directory was not the sole directory omission");
    for (const code of ["EIO", "EACCES", "EPERM", "ENOENT", "EISDIR", "ENOTDIR"]) {
        omissions.length = 0; const error = ioError(code);
        await rejectsWith(materializeFileChanges([{ action: "created", path: "uncertain.md" }], async () => { throw error; },
            () => false, () => false, observe), error, `${code} was swallowed during omission classification`);
        check(omissions.length === 0, `${code} created a false positive omission`);
    }
    omissions.length = 0;
    const absent = await materializeFileChanges([{ action: "created", path: "absent.md" }], async () => null,
        () => false, () => false, observe);
    check(absent.length === 1 && absent[0].action === "deleted" && omissions.length === 0, "confirmed absence was misclassified as an omission");
    const callbackError = new Error("observation callback failed");
    await rejectsWith(materializeFileChanges([{ action: "modified", path: "excluded.md" }], async () => null,
        () => false, () => true, () => { throw callbackError; }), callbackError, "observation failure allowed materialization to appear complete");
    const mutable = { action: "created" as const, path: "original-dir" };
    const result = await materializeFileChanges([mutable], async path => {
        mutable.path = "unrelated.md"; throw new PathIsDirectoryError(path);
    }, () => false, () => false, observe);
    check(result.length === 0 && omissions.at(-1)?.path === "original-dir", "awaited directory classification moved to a caller-mutated path");
}

async function materializationCooperatesAcrossSkippedPaths(): Promise<void> {
    const changes = Array.from({ length: 513 }, (_, index) => ({ action: "modified" as const, path: `ignored-${index}.md` }));
    const boundaries: number[] = [];
    let omitted = 0;
    const result = await materializeFileChanges(changes,
        async () => { throw new Error("excluded paths must not be read"); }, () => false, () => true,
        () => { omitted++; }, async () => { boundaries.push(omitted); });
    check(result.length === 0 && omitted === 513, "cooperation changed explicit policy omission semantics");
    check(JSON.stringify(boundaries) === "[256,512]", "skipped metadata did not yield in bounded path groups");
    omitted = 0;
    const failure = new Error("materialization cooperation cancelled");
    let caught: unknown;
    try {
        await materializeFileChanges(changes, async () => null, () => false, () => true,
            () => { omitted++; }, async () => { throw failure; });
    } catch (error) { caught = error; }
    check(caught === failure && omitted === 256, "cancelled planning entered another metadata group or hid its failure");
}

function gate<T>() {
    let resolve!: (value: T) => void;
    let reject!: (reason: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
const nextTurn = () => new Promise<void>(resolve => setImmediate(resolve));
const hints = (count: number): DirtyFileChange[] => Array.from({ length: count }, (_, index) => ({
    action: "modified", path: `${index}.md`, hash: `hash-${index}`, mtime: 20, size: 10,
}));

async function concurrentMaterializationIsOrdered(): Promise<void> {
    const changes = hints(8), gates = new Map<string, ReturnType<typeof gate<FileStat | null>>>();
    const omissions: string[] = [], calls: string[] = [];
    const pending = materializeFileChanges(changes, path => {
        calls.push(path); const held = gate<FileStat | null>(); gates.set(path, held); return held.promise;
    }, path => !["0.md", "3.md"].includes(path), path => ["1.md", "5.md"].includes(path),
    (path, reason) => omissions.push(`${path}:${reason}`), undefined, { readConcurrency: () => 8 });
    check(calls.join() === "0.md,2.md,3.md,4.md,6.md,7.md", "concurrent materialization inspected excluded paths or reordered admission");
    gates.get("7.md")!.resolve(file(11));
    gates.get("6.md")!.resolve(file());
    gates.get("4.md")!.reject(new PathIsDirectoryError("4.md"));
    gates.get("3.md")!.resolve(file());
    gates.get("2.md")!.resolve(null);
    gates.get("0.md")!.reject(new PathIsDirectoryError("0.md"));
    const result = await pending;
    check(result.map(row => row.path).join() === "2.md,3.md,4.md,6.md,7.md", "reverse completion reordered final changes");
    check(result.map(row => row.action).join() === "deleted,created,deleted,modified,modified", "parallel directory/missing/tracked classification changed");
    check(result[3].hash === "hash-6" && result[4].hash === undefined, "parallel stat drift reused an invalid hash hint");
    check(omissions.join() === "0.md:untracked-directory,1.md:excluded,5.md:excluded", "reverse completion reordered omission callbacks");
    check(result.every(row => !("journalId" in row) && !("data" in row)), "materialized metadata acquired ownership or source bytes");

    const mutable = hints(2);
    const held = gate<FileStat | null>();
    const captured = materializeFileChanges(mutable, () => held.promise,
        () => true, () => false, undefined, undefined, { readConcurrency: () => 2 });
    mutable[0].path = "new-path.md"; mutable[0].hash = "new-hash";
    mutable[1].mtime = 99; mutable[1].size = 999; mutable[1].hash = "changed";
    held.resolve(file());
    const detached = await captured;
    check(detached.map(row => row.path).join() === "0.md,1.md" && detached.map(row => row.hash).join() === "hash-0,hash-1",
        "caller mutation during stat changed the captured group's scalar hints");
}

async function materializationReadsLiveConcurrency(): Promise<void> {
    const changes = hints(10), groups: number[] = [], admitted: string[] = [];
    let concurrency = 3, active = 0, maximum = 0;
    let queued: Array<ReturnType<typeof gate<FileStat | null>>> = [];
    const result = await materializeFileChanges(changes, path => {
        admitted.push(path); active++; maximum = Math.max(maximum, active);
        const held = gate<FileStat | null>(); queued.push(held);
        if (queued.length === 1) queueMicrotask(() => {
            const group = queued; queued = []; groups.push(group.length);
            concurrency = [1, 4, 2][groups.length - 1] ?? 2;
            for (const row of group.reverse()) { active--; row.resolve(file()); }
        });
        return held.promise;
    }, () => true, () => false, undefined, undefined, { readConcurrency: () => concurrency });
    check(groups.join() === "3,1,4,2", "materialization did not refresh concurrency after each actual joined group");
    check(admitted.join() === changes.map(row => row.path).join() && result.map(row => row.path).join() === admitted.join(),
        "changed concurrency skipped, duplicated or reordered a path");
    check(maximum === 4 && active === 0, "configured concurrent native stat limit was exceeded");

    let started = 0, finished = false;
    const held = gate<FileStat | null>();
    const serial = materializeFileChanges(hints(2), () => {
        started++; return started === 1 ? held.promise : Promise.resolve(file());
    }, () => true, () => false).then(value => { finished = true; return value; });
    await nextTurn();
    check(started === 1 && !finished, "existing callers no longer default to one native stat");
    held.resolve(file()); await serial;
    check(started === 2 && finished, "default serial materialization did not finish");
}

async function materializationBoundsAdmissionAndValidatesLimits(): Promise<void> {
    for (const concurrency of [0, -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
        let started = 0, error: unknown;
        try {
            await materializeFileChanges(hints(1), async () => { started++; return file(); },
                () => true, () => false, undefined, undefined, { readConcurrency: () => concurrency });
        } catch (caught) { error = caught; }
        check(error instanceof RangeError && started === 0, `invalid concurrency ${concurrency} admitted native stat`);
    }
    const configError = new Error("configuration unavailable");
    await rejectsWith(materializeFileChanges(hints(1), async () => file(), () => true, () => false,
        undefined, undefined, { readConcurrency: () => { throw configError; } }), configError, "configuration failure was hidden");

    let liveLimit = 2, completedReads = 0, dynamicError: unknown;
    try {
        await materializeFileChanges(hints(4), async () => {
            completedReads++; liveLimit = 0; return file();
        }, () => true, () => false, undefined, undefined, { readConcurrency: () => liveLimit });
    } catch (error) { dynamicError = error; }
    check(dynamicError instanceof RangeError && completedReads === 2, "invalid later limit admitted another group or spun without progress");

    const changes = hints(25_000), controller = new AbortController(), failure = new Error("bounded capture cancelled");
    let visited = 0, started = 0, finished = false;
    const held = gate<FileStat | null>();
    const captured = new Proxy(changes, { get(target, key, receiver) {
        if (typeof key === "string" && /^\d+$/.test(key)) visited++;
        return Reflect.get(target, key, receiver);
    } });
    captured[Symbol.iterator] = function* () { throw new Error("materialization must use bounded indexed input"); };
    const pending = materializeFileChanges(captured, () => { started++; return held.promise; },
        () => true, () => false, undefined, undefined,
        { readConcurrency: () => Number.MAX_SAFE_INTEGER, signal: controller.signal });
    const observed = pending.then(() => { finished = true; }, error => { finished = true; return error; });
    check(visited === 256 && started === 256, "materialization visited the full backlog or exceeded its hard group cap");
    controller.abort(failure); await nextTurn();
    check(!finished && visited === 256 && started === 256, "abort virtually completed or refilled a held native stat group");
    held.resolve(file());
    check(await observed === failure && finished, "bounded native group did not join and preserve abort identity");
}

async function failedConcurrentStatsJoinBeforeReturning(): Promise<void> {
    for (const synchronous of [false, true]) {
        const held = gate<FileStat | null>(), rejected = gate<FileStat | null>();
        const failure = ioError("EIO"); let started = 0, finished = false;
        const pending = materializeFileChanges(hints(3), path => {
            started++;
            if (path === "0.md") return held.promise;
            if (synchronous) throw failure;
            return rejected.promise;
        }, () => true, () => false, undefined, undefined, { readConcurrency: () => 2 });
        const observed = pending.then(() => { finished = true; }, error => { finished = true; return error; });
        if (!synchronous) rejected.reject(failure);
        await nextTurn();
        check(started === 2 && !finished, "stat failure returned/refilled before its native sibling settled");
        held.resolve(file());
        check(await observed === failure && started === 2, "joined stat failure changed error identity or admitted the next group");
    }
    const first = gate<FileStat | null>(), second = gate<FileStat | null>();
    const firstError = ioError("EACCES"), secondError = ioError("EIO");
    const pending = materializeFileChanges(hints(2), path => path === "0.md" ? first.promise : second.promise,
        () => true, () => false, undefined, undefined, { readConcurrency: () => 2 });
    second.reject(secondError); first.reject(firstError);
    await rejectsWith(pending, firstError, "multiple stat failures were chosen by completion order instead of input order");
}

async function materializationCancellationJoinsActualIO(): Promise<void> {
    const failure = new Error("stop materialization");
    const before = new AbortController(); before.abort(failure);
    let started = 0;
    await rejectsWith(materializeFileChanges(hints(1), async () => { started++; return file(); },
        () => true, () => false, undefined, undefined, { signal: before.signal }), failure, "pre-aborted materialization did not reject");
    check(started === 0, "pre-aborted materialization admitted IO");

    const during = new AbortController(), held = gate<FileStat | null>();
    let finished = false;
    const pending = materializeFileChanges(hints(4), () => {
        started++; during.abort(failure); return held.promise;
    }, () => true, () => false, undefined, undefined, { readConcurrency: () => 4, signal: during.signal });
    const observed = pending.then(() => { finished = true; }, error => { finished = true; return error; });
    await nextTurn();
    check(started === 1 && !finished, "synchronous abort from one admission started a sibling or released its owner");
    held.resolve(file());
    check(await observed === failure && started === 1, "native completion lost cancellation or admitted later work");

    const after = new AbortController(), gates = [gate<FileStat | null>(), gate<FileStat | null>()];
    let calls = 0, observations = 0, settled = false;
    const both = materializeFileChanges(hints(3), () => gates[calls++].promise,
        () => false, () => false, () => { observations++; }, undefined,
        { readConcurrency: () => 2, signal: after.signal });
    const bothObserved = both.then(() => { settled = true; }, error => { settled = true; return error; });
    gates[0].reject(new PathIsDirectoryError("0.md")); after.abort(failure); await nextTurn();
    check(!settled && calls === 2 && observations === 0, "abort before directory classification bypassed native join");
    gates[1].resolve(file());
    check(await bothObserved === failure && observations === 0 && calls === 2, "cancelled stat observations or new work escaped after native completion");

    const omitted = new AbortController();
    await rejectsWith(materializeFileChanges(hints(1), async () => { throw new Error("excluded native stat"); },
        () => false, () => true, () => omitted.abort(failure), undefined, { signal: omitted.signal }),
    failure, "synchronous abort in final omission callback returned successful materialization");
}

async function materializationPolicyFailuresDoNotStrandWork(): Promise<void> {
    for (const predicate of ["excluded", "tracked"] as const) {
        const failure = new Error(`${predicate} callback failed`); let started = 0;
        await rejectsWith(materializeFileChanges(hints(3), async () => { started++; return file(); },
            path => { if (predicate === "tracked" && path === "1.md") throw failure; return true; },
            path => { if (predicate === "excluded" && path === "1.md") throw failure; return false; },
            undefined, undefined, { readConcurrency: () => 3 }), failure, "synchronous classification error was hidden");
        check(started === 0, "classification failure stranded a previously dispatched native stat");
    }
    const excluded: DirtyFileChange = { action: "modified", path: "excluded.md" };
    for (const key of ["hash", "mtime", "size"] as const) Object.defineProperty(excluded, key, {
        get() { throw new Error("excluded source metadata must not be inspected"); },
    });
    check((await materializeFileChanges([excluded], async () => { throw new Error("excluded stat"); },
        () => { throw new Error("excluded tracking"); }, () => true, undefined, undefined,
        { readConcurrency: () => 4 })).length === 0, "excluded paths acquired unnecessary metadata dependencies");

    const failure = new Error("omission observer failed"), held = gate<FileStat | null>();
    let started = 0, finished = false, observed = 0;
    const pending = materializeFileChanges(hints(3), () => { started++; return held.promise; },
        () => true, path => path === "0.md", () => { observed++; throw failure; }, undefined,
        { readConcurrency: () => 2 });
    const outcome = pending.then(() => { finished = true; }, error => { finished = true; return error; });
    await nextTurn();
    check(started === 1 && !finished && observed === 0, "omission callback bypassed the group's native join");
    held.resolve(file());
    check(await outcome === failure && started === 1 && observed === 1, "omission failure admitted the next group or was hidden");
}

async function concurrentMaterializationCooperatesAtExactInputBoundaries(): Promise<void> {
    for (const allExcluded of [false, true]) {
        let completed = 0, omitted = 0, visited = 0, active = 0;
        const boundaries: number[] = [];
        const result = await materializeFileChanges(hints(769), async () => {
            active++; await Promise.resolve(); active--; completed++; return file();
        }, () => true, path => { visited++; return allExcluded || Number.parseInt(path) % 2 === 0; },
        () => { omitted++; }, async () => {
            check(active === 0 && completed + omitted === visited, "cooperation began before native stats/observations settled");
            boundaries.push(visited);
        }, { readConcurrency: () => 7 });
        check(boundaries.join() === "256,512,768" && visited === 769, "parallel or excluded groups crossed an exact host-yield boundary");
        check(result.length + omitted === 769 && result.length === completed, "cooperation dropped paths or manufactured observations");
    }
    const controller = new AbortController(), failure = new Error("stopped while yielding"), held = gate<void>();
    let visits = 0, yields = 0;
    const pending = materializeFileChanges(hints(513), async () => { visits++; return file(); },
        () => true, () => false, undefined, async () => { yields++; await held.promise; },
        { readConcurrency: () => 3, signal: controller.signal });
    await nextTurn();
    check(visits === 256 && yields === 1, "held cooperation failed to fence the next stat group");
    controller.abort(failure); held.resolve();
    await rejectsWith(pending, failure, "abort during host cooperation was ignored");
    check(visits === 256, "post-cooperation cancellation admitted further stats");
}

async function concurrentMaterializationPreservesDirtyProvenance(): Promise<void> {
    for (const fail of [false, true]) {
        const dirty = new DirtyPathSet();
        dirty.add({ action: "modified", path: "stable.md", hash: "stable", mtime: 20, size: 10 }, 7);
        dirty.add({ action: "modified", path: "scan.md", hash: "before", mtime: 20, size: 10 }, 8);
        dirty.add({ action: "modified", path: "scan.md", hash: "scan", mtime: 20, size: 10 });
        dirty.add({ action: "modified", path: "newer.md", hash: "old", mtime: 20, size: 10 }, 9);
        const original = dirty.captureRetirement([{ path: "stable.md", throughId: 7 }]);
        const snapshot = dirty.take(), captured = JSON.stringify(snapshot);
        snapshot.forEach(Object.freeze);
        const held = gate<FileStat | null>(), failure = ioError("EIO");
        const pending = materializeFileChanges(snapshot, async path => {
            if (path === "newer.md") {
                dirty.add({ action: "modified", path, hash: "new", mtime: 21, size: 11 }, 10);
                if (fail) throw failure;
            }
            return held.promise;
        }, () => true, () => false, undefined, undefined, { readConcurrency: () => 3 });
        held.resolve(file());
        if (fail) await rejectsWith(pending, failure, "parallel failure did not preserve original IO error");
        else {
            const output = await pending;
            check(output.every(row => !("journalId" in row)) && output !== snapshot, "materialization returned a dirty ownership token");
        }
        check(JSON.stringify(snapshot) === captured, "materialization mutated original detached dirty proofs");
        dirty.restore(snapshot);
        check(dirty.commitRetirement(original) === 1, "read-only materialization invalidated exact take/restore provenance");
        check(dirty.commitRetirement(dirty.captureRetirement([{ path: "scan.md", throughId: 8 }])) === 0,
            "materialization promoted an inherited journal watermark to ACK authority");
        const newer = dirty.capture().get("newer.md")!;
        check(newer.hash === "new" && newer.journalId === 10 && newer.size === 11,
            "restoring materialized group overwrote the newer in-flight generation");
        check(dirty.commitRetirement(dirty.captureRetirement([{ path: "newer.md", throughId: 9 }])) === 0,
            "old materialized generation retired a newer edit");
    }
}

let completed = false;
process.once("beforeExit", () => {
    if (!completed) { console.error("file-safety.test: asynchronous suite did not complete"); process.exitCode = 1; }
});
void distinctStatOutcomes()
    .then(listingsFailClosed)
    .then(dirtyMaterialization)
    .then(failedStatsPreserveNewerGeneration)
    .then(explicitMaterializationOmissions)
    .then(materializationCooperatesAcrossSkippedPaths)
    .then(concurrentMaterializationIsOrdered)
    .then(materializationReadsLiveConcurrency)
    .then(materializationBoundsAdmissionAndValidatesLimits)
    .then(failedConcurrentStatsJoinBeforeReturning)
    .then(materializationCancellationJoinsActualIO)
    .then(materializationPolicyFailuresDoNotStrandWork)
    .then(concurrentMaterializationCooperatesAtExactInputBoundaries)
    .then(concurrentMaterializationPreservesDirtyProvenance)
    .then(() => { completed = true; console.log(`file-safety.test: ${assertions} assertions passed`); })
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
