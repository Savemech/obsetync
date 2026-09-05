import {
    collectAdapterFileStats,
    isMissingPathError,
    materializeFileChanges,
    PathIsDirectoryError,
    readFileStat,
    type AdapterPathStat,
} from "./file-safety";
import { DirtyPathSet } from "./dirty-set";

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

void distinctStatOutcomes()
    .then(listingsFailClosed)
    .then(dirtyMaterialization)
    .then(failedStatsPreserveNewerGeneration)
    .then(() => console.log(`file-safety.test: ${assertions} assertions passed`))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
