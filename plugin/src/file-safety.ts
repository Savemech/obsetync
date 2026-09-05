import type { DirtyFileChange } from "./dirty-set";
import type { FileChange } from "./push";

/** Metadata for a regular file, never a directory. */
export interface FileStat {
    mtime: number;
    size: number;
}

export interface AdapterPathStat extends FileStat {
    type: "file" | "folder";
}

/** A directory is a distinct state, not an absent or unreadable file. */
export class PathIsDirectoryError extends Error {
    readonly code = "EISDIR";

    constructor(readonly path: string) {
        super(`expected a file, found a directory: ${path}`);
        this.name = "PathIsDirectoryError";
    }
}

/** Only a positive missing-path result may become a deletion. Adapter errors
 * without a code remain errors: matching arbitrary message text is unsafe. */
export function isMissingPathError(error: unknown): boolean {
    return typeof error === "object" && error !== null &&
        "code" in error && error.code === "ENOENT";
}

/** Preserve the four filesystem outcomes: regular file, missing, directory,
 * and unavailable. The last two reject with distinct errors so existing file
 * readers cannot accidentally accept directory metadata or swallow EIO. */
export async function readFileStat(
    path: string,
    readStat: (path: string) => Promise<AdapterPathStat | null>,
): Promise<FileStat | null> {
    let stat: AdapterPathStat | null;
    try {
        stat = await readStat(path);
    } catch (error) {
        if (isMissingPathError(error)) return null;
        throw error;
    }
    if (stat === null) return null;
    if (stat?.type === "folder") throw new PathIsDirectoryError(path);
    if (
        stat?.type !== "file" || !Number.isFinite(stat.mtime) ||
        !Number.isFinite(stat.size) || stat.size < 0
    ) {
        throw new Error(`invalid file metadata: ${path}`);
    }
    return { mtime: stat.mtime, size: stat.size };
}

/** A config listing is authoritative only if every directory and listed file
 * was inspected successfully. Never return a partial map after a transient
 * read failure: its omissions would otherwise look like remote deletions. */
export async function collectAdapterFileStats(
    root: string,
    adapter: {
        stat(path: string): Promise<AdapterPathStat | null>;
        list(path: string): Promise<{ files: string[]; folders: string[] }>;
    },
): Promise<Map<string, FileStat>> {
    let rootStat: AdapterPathStat | null;
    try {
        rootStat = await adapter.stat(root);
    } catch (error) {
        if (isMissingPathError(error)) return new Map();
        throw error;
    }
    if (rootStat === null) return new Map();
    if (rootStat?.type !== "folder") {
        throw new Error(`expected a directory for configuration listing: ${root}`);
    }

    const result = new Map<string, FileStat>();
    const visit = async (dir: string): Promise<void> => {
        const listing = await adapter.list(dir);
        for (const path of listing.files) {
            const stat = await readFileStat(path, (file) => adapter.stat(file));
            if (!stat) {
                // A moving snapshot must be retried, not silently truncated.
                throw new Error(`file disappeared during configuration listing: ${path}`);
            }
            result.set(path, stat);
        }
        for (const folder of listing.folders) await visit(folder);
    };
    await visit(root);
    return result;
}

/** Re-stat detached dirty hints immediately before publication. On errors the
 * caller restores its entire snapshot with DirtyPathSet.restore(), preserving
 * any newer events. Hash hints are reusable only for unchanged fingerprints. */
export async function materializeFileChanges(
    changes: readonly DirtyFileChange[],
    statFile: (path: string) => Promise<FileStat | null>,
    isTracked: (path: string) => boolean,
    isExcluded: (path: string) => boolean,
): Promise<FileChange[]> {
    const materialized: FileChange[] = [];
    for (const change of changes) {
        if (isExcluded(change.path)) continue;
        const tracked = isTracked(change.path);
        let stat: FileStat | null;
        try {
            stat = await statFile(change.path);
        } catch (error) {
            if (!(error instanceof PathIsDirectoryError)) throw error;
            // Old versions journaled folder events. Untracked folders have no
            // file state to publish. A tracked file replaced by a directory is
            // a real removal of that file; normal deletion guards still apply.
            if (tracked) materialized.push({ action: "deleted", path: change.path });
            continue;
        }
        if (!stat) {
            materialized.push({ action: "deleted", path: change.path });
            continue;
        }
        const current: FileChange = {
            action: tracked ? "modified" : "created",
            path: change.path,
            mtime: stat.mtime,
            size: stat.size,
        };
        if (
            change.hash !== undefined &&
            change.mtime === stat.mtime && change.size === stat.size
        ) {
            current.hash = change.hash;
        }
        materialized.push(current);
    }
    return materialized;
}
