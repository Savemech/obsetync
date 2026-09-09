import type { DirtyFileChange } from "./dirty-set";
import type { FileChange } from "./push";
import { throwIfWorkAborted } from "./work-scheduler";

/** Metadata for a regular file, never a directory. */
export interface FileStat {
    mtime: number;
    size: number;
}

export interface AdapterPathStat extends FileStat {
    type: "file" | "folder";
}

/** Positive local policy/type classifications only, not an IO fallback. */
export type FileChangeOmissionReason = "excluded" | "untracked-directory";

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

export interface MaterializeFileChangesOptions {
    /** Read again between joined groups; omitted callers remain serial. The
     * scheduling cap is 256, not a claim about native adapter memory use. */
    readConcurrency?: () => number;
    signal?: AbortSignal;
}

const MATERIALIZE_GROUP_LIMIT = 256;

/** Re-stat detached dirty hints immediately before publication. On errors the
 * caller restores its entire snapshot with DirtyPathSet.restore(), preserving
 * any newer events. Hash hints are reusable only for unchanged fingerprints.
 * Omission callbacks are observations, not permission to ACK: the caller must
 * finish the ENTIRE materialization/review and check journal/dependency/live
 * generations before consuming them. A later error invalidates that attempt.
 * Inputs remain caller-owned: only one bounded group's scalar metadata is
 * captured before IO, never a replacement journal/dirty ownership token. Every
 * started native stat settles before rejection, cancellation or the next group.
 * The returned metadata array still scales with the supplied reviewed cut. */
export async function materializeFileChanges(
    changes: readonly DirtyFileChange[],
    statFile: (path: string) => Promise<FileStat | null>,
    isTracked: (path: string) => boolean,
    isExcluded: (path: string) => boolean,
    onOmission?: (path: string, reason: FileChangeOmissionReason) => void,
    cooperate?: () => Promise<void>,
    options: MaterializeFileChangesOptions = {},
): Promise<FileChange[]> {
    const { signal, readConcurrency } = options;
    throwIfWorkAborted(signal);
    const materialized: FileChange[] = [];
    const count = changes.length;
    for (let index = 0; index < count;) {
        throwIfWorkAborted(signal);
        if (index > 0 && index % MATERIALIZE_GROUP_LIMIT === 0) {
            await cooperate?.();
            throwIfWorkAborted(signal);
        }
        const concurrency = readConcurrency?.() ?? 1;
        if (!Number.isSafeInteger(concurrency) || concurrency <= 0) {
            throw new RangeError("metadata read concurrency must be a positive safe integer");
        }
        const length = Math.min(concurrency, count - index,
            MATERIALIZE_GROUP_LIMIT - index % MATERIALIZE_GROUP_LIMIT);
        const group: Array<{
            path: string; hash?: string; mtime?: number; size?: number;
            excluded: boolean; tracked: boolean; statIndex: number;
        }> = [];
        // Classify/capture before starting this group. A synchronous policy
        // failure cannot strand an earlier native stat without a join owner.
        for (let offset = 0; offset < length; offset++) {
            throwIfWorkAborted(signal);
            const change = changes[index + offset], path = change.path;
            const excluded = isExcluded(path);
            if (excluded) {
                group.push({ path, excluded: true, tracked: false, statIndex: -1 });
                continue;
            }
            group.push({ path, hash: change.hash, mtime: change.mtime, size: change.size,
                excluded: false, tracked: isTracked(path), statIndex: -1 });
        }
        const work: Promise<FileStat | null>[] = [];
        for (const row of group) {
            if (row.excluded) continue;
            row.statIndex = work.length;
            // The async owner captures synchronous adapter throws as well.
            // Never race its real completion against abort or a timeout.
            work.push((async () => {
                throwIfWorkAborted(signal);
                const stat = await statFile(row.path);
                return stat ? { mtime: stat.mtime, size: stat.size } : null;
            })());
        }
        const settled = work.length ? await Promise.allSettled(work) : [];
        throwIfWorkAborted(signal);
        // Native completion order cannot reorder output or omission callbacks.
        // Failure returns no partial successful materialization; observations
        // preceding that failure remain provisional, just as for serial IO.
        for (const row of group) {
            throwIfWorkAborted(signal);
            const { path, tracked } = row;
            if (row.excluded) { onOmission?.(path, "excluded"); continue; }
            const result = settled[row.statIndex];
            if (result.status === "rejected") {
                if (!(result.reason instanceof PathIsDirectoryError)) throw result.reason;
                // Old versions journaled folder events. A tracked file replaced
                // by a directory remains a deletion subject to normal guards.
                if (tracked) materialized.push({ action: "deleted", path });
                else onOmission?.(path, "untracked-directory");
                continue;
            }
            const stat = result.value;
            if (!stat) { materialized.push({ action: "deleted", path }); continue; }
            const current: FileChange = {
                action: tracked ? "modified" : "created", path,
                mtime: stat.mtime, size: stat.size,
            };
            if (row.hash !== undefined && row.mtime === stat.mtime && row.size === stat.size) {
                current.hash = row.hash;
            }
            materialized.push(current);
        }
        index += group.length;
    }
    throwIfWorkAborted(signal);
    return materialized;
}
