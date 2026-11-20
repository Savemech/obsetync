export interface MetadataStat {
    mtime: number;
    size: number;
}

export interface MetadataScanPlan {
    visibleFiles: number;
    visibleBytes: number;
    toHash: Array<{ path: string; stat: MetadataStat }>;
    deleted: string[];
}

/** Automatic work above this size requires an explicit Full Rescan. */
export const AUTOMATIC_CHANGE_LIMIT = 10_000;

/** Shared publish gate for metadata scans, journal recovery, and live queues. */
export function automaticChangeNeedsReview(
    total: number,
    trackedDeletions: number,
    baseCount: number,
): boolean {
    if (total >= AUTOMATIC_CHANGE_LIMIT) return true;
    return (
        baseCount >= 100 &&
        trackedDeletions >= 100 &&
        trackedDeletions * 4 >= baseCount
    );
}

/** Pure metadata phase shared by automatic and manual scans. */
export function planMetadataScan(
    stats: Map<string, MetadataStat>,
    basePaths: string[],
    getBase: (path: string) => MetadataStat | null,
    isExcluded: (path: string) => boolean,
): MetadataScanPlan {
    const toHash: Array<{ path: string; stat: MetadataStat }> = [];
    let visibleFiles = 0, visibleBytes = 0;
    for (const [path, stat] of stats) {
        if (isExcluded(path)) continue;
        visibleFiles++;
        visibleBytes += stat.size;
        const base = getBase(path);
        if (base && base.mtime === stat.mtime && base.size === stat.size) continue;
        // Do not gate on `mtime > lastSync`: copied-in files can be older than
        // the previous sync, and filesystems can preserve or regress mtimes.
        // Metadata differs, so hash it (or defer its large-file hash to push).
        toHash.push({ path, stat });
    }

    const deleted = basePaths.filter((path) => !stats.has(path) && !isExcluded(path));
    return { toHash, deleted, visibleFiles, visibleBytes };
}
