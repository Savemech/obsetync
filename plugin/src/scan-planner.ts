export interface MetadataStat {
    mtime: number;
    size: number;
}

export interface MetadataScanPlan {
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

/**
 * A startup audit is recovery, not user intent. Refuse to automatically
 * publish a vault-sized expansion or a suspicious mass deletion; Full Rescan
 * remains the explicit confirmation path after the user reviews ignores.
 */
export function metadataScanNeedsReview(
    plan: MetadataScanPlan,
    baseCount: number,
): boolean {
    const total = plan.toHash.length + plan.deleted.length;
    return automaticChangeNeedsReview(total, plan.deleted.length, baseCount);
}

/** Pure metadata phase shared by startup scans and tests. */
export function planMetadataScan(
    stats: Map<string, MetadataStat>,
    basePaths: string[],
    getBase: (path: string) => MetadataStat | null,
    isExcluded: (path: string) => boolean,
): MetadataScanPlan {
    const toHash: Array<{ path: string; stat: MetadataStat }> = [];
    for (const [path, stat] of stats) {
        if (isExcluded(path)) continue;
        const base = getBase(path);
        if (base && base.mtime === stat.mtime && base.size === stat.size) continue;
        // Do not gate on `mtime > lastSync`: copied-in files can be older than
        // the previous sync, and filesystems can preserve or regress mtimes.
        // Metadata differs, so hash it (or defer its large-file hash to push).
        toHash.push({ path, stat });
    }

    const deleted = basePaths.filter((path) => !isExcluded(path) && !stats.has(path));
    return { toHash, deleted };
}
