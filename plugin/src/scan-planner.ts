export interface MetadataStat {
    mtime: number;
    size: number;
}

export interface MetadataScanPlan {
    toHash: Array<{ path: string; stat: MetadataStat }>;
    deleted: string[];
}

/** Automatic startup work above this size requires an explicit Full Rescan. */
export const AUTOMATIC_METADATA_CHANGE_LIMIT = 10_000;

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
    if (total >= AUTOMATIC_METADATA_CHANGE_LIMIT) return true;
    return (
        baseCount >= 100 &&
        plan.deleted.length >= 100 &&
        plan.deleted.length * 4 >= baseCount
    );
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
