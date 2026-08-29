export interface MetadataStat {
    mtime: number;
    size: number;
}

export interface MetadataScanPlan {
    toHash: Array<{ path: string; stat: MetadataStat }>;
    deleted: string[];
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
