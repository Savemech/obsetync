export interface ReconcileContentSource {
    path: string;
    hash: string;
    size: number;
}

export interface ReconcileContentRef {
    path: string;
    size: number;
}

export interface ReconcileContentIndex {
    smallByHash: Map<string, ReconcileContentRef>;
    largeByHash: Map<string, ReconcileContentRef>;
    /** Physical tracked files. Duplicate content still represents two files. */
    filesTotal: number;
    /** Physical tracked bytes, before content-addressed deduplication. */
    bytesTotal: number;
}

export interface ReconcileManifestRange {
    hash: string;
    offset: number;
    size: number;
}

/**
 * Build the content-addressed lookup used by reconcile while retaining the
 * physical workload totals used to calculate end-to-end deduplication.
 */
export function buildReconcileContentIndex(
    sources: Iterable<ReconcileContentSource>,
    largeFileThreshold: number,
): ReconcileContentIndex {
    const smallByHash = new Map<string, ReconcileContentRef>();
    const largeByHash = new Map<string, ReconcileContentRef>();
    let filesTotal = 0;
    let bytesTotal = 0;

    for (const source of sources) {
        filesTotal++;
        bytesTotal += source.size;
        const index = source.size >= largeFileThreshold ? largeByHash : smallByHash;
        if (!index.has(source.hash)) {
            index.set(source.hash, { path: source.path, size: source.size });
        }
    }

    return { smallByHash, largeByHash, filesTotal, bytesTotal };
}

/** Sum unique known content objects; duplicate/unknown server responses add no bytes. */
export function sumIndexedContentBytes(
    hashes: Iterable<string>,
    index: ReadonlyMap<string, ReconcileContentRef>,
): number {
    const seen = new Set<string>();
    let bytes = 0;
    for (const hash of hashes) {
        if (seen.has(hash)) continue;
        seen.add(hash);
        bytes += index.get(hash)?.size ?? 0;
    }
    return bytes;
}

/** Convert a server missing bitmap into ordered, unique file ranges. A
 * repeated content hash is uploaded from its first occurrence only. */
export function selectReconcileMissingRanges(
    chunks: readonly ReconcileManifestRange[],
    missingHashes: Iterable<string>,
): ReconcileManifestRange[] {
    const missing = new Set(missingHashes);
    const selected = new Set<string>();
    const ranges: ReconcileManifestRange[] = [];
    for (const chunk of chunks) {
        if (missing.has(chunk.hash) && !selected.has(chunk.hash)) {
            selected.add(chunk.hash);
            ranges.push(chunk);
        }
    }
    return ranges;
}
