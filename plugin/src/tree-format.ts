export interface RootVersionReader {
    wasm_root_version_from_bytes(bytes: Uint8Array): number | undefined;
}

export interface FormatMutableTree {
    tree_version(): number;
    rebuild_from_entries_in_version(version: number, entriesJson: string): void;
}

export interface TreeSyncBaseView {
    allPaths(): string[];
    getEntry(path: string): { hash: string; mtime: number; size: number } | null;
    getTreeMtime(path: string): number | null;
}

export interface TreeFormatAlignment {
    changed: boolean;
    version: 1 | 2;
}

/** Align an in-memory Merkle graph with the format of an authenticated server
 * root. Rebuild input comes from sync-base—the verified semantic snapshot—not
 * live disk, so queued local edits remain queued and are applied by push. The
 * WASM replacement is atomic: failure leaves the previous graph untouched. */
export function alignTreeFormatFromRoot(
    wasm: RootVersionReader,
    tree: FormatMutableTree,
    syncBase: TreeSyncBaseView,
    rootBytes: Uint8Array,
): TreeFormatAlignment {
    const observed = wasm.wasm_root_version_from_bytes(rootBytes);
    if (observed !== 1 && observed !== 2) {
        throw new Error(`server root has unsupported Tree version ${observed ?? "missing"}`);
    }
    if (tree.tree_version() === observed) {
        return { changed: false, version: observed };
    }

    const entries = syncBase.allPaths().map((path) => {
        const entry = syncBase.getEntry(path);
        if (!entry) throw new Error(`sync-base entry vanished during Tree v${observed} rebuild`);
        return {
            path,
            hash: entry.hash,
            mtime_ms: syncBase.getTreeMtime(path) ?? entry.mtime,
            size: entry.size,
        };
    });
    tree.rebuild_from_entries_in_version(observed, JSON.stringify(entries));
    return { changed: true, version: observed };
}
