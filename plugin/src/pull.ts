import { SyncApi, FileDelta, FileManifest } from "./api";
import { PlatformIO } from "./platform";
import { SyncBase } from "./sync-base";

const CHUNK_THRESHOLD = 1_048_576; // 1MB
const DOWNLOAD_CONCURRENCY = 6;

/**
 * Pull path: fetch server-computed deltas and apply to the local vault.
 * Pure HTTP — no WASM needed.
 */
export async function pull(
    api: SyncApi,
    io: PlatformIO,
    syncBase: SyncBase,
    vaultId: string,
    localRootHash: string | null,
    onProgress?: (msg: string) => void
): Promise<{ newRootHash: string | null; applied: number }> {
    if (!localRootHash) {
        // No local root — first sync. Get the full remote root.
        const rootBytes = await api.getRoot(vaultId);
        if (!rootBytes) return { newRootHash: null, applied: 0 };
        // TODO: apply all files from a fresh diff against empty root
        // For now, return the root hash so the caller can do a full scan.
        return { newRootHash: null, applied: 0 };
    }

    onProgress?.("checking for remote changes...");
    const deltas = await api.getDiff(vaultId, localRootHash);

    if (!deltas || deltas.length === 0) {
        // Successful no-op sync — advance the last-sync timestamp so the UI
        // reflects the check rather than showing a stale timestamp from the
        // last round that had actual changes.
        syncBase.setLastSyncTimestamp(Date.now());
        await syncBase.save();
        onProgress?.("up to date");
        return { newRootHash: localRootHash, applied: 0 };
    }

    onProgress?.(`${deltas.length} changes to apply`);

    // Sort deltas: renames first, then deletions, then modifications, then additions.
    const renames: FileDelta[] = [];
    const deletions: FileDelta[] = [];
    const modifications: FileDelta[] = [];
    const additions: FileDelta[] = [];
    for (const d of deltas) {
        if (d.action === "renamed") renames.push(d);
        else if (d.action === "deleted") deletions.push(d);
        else if (d.action === "modified") modifications.push(d);
        else additions.push(d);
    }

    // Apply renames.
    for (const delta of renames) {
        if (delta.old_path) {
            await io.renameFile(delta.old_path, delta.path);
            syncBase.removeEntry(delta.old_path);
            if (delta.hash) {
                const stat = await io.stat(delta.path);
                syncBase.setEntry(
                    delta.path,
                    delta.hash,
                    stat?.mtime ?? Date.now(),
                    stat?.size ?? 0
                );
            }
        }
    }

    // Apply deletions.
    for (const delta of deletions) {
        await io.deleteFile(delta.path);
        syncBase.removeEntry(delta.path);
    }

    // Apply modifications and additions (can be parallelized).
    const toDownload = [...modifications, ...additions];
    for (let i = 0; i < toDownload.length; i += DOWNLOAD_CONCURRENCY) {
        const batch = toDownload.slice(i, i + DOWNLOAD_CONCURRENCY);
        await Promise.all(
            batch.map((delta) => applyContentDelta(api, io, syncBase, delta))
        );
        onProgress?.(
            `${Math.min(i + DOWNLOAD_CONCURRENCY, toDownload.length)}/${toDownload.length} files applied`
        );
    }

    // Fetch new root bytes and extract hash.
    const newRootBytes = await api.getRoot(vaultId);
    let newRootHash: string | null = localRootHash;
    if (newRootBytes) {
        // The root hash can be derived from the diff response or the server.
        // For now, we store the root bytes so we have them locally.
        // TODO: extract hash from WASM or compute from bytes.
    }

    syncBase.setLastSyncTimestamp(Date.now());
    await syncBase.save();

    return { newRootHash, applied: deltas.length };
}

async function applyContentDelta(
    api: SyncApi,
    io: PlatformIO,
    syncBase: SyncBase,
    delta: FileDelta
): Promise<void> {
    if (!delta.hash) return;

    const size = delta.size ?? 0;

    if (size >= CHUNK_THRESHOLD) {
        // Large file: fetch manifest, then missing chunks, reassemble.
        await applyLargeFile(api, io, delta.path, delta.hash);
    } else {
        // Small file: fetch whole blob.
        const data = await api.getContent(delta.hash);
        await io.writeFile(delta.path, data);
    }

    const stat = await io.stat(delta.path);
    syncBase.setEntry(
        delta.path,
        delta.hash,
        stat?.mtime ?? Date.now(),
        stat?.size ?? size
    );
}

async function applyLargeFile(
    api: SyncApi,
    io: PlatformIO,
    path: string,
    hash: string
): Promise<void> {
    const manifest = await api.getManifest(hash);

    // Fetch all chunks.
    const chunkData: Uint8Array[] = [];
    for (let i = 0; i < manifest.chunks.length; i += DOWNLOAD_CONCURRENCY) {
        const batch = manifest.chunks.slice(i, i + DOWNLOAD_CONCURRENCY);
        const results = await Promise.all(
            batch.map((c) => api.getContentChunk(c.hash))
        );
        chunkData.push(...results);
    }

    // Reassemble.
    const totalSize = manifest.total_size;
    const assembled = new Uint8Array(totalSize);
    let offset = 0;
    for (const chunk of chunkData) {
        assembled.set(chunk, offset);
        offset += chunk.length;
    }

    await io.writeFile(path, assembled);
}
