import { SyncApi, FileDelta, FileManifest } from "./api";
import { PlatformIO } from "./platform";
import { SyncBase } from "./sync-base";
import type { WasmModule } from "./push";

const CHUNK_THRESHOLD = 1_048_576; // 1MB
const DOWNLOAD_CONCURRENCY = 6;

/** Sentinel device-root that tells the server "I'm fresh, give me every
 *  file as an addition." Matches the all-zero branch in `post_diff`. */
const ZERO_ROOT = "0".repeat(64);

/**
 * Pull path: fetch server-computed deltas and apply to the local vault.
 * Pure HTTP — no WASM needed except for `wasm_root_hash_from_bytes` to
 * extract the root hash from the server's root blob after a fresh seed.
 */
export async function pull(
    api: SyncApi,
    io: PlatformIO,
    syncBase: SyncBase,
    vaultId: string,
    localRootHash: string | null,
    wasm: WasmModule | null,
    onProgress?: (msg: string) => void
): Promise<{ newRootHash: string | null; newRootBytes: Uint8Array | null; applied: number }> {
    // --- First-time client: bulk-seed from the server ------------------
    //
    // The server's `post_diff` treats an all-zeros device_root as "empty
    // tree" and returns every file as an addition. We apply those, pull
    // down the current root bytes, derive the hash via WASM, and save it
    // as our local root. Subsequent syncs hit the normal incremental path.
    if (!localRootHash) {
        onProgress?.("first sync: downloading all files from server...");
        const deltas = await api.getDiff(vaultId, ZERO_ROOT);
        if (!deltas || deltas.length === 0) {
            return { newRootHash: null, newRootBytes: null, applied: 0 };
        }
        await applyDeltas(api, io, syncBase, deltas, onProgress);

        // Establish newRootHash + raw root bytes from the server's current
        // root. Caller persists the bytes to cached-root.bin so restart
        // doesn't force another full re-seed.
        let newRootHash: string | null = null;
        let newRootBytes: Uint8Array | null = null;
        try {
            newRootBytes = await api.getRoot(vaultId);
            if (newRootBytes && wasm) {
                newRootHash = wasm.wasm_root_hash_from_bytes(newRootBytes) ?? null;
            }
        } catch (e) {
            console.warn("[obsetync] first-sync root-hash fetch failed:", e);
        }

        syncBase.setLastSyncTimestamp(Date.now());
        await syncBase.save();
        onProgress?.(`first sync: applied ${deltas.length} files`);
        return { newRootHash, newRootBytes, applied: deltas.length };
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
        return { newRootHash: localRootHash, newRootBytes: null, applied: 0 };
    }

    onProgress?.(`${deltas.length} changes to apply`);
    await applyDeltas(api, io, syncBase, deltas, onProgress);

    // Extract the new root hash from the server's current root bytes so
    // subsequent incremental syncs know what to diff against.
    let newRootHash: string | null = localRootHash;
    let newRootBytes: Uint8Array | null = null;
    try {
        newRootBytes = await api.getRoot(vaultId);
        if (newRootBytes && wasm) {
            newRootHash = wasm.wasm_root_hash_from_bytes(newRootBytes) ?? localRootHash;
        }
    } catch (e) {
        console.warn("[obsetync] post-pull root-hash fetch failed:", e);
    }

    syncBase.setLastSyncTimestamp(Date.now());
    await syncBase.save();

    return { newRootHash, newRootBytes, applied: deltas.length };
}

/** Apply a delta stream: renames, deletions, then file content (parallel). */
async function applyDeltas(
    api: SyncApi,
    io: PlatformIO,
    syncBase: SyncBase,
    deltas: FileDelta[],
    onProgress?: (msg: string) => void,
): Promise<void> {
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

    for (const delta of deletions) {
        await io.deleteFile(delta.path);
        syncBase.removeEntry(delta.path);
    }

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
