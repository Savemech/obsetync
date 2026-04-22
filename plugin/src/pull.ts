import { SyncApi, FileDelta, FileManifest } from "./api";
import { PlatformIO } from "./platform";
import { SyncBase } from "./sync-base";
import { hashFileStreaming, type WasmModule } from "./push";

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
        await applyDeltas(api, io, syncBase, wasm, deltas, onProgress);

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
        // Empty delta list can mean one of two things:
        //
        //   (a) Same root on both sides — server sends 304, middleware
        //       promotes to 200 with empty body. localRootHash stays valid.
        //   (b) Different roots but identical content (only mtime/size
        //       differ between server and client trees). Server computed
        //       deltas and got []. We MUST advance localRootHash here —
        //       otherwise every future pull will keep returning 0 deltas
        //       against our stale root and we'll be stuck forever, even
        //       though the server is semantically ahead.
        //
        // We can't distinguish (a) from (b) on the client (middleware ate
        // the 304 status to keep the AEAD envelope intact). Cheapest
        // correct fix: always refresh the server root when pull returns
        // empty deltas. Adds one HTTP round-trip per idle pull — fine.
        syncBase.setLastSyncTimestamp(Date.now());
        await syncBase.save();

        let newRootHash: string | null = localRootHash;
        let newRootBytes: Uint8Array | null = null;
        try {
            newRootBytes = await api.getRoot(vaultId);
            if (newRootBytes && wasm) {
                newRootHash = wasm.wasm_root_hash_from_bytes(newRootBytes) ?? localRootHash;
            }
        } catch (e) {
            console.warn("[obsetync] idle-pull root-hash fetch failed:", e);
        }

        onProgress?.("up to date");
        return { newRootHash, newRootBytes, applied: 0 };
    }

    onProgress?.(`${deltas.length} changes to apply`);
    await applyDeltas(api, io, syncBase, wasm, deltas, onProgress);

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

/** Counters for the three-tier resolution of a content delta. Summed
 *  across the whole apply loop and logged at the end, so we can tell
 *  at a glance whether a 3000-file delta was actually 3000 downloads
 *  or mostly free cache hits. */
interface ApplyStats {
    /** sync-base already records `delta.hash` at the target path + disk
     *  metadata matches; no hash, no network, no disk write. */
    cacheHit: number;
    /** sync-base disagreed (or was absent) but hashing the on-disk file
     *  locally matched `delta.hash`; sync-base repaired, no network. */
    localHit: number;
    /** Had to fetch from the server. Actual bandwidth used. */
    downloaded: number;
    /** Sum of bytes we avoided sending over the wire. */
    bytesSkipped: number;
    /** Sum of bytes we actually pulled from the server. */
    bytesDownloaded: number;
}

/** Apply a delta stream: renames, deletions, then file content (parallel). */
async function applyDeltas(
    api: SyncApi,
    io: PlatformIO,
    syncBase: SyncBase,
    wasm: WasmModule | null,
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

    const stats: ApplyStats = {
        cacheHit: 0,
        localHit: 0,
        downloaded: 0,
        bytesSkipped: 0,
        bytesDownloaded: 0,
    };

    const toDownload = [...modifications, ...additions];
    for (let i = 0; i < toDownload.length; i += DOWNLOAD_CONCURRENCY) {
        const batch = toDownload.slice(i, i + DOWNLOAD_CONCURRENCY);
        await Promise.all(
            batch.map((delta) => applyContentDelta(api, io, syncBase, wasm, delta, stats))
        );
        onProgress?.(
            `${Math.min(i + DOWNLOAD_CONCURRENCY, toDownload.length)}/${toDownload.length} files applied`
        );
    }

    if (toDownload.length > 0) {
        const fmt = (n: number) => (n >= 1_048_576)
            ? `${(n / 1_048_576).toFixed(1)} MB`
            : n >= 1024 ? `${(n / 1024).toFixed(0)} KB` : `${n} B`;
        console.log(
            `[obsetync] applyDeltas: ${stats.cacheHit} cache-hit, ` +
            `${stats.localHit} local-hash-hit, ${stats.downloaded} downloaded — ` +
            `${fmt(stats.bytesSkipped)} saved, ${fmt(stats.bytesDownloaded)} transferred`
        );
    }
}

async function applyContentDelta(
    api: SyncApi,
    io: PlatformIO,
    syncBase: SyncBase,
    wasm: WasmModule | null,
    delta: FileDelta,
    stats: ApplyStats,
): Promise<void> {
    if (!delta.hash) return;

    const size = delta.size ?? 0;

    // --- Tier 1: sync-base cache hit --------------------------------------
    // If sync-base already records this path at this exact hash AND the
    // on-disk (mtime, size) match the sync-base entry, we know the file is
    // byte-identical to what the server wants. Zero work.
    const stat = await io.stat(delta.path);
    if (stat) {
        const base = syncBase.getEntry(delta.path);
        if (
            base &&
            base.hash === delta.hash &&
            base.mtime === stat.mtime &&
            base.size === stat.size
        ) {
            stats.cacheHit++;
            stats.bytesSkipped += size || stat.size;
            return;
        }

        // --- Tier 2: local hash matches target --------------------------
        // sync-base disagrees (or is missing) but the on-disk file hashes
        // to the exact value the server is offering. Common after a
        // rollback or stub-WASM recovery — the content is correct, only
        // our metadata was stale. Repair sync-base and skip the download.
        if (wasm) {
            try {
                const actualHash = await hashFileStreaming(delta.path, io, wasm);
                if (actualHash === delta.hash) {
                    syncBase.setEntry(delta.path, delta.hash, stat.mtime, stat.size);
                    stats.localHit++;
                    stats.bytesSkipped += size || stat.size;
                    return;
                }
            } catch (e) {
                // Hash failed (read error, permission issue, etc.) — fall
                // through to the download path so we still end up correct.
                console.warn(`[obsetync] local-hash check failed for ${delta.path}:`, e);
            }
        }
    }

    // --- Tier 3: actual download from server -----------------------------
    if (size >= CHUNK_THRESHOLD) {
        await applyLargeFile(api, io, delta.path, delta.hash);
    } else {
        const data = await api.getContent(delta.hash);
        await io.writeFile(delta.path, data);
    }
    stats.downloaded++;
    stats.bytesDownloaded += size;

    const postStat = await io.stat(delta.path);
    syncBase.setEntry(
        delta.path,
        delta.hash,
        postStat?.mtime ?? Date.now(),
        postStat?.size ?? size,
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
