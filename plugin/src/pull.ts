import { ObsetyncApi, FileDelta } from "./api";
import { PlatformIO } from "./platform";
import { ObsetyncSyncBase } from "./sync-base";
import { hashFileStreaming, type WasmModule, type WasmTree } from "./push";

const CHUNK_THRESHOLD = 1_048_576; // 1MB
const DOWNLOAD_CONCURRENCY = 6;

/** Sentinel device-root that tells the server "I'm fresh, give me every
 *  file as an addition." Matches the all-zero branch in `post_diff`. */
const ZERO_ROOT = "0".repeat(64);

export interface PullResult {
    /** Server's current root hash after this pull (from getRoot). */
    newRootHash: string | null;
    newRootBytes: Uint8Array | null;
    applied: number;
    /** True when the rebased local tree reproduces `newRootHash` exactly,
     *  false when it doesn't, null when no comparison was possible (no tree,
     *  tree not yet bootstrapped, or no server root). The caller must only
     *  advance its treeBaseRoot on `true` — never past content it hasn't
     *  verifiably applied (that's how the 2026-07-13 revert started). */
    treeParity: boolean | null;
    /** True when every applied upsert delta carried the server-side
     *  mtime_ms (server ≥ 1.4.0). Without it exact parity is unreachable
     *  because leaf hashes cover mtime. */
    deltasHadMtime: boolean;
    /** Files this pull could not fetch (after one retry) and DEFERRED —
     *  left untouched on disk, excluded from the tree rebase, retried next
     *  pull. Non-zero means the tree is knowingly missing content the server
     *  has, so the caller must NOT advance treeBaseRoot (a later fast-forward
     *  would read those gaps as deletions — the 2026-07-13 failure mode). */
    failedCount: number;
}

/**
 * Pull path: fetch server-computed deltas, apply to the local vault, and
 * REBASE the in-memory Merkle tree with the same deltas. The tree must
 * advance in lockstep with disk + sync-base: a tree left behind on pull is
 * exactly the stale tree that, pushed later with a freshly-observed parent,
 * fast-forwards the server back in time (incident 2026-07-13).
 */
export async function pull(
    api: ObsetyncApi,
    io: PlatformIO,
    syncBase: ObsetyncSyncBase,
    vaultId: string,
    localRootHash: string | null,
    wasm: WasmModule | null,
    tree: WasmTree | null,
    onProgress?: (msg: string) => void,
    /** Called with every path this pull is about to touch (targets, rename
     *  old_paths, deletions) BEFORE any disk write, so the engine can tell
     *  its own write-echo vault events apart from real user edits. */
    onDeltasKnown?: (paths: string[]) => void,
    /** Paths with UNSYNCED local edits (pending queue / journal). Their disk
     *  state is newer than anything the server can send — applying the
     *  server's version would overwrite bytes that exist nowhere else (the
     *  startup order is pull → journal recovery, so a journaled edit from
     *  last session would be clobbered before recovery ever reads it).
     *  These paths are skipped on DISK but still applied to the tree: the
     *  local edit pushes right after, and the server merge reconciles. */
    skipPaths?: Set<string>,
    /** Slice 2 ignore predicate. Ignored UPSERTS are dropped (never fetched —
     *  this is what stops a stale device choking on a target/ binary — never
     *  tracked, never in the tree). Ignored DELETES untrack the path (sync-base
     *  + tree) WITHOUT deleting disk, so a server-side purge of ignored paths
     *  converges the fleet while every device keeps its local copy. */
    isIgnored?: (path: string) => boolean,
): Promise<PullResult> {
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
            return {
                newRootHash: null,
                newRootBytes: null,
                applied: 0,
                treeParity: null,
                deltasHadMtime: false,
                failedCount: 0,
            };
        }
        onDeltasKnown?.(deltaPaths(deltas));
        const { kept, ignoredDeletes, ignoredUpserts } = splitIgnored(deltas, isIgnored);
        for (const d of ignoredDeletes) syncBase.removeEntry(d.path);
        if (ignoredUpserts > 0 || ignoredDeletes.length > 0) {
            console.log(
                `[obsetync] pull: skipped ${ignoredUpserts} ignored addition(s), ` +
                `untracked ${ignoredDeletes.length} ignored deletion(s)`,
            );
        }
        const failed = await applyDeltas(api, io, syncBase, wasm, kept, onProgress, skipPaths);

        // Rebase: sync-base was just seeded with the full server state, so a
        // fresh bootstrap from it materializes the server's tree locally.
        // Deferred files never got a sync-base entry, so the bootstrap already
        // excludes them; filter the delta list too for the incremental branch.
        const appliedDeltas = excludeDeltas(kept, failed).concat(ignoredDeletes);
        const deltasHadMtime = rebaseTree(tree, syncBase, appliedDeltas);

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
        onProgress?.(`first sync: applied ${kept.length - failed.length} files`);
        return {
            newRootHash,
            newRootBytes,
            applied: kept.length - failed.length,
            treeParity: parity(tree, newRootHash),
            deltasHadMtime,
            failedCount: failed.length,
        };
    }

    onProgress?.("checking for remote changes...");
    const deltas = await api.getDiff(vaultId, localRootHash);

    if (!deltas || deltas.length === 0) {
        // Empty delta list can mean one of two things:
        //
        //   (a) Same root on both sides — server sends 304, middleware
        //       promotes to 200 with empty body.
        //   (b) Different roots but identical content (only mtime/size
        //       differ between server and client trees). Server computed
        //       deltas and got [].
        //
        // We still refresh the server root for observability, but the
        // CALLER must not advance its treeBaseRoot past a root whose
        // content it hasn't verifiably applied — signalled via treeParity.
        // (An earlier version advanced unconditionally here; combined with
        // the tree-less pull path it let sync state outrun reality.)
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
        return {
            newRootHash,
            newRootBytes,
            applied: 0,
            treeParity: parity(tree, newRootHash),
            deltasHadMtime: false,
            failedCount: 0,
        };
    }

    onProgress?.(`${deltas.length} changes to apply`);
    onDeltasKnown?.(deltaPaths(deltas));
    const { kept, ignoredDeletes, ignoredUpserts } = splitIgnored(deltas, isIgnored);
    // Untrack ignored paths the server dropped (a purge) WITHOUT touching disk.
    for (const d of ignoredDeletes) syncBase.removeEntry(d.path);
    if (ignoredUpserts > 0 || ignoredDeletes.length > 0) {
        console.log(
            `[obsetync] pull: skipped ${ignoredUpserts} ignored upsert(s), ` +
            `untracked ${ignoredDeletes.length} ignored deletion(s)`,
        );
    }
    const failed = await applyDeltas(api, io, syncBase, wasm, kept, onProgress, skipPaths);

    // Rebase the Merkle tree with the exact deltas just applied to disk +
    // sync-base. THE key invariant of the pull path: tree, sync-base, and
    // disk advance together or not at all — so DEFERRED (unfetched) files are
    // excluded here, or the tree would claim content that never hit disk.
    // Ignored deletions ARE included: they drop the leaf so the tree converges
    // with a server that purged them.
    const appliedDeltas = excludeDeltas(kept, failed).concat(ignoredDeletes);
    const deltasHadMtime = rebaseTree(tree, syncBase, appliedDeltas);

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

    return {
        newRootHash,
        newRootBytes,
        applied: kept.length - failed.length,
        treeParity: parity(tree, newRootHash),
        deltasHadMtime,
        failedCount: failed.length,
    };
}

/** Drop the deferred (unfetched) deltas from a set before rebasing the tree,
 *  so the tree never advances past content that isn't on disk. Identity match
 *  — `failed` holds the very objects from `deltas`. */
function excludeDeltas(deltas: FileDelta[], failed: FileDelta[]): FileDelta[] {
    if (failed.length === 0) return deltas;
    const drop = new Set(failed);
    return deltas.filter((d) => !drop.has(d));
}

/** Partition a delta set by the ignore predicate (Slice 2):
 *   - `kept`           — normal deltas to apply to disk.
 *   - `ignoredDeletes` — ignored paths the server dropped: untrack them (tree +
 *                        sync-base) but keep the local file on disk.
 *  Ignored UPSERTS are discarded outright (never fetched, never tracked). */
function splitIgnored(
    deltas: FileDelta[],
    isIgnored?: (path: string) => boolean,
): { kept: FileDelta[]; ignoredDeletes: FileDelta[]; ignoredUpserts: number } {
    if (!isIgnored) return { kept: deltas, ignoredDeletes: [], ignoredUpserts: 0 };
    const kept: FileDelta[] = [];
    const ignoredDeletes: FileDelta[] = [];
    let ignoredUpserts = 0;
    for (const d of deltas) {
        if (isIgnored(d.path)) {
            if (d.action === "deleted") ignoredDeletes.push(d);
            else ignoredUpserts++;
            continue;
        }
        kept.push(d);
    }
    return { kept, ignoredDeletes, ignoredUpserts };
}

/** Every vault path a delta set will touch (targets + rename sources). */
function deltaPaths(deltas: FileDelta[]): string[] {
    const paths: string[] = [];
    for (const d of deltas) {
        paths.push(d.path);
        if (d.old_path) paths.push(d.old_path);
    }
    return paths;
}

/** Compare the tree's actual root to the server's. Null when either side
 *  is unavailable (no tree yet, or the root fetch failed). */
function parity(tree: WasmTree | null, serverRootHash: string | null): boolean | null {
    if (!tree || !serverRootHash) return null;
    let local: string | null = null;
    try {
        local = tree.root_hash_hex() ?? null;
    } catch {
        return null;
    }
    if (!local) return null;
    return local === serverRootHash;
}

/**
 * Mirror a just-applied delta set into the WASM Merkle tree (D1 fix).
 *
 * - Tree not bootstrapped yet → build it from sync-base, which at this
 *   point already reflects the deltas. One O(n log n) build.
 * - Tree live → one delete_batch (deletions + rename old_paths) and one
 *   update_batch (adds/mods/renames), same batching the push path uses.
 *
 * Entry mtimes come from the server's delta (`mtime_ms`) so leaf metadata
 * — and therefore the root hash — can match the server byte-for-byte.
 * Falls back to sync-base's recorded tree-mtime when a delta lacks it
 * (server < 1.4.0); returns whether every upsert carried a server mtime.
 */
function rebaseTree(
    tree: WasmTree | null,
    syncBase: ObsetyncSyncBase,
    deltas: FileDelta[],
): boolean {
    let allHadMtime = true;
    for (const d of deltas) {
        if (d.action !== "deleted" && d.mtime_ms === undefined) allHadMtime = false;
    }
    if (!tree) return allHadMtime;

    try {
        if (!tree.root_hash_hex()) {
            // Bootstrap from sync-base (already delta-updated). Mirrors the
            // first-push bootstrap in push.ts.
            const paths = syncBase.allPaths();
            const entries = paths.map((p) => {
                const e = syncBase.getEntry(p)!;
                return {
                    path: p,
                    hash: e.hash,
                    mtime_ms: syncBase.getTreeMtime(p) ?? e.mtime,
                    size: e.size,
                };
            });
            tree.build_from_entries(JSON.stringify(entries));
            return allHadMtime;
        }

        const deletePaths: string[] = [];
        const upserts: { path: string; hash: string; mtime_ms: number; size: number }[] = [];
        for (const d of deltas) {
            if (d.action === "deleted") {
                deletePaths.push(d.path);
            } else if (d.action === "renamed") {
                if (d.old_path) deletePaths.push(d.old_path);
                if (d.hash) {
                    upserts.push({
                        path: d.path,
                        hash: d.hash,
                        mtime_ms: d.mtime_ms ?? syncBase.getTreeMtime(d.path) ?? Date.now(),
                        size: d.size ?? syncBase.getEntry(d.path)?.size ?? 0,
                    });
                }
            } else if (d.hash) {
                upserts.push({
                    path: d.path,
                    hash: d.hash,
                    mtime_ms: d.mtime_ms ?? syncBase.getTreeMtime(d.path) ?? Date.now(),
                    size: d.size ?? syncBase.getEntry(d.path)?.size ?? 0,
                });
            }
        }
        if (deletePaths.length > 0) tree.delete_batch(JSON.stringify(deletePaths));
        if (upserts.length > 0) tree.update_batch(JSON.stringify(upserts));
    } catch (e) {
        // A failed rebase leaves the tree behind disk/sync-base — the caller
        // sees treeParity=false and blocks pushes rather than publishing a
        // root derived from a diverged tree.
        console.error("[obsetync] tree rebase after pull failed:", e);
    }
    return allHadMtime;
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
    api: ObsetyncApi,
    io: PlatformIO,
    syncBase: ObsetyncSyncBase,
    wasm: WasmModule | null,
    deltas: FileDelta[],
    onProgress?: (msg: string) => void,
    skipPaths?: Set<string>,
): Promise<FileDelta[]> {
    const renames: FileDelta[] = [];
    const deletions: FileDelta[] = [];
    const modifications: FileDelta[] = [];
    const additions: FileDelta[] = [];
    const deferred: string[] = [];
    for (const d of deltas) {
        // Locally-edited paths keep their disk bytes; the pending push +
        // server merge reconcile them. (Renames are included when either
        // end touches an edited path.)
        if (
            skipPaths &&
            (skipPaths.has(d.path) || (d.old_path !== undefined && skipPaths.has(d.old_path)))
        ) {
            deferred.push(d.path);
            continue;
        }
        if (d.action === "renamed") renames.push(d);
        else if (d.action === "deleted") deletions.push(d);
        else if (d.action === "modified") modifications.push(d);
        else additions.push(d);
    }
    if (deferred.length > 0) {
        console.log(
            `[obsetync] pull deferred ${deferred.length} locally-edited file(s): ` +
            `${deferred.slice(0, 3).join(", ")}${deferred.length > 3 ? ", …" : ""} — ` +
            `local bytes win until the pending push merges them`,
        );
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
                    stat?.size ?? 0,
                    delta.mtime_ms,
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
    // Files that threw while applying (a dropped connection, a large-file
    // manifest fetch that timed out under memory pressure on mobile). A single
    // one of these MUST NOT abort the whole pull: a 68k-delta catch-up on a
    // stale device would then restart from zero forever (incident 2026-07-15).
    // We collect them, retry once, and defer whatever still fails.
    const failed: FileDelta[] = [];
    const startedAt = Date.now();
    for (let i = 0; i < toDownload.length; i += DOWNLOAD_CONCURRENCY) {
        const batch = toDownload.slice(i, i + DOWNLOAD_CONCURRENCY);
        const results = await Promise.allSettled(
            batch.map((delta) => applyContentDelta(api, io, syncBase, wasm, delta, stats))
        );
        results.forEach((r, j) => {
            if (r.status === "rejected") failed.push(batch[j]);
        });
        // One tick per batch: position, what was verified-for-free vs actually
        // downloaded, bytes moved, deferred count, and the current rate —
        // everything a human needs to see that a big pull is alive and moving.
        const done = Math.min(i + DOWNLOAD_CONCURRENCY, toDownload.length);
        const verified = stats.cacheHit + stats.localHit;
        const elapsed = (Date.now() - startedAt) / 1000;
        const rate = elapsed > 0 ? ` · ${(done / elapsed).toFixed(0)} f/s` : "";
        const failMsg = failed.length > 0 ? ` · ✗${failed.length} deferred` : "";
        onProgress?.(
            `${done}/${toDownload.length} files applied · ` +
            `✓${verified} verified · ↓${stats.downloaded} (${fmtBytes(stats.bytesDownloaded)})${failMsg}${rate}`
        );
    }

    // One retry pass — most failures are transient. A file that STILL fails is
    // deferred: disk untouched, sync-base entry unchanged, and (by the caller)
    // excluded from the tree rebase, so the tree never claims content that
    // isn't on disk. The next pull retries it.
    const unfetched: FileDelta[] = [];
    if (failed.length > 0) {
        console.warn(`[obsetync] pull: ${failed.length} file(s) failed first pass — retrying once`);
        for (const delta of failed) {
            try {
                await applyContentDelta(api, io, syncBase, wasm, delta, stats);
            } catch (e) {
                unfetched.push(delta);
                console.warn(`[obsetync] pull: deferring ${delta.path}: ${String((e as any)?.message ?? e)}`);
            }
        }
        if (unfetched.length > 0) {
            console.error(
                `[obsetync] pull: ${unfetched.length} file(s) could not be fetched — deferred to ` +
                `next pull: ${unfetched.slice(0, 5).map((d) => d.path).join(", ")}` +
                `${unfetched.length > 5 ? ", …" : ""}`
            );
        }
    }

    if (toDownload.length > 0) {
        console.log(
            `[obsetync] applyDeltas: ${stats.cacheHit} cache-hit, ` +
            `${stats.localHit} local-hash-hit, ${stats.downloaded} downloaded, ` +
            `${unfetched.length} deferred — ` +
            `${fmtBytes(stats.bytesSkipped)} saved, ${fmtBytes(stats.bytesDownloaded)} transferred`
        );
    }
    return unfetched;
}

/** Human-readable byte count for progress messages. */
function fmtBytes(n: number): string {
    if (n >= 1_048_576) return `${(n / 1_048_576).toFixed(1)} MB`;
    if (n >= 1024) return `${(n / 1024).toFixed(0)} KB`;
    return `${n} B`;
}

async function applyContentDelta(
    api: ObsetyncApi,
    io: PlatformIO,
    syncBase: ObsetyncSyncBase,
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
                    syncBase.setEntry(
                        delta.path,
                        delta.hash,
                        stat.mtime,
                        stat.size,
                        delta.mtime_ms,
                    );
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
        delta.mtime_ms,
    );
}

async function applyLargeFile(
    api: ObsetyncApi,
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
