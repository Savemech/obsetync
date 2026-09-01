import { ObsetyncApi, FileDelta, type FileManifest } from "./api";
import { PlatformIO } from "./platform";
import { ObsetyncSyncBase } from "./sync-base";
import { hashFileStreaming, type WasmModule, type WasmTree } from "./push";
import type { PullWriteExpectation } from "./pull-echo";
import { conflictCopyPath } from "./conflict-path";
import type { PerfOperation } from "./perf-trace";
import { BulkObjectKind } from "./bulk-codec";
import { planByteBoundedBatches } from "./hash-runtime";

const CHUNK_THRESHOLD = 1_048_576; // 1MB
const MAX_CONTENT_CHUNK = 4 * 1_048_576;
const DESKTOP_BULK_DOWNLOAD_BYTES = 8 * 1_048_576;
const MOBILE_BULK_DOWNLOAD_BYTES = 2 * 1_048_576;
const BULK_DOWNLOAD_FILES = 256;
const TRANSFER_DIR = ".obsidian/plugins/obsetync/transfers";

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
    /** Server deltas deliberately or involuntarily deferred: fetch failures
     *  plus paths with an unsynced local edit. They stay out of BOTH disk and
     *  the tree rebase, so treeBaseRoot remains the older honest merge base. */
    deferredCount: number;
    /** Subset of deferredCount caused by unsynced local edits. */
    localDeferredCount: number;
    /** Number of files this pull actually fetched from the server. Zero
     *  means every applied delta verified against local disk — the content is
     *  provably identical to the server, so a tree-hash mismatch is metadata
     *  (mtime) only, not a real divergence, and is safe to adopt rather than
     *  pause. */
    downloaded: number;
}

/** Set-like live guard. A real Set is accepted, while the sync engine can
 *  provide a dynamic view that also sees editor events arriving mid-pull. */
export interface PullPathGuard {
    has(path: string): boolean;
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
    /** Called with the exact writes this pull will perform, after ignored and
     *  locally-edited paths have been removed. Upserts carry their expected
     *  content hash so delayed vault-event echoes can be authenticated. */
    onWritesKnown?: (writes: PullWriteExpectation[]) => void,
    /** Paths with UNSYNCED local edits (pending queue / journal). Their disk
     *  state is newer than anything the server can send — applying the
     *  server's version would overwrite bytes that exist nowhere else (the
     *  startup order is pull → journal recovery, so a journaled edit from
     *  last session would be clobbered before recovery ever reads it).
     *  These paths are skipped on disk AND in the tree. Keeping the previous
     *  treeBaseRoot forces a real server-side three-way merge. */
    skipPaths?: PullPathGuard,
    /** Slice 2 ignore predicate. Ignored UPSERTS are dropped (never fetched —
     *  this is what stops a stale device choking on a target/ binary — never
     *  tracked, never in the tree). Ignored DELETES untrack the path (sync-base
     *  + tree) WITHOUT deleting disk, so a server-side purge of ignored paths
     *  converges the fleet while every device keeps its local copy. */
    isIgnored?: (path: string) => boolean,
    perf?: PerfOperation,
): Promise<PullResult> {
    // --- First-time client: bulk-seed from the server ------------------
    //
    // The server's `post_diff` treats an all-zeros device_root as "empty
    // tree" and returns every file as an addition. We apply those, pull
    // down the current root bytes, derive the hash via WASM, and save it
    // as our local root. Subsequent syncs hit the normal incremental path.
    if (!localRootHash) {
        onProgress?.("first sync: downloading all files from server...");
        const endCheck = perf?.phase("check");
        let deltas: FileDelta[] | null;
        try {
            deltas = await api.getDiff(vaultId, ZERO_ROOT, perf);
        } finally {
            endCheck?.();
        }
        setDeltaWorkload(perf, deltas ?? []);
        if (!deltas || deltas.length === 0) {
            perf?.setWorkload({ filesNeeded: 0, bytesNeeded: 0 });
            // An existing server can legitimately have a committed empty
            // root. Fetch and reproduce it instead of treating [] as "no
            // server": otherwise the first later local edit would push with
            // an empty parent and be rejected because a current root exists.
            const endTree = perf?.phase("tree_update");
            const deltasHadMtime = rebaseTree(tree, syncBase, []);
            endTree?.();
            let newRootHash: string | null = null;
            let newRootBytes: Uint8Array | null = null;
            try {
                newRootBytes = await api.getRoot(vaultId, perf);
                if (newRootBytes && wasm) {
                    newRootHash = wasm.wasm_root_hash_from_bytes(newRootBytes) ?? null;
                }
            } catch (e) {
                console.warn("[obsetync] empty first-sync root fetch failed:", e);
            }
            syncBase.setLastSyncTimestamp(Date.now());
            const endCheckpoint = perf?.phase("checkpoint");
            try {
                await syncBase.save();
            } finally {
                endCheckpoint?.();
            }
            onProgress?.("first sync: applied 0 files");
            return {
                newRootHash,
                newRootBytes,
                applied: 0,
                treeParity: parity(tree, newRootHash),
                deltasHadMtime,
                deferredCount: 0,
                localDeferredCount: 0,
                downloaded: 0,
            };
        }
        const { kept, ignoredDeletes, ignoredUpserts } = splitIgnored(deltas, isIgnored);
        for (const d of ignoredDeletes) syncBase.removeEntry(d.path);
        if (ignoredUpserts > 0 || ignoredDeletes.length > 0) {
            console.log(
                `[obsetync] pull: skipped ${ignoredUpserts} ignored addition(s), ` +
                `untracked ${ignoredDeletes.length} ignored deletion(s)`,
            );
        }
        const endApply = perf?.phase("apply");
        let applyResult: Awaited<ReturnType<typeof applyDeltas>>;
        try {
            applyResult = await applyDeltas(
                api,
                io,
                syncBase,
                wasm,
                kept,
                onProgress,
                skipPaths,
                onWritesKnown,
                perf,
            );
        } finally {
            endApply?.();
        }
        const { deferred, downloaded, localDeferredCount, bytesDownloaded } = applyResult;
        perf?.setWorkload({
            filesNeeded: downloaded,
            bytesNeeded: bytesDownloaded,
        });
        perf?.increment({
            filesCompleted: kept.length - deferred.length,
            bytesTransferred: bytesDownloaded,
        });

        // Rebase: sync-base was just seeded with the full server state, so a
        // fresh bootstrap from it materializes the server's tree locally.
        // Deferred files never got a sync-base entry, so the bootstrap already
        // excludes them; filter the delta list too for the incremental branch.
        const appliedDeltas = excludeDeltas(kept, deferred).concat(ignoredDeletes);
        const endTree = perf?.phase("tree_update");
        const deltasHadMtime = rebaseTree(tree, syncBase, appliedDeltas);
        endTree?.();

        // Establish newRootHash + raw root bytes from the server's current
        // root. Caller persists the bytes to cached-root.bin so restart
        // doesn't force another full re-seed.
        let newRootHash: string | null = null;
        let newRootBytes: Uint8Array | null = null;
        try {
            newRootBytes = await api.getRoot(vaultId, perf);
            if (newRootBytes && wasm) {
                newRootHash = wasm.wasm_root_hash_from_bytes(newRootBytes) ?? null;
            }
        } catch (e) {
            console.warn("[obsetync] first-sync root-hash fetch failed:", e);
        }

        syncBase.setLastSyncTimestamp(Date.now());
        const endCheckpoint = perf?.phase("checkpoint");
        try {
            await syncBase.save();
        } finally {
            endCheckpoint?.();
        }
        onProgress?.(`first sync: applied ${kept.length - deferred.length} files`);
        return {
            newRootHash,
            newRootBytes,
            applied: kept.length - deferred.length,
            treeParity: parity(tree, newRootHash),
            deltasHadMtime,
            deferredCount: deferred.length,
            localDeferredCount,
            downloaded,
        };
    }

    onProgress?.("checking for remote changes...");
    const endCheck = perf?.phase("check");
    let deltas: FileDelta[] | null;
    try {
        deltas = await api.getDiff(vaultId, localRootHash, perf);
    } finally {
        endCheck?.();
    }
    setDeltaWorkload(perf, deltas ?? []);

    if (!deltas || deltas.length === 0) {
        perf?.setWorkload({ filesNeeded: 0, bytesNeeded: 0 });
        // Empty delta list can mean one of two things:
        //
        //   (a) Same root on both sides — the encrypted semantic status is
        //       304 and the API wrapper returns null.
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
        const endCheckpoint = perf?.phase("checkpoint");
        try {
            await syncBase.save();
        } finally {
            endCheckpoint?.();
        }

        let newRootHash: string | null = localRootHash;
        let newRootBytes: Uint8Array | null = null;
        try {
            newRootBytes = await api.getRoot(vaultId, perf);
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
            deferredCount: 0,
            localDeferredCount: 0,
            downloaded: 0,
        };
    }

    onProgress?.(`${deltas.length} changes to apply`);
    const { kept, ignoredDeletes, ignoredUpserts } = splitIgnored(deltas, isIgnored);
    // Untrack ignored paths the server dropped (a purge) WITHOUT touching disk.
    for (const d of ignoredDeletes) syncBase.removeEntry(d.path);
    if (ignoredUpserts > 0 || ignoredDeletes.length > 0) {
        console.log(
            `[obsetync] pull: skipped ${ignoredUpserts} ignored upsert(s), ` +
            `untracked ${ignoredDeletes.length} ignored deletion(s)`,
        );
    }
    const endApply = perf?.phase("apply");
    let applyResult: Awaited<ReturnType<typeof applyDeltas>>;
    try {
        applyResult = await applyDeltas(
            api,
            io,
            syncBase,
            wasm,
            kept,
            onProgress,
            skipPaths,
            onWritesKnown,
            perf,
        );
    } finally {
        endApply?.();
    }
    const { deferred, downloaded, localDeferredCount, bytesDownloaded } = applyResult;
    perf?.setWorkload({
        filesNeeded: downloaded,
        bytesNeeded: bytesDownloaded,
    });
    perf?.increment({
        filesCompleted: kept.length - deferred.length,
        bytesTransferred: bytesDownloaded,
    });

    // Rebase the Merkle tree with the exact deltas just applied to disk +
    // sync-base. THE key invariant of the pull path: tree, sync-base, and
    // disk advance together or not at all — so DEFERRED (unfetched) files are
    // excluded here, or the tree would claim content that never hit disk.
    // Ignored deletions ARE included: they drop the leaf so the tree converges
    // with a server that purged them.
    const appliedDeltas = excludeDeltas(kept, deferred).concat(ignoredDeletes);
    const endTree = perf?.phase("tree_update");
    const deltasHadMtime = rebaseTree(tree, syncBase, appliedDeltas);
    endTree?.();

    // Extract the new root hash from the server's current root bytes so
    // subsequent incremental syncs know what to diff against.
    let newRootHash: string | null = localRootHash;
    let newRootBytes: Uint8Array | null = null;
    try {
        newRootBytes = await api.getRoot(vaultId, perf);
        if (newRootBytes && wasm) {
            newRootHash = wasm.wasm_root_hash_from_bytes(newRootBytes) ?? localRootHash;
        }
    } catch (e) {
        console.warn("[obsetync] post-pull root-hash fetch failed:", e);
    }

    syncBase.setLastSyncTimestamp(Date.now());
    const endCheckpoint = perf?.phase("checkpoint");
    try {
        await syncBase.save();
    } finally {
        endCheckpoint?.();
    }

    return {
        newRootHash,
        newRootBytes,
        applied: kept.length - deferred.length,
        treeParity: parity(tree, newRootHash),
        deltasHadMtime,
        deferredCount: deferred.length,
        localDeferredCount,
        downloaded,
    };
}

function setDeltaWorkload(perf: PerfOperation | undefined, deltas: FileDelta[]): void {
    perf?.setWorkload({
        filesTotal: deltas.length,
        bytesTotal: deltas.reduce(
            (sum, delta) => sum + (delta.action === "deleted" ? 0 : delta.size ?? 0),
            0,
        ),
    });
}

async function tracedHashFile(
    path: string,
    io: PlatformIO,
    wasm: WasmModule,
    perf?: PerfOperation,
): Promise<string> {
    const endHash = perf?.phase("hash");
    try {
        return await hashFileStreaming(path, io, wasm);
    } finally {
        endHash?.();
    }
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
        if (d.action === "renamed" && d.old_path) {
            const oldIgnored = isIgnored(d.old_path);
            const targetIgnored = isIgnored(d.path);
            if (oldIgnored && !targetIgnored) {
                // Crossing into sync scope is an addition. Never rename the
                // user's ignored local source out from under them.
                kept.push({
                    action: "added",
                    path: d.path,
                    hash: d.hash,
                    size: d.size,
                    mtime_ms: d.mtime_ms,
                });
                continue;
            }
            if (targetIgnored) {
                // Crossing out of sync scope removes the formerly tracked
                // source but deliberately leaves all ignored disk paths alone.
                ignoredDeletes.push({ action: "deleted", path: d.old_path });
                ignoredUpserts++;
                continue;
            }
        }
        if (isIgnored(d.path)) {
            if (d.action === "deleted") ignoredDeletes.push(d);
            else ignoredUpserts++;
            continue;
        }
        kept.push(d);
    }
    return { kept, ignoredDeletes, ignoredUpserts };
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
 * - Tree live → apply deletions and upserts to one candidate, then atomically
 *   commit it. A failure aborts the candidate and preserves the committed
 *   root, so pull can never strand a half-rebased tree.
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
        if (deletePaths.length === 0 && upserts.length === 0) return allHadMtime;

        try {
            tree.begin_candidate();
            if (deletePaths.length > 0) {
                tree.candidate_delete_batch(JSON.stringify(deletePaths));
            }
            if (upserts.length > 0) {
                tree.candidate_update_batch(JSON.stringify(upserts));
            }
            tree.commit_candidate();
        } catch (error) {
            // commit_candidate validates the complete graph before advancing
            // the committed root. If any candidate operation or that final
            // validation fails, discard all newly-created chunks together.
            if (tree.has_candidate()) {
                try {
                    tree.abort_candidate();
                } catch (abortError) {
                    console.error("[obsetync] failed to abort pull tree candidate:", abortError);
                }
            }
            throw error;
        }
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
    skipPaths?: PullPathGuard,
    onWritesKnown?: (writes: PullWriteExpectation[]) => void,
    perf?: PerfOperation,
): Promise<{
    deferred: FileDelta[];
    downloaded: number;
    localDeferredCount: number;
    bytesDownloaded: number;
}> {
    const renames: FileDelta[] = [];
    const deletions: FileDelta[] = [];
    const modifications: FileDelta[] = [];
    const additions: FileDelta[] = [];
    const locallyDeferred: FileDelta[] = [];
    const locallyDeferredSet = new Set<FileDelta>();
    const shouldSkip = (delta: FileDelta): boolean =>
        !!skipPaths && (
            skipPaths.has(delta.path) ||
            (delta.old_path !== undefined && skipPaths.has(delta.old_path))
        );
    const deferLocal = (delta: FileDelta): void => {
        if (locallyDeferredSet.has(delta)) return;
        locallyDeferredSet.add(delta);
        locallyDeferred.push(delta);
    };
    for (const d of deltas) {
        // Locally-edited paths keep their disk bytes; the pending push +
        // server merge reconcile them. (Renames are included when either
        // end touches an edited path.)
        if (shouldSkip(d)) {
            deferLocal(d);
            continue;
        }
        if (d.action === "renamed") renames.push(d);
        else if (d.action === "deleted") deletions.push(d);
        else if (d.action === "modified") modifications.push(d);
        else additions.push(d);
    }
    if (locallyDeferred.length > 0) {
        console.log(
            `[obsetync] pull deferred ${locallyDeferred.length} locally-edited file(s): ` +
            `${locallyDeferred.slice(0, 3).map((d) => d.path).join(", ")}` +
            `${locallyDeferred.length > 3 ? ", …" : ""} — ` +
            `honest base retained for server-side merge`,
        );
    }

    const writes: PullWriteExpectation[] = [];
    for (const delta of renames) {
        if (delta.old_path) writes.push({ path: delta.old_path, action: "delete" });
        if (delta.hash) writes.push({ path: delta.path, action: "upsert", hash: delta.hash });
    }
    // Deletions are registered immediately before the adapter mutation, only
    // after proving the disk still holds the known base. Unlike an upsert,
    // a delete echo has no content hash with which to authenticate an early
    // expectation.
    for (const delta of [...modifications, ...additions]) {
        if (delta.hash) writes.push({ path: delta.path, action: "upsert", hash: delta.hash });
    }
    if (writes.length > 0) onWritesKnown?.(writes);

    for (const delta of renames) {
        // Re-check immediately before touching disk: this guard is live and
        // can observe an editor event that arrived after delta partitioning.
        if (shouldSkip(delta)) {
            deferLocal(delta);
            continue;
        }
        if (delta.old_path && delta.hash) {
            const oldBase = syncBase.getEntry(delta.old_path);
            const targetBase = syncBase.getEntry(delta.path);
            const markerPath = renameCheckpointPath(wasm, delta.old_path, delta.path, delta.hash);
            if (await io.exists(delta.old_path)) {
                // A rename target should be absent in the source tree. Never
                // overwrite an unexpected local file just to apply the delta.
                if (await io.exists(delta.path)) {
                    deferLocal(delta);
                    continue;
                }
                const sourceStat = await io.stat(delta.old_path);
                const metadataStillMatches = !!oldBase && !!sourceStat &&
                    oldBase.hash === delta.hash &&
                    oldBase.mtime === sourceStat.mtime && oldBase.size === sourceStat.size;
                if (!metadataStillMatches) {
                    const canVerifyWithoutLargeMobileRead = !!wasm && !!sourceStat && (
                        sourceStat.size < CHUNK_THRESHOLD ||
                        io.getAbsolutePath(delta.old_path) !== null
                    );
                    if (!canVerifyWithoutLargeMobileRead) {
                        deferLocal(delta);
                        continue;
                    }
                    try {
                        if (await tracedHashFile(delta.old_path, io, wasm!, perf) !== delta.hash) {
                            deferLocal(delta);
                            continue;
                        }
                    } catch {
                        deferLocal(delta);
                        continue;
                    }
                }
                // Persist intent after verifying source + target state but
                // before the non-idempotent adapter rename. If the renderer
                // dies after the move and before sync-base is checkpointed,
                // the next run can distinguish our completed rename from an
                // unrelated same-size file already at the target.
                if (!markerPath) {
                    deferLocal(delta);
                    continue;
                }
                try {
                    await writeRenameCheckpoint(io, markerPath, {
                        version: 1,
                        oldPath: delta.old_path,
                        targetPath: delta.path,
                        fileHash: delta.hash.toLowerCase(),
                        size: delta.size ?? sourceStat?.size ?? oldBase?.size ?? 0,
                    });
                } catch {
                    deferLocal(delta);
                    continue;
                }
                if (shouldSkip(delta) || await io.exists(delta.path)) {
                    await safeDelete(io, markerPath);
                    deferLocal(delta);
                    continue;
                }
                await io.renameFile(delta.old_path, delta.path);
            } else {
                // Crash recovery: adapter.rename() may have committed before
                // the sync-base checkpoint. A target already present in the
                // base is independently durable; otherwise require either a
                // content hash or the exact pre-rename WAL marker plus the
                // source's unchanged local metadata. Size alone is not proof.
                const targetStat = await io.stat(delta.path);
                let targetVerified = !!targetStat && !!targetBase &&
                    targetBase.hash === delta.hash &&
                    targetBase.mtime === targetStat.mtime &&
                    targetBase.size === targetStat.size;
                if (!targetVerified && targetStat) {
                    const canHashTarget = !!wasm && (
                        targetStat.size < CHUNK_THRESHOLD ||
                        io.getAbsolutePath(delta.path) !== null
                    );
                    if (canHashTarget) {
                        try {
                            targetVerified =
                                await tracedHashFile(delta.path, io, wasm!, perf) === delta.hash;
                        } catch {
                            targetVerified = false;
                        }
                    }
                }
                if (!targetVerified && targetStat && markerPath && oldBase) {
                    const marker = await readRenameCheckpoint(io, markerPath);
                    targetVerified = !!marker &&
                        marker.version === 1 &&
                        marker.oldPath === delta.old_path &&
                        marker.targetPath === delta.path &&
                        marker.fileHash === delta.hash.toLowerCase() &&
                        marker.size === targetStat.size &&
                        oldBase.hash === delta.hash &&
                        oldBase.mtime === targetStat.mtime &&
                        oldBase.size === targetStat.size;
                }
                if (!targetVerified) {
                    deferLocal(delta);
                    continue;
                }
            }
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
            // Rename is the only non-idempotent metadata operation. Persist it
            // immediately so a renderer kill never asks the next run to rename
            // an already-moved source again.
            const endCheckpoint = perf?.phase("checkpoint");
            try {
                await syncBase.checkpoint();
            } finally {
                endCheckpoint?.();
            }
            if (markerPath) await safeDelete(io, markerPath);
        } else {
            deferLocal(delta);
        }
    }

    let appliedDeletions = 0;
    for (const delta of deletions) {
        if (shouldSkip(delta)) {
            deferLocal(delta);
            continue;
        }
        // Startup pull runs before the metadata audit. An edit made while the
        // plugin was unloaded therefore has no journal row yet; compare disk
        // with the honest sync base before honoring a remote delete. The
        // later metadata audit will queue a deferred local version for merge.
        if (!(await diskStillMatchesDeleteBase(io, syncBase, wasm, delta.path, perf))) {
            console.warn(
                `[obsetync] deferred remote delete for locally changed ${delta.path}`,
            );
            deferLocal(delta);
            continue;
        }
        if (shouldSkip(delta)) {
            deferLocal(delta);
            continue;
        }
        onWritesKnown?.([{ path: delta.path, action: "delete" }]);
        await io.deleteFile(delta.path);
        syncBase.removeEntry(delta.path);
        appliedDeletions++;
    }
    if (appliedDeletions > 0) {
        const endCheckpoint = perf?.phase("checkpoint");
        try {
            await syncBase.checkpoint();
        } finally {
            endCheckpoint?.();
        }
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
    let completed = 0;
    const checkpointAndReport = async (batchLength: number): Promise<void> => {
        // Persist every completed batch as a tiny WAL append. A process kill
        // resumes from here without re-hashing all already-applied files.
        const endCheckpoint = perf?.phase("checkpoint");
        try {
            await syncBase.checkpoint();
        } finally {
            endCheckpoint?.();
        }
        completed += batchLength;
        const verified = stats.cacheHit + stats.localHit;
        const elapsed = (Date.now() - startedAt) / 1000;
        const rate = elapsed > 0 ? ` · ${(completed / elapsed).toFixed(0)} f/s` : "";
        const failMsg = failed.length > 0 ? ` · ✗${failed.length} deferred` : "";
        onProgress?.(
            `${completed}/${toDownload.length} files applied · ` +
            `✓${verified} verified · ↓${stats.downloaded} (${fmtBytes(stats.bytesDownloaded)})${failMsg}${rate}`
        );
    };

    const applyLargeBatch = async (batch: FileDelta[]) => {
        const results = await Promise.allSettled(
            batch.map((delta) => applyContentDelta(
                api,
                io,
                syncBase,
                wasm,
                delta,
                stats,
                () => shouldSkip(delta),
                perf,
            ))
        );
        results.forEach((r, j) => {
            if (r.status === "rejected") failed.push(batch[j]);
            else if (!r.value) deferLocal(batch[j]);
        });
        await checkpointAndReport(batch.length);
    };

    const applySmallBatch = async (batch: FileDelta[]): Promise<void> => {
        const preparations = await Promise.allSettled(batch.map((delta) =>
            prepareContentDelta(
                io,
                syncBase,
                wasm,
                delta,
                stats,
                () => shouldSkip(delta),
                perf,
            )));
        const pending: Array<{ delta: FileDelta; preparation: PendingContentDownload }> = [];
        preparations.forEach((result, index) => {
            const delta = batch[index];
            if (result.status === "rejected") {
                failed.push(delta);
            } else if (result.value.kind === "deferred") {
                deferLocal(delta);
            } else if (result.value.kind === "download") {
                pending.push({ delta, preparation: result.value });
            }
        });

        if (pending.length > 0) {
            let downloaded: Map<string, Uint8Array>;
            try {
                const endDownload = perf?.phase("download");
                try {
                    downloaded = await getSmallContentBatch(
                        api,
                        pending.map(({ delta }) => delta.hash!),
                        perf,
                    );
                } finally {
                    endDownload?.();
                }
            } catch {
                // A page is the retry unit. Keep every path honest and let the
                // existing one-pass recovery retry them independently below.
                failed.push(...pending.map(({ delta }) => delta));
                await checkpointAndReport(batch.length);
                return;
            }
            const countedHashes = new Set<string>();
            const applied = await Promise.allSettled(pending.map(({ delta, preparation }) => {
                const canonicalHash = delta.hash!.toLowerCase();
                const countTransferredBytes = !countedHashes.has(canonicalHash);
                countedHashes.add(canonicalHash);
                return finishContentDownload(
                    api,
                    io,
                    syncBase,
                    wasm,
                    delta,
                    stats,
                    preparation,
                    () => shouldSkip(delta),
                    perf,
                    downloaded.get(canonicalHash),
                    countTransferredBytes,
                );
            }));
            applied.forEach((result, index) => {
                if (result.status === "rejected") failed.push(pending[index].delta);
                else if (!result.value) deferLocal(pending[index].delta);
            });
        }
        await checkpointAndReport(batch.length);
    };

    // Small files retain bounded parallelism. Large files run strictly one at
    // a time: six simultaneous encrypted/chunked transfers were enough to
    // exceed the practical Jetsam limit on older 2 GB iPads.
    const smallDownloads = toDownload.filter((delta) => (delta.size ?? 0) < CHUNK_THRESHOLD);
    const largeDownloads = toDownload.filter((delta) => (delta.size ?? 0) >= CHUNK_THRESHOLD);
    const desktop = smallDownloads.length > 0 &&
        io.getAbsolutePath(smallDownloads[0].path) !== null;
    const smallBatches = planByteBoundedBatches(
        smallDownloads,
        (delta) => delta.size ?? 0,
        {
            maxFiles: BULK_DOWNLOAD_FILES,
            maxBytes: desktop ? DESKTOP_BULK_DOWNLOAD_BYTES : MOBILE_BULK_DOWNLOAD_BYTES,
            maxSingleBytes: CHUNK_THRESHOLD - 1,
        },
    );
    for (const batch of smallBatches) {
        await applySmallBatch(batch);
    }
    for (const delta of largeDownloads) {
        await applyLargeBatch([delta]);
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
                const applied = await applyContentDelta(
                    api,
                    io,
                    syncBase,
                    wasm,
                    delta,
                    stats,
                    () => shouldSkip(delta),
                    perf,
                );
                if (!applied) {
                    deferLocal(delta);
                    continue;
                }
                const endCheckpoint = perf?.phase("checkpoint");
                try {
                    await syncBase.checkpoint();
                } finally {
                    endCheckpoint?.();
                }
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
        const deferredTotal = locallyDeferred.length + unfetched.length;
        console.log(
            `[obsetync] applyDeltas: ${stats.cacheHit} cache-hit, ` +
            `${stats.localHit} local-hash-hit, ${stats.downloaded} downloaded, ` +
            `${deferredTotal} deferred — ` +
            `${fmtBytes(stats.bytesSkipped)} saved, ${fmtBytes(stats.bytesDownloaded)} transferred`
        );
    }
    return {
        deferred: locallyDeferred.concat(unfetched),
        downloaded: stats.downloaded,
        localDeferredCount: locallyDeferred.length,
        bytesDownloaded: stats.bytesDownloaded,
    };
}

/** Human-readable byte count for progress messages. */
function fmtBytes(n: number): string {
    if (n >= 1_048_576) return `${(n / 1_048_576).toFixed(1)} MB`;
    if (n >= 1024) return `${(n / 1024).toFixed(0)} KB`;
    return `${n} B`;
}

/** A remote delete is safe only when the target is absent or is still the
 * exact last-synced base. Stat equality is the zero-read fast path; changed
 * metadata is resolved by a bounded small/mobile or streaming desktop hash.
 * Large mobile files with changed metadata are deferred rather than read as
 * one huge JS buffer immediately before deletion. */
async function diskStillMatchesDeleteBase(
    io: PlatformIO,
    syncBase: ObsetyncSyncBase,
    wasm: WasmModule | null,
    path: string,
    perf?: PerfOperation,
): Promise<boolean> {
    const stat = await io.stat(path);
    if (!stat) return true;
    const base = syncBase.getEntry(path);
    if (!base) return false;
    if (base.mtime === stat.mtime && base.size === stat.size) return true;

    const canHashWithoutLargeMobileRead =
        stat.size < CHUNK_THRESHOLD || io.getAbsolutePath(path) !== null;
    if (!wasm || !canHashWithoutLargeMobileRead) return false;
    try {
        return await tracedHashFile(path, io, wasm, perf) === base.hash;
    } catch {
        return false;
    }
}

type ContentPreparation =
    | { kind: "applied" }
    | { kind: "deferred" }
    | PendingContentDownload;

interface PendingContentDownload {
    kind: "download";
    size: number;
    preserveExisting: boolean;
}

/** Resolve the two zero-network tiers first. This separation lets a large
 *  delta set collect only genuine misses into bounded bulk download pages. */
async function prepareContentDelta(
    io: PlatformIO,
    syncBase: ObsetyncSyncBase,
    wasm: WasmModule | null,
    delta: FileDelta,
    stats: ApplyStats,
    shouldDefer?: () => boolean,
    perf?: PerfOperation,
): Promise<ContentPreparation> {
    if (!delta.hash) return { kind: "applied" };
    if (shouldDefer?.()) return { kind: "deferred" };

    const size = delta.size ?? 0;
    let preserveExisting = false;

    // --- Tier 1: sync-base cache hit --------------------------------------
    // If sync-base already records this path at this exact hash AND the
    // on-disk (mtime, size) match the sync-base entry, we know the file is
    // byte-identical to what the server wants. Zero work.
    const stat = await io.stat(delta.path);
    if (stat) {
        const base = syncBase.getEntry(delta.path);
        const metadataMatchesBase = !!base &&
            base.mtime === stat.mtime && base.size === stat.size;
        if (
            base &&
            base.hash === delta.hash &&
            base.mtime === stat.mtime &&
            base.size === stat.size
        ) {
            if (shouldDefer?.()) return { kind: "deferred" };
            // Content and local disk metadata are already right, but a
            // metadata-only server delta can still change the tree mtime.
            // Record it so the rebased Merkle root reproduces the server.
            if (
                delta.mtime_ms !== undefined &&
                syncBase.getTreeMtime(delta.path) !== delta.mtime_ms
            ) {
                syncBase.setEntry(
                    delta.path,
                    delta.hash,
                    stat.mtime,
                    stat.size,
                    delta.mtime_ms,
                );
            }
            stats.cacheHit++;
            stats.bytesSkipped += size || stat.size;
            return { kind: "applied" };
        }

        // --- Tier 2: local hash matches target --------------------------
        // sync-base disagrees (or is missing) but the on-disk file hashes
        // to the exact value the server is offering. Common after a
        // rollback or stub-WASM recovery — the content is correct, only
        // our metadata was stale. Repair sync-base and skip the download.
        // Desktop streams this check from Node fs. Obsidian mobile exposes no
        // ranged read, so hashing an existing large file would allocate the
        // whole file immediately before downloading its replacement — a bad
        // peak-memory trade on older iPads.
        const canHashLocallyWithoutLargeMobileRead =
            stat.size < CHUNK_THRESHOLD || io.getAbsolutePath(delta.path) !== null;
        if (wasm && canHashLocallyWithoutLargeMobileRead) {
            try {
                const actualHash = await tracedHashFile(delta.path, io, wasm, perf);
                if (actualHash === delta.hash) {
                    if (shouldDefer?.()) return { kind: "deferred" };
                    syncBase.setEntry(
                        delta.path,
                        delta.hash,
                        stat.mtime,
                        stat.size,
                        delta.mtime_ms,
                    );
                    stats.localHit++;
                    stats.bytesSkipped += size || stat.size;
                    return { kind: "applied" };
                }
                // The target differs, but overwriting is safe when disk still
                // holds the exact previously-synced base. Any third hash is an
                // unjournaled/local collision and must be preserved first.
                preserveExisting = !base || actualHash !== base.hash;
            } catch (e) {
                // Hash failed (read error, permission issue, etc.) — fall
                // through to the download path so we still end up correct.
                console.warn(`[obsetync] local-hash check failed for ${delta.path}:`, e);
                preserveExisting = !metadataMatchesBase;
            }
        } else {
            // Large mobile files cannot be reread without one huge JS
            // allocation. Exact sync-base metadata is our safe fast path;
            // anything else is moved aside without reading its bytes.
            preserveExisting = !metadataMatchesBase;
        }
    }

    if (shouldDefer?.()) return { kind: "deferred" };
    return { kind: "download", size, preserveExisting };
}

async function finishContentDownload(
    api: ObsetyncApi,
    io: PlatformIO,
    syncBase: ObsetyncSyncBase,
    wasm: WasmModule | null,
    delta: FileDelta,
    stats: ApplyStats,
    preparation: PendingContentDownload,
    shouldDefer?: () => boolean,
    perf?: PerfOperation,
    prefetchedSmallData?: Uint8Array,
    countTransferredBytes = true,
): Promise<boolean> {
    if (!delta.hash) return true;
    const { size, preserveExisting } = preparation;

    // --- Tier 3: actual download from server -----------------------------
    if (shouldDefer?.()) return false;
    if (size >= CHUNK_THRESHOLD) {
        try {
            await applyLargeFile(
                api,
                io,
                delta.path,
                delta.hash,
                size,
                wasm,
                shouldDefer,
                preserveExisting,
                perf,
            );
        } catch (error) {
            if (error instanceof LocalEditDuringPull) return false;
            throw error;
        }
    } else {
        let data: Uint8Array;
        if (prefetchedSmallData) {
            data = prefetchedSmallData;
        } else {
            const endDownload = perf?.phase("download");
            try {
                data = await api.getContent(delta.hash, perf);
            } finally {
                endDownload?.();
            }
        }
        const endHash = perf?.phase("hash");
        let actualHash: string | null = null;
        try {
            actualHash = wasm ? wasm.wasm_hash(data).toLowerCase() : null;
        } finally {
            endHash?.();
        }
        if (!actualHash || actualHash !== delta.hash.toLowerCase()) {
            throw new Error(`small-file content hash mismatch for ${delta.path}`);
        }
        if (shouldDefer?.()) return false;
        if (preserveExisting && await io.exists(delta.path)) {
            const conflictPath = await uniqueLocalConflictPath(io, delta.path);
            await io.renameFile(delta.path, conflictPath);
            console.warn(
                `[obsetync] preserved unsynced local bytes as ${conflictPath} before pull`,
            );
        }
        await io.writeFile(delta.path, data);
    }
    stats.downloaded++;
    if (countTransferredBytes) stats.bytesDownloaded += size;

    const postStat = await io.stat(delta.path);
    syncBase.setEntry(
        delta.path,
        delta.hash,
        postStat?.mtime ?? Date.now(),
        postStat?.size ?? size,
        delta.mtime_ms,
    );
    return true;
}

async function applyContentDelta(
    api: ObsetyncApi,
    io: PlatformIO,
    syncBase: ObsetyncSyncBase,
    wasm: WasmModule | null,
    delta: FileDelta,
    stats: ApplyStats,
    shouldDefer?: () => boolean,
    perf?: PerfOperation,
): Promise<boolean> {
    const preparation = await prepareContentDelta(
        io,
        syncBase,
        wasm,
        delta,
        stats,
        shouldDefer,
        perf,
    );
    if (preparation.kind === "applied") return true;
    if (preparation.kind === "deferred") return false;
    return finishContentDownload(
        api,
        io,
        syncBase,
        wasm,
        delta,
        stats,
        preparation,
        shouldDefer,
        perf,
    );
}

async function getSmallContentBatch(
    api: ObsetyncApi,
    hashes: readonly string[],
    perf?: PerfOperation,
): Promise<Map<string, Uint8Array>> {
    // Test/old embedding compatibility: production ObsetyncApi always has
    // getObjects, while small pure-unit fixtures may implement only getContent.
    const bulk = (api as any).getObjects as
        | ((kind: BulkObjectKind, hashes: readonly string[], perf?: PerfOperation) =>
            Promise<Map<string, Uint8Array>>)
        | undefined;
    if (bulk) return bulk.call(api, BulkObjectKind.Content, hashes, perf);
    const output = new Map<string, Uint8Array>();
    await Promise.all([...new Set(hashes.map((hash) => hash.toLowerCase()))].map(async (hash) => {
        output.set(hash, await api.getContent(hash, perf));
    }));
    return output;
}

class LocalEditDuringPull extends Error {}

interface RenameTransferCheckpoint {
    version: 1;
    oldPath: string;
    targetPath: string;
    fileHash: string;
    size: number;
}

function renameCheckpointPath(
    wasm: WasmModule | null,
    oldPath: string,
    targetPath: string,
    fileHash: string,
): string | null {
    if (!wasm) return null;
    try {
        const identity = new TextEncoder().encode(`${oldPath}\0${targetPath}`);
        const pathHash = wasm.wasm_hash(identity).slice(0, 16).toLowerCase();
        if (!/^[0-9a-f]{16}$/.test(pathHash)) return null;
        const address = /^[0-9a-f]{64}$/i.test(fileHash)
            ? fileHash.slice(0, 16).toLowerCase()
            : wasm.wasm_hash(new TextEncoder().encode(fileHash)).slice(0, 16).toLowerCase();
        if (!/^[0-9a-f]{16}$/.test(address)) return null;
        return `${TRANSFER_DIR}/${address}-${pathHash}.rename.json`;
    } catch {
        return null;
    }
}

async function readRenameCheckpoint(
    io: PlatformIO,
    path: string,
): Promise<RenameTransferCheckpoint | null> {
    try {
        const value = JSON.parse(
            new TextDecoder().decode(await io.readFile(path)),
        ) as Partial<RenameTransferCheckpoint>;
        if (
            value?.version !== 1 ||
            typeof value.oldPath !== "string" ||
            typeof value.targetPath !== "string" ||
            typeof value.fileHash !== "string" ||
            !Number.isSafeInteger(value.size) ||
            value.size! < 0
        ) {
            return null;
        }
        return value as RenameTransferCheckpoint;
    } catch {
        return null;
    }
}

async function writeRenameCheckpoint(
    io: PlatformIO,
    path: string,
    checkpoint: RenameTransferCheckpoint,
): Promise<void> {
    await io.writeFile(path, new TextEncoder().encode(JSON.stringify(checkpoint)));
}

interface LargeTransferCheckpoint {
    version: 1;
    targetPath: string;
    fileHash: string;
    totalSize: number;
    nextChunk: number;
    bytesWritten: number;
}

/** Reject manifests that could overlap, leave holes, overrun allocations, or
 *  point at a different content address. Returned values are safe JS ints. */
export function validateManifest(
    value: FileManifest,
    expectedHash: string,
    expectedSize?: number,
): FileManifest {
    const manifest = value as Partial<FileManifest> | null;
    if (!manifest || typeof manifest !== "object") throw new Error("invalid large-file manifest");
    const canonicalHash = expectedHash.toLowerCase();
    if (!/^[0-9a-f]{64}$/.test(canonicalHash) || manifest.file_hash?.toLowerCase() !== canonicalHash) {
        throw new Error("large-file manifest hash mismatch");
    }
    if (!Number.isSafeInteger(manifest.total_size) || manifest.total_size! < 0) {
        throw new Error("invalid large-file total size");
    }
    if (expectedSize !== undefined && expectedSize > 0 && manifest.total_size !== expectedSize) {
        throw new Error("large-file manifest size disagrees with tree entry");
    }
    if (!Array.isArray(manifest.chunks) || manifest.chunks.length > 1_000_000) {
        throw new Error("invalid large-file chunk list");
    }

    let expectedOffset = 0;
    for (const chunk of manifest.chunks) {
        if (
            !chunk ||
            typeof chunk.hash !== "string" ||
            !/^[0-9a-f]{64}$/i.test(chunk.hash) ||
            !Number.isSafeInteger(chunk.offset) ||
            !Number.isSafeInteger(chunk.size) ||
            chunk.offset !== expectedOffset ||
            chunk.size <= 0 ||
            chunk.size > MAX_CONTENT_CHUNK
        ) {
            throw new Error("invalid large-file chunk layout");
        }
        expectedOffset += chunk.size;
        if (!Number.isSafeInteger(expectedOffset) || expectedOffset > manifest.total_size!) {
            throw new Error("large-file chunk layout exceeds total size");
        }
    }
    if (expectedOffset !== manifest.total_size) {
        throw new Error("large-file chunk layout does not cover the file");
    }
    if (manifest.total_size > 0 && manifest.chunks.length === 0) {
        throw new Error("non-empty large file has no chunks");
    }
    return manifest as FileManifest;
}

async function getManifestForPull(
    api: ObsetyncApi,
    hash: string,
    perf?: PerfOperation,
): Promise<FileManifest> {
    const bulk = (api as any).getObjects as
        | ((kind: BulkObjectKind, hashes: readonly string[], perf?: PerfOperation) =>
            Promise<Map<string, Uint8Array>>)
        | undefined;
    if (!bulk) return api.getManifest(hash, perf);
    const objects = await bulk.call(api, BulkObjectKind.Manifest, [hash], perf);
    const bytes = objects.get(hash.toLowerCase());
    if (!bytes) throw new Error(`manifest ${hash} missing from bulk response`);
    try {
        return JSON.parse(new TextDecoder().decode(bytes)) as FileManifest;
    } catch {
        throw new Error(`manifest ${hash} is not valid JSON`);
    }
}

async function getContentChunkBatch(
    api: ObsetyncApi,
    hashes: readonly string[],
    perf?: PerfOperation,
): Promise<Map<string, Uint8Array>> {
    const bulk = (api as any).getObjects as
        | ((kind: BulkObjectKind, hashes: readonly string[], perf?: PerfOperation) =>
            Promise<Map<string, Uint8Array>>)
        | undefined;
    if (bulk) return bulk.call(api, BulkObjectKind.ContentChunk, hashes, perf);
    const output = new Map<string, Uint8Array>();
    await Promise.all([...new Set(hashes.map((hash) => hash.toLowerCase()))].map(async (hash) => {
        output.set(hash, await api.getContentChunk(hash, perf));
    }));
    return output;
}

/** Download a large file into an internal staging file. Each chunk is
 *  length/hash checked, appended, and durably checkpointed before proceeding;
 *  an iOS process kill resumes at the next chunk instead of starting over. */
export async function applyLargeFile(
    api: ObsetyncApi,
    io: PlatformIO,
    path: string,
    hash: string,
    expectedSize: number,
    wasm: WasmModule | null,
    shouldAbort?: () => boolean,
    preserveExisting = false,
    perf?: PerfOperation,
): Promise<void> {
    if (shouldAbort?.()) throw new LocalEditDuringPull();
    if (!wasm) throw new Error("WASM hash verifier unavailable for large file");
    const endManifestDownload = perf?.phase("download");
    let rawManifest: FileManifest;
    try {
        rawManifest = await getManifestForPull(api, hash, perf);
    } finally {
        endManifestDownload?.();
    }
    const manifest = validateManifest(rawManifest, hash, expectedSize);
    const endPathHash = perf?.phase("hash");
    let pathHash: string;
    try {
        pathHash = wasm.wasm_hash(new TextEncoder().encode(path)).slice(0, 16);
    } finally {
        endPathHash?.();
    }
    const transferKey = `${hash.toLowerCase()}-${pathHash}`;
    const stagingPath = `${TRANSFER_DIR}/${transferKey}.part`;
    const checkpointPath = `${TRANSFER_DIR}/${transferKey}.checkpoint.json`;

    let checkpoint = await readLargeCheckpoint(io, checkpointPath);
    const expectedBytes = (nextChunk: number): number =>
        nextChunk === manifest.chunks.length
            ? manifest.total_size
            : manifest.chunks[nextChunk]?.offset ?? -1;
    const stagingStat = await io.stat(stagingPath);
    const resumable =
        checkpoint?.version === 1 &&
        checkpoint.targetPath === path &&
        checkpoint.fileHash === hash.toLowerCase() &&
        checkpoint.totalSize === manifest.total_size &&
        Number.isInteger(checkpoint.nextChunk) &&
        checkpoint.nextChunk >= 0 &&
        checkpoint.nextChunk <= manifest.chunks.length &&
        checkpoint.bytesWritten === expectedBytes(checkpoint.nextChunk) &&
        stagingStat?.size === checkpoint.bytesWritten;

    if (!resumable) {
        await safeDelete(io, stagingPath);
        await safeDelete(io, checkpointPath);
        await io.writeFile(stagingPath, new Uint8Array());
        checkpoint = {
            version: 1,
            targetPath: path,
            fileHash: hash.toLowerCase(),
            totalSize: manifest.total_size,
            nextChunk: 0,
            bytesWritten: 0,
        };
        const endCheckpoint = perf?.phase("checkpoint");
        try {
            await writeLargeCheckpoint(io, checkpointPath, checkpoint);
        } finally {
            endCheckpoint?.();
        }
    }

    // A fresh transfer can prove the manifest's ordered concatenation while
    // bytes are already crossing the WASM boundary. Resumed prefixes were
    // individually verified before their checkpoint; mobile has no ranged
    // read with which to replay them without a whole-file allocation.
    const wholeHasher = checkpoint!.nextChunk === 0 ? new wasm.Hasher() : null;
    let assembledHash: string | null = null;
    try {
        const pendingChunks = manifest.chunks
            .map((chunk, index) => ({ chunk, index }))
            .slice(checkpoint!.nextChunk);
        const desktop = typeof (io as any).getAbsolutePath === "function" &&
            io.getAbsolutePath(path) !== null;
        const bulkEnabled = typeof (api as any).supportsBulkHttp === "function"
            ? await api.supportsBulkHttp(perf)
            : false;
        const chunkBatches = planByteBoundedBatches(
            pendingChunks,
            ({ chunk }) => chunk.size,
            {
                // Legacy GET keeps its original one-chunk transaction and
                // checkpoint semantics. Only an authenticated bulk page may
                // group several chunks before the first one is applied.
                maxFiles: bulkEnabled ? BULK_DOWNLOAD_FILES : 1,
                maxBytes: desktop ? DESKTOP_BULK_DOWNLOAD_BYTES : MOBILE_BULK_DOWNLOAD_BYTES,
                maxSingleBytes: CHUNK_THRESHOLD - 1,
            },
        );
        for (const batch of chunkBatches) {
            if (shouldAbort?.()) throw new LocalEditDuringPull();
            const endDownload = perf?.phase("download");
            let downloaded: Map<string, Uint8Array>;
            try {
                downloaded = await getContentChunkBatch(
                    api,
                    batch.map(({ chunk }) => chunk.hash),
                    perf,
                );
            } finally {
                endDownload?.();
            }
            for (const { chunk, index } of batch) {
                if (shouldAbort?.()) throw new LocalEditDuringPull();
                const data = downloaded.get(chunk.hash.toLowerCase());
                if (!data || data.length !== chunk.size) {
                    throw new Error(`large-file chunk ${index} length mismatch`);
                }
                const endHash = perf?.phase("hash");
                let actualHash: string;
                try {
                    actualHash = wholeHasher
                        ? wholeHasher.update_and_hash(data)
                        : wasm.wasm_hash(data);
                } finally {
                    endHash?.();
                }
                if (actualHash.toLowerCase() !== chunk.hash.toLowerCase()) {
                    throw new Error(`large-file chunk ${index} hash mismatch`);
                }
                await io.appendFile(stagingPath, data);
                checkpoint = {
                    ...checkpoint!,
                    nextChunk: index + 1,
                    bytesWritten: chunk.offset + chunk.size,
                };
                const endCheckpoint = perf?.phase("checkpoint");
                try {
                    await writeLargeCheckpoint(io, checkpointPath, checkpoint);
                } finally {
                    endCheckpoint?.();
                }
                // Apply/checkpoint records independently even though transport
                // grouped them; a kill repeats less than one completed pack.
                await new Promise<void>((resolve) => globalThis.setTimeout(resolve, 0));
            }
        }
        if (wholeHasher) assembledHash = wholeHasher.finalize().toLowerCase();
    } finally {
        wholeHasher?.free();
    }

    if (assembledHash !== null && assembledHash !== hash.toLowerCase()) {
        // Do not leave a completed-looking checkpoint: the next retry must
        // fetch from chunk zero rather than promoting known-wrong bytes.
        await safeDelete(io, stagingPath);
        await safeDelete(io, checkpointPath);
        throw new Error("large-file assembled content hash mismatch");
    }

    const completed = await io.stat(stagingPath);
    if (completed?.size !== manifest.total_size) {
        throw new Error("large-file staging size mismatch after download");
    }
    if (shouldAbort?.()) throw new LocalEditDuringPull();
    if (preserveExisting && await io.exists(path)) {
        const conflictPath = await uniqueLocalConflictPath(io, path);
        await io.renameFile(path, conflictPath);
        console.warn(
            `[obsetync] preserved unsynced local bytes as ${conflictPath} before pull`,
        );
    }
    await io.replaceFile(stagingPath, path);
    await safeDelete(io, checkpointPath);
}

async function readLargeCheckpoint(
    io: PlatformIO,
    path: string,
): Promise<LargeTransferCheckpoint | null> {
    try {
        return JSON.parse(new TextDecoder().decode(await io.readFile(path))) as LargeTransferCheckpoint;
    } catch {
        return null;
    }
}

async function writeLargeCheckpoint(
    io: PlatformIO,
    path: string,
    checkpoint: LargeTransferCheckpoint,
): Promise<void> {
    await io.writeFile(path, new TextEncoder().encode(JSON.stringify(checkpoint)));
}

async function safeDelete(io: PlatformIO, path: string): Promise<void> {
    try { await io.deleteFile(path); } catch { /* absent/stale file */ }
}

async function uniqueLocalConflictPath(io: PlatformIO, path: string): Promise<string> {
    const now = Date.now();
    for (let attempt = 0; attempt < 1_000; attempt++) {
        const candidate = conflictCopyPath(
            path,
            "local-before-pull",
            new Date(now + attempt * 60_000),
        );
        if (!(await io.exists(candidate))) return candidate;
    }
    throw new Error(`could not allocate a conflict-copy path for ${path}`);
}
