import { ObsetyncApi, FileDelta, type DiffPage, type FileManifest } from "./api";
import { PlatformIO } from "./platform";
import { ObsetyncSyncBase, type DiffPageCheckpoint } from "./sync-base";
import { hashFileStreaming, type WasmModule, type WasmTree } from "./push";
import type { PullWriteExpectation } from "./pull-echo";
import { conflictCopyPath } from "./conflict-path";
import type { PerfOperation } from "./perf-trace";
import { BulkObjectKind } from "./bulk-codec";
import { getHashTuning, planByteBoundedBatches } from "./hash-runtime";
import { diffCursorFromHex, diffCursorToHex } from "./diff-page-codec";
import type { TransientWorkScope } from "./transient-memory";
import { throwIfWorkAborted, yieldWork } from "./work-scheduler";
import {
    abortTreeCandidateAfterReachabilityRetirement,
    beginTreeCandidate,
    drainTreeReachabilityRetirement,
} from "./tree-candidate-job";
import {
    abortTreeCandidateAfterMutationRetirement,
    applyTreeCandidateMutation,
    drainTreeCandidateMutationRetirement,
    hasPendingTreeCandidateMutationRetirement,
} from "./tree-candidate-mutation-job";
import { admitCandidateMutationOutput } from "./candidate-mutation-output-admission";
import { admitCandidateOpenRoot } from "./candidate-open-root-admission";
import type { RootTreeResidentAdmission } from "./root-tree-resident-admission";
import { settleTreeCandidateOutputWithAdmission } from "./tree-output-settlement";

const CHUNK_THRESHOLD = 1_048_576; // 1MB
const MAX_CONTENT_CHUNK = 4 * 1_048_576;
const DESKTOP_BULK_DOWNLOAD_BYTES = 8 * 1_048_576;
const MOBILE_BULK_DOWNLOAD_BYTES = 2 * 1_048_576;
const BULK_DOWNLOAD_FILES = 256;
const TRANSFER_DIR = ".obsidian/plugins/obsetync/transfers";
const STAGING_CHECKPOINT_MAX_BYTES = 16 * 1024;
const STAGING_QUOTA_BYTES = 4 * 1024 * 1024 * 1024;
const STAGING_QUOTA_FILES = 128;
const STAGING_QUOTA_WAITERS = 16;

export const LARGE_TRANSFER_STAGING_LIMITS = {
    bytes: STAGING_QUOTA_BYTES,
    files: STAGING_QUOTA_FILES,
    checkpointBytes: STAGING_CHECKPOINT_MAX_BYTES,
} as const;

export class LargeTransferStagingQuotaError extends Error {
    readonly code = "LARGE_TRANSFER_STAGING_QUOTA";
    constructor(message: string) {
        super(message);
        this.name = "LargeTransferStagingQuotaError";
    }
}

export async function allSettledBounded<T, R>(
    items: readonly T[],
    concurrency: number,
    mapper: (item: T, index: number) => Promise<R>,
): Promise<Array<PromiseSettledResult<R>>> {
    if (!Number.isInteger(concurrency) || concurrency <= 0) {
        throw new RangeError("bounded apply concurrency must be positive");
    }
    const results = new Array<PromiseSettledResult<R>>(items.length);
    let next = 0;
    const worker = async () => {
        for (;;) {
            const index = next++;
            if (index >= items.length) return;
            try {
                results[index] = { status: "fulfilled", value: await mapper(items[index], index) };
            } catch (reason) {
                results[index] = { status: "rejected", reason };
            }
        }
    };
    await Promise.all(
        Array.from({ length: Math.min(concurrency, items.length) }, () => worker()),
    );
    return results;
}

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
    /** Remote upserts intentionally omitted by local policy (currently ignore
     *  rules). They are not local deletions and therefore also prevent base
     *  adoption/cursor completion until a preservation model exists. */
    remoteOmissionCount: number;
    /** Number of files this pull actually fetched from the server. Zero
     *  means every applied delta verified against local disk — the content is
     *  provably identical to the server, so a tree-hash mismatch is metadata
     *  (mtime) only, not a real divergence, and is safe to adopt rather than
     *  pause. */
    downloaded: number;
    /** A completed paged-diff checkpoint can survive a renderer restart while
     *  the volatile/cached tree still represents an earlier page. No deltas
     *  remain to replay into that tree, so the engine must rebuild it from the
     *  recovered sync-base before evaluating parity or adopting the root. */
    requiresTreeRebuild?: boolean;
}

/** Pull already changed disk and/or sync-base, but its matching in-memory
 * tree transaction failed. The caller must preserve the previous merge base
 * and reconstruct the volatile tree before doing any more ordinary work. */
export class PullTreeRebaseError extends Error {
    readonly code = "PULL_TREE_REBASE_FAILED";

    constructor(readonly rebaseCause: unknown) {
        super("pull tree rebase failed; volatile tree repair required");
        this.name = "PullTreeRebaseError";
    }
}

/** Set-like live guard. A real Set is accepted, while the sync engine can
 *  provide a dynamic view that also sees editor events arriving mid-pull. */
export interface PullPathGuard {
    has(path: string): boolean;
}

interface PullLargeTransferScope {
    /** Vault and remote diff generation which selected this file. */
    vaultId: string;
    rootScope: string;
}

/** Engine-owned asynchronous mutation boundary. Direct structural callers
 * may omit it and retain the synchronous V1 compatibility path. Production
 * supplies a resident ledger, exact wrapper/scope checks, cancellable host
 * turns for new work, and uncancellable turns for mandatory retirement. */
export interface PullTreeMutationContext {
    readonly signal?: AbortSignal;
    readonly residentAdmission: RootTreeResidentAdmission;
    cooperate(): Promise<void>;
    cooperateRetirement(): Promise<void>;
    assertCurrent(): void;
}

function assertPullTreeMutationCurrent(mutation?: PullTreeMutationContext): void {
    if (!mutation) return;
    throwIfWorkAborted(mutation.signal);
    mutation.assertCurrent();
    // A scope callback can synchronously stop its own operation.
    throwIfWorkAborted(mutation.signal);
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
    /** Slice 2 ignore predicate. Ignored UPSERTS are omitted locally and keep
     *  the previous verified merge base; they are never misrepresented as
     *  deletions by adopting a root the local tree cannot reproduce. Ignored
     *  DELETES untrack the path (sync-base + tree) WITHOUT deleting disk. */
    isIgnored?: (path: string) => boolean,
    perf?: PerfOperation,
    beforeHeavyBatch?: () => Promise<void>,
    treeMutation?: PullTreeMutationContext,
): Promise<PullResult> {
    if (treeMutation) {
        if (!tree) throw new TypeError("pull tree mutation context requires a tree owner");
        // Direct engine calls and future callers share the same boundary:
        // retained native owners are retired before even a capability probe.
        // Cleanup keeps its originally captured uncancellable cooperation;
        // only new work observes the current operation signal/scope.
        await drainTreeCandidateMutationRetirement(tree);
        await drainTreeReachabilityRetirement(tree);
        assertPullTreeMutationCurrent(treeMutation);
    }
    // New servers stream a fixed snapshot through bounded binary pages. Keep
    // the legacy JSON path below intact for compatibility with 1.10.x and for
    // narrowly-scoped tests/mocks that intentionally expose only getDiff().
    const paged = await pullPagedIfSupported(
        api,
        io,
        syncBase,
        vaultId,
        localRootHash,
        wasm,
        tree,
        onProgress,
        onWritesKnown,
        skipPaths,
        isIgnored,
        perf,
        beforeHeavyBatch,
        treeMutation,
    );
    assertPullTreeMutationCurrent(treeMutation);
    if (paged !== undefined) return paged;

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
            const deltasHadMtime = await rebaseTree(tree, syncBase, [], treeMutation);
            assertPullTreeMutationCurrent(treeMutation);
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
                remoteOmissionCount: 0,
                downloaded: 0,
            };
        }
        // The diff already proved that this is not a genuinely empty remote.
        // Publish that fact before any file work so a renderer kill cannot
        // turn an incomplete bootstrap into an empty-parent first push.
        if (syncBase.setVerifiedBaseRequired(true)) await syncBase.checkpoint();
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
                beforeHeavyBatch,
                { vaultId, rootScope: ZERO_ROOT },
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
        const deltasHadMtime = await rebaseTree(tree, syncBase, appliedDeltas, treeMutation);
        assertPullTreeMutationCurrent(treeMutation);
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
            remoteOmissionCount: ignoredUpserts,
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
            remoteOmissionCount: 0,
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
            beforeHeavyBatch,
            { vaultId, rootScope: localRootHash.toLowerCase() },
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
    const deltasHadMtime = await rebaseTree(tree, syncBase, appliedDeltas, treeMutation);
    assertPullTreeMutationCurrent(treeMutation);
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
        remoteOmissionCount: ignoredUpserts,
        downloaded,
    };
}

interface PagedDiffApi {
    supportsPagedDiff(perf?: PerfOperation): Promise<boolean>;
    getDiffPage(
        vaultId: string,
        fromRootHash: string,
        toRootHash: string | null,
        cursor: Uint8Array | null,
        perf?: PerfOperation,
    ): Promise<DiffPage | null | undefined>;
    getRootAt(vaultId: string, rootHash: string, perf?: PerfOperation): Promise<Uint8Array | null>;
}

/**
 * Apply a snapshot one bounded page at a time. Entry mutations and the next
 * cursor enter the same sync-base WAL append, in that order. A torn append can
 * therefore lose the cursor (causing a safe idempotent replay), but can never
 * preserve a cursor that skips uncommitted local state.
 */
async function pullPagedIfSupported(
    api: ObsetyncApi,
    io: PlatformIO,
    syncBase: ObsetyncSyncBase,
    vaultId: string,
    localRootHash: string | null,
    wasm: WasmModule | null,
    tree: WasmTree | null,
    onProgress?: (msg: string) => void,
    onWritesKnown?: (writes: PullWriteExpectation[]) => void,
    skipPaths?: PullPathGuard,
    isIgnored?: (path: string) => boolean,
    perf?: PerfOperation,
    beforeHeavyBatch?: () => Promise<void>,
    treeMutation?: PullTreeMutationContext,
): Promise<PullResult | undefined> {
    const candidate = api as unknown as Partial<PagedDiffApi>;
    if (
        typeof candidate.supportsPagedDiff !== "function" ||
        typeof candidate.getDiffPage !== "function" ||
        typeof candidate.getRootAt !== "function"
    ) {
        return undefined;
    }

    const endCapabilityCheck = perf?.phase("check");
    let supported: boolean;
    try {
        supported = await candidate.supportsPagedDiff.call(api, perf);
    } finally {
        endCapabilityCheck?.();
    }
    if (!supported) {
        if (syncBase.clearDiffPageCheckpoint()) await syncBase.checkpoint();
        return undefined;
    }

    const fromRoot = (localRootHash ?? ZERO_ROOT).toLowerCase();
    let checkpoint = syncBase.diffPageCheckpoint;
    if (checkpoint && (checkpoint.vaultId !== vaultId || checkpoint.fromRoot !== fromRoot)) {
        syncBase.clearDiffPageCheckpoint();
        await syncBase.checkpoint();
        checkpoint = null;
    }
    if (checkpoint) perf?.increment({ resumedPages: 1 });

    let toRoot: string | null = checkpoint?.toRoot ?? null;
    let cursor = checkpoint ? diffCursorFromHex(checkpoint.nextCursorHex) : null;
    let complete = checkpoint?.complete ?? false;
    let recordsSeen = checkpoint?.recordsSeen ?? 0;
    let filesApplied = checkpoint?.filesApplied ?? 0;
    let bytesTotal = checkpoint?.bytesTotal ?? 0;
    let downloaded = checkpoint?.downloaded ?? 0;
    let bytesDownloaded = checkpoint?.bytesDownloaded ?? 0;
    let deltasHadMtime = checkpoint?.deltasHadMtime ?? true;
    let deferredCount = 0;
    let localDeferredCount = 0;
    let remoteOmissionCount = 0;
    let durableCursorAllowed = true;
    let processedPageThisRun = false;
    // Any checkpoint recovered at entry may cover pages which this volatile
    // tree never observed in this renderer. Even when later pages are processed
    // now, only a base-derived replacement proves the full resumed prefix.
    let requiresTreeRebuild = checkpoint !== null;

    if (localRootHash === null) {
        onProgress?.("first sync: downloading paged snapshot...");
    }

    while (!complete) {
        await beforeHeavyBatch?.();
        const endCheck = perf?.phase("check");
        let page: DiffPage | null | undefined;
        try {
            page = await candidate.getDiffPage.call(api, vaultId, fromRoot, toRoot, cursor, perf);
        } finally {
            endCheck?.();
        }
        // A capability-aware server returns null only when the vault has no
        // current root. Let the legacy empty-vault path preserve its existing
        // return semantics; undefined also covers a rolled-back server.
        if (page === null || page === undefined) {
            if (checkpoint) {
                syncBase.clearDiffPageCheckpoint();
                await syncBase.checkpoint();
            }
            return undefined;
        }
        processedPageThisRun = true;
        if (toRoot === null) toRoot = page.toRoot;
        if (page.toRoot !== toRoot) throw new Error("paged diff target changed mid-snapshot");
        if (localRootHash === null && syncBase.setVerifiedBaseRequired(true)) {
            // Persist the remote-state fence before applying the first page.
            // Its eventual verified adoption clears the fence atomically with
            // treeBaseRoot; interruption leaves publication fail-closed.
            await syncBase.checkpoint();
        }

        const pageBytesTotal = page.deltas.reduce(
            (sum, delta) => sum + (delta.action === "deleted" ? 0 : delta.size ?? 0),
            0,
        );
        recordsSeen += page.deltas.length;
        bytesTotal += pageBytesTotal;
        perf?.setWorkload({ filesTotal: recordsSeen, bytesTotal });
        if (localRootHash !== null && page.deltas.length > 0) {
            onProgress?.(`${recordsSeen} changes to apply (paged)`);
        }

        const { kept, ignoredDeletes, ignoredUpserts } = splitIgnored(page.deltas, isIgnored);
        remoteOmissionCount += ignoredUpserts;
        for (const delta of ignoredDeletes) syncBase.removeEntry(delta.path);
        if (ignoredUpserts > 0 || ignoredDeletes.length > 0) {
            console.log(
                `[obsetync] paged pull: skipped ${ignoredUpserts} ignored upsert(s), ` +
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
                beforeHeavyBatch,
                { vaultId, rootScope: toRoot!.toLowerCase() },
            );
        } finally {
            endApply?.();
        }
        const pageDeferred = applyResult.deferred.length;
        deferredCount += pageDeferred;
        localDeferredCount += applyResult.localDeferredCount;
        downloaded += applyResult.downloaded;
        bytesDownloaded += applyResult.bytesDownloaded;
        const pageApplied = kept.length - pageDeferred;
        filesApplied += pageApplied;
        perf?.increment({
            filesCompleted: pageApplied,
            bytesTransferred: applyResult.bytesDownloaded,
        });

        const appliedDeltas = excludeDeltas(kept, applyResult.deferred).concat(ignoredDeletes);
        const endTree = perf?.phase("tree_update");
        try {
            deltasHadMtime = await rebaseTree(tree, syncBase, appliedDeltas, treeMutation) && deltasHadMtime;
            assertPullTreeMutationCurrent(treeMutation);
        } finally {
            endTree?.();
        }

        // Once any path is deferred, no later cursor is safe to persist: a
        // crash must replay from before that page. The current run may still
        // apply later pages to maximize useful progress.
        if (pageDeferred > 0 || ignoredUpserts > 0) durableCursorAllowed = false;
        cursor = page.nextCursor;
        complete = cursor === null;
        if (durableCursorAllowed) {
            const next: DiffPageCheckpoint = {
                version: 1,
                vaultId,
                fromRoot,
                toRoot,
                nextCursorHex: diffCursorToHex(cursor),
                complete,
                recordsSeen,
                filesApplied,
                bytesTotal,
                downloaded,
                bytesDownloaded,
                deltasHadMtime,
            };
            syncBase.setDiffPageCheckpoint(next);
        }
        // This append follows all page entry mutations. It is the durable
        // page boundary used by mobile renderer-kill recovery.
        const endCheckpoint = perf?.phase("checkpoint");
        try {
            await syncBase.checkpoint();
        } finally {
            endCheckpoint?.();
        }
    }

    if (!toRoot) throw new Error("completed paged diff has no target root");
    if (!processedPageThisRun) {
        // Restart after the final cursor append: no delta remains with which
        // to advance a non-empty stale cached tree. Signal the engine's
        // admitted full replacement from WAL-recovered sync-base instead of
        // treating rebaseTree(..., []) as a rebuild (it is intentionally a
        // no-op for a populated tree).
        requiresTreeRebuild = true;
    }

    const rootBytes = await candidate.getRootAt.call(api, vaultId, toRoot, perf);
    if (!rootBytes) throw new Error("paged diff snapshot root expired before completion");
    if (wasm) {
        const actualRoot = wasm.wasm_root_hash_from_bytes(rootBytes);
        if (actualRoot !== toRoot) {
            throw new Error("paged diff snapshot root bytes failed hash verification");
        }
    }

    syncBase.setLastSyncTimestamp(Date.now());
    perf?.setWorkload({
        filesTotal: recordsSeen,
        bytesTotal,
        filesNeeded: downloaded,
        bytesNeeded: bytesDownloaded,
    });
    if (deferredCount > 0 || remoteOmissionCount > 0) {
        // Do not strand a completed cursor past files that were never applied
        // or were intentionally omitted from the local authority scope.
        // Already-written paths remain useful cache hits on the safe replay.
        syncBase.clearDiffPageCheckpoint();
        await syncBase.save();
    } else {
        // Keep the completed marker until adoptPullResult durably advances
        // treeBaseRoot and clears it in the same snapshot.
        await syncBase.checkpoint();
    }
    onProgress?.(
        localRootHash === null
            ? `first sync: applied ${filesApplied} files`
            : `${filesApplied} files applied from paged snapshot`,
    );

    return {
        newRootHash: toRoot,
        newRootBytes: rootBytes,
        applied: filesApplied,
        treeParity: requiresTreeRebuild ? null : parity(tree, toRoot),
        deltasHadMtime,
        deferredCount,
        localDeferredCount,
        remoteOmissionCount,
        downloaded,
        requiresTreeRebuild,
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
async function rebaseTree(
    tree: WasmTree | null,
    syncBase: ObsetyncSyncBase,
    deltas: FileDelta[],
    mutation?: PullTreeMutationContext,
): Promise<boolean> {
    let allHadMtime = true;
    for (const d of deltas) {
        if (d.action !== "deleted" && d.mtime_ms === undefined) allHadMtime = false;
    }
    if (!tree) return allHadMtime;

    try {
        if (!tree.root_hash_hex()) {
            if (mutation) {
                throw new Error("admitted pull rebase requires a populated committed tree");
            }
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

        if (mutation) {
            await rebaseTreeWithAdmittedMutation(tree, deletePaths, upserts, mutation);
        } else {
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
                // Direct structural/V1 callers have no host turn between
                // candidate ownership and this synchronous compatibility
                // abort, so no newer candidate can have replaced it.
                if (tree.has_candidate()) {
                    try {
                        tree.abort_candidate();
                    } catch (abortError) {
                        console.error("[obsetync] failed to abort pull tree candidate:", abortError);
                    }
                }
                throw error;
            }
        }
    } catch (e) {
        // Disk/sync-base may already contain the applied remote state. Parity
        // is not a safe substitute for this error: metadata-only fallback can
        // legitimately adopt a mismatching root. Force the engine through its
        // admitted base-derived repair before any root/base advancement.
        console.error("[obsetync] tree rebase after pull failed:", e);
        throw new PullTreeRebaseError(e);
    }
    return allHadMtime;
}

async function rebaseTreeWithAdmittedMutation(
    tree: WasmTree,
    deletePaths: string[],
    upserts: Array<{ path: string; hash: string; mtime_ms: number; size: number }>,
    mutation: PullTreeMutationContext,
): Promise<void> {
    const revisionReader = tree.candidate_revision;
    if (typeof revisionReader !== "function") {
        throw new TypeError("admitted pull rebase requires an exact candidate revision witness");
    }
    const readRevision = (): number => {
        const revision = revisionReader.call(tree);
        if (!Number.isSafeInteger(revision) || revision < 0) {
            throw new TypeError("pull candidate revision witness is invalid");
        }
        return revision;
    };
    let candidateOpen = false;
    let candidateOwnedRevision: number | undefined;
    const recordOwnedCandidateRevision = (): void => {
        const revision = readRevision();
        candidateOwnedRevision = revision;
        try {
            const committed = tree.committed_revision?.();
            if (!Number.isSafeInteger(committed) || committed! < 0 ||
                !mutation.residentAdmission.advanceV2CandidateRevision(tree, committed!, revision)) {
                mutation.residentAdmission.invalidateV2Graph(tree);
            }
        } catch {
            try { mutation.residentAdmission.invalidateV2Graph(tree); } catch { /* optional provenance */ }
        }
    };
    const assertCandidateCurrent = (): void => {
        mutation.assertCurrent();
        if (!candidateOpen || tree.has_candidate() !== true ||
            candidateOwnedRevision === undefined || readRevision() !== candidateOwnedRevision) {
            throw new Error("pull candidate ownership changed during rebase");
        }
    };
    const assertSettlementOwner = (): void => {
        if (!candidateOpen || tree.has_candidate() !== true ||
            candidateOwnedRevision === undefined || readRevision() !== candidateOwnedRevision) {
            throw new Error("pull candidate ownership changed before settlement");
        }
    };
    const abortCandidate = () => settleTreeCandidateOutputWithAdmission(
        tree,
        "abort",
        mutation.residentAdmission,
        assertSettlementOwner,
    );
    try {
        const candidateOpenMemoryAvailable =
            typeof tree.candidate_open_memory_plan_v1_job === "function" &&
            typeof tree.resume_candidate_open_memory_v1_job === "function";
        await beginTreeCandidate(tree, {
            signal: mutation.signal,
            cooperate: mutation.cooperate,
            cooperateRetirement: mutation.cooperateRetirement,
            assertCurrent: mutation.assertCurrent,
            onOpenMemoryPlan: candidateOpenMemoryAvailable
                ? plan => admitCandidateOpenRoot(mutation.residentAdmission, tree, plan)
                : undefined,
            abortCandidateOpened: candidateOpenMemoryAvailable
                ? expectedRevision => settleTreeCandidateOutputWithAdmission(
                    tree,
                    "abort",
                    mutation.residentAdmission,
                    () => {
                        if (tree.has_candidate() !== true || readRevision() !== expectedRevision) {
                            throw new Error("pull candidate ownership changed before begin cleanup");
                        }
                    },
                )
                : undefined,
            onCandidateOpened: () => {
                // The callback is synchronous with native publication. Record
                // the exact visible revision before any later host turn.
                recordOwnedCandidateRevision();
                candidateOpen = true;
            },
        });
        if (deletePaths.length > 0) {
            const payload = JSON.stringify(deletePaths);
            await applyTreeCandidateMutation(tree, "delete", payload, {
                signal: mutation.signal,
                cooperate: mutation.cooperate,
                cooperateRetirement: mutation.cooperateRetirement,
                assertCurrent: assertCandidateCurrent,
                legacy: () => tree.candidate_delete_batch(payload),
                onCandidateMutated: recordOwnedCandidateRevision,
                onOutputMemoryPlan: plan =>
                    admitCandidateMutationOutput(mutation.residentAdmission, tree, plan),
            });
        }
        if (upserts.length > 0) {
            const payload = JSON.stringify(upserts);
            await applyTreeCandidateMutation(tree, "update", payload, {
                signal: mutation.signal,
                cooperate: mutation.cooperate,
                cooperateRetirement: mutation.cooperateRetirement,
                assertCurrent: assertCandidateCurrent,
                legacy: () => tree.candidate_update_batch(payload),
                onCandidateMutated: recordOwnedCandidateRevision,
                onOutputMemoryPlan: plan =>
                    admitCandidateMutationOutput(mutation.residentAdmission, tree, plan),
            });
        }
        throwIfWorkAborted(mutation.signal);
        assertCandidateCurrent();
        // `assertCurrent` and the native ownership getters are synchronous
        // user/host boundaries. They may stop the engine while validating
        // the final witness, so sample cancellation again with no await left
        // before the atomic commit.
        throwIfWorkAborted(mutation.signal);
        // No await separates the final ownership witness from atomic native
        // commit. A live editor change affects the queued local overlay, not
        // this remote sync-base candidate, so disk is deliberately not read.
        settleTreeCandidateOutputWithAdmission(tree, "commit", mutation.residentAdmission, () => {
            // Final observable boundary before the pinned atomic native call.
            // Unlike cleanup abort, a pull commit remains cancellable here.
            throwIfWorkAborted(mutation.signal);
            assertCandidateCurrent();
            throwIfWorkAborted(mutation.signal);
        });
        candidateOpen = false;
    } catch (error) {
        if (candidateOpen && candidateOwnedRevision !== undefined) {
            try {
                const aborted = hasPendingTreeCandidateMutationRetirement(tree)
                    ? await abortTreeCandidateAfterMutationRetirement(tree, candidateOwnedRevision, abortCandidate)
                    : await abortTreeCandidateAfterReachabilityRetirement(tree, candidateOwnedRevision, abortCandidate);
                if (!aborted) {
                    console.warn("[obsetync] skipped abort of a newer pull candidate tree");
                }
            } catch (abortError) {
                console.error("[obsetync] failed to retire/abort pull tree candidate:", abortError);
                if ((typeof error === "object" && error !== null) || typeof error === "function") {
                    try {
                        Object.defineProperty(error, "treeCandidateCleanupErrors", {
                            configurable: true,
                            value: [abortError],
                        });
                    } catch { /* Preserve a frozen/foreign primary error. */ }
                }
            }
        }
        throw error;
    }
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
    beforeHeavyBatch?: () => Promise<void>,
    largeTransferScope?: PullLargeTransferScope,
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
                beforeHeavyBatch,
                largeTransferScope,
            ))
        );
        results.forEach((r, j) => {
            if (r.status === "rejected") failed.push(batch[j]);
            else if (!r.value) deferLocal(batch[j]);
        });
        await checkpointAndReport(batch.length);
    };

    const applySmallBatch = async (batch: FileDelta[]): Promise<void> => {
        const activeTuning = getHashTuning();
        const preparations = await allSettledBounded(
            batch,
            activeTuning.readConcurrency,
            (delta) => prepareContentDelta(
                io,
                syncBase,
                wasm,
                delta,
                stats,
                () => shouldSkip(delta),
                perf,
            ),
        );
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

        let downloaded: PullObjects | undefined;
        try {
            if (pending.length > 0) {
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
                const applied = await allSettledBounded(
                    pending,
                    activeTuning.applyConcurrency,
                    ({ delta, preparation }) => {
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
                            downloaded!.objects.get(canonicalHash),
                            countTransferredBytes,
                            beforeHeavyBatch,
                            downloaded,
                            largeTransferScope,
                        );
                    },
                );
                applied.forEach((result, index) => {
                    if (result.status === "rejected") failed.push(pending[index].delta);
                    else if (!result.value) deferLocal(pending[index].delta);
                });
            }
            await checkpointAndReport(batch.length);
        } finally {
            // allSettledBounded drains every native sibling before this owner
            // is closed, including when one apply fails or a local edit wins.
            downloaded?.release();
        }
    };

    // Small files retain bounded parallelism. Large files run strictly one at
    // a time: six simultaneous encrypted/chunked transfers were enough to
    // exceed the practical Jetsam limit on older 2 GB iPads.
    const smallDownloads = toDownload.filter((delta) => (delta.size ?? 0) < CHUNK_THRESHOLD);
    const largeDownloads = toDownload.filter((delta) => (delta.size ?? 0) >= CHUNK_THRESHOLD);
    const desktop = smallDownloads.length > 0 &&
        io.getAbsolutePath(smallDownloads[0].path) !== null;
    const downloadTuning = getHashTuning();
    const smallBatches = planByteBoundedBatches(
        smallDownloads,
        (delta) => delta.size ?? 0,
        {
            maxFiles: Math.min(BULK_DOWNLOAD_FILES, downloadTuning.maxBatchFiles),
            maxBytes: Math.min(
                desktop ? DESKTOP_BULK_DOWNLOAD_BYTES : MOBILE_BULK_DOWNLOAD_BYTES,
                downloadTuning.maxBatchBytes,
            ),
            maxSingleBytes: CHUNK_THRESHOLD - 1,
        },
    );
    for (const batch of smallBatches) {
        await beforeHeavyBatch?.();
        await applySmallBatch(batch);
    }
    for (const delta of largeDownloads) {
        await beforeHeavyBatch?.();
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
                await beforeHeavyBatch?.();
                const applied = await applyContentDelta(
                    api,
                    io,
                    syncBase,
                    wasm,
                    delta,
                    stats,
                    () => shouldSkip(delta),
                    perf,
                    beforeHeavyBatch,
                    largeTransferScope,
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
    targetState: LargeTransferTargetState;
}

export interface LargeTransferTargetState {
    present: boolean;
    size: number;
    mtime: number;
}

function targetState(stat: Awaited<ReturnType<PlatformIO["stat"]>>): LargeTransferTargetState {
    return stat
        ? { present: true, size: stat.size, mtime: stat.mtime }
        : { present: false, size: 0, mtime: 0 };
}

function largeTransferFileGeneration(wasm: WasmModule, delta: FileDelta): string {
    return wasm.wasm_hash(new TextEncoder().encode(JSON.stringify({
        action: delta.action,
        hash: delta.hash?.toLowerCase() ?? null,
        size: delta.size ?? 0,
        mtime: delta.mtime_ms ?? null,
    }))).toLowerCase();
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
    return { kind: "download", size, preserveExisting, targetState: targetState(stat) };
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
    beforeHeavyBatch?: () => Promise<void>,
    prefetchedObjects?: PullObjects,
    largeTransferScope?: PullLargeTransferScope,
): Promise<boolean> {
    if (!delta.hash) return true;
    const { size, preserveExisting } = preparation;

    // --- Tier 3: actual download from server -----------------------------
    if (shouldDefer?.()) return false;
    if (size < CHUNK_THRESHOLD && !prefetchedObjects) {
        // Preparation/local hashing has already settled before admission. Do
        // not start a second global reservation beneath an owned download.
        const endDownload = perf?.phase("download");
        let owned: PullObjects;
        try { owned = await getSmallContentBatch(api, [delta.hash], perf); }
        finally { endDownload?.(); }
        try {
            const applied = await finishContentDownload(
                api, io, syncBase, wasm, delta, stats, preparation, shouldDefer,
                perf, owned.objects.get(delta.hash.toLowerCase()), countTransferredBytes,
                beforeHeavyBatch, owned, largeTransferScope,
            );
            if (applied) {
                const endCheckpoint = perf?.phase("checkpoint");
                try { await syncBase.checkpoint(); }
                finally { endCheckpoint?.(); }
            }
            return applied;
        } finally { owned.release(); }
    }
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
                beforeHeavyBatch,
                largeTransferScope && {
                    ...largeTransferScope,
                    fileGeneration: largeTransferFileGeneration(wasm!, delta),
                    targetState: preparation.targetState,
                },
            );
        } catch (error) {
            if (error instanceof LocalEditDuringPull) return false;
            throw error;
        }
    } else {
        const data = prefetchedSmallData;
        if (!data) throw new Error("small-file content missing from download batch");
        const applied = await runPullWork(prefetchedObjects?.memory, 3 * data.byteLength + 64 * 1024, async () => {
            const endHash = perf?.phase("hash");
            let actualHash: string | null = null;
            try {
                actualHash = wasm ? wasm.wasm_hash(data).toLowerCase() : null;
            } finally {
                endHash?.();
            }
            if (!actualHash || actualHash !== delta.hash!.toLowerCase()) {
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
            return true;
        });
        if (!applied) return false;
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
    beforeHeavyBatch?: () => Promise<void>,
    largeTransferScope?: PullLargeTransferScope,
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
        undefined,
        true,
        beforeHeavyBatch,
        undefined,
        largeTransferScope,
    );
}

async function getSmallContentBatch(
    api: ObsetyncApi,
    hashes: readonly string[],
    perf?: PerfOperation,
): Promise<PullObjects> {
    return getPullObjects(api, BulkObjectKind.Content, hashes, (hash) => api.getContent(hash, perf), perf);
}

interface PullObjects {
    objects: Map<string, Uint8Array>;
    /** Missing only on explicit old embedding/pure mock compatibility paths. */
    memory?: TransientWorkScope;
    release(): void;
}

async function runPullWork<T>(memory: TransientWorkScope | undefined, bytes: number,
    work: () => T | Promise<T>): Promise<T> {
    return memory ? memory.run(bytes, work) : work();
}

async function getPullObjects(
    api: ObsetyncApi,
    kind: BulkObjectKind,
    hashes: readonly string[],
    legacySingle: (hash: string) => Promise<Uint8Array>,
    perf?: PerfOperation,
): Promise<PullObjects> {
    if (typeof api.getObjectsOwned === "function") return api.getObjectsOwned(kind, hashes, perf);
    // Production always takes the owned API above. Old embeddings and pure
    // fixtures have no reservation: do not invent one after their allocation.
    if (typeof api.getObjects === "function") {
        const objects = await api.getObjects(kind, hashes, perf);
        return { objects, release() { objects.clear(); } };
    }
    const objects = new Map<string, Uint8Array>();
    // Compatibility still drains every native read on failure.
    const results = await allSettledBounded([...new Set(hashes.map((hash) => hash.toLowerCase()))],
        getHashTuning().networkConcurrency, async (hash) => { objects.set(hash, await legacySingle(hash)); });
    const rejected = results.find((result) => result.status === "rejected");
    if (rejected?.status === "rejected") throw rejected.reason;
    return { objects, release() { objects.clear(); } };
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

export interface LargeTransferResumeScope extends PullLargeTransferScope {
    /** Remote per-file generation derived from action/hash/size/mtime. */
    fileGeneration: string;
    /** Local target generation captured before network work began. */
    targetState: LargeTransferTargetState;
}

interface LargeTransferCheckpoint extends LargeTransferResumeScope {
    version: 2;
    targetPath: string;
    fileHash: string;
    manifestGeneration: string;
    totalSize: number;
    nextChunk: number;
    bytesWritten: number;
    /** Written only after every chunk was verified and, for a fresh transfer,
     * the independently assembled whole-file hash also matched. */
    completedHash?: string;
}

interface StagingReservation {
    io: PlatformIO;
    targetPath: string;
    stagingPath: string;
    checkpointPath: string;
    plannedBytes: number;
}

interface StagingLease {
    release(): void;
}

const activeStagingReservations = new Set<StagingReservation>();
let stagingQuotaTail = Promise.resolve();
let stagingQuotaWaiters = 0;

async function withStagingQuotaLock<T>(work: () => Promise<T>): Promise<T> {
    if (stagingQuotaWaiters >= STAGING_QUOTA_WAITERS) {
        throw new LargeTransferStagingQuotaError("staging quota admission queue is full");
    }
    stagingQuotaWaiters++;
    const previous = stagingQuotaTail;
    let releaseTurn!: () => void;
    stagingQuotaTail = new Promise<void>((resolve) => { releaseTurn = resolve; });
    await previous;
    try {
        return await work();
    } finally {
        stagingQuotaWaiters--;
        releaseTurn();
    }
}

/** Reject manifests that could overlap, leave holes, overrun allocations, or
 *  point at a different content address. Returned values are safe JS ints. */
export function validateManifest(
    value: unknown,
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

/** Bind restart state to the exact validated chunk layout without building a
 * second manifest-sized JSON/string buffer. */
async function largeManifestGeneration(
    wasm: WasmModule,
    manifest: FileManifest,
    perf?: PerfOperation,
    shouldAbort?: () => boolean,
): Promise<string> {
    const encoder = new TextEncoder();
    const hasher = new wasm.Hasher();
    const endHash = perf?.phase("hash");
    try {
        hasher.update(encoder.encode(`large-manifest-v1\n${manifest.file_hash.toLowerCase()}\n${manifest.total_size}\n`));
        for (let index = 0; index < manifest.chunks.length; index++) {
            if (shouldAbort?.()) throw new LocalEditDuringPull();
            const chunk = manifest.chunks[index];
            hasher.update(encoder.encode(`${chunk.hash.toLowerCase()}\n${chunk.offset}\n${chunk.size}\n`));
            if ((index + 1) % 256 === 0) await yieldWork({ perf });
        }
        const generation = hasher.finalize().toLowerCase();
        if (!/^[0-9a-f]{64}$/.test(generation)) {
            throw new Error("invalid large-file manifest generation");
        }
        return generation;
    } finally {
        try { hasher.free(); }
        finally { endHash?.(); }
    }
}

async function getManifestForPull(
    api: ObsetyncApi,
    hash: string,
    expectedSize: number,
    perf?: PerfOperation,
): Promise<FileManifest> {
    if (typeof api.getObjectsOwned !== "function" && typeof api.getObjects !== "function") {
        return validateManifest(await api.getManifest(hash, perf), hash, expectedSize);
    }
    const owned = await getPullObjects(api, BulkObjectKind.Manifest, [hash],
        async () => { throw new Error("manifest bulk API unavailable"); }, perf);
    try {
        const bytes = owned.objects.get(hash.toLowerCase());
        if (!bytes) throw new Error("manifest missing from download batch");
        return await runPullWork(owned.memory, 4 * bytes.byteLength + 64 * 1024, () => {
            let manifest: unknown;
            try { manifest = JSON.parse(new TextDecoder().decode(bytes)); }
            catch { throw new Error("manifest is not valid JSON"); }
            // Covers retained bytes and decode work through validation. The
            // returned metadata graph is not claimed as byte-buffer accounting.
            return validateManifest(manifest, hash, expectedSize);
        });
    } finally { owned.release(); }
}

async function getContentChunkBatch(
    api: ObsetyncApi,
    hashes: readonly string[],
    perf?: PerfOperation,
): Promise<PullObjects> {
    return getPullObjects(api, BulkObjectKind.ContentChunk, hashes,
        (hash) => api.getContentChunk(hash, perf), perf);
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
    beforeHeavyBatch?: () => Promise<void>,
    resumeScope?: LargeTransferResumeScope,
): Promise<void> {
    if (shouldAbort?.()) throw new LocalEditDuringPull();
    if (!wasm) throw new Error("WASM hash verifier unavailable for large file");
    const currentTargetState = targetState(await io.stat(path));
    const effectiveScope: LargeTransferResumeScope = resumeScope
        ? { ...resumeScope, targetState: { ...resumeScope.targetState } }
        : { vaultId: "legacy", rootScope: hash.toLowerCase(), fileGeneration: hash.toLowerCase(),
            targetState: currentTargetState };
    if (!sameTargetState(currentTargetState, effectiveScope.targetState)) {
        throw new LocalEditDuringPull();
    }
    validateLargeTransferScope(effectiveScope);
    const endManifestDownload = perf?.phase("download");
    let rawManifest: FileManifest;
    try {
        rawManifest = await getManifestForPull(api, hash, expectedSize, perf);
    } finally {
        endManifestDownload?.();
    }
    const manifest = validateManifest(rawManifest, hash, expectedSize);
    const manifestGeneration = await largeManifestGeneration(wasm, manifest, perf, shouldAbort);
    const endPathHash = perf?.phase("hash");
    let pathHash: string;
    try {
        pathHash = wasm.wasm_hash(new TextEncoder().encode(path)).slice(0, 16).toLowerCase();
        if (!/^[0-9a-f]{16}$/.test(pathHash)) {
            throw new Error("invalid large-transfer path generation");
        }
    } finally {
        endPathHash?.();
    }
    const transferKey = `${hash.toLowerCase()}-${pathHash}`;
    const stagingPath = `${TRANSFER_DIR}/${transferKey}.part`;
    const checkpointPath = `${TRANSFER_DIR}/${transferKey}.checkpoint.json`;

    let checkpoint = await readLargeCheckpoint(io, checkpointPath);
    const checkpointStat = await io.stat(checkpointPath);
    const expectedBytes = (nextChunk: number): number =>
        nextChunk === manifest.chunks.length
            ? manifest.total_size
            : manifest.chunks[nextChunk]?.offset ?? -1;
    const stagingStat = await io.stat(stagingPath);
    const resumable =
        checkpoint?.version === 2 &&
        checkpoint.targetPath === path &&
        checkpoint.fileHash === hash.toLowerCase() &&
        checkpoint.manifestGeneration === manifestGeneration &&
        checkpoint.totalSize === manifest.total_size &&
        checkpoint.vaultId === effectiveScope.vaultId &&
        checkpoint.rootScope === effectiveScope.rootScope &&
        checkpoint.fileGeneration === effectiveScope.fileGeneration &&
        sameTargetState(checkpoint.targetState, effectiveScope.targetState) &&
        Number.isInteger(checkpoint.nextChunk) &&
        checkpoint.nextChunk >= 0 &&
        checkpoint.nextChunk <= manifest.chunks.length &&
        checkpoint.bytesWritten === expectedBytes(checkpoint.nextChunk) &&
        stagingStat?.size === checkpoint.bytesWritten &&
        (checkpoint.nextChunk < manifest.chunks.length
            ? checkpoint.completedHash === undefined
            : checkpoint.completedHash === hash.toLowerCase());
    if (!resumable && (stagingStat !== null || checkpointStat !== null)) {
        // A killed append may leave a valid checkpoint with an uncheckpointed
        // part tail (or no part after external cleanup). It is safe to restart
        // that exact scoped temp pair, but an orphan part or malformed/wrong-
        // target checkpoint remains opaque and is never overwritten.
        if (!checkpointStat || !checkpoint ||
            !validStoredLargeCheckpoint(checkpoint, transferKey, checkpoint.bytesWritten) ||
            checkpoint.targetPath !== path) {
            throw new LargeTransferStagingQuotaError(
                "current staging pair is malformed and requires explicit repair",
            );
        }
    }

    const stagingLease = await reserveLargeTransferStaging(
        io,
        path,
        stagingPath,
        checkpointPath,
        manifest.total_size,
        stagingStat?.size ?? null,
        checkpointStat?.size ?? null,
    );
    try {
        if (!resumable) {
            if (stagingStat || checkpointStat) {
                await deleteValidatedInternalPair(io, stagingPath, checkpointPath);
            }
            await io.writeFile(stagingPath, new Uint8Array());
            checkpoint = {
                version: 2,
                ...effectiveScope,
                targetPath: path,
                fileHash: hash.toLowerCase(),
                manifestGeneration,
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
                maxFiles: bulkEnabled
                    ? Math.min(BULK_DOWNLOAD_FILES, getHashTuning().maxBatchFiles)
                    : 1,
                maxBytes: Math.min(
                    desktop ? DESKTOP_BULK_DOWNLOAD_BYTES : MOBILE_BULK_DOWNLOAD_BYTES,
                    getHashTuning().maxBatchBytes,
                ),
                maxSingleBytes: CHUNK_THRESHOLD - 1,
            },
        );
        for (const batch of chunkBatches) {
            await beforeHeavyBatch?.();
            if (shouldAbort?.()) throw new LocalEditDuringPull();
            const endDownload = perf?.phase("download");
            let downloaded: PullObjects;
            try {
                downloaded = await getContentChunkBatch(
                    api,
                    batch.map(({ chunk }) => chunk.hash),
                    perf,
                );
            } finally {
                endDownload?.();
            }
            try {
                for (const { chunk, index } of batch) {
                    if (shouldAbort?.()) throw new LocalEditDuringPull();
                    const data = downloaded.objects.get(chunk.hash.toLowerCase());
                    if (!data || data.length !== chunk.size) {
                        throw new Error(`large-file chunk ${index} length mismatch`);
                    }
                    const endHash = perf?.phase("hash");
                    let actualHash: string;
                    try {
                        actualHash = await runPullWork(downloaded.memory, 2 * data.byteLength + 64 * 1024,
                            () => wholeHasher ? wholeHasher.update_and_hash(data) : wasm.wasm_hash(data));
                    } finally {
                        endHash?.();
                    }
                    if (actualHash.toLowerCase() !== chunk.hash.toLowerCase()) {
                        throw new Error(`large-file chunk ${index} hash mismatch`);
                    }
                    if (shouldAbort?.()) throw new LocalEditDuringPull();
                    // The staging lease already reserves the manifest's full
                    // final size; validated offsets make every append consume
                    // that fixed reservation rather than growing quota ad hoc.
                    if (io.appendFileOwned) {
                        // Append independently borrows the same work quota; nesting
                        // it inside the hash run could deadlock at full admission.
                        await io.appendFileOwned(stagingPath, data, downloaded.memory);
                    } else {
                        // Explicit old embedding/test compatibility only. Actual
                        // PlatformIO implements memory-aware capability checking.
                        await io.appendFile(stagingPath, data);
                    }
                    checkpoint = {
                        ...checkpoint!,
                        nextChunk: index + 1,
                        bytesWritten: chunk.offset + chunk.size,
                    };
                    const endCheckpoint = perf?.phase("checkpoint");
                    try {
                        await runPullWork(downloaded.memory, 64 * 1024,
                            () => writeLargeCheckpoint(io, checkpointPath, checkpoint!));
                    } finally {
                        endCheckpoint?.();
                    }
                    // Apply/checkpoint records independently even though transport
                    // grouped them; a kill repeats less than one completed pack.
                    await yieldWork({ perf });
                }
            } finally { downloaded.release(); }
        }
        if (wholeHasher) assembledHash = wholeHasher.finalize().toLowerCase();
    } finally {
        wholeHasher?.free();
    }

    if (assembledHash !== null && assembledHash !== hash.toLowerCase()) {
        // No completion marker exists yet, so even failed cleanup cannot make
        // a later restart promote these known-wrong bytes.
        await deleteValidatedInternalPair(io, stagingPath, checkpointPath);
        throw new Error("large-file assembled content hash mismatch");
    }

    checkpoint = { ...checkpoint!, completedHash: hash.toLowerCase() };
    const endCompletionCheckpoint = perf?.phase("checkpoint");
    try {
        await writeLargeCheckpoint(io, checkpointPath, checkpoint);
    } finally {
        endCompletionCheckpoint?.();
    }

    const completed = await io.stat(stagingPath);
    if (completed?.size !== manifest.total_size) {
        throw new Error("large-file staging size mismatch after download");
    }
    if (shouldAbort?.()) throw new LocalEditDuringPull();
    if (!sameTargetState(targetState(await io.stat(path)), effectiveScope.targetState)) {
        throw new LocalEditDuringPull();
    }
    if (preserveExisting && await io.exists(path)) {
        const conflictPath = await uniqueLocalConflictPath(io, path);
        await io.renameFile(path, conflictPath);
        console.warn(
            `[obsetync] preserved unsynced local bytes as ${conflictPath} before pull`,
        );
    }
        await io.replaceFile(stagingPath, path);
        await safeDelete(io, checkpointPath);
    } finally {
        stagingLease.release();
    }
}

async function readLargeCheckpoint(
    io: PlatformIO,
    path: string,
): Promise<LargeTransferCheckpoint | null> {
    try {
        const stat = await io.stat(path);
        if (!stat || !Number.isSafeInteger(stat.size) || stat.size < 0 ||
            stat.size > STAGING_CHECKPOINT_MAX_BYTES) return null;
        return JSON.parse(new TextDecoder().decode(await io.readFile(path))) as LargeTransferCheckpoint;
    } catch {
        return null;
    }
}

function validLargeTargetState(value: unknown): value is LargeTransferTargetState {
    if (!value || typeof value !== "object") return false;
    const candidate = value as Partial<LargeTransferTargetState>;
    return typeof candidate.present === "boolean" &&
        Number.isSafeInteger(candidate.size) && candidate.size! >= 0 &&
        Number.isFinite(candidate.mtime) && candidate.mtime! >= 0 &&
        (candidate.present || (candidate.size === 0 && candidate.mtime === 0));
}

function sameTargetState(left: unknown, right: unknown): boolean {
    return validLargeTargetState(left) && validLargeTargetState(right) &&
        left.present === right.present && left.size === right.size && left.mtime === right.mtime;
}

function validateLargeTransferScope(scope: LargeTransferResumeScope): void {
    if (typeof scope.vaultId !== "string" || scope.vaultId.length === 0 || scope.vaultId.length > 4096 ||
        !/^[0-9a-f]{64}$/.test(scope.rootScope) ||
        !/^[0-9a-f]{64}$/.test(scope.fileGeneration) ||
        !validLargeTargetState(scope.targetState)) {
        throw new TypeError("invalid large-transfer resume scope");
    }
}

function validStoredLargeCheckpoint(
    value: LargeTransferCheckpoint | null,
    transferKey: string,
    partBytes: number,
): value is LargeTransferCheckpoint {
    return value?.version === 2 && typeof value.targetPath === "string" &&
        value.targetPath.length > 0 && value.targetPath.length <= 4096 &&
        value.fileHash === transferKey.slice(0, 64) &&
        /^[0-9a-f]{64}$/.test(value.manifestGeneration) &&
        Number.isSafeInteger(value.totalSize) && value.totalSize >= 0 &&
        value.totalSize <= STAGING_QUOTA_BYTES &&
        Number.isSafeInteger(value.nextChunk) && value.nextChunk >= 0 &&
        Number.isSafeInteger(value.bytesWritten) && value.bytesWritten >= 0 &&
        (value.nextChunk === 0) === (value.bytesWritten === 0) &&
        value.bytesWritten <= value.totalSize && value.bytesWritten === partBytes &&
        (value.completedHash === undefined || value.completedHash === value.fileHash) &&
        typeof value.vaultId === "string" && value.vaultId.length > 0 && value.vaultId.length <= 4096 &&
        /^[0-9a-f]{64}$/.test(value.rootScope) &&
        /^[0-9a-f]{64}$/.test(value.fileGeneration) &&
        validLargeTargetState(value.targetState);
}

function checkedStagingAdd(left: number, right: number): number {
    const total = left + right;
    if (!Number.isSafeInteger(total) || total < 0) {
        throw new LargeTransferStagingQuotaError("staging quota accounting overflow");
    }
    return total;
}

async function deleteValidatedInternalPair(
    io: PlatformIO,
    stagingPath: string,
    checkpointPath: string,
): Promise<void> {
    // Retire authority first: if deleting the part subsequently fails, an
    // orphan temp file is quota-counted but can never look resumable.
    let failed = false;
    for (const path of [checkpointPath, stagingPath]) {
        try { await io.deleteFile(path); }
        catch { if (await io.stat(path)) failed = true; }
    }
    if (failed || await io.stat(stagingPath) || await io.stat(checkpointPath)) {
        throw new LargeTransferStagingQuotaError("obsolete staging cleanup did not retire its pair");
    }
}

/** Reserve the complete eventual pair before creating or appending it.
 * Enumeration is confined to the plugin's transfer directory and rejects any
 * shape that cannot be proven to be a bounded internal checkpoint/part pair. */
async function reserveLargeTransferStaging(
    io: PlatformIO,
    targetPath: string,
    stagingPath: string,
    currentCheckpointPath: string,
    totalSize: number,
    expectedStagingBytes: number | null,
    expectedCheckpointBytes: number | null,
): Promise<StagingLease> {
    if (!Number.isSafeInteger(totalSize) || totalSize < 0) {
        throw new LargeTransferStagingQuotaError("invalid staged transfer size");
    }
    for (const expected of [expectedStagingBytes, expectedCheckpointBytes]) {
        if (expected !== null && (!Number.isSafeInteger(expected) || expected < 0)) {
            throw new LargeTransferStagingQuotaError("invalid current staging evidence");
        }
    }
    const plannedBytes = checkedStagingAdd(totalSize, STAGING_CHECKPOINT_MAX_BYTES);
    return withStagingQuotaLock(async () => {
        if (!io.listDirectory) {
            throw new LargeTransferStagingQuotaError("staging quota cannot be proven without directory listing");
        }
        let targetActive = false;
        for (const active of activeStagingReservations) {
            if (active.io === io && (active.targetPath === targetPath ||
                active.checkpointPath === currentCheckpointPath)) {
                targetActive = true;
                break;
            }
        }
        if (targetActive ||
            activeStagingReservations.size >= STAGING_QUOTA_WAITERS) {
            throw new LargeTransferStagingQuotaError("staging transfer admission is busy");
        }
        let listing: Awaited<ReturnType<NonNullable<PlatformIO["listDirectory"]>>>;
        try {
            listing = await io.listDirectory(TRANSFER_DIR);
        } catch {
            // A clean installation has no transfer directory yet. Creating
            // this exact internal directory and retrying still proves an empty
            // quota; any persistent listing/permission error remains fail-closed.
            try {
                await io.mkdir(TRANSFER_DIR);
                listing = await io.listDirectory(TRANSFER_DIR);
            } catch {
                throw new LargeTransferStagingQuotaError("staging directory cannot be enumerated");
            }
        }
        const entryCount = checkedStagingAdd(listing.files.length, listing.folders.length);
        if (entryCount > STAGING_QUOTA_FILES || listing.folders.length > 0) {
            throw new LargeTransferStagingQuotaError("staging directory entry quota cannot be proven");
        }

        const activePaths = new Set<string>();
        let usedBytes = 0;
        let usedFiles = 0;
        for (const active of activeStagingReservations) {
            if (active.io === io) {
                activePaths.add(active.stagingPath);
                activePaths.add(active.checkpointPath);
            }
            usedBytes = checkedStagingAdd(usedBytes, active.plannedBytes);
            usedFiles = checkedStagingAdd(usedFiles, 2);
        }

        const evidence = new Map<string, number>();
        const observePath = async (path: string): Promise<number> => {
            const known = evidence.get(path);
            if (known !== undefined) return known;
            const stat = await io.stat(path);
            if (!stat || !Number.isSafeInteger(stat.size) || stat.size < 0) {
                throw new LargeTransferStagingQuotaError("internal staging entry size cannot be proven");
            }
            evidence.set(path, stat.size);
            return stat.size;
        };

        const prefix = `${TRANSFER_DIR}/`;
        const pairs = new Map<string, { part?: string; checkpoint?: string }>();
        const seen = new Set<string>();
        const accountOpaque = async (path: string): Promise<void> => {
            usedBytes = checkedStagingAdd(usedBytes, await observePath(path));
            usedFiles = checkedStagingAdd(usedFiles, 1);
        };
        for (const path of listing.files) {
            if (seen.has(path)) {
                throw new LargeTransferStagingQuotaError("duplicate staging directory entry");
            }
            seen.add(path);
            if (!path.startsWith(prefix) || path.slice(prefix.length).includes("/")) {
                throw new LargeTransferStagingQuotaError("staging listing escaped its internal directory");
            }
            if (activePaths.has(path)) continue;
            await observePath(path);
            if (path === stagingPath || path === currentCheckpointPath) continue;
            const name = path.slice(prefix.length);
            const match = /^([0-9a-f]{64}-[0-9a-f]{16})\.(part|checkpoint\.json)$/.exec(name);
            if (!match) {
                // Other internal protocols (for example rename checkpoints)
                // share this directory. Their stat is enough for quota proof;
                // this maintenance pass never reads or deletes them.
                await accountOpaque(path);
                continue;
            }
            const pair = pairs.get(match[1]) ?? {};
            if (match[2] === "part") pair.part = path;
            else pair.checkpoint = path;
            pairs.set(match[1], pair);
        }

        for (const [transferKey, pair] of pairs) {
            if (!pair.part || !pair.checkpoint) {
                await accountOpaque(pair.part ?? pair.checkpoint!);
                continue;
            }
            const partBytes = await observePath(pair.part);
            const checkpointBytes = await observePath(pair.checkpoint);
            const checkpoint = checkpointBytes <= STAGING_CHECKPOINT_MAX_BYTES
                ? await readLargeCheckpoint(io, pair.checkpoint)
                : null;
            if (validStoredLargeCheckpoint(checkpoint, transferKey, partBytes) &&
                checkpoint.targetPath === targetPath) {
                // A different content-addressed key for the exact target is
                // obsolete. Delete only after its checkpoint and part agree.
                await deleteValidatedInternalPair(io, pair.part, pair.checkpoint);
                evidence.delete(pair.part);
                evidence.delete(pair.checkpoint);
                continue;
            }
            // Malformed, orphaned, oversized and other-target entries remain
            // opaque owned bytes. They count against quota but are never GC'd.
            usedBytes = checkedStagingAdd(usedBytes, partBytes);
            usedBytes = checkedStagingAdd(usedBytes, checkpointBytes);
            usedFiles = checkedStagingAdd(usedFiles, 2);
        }

        let confirmed: Awaited<ReturnType<NonNullable<PlatformIO["listDirectory"]>>>;
        try { confirmed = await io.listDirectory(TRANSFER_DIR); }
        catch { throw new LargeTransferStagingQuotaError("staging evidence changed after enumeration"); }
        if (confirmed.folders.length > 0 ||
            checkedStagingAdd(confirmed.files.length, confirmed.folders.length) > STAGING_QUOTA_FILES) {
            throw new LargeTransferStagingQuotaError("staging evidence changed after enumeration");
        }
        const confirmedPaths = new Set<string>();
        for (const path of confirmed.files) {
            if (confirmedPaths.has(path) || !path.startsWith(prefix) ||
                path.slice(prefix.length).includes("/")) {
                throw new LargeTransferStagingQuotaError("staging evidence changed after enumeration");
            }
            confirmedPaths.add(path);
            if (activePaths.has(path)) continue;
            const expected = evidence.get(path);
            const stat = await io.stat(path);
            if (expected === undefined || !stat || stat.size !== expected) {
                throw new LargeTransferStagingQuotaError("staging evidence changed after enumeration");
            }
        }
        for (const path of evidence.keys()) {
            if (!confirmedPaths.has(path)) {
                throw new LargeTransferStagingQuotaError("staging evidence changed after enumeration");
            }
        }
        const confirmedCurrentBytes = (path: string): number | null =>
            confirmedPaths.has(path) ? evidence.get(path) ?? null : null;
        if (confirmedCurrentBytes(stagingPath) !== expectedStagingBytes ||
            confirmedCurrentBytes(currentCheckpointPath) !== expectedCheckpointBytes) {
            throw new LargeTransferStagingQuotaError("current staging pair changed before admission");
        }

        usedBytes = checkedStagingAdd(usedBytes, plannedBytes);
        usedFiles = checkedStagingAdd(usedFiles, 2);
        if (usedBytes > STAGING_QUOTA_BYTES || usedFiles > STAGING_QUOTA_FILES) {
            throw new LargeTransferStagingQuotaError("staging quota is full");
        }
        const reservation = {
            io,
            targetPath,
            stagingPath,
            checkpointPath: currentCheckpointPath,
            plannedBytes,
        };
        activeStagingReservations.add(reservation);
        let released = false;
        return {
            release() {
                if (released) return;
                released = true;
                activeStagingReservations.delete(reservation);
            },
        };
    });
}

async function writeLargeCheckpoint(
    io: PlatformIO,
    path: string,
    checkpoint: LargeTransferCheckpoint,
): Promise<void> {
    const encoded = new TextEncoder().encode(JSON.stringify(checkpoint));
    if (encoded.byteLength > STAGING_CHECKPOINT_MAX_BYTES) {
        throw new LargeTransferStagingQuotaError("large-transfer checkpoint exceeds staging quota");
    }
    await io.writeFile(path, encoded);
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
